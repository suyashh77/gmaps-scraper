"""Command-line interface for the Google Maps reviews scraper project."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import load_config
from .models import ScrapeResult
from .scraper import CaptchaDetectedError, GoogleMapsReviewsScraper
from .storage import SQLiteStorage, write_reviews_csv, write_reviews_jsonl, write_scrape_result_json
from .utils import slugify


def _configure_logging(config: dict[str, Any]) -> logging.Logger:
    level = getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO)
    logger = logging.getLogger("gmaps_reviews_scraper")
    logger.setLevel(level)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    log_file = Path(config["logging"]["file"])
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    return logger


def _parse_bool_option(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def _read_queries(query: str | None, queries_file: str | None) -> list[str]:
    if query:
        return [query.strip()]

    if not queries_file:
        raise ValueError("Either --query or --queries-file must be supplied.")

    lines = Path(queries_file).read_text(encoding="utf-8").splitlines()
    parsed: list[str] = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parsed.append(line)
    if not parsed:
        raise ValueError(f"No queries found in file: {queries_file}")
    return parsed


def _build_cli_overrides(args: argparse.Namespace) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if args.output_dir:
        sqlite_name = Path("reviews.db").name
        overrides.setdefault("output", {})
        overrides["output"]["directory"] = args.output_dir
        overrides["output"]["sqlite_path"] = str(Path(args.output_dir) / sqlite_name)
    if args.headless is not None:
        overrides.setdefault("browser", {})
        overrides["browser"]["headless"] = args.headless
    if args.log_level:
        overrides.setdefault("logging", {})
        overrides["logging"]["level"] = args.log_level.upper()
    return overrides


def _export_result(config: dict[str, Any], result: ScrapeResult) -> dict[str, str]:
    output_dir = Path(config["output"]["directory"])
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    stem = f"{_query_to_file_stem(result.query)}_{timestamp}"
    written: dict[str, str] = {}

    if bool(config["output"]["write_json"]):
        json_path = output_dir / f"{stem}.json"
        write_scrape_result_json(json_path, result)
        written["json"] = str(json_path)

    if bool(config["output"]["write_jsonl"]):
        jsonl_path = output_dir / f"{stem}.jsonl"
        write_reviews_jsonl(jsonl_path, result.reviews)
        written["jsonl"] = str(jsonl_path)

    if bool(config["output"]["write_csv"]):
        csv_path = output_dir / f"{stem}.csv"
        write_reviews_csv(csv_path, result.reviews)
        written["csv"] = str(csv_path)

    return written


def _query_to_file_stem(query: str) -> str:
    """Build a short, filesystem-safe filename stem for query-derived outputs."""
    slug = slugify(query)
    if len(slug) <= 80:
        return slug
    digest = hashlib.sha1(query.encode("utf-8")).hexdigest()[:10]
    return f"{slug[:60]}_{digest}"


async def _run_all_queries(
    queries: list[str],
    config: dict[str, Any],
    max_reviews: int,
) -> int:
    logger = _configure_logging(config)
    sqlite_path = Path(config["output"]["sqlite_path"])
    output_dir = Path(config["output"]["directory"])
    output_dir.mkdir(parents=True, exist_ok=True)

    completed = 0
    failed = 0
    stop_on_captcha = bool(config.get("runtime", {}).get("stop_on_captcha", True))
    restart_every = int(config["browser"]["restart_every_n_queries"])

    with SQLiteStorage(sqlite_path) as storage:
        scraper = GoogleMapsReviewsScraper(config=config, logger=logger)
        await scraper.start()
        try:
            for index, query in enumerate(queries, start=1):
                logger.info("Running query %s/%s: %s", index, len(queries), query)
                run_id = storage.start_run(query=query, max_reviews=max_reviews)

                try:
                    result = await scraper.scrape_query(query=query, max_reviews=max_reviews)
                    storage.save_business(run_id=run_id, business=result.business)
                    storage.save_reviews_batch(run_id=run_id, reviews=result.reviews)
                    storage.finish_run(run_id=run_id, status="completed")
                    exported_paths = _export_result(config=config, result=result)
                    completed += 1

                    logger.info(
                        "Query completed: '%s' | reviews=%s | stopped_reason=%s",
                        query,
                        len(result.reviews),
                        result.stopped_reason,
                    )
                    for kind, path in exported_paths.items():
                        logger.info("  wrote %s: %s", kind, path)
                except Exception as exc:
                    failed += 1
                    storage.finish_run(run_id=run_id, status="failed", error_message=str(exc))
                    logger.exception("Query failed: '%s' | %s", query, exc)

                    if bool(config["output"]["save_debug_artifacts_on_error"]):
                        debug_stem = f"{_query_to_file_stem(query)}_error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                        artifacts = await scraper.save_debug_artifacts(output_dir=output_dir, file_stem=debug_stem)
                        logger.info("Debug screenshot: %s", artifacts["screenshot"])
                        logger.info("Debug html: %s", artifacts["html"])

                    if isinstance(exc, CaptchaDetectedError) and stop_on_captcha:
                        logger.error("Stopping early because captcha was detected.")
                        break

                if restart_every > 0 and (index % restart_every == 0) and (index < len(queries)):
                    logger.info("Restarting browser after %s queries to keep session fresh.", restart_every)
                    await scraper.restart()
        finally:
            await scraper.close()

    logger.info("Run summary | completed=%s failed=%s sqlite=%s", completed, failed, sqlite_path)
    return 0 if failed == 0 else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Google Maps reviews scraper (query mode, newest-first sorting).",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="YAML config path. If missing, defaults are used.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--query", help='Single query, for example: "Food Lion Tryon Road Raleigh"')
    group.add_argument("--queries-file", help="Text file containing one query per line.")
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=1500,
        help="Per-query cap (1..1500).",
    )
    parser.add_argument("--output-dir", help="Override output directory.")
    parser.add_argument(
        "--headless",
        type=_parse_bool_option,
        default=None,
        help="Override headless mode: true or false.",
    )
    parser.add_argument("--log-level", help="Override log level, e.g. INFO or DEBUG.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.max_reviews < 1 or args.max_reviews > 1500:
        parser.error("--max-reviews must be between 1 and 1500.")

    queries = _read_queries(args.query, args.queries_file)
    overrides = _build_cli_overrides(args)
    config = load_config(config_path=args.config, cli_overrides=overrides)
    return asyncio.run(_run_all_queries(queries=queries, config=config, max_reviews=args.max_reviews))


if __name__ == "__main__":
    raise SystemExit(main())
