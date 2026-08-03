#!/usr/bin/env python3
"""
Single entry point for the Google Maps review scraper.

Modes:
  run                    Run single-process scraper (processes all pending stores from DB).
  workers                Run multi-worker scraper (N processes claiming from DB).
  query "Search query"   Scrape one place by search query; output optional JSON.
  init [stores.xlsx]     Init DB schema and optionally import stores from Excel/CSV.
  stats [--failed|--sessions|--monitor]  Show stats.
  merge OUT DB1 [DB2...] Merge databases into OUT.
  split FILE N            Split input file into N parts.
"""

import argparse
import asyncio
import json
import os
import sys

# Ensure package is on path when run as script
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from step2_review_extractor.config import load_config
from step2_review_extractor.database import DatabaseManager
from step2_review_extractor.merge import merge_databases
from step2_review_extractor.scraper import GoogleMapsScraper
from step2_review_extractor.splitter import split_input
from step2_review_extractor.worker import main as workers_main


def cmd_run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    db_path = config["database"]["path"]
    with DatabaseManager(db_path) as db:
        db.init_schema()
        scraper = GoogleMapsScraper(config=config, db=db, worker_id=0)
        asyncio.run(scraper.run())


def cmd_workers(args: argparse.Namespace) -> None:
    workers_main(config_path=args.config)


def cmd_query(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    scraper = GoogleMapsScraper(config=config, db=None, worker_id=0)

    async def do() -> None:
        await scraper.start_browser()
        try:
            reviews = await scraper.scrape_query(args.query, max_reviews=min(args.max_reviews, 1500))
            print(f"Collected {len(reviews)} reviews for: {args.query}")
            if args.json_out:
                with open(args.json_out, "w", encoding="utf-8") as f:
                    json.dump(
                        {"query": args.query, "count": len(reviews), "reviews": reviews},
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
                print(f"Saved to {args.json_out}")
        finally:
            await scraper.close_browser()

    asyncio.run(do())


def cmd_init(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    db_path = config["database"]["path"]
    input_file = args.input or config["input"]["file_path"]
    with DatabaseManager(db_path) as db:
        db.init_schema()
        print(f"Database ready: {db_path}")
        if os.path.exists(input_file):
            imported, skipped = db.import_excel(input_file)
            print(f"Imported {imported} stores ({skipped} skipped)")
        else:
            print(f"Input file not found: {input_file}")


def cmd_stats(args: argparse.Namespace) -> None:
    import step2_review_extractor.stats as st
    cfg = load_config(args.config)
    db_path = args.db or cfg["database"]["path"]
    if args.monitor:
        st.monitor_mode(db_path)
    elif args.failed:
        st.show_failed(db_path)
    elif args.sessions:
        st.show_sessions(db_path)
    else:
        st.show_stats(db_path)


def cmd_merge(args: argparse.Namespace) -> None:
    merge_databases(args.output, args.databases)


def cmd_split(args: argparse.Namespace) -> None:
    split_input(args.file, args.parts)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Google Maps review scraper",
        epilog="Commands: run, workers, query, init, stats, merge, split",
    )
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    p_run = sub.add_parser("run", help="Single-process scraper (pending stores from DB)")
    p_run.set_defaults(func=cmd_run)

    # workers
    p_workers = sub.add_parser("workers", help="Multi-worker scraper")
    p_workers.set_defaults(func=cmd_workers)

    # query
    p_query = sub.add_parser("query", help="Scrape one place by search query")
    p_query.add_argument("query", help="Google Maps search query")
    p_query.add_argument("--max-reviews", type=int, default=1500)
    p_query.add_argument("--json-out", type=str, help="Write reviews to JSON file")
    p_query.set_defaults(func=cmd_query)

    # init
    p_init = sub.add_parser("init", help="Init DB and optionally import stores")
    p_init.add_argument("input", nargs="?", help="Stores file (Excel/CSV); default from config")
    p_init.set_defaults(func=cmd_init)

    # stats
    p_stats = sub.add_parser("stats", help="Show DB stats")
    p_stats.add_argument("--db", type=str)
    p_stats.add_argument("--failed", action="store_true")
    p_stats.add_argument("--sessions", action="store_true")
    p_stats.add_argument("--monitor", action="store_true")
    p_stats.set_defaults(func=cmd_stats)

    # merge
    p_merge = sub.add_parser("merge", help="Merge databases into one")
    p_merge.add_argument("output", help="Output database path")
    p_merge.add_argument("databases", nargs="+", help="Input database paths")
    p_merge.set_defaults(func=cmd_merge)

    # split
    p_split = sub.add_parser("split", help="Split input file into N parts")
    p_split.add_argument("file", help="Input Excel/CSV")
    p_split.add_argument("parts", type=int, help="Number of parts")
    p_split.set_defaults(func=cmd_split)

    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
