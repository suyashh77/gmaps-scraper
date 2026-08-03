"""Configuration loading and validation for the scraper project."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG: dict[str, Any] = {
    "browser": {
        "headless": True,
        "navigation_timeout_ms": 45_000,
        "action_timeout_ms": 6_000,
        "restart_every_n_queries": 0,
        "locale": "en-US",
        "timezone_id": "America/New_York",
        "viewport_width_min": 1280,
        "viewport_width_max": 1920,
        "viewport_height_min": 800,
        "viewport_height_max": 1080,
        "user_agents": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ],
    },
    "scraping": {
        "max_reviews_per_query": 1500,
        "sort_by_newest": True,
        "max_scroll_attempts": 2200,
        "stall_threshold": 45,
        "scroll_delta_min": 2800,
        "scroll_delta_max": 6200,
        "min_scroll_delay_sec": 0.4,
        "max_scroll_delay_sec": 1.0,
        "max_query_retries": 2,
        "retry_backoff_sec": 3.0,
    },
    "output": {
        "directory": "outputs",
        "sqlite_path": "outputs/reviews.db",
        "write_json": True,
        "write_jsonl": True,
        "write_csv": True,
        "save_debug_artifacts_on_error": True,
    },
    "logging": {
        "level": "INFO",
        "file": "gmaps_scraper.log",
    },
    "runtime": {
        "stop_on_captcha": True,
        "captcha_keywords": [
            "unusual traffic",
            "automated queries",
            "not a robot",
            "captcha",
            "recaptcha",
            "sorry/index",
            "systems have detected",
        ],
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge two mappings."""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _parse_bool(value: str | bool | None, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _apply_env(config: dict[str, Any]) -> None:
    """Apply optional environment overrides in-place.

    Supported keys:
    - GMAPS_HEADLESS=true|false
    - GMAPS_LOG_LEVEL=DEBUG|INFO|...
    - GMAPS_OUTPUT_DIR=path
    """
    headless = os.getenv("GMAPS_HEADLESS")
    if headless is not None:
        config["browser"]["headless"] = _parse_bool(headless, config["browser"]["headless"])

    log_level = os.getenv("GMAPS_LOG_LEVEL")
    if log_level:
        config["logging"]["level"] = str(log_level).upper()

    out_dir = os.getenv("GMAPS_OUTPUT_DIR")
    if out_dir:
        config["output"]["directory"] = out_dir
        default_sqlite = Path(config["output"]["sqlite_path"]).name
        config["output"]["sqlite_path"] = str(Path(out_dir) / default_sqlite)


def _validate(config: dict[str, Any]) -> None:
    """Validate required invariants and fail fast on invalid values."""
    max_reviews = int(config["scraping"]["max_reviews_per_query"])
    if max_reviews < 1 or max_reviews > 1500:
        raise ValueError("scraping.max_reviews_per_query must be between 1 and 1500")

    max_scroll = int(config["scraping"]["max_scroll_attempts"])
    if max_scroll < 1:
        raise ValueError("scraping.max_scroll_attempts must be >= 1")

    stall_threshold = int(config["scraping"]["stall_threshold"])
    if stall_threshold < 1:
        raise ValueError("scraping.stall_threshold must be >= 1")

    min_delay = float(config["scraping"]["min_scroll_delay_sec"])
    max_delay = float(config["scraping"]["max_scroll_delay_sec"])
    if min_delay < 0 or max_delay < min_delay:
        raise ValueError("scraping scroll delay bounds are invalid")

    viewport_min = int(config["browser"]["viewport_width_min"])
    viewport_max = int(config["browser"]["viewport_width_max"])
    if viewport_min < 800 or viewport_max < viewport_min:
        raise ValueError("browser viewport width bounds are invalid")

    if not config["browser"].get("user_agents"):
        raise ValueError("browser.user_agents must contain at least one value")


def load_config(
    config_path: str | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load config file, merge with defaults, apply env + CLI overrides, validate."""
    merged = deepcopy(DEFAULT_CONFIG)
    path = config_path or "config.yaml"
    cfg_file = Path(path)
    if cfg_file.exists():
        loaded = yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Config file must be a mapping: {cfg_file}")
        merged = _deep_merge(merged, loaded)

    _apply_env(merged)
    if cli_overrides:
        merged = _deep_merge(merged, cli_overrides)

    _validate(merged)
    return merged
