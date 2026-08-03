"""Configuration loading from YAML with env overrides and defaults."""

from __future__ import annotations

import os
from typing import Any

import yaml

DEFAULT_CONFIG: dict[str, Any] = {
    "browser": {"headless": False, "restart_every_n_stores": 50},
    "workers": {"count": 4, "restart_browser_every": 50},
    "laptop": {
        "id": 1,
        "viewport_width_min": 1280,
        "viewport_width_max": 1920,
    },
    "database": {"path": "reviews.db"},
    "input": {"file_path": "stores.xlsx"},
    "scraping": {
        "max_reviews_per_store": 1500,
        "batch_save_interval": 50,
        "max_scroll_attempts": 2200,
        "stall_threshold": 45,
        "sort_by_newest": True,
    },
    "rate_limiting": {
        "min_scroll_delay": 0.4,
        "max_scroll_delay": 1.0,
        "min_delay_between_stores": 4,
        "max_delay_between_stores": 9,
        "idle_pause_every_n_scrolls": 10,
        "idle_pause_min": 2.0,
        "idle_pause_max": 5.0,
        "captcha_pause_min": 1800,
        "captcha_pause_max": 3600,
    },
    "retry": {"max_retries": 3, "backoff_multiplier": 2},
    "logging": {"level": "INFO", "file": "scraper.log"},
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _apply_env(config: dict[str, Any]) -> None:
    """Apply env vars on top of config (in-place)."""
    if os.getenv("DATABASE_PATH"):
        config.setdefault("database", {})["path"] = os.environ["DATABASE_PATH"]
    if os.getenv("LOG_LEVEL"):
        config.setdefault("logging", {})["level"] = os.environ["LOG_LEVEL"]
    if os.getenv("HEADLESS", "").lower() in ("1", "true", "yes"):
        config.setdefault("browser", {})["headless"] = True


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """Load YAML config, merge with defaults, then apply env overrides."""
    path = config_path or os.getenv("CONFIG_PATH", "config.yaml")
    if not os.path.exists(path):
        config = dict(DEFAULT_CONFIG)
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError("Config file must contain a top-level mapping")
        config = _deep_merge(DEFAULT_CONFIG, data)
    _apply_env(config)
    return config
