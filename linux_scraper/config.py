"""Configuration with hardcoded defaults — no YAML file needed."""

from __future__ import annotations

import copy
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "browser": {"headless": True, "restart_every_n_stores": 50},
    "workers": {"count": 1, "restart_browser_every": 50},
    "laptop": {
        "id": 1,
        "viewport_width_min": 1280,
        "viewport_width_max": 1920,
    },
    "auth": {"accounts": []},
    "scraping": {
        "max_reviews_per_store": 1500,
        "batch_save_interval": 50,
        "max_scroll_attempts": 5000,
        "stall_threshold": 30,
        "sort_by_newest": True,
    },
    "rate_limiting": {
        "min_scroll_delay": 2.0,
        "max_scroll_delay": 6.0,
        "min_delay_between_stores": 5,
        "max_delay_between_stores": 10,
        "idle_pause_every_n_scrolls": 10,
        "idle_pause_min": 2.0,
        "idle_pause_max": 5.0,
        "captcha_pause_min": 1800,
        "captcha_pause_max": 3600,
    },
    "retry": {"max_retries": 4, "backoff_multiplier": 2},
    "logging": {"level": "INFO", "file": "scraper.log"},
    "report_interval_hours": 12,
}


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """Return hardcoded config. No YAML dependency needed."""
    return copy.deepcopy(DEFAULT_CONFIG)
