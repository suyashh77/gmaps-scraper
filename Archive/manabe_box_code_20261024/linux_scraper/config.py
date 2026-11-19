"""Configuration with hardcoded defaults — no YAML file needed."""

from __future__ import annotations

import copy
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "browser": {"headless": True, "restart_every_n_stores": 1},
    "workers": {"count": 2, "restart_browser_every": 1, "stagger_s": 45},
    "laptop": {
        "id": 1,
        "viewport_width_min": 1280,
        "viewport_width_max": 1920,
    },
    "auth": {"accounts": []},
    "proxy": {"servers": []},
    "scraping": {
        "max_reviews_per_store": 1_000_000,   # effectively no cap: go for every review Google lists
        "target_all_live": True,
        "batch_save_interval": 50,
        "max_scroll_attempts": 1_000_000,
        "stall_threshold": 60,
        "sort_by_newest": True,
        "no_progress_timeout_s": 600,
        "min_rate_window_s": 900,
        "min_rate_reviews": 20,
        "early_stuck_s": 180,
        "session_swaps_per_store": 1,
        # Off: with signed-out sessions, dropping NID/SOCS made Maps serve the
        # limited view (no Reviews tab) 2/2 times vs 0/2 with them kept.
        "clear_tracking_cookies_on_swap": False,
    },
    "rate_limiting": {
        "min_scroll_delay": 1.5,
        "max_scroll_delay": 3.5,
        "min_delay_between_stores": 3,
        "max_delay_between_stores": 6,
        "idle_pause_every_n_scrolls": 25,
        "idle_pause_min": 1.5,
        "idle_pause_max": 3.0,
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
