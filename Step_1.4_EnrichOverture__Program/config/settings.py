"""
config/settings.py
==================
Central configuration for the EnrichOverture project.

All paths, timeouts, and scraper tuning knobs live here.
Import this module anywhere you need a setting — never hard-code paths.
"""

import os

# ---------------------------------------------------------------------------
# Directory layout
# ---------------------------------------------------------------------------

# Project root is one level above this file (EnrichOverture/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_DIR              = os.path.join(BASE_DIR, "data", "input")
OUTPUT_DIR             = os.path.join(BASE_DIR, "data", "output")
LOG_DIR                = os.path.join(BASE_DIR, "logs")
DEBUG_SCREENSHOTS_DIR  = os.path.join(BASE_DIR, "debug_screenshots")

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------

DB_PATH  = os.path.join(OUTPUT_DIR, "enrichment.db")   # Main SQLite database
LOG_FILE = os.path.join(LOG_DIR, "app.log")             # Shared log file

# ---------------------------------------------------------------------------
# Browser / scraper settings
# ---------------------------------------------------------------------------

# Set HEADLESS=False to watch the browser during debugging
HEADLESS   = True
TIMEOUT    = 30_000   # Playwright timeout in milliseconds (30 s)
MAX_RETRIES = 3       # Number of times to retry a failed network request
DELAY_MIN  = 2        # Minimum random delay between actions (seconds)
DELAY_MAX  = 5        # Maximum random delay between actions (seconds)

# ---------------------------------------------------------------------------
# Auto-create directories on import
# ---------------------------------------------------------------------------

for _dir in (INPUT_DIR, OUTPUT_DIR, LOG_DIR, DEBUG_SCREENSHOTS_DIR):
    os.makedirs(_dir, exist_ok=True)
