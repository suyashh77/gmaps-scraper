"""Per-batch review cap.

Every batch built before 2026-10-12 is capped at 1,500 reviews per store. A batch can lift that by
carrying `review_cap` in its metadata table (r09 --cap 0 writes "0"): absent means 1,500, "0" or
"none" means no cap, a number is the cap. Anything unreadable falls back to 1,500 so a bad value can
never turn into an unbounded scrape.
"""

from __future__ import annotations

DEFAULT_CAP = 1500
UNCAPPED = 1_000_000
CAPPED_TIMEOUT_CEILING_MIN = 180        # the budget every capped batch has always had
UNCAPPED_TIMEOUT_CEILING_MIN = 480      # a store of thousands of reviews needs hours, not 3
TIMEOUT_FLOOR_MIN = 60
TIMEOUT_MIN_PER_REVIEW = 2              # unchanged from worker.py: min(180, 60 + effective * 2)


def review_cap(metadata_value) -> int:
    """The cap for a batch from its `review_cap` metadata value (None when the key is absent)."""
    text = str(metadata_value).strip().lower() if metadata_value is not None else ""
    if text in ("0", "none"):
        return UNCAPPED
    if text.isdigit() and int(text) > 0:
        return int(text)
    return DEFAULT_CAP


def store_timeout_minutes(effective: int, cap: int) -> int:
    """Time allowed for one store: a one-hour floor plus two minutes per review still needed,
    up to a ceiling that is 180 minutes for capped batches and 8 hours for uncapped ones."""
    ceiling = CAPPED_TIMEOUT_CEILING_MIN if cap <= DEFAULT_CAP else UNCAPPED_TIMEOUT_CEILING_MIN
    return min(ceiling, TIMEOUT_FLOOR_MIN + effective * TIMEOUT_MIN_PER_REVIEW)
