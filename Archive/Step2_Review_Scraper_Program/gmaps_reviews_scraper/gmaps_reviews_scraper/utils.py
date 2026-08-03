"""Small utility helpers shared across modules."""

from __future__ import annotations

import re
from datetime import datetime, timezone


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp with second precision."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_maps_search_url(query: str) -> str:
    """Build a direct Google Maps URL from free-form query text.

    If the user already provides a full Google Maps URL, use it as-is.
    """
    candidate = query.strip()
    if candidate.lower().startswith(("http://", "https://")):
        return candidate
    normalized = "+".join(candidate.split())
    return f"https://www.google.com/maps/search/{normalized}"


def safe_int(text: str | None, default: int = 0) -> int:
    """Extract first integer from text like '1,234 reviews'."""
    if not text:
        return default
    match = re.search(r"(\d[\d,]*)", text)
    if not match:
        return default
    return int(match.group(1).replace(",", ""))


def parse_rating_from_aria(text: str | None) -> int | None:
    """Extract star rating integer from aria labels like '5 stars'."""
    if not text:
        return None
    match = re.search(r"(\d+)", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def safe_float(text: str | None, default: float | None = None) -> float | None:
    """Parse float value from text with comma or dot decimal separators."""
    if text is None:
        return default
    cleaned = text.strip().replace(",", ".")
    if not cleaned:
        return default
    try:
        return float(cleaned)
    except ValueError:
        return default


def slugify(value: str) -> str:
    """Create filesystem-safe slug from input text."""
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return cleaned or "query"
