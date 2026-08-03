"""Data models used by the scraper and storage layer."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .utils import utc_now_iso


@dataclass(slots=True)
class BusinessProfile:
    """Structured snapshot of the target Google Maps business page."""

    query: str
    name: str = ""
    address: str = ""
    phone: str = ""
    website: str = ""
    category: str = ""
    overall_rating: float | None = None
    total_reviews: int = 0
    maps_url: str = ""
    captured_at: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ReviewRecord:
    """One Google Maps review record."""

    query: str
    business_name: str
    review_id: str
    reviewer_name: str = "Anonymous"
    rating: int | None = None
    date_relative: str = ""
    review_text: str = ""
    helpful_count: int = 0
    photo_count: int = 0
    has_owner_response: bool = False
    reviewer_review_count: int = 0
    reviewer_photo_count: int = 0
    is_local_guide: bool = False
    service_type: str = ""
    scraped_at: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["has_owner_response"] = int(self.has_owner_response)
        payload["is_local_guide"] = int(self.is_local_guide)
        return payload


@dataclass(slots=True)
class ScrapeResult:
    """Top-level result for one query run."""

    query: str
    business: BusinessProfile
    reviews: list[ReviewRecord]
    target_reviews: int
    reached_limit: bool
    stopped_reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "target_reviews": self.target_reviews,
            "reached_limit": self.reached_limit,
            "stopped_reason": self.stopped_reason,
            "business": self.business.to_dict(),
            "review_count": len(self.reviews),
            "reviews": [r.to_dict() for r in self.reviews],
        }
