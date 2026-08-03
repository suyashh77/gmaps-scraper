"""
ETA tracker: tracks scraping speed and predicts completion time.

Tracks per-store timing, scroll speed, review collection rate,
and provides live ETA estimates based on exponential moving averages.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any


class ETATracker:
    """
    Tracks scraping performance metrics and predicts ETAs.
    
    Uses exponential moving averages (EMA) to smooth out per-store variance
    and provide increasingly accurate ETAs as more data is collected.
    
    Thread-safe for reading, but each worker should have its own tracker
    and flush to the shared DB periodically.
    """

    def __init__(self, total_stores: int, db_path: str | None = None):
        self.total_stores = total_stores
        self.db_path = db_path
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.total_reviews = 0
        self.start_time = time.time()

        # Per-store timing samples (for EMA)
        self._store_times: list[float] = []        # seconds per store
        self._reviews_per_store: list[int] = []    # reviews collected per store
        self._nav_times: list[float] = []           # navigation time samples
        self._scroll_times: list[float] = []        # scroll/collection time samples

        # EMA smoothing factor (higher = more weight on recent data)
        self._alpha = 0.3

        # Current store tracking
        self._current_store_start: float | None = None
        self._current_nav_start: float | None = None

    # ── Per-store lifecycle ────────────────────────────────────────────────

    def store_started(self) -> None:
        """Call when starting to scrape a new store."""
        self._current_store_start = time.time()
        self._current_nav_start = time.time()

    def navigation_done(self) -> None:
        """Call when navigation to the store page is complete."""
        if self._current_nav_start:
            nav_time = time.time() - self._current_nav_start
            self._nav_times.append(nav_time)
            self._current_nav_start = None

    def store_completed(self, reviews_collected: int, status: str = "completed") -> None:
        """Call when a store is fully processed."""
        if self._current_store_start:
            elapsed = time.time() - self._current_store_start
            self._store_times.append(elapsed)
            self._reviews_per_store.append(reviews_collected)
            self._current_store_start = None

        if status == "completed":
            self.completed += 1
            self.total_reviews += reviews_collected
        elif status == "failed":
            self.failed += 1
        elif status == "skipped":
            self.skipped += 1

    # ── EMA calculation ───────────────────────────────────────────────────

    def _ema(self, values: list[float]) -> float:
        """Exponential moving average — recent values weighted more."""
        if not values:
            return 0.0
        if len(values) == 1:
            return values[0]
        ema = values[0]
        for v in values[1:]:
            ema = self._alpha * v + (1 - self._alpha) * ema
        return ema

    # ── Metrics ───────────────────────────────────────────────────────────

    @property
    def processed(self) -> int:
        return self.completed + self.failed + self.skipped

    @property
    def remaining(self) -> int:
        return max(0, self.total_stores - self.processed)

    @property
    def elapsed_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def avg_seconds_per_store(self) -> float:
        """EMA-smoothed average time per store."""
        if self._store_times:
            return self._ema(self._store_times)
        # Fallback: use total elapsed time
        if self.processed > 0:
            return self.elapsed_seconds / self.processed
        return 120.0  # default estimate: 2 minutes per store

    @property
    def avg_nav_time(self) -> float:
        """Average navigation time to load a store page."""
        return self._ema(self._nav_times) if self._nav_times else 8.0

    @property
    def avg_reviews_per_store(self) -> float:
        """Average reviews collected per completed store."""
        if self._reviews_per_store:
            return self._ema([float(r) for r in self._reviews_per_store])
        return 0.0

    @property
    def reviews_per_second(self) -> float:
        """Overall reviews per second rate."""
        if self.elapsed_seconds > 0 and self.total_reviews > 0:
            return self.total_reviews / self.elapsed_seconds
        return 0.0

    # ── ETA prediction ────────────────────────────────────────────────────

    def eta_seconds(self, num_workers: int = 1) -> float:
        """Predicted seconds remaining, accounting for parallel workers."""
        if self.remaining == 0:
            return 0.0
        per_store = self.avg_seconds_per_store
        # Divide by workers (parallel execution)
        effective_workers = max(1, num_workers)
        return (self.remaining * per_store) / effective_workers

    def eta_datetime(self, num_workers: int = 1) -> datetime:
        """Predicted completion datetime."""
        return datetime.now() + timedelta(seconds=self.eta_seconds(num_workers))

    def eta_string(self, num_workers: int = 1) -> str:
        """Human-readable ETA string."""
        secs = self.eta_seconds(num_workers)
        if secs <= 0:
            return "done"
        hours, rem = divmod(int(secs), 3600)
        minutes, sec = divmod(rem, 60)
        if hours >= 24:
            days, hours = divmod(hours, 24)
            return f"{days}d {hours}h {minutes}m"
        if hours:
            return f"{hours}h {minutes}m"
        if minutes:
            return f"{minutes}m {sec}s"
        return f"{sec}s"

    def elapsed_string(self) -> str:
        """Human-readable elapsed time."""
        secs = self.elapsed_seconds
        hours, rem = divmod(int(secs), 3600)
        minutes, sec = divmod(rem, 60)
        if hours:
            return f"{hours}h {minutes}m {sec}s"
        if minutes:
            return f"{minutes}m {sec}s"
        return f"{sec}s"

    # ── Progress display ──────────────────────────────────────────────────

    def progress_line(self, num_workers: int = 1, current_store: str = "") -> str:
        """One-line progress summary for console output."""
        pct = (self.processed / self.total_stores * 100) if self.total_stores else 0
        eta = self.eta_string(num_workers)
        parts = [
            f"[{self.processed}/{self.total_stores} stores ({pct:.0f}%)]",
            f"Reviews: {self.total_reviews:,}",
            f"Elapsed: {self.elapsed_string()}",
            f"ETA: {eta}",
        ]
        if self.processed > 0:
            parts.append(f"Avg: {self.avg_seconds_per_store:.0f}s/store")
        if current_store:
            parts.append(f"Current: {current_store[:30]}")
        return "  " + " | ".join(parts)

    def summary(self, num_workers: int = 1) -> str:
        """Multi-line final summary."""
        lines = [
            f"  Stores processed: {self.processed}/{self.total_stores}",
            f"    Completed: {self.completed}",
            f"    Failed:    {self.failed}",
            f"    Skipped:   {self.skipped}",
            f"  Total reviews:   {self.total_reviews:,}",
            f"  Total time:      {self.elapsed_string()}",
        ]
        if self.completed > 0:
            lines.append(f"  Avg time/store:  {self.avg_seconds_per_store:.1f}s")
            lines.append(f"  Avg reviews:     {self.avg_reviews_per_store:.0f}/store")
        if self.reviews_per_second > 0:
            lines.append(f"  Review rate:     {self.reviews_per_second:.1f} reviews/sec")
        if self.remaining > 0:
            lines.append(f"  Remaining:       {self.remaining} stores")
            lines.append(f"  ETA:             {self.eta_string(num_workers)} ({self.eta_datetime(num_workers).strftime('%H:%M')})")
        return "\n".join(lines)
