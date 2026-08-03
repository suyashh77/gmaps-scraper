"""Persistence layer for scraper runs and review exports."""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from .models import BusinessProfile, ReviewRecord, ScrapeResult
from .utils import utc_now_iso


class SQLiteStorage:
    """Simple SQLite storage for run metadata and deduplicated reviews."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        return self.conn

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "SQLiteStorage":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def _init_schema(self) -> None:
        assert self.conn is not None
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                max_reviews INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                error_message TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS businesses (
                run_id INTEGER PRIMARY KEY,
                query TEXT NOT NULL,
                name TEXT,
                address TEXT,
                phone TEXT,
                website TEXT,
                category TEXT,
                overall_rating REAL,
                total_reviews INTEGER,
                maps_url TEXT,
                captured_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                review_id TEXT NOT NULL,
                reviewer_name TEXT,
                rating INTEGER,
                date_relative TEXT,
                review_text TEXT,
                helpful_count INTEGER DEFAULT 0,
                photo_count INTEGER DEFAULT 0,
                has_owner_response INTEGER DEFAULT 0,
                reviewer_review_count INTEGER DEFAULT 0,
                reviewer_photo_count INTEGER DEFAULT 0,
                is_local_guide INTEGER DEFAULT 0,
                service_type TEXT DEFAULT '',
                scraped_at TEXT NOT NULL,
                UNIQUE(run_id, review_id),
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_run_id ON reviews(run_id)")
        self.conn.commit()

    def start_run(self, query: str, max_reviews: int) -> int:
        assert self.conn is not None
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO runs (query, max_reviews, started_at, status)
            VALUES (?, ?, ?, 'running')
            """,
            (query, max_reviews, utc_now_iso()),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, error_message: str = "") -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            UPDATE runs
            SET ended_at = ?, status = ?, error_message = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), status, error_message[:1000], run_id),
        )
        self.conn.commit()

    def save_business(self, run_id: int, business: BusinessProfile) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT INTO businesses (
                run_id, query, name, address, phone, website, category,
                overall_rating, total_reviews, maps_url, captured_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                query = excluded.query,
                name = excluded.name,
                address = excluded.address,
                phone = excluded.phone,
                website = excluded.website,
                category = excluded.category,
                overall_rating = excluded.overall_rating,
                total_reviews = excluded.total_reviews,
                maps_url = excluded.maps_url,
                captured_at = excluded.captured_at
            """,
            (
                run_id,
                business.query,
                business.name,
                business.address,
                business.phone,
                business.website,
                business.category,
                business.overall_rating,
                business.total_reviews,
                business.maps_url,
                business.captured_at,
            ),
        )
        self.conn.commit()

    def save_reviews_batch(self, run_id: int, reviews: Iterable[ReviewRecord]) -> int:
        assert self.conn is not None
        rows: list[tuple[Any, ...]] = []
        for review in reviews:
            rows.append(
                (
                    run_id,
                    review.review_id,
                    review.reviewer_name,
                    review.rating,
                    review.date_relative,
                    review.review_text,
                    int(review.helpful_count),
                    int(review.photo_count),
                    int(review.has_owner_response),
                    int(review.reviewer_review_count),
                    int(review.reviewer_photo_count),
                    int(review.is_local_guide),
                    review.service_type,
                    review.scraped_at,
                )
            )
        if not rows:
            return 0
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT OR IGNORE INTO reviews (
                run_id, review_id, reviewer_name, rating, date_relative, review_text,
                helpful_count, photo_count, has_owner_response, reviewer_review_count,
                reviewer_photo_count, is_local_guide, service_type, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return int(cur.rowcount)

    def get_review_count(self, run_id: int) -> int:
        assert self.conn is not None
        cur = self.conn.execute("SELECT COUNT(*) AS c FROM reviews WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        return int(row["c"]) if row else 0


def write_scrape_result_json(output_path: str | Path, result: ScrapeResult) -> None:
    """Write full payload with business info and review list."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def write_reviews_jsonl(output_path: str | Path, reviews: Iterable[ReviewRecord]) -> None:
    """Write line-delimited JSON records for streaming-style downstream processing."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for review in reviews:
            f.write(json.dumps(review.to_dict(), ensure_ascii=False))
            f.write("\n")


def write_reviews_csv(output_path: str | Path, reviews: list[ReviewRecord]) -> None:
    """Write flattened review records to CSV."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "query",
        "business_name",
        "review_id",
        "reviewer_name",
        "rating",
        "date_relative",
        "review_text",
        "helpful_count",
        "photo_count",
        "has_owner_response",
        "reviewer_review_count",
        "reviewer_photo_count",
        "is_local_guide",
        "service_type",
        "scraped_at",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for review in reviews:
            writer.writerow(review.to_dict())
