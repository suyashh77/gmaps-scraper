"""
SQLite layer for stores and reviews: schema, import, worker-safe claiming, batch saves.
"""

from __future__ import annotations

import os
import random
import re
import sqlite3
import time
from datetime import datetime
from typing import Any, Iterable, Sequence

import pandas as pd

from .config import load_config


class DatabaseManager:
    """SQLite manager with worker-safe store claiming and review deduplication."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> DatabaseManager:
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stores (
                store_id INTEGER PRIMARY KEY AUTOINCREMENT,
                input_name TEXT,
                input_street TEXT,
                input_city TEXT,
                input_state TEXT,
                input_zip TEXT,
                input_lat REAL,
                input_lon REAL,
                google_place_id TEXT UNIQUE,
                gmaps_name TEXT,
                gmaps_address TEXT,
                gmaps_phone TEXT,
                gmaps_website TEXT,
                gmaps_rating REAL,
                gmaps_reviews_count INTEGER,
                gmaps_lat REAL,
                gmaps_lon REAL,
                gmaps_category TEXT,
                gmaps_price_level TEXT,
                status TEXT DEFAULT 'pending' CHECK(status IN ('pending','in_progress','completed','failed','skipped')),
                worker_id INTEGER,
                reviews_scraped INTEGER DEFAULT 0,
                target_reviews INTEGER DEFAULT 0,
                error_message TEXT,
                attempts INTEGER DEFAULT 0,
                last_attempt DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER NOT NULL,
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
                scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(store_id, review_id),
                FOREIGN KEY (store_id) REFERENCES stores(store_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scrape_sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                ended_at DATETIME,
                stores_processed INTEGER DEFAULT 0,
                stores_completed INTEGER DEFAULT 0,
                stores_failed INTEGER DEFAULT 0,
                reviews_collected INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running' CHECK(status IN ('running','completed','interrupted'))
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_status ON stores(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_worker ON stores(worker_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_place_id ON stores(google_place_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_store_id ON reviews(store_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_store_review ON reviews(store_id, review_id)")
        self.conn.commit()

    @staticmethod
    def _find_col(df: pd.DataFrame, variants: Sequence[str]) -> str | None:
        cols = {c.lower(): c for c in df.columns}
        for variant in variants:
            if variant.lower() in cols:
                return cols[variant.lower()]
        return None

    def import_excel(self, file_path: str) -> tuple[int, int]:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file not found: {file_path}")
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext in (".xls", ".xlsx"):
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        df.columns = [c.strip().lower() for c in df.columns]
        mapping = {
            "input_name": self._find_col(df, ["input_name", "business_name", "name"]),
            "input_street": self._find_col(df, ["input_street", "street", "address"]),
            "input_city": self._find_col(df, ["input_city", "city"]),
            "input_state": self._find_col(df, ["input_state", "state"]),
            "input_zip": self._find_col(df, ["input_zip", "zip", "zipcode", "zip_code"]),
            "input_lat": self._find_col(df, ["input_lat", "lat", "latitude"]),
            "input_lon": self._find_col(df, ["input_lon", "lon", "lng", "long", "longitude"]),
            "google_place_id": self._find_col(df, ["google_place_id", "place_id"]),
            "gmaps_name": self._find_col(df, ["gmaps_name"]),
            "gmaps_address": self._find_col(df, ["gmaps_address"]),
            "gmaps_phone": self._find_col(df, ["gmaps_phone"]),
            "gmaps_website": self._find_col(df, ["gmaps_website"]),
            "gmaps_rating": self._find_col(df, ["gmaps_rating"]),
            "gmaps_reviews_count": self._find_col(df, ["gmaps_reviews_count"]),
            "gmaps_lat": self._find_col(df, ["gmaps_lat"]),
            "gmaps_lon": self._find_col(df, ["gmaps_lon"]),
            "gmaps_category": self._find_col(df, ["gmaps_category"]),
            "gmaps_price_level": self._find_col(df, ["gmaps_price_level"]),
        }

        imported = 0
        skipped = 0
        cur = self.conn.cursor()

        for _, row in df.iterrows():
            values: dict[str, Any] = {}
            for key, src_col in mapping.items():
                val = row[src_col] if src_col and src_col in df.columns else None
                values[key] = None if pd.isna(val) else val

            if not values.get("google_place_id"):
                lat = values.get("input_lat") or values.get("gmaps_lat")
                lon = values.get("input_lon") or values.get("gmaps_lon")
                name = str(values.get("input_name") or values.get("gmaps_name") or "")
                if lat and lon:
                    values["google_place_id"] = f"geo_{lat}_{lon}"
                else:
                    values["google_place_id"] = f"name_{re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')}"

            gmaps_count = values.get("gmaps_reviews_count")
            try:
                parsed_count = int(gmaps_count) if gmaps_count is not None else 0
            except (TypeError, ValueError):
                parsed_count = 0
            values["target_reviews"] = min(max(parsed_count, 1), 1500) if parsed_count else 1500

            for float_col in ["input_lat", "input_lon", "gmaps_lat", "gmaps_lon", "gmaps_rating"]:
                if values.get(float_col) is not None:
                    try:
                        values[float_col] = float(values[float_col])
                    except (TypeError, ValueError):
                        values[float_col] = None

            if values.get("gmaps_reviews_count") is not None:
                try:
                    values["gmaps_reviews_count"] = int(values["gmaps_reviews_count"])
                except (TypeError, ValueError):
                    values["gmaps_reviews_count"] = None

            for txt_col in [
                "input_name", "input_street", "input_city", "input_state", "input_zip",
                "google_place_id", "gmaps_name", "gmaps_address", "gmaps_phone", "gmaps_website",
                "gmaps_category", "gmaps_price_level",
            ]:
                if values.get(txt_col) is not None:
                    values[txt_col] = str(values[txt_col])

            cur.execute(
                """
                INSERT OR IGNORE INTO stores (
                    input_name, input_street, input_city, input_state, input_zip,
                    input_lat, input_lon, google_place_id,
                    gmaps_name, gmaps_address, gmaps_phone, gmaps_website,
                    gmaps_rating, gmaps_reviews_count, gmaps_lat, gmaps_lon,
                    gmaps_category, gmaps_price_level, target_reviews
                ) VALUES (
                    :input_name, :input_street, :input_city, :input_state, :input_zip,
                    :input_lat, :input_lon, :google_place_id,
                    :gmaps_name, :gmaps_address, :gmaps_phone, :gmaps_website,
                    :gmaps_rating, :gmaps_reviews_count, :gmaps_lat, :gmaps_lon,
                    :gmaps_category, :gmaps_price_level, :target_reviews
                )
                """,
                values,
            )
            if cur.rowcount > 0:
                imported += 1
            else:
                skipped += 1

        self.conn.commit()
        return imported, skipped

    def get_pending_stores(self) -> list[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM stores WHERE status IN ('pending','in_progress') ORDER BY store_id")
        return cur.fetchall()

    def claim_next_store(self, worker_id: int, max_retries: int = 3) -> sqlite3.Row | None:
        now = datetime.now().isoformat()
        for _ in range(6):
            try:
                cur = self.conn.cursor()
                cur.execute("BEGIN IMMEDIATE")
                cur.execute(
                    """
                    SELECT store_id FROM stores
                    WHERE status = 'pending' AND attempts < ?
                    ORDER BY store_id LIMIT 1
                    """,
                    (max_retries,),
                )
                row = cur.fetchone()
                if not row:
                    self.conn.commit()
                    return None
                store_id = row[0]
                cur.execute(
                    """
                    UPDATE stores
                    SET status='in_progress', worker_id=?, attempts=attempts+1, last_attempt=?, updated_at=?
                    WHERE store_id=?
                    """,
                    (worker_id, now, now, store_id),
                )
                self.conn.commit()
                cur.execute("SELECT * FROM stores WHERE store_id=?", (store_id,))
                return cur.fetchone()
            except sqlite3.OperationalError as exc:
                if "database is locked" in str(exc).lower():
                    time.sleep(random.uniform(0.08, 0.25))
                    continue
                raise
        return None

    def mark_store_in_progress(self, store_id: int, worker_id: int | None = None) -> None:
        now = datetime.now().isoformat()
        self.conn.execute(
            """
            UPDATE stores
            SET status='in_progress', worker_id=?, attempts=attempts+1, last_attempt=?, updated_at=?
            WHERE store_id=?
            """,
            (worker_id, now, now, store_id),
        )
        self.conn.commit()

    def mark_store_completed(self, store_id: int, reviews_scraped: int) -> None:
        self.conn.execute(
            """
            UPDATE stores
            SET status='completed', reviews_scraped=?, error_message=NULL, updated_at=?
            WHERE store_id=?
            """,
            (reviews_scraped, datetime.now().isoformat(), store_id),
        )
        self.conn.commit()

    def mark_store_failed(self, store_id: int, error_message: str) -> None:
        self.conn.execute(
            """
            UPDATE stores
            SET status='failed', error_message=?, updated_at=?
            WHERE store_id=?
            """,
            (str(error_message)[:500], datetime.now().isoformat(), store_id),
        )
        self.conn.commit()

    def mark_store_skipped(self, store_id: int, reason: str) -> None:
        self.conn.execute(
            """
            UPDATE stores
            SET status='skipped', error_message=?, updated_at=?
            WHERE store_id=?
            """,
            (reason[:500], datetime.now().isoformat(), store_id),
        )
        self.conn.commit()

    def reset_interrupted_stores(self) -> int:
        cur = self.conn.cursor()
        cur.execute("UPDATE stores SET status='pending', worker_id=NULL WHERE status='in_progress'")
        count = cur.rowcount
        self.conn.commit()
        return count

    def release_worker_stores(self, worker_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE stores SET status='pending', worker_id=NULL WHERE status='in_progress' AND worker_id=?",
            (worker_id,),
        )
        count = cur.rowcount
        self.conn.commit()
        return count

    def update_store_reviews_count(self, store_id: int, count: int) -> None:
        self.conn.execute(
            "UPDATE stores SET reviews_scraped=?, updated_at=? WHERE store_id=?",
            (count, datetime.now().isoformat(), store_id),
        )
        self.conn.commit()

    def get_existing_review_ids(self, store_id: int) -> set[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT review_id FROM reviews WHERE store_id=?", (store_id,))
        return {row[0] for row in cur.fetchall()}

    def save_reviews_batch(self, store_id: int, reviews: Iterable[dict[str, Any]]) -> int:
        rows = []
        for r in reviews:
            if not r or not r.get("review_id"):
                continue
            rows.append(
                (
                    store_id,
                    r.get("review_id"),
                    r.get("reviewer_name", "Anonymous"),
                    r.get("rating"),
                    r.get("date_relative", ""),
                    r.get("review_text", ""),
                    int(r.get("helpful_count", 0) or 0),
                    int(r.get("photo_count", 0) or 0),
                    int(r.get("has_owner_response", 0) or 0),
                    int(r.get("reviewer_review_count", 0) or 0),
                    int(r.get("reviewer_photo_count", 0) or 0),
                    int(r.get("is_local_guide", 0) or 0),
                    r.get("service_type", ""),
                )
            )
        if not rows:
            return 0
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT OR IGNORE INTO reviews (
                store_id, review_id, reviewer_name, rating,
                date_relative, review_text, helpful_count,
                photo_count, has_owner_response, reviewer_review_count,
                reviewer_photo_count, is_local_guide, service_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return cur.rowcount

    def get_review_count(self, store_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM reviews WHERE store_id=?", (store_id,))
        return cur.fetchone()[0]

    def start_session(self) -> int:
        cur = self.conn.cursor()
        cur.execute("INSERT INTO scrape_sessions DEFAULT VALUES")
        self.conn.commit()
        return cur.lastrowid

    def end_session(
        self,
        session_id: int,
        stores_processed: int,
        stores_completed: int,
        stores_failed: int,
        reviews_collected: int,
        status: str = "completed",
    ) -> None:
        self.conn.execute(
            """
            UPDATE scrape_sessions
            SET ended_at=?, stores_processed=?, stores_completed=?, stores_failed=?, reviews_collected=?, status=?
            WHERE session_id=?
            """,
            (
                datetime.now().isoformat(),
                stores_processed,
                stores_completed,
                stores_failed,
                reviews_collected,
                status,
                session_id,
            ),
        )
        self.conn.commit()
