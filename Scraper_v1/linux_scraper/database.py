"""
SQLite database layer for stores and reviews.
Uses WAL mode + busy timeout for safe multi-worker concurrent access.

Key features:
  - WAL mode           → multiple readers, one writer at a time
  - busy_timeout=30s   → auto-retry on SQLITE_BUSY instead of crashing
  - BEGIN IMMEDIATE     → claim stores without race conditions
  - executemany()       → fast bulk review inserts
  - metadata table      → machine_id for merge traceability
"""

from __future__ import annotations

import os
import re
import sqlite3
import string
import random
import time
from datetime import datetime
from typing import Any, Iterable, Sequence


# ── helpers ──────────────────────────────────────────────────────────────────

def _row_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """Row factory that returns dict-like rows (similar to psycopg2 RealDictCursor)."""
    cols = [desc[0] for desc in cursor.description]
    return dict(zip(cols, row))


def _generate_machine_id(length: int = 6) -> str:
    """Generate a short random machine ID (e.g. 'a1b2c3')."""
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def get_or_create_machine_id(project_dir: str) -> str:
    """Get the machine ID from machine_id.txt, or create one if it doesn't exist."""
    id_file = os.path.join(project_dir, "machine_id.txt")
    if os.path.exists(id_file):
        with open(id_file, "r") as f:
            machine_id = f.read().strip()
            if machine_id:
                return machine_id
    machine_id = _generate_machine_id()
    with open(id_file, "w") as f:
        f.write(machine_id)
    return machine_id


# ── main class ────────────────────────────────────────────────────────────────

class DatabaseManager:
    """
    SQLite manager with worker-safe store claiming and review deduplication.

    Usage:
        with DatabaseManager(db_path) as db:
            db.init_schema()
            store = db.claim_next_store(worker_id=1)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None

    # ── connection ────────────────────────────────────────────────────────────

    def connect(self) -> sqlite3.Connection:
        dirname = os.path.dirname(self.db_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.row_factory = _row_factory
        # Enable WAL mode for concurrent read/write access
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        return self.conn

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def __enter__(self) -> DatabaseManager:
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # ── schema ────────────────────────────────────────────────────────────────

    def init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                store_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                machine_id        TEXT DEFAULT '',
                input_name        TEXT,
                input_street      TEXT,
                input_city        TEXT,
                input_state       TEXT,
                input_zip         TEXT,
                input_lat         REAL,
                input_lon         REAL,
                google_place_id   TEXT UNIQUE,
                gmaps_name        TEXT,
                gmaps_address     TEXT,
                gmaps_phone       TEXT,
                gmaps_website     TEXT,
                gmaps_rating      REAL,
                gmaps_reviews_count INTEGER,
                gmaps_lat         REAL,
                gmaps_lon         REAL,
                gmaps_category    TEXT,
                gmaps_price_level TEXT,
                status            TEXT DEFAULT 'pending'
                                  CHECK(status IN ('pending','in_progress','completed','failed','skipped')),
                worker_id         INTEGER,
                reviews_scraped   INTEGER DEFAULT 0,
                target_reviews    INTEGER DEFAULT 0,
                master_reviews    INTEGER DEFAULT 0,
                error_message     TEXT,
                attempts          INTEGER DEFAULT 0,
                last_attempt      TEXT,
                created_at        TEXT DEFAULT (datetime('now')),
                updated_at        TEXT DEFAULT (datetime('now'))
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id              INTEGER NOT NULL REFERENCES stores(store_id),
                machine_id            TEXT DEFAULT '',
                review_id             TEXT NOT NULL,
                reviewer_name         TEXT,
                rating                INTEGER,
                date_relative         TEXT,
                review_text           TEXT,
                helpful_count         INTEGER DEFAULT 0,
                photo_count           INTEGER DEFAULT 0,
                has_owner_response    INTEGER DEFAULT 0,
                reviewer_review_count INTEGER DEFAULT 0,
                reviewer_photo_count  INTEGER DEFAULT 0,
                is_local_guide        INTEGER DEFAULT 0,
                service_type          TEXT DEFAULT '',
                scraped_at            TEXT DEFAULT (datetime('now')),
                UNIQUE(store_id, review_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scrape_sessions (
                session_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at        TEXT DEFAULT (datetime('now')),
                ended_at          TEXT,
                stores_processed  INTEGER DEFAULT 0,
                stores_completed  INTEGER DEFAULT 0,
                stores_failed     INTEGER DEFAULT 0,
                reviews_collected INTEGER DEFAULT 0,
                status            TEXT DEFAULT 'running'
                                  CHECK(status IN ('running','completed','interrupted'))
            )
        """)
        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_status   ON stores(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_worker   ON stores(worker_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_place_id ON stores(google_place_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_store   ON reviews(store_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_dedup   ON reviews(store_id, review_id)")
        self.conn.commit()

    def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata key-value pair."""
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (key, value),
        )
        self.conn.commit()

    def get_metadata(self, key: str) -> str | None:
        """Get a metadata value by key."""
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM metadata WHERE key=?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    # ── import ────────────────────────────────────────────────────────────────

    @staticmethod
    def _find_col(df: Any, variants: Sequence[str]) -> str | None:
        cols = {c.lower(): c for c in df.columns}
        for variant in variants:
            if variant.lower() in cols:
                return cols[variant.lower()]
        return None

    def import_csv(
        self,
        file_path: str,
        machine_id: str = "",
        partition: tuple[int, int] | None = None,
    ) -> tuple[int, int, dict[str, int]]:
        """Import stores from CSV or Excel file into the database.

        Parameters
        ----------
        partition : (machine_number, total_machines) — 1-indexed.
                    e.g. (1, 3) imports rows where row_index % 3 == 0.

        Returns (imported, skipped, type_counts) where type_counts has
        keys 'fresh' and 'incomplete'.

        Recognises master DB export columns:
          - reviews_scraped / reviews_count → master_reviews (already collected)
          - target_reviews                  → target_reviews
          - scrape_status                   → used to skip 'complete' stores
        """
        import pandas as pd

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file not found: {file_path}")
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            try:
                df = pd.read_csv(file_path, encoding="utf-8-sig")
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding="latin-1")
        elif ext in (".xls", ".xlsx"):
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        df.columns = [c.strip().lower() for c in df.columns]

        if len(df) == 0:
            raise ValueError("File contains no data rows — check that the file is not empty.")

        # Apply partition filter: keep only this machine's slice
        if partition:
            machine_num, total_machines = partition
            # 1-indexed: machine 1 gets rows 0,3,6,…; machine 2 gets 1,4,7,…
            df = df.iloc[[(i % total_machines) == (machine_num - 1) for i in range(len(df))]]
            df = df.reset_index(drop=True)

        mapping = {
            "input_name":           self._find_col(df, ["input_name", "store_name", "business_name", "name"]),
            "input_street":         self._find_col(df, ["input_street", "street", "address"]),
            "input_city":           self._find_col(df, ["input_city", "city"]),
            "input_state":          self._find_col(df, ["input_state", "state"]),
            "input_zip":            self._find_col(df, ["input_zip", "zip", "zipcode", "zip_code"]),
            "input_lat":            self._find_col(df, ["input_lat", "lat", "latitude", "source_lat"]),
            "input_lon":            self._find_col(df, ["input_lon", "lon", "lng", "long", "longitude", "source_lon"]),
            "google_place_id":      self._find_col(df, ["google_place_id", "place_id"]),
            "gmaps_name":           self._find_col(df, ["gmaps_name", "outscraper_name"]),
            "gmaps_address":        self._find_col(df, ["gmaps_address", "full_address", "outscraper_address"]),
            "gmaps_phone":          self._find_col(df, ["gmaps_phone", "phone"]),
            "gmaps_website":        self._find_col(df, ["gmaps_website"]),
            "gmaps_rating":         self._find_col(df, ["gmaps_rating", "rating"]),
            "gmaps_reviews_count":  self._find_col(df, ["gmaps_reviews_count", "target_reviews"]),
            "gmaps_lat":            self._find_col(df, ["gmaps_lat"]),
            "gmaps_lon":            self._find_col(df, ["gmaps_lon"]),
            "gmaps_category":       self._find_col(df, ["gmaps_category", "category"]),
            "gmaps_price_level":    self._find_col(df, ["gmaps_price_level"]),
        }

        # Master DB export columns
        master_reviews_col = self._find_col(df, ["reviews_scraped", "reviews_count"])
        scrape_status_col = self._find_col(df, ["scrape_status"])
        target_reviews_col = self._find_col(df, ["target_reviews"])

        imported = skipped = 0
        type_counts = {"fresh": 0, "incomplete": 0}
        cur = self.conn.cursor()
        try:
            for row_idx, (_, row) in enumerate(df.iterrows()):
                values: dict[str, Any] = {}
                for key, src_col in mapping.items():
                    val = row[src_col] if src_col and src_col in df.columns else None
                    values[key] = None if pd.isna(val) else val

                # Skip rows with no identifying information at all
                if not values.get("google_place_id") and not values.get("input_name") and not values.get("gmaps_name"):
                    skipped += 1
                    continue

                # Skip stores already marked 'complete' in master export
                if scrape_status_col and scrape_status_col in df.columns:
                    status_val = row[scrape_status_col]
                    if not pd.isna(status_val) and str(status_val).lower() == "complete":
                        skipped += 1
                        continue

                # Synthesise a place_id if missing
                if not values.get("google_place_id"):
                    lat  = values.get("input_lat") or values.get("gmaps_lat")
                    lon  = values.get("input_lon") or values.get("gmaps_lon")
                    name = str(values.get("input_name") or values.get("gmaps_name") or "")
                    if lat and lon:
                        values["google_place_id"] = f"geo_{lat}_{lon}"
                    else:
                        values["google_place_id"] = (
                            f"name_{re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')}_{row_idx}"
                        )

                # ── master_reviews: how many reviews already exist in master DB ──
                master_rev = 0
                if master_reviews_col and master_reviews_col in df.columns:
                    mr_val = row[master_reviews_col]
                    if not pd.isna(mr_val):
                        try:
                            master_rev = int(mr_val)
                        except (TypeError, ValueError):
                            master_rev = 0
                values["master_reviews"] = master_rev

                # ── target_reviews: use explicit target column, else gmaps_reviews_count ──
                target = 0
                if target_reviews_col and target_reviews_col in df.columns:
                    tv = row[target_reviews_col]
                    if not pd.isna(tv):
                        try:
                            target = int(tv)
                        except (TypeError, ValueError):
                            target = 0
                if target == 0:
                    gmaps_count = values.get("gmaps_reviews_count")
                    try:
                        target = int(gmaps_count) if gmaps_count is not None else 0
                    except (TypeError, ValueError):
                        target = 0
                # No artificial cap — use the real target from master DB
                values["target_reviews"] = max(target, 1)

                # Track store type
                if master_rev > 0:
                    type_counts["incomplete"] += 1
                else:
                    type_counts["fresh"] += 1

                # Machine ID for merge traceability
                values["machine_id"] = machine_id

                # Type coercions
                for col in ["input_lat", "input_lon", "gmaps_lat", "gmaps_lon", "gmaps_rating"]:
                    if values.get(col) is not None:
                        try:
                            values[col] = float(values[col])
                        except (TypeError, ValueError):
                            values[col] = None
                if values.get("gmaps_reviews_count") is not None:
                    try:
                        values["gmaps_reviews_count"] = int(values["gmaps_reviews_count"])
                    except (TypeError, ValueError):
                        values["gmaps_reviews_count"] = None
                for col in [
                    "input_name", "input_street", "input_city", "input_state", "input_zip",
                    "google_place_id", "gmaps_name", "gmaps_address", "gmaps_phone",
                    "gmaps_website", "gmaps_category", "gmaps_price_level",
                ]:
                    if values.get(col) is not None:
                        values[col] = str(values[col])

                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO stores (
                            machine_id, input_name, input_street, input_city, input_state, input_zip,
                            input_lat, input_lon, google_place_id,
                            gmaps_name, gmaps_address, gmaps_phone, gmaps_website,
                            gmaps_rating, gmaps_reviews_count, gmaps_lat, gmaps_lon,
                            gmaps_category, gmaps_price_level, target_reviews, master_reviews
                        ) VALUES (
                            :machine_id, :input_name, :input_street, :input_city, :input_state, :input_zip,
                            :input_lat, :input_lon, :google_place_id,
                            :gmaps_name, :gmaps_address, :gmaps_phone, :gmaps_website,
                            :gmaps_rating, :gmaps_reviews_count, :gmaps_lat, :gmaps_lon,
                            :gmaps_category, :gmaps_price_level, :target_reviews, :master_reviews
                        )
                    """, values)
                    if cur.rowcount > 0:
                        imported += 1
                    else:
                        skipped += 1
                except sqlite3.IntegrityError:
                    skipped += 1
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return imported, skipped, type_counts

    # ── store queue ───────────────────────────────────────────────────────────

    def get_pending_stores(self) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT * FROM stores WHERE status IN ('pending','in_progress') ORDER BY store_id"
        )
        return cur.fetchall()

    def claim_next_store(self, worker_id: int, max_retries: int = 3) -> dict | None:
        """
        Atomically claim the next pending store using BEGIN IMMEDIATE.
        SQLite equivalent of PostgreSQL's SELECT FOR UPDATE SKIP LOCKED.
        Uses retry with backoff on SQLITE_BUSY.

        Priority order:
          1. Incomplete stores (master_reviews > 0) — closer to done
          2. Fresh stores (master_reviews = 0) — full scrape needed
        Within each group, smallest effective target first.
        """
        now = datetime.now().isoformat()
        for attempt in range(5):
            try:
                cur = self.conn.cursor()
                cur.execute("BEGIN IMMEDIATE")
                cur.execute("""
                    SELECT store_id FROM stores
                    WHERE status = 'pending' AND attempts < ?
                    ORDER BY
                        CASE WHEN master_reviews > 0 THEN 0 ELSE 1 END,
                        (target_reviews - master_reviews) ASC,
                        store_id
                    LIMIT 1
                """, (max_retries,))
                row = cur.fetchone()
                if not row:
                    self.conn.commit()
                    return None
                store_id = row["store_id"]
                cur.execute("""
                    UPDATE stores
                    SET status='in_progress', worker_id=?,
                        attempts=attempts+1, last_attempt=?, updated_at=?
                    WHERE store_id=?
                """, (worker_id, now, now, store_id))
                self.conn.commit()
                # Fetch full row
                cur.execute("SELECT * FROM stores WHERE store_id=?", (store_id,))
                return cur.fetchone()
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() or "busy" in str(e).lower():
                    self.conn.rollback()
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise
        return None

    def mark_store_in_progress(self, store_id: int, worker_id: int | None = None) -> None:
        now = datetime.now().isoformat()
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE stores
            SET status='in_progress', worker_id=?,
                attempts=attempts+1, last_attempt=?, updated_at=?
            WHERE store_id=?
        """, (worker_id, now, now, store_id))
        self.conn.commit()

    def mark_store_completed(self, store_id: int, reviews_scraped: int) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE stores
            SET status='completed', reviews_scraped=?,
                worker_id=NULL, error_message=NULL, updated_at=?
            WHERE store_id=?
        """, (reviews_scraped, datetime.now().isoformat(), store_id))
        self.conn.commit()

    def mark_store_failed(self, store_id: int, error_message: str) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE stores
            SET status='failed', worker_id=NULL, error_message=?, updated_at=?
            WHERE store_id=?
        """, (str(error_message)[:500], datetime.now().isoformat(), store_id))
        self.conn.commit()

    def mark_store_skipped(self, store_id: int, reason: str) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE stores
            SET status='skipped', worker_id=NULL, error_message=?, updated_at=?
            WHERE store_id=?
        """, (reason[:500], datetime.now().isoformat(), store_id))
        self.conn.commit()

    def reset_interrupted_stores(self) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE stores SET status='pending', worker_id=NULL WHERE status='in_progress'"
        )
        count = cur.rowcount
        self.conn.commit()
        return count

    def reset_failed_stores(self) -> int:
        """Reset all failed stores back to pending so they get retried."""
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE stores SET status='pending', worker_id=NULL, attempts=0, error_message=NULL "
            "WHERE status='failed'"
        )
        count = cur.rowcount
        self.conn.commit()
        return count

    def release_worker_stores(self, worker_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE stores SET status='pending', worker_id=NULL "
            "WHERE status='in_progress' AND worker_id=?",
            (worker_id,),
        )
        count = cur.rowcount
        self.conn.commit()
        return count

    def update_store_reviews_count(self, store_id: int, count: int) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE stores SET reviews_scraped=?, updated_at=? WHERE store_id=?",
            (count, datetime.now().isoformat(), store_id),
        )
        self.conn.commit()

    def update_store_live_review_count(self, store_id: int, live_count: int) -> None:
        """Save the live Google Maps review count for a store (before scraping)."""
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE stores SET gmaps_reviews_count=?, updated_at=? WHERE store_id=?",
            (live_count, datetime.now().isoformat(), store_id),
        )
        self.conn.commit()

    def is_store_completed(self, store_id: int) -> bool:
        """Check if a store has already been fully scraped."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT status FROM stores WHERE store_id=?",
            (store_id,),
        )
        row = cur.fetchone()
        return bool(row and row["status"] == "completed")

    def get_store_type_stats(self) -> dict[str, Any]:
        """Return counts by store type (fresh vs incomplete) and status."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT
                CASE WHEN master_reviews > 0 THEN 'incomplete' ELSE 'fresh' END AS store_type,
                status,
                COUNT(*) AS cnt,
                SUM(target_reviews) AS total_target,
                SUM(master_reviews) AS total_master,
                SUM(reviews_scraped) AS total_scraped
            FROM stores
            GROUP BY store_type, status
        """)
        return cur.fetchall()

    # ── reviews ───────────────────────────────────────────────────────────────

    def get_existing_review_ids(self, store_id: int) -> set[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT review_id FROM reviews WHERE store_id=?", (store_id,))
        return {row["review_id"] for row in cur.fetchall()}

    def save_reviews_batch(self, store_id: int, reviews: Iterable[dict[str, Any]], machine_id: str = "") -> int:
        """Bulk-insert reviews using executemany() with INSERT OR IGNORE."""
        rows = []
        for r in reviews:
            if not r or not r.get("review_id"):
                continue
            rows.append((
                store_id,
                machine_id,
                r.get("review_id"),
                r.get("reviewer_name", "Anonymous"),
                r.get("rating"),
                r.get("date_relative", ""),
                (r.get("review_text", "") or "")[:5000],
                int(r.get("helpful_count", 0) or 0),
                int(r.get("photo_count", 0) or 0),
                int(r.get("has_owner_response", 0) or 0),
                int(r.get("reviewer_review_count", 0) or 0),
                int(r.get("reviewer_photo_count", 0) or 0),
                int(r.get("is_local_guide", 0) or 0),
                r.get("service_type", ""),
            ))
        if not rows:
            return 0
        cur = self.conn.cursor()
        try:
            cur.executemany("""
                INSERT OR IGNORE INTO reviews (
                    store_id, machine_id, review_id, reviewer_name, rating,
                    date_relative, review_text, helpful_count,
                    photo_count, has_owner_response, reviewer_review_count,
                    reviewer_photo_count, is_local_guide, service_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            inserted = cur.rowcount
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return inserted

    def get_review_count(self, store_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM reviews WHERE store_id=?", (store_id,))
        return cur.fetchone()["cnt"]

    # ── sessions ──────────────────────────────────────────────────────────────

    def start_session(self) -> int:
        cur = self.conn.cursor()
        cur.execute("INSERT INTO scrape_sessions DEFAULT VALUES")
        session_id = cur.lastrowid
        self.conn.commit()
        return session_id

    def end_session(
        self,
        session_id: int,
        stores_processed: int,
        stores_completed: int,
        stores_failed: int,
        reviews_collected: int,
        status: str = "completed",
    ) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE scrape_sessions
            SET ended_at=?, stores_processed=?, stores_completed=?,
                stores_failed=?, reviews_collected=?, status=?
            WHERE session_id=?
        """, (
            datetime.now().isoformat(), stores_processed, stores_completed,
            stores_failed, reviews_collected, status, session_id,
        ))
        self.conn.commit()


# ── factory ───────────────────────────────────────────────────────────────────

def make_db(db_path: str) -> DatabaseManager:
    """Create a DatabaseManager from a file path."""
    return DatabaseManager(db_path)
