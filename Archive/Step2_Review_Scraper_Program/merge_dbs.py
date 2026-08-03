#!/usr/bin/env python3
"""
Merge multiple scraper .db files into one master database.

Usage:
  python merge_dbs.py output.db input1.db input2.db ...
  python merge_dbs.py output.db ./collected_dbs/

The second form reads all .db files from a folder.

Deduplication:
  - Stores are matched by google_place_id — if the same store appears
    in multiple DBs, the one with the most reviews wins.
  - Reviews are deduplicated by (google_place_id, review_id).
  - machine_id is preserved on every row for audit trail.
"""

import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


def _row_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    cols = [desc[0] for desc in cursor.description]
    return dict(zip(cols, row))


def _get_db_files(args: list[str]) -> list[str]:
    """Parse CLI args into a list of .db file paths."""
    files = []
    for arg in args:
        if os.path.isdir(arg):
            for f in sorted(os.listdir(arg)):
                if f.endswith(".db"):
                    files.append(os.path.join(arg, f))
        elif os.path.isfile(arg) and arg.endswith(".db"):
            files.append(arg)
        else:
            print(f"  WARNING: Skipping '{arg}' (not a .db file or directory)")
    return files


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a table has a specific column."""
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row["name"] for row in cur.fetchall()]
    return column in cols


def merge(output_path: str, input_paths: list[str]) -> None:
    """Merge multiple .db files into output_path."""
    if os.path.exists(output_path):
        print(f"  WARNING: Output file '{output_path}' already exists. Appending to it.")

    # Create output DB with schema
    out = sqlite3.connect(output_path, timeout=30)
    out.row_factory = _row_factory
    out.execute("PRAGMA journal_mode=WAL")
    out.execute("PRAGMA busy_timeout=30000")
    cur = out.cursor()

    # Create schema in output
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
            status            TEXT DEFAULT 'pending',
            worker_id         INTEGER,
            reviews_scraped   INTEGER DEFAULT 0,
            target_reviews    INTEGER DEFAULT 0,
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_place_id ON stores(google_place_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_store   ON reviews(store_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_dedup   ON reviews(store_id, review_id)")
    out.commit()

    # Track merge stats
    total_stores_added = 0
    total_reviews_added = 0
    total_stores_skipped = 0
    files_processed = 0

    for db_path in input_paths:
        print(f"\n  Processing: {os.path.basename(db_path)}")
        try:
            inp = sqlite3.connect(db_path, timeout=10)
            inp.row_factory = _row_factory
        except Exception as e:
            print(f"    ERROR: Could not open '{db_path}': {e}")
            continue

        inp_cur = inp.cursor()

        # Get machine_id from metadata or filename
        machine_id = ""
        try:
            inp_cur.execute("SELECT value FROM metadata WHERE key='machine_id'")
            row = inp_cur.fetchone()
            if row:
                machine_id = row["value"]
        except Exception:
            pass
        if not machine_id:
            # Fallback: extract from filename pattern _reviews_XXXXXX.db
            import re
            m = re.search(r"_reviews_([a-z0-9]+)\.db$", db_path)
            if m:
                machine_id = m.group(1)
            else:
                machine_id = os.path.splitext(os.path.basename(db_path))[0]

        # Check if source has machine_id columns
        has_store_mid = _has_column(inp, "stores", "machine_id")
        has_review_mid = _has_column(inp, "reviews", "machine_id")

        # Read all stores from input
        inp_cur.execute("SELECT * FROM stores")
        input_stores = inp_cur.fetchall()
        stores_added = 0
        stores_skipped = 0

        # Map: input store_id -> output store_id (for review remapping)
        id_map: dict[int, int] = {}

        for store in input_stores:
            place_id = store.get("google_place_id", "")
            if not place_id:
                continue

            src_machine_id = store.get("machine_id", machine_id) if has_store_mid else machine_id

            # Check if this store already exists in output
            cur.execute("SELECT store_id, reviews_scraped FROM stores WHERE google_place_id=?", (place_id,))
            existing = cur.fetchone()

            if existing:
                # Keep the store with more reviews
                existing_reviews = existing["reviews_scraped"] or 0
                incoming_reviews = store.get("reviews_scraped") or 0
                if incoming_reviews > existing_reviews:
                    # Update existing store with this one's data
                    cur.execute("""
                        UPDATE stores SET
                            machine_id=?, input_name=?, input_street=?, input_city=?,
                            input_state=?, input_zip=?, input_lat=?, input_lon=?,
                            gmaps_name=?, gmaps_address=?, gmaps_phone=?, gmaps_website=?,
                            gmaps_rating=?, gmaps_reviews_count=?, gmaps_lat=?, gmaps_lon=?,
                            gmaps_category=?, gmaps_price_level=?, status=?,
                            reviews_scraped=?, target_reviews=?, updated_at=?
                        WHERE store_id=?
                    """, (
                        src_machine_id,
                        store.get("input_name"), store.get("input_street"),
                        store.get("input_city"), store.get("input_state"),
                        store.get("input_zip"), store.get("input_lat"),
                        store.get("input_lon"), store.get("gmaps_name"),
                        store.get("gmaps_address"), store.get("gmaps_phone"),
                        store.get("gmaps_website"), store.get("gmaps_rating"),
                        store.get("gmaps_reviews_count"), store.get("gmaps_lat"),
                        store.get("gmaps_lon"), store.get("gmaps_category"),
                        store.get("gmaps_price_level"), store.get("status"),
                        incoming_reviews, store.get("target_reviews"),
                        datetime.now().isoformat(),
                        existing["store_id"],
                    ))
                id_map[store["store_id"]] = existing["store_id"]
                stores_skipped += 1
            else:
                # Insert new store
                cur.execute("""
                    INSERT INTO stores (
                        machine_id, input_name, input_street, input_city, input_state, input_zip,
                        input_lat, input_lon, google_place_id,
                        gmaps_name, gmaps_address, gmaps_phone, gmaps_website,
                        gmaps_rating, gmaps_reviews_count, gmaps_lat, gmaps_lon,
                        gmaps_category, gmaps_price_level, status,
                        reviews_scraped, target_reviews
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    src_machine_id,
                    store.get("input_name"), store.get("input_street"),
                    store.get("input_city"), store.get("input_state"),
                    store.get("input_zip"), store.get("input_lat"),
                    store.get("input_lon"), store.get("google_place_id"),
                    store.get("gmaps_name"), store.get("gmaps_address"),
                    store.get("gmaps_phone"), store.get("gmaps_website"),
                    store.get("gmaps_rating"), store.get("gmaps_reviews_count"),
                    store.get("gmaps_lat"), store.get("gmaps_lon"),
                    store.get("gmaps_category"), store.get("gmaps_price_level"),
                    store.get("status"),
                    store.get("reviews_scraped"), store.get("target_reviews"),
                ))
                id_map[store["store_id"]] = cur.lastrowid
                stores_added += 1

        out.commit()

        # Now merge reviews
        reviews_added = 0
        inp_cur.execute("SELECT * FROM reviews")
        input_reviews = inp_cur.fetchall()

        for review in input_reviews:
            src_store_id = review.get("store_id")
            if src_store_id not in id_map:
                continue  # store wasn't imported (no place_id?)

            out_store_id = id_map[src_store_id]
            src_review_mid = review.get("machine_id", machine_id) if has_review_mid else machine_id

            try:
                cur.execute("""
                    INSERT OR IGNORE INTO reviews (
                        store_id, machine_id, review_id, reviewer_name, rating,
                        date_relative, review_text, helpful_count,
                        photo_count, has_owner_response, reviewer_review_count,
                        reviewer_photo_count, is_local_guide, service_type, scraped_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    out_store_id,
                    src_review_mid,
                    review.get("review_id"),
                    review.get("reviewer_name"),
                    review.get("rating"),
                    review.get("date_relative"),
                    review.get("review_text"),
                    review.get("helpful_count", 0),
                    review.get("photo_count", 0),
                    review.get("has_owner_response", 0),
                    review.get("reviewer_review_count", 0),
                    review.get("reviewer_photo_count", 0),
                    review.get("is_local_guide", 0),
                    review.get("service_type", ""),
                    review.get("scraped_at"),
                ))
                if cur.rowcount > 0:
                    reviews_added += 1
            except sqlite3.IntegrityError:
                pass  # duplicate review

        out.commit()
        inp.close()

        total_stores_added += stores_added
        total_stores_skipped += stores_skipped
        total_reviews_added += reviews_added
        files_processed += 1

        print(f"    Machine ID: {machine_id}")
        print(f"    Stores: {stores_added} added, {stores_skipped} already existed")
        print(f"    Reviews: {reviews_added} added")

    # Update review counts on merged stores
    cur.execute("""
        UPDATE stores SET reviews_scraped = (
            SELECT COUNT(*) FROM reviews WHERE reviews.store_id = stores.store_id
        )
    """)
    out.commit()

    # Save merge metadata
    cur.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        ("merged_at", datetime.now().isoformat()),
    )
    cur.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        ("merge_sources", str(files_processed)),
    )
    out.commit()

    # Final stats
    cur.execute("SELECT COUNT(*) AS cnt FROM stores")
    final_stores = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
    final_reviews = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(DISTINCT machine_id) AS cnt FROM stores WHERE machine_id != ''")
    unique_machines = cur.fetchone()["cnt"]

    out.close()

    print()
    print("=" * 62)
    print("  MERGE COMPLETE!")
    print("=" * 62)
    print(f"  Files processed:    {files_processed}")
    print(f"  Unique machines:    {unique_machines}")
    print(f"  Total stores:       {final_stores:,}")
    print(f"  Total reviews:      {final_reviews:,}")
    print(f"  Output:             {output_path}")
    print("=" * 62)


def main():
    if len(sys.argv) < 3:
        print("Usage: python merge_dbs.py OUTPUT.db INPUT1.db INPUT2.db ...")
        print("       python merge_dbs.py OUTPUT.db ./folder_of_dbs/")
        sys.exit(1)

    output_path = sys.argv[1]
    input_args = sys.argv[2:]
    input_files = _get_db_files(input_args)

    if not input_files:
        print("ERROR: No .db files found in the provided arguments.")
        sys.exit(1)

    # Make sure output is not in the input list
    output_abs = os.path.abspath(output_path)
    input_files = [f for f in input_files if os.path.abspath(f) != output_abs]

    print()
    print("=" * 62)
    print("  GOOGLE MAPS SCRAPER — DB MERGE TOOL")
    print("=" * 62)
    print(f"  Output: {output_path}")
    print(f"  Input files: {len(input_files)}")
    for f in input_files:
        print(f"    - {os.path.basename(f)}")

    merge(output_path, input_files)


if __name__ == "__main__":
    main()
