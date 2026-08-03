"""
src/database.py
===============
Database layer for the EnrichOverture project.

Provides all SQLite read/write operations:
  - ``init_db``                    — create schema and indexes
  - ``import_input_data``          — load a CSV file into the DB
  - ``import_dataframe``           — load a pandas DataFrame into the DB
  - ``get_pending_businesses``     — fetch un-processed records (all states)
  - ``get_pending_businesses_by_state`` — fetch un-processed records for one state
  - ``get_db_stats_by_state``      — summary counts for a given state
  - ``update_business_status``     — write enrichment results back to the DB

Schema overview
---------------
All business records live in a single ``supermarkets`` table.
Input columns (``input_*``) are populated at import time and never changed.
Enriched columns (``gmaps_*``) are written after a successful scrape.
The ``status`` column drives the processing pipeline:
  pending   → not yet processed
  success   → matched and enriched
  not_found → no Google Maps listing found
  duplicate → another row already holds this Place ID
  failed    → scrape error (will be retried)
"""

import sqlite3
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from config.settings import DB_PATH

logger = logging.getLogger("Database")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS supermarkets (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,

        -- Input data (from Overture Maps / Excel import, never modified)
        input_name      TEXT,
        input_street    TEXT,
        input_city      TEXT,
        input_state     TEXT,
        input_zip       TEXT,
        input_lat       REAL,
        input_lon       REAL,

        -- Enriched data (populated by the scraper on success)
        google_place_id TEXT UNIQUE,        -- Google Maps Place ID (ChIJ…)
        gmaps_name      TEXT,               -- Business name as shown on Google Maps
        gmaps_address   TEXT,               -- Formatted address from Google Maps
        gmaps_phone     TEXT,
        gmaps_website   TEXT,
        gmaps_rating    REAL,               -- Star rating (0.0 – 5.0)
        gmaps_reviews_count INTEGER,
        gmaps_lat       REAL,
        gmaps_lon       REAL,
        gmaps_category  TEXT,               -- e.g. "Grocery store"
        gmaps_price_level TEXT,             -- "$", "$$", etc.
        gmaps_hours     TEXT,               -- Raw hours text from the listing
        gmaps_url       TEXT,               -- Full Google Maps URL

        -- Processing metadata
        status          TEXT DEFAULT 'pending',  -- pending|success|not_found|duplicate|failed
        error_message   TEXT,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db() -> None:
    """
    Create the database schema and indexes if they do not already exist.

    Also runs a lightweight migration to add ``gmaps_url`` to databases
    created before that column was introduced.

    Safe to call on every startup — all statements use ``IF NOT EXISTS``.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # WAL mode gives better concurrent read/write performance when multiple
    # worker processes are writing results at the same time.
    cursor.execute("PRAGMA journal_mode=WAL;")

    cursor.execute(_CREATE_TABLE_SQL)

    # Indexes for the two most-queried columns
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_status   ON supermarkets(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_place_id ON supermarkets(google_place_id)")

    # Migration: add gmaps_url if this is an older database
    cursor.execute("PRAGMA table_info(supermarkets)")
    existing_columns = [col[1] for col in cursor.fetchall()]
    if "gmaps_url" not in existing_columns:
        cursor.execute("ALTER TABLE supermarkets ADD COLUMN gmaps_url TEXT")
        logger.info("Migration applied: added gmaps_url column.")

    conn.commit()
    conn.close()
    logger.info(f"Database ready at: {DB_PATH}")


def import_input_data(csv_path: str, limit: Optional[int] = None) -> int:
    """
    Import business records from a CSV file into the database.

    Rows that already exist (matched on name + lat + lon) are skipped so
    that re-running the import is safe.

    Args:
        csv_path: Absolute path to the input CSV file.
        limit:    Optional row limit (useful for testing).

    Returns:
        Number of new rows inserted.
    """
    try:
        df = pd.read_csv(csv_path)
        if limit:
            df = df.head(limit)
        return import_dataframe(df)
    except Exception as exc:
        logger.error(f"CSV import failed: {exc}")
        raise


def import_dataframe(df: pd.DataFrame) -> int:
    """
    Import a pandas DataFrame into the database.

    Deduplication key: ``(input_name, input_lat, input_lon)``.
    Rows that match an existing record are silently skipped.

    Args:
        df: DataFrame with columns matching the Overture Maps schema
            (name, street, city, state, zipcode, latitude, longitude).

    Returns:
        Number of new rows inserted.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        inserted = 0
        for _, row in df.iterrows():
            # Skip if a record with the same name and coordinates already exists
            cursor.execute(
                "SELECT id FROM supermarkets WHERE input_name=? AND input_lat=? AND input_lon=?",
                (row.get("name"), row.get("latitude"), row.get("longitude")),
            )
            if cursor.fetchone():
                continue  # Already in the database

            cursor.execute(
                """
                INSERT INTO supermarkets
                    (input_name, input_street, input_city, input_state,
                     input_zip, input_lat, input_lon, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    row.get("name"),
                    row.get("street"),
                    row.get("city"),
                    row.get("state"),
                    row.get("zipcode"),
                    row.get("latitude"),
                    row.get("longitude"),
                ),
            )
            inserted += 1

        conn.commit()
        logger.info(f"Imported {inserted} new records from DataFrame.")
        return inserted

    except Exception as exc:
        logger.error(f"DataFrame import failed: {exc}")
        raise
    finally:
        conn.close()


def get_pending_businesses(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch a batch of ``pending`` records across all states.

    NC records are prioritised (returned first) to allow the NC-specific
    runner to work correctly even when other states are in the database.

    Args:
        limit: Maximum number of records to return.

    Returns:
        List of row dicts.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT * FROM supermarkets
        WHERE status = 'pending'
        ORDER BY CASE WHEN input_state = 'NC' THEN 0 ELSE 1 END, id ASC
        LIMIT ?
        """,
        (limit,),
    )

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_pending_businesses_by_state(state: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch a batch of ``pending`` records for a specific state.

    Args:
        state: Two-letter state abbreviation (e.g. ``"NC"``).
        limit: Maximum number of records to return.

    Returns:
        List of row dicts ordered by ``id ASC``.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT * FROM supermarkets
        WHERE status = 'pending' AND input_state = ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (state, limit),
    )

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_db_stats_by_state(state: str) -> Dict[str, int]:
    """
    Return a summary of record counts for a given state.

    Keys in the returned dict:
        pending, success, not_found, duplicate, failed,
        total, with_place_id, with_category

    Args:
        state: Two-letter state abbreviation.

    Returns:
        Dict mapping status name → count.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Status breakdown
    cursor.execute(
        "SELECT status, COUNT(*) FROM supermarkets WHERE input_state=? GROUP BY status",
        (state,),
    )
    stats: Dict[str, int] = {row[0]: row[1] for row in cursor.fetchall()}

    # Totals
    cursor.execute("SELECT COUNT(*) FROM supermarkets WHERE input_state=?", (state,))
    stats["total"] = cursor.fetchone()[0]

    # Data-quality counts
    cursor.execute(
        "SELECT COUNT(*) FROM supermarkets "
        "WHERE input_state=? AND google_place_id IS NOT NULL AND google_place_id != ''",
        (state,),
    )
    stats["with_place_id"] = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM supermarkets "
        "WHERE input_state=? AND gmaps_category IS NOT NULL AND gmaps_category != ''",
        (state,),
    )
    stats["with_category"] = cursor.fetchone()[0]

    conn.close()
    return stats


def update_business_status(
    business_id: int,
    status: str,
    enriched_data: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """
    Write the result of a scrape attempt back to the database.

    On success, all ``gmaps_*`` columns are updated with the scraped data.
    On failure or not-found, only ``status`` and ``error_message`` change.

    Duplicate Place ID handling:
        If the ``google_place_id`` UNIQUE constraint fires, the row is
        marked ``duplicate`` instead of ``success``.  The ``gmaps_url``
        and ``gmaps_name`` are still stored so the record is not empty.

    Args:
        business_id:   Primary key of the row to update.
        status:        New status string (``'success'``, ``'not_found'``,
                       ``'failed'``, ``'duplicate'``).
        enriched_data: Dict of scraped fields (required when status is
                       ``'success'``).
        error:         Human-readable error message (for non-success rows).
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        if status == "success" and enriched_data:
            # Write all enriched fields.
            # COALESCE preserves any value already stored — so a second
            # scrape of the same row will not overwrite good data with None.
            cursor.execute(
                """
                UPDATE supermarkets SET
                    status              = ?,
                    google_place_id     = COALESCE(google_place_id, ?),
                    gmaps_name          = COALESCE(gmaps_name, ?),
                    gmaps_address       = COALESCE(gmaps_address, ?),
                    gmaps_phone         = COALESCE(gmaps_phone, ?),
                    gmaps_website       = COALESCE(gmaps_website, ?),
                    gmaps_rating        = COALESCE(gmaps_rating, ?),
                    gmaps_reviews_count = COALESCE(gmaps_reviews_count, ?),
                    gmaps_lat           = COALESCE(gmaps_lat, ?),
                    gmaps_lon           = COALESCE(gmaps_lon, ?),
                    gmaps_category      = COALESCE(gmaps_category, ?),
                    gmaps_price_level   = COALESCE(gmaps_price_level, ?),
                    gmaps_hours         = COALESCE(gmaps_hours, ?),
                    gmaps_url           = COALESCE(gmaps_url, ?),
                    error_message       = NULL,
                    updated_at          = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    status,
                    enriched_data.get("place_id"),
                    enriched_data.get("name"),
                    enriched_data.get("address"),
                    enriched_data.get("phone"),
                    enriched_data.get("website"),
                    enriched_data.get("rating"),
                    enriched_data.get("reviews_count"),
                    enriched_data.get("latitude"),
                    enriched_data.get("longitude"),
                    enriched_data.get("category"),
                    enriched_data.get("price_level"),
                    enriched_data.get("hours"),
                    enriched_data.get("gmaps_url"),
                    business_id,
                ),
            )
        else:
            # Non-success: just update status and error message
            cursor.execute(
                """
                UPDATE supermarkets SET
                    status        = ?,
                    error_message = ?,
                    updated_at    = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (status, error, business_id),
            )

        conn.commit()

    except sqlite3.IntegrityError as exc:
        # The only UNIQUE constraint is on google_place_id.
        # This fires when two input rows map to the same Google Maps listing.
        if "UNIQUE constraint failed" in str(exc) and "google_place_id" in str(exc):
            logger.warning(
                f"Duplicate Place ID for business {business_id}: "
                f"{enriched_data.get('place_id') if enriched_data else 'unknown'}"
            )
            try:
                # Mark as duplicate; store name/address/url for reference
                cursor.execute(
                    """
                    UPDATE supermarkets SET
                        status        = 'duplicate',
                        gmaps_name    = COALESCE(gmaps_name, ?),
                        gmaps_address = COALESCE(gmaps_address, ?),
                        gmaps_url     = COALESCE(gmaps_url, ?),
                        error_message = ?,
                        updated_at    = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        enriched_data.get("name") if enriched_data else None,
                        enriched_data.get("address") if enriched_data else None,
                        enriched_data.get("gmaps_url") if enriched_data else None,
                        f"Duplicate Place ID: {enriched_data.get('place_id') if enriched_data else 'unknown'}",
                        business_id,
                    ),
                )
                conn.commit()
            except Exception as inner_exc:
                logger.error(f"Failed to mark duplicate for {business_id}: {inner_exc}")
        else:
            logger.error(f"Integrity error for business {business_id}: {exc}")

    except Exception as exc:
        logger.error(f"Failed to update business {business_id}: {exc}")

    finally:
        conn.close()
