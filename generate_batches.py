"""
Generate linux-scraper-compatible .db batch files from the master reviews DB.

Rules:
  - target_reviews capped at 1500
  - If reviews_scraped within ±1 of target → mark complete
  - If reviews_scraped >= target → mark complete
  - Remaining stores split into batches of 500, sorted by target_reviews ASC
  - Each batch is a standalone .db file matching linux_scraper schema
"""

import sqlite3
import os
import math

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_DB = os.path.join(SCRIPT_DIR, "Data", "Step_5_master_reviews.db")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "Data")
BATCH_SIZE = 500
TARGET_CAP = 1500

conn = sqlite3.connect(MASTER_DB)
cur = conn.cursor()

# ── Step 1: Cap target_reviews at 1500 ───────────────────────────────────────
cur.execute("UPDATE stores SET target_reviews = ? WHERE target_reviews > ?", (TARGET_CAP, TARGET_CAP))
print(f"Capped {cur.rowcount} stores to target_reviews={TARGET_CAP}")

# ── Step 2: Mark complete if within ±1 of target or overshoot ────────────────
cur.execute("""
    UPDATE stores SET scrape_status = 'complete'
    WHERE target_reviews IS NOT NULL AND target_reviews > 0
    AND reviews_scraped >= (target_reviews - 1)
    AND scrape_status != 'complete'
""")
print(f"Marked {cur.rowcount} additional stores as complete (within ±1 or overshoot)")
conn.commit()

# ── Step 3: Stats ────────────────────────────────────────────────────────────
cur.execute("""
    SELECT scrape_status, COUNT(*),
           COALESCE(SUM(target_reviews),0),
           COALESCE(SUM(reviews_scraped),0)
    FROM stores GROUP BY scrape_status
""")
print("\n=== Updated status distribution ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} stores, target_sum={r[2]}, scraped_sum={r[3]}")

# ── Step 4: Get remaining stores to scrape ───────────────────────────────────
cur.execute("""
    SELECT store_id, place_id, store_name, business_name, outscraper_name,
           full_address, street, city, state, zip,
           source_lat, source_lon, outscraper_address,
           phone, rating, target_reviews, reviews_scraped,
           business_status, category, subtypes, type, scrape_status
    FROM stores
    WHERE scrape_status != 'complete'
    AND target_reviews IS NOT NULL AND target_reviews > 0
    ORDER BY target_reviews ASC, store_id ASC
""")
remaining = cur.fetchall()

# Also count no-target stores
cur.execute("SELECT COUNT(*) FROM stores WHERE target_reviews IS NULL OR target_reviews = 0")
no_target_count = cur.fetchone()[0]

total_remaining_reviews = sum(max(0, (r[15] or 0) - (r[16] or 0)) for r in remaining)
print(f"\nStores still needing scraping: {len(remaining)}")
print(f"Stores with no target (excluded): {no_target_count}")
print(f"Total remaining reviews to scrape: {total_remaining_reviews:,}")

# ── Step 5: Create batch .db files ───────────────────────────────────────────
# Clean old batch files
for old in [f for f in os.listdir(OUTPUT_DIR) if f.startswith("batch_") and f.endswith(".db")]:
    os.remove(os.path.join(OUTPUT_DIR, old))

num_batches = math.ceil(len(remaining) / BATCH_SIZE)
print(f"\nCreating {num_batches} batch DB files in {OUTPUT_DIR}/\n")

for batch_num in range(num_batches):
    start = batch_num * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(remaining))
    batch_stores = remaining[start:end]

    db_path = os.path.join(OUTPUT_DIR, f"batch_{batch_num + 1:02d}.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    bconn = sqlite3.connect(db_path)
    bcur = bconn.cursor()

    # Create linux scraper schema
    bcur.executescript("""
        CREATE TABLE IF NOT EXISTS metadata (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
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
        );
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
        );
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
        );
        CREATE INDEX IF NOT EXISTS idx_stores_status   ON stores(status);
        CREATE INDEX IF NOT EXISTS idx_stores_place_id ON stores(google_place_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_store   ON reviews(store_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_dedup   ON reviews(store_id, review_id);
    """)

    batch_reviews_needed = 0
    for s in batch_stores:
        target = s[15] or 0
        already_scraped = s[16] or 0
        remaining_needed = max(0, target - already_scraped)
        batch_reviews_needed += remaining_needed

        bcur.execute("""
            INSERT INTO stores (
                machine_id, input_name, input_street, input_city, input_state, input_zip,
                input_lat, input_lon, google_place_id,
                gmaps_name, gmaps_address, gmaps_phone, gmaps_website,
                gmaps_rating, gmaps_reviews_count, gmaps_lat, gmaps_lon,
                gmaps_category, gmaps_price_level,
                target_reviews, master_reviews, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        """, (
            '',
            s[2] or s[3],       # input_name
            s[6],               # street
            s[7],               # city
            s[8],               # state
            s[9],               # zip
            s[10],              # source_lat
            s[11],              # source_lon
            s[1],               # place_id → google_place_id
            s[4],               # outscraper_name → gmaps_name
            s[5],               # full_address → gmaps_address
            s[13],              # phone
            None,               # website
            s[14],              # rating
            target,             # gmaps_reviews_count
            None, None,         # gmaps_lat, gmaps_lon
            s[18],              # category
            None,               # price_level
            target,             # target_reviews
            already_scraped,    # master_reviews
        ))

    bconn.commit()
    bconn.close()

    target_lo = batch_stores[0][15] or 0
    target_hi = batch_stores[-1][15] or 0
    print(f"  batch_{batch_num + 1:02d}.db: {len(batch_stores):>3} stores | "
          f"target=[{target_lo:>4}-{target_hi:>4}] | "
          f"reviews_needed={batch_reviews_needed:>6,}")

conn.close()
print(f"\nDone! {num_batches} batch files ready in {OUTPUT_DIR}/")
