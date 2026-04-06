"""
Merge scraper result databases back into the master reviews database.

Takes one or more scraper .db files (from linux_scraper runs on different
machines) and merges their reviews into Step_5_master_reviews.db.

Deduplication: (store_id, reviewer_name, rating, review_text[:50])
— matches the dedup strategy used in prior merges.

Usage:
    python -m linux_scraper.merge_results machine1.db machine2.db machine3.db
    python -m linux_scraper.merge_results machine1.db --master PATH/TO/master.db
    python -m linux_scraper.merge_results results/*.db --dry-run
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = lambda c, r: dict(zip([d[0] for d in c.description], r))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _build_dedup_key(r: dict) -> tuple:
    """Build dedup key matching master DB convention."""
    text = (r.get("review_text") or "")[:50]
    return (
        r["store_id"],
        r.get("reviewer_name") or "",
        r.get("rating"),
        text,
    )


def merge_one(master_conn: sqlite3.Connection, scraper_db_path: str,
              dry_run: bool = False) -> dict:
    """Merge one scraper DB into master. Returns stats dict."""
    db_name = os.path.basename(scraper_db_path)
    scraper = _connect(scraper_db_path)
    sc = scraper.cursor()
    mc = master_conn.cursor()

    stats = {
        "db": db_name,
        "stores_in_scraper": 0,
        "stores_matched": 0,
        "stores_unmatched": 0,
        "reviews_in_scraper": 0,
        "reviews_new": 0,
        "reviews_duplicate": 0,
        "stores_updated": 0,
    }

    # ── Map scraper store_ids to master store_ids via place_id ──
    sc.execute("SELECT store_id, google_place_id FROM stores")
    scraper_stores = sc.fetchall()
    stats["stores_in_scraper"] = len(scraper_stores)

    # Build place_id → master store_id lookup
    mc.execute("SELECT store_id, place_id FROM stores")
    master_pid_map = {}
    for r in mc.fetchall():
        if r["place_id"]:
            master_pid_map[r["place_id"]] = r["store_id"]

    # Map scraper store_id → master store_id
    sid_map = {}
    unmatched = []
    for s in scraper_stores:
        pid = s["google_place_id"]
        if pid and pid in master_pid_map:
            sid_map[s["store_id"]] = master_pid_map[pid]
            stats["stores_matched"] += 1
        else:
            unmatched.append(pid)
            stats["stores_unmatched"] += 1

    if unmatched:
        print(f"  WARNING: {len(unmatched)} stores not found in master (skipping their reviews)")
        for pid in unmatched[:5]:
            print(f"    {pid}")
        if len(unmatched) > 5:
            print(f"    ... and {len(unmatched) - 5} more")

    # ── Load existing dedup keys for matched stores ──
    matched_master_ids = set(sid_map.values())
    existing_keys = set()
    for master_sid in matched_master_ids:
        mc.execute(
            "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=?",
            (master_sid,),
        )
        for r in mc.fetchall():
            key = (
                master_sid,
                r.get("reviewer_name") or "",
                r.get("rating"),
                (r.get("review_text") or "")[:50],
            )
            existing_keys.add(key)

    # ── Read scraper reviews and insert new ones ──
    sc.execute("""
        SELECT store_id, reviewer_name, rating, date_relative, review_text,
               helpful_count, photo_count, has_owner_response,
               reviewer_review_count, reviewer_photo_count, is_local_guide,
               service_type, scraped_at
        FROM reviews
    """)
    scraper_reviews = sc.fetchall()
    stats["reviews_in_scraper"] = len(scraper_reviews)

    to_insert = []
    for r in scraper_reviews:
        scraper_sid = r["store_id"]
        master_sid = sid_map.get(scraper_sid)
        if master_sid is None:
            continue

        key = (
            master_sid,
            r.get("reviewer_name") or "",
            r.get("rating"),
            (r.get("review_text") or "")[:50],
        )
        if key in existing_keys:
            stats["reviews_duplicate"] += 1
            continue

        existing_keys.add(key)
        stats["reviews_new"] += 1
        to_insert.append((
            master_sid,
            r.get("reviewer_name"),
            r.get("rating"),
            r.get("date_relative"),
            r.get("review_text"),
            int(r.get("helpful_count") or 0),
            int(r.get("photo_count") or 0),
            int(r.get("has_owner_response") or 0),
            int(r.get("reviewer_review_count") or 0),
            int(r.get("reviewer_photo_count") or 0),
            int(r.get("is_local_guide") or 0),
            r.get("service_type") or "",
            r.get("scraped_at"),
            db_name,
        ))

    if not dry_run and to_insert:
        mc.executemany("""
            INSERT INTO reviews (
                store_id, reviewer_name, rating, date_relative, review_text,
                helpful_count, photo_count, has_owner_response,
                reviewer_review_count, reviewer_photo_count, is_local_guide,
                service_type, scraped_at, source_db
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, to_insert)

    # ── Update store review counts and status ──
    if not dry_run:
        for master_sid in matched_master_ids:
            mc.execute(
                "SELECT COUNT(*) as cnt FROM reviews WHERE store_id=?",
                (master_sid,),
            )
            new_count = mc.fetchone()["cnt"]
            mc.execute(
                "SELECT target_reviews, reviews_scraped FROM stores WHERE store_id=?",
                (master_sid,),
            )
            store = mc.fetchone()
            old_count = store["reviews_scraped"] or 0
            target = store["target_reviews"] or 0

            if new_count > old_count:
                stats["stores_updated"] += 1
                new_status = "complete" if (new_count >= target or target == 0) else "incomplete"
                mc.execute(
                    "UPDATE stores SET reviews_scraped=?, scrape_status=? WHERE store_id=?",
                    (new_count, new_status, master_sid),
                )

        master_conn.commit()

    scraper.close()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Merge scraper result databases into master reviews DB",
    )
    parser.add_argument("scraper_dbs", nargs="+", metavar="DB",
                        help="Scraper result .db files to merge")
    parser.add_argument("--master", default="Step_5_master_reviews.db",
                        help="Path to master reviews database")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be merged without writing")
    args = parser.parse_args()

    if not os.path.exists(args.master):
        print(f"ERROR: Master database not found: {args.master}")
        sys.exit(1)

    for db in args.scraper_dbs:
        if not os.path.exists(db):
            print(f"ERROR: Scraper database not found: {db}")
            sys.exit(1)

    if args.dry_run:
        print("=== DRY RUN — no changes will be written ===\n")

    # Backup master before merging
    if not args.dry_run:
        backup = args.master.replace(".db", f"_backup_{datetime.now():%Y%m%d_%H%M%S}.db")
        print(f"Backing up master to: {backup}")
        import shutil
        shutil.copy2(args.master, backup)
        print()

    master = _connect(args.master)

    # Pre-merge stats
    mc = master.cursor()
    mc.execute("SELECT COUNT(*) as cnt FROM reviews")
    pre_reviews = mc.fetchone()["cnt"]
    mc.execute("SELECT COUNT(*) as cnt FROM stores WHERE scrape_status='complete'")
    pre_complete = mc.fetchone()["cnt"]

    total_new = 0
    total_dup = 0

    for db_path in args.scraper_dbs:
        print(f"Merging: {os.path.basename(db_path)}")
        stats = merge_one(master, db_path, dry_run=args.dry_run)
        total_new += stats["reviews_new"]
        total_dup += stats["reviews_duplicate"]
        print(f"  Stores matched:    {stats['stores_matched']}/{stats['stores_in_scraper']}")
        print(f"  Reviews in file:   {stats['reviews_in_scraper']:,}")
        print(f"  New reviews:       {stats['reviews_new']:,}")
        print(f"  Duplicates:        {stats['reviews_duplicate']:,}")
        print(f"  Stores updated:    {stats['stores_updated']}")
        print()

    # Post-merge stats
    mc.execute("SELECT COUNT(*) as cnt FROM reviews")
    post_reviews = mc.fetchone()["cnt"]
    mc.execute("SELECT COUNT(*) as cnt FROM stores WHERE scrape_status='complete'")
    post_complete = mc.fetchone()["cnt"]
    mc.execute("SELECT scrape_status, COUNT(*) as cnt FROM stores GROUP BY scrape_status")
    by_status = {r["scrape_status"]: r["cnt"] for r in mc.fetchall()}

    master.close()

    print("=" * 50)
    print("MERGE COMPLETE" if not args.dry_run else "DRY RUN COMPLETE")
    print(f"  Reviews: {pre_reviews:,} → {post_reviews:,} (+{total_new:,} new, {total_dup:,} duplicates)")
    print(f"  Complete stores: {pre_complete} → {post_complete}")
    print(f"  Status: {by_status}")


if __name__ == "__main__":
    main()
