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
from pathlib import Path
from datetime import datetime


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = lambda c, r: dict(zip([d[0] for d in c.description], r))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    # The scraper writes with synchronous=FULL; the merge used to write with the
    # NORMAL default, so the most valuable write in the pipeline was the least
    # durable one.
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return any(r["name"] == col for r in conn.execute("PRAGMA table_info(" + table + ")"))


def _acquire_lock(master_path: str):
    """Refuse to run while another merge is in flight.

    `existing_keys` below is a snapshot taken at the start of the run, so two
    merges overlapping both see "not present" for the same review and both
    insert it. That is the exact shape of the duplicate rows found in the
    September audit: whole stores duplicated exactly twice from a single source
    file, one of which crossed its target on the duplicates and was stamped
    complete while missing 467 real reviews.
    """
    lock_path = master_path + ".merge.lock"
    handle = open(lock_path, "w", encoding="utf-8")
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        print("ERROR: another merge is already running against this master.")
        print("       Wait for it to finish, or delete " + lock_path + " if it is stale.")
        sys.exit(1)
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def backup_master(master_path: str) -> str:
    """Point-in-time copy of master into a backups/ folder beside it.

    VACUUM INTO reads through the WAL, so committed pages that a checkpoint
    could not flush (another connection holding a lock) are still in the copy.
    """
    backups = os.path.join(os.path.dirname(os.path.abspath(master_path)), "backups")
    os.makedirs(backups, exist_ok=True)
    name = os.path.basename(master_path).replace(
        ".db", f"_backup_{datetime.now():%Y%m%d_%H%M%S}.db")
    dest = os.path.join(backups, name)
    if os.path.exists(dest):
        os.remove(dest)
    src = sqlite3.connect(Path(master_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=60)
    try:
        src.execute("VACUUM INTO ?", (dest,))
    finally:
        src.close()
    return dest


def _backfill_place_ids(mc, scraper_stores, sid_map: dict, master_pid_map: dict) -> int:
    """Give master the ChIJ id the scraper landed on, for stores that had none.

    Never reassigns an id another master store already holds: that is exactly
    the shared-page contamination r06 exists to adjudicate, not something to
    settle silently here.
    """
    n = 0
    for s in scraper_stores:
        pid = s["google_place_id"] or ""
        msid = sid_map.get(s["store_id"])
        if msid is None or not pid.startswith("ChIJ") or pid in master_pid_map:
            continue
        mc.execute("UPDATE stores SET place_id=? WHERE store_id=? "
                   "AND (place_id IS NULL OR place_id LIKE '0x%')", (pid, msid))
        if mc.rowcount:
            master_pid_map[pid] = msid
            n += 1
    return n


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
        "_pre_reviews": master_conn.execute(
            "SELECT COUNT(*) AS c FROM reviews").fetchone()["c"],
    }

    # ── Map scraper store_ids to master store_ids ──
    #
    # A batch built by r09 carries master_store_id, which is exact and does not
    # depend on the store having a place_id (stores queued by feature id have
    # none until the scraper lands on them). Older batches fall back to place_id.
    has_msid = _has_column(scraper, "stores", "master_store_id")
    sc.execute("SELECT store_id, google_place_id" + (", master_store_id" if has_msid else "")
               + " FROM stores")
    scraper_stores = sc.fetchall()
    stats["stores_in_scraper"] = len(scraper_stores)

    mc.execute("SELECT store_id, place_id FROM stores")
    master_rows = mc.fetchall()
    master_ids = {r["store_id"] for r in master_rows}
    master_pid_map = {r["place_id"]: r["store_id"] for r in master_rows if r["place_id"]}

    sid_map = {}
    unmatched = []
    for s in scraper_stores:
        pid = s["google_place_id"]
        msid = s["master_store_id"] if has_msid else None
        if msid in master_ids:
            sid_map[s["store_id"]] = msid
            stats["stores_matched"] += 1
        elif pid and pid in master_pid_map:
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
    # Reviews r12 took out (contaminated pages, duplicate copies) count as known, so
    # re-merging an old batch cannot put them back.
    sources = ["reviews"]
    if mc.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                  "AND name='quarantined_reviews'").fetchone():
        sources.append("quarantined_reviews")
    for master_sid in matched_master_ids:
        mc.execute(
            " UNION ALL ".join("SELECT reviewer_name, rating, review_text FROM " + t +
                               " WHERE store_id=?" for t in sources),
            (master_sid,) * len(sources),
        )
        for r in mc.fetchall():
            key = (
                master_sid,
                (r.get("reviewer_name") or "").strip(),
                r.get("rating"),
                (r.get("review_text") or "").strip(),
            )
            existing_keys.add(key)

    # ── Read scraper reviews and insert new ones ──
    #
    # review_id and machine_id are selected now. They were previously dropped at
    # this boundary, which threw away Google's own stable review identifier and
    # forced every future merge onto a lossy content key. The batch schema has
    # carried both all along.
    batch_has_rid = _has_column(scraper, "reviews", "review_id")
    batch_has_mid = _has_column(scraper, "reviews", "machine_id")
    extra = ""
    if batch_has_rid:
        extra += ", review_id"
    if batch_has_mid:
        extra += ", machine_id"
    sc.execute("""
        SELECT store_id, reviewer_name, rating, date_relative, review_text,
               helpful_count, photo_count, has_owner_response,
               reviewer_review_count, reviewer_photo_count, is_local_guide,
               service_type, scraped_at""" + extra + """
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

        # Full review text, not a 50-character prefix. The prefix key merged two
        # genuinely different reviews that opened with the same sentence, and it
        # degenerated to name|rating|"" for the 49% of reviews that carry no text
        # at all, where it silently dropped real rating-only reviews from
        # different people who happen to share a first name.
        key = (
            master_sid,
            (r.get("reviewer_name") or "").strip(),
            r.get("rating"),
            (r.get("review_text") or "").strip(),
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
            r.get("review_id") if batch_has_rid else None,
            r.get("machine_id") if batch_has_mid else None,
        ))

    if not dry_run and to_insert:
        # Build the column list and the rows together, so an older master
        # without the identity columns still merges instead of failing.
        base_cols = ["store_id", "reviewer_name", "rating", "date_relative", "review_text",
                     "helpful_count", "photo_count", "has_owner_response",
                     "reviewer_review_count", "reviewer_photo_count", "is_local_guide",
                     "service_type", "scraped_at", "source_db"]
        keep_rid = _has_column(master_conn, "reviews", "review_id")
        keep_mid = _has_column(master_conn, "reviews", "machine_id")
        cols = list(base_cols)
        if keep_rid:
            cols.append("review_id")
        if keep_mid:
            cols.append("machine_id")

        rows = []
        for t in to_insert:
            row = list(t[:14])                    # the base columns, in order
            if keep_rid:
                row.append(t[14])                 # review_id
            if keep_mid:
                row.append(t[15])                 # machine_id
            rows.append(tuple(row))

        mc.execute("BEGIN IMMEDIATE")             # serialise against other writers
        mc.executemany(
            "INSERT INTO reviews (" + ", ".join(cols) + ") VALUES ("
            + ", ".join(["?"] * len(cols)) + ")", rows)

    # ── Update store review counts and status ──
    if not dry_run:
        # A store with a status_reason carries a ruling (Remediation/r12): its
        # status is a decision, not a function of the counts, so keep it.
        reason_col = (", status_reason" if _has_column(master_conn, "stores", "status_reason")
                      else ", NULL AS status_reason")
        for master_sid in matched_master_ids:
            mc.execute(
                "SELECT COUNT(*) as cnt FROM reviews WHERE store_id=?",
                (master_sid,),
            )
            new_count = mc.fetchone()["cnt"]
            mc.execute(
                "SELECT target_reviews, reviews_scraped, scrape_status" + reason_col +
                " FROM stores WHERE store_id=?",
                (master_sid,),
            )
            store = mc.fetchone()
            old_count = store["reviews_scraped"] or 0
            target = store["target_reviews"]

            # Recompute unconditionally.
            #
            # The old version only wrote when the count went UP, so any deletion
            # left reviews_scraped permanently overstated and the status frozen.
            #
            # And a missing target no longer means "complete". `target or 0`
            # followed by `target == 0 -> complete` marked 157 stores finished on
            # their first merge, 152 of them holding no reviews at all. Because
            # the scraping export selects only pending and incomplete stores and
            # nothing ever demotes a store, those rows became unreachable by
            # every tool in the pipeline. 'unknown_target' keeps them visible.
            if store["status_reason"]:
                new_status = store["scrape_status"]
            elif target is None or target <= 0:
                new_status = "unknown_target"
            elif new_count >= target:
                new_status = "complete"
            elif new_count == 0:
                new_status = "not_started"
            else:
                new_status = "incomplete"

            if new_count != old_count or new_status != (store.get("scrape_status") or ""):
                stats["stores_updated"] += 1
            mc.execute(
                "UPDATE stores SET reviews_scraped=?, scrape_status=? WHERE store_id=?",
                (new_count, new_status, master_sid),
            )

        stats["place_ids_backfilled"] = _backfill_place_ids(
            mc, scraper_stores, sid_map, master_pid_map)
        master_conn.commit()

        # Read back what we actually wrote rather than trusting the counter we
        # incremented in memory. This is the same discipline the scraper had to
        # learn after the phantom-review incident.
        post = mc.execute("SELECT COUNT(*) AS c FROM reviews").fetchone()["c"]
        if post != stats["_pre_reviews"] + stats["reviews_new"]:
            raise RuntimeError(
                "Merge verification failed: expected "
                + str(stats["_pre_reviews"] + stats["reviews_new"])
                + " reviews after insert, found " + str(post)
                + ". Master has been left as-is; restore the backup before retrying."
            )

    scraper.close()
    return stats


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Merge scraper result databases into master reviews DB",
    )
    parser.add_argument("scraper_dbs", nargs="+", metavar="DB",
                        help="Scraper result .db files to merge")
    parser.add_argument("--master", default="Step_5_master_reviews.db",
                        help="Path to master reviews database")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be merged without writing")
    args = parser.parse_args(argv)

    if not os.path.exists(args.master):
        print(f"ERROR: Master database not found: {args.master}")
        sys.exit(1)

    for db in args.scraper_dbs:
        if not os.path.exists(db):
            print(f"ERROR: Scraper database not found: {db}")
            sys.exit(1)

    if args.dry_run:
        print("=== DRY RUN - no changes will be written ===\n")

    lock = None
    if not args.dry_run:
        lock = _acquire_lock(args.master)

    # Backup master before merging.
    #
    # The copy is preceded by a WAL checkpoint. Copying only the .db while
    # committed pages are still sitting in the -wal sidecar produces a backup
    # that is quietly missing the most recent writes, which is the same trap the
    # project documented for batch files being scp'd off a VM.
    if not args.dry_run:
        print(f"Backing up master to: {backup_master(args.master)}")
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

    if not args.dry_run:
        master.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    master.close()
    if lock is not None:
        lock.close()

    print("=" * 50)
    print("MERGE COMPLETE" if not args.dry_run else "DRY RUN COMPLETE")
    print(f"  Reviews: {pre_reviews:,} -> {post_reviews:,} (+{total_new:,} new, {total_dup:,} duplicates)")
    print(f"  Complete stores: {pre_complete} -> {post_complete}")
    print(f"  Status: {by_status}")


if __name__ == "__main__":
    main()
