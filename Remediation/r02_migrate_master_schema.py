"""R02 - Add the provenance and identity columns master is missing.

WHY THIS EXISTS
    The batch databases capture two fields per review that the merge step
    silently discards:

      review_id   Google's own data-review-id. It is stable within a session,
                  which makes it the exact key for "did this run already see
                  this review". merge_results.py does not select it, so master
                  has no column for it, so every future merge is forced onto
                  the weak 50-character-prefix content key. That key is
                  responsible for the surviving duplicate pairs and, worse,
                  can drop two genuinely different reviews that happen to open
                  with the same sentence.

      machine_id  Which machine produced the row. The quality report states
                  that every review is traceable to the run that produced it.
                  Only source_db survives, which names the file, not the host.

    This migration also adds columns that make the known defects queryable
    instead of anecdotal:

      stores.qa_flags        comma-separated defect tags (OVER_SCRAPED,
                             INVALID_PLACE_ID, NULL_CATEGORY, CAP_TRUNCATED,
                             DISPUTED_EXCLUSION, NON_NC_GEO)
      stores.verified_at     when a human or tool last confirmed the store's
                             identity against Google
      stores.review_cap      the cap in force when this store was scraped,
                             so a store truncated at 1,500 is distinguishable
                             from a store that genuinely has 1,500 reviews
      reviews.review_text_sha256  full-text hash, the dedup key that should
                             have been used from the start

    Adding a column is not the same as filling it. This tool only creates the
    columns and backfills what can be computed from data already in master
    (the hash, and review_cap where the count sits exactly at the cap).
    review_id and machine_id stay NULL for the 1.2M historical rows; they can
    only be populated by re-scraping, and the NULL is the honest record of
    that. Every future merge through r01 carries them.

IDEMPOTENT
    Uses ALTER TABLE inside a swallowed duplicate-column error, the standard
    migration idiom already used elsewhere in this codebase. Safe to re-run.

USAGE
    python Remediation/r02_migrate_master_schema.py            # dry run
    python Remediation/r02_migrate_master_schema.py --apply
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_ro, connect_rw

NEW_COLUMNS = [
    ("reviews", "review_id", "TEXT"),
    ("reviews", "machine_id", "TEXT"),
    ("reviews", "review_text_sha256", "TEXT"),
    ("stores", "qa_flags", "TEXT"),
    ("stores", "verified_at", "TEXT"),
    ("stores", "review_cap", "INTEGER"),
]

INDEXES = [
    ("idx_reviews_store", "reviews(store_id)"),
    ("idx_reviews_sha", "reviews(store_id, review_text_sha256)"),
    ("idx_reviews_reviewid", "reviews(store_id, review_id)"),
    ("idx_stores_place", "stores(place_id)"),
]


def existing_columns(conn, table: str) -> set:
    return {r[1] for r in conn.execute("PRAGMA table_info(" + table + ")")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--cap", type=int, default=1500)
    args = ap.parse_args()

    probe = connect_ro(args.master)
    missing = [(t, c, ty) for (t, c, ty) in NEW_COLUMNS
               if c not in existing_columns(probe, t)]
    have_idx = {r[0] for r in probe.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    probe.close()

    print("Columns to add : " + (", ".join(t + "." + c for t, c, _ in missing)
                                 if missing else "none, already migrated"))
    print("Indexes to add : " + (", ".join(n for n, _ in INDEXES if n not in have_idx)
                                 or "none"))

    if not args.apply:
        print("\nDRY RUN. Re-run with --apply to migrate.")
        return 0

    dest = backup(args.master, "r02")
    print("\nBackup written: " + os.path.basename(dest))

    conn = connect_rw(args.master)
    cur = conn.cursor()

    for table, col, coltype in NEW_COLUMNS:
        try:
            cur.execute("ALTER TABLE " + table + " ADD COLUMN " + col + " " + coltype)
            print("  added   " + table + "." + col)
        except sqlite3.OperationalError as exc:
            if "duplicate column" not in str(exc).lower():
                raise
            print("  present " + table + "." + col)

    for name, spec in INDEXES:
        cur.execute("CREATE INDEX IF NOT EXISTS " + name + " ON " + spec)
        print("  index   " + name)
    conn.commit()

    # Backfill the full-text hash. This is the key every future dedup should
    # use; computing it now makes the existing duplicates findable in one pass.
    print("\nBackfilling review_text_sha256 ...")
    todo = cur.execute(
        "SELECT COUNT(*) FROM reviews WHERE review_text_sha256 IS NULL").fetchone()[0]
    print("  rows needing a hash: " + format(todo, ","))
    done = 0
    while True:
        rows = cur.execute(
            "SELECT id, reviewer_name, rating, review_text FROM reviews "
            "WHERE review_text_sha256 IS NULL LIMIT 50000").fetchall()
        if not rows:
            break
        payload = []
        for r in rows:
            blob = "\x1f".join([
                str(r["reviewer_name"] or ""),
                str(r["rating"] if r["rating"] is not None else ""),
                (r["review_text"] or "").strip(),
            ]).encode("utf-8", "replace")
            payload.append((hashlib.sha256(blob).hexdigest(), r["id"]))
        cur.execute("BEGIN IMMEDIATE")
        cur.executemany("UPDATE reviews SET review_text_sha256=? WHERE id=?", payload)
        conn.commit()
        done += len(payload)
        print("  " + format(done, ",") + " / " + format(todo, ","), end="\r")
    print("\n  hashed " + format(done, ",") + " rows")

    # Record the cap that was in force, and flag stores sitting exactly on it.
    # A store at 1,500 is truncated by our own policy, not complete.
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE stores SET review_cap=? WHERE review_cap IS NULL", (args.cap,))
    capped = cur.execute(
        "SELECT COUNT(*) FROM stores WHERE reviews_scraped >= ?", (args.cap,)).fetchone()[0]
    conn.commit()
    print("  review_cap recorded; " + str(capped)
          + " stores sit at or above the cap and are truncated by policy")

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    print("\nMigration complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
