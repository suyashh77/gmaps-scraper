"""R07 - Build the known_review_keys skip-list into a batch database.

WHY THIS EXISTS
    `known_review_keys` is read by database.get_known_review_keys() and used by
    the scroll loop to recognise reviews master already holds, which is what
    drives the 60-consecutive-known early exit. batch1.db holds 399,984 of these
    keys and batch2.db holds 399,076.

    Nothing in the repository writes them. `grep -rn known_review_keys` finds a
    CREATE TABLE, two readers, and no writer at all. They were produced by a
    script that was never committed.

    That makes the single most important correctness input to the scrape
    unauditable. Nobody can say which master snapshot the keys came from, whether
    every store was covered, or whether the key format still matches
    DatabaseManager.make_dedup_key. A format drift silently disables the
    skip-list, which costs a full re-scrape; an over-broad key silently skips
    real reviews, which loses data. Neither failure is visible in a log.

    This tool writes them, records where they came from, and verifies the format
    against the live implementation rather than assuming it.

USAGE
    python Remediation/r07_build_known_keys.py --batch Data/batches/run1.db
    python Remediation/r07_build_known_keys.py --batch Data/batches/run1.db --apply
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_ro, connect_rw

# Import the real key function so this tool can never drift from the scraper.
_SCRAPER_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "Scraper_v2")
if _SCRAPER_DIR not in sys.path:
    sys.path.insert(0, _SCRAPER_DIR)
try:
    from linux_scraper.database import DatabaseManager
    MAKE_KEY = DatabaseManager.make_dedup_key
except Exception:                                   # noqa: BLE001
    MAKE_KEY = None


def fallback_key(reviewer_name, rating, review_text) -> str:
    text = (review_text or "")[:50]
    return f"{(reviewer_name or '').strip()}|{rating}|{text}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True, help="batch database to populate")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(args.batch):
        print("No such batch database: " + args.batch)
        return 1

    key_fn = MAKE_KEY or fallback_key
    if MAKE_KEY is None:
        print("WARNING: could not import DatabaseManager.make_dedup_key.")
        print("         Falling back to a local copy of the key format, which is")
        print("         exactly the drift risk this tool exists to remove.")
    else:
        print("Key format: DatabaseManager.make_dedup_key (imported from the scraper)")

    m = connect_ro(args.master)
    b = connect_ro(args.batch) if not args.apply else connect_rw(args.batch)

    pid_to_master = {r["place_id"]: r["store_id"] for r in
                     m.execute("SELECT store_id, place_id FROM stores "
                               "WHERE place_id IS NOT NULL")}

    master_ids = {r["store_id"] for r in m.execute("SELECT store_id FROM stores")}
    has_msid = any(r["name"] == "master_store_id" for r in b.execute("PRAGMA table_info(stores)"))
    msid_col = ", master_store_id" if has_msid else ", NULL AS master_store_id"
    has_mr = any(r["name"] == "master_reviews" for r in b.execute("PRAGMA table_info(stores)"))
    mr_col = ", COALESCE(master_reviews, 0) AS held_in_master" if has_mr else ", 1 AS held_in_master"
    pairs, missing, unfiltered = [], 0, 0
    for r in b.execute("SELECT store_id, google_place_id" + msid_col + mr_col + " FROM stores"):
        if not r["held_in_master"]:
            unfiltered += 1             # r09 --also-ids: collect in full, so no skip-list
            continue
        msid = r["master_store_id"] if r["master_store_id"] in master_ids else None
        if msid is None:
            msid = pid_to_master.get(r["google_place_id"])
        if msid is None:
            missing += 1
            continue
        pairs.append((r["store_id"], msid))

    print("\nbatch stores            : " + format(len(pairs) + missing + unfiltered, ","))
    print("matched into master     : " + format(len(pairs), ","))
    print("unmatched (no keys)     : " + format(missing, ","))
    print("collected in full       : " + format(unfiltered, ",") + "   (batch says nothing is held: no skip-list)")

    rows, covered = [], 0
    for batch_sid, master_sid in pairs:
        n = 0
        for rv in m.execute(
            "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=?",
            (master_sid,),
        ):
            rows.append((batch_sid, key_fn(rv["reviewer_name"], rv["rating"],
                                           rv["review_text"])))
            n += 1
        if n:
            covered += 1

    print("stores with prior reviews: " + format(covered, ","))
    print("keys to write            : " + format(len(rows), ","))

    existing = b.execute("SELECT COUNT(*) FROM known_review_keys").fetchone()[0]
    print("keys already present     : " + format(existing, ","))

    if not args.apply:
        print("\nDRY RUN. Re-run with --apply to write them.")
        m.close()
        b.close()
        return 0

    dest = backup(args.batch, "r07")
    print("\nBackup written: " + os.path.basename(dest))

    cur = b.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("DELETE FROM known_review_keys")
    cur.executemany(
        "INSERT OR IGNORE INTO known_review_keys (store_id, dedup_key) VALUES (?, ?)", rows)
    written = cur.execute("SELECT COUNT(*) FROM known_review_keys").fetchone()[0]

    # Provenance. Without this the next person has the same problem we had.
    cur.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)")
    for k, v in [
        ("known_keys_source", os.path.basename(args.master)),
        ("known_keys_built_at", datetime.now().isoformat(timespec="seconds")),
        ("known_keys_count", str(written)),
        ("known_keys_stores_covered", str(covered)),
        ("known_keys_format", "DatabaseManager.make_dedup_key" if MAKE_KEY else "fallback"),
        ("known_keys_tool", "Remediation/r07_build_known_keys.py"),
    ]:
        cur.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", (k, v))
    b.commit()
    b.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print("wrote " + format(written, ",") + " keys across " + str(covered) + " stores")
    print("provenance recorded in the batch metadata table")
    m.close()
    b.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
