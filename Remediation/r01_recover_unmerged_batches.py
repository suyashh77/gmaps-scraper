"""R01 - Recover reviews that were collected but never merged into master.

WHY THIS EXISTS
    batch1.db on machine tqehk2 ran 2026-05-08 -> 2026-05-10 and collected
    2,263 reviews. Every store row in that file still reads status='pending',
    reviews_scraped=0, and none of those reviews reached master. The work was
    done and then lost in the gap between the scraper and the merge step.

    Among them is a store the status report lists as "0 / 1500, not started"
    which in fact has a complete 1,500-review harvest sitting on disk.

WHAT IT DOES
    Scans every batch-shaped .db under Data/batches/, maps its stores to master by
    place_id, and inserts the reviews master does not already hold.

    It differs from linux_scraper/merge_results.py in four ways, each a bug
    fix rather than a preference:
      1. carries review_id and machine_id through to master. merge_results
         drops both, discarding Google's own review identifier and the
         per-machine provenance the documentation claims every review has.
      2. recomputes reviews_scraped unconditionally. merge_results only
         updates when the count went UP, so any deletion leaves the counter
         permanently overstated.
      3. never marks a store 'complete' merely because target_reviews is 0
         or NULL. That single expression is the root cause of stale
         scrape_status.
      4. checkpoints and backs up the WAL sidecar along with the .db.

USAGE
    python Remediation/r01_recover_unmerged_batches.py            # dry run
    python Remediation/r01_recover_unmerged_batches.py --apply
"""

from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import (DATA, MASTER, backup, connect_ro, connect_rw, counts,
                        print_delta)

BATCH_REQUIRED_COLS = {"google_place_id", "status", "target_reviews"}


def is_batch_db(path: str) -> bool:
    if os.path.abspath(path) == os.path.abspath(MASTER):
        return False
    if "backup" in os.path.basename(path).lower():
        return False
    try:
        conn = connect_ro(path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(stores)")}
        conn.close()
        return BATCH_REQUIRED_COLS.issubset(cols)
    except sqlite3.Error:
        return False


def master_has_column(conn, table: str, col: str) -> bool:
    return any(r[1] == col for r in conn.execute("PRAGMA table_info(" + table + ")"))


def scan(batch_path: str, master) -> dict:
    """Return the reviews in batch_path that master does not already hold."""
    b = connect_ro(batch_path)
    mc = master.cursor()

    pid_to_master = {}
    for r in mc.execute("SELECT store_id, place_id FROM stores WHERE place_id IS NOT NULL"):
        pid_to_master[r["place_id"]] = r["store_id"]

    sid_map, unmatched = {}, []
    for r in b.execute("SELECT store_id, google_place_id, input_name FROM stores"):
        pid = r["google_place_id"]
        if pid and pid in pid_to_master:
            sid_map[r["store_id"]] = pid_to_master[pid]
        elif pid is not None:
            unmatched.append((r["input_name"], pid))

    bcols = {c[1] for c in b.execute("PRAGMA table_info(reviews)")}
    has_rid = "review_id" in bcols
    has_mid = "machine_id" in bcols

    touched = sorted(set(sid_map.values()))
    existing = set()
    for msid in touched:
        for r in mc.execute(
            "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=?",
            (msid,),
        ):
            existing.add((msid, r["reviewer_name"] or "", r["rating"],
                          (r["review_text"] or "")[:50]))

    cols = ["store_id", "reviewer_name", "rating", "date_relative", "review_text",
            "helpful_count", "photo_count", "has_owner_response",
            "reviewer_review_count", "reviewer_photo_count", "is_local_guide",
            "service_type", "scraped_at"]
    if has_rid:
        cols.append("review_id")
    if has_mid:
        cols.append("machine_id")

    new_rows, dupes = [], 0
    for r in b.execute("SELECT " + ", ".join(cols) + " FROM reviews"):
        msid = sid_map.get(r["store_id"])
        if msid is None:
            continue
        key = (msid, r["reviewer_name"] or "", r["rating"], (r["review_text"] or "")[:50])
        if key in existing:
            dupes += 1
            continue
        existing.add(key)
        new_rows.append((msid, dict(r)))
    b.close()
    return {"path": batch_path, "new": new_rows, "dupes": dupes,
            "unmatched": unmatched, "touched": touched,
            "has_rid": has_rid, "has_mid": has_mid}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="write to master (default is a dry run)")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--batches", nargs="*", default=None)
    args = ap.parse_args()

    paths = args.batches or sorted(glob.glob(os.path.join(DATA, "batches", "*.db")))
    batches = [p for p in paths if is_batch_db(p)]
    if not batches:
        print("No batch databases found.")
        return 1

    print("Master : " + args.master)
    print("Batches: " + str(len(batches)) + " found\n")

    if args.apply:
        dest = backup(args.master, "r01")
        print("Backup written: " + os.path.basename(dest) + "\n")

    master = connect_rw(args.master) if args.apply else connect_ro(args.master)
    before = counts(master)

    ext_rid = master_has_column(master, "reviews", "review_id")
    ext_mid = master_has_column(master, "reviews", "machine_id")
    if not (ext_rid and ext_mid):
        print("NOTE: master.reviews has no review_id/machine_id column. Run r02 first")
        print("      so Google's own review identifier survives the merge.\n")

    grand_new = 0
    for path in batches:
        res = scan(path, master)
        name = os.path.basename(path)
        print(name)
        print("  stores touched in master : " + str(len(res["touched"])))
        print("  reviews new to master    : " + format(len(res["new"]), ","))
        print("  reviews already present  : " + format(res["dupes"], ","))
        if res["unmatched"]:
            print("  stores NOT in master     : " + str(len(res["unmatched"]))
                  + " (their reviews cannot be placed)")
            for nm, pid in res["unmatched"][:5]:
                print("      " + str(nm) + "  " + str(pid))
        grand_new += len(res["new"])

        if args.apply and res["new"]:
            cur = master.cursor()
            cols = ["store_id", "reviewer_name", "rating", "date_relative", "review_text",
                    "helpful_count", "photo_count", "has_owner_response",
                    "reviewer_review_count", "reviewer_photo_count", "is_local_guide",
                    "service_type", "scraped_at", "source_db"]
            if ext_rid:
                cols.append("review_id")
            if ext_mid:
                cols.append("machine_id")
            ph = ",".join(["?"] * len(cols))
            rows = []
            for msid, r in res["new"]:
                v = [msid, r["reviewer_name"], r["rating"], r["date_relative"],
                     r["review_text"],
                     int(r["helpful_count"] or 0), int(r["photo_count"] or 0),
                     int(r["has_owner_response"] or 0),
                     int(r["reviewer_review_count"] or 0),
                     int(r["reviewer_photo_count"] or 0),
                     int(r["is_local_guide"] or 0),
                     r["service_type"] or "", r["scraped_at"], name]
                if ext_rid:
                    v.append(r.get("review_id"))
                if ext_mid:
                    v.append(r.get("machine_id"))
                rows.append(tuple(v))
            cur.execute("BEGIN IMMEDIATE")
            cur.executemany(
                "INSERT INTO reviews (" + ",".join(cols) + ") VALUES (" + ph + ")", rows)
            for msid in res["touched"]:
                n = cur.execute("SELECT COUNT(*) FROM reviews WHERE store_id=?",
                                (msid,)).fetchone()[0]
                tgt = cur.execute("SELECT target_reviews FROM stores WHERE store_id=?",
                                  (msid,)).fetchone()[0]
                if tgt:
                    status = "complete" if n >= tgt else "incomplete"
                else:
                    status = "unknown_target"
                cur.execute(
                    "UPDATE stores SET reviews_scraped=?, scrape_status=? WHERE store_id=?",
                    (n, status, msid))
            master.commit()
        print()

    after = counts(master)
    print_delta(before, after)
    verb = "recovered" if args.apply else "recoverable"
    print("\n" + ("APPLIED" if args.apply else "DRY RUN") + " - "
          + format(grand_new, ",") + " reviews " + verb)
    if not args.apply:
        print("Re-run with --apply to write them.")
    master.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
