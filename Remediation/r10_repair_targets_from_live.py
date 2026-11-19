"""R10 - Replace fictional review targets with the counts Google actually reports.

THE PROBLEM
    target_reviews is not a measurement for a large part of the inventory. It was
    itself capped at 1,500, so 315 stores carry exactly that number regardless of
    their real size, and others carry stale values from source metadata that saw
    the store years ago. The consequences are not cosmetic:

      * completion is measured against a truncated denominator, so the headline
        fill rate is a ceiling on a floor
      * a store with a real total of 3,343 is declared finished at 1,500
      * a store with a real total of 12 and a target of 1,500 churns through the
        whole escalation ladder on every visit
      * stores that progress normally get marked failed for not reaching a number
        that was never real

THE FIX
    Every scraper visit now records Google's own review count in the batch
    column gmaps_reviews_count. That is a measurement, taken from the store's own
    page at scrape time. This tool copies it into master.

    It only ever writes a target it can defend:
      * the live count must be present and positive
      * a store already holding MORE reviews than Google now shows keeps the
        larger number, because Google removes reviews over time and the corpus
        should not shrink to match
      * the cap is applied last and recorded, so a truncated target is visibly
        truncated rather than silently indistinguishable from a real one

USAGE
    python Remediation/r10_repair_targets_from_live.py --batch Data/batches/run1.db
    python Remediation/r10_repair_targets_from_live.py --batch Data/batches/run1.db --apply
    python Remediation/r10_repair_targets_from_live.py --batch Data/batches/run1.db --apply --cap 0
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import DATA, MASTER, backup, connect_ro, connect_rw


def recompute_statuses(conn) -> None:
    """Recompute status against the repaired targets.

    A store with a status_reason carries a ruling (r12/r13): its status is a
    decision, not a function of the counts, so it is left alone — as merge_results,
    r03 and r09 already do.
    """
    has_reason = any(c[1] == "status_reason" for c in conn.execute("PRAGMA table_info(stores)"))
    conn.execute("""
        UPDATE stores SET scrape_status = CASE
          WHEN target_reviews IS NULL OR target_reviews = 0 THEN 'unknown_target'
          WHEN reviews_scraped >= target_reviews            THEN 'complete'
          WHEN reviews_scraped = 0                          THEN 'not_started'
          ELSE 'incomplete' END
    """ + (" WHERE status_reason IS NULL" if has_reason else ""))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", nargs="*", default=None,
                    help="batch DBs holding recorded live counts (default: Data/batches/*.db)")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--cap", type=int, default=1500,
                    help="review cap to apply; 0 means no cap")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    paths = args.batch or sorted(glob.glob(os.path.join(DATA, "batches", "*.db")))
    live: dict[str, int] = {}
    for p in paths:
        if os.path.abspath(p) == os.path.abspath(args.master) or "backup" in p.lower():
            continue
        try:
            b = connect_ro(p)
            cols = {c[1] for c in b.execute("PRAGMA table_info(stores)")}
            if not {"google_place_id", "gmaps_reviews_count"} <= cols:
                b.close()
                continue
            for r in b.execute(
                "SELECT google_place_id, gmaps_reviews_count FROM stores "
                "WHERE gmaps_reviews_count IS NOT NULL AND gmaps_reviews_count > 0"
            ):
                pid = r["google_place_id"]
                if pid:
                    live[pid] = max(live.get(pid, 0), int(r["gmaps_reviews_count"]))
            b.close()
        except Exception:                                    # noqa: BLE001
            continue

    print("Live counts recovered from batch files: " + format(len(live), ","))
    if not live:
        print("Nothing to do. Run the scraper first; it records a live count per visit.")
        return 0

    m = connect_ro(args.master)
    rows = m.execute(
        "SELECT store_id, place_id, store_name, target_reviews, reviews_scraped "
        "FROM stores WHERE place_id IS NOT NULL").fetchall()
    m.close()

    updates, raised, lowered, capped, skipped = [], 0, 0, 0, 0
    for r in rows:
        lc = live.get(r["place_id"])
        if not lc:
            continue
        have = r["reviews_scraped"] or 0
        old = r["target_reviews"] or 0
        # Never shrink below what we already hold: Google removes reviews, and a
        # target under the collected count would make a complete store look
        # over-scraped forever.
        new = max(lc, have)
        if args.cap > 0:
            if new > args.cap:
                capped += 1
            new = min(new, args.cap)
        if new == old:
            skipped += 1
            continue
        if new > old:
            raised += 1
        else:
            lowered += 1
        updates.append((new, r["store_id"], r["store_name"], old, lc, have))

    print("\nTARGET REPAIR")
    print("  stores with a live count in master   " + format(
        sum(1 for r in rows if live.get(r["place_id"])), ">7,"))
    print("  targets unchanged                    " + format(skipped, ">7,"))
    print("  targets raised                       " + format(raised, ">7,"))
    print("  targets lowered                      " + format(lowered, ">7,"))
    print("  hit the cap of " + str(args.cap).ljust(21) + format(capped, ">7,"))

    if updates:
        print("\n  biggest corrections:")
        for new, sid, name, old, lc, have in sorted(
                updates, key=lambda u: -abs(u[0] - u[3]))[:12]:
            print("   " + str(sid).rjust(5) + "  " + str(name)[:26].ljust(26)
                  + str(old).rjust(6) + " -> " + str(new).ljust(6)
                  + "  (google says " + str(lc) + ", we hold " + str(have) + ")")

    if not args.apply:
        print("\nDRY RUN. Re-run with --apply.")
        return 0

    dest = backup(args.master, "r10")
    print("\nBackup written: " + os.path.basename(dest))

    conn = connect_rw(args.master)
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    for new, sid, *_ in updates:
        cur.execute("UPDATE stores SET target_reviews=? WHERE store_id=?", (new, sid))
    recompute_statuses(conn)
    conn.commit()
    print("  updated " + format(len(updates), ",") + " targets and recomputed every unruled status")
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
