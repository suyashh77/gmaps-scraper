"""R15 - Rule near-miss stores complete (owner rule 2026-10-09).

Reviews get deleted on Google, so a store within tolerance of its target is complete: within 20
for targets over 1,000, within 10 for targets over 100, under 10 below that (the rule lives in
r13_rulings.stalled_is_complete). Only incomplete stores with a normal place_id and no earlier
ruling are touched; stores with a malformed place_id may hold another store's reviews and go to
manual instead. Rulings set status_reason, so r10/r12 recompute never undoes them.

USAGE
    python Remediation/r15_tolerance.py            # dry run
    python Remediation/r15_tolerance.py --apply    # backs master up first
Re-run after each manual round: a corrected target can bring a store inside tolerance.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_rw          # noqa: E402
import r13_rulings as R13                                   # noqa: E402

RULING_DATE = "2026-10-09"
# Rulings that completed a store by count alone. A store short of its target under the current rule
# is reopened; rulings that do not rest on a count ("no data", unverifiable, closed...) are left.
COUNT_RULINGS = ("stalled%", "within tolerance%", "no target;%")


def reopen_short(con, ruling_date: str = RULING_DATE) -> list:
    """Reopen stores that were ruled complete by count but are short of their target under the
    owner's rule (r13.stalled_is_complete). The old reason is kept in reopened_rulings. Returns ids."""
    con.execute("CREATE TABLE IF NOT EXISTS reopened_rulings (store_id INTEGER PRIMARY KEY, "
                "old_reason TEXT, reopened_at TEXT)")
    like = " OR ".join("status_reason LIKE ?" for _ in COUNT_RULINGS)
    rows = con.execute(
        "SELECT store_id, target_reviews, reviews_scraped, status_reason FROM stores "
        f"WHERE scrape_status='complete' AND target_reviews > 0 AND ({like}) ORDER BY store_id",
        COUNT_RULINGS).fetchall()
    short = [r for r in rows if not R13.stalled_is_complete(r["reviews_scraped"] or 0, r["target_reviews"])]
    for r in short:
        con.execute("INSERT OR IGNORE INTO reopened_rulings VALUES (?,?,?)",
                    (r["store_id"], r["status_reason"], ruling_date))
        con.execute("UPDATE stores SET status_reason=NULL, scrape_status=CASE WHEN COALESCE(reviews_scraped,0)=0 "
                    "THEN 'not_started' ELSE 'incomplete' END WHERE store_id=?", (r["store_id"],))
    return [r["store_id"] for r in short]


def apply_tolerance(con, ruling_date: str = RULING_DATE) -> list:
    """Rule within-tolerance incomplete stores complete. Returns the store ids ruled."""
    rows = con.execute(
        "SELECT store_id, target_reviews, reviews_scraped FROM stores "
        "WHERE scrape_status='incomplete' AND status_reason IS NULL AND reviews_scraped > 0 "
        "AND target_reviews > 0 AND (place_id LIKE 'ChIJ%' OR place_id LIKE '0x%:0x%') "
        "ORDER BY store_id").fetchall()
    near = [r for r in rows if R13.stalled_is_complete(r["reviews_scraped"], r["target_reviews"])]
    R13.step_store_rulings(
        con, [dict(store_id=r["store_id"], decision="COMPLETE", target="",
                   reason=f"within tolerance: {r['reviews_scraped']} of "
                          f"{r['target_reviews']} (owner rule {ruling_date})")
              for r in near], ruling_date=ruling_date)
    return [r["store_id"] for r in near]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if args.apply:
        print("backup: " + backup(args.master, "r15"))
    con = connect_rw(args.master)
    con.row_factory = sqlite3.Row
    con.execute("BEGIN IMMEDIATE")
    reopened = reopen_short(con)
    print(f"reopened for rescrape: {len(reopened)} stores")
    ruled = apply_tolerance(con)
    left = con.execute("SELECT COUNT(*), COALESCE(SUM(target_reviews - reviews_scraped),0) FROM stores "
                       "WHERE scrape_status='incomplete'").fetchone()
    print(f"ruled complete: {len(ruled)} stores; still incomplete: {left[0]} stores, "
          f"{left[1]:,} reviews short")
    if args.apply:
        con.commit()
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    else:
        con.rollback()
        print("DRY RUN - rolled back. Re-run with --apply.")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
