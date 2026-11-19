"""R18 - Compare a full, unfiltered rescrape with what a store holds. Read-only: moves nothing.

A store that holds more reviews than Google shows is queued with `r09 --also-ids`, so it is collected
in full with no skip-list. Its fresh scrape is then the page as it is today. This script compares the
two and reports, per store:

  CONSISTENT        the scrape returned at least 90 % of Google's live count and at least 90 % of what
                    it returned is already held. The store is the right store; the extra reviews we
                    hold are ones the page no longer shows (deleted since). Decided: keep all.
  MISMATCH          under half of the page's reviews are held. Wrong page, or the page changed.
  PARTIAL           in between.
  SCRAPE_TOO_SMALL  the scrape returned under 90 % of the live count: not a trustworthy snapshot.
  NO_FULL_SCRAPE    no completed, skip-list-free scrape on file yet.

Everything except CONSISTENT is not decidable with accuracy and goes to a manual workbook, together
with every store that is still short of its target after the completion rules.

USAGE
    python Remediation/r18_compare_rescrape.py                # report + Round 6 manual workbook
    python Remediation/r18_compare_rescrape.py --stores 3179 3249 --no-manual
"""

from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, connect_ro                      # noqa: E402
import make_manual_round5 as M5                                  # noqa: E402

DATA = os.path.dirname(MASTER)
RETURNS = os.path.join(DATA, "returns")
REPORT = os.path.join(DATA, "exports", "OverTarget_Compare_2026-10-11.xlsx")
MANUAL = os.path.join(DATA, "exports", "Manual_Check_Round6_2026-10-11.xlsx")
MIN_SHARE_OF_LIVE = 0.9        # a snapshot this complete can be trusted
CONSISTENT_SHARE = 0.9         # share of the page we already hold
MISMATCH_SHARE = 0.5
OVER_BY = 5                    # "holds more than target" means more than this many extra
CAP = 1500


def _key(name, rating, text) -> tuple:
    return ((name or "").strip(), rating, (text or "").strip())


def latest_full_scrape(returns_dir: str, master_store_id: int):
    """Review keys of the newest completed, skip-list-free scrape of this store, or None."""
    found = None
    for path in sorted(glob.glob(os.path.join(returns_dir, "*", "*.db"))):
        if any(x in path for x in ("pre_timeout", "interim", "backup")):
            continue
        b = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            tables = {r[0] for r in b.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"stores", "reviews"} <= tables or "master_store_id" not in [
                    r[1] for r in b.execute("PRAGMA table_info(stores)")]:
                continue
            row = b.execute("SELECT store_id, status FROM stores WHERE master_store_id=?",
                            (master_store_id,)).fetchone()
            if not row or row[1] != "completed":
                continue
            if "known_review_keys" in tables and b.execute(
                    "SELECT 1 FROM known_review_keys WHERE store_id=? LIMIT 1", (row[0],)).fetchone():
                continue
            found = {_key(*r) for r in b.execute(
                "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=?", (row[0],))}
        finally:
            b.close()
    return found


def compare_store(held: set, fresh, live: int) -> dict:
    """Verdict for one store. `held` and `fresh` are sets of (reviewer, rating, text) keys."""
    out = {"held": len(held), "fresh": 0 if fresh is None else len(fresh), "overlap": 0,
           "held_not_in_fresh": 0, "fresh_not_held": 0}
    if fresh is None:
        return {**out, "verdict": "NO_FULL_SCRAPE"}
    out["overlap"] = len(held & fresh)
    out["held_not_in_fresh"] = len(held - fresh)
    out["fresh_not_held"] = len(fresh - held)
    if len(fresh) < MIN_SHARE_OF_LIVE * live:
        return {**out, "verdict": "SCRAPE_TOO_SMALL"}
    share = out["overlap"] / max(len(fresh), 1)
    verdict = ("CONSISTENT" if share >= CONSISTENT_SHARE else "MISMATCH" if share < MISMATCH_SHARE else "PARTIAL")
    return {**out, "verdict": verdict}


def needs_manual(verdict: str) -> bool:
    return verdict != "CONSISTENT"


def over_target_ids(con) -> list:
    return [r[0] for r in con.execute(
        "SELECT store_id FROM stores WHERE target_reviews > 0 AND reviews_scraped > MIN(target_reviews, ?) + ? "
        "ORDER BY store_id", (CAP, OVER_BY))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stores", type=int, nargs="*", default=None,
                    help="default: every store holding more than its target by over 5")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--returns", default=RETURNS)
    ap.add_argument("--report", default=REPORT)
    ap.add_argument("--manual-out", default=MANUAL)
    ap.add_argument("--no-manual", action="store_true")
    args = ap.parse_args()

    con = connect_ro(args.master)
    con.row_factory = sqlite3.Row
    ids = args.stores if args.stores else over_target_ids(con)
    rows = []
    for sid in ids:
        s = con.execute("SELECT store_name, city, target_reviews, reviews_scraped FROM stores WHERE store_id=?",
                        (sid,)).fetchone()
        held = {_key(*r) for r in con.execute(
            "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=?", (sid,))}
        live = min(s["target_reviews"] or 0, CAP)
        rows.append({"master_store_id": sid, "store_name": s["store_name"], "city": s["city"],
                     "google_count": live, **compare_store(held, latest_full_scrape(args.returns, sid), live)})
    report = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    report.to_excel(args.report, index=False)
    print(report.verdict.value_counts().to_dict() if len(report) else "no stores")
    print("report -> " + args.report)

    if not args.no_manual:
        undecided = [r["master_store_id"] for r in rows if needs_manual(r["verdict"])]
        short = [r[0] for r in con.execute("SELECT store_id FROM stores WHERE scrape_status IN ('incomplete','not_started')")]
        ask = sorted(set(undecided) | set(short))
        con.close()
        counts = M5.build(args.master, args.manual_out, count_check_ids=ask)
        print(f"manual workbook ({len(undecided)} undecided over-target + {len(short)} still short) -> "
              f"{args.manual_out} {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
