"""R14 - Apply the completed manual Google checks (2026-10-08) to master.

Reads the completed workbooks (Manual_Check_Round2/3 ..._done.xlsx):

  closed_flag_check, no_target_check, failed_stores_check
      CLOSED_PERMANENT / CLOSED_TEMPORARY -> the store and its reviews leave the dataset
                                             (Data/Step_5_closed_stores.db)
      OPEN_OK                             -> closed flag cleared; Google's count becomes the target
                                             (0 reviews: complete, "no data")
      NOT_ON_GOOGLE                       -> out of scope (Data/Step_5_out_of_scope.db)
      anything else                       -> listed, never guessed

  shared_page_check, held_back_check
      A store whose saved link opens another store's page loses every review it holds, its
      partners lose the reviews that match them, and it is re-pointed at its real page
      (or removed if the real store is closed / missing). See r12.apply_page_owner_answers.

USAGE
    python Remediation/r14_manual_answers.py --round2 X_done.xlsx --round3 Y_done.xlsx           # dry run
    python Remediation/r14_manual_answers.py --round2 X_done.xlsx --round3 Y_done.xlsx --apply
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_rw             # noqa: E402
import r12_rulings as R12                                     # noqa: E402
import r13_rulings as R13                                     # noqa: E402

RULING_DATE = "2026-10-08"


def _clean(v) -> str:
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()


def _count(v):
    s = _clean(v)
    try:
        return int(float(s))
    except ValueError:
        return None


def rulings_from_sheet(df: pd.DataFrame, sheet: str, ruling_date: str = RULING_DATE):
    """Decision rows for r13.step_store_rulings, plus the store ids left unruled."""
    rows, skipped = [], []
    for r in df.to_dict("records"):
        sid, outcome, n = int(r["master_store_id"]), _clean(r.get("outcome")).upper(), _count(r.get("google_review_count"))
        why = "; ".join(x for x in (f"manual check {ruling_date} ({sheet})", _clean(r.get("evidence")),
                                    f"Google shows {n} reviews" if n is not None else "",
                                    _clean(r.get("notes"))) if x)[:300]
        if outcome in ("CLOSED_PERMANENT", "CLOSED_TEMPORARY"):
            rows.append(dict(store_id=sid, decision=outcome, target="", reason=why))
        elif outcome == "OPEN_OK" and n == 0:
            rows.append(dict(store_id=sid, decision="NO_DATA", target="", reason=why))
        elif outcome == "OPEN_OK":
            rows.append(dict(store_id=sid, decision="OPEN", target="" if n is None else str(n), reason=why))
        elif outcome == "NOT_ON_GOOGLE":
            rows.append(dict(store_id=sid, decision="OUT", target="", reason=why))
        else:
            skipped.append(sid)
    return rows, skipped


def page_answers_from_sheets(frames: list) -> list:
    """One answer per store (a later sheet replaces an earlier one) for R12.apply_page_owner_answers."""
    by_store = {}
    for df in frames:
        for r in df.to_dict("records"):
            opens, real = _clean(r.get("stored_link_opens_own_store")).upper(), _clean(r.get("real_store_exists")).upper()
            if not opens:
                continue
            sid = int(r["master_store_id"])
            n = _count(r.get("real_store_review_count"))
            by_store[sid] = dict(
                store_id=sid, opens_own=opens, real_exists=real, real_url=_clean(r.get("real_store_url")),
                real_address=_clean(r.get("real_store_address")), real_count="" if n is None else str(n),
                partners=[int(x) for x in re.findall(r"store (\d+)", _clean(r.get("shares_page_with")))])
    return list(by_store.values())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--round2", required=True)
    ap.add_argument("--round3", required=True)
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--ruling-date", default=RULING_DATE)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    rulings, skipped, frames = [], [], []
    for path, sheets in ((args.round2, ("closed_flag_check",)),
                         (args.round3, ("no_target_check", "failed_stores_check"))):
        for sh in sheets:
            rows, sk = rulings_from_sheet(pd.read_excel(path, sheet_name=sh), sh, args.ruling_date)
            rulings += rows
            skipped += sk
    frames.append(pd.read_excel(args.round2, sheet_name="shared_page_check"))
    frames.append(pd.read_excel(args.round3, sheet_name="held_back_check"))
    answers = page_answers_from_sheets(frames)

    if args.apply:
        print("backup: " + backup(args.master, "r14"))
    con = connect_rw(args.master)
    con.row_factory = sqlite3.Row
    con.execute("BEGIN IMMEDIATE")
    R12.ensure_schema(con)
    owner = R12.apply_page_owner_answers(con, answers, args.ruling_date)
    counts = R13.step_store_rulings(con, rulings, ruling_date=args.ruling_date)
    R12.recompute(con)
    print("page-owner answers: " + str({k: (v if k == 'quarantined' else len(v)) for k, v in owner.items()}))
    print("store rulings:      " + str(counts))
    print("left unruled:       " + str(skipped))
    if not args.apply:
        con.rollback()
        con.close()
        print("\nDRY RUN - rolled back. Re-run with --apply.")
        return 0
    con.commit()
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    print("out of scope -> " + str(R13.split_out_of_scope(args.master, R13.OUT_OF_SCOPE_DB)))
    print("closed       -> " + str(R13.split_out_of_scope(args.master, R13.CLOSED_DB, status="closed")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
