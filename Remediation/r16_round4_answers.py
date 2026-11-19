"""R16 - Apply the completed Round 4 manual checks (2026-10-09) to master.

Sheets read: place_id_check, location_check, category_check, no_target_check, target_check.

  CLOSED_PERMANENT / CLOSED_TEMPORARY      -> store leaves master (Data/Step_5_closed_stores.db)
  NOT_ON_GOOGLE / NOT_IN_NC                -> out of scope (Data/Step_5_out_of_scope.db)
  OPEN_OK + new Google URL                 -> store re-pointed at that page, target = Google's count;
                                              if master already holds that page under another store,
                                              the store is a duplicate of it and folds into it
                                              (Data/Step_5_excluded_stores.db)
  OPEN_OK + count, no new URL              -> target = Google's count (never below what we hold)
  IN_NC (location_check)                   -> coordinates from the URL, address from the sheet
  OPEN_OK (category_check)                 -> category = google_category
  UNSURE / WRONG_PAGE / blank              -> listed, never guessed
A store with a closed / not-on-Google answer is not also re-pointed or re-targeted.
After the answers, stores within the owner's tolerance are ruled complete (r15).

USAGE
    python Remediation/r16_round4_answers.py ANSWERS_done.xlsx            # dry run
    python Remediation/r16_round4_answers.py ANSWERS_done.xlsx --apply    # backs master up first
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_rw             # noqa: E402
import r12_rulings as R12                                       # noqa: E402
import r13_rulings as R13                                       # noqa: E402
import r15_tolerance as R15                                     # noqa: E402

RULING_DATE = "2026-10-09"
EXCLUDED_DB = os.path.join(os.path.dirname(MASTER), "Step_5_excluded_stores.db")
SHEETS = ("place_id_check", "location_check", "category_check", "no_target_check", "target_check")
PLACE_URL = "https://www.google.com/maps/place/"
CLOSED = ("CLOSED_PERMANENT", "CLOSED_TEMPORARY")
GONE = ("NOT_ON_GOOGLE", "NOT_IN_NC")
TERMINAL = ("OUT",) + CLOSED


def _blank(v) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v)) or str(v).strip() == ""


def place_url(v) -> str:
    return str(v).strip() if not _blank(v) and str(v).strip().startswith(PLACE_URL) else ""


def count_from(v):
    if _blank(v):
        return None
    try:
        return int(float(str(v).strip()))
    except ValueError:
        return None


def latlon_from_url(url: str):
    m = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url or "")
    return (float(m.group(1)), float(m.group(2))) if m else None


def _text(v) -> str:
    return "" if _blank(v) else str(v).strip()


def _action(sheet, row, outcome):
    sid, url, n = int(row["master_store_id"]), place_url(row.get("correct_google_url")), count_from(row.get("google_review_count"))
    base = dict(store_id=sid, sheet=sheet, url=url, count=n, outcome=outcome,
                why=f"manual round 4 ({sheet}); {_text(row.get('notes'))[:160]}".rstrip("; "))
    if outcome in CLOSED:
        return dict(base, action=outcome)
    if outcome in GONE:
        return dict(base, action="OUT")
    if outcome not in ("OPEN_OK", "IN_NC"):
        return dict(base, action="SKIP", reason=f"{outcome or 'blank'} on {sheet}")
    if sheet == "place_id_check":
        return dict(base, action="REPOINT") if url else dict(base, action="SKIP", reason="OPEN_OK without a usable URL")
    if sheet == "location_check":
        return dict(base, action="LOCATION", address=_text(row.get("real_address")))
    if sheet == "category_check":
        cat = _text(row.get("google_category"))
        return dict(base, action="CATEGORY", category=cat) if cat else dict(base, action="SKIP", reason="no category given")
    if n is None:
        return dict(base, action="SKIP", reason=f"OPEN_OK without a count on {sheet}")
    if sheet == "no_target_check":
        return dict(base, action="NO_DATA" if n == 0 else "OPEN")      # open, Google shows no reviews
    return dict(base, action="REPOINT" if url else "TARGET")


def plan_actions(frames: dict, include_skipped: bool = False) -> list:
    """One or more actions per store; a closed / not-on-Google answer replaces all the others."""
    by_store: dict = {}
    for sheet in SHEETS:
        for row in frames.get(sheet, pd.DataFrame()).to_dict("records"):
            if _blank(row.get("master_store_id")):
                continue
            a = _action(sheet, row, _text(row.get("outcome")).upper())
            by_store.setdefault(a["store_id"], []).append(a)
    out = []
    for acts in by_store.values():
        terminal = [a for a in acts if a["action"] in TERMINAL]
        keep = terminal[:1] or [a for a in acts if a["action"] != "SKIP"] or acts[:1]
        out += [a for a in keep if include_skipped or a["action"] != "SKIP"]
    return out


def holder_of(con, store_id: int, fid: str):
    """Another store that already holds this Google page: same id, or same place half after the colon."""
    half = fid.split(":")[-1]
    row = con.execute(
        "SELECT store_id FROM stores WHERE store_id <> ? AND (LOWER(place_id) = ? OR LOWER(COALESCE(feature_id,'')) = ? "
        "OR LOWER(place_id) LIKE ? OR LOWER(COALESCE(feature_id,'')) LIKE ?) ORDER BY store_id LIMIT 1",
        (store_id, fid, fid, "%:" + half, "%:" + half)).fetchone()
    return row[0] if row else None


def _own_page(con, store_id: int, fid: str) -> bool:
    r = con.execute("SELECT LOWER(COALESCE(place_id,'')), LOWER(COALESCE(feature_id,'')) FROM stores "
                    "WHERE store_id=?", (store_id,)).fetchone()
    return bool(r) and fid in tuple(r)


def apply_actions(con, actions: list, ruling_date: str = RULING_DATE) -> dict:
    """Write the actions to master. Returns counts; the caller recomputes statuses and commits."""
    rulings, counts = [], {}

    def ruling(a, decision, target=""):
        rulings.append(dict(store_id=a["store_id"], decision=decision, target=target, reason=a["why"]))

    for a in actions:
        act, sid = a["action"], a["store_id"]
        if act in CLOSED:
            ruling(a, act)
        elif act == "OUT":
            ruling(a, "OUT")
        elif act == "OPEN":
            ruling(a, "OPEN", str(a["count"]))
        elif act == "NO_DATA":
            ruling(a, "NO_DATA")
        elif act == "TARGET":
            ruling(a, "TARGET", str(a["count"]))
        elif act == "REPOINT":
            fid = R12.feature_id_from_url(a["url"])
            keeper = holder_of(con, sid, fid) if fid else None
            if not fid:
                act = "SKIP"
            elif keeper:
                act = "DUP"
                ruling(a, f"DUP:{keeper}")
            elif _own_page(con, sid, fid):
                act = "TARGET"
                ruling(a, "TARGET", str(a["count"]))
            else:
                con.execute("UPDATE stores SET place_id=?, feature_id=?, business_status='OPERATIONAL', "
                            "status_reason=NULL, target_reviews=MIN(MAX(?, COALESCE(reviews_scraped,0)), ?) "
                            "WHERE store_id=?", (fid, fid, a["count"] or 0, R13.CAP, sid))
        elif act == "LOCATION":
            ll = latlon_from_url(a["url"])
            con.execute("UPDATE stores SET source_lat=COALESCE(?, source_lat), source_lon=COALESCE(?, source_lon), "
                        "full_address=COALESCE(NULLIF(?, ''), full_address) WHERE store_id=?",
                        (ll[0] if ll else None, ll[1] if ll else None, a.get("address", ""), sid))
        elif act == "CATEGORY":
            con.execute("UPDATE stores SET category=? WHERE store_id=?", (a["category"], sid))
        counts[act] = counts.get(act, 0) + 1
    R13.step_store_rulings(con, rulings, ruling_date=ruling_date)
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("answers")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    present = pd.ExcelFile(args.answers).sheet_names
    frames = {s: pd.read_excel(args.answers, sheet_name=s) for s in SHEETS if s in present}
    plan = plan_actions(frames, include_skipped=True)
    skipped = [a for a in plan if a["action"] == "SKIP"]
    actions = [a for a in plan if a["action"] != "SKIP"]
    if args.apply:
        print("backup: " + backup(args.master, "r16"))
    con = connect_rw(args.master)
    con.row_factory = sqlite3.Row
    con.execute("BEGIN IMMEDIATE")
    R12.ensure_schema(con)
    counts = apply_actions(con, actions, RULING_DATE)
    R12.recompute(con)
    tolerated = R15.apply_tolerance(con, RULING_DATE)
    left = con.execute("SELECT COUNT(*), COALESCE(SUM(target_reviews - reviews_scraped),0) FROM stores "
                       "WHERE scrape_status='incomplete'").fetchone()
    print("actions:   " + str(counts))
    print(f"tolerance: {len(tolerated)} more stores ruled complete")
    print(f"left unruled ({len(skipped)}): " + "; ".join(f"{a['store_id']} {a['reason']}" for a in skipped))
    print(f"incomplete after: {left[0]} stores, {left[1]:,} reviews short")
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
    print("duplicates   -> " + str(R13.split_out_of_scope(args.master, EXCLUDED_DB, status="duplicate")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
