"""Build the Round 5 manual-check workbook: stores with no review target, plus named count checks.

Sheets: no_target_check (every store with no target), target_check (only for --count-check ids).
Same columns as Round 4, so r16_round4_answers.py reads the answers back unchanged.

USAGE
    python Remediation/make_manual_round5.py [--count-check 3249] [--out Data/exports/Manual_Check_Round5_2026-10-11.xlsx]
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

from openpyxl import Workbook
from openpyxl.styles import Alignment

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, connect_ro                                            # noqa: E402
from make_manual_round4 import SHEETS, _base, _write, last_scraper_results           # noqa: E402

DEFAULT_OUT = os.path.join(os.path.dirname(MASTER), "exports", "Manual_Check_Round5_2026-10-11.xlsx")
INSTRUCTIONS = """ROUND 5 MANUAL CHECK - READ THIS SHEET FIRST
WHY THIS EXISTS
We cannot compute a completion target for these stores, because we do not know how many reviews Google shows for them. You can, on Google Maps. You are not copying reviews. You are recording facts about Google pages.
WHAT YOU NEED
A normal browser. Do NOT sign in to Google.
GENERAL RULES
1. Fill ONLY the yellow columns. Do not edit, sort, delete or merge anything else. One row per store.
2. Never guess. If you cannot tell, choose UNSURE and explain in 'notes'.
3. Numbers: digits only (1,234 -> 1234). If Google shows '1.2K', open the Reviews tab for the exact figure. 0 if Google shows no reviews.
4. Links: copy the FULL address-bar URL (starts with https://www.google.com/maps/place/). No shortened maps.app.goo.gl links; do not edit the URL.
5. Use the drop-down values. 'Right store' means the name AND the address match what we hold.
SHEET no_target_check - what to do
Open maps_link (or search_link). Record:
   outcome: OPEN_OK = right store, no closed notice | CLOSED_PERMANENT | CLOSED_TEMPORARY | NOT_ON_GOOGLE | WRONG_PAGE = the link opens a different business | UNSURE
   google_review_count: the number next to the stars NOW (0 if none).   google_rating: e.g. 4.3.
   notes: anything unusual.
SHEET target_check (if present) - what to do
We want to verify this store's target: either we hold FEWER reviews than Google's count, or MORE (so part of what we hold may belong to another store). Open maps_link. Record outcome and google_review_count (the number next to the stars NOW); also put the street address Google shows in 'notes' so we can compare it with 'address'. correct_google_url only if the saved link opens a different business.
WHAT WE DO WITH YOUR ANSWERS
no_target_check: your count becomes the store's target (0 reviews: complete, 'no data'). CLOSED_*: removed from the dataset. NOT_ON_GOOGLE: out of scope. target_check: your count becomes the target (never below what we hold); the completion rules then decide whether the store is complete or rescraped.
WHEN YOU ARE DONE
Save under a new name ending _done.xlsx and send it back. Do not rename sheets or columns."""


def build(master: str, out: str, count_check_ids=()) -> dict:
    con = connect_ro(master)
    con.row_factory = sqlite3.Row
    results = last_scraper_results(os.path.join(os.path.dirname(master), "returns"))
    selections = {"no_target_check": ("(target_reviews IS NULL OR target_reviews = 0) "
                                       "AND COALESCE(status_reason, '') NOT LIKE 'no data%'", ())}
    if count_check_ids:
        marks = ",".join("?" * len(count_check_ids))
        selections["target_check"] = (f"store_id IN ({marks})", tuple(count_check_ids))
    wb = Workbook()
    ws = wb.active
    ws.title = "READ FIRST"
    for line in INSTRUCTIONS.split("\n"):
        ws.append([line])
    ws.column_dimensions["A"].width = 140
    for row in ws.iter_rows():
        row[0].alignment = Alignment(wrap_text=True, vertical="top")
    counts = {}
    for name, (where, params) in selections.items():
        rows = [_base(r, {}, results) for r in con.execute(
            f"SELECT * FROM stores WHERE {where} ORDER BY store_id", params)]
        extra, answers, dropdowns = SHEETS[name]
        _write(wb.create_sheet(name), rows, extra, answers, dropdowns)
        counts[name] = len(rows)
    con.close()
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    wb.save(out)
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--count-check", type=int, nargs="*", default=[], metavar="STORE_ID")
    a = ap.parse_args()
    print(build(a.master, a.out, a.count_check), "->", a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
