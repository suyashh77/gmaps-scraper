"""Build the Round 4 manual-check workbook (stores whose identity or location master cannot settle).

Sheets: place_id_check (malformed place_id; includes stores holding another store's reviews),
location_check (coordinates outside NC), category_check (no category), no_target_check
(reviews held but no target; same shape r14_manual_answers reads).

USAGE
    python Remediation/make_manual_round4.py [--out Data/exports/Manual_Check_Round4_2026-10-09.xlsx]
"""

from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import sys
from urllib.parse import quote_plus

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, connect_ro                                  # noqa: E402
from r00_validate import NC_LAT, NC_LON, cross_store_contamination          # noqa: E402

DEFAULT_OUT = os.path.join(os.path.dirname(MASTER), "exports", "Manual_Check_Round4_2026-10-09.xlsx")
YELLOW = PatternFill("solid", fgColor="FFF2CC")
EXISTS = ["OPEN_OK", "CLOSED_PERMANENT", "CLOSED_TEMPORARY", "NOT_ON_GOOGLE", "WRONG_PAGE", "UNSURE"]
FACTS = ["master_store_id", "store_name", "address", "city", "category", "reviews_we_hold", "target"]
SHEETS = {   # name -> (extra fact columns, answer columns, {answer column: dropdown})
    "place_id_check": (["shares_reviews_with", "maps_link", "search_link"],
                       ["outcome", "google_review_count", "correct_google_url", "notes"],
                       {"outcome": EXISTS}),
    "location_check": (["lat_lon_we_hold", "maps_link", "search_link"],
                       ["outcome", "real_address", "correct_google_url", "google_review_count", "notes"],
                       {"outcome": ["IN_NC", "NOT_IN_NC", "CLOSED_PERMANENT", "CLOSED_TEMPORARY",
                                    "NOT_ON_GOOGLE", "UNSURE"]}),
    "category_check": (["maps_link", "search_link"],
                       ["outcome", "google_category", "google_review_count", "notes"],
                       {"outcome": EXISTS}),
    "no_target_check": (["maps_link", "search_link"],
                        ["outcome", "google_review_count", "google_rating", "notes"],
                        {"outcome": EXISTS}),
    "target_check": (["short_by", "scraper_last_result", "maps_link", "search_link"],
                     ["outcome", "google_review_count", "correct_google_url", "notes"],
                     {"outcome": EXISTS}),
}
INSTRUCTIONS = """ROUND 4 MANUAL CHECK - READ THIS SHEET FIRST
WHY THIS EXISTS
Our scraper finds a store by its saved Google link (place_id). For some stores that link is damaged, or the store's coordinates are outside North Carolina, or Google's category is missing. Master cannot settle these from its own data; you can, on Google Maps. You are not copying reviews. You are recording facts about Google pages.
WHAT YOU NEED
A normal browser. Do NOT sign in to Google. About 3 hours in total (see the row counts on each sheet).
GENERAL RULES
1. Fill ONLY the yellow columns. Do not edit, sort, delete or merge anything else. One row per store.
2. Never guess. If you cannot tell, choose UNSURE and explain in 'notes'.
3. Numbers: digits only (1,234 -> 1234). If Google shows '1.2K', open the Reviews tab for the exact figure.
4. Links: copy the FULL address-bar URL (starts with https://www.google.com/maps/place/). No shortened maps.app.goo.gl links; do not edit the URL.
5. Use the drop-down values. 'Right store' means the name AND the address match what we hold.
SHEET place_id_check - what to do
The saved link is damaged, so maps_link is blank; use search_link (name + city). Where 'shares_reviews_with' names another store, our reviews for this row were copied from that other store's page, so also make sure the page you pick is a DIFFERENT page from that store's.
   outcome: OPEN_OK = right store found, no closed notice | CLOSED_PERMANENT | CLOSED_TEMPORARY | NOT_ON_GOOGLE | WRONG_PAGE = the only match is another store's page | UNSURE
   google_review_count: the number next to the stars.
   correct_google_url: the right store's full URL (rule 4). This puts the store back in the scraping queue.
SHEET location_check - what to do
The coordinates we hold are outside NC (often another state or country). Open search_link and find the store of this name in this city.
   outcome: IN_NC = the store exists in NC | NOT_IN_NC = the only match is elsewhere | CLOSED_PERMANENT | CLOSED_TEMPORARY | NOT_ON_GOOGLE | UNSURE
   real_address: the street address Google shows. correct_google_url, google_review_count as above.
SHEET category_check - what to do
Open maps_link (or search_link). outcome as in place_id_check. google_category = the grey label under the stars (e.g. 'Grocery store'); leave blank if none.
SHEET no_target_check - what to do
We hold reviews but no target. Open maps_link. outcome as above; google_review_count = the number next to the stars (0 if none); google_rating e.g. 4.3.
SHEET target_check - what to do
We hold fewer reviews than our target and want to know what the target should really be (Google reviews get deleted, so our target may be stale). Open maps_link. outcome as in place_id_check; google_review_count = the number next to the stars NOW, digits only. correct_google_url only if the saved link opens a different business. 'scraper_last_result' is the scraper's last message, for information only.
WHAT WE DO WITH YOUR ANSWERS
target_check: your count becomes the target. We then accept the store as complete when we are within 20 reviews (target over 1,000) or within 10 (target over 100; under 10 below that); otherwise it goes back in the scraping queue.
A correct_google_url re-points the store and queues it; CLOSED_* and NOT_IN_NC/NOT_ON_GOOGLE remove it from the dataset (kept in a side database, never deleted); IN_NC fixes the coordinates; categories and counts are filled in.
WHEN YOU ARE DONE
Save under a new name ending _done.xlsx and send it back. Do not rename sheets or columns."""


def _links(row) -> tuple[str, str]:
    pid = row["place_id"] or ""
    maps = f"https://www.google.com/maps/place/?q=place_id:{pid}" if pid.startswith("ChIJ") else ""
    q = " ".join(x for x in (row["store_name"], row["city"], "NC") if x)
    return maps, "https://www.google.com/maps/search/" + quote_plus(q)


def last_scraper_results(returns_dir: str) -> dict:
    """master_store_id -> the scraper's latest status and message across the returned batch DBs."""
    latest: dict = {}
    for path in glob.glob(os.path.join(returns_dir, "*", "*.db")):
        if "pre_timeout" in path:
            continue
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        if "master_store_id" in [c[1] for c in con.execute("PRAGMA table_info(stores)")]:
            for sid, status, err, at in con.execute(
                    "SELECT master_store_id, status, error_message, updated_at FROM stores"):
                if sid is not None and (sid not in latest or (at or "") >= latest[sid][0]):
                    latest[sid] = (at or "", f"{status}: {err}" if err else status)
        con.close()
    return {sid: msg for sid, (_, msg) in latest.items()}


def _address(row) -> str:
    """The address we meant (street, city, state zip) beside the one Google gave, when they disagree.

    full_address came from the Google page the scraper landed on. When that page is in another city
    than the store we meant (a wrong-page link), both are shown so the reader is not misled."""
    keys = row.keys()
    street, zip_ = (row["street"] if "street" in keys else None), (row["zip"] if "zip" in keys else None)
    tail = " ".join(x for x in (row["state"], zip_) if x)
    meant = ", ".join(x for x in (street, row["city"], tail) if x)
    google = row["full_address"]
    if not google:
        return meant
    if street and row["city"] and row["city"].lower() not in google.lower():
        return f"{meant} | Google page says: {google}"
    return google


def _base(row, partners, results=None) -> dict:
    maps, search = _links(row)
    target = row["target_reviews"] or 0
    return {"master_store_id": row["store_id"], "store_name": row["store_name"],
            "address": _address(row), "city": row["city"], "category": row["category"],
            "reviews_we_hold": row["reviews_scraped"], "target": row["target_reviews"] or "none",
            "shares_reviews_with": partners.get(row["store_id"], ""),
            "lat_lon_we_hold": f"{row['source_lat']}, {row['source_lon']}",
            "short_by": target - (row["reviews_scraped"] or 0),
            "scraper_last_result": (results or {}).get(row["store_id"], ""),
            "maps_link": maps, "search_link": search}


def _selections() -> dict:
    out_of_nc = (f"source_lat NOT BETWEEN {NC_LAT[0]} AND {NC_LAT[1]} "
                 f"OR source_lon NOT BETWEEN {NC_LON[0]} AND {NC_LON[1]}")
    return {
        "place_id_check": "place_id NOT LIKE 'ChIJ%' AND place_id NOT LIKE '0x%:0x%'",
        "location_check": out_of_nc,
        "category_check": "category IS NULL OR TRIM(category) = ''",
        "no_target_check": "(target_reviews IS NULL OR target_reviews = 0) AND reviews_scraped > 0",
        # malformed place_ids are already on place_id_check
        "target_check": "scrape_status = 'incomplete' AND (place_id LIKE 'ChIJ%' OR place_id LIKE '0x%:0x%')",
    }


def _write(ws, rows, extra, answers, dropdowns) -> None:
    head = FACTS + extra + answers
    ws.append(head)
    for r in rows:
        ws.append([r[h] for h in FACTS + extra] + [None] * len(answers))
    last = max(ws.max_row, 2)
    for i, name in enumerate(head, 1):
        col = get_column_letter(i)
        ws[f"{col}1"].font = Font(bold=True)
        ws.column_dimensions[col].width = 44 if "link" in name or name == "notes" else 18
        if name not in answers:
            continue
        for row in range(2, last + 1):
            ws[f"{col}{row}"].fill = YELLOW
        if name in dropdowns:
            dv = DataValidation(type="list", formula1='"' + ",".join(dropdowns[name]) + '"')
            ws.add_data_validation(dv)
            dv.add(f"{col}2:{col}{last}")
    ws.freeze_panes = "C2"


def build(master: str, out: str) -> dict:
    con = connect_ro(master)
    con.row_factory = sqlite3.Row
    pairs, _ = cross_store_contamination(con)
    partners: dict = {}
    for a, b, n in pairs:
        partners[a] = f"store {b} ({n} identical reviews)"
        partners[b] = f"store {a} ({n} identical reviews)"
    wb = Workbook()
    ws = wb.active
    ws.title = "READ FIRST"
    for line in INSTRUCTIONS.split("\n"):
        ws.append([line])
    ws.column_dimensions["A"].width = 140
    for row in ws.iter_rows():
        row[0].alignment = Alignment(wrap_text=True, vertical="top")
    results = last_scraper_results(os.path.join(os.path.dirname(master), "returns"))
    counts = {}
    for name, where in _selections().items():
        rows = [_base(r, partners, results) for r in con.execute(
            f"SELECT * FROM stores WHERE {where} ORDER BY store_id")]
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
    a = ap.parse_args()
    print(build(a.master, a.out), "->", a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
