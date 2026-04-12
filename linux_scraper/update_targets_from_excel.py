"""
Update store targets from list2_forrester_outscrpenrich.xlsx and mark stores
as complete or pending-rescrape.

Matching: place_id first, then name+address fuzzy fallback.
Target: reviews_outscraper column from the Excel.
Complete: reviews_scraped >= reviews_outscraper.
Rescrape: reviews_scraped < reviews_outscraper → reset to pending with attempts=0.
"""

import os
import re
import sqlite3

import openpyxl

EXCEL = "list2_forrester_outscrpenrich.xlsx"
DB = "list2_forrester_1000_reviews_a0ul1w.db"


def normalize(s: str) -> str:
    """Lowercase, strip, collapse whitespace, remove punctuation."""
    if not s:
        return ""
    s = re.sub(r"[^\w\s]", "", s.lower().strip())
    return re.sub(r"\s+", " ", s)


def load_excel(path: str) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        d = dict(zip(headers, row))
        rows.append(d)
    wb.close()
    return rows


def main():
    excel_rows = load_excel(EXCEL)
    print(f"Loaded {len(excel_rows)} rows from Excel")

    conn = sqlite3.connect(DB)
    conn.row_factory = lambda c, r: dict(zip([d[0] for d in c.description], r))
    cur = conn.cursor()

    cur.execute(
        "SELECT store_id, google_place_id, input_name, gmaps_name, "
        "gmaps_address, input_street, input_city, input_state, "
        "reviews_scraped, target_reviews, status, attempts FROM stores"
    )
    db_stores = cur.fetchall()
    print(f"Loaded {len(db_stores)} stores from DB")

    # Build lookup by place_id
    pid_lookup = {}
    for s in db_stores:
        pid = s["google_place_id"]
        if pid and pid.startswith("ChIJ"):
            pid_lookup[pid] = s

    # Build lookup by normalized name+address for fallback
    name_addr_lookup = {}
    for s in db_stores:
        name = normalize(s["gmaps_name"] or s["input_name"] or "")
        addr = normalize(s["gmaps_address"] or "")
        if not addr:
            parts = [s["input_street"] or "", s["input_city"] or "", s["input_state"] or ""]
            addr = normalize(" ".join(p for p in parts if p))
        if name:
            name_addr_lookup[(name, addr)] = s

    matched = 0
    marked_complete = 0
    marked_rescrape = 0
    already_complete = 0
    unmatched = []

    for ex in excel_rows:
        target = ex.get("reviews_outscraper")
        if target is None:
            continue
        try:
            target = int(target)
        except (TypeError, ValueError):
            continue

        ex_pid = ex.get("place_id") or ""
        ex_name = normalize(ex.get("name") or "")
        ex_addr = normalize(ex.get("address") or "")

        # Match by place_id first
        store = None
        if ex_pid and ex_pid.startswith("ChIJ"):
            store = pid_lookup.get(ex_pid)

        # Fallback: name + address
        if not store and ex_name:
            store = name_addr_lookup.get((ex_name, ex_addr))
            # Try partial name match if exact fails
            if not store:
                for (db_name, db_addr), s in name_addr_lookup.items():
                    if ex_name and db_name and (ex_name in db_name or db_name in ex_name):
                        # Also check address overlap
                        if ex_addr and db_addr:
                            ex_words = set(ex_addr.split())
                            db_words = set(db_addr.split())
                            overlap = len(ex_words & db_words)
                            if overlap >= 2:
                                store = s
                                break

        if not store:
            unmatched.append(
                f"  name={ex.get('name', '')[:40]}, addr={ex.get('address', '')[:50]}, "
                f"pid={ex_pid[:20]}, target={target}"
            )
            continue

        matched += 1
        sid = store["store_id"]
        scraped = store["reviews_scraped"] or 0

        # Update target_reviews
        cur.execute(
            "UPDATE stores SET target_reviews=?, updated_at=datetime('now') WHERE store_id=?",
            (target, sid),
        )

        if scraped >= target:
            # Already hit the target - mark complete
            if store["status"] == "completed":
                already_complete += 1
            else:
                cur.execute(
                    "UPDATE stores SET status='completed', error_message=NULL, updated_at=datetime('now') "
                    "WHERE store_id=?",
                    (sid,),
                )
                marked_complete += 1
        else:
            # Needs more reviews - reset for rescraping
            cur.execute(
                "UPDATE stores SET status='pending', worker_id=NULL, attempts=0, "
                "error_message=NULL, updated_at=datetime('now') WHERE store_id=?",
                (sid,),
            )
            marked_rescrape += 1

    conn.commit()

    # Final status summary
    cur.execute("SELECT status, COUNT(*) as cnt FROM stores GROUP BY status")
    final_status = {r["status"]: r["cnt"] for r in cur.fetchall()}
    cur.execute("SELECT COUNT(*) as cnt FROM reviews")
    total_reviews = cur.fetchone()["cnt"]

    conn.close()

    print(f"\n=== Results ===")
    print(f"Matched:              {matched}/{len(excel_rows)}")
    print(f"Already complete:     {already_complete}")
    print(f"Newly marked complete:{marked_complete}")
    print(f"Marked for rescrape:  {marked_rescrape}")
    print(f"Unmatched Excel rows: {len(unmatched)}")

    if unmatched:
        print(f"\n=== Unmatched rows (first 20) ===")
        for u in unmatched[:20]:
            print(u)

    print(f"\n=== Final DB Status ===")
    for status, cnt in sorted(final_status.items()):
        print(f"  {status}: {cnt}")
    print(f"  Total reviews: {total_reviews:,}")


if __name__ == "__main__":
    main()
