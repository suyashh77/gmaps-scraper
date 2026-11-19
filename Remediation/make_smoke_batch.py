"""Build a small batch database for smoke-testing the scraper.

Picks a deliberate mix rather than the first N rows, because the point is to
exercise the code paths that were wrong:

  * a large chain with many nearby branches, which is what the restored
    place_id verification and the 3 km chain distance limit exist to get right
  * an independent store, where the name is distinguishing and the looser 10 km
    limit applies
  * a store with a modest target so the run finishes quickly
  * a store already holding reviews, to exercise the resume and skip-list path

Usage:
    python Remediation/make_smoke_batch.py --out Data/batches/smoke.db --n 5
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Scraper_v2"))

from lib_master import MASTER, connect_ro                       # noqa: E402
from linux_scraper.database import make_db                      # noqa: E402

OPEN = ("place_id LIKE 'ChIJ%' AND business_status IS NOT 'CLOSED_PERMANENTLY' "
        "AND COALESCE(qa_flags,'') NOT LIKE '%NEEDS_ADJUDICATION%'")

PICKS = [
    # label, SQL predicate, order
    ("chain: Dollar General",
     OPEN + " AND store_name LIKE 'Dollar General%' AND reviews_scraped < target_reviews",
     "target_reviews - reviews_scraped ASC"),
    ("chain: Food Lion",
     OPEN + " AND store_name LIKE 'Food Lion%' AND reviews_scraped < target_reviews",
     "target_reviews - reviews_scraped ASC"),
    ("chain: Costco, newly recovered",
     OPEN + " AND store_name LIKE '%Costco%'",
     "store_id ASC"),
    ("independent grocery",
     OPEN + " AND category = 'Grocery store' AND store_name NOT LIKE 'Food Lion%' "
     "AND store_name NOT LIKE 'Dollar%' AND store_name NOT LIKE 'Walmart%' "
     "AND reviews_scraped < target_reviews",
     "target_reviews - reviews_scraped ASC"),
    ("partially scraped, resume path",
     OPEN + " AND reviews_scraped BETWEEN 5 AND 60 AND reviews_scraped < target_reviews",
     "reviews_scraped DESC"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data", "batches", "smoke.db"))
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--cap", type=int, default=40,
                    help="cap target_reviews so the smoke test finishes quickly")
    args = ap.parse_args()

    if os.path.exists(args.out):
        os.remove(args.out)
    for side in ("-wal", "-shm"):
        if os.path.exists(args.out + side):
            os.remove(args.out + side)

    m = connect_ro(args.master)
    chosen, seen = [], set()
    for label, pred, order in PICKS:
        row = m.execute(
            "SELECT store_id, place_id, store_name, street, city, state, zip, "
            "source_lat, source_lon, full_address, target_reviews, reviews_scraped, "
            "category FROM stores WHERE " + pred + " ORDER BY " + order + " LIMIT 5"
        ).fetchall()
        for r in row:
            if r["place_id"] in seen:
                continue
            seen.add(r["place_id"])
            chosen.append((label, r))
            break
    m.close()

    if not chosen:
        print("Could not select any stores.")
        return 1

    db = make_db(args.out)
    con = db.connect()
    db.init_schema()
    cur = con.cursor()
    for i, (label, r) in enumerate(chosen, start=1):
        target = r["target_reviews"] or 0
        capped = min(target, args.cap) if target else args.cap
        cur.execute(
            "INSERT INTO stores (store_id, machine_id, input_name, input_street, "
            "input_city, input_state, input_zip, input_lat, input_lon, "
            "google_place_id, gmaps_name, gmaps_address, status, target_reviews, "
            "master_reviews, reviews_scraped, attempts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,0,0,0)",
            (i, "smoke", r["store_name"], r["street"], r["city"], r["state"], r["zip"],
             r["source_lat"], r["source_lon"], r["place_id"], r["store_name"],
             r["full_address"], capped))
    cur.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)")
    cur.execute("INSERT OR REPLACE INTO metadata (key,value) VALUES ('machine_id','smoke')")
    con.commit()
    db.close()

    print("Smoke batch: " + args.out)
    print("  " + "role".ljust(30) + "target  store")
    for i, (label, r) in enumerate(chosen, start=1):
        target = min(r["target_reviews"] or args.cap, args.cap)
        print("  " + label.ljust(30) + str(target).rjust(6) + "  "
              + str(r["store_name"])[:30] + " / " + str(r["city"])[:16])
    print("\n  master place_ids are carried through, so verification can use them.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
