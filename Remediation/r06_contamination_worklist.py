"""R06 - Build the human adjudication queue for stores whose identity is suspect.

Three populations end up here. They overlap, and the CSV says which tests each
store failed, because a store failing two tests is far more likely to be wrong
than a store failing one.

  GEO_MISMATCH      The store's Overture coordinates sit more than 25 km from
                    the ZIP printed in its own Google address. These two facts
                    come from independent sources, so when they disagree one of
                    them belongs to a different store. 106 stores, 61 of them
                    more than 100 km apart.

                    Cause: the enrichment program's name check computed a
                    similarity score, logged it, and accepted the result on both
                    branches, so it took Google's first search result for all
                    16,680 queries. It cannot be found using the recorded
                    gmaps_lat/lon, because most of those values are the query
                    coordinate echoed back out of the URL.

  REVIEW_OVERLAP    The store shares twenty or more byte-identical reviews with
                    another store. One review page collected into several store
                    ids. A person can review two branches of a chain; they do not
                    write the same 219 reviews at five different shops.

  OVER_SCRAPED      Target of 50 or more and more than three times as many
                    reviews collected. The classic chain-poisoning signature.

WHY THIS IS A WORKLIST AND NOT A FIX
    Every remedy here destroys or moves real reviews, and which remedy is right
    depends on which half of the contradiction is wrong. If the address is wrong
    the reviews are fine and the metadata needs repair; if the place_id is wrong
    the reviews belong to a different store and must go. A script cannot tell
    those apart. Opening the place_id settles it in about fifteen seconds:

        https://www.google.com/maps/place/?q=place_id:<place_id>

    The CSV carries that link per row, ordered worst first.

USAGE
    python Remediation/r06_contamination_worklist.py
    python Remediation/r06_contamination_worklist.py --out queue.csv --min-km 25
"""

from __future__ import annotations

import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, connect_ro

import r00_validate as V


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--min-km", type=float, default=25.0)
    ap.add_argument("--min-shared", type=int, default=20)
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "contamination_worklist.csv"))
    args = ap.parse_args()

    conn = connect_ro(args.master)
    cur = conn.cursor()

    flags: dict[int, set] = {}
    detail: dict[int, dict] = {}

    for g in V.geo_inconsistent(conn, args.min_km):
        sid = g["store_id"]
        flags.setdefault(sid, set()).add("GEO_MISMATCH")
        detail.setdefault(sid, {})["km_off"] = g["km_off"]
        detail[sid]["address"] = g["address"]

    pairs, involved = V.cross_store_contamination(conn, args.min_shared)
    partners: dict[int, list] = {}
    for a, b, n in pairs:
        partners.setdefault(a, []).append(f"{b}({n})")
        partners.setdefault(b, []).append(f"{a}({n})")
    for sid in involved:
        flags.setdefault(sid, set()).add("REVIEW_OVERLAP")

    for r in cur.execute(
        "SELECT store_id, reviews_scraped, target_reviews FROM stores "
        "WHERE target_reviews >= 50 AND reviews_scraped > target_reviews * 3"
    ):
        flags.setdefault(r["store_id"], set()).add("OVER_SCRAPED")

    if not flags:
        print("No contaminated stores found. Nothing to adjudicate.")
        return 0

    rows = []
    for sid, fl in flags.items():
        s = cur.execute(
            "SELECT store_id, store_name, city, state, zip, full_address, category, "
            "place_id, reviews_scraped, target_reviews, source_lat, source_lon "
            "FROM stores WHERE store_id=?", (sid,)).fetchone()
        if s is None:
            continue
        d = detail.get(sid, {})
        rows.append({
            "store_id": sid,
            "n_flags": len(fl),
            "flags": ",".join(sorted(fl)),
            "km_off": d.get("km_off", ""),
            "shares_reviews_with": ";".join(partners.get(sid, [])),
            "store_name": s["store_name"],
            "source_city": s["city"],
            "google_address": s["full_address"],
            "category": s["category"],
            "reviews_scraped": s["reviews_scraped"],
            "target_reviews": s["target_reviews"],
            "place_id": s["place_id"],
            "verify_url": "https://www.google.com/maps/place/?q=place_id:" + str(s["place_id"]),
            "ruling": "",          # KEEP / REASSIGN / DELETE_REVIEWS / RESCRAPE
            "notes": "",
        })

    # Worst first: most tests failed, then furthest off, then most reviews at risk.
    rows.sort(key=lambda r: (-r["n_flags"],
                             -(float(r["km_off"]) if r["km_off"] != "" else 0),
                             -(r["reviews_scraped"] or 0)))

    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    at_risk = sum(r["reviews_scraped"] or 0 for r in rows)
    multi = [r for r in rows if r["n_flags"] > 1]
    print("CONTAMINATION WORKLIST")
    print("  stores to adjudicate      " + format(len(rows), ">7,"))
    print("  failing more than one test" + format(len(multi), ">7,")
          + "   <- start here, these are near certain")
    print("  reviews at risk           " + format(at_risk, ">7,"))
    print()
    for name in ("GEO_MISMATCH", "REVIEW_OVERLAP", "OVER_SCRAPED"):
        n = sum(1 for r in rows if name in r["flags"])
        print("  " + name.ljust(26) + format(n, ">7,"))
    print("\n  worst 15:")
    print("  " + "flags".ljust(34) + "km".rjust(7) + "revs".rjust(7) + "  store")
    for r in rows[:15]:
        km = str(r["km_off"]) if r["km_off"] != "" else "-"
        print("  " + r["flags"].ljust(34) + km.rjust(7)
              + str(r["reviews_scraped"] or 0).rjust(7) + "  "
              + str(r["store_name"])[:26] + " / " + str(r["source_city"])[:16])
    print("\nWrote " + args.out)
    print("Fill in the 'ruling' column, then feed it back through r07 to apply.")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
