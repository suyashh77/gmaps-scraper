"""R08 - Fill the metadata gaps in the recovered stores.

r04 puts back stores that were excluded by accident, using whatever the
exclusion audit recorded about them. That audit was built for a different
purpose, so a handful of rows arrive with a missing city or a coordinate that
was already wrong upstream. This closes those gaps using evidence that is
already in the database, and flags the ones that need a human rather than
guessing.

WHAT IT REPAIRS

  city from the ZIP
      The modal city among other master stores sharing the same ZIP. Every
      repaired store is flagged CITY_INFERRED so the assumption stays visible to
      anyone filtering the data later.

      The obvious alternative was to read the city out of the store name, since
      several of these read "Food Lion Grocery Store of Charlotte". That was
      tried and rejected: Food Lion names its stores after the shopping centre,
      so the same pattern yields "Greystone Village", "Riverwood" and "Little
      Rock Road", which are not cities. The ZIP is grounded in the coordinates of
      real stores; the name is grounded in a marketing convention.

  bad geocode
      One recovered store carries coordinates in Texas: the 7-Eleven listed at
      Denver, NC whose Overture row has always had them. The coordinate is not
      guessed at or deleted. The store is flagged BAD_GEOCODE so the geographic
      checks stop counting it as contamination and a person can re-resolve it.

USAGE
    python Remediation/r08_repair_recovered_metadata.py            # dry run
    python Remediation/r08_repair_recovered_metadata.py --apply
"""

from __future__ import annotations

import argparse
import collections
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_ro, connect_rw

NC_LAT = (33.70, 36.70)
NC_LON = (-84.45, -75.35)

NAME_CITY = re.compile(r"\bof\s+([A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*){0,2})\s*$")


def add_flag(existing: str | None, flag: str) -> str:
    flags = [f for f in (existing or "").split(",") if f]
    if flag not in flags:
        flags.append(flag)
    return ",".join(flags)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--master", default=MASTER)
    args = ap.parse_args()

    conn = connect_ro(args.master)

    zip_city: dict[str, collections.Counter] = {}
    for r in conn.execute(
        "SELECT zip, city FROM stores WHERE city IS NOT NULL AND TRIM(city) <> '' "
        "AND zip IS NOT NULL"
    ):
        z = (r["zip"] or "")[:5]
        if len(z) == 5:
            zip_city.setdefault(z, collections.Counter())[r["city"]] += 1

    city_fixes, flag_fixes = [], []

    for r in conn.execute(
        "SELECT store_id, store_name, zip, qa_flags FROM stores "
        "WHERE city IS NULL OR TRIM(city) = ''"
    ):
        city, how = None, None
        z = (r["zip"] or "")[:5]
        if z in zip_city and zip_city[z]:
            top, n = zip_city[z].most_common(1)[0]
            city = top
            how = "ZIP " + z + ", " + str(n) + " other stores agree"
        if city:
            city_fixes.append((r["store_id"], r["store_name"], city, how,
                               add_flag(r["qa_flags"], "CITY_INFERRED")))

    for r in conn.execute(
        "SELECT store_id, store_name, city, source_lat, source_lon, qa_flags FROM stores "
        "WHERE source_lat IS NOT NULL AND source_lon IS NOT NULL "
        "AND (source_lat NOT BETWEEN ? AND ? OR source_lon NOT BETWEEN ? AND ?)",
        (NC_LAT[0], NC_LAT[1], NC_LON[0], NC_LON[1]),
    ):
        flag_fixes.append((r["store_id"], r["store_name"], r["city"],
                           r["source_lat"], r["source_lon"],
                           add_flag(r["qa_flags"], "BAD_GEOCODE")))

    print("CITY BACKFILL")
    if not city_fixes:
        print("  nothing to do")
    for sid, name, city, how, _ in city_fixes:
        print("  " + str(sid).rjust(5) + "  " + str(name)[:38].ljust(38)
              + " -> " + city + "  (" + how + ")")

    print("\nBAD GEOCODE, flagged not altered")
    if not flag_fixes:
        print("  nothing to do")
    for sid, name, city, lat, lon, _ in flag_fixes:
        print("  " + str(sid).rjust(5) + "  " + str(name)[:30].ljust(30)
              + " " + str(city)[:14].ljust(14) + " " + str(lat) + "," + str(lon))

    conn.close()
    if not args.apply:
        print("\nDRY RUN. Re-run with --apply.")
        return 0
    if not city_fixes and not flag_fixes:
        return 0

    dest = backup(args.master, "r08")
    print("\nBackup written: " + os.path.basename(dest))

    w = connect_rw(args.master)
    cur = w.cursor()
    cur.execute("BEGIN IMMEDIATE")
    for sid, _, city, _, flags in city_fixes:
        cur.execute("UPDATE stores SET city=?, qa_flags=? WHERE store_id=?",
                    (city, flags or None, sid))
    for sid, _, _, _, _, flags in flag_fixes:
        cur.execute("UPDATE stores SET qa_flags=? WHERE store_id=?", (flags, sid))
    w.commit()
    print("  cities filled : " + str(len(city_fixes)))
    print("  geocodes flagged: " + str(len(flag_fixes)))
    w.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    w.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
