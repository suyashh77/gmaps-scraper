"""
s11_category_census.py - turn the probe results into a ruling worksheet.

Produces the thing the research group actually has to act on: every Google
category the probe found, how many stores carry it, what those stores look
like, and a blank `ruling` column to fill in.

Only VERIFIED pages count. A page we landed on but could not confirm is a
store we may not have found at all - the smoke test text-searched "Food Lion
Grocery Store of Reidsville" onto a Whole Foods 32 km away - and letting those
categories into the census would repeat the wrong-store defect that put 323
place_ids on the wrong stores in the first place.

    python Frame/s11_category_census.py                 # write the worksheet
    python Frame/s11_category_census.py --apply rules.csv   # apply filled rulings

Writes Frame/reports/google_category_census.csv (the worksheet) and, with
--apply, Frame/build/final_list.csv.
"""
import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
PROBE = HERE / "build" / "probe.db"
REPORTS = HERE / "reports"
BUILD = HERE / "build"


def load_probed():
    c = sqlite3.connect(PROBE)
    d = pd.read_sql("SELECT * FROM probe", c)
    c.close()
    return d


def census(args):
    d = load_probed()
    done = d[d.status == "done"]
    unver = d[d.status == "unverified"]
    print(f"probe.db: {len(d):,} stores, {len(done):,} verified, "
          f"{len(unver):,} unverified, "
          f"{int((d.status=='pending').sum()):,} still pending")
    if not len(done):
        print("nothing verified yet - let the probe run")
        return 1

    g = (done.groupby("g_category")
             .agg(stores=("store_id", "size"),
                  reviews=("g_total_reviews", "sum"),
                  median_reviews=("g_total_reviews", "median"),
                  closed=("g_closed", "sum"),
                  counties=("county", "nunique"))
             .sort_values("stores", ascending=False)
             .reset_index())
    # three real store names per category, so a ruling can be made on sight
    samples = (done.groupby("g_category")["g_name"]
                   .apply(lambda s: " | ".join(str(x) for x in s.dropna().head(3)))
                   .rename("examples"))
    g = g.merge(samples, on="g_category", how="left")
    # what our own frame had guessed, for comparison
    guess = (done.groupby("g_category")["food_access_type"]
                 .apply(lambda s: s.mode().iloc[0] if len(s.mode()) else "")
                 .rename("our_guess"))
    g = g.merge(guess, on="g_category", how="left")
    g["ruling"] = ""          # IN / OUT  <- the group fills this
    g["store_type"] = ""      # optional finer label
    g["note"] = ""

    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / "google_category_census.csv"
    g.to_csv(out, index=False)

    cum = g.stores.cumsum() / g.stores.sum()
    n80 = int((cum <= 0.80).sum()) + 1
    n95 = int((cum <= 0.95).sum()) + 1
    print(f"\n{len(g)} distinct Google categories over {int(g.stores.sum()):,} verified stores")
    print(f"  top {n80} categories cover 80% of stores")
    print(f"  top {n95} cover 95%")
    print(f"\nwrote {out}")
    print("\n  --- top 30 ---")
    for r in g.head(30).itertuples():
        print(f"  {r.stores:6,}  {str(r.g_category)[:34]:34s}  "
              f"(we guessed: {str(r.our_guess)[:16]:16s})  {str(r.examples)[:46]}")
    return 0


def apply_rules(args):
    rules = pd.read_csv(args.apply)
    if "ruling" not in rules or "g_category" not in rules:
        print("rules file needs g_category and ruling columns")
        return 1
    rules["ruling"] = rules.ruling.astype(str).str.strip().str.upper()
    unruled = rules[~rules.ruling.isin(["IN", "OUT"])]
    if len(unruled):
        print(f"REFUSING: {len(unruled)} categories have no IN/OUT ruling.")
        print("An unruled category is a hard error here, not a silent drop.")
        for r in unruled.head(15).itertuples():
            print(f"  {r.stores:6,}  {r.g_category}")
        return 2

    d = load_probed()
    done = d[d.status == "done"].copy()
    lut = dict(zip(rules.g_category, rules.ruling))
    types = dict(zip(rules.g_category, rules.get("store_type", pd.Series(dtype=str))))
    done["ruling"] = done.g_category.map(lut)
    done["store_type"] = done.g_category.map(types)
    keep = done[done.ruling == "IN"].copy()
    drop = done[done.ruling == "OUT"]

    BUILD.mkdir(parents=True, exist_ok=True)
    keep.to_csv(BUILD / "final_list.csv", index=False)
    drop.to_csv(REPORTS / "dropped_by_google_category.csv", index=False)
    print(f"verified stores {len(done):,}")
    print(f"  ruled IN  {len(keep):,}  -> {BUILD/'final_list.csv'}")
    print(f"  ruled OUT {len(drop):,}  -> {REPORTS/'dropped_by_google_category.csv'}")
    print(f"\n  counties {keep.county.nunique()}   cities {keep.city.nunique()}")
    print(f"  with place_id {int(keep.g_place_id.notna().sum()):,}")
    print(f"  total reviews available {int(keep.g_total_reviews.sum()):,}")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", metavar="RULES.CSV",
                   help="apply a filled-in ruling worksheet")
    a = p.parse_args()
    return apply_rules(a) if a.apply else census(a)


if __name__ == "__main__":
    sys.exit(main())
