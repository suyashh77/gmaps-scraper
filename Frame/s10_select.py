"""
s10_select.py - cut the 36,820 frame to the probe list, recording a reason for
every single store that does not make it.

Scope decision (research group, 9 Sep 2026): the study wants established
bricks-and-mortar food stores. Not temporary stalls, not farmers' markets, not
roadside produce stands, not sole-proprietor operations. That reverses the
earlier "widen and tag" call and it changes what "correct" means: the Census
employer-establishment count stops being a floor we are allowed to exceed and
becomes the number we are trying to land on, because CBP counts exactly the
population the group just described - businesses with payroll.

Every store gets a `disposition` and, if dropped, a `drop_reason`. Nothing is
deleted silently; the dropped rows are written out in full so any of these
decisions can be argued with or reversed.

Then the survivors are deduplicated with the project's own geomatch library
(the fixed one - see the note in geomatch/parser.py about store numbers being
read as house numbers).

Writes Frame/build/probe_list.csv and Frame/reports/dropped_stores.csv.
"""
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

import pandas as pd
import geomatch

HERE = Path(__file__).parent
DB = HERE.parent / "Data" / "Step_1_NC_Food_Retail_Frame.db"
BUILD = HERE / "build"
REPORTS = HERE / "reports"

# Store formats that are an established shop somebody works at.
STORE_TYPES = {
    "supermarket", "grocery", "convenience", "dollar", "supercenter",
    "warehouse_club", "gas_convenience",
    "ethnic_grocery", "health_food", "specialty", "butcher", "seafood",
}
# Formats the group has ruled out of scope: transient, seasonal, or not retail.
OUT_OF_SCOPE = {
    "farmers_market": "Temporary or periodic market, not an established store",
    "produce": "Roadside produce stand, not an established store",
    "farm_direct": "On-farm sales, not an established store",
    "food_assistance": "Food bank, not retail",
    "prepared": "Prepared food service, not grocery retail",
    "unknown": "No source could say what kind of store this is",
}

DROP_ORDER = [
    ("OUT_OF_SCOPE_FORMAT", "store format is out of scope for this study"),
    ("UNCORROBORATED", "only one source says this store exists, and it is not on the SNAP register"),
    ("GEOMATCH_DUPLICATE", "geomatch matched it to another store already on the list"),
]


def main():
    s = pd.read_sql("select * from stores", sqlite3.connect(DB))
    for c in ["in_snap", "in_overture", "in_osm", "in_master",
              "in_excluded", "in_overture_2601"]:
        s[c] = s[c].astype(bool)
    n0 = len(s)
    print(f"frame in: {n0:,} stores")

    s["disposition"] = "KEEP"
    s["drop_reason"] = None
    s["drop_detail"] = None

    # ---- filter 1: out-of-scope store formats -----------------------------
    m = s.food_access_type.isin(OUT_OF_SCOPE) & (s.disposition == "KEEP")
    s.loc[m, "disposition"] = "DROP"
    s.loc[m, "drop_reason"] = "OUT_OF_SCOPE_FORMAT"
    s.loc[m, "drop_detail"] = s.loc[m, "food_access_type"].map(OUT_OF_SCOPE)
    print(f"  drop OUT_OF_SCOPE_FORMAT : {int(m.sum()):6,}")

    # anything not in either set has no ruling we trust
    m = (~s.food_access_type.isin(STORE_TYPES)) & (s.disposition == "KEEP")
    s.loc[m, "disposition"] = "DROP"
    s.loc[m, "drop_reason"] = "OUT_OF_SCOPE_FORMAT"
    s.loc[m, "drop_detail"] = "Store format not on the in-scope list"
    print(f"  drop (format not listed) : {int(m.sum()):6,}")

    # ---- filter 2: uncorroborated ----------------------------------------
    # One source asserting a store exists is not evidence, unless that source
    # is the federal SNAP register, where a person verified the address.
    corroborated = (s.n_sources >= 2) | s.in_snap
    m = (~corroborated) & (s.disposition == "KEEP")
    s.loc[m, "disposition"] = "DROP"
    s.loc[m, "drop_reason"] = "UNCORROBORATED"
    s.loc[m, "drop_detail"] = (
        "Seen by exactly one source (" + s.loc[m, "ruling_vocab"].fillna("?")
        + ") and not SNAP-authorised")
    print(f"  drop UNCORROBORATED      : {int(m.sum()):6,}")

    # ---- filter 3: geomatch dedupe of the survivors -----------------------
    keep = s[s.disposition == "KEEP"].copy()
    keep["addr"] = (keep.street.fillna("") + ", " + keep.city.fillna("")
                    + ", NC " + keep.zip.fillna("").astype(str)).str.strip(", ")
    # County is the blocking key and some rows have none. Bucket those together
    # rather than dropping them, so a store with no county still gets compared.
    keep["_blk"] = keep.county.fillna("__NO_COUNTY__")
    print(f"\n  deduplicating {len(keep):,} survivors with geomatch, by county...")

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    t0, dupes, npairs = time.time(), [], 0
    counties = list(keep.groupby("_blk", dropna=False))
    for i, (cty, g) in enumerate(counties, 1):
        if len(g) < 2:
            continue
        r = geomatch.dedupe(g, address="addr", name="name",
                            lat="lat", lon="lon", id="store_id")
        npairs += len(r)
        mm = r[r.decision == "match"]
        for x in mm.itertuples():
            a, b = find(x.id_left), find(x.id_right)
            if a != b:
                parent[b] = a
            dupes.append({"county": cty, "score": x.score,
                          "kept_id": a, "dropped_id": b,
                          "why": getattr(x, "why", "")})
        if i % 20 == 0 or i == len(counties):
            print(f"    [{i:3d}/{len(counties)}] {npairs:,} pairs, "
                  f"{len(dupes)} matches, {time.time()-t0:.0f}s", flush=True)

    comp = Counter(find(i) for i in keep.store_id)
    losers = {sid for sid in keep.store_id if find(sid) != sid}
    m = s.store_id.isin(losers) & (s.disposition == "KEEP")
    s.loc[m, "disposition"] = "DROP"
    s.loc[m, "drop_reason"] = "GEOMATCH_DUPLICATE"
    s.loc[m, "drop_detail"] = "geomatch judged this the same physical store as another kept row"
    print(f"  drop GEOMATCH_DUPLICATE  : {int(m.sum()):6,}   "
          f"({npairs:,} pairs evaluated, {len(dupes)} matched)")

    # ---- output -----------------------------------------------------------
    BUILD.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    probe = s[s.disposition == "KEEP"].copy()
    probe["probe_status"] = "pending"
    probe.to_csv(BUILD / "probe_list.csv", index=False)
    dropped = s[s.disposition == "DROP"]
    dropped.to_csv(REPORTS / "dropped_stores.csv", index=False)
    if dupes:
        pd.DataFrame(dupes).to_csv(REPORTS / "geomatch_duplicates.csv", index=False)

    print(f"\n=== result ===")
    print(f"  in                {n0:7,}")
    for reason, _ in DROP_ORDER:
        n = int((s.drop_reason == reason).sum())
        if n:
            print(f"  -{reason:24s} {n:7,}")
    print(f"  PROBE LIST        {len(probe):7,}")
    print(f"\n  counties {probe.county.nunique()}   cities {probe.city.nunique()}   "
          f"with place_id {int(probe.place_id.notna().sum()):,}")
    print("\n  probe list by store type:")
    for k, v in probe.food_access_type.value_counts().items():
        print(f"    {v:6,}  {k}")
    print(f"\n  wrote {BUILD/'probe_list.csv'}")
    print(f"  wrote {REPORTS/'dropped_stores.csv'}  ({len(dropped):,} rows, every one with a reason)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
