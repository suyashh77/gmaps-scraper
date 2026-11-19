"""R05 - Identify and export the stores that left the pipeline without a reason.

THE FINDING
    The North Carolina candidate set holds 16,680 stores. Master holds 4,889.
    The exclusion audit explains 7,497. That accounts for 12,386. The remaining
    4,294 are in neither place: no reason code, no audit row, no mention in any
    document under Data/.

    They divide into two populations with different remedies:

      NEVER_ENRICHED    ~2,314 rows that never reached the Google lookup at all.
                        Step 1.5 reports 7,858 matched and 6,607 unmatched,
                        which sums to 14,465 against an input of 16,680. The
                        2,215-row difference is presented in
                        "Data/README.md" (Part A) as a deliberate step,
                        "Match + dedup unmatched cohort (-2,215)". It is not a
                        dedup. Only 113 of these share a name and street with a
                        retained row, and 1,375 cannot be explained as a
                        duplicate by name+street OR name+city.

      ENRICHED_THEN_LOST ~1,988 rows that WERE looked up at Step 1.5 and then
                        appear in neither master nor the exclusion audit. Their
                        disappearance is in the Step 1.75 reconciliation.

    This is not a rounding error. Harris Teeter alone loses 133 of 185 upstream
    rows to this path, 61 of them at unambiguously distinct street addresses
    with no duplicate anywhere in the data.

HOW ROWS ARE MATCHED BACK
    Overture source coordinates propagate unchanged into stores.source_lat /
    source_lon, so rounding both to five decimal places gives an exact join key.
    4,888 of the 4,889 master rows key back to Step 1.3 through it, which is
    what makes the residual trustworthy rather than an artifact of a fuzzy join.

OUTPUT
    A CSV of the lost rows, ranked so the enrichment re-run does the most
    valuable work first: grocery-relevant Overture category, then a named chain,
    then rows in the ZIP prefixes with the worst retention. Column names match
    what Step_1.4_EnrichOverture__Program expects for its Excel import.

USAGE
    python Remediation/r05_lost_store_worklist.py
    python Remediation/r05_lost_store_worklist.py --out Data/Step_1.10_Lost_Stores.csv
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import DATA, MASTER, connect_ro

CANDIDATES = os.path.join(DATA, "Step_1.3_POI_Overture_NC.xlsx")
EXCLUSIONS = os.path.join(DATA, "Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx")

# Overture categories that plainly belong to a food-retail study. Used only to
# RANK the re-enrichment queue, never to drop a row: the include/exclude ruling
# happens later against Google's category, exactly as it does today.
GROCERY_ISH = {
    "grocery_store", "supermarket", "convenience_store", "organic_grocery_store",
    "discount_store", "dollar_store", "warehouse_store", "wholesale_store",
    "superstore", "department_store", "general_store", "health_food_store",
    "asian_grocery_store", "mexican_grocery_store", "indian_grocery_store",
    "international_grocery_store", "food_and_beverage_store", "farmers_market",
    "produce_market", "butcher_shop", "fishmonger", "fruits_and_vegetables",
}

CHAINS = ["food lion", "walmart", "dollar general", "family dollar", "dollar tree",
          "harris teeter", "ingles", "aldi", "lidl", "publix", "lowes foods",
          "food city", "piggly wiggly", "costco", "sam's club", "sams club",
          "bj's", "target", "whole foods", "trader joe", "earth fare", "wegmans",
          "compare foods", "carlie c", "iga", "sheetz", "circle k", "7-eleven"]

# ZIP prefixes with the worst store retention. Recovering here corrects the
# geographic bias rather than merely raising the row count.
PRIORITY_ZIP3 = {"276": 14.4, "279": 17.5, "278": 20.6, "275": 22.6}


def key_of(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return None
    return (round(float(lat), 5), round(float(lon), 5))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "lost_stores_worklist.csv"))
    ap.add_argument("--master", default=MASTER)
    args = ap.parse_args()

    for path in (CANDIDATES, EXCLUSIONS):
        if not os.path.exists(path):
            print("Missing " + path)
            return 1

    cand = pd.read_excel(CANDIDATES)
    cand["key"] = [key_of(a, b) for a, b in zip(cand["latitude"], cand["longitude"])]

    conn = connect_ro(args.master)
    master_keys = {key_of(r[0], r[1]) for r in
                   conn.execute("SELECT source_lat, source_lon FROM stores")}
    master_keys.discard(None)
    conn.close()

    ex = pd.read_excel(EXCLUSIONS)
    ex_keys = {key_of(a, b) for a, b in zip(ex["source_lat"], ex["source_lon"])}
    ex_keys.discard(None)

    cand["in_master"] = cand["key"].isin(master_keys)
    cand["in_exclusions"] = cand["key"].isin(ex_keys)
    lost = cand[~cand["in_master"] & ~cand["in_exclusions"]].copy()

    print("RECONCILIATION OF THE NC CANDIDATE SET")
    print("  candidates (Step 1.3)      " + format(len(cand), ">8,"))
    print("  reached master             " + format(int(cand["in_master"].sum()), ">8,"))
    print("  explained by the audit     " + format(int(cand["in_exclusions"].sum()), ">8,"))
    print("  BOTH (counted twice)       "
          + format(int((cand["in_master"] & cand["in_exclusions"]).sum()), ">8,"))
    print("  NEITHER, no reason on file " + format(len(lost), ">8,"))

    # Could a lost row simply be a duplicate of a row we kept?
    def norm(s):
        return "".join(ch for ch in str(s or "").lower() if ch.isalnum())
    kept = cand[cand["in_master"] | cand["in_exclusions"]]
    kept_ns = {(norm(n), norm(s)) for n, s in zip(kept["name"], kept["street"])}
    kept_nc = {(norm(n), norm(c)) for n, c in zip(kept["name"], kept["city"])}
    lost["dup_name_street"] = [(norm(n), norm(s)) in kept_ns
                               for n, s in zip(lost["name"], lost["street"])]
    lost["dup_name_city"] = [(norm(n), norm(c)) in kept_nc
                             for n, c in zip(lost["name"], lost["city"])]
    hard = lost[~lost["dup_name_street"] & ~lost["dup_name_city"]]
    print("    of which share name+street with a kept row  "
          + format(int(lost["dup_name_street"].sum()), ">6,"))
    print("    of which share name+city with a kept row    "
          + format(int(lost["dup_name_city"].sum()), ">6,"))
    print("    UNEXPLAINABLE AS A DUPLICATE (the floor)    " + format(len(hard), ">6,"))

    # Rank the queue.
    def cats(row):
        return (str(row["primary_category"] or "") + " "
                + str(row["alternate_categories"] or "")).lower()
    lost["grocery_relevant"] = [any(g in cats(r) for g in GROCERY_ISH)
                                for _, r in lost.iterrows()]
    lost["chain"] = [next((c for c in CHAINS if c in str(n or "").lower()), "")
                     for n in lost["name"]]
    lost["zip3"] = lost["zipcode"].astype(str).str[:3]
    lost["priority_region"] = lost["zip3"].isin(PRIORITY_ZIP3)
    lost["hard_loss"] = ~lost["dup_name_street"] & ~lost["dup_name_city"]
    lost["priority"] = (lost["grocery_relevant"].astype(int) * 4
                        + (lost["chain"] != "").astype(int) * 2
                        + lost["priority_region"].astype(int) * 2
                        + lost["hard_loss"].astype(int))

    print("\n  grocery-relevant by Overture category  "
          + format(int(lost["grocery_relevant"].sum()), ">6,"))
    print("  in a worst-retention ZIP prefix        "
          + format(int(lost["priority_region"].sum()), ">6,"))
    print("\n  top chains among the lost rows:")
    for c, n in lost.loc[lost["chain"] != "", "chain"].value_counts().head(12).items():
        print("    " + str(c).ljust(20) + format(n, ">5"))

    out = lost.sort_values(["priority", "grocery_relevant"], ascending=False)[[
        "name", "street", "city", "state", "zipcode", "latitude", "longitude",
        "primary_category", "alternate_categories", "phones", "websites",
        "chain", "grocery_relevant", "hard_loss", "priority_region", "priority"]]
    out.to_csv(args.out, index=False, encoding="utf-8")
    print("\nWrote " + args.out + "  (" + format(len(out), ",") + " rows)")
    print("\nThese stores are resolved on Google by the Frame probe, not the archived")
    print("Step 1.4 program (its verifier was inert). They are already in the frame's")
    print("candidate set; see Frame/README.md, 'Step 1b - the Google probe'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
