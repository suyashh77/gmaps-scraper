"""R04 - Put back the stores that were excluded by accident rather than by decision.

THE THREE CLASSES THIS RECOVERS

  1  WAREHOUSE_STORE_VOCAB  (8 stores, all of them Costco)
     Step_1.8_Grocery_Store_Classification.xlsx contains three warehouse
     strings and rules on them inconsistently:
         "Warehouse club"  -> yes
         "Warehouse store" -> No
         "Warehouse"       -> No
     Google labels Costco "Warehouse store" and labels Sam's Club and BJ's
     "Warehouse club". So Sam's Club and BJ's are in the dataset and every
     Costco in North Carolina is not, and the only thing separating them is
     which synonym Google happened to use. That is a typo in a lookup table,
     not a research judgement, and it is why the quality report records
     "Costco 0, NC has 9" as an unexplained anomaly.

  2  COMMA_SPLIT  (75 stores)
     Some rows carry a multi-category string such as
     "Convenience store, Gas station". The classifier compared the whole
     string against the include list by exact equality, so any store with a
     second category fell through to excluded. 75 of the 76 multi-category
     excluded rows have an INCLUDED category as their first token. Reading
     the primary category the way every other stage reads it retains them.

  3  MANUAL_REVIEW_INCLUDED_CATEGORY  (277 stores)
     Excluded as MANUAL_REVIEW_UNMATCHED, meaning the reconciliation step
     could not tie the Overture row to an Outscraper candidate. But these rows
     already carry a valid ChIJ place_id AND a Google-assigned category that
     is on the include list. Google had already resolved the identity the
     matcher failed to resolve. 633 rows qualify; 356 are already in master
     through the other pathway, leaving 277 genuinely new.

WHAT IT DOES NOT DO
     It does not invent target_reviews. A recovered store enters master with
     reviews_scraped = 0 and target_reviews = NULL, flagged NOT_STARTED, and
     the scraper sets a real target from Google's live count on first visit.
     Writing a guessed target is how the 157 bogus targets got there.

     It does not touch the 2,893+ stores lost between Step 1.3 and Step 1.5.
     Those never reached Google at all, so no SQL can recover them; they need
     an enrichment re-run. r05 emits that worklist.

USAGE
    python Remediation/r04_recover_inventory.py            # dry run
    python Remediation/r04_recover_inventory.py --apply
    python Remediation/r04_recover_inventory.py --csv out.csv
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import DATA, MASTER, backup, connect_ro, connect_rw, counts, print_delta

EXCLUSION_XLSX = os.path.join(DATA, "Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx")

INCLUDED_CATEGORIES = {
    "Convenience store", "Grocery store", "Dollar store", "Supermarket",
    "Department store", "Mexican grocery store", "General store", "Discount store",
    "Asian grocery store", "Health food store", "Warehouse club",
    "Indian grocery store", "Discount supermarket",
}

# The vocabulary repair. "Warehouse" alone stays out: it is a generic label
# that also covers storage facilities, and it must be adjudicated per store
# rather than swept in.
VOCAB_REPAIR = {"warehouse store"}


def first_category(value) -> str:
    return str(value or "").split(",")[0].strip()


def load_candidates() -> pd.DataFrame:
    ex = pd.read_excel(EXCLUSION_XLSX)
    ex["cat1"] = ex["google_category"].map(first_category)
    ex["pid"] = ex["google_place_id"].astype(str)
    ex["valid_pid"] = ex["pid"].str.startswith("ChIJ")
    ex["reason"] = ex["canonical_exclusion_reason"].astype(str)

    rows = []

    vocab = ex[ex["valid_pid"] & (ex["cat1"].str.lower().isin(VOCAB_REPAIR))]
    for _, r in vocab.iterrows():
        rows.append((r, "WAREHOUSE_STORE_VOCAB"))

    multi = ex[ex["valid_pid"]
               & ex["google_category"].astype(str).str.contains(",", na=False)
               & ex["cat1"].isin(INCLUDED_CATEGORIES)]
    for _, r in multi.iterrows():
        rows.append((r, "COMMA_SPLIT"))

    mru = ex[ex["valid_pid"]
             & ex["reason"].str.contains("MANUAL_REVIEW_UNMATCHED", na=False)
             & ex["cat1"].isin(INCLUDED_CATEGORIES)]
    for _, r in mru.iterrows():
        rows.append((r, "MANUAL_REVIEW_INCLUDED_CATEGORY"))

    seen, out = set(), []
    for r, why in rows:                      # first class wins on overlap
        if r["pid"] in seen:
            continue
        seen.add(r["pid"])
        out.append({
            "place_id": r["pid"],
            "store_name": r.get("source_business_name") or r.get("outscraper_business_name"),
            "business_name": r.get("outscraper_business_name"),
            "full_address": r.get("google_maps_address"),
            "street": r.get("source_street"),
            "city": r.get("source_city"),
            "state": r.get("source_state") or "NC",
            "zip": r.get("source_zip"),
            "source_lat": r.get("source_lat"),
            "source_lon": r.get("source_lon"),
            "category": r["cat1"],
            "business_status": r.get("google_business_status"),
            "recovery_class": why,
        })
    return pd.DataFrame(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--csv", default=None, help="write the recovery worklist to CSV")
    args = ap.parse_args()

    if not os.path.exists(EXCLUSION_XLSX):
        print("Missing " + EXCLUSION_XLSX)
        return 1

    cand = load_candidates()
    probe = connect_ro(args.master)
    in_master = {r[0] for r in probe.execute("SELECT place_id FROM stores")}
    before = counts(probe)
    has_flags = any(r[1] == "qa_flags" for r in probe.execute("PRAGMA table_info(stores)"))
    probe.close()

    cand["already_in_master"] = cand["place_id"].isin(in_master)

    # A permanently closed store was excluded correctly and stays excluded; the
    # pipeline already drops CLOSED_STORE and re-adding them would contradict a
    # decision that was made on purpose.
    #
    # CLOSED_TEMPORARILY is a different case and is kept. A store that is shut
    # this month is still part of the food landscape a resident experienced, and
    # its accumulated reviews are valid evidence about that store. It carries a
    # flag so the researcher can drop it with one WHERE clause if they disagree.
    status = cand["business_status"].astype(str).str.upper()
    cand["closed_permanently"] = status.str.contains("CLOSED_PERMANENTLY")
    cand["closed_temporarily"] = status.str.contains("CLOSED_TEMPORARILY")

    # "Warehouse store" also covers wholesale distributors that are not retail
    # grocery. Costco is unambiguous; anything else in that class is a judgement
    # call and is inserted flagged rather than silently accepted.
    is_costco = cand["store_name"].astype(str).str.contains("costco", case=False, na=False)
    cand["needs_adjudication"] = (
        (cand["recovery_class"] == "WAREHOUSE_STORE_VOCAB") & ~is_costco)

    new = cand[~cand["already_in_master"] & ~cand["closed_permanently"]].copy()
    skipped_closed = int((~cand["already_in_master"] & cand["closed_permanently"]).sum())

    print("RECOVERY CANDIDATES")
    print("  " + "class".ljust(34) + "eligible   already in master   NEW")
    for cls in ["WAREHOUSE_STORE_VOCAB", "COMMA_SPLIT", "MANUAL_REVIEW_INCLUDED_CATEGORY"]:
        sub = cand[cand["recovery_class"] == cls]
        print("  " + cls.ljust(34) + format(len(sub), ">8")
              + format(int(sub["already_in_master"].sum()), ">20")
              + format(int((~sub["already_in_master"]).sum()), ">6"))
    print("  " + "TOTAL".ljust(34) + format(len(cand), ">8")
          + format(int(cand["already_in_master"].sum()), ">20")
          + format(len(new), ">6"))
    print("\n  held back, permanently closed : " + str(skipped_closed))
    print("  inserted but flagged, temporarily closed : "
          + str(int(new["closed_temporarily"].sum())))
    print("  inserted but flagged for adjudication    : "
          + str(int(new["needs_adjudication"].sum())))

    print("\n  new stores by category:")
    for c, n in new["category"].value_counts().items():
        print("    " + str(c).ljust(28) + format(n, ">5"))

    costco = new[new["store_name"].astype(str).str.contains("Costco", case=False, na=False)]
    if len(costco):
        print("\n  Costco locations recovered: " + str(len(costco)))
        for _, r in costco.iterrows():
            print("    " + str(r["city"]).ljust(18) + str(r["place_id"]))

    if args.csv:
        cand.to_csv(args.csv, index=False, encoding="utf-8")
        print("\nWrote " + args.csv)

    if not args.apply:
        print("\nDRY RUN. Re-run with --apply to insert them into master.")
        return 0

    dest = backup(args.master, "r04")
    print("\nBackup written: " + os.path.basename(dest))

    conn = connect_rw(args.master)
    cur = conn.cursor()
    cols = ["place_id", "store_name", "business_name", "full_address", "street", "city",
            "state", "zip", "source_lat", "source_lon", "category", "business_status",
            "reviews_scraped", "scrape_status"]
    if has_flags:
        cols.append("qa_flags")
    ph = ",".join(["?"] * len(cols))

    payload = []
    for _, r in new.iterrows():
        v = [r["place_id"], r["store_name"], r["business_name"], r["full_address"],
             r["street"], r["city"], r["state"], r["zip"],
             None if pd.isna(r["source_lat"]) else float(r["source_lat"]),
             None if pd.isna(r["source_lon"]) else float(r["source_lon"]),
             r["category"], r["business_status"], 0, "not_started"]
        if has_flags:
            f = ["RECOVERED_" + r["recovery_class"], "NOT_STARTED", "NO_TARGET"]
            if r["closed_temporarily"]:
                f.append("CLOSED_TEMPORARILY")
            if r["needs_adjudication"]:
                f.append("NEEDS_ADJUDICATION")
            v.append(",".join(f))
        payload.append(tuple(v))

    cur.execute("BEGIN IMMEDIATE")
    cur.executemany(
        "INSERT OR IGNORE INTO stores (" + ",".join(cols) + ") VALUES (" + ph + ")", payload)
    inserted = cur.rowcount
    conn.commit()
    print("  inserted " + str(inserted) + " stores")

    after = counts(conn)
    print_delta(before, after)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    print("\nRecovered stores have no target and no reviews. Queue them with r06 "
          "so the scraper sets a live target on first visit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
