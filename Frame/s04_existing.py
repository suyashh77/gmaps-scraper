"""
s04_existing.py - normalise the assets this project already owns.

Three of them, all folded into the same schema as the external sources so that
conflation treats every record identically:

  master        5,226 stores in Step_5_master_reviews.db. These carry a
                verified Google place_id and, for most, collected reviews.
                They are not discarded and re-derived; they are a source.
  excluded      7,497 store-level rows from the Step 1.9 exclusion audit. A
                store that was excluded is a candidate with a recorded
                decision, and r04 already showed 337 of those decisions were
                wrong. They re-enter the frame and get re-ruled.
  overture_2601 16,680 rows of Step_1.3, the January Overture pull. Superseded
                by s02 as a discovery source, but retained because the
                definition of done requires every one of those 16,680 to reach
                a disposition rather than evaporate.

Writes Frame/sources/existing_nc.csv.
"""
import json
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
ROOT = HERE.parent
OUT = HERE / "sources" / "existing_nc.csv"
META = HERE / "sources" / "existing_nc.meta.json"

MASTER = ROOT / "Data" / "Step_5_master_reviews.db"
EXCLUDED = ROOT / "Data" / "Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx"
CANDIDATES = ROOT / "Data" / "Step_1.3_POI_Overture_NC.xlsx"

COLS = ["source", "source_id", "name", "street", "city", "state", "zip",
        "lat", "lon", "place_id", "prior_category", "prior_status",
        "prior_reviews_scraped", "prior_target", "prior_exclusion_reason"]


def load_master():
    db = sqlite3.connect(MASTER)
    d = pd.read_sql("""
        SELECT store_id, place_id, store_name, full_address, street, city,
               state, zip, source_lat, source_lon, category, business_status,
               reviews_scraped, target_reviews
        FROM stores
    """, db)
    db.close()
    out = pd.DataFrame({
        "source": "master",
        "source_id": "mst:" + d.store_id.astype(str),
        "name": d.store_name,
        "street": d.street,
        "city": d.city,
        "state": d.state.fillna("NC"),
        "zip": d.zip,
        "lat": d.source_lat,
        "lon": d.source_lon,
        "place_id": d.place_id,
        "prior_category": d.category,
        "prior_status": d.business_status,
        "prior_reviews_scraped": d.reviews_scraped,
        "prior_target": d.target_reviews,
        "prior_exclusion_reason": None,
    })
    print(f"  master        {len(out):6,} rows  ({out.place_id.notna().sum():,} with place_id)")
    return out


def load_excluded():
    d = pd.read_excel(EXCLUDED)
    out = pd.DataFrame({
        "source": "excluded",
        "source_id": "exc:" + d.dedup_store_id.astype(str),
        "name": d.source_business_name.fillna(d.outscraper_business_name),
        "street": d.source_street,
        "city": d.source_city,
        "state": d.source_state.fillna("NC"),
        "zip": d.source_zip,
        "lat": d.source_lat,
        "lon": d.source_lon,
        "place_id": d.google_place_id,
        "prior_category": d.google_category,
        "prior_status": d.google_business_status,
        "prior_reviews_scraped": None,
        "prior_target": None,
        "prior_exclusion_reason": d.canonical_exclusion_reason,
    })
    print(f"  excluded      {len(out):6,} rows  ({out.place_id.notna().sum():,} with place_id)")
    for r, n in out.prior_exclusion_reason.value_counts().items():
        print(f"      {n:6,}  {r}")
    return out


def load_candidates():
    d = pd.read_excel(CANDIDATES)
    out = pd.DataFrame({
        "source": "overture_2601",
        "source_id": "o26:" + d.index.astype(str),
        "name": d.name,
        "street": d.street,
        "city": d.city,
        "state": d.state.fillna("NC"),
        "zip": d.zipcode,
        "lat": d.latitude,
        "lon": d.longitude,
        "place_id": None,
        "prior_category": d.primary_category,
        "prior_status": None,
        "prior_reviews_scraped": None,
        "prior_target": None,
        "prior_exclusion_reason": None,
    })
    print(f"  overture_2601 {len(out):6,} rows")
    return out


def main():
    print("Existing project assets")
    parts = [load_master(), load_excluded(), load_candidates()]
    df = pd.concat(parts, ignore_index=True)[COLS]

    # zip arrives as float from Excel in places; normalise to 5-char text
    df["zip"] = (df.zip.astype("string")
                   .str.extract(r"(\d{5})", expand=False))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    META.write_text(json.dumps({
        "source": "project assets",
        "inputs": {
            "master": str(MASTER.name),
            "excluded": str(EXCLUDED.name),
            "candidates": str(CANDIDATES.name),
        },
        "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "rows": len(df),
        "by_source": df.source.value_counts().to_dict(),
    }, indent=2))

    print(f"\n  wrote {len(df):,} rows -> {OUT}")
    print(f"  with coordinates: {df.lat.notna().sum():,}")
    print(f"  with place_id   : {df.place_id.notna().sum():,}"
          f"  (distinct {df.place_id.nunique():,})")


if __name__ == "__main__":
    sys.exit(main())
