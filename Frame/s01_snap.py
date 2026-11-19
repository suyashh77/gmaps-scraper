"""
s01_snap.py — USDA FNS SNAP Retailer Location data, North Carolina.

The administrative spine of the frame. Every row is a retailer the federal
government has authorised to accept SNAP benefits, which means a human verified
the address and the store sells food. It is the only source in this pipeline
with a legal basis rather than a crowdsourced or commercial one, and it is the
frame the food-access literature expects.

Writes Frame/sources/snap_nc.csv.
"""
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

SERVICE = ("https://services1.arcgis.com/RLQu0rK7h4kbsBq5/arcgis/rest/"
           "services/snap_retailer_location_data/FeatureServer/0")
OUT = Path(__file__).parent / "sources" / "snap_nc.csv"
META = Path(__file__).parent / "sources" / "snap_nc.meta.json"
PAGE = 1000


def fetch_all(where="State='NC'"):
    """Page through the feature service. maxRecordCount is 1000."""
    session = requests.Session()
    total = session.get(f"{SERVICE}/query", timeout=60, params={
        "where": where, "returnCountOnly": "true", "f": "json",
    }).json()["count"]
    print(f"  service reports {total} rows for {where}")

    rows, offset = [], 0
    while offset < total:
        r = session.get(f"{SERVICE}/query", timeout=120, params={
            "where": where,
            "outFields": "*",
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": PAGE,
            "orderByFields": "Record_ID",
            "f": "json",
        })
        r.raise_for_status()
        feats = r.json().get("features", [])
        if not feats:
            break
        rows.extend(f["attributes"] for f in feats)
        offset += len(feats)
        print(f"  {offset}/{total}", end="\r", flush=True)
        time.sleep(0.2)
    print()
    return rows, total


def main():
    print("USDA FNS SNAP Retailer Location data — NC")
    rows, reported = fetch_all()
    df = pd.DataFrame(rows)
    if len(df) != reported:
        print(f"  WARNING: fetched {len(df)}, service reported {reported}")

    df = df.rename(columns={
        "Store_Name": "name",
        "Store_Street_Address": "street",
        "Additonal_Address": "street2",   # sic — the field is misspelled upstream
        "City": "city",
        "State": "state",
        "Zip_Code": "zip",
        "County": "county",
        "Store_Type": "snap_store_type",
        "Latitude": "lat",
        "Longitude": "lon",
    })
    df["source"] = "snap"
    df["source_id"] = "snap:" + df["Record_ID"].astype(str)

    keep = ["source", "source_id", "name", "street", "street2", "city", "state",
            "zip", "county", "snap_store_type", "lat", "lon",
            "Incentive_Program", "Grantee_Name"]
    df = df[[c for c in keep if c in df.columns]]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    META.write_text(json.dumps({
        "source": "USDA FNS SNAP Retailer Location data",
        "service": SERVICE,
        "where": "State='NC'",
        "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "rows": len(df),
        "service_reported": reported,
    }, indent=2))

    print(f"\n  wrote {len(df)} rows -> {OUT}")
    print("\n  Store_Type distribution:")
    for t, n in df.snap_store_type.value_counts().items():
        print(f"    {n:6d}  {t}")
    print(f"\n  distinct cities: {df.city.nunique()}   counties: {df.county.nunique()}")
    print(f"  missing coords : {df.lat.isna().sum()}")


if __name__ == "__main__":
    sys.exit(main())
