"""
s13_dataaxle.py - ingest Data Axle exports and reconcile them against our list.

Data Axle Reference Solutions is behind an NC State / NC LIVE login and its
licence covers manual download through the web UI, not automated extraction.
So the download is a human job; this script takes whatever CSVs come out of it.

Drop every export into Frame/sources/dataaxle/ (any number of files, any split
by NAICS or ZIP) and run this. It concatenates them, normalises the columns,
deduplicates, and links the result against our store list with geomatch.

Why bother: Data Axle is the only source we have with a *properly assigned*
NAICS code per business. Overture and OSM tell you a place exists and guess at
what it is; Google tells you what it calls itself. Data Axle is an independent
industry classification, which makes it the check on whether the Google-based
rules are picking out the right stores rather than a self-consistent story.

    python Frame/s13_dataaxle.py --ingest
    python Frame/s13_dataaxle.py --reconcile --list Frame/build/probe_list.csv
"""
import argparse
import glob
import re
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
SRC = HERE / "sources" / "dataaxle"
OUT = HERE / "sources" / "dataaxle_nc.csv"
REPORTS = HERE / "reports"

# Data Axle column headings vary by export template; map what we have seen.
COLMAP = {
    "company name": "name", "company": "name", "business name": "name",
    "address line 1": "street", "street address": "street", "address": "street",
    "city": "city", "state": "state",
    "zipcode": "zip", "zip code": "zip", "postal code": "zip", "zip": "zip",
    "county": "county",
    "primary naics": "naics", "naics code": "naics", "naics": "naics",
    "primary naics description": "naics_desc", "naics description": "naics_desc",
    "primary sic": "sic", "sic code": "sic",
    "location employee size actual": "employees",
    "employee size (location)": "employees", "employees": "employees",
    "location sales volume actual": "sales", "sales volume (location)": "sales",
    "latitude": "lat", "longitude": "lon",
    "phone number": "phone", "phone": "phone",
    "years in database": "years", "year established": "year_established",
    "location type": "location_type",
}


def ingest(args):
    SRC.mkdir(parents=True, exist_ok=True)
    files = sorted(glob.glob(str(SRC / "*.csv")) + glob.glob(str(SRC / "*.CSV")))
    if not files:
        print(f"No CSVs found in {SRC}")
        print("Download the Data Axle exports there first - see the search plan in")
        print("Frame/reports/dataaxle_search_plan.csv")
        return 1

    frames = []
    for fp in files:
        try:
            d = pd.read_csv(fp, dtype=str, encoding="utf-8", on_bad_lines="skip")
        except UnicodeDecodeError:
            d = pd.read_csv(fp, dtype=str, encoding="latin-1", on_bad_lines="skip")
        d.columns = [str(c).strip().lower() for c in d.columns]
        d = d.rename(columns={c: COLMAP[c] for c in d.columns if c in COLMAP})
        d["_source_file"] = Path(fp).name
        frames.append(d)
        print(f"  {Path(fp).name:44s} {len(d):6,} rows, {len(d.columns)} cols")

    d = pd.concat(frames, ignore_index=True)
    keep = [c for c in ["name", "street", "city", "state", "zip", "county",
                        "naics", "naics_desc", "sic", "employees", "sales",
                        "lat", "lon", "phone", "year_established",
                        "location_type", "_source_file"] if c in d.columns]
    d = d[keep]
    if "name" not in d.columns:
        print("\nERROR: no recognisable company-name column. Columns seen:")
        print(sorted(set(c for f in frames for c in f.columns))[:40])
        print("\nAdd the right heading to COLMAP at the top of this file.")
        return 2

    before = len(d)
    d["zip5"] = d.get("zip", pd.Series(dtype=str)).astype(str).str.extract(r"(\d{5})", expand=False)
    d = d[d.name.notna()]
    # the same business can arrive in two exports if searches overlap
    d = d.drop_duplicates(subset=[c for c in ["name", "street", "zip5"] if c in d.columns])
    if "state" in d.columns:
        nc = d.state.astype(str).str.upper().str.strip().isin(["NC", "NORTH CAROLINA"])
        if nc.any():
            d = d[nc]

    d.to_csv(OUT, index=False)
    print(f"\n{before:,} rows in, {len(d):,} after dedup and NC filter")
    print(f"wrote {OUT}")
    if "naics" in d.columns:
        d["naics6"] = d.naics.astype(str).str.extract(r"(\d{6})", expand=False)
        print("\nby NAICS:")
        for c, n in d.naics6.value_counts().head(15).items():
            desc = d.loc[d.naics6 == c, "naics_desc"].dropna()
            print(f"  {n:6,}  {c}  {desc.iloc[0][:44] if len(desc) else ''}")
    if "employees" in d.columns:
        e = pd.to_numeric(d.employees, errors="coerce")
        print(f"\nemployee counts present for {int(e.notna().sum()):,} "
              f"({e.notna().mean()*100:.0f}%); median {e.median():.0f}")
    return 0


def reconcile(args):
    import geomatch
    if not OUT.exists():
        print(f"run --ingest first ({OUT} missing)")
        return 1
    da = pd.read_csv(OUT, dtype=str, low_memory=False)
    ours = pd.read_csv(args.lst, low_memory=False)
    print(f"Data Axle {len(da):,}   our list {len(ours):,}")

    da["addr"] = (da.get("street", "").fillna("") + ", " + da.get("city", "").fillna("")
                  + ", NC " + da.get("zip5", "").fillna("")).str.strip(", ")
    ours["addr"] = (ours.street.fillna("") + ", " + ours.city.fillna("")
                    + ", NC " + ours.zip.fillna("").astype(str)).str.strip(", ")
    da["_id"] = range(len(da))

    # Block by ZIP3 so this stays tractable and stays exact - a business and its
    # match are in the same postal area or the address is wrong anyway.
    da["z3"] = da.zip5.astype(str).str[:3]
    ours["z3"] = ours.zip.astype(str).str.extract(r"(\d{3})", expand=False)
    hits, n = [], 0
    for z3 in sorted(set(da.z3.dropna()) & set(ours.z3.dropna())):
        L, R = da[da.z3 == z3], ours[ours.z3 == z3]
        if not len(L) or not len(R):
            continue
        r = geomatch.link(L, R, address="addr", name="name", id="_id"
                          if "_id" in L.columns else None)
        n += len(r)
        hits.append(r[r.decision == "match"])
        print(f"  ZIP3 {z3}: {len(L):5,} x {len(R):5,} -> "
              f"{int((r.decision=='match').sum()):5,} matches", flush=True)
    m = pd.concat(hits, ignore_index=True) if hits else pd.DataFrame()

    REPORTS.mkdir(parents=True, exist_ok=True)
    m.to_csv(REPORTS / "dataaxle_matches.csv", index=False)
    matched = m.id_left.nunique() if len(m) else 0
    print(f"\n{n:,} pairs evaluated")
    print(f"  Data Axle businesses matched to our list : {matched:,} "
          f"({matched/len(da)*100:.1f}% of Data Axle)")
    print(f"  Data Axle businesses we do NOT have      : {len(da)-matched:,}"
          f"   <-- candidates we may be missing")
    print(f"\nwrote {REPORTS/'dataaxle_matches.csv'}")
    if len(da) - matched:
        unm = da[~da._id.isin(set(m.id_left))] if len(m) else da
        unm.to_csv(REPORTS / "dataaxle_unmatched.csv", index=False)
        print(f"wrote {REPORTS/'dataaxle_unmatched.csv'} - review these for real stores we lack")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ingest", action="store_true")
    p.add_argument("--reconcile", action="store_true")
    p.add_argument("--list", dest="lst", default=str(HERE / "build" / "probe_list.csv"))
    a = p.parse_args()
    if a.ingest:
        return ingest(a)
    if a.reconcile:
        return reconcile(a)
    p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
