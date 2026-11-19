"""
s12_validate_census.py - validate the store list against the US Census, per county.

This is the gate the original five were missing. All of those tested RECALL -
did we find everything. None tested PRECISION - did we invent anything. That is
how 506 industrial distributors entered the frame as warehouse clubs behind a
fully green validation board.

County Business Patterns counts establishments with paid employees, per county,
per NAICS. Nothing in this project touched it, and after the scope decision of
9 Sep 2026 it counts exactly the population the group wants: established stores,
not sole proprietors. So it is a target, not a floor.

Two things to hold in mind when reading a ratio:

  CBP undercounts    non-employer businesses are absent, so a small genuine
                     shop may be real and not in CBP.
  CBP over-splits    one Walmart can register as several establishments, and a
                     store's NAICS is self-assigned.

So a county at 1.3x is fine and a county at 4x is a defect. The gate flags
outliers for inspection; it does not pretend the right answer is exactly 1.0.

    python Frame/s12_validate_census.py --list Frame/build/final_list.csv
"""
import argparse
import io
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).parent
REPORTS = HERE / "reports"
CACHE = HERE / "sources" / "cbp23co.csv"
CBP_URL = "https://www2.census.gov/programs-surveys/cbp/datasets/2023/cbp23co.zip"

# Our store types -> the NAICS codes that count the same thing.
GROUPS = {
    "supermarket + grocery": (["supermarket", "grocery"], ["445110"]),
    "convenience incl fuel": (["convenience", "gas_convenience"], ["445120", "447110"]),
    "club + supercentre":    (["warehouse_club", "supercenter"], ["452311"]),
    "dollar / gen merch":    (["dollar"], ["452319"]),
    "specialty food":        (["butcher", "seafood", "specialty",
                               "ethnic_grocery", "health_food"],
                              ["445210", "445220", "445291", "445292", "445299"]),
}

# NC county FIPS -> name, built from the CBP file itself (it has no names), so
# we join on the 3-digit county code the SNAP data also carries via lat/lon.
def load_cbp():
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    if not CACHE.exists():
        print("downloading Census CBP 2023 county file...")
        r = requests.get(CBP_URL, timeout=300)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        d = pd.read_csv(z.open(z.namelist()[0]), dtype=str)
        d[d.fipstate == "37"].to_csv(CACHE, index=False)
    d = pd.read_csv(CACHE, dtype=str)
    d["est"] = pd.to_numeric(d.est, errors="coerce").fillna(0)
    # The county file has no `lfo` column (that is state-file only), so there
    # is no legal-form-of-organisation split to collapse here.
    return d[d.fipstate == "37"]


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--list", dest="lst", default=str(HERE / "build" / "probe_list.csv"))
    p.add_argument("--type-col", default="food_access_type")
    p.add_argument("--flag-above", type=float, default=2.0,
                   help="flag a category running this many times CBP")
    a = p.parse_args()

    f = pd.read_csv(a.lst, low_memory=False)
    cbp = load_cbp()
    print(f"store list: {len(f):,} rows from {Path(a.lst).name}")

    rows = []
    for label, (types, codes) in GROUPS.items():
        ours = int(f[a.type_col].isin(types).sum())
        theirs = int(cbp[cbp.naics.isin(codes)].est.sum())
        rows.append({"group": label, "census": theirs, "ours": ours,
                     "ratio": round(ours / theirs, 2) if theirs else None})
    t = pd.DataFrame(rows)
    tot_o, tot_c = t.ours.sum(), t.census.sum()

    print(f"\n{'group':26s} {'census':>8s} {'ours':>8s} {'ratio':>7s}")
    print("-" * 54)
    fails = 0
    for r in t.itertuples():
        flag = ""
        if r.ratio and r.ratio > a.flag_above:
            flag = "  <-- OVER"
            fails += 1
        print(f"{r.group:26s} {r.census:8,} {r.ours:8,} {str(r.ratio):>7s}{flag}")
    print("-" * 54)
    print(f"{'TOTAL':26s} {tot_c:8,} {tot_o:8,} {tot_o/tot_c:7.2f}")

    REPORTS.mkdir(parents=True, exist_ok=True)
    t.to_csv(REPORTS / "census_validation.csv", index=False)

    # per-county, on the biggest group, where an outlier is most diagnostic
    if "county" in f.columns:
        print("\nper-county check on supermarket + grocery (NAICS 445110):")
        cty = cbp[cbp.naics == "445110"][["fipscty", "est"]].copy()
        ours = (f[f[a.type_col].isin(["supermarket", "grocery"])]
                .groupby("county").size().rename("ours"))
        print(f"  CBP publishes 445110 for {len(cty)} of 100 NC counties "
              f"(the rest are suppressed for disclosure)")
        grp = f[f[a.type_col].isin(["supermarket", "grocery"])]
        with_cty = int(grp.county.notna().sum())
        print(f"  our list has {len(grp):,} stores in this group, of which "
              f"{with_cty:,} carry a county ({with_cty/len(grp)*100:.0f}%)")
        print("  NOTE: county is only populated for stores the SNAP register saw, so a")
        print("        per-county ratio computed now would be measuring SNAP coverage,")
        print("        not our coverage. The Google probe returns an address for every")
        print("        store; geocode county from that before trusting a per-county gate.")
        print(f"  statewide, stores-with-county vs CBP: {with_cty:,} / "
              f"{int(cty.est.sum()):,} = {with_cty/cty.est.sum():.2f}x")

    print(f"\nwrote {REPORTS/'census_validation.csv'}")
    print(f"\n{fails} of {len(t)} groups above {a.flag_above}x")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
