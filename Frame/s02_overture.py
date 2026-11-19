"""
s02_overture.py - Overture Maps places, North Carolina, current release.

Two changes from the 2026-01 pull that produced Step_1.3:

1. Release 2026-08-19.0 instead of 2026-01-21.0 (seven months of new stores).
2. The category list comes from category_ruleset.csv, not from a literal in the
   query. The old query listed 21 categories and did not include
   discount_store or department_store as primaries, so dollar stores and
   supercenters only entered the frame when they happened to carry a grocery
   string in categories.alternate. Overture NC currently holds 2,988
   discount_store and 699 department_store rows as primary.

Nothing is filtered on tier here. Every row that matches any ruled-in category
is pulled and tagged; inclusion is applied downstream so that a row can always
be traced to a ruling rather than vanishing inside a WHERE clause.

Writes Frame/sources/overture_nc.csv.
"""
import json
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).parent
RULESET = HERE / "category_ruleset.csv"
OUT = HERE / "sources" / "overture_nc.csv"
META = HERE / "sources" / "overture_nc.meta.json"

RELEASE = "2026-08-19.0"
SRC = f"s3://overturemaps-us-west-2/release/{RELEASE}/theme=places/type=place/*"

# Generous NC bounding box; the region filter does the real work.
BBOX = dict(xmin=-84.40, xmax=-75.30, ymin=33.70, ymax=36.65)


def main():
    rules = pd.read_csv(RULESET)
    included = sorted(rules.loc[rules.include, "category"])
    print(f"ruleset: {len(rules)} rulings, {len(included)} included categories")

    cats = ", ".join(f"'{c}'" for c in included)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")

    q = f"""
    SELECT
        id                              AS overture_id,
        names.primary                   AS name,
        categories.primary              AS primary_category,
        categories.alternate            AS alternate_categories,
        confidence                      AS confidence,
        addresses[1].freeform           AS street,
        addresses[1].locality           AS city,
        addresses[1].region             AS state,
        addresses[1].postcode           AS zip,
        phones[1]                       AS phone,
        websites[1]                     AS website,
        ST_Y(geometry)                  AS lat,
        ST_X(geometry)                  AS lon
    FROM read_parquet('{SRC}', filename=false, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {BBOX['xmin']} AND {BBOX['xmax']}
      AND bbox.ymin BETWEEN {BBOX['ymin']} AND {BBOX['ymax']}
      AND addresses[1].region = 'NC'
      AND (
            categories.primary IN ({cats})
         OR list_has_any(categories.alternate, [{cats}])
      )
    """
    t0 = time.time()
    print(f"querying Overture {RELEASE} ... (NC bbox + region, ruled-in categories)")
    con.execute("LOAD spatial;") if _has_spatial(con) else None
    df = con.execute(q).df()
    print(f"  {len(df):,} rows in {time.time() - t0:.0f}s")

    df["source"] = "overture"
    df["source_id"] = "ovt:" + df.overture_id.astype(str)
    df["alternate_categories"] = df.alternate_categories.apply(
        lambda x: "|".join(x) if isinstance(x, (list, tuple)) else ""
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    META.write_text(json.dumps({
        "source": "Overture Maps places",
        "release": RELEASE,
        "s3": SRC,
        "bbox": BBOX,
        "region": "NC",
        "included_categories": included,
        "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "rows": len(df),
    }, indent=2))

    print(f"  wrote {len(df):,} rows -> {OUT}")
    print("\n  top primary categories:")
    for c, n in df.primary_category.value_counts().head(20).items():
        print(f"    {n:6d}  {c}")

    matched_on_alt = df[~df.primary_category.isin(included)]
    print(f"\n  rows admitted only via categories.alternate: {len(matched_on_alt):,}")
    print(f"  distinct cities: {df.city.nunique()}   missing coords: {df.lat.isna().sum()}")


def _has_spatial(con):
    try:
        con.execute("INSTALL spatial;")
        return True
    except Exception:
        return False


if __name__ == "__main__":
    sys.exit(main())
