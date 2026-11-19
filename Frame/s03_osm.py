"""
s03_osm.py - OpenStreetMap food retail in North Carolina, via Overpass.

Third independent source. OSM matters here for a specific reason: it is edited
by people who live in the place, so it carries small independent stores -
rural corner stores, tiendas, produce stands - that commercial POI aggregators
under-collect. Those are precisely the stores whose absence turns a served
neighbourhood into an apparent food desert.

Writes Frame/sources/osm_nc.csv.
"""
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).parent
OUT = HERE / "sources" / "osm_nc.csv"
META = HERE / "sources" / "osm_nc.meta.json"

# kumi first: the main instance rate-limits and times out on a query this size.
# A descriptive User-Agent is mandatory - the default requests UA is rejected at
# the Apache layer with a bare 406 that looks like a query syntax error.
ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
]
HEADERS = {
    "User-Agent": "NCSU-ISE-FoodAccess-Research/1.0 (academic use; NC grocery POI frame)",
    "Content-Type": "text/plain; charset=utf-8",
}

SHOP_VALUES = [
    "supermarket", "convenience", "grocery", "greengrocer", "butcher",
    "seafood", "deli", "general", "variety_store", "wholesale",
    "health_food", "farm", "dairy", "cheese", "spices", "frozen_food",
    "bakery", "department_store", "food", "nutrition_supplements",
]

QUERY = """
[out:json][timeout:600];
area["ISO3166-2"="US-NC"][admin_level=4]->.nc;
(
  nwr["shop"~"^(%s)$"](area.nc);
  nwr["amenity"="marketplace"](area.nc);
);
out center tags;
""" % "|".join(SHOP_VALUES)


def run_overpass():
    last = None
    for url in ENDPOINTS:
        try:
            print(f"  trying {url}")
            r = requests.post(url, data=QUERY.encode("utf-8"),
                              headers=HEADERS, timeout=900)
            if r.status_code == 200:
                return r.json(), url
            print(f"    HTTP {r.status_code}")
            last = f"HTTP {r.status_code}"
        except Exception as e:
            print(f"    {type(e).__name__}: {e}")
            last = f"{type(e).__name__}: {e}"
        time.sleep(5)
    raise RuntimeError(f"all Overpass endpoints failed; last: {last}")


def main():
    print("OpenStreetMap food retail - NC (Overpass)")
    data, used = run_overpass()
    els = data.get("elements", [])
    print(f"  {len(els):,} elements")

    rows = []
    for e in els:
        tags = e.get("tags", {})
        lat = e.get("lat") or (e.get("center") or {}).get("lat")
        lon = e.get("lon") or (e.get("center") or {}).get("lon")
        if lat is None or lon is None:
            continue
        rows.append({
            "source": "osm",
            "source_id": f"osm:{e['type']}/{e['id']}",
            "name": tags.get("name"),
            "osm_shop": tags.get("shop"),
            "osm_amenity": tags.get("amenity"),
            "brand": tags.get("brand"),
            "operator": tags.get("operator"),
            "street": " ".join(x for x in [tags.get("addr:housenumber"),
                                           tags.get("addr:street")] if x) or None,
            "city": tags.get("addr:city"),
            "state": tags.get("addr:state") or "NC",
            "zip": tags.get("addr:postcode"),
            "phone": tags.get("phone") or tags.get("contact:phone"),
            "website": tags.get("website") or tags.get("contact:website"),
            "lat": lat,
            "lon": lon,
        })

    df = pd.DataFrame(rows)
    named = df[df["name"].notna()]
    print(f"  {len(df):,} with coordinates, {len(named):,} with a name")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    META.write_text(json.dumps({
        "source": "OpenStreetMap via Overpass",
        "endpoint": used,
        "query": QUERY,
        "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "rows": len(df),
        "rows_named": int(len(named)),
    }, indent=2))

    print(f"  wrote {len(df):,} rows -> {OUT}")
    print("\n  by shop tag:")
    for c, n in df.osm_shop.value_counts().head(25).items():
        print(f"    {n:6d}  {c}")
    print(f"    {int(df.osm_amenity.eq('marketplace').sum()):6d}  amenity=marketplace")


if __name__ == "__main__":
    sys.exit(main())
