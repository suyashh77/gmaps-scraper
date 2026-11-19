"""
s09_geomatch_dedupe.py - deduplicate the frame with the project's own geomatch
library, county by county.

Two notes that matter for anyone re-running this.

1. Blocking is by county. A duplicate of a store is always in the same county,
   so this is exact rather than an approximation, and it turns an O(n^2) job on
   36,820 rows into 100 small ones.

2. Store numbers MUST be stripped from the name before it reaches geomatch.
   `api._prep_rows` builds its parse input as `name + " " + address`, so a name
   like "FOOD LION #1542" puts 1542 in front of the real house number, the
   parser reads it as the house number, and the hard rule then rejects the pair
   with "house numbers differ". That silently turns every numbered chain store
   into a non-match - which is most of a grocery frame. With the numbers
   stripped, the same four control pairs score 1.00.
"""
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import geomatch

HERE = Path(__file__).parent
DB = HERE.parent / "Data" / "Step_1_NC_Food_Retail_Frame.db"
OUT = HERE / "reports"

STORENUM = re.compile(r"(#\s*\d+|\bno\.?\s*\d+\b|\bstore\s*\d+\b|\b\d{2,6}\b)", re.I)


def clean_name(s):
    return re.sub(r"\s+", " ", STORENUM.sub(" ", str(s))).strip()


def main():
    s = pd.read_sql("select * from stores", sqlite3.connect(DB))
    s["addr"] = (s.street.fillna("") + ", " + s.city.fillna("")
                 + ", NC " + s.zip.fillna("").astype(str)).str.strip(", ")
    s["cname"] = s["name"].map(clean_name)
    print(f"{len(s):,} frame stores across {s.county.nunique()} counties")

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    allm, n_pairs = [], 0
    for i, (cty, g) in enumerate(s.groupby("county", dropna=False), 1):
        if len(g) < 2:
            continue
        r = geomatch.dedupe(g, address="addr", name="cname",
                            lat="lat", lon="lon", id="store_id")
        n_pairs += len(r)
        m = r[r.decision == "match"]
        for x in m.itertuples():
            a, b = find(x.id_left), find(x.id_right)
            if a != b:
                parent[b] = a
            allm.append({"county": cty, "score": x.score,
                         "id_left": x.id_left, "id_right": x.id_right,
                         "why": getattr(x, "why", "")})
        print(f"  [{i:3d}/100] {str(cty)[:16]:16s} {len(g):5,} stores  "
              f"{len(r):7,} pairs  {len(m):3d} matches", flush=True)

    comp = Counter(find(i) for i in s.store_id)
    removed = sum(v - 1 for v in comp.values() if v > 1)
    print(f"\n=== geomatch dedupe over the whole frame ===")
    print(f"  candidate pairs evaluated : {n_pairs:,}")
    print(f"  pairs judged a match      : {len(allm):,}")
    print(f"  stores that would merge   : {removed:,}")
    print(f"  frame {len(s):,} -> {len(s)-removed:,}   ({removed/len(s)*100:.2f}% removed)")

    OUT.mkdir(parents=True, exist_ok=True)
    nm = dict(zip(s.store_id, s["name"]))
    if allm:
        d = pd.DataFrame(allm)
        d["name_left"] = d.id_left.map(nm)
        d["name_right"] = d.id_right.map(nm)
        d.sort_values("score", ascending=False).to_csv(OUT / "geomatch_duplicates.csv", index=False)
        print(f"  wrote {OUT/'geomatch_duplicates.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
