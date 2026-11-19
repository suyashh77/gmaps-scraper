"""
s08_freeze.py - freeze the frame to SQLite and stamp it.

Produces Data/Step_1_NC_Food_Retail_Frame.db with one `stores` table, plus a
SHA256 and a manifest recording every source, its retrieval date and its row
count. A frozen snapshot with a hash is the difference between "the dataset"
and "whatever the scripts produced last time somebody ran them".

Also reports the Step 2 workload the frame implies, since the whole point of
rebuilding the frame is to know what still has to be scraped.
"""
import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
ROOT = HERE.parent
BUILD = HERE / "build"
REPORTS = HERE / "reports"
OUT = ROOT / "Data" / "Step_1_NC_Food_Retail_Frame.db"
MANIFEST = REPORTS / "frame_manifest.json"

# A department only over-counts the frame when its parent store is also in the
# frame, so the test is chain-name AND department-word. Without the chain half
# it matches every standalone bakery, deli and CVS Pharmacy in North Carolina
# and reports a 6% defect rate that is not real.
DEPT_RE = (r"\b(pharmacy|bakery|deli|grocery pickup|pickup and delivery|"
           r"fuel station|fuel center|business center|distribution center|"
           r"garden center|money center|vision center|auto care)\b")
CHAIN_RE = (r"(walmart|target|harris teeter|food lion|publix|ingles|kroger|"
            r"lowes foods|costco|sam.?s club|bj.?s|aldi|lidl|whole foods|"
            r"fresh market|earth fare|piggly wiggly|save.?a.?lot)")


def sha256(path, buf=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(buf):
            h.update(chunk)
    return h.hexdigest()


def main():
    f = pd.read_csv(BUILD / "frame.csv")
    inframe = f[~f.tier.isin(["OUT", "UNRULED", "NO_CATEGORY"])].copy()
    print(f"frame: {len(f):,} clusters, {len(inframe):,} in frame")

    # Residual sub-entity inflation, reported rather than hidden.
    lname = inframe["name"].astype(str).str.lower()
    dept = inframe[lname.str.contains(DEPT_RE, na=False, regex=True)
                   & lname.str.contains(CHAIN_RE, na=False, regex=True)]
    print(f"residual chain sub-entities (no parent within 250 m): {len(dept):,}"
          f"  ({len(dept)/len(inframe)*100:.2f}%)")
    dept.to_csv(REPORTS / "residual_subentities.csv", index=False)

    inframe = inframe.rename(columns={"frame_id": "store_id"})
    inframe["state"] = "NC"
    # frozen_at deliberately does NOT go in the table. A timestamp inside the
    # hashed artifact changes the SHA on every run, which makes the hash
    # useless for the one thing it is for: telling you whether two people are
    # looking at the same data. It lives in the manifest instead.

    cols = ["store_id", "name", "street", "city", "state", "zip", "county",
            "lat", "lon", "place_id", "tier", "food_access_type",
            "scrape_priority", "ruling_basis", "ruling_vocab", "ruling_category",
            "n_records", "n_sources", "in_snap", "in_overture", "in_osm",
            "in_master", "in_excluded", "in_overture_2601",
            "prior_exclusion_reason", "prior_reviews_scraped", "source_ids"]
    out = inframe[cols]

    if OUT.exists():
        OUT.unlink()
    con = sqlite3.connect(OUT)
    out.to_sql("stores", con, index=False)
    con.execute("CREATE UNIQUE INDEX idx_store_id ON stores(store_id)")
    con.execute("CREATE INDEX idx_place_id ON stores(place_id)")
    con.execute("CREATE INDEX idx_tier ON stores(tier, scrape_priority)")
    con.execute("CREATE INDEX idx_geo ON stores(lat, lon)")
    con.commit()
    con.close()

    digest = sha256(OUT)
    srcs = {}
    for m in sorted((HERE / "sources").glob("*.meta.json")):
        srcs[m.stem.replace(".meta", "")] = json.loads(m.read_text())

    MANIFEST.write_text(json.dumps({
        "artifact": OUT.name,
        "sha256": digest,
        "frozen_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stores": len(out),
        "clusters_total": len(f),
        "by_tier": f.tier.value_counts().to_dict(),
        "by_food_access_type": out.food_access_type.value_counts().to_dict(),
        "counties": int(out.county.nunique()),
        "cities": int(out.city.nunique()),
        "residual_subentities": len(dept),
        "sources": srcs,
    }, indent=2, default=str))

    print(f"\nwrote {OUT}  ({OUT.stat().st_size/1e6:.1f} MB)")
    print(f"  sha256 {digest}")
    print(f"  manifest -> {MANIFEST}")

    # ---- Step 2 workload --------------------------------------------------
    print("\n--- Step 2 workload implied by this frame ---")
    have_pid = out.place_id.notna()
    scraped = out.prior_reviews_scraped.fillna(0) > 0
    print(f"  stores in frame                      {len(out):7,}")
    print(f"  already carry a Google place_id      {int(have_pid.sum()):7,}"
          f"   ({have_pid.mean()*100:.1f}%)")
    print(f"  need a place_id resolved             {int((~have_pid).sum()):7,}")
    print(f"  already have some reviews collected  {int(scraped.sum()):7,}")
    print(f"  never scraped                        {int((~scraped).sum()):7,}")
    print()
    for p, label in [(1, "P1 CORE"), (2, "P2 FRESH+SPECIALTY"), (3, "P3 MARGINAL")]:
        g = out[out.scrape_priority == p]
        if not len(g):
            continue
        gs = g.prior_reviews_scraped.fillna(0) > 0
        print(f"  {label:22s} {len(g):7,} stores  "
              f"{int(g.place_id.notna().sum()):6,} with place_id  "
              f"{int((~gs).sum()):6,} never scraped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
