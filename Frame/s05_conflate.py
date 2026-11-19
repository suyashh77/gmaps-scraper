"""
s05_conflate.py - fold every source record into one row per physical store.

Inputs are the four source files, roughly 85,000 records describing perhaps
25,000 real stores. The job is to decide which records are the same shop.

Matching is deliberately conservative and tiered, strongest evidence first:

  T0  identical Google place_id, name and location   exact identity, corroborated
      agreeing
  T1  within 100 m and name similarity >= 80         same shop, same spot
  T2  within 250 m and name similarity >= 90         geocoding slop
  T3  same normalised name, house number and ZIP     coordinates disagree but
                                                     the postal address does not

Tier 3 exists because Overture and Google routinely disagree by 1-2 km on the
same store, which is the disagreement that made the old pipeline's distance
veto useless. Tier 1 and 2 are kept tight so that two branches of one chain in
one shopping centre stay separate; the previous pipeline's failure mode was
collapsing or confusing exactly those.

Clusters are formed by union-find over accepted pairs, so identity is
transitive across sources: an OSM node that matches a SNAP row that matches a
master store lands in one cluster.

Writes Frame/build/clusters.csv (record -> cluster) and
Frame/build/frame_raw.csv (one canonical row per cluster).
"""
import math
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

HERE = Path(__file__).parent
SRC = HERE / "sources"
BUILD = HERE / "build"

CELL = 0.0025           # ~275 m latitude
R_EARTH = 6371.0088

# Chain lexicon. A chain name is the part of identity that survives a store
# number, and chains are where wrong-branch matching happens, so they are
# detected explicitly rather than left to fuzzy matching.
CHAINS = [
    "food lion", "harris teeter", "ingles", "publix", "aldi", "lidl",
    "walmart", "target", "costco", "sams club", "bjs wholesale", "kroger",
    "whole foods", "trader joes", "earth fare", "fresh market", "wegmans",
    "dollar general", "dollar tree", "family dollar", "save a lot",
    "piggly wiggly", "food city", "compare foods", "super g mart",
    "seven eleven", "circle k", "sheetz", "wawa", "speedway", "bp", "shell",
    "exxon", "sunoco", "marathon", "citgo", "kangaroo express", "quality mart",
    "cvs", "walgreens", "rite aid", "dg market", "weis", "giant",
    "el super", "la fiesta", "tienda", "mercado", "carniceria",
]

_SUFFIX = re.compile(
    r"\b(inc|llc|l l c|ltd|co|corp|company|incorporated|the|of|store|stores|"
    r"supermarket|supermercado|market|grocery|groceries|foods|food)\b")
_STORENUM = re.compile(r"(#\s*\d+|\bno\.?\s*\d+\b|\bstore\s*\d+\b|\b\d{3,6}\b)")
_NONALNUM = re.compile(r"[^a-z0-9 ]")
_WS = re.compile(r"\s+")


def norm_name(s):
    """Aggressive normalisation used for similarity scoring."""
    s = _NONALNUM.sub(" ", str(s).lower())
    s = _STORENUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def core_name(s):
    """Normalised name with generic retail words stripped, for blocking."""
    s = norm_name(s)
    s = _SUFFIX.sub(" ", s)
    return _WS.sub(" ", s).strip()


def chain_of(s):
    n = norm_name(s)
    for c in CHAINS:
        if c in n:
            return c
    return None


def house_number(street):
    m = re.match(r"\s*(\d+)", str(street or ""))
    return m.group(1) if m else None


def hav(a, b, c, d):
    p = math.pi / 180
    return 2 * R_EARTH * math.asin(math.sqrt(
        math.sin((c - a) * p / 2) ** 2
        + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2))


class DSU:
    def __init__(self, n):
        self.p = list(range(n))

    def find(self, x):
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def load_sources():
    frames = []

    p = SRC / "snap_nc.csv"
    if p.exists():
        d = pd.read_csv(p, dtype=str)
        frames.append(pd.DataFrame({
            "source": "snap", "source_id": d.source_id, "name": d.name,
            "street": d.street, "city": d.city, "zip": d.zip,
            "county": d.county,
            "lat": pd.to_numeric(d.lat, errors="coerce"),
            "lon": pd.to_numeric(d.lon, errors="coerce"),
            "place_id": None, "raw_category": d.snap_store_type,
            "alt_categories": "", "prior_exclusion_reason": None,
            "prior_reviews_scraped": None, "prior_target": None,
        }))

    p = SRC / "overture_nc.csv"
    if p.exists():
        d = pd.read_csv(p, dtype=str)
        frames.append(pd.DataFrame({
            "source": "overture", "source_id": d.source_id, "name": d.name,
            "street": d.street, "city": d.city, "zip": d.zip, "county": None,
            "lat": pd.to_numeric(d.lat, errors="coerce"),
            "lon": pd.to_numeric(d.lon, errors="coerce"),
            "place_id": None, "raw_category": d.primary_category,
            "alt_categories": d.alternate_categories.fillna(""),
            "prior_exclusion_reason": None,
            "prior_reviews_scraped": None, "prior_target": None,
        }))

    p = SRC / "osm_nc.csv"
    if p.exists():
        d = pd.read_csv(p, dtype=str)
        d = d[d["name"].notna()]
        frames.append(pd.DataFrame({
            "source": "osm", "source_id": d.source_id, "name": d["name"],
            "street": d.street, "city": d.city, "zip": d.zip, "county": None,
            "lat": pd.to_numeric(d.lat, errors="coerce"),
            "lon": pd.to_numeric(d.lon, errors="coerce"),
            "place_id": None,
            "raw_category": d.osm_shop.fillna(d.osm_amenity),
            "alt_categories": "", "prior_exclusion_reason": None,
            "prior_reviews_scraped": None, "prior_target": None,
        }))

    p = SRC / "existing_nc.csv"
    if p.exists():
        d = pd.read_csv(p, dtype=str)
        frames.append(pd.DataFrame({
            "source": d.source, "source_id": d.source_id, "name": d["name"],
            "street": d.street, "city": d.city, "zip": d.zip, "county": None,
            "lat": pd.to_numeric(d.lat, errors="coerce"),
            "lon": pd.to_numeric(d.lon, errors="coerce"),
            "place_id": d.place_id, "raw_category": d.prior_category,
            "alt_categories": "",
            "prior_exclusion_reason": d.prior_exclusion_reason,
            "prior_reviews_scraped": pd.to_numeric(d.prior_reviews_scraped, errors="coerce"),
            "prior_target": pd.to_numeric(d.prior_target, errors="coerce"),
        }))

    df = pd.concat(frames, ignore_index=True)
    df = df[df["name"].notna() & (df["name"].astype(str).str.strip() != "")]
    return df.reset_index(drop=True)


def main():
    t0 = time.time()
    print("Conflating sources into physical stores")
    df = load_sources()
    print(f"  {len(df):,} named records")
    for s, n in df.source.value_counts().items():
        print(f"    {n:7,}  {s}")

    df["nname"] = df["name"].map(norm_name)
    df["cname"] = df["name"].map(core_name)
    df["chain"] = df["name"].map(chain_of)
    df["hnum"] = df.street.map(house_number)
    df["zip5"] = df.zip.astype("string").str.extract(r"(\d{5})", expand=False)

    dsu = DSU(len(df))
    pair_tier = {}
    nname_pre = df.nname.values
    lat_pre = df.lat.values
    lon_pre = df.lon.values

    def link(i, j, tier):
        key = (i, j) if i < j else (j, i)
        if key not in pair_tier or tier < pair_tier[key]:
            pair_tier[key] = tier
        dsu.union(i, j)

    # ---- T0: identical place_id, but only when corroborated ---------------
    #
    # A place_id is not trustworthy identity in this project's own data. 323
    # place_ids in master and the exclusion audit are each claimed by two or
    # more genuinely different stores - "Teresita Supermarket" and "Texoma
    # Tackle & Convenience Store" share one id, "El Torito" in Asheville shares
    # one with "La placita II" in Farmville, 452 km away. That is the Step 1.4
    # defect written into the data: verify_business logged a name similarity
    # and then accepted on both branches, so the enrichment took Google's first
    # search result for every query and reused ids across different stores.
    #
    # So a shared place_id merges only when the name or the location agrees.
    # A shared id that both name and distance contradict is a contamination
    # flag, recorded for adjudication, never a merge.
    by_pid = defaultdict(list)
    for i, pid in enumerate(df.place_id):
        if isinstance(pid, str) and pid.strip():
            by_pid[pid.strip()].append(i)

    n0 = 0
    collisions = []
    for pid, idxs in by_pid.items():
        if len(idxs) < 2:
            continue
        anchor = idxs[0]
        for j in idxs[1:]:
            sim = fuzz.token_set_ratio(nname_pre[anchor], nname_pre[j])
            km = None
            if not (math.isnan(lat_pre[anchor]) or math.isnan(lat_pre[j])):
                km = hav(lat_pre[anchor], lon_pre[anchor], lat_pre[j], lon_pre[j])
            # Calibrated on known pairs. Every correctly-shared id scores 100
            # and sits under 100 m; every crossed id scores 86 or less, or is
            # hundreds of km away. "Walmart Wilkesboro" against "Lowes Foods of
            # Wilkesboro" scores 71 at 390 m - same shopping centre, different
            # store - and "74 Kwik Stop" against "Warne Kwik Stop" scores 86 at
            # 431 km. Both are refusals.
            if sim >= 90 and (km is None or km <= 1.0):
                link(anchor, j, 0)
                n0 += 1
            else:
                collisions.append({
                    "place_id": pid,
                    "name_a": df["name"].iat[anchor], "source_a": df.source.iat[anchor],
                    "name_b": df["name"].iat[j], "source_b": df.source.iat[j],
                    "name_sim": sim, "km_apart": km,
                })
    print(f"  T0 place_id       : {n0:,} links over {len(by_pid):,} ids")
    print(f"     REFUSED (one id, different stores): {len(collisions):,} pairs")
    if collisions:
        BUILD.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(collisions).to_csv(BUILD / "placeid_collisions.csv", index=False)

    # ---- T1/T2: geographic + name ----------------------------------------
    grid = defaultdict(list)
    geo = df.dropna(subset=["lat", "lon"])
    for i, la, lo in zip(geo.index, geo.lat, geo.lon):
        grid[(int(la / CELL), int(lo / CELL))].append(i)

    lat = df.lat.values
    lon = df.lon.values
    nname = df.nname.values
    chain = df.chain.values
    n1 = n2 = 0
    seen = set()
    for (gy, gx), idxs in grid.items():
        cand = []
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                cand.extend(grid.get((gy + dy, gx + dx), ()))
        for i in idxs:
            for j in cand:
                if j <= i:
                    continue
                key = (i, j)
                if key in seen:
                    continue
                seen.add(key)
                d = hav(lat[i], lon[i], lat[j], lon[j])
                if d > 0.25:
                    continue
                # two different chains at one address are two different stores
                if chain[i] and chain[j] and chain[i] != chain[j]:
                    continue
                sim = fuzz.token_set_ratio(nname[i], nname[j])
                if d <= 0.10 and sim >= 80:
                    link(i, j, 1); n1 += 1
                elif sim >= 90:
                    link(i, j, 2); n2 += 1
    print(f"  T1 <=100m sim>=80 : {n1:,} links")
    print(f"  T2 <=250m sim>=90 : {n2:,} links")

    # ---- T3: same name + house number + ZIP -------------------------------
    by_addr = defaultdict(list)
    for i, (cn, hn, z) in enumerate(zip(df.cname, df.hnum, df.zip5)):
        if cn and hn and isinstance(z, str):
            by_addr[(cn, hn, z)].append(i)
    n3 = 0
    n3_refused = 0
    for idxs in by_addr.values():
        if len(idxs) < 2:
            continue
        a = idxs[0]
        for j in idxs[1:]:
            if dsu.find(a) == dsu.find(j):
                continue
            # T3 absorbs coordinate disagreement, not coordinate nonsense.
            if not (math.isnan(lat[a]) or math.isnan(lat[j])):
                if hav(lat[a], lon[a], lat[j], lon[j]) > 25.0:
                    n3_refused += 1
                    continue
            link(a, j, 3); n3 += 1
    print(f"  T3 name+hnum+zip  : {n3:,} links ({n3_refused:,} refused, >25 km apart)")

    # ---- T4: absorb departments into the store that contains them ---------
    #
    # Overture lists "Walmart Pharmacy", "Walmart Bakery", "Walmart Deli" and
    # "Walmart Grocery Pickup" as separate places at the same coordinates as
    # the Walmart. Left alone they become four extra stores, and the frame
    # reports 443 Walmarts in a state that has roughly 200. They are counted
    # once here, against the parent store.
    #
    # Format names are NOT departments: "Walmart Supercenter", "Neighborhood
    # Market" and "Express" are genuinely different stores, as is a standalone
    # pharmacy with no parent nearby - which is why absorption requires finding
    # a co-located non-department record of the same chain, and does nothing
    # when there isn't one.
    dept_re = re.compile(
        r"\b(pharmacy|bakery|deli|delicatessen|grocery pickup|pickup and delivery|"
        r"pickup delivery|curbside|connection center|auto care|auto center|"
        r"vision center|optical|photo center|portrait studio|garden center|"
        r"money center|customer service|fuel station|fuel center|tire lube|"
        r"tire and lube|nail salon|hair salon|floral|florist|meat department|"
        r"produce department|business center|distribution center|"
        r"employment center|optical center|hearing center)\b")

    is_dept = [bool(dept_re.search(n)) for n in nname]
    n4 = 0
    for i in range(len(df)):
        if not is_dept[i] or chain[i] is None or math.isnan(lat[i]):
            continue
        gy, gx = int(lat[i] / CELL), int(lon[i] / CELL)
        best_j, best_d = None, 0.25
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                for j in grid.get((gy + dy, gx + dx), ()):
                    if j == i or is_dept[j] or chain[j] != chain[i]:
                        continue
                    if math.isnan(lat[j]):
                        continue
                    d = hav(lat[i], lon[i], lat[j], lon[j])
                    if d < best_d:
                        best_d, best_j = d, j
        if best_j is not None and dsu.find(i) != dsu.find(best_j):
            link(best_j, i, 4)
            n4 += 1
    print(f"  T4 dept -> parent : {n4:,} absorbed")

    df["cluster_id"] = [dsu.find(i) for i in range(len(df))]
    ncl = df.cluster_id.nunique()
    print(f"\n  {len(df):,} records -> {ncl:,} clusters in {time.time()-t0:.0f}s")

    BUILD.mkdir(parents=True, exist_ok=True)
    df.to_csv(BUILD / "clusters.csv", index=False)

    sizes = df.groupby("cluster_id").size()
    print("\n  cluster size distribution:")
    for k in (1, 2, 3, 4, 5):
        print(f"    {k}{'  ' if k < 5 else '+ '} record(s): {int((sizes == k).sum() if k < 5 else (sizes >= 5).sum()):,}")
    print(f"    largest cluster: {int(sizes.max())} records")

    # how many clusters each source contributed to
    print("\n  clusters touched by each source:")
    for s in df.source.unique():
        print(f"    {s:14s} {df[df.source == s].cluster_id.nunique():7,}")

    print("\n  clusters by source combination (top 12):")
    combo = (df.groupby("cluster_id").source
               .agg(lambda x: "+".join(sorted(set(x)))).value_counts())
    for c, n in combo.head(12).items():
        print(f"    {n:7,}  {c}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
