"""
s15_poi_decision_sheet.py - one row per store, everything we know about it, for the
final POI-list ruling.

The research group has to decide, once, which stores are in the study. This script puts
every fact we hold about every candidate on one Excel row so that decision can be made
on one sheet instead of five databases.

Sources (all read-only):

  Data/Step_1_NC_Food_Retail_Frame.db   stores          36,820  the frozen four-source frame - base universe
  Frame/reports/dropped_stores.csv                       20,069  why s10 left a frame row off the probe list
  Frame/build/probe.db                  probe           16,751  what Google says about each probed store
  Data/Step_5_master_reviews.db         stores / reviews  8,380  scrape state, joined on Google place_id
  dewy data/nc_grocery_pois.db          nc_grocery_pois 21,532  SafeGraph Places (Dewey, Aug-2026 release)
  Dr.ben_data/unique_stores_visited.db  unique_stores    6,993  SafeGraph/Advan weekly Patterns, 2017-2026

The two SafeGraph lists share no key with ours, so they are linked with the project's own
geomatch (name + address + coordinates), blocked by ZIP3 exactly as s13 does, then reduced
to a one-to-one assignment (best score, then name similarity, then distance). A second,
proximity pass then catches records geomatch's strict address rule missed: same spot
(<150 m) and the name or house number agrees. Anything that still does not land on a frame
row is appended as its own row so nothing is lost.

Usage
  python Frame/s15_poi_decision_sheet.py --match            # geomatch + proximity links, slow (~2 h), cached in Frame/build/
  python Frame/s15_poi_decision_sheet.py --proximity-only   # keep the cached geomatch links, redo the proximity pass (~1 min)
  python Frame/s15_poi_decision_sheet.py --export           # assemble Data/exports/POI_Decision_Sheet_<date>.xlsx
  python Frame/s15_poi_decision_sheet.py --match --export
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
FRAME_DB = ROOT / "Data" / "Step_1_NC_Food_Retail_Frame.db"
DROPPED_CSV = HERE / "reports" / "dropped_stores.csv"
PROBE_DB = HERE / "build" / "probe.db"
MASTER_DB = ROOT / "Data" / "Step_5_master_reviews.db"
# Large third-party inputs live outside the repo (moved 2026-10-24); override with GMAPS_EXTERNAL_DATA.
EXTERNAL = Path(os.environ.get("GMAPS_EXTERNAL_DATA", ROOT.parent / "Gmaps Scraper - external data"))
DEWEY_DB = EXTERNAL / "dewy data" / "nc_grocery_pois.db"
PATTERNS_DB = EXTERNAL / "Dr.ben_data" / "unique_stores_visited.db"
BUILD = HERE / "build"
EXPORTS = ROOT / "Data" / "exports"

MATCH_DEWEY_FRAME = BUILD / "s15_match_dewey_frame.csv"
MATCH_PATTERNS_FRAME = BUILD / "s15_match_patterns_frame.csv"
MATCH_PATTERNS_DEWEY = BUILD / "s15_match_patterns_dewey.csv"

PAIR_COLUMNS = ["id_left", "id_right", "score", "decision", "why", "name_sim", "dist_km"]

# geomatch v1 exposed the two features the sheet reads under feature names; v2 returns
# name_sim natively and the distance in metres. Both are accepted so a cached v1 match file
# and a fresh v2 run normalise to the same seven columns.
FEATURE_ALIASES = {"name_sim": ["name_sim", "f_token_set_ratio_poi"],
                   "dist_km": ["dist_km", "f_coord_distance_km"]}
METRES_PER_KM = 1000.0

# Patterns uses this as "still open"; Dewey Places sets TRACKING_CLOSED_SINCE on almost
# every row, so only CLOSED_ON means closed there (see the Notes sheet).
PATTERNS_OPEN_SENTINEL = "2038-01-01"

ZIP5_RE = re.compile(r"(\d{5})")


# --------------------------------------------------------------------------- helpers
def zip5(value) -> str | None:
    """'27284.0', '28105-5766', 27284.0 -> '27284'; anything without five digits -> None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value)
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    m = ZIP5_RE.search(text)
    return m.group(1) if m else None


def zip3(value) -> str | None:
    z = zip5(value)
    return z[:3] if z else None


def build_addr(df: pd.DataFrame, street: str, city: str, zipcol: str) -> pd.Series:
    """'<street>, <city>, NC <zip5>' with blank parts left out."""
    st = df[street].fillna("").astype(str).str.strip()
    ct = df[city].fillna("").astype(str).str.strip()
    zp = df[zipcol].map(zip5).fillna("")
    state_zip = ("NC " + zp).str.strip()
    parts = pd.concat([st, ct, state_zip], axis=1)
    return parts.apply(lambda r: ", ".join(p for p in r if p), axis=1)


def coalesce(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """First non-blank value across cols, row-wise."""
    out = pd.Series([None] * len(df), index=df.index, dtype=object)
    for c in cols:
        if c not in df.columns:
            continue
        v = df[c]
        blank = v.isna() | (v.astype(str).str.strip() == "")
        out = out.where(out.notna(), v.where(~blank))
    return out


SOURCE_MARKERS = [
    ("frame", "frame_store_id"),
    ("google", "google_probed_at"),
    ("master", "master_store_id"),
    ("dewey", "dewey_placekey"),
    ("patterns", "patterns_store_id"),
]


def source_flags(df: pd.DataFrame) -> pd.Series:
    """'frame|google|dewey' - which sources contributed to the row."""
    flags = pd.Series([""] * len(df), index=df.index, dtype=object)
    for label, col in SOURCE_MARKERS:
        if col not in df.columns:
            continue
        present = df[col].notna()
        flags = flags.where(~present, flags.where(flags == "", flags + "|") + label)
    return flags


def normalize_pairs(raw: pd.DataFrame) -> pd.DataFrame:
    """geomatch output (v1 or v2) reduced to PAIR_COLUMNS, distance always in km.

    Rows are returned as given, decisions included: dropping non-matches is
    assign_one_to_one's job, so a caller can still audit what was refused.
    """
    out = raw.copy()
    for target, sources in FEATURE_ALIASES.items():
        got = next((c for c in sources if c in out.columns), None)
        out[target] = out[got] if got else None
    if "dist_km" not in raw.columns and "dist_m" in raw.columns:
        out["dist_km"] = pd.to_numeric(raw["dist_m"], errors="coerce") / METRES_PER_KM
    for c in PAIR_COLUMNS:
        if c not in out.columns:
            out[c] = None
    return out[PAIR_COLUMNS]


def assign_one_to_one(pairs: pd.DataFrame) -> pd.DataFrame:
    """Reduce scored candidate pairs to a one-to-one assignment.

    Keeps only decision == 'match'; walks pairs best-first (score desc, name_sim desc,
    dist_km asc) and accepts a pair when neither side is taken yet. n_alternatives is how
    many match-grade pairs the left record had - >1 means the frame holds duplicates
    (or the SafeGraph list does) and the ruling should look at both.
    """
    cols = PAIR_COLUMNS + ["n_alternatives"]
    if pairs is None or len(pairs) == 0:
        return pd.DataFrame(columns=cols)
    m = pairs[pairs.decision == "match"].copy()
    if len(m) == 0:
        return pd.DataFrame(columns=cols)
    m["name_sim"] = pd.to_numeric(m.get("name_sim"), errors="coerce").fillna(0.0)
    m["dist_km"] = pd.to_numeric(m.get("dist_km"), errors="coerce").fillna(9e9)
    m = m.sort_values(["score", "name_sim", "dist_km"], ascending=[False, False, True])
    alternatives = m.groupby("id_left").size()
    taken_left: set = set()
    taken_right: set = set()
    keep = []
    for row in m.itertuples(index=False):
        if row.id_left in taken_left or row.id_right in taken_right:
            continue
        taken_left.add(row.id_left)
        taken_right.add(row.id_right)
        keep.append(row)
    out = pd.DataFrame(keep, columns=m.columns)
    out["n_alternatives"] = out.id_left.map(alternatives).astype(int)
    return out[[c for c in cols if c in out.columns]].reset_index(drop=True)


# --------------------------------------------------------------------------- proximity pass
# geomatch's accept rule wants house number + street + ZIP to agree. SafeGraph records often
# carry a neighbouring ZIP, a spelled-out "Boulevard" or a corporate name ("Food Lion, LLC"),
# so a second pass looks at what is physically at the same spot.
PROXIMITY_RADIUS_KM = 0.15
PROXIMITY_NAME_MIN = 85          # rapidfuzz token_set_ratio on normalised names
PROXIMITY_SCORE = 0.8            # below every geomatch accept, so the two passes sort apart
CORPORATE_SUFFIX = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c|corp|corporation|co|company|ltd|markets?|stores?|"
    r"supermarkets?|wholesale)\b\.?", re.I)
STORE_NUMBER = re.compile(r"(#\s*\d+|\bno\.?\s*\d+\b|\bstore\s*\d+\b|\b\d{2,6}\b)", re.I)
HOUSE_NUMBER = re.compile(r"^\s*(\d+)")
GRID_DEG = 0.002                 # ~200 m cells; neighbours are searched in the 3x3 block


def norm_name(name) -> str:
    """Lower-case, drop punctuation, corporate suffixes and store numbers."""
    text = re.sub(r"[^\w\s]", "", str(name or "")).lower()
    text = CORPORATE_SUFFIX.sub(" ", text)
    text = STORE_NUMBER.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _house_number(addr) -> str | None:
    m = HOUSE_NUMBER.match(str(addr or ""))
    return m.group(1) if m else None


def _haversine_km(lat1, lon1, lat2, lon2) -> pd.Series:
    import numpy as np

    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi, dlmb = p2 - p1, np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlmb / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(a))


def _grid_candidates(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Every (left, right) pair whose grid cells touch - the cheap superset of 'nearby'."""
    lft = left.dropna(subset=["lat", "lon"]).copy()
    rgt = right.dropna(subset=["lat", "lon"]).copy()
    lft["gx"], lft["gy"] = (lft.lon // GRID_DEG).astype(int), (lft.lat // GRID_DEG).astype(int)
    rgt["gx"], rgt["gy"] = (rgt.lon // GRID_DEG).astype(int), (rgt.lat // GRID_DEG).astype(int)
    shifted = [rgt.assign(gx=rgt.gx + dx, gy=rgt.gy + dy)
               for dx in (-1, 0, 1) for dy in (-1, 0, 1)]
    return lft.merge(pd.concat(shifted, ignore_index=True), on=["gx", "gy"], suffixes=("_l", "_r"))


def _empty_pairs() -> pd.DataFrame:
    return assign_one_to_one(pd.DataFrame(columns=PAIR_COLUMNS))


def proximity_pass(left: pd.DataFrame, right: pd.DataFrame,
                   taken_left: set, taken_right: set) -> pd.DataFrame:
    """One-to-one links for records within PROXIMITY_RADIUS_KM whose name or house number agrees.

    Both frames need _id, name, addr, lat, lon. Returns PAIR_COLUMNS + n_alternatives with
    why starting 'proximity pass' and score PROXIMITY_SCORE, so a reader can tell these from
    geomatch's rule-based accepts.
    """
    from rapidfuzz import fuzz

    cand = _grid_candidates(left[~left._id.isin(taken_left)], right[~right._id.isin(taken_right)])
    if len(cand) == 0:
        return _empty_pairs()
    cand["dist_km"] = _haversine_km(cand.lat_l, cand.lon_l, cand.lat_r, cand.lon_r)
    cand = cand[cand.dist_km <= PROXIMITY_RADIUS_KM].copy()
    if len(cand) == 0:
        return _empty_pairs()
    cand["name_sim"] = [fuzz.token_set_ratio(norm_name(a), norm_name(b)) / 100.0
                        for a, b in zip(cand.name_l, cand.name_r)]
    house_l = cand.addr_l.map(_house_number)
    same_house = house_l.notna() & (house_l == cand.addr_r.map(_house_number))
    good_name = cand.name_sim * 100 >= PROXIMITY_NAME_MIN
    cand = cand[same_house | good_name].copy()
    reasons = [" + ".join(r for r, ok in (("same house number", h), ("name agrees", n)) if ok)
               for h, n in zip(same_house[cand.index], good_name[cand.index])]
    metres = (cand.dist_km * 1000).round().astype(int).astype(str)
    cand["why"] = "proximity pass: " + pd.Series(reasons, index=cand.index) + " within " + metres + " m"
    cand["score"] = PROXIMITY_SCORE
    cand["decision"] = "match"
    pairs = cand.rename(columns={"_id_l": "id_left", "_id_r": "id_right"})[PAIR_COLUMNS]
    return assign_one_to_one(pairs)


CONFIDENCE = [
    ("same house number + name agrees", "B proximity house+name"),
    ("name agrees", "C proximity name"),
    ("same house number", "D proximity house only - verify"),
]


def match_confidence(why: pd.Series) -> pd.Series:
    """Rank a match_why string so the sheet can be filtered by how much to trust the link."""
    def label(w):
        if w is None or (isinstance(w, float) and pd.isna(w)):
            return None
        text = str(w)
        if not text.startswith("proximity pass"):
            return "A geomatch"
        return next((lab for needle, lab in CONFIDENCE if needle in text), "D proximity house only - verify")
    return why.map(label)


# --------------------------------------------------------------------------- loading
def read_sql(db: Path, sql: str) -> pd.DataFrame:
    if not db.exists():
        sys.exit(f"missing input: {db}")
    con = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    try:
        return pd.read_sql(sql, con)
    finally:
        con.close()


def load_frame() -> pd.DataFrame:
    f = read_sql(FRAME_DB, "SELECT * FROM stores")
    d = pd.read_csv(DROPPED_CSV, usecols=["store_id", "drop_reason", "drop_detail"],
                    low_memory=False)
    f = f.merge(d, on="store_id", how="left")
    f["disposition"] = f.drop_reason.isna().map({True: "PROBE_LIST", False: "DROPPED_BY_S10"})
    return f


def load_probe() -> pd.DataFrame:
    return read_sql(PROBE_DB, "SELECT * FROM probe")


def load_master() -> pd.DataFrame:
    s = read_sql(MASTER_DB, "SELECT * FROM stores")
    r = read_sql(MASTER_DB, """
        SELECT store_id,
               COUNT(*)                                                         AS reviews_in_db,
               SUM(CASE WHEN review_text IS NOT NULL AND review_text <> '' THEN 1 ELSE 0 END)
                                                                               AS reviews_with_text,
               MAX(scraped_at)                                                  AS last_scraped_at
        FROM reviews GROUP BY store_id""")
    return s.merge(r, on="store_id", how="left")


def require_unique(df: pd.DataFrame, col: str, source: str) -> None:
    """The assembly merges on this column; a duplicate would silently double rows."""
    if not df[col].is_unique:
        sys.exit(f"{source}: {col} is not unique ({int(df[col].duplicated().sum())} duplicates) "
                 "- assemble() merges assume it is a key")


def load_dewey() -> pd.DataFrame:
    d = read_sql(DEWEY_DB, "SELECT * FROM nc_grocery_pois")
    require_unique(d, "PLACEKEY", DEWEY_DB.name)
    d["closed"] = d.CLOSED_ON.notna().astype(int)
    return d


def load_patterns() -> pd.DataFrame:
    p = read_sql(PATTERNS_DB, "SELECT * FROM unique_stores")
    require_unique(p, "store_id", PATTERNS_DB.name)
    p["closed"] = (p.close_date.astype(str).str[:10] < PATTERNS_OPEN_SENTINEL).astype(int)
    return p


# --------------------------------------------------------------------------- matching
def _link_blocked(left: pd.DataFrame, right: pd.DataFrame, label: str) -> pd.DataFrame:
    """geomatch.link within ZIP3 blocks. Both frames need name, addr, lat, lon, _id, z3."""
    import geomatch

    hits = []
    blocks = sorted(set(left.z3.dropna()) & set(right.z3.dropna()))
    t0 = time.time()
    for i, z in enumerate(blocks, 1):
        L, R = left[left.z3 == z], right[right.z3 == z]
        if not len(L) or not len(R):
            continue
        t = time.time()
        r = normalize_pairs(geomatch.link(L, R, address="addr", name="name", lat="lat", lon="lon",
                                          id="_id", one_to_one=True, verbose=False))
        hits.append(r)
        print(f"  [{label}] ZIP3 {z} ({i}/{len(blocks)}): {len(L):5,} x {len(R):5,} -> "
              f"{int((r.decision == 'match').sum()):5,} match pairs  {time.time() - t:5.0f}s",
              flush=True)
    print(f"  [{label}] done in {(time.time() - t0) / 60:.1f} min")
    return pd.concat(hits, ignore_index=True) if hits else pd.DataFrame(columns=PAIR_COLUMNS)


def _prep_frame(f: pd.DataFrame) -> pd.DataFrame:
    x = f[["store_id", "name", "street", "city", "zip", "lat", "lon"]].copy()
    x["addr"] = build_addr(x, "street", "city", "zip")
    x["z3"] = x.zip.map(zip3)
    x["_id"] = x.store_id
    return x


def _prep_dewey(d: pd.DataFrame) -> pd.DataFrame:
    x = d[["PLACEKEY", "LOCATION_NAME", "STREET_ADDRESS", "CITY", "POSTAL_CODE",
           "LATITUDE", "LONGITUDE"]].rename(columns={
               "LOCATION_NAME": "name", "LATITUDE": "lat", "LONGITUDE": "lon"})
    x = x.copy()
    x["addr"] = build_addr(x, "STREET_ADDRESS", "CITY", "POSTAL_CODE")
    x["z3"] = x.POSTAL_CODE.map(zip3)
    x["_id"] = x.PLACEKEY
    return x


def _prep_patterns(p: pd.DataFrame) -> pd.DataFrame:
    x = p[["store_id", "store_name", "address", "city", "zip_code", "latitude", "longitude"]]
    x = x.rename(columns={"store_name": "name", "latitude": "lat", "longitude": "lon"}).copy()
    x["addr"] = build_addr(x, "address", "city", "zip_code")
    x["z3"] = x.zip_code.map(zip3)
    x["_id"] = x.store_id
    return x


def _geomatch_rows(path: Path) -> pd.DataFrame:
    """The cached first-pass links, without any proximity rows a previous run appended."""
    cached = _read_match(path)
    return cached[~cached.why.astype(str).str.startswith("proximity pass")]


def _two_pass(left: pd.DataFrame, right: pd.DataFrame, label: str, path: Path,
              geomatch_first: bool) -> pd.DataFrame:
    """geomatch (or its cached result) followed by the proximity pass; writes and returns both."""
    if geomatch_first:
        first = assign_one_to_one(_link_blocked(left, right, label))
    else:  # cached links are only valid for ids still on both sides (leftovers shrink)
        first = _geomatch_rows(path)
        first = first[first.id_left.isin(set(left._id)) & first.id_right.isin(set(right._id))]
    second = proximity_pass(left, right, taken_left=set(first.id_left), taken_right=set(first.id_right))
    both = pd.concat([first, second], ignore_index=True)
    both.to_csv(path, index=False)
    print(f"  [{label}] geomatch {len(first):,} + proximity {len(second):,} = {len(both):,} "
          f"-> {path.name}")
    return both


def run_match(geomatch_first: bool = True) -> None:
    BUILD.mkdir(parents=True, exist_ok=True)
    frame = _prep_frame(load_frame())
    dewey = _prep_dewey(load_dewey())
    patterns = _prep_patterns(load_patterns())
    print(f"frame {len(frame):,}   dewey {len(dewey):,}   patterns {len(patterns):,}")

    print("\n=== Dewey Places -> frame ===")
    df_pairs = _two_pass(dewey, frame, "dewey->frame", MATCH_DEWEY_FRAME, geomatch_first)

    print("\n=== Patterns -> frame ===")
    pf_pairs = _two_pass(patterns, frame, "patterns->frame", MATCH_PATTERNS_FRAME, geomatch_first)

    print("\n=== leftover Patterns -> leftover Dewey ===")
    pat_left = patterns[~patterns._id.isin(set(pf_pairs.id_left))]
    dew_left = dewey[~dewey._id.isin(set(df_pairs.id_left))]
    _two_pass(pat_left, dew_left, "patterns->dewey", MATCH_PATTERNS_DEWEY, geomatch_first)


# --------------------------------------------------------------------------- assembly
def _read_match(path: Path) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"missing {path} - run with --match first")
    return pd.read_csv(path, low_memory=False)


def _prefix(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    return df.rename(columns={c: f"{prefix}_{c}" for c in df.columns})


def _match_block(pairs: pd.DataFrame, left_name: str, right_name: str, prefix: str) -> pd.DataFrame:
    """Rename a cached match file into <prefix>_match_* columns keyed by the two ids."""
    return pairs.drop(columns=["decision"]).rename(columns={
        "id_left": left_name, "id_right": right_name,
        "score": f"{prefix}_match_score", "why": f"{prefix}_match_why",
        "name_sim": f"{prefix}_match_name_sim", "dist_km": f"{prefix}_match_dist_km",
        "n_alternatives": f"{prefix}_match_alternatives"})


def _google_block(probe: pd.DataFrame) -> pd.DataFrame:
    g = probe.drop(columns=["name", "street", "city", "state", "zip", "county", "lat", "lon",
                            "food_access_type"]).copy()
    g = g.rename(columns={c: c[2:] for c in g.columns if c.startswith("g_")})
    g = g.rename(columns={"status": "probe_status", "place_id_in": "place_id_searched",
                          "error": "probe_error", "attempts": "probe_attempts"})
    shared = g.place_id.dropna().value_counts()
    g["place_id_shared_by_n_frame_rows"] = g.place_id.map(shared)
    return _prefix(g, "google").rename(columns={"google_store_id": "frame_store_id"})


def _master_block(master: pd.DataFrame) -> pd.DataFrame:
    return _prefix(master, "master").rename(columns={"master_place_id": "join_place_id"})


def _dewey_block(dewey: pd.DataFrame) -> pd.DataFrame:
    d = dewey.drop(columns=["REGION", "operational_status"]).copy()
    d.columns = [c.lower() for c in d.columns]
    return _prefix(d, "dewey")


def _patterns_block(patterns: pd.DataFrame) -> pd.DataFrame:
    return _prefix(patterns.drop(columns=["state"]), "patterns")


BEST_COLUMNS = {
    "name": ["frame_name", "google_name", "dewey_location_name", "patterns_store_name",
             "master_store_name"],
    "street": ["frame_street", "dewey_street_address", "patterns_address", "master_street"],
    "city": ["frame_city", "dewey_city", "patterns_city", "master_city"],
    "zip": ["frame_zip", "dewey_postal_code", "patterns_zip_code", "master_zip"],
    "county": ["frame_county"],
    "lat": ["frame_lat", "google_lat", "dewey_latitude", "patterns_latitude", "master_source_lat"],
    "lon": ["frame_lon", "google_lon", "dewey_longitude", "patterns_longitude", "master_source_lon"],
}


def assemble() -> pd.DataFrame:
    frame = _prefix(load_frame(), "frame")
    google = _google_block(load_probe())
    master = _master_block(load_master())
    dewey = _dewey_block(load_dewey())
    patterns = _patterns_block(load_patterns())
    m_df = _match_block(_read_match(MATCH_DEWEY_FRAME), "dewey_placekey", "frame_store_id", "dewey")
    m_pf = _match_block(_read_match(MATCH_PATTERNS_FRAME), "patterns_store_id", "frame_store_id",
                        "patterns")
    m_pd = _match_block(_read_match(MATCH_PATTERNS_DEWEY), "patterns_store_id", "dewey_placekey",
                        "patterns")

    # frame + google, then master via Google place_id. The frame's prior id is this row's
    # own scrape record, so it wins when master has it; the probe's answer is the fallback
    # (it can be a collision shared with a neighbouring row). Blank when no master row exists.
    rows = frame.merge(google, on="frame_store_id", how="left")
    known = set(master.join_place_id.dropna())
    rows["_f"] = rows.frame_place_id.where(rows.frame_place_id.isin(known))
    rows["_g"] = rows.google_place_id.where(rows.google_place_id.isin(known))
    rows["join_place_id"] = coalesce(rows, ["_f", "_g"])
    rows = rows.drop(columns=["_g", "_f"]).merge(master, on="join_place_id", how="left")

    # SafeGraph lists onto the frame rows they matched
    rows = rows.merge(m_df, on="frame_store_id", how="left").merge(dewey, on="dewey_placekey", how="left")
    rows = rows.merge(m_pf, on="frame_store_id", how="left").merge(patterns, on="patterns_store_id",
                                                                   how="left")
    rows["row_key"] = "frame:" + rows.frame_store_id.astype(int).astype(str)

    # Dewey rows that landed nowhere become their own rows, carrying any leftover Patterns match
    dewey_only = dewey[~dewey.dewey_placekey.isin(set(m_df.dewey_placekey))]
    dewey_only = dewey_only.merge(m_pd, on="dewey_placekey", how="left").merge(
        patterns, on="patterns_store_id", how="left")
    dewey_only["row_key"] = "dewey:" + dewey_only.dewey_placekey.astype(str)

    # Patterns rows that landed nowhere at all
    placed = set(m_pf.patterns_store_id) | set(m_pd.patterns_store_id)
    patterns_only = patterns[~patterns.patterns_store_id.isin(placed)].copy()
    patterns_only["row_key"] = "patterns:" + patterns_only.patterns_store_id.astype(int).astype(str)

    # master rows whose place_id no frame row carries
    master_only = master[~master.join_place_id.isin(set(rows.join_place_id.dropna()))].copy()
    master_only["row_key"] = "master:" + master_only.master_store_id.astype(int).astype(str)

    parts = [p for p in (rows, dewey_only, patterns_only, master_only) if len(p)]
    out = pd.concat(parts, ignore_index=True, sort=False)
    out["sources"] = source_flags(out)
    out["dewey_match_confidence"] = match_confidence(out.dewey_match_why)
    out["patterns_match_confidence"] = match_confidence(out.patterns_match_why)
    for best, cands in BEST_COLUMNS.items():
        out[f"best_{best}"] = coalesce(out, cands)
    out["best_zip"] = out.best_zip.map(zip5)
    out["in_probe_list"] = out.frame_disposition.eq("PROBE_LIST").astype(int)
    out["ruling"] = ""
    out["ruling_note"] = ""
    return out[order_columns(out)]


LEAD_COLUMNS = ["row_key", "sources", "in_probe_list", "ruling", "ruling_note",
                "best_name", "best_street", "best_city", "best_zip", "best_county",
                "best_lat", "best_lon",
                "frame_tier", "frame_food_access_type", "frame_disposition", "frame_drop_reason",
                "google_category", "google_probe_status", "google_closed", "google_total_reviews",
                "master_scrape_status", "master_reviews_in_db",
                "dewey_food_access_type", "dewey_sub_category", "dewey_closed", "dewey_match_confidence",
                "patterns_sub_category", "patterns_brand", "patterns_closed",
                "patterns_avg_weekly_visits", "patterns_match_confidence"]


def order_columns(df: pd.DataFrame) -> list[str]:
    lead = [c for c in LEAD_COLUMNS if c in df.columns]
    rest = [c for c in df.columns if c not in lead]
    groups = ["frame_", "google_", "master_", "dewey_", "patterns_"]
    grouped = [c for g in groups for c in rest if c.startswith(g)]
    tail = [c for c in rest if c not in grouped]
    return lead + grouped + tail


# --------------------------------------------------------------------------- summaries
def _count(df: pd.DataFrame, col: str, weight: str | None = None) -> pd.DataFrame:
    g = df.groupby(df[col].fillna("(none)"), dropna=False)
    out = g.size().rename("stores").to_frame()
    if weight and weight in df.columns:
        out[weight] = g[weight].sum(min_count=1)
    return out.sort_values("stores", ascending=False).reset_index()


def summaries(out: pd.DataFrame) -> dict[str, pd.DataFrame]:
    probe = out[out.in_probe_list == 1]
    return {
        "Summary_Sources": _count(out, "sources"),
        "Summary_GoogleCategory": _count(probe, "google_category", "google_total_reviews"),
        "Summary_FrameType": _count(out[out.frame_store_id.notna()], "frame_food_access_type",
                                    "google_total_reviews"),
        "Summary_DeweyType": _count(out[out.dewey_placekey.notna()], "dewey_food_access_type"),
        "Summary_PatternsType": _count(out[out.patterns_store_id.notna()], "patterns_sub_category",
                                       "patterns_total_visits"),
        "Xtab_Google_x_Frame": pd.crosstab(probe.google_category.fillna("(none)"),
                                           probe.frame_food_access_type.fillna("(none)"),
                                           margins=True).reset_index(),
        "Xtab_Google_x_Dewey": pd.crosstab(out.google_category.fillna("(none)"),
                                           out.dewey_food_access_type.fillna("(no dewey match)"),
                                           margins=True).reset_index(),
    }


COLUMN_NOTES = {
    "row_key": "frame:<store_id> for frame rows; dewey:/patterns:/master: for rows that matched nothing in the frame",
    "sources": "which sources contributed: frame | google (probe) | master (scraped) | dewey (Places) | patterns (Patterns)",
    "in_probe_list": "1 = on the 16,751-store probe list (s10 kept it); 0 = dropped by s10 or not in the frame",
    "ruling": "EMPTY - for the research lead to fill: IN / OUT",
    "ruling_note": "EMPTY - reason for the ruling",
    "best_*": "first non-blank across frame, Google, Dewey, Patterns, master - for reading, not for joining",
    "frame_*": "Data/Step_1_NC_Food_Retail_Frame.db (SNAP + Overture + OSM + prior assets); frame_food_access_type is OUR category from Frame/category_ruleset.csv",
    "frame_disposition / frame_drop_reason / frame_drop_detail": "PROBE_LIST or DROPPED_BY_S10 with the s10 reason (Frame/reports/dropped_stores.csv)",
    "google_*": "Frame/build/probe.db - what Google Maps returned when we searched the store; google_category is Google's label; google_total_reviews is the live count; google_closed 1 = permanently closed",
    "google_probe_status": "done / unverified (Google page name did not match) / failed (browser error) / pending",
    "google_place_id_shared_by_n_frame_rows": ">1 means several frame rows resolved to the same Google place - probable duplicates",
    "master_*": "Data/Step_5_master_reviews.db stores - joined on Google place_id; master_reviews_in_db / master_reviews_with_text are live counts from the reviews table",
    "join_place_id": "the Google place_id that attached the master row (the frame's prior id when master has it, else the probe's answer); blank = no master row",
    "dewey_*": "dewy data/nc_grocery_pois.db - SafeGraph Places (Dewey, release 2026-08); dewey_food_access_type is the project taxonomy applied to NAICS in extract_dewey_nc.py",
    "dewey_closed": "1 only when CLOSED_ON is set. Ignore the source's operational_status: TRACKING_CLOSED_SINCE is populated on 21,529/21,532 rows so it marked everything closed",
    "dewey_match_*": "geomatch score / rule / name similarity / distance for the Dewey->frame link; alternatives >1 = other frame rows also matched",
    "*_match_confidence": "A geomatch (address rule, trust it) > B proximity house+name > C proximity name (same chain within 150 m, street number differs) > D proximity house only (same street number, different name - strip-mall neighbours and rebranded gas stations live here; verify before relying on it)",
    "patterns_*": "Dr.ben_data/unique_stores_visited.db - SafeGraph/Advan weekly Patterns 2017-01-02..2026-09-06 aggregated per store (visits, dwell, distance from home)",
    "patterns_closed": "1 when close_date is before the 2038-01-01 open sentinel",
    "patterns_match_*": "geomatch link details, Patterns->frame (or Patterns->Dewey for dewey: rows)",
}


def notes_sheet(out: pd.DataFrame) -> pd.DataFrame:
    n_frame = int(out.frame_store_id.notna().sum())
    n_dewey = int(out.row_key.str.startswith("dewey:").sum())
    n_patt = int(out.row_key.str.startswith("patterns:").sum())
    n_master = int(out.row_key.str.startswith("master:").sum())
    lines = [
        ("Built", str(date.today())),
        ("Rows", f"{len(out):,} - one per store; {n_frame:,} frame rows, {n_dewey:,} Dewey-only, "
                 f"{n_patt:,} Patterns-only, {n_master:,} master-only"),
        ("Base universe", "the frozen 36,820-store frame (Data/Step_1_NC_Food_Retail_Frame.db); every row "
                          "is kept, including the 20,069 s10 dropped, so all categories are visible"),
        ("Google", "Frame/build/probe.db - 16,751 probed (done / unverified / failed)"),
        ("Master", "Data/Step_5_master_reviews.db joined on Google place_id (frame's prior id first, probe's answer second)"),
        ("Dewey Places", "dewy data/nc_grocery_pois.db, 21,532 NC food-retail POIs, SafeGraph release 2026-08 via Dewey"),
        ("Patterns", "Dr.ben_data/unique_stores_visited.db, 6,993 NC stores with 2017-2026 weekly foot traffic"),
        ("Matching - pass 1", "geomatch.link (name + address + lat/lon), blocked by ZIP3, decision == 'match' only, "
                              "then one-to-one by score > name similarity > distance (score 0.87-1.0)"),
        ("Matching - pass 2", "proximity: frame store within 150 m whose house number or normalised name agrees "
                              "(token_set_ratio >= 85); score fixed at 0.8, why starts 'proximity pass'. Lower confidence "
                              "than pass 1 - check the name columns when it matters"),
        ("Matching - leftovers", "Patterns rows placed on no frame row were then matched to Dewey-only rows the same way. "
                                 "Match files: Frame/build/s15_match_*.csv"),
        ("Caveat - Dewey status", "operational_status in the Dewey extract is wrong (everything 'closed'); use dewey_closed"),
        ("Caveat - place_id collisions", "2,246 probe rows share a Google place_id with another row - see "
                                         "google_place_id_shared_by_n_frame_rows"),
        ("Caveat - ZIP3 blocking", "a SafeGraph record whose ZIP3 differs from ours cannot match and appears as its own row"),
        ("Regenerate", "python Frame/s15_poi_decision_sheet.py --match --export"),
    ]
    return pd.DataFrame(lines, columns=["item", "detail"])


# --------------------------------------------------------------------------- export
def write_xlsx(out: pd.DataFrame, path: Path) -> None:
    from openpyxl.utils import get_column_letter

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        out.to_excel(xw, sheet_name="Stores", index=False)
        ws = xw.sheets["Stores"]
        ws.freeze_panes = "F2"
        ws.auto_filter.ref = ws.dimensions
        for i, col in enumerate(out.columns, 1):
            ws.column_dimensions[get_column_letter(i)].width = min(max(len(col) + 2, 12), 40)
        for name, df in summaries(out).items():
            df.to_excel(xw, sheet_name=name, index=False)
        pd.DataFrame(list(COLUMN_NOTES.items()), columns=["column", "meaning"]).to_excel(
            xw, sheet_name="Columns", index=False)
        notes_sheet(out).to_excel(xw, sheet_name="Notes", index=False)


def run_export() -> Path:
    out = assemble()
    path = EXPORTS / f"POI_Decision_Sheet_{date.today():%Y-%m-%d}.xlsx"
    print(f"{len(out):,} rows x {len(out.columns)} columns")
    print(out.sources.value_counts().to_string())
    write_xlsx(out, path)
    print(f"\nwrote {path}")
    return path


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--match", action="store_true", help="run geomatch + proximity links (slow)")
    p.add_argument("--proximity-only", action="store_true",
                   help="reuse the cached geomatch links, redo only the proximity pass (fast)")
    p.add_argument("--export", action="store_true", help="assemble and write the xlsx")
    a = p.parse_args()
    if not (a.match or a.proximity_only or a.export):
        p.print_help()
        return 0
    if a.match or a.proximity_only:
        run_match(geomatch_first=a.match)
    if a.export:
        run_export()
    return 0


if __name__ == "__main__":
    sys.exit(main())
