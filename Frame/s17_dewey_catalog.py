"""
s17_dewey_catalog.py - every NC Dewey (SafeGraph Places) POI, every category, and how far
our reviews got for each one. The worksheets for ruling what counts as a grocery store.

Unlike s16 (the 21,532 POIs the extract pre-filtered as food retail, reached through the
frame), this starts from all 740,732 NC POIs and links master stores to them directly, so
a store of ours that SafeGraph files under a non-grocery category still shows up.

Link: Dewey leads - every POI is kept, and for each one we ask which of our stores it is.
Three passes, one-to-one, each on what the last left: geomatch (G; food-related POIs,
kept only when the name or the phone also agrees), proximity within 250 m on name and
house number (A-D), and a phone number unique on both sides within 2 km (P). See
GRADE_MEANING. An open POI beats a closed one at equal grade.

Outputs (dewy data/catalog/, overwritten on every run)
  NC_Dewey_All_POIs.xlsx       one row per POI (740,732) with our review status attached
  Dewey_Categories.xlsx        every unique top category and top/sub pair, with counts,
                               brands, our status mix, and a blank ruling column
  Master_to_Dewey_Match.xlsx   every master store and the POI it linked to (or none)
  by_category/NN_<top>.xlsx    one file per food-relevant top category: summary, all
                               rows, one sheet per subcategory

Usage
  python Frame/s17_dewey_catalog.py
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import s15_poi_decision_sheet as s15  # noqa: E402
import s16_dewey_report as s16  # noqa: E402

ROOT = HERE.parent
DEWEY_PARQUET = s15.EXTERNAL / "dewy data" / "nc_all_pois.parquet"
OUT_DIR = s15.EXTERNAL / "dewy data" / "catalog"
CATALOG_PARQUET = OUT_DIR / "nc_dewey_catalog.parquet"

EARTH_RADIUS_M = 6_371_000.0
RADIUS_NAME_M = 250.0      # SafeGraph pins the building, Google the entrance; big boxes differ ~200 m
RADIUS_HOUSE_M = 150.0
NAME_STRONG = 0.85         # rapidfuzz token_set_ratio on s15.norm_name, as in s15's proximity pass
NAME_PARTIAL = 0.50
NAME_WHOLE_MIN = 0.50      # token_sort floor: token_set alone scores any word-subset as 1.0
GRADE_RANK = {"A": 4, "B": 3, "C": 2, "D": 1}
OPEN_BONUS = 0.5           # breaks a tie towards the open POI, never lifts a grade over the next
GRADE_MEANING = {
    "G": "geomatch address rule (house number + street + ZIP agree) and the name or phone agrees",
    "P": "unique shared phone number within 2 km (renamed store)",
    "A": "name agrees + same house number, within 250 m",
    "B": "name agrees, within 150 m",
    "C": "name agrees, 150-250 m apart - usually a large store or plaza",
    "D": "same house number, name only partly agrees, within 150 m - verify",
}

STATUS_COMPLETE = "complete"
STATUS_INCOMPLETE = "incomplete"
STATUS_NOT_STARTED = "not started"
STATUS_UNKNOWN_TARGET = "unknown target"
STATUS_NOT_IN_DB = "not in our DB"
STATUSES = [STATUS_COMPLETE, STATUS_INCOMPLETE, STATUS_NOT_STARTED, STATUS_UNKNOWN_TARGET, STATUS_NOT_IN_DB]
MASTER_STATUS = {"complete": STATUS_COMPLETE, "incomplete": STATUS_INCOMPLETE,
                 "not_started": STATUS_NOT_STARTED, "unknown_target": STATUS_UNKNOWN_TARGET}
STATUS_MEANING = {
    STATUS_COMPLETE: "linked to a master store holding at least Google's review count",
    STATUS_INCOMPLETE: "linked to a master store with some reviews, fewer than Google's count",
    STATUS_NOT_STARTED: "linked to a master store with no reviews yet",
    STATUS_UNKNOWN_TARGET: "linked to a master store whose Google review count is unknown",
    STATUS_NOT_IN_DB: "no master store linked to this POI",
}

NO_SUBCATEGORY = "(no subcategory)"
NO_BRAND = "(no brand)"
DECISION_COLUMN = "is_grocery (Y / N / some)"
TOP_N_LISTED = 5

# Top categories that can hold food retail. Anything else gets a file only if SafeGraph
# filed at least MIN_OUR_STORES_FOR_FILE of our master stores under it.
FOOD_TOP_CATEGORIES = frozenset({
    "Grocery Stores", "Specialty Food Stores", "Food and Beverage Stores",
    "Beer, Wine, and Liquor Stores", "Gasoline Stations",
    "General Merchandise Stores, including Warehouse Clubs and Supercenters", "Department Stores",
    "Health and Personal Care Stores", "Other Miscellaneous Store Retailers",
    "Miscellaneous Store Retailers", "Retail Trade",
    "Grocery and Related Product Merchant Wholesalers", "Bakeries and Tortilla Manufacturing",
    "Fruit and Tree Nut Farming", "Vegetable and Melon Farming",
    "Animal Slaughtering and Processing",
})
MIN_OUR_STORES_FOR_FILE = 25

SHEET_MAX = 31
SHEET_BAD = re.compile(r"[\[\]:*?/\\]")
COL_WIDTH_MAX = 40

POI_COLUMNS = {  # parquet column -> catalog column; POLYGON_* and OPEN_HOURS are left out
    "PLACEKEY": "placekey", "LOCATION_NAME": "location_name", "TOP_CATEGORY": "top_category",
    "SUB_CATEGORY": "sub_category", "NAICS_CODE": "naics_code", "CATEGORY_TAGS": "category_tags",
    "STREET_ADDRESS": "street_address", "CITY": "city", "POSTAL_CODE": "postal_code",
    "LATITUDE": "latitude", "LONGITUDE": "longitude", "OPENED_ON": "opened_on",
    "CLOSED_ON": "closed_on", "PHONE_NUMBER": "phone_number", "WEBSITE": "website",
    "PARENT_PLACEKEY": "parent_placekey", "SAFEGRAPH_PLACE_ID": "safegraph_place_id",
    "TOP_CATEGORY_2022": "top_category_2022", "SUB_CATEGORY_2022": "sub_category_2022",
    "NAICS_CODE_2022": "naics_code_2022",
}
OUR_COLUMNS = {  # master column -> catalog column
    "store_id": "our_store_id", "store_name": "our_store_name", "place_id": "our_place_id",
    "full_address": "our_address", "category": "our_google_category",
    "target_reviews": "our_target_reviews", "reviews_in_db": "our_reviews_in_db",
    "reviews_with_text": "our_reviews_with_text",
}
LEAD_COLUMNS = ["placekey", "location_name", "brand_name", "top_category", "sub_category",
                "review_status", "is_closed", "city", "postal_code"]


# --------------------------------------------------------------------------- matching
def grade(name_sim: float, same_house: bool, dist_m: float) -> str | None:
    """Confidence grade for one master-store / POI pair, None when it is not a link."""
    if dist_m > RADIUS_NAME_M:
        return None
    if name_sim >= NAME_STRONG:
        if same_house:
            return "A"
        return "B" if dist_m <= RADIUS_HOUSE_M else "C"
    if same_house and name_sim >= NAME_PARTIAL and dist_m <= RADIUS_HOUSE_M:
        return "D"
    return None


def _norm(name) -> str:
    """s15.norm_name, but a missing name is '' - norm_name turns NaN into the text 'nan'."""
    return s15.norm_name(name) if isinstance(name, str) else ""


def _master_names(master: pd.DataFrame) -> list[list[str]]:
    """Every distinct normalised name a master store goes by (Google, business, Outscraper)."""
    cols = [c for c in ("store_name", "business_name", "outscraper_name") if c in master.columns]
    out = []
    for row in master[cols].itertuples(index=False):
        names = {_norm(v) for v in row}
        out.append(sorted(n for n in names if n))
    return out


def _place_tokens(master: pd.DataFrame) -> list[frozenset[str]]:
    """The store's own town words (+ 'nc'). token_set_ratio scores a name that merely shares
    them ("Wild Market, Maggie Valley" vs "Maggie Valley Spa") as a near-perfect match."""
    cities = master["city"] if "city" in master.columns else pd.Series(None, index=master.index)
    return [frozenset(_norm(c).split()) | {"nc"} for c in cities]


def _strip_place(name: str, place: frozenset[str]) -> str:
    """name without the town words; unchanged when nothing else would be left."""
    kept = " ".join(t for t in name.split() if t not in place)
    return kept or name


def _similarity_normed(ours: str, theirs: str, place: frozenset[str]) -> float:
    from rapidfuzz import fuzz

    if not ours or not theirs:
        return 0.0
    a, b = _strip_place(ours, place), _strip_place(theirs, place)
    whole = fuzz.token_sort_ratio(a, b) / 100.0
    return fuzz.token_set_ratio(a, b) / 100.0 if whole >= NAME_WHOLE_MIN else whole


def name_similarity(ours, theirs, place: frozenset[str]) -> float:
    """0-1 name agreement after normalising and dropping the store's town words.

    token_set_ratio forgives extra words ("Shell" vs "Shell Oil"), but scores any word
    subset as a perfect match ("country" vs "High Country Ceramic Arts"), so it only
    counts when the whole names also broadly agree; otherwise the whole-name score stands.
    """
    return _similarity_normed(_norm(ours), _norm(theirs), place)


def _candidates(master: pd.DataFrame, dewey: pd.DataFrame) -> pd.DataFrame:
    """(master row, dewey row, metres) for every POI within RADIUS_NAME_M of a master store."""
    from sklearn.neighbors import BallTree

    tree = BallTree(np.radians(dewey[["LATITUDE", "LONGITUDE"]].to_numpy(dtype=float)),
                    metric="haversine")
    pts = np.radians(master[["source_lat", "source_lon"]].to_numpy(dtype=float))
    idx, dist = tree.query_radius(pts, r=RADIUS_NAME_M / EARTH_RADIUS_M, return_distance=True)
    lengths = [len(i) for i in idx]
    return pd.DataFrame({
        "m": np.repeat(np.arange(len(master)), lengths),
        "d": np.concatenate(idx).astype(int),
        "dist_m": np.concatenate(dist) * EARTH_RADIUS_M,
    })


def _score_candidates(cand: pd.DataFrame, master: pd.DataFrame, dewey: pd.DataFrame) -> pd.DataFrame:
    names = _master_names(master)
    m_house = master.street.where(master.street.notna(), master.full_address).map(s15._house_number)
    mh = m_house.to_numpy()
    d_rows = np.unique(cand.d.to_numpy())
    d_name = dict(zip(d_rows, dewey.LOCATION_NAME.iloc[d_rows].map(_norm)))
    d_house = dict(zip(d_rows, dewey.STREET_ADDRESS.iloc[d_rows].map(s15._house_number)))
    place = _place_tokens(master)

    sims = [max((_similarity_normed(n, d_name[d], place[m]) for n in names[m]), default=0.0)
            for m, d in zip(cand.m, cand.d)]
    house = [mh[m] is not None and mh[m] == d_house[d] for m, d in zip(cand.m, cand.d)]
    grades = [grade(n, h, x) for n, h, x in zip(sims, house, cand.dist_m)]
    scored = cand.assign(name_sim=sims, same_house=house, match_grade=grades)
    return scored[scored.match_grade.notna()]


def match_master_to_dewey(master: pd.DataFrame, dewey: pd.DataFrame) -> pd.DataFrame:
    """One-to-one links master store -> Dewey POI.

    master needs store_id, store_name (business_name, outscraper_name optional), street,
    full_address, source_lat, source_lon; dewey needs PLACEKEY, LOCATION_NAME,
    STREET_ADDRESS, LATITUDE, LONGITUDE, CLOSED_ON. Returns master_store_id, placekey,
    match_grade, name_sim, dist_m, n_candidates (graded POIs the store had).
    """
    cols = ["master_store_id", "placekey", "match_grade", "name_sim", "dist_m", "n_candidates"]
    master = master.dropna(subset=["source_lat", "source_lon"]).reset_index(drop=True)
    dewey = dewey.dropna(subset=["LATITUDE", "LONGITUDE"]).reset_index(drop=True)
    if master.empty or dewey.empty:
        return pd.DataFrame(columns=cols)
    graded = _score_candidates(_candidates(master, dewey), master, dewey)
    if graded.empty:
        return pd.DataFrame(columns=cols)
    is_open = dewey.CLOSED_ON.isna().to_numpy()
    pairs = pd.DataFrame({                     # Dewey on the left: alternatives are counted per POI
        "id_left": dewey.PLACEKEY.to_numpy()[graded.d],
        "id_right": master.store_id.to_numpy()[graded.m],
        "score": graded.match_grade.map(GRADE_RANK).to_numpy() + OPEN_BONUS * is_open[graded.d],
        "decision": "match", "why": graded.match_grade.to_numpy(),
        "name_sim": graded.name_sim.to_numpy(), "dist_km": graded.dist_m.to_numpy() / 1000.0,
    })
    links = s15.assign_one_to_one(pairs)
    return pd.DataFrame({
        "master_store_id": links.id_right, "placekey": links.id_left, "match_grade": links.why,
        "name_sim": links.name_sim.astype(float).round(2),
        "dist_m": (links.dist_km.astype(float) * 1000).round(0),
        "n_candidates": links.n_alternatives.astype(int),
    })[cols]


LINK_COLUMNS = ["master_store_id", "placekey", "match_grade", "name_sim", "dist_m", "n_candidates"]
GEOMATCH_CACHE = HERE / "build" / "s17_geomatch_dewey_master.csv"
# geomatch is slow, so its pass runs on the food-related POIs; the proximity and phone
# passes that follow still search every POI.
GEOMATCH_TOPS = FOOD_TOP_CATEGORIES | {"Restaurants and Other Eating Places"}
PHONE_RADIUS_M = 2000.0     # a unique shared phone links a renamed store this far apart


def _phone10(value) -> str | None:
    """'+1 (919) 555-0101' -> '9195550101'; anything without ten digits -> None."""
    if not isinstance(value, str):
        return None
    digits = re.sub(r"\D", "", value)
    return digits[-10:] if len(digits) >= 10 else None


def _phones(df: pd.DataFrame, col: str, key: str) -> dict:
    if col not in df.columns:
        return {}
    return {k: p for k, p in zip(df[key], df[col].map(_phone10)) if p}


def _screen_geomatch(geo: pd.DataFrame, dewey: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    """Keep a geomatch pair only when the names agree or the phones agree.

    geomatch's rule is an address rule, so it pairs any two businesses at one address: the
    audit found our Harris Teeter on its pharmacy's POI and an Ingles on a Dollar Tree. The
    kept pairs carry our name similarity, so the assignment then prefers the right tenant.
    """
    if geo.empty:
        return geo
    d_name = dict(zip(dewey.PLACEKEY, dewey.LOCATION_NAME.map(_norm)))
    m_names = dict(zip(master.store_id, _master_names(master)))
    m_place = dict(zip(master.store_id, _place_tokens(master)))
    d_phone, m_phone = _phones(dewey, "PHONE_NUMBER", "PLACEKEY"), _phones(master, "phone", "store_id")
    sims, keep = [], []
    for pk, sid in zip(geo.id_left, pd.to_numeric(geo.id_right)):
        sid = int(sid)
        sim = max((_similarity_normed(n, d_name.get(pk, ""), m_place.get(sid, frozenset()))
                   for n in m_names.get(sid, [])), default=0.0)
        same_phone = d_phone.get(pk) is not None and d_phone.get(pk) == m_phone.get(sid)
        sims.append(sim)
        keep.append(sim >= NAME_PARTIAL or same_phone)
    return geo.assign(name_sim=sims)[keep]


def _phone_links(dewey: pd.DataFrame, master: pd.DataFrame, taken_pk: set, taken_sid: set) -> pd.DataFrame:
    """Grade P: a phone number unique on both sides, shared by an unlinked POI and an
    unlinked store within PHONE_RADIUS_M. Catches renamed stores the name tests miss."""
    d_phone, m_phone = _phones(dewey, "PHONE_NUMBER", "PLACEKEY"), _phones(master, "phone", "store_id")
    d_count = pd.Series(list(d_phone.values())).value_counts()
    m_count = pd.Series(list(m_phone.values())).value_counts()
    by_phone = {p: sid for sid, p in m_phone.items() if m_count[p] == 1 and sid not in taken_sid}
    d = dewey.set_index("PLACEKEY")
    m = master.set_index("store_id")
    m_names = dict(zip(master.store_id, _master_names(master)))
    rows = []
    for pk, p in d_phone.items():
        if pk in taken_pk or d_count[p] != 1 or p not in by_phone:
            continue
        sid = by_phone[p]
        dist = float(s15._haversine_km(d.at[pk, "LATITUDE"], d.at[pk, "LONGITUDE"],
                                       m.at[sid, "source_lat"], m.at[sid, "source_lon"])) * 1000
        if dist <= PHONE_RADIUS_M:
            sim = max((_similarity_normed(n, _norm(d.at[pk, "LOCATION_NAME"]), frozenset())
                       for n in m_names.get(sid, [])), default=0.0)
            rows.append({"master_store_id": sid, "placekey": pk, "match_grade": "P",
                         "name_sim": round(sim, 2), "dist_m": round(dist), "n_candidates": 1})
    return pd.DataFrame(rows, columns=LINK_COLUMNS)


def link_dewey_to_master(dewey: pd.DataFrame, master: pd.DataFrame, geo_pairs: pd.DataFrame) -> pd.DataFrame:
    """Dewey leads: for each POI, the store of ours it is, if any. Three passes, each on
    what the previous left: geomatch (G, screened on name or phone), proximity (A-D),
    unique shared phone (P). One-to-one throughout.

    geo_pairs is geomatch output in s15's pair format with Dewey on the left
    (id_left = PLACEKEY, id_right = our store_id).
    """
    geo = s15.assign_one_to_one(_screen_geomatch(geo_pairs[geo_pairs.decision == "match"]
                                                 if len(geo_pairs) else geo_pairs, dewey, master))
    first = pd.DataFrame({
        "master_store_id": pd.to_numeric(geo.id_right).astype("Int64"), "placekey": geo.id_left.astype(object),
        "match_grade": "G", "name_sim": pd.to_numeric(geo.name_sim, errors="coerce").round(2),
        "dist_m": (pd.to_numeric(geo.dist_km, errors="coerce") * 1000).round(0),
        "n_candidates": pd.to_numeric(geo.n_alternatives).astype("Int64"),
    })
    taken_sid, taken_pk = set(first.master_store_id), set(first.placekey)
    second = match_master_to_dewey(master[~master.store_id.isin(taken_sid)], dewey[~dewey.PLACEKEY.isin(taken_pk)])
    taken_sid |= set(second.master_store_id)
    taken_pk |= set(second.placekey)
    third = _phone_links(dewey, master, taken_pk, taken_sid)
    parts = [p for p in (first, second, third) if len(p)]
    both = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=LINK_COLUMNS)
    return both[LINK_COLUMNS].reset_index(drop=True)


def geomatch_pairs(master: pd.DataFrame, dewey: pd.DataFrame, run: bool) -> pd.DataFrame:
    """geomatch with Dewey on the left: food-related POIs against our stores, blocked by
    ZIP3 (cached). run=False reuses GEOMATCH_CACHE and returns no pairs when there is none."""
    empty = pd.DataFrame(columns=s15.PAIR_COLUMNS)
    if not run:
        if GEOMATCH_CACHE.exists():
            return pd.read_csv(GEOMATCH_CACHE, low_memory=False)
        print("  no geomatch cache - proximity and phone passes only (run with --geomatch)", flush=True)
        return empty
    left = s15._prep_dewey(dewey[dewey.TOP_CATEGORY.isin(GEOMATCH_TOPS)])
    right = master[["store_id", "store_name", "street", "city", "zip", "source_lat", "source_lon"]].rename(
        columns={"store_name": "name", "source_lat": "lat", "source_lon": "lon"}).copy()
    right["addr"] = s15.build_addr(right, "street", "city", "zip")
    right["z3"] = right.zip.map(s15.zip3)
    right["_id"] = right.store_id
    print(f"  geomatch: {len(left):,} food-related Dewey POIs x {len(right):,} of our stores, blocked by ZIP3",
          flush=True)
    pairs = s15._link_blocked(left, right.dropna(subset=["lat", "lon"]), "dewey->ours")
    GEOMATCH_CACHE.parent.mkdir(parents=True, exist_ok=True)
    pairs.to_csv(GEOMATCH_CACHE, index=False)
    return pairs


def review_status(master_status: pd.Series) -> pd.Series:
    """Master scrape_status -> catalog label; no linked store -> 'not in our DB'."""
    return master_status.map(MASTER_STATUS).fillna(STATUS_NOT_IN_DB)


# --------------------------------------------------------------------------- categories
def _top_values(df: pd.DataFrame, keys: list[str], col: str, skip: str | None = None) -> pd.Series:
    """'Food Lion (812); ALDI (95)' - the TOP_N_LISTED commonest values of col per group."""
    src = df if skip is None else df[df[col] != skip]
    counts = src.groupby(keys + [col]).size().rename("n").reset_index()
    counts = counts.sort_values(keys + ["n"], ascending=[True] * len(keys) + [False])
    counts = counts.groupby(keys).head(TOP_N_LISTED)
    counts["txt"] = counts[col].astype(str) + " (" + counts.n.astype(str) + ")"
    return counts.groupby(keys)["txt"].agg("; ".join)


def category_summary(catalog: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """One row per value of keys: POI counts, NAICS codes, brands, names, our status mix."""
    work = catalog.assign(**{k: catalog[k].fillna(NO_SUBCATEGORY if k == "sub_category" else "(none)")
                             for k in keys})
    g = work.groupby(keys)
    closed = g.is_closed.sum().astype(int)
    out = pd.DataFrame({
        "pois": g.size(), "open_pois": g.size() - closed, "closed_pois": closed,
        "naics_codes": g.naics_code.agg(lambda s: ", ".join(sorted(set(s.dropna().astype(str))))),
    })
    status = work.groupby(keys + ["review_status"]).size().unstack(fill_value=0)
    status = status.reindex(columns=STATUSES, fill_value=0)
    out["in_our_db"] = status.drop(columns=STATUS_NOT_IN_DB).sum(axis=1)
    out = out.join(status)
    out["top_brands"] = _top_values(work, keys, "brand_name", skip=NO_BRAND)
    out["common_names"] = _top_values(work, keys, "location_name")
    out = out.fillna({"top_brands": "", "common_names": ""}).reset_index()
    out[DECISION_COLUMN] = ""
    out["notes"] = ""
    return out.sort_values("pois", ascending=False).reset_index(drop=True)


def food_relevant(tops: pd.DataFrame) -> pd.DataFrame:
    """Adds gets_file: 'food list', 'holds our stores' or '' per top category."""
    fixed = tops.top_category.isin(FOOD_TOP_CATEGORIES)
    ours = tops.in_our_db >= MIN_OUR_STORES_FOR_FILE
    label = np.where(fixed, "food list", np.where(ours, "holds our stores", ""))
    return tops.assign(gets_file=label)


# --------------------------------------------------------------------------- names
def sheet_name(name: str, used: set[str]) -> str:
    """Excel-safe, <=31 chars, unique (case-insensitively) against used; records the result."""
    base = SHEET_BAD.sub("-", str(name)).strip().strip("'") or "blank"
    candidate, n = base[:SHEET_MAX], 1
    while candidate.lower() in used:
        n += 1
        suffix = f" ({n})"
        candidate = base[:SHEET_MAX - len(suffix)] + suffix
    used.add(candidate.lower())
    return candidate


def file_slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", str(name)).strip("_")


# --------------------------------------------------------------------------- assembly
def _tags(raw) -> str:
    try:
        return "; ".join(json.loads(raw)) if isinstance(raw, str) else ""
    except (ValueError, TypeError):
        return str(raw)


def load_dewey() -> pd.DataFrame:
    if not DEWEY_PARQUET.exists():
        sys.exit(f"missing input: {DEWEY_PARQUET}")
    d = pd.read_parquet(DEWEY_PARQUET, columns=list(POI_COLUMNS) + ["BRANDS"])
    s15.require_unique(d, "PLACEKEY", DEWEY_PARQUET.name)
    return d


def build_catalog(dewey: pd.DataFrame, master: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    """One row per POI: Dewey columns, brand, closed flag, our store and its review status."""
    s15.require_unique(master, "store_id", "master stores")
    cat = dewey[list(POI_COLUMNS)].rename(columns=POI_COLUMNS)
    cat["brand_name"] = s16.brand_name(dewey.BRANDS).to_numpy()
    cat["category_tags"] = cat.category_tags.map(_tags)
    cat["is_closed"] = cat.closed_on.notna().astype(int)
    for c in ("opened_on", "closed_on"):
        cat[c] = cat[c].map(lambda v: None if pd.isna(v) else str(v))
    ours = master[list(OUR_COLUMNS) + ["scrape_status"]].rename(columns=OUR_COLUMNS)
    linked = links.rename(columns={"master_store_id": "our_store_id"}).merge(ours, on="our_store_id")
    cat = cat.merge(linked, on="placekey", how="left")
    cat["review_status"] = review_status(cat.pop("scrape_status"))
    cat = cat.rename(columns={"name_sim": "match_name_sim", "dist_m": "match_dist_m",
                              "n_candidates": "match_candidates"})
    return cat[LEAD_COLUMNS + [c for c in cat.columns if c not in LEAD_COLUMNS]]


def build_master_audit(master: pd.DataFrame, catalog: pd.DataFrame) -> pd.DataFrame:
    """Every master store with the POI it linked to, so unlinked stores are visible."""
    keep = ["store_id", "store_name", "full_address", "city", "zip", "category", "scrape_status",
            "target_reviews", "reviews_in_db", "source_lat", "source_lon"]
    poi = catalog[catalog.our_store_id.notna()][[
        "our_store_id", "placekey", "location_name", "street_address", "top_category",
        "sub_category", "is_closed", "match_grade", "match_name_sim", "match_dist_m",
        "match_candidates"]].rename(columns={
            "our_store_id": "store_id", "location_name": "dewey_name",
            "street_address": "dewey_address", "top_category": "dewey_top_category",
            "sub_category": "dewey_sub_category", "is_closed": "dewey_closed"})
    out = master[keep].merge(poi, on="store_id", how="left")
    out.insert(1, "linked", out.placekey.notna().map({True: "yes", False: "no"}))
    return out.sort_values(["linked", "store_id"], ascending=[False, True]).reset_index(drop=True)


# --------------------------------------------------------------------------- writing
def _cells(df: pd.DataFrame) -> list[list]:
    """Rows with NaN/NaT as None (xlsxwriter rejects NaN) and numpy scalars as Python ones."""
    obj = df.astype(object).where(df.notna(), None)
    return obj.values.tolist()


def write_sheet(wb, name: str, df: pd.DataFrame, used: set[str]) -> None:
    """Streams df into a new sheet: bold frozen header, autofilter, capped column widths."""
    ws = wb.add_worksheet(sheet_name(name, used))
    bold = wb.add_format({"bold": True, "bg_color": "#DDEBF7"})
    ws.write_row(0, 0, list(df.columns), bold)
    for c, col in enumerate(df.columns):
        sample = df[col].head(200).astype(str).str.len().max() if len(df) else 0
        ws.set_column(c, c, min(max(len(str(col)) + 2, int(sample or 0) + 2, 8), COL_WIDTH_MAX))
    for r, row in enumerate(_cells(df), start=1):
        ws.write_row(r, 0, row)
    ws.freeze_panes(1, 2)
    ws.autofilter(0, 0, max(len(df), 1), max(len(df.columns) - 1, 0))


def write_workbook(path: Path, sheets: list[tuple[str, pd.DataFrame]]) -> None:
    import xlsxwriter

    path.parent.mkdir(parents=True, exist_ok=True)
    wb = xlsxwriter.Workbook(str(path), {"constant_memory": True, "strings_to_urls": False,
                                         "strings_to_numbers": False})
    used: set[str] = set()
    try:
        for name, df in sheets:
            write_sheet(wb, name, df, used)
    finally:
        wb.close()


def _pairs(rows: dict[str, str] | list[tuple[str, str]], heading: str) -> list[tuple[str, str]]:
    items = rows.items() if isinstance(rows, dict) else rows
    return [("", ""), (heading, "")] + [(str(k), str(v)) for k, v in items]


def notes(catalog: pd.DataFrame, links: pd.DataFrame, n_master: int) -> pd.DataFrame:
    head = [
        ("Built", str(date.today())),
        ("Source", "SafeGraph Global Places 2026-08 via Dewey Data, all NC POIs "
                   "(dewy data/nc_all_pois.parquet, extracted 2026-09-16)"),
        ("POIs", f"{len(catalog):,}"),
        ("Master stores linked", f"{len(links):,} of {n_master:,} "
                                 "(Master_to_Dewey_Match.xlsx lists the rest)"),
        ("is_closed", "1 when SafeGraph set CLOSED_ON; the extract's operational_status is wrong "
                      "and is not used"),
        ("Link", f"master store -> POI within {RADIUS_NAME_M:.0f} m on name (token-set >= "
                 f"{NAME_STRONG:.2f}) and house number, one-to-one best-first, open POI preferred"),
        ("Regenerate", "python Frame/s17_dewey_catalog.py"),
    ]
    grades = links.match_grade.value_counts().reindex(list(GRADE_MEANING), fill_value=0)
    grade_rows = [(f"{g} ({int(grades[g]):,} links)", m) for g, m in GRADE_MEANING.items()]
    rows = head + _pairs(STATUS_MEANING, "REVIEW STATUS") + _pairs(grade_rows, "MATCH GRADE")
    return pd.DataFrame(rows, columns=["item", "detail"])


def status_summary(catalog: pd.DataFrame) -> pd.DataFrame:
    s = catalog.review_status.value_counts().reindex(STATUSES, fill_value=0)
    return pd.DataFrame({"review_status": s.index, "pois": s.values,
                         "open_pois": [int(((catalog.review_status == k) & (catalog.is_closed == 0)).sum())
                                       for k in s.index]})


def write_category_files(catalog: pd.DataFrame, tops: pd.DataFrame, subs: pd.DataFrame) -> int:
    """One workbook per food-relevant top category; returns how many were written."""
    folder = OUT_DIR / "by_category"
    folder.mkdir(parents=True, exist_ok=True)
    for old in folder.glob("*.xlsx"):
        old.unlink()
    chosen = tops[tops.gets_file != ""].reset_index(drop=True)
    for i, top in enumerate(chosen.top_category, start=1):
        rows = catalog[catalog.top_category == top]
        mine = subs[subs.top_category == top]
        sheets = [("Summary", mine), ("All", rows)]
        for sub in mine.sub_category:
            part = rows[rows.sub_category.fillna(NO_SUBCATEGORY) == sub]
            sheets.append((sub, part))
        write_workbook(folder / f"{i:02d}_{file_slug(top)}.xlsx", sheets)
        print(f"  by_category/{i:02d}_{file_slug(top)}.xlsx  {len(rows):,} POIs, {len(mine)} subcategories",
              flush=True)
    return len(chosen)


def main(argv: list[str] | None = None) -> int:
    run_geomatch = "--geomatch" in (sys.argv[1:] if argv is None else argv)
    print("loading Dewey + master ...", flush=True)
    dewey = load_dewey()
    master = s15.load_master()
    print(f"  {len(dewey):,} POIs, {len(master):,} master stores; matching ...", flush=True)
    links = link_dewey_to_master(dewey, master, geomatch_pairs(master, dewey, run_geomatch))
    catalog = build_catalog(dewey, master, links)
    audit = build_master_audit(master, catalog)
    tops = food_relevant(category_summary(catalog, ["top_category"]))
    subs = category_summary(catalog, ["top_category", "sub_category"])
    subs = subs.merge(tops[["top_category", "gets_file"]], on="top_category", how="left")
    print(f"  {len(links):,} links; writing workbooks to {OUT_DIR} ...", flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    catalog.to_parquet(CATALOG_PARQUET, index=False)   # s18 reads this; no re-match needed

    write_workbook(OUT_DIR / "Dewey_Categories.xlsx", [
        ("Top_Categories", tops), ("Sub_Categories", subs), ("Notes", notes(catalog, links, len(master)))])
    write_workbook(OUT_DIR / "Master_to_Dewey_Match.xlsx", [
        ("Master_Stores", audit),
        ("Grade_x_Status", pd.crosstab(audit.match_grade.fillna("not linked"), audit.scrape_status.fillna("(no status)"),
                                       margins=True, margins_name="TOTAL").reset_index())])
    n_files = write_category_files(catalog, tops, subs)
    write_workbook(OUT_DIR / "NC_Dewey_All_POIs.xlsx", [
        ("All_POIs", catalog), ("Status_Summary", status_summary(catalog)),
        ("Notes", notes(catalog, links, len(master)))])

    print(f"\n{len(catalog):,} POIs | {len(tops)} top categories | {len(subs)} top/sub pairs | "
          f"{n_files} category files")
    print(status_summary(catalog).to_string(index=False))
    print(links.match_grade.value_counts().sort_index().to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
