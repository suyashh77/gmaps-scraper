"""
s18_poi_rulings.py - turn the Dewey catalog (s17) into a short list of decisions, and turn
the decisions back into the final POI list.

  prep    -> dewy data/catalog/ruling_page_data.json + ruling_page_pois.json
          Food-relevant POIs are split into chains (a store name with more than five POIs,
          CHAIN_MIN_POIS) and independents. Each subcategory's independents, each chain, and
          each non-food top category become one item, bucketed:
            maybe      needs a look: contested or suggested-in (nothing is in until approved)
            ruled out  suggested out (the 9 Sep 2026 scope decision, STATUS.md); stays out
                       unless flipped
          ruling_page_pois.json holds the full POI list behind every maybe item.
  apply   <docs.json> the ruling page's saved documents -> Final_POI_List.xlsx +
          final_poi_list.parquet

Rulings: {"kind": "poi" | "top" | "sub" | "chain", "key": ..., "ruling": "Y" | "N" | "M" | null}. Per POI:
a ruling on the store itself wins, then a top-category ruling; else a chain POI follows its
chain, an independent its subcategory; an explicit M (maybe) leaves it undecided; with no explicit ruling a suggested-out item is out and everything else is
undecided. Nothing is ever in without an explicit Y. Undecided POIs are listed, never included.

Usage
  python Frame/s17_dewey_catalog.py          # once, builds catalog/nc_dewey_catalog.parquet
  python Frame/s18_poi_rulings.py prep
  python Frame/s18_poi_rulings.py apply <docs.json>
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import NamedTuple

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import s17_dewey_catalog as s17  # noqa: E402

PAGE_DATA = s17.OUT_DIR / "ruling_page_data.json"
PAGE_POIS = s17.OUT_DIR / "ruling_page_pois.json"
FINAL_XLSX = s17.OUT_DIR / "Final_POI_List.xlsx"
FINAL_PARQUET = s17.OUT_DIR / "final_poi_list.parquet"

YES, NO, MAYBE = "Y", "N", "MAYBE"
MAYBE_RULING = "M"               # an explicit "still maybe": reopens a ruled-out item
RULINGS = frozenset({YES, NO, MAYBE_RULING})
KINDS = frozenset({"poi", "top", "sub", "chain"})
BUCKET_MAYBE, BUCKET_OUT = "maybe", "out"
CHAIN_MIN_POIS = 6                 # "more than five" stores under one name
SEP = " || "
FOOD_TOPS = s17.FOOD_TOP_CATEGORIES | {"Restaurants and Other Eating Places"}

SCOPE = "9 Sep scope decision"
EXACT = {  # NAICS code -> (suggestion, reason)
    **dict.fromkeys(["445110", "44511", "4451", "445"], (YES, "Full-line grocery - core of the study")),
    "452311": (YES, "Supercenters and warehouse clubs - in the agreed store types"),
    "445210": (YES, "Butcher / meat market - in the agreed store types"),
    **dict.fromkeys(["445220", "44522"], (YES, "Seafood market - in the agreed store types")),
    **dict.fromkeys(["445120", "44512"], (MAYBE, f"Convenience stores: the {SCOPE} left these to be "
                                                 "decided explicitly")),
    **dict.fromkeys(["447110", "44711", "447", "4471"], (
        MAYBE, f"Gas stations with a store: STATUS.md lists gas stations out ({SCOPE}), but s10 kept "
               "gas_convenience and master already holds them")),
    **dict.fromkeys(["4452", "445299"], (MAYBE, "Mixed bag: ethnic grocers sit next to snow-cone and "
                                                "jerky stands - look at the names")),
    **dict.fromkeys(["45231", "4523"], (MAYBE, "General merchandise with no finer code - look at the names")),
    "452319": (MAYBE, "Dollar and discount stores - dollar stores are in the agreed store types, "
                      "other discounters are not"),
    "445230": (NO, f"Produce markets / stands - ruled out in the {SCOPE}"),
    **dict.fromkeys(["445291", "311811", "311812"], (NO, f"Bakeries - listed out in the {SCOPE}")),
    "445292": (NO, "Candy and nut shops - not grocery"),
    **dict.fromkeys(["445310", "44531", "4453"], (NO, "Beer, wine and liquor - not food access")),
    "446110": (NO, "Pharmacies - not in the agreed store types"),
    "446191": (NO, "Supplement stores (GNC) - not food"),
    "452210": (NO, "Department stores (Ross, TJ Maxx) - supercenters are 452311"),
    **dict.fromkeys(["447190", "44719"], (NO, "Fuel only / EV chargers - no store")),
    "44": (NO, "Retail with no finer code - no evidence it sells food"),
}
PREFIX = [  # checked when no exact code matches
    ("722", NO, f"Prepared food service - ruled out in the {SCOPE}"),
    ("424", NO, "Wholesale, not retail"),
    ("111", NO, f"Farm-direct - ruled out in the {SCOPE}"),
    ("112", NO, f"Farm-direct - ruled out in the {SCOPE}"),
    ("311", NO, "Food manufacturing, not retail"),
    ("446", NO, "Health and personal care - not food"),
    ("453", NO, "Miscellaneous retail - not food"),
]
DEFAULT = (NO, "Not food retail")

METRICS = ["pois", "open_pois", "ours", "complete", "incomplete", "not_started", "unknown_target",
           "reviews", "ours_on_closed_pois"]
LIST_COLUMNS = ["placekey", "location_name", "brand_name", "street_address", "city", "postal_code", "top_category",
                "sub_category", "category_tags", "review_status", "our_reviews_in_db", "is_closed"]
STORE_NUMBER = re.compile(r"#\s*\d+|\b\d{2,6}\b")   # "#3", "1234"; keeps the 7 of 7-Eleven
NAME_NOISE = frozenset({"store", "stores", "inc", "llc", "co", "no"})


# --------------------------------------------------------------------------- keys + suggestions
def sub_key(top: str, sub) -> str:
    sub = sub if isinstance(sub, str) and sub else s17.NO_SUBCATEGORY
    return f"{top}{SEP}{sub}"


def _codes(naics_codes: str) -> list[str]:
    return [c.strip() for c in str(naics_codes or "").split(",") if c.strip()]


def suggest(naics_codes: str) -> tuple[str, str]:
    """(suggestion, reason) for a category from its NAICS code(s), e.g. '44511, 445110'."""
    codes = _codes(naics_codes)
    for code in codes:
        if code in EXACT:
            return EXACT[code]
    for code in codes:
        for prefix, ruling, reason in PREFIX:
            if code.startswith(prefix):
                return ruling, reason
    return DEFAULT


def chain_key(name) -> str:
    """'Food Lion #1234' and 'FOOD LION' -> 'food lion'. Store numbers and filler words go."""
    if not isinstance(name, str):
        return ""
    text = re.sub(r"[^\w\s]", " ", STORE_NUMBER.sub(" ", name.lower()))
    return " ".join(t for t in text.split() if t not in NAME_NOISE)


def chain_ids(pois: pd.DataFrame) -> pd.Series:
    """Chain key per POI when its name has CHAIN_MIN_POIS or more POIs in pois, else None."""
    keys = pois.location_name.map(chain_key)
    counts = keys[keys != ""].value_counts()
    chains = set(counts[counts >= CHAIN_MIN_POIS].index)
    return keys.where(keys.isin(chains), None)


# --------------------------------------------------------------------------- shared shaping
def _food_tops(catalog: pd.DataFrame) -> set[str]:
    ours = catalog[catalog.our_store_id.notna()].groupby("top_category").size()
    held = set(ours[ours >= s17.MIN_OUR_STORES_FOR_FILE].index)
    return (FOOD_TOPS | held) & set(catalog.top_category.dropna())


def _shape(catalog: pd.DataFrame) -> pd.DataFrame:
    """catalog + is_food, sub_key, chain (food POIs only), sub_suggestion."""
    df = catalog.assign(is_food=catalog.top_category.isin(_food_tops(catalog)))
    df["sub_key"] = [sub_key(t, s) for t, s in zip(df.top_category, df.sub_category)]
    df["chain"] = None
    df.loc[df.is_food, "chain"] = chain_ids(df[df.is_food])
    codes = df[df.is_food].groupby("sub_key").naics_code.agg(
        lambda s: ", ".join(sorted(set(s.dropna().astype(str)))))
    sub_sugg = {k: suggest(c) for k, c in codes.items()}
    df["sub_suggestion"] = df.sub_key.map(lambda k: sub_sugg.get(k, (None,))[0])
    df["sub_reason"] = df.sub_key.map(lambda k: sub_sugg.get(k, (None, ""))[1])
    df["sub_naics"] = df.sub_key.map(codes)
    return df


def _chain_suggestion(sub_suggestions: pd.Series) -> str:
    """A chain is out by default only when every POI of it sits in a suggested-out category."""
    values = set(sub_suggestions.dropna())
    if values == {NO}:
        return NO
    return YES if values == {YES} else MAYBE


def _metrics(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """METRICS per group. Our stores, statuses and reviews count open POIs only; a store of
    ours on a closed POI is counted separately so it is never silently lost."""
    is_open = df.is_closed == 0
    ours = df.our_store_id.notna()
    status = df.review_status
    work = pd.DataFrame({
        **{k: df[k] for k in keys},
        "pois": 1, "open_pois": is_open.astype(int), "ours": (ours & is_open).astype(int),
        "complete": ((status == s17.STATUS_COMPLETE) & is_open).astype(int),
        "incomplete": ((status == s17.STATUS_INCOMPLETE) & is_open).astype(int),
        "not_started": ((status == s17.STATUS_NOT_STARTED) & is_open).astype(int),
        "unknown_target": ((status == s17.STATUS_UNKNOWN_TARGET) & is_open).astype(int),
        "reviews": pd.to_numeric(df.our_reviews_in_db, errors="coerce").fillna(0).where(is_open, 0),
        "ours_on_closed_pois": (ours & ~is_open).astype(int),
    })
    return work.groupby(keys, as_index=False, dropna=False)[METRICS].sum().astype({m: int for m in METRICS})


def _top_counts(values: pd.Series, n: int = 6) -> str:
    counts = values.fillna("(none)").value_counts().head(n)
    return "; ".join(f"{k} ({v})" for k, v in counts.items())


# --------------------------------------------------------------------------- page data
def _records(df: pd.DataFrame) -> list[dict]:
    return json.loads(df.to_json(orient="records"))   # numpy scalars -> plain JSON types


def _subs(df: pd.DataFrame) -> pd.DataFrame:
    indep = df[df.is_food & df.chain.isna()]
    subs = _metrics(indep.assign(sub_category=indep.sub_category.fillna(s17.NO_SUBCATEGORY)),
                    ["top_category", "sub_category", "sub_key"])
    first = indep.drop_duplicates("sub_key").set_index("sub_key")
    subs["naics_codes"] = subs.sub_key.map(first.sub_naics)
    subs["suggestion"] = subs.sub_key.map(first.sub_suggestion)
    subs["reason"] = subs.sub_key.map(first.sub_reason)
    subs["common_names"] = subs.sub_key.map(indep.groupby("sub_key").location_name.agg(_top_counts))
    return subs


def _chains(df: pd.DataFrame) -> pd.DataFrame:
    ch = df[df.chain.notna()]
    out = _metrics(ch, ["chain"])
    g = ch.groupby("chain")
    out["name"] = out.chain.map(g.location_name.agg(lambda s: s.value_counts().index[0]))
    out["categories"] = out.chain.map(g.sub_category.agg(_top_counts))
    out["suggestion"] = out.chain.map(g.sub_suggestion.agg(_chain_suggestion))
    out["reason"] = [
        "Every store sits in a suggested-out category" if s == NO else
        "Every store sits in a suggested-in category - approve to include" if s == YES else
        "Sits in a contested category" for s in out.suggestion]
    return out


def page_data(catalog: pd.DataFrame) -> dict:
    df = _shape(catalog)
    subs, chains = _subs(df), _chains(df)
    auto = _metrics(df[~df.is_food], ["top_category"])
    first = df[~df.is_food].groupby("top_category")
    auto["common_names"] = auto.top_category.map(first.location_name.agg(_top_counts))
    auto["reason"] = auto.top_category.map(first.naics_code.agg(
        lambda s: suggest(", ".join(sorted(set(s.dropna().astype(str)))))[1]))
    auto["suggestion"] = NO
    for t in (subs, chains, auto):
        t["bucket"] = [BUCKET_OUT if s == NO else BUCKET_MAYBE for s in t.suggestion]
    return {"built": str(date.today()), "total_pois": int(len(catalog)), "chain_min_pois": CHAIN_MIN_POIS,
            "subs": _records(subs.sort_values("open_pois", ascending=False)),
            "chains": _records(chains.sort_values("open_pois", ascending=False)),
            "auto_no": _records(auto.sort_values(["ours", "pois"], ascending=False))}


def poi_lists(catalog: pd.DataFrame) -> dict:
    """The full POI list behind every maybe item: {"sub:<key>" | "chain:<key>": {cols, rows}}."""
    df = _shape(catalog)
    df = df[df.is_food]
    chain_sugg = df[df.chain.notna()].groupby("chain").sub_suggestion.agg(_chain_suggestion)
    out: dict[str, dict] = {}
    groups = [("sub:", df[df.chain.isna() & (df.sub_suggestion != NO)].groupby("sub_key")),
              ("chain:", df[df.chain.isin(set(chain_sugg[chain_sugg != NO].index))].groupby("chain"))]
    for prefix, grouped in groups:
        for key, rows in grouped:
            rows = rows.sort_values(["city", "location_name"])[LIST_COLUMNS]
            out[prefix + key] = {"cols": LIST_COLUMNS, "rows": json.loads(rows.to_json(orient="values"))}
    return out


# --------------------------------------------------------------------------- apply
class Selection(NamedTuple):
    final: pd.DataFrame       # open POIs ruled in
    closed: pd.DataFrame      # POIs ruled in whose SafeGraph record is closed
    undecided: pd.DataFrame   # POIs no explicit ruling or suggested-out default reached


def _latest(rulings: list[dict]) -> dict[tuple[str, str], str]:
    """(kind, key) -> ruling, later records winning; a null ruling clears; rejects bad values."""
    out: dict[tuple[str, str], str] = {}
    for r in rulings:
        kind, key, ruling = r.get("kind"), r.get("key"), r.get("ruling")
        if kind not in KINDS or not isinstance(key, str) or (ruling is not None and ruling not in RULINGS):
            raise ValueError(f"bad ruling record: {r!r}")
        if ruling is None:
            out.pop((kind, key), None)
        else:
            out[(kind, key)] = ruling
    return out


def apply_rulings(catalog: pd.DataFrame, rulings: list[dict]) -> Selection:
    ruled = _latest(rulings)
    df = _shape(catalog)
    chain_sugg = df[df.chain.notna()].groupby("chain").sub_suggestion.agg(_chain_suggestion).to_dict()

    def decide(pk: str, top: str, food: bool, sk: str, chain, sub_sugg) -> str | None:
        for key in (("poi", pk), ("top", top)):
            if key in ruled:
                return ruled[key]
        if not food:
            return NO
        if isinstance(chain, str):
            return ruled.get(("chain", chain), NO if chain_sugg.get(chain) == NO else None)
        return ruled.get(("sub", sk), NO if sub_sugg == NO else None)

    decision = pd.Series([decide(*v) for v in zip(df.placekey, df.top_category, df.is_food, df.sub_key,
                                                  df.chain, df.sub_suggestion)], index=df.index)
    decision = decision.where(decision != MAYBE_RULING, None)      # an explicit maybe is undecided
    ruled_in = decision == YES
    return Selection(
        final=catalog[ruled_in & (df.is_closed == 0)].reset_index(drop=True),
        closed=catalog[ruled_in & (df.is_closed != 0)].reset_index(drop=True),
        undecided=catalog[decision.isna()].reset_index(drop=True),
    )


# --------------------------------------------------------------------------- commands
def _load_catalog() -> pd.DataFrame:
    if not s17.CATALOG_PARQUET.exists():
        sys.exit(f"missing {s17.CATALOG_PARQUET} - run python Frame/s17_dewey_catalog.py first")
    return pd.read_parquet(s17.CATALOG_PARQUET)


def cmd_prep() -> int:
    catalog = _load_catalog()
    data = page_data(catalog)
    PAGE_DATA.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    lists = poi_lists(catalog)
    PAGE_POIS.write_text(json.dumps(lists, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    for name in ("subs", "chains", "auto_no"):
        buckets = pd.Series([r["bucket"] for r in data[name]]).value_counts().to_dict()
        print(f"{name:8s} {len(data[name]):5d}  {buckets}")
    print(f"{len(lists)} POI lists, {sum(len(v['rows']) for v in lists.values()):,} rows -> {PAGE_POIS}")
    return 0


def cmd_apply(path: Path) -> int:
    rulings = json.loads(path.read_text(encoding="utf-8"))
    sel = apply_rulings(_load_catalog(), rulings)
    summary = s17.category_summary(sel.final, ["top_category", "sub_category"]).drop(
        columns=[s17.DECISION_COLUMN, "notes"])
    sel.final.to_parquet(FINAL_PARQUET, index=False)
    s17.write_workbook(FINAL_XLSX, [
        ("Final_POIs", sel.final), ("Summary", summary), ("Closed_In_Scope", sel.closed),
        ("Undecided", sel.undecided), ("Rulings", pd.DataFrame(rulings))])
    print(f"{len(sel.final):,} POIs in the final list, {len(sel.closed):,} closed, "
          f"{len(sel.undecided):,} undecided -> {FINAL_XLSX}")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] == ["prep"]:
        return cmd_prep()
    if argv[:1] == ["apply"] and len(argv) == 2:
        return cmd_apply(Path(argv[1]))
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
