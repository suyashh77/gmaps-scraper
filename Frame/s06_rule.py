"""
s06_rule.py - apply the ruleset and assemble one canonical row per store.

Two jobs.

Ruling. Every record carries a category string from one of four vocabularies.
Each string is looked up in category_ruleset.csv. For Overture a record is
ruled on its primary category, falling back to its alternates only when the
primary has no ruling - so a restaurant that happens to carry a "food"
alternate stays a restaurant. A cluster then takes the most inclusive tier
among its records, because a store that SNAP calls a Supermarket is a
supermarket whatever Overture's crowd-sourced primary happens to say.

A category string with no ruling produces tier UNRULED. Those clusters are
written to a worklist and counted in the build output. They are never dropped,
because dropping them silently is the exact defect that removed 162 Google
categories and every Costco in North Carolina from the previous frame.

Assembly. Attributes are taken from the most authoritative source that has
them, which is not the same order for every field: SNAP is a federally
verified address, master carries the Google place_id and the review history,
Overture has the best coordinates and names.

Writes Frame/build/frame.csv and Frame/reports/unruled_worklist.csv.
"""
import sys
import unicodedata
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
BUILD = HERE / "build"
REPORTS = HERE / "reports"

# NO_CATEGORY is distinct from UNRULED on purpose. UNRULED means a category
# string exists and nobody has ruled on it - a pipeline defect. NO_CATEGORY
# means the record carries no category at all, so no ruleset can decide it and
# only a person can. Collapsing the two would hide a real defect behind a
# handful of legitimately undecidable rows.
TIER_ORDER = ["CORE", "FRESH", "SPECIALTY", "MARGINAL", "OUT", "UNRULED",
              "NO_CATEGORY"]
TIER_RANK = {t: i for i, t in enumerate(TIER_ORDER)}

SRC_VOCAB = {
    "snap": "snap", "osm": "osm", "overture": "overture",
    "overture_2601": "overture", "master": "google", "excluded": "google",
}

# Which source wins for each attribute, best first.
# SNAP is authoritative for the ADDRESS and deliberately last for the NAME:
# its strings are administrative ("DollarTree 5360", "FOOD LION  167"), which
# make poor display names and break chain matching. Overture carries properly
# cased business names, so it leads.
NAME_PRI = ["overture", "master", "osm", "snap", "excluded", "overture_2601"]
ADDR_PRI = ["snap", "master", "overture", "osm", "excluded", "overture_2601"]
GEO_PRI = ["overture", "snap", "master", "osm", "overture_2601", "excluded"]

SCRAPE_PRIORITY = {"CORE": 1, "FRESH": 2, "SPECIALTY": 2, "MARGINAL": 3,
                   "UNRULED": 4, "NO_CATEGORY": 4, "OUT": 9}


def fold(s):
    """Casefold and strip accents so 'Tienda de alimentacion' matches the
    accented string Google actually emits, and mojibake does not create a
    second unruled category."""
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.strip().casefold()


def load_ruleset():
    """Base ruleset plus the generated rulings for alternate-only arrivals.
    The base file wins on conflict, so a hand ruling is never overwritten by
    the generated class ruling."""
    parts = [pd.read_csv(HERE / "category_ruleset.csv")]
    alt = HERE / "category_ruleset_alt.csv"
    if alt.exists():
        parts.append(pd.read_csv(alt))
    r = pd.concat(parts, ignore_index=True)
    r = r.drop_duplicates(subset=["vocabulary", "category"], keep="first")
    lut = {}
    for row in r.itertuples():
        lut[(row.vocabulary, fold(row.category))] = (
            row.tier, row.food_access_type if isinstance(row.food_access_type, str) else "")
    return lut


def rule_record(vocab, primary, alternates, lut):
    """(tier, food_access_type, basis) for one record."""
    has_primary = isinstance(primary, str) and primary.strip()
    has_alt = isinstance(alternates, str) and alternates.strip()
    if not has_primary and not has_alt:
        return "NO_CATEGORY", "", "no_category"
    hit = lut.get((vocab, fold(primary)))
    if hit:
        return hit[0], hit[1], "primary"
    best = None
    if isinstance(alternates, str) and alternates:
        for a in alternates.split("|"):
            h = lut.get((vocab, fold(a)))
            if h and (best is None or TIER_RANK[h[0]] < TIER_RANK[best[0]]):
                best = h
    if best:
        return best[0], best[1], "alternate"
    return "UNRULED", "", "none"


def pick(d, col, order):
    """Per cluster, the first non-null value of `col` under a source priority
    order. Vectorised: rank the sources, drop nulls, sort, take the first of
    each cluster. Doing this with a mask per cluster is 50k x 6 masks and does
    not finish in useful time."""
    rank = {s: i for i, s in enumerate(order)}
    t = d.loc[d[col].notna(), ["cluster_id", "source", col]].copy()
    t["_r"] = t.source.map(rank).fillna(99).astype(int)
    t = t.sort_values(["cluster_id", "_r"], kind="stable")
    return t.groupby("cluster_id", sort=False)[col].first()


def main():
    lut = load_ruleset()
    d = pd.read_csv(BUILD / "clusters.csv", dtype=str)
    d["lat"] = pd.to_numeric(d.lat, errors="coerce")
    d["lon"] = pd.to_numeric(d.lon, errors="coerce")
    d["prior_reviews_scraped"] = pd.to_numeric(d.prior_reviews_scraped, errors="coerce")
    d["vocab"] = d.source.map(SRC_VOCAB)
    print(f"{len(d):,} records in {d.cluster_id.nunique():,} clusters")

    ruled = [rule_record(v, c, a, lut)
             for v, c, a in zip(d.vocab, d.raw_category, d.alt_categories)]
    d["rec_tier"] = [x[0] for x in ruled]
    d["rec_fat"] = [x[1] for x in ruled]
    d["rec_basis"] = [x[2] for x in ruled]
    d["rec_rank"] = d.rec_tier.map(TIER_RANK)

    print("\nrecord-level rulings:")
    for t in TIER_ORDER:
        n = int((d.rec_tier == t).sum())
        print(f"  {t:9s} {n:7,}")

    # The winning record per cluster decides tier and ruling provenance.
    best = (d.sort_values(["cluster_id", "rec_rank"], kind="stable")
              .groupby("cluster_id", sort=False)
              .first()[["rec_tier", "rec_basis", "vocab", "raw_category"]])

    f = pd.DataFrame(index=best.index)
    f["name"] = pick(d, "name", NAME_PRI)
    f["street"] = pick(d, "street", ADDR_PRI)
    f["city"] = pick(d, "city", ADDR_PRI)
    f["zip"] = pick(d, "zip5", ADDR_PRI)
    f["county"] = pick(d, "county", ["snap"])
    f["lat"] = pick(d, "lat", GEO_PRI)
    f["lon"] = pick(d, "lon", GEO_PRI)
    f["place_id"] = pick(d, "place_id", ["master", "excluded"])
    f["tier"] = best.rec_tier
    # food_access_type comes from the best-ranked record that carries one
    fat = d.loc[d.rec_fat.astype(str) != "", ["cluster_id", "rec_fat", "rec_rank"]]
    fat = (fat.sort_values(["cluster_id", "rec_rank"], kind="stable")
              .groupby("cluster_id", sort=False).rec_fat.first())
    f["food_access_type"] = fat
    f["food_access_type"] = f.food_access_type.fillna("")
    f["scrape_priority"] = f.tier.map(SCRAPE_PRIORITY)
    f["ruling_basis"] = best.rec_basis
    f["ruling_vocab"] = best.vocab
    f["ruling_category"] = best.raw_category

    g = d.groupby("cluster_id", sort=False)
    f["n_records"] = g.size()
    f["n_sources"] = g.source.nunique()
    present = pd.crosstab(d.cluster_id, d.source) > 0
    for s in ["snap", "overture", "osm", "master", "excluded", "overture_2601"]:
        f[f"in_{s}"] = present[s] if s in present else False
    f["prior_exclusion_reason"] = (
        d.loc[d.prior_exclusion_reason.notna()]
         .groupby("cluster_id", sort=False).prior_exclusion_reason.first())
    f["prior_reviews_scraped"] = g.prior_reviews_scraped.max()
    f["source_ids"] = g.source_id.apply(lambda s: "|".join(s.astype(str)))
    f = f.reset_index().rename(columns={"cluster_id": "frame_id"})
    BUILD.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    f.to_csv(BUILD / "frame.csv", index=False)

    print(f"\n{len(f):,} clusters ruled:")
    for t in TIER_ORDER:
        n = int((f.tier == t).sum())
        print(f"  {t:9s} {n:7,}")

    inc = f[~f.tier.isin(["OUT", "UNRULED", "NO_CATEGORY"])]
    print(f"\nIN FRAME (CORE+FRESH+SPECIALTY+MARGINAL): {len(inc):,}")
    print(f"  cities: {inc.city.nunique()}   counties: {inc.county.nunique()}")
    print("\n  by food_access_type:")
    for t, n in inc.food_access_type.value_counts().items():
        print(f"    {n:7,}  {t}")
    print("\n  by scrape priority:")
    for p, n in sorted(inc.scrape_priority.value_counts().items()):
        print(f"    P{p}: {n:,}")

    unr = f[f.tier == "UNRULED"]
    unr.sort_values("n_sources", ascending=False).to_csv(
        REPORTS / "unruled_worklist.csv", index=False)
    print(f"\nUNRULED clusters written to reports/unruled_worklist.csv: {len(unr):,}")
    if len(unr):
        print("  most common unruled categories:")
        for c, n in unr.ruling_category.value_counts().head(10).items():
            print(f"    {n:6,}  [{c}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
