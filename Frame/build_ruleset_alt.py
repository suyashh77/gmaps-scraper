"""
build_ruleset_alt.py - rule the categories that reach the pull only through a
food string in categories.alternate.

s02 pulls an Overture row when its primary category is ruled in OR any of its
alternates is. That second arm is deliberate - it is how a store Overture
labels `shopping` but tags `grocery_store` gets in - but it also drags in
furniture stores, livestock breeders and antique shops that carry an incidental
food alternate.

8,060 clusters arrived that way, spread over 550 primary categories. None came
from SNAP or OSM and six from master; 7,979 are Overture-only. They are ruled
here rather than left UNRULED, because "no ruling" has to keep meaning "nobody
has looked at this yet".

The default for this population is OUT: a furniture store is not a food
retailer whatever its alternate array says. EXCEPTIONS lists the ones that are
genuinely food retail and get a real tier. Every row written carries a
rationale saying it was ruled as part of this class, so the decision is visible
in the ruleset rather than buried in code.

Writes Frame/category_ruleset_alt.csv. Re-runnable: it reads the current
unruled worklist, so a source refresh that introduces new categories surfaces
them here instead of silently dropping them.
"""
import csv
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
WORKLIST = HERE / "reports" / "unruled_worklist.csv"
OUT = HERE / "category_ruleset_alt.csv"

CLASS_NOTE = ("Non-food primary category; the record entered the pull only via a "
              "food string in categories.alternate. Ruled OUT as a class.")

# Categories in this population that ARE food retail, ruled individually.
EXCEPTIONS = {
    "truck_gas_station": ("MARGINAL", "gas_convenience",
                          "Truck stops carry a full convenience range; in rural NC often the only option."),
    "candy_store": ("SPECIALTY", "specialty", "Retail confection; consistent with the Google ruling."),
    "chocolate_shop": ("SPECIALTY", "specialty", "Retail confection."),
    "nut_store": ("SPECIALTY", "specialty", "Retail food."),
    "gourmet_store": ("SPECIALTY", "specialty", "Specialty food retail."),
    "farmers_market_organizer": ("FRESH", "farmers_market", "Operates a market."),
    "orchard": ("FRESH", "farm_direct", "On-farm retail."),
    "winery": ("OUT", "", "Alcohol only; out of scope for food access, consistent with liquor_store."),
    "brewery": ("OUT", "", "Alcohol only."),
    "distillery": ("OUT", "", "Alcohol only."),
    "bar": ("OUT", "", "Alcohol and prepared food."),
}


def main():
    if not WORKLIST.exists():
        print(f"no worklist at {WORKLIST}; run s06_rule.py first")
        return 1

    u = pd.read_csv(WORKLIST)
    u = u[u.ruling_category.notna()]
    # Key on (vocabulary, category): the same string can mean different things
    # in two vocabularies, and the ruleset is keyed that way.
    cats = u.groupby([u.ruling_vocab.fillna("overture"),
                      u.ruling_category.astype(str)]).size().sort_values(ascending=False)
    print(f"{len(u):,} unruled clusters over {len(cats)} (vocabulary, category) pairs")

    rows = []
    for (vocab, cat), n in cats.items():
        if cat in EXCEPTIONS:
            tier, fat, why = EXCEPTIONS[cat]
        else:
            tier, fat, why = "OUT", "", CLASS_NOTE
        rows.append({
            "vocabulary": vocab,
            "category": cat,
            "tier": tier,
            "include": tier != "OUT",
            "food_access_type": fat,
            "rationale": f"{why} (n={n} clusters)",
        })

    # Carry forward what was generated before, so a rerun against a shorter
    # worklist does not un-rule categories already decided.
    if OUT.exists():
        prev = pd.read_csv(OUT).to_dict("records")
        have = {(r["vocabulary"], r["category"]) for r in rows}
        rows.extend(r for r in prev if (r["vocabulary"], r["category"]) not in have)

    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    n_out = sum(1 for r in rows if r["tier"] == "OUT")
    print(f"wrote {len(rows)} rulings -> {OUT}")
    print(f"  OUT as a class : {n_out}")
    print(f"  ruled in       : {len(rows) - n_out}")
    for r in rows:
        if r["tier"] != "OUT":
            print(f"    {r['tier']:9s} {r['category']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
