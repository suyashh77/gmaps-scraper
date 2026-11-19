"""
s16_dewey_report.py - every Dewey (SafeGraph Places) POI with its categories and
whether master already holds reviews for it.

Answers one question per row: "we have this store in Dewey - do we have its Google
reviews?" It reuses the s15 join (frame + probe + master + Dewey + Patterns, one row per
store) and keeps only the rows that carry a Dewey PLACEKEY, so the numbers agree with
POI_Decision_Sheet_<date>.xlsx.

Sheets
  Dewey_POIs          21,532 rows - Dewey columns, the frame/probe/master facts attached to
                      each, have_reviews yes/no and a review_status label
  Summary_by_Type     per dewey_food_access_type: POIs, matched, probed, in master, have
                      reviews, reviews held (each master store counted once)
  Summary_by_NAICS    same per SafeGraph sub_category
  Summary_by_Brand    same per brand (top 60)
  Status_x_Type       review_status x food_access_type
  Notes               what the columns and statuses mean

Usage
  python Frame/s16_dewey_report.py            # -> Data/exports/Dewey_Overlap_Report_<date>.xlsx
Needs the cached s15 match files (Frame/build/s15_match_*.csv); run s15 --match first if absent.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import s15_poi_decision_sheet as s15  # noqa: E402

EXPORTS = HERE.parent / "Data" / "exports"

STATUS_HAVE_REVIEWS = "1 reviews held in master"
STATUS_IN_MASTER_NONE = "2 in master, no reviews yet"
STATUS_PROBED_NOT_MASTER = "3 probed on Google, not in master"
STATUS_FRAME_NOT_PROBED = "4 matched a frame store, never probed"
STATUS_NO_MATCH = "5 no match to our data"

STATUS_MEANING = {
    STATUS_HAVE_REVIEWS: "master.reviews has 1+ rows for the store this POI matched",
    STATUS_IN_MASTER_NONE: "store is in master.stores (queued / not started / held back) but holds 0 reviews",
    STATUS_PROBED_NOT_MASTER: "frame store was probed on Google Maps but never imported into master",
    STATUS_FRAME_NOT_PROBED: "POI matched a frame row that s10 left off the 16,751 probe list",
    STATUS_NO_MATCH: "geomatch + proximity found no frame row (different ZIP3, no counterpart, or a name/address we could not link)",
}

DEWEY_COLS = [
    "dewey_placekey", "dewey_location_name", "dewey_brands", "dewey_street_address", "dewey_city",
    "dewey_postal_code", "dewey_latitude", "dewey_longitude", "dewey_naics_code", "dewey_top_category",
    "dewey_sub_category", "dewey_category_tags", "dewey_food_access_type", "dewey_closed",
    "dewey_opened_on", "dewey_closed_on", "dewey_phone_number", "dewey_website",
]
LINK_COLS = ["dewey_match_confidence", "dewey_match_why", "dewey_match_name_sim", "dewey_match_dist_km",
             "dewey_match_alternatives"]
OUR_COLS = [
    "frame_store_id", "frame_name", "frame_food_access_type", "frame_tier", "frame_disposition",
    "google_probed_at", "google_probe_status", "google_name", "google_category", "google_closed", "google_total_reviews",
    "google_place_id", "master_store_id", "master_store_name", "master_category", "master_scrape_status",
    "master_target_reviews", "master_reviews_in_db", "master_reviews_with_text",
    "patterns_store_id", "patterns_brand", "patterns_avg_weekly_visits",
]


def _present(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].notna() if col in df.columns else pd.Series(False, index=df.index)


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    """Numeric column with blanks as 0; all zeros when the column is absent."""
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0)


def brand_name(raw: pd.Series) -> pd.Series:
    """'[{"safegraph_brand_id":..,"safegraph_brand_name":"Food Lion"}]' -> 'Food Lion'; several -> joined with '; '."""
    import json

    def one(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "(no brand)"
        try:
            names = [b.get("safegraph_brand_name", "") for b in json.loads(str(v))]
        except (ValueError, TypeError, AttributeError):
            return str(v)
        names = [n for n in names if n]
        return "; ".join(names) if names else "(no brand)"

    return raw.map(one)


def review_status(df: pd.DataFrame) -> pd.Series:
    """One label per Dewey row, from the strongest claim we can make downwards."""
    reviews = _num(df, "master_reviews_in_db")
    in_master = _present(df, "master_store_id")
    probed = _present(df, "google_probed_at")
    in_frame = _present(df, "frame_store_id")
    out = pd.Series(STATUS_NO_MATCH, index=df.index, dtype=object)
    out = out.mask(in_frame, STATUS_FRAME_NOT_PROBED)
    out = out.mask(probed, STATUS_PROBED_NOT_MASTER)
    out = out.mask(in_master, STATUS_IN_MASTER_NONE)
    out = out.mask(in_master & (reviews > 0), STATUS_HAVE_REVIEWS)
    return out


def have_reviews(df: pd.DataFrame) -> pd.Series:
    reviews = _num(df, "master_reviews_in_db")
    return (reviews > 0).map({True: "yes", False: "no"})


SCRAPE_LIST_COLUMNS = ["dewey_placekey", "dewey_location_name", "dewey_food_access_type",
                       "google_place_id", "google_feature_id", "google_category",
                       "google_total_reviews", "master_store_id", "master_reviews_in_db",
                       "master_target_reviews", "never_scraped", "reviews_outstanding"]


def scrape_candidates(d: pd.DataFrame) -> pd.DataFrame:
    """Dewey POIs whose Google reviews we can still collect, in the order to collect them.

    A POI is scrapeable when the frame row it matched was probed successfully, Google did
    not mark it permanently closed, and it carries an identity the scraper can navigate by
    (a ChIJ place_id, or Google's hex feature id via maps?cid=). Stores master has already
    filled to target are dropped.

    Ordered never-scraped first, then largest outstanding count - the same order
    claim_next_store uses, so a --limit here keeps fresh stores rather than big resumes.
    """
    probed = (d.get("google_probe_status") == "done") & _present(d, "google_probed_at")
    open_ = _num(d, "google_closed") != 1
    pid = d["google_place_id"].astype(str).str.startswith("ChIJ") if "google_place_id" in d else False
    fid = d["google_feature_id"].astype(str).str.startswith("0x") if "google_feature_id" in d else False
    have = _num(d, "master_reviews_in_db")
    target = _num(d, "master_target_reviews")
    google = _num(d, "google_total_reviews")
    # what is still missing: master's own target when it has one, else Google's live count
    want = target.where(target > 0, google)
    unfinished = have < want

    out = d[probed & open_ & (pid | fid) & unfinished].copy()
    out["never_scraped"] = (_num(out, "master_reviews_in_db") == 0).astype(int)
    out["reviews_outstanding"] = (
        _num(out, "master_target_reviews").where(_num(out, "master_target_reviews") > 0,
                                                 _num(out, "google_total_reviews"))
        - _num(out, "master_reviews_in_db")).clip(lower=0).astype(int)
    out = out.sort_values(["never_scraped", "reviews_outstanding"], ascending=[False, False])
    cols = [c for c in SCRAPE_LIST_COLUMNS if c in out.columns]
    return out[cols].reset_index(drop=True)


def summary_by(df: pd.DataFrame, col: str, limit: int | None = None) -> pd.DataFrame:
    """Counts per value of col. Rows are counted as POIs; reviews are counted once per master store."""
    key = df[col].fillna("(none)")
    reviews = _num(df, "master_reviews_in_db")
    text = _num(df, "master_reviews_with_text")
    google = _num(df, "google_total_reviews")
    work = pd.DataFrame({
        "k": key, "frame": _present(df, "frame_store_id"), "probed": _present(df, "google_probed_at"),
        "master": _present(df, "master_store_id"), "have": reviews > 0,
        "mid": df["master_store_id"] if "master_store_id" in df.columns else pd.Series(None, index=df.index, dtype=object), "reviews": reviews, "text": text, "google": google,
        "closed": _num(df, "dewey_closed") == 1,
    })

    def agg(g: pd.DataFrame) -> pd.Series:
        once = g.dropna(subset=["mid"]).drop_duplicates("mid")
        return pd.Series({
            "dewey_pois": len(g), "dewey_closed": int(g.closed.sum()),
            "matched_to_frame": int(g.frame.sum()), "probed": int(g.probed.sum()),
            "in_master": int(g.master.sum()), "distinct_master_stores": int(once.shape[0]),
            "have_reviews": int(g.have.sum()), "pct_have_reviews": round(100 * g.have.mean(), 1) if len(g) else 0.0,
            "reviews_in_master": int(once.reviews.sum()), "reviews_with_text": int(once.text.sum()),
            "google_live_reviews_capped": int(g.google.sum()),
        })

    out = work.groupby("k", sort=False)[list(work.columns)].apply(agg).reset_index().rename(columns={"k": col})
    out = out.sort_values("dewey_pois", ascending=False).reset_index(drop=True)
    if limit:
        out = out.head(limit)
    total = agg(work).to_frame().T
    total.insert(0, col, "TOTAL")
    return pd.concat([out, total], ignore_index=True)


def build() -> pd.DataFrame:
    sheet = s15.assemble()
    d = sheet[sheet.dewey_placekey.notna()].copy()
    if not d.dewey_placekey.is_unique:
        sys.exit("dewey_placekey is not unique in the s15 join - the one-to-one assignment is broken")
    d["dewey_brand_name"] = brand_name(d.dewey_brands)
    d["have_reviews"] = have_reviews(d)
    d["review_status"] = review_status(d)
    d["matched_to_frame"] = _present(d, "frame_store_id").map({True: "yes", False: "no"})
    d["probed_on_google"] = _present(d, "google_probed_at").map({True: "yes", False: "no"})
    d["in_master"] = _present(d, "master_store_id").map({True: "yes", False: "no"})
    d["master_rows_sharing_store"] = d.master_store_id.map(sheet.master_store_id.value_counts())
    lead = ["dewey_placekey", "dewey_location_name", "dewey_brand_name", "dewey_food_access_type",
            "dewey_sub_category", "dewey_city", "dewey_postal_code", "have_reviews", "review_status",
            "master_reviews_in_db", "master_reviews_with_text", "google_total_reviews",
            "matched_to_frame", "probed_on_google", "in_master", "dewey_match_confidence"]
    rest = [c for c in DEWEY_COLS + LINK_COLS + OUR_COLS + ["master_rows_sharing_store"]
            if c in d.columns and c not in lead]
    return d[[c for c in lead if c in d.columns] + rest].sort_values(
        ["dewey_food_access_type", "dewey_location_name"]).reset_index(drop=True)


COLUMN_NOTES = [
    ("dewey_*", "SafeGraph Global Places columns as extracted (dewy data/nc_grocery_pois.db); dewey_food_access_type is the project taxonomy applied to NAICS"),
    ("dewey_brand_name", "brand name(s) parsed out of the SafeGraph BRANDS JSON; '(no brand)' for independents"),
    ("dewey_closed", "1 only when CLOSED_ON is set; the source's operational_status is wrong and is not carried"),
    ("have_reviews", "yes when master.reviews holds 1+ rows for the matched store"),
    ("review_status", "see the Statuses table below; numbered so it sorts from best to worst"),
    ("master_reviews_in_db / _with_text", "live counts from Data/Step_5_master_reviews.db for the matched store"),
    ("google_total_reviews", "Google's live review count from the probe, capped at 1,500 (the scraper cap)"),
    ("matched_to_frame / probed_on_google / in_master", "which of our lists the POI reached"),
    ("dewey_match_confidence", "A geomatch (address rule) > B proximity house+name > C proximity name > D proximity house only - verify"),
    ("dewey_match_why / _name_sim / _dist_km / _alternatives", "how the link to the frame row was made; alternatives > 1 means other frame rows also matched"),
    ("frame_* / google_* / master_* / patterns_*", "the frame row this POI matched and what Google, master and Patterns hold for it"),
    ("master_rows_sharing_store", "> 1 means this master store is attached to several frame rows (place_id collision); its reviews are counted once in the summaries"),
]


def notes(d: pd.DataFrame) -> pd.DataFrame:
    n_have = int((d.have_reviews == "yes").sum())
    lines = [
        ("Built", str(date.today())),
        ("Rows", f"{len(d):,} Dewey POIs, one per PLACEKEY; {n_have:,} have reviews in master"),
        ("Source", "SafeGraph Global Places release 2026-08 via Dewey Data, NC food retail extract of 2026-09-16 (dewy data/README.md)"),
        ("Join", "Frame/s15_poi_decision_sheet.py: Dewey -> frame by geomatch (ZIP3 blocks) then 150 m proximity; frame -> probe by store_id; probe/frame -> master by Google place_id"),
        ("Regenerate", "python Frame/s16_dewey_report.py (after s15 --proximity-only if the frame or probe changed)"),
    ]
    header = pd.DataFrame(lines, columns=["item", "detail"])
    cols = pd.DataFrame(COLUMN_NOTES, columns=["item", "detail"])
    statuses = pd.DataFrame(list(STATUS_MEANING.items()), columns=["item", "detail"])
    return pd.concat([header, pd.DataFrame([("", "")], columns=["item", "detail"]),
                      pd.DataFrame([("COLUMNS", "")], columns=["item", "detail"]), cols,
                      pd.DataFrame([("", ""), ("STATUSES", "")], columns=["item", "detail"]), statuses],
                     ignore_index=True)


def write_xlsx(d: pd.DataFrame, path: Path) -> None:
    from openpyxl.utils import get_column_letter

    path.parent.mkdir(parents=True, exist_ok=True)
    xtab = pd.crosstab(d.review_status, d.dewey_food_access_type.fillna("(none)"), margins=True,
                       margins_name="TOTAL").reset_index()
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        d.to_excel(xw, sheet_name="Dewey_POIs", index=False)
        ws = xw.sheets["Dewey_POIs"]
        ws.freeze_panes = "C2"
        ws.auto_filter.ref = ws.dimensions
        for i, col in enumerate(d.columns, 1):
            ws.column_dimensions[get_column_letter(i)].width = min(max(len(col) + 2, 12), 40)
        summary_by(d, "dewey_food_access_type").to_excel(xw, sheet_name="Summary_by_Type", index=False)
        summary_by(d, "dewey_sub_category").to_excel(xw, sheet_name="Summary_by_NAICS", index=False)
        summary_by(d, "dewey_brand_name", limit=60).to_excel(xw, sheet_name="Summary_by_Brand", index=False)
        xtab.to_excel(xw, sheet_name="Status_x_Type", index=False)
        notes(d).to_excel(xw, sheet_name="Notes", index=False)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scrape-list", metavar="CSV", default=None,
                    help="also write the scrapeable POIs (probed, open, identifiable, "
                         "unfinished) as a CSV for r11/r09")
    a = ap.parse_args()

    d = build()
    path = EXPORTS / f"Dewey_Overlap_Report_{date.today():%Y-%m-%d}.xlsx"
    write_xlsx(d, path)
    print(f"{len(d):,} Dewey POIs -> {path}")
    print(d.review_status.value_counts().sort_index().to_string())

    if a.scrape_list:
        cand = scrape_candidates(d)
        out = Path(a.scrape_list)
        out.parent.mkdir(parents=True, exist_ok=True)
        cand.to_csv(out, index=False)
        fresh = int(cand.never_scraped.sum())
        print("")
        print(f"scrape list: {len(cand):,} POIs -> {out}")
        print(f"  never scraped {fresh:,} (queued first) | resumed {len(cand) - fresh:,} (last)")
        print(f"  reviews outstanding {int(cand.reviews_outstanding.sum()):,}")
        print(f"  already in master {int(cand.master_store_id.notna().sum()):,} | "
              f"need r11 import {int(cand.master_store_id.isna().sum()):,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
