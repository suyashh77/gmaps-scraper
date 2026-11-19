"""R00 - The completeness and correctness test suite for the master dataset.

WHY THIS EXISTS
    "Authoritative" has been a prose description in a markdown file. That makes
    it unfalsifiable. This turns it into a command that exits 0 or 1.

    Every check below corresponds to a defect that is either documented in the
    quality report or was found during the September audit. A check that passes
    is a claim the dataset can defend; a check that fails prints the exact rows
    so the failure is actionable rather than a percentage.

SEVERITY
    BLOCKER  the dataset is wrong in a way that corrupts downstream analysis
    HIGH     material incompleteness or a documented defect still open
    MED      quality issue that an analyst must be told about
    INFO     recorded for the report, never fails the build

USAGE
    python Remediation/r00_validate.py
    python Remediation/r00_validate.py --json report.json
    python Remediation/r00_validate.py --fail-on BLOCKER
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, connect_ro

NC_LAT = (33.70, 36.70)
NC_LON = (-84.45, -75.35)
CAP = 1500

RESULTS: list[dict] = []


def check(name, severity, sql, ok_when_zero=True, note="", sample_sql=None):
    """Register one check. sql must return a single integer in column 0."""
    RESULTS.append({"name": name, "severity": severity, "sql": sql,
                    "ok_when_zero": ok_when_zero, "note": note,
                    "sample_sql": sample_sql})


# ── Referential integrity ────────────────────────────────────────────────
check("orphan reviews", "BLOCKER",
      "SELECT COUNT(*) FROM reviews r LEFT JOIN stores s USING(store_id) "
      "WHERE s.store_id IS NULL",
      note="A review must always name a store that exists.")

check("duplicate place_id", "BLOCKER",
      "SELECT COUNT(*) FROM (SELECT place_id FROM stores WHERE place_id IS NOT NULL "
      "GROUP BY place_id HAVING COUNT(*) > 1)",
      note="place_id is the only stable store identity; duplicates split a store's reviews.")

check("stores with no place_id", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE place_id IS NULL OR TRIM(place_id) = ''",
      note="A store with no identifier can never be re-visited or refreshed.")

check("non-standard place_id", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE place_id NOT LIKE 'ChIJ%' "
      "AND place_id NOT LIKE '0x%:0x%' AND store_id NOT IN (SELECT store_id FROM ruled)",
      note="Not re-scrapable. A hex feature id (0x...:0x...) is navigable via "
           "maps?cid= and is not counted here; the merge replaces it with the "
           "ChIJ id once the scraper lands. Documented as 67 open in the quality report.",
      sample_sql="SELECT store_id, store_name, city, place_id FROM stores "
                 "WHERE place_id NOT LIKE 'ChIJ%' AND place_id NOT LIKE '0x%:0x%' LIMIT 25")

# ── Counter truth ────────────────────────────────────────────────────────
check("reviews_scraped disagrees with actual count", "BLOCKER",
      "SELECT COUNT(*) FROM stores s WHERE COALESCE(s.reviews_scraped,0) <> "
      "(SELECT COUNT(*) FROM reviews r WHERE r.store_id = s.store_id)",
      note="The stored counter drives scheduling. If it lies, the queue is fiction.",
      sample_sql="SELECT s.store_id, s.store_name, s.reviews_scraped AS stored, "
                 "(SELECT COUNT(*) FROM reviews r WHERE r.store_id=s.store_id) AS actual "
                 "FROM stores s WHERE COALESCE(s.reviews_scraped,0) <> "
                 "(SELECT COUNT(*) FROM reviews r WHERE r.store_id=s.store_id) LIMIT 25")

check("scrape_status disagrees with the counts", "MED",
      "SELECT COUNT(*) FROM stores WHERE target_reviews IS NOT NULL AND target_reviews > 0 "
      "AND ((reviews_scraped >= target_reviews AND scrape_status <> 'complete') "
      "  OR (reviews_scraped <  target_reviews AND scrape_status =  'complete'))"
      " AND store_id NOT IN (SELECT store_id FROM ruled)",
      note="Stale column. The canonical test is reviews_scraped >= target_reviews. "
           "Stores carrying a ruling (r12/r13) are a decision, not a count.")

# ── Over-scrape / wrong-location contamination ───────────────────────────
# The headline "157 over-scraped stores" is three different defects wearing one
# number, and they have three different fixes. Splitting them is what makes the
# problem tractable: only the third class needs a human looking at Google.
check("over-scrape, all classes", "MED",
      "SELECT COUNT(*) FROM stores WHERE target_reviews IS NOT NULL AND target_reviews > 0 "
      "AND reviews_scraped > target_reviews + 5 "
      "AND store_id NOT IN (SELECT store_id FROM accepted_over)",
      note="Umbrella count. See the three classified checks below for what to do.",
      sample_sql="SELECT store_id, store_name, city, category, reviews_scraped, "
                 "target_reviews, reviews_scraped - target_reviews AS overshoot "
                 "FROM stores WHERE target_reviews > 0 AND reviews_scraped > target_reviews + 5 "
                 "ORDER BY overshoot DESC LIMIT 30")

check("over-scrape class A: stale target", "MED",
      "SELECT COUNT(*) FROM stores WHERE target_reviews > 0 AND target_reviews < 10 "
      "AND reviews_scraped > target_reviews + 5",
      note="Target came from stale source metadata, so best-effort mode correctly "
           "trusted Google's live count. The reviews are fine; the TARGET is wrong. "
           "Fix by back-filling target, not by touching reviews.")

check("over-scrape class B: double scrape", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE target_reviews >= 10 "
      "AND reviews_scraped BETWEEN target_reviews * 1.7 AND target_reviews * 2.3 "
      "AND store_id NOT IN (SELECT store_id FROM accepted_over)",
      note="Store was claimed and scraped twice inside one batch. Google's review ids "
           "are not stable between sessions, so the second pass looked new and both "
           "copies were kept. Fix by exact-duplicate deletion.")

check("over-scrape class C: wrong location", "BLOCKER",
      "SELECT COUNT(*) FROM stores WHERE target_reviews >= 50 "
      "AND reviews_scraped > target_reviews * 3",
      note="A trustworthy target and a large multiple of it. This is the genuine "
           "chain-poisoning signature and the only class that needs a human to open "
           "the place_id on Google and adjudicate.",
      sample_sql="SELECT store_id, store_name, city, reviews_scraped, target_reviews, "
                 "place_id FROM stores WHERE target_reviews >= 50 "
                 "AND reviews_scraped > target_reviews * 3 ORDER BY reviews_scraped DESC")

# ── Targets ──────────────────────────────────────────────────────────────
check("stores with no target", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE (target_reviews IS NULL OR target_reviews = 0) "
      "AND COALESCE(status_reason,'') NOT LIKE 'no data%'",
      note="Completion is undefined for these; they are excluded from every fill-rate. "
           "Stores ruled 'no data' (open, Google shows 0 reviews) are not counted.")

check("stores with reviews but no target", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE (target_reviews IS NULL OR target_reviews = 0) "
      "AND reviews_scraped > 0",
      note="Scraped without a completion criterion.")

# ── Completeness ─────────────────────────────────────────────────────────
check("incomplete stores", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE target_reviews > 0 "
      "AND reviews_scraped < target_reviews - 1"
      " AND store_id NOT IN (SELECT store_id FROM ruled)",
      note="Ruled-complete stores (Google shows no more) are not counted. The authoritative bar is >=99% of stores within 1 review of target.")

check("stores never started", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE COALESCE(reviews_scraped,0) = 0"
      " AND store_id NOT IN (SELECT store_id FROM ruled)",
      note="Zero reviews collected (stores ruled complete excluded).")

# The lockout. A store marked complete is filtered out of the scraping export and
# skipped on import, and nothing anywhere demotes complete back to pending. So a
# store that reaches this state is not merely mislabelled, it is unreachable by
# every tool in the pipeline. It gets here because the merge treats a target of
# 0 or NULL as "complete" on the store's first merge.
check("marked complete while holding zero reviews", "BLOCKER",
      "SELECT COUNT(*) FROM stores WHERE scrape_status = 'complete' "
      "AND COALESCE(reviews_scraped,0) = 0 AND store_id NOT IN (SELECT store_id FROM ruled)",
      note="Locked out of every future batch: exports select only pending and "
           "incomplete, and no code path ever demotes a complete store.",
      sample_sql="SELECT store_id, store_name, city, target_reviews FROM stores "
                 "WHERE scrape_status='complete' AND COALESCE(reviews_scraped,0)=0 LIMIT 20")

check("target sitting exactly on the 1,500 cap", "MED",
      "SELECT COUNT(*) FROM stores WHERE target_reviews = 1500",
      note="The target itself was capped, so the completion denominator is "
           "truncated. The true number of reviews these stores have was never "
           "recorded and cannot be recovered from this database.")

check("stores truncated at the cap", "MED",
      "SELECT COUNT(*) FROM stores WHERE reviews_scraped >= " + str(CAP),
      note="A store at the cap is truncated by our own policy, not complete. "
           "It must be labelled as censored data for any per-store analysis.")

# ── Geography ────────────────────────────────────────────────────────────
check("stores outside North Carolina", "BLOCKER",
      "SELECT COUNT(*) FROM stores WHERE state IS NOT NULL AND UPPER(TRIM(state)) "
      "NOT IN ('NC','NORTH CAROLINA')",
      note="Scope is North Carolina only.",
      sample_sql="SELECT store_id, store_name, city, state, full_address FROM stores "
                 "WHERE UPPER(TRIM(state)) NOT IN ('NC','NORTH CAROLINA') LIMIT 25")

check("coordinates outside the NC bounding box", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE source_lat IS NOT NULL AND source_lon IS NOT NULL "
      "AND (source_lat NOT BETWEEN " + str(NC_LAT[0]) + " AND " + str(NC_LAT[1]) +
      " OR source_lon NOT BETWEEN " + str(NC_LON[0]) + " AND " + str(NC_LON[1]) + ")"
      " AND store_id NOT IN (SELECT store_id FROM accepted_coords)",
      note="Either a bad geocode or a store that is not in NC.",
      sample_sql="SELECT store_id, store_name, city, source_lat, source_lon FROM stores "
                 "WHERE source_lat NOT BETWEEN 33.70 AND 36.70 "
                 "OR source_lon NOT BETWEEN -84.45 AND -75.35 LIMIT 25")

check("stores with no city", "MED",
      "SELECT COUNT(*) FROM stores WHERE city IS NULL OR TRIM(city) = ''")

# Geographic self-consistency. This one is computed in Python rather than SQL
# because it needs a haversine and a per-ZIP centroid; see geo_inconsistent().
#
# It exists because the enrichment program's name-similarity gate was inert: it
# computed a score, logged it, and accepted the result regardless, so the
# program effectively took Google's first search result for every query. A store
# matched to a listing in another city keeps a completely plausible review count,
# which is why the over-scrape heuristic never caught these.
# (implemented in geo_inconsistent(), registered as a python check below)

# ── Classification ───────────────────────────────────────────────────────
check("stores with no category", "HIGH",
      "SELECT COUNT(*) FROM stores WHERE category IS NULL OR TRIM(category) = ''",
      note="Cannot participate in category-stratified analysis.",
      sample_sql="SELECT store_id, store_name, city, subtypes FROM stores "
                 "WHERE category IS NULL OR TRIM(category)='' LIMIT 25")

# ── Review-level correctness ─────────────────────────────────────────────
check("within-store duplicate reviews (50-char key)", "MED",
      "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) AS n FROM reviews "
      "GROUP BY store_id, COALESCE(reviewer_name,''), rating, "
      "SUBSTR(COALESCE(review_text,''),1,50) HAVING n > 1)",
      note="Survived the merge dedup key.")

check("exact duplicate reviews, text-bearing", "HIGH",
      "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) AS n FROM reviews "
      "WHERE TRIM(COALESCE(review_text,'')) <> '' "
      "GROUP BY store_id, COALESCE(reviewer_name,''), rating, "
      "TRIM(COALESCE(review_text,'')) HAVING n > 1)",
      note="Identical full text, rating and reviewer on one store. Duplicates "
           "beyond reasonable doubt, and safe to collapse.")

# Deliberately not a failure. Same store, same reviewer name, same stars, no
# text. "Chris" appears at 240 distinct stores in this corpus, so two different
# people each leaving an untexted rating at one busy supermarket is an ordinary
# event rather than a scraping artifact. Deleting these would be silent data
# loss aimed squarely at the rating-only half of the corpus.
check("rating-only same-name pairs (kept on purpose)", "INFO",
      "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) AS n FROM reviews "
      "WHERE TRIM(COALESCE(review_text,'')) = '' "
      "GROUP BY store_id, COALESCE(reviewer_name,''), rating HAVING n > 1)",
      note="Not deduplicated, by decision. Two people can share a first name.")

check("reviews with an out-of-range rating", "BLOCKER",
      "SELECT COUNT(*) FROM reviews WHERE rating IS NOT NULL "
      "AND (rating < 1 OR rating > 5 OR rating <> CAST(rating AS INTEGER))")

check("reviews with no rating", "MED",
      "SELECT COUNT(*) FROM reviews WHERE rating IS NULL",
      note="Rating extraction failed for these rows.")

check("reviews with no date", "MED",
      "SELECT COUNT(*) FROM reviews WHERE date_relative IS NULL OR TRIM(date_relative) = ''")

check("review text containing mojibake", "HIGH",
      "SELECT COUNT(*) FROM reviews WHERE review_text LIKE '%Ã%' OR review_text LIKE '%Â%' "
      "OR review_text LIKE '%â€%'",
      note="UTF-8 decoded as Latin-1. Corrupts non-English store and reviewer names.")

check("review text containing a null byte", "HIGH",
      "SELECT COUNT(*) FROM reviews WHERE INSTR(review_text, CHAR(0)) > 0")

# Do not use LIKE here. SQLite's LIKE is case-insensitive for ASCII, so
# LIKE '%More' matches any review ending in the ordinary word "more" and reports
# thousands of healthy rows as damaged. GLOB is case-sensitive and is the correct
# operator for a suffix test.
check("review text ending in Google's See-more marker", "INFO",
      "SELECT COUNT(*) FROM reviews WHERE review_text GLOB '*More' "
      "OR review_text GLOB '*Read more'",
      note="Candidate clipping from a See-more expansion that did not fire. "
           "Reviews ending in a typed ellipsis are NOT counted here: a trailing "
           "U+2026 is overwhelmingly the reviewer's own punctuation after an emoji, "
           "and cannot be told apart from clipping using stored text alone.")

# ── Dead fields ──────────────────────────────────────────────────────────
check("helpful_count never populated", "HIGH",
      "SELECT CASE WHEN (SELECT COUNT(*) FROM reviews WHERE COALESCE(helpful_count,0) <> 0) = 0 "
      "THEN 1 ELSE 0 END",
      note="A field that is uniformly zero across 1.2M rows is an extraction failure, "
           "not a measurement. Selector is case-sensitive and never matches.")

check("photo_count never populated", "HIGH",
      "SELECT CASE WHEN (SELECT COUNT(*) FROM reviews WHERE COALESCE(photo_count,0) <> 0) = 0 "
      "THEN 1 ELSE 0 END",
      note="locator().count() returns 0 for a non-matching selector and never raises, "
           "so the failure is invisible.")

check("service_type never populated", "HIGH",
      "SELECT CASE WHEN (SELECT COUNT(*) FROM reviews "
      "WHERE service_type IS NOT NULL AND TRIM(service_type) <> '') = 0 "
      "THEN 1 ELSE 0 END",
      note="Dead column.")

# ── Provenance ───────────────────────────────────────────────────────────
check("reviews with no source_db", "HIGH",
      "SELECT COUNT(*) FROM reviews WHERE source_db IS NULL OR TRIM(source_db) = ''",
      note="Provenance is the difference between a dataset and a pile of scraped HTML.")


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def geo_inconsistent(conn, threshold_km: float = 25.0):
    """Stores sitting far from the place their own address names.

    The enrichment program's name-similarity gate computed a score, logged it,
    and then accepted the result regardless, on both branches. With verification
    inert it took Google's first search result for every query, and a first
    result in the wrong city carries a completely ordinary review count, so the
    over-scrape heuristic never sees it.

    The test needs no external geocoder. Store coordinates come from Overture and
    are independent of the address Google returned, so we build a centroid for
    every ZIP from the coordinates of the stores in it, then ask how far each
    store sits from the centroid of the ZIP printed in its own address. A store
    whose address says one place while its coordinates say somewhere 300 km away
    is internally inconsistent, whichever half is wrong.
    """
    zip_pts: dict[str, list] = {}
    has_flags = any(r[1] == "qa_flags" for r in conn.execute("PRAGMA table_info(stores)"))
    # A store already flagged BAD_GEOCODE has a coordinate known to be wrong at
    # source, so it fails this test for a reason that is already recorded.
    # Counting it again as suspected contamination would be double-reporting.
    skip = " AND COALESCE(qa_flags,'') NOT LIKE '%BAD_GEOCODE%'" if has_flags else ""
    rows = conn.execute(
        "SELECT store_id, store_name, city, zip, full_address, outscraper_address, "
        "source_lat, source_lon, reviews_scraped, place_id FROM stores "
        "WHERE source_lat IS NOT NULL AND source_lon IS NOT NULL" + skip).fetchall()
    for r in rows:
        z = (r["zip"] or "")[:5]
        if len(z) == 5:
            zip_pts.setdefault(z, []).append((r["source_lat"], r["source_lon"]))
    centroid = {z: (sum(a for a, _ in v) / len(v), sum(b for _, b in v) / len(v))
                for z, v in zip_pts.items() if len(v) >= 2}

    pat = re.compile(r",\s*([A-Za-z .'-]+),\s*[A-Z]{2}\s+(\d{5})")
    out = []
    for r in rows:
        addr = r["full_address"] or r["outscraper_address"] or ""
        m = pat.search(addr)
        if not m:
            continue
        azip = m.group(2)
        if azip not in centroid:
            continue
        d = _haversine_km(r["source_lat"], r["source_lon"], *centroid[azip])
        if d > threshold_km:
            out.append({"store_id": r["store_id"], "store_name": r["store_name"],
                        "source_city": r["city"], "address": addr,
                        "km_off": round(d, 1),
                        "reviews": r["reviews_scraped"] or 0,
                        "place_id": r["place_id"]})
    out.sort(key=lambda d: -d["km_off"])
    return out


def cross_store_contamination(conn, min_shared: int = 20):
    """Pairs of stores sharing many byte-identical reviews.

    One review page scraped into several store_ids. Legitimate overlap exists,
    since a person can review two branches of a chain, but sharing twenty or
    more identical full texts with the same reviewer names is not that.
    """
    rows = conn.execute(
        "SELECT store_id, reviewer_name, TRIM(review_text) AS t FROM reviews "
        "WHERE TRIM(COALESCE(review_text,'')) <> ''").fetchall()
    by_key: dict[tuple, set] = {}
    for r in rows:
        by_key.setdefault((r["reviewer_name"] or "", r["t"]), set()).add(r["store_id"])
    pair_counts: dict[tuple, int] = {}
    for stores in by_key.values():
        if len(stores) < 2 or len(stores) > 8:
            continue
        s = sorted(stores)
        for i in range(len(s)):
            for j in range(i + 1, len(s)):
                pair_counts[(s[i], s[j])] = pair_counts.get((s[i], s[j]), 0) + 1
    pairs = [(a, b, n) for (a, b), n in pair_counts.items() if n >= min_shared]
    pairs.sort(key=lambda t: -t[2])
    involved = {x for a, b, _ in pairs for x in (a, b)}
    return pairs, involved


EXCLUDED_STATUSES = ("out_of_scope", "duplicate", "bad_place_id", "closed")


def scope_to_study(conn) -> int:
    """Shadow stores/reviews with TEMP views holding only the stores in the study.

    r12 rulings mark stores out_of_scope, duplicate or bad_place_id instead of
    deleting them. Every check here reads the unqualified names, which SQLite
    resolves to the temp schema first, so one place decides what is measured.
    Also defines `ruled` (stores whose status is a decision, not a count).
    Returns the number of stores excluded.
    """
    # Stores whose out-of-NC coordinates a person checked against Google (r13 accepted_flags).
    accepted = ("SELECT store_id FROM main.accepted_flags WHERE check_name='coords_outside_nc'"
                if conn.execute("SELECT 1 FROM main.sqlite_master WHERE name='accepted_flags'").fetchone()
                else "SELECT NULL AS store_id WHERE 0")
    conn.execute("CREATE TEMP VIEW accepted_coords AS " + accepted)
    # Stores holding more reviews than Google shows because several scrapes added up over months
    # (some reviews since deleted on Google); only the umbrella over-scrape count honours this.
    conn.execute("CREATE TEMP VIEW accepted_over AS " + accepted.replace("coords_outside_nc", "over_target_accumulated"))
    cols = {r[1] for r in conn.execute("PRAGMA main.table_info(stores)")}
    if "status_reason" not in cols:
        conn.execute("CREATE TEMP VIEW ruled AS SELECT NULL AS store_id WHERE 0")
        return 0
    marks = ",".join("'" + s + "'" for s in EXCLUDED_STATUSES)
    conn.execute("CREATE TEMP VIEW ruled AS SELECT store_id FROM main.stores "
                 "WHERE status_reason IS NOT NULL")
    conn.execute("CREATE TEMP VIEW stores AS SELECT * FROM main.stores "
                 "WHERE COALESCE(scrape_status,'') NOT IN (" + marks + ")")
    # Exclude the ruled-out stores' reviews rather than keep the in-scope ones, so a
    # review naming no store at all is still visible to the orphan check.
    conn.execute("CREATE TEMP VIEW reviews AS SELECT * FROM main.reviews "
                 "WHERE store_id NOT IN (SELECT store_id FROM main.stores "
                 "WHERE scrape_status IN (" + marks + "))")
    return conn.execute("SELECT COUNT(*) FROM main.stores WHERE scrape_status IN ("
                        + marks + ")").fetchone()[0]


def run(master_path: str, verbose: bool) -> dict:
    conn = connect_ro(master_path)
    excluded = scope_to_study(conn)
    cur = conn.cursor()

    summary = {"master": master_path, "generated": datetime.now().isoformat(timespec="seconds"),
               "checks": [], "counts": {}}
    summary["counts"]["stores"] = cur.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    summary["counts"]["reviews"] = cur.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    summary["counts"]["reviews_with_text"] = cur.execute(
        "SELECT COUNT(*) FROM reviews WHERE review_text IS NOT NULL "
        "AND TRIM(review_text) <> ''").fetchone()[0]
    summary["counts"]["cities"] = cur.execute(
        "SELECT COUNT(DISTINCT city) FROM stores WHERE city IS NOT NULL").fetchone()[0]

    fills = cur.execute(
        "SELECT SUM(MIN(reviews_scraped, target_reviews)), SUM(target_reviews) "
        "FROM stores WHERE target_reviews > 0").fetchone()
    # What is still to collect leaves out stores ruled complete (Google shows no more).
    open_gap = cur.execute(
        "SELECT COALESCE(SUM(target_reviews - MIN(reviews_scraped, target_reviews)),0) "
        "FROM stores WHERE target_reviews > 0"
        " AND store_id NOT IN (SELECT store_id FROM ruled)").fetchone()[0]
    summary["counts"]["capped_fill_pct"] = round(100.0 * (fills[0] or 0) / (fills[1] or 1), 2)
    summary["counts"]["reviews_remaining"] = int(open_gap)

    width = max(len(c["name"]) for c in RESULTS)
    print("=" * (width + 34))
    print("MASTER DATASET VALIDATION")
    print("=" * (width + 34))
    summary["counts"]["excluded_by_ruling"] = excluded
    print("  stores            " + format(summary["counts"]["stores"], ">10,")
          + ("   (+" + format(excluded, ",") + " excluded by r12 rulings)" if excluded else ""))
    print("  reviews           " + format(summary["counts"]["reviews"], ">10,"))
    print("  reviews with text " + format(summary["counts"]["reviews_with_text"], ">10,"))
    print("  cities            " + format(summary["counts"]["cities"], ">10,"))
    print("  capped fill       " + format(summary["counts"]["capped_fill_pct"], ">10.2f") + " %")
    print("  reviews remaining " + format(summary["counts"]["reviews_remaining"], ">10,"))
    print("=" * (width + 34))

    worst = "PASS"
    order = {"PASS": 0, "INFO": 1, "MED": 2, "HIGH": 3, "BLOCKER": 4}
    for c in RESULTS:
        try:
            value = cur.execute(c["sql"]).fetchone()[0] or 0
        except Exception as exc:                      # noqa: BLE001
            value, status = -1, "ERROR"
            print("  ERROR   " + c["name"].ljust(width) + "  " + str(exc))
            summary["checks"].append({**{k: c[k] for k in ("name", "severity", "note")},
                                      "value": None, "status": "ERROR", "error": str(exc)})
            continue
        passed = (value == 0) if c["ok_when_zero"] else (value > 0)
        status = "PASS" if passed else c["severity"]
        if order[status] > order[worst]:
            worst = status
        mark = "ok  " if passed else "FAIL"
        print("  " + mark + "  " + c["name"].ljust(width) + "  "
              + format(value, ">12,") + ("" if passed else "   [" + c["severity"] + "]"))
        if not passed and c["note"]:
            print("        " + c["note"])
        if not passed and verbose and c.get("sample_sql"):
            for row in cur.execute(c["sample_sql"]).fetchall():
                print("           " + " | ".join(str(x) for x in tuple(row)))
        summary["checks"].append({"name": c["name"], "severity": c["severity"],
                                  "note": c["note"], "value": value, "status": status})

    # ── Checks that need more than one SQL statement ─────────────────────
    # Stores a person has accepted (r13 accepted_flags) are judged correct.
    accepted = set()
    if conn.execute("SELECT 1 FROM main.sqlite_master WHERE name='accepted_flags'").fetchone():
        accepted = {r[0] for r in conn.execute(
            "SELECT store_id FROM accepted_flags WHERE check_name='far_from_address'")}
    geo = [g for g in geo_inconsistent(conn) if g["store_id"] not in accepted]
    far = [g for g in geo if g["km_off"] > 100]
    geo_reviews = sum(g["reviews"] for g in geo)
    passed = not geo
    if not passed and order["BLOCKER"] > order[worst]:
        worst = "BLOCKER"
    print("  " + ("ok  " if passed else "FAIL") + "  "
          + "stores far from their own address".ljust(width) + "  "
          + format(len(geo), ">12,") + ("" if passed else "   [BLOCKER]"))
    if not passed:
        print("        " + format(len(far), ",") + " of them are more than 100 km out, and "
              + format(geo_reviews, ",") + " reviews hang off the whole set.")
        print("        Store coordinates come from Overture and the address comes from Google, "
              "so the two")
        print("        disagreeing means one of them belongs to a different store.")
        if verbose:
            for g in geo[:20]:
                print("           " + str(g["km_off"]).rjust(6) + " km  "
                      + str(g["store_id"]).rjust(5) + "  "
                      + str(g["store_name"])[:28].ljust(28) + " "
                      + str(g["source_city"])[:14].ljust(14) + " -> "
                      + str(g["address"])[:44])
    summary["checks"].append({"name": "stores far from their own address",
                              "severity": "BLOCKER", "value": len(geo),
                              "status": "PASS" if passed else "BLOCKER",
                              "note": "reviews affected: " + str(geo_reviews)})
    summary["geo_inconsistent"] = geo[:200]

    pairs, involved = cross_store_contamination(conn)
    passed = not pairs
    if not passed and order["BLOCKER"] > order[worst]:
        worst = "BLOCKER"
    print("  " + ("ok  " if passed else "FAIL") + "  "
          + "store pairs sharing 20+ identical reviews".ljust(width) + "  "
          + format(len(pairs), ">12,") + ("" if passed else "   [BLOCKER]"))
    if not passed:
        print("        " + str(len(involved)) + " stores involved. One review page was "
              "collected into several store ids.")
        if verbose:
            for a, b, n in pairs[:15]:
                print("           " + str(n).rjust(4) + " shared  stores "
                      + str(a) + " and " + str(b))
    summary["checks"].append({"name": "store pairs sharing 20+ identical reviews",
                              "severity": "BLOCKER", "value": len(pairs),
                              "status": "PASS" if passed else "BLOCKER",
                              "note": str(len(involved)) + " stores involved"})

    print("=" * (width + 34))
    print("WORST SEVERITY: " + worst)
    summary["worst"] = worst
    conn.close()
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--json", default=None, help="write the full result as JSON")
    ap.add_argument("-v", "--verbose", action="store_true", help="print offending rows")
    ap.add_argument("--fail-on", default="BLOCKER",
                    choices=["BLOCKER", "HIGH", "MED", "NEVER"])
    args = ap.parse_args()

    summary = run(args.master, args.verbose)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)
        print("Wrote " + args.json)

    if args.fail_on == "NEVER":
        return 0
    order = {"PASS": 0, "INFO": 1, "MED": 2, "HIGH": 3, "BLOCKER": 4}
    return 1 if order[summary["worst"]] >= order[args.fail_on] else 0


if __name__ == "__main__":
    raise SystemExit(main())
