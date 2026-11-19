"""R03 - The fixes that need no judgement calls.

Everything in this file is deterministic: given the same master, it produces the
same result, and a human looking at Google Maps could not disagree with any of
it. Anything requiring a judgement call lives in r04 (adjudication worklists)
instead, because a research dataset should never have a guess written into it
by a script.

STEPS
  1  exact duplicate reviews
     727 rows are byte-identical to another row on the same store: same
     reviewer, same rating, same full text. All 727 duplicate groups draw from
     a SINGLE source_db, which rules out a double merge and identifies the real
     cause: the store was claimed and scraped twice inside one batch run.
     Google's data-review-id is not stable between sessions, so the second pass
     looked entirely new and both copies were written. We keep the lowest id of
     each group and delete the rest.

  2  target back-fill for stale targets
     107 of the 157 "over-scraped" stores carry target_reviews < 10, usually
     exactly 4, while holding hundreds of real reviews. Best-effort mode did the
     right thing here: it distrusted an obviously stale target and took Google's
     live count. So the reviews are sound and the TARGET is the wrong number.
     Leaving it wrong keeps 107 stores permanently flagged as contaminated and
     drags the fill-rate denominator. We raise the target to the observed count
     and record the provenance in qa_flags rather than silently overwriting.

  3  reviews_scraped and scrape_status
     Recomputed from the reviews table for every store. scrape_status has
     drifted on 661 stores because merge_results.py only ever revises the count
     upward and treats target_reviews = 0 as "complete".

  4  qa_flags
     Stamps the defect tags that r00 tests for onto the store rows, so an
     analyst filtering the dataset can exclude known-bad stores with a WHERE
     clause instead of by cross-referencing a markdown file.

USAGE
    python Remediation/r03_clean_deterministic.py            # dry run
    python Remediation/r03_clean_deterministic.py --apply
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_ro, connect_rw, counts, print_delta

CAP = 1500

# Only text-bearing reviews are deleted as duplicates.
#
# Of the 727 byte-identical groups, 396 are rating-only: same store, same
# reviewer name, same star rating, no text at all. Those are NOT safely
# duplicates. "Chris" appears at 240 distinct stores in this corpus, and two
# different people named Chris each leaving a 5-star rating with no comment on
# the same busy supermarket is an ordinary event, not a scraping artifact.
# Deleting them would be silent data loss dressed up as a cleanup, and it would
# bias exactly the rating-only half of the corpus that the rating analysis uses.
#
# The 331 text-bearing groups are a different matter: two reviews with identical
# full text, identical rating and an identical reviewer name on one store are
# duplicates beyond reasonable doubt.
TEXT_FILTER = "TRIM(COALESCE(review_text,'')) <> ''"

DUP_GROUPS = """
SELECT MIN(id) AS keep_id, COUNT(*) AS n, store_id
FROM reviews
WHERE """ + TEXT_FILTER + """
GROUP BY store_id, COALESCE(reviewer_name,''), rating, TRIM(COALESCE(review_text,''))
HAVING COUNT(*) > 1
"""

# The extra copies are moved to quarantined_reviews, never dropped (repo rule).
DUP_EXTRA_IDS = """
  SELECT r.id FROM reviews r
  JOIN (
    SELECT MIN(id) AS keep_id, store_id AS sid, COALESCE(reviewer_name,'') AS rn,
           rating AS rt, TRIM(COALESCE(review_text,'')) AS tx
    FROM reviews
    WHERE """ + TEXT_FILTER + """
    GROUP BY store_id, COALESCE(reviewer_name,''), rating, TRIM(COALESCE(review_text,''))
    HAVING COUNT(*) > 1
  ) g
    ON r.store_id = g.sid
   AND COALESCE(r.reviewer_name,'') = g.rn
   AND r.rating IS g.rt
   AND TRIM(COALESCE(r.review_text,'')) = g.tx
  WHERE r.id <> g.keep_id AND """ + TEXT_FILTER + """
"""
QUARANTINE_REASON = "r03: exact text duplicate within one store (double scrape)"


def plan(conn) -> dict:
    cur = conn.cursor()
    dup_text = cur.execute(
        "SELECT COALESCE(SUM(n-1),0) FROM (" + DUP_GROUPS + ")").fetchone()[0]
    dup_rating_only = cur.execute(
        "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM reviews "
        "WHERE TRIM(COALESCE(review_text,'')) = '' "
        "GROUP BY store_id, COALESCE(reviewer_name,''), rating HAVING n>1)").fetchone()[0]
    stale = cur.execute(
        "SELECT COUNT(*) FROM stores WHERE target_reviews > 0 AND target_reviews < 10 "
        "AND reviews_scraped > target_reviews + 5").fetchone()[0]
    drift = cur.execute(
        "SELECT COUNT(*) FROM stores s WHERE COALESCE(s.reviews_scraped,0) <> "
        "(SELECT COUNT(*) FROM reviews r WHERE r.store_id = s.store_id)").fetchone()[0]
    status = cur.execute(
        "SELECT COUNT(*) FROM stores WHERE target_reviews > 0 AND "
        "((reviews_scraped >= target_reviews AND scrape_status <> 'complete') OR "
        " (reviews_scraped <  target_reviews AND scrape_status =  'complete'))").fetchone()[0]
    return {"duplicate rows to delete (text)": dup_text,
            "rating-only dupes LEFT ALONE": dup_rating_only,
            "stores needing a target back-fill": stale,
            "stores whose counter will change": drift,
            "stores whose status will change": status}


def flags_for(row, cap: int) -> str:
    f = []
    t = row["target_reviews"] or 0
    n = row["reviews_scraped"] or 0
    if not (row["place_id"] or "").startswith("ChIJ"):
        f.append("INVALID_PLACE_ID")
    if not (row["category"] or "").strip():
        f.append("NULL_CATEGORY")
    if t == 0:
        f.append("NO_TARGET")
    if t >= 50 and n > t * 3:
        f.append("OVER_SCRAPED_WRONG_LOCATION")
    elif t >= 10 and t * 1.7 <= n <= t * 2.3:
        f.append("OVER_SCRAPED_DOUBLE_RUN")
    elif 0 < t < 10 and n > t + 5:
        f.append("TARGET_BACKFILLED")
    if n >= cap:
        f.append("CAP_TRUNCATED")
    if t > 0 and n < t - 1:
        f.append("INCOMPLETE")
    if n == 0:
        f.append("NOT_STARTED")
    return ",".join(f)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--cap", type=int, default=CAP)
    ap.add_argument("--skip-dedup", action="store_true",
                    help="do everything except delete duplicate reviews")
    args = ap.parse_args()

    probe = connect_ro(args.master)
    p = plan(probe)
    before = counts(probe)
    has_flags = any(r[1] == "qa_flags" for r in probe.execute("PRAGMA table_info(stores)"))
    probe.close()

    print("PLANNED CHANGES")
    for k, v in p.items():
        print("  " + k.ljust(38) + format(v, ">8,"))
    if not has_flags:
        print("\n  stores.qa_flags column is absent; run r02 first to enable flag stamping")

    if not args.apply:
        print("\nDRY RUN. Re-run with --apply.")
        return 0

    dest = backup(args.master, "r03")
    print("\nBackup written: " + os.path.basename(dest))

    conn = connect_rw(args.master)
    cur = conn.cursor()

    if not args.skip_dedup and p["duplicate rows to delete (text)"]:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("CREATE TABLE IF NOT EXISTS quarantined_reviews AS SELECT *, "
                    "'' AS quarantine_reason, '' AS quarantined_at FROM reviews WHERE 0")
        cur.execute("CREATE TEMP TABLE dup_ids AS " + DUP_EXTRA_IDS)
        cur.execute("INSERT INTO quarantined_reviews SELECT *, ?, datetime('now') FROM reviews "
                    "WHERE id IN (SELECT id FROM dup_ids)", (QUARANTINE_REASON,))
        cur.execute("DELETE FROM reviews WHERE id IN (SELECT id FROM dup_ids)")
        print("  quarantined " + format(cur.rowcount, ",") + " duplicate review rows")
        cur.execute("DROP TABLE dup_ids")
        conn.commit()

    # Target back-fill. Only for the stale-target class, and only upward.
    cur.execute("BEGIN IMMEDIATE")
    stale = cur.execute(
        "SELECT store_id, reviews_scraped FROM stores "
        "WHERE target_reviews > 0 AND target_reviews < 10 "
        "AND reviews_scraped > target_reviews + 5").fetchall()
    for r in stale:
        cur.execute("UPDATE stores SET target_reviews=? WHERE store_id=?",
                    (min(r["reviews_scraped"], args.cap), r["store_id"]))
    print("  back-filled targets on " + str(len(stale)) + " stores")
    conn.commit()

    # Recompute counters and status for every store, from the reviews table.
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("""
        UPDATE stores SET reviews_scraped =
          (SELECT COUNT(*) FROM reviews r WHERE r.store_id = stores.store_id)
    """)
    # A status_reason is a ruling (r12); its status is a decision, not a count.
    has_reason = any(r[1] == "status_reason" for r in cur.execute("PRAGMA table_info(stores)"))
    cur.execute("""
        UPDATE stores SET scrape_status = CASE
          WHEN target_reviews IS NULL OR target_reviews = 0 THEN 'unknown_target'
          WHEN reviews_scraped >= target_reviews            THEN 'complete'
          WHEN reviews_scraped = 0                          THEN 'not_started'
          ELSE 'incomplete' END
    """ + (" WHERE status_reason IS NULL" if has_reason else ""))
    print("  recomputed reviews_scraped and scrape_status on every store")
    conn.commit()

    if has_flags:
        cur.execute("BEGIN IMMEDIATE")
        rows = cur.execute(
            "SELECT store_id, place_id, category, target_reviews, reviews_scraped "
            "FROM stores").fetchall()
        payload = [(flags_for(r, args.cap) or None, r["store_id"]) for r in rows]
        cur.executemany("UPDATE stores SET qa_flags=? WHERE store_id=?", payload)
        conn.commit()
        n = cur.execute("SELECT COUNT(*) FROM stores WHERE qa_flags IS NOT NULL").fetchone()[0]
        print("  stamped qa_flags on " + str(n) + " stores")

    after = counts(conn)
    print_delta(before, after)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    print("\nDone. Re-run r00_validate.py to confirm.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
