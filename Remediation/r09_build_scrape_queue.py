"""R09 - Build a scrape queue from the current state of master.

The existing batch1.db and batch2.db were generated on 8 May and are now stale:
master has since gained 2,262 recovered reviews, 337 restored stores, corrected
targets on 107 stores and honest statuses everywhere. Scraping against the old
queue would re-collect work already done and skip the stores that were just
made reachable again.

WHAT GOES IN
    Stores that are genuinely outstanding: incomplete, not started, or carrying
    no known target, with a canonical place_id so the scraper can navigate by
    identity rather than by text search.

WHAT IS HELD BACK, AND WHY
    contaminated stores    Anything on the r06 adjudication list. A store whose
                           identity is in question should not have more reviews
                           attached to it until a person has ruled on which of
                           its two contradictory locations is real. Scraping it
                           first would add good reviews to a possibly wrong
                           store and make the eventual cleanup harder.
    closed stores          Permanently closed, per the existing exclusion policy.
    invalid place_ids      Not navigable by identity. A store with Google's
                           feature id (0x...:0x...) but no ChIJ id IS queued:
                           the scraper opens it via maps?cid= and records the
                           ChIJ id it lands on.

TARGETS
    A store with no known target gets `--default-target`, and the scraper
    replaces it with Google's live count on arrival. That is the honest way to
    handle 157 stores whose real size nobody ever recorded: give the scraper a
    budget, let it learn the truth on the page, and never write a guess into
    master.

USAGE
    python Remediation/r09_build_scrape_queue.py --out Data/batches/run1.db
    python Remediation/r09_build_scrape_queue.py --out Data/batches/run1.db --limit 400
    python Remediation/r09_build_scrape_queue.py --split 2 --out Data/batches/run
    python Remediation/r09_build_scrape_queue.py --category "Grocery store" --split 3 --out Data/batches/grocery
"""

from __future__ import annotations

import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Scraper_v2"))

from lib_master import DATA, MASTER, connect_ro                 # noqa: E402
from r11_import_probe_stores import identities_from_csv        # noqa: E402
import r13_rulings as R13                                       # noqa: E402

LEGACY_CAP = 1500        # the cap every earlier batch was built with
from linux_scraper.database import make_db                      # noqa: E402

WORKLIST = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "contamination_worklist.csv")


def held_back_ids(path: str = WORKLIST) -> set:
    """Worklist stores still awaiting a ruling. A filled `ruling` column releases a store."""
    if not os.path.exists(path):
        return set()
    out = set()
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if (row.get("ruling") or "").strip():
                continue
            try:
                out.add(int(row["store_id"]))
            except (KeyError, ValueError):
                continue
    return out


def queue_order(queue: list, by_need: bool = False, by_pct: bool = False) -> list:
    """Order a queue the way the scraper claims from it.

    linux_scraper.database.claim_next_store hands out never-scraped stores first
    (master_reviews = 0), then the largest remaining need. The builder sorts the same
    way so that --limit keeps fresh work rather than the biggest resumes, and every
    --split bin gets a comparable mix instead of one machine taking all the resumes.

    Each entry is (need, row, target, have) as main() builds it. store_id is the third
    key because claim_next_store breaks ties on it too, and ties are common: every store
    given --default-target with nothing scraped lands on the same (group, need).

    by_need (--order need) drops the fresh-first group: biggest need first, full stop.
    The batch then carries claim_order=need so claim_next_store agrees.
    """
    # rows are sqlite3.Row in main(); index access works for Row and dict alike
    if by_pct:      # --order pct: furthest short of its own target first (need / target), bigger need on ties
        return sorted(queue, key=lambda t: (-(t[0] / max(t[2], 1)), -t[0], t[1]["store_id"] or 0))
    return sorted(queue, key=lambda t: (0 if by_need else (1 if t[3] else 0), -t[0],
                                        t[1]["store_id"] or 0))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--out", default=os.path.join(DATA, "batches", "run1.db"))
    ap.add_argument("--split", type=int, default=1,
                    help="write N disjoint batches, cost-balanced, for N machines")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--default-target", type=int, default=200)
    ap.add_argument("--order", choices=("fresh", "need", "pct"), default="fresh",
                    help="fresh: never-scraped stores first (default); need: biggest need first; "
                         "pct: biggest share of the store's own target missing first")
    ap.add_argument("--cap", type=int, default=1500,
                    help="most reviews to collect per store; 0 = no cap. Written to the batch as "
                         "metadata review_cap, which the scraper honours (absent means 1,500)")
    ap.add_argument("--more-than-cap", action="store_true",
                    help="also queue every store Google shows more than 1,500 reviews for and we hold fewer than "
                         "that count, at Google's full count (from the probe), whatever its earlier ruling. "
                         "Use with --cap 0.")
    ap.add_argument("--probe", default=R13.PROBE_DB, help="probe database holding Google's review counts")
    ap.add_argument("--category", nargs="*", default=None,
                    help="only stores whose category is one of these, e.g. 'Grocery store'")
    ap.add_argument("--place-ids-file", metavar="CSV", default=None,
                    help="only stores whose place_id / feature_id appears in this CSV "
                         "(written by Frame/s16_dewey_report.py --scrape-list)")
    ap.add_argument("--by-name", metavar="CSV", default=None,
                    help="CSV with a store_id column: queue these stores with no place_id "
                         "or feature id, whatever their status, so the scraper finds them by "
                         "name at their coordinates (their stored id opens the wrong page)")
    ap.add_argument("--also-ids", type=int, nargs="*", default=[], metavar="STORE_ID",
                    help="also queue these master store_ids even though they are not short of target, "
                         "to be collected in full: the batch row says nothing is held, so r07 gives them "
                         "no skip-list. For a store that holds more than Google shows.")
    ap.add_argument("--ignore-held-back", action="store_true",
                    help="queue stores even if they are on the old r06 contamination worklist "
                         "(its adjudications were settled by later rulings)")
    args = ap.parse_args()

    also = set(args.also_ids)
    by_name = set()
    if args.by_name:
        with open(args.by_name, newline="", encoding="utf-8") as fh:
            by_name = {int(r["store_id"]) for r in csv.DictReader(fh) if r.get("store_id")}

    m = connect_ro(args.master)
    skip = set() if args.ignore_held_back else held_back_ids()
    wanted = identities_from_csv(args.place_ids_file) if args.place_ids_file else None

    cols = {r["name"] for r in m.execute("PRAGMA table_info(stores)")}
    fid_col = "feature_id" if "feature_id" in cols else "NULL AS feature_id"
    # Stores carrying a ruling (r12: out_of_scope, duplicate, bad_place_id, ruled
    # complete) are not scraped.
    ruled = " AND status_reason IS NULL" if "status_reason" in cols else ""
    select_cols = ("store_id, place_id, " + fid_col + ", store_name, street, city, state, zip, "
                   "source_lat, source_lon, full_address, target_reviews, reviews_scraped, "
                   "category, business_status, qa_flags")
    rows = m.execute(
        "SELECT " + select_cols + " FROM stores "
        "WHERE ((place_id LIKE 'ChIJ%' OR feature_id LIKE '0x%') "
        "AND COALESCE(business_status,'') <> 'CLOSED_PERMANENTLY' "
        "AND (target_reviews IS NULL OR target_reviews <= 0 "
        "     OR reviews_scraped < target_reviews)" + ruled
        + (" AND category IN (" + ",".join("?" * len(args.category)) + ")"
           if args.category else "") + ")"
        + (" OR store_id IN (" + ",".join("?" * len(by_name)) + ")" if by_name else "")
        + (" OR store_id IN (" + ",".join("?" * len(also)) + ")" if also else ""),
        tuple(args.category or ()) + tuple(sorted(by_name)) + tuple(sorted(also)),
    ).fetchall()
    beyond = {}              # store_id -> Google's full review count, for stores with reviews past the cap
    if args.more_than_cap:
        live = R13.load_live_counts(args.probe)
        queued_ids = {r["store_id"] for r in rows}
        for r in m.execute("SELECT " + select_cols + " FROM stores WHERE target_reviews >= ? "
                           "AND COALESCE(business_status,'') <> 'CLOSED_PERMANENTLY' "
                           "AND (place_id LIKE 'ChIJ%' OR feature_id LIKE '0x%')", (LEGACY_CAP,)).fetchall():
            g = live.get((r["place_id"] or "").lower()) or live.get((r["feature_id"] or "").lower())
            if g and g > LEGACY_CAP and g > (r["reviews_scraped"] or 0):
                beyond[r["store_id"]] = g
                if r["store_id"] not in queued_ids:
                    rows.append(r)
    m.close()

    queue = []
    held = 0
    off_list = 0
    for r in rows:
        if r["store_id"] in by_name or r["store_id"] in also or r["store_id"] in beyond:
            pass                                     # explicitly asked for: no filters
        elif r["store_id"] in skip:
            held += 1
            continue
        if wanted is not None and r["store_id"] not in by_name and not ({
                (r["place_id"] or "").lower(),
                                        (r["feature_id"] or "").lower()} & wanted):
            off_list += 1
            continue
        target = beyond.get(r["store_id"]) or r["target_reviews"] or 0
        if target > 0:
            target = min(target, args.cap) if args.cap else target       # --cap 0 = no cap
        else:
            target = args.default_target
        have = 0 if r["store_id"] in also else (r["reviews_scraped"] or 0)
        need = max(target - have, 0)
        if need <= 0:
            continue
        queue.append((need, r, target, have))

    # Never-scraped stores first, then longest job first within each group — the same
    # order claim_next_store uses, so --limit and --split cannot contradict the scraper.
    queue = queue_order(queue, by_need=args.order == "need", by_pct=args.order == "pct")
    if args.limit:
        queue = queue[:args.limit]

    print("SCRAPE QUEUE" + ("  categories=" + str(args.category) if args.category else ""))
    print("  outstanding stores          " + format(len(rows), ">7,"))
    print("  held back for adjudication  " + format(held, ">7,"))
    if wanted is not None:
        print("  not on the id list          " + format(off_list, ">7,")
              + "   (list carries " + format(len(wanted), ",") + " ids)")
    print("  queued                      " + format(len(queue), ">7,"))
    print("  reviews to collect          " + format(sum(t[0] for t in queue), ">7,"))
    fresh = sum(1 for t in queue if not t[3])
    print("  never scraped                  " + format(fresh, ">5,")
          + "   resumed " + format(len(queue) - fresh, ">5,")
          + {"need": "   (biggest need first)", "pct": "   (biggest share of target missing first)"}
          .get(args.order, "   (never scraped first)"))
    no_target = sum(1 for t in queue if not (t[1]["target_reviews"] or 0))
    print("  of which have no known target " + format(no_target, ">5,")
          + "  (scraper sets a live target on arrival)")

    # Cost-balanced split: walk longest-first, always adding to the lightest bin.
    bins = [[] for _ in range(max(1, args.split))]
    loads = [0] * len(bins)
    for need, r, target, have in queue:
        i = loads.index(min(loads))
        bins[i].append((r, target, have))
        loads[i] += need

    for idx, bucket in enumerate(bins, start=1):
        path = args.out if args.split == 1 else f"{args.out}{idx}.db"
        if not path.endswith(".db"):
            path += ".db"
        for side in ("", "-wal", "-shm"):
            if os.path.exists(path + side):
                os.remove(path + side)

        db = make_db(path)
        con = db.connect()
        db.init_schema()
        cur = con.cursor()
        for sid, (r, target, have) in enumerate(bucket, start=1):
            cur.execute(
                "INSERT INTO stores (store_id, machine_id, input_name, input_street, "
                "input_city, input_state, input_zip, input_lat, input_lon, "
                "google_place_id, google_feature_id, master_store_id, gmaps_name, "
                "gmaps_address, gmaps_category, status, "
                "target_reviews, master_reviews, reviews_scraped, attempts) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,0,0)",
                (sid, "", r["store_name"], r["street"], r["city"], r["state"], r["zip"],
                 r["source_lat"], r["source_lon"],
                 None if r["store_id"] in by_name else
                 (r["place_id"] if (r["place_id"] or "").startswith("ChIJ") else None),
                 None if r["store_id"] in by_name else r["feature_id"],
                 r["store_id"], r["store_name"],
                 r["full_address"], r["category"], target, have))
        cur.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)")
        cur.execute("INSERT OR REPLACE INTO metadata (key,value) VALUES "
                    "('built_from', ?)", (os.path.basename(args.master),))
        cur.execute("INSERT OR REPLACE INTO metadata (key,value) VALUES ('claim_order', ?)",
                    (args.order,))
        cur.execute("INSERT OR REPLACE INTO metadata (key,value) VALUES ('review_cap', ?)",
                    (str(args.cap),))
        con.commit()
        db.close()
        print("  wrote " + os.path.basename(path) + ": " + format(len(bucket), ",")
              + " stores, " + format(loads[idx - 1], ",") + " reviews of work")

    print("\nNext: seed the skip-list so resumed stores do not re-collect what master has")
    print("  python Remediation/r07_build_known_keys.py --batch <the batch> --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
