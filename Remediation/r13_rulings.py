"""R13 - Second round of rulings (2026-10-02), after r12.

WHAT IT DOES, IN ORDER
  1  targets from the probe
     A store with no target gets Google's live review count from Frame/build/probe.db
     (matched on place_id or feature id). As in r10: a store already holding more
     reviews keeps the larger number, and the 1,500 cap is applied last.
  2  wrong location, verified
     Class C stores (target >= 50, reviews > 3x target). Checked 2026-10-02: the live
     Google count matches the target, and the excess arrived in ONE batch whose text
     describes another store (Food Lion / Aldi reviews on a Dollar General). Rule: a
     single source_db that brought more than 1.5x the store's whole target collected
     another page; its reviews are quarantined. Other batches' reviews stay.
  3  stalled stores ruled complete
     Short by fewer than 10 reviews, or by fewer than 20 when the target is over 100
     (shortfall measured against the 1,500 cap). dewey3/4 re-ran these and Google
     gave nothing more.
  4  accepted flags
     accepted_flags(store_id, check_name, reason, ruled_at) records validator flags a
     person has accepted. r00 drops accepted stores from that check. Today: every
     store currently far from its own address (the check's ZIP-centroid flaw).
  5  out-of-scope stores move to their own file
     Stores marked out_of_scope by r12, with their reviews and quarantined reviews,
     are copied to Data/Step_5_out_of_scope.db, verified, then removed from master.
     Ruling of 2026-10-02 ("move out of scope to a different file"): an explicit
     exception to keeping scope rows in master. Nothing is lost; the copy is checked
     row-for-row before anything leaves master.

USAGE
    python Remediation/r13_rulings.py            # dry run: counts only, rolled back
    python Remediation/r13_rulings.py --apply
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import DATA, MASTER, REPO, backup, connect_ro, connect_rw   # noqa: E402
from r00_validate import geo_inconsistent, scope_to_study                  # noqa: E402
import r12_rulings as R12                                        # noqa: E402

RULING_DATE = "2026-10-02"
CAP = 1500
WRONG_SOURCE_RATIO = 1.5
CLASS_C_MIN_TARGET = 50
CLASS_C_RATIO = 3
STALLED_SHORT = 10
STALLED_SHORT_BIG = 20
BIG_STORE_TARGET = 100
TOLERANCE_TIERS = ((1500, 100), (500, 50), (100, 20), (0, 10))   # owner rule 2026-10-24: (target over, reviews that may be missing)
FAR_FROM_ADDRESS = "far_from_address"
PROBE_DB = os.path.join(REPO, "Frame", "build", "probe.db")
OUT_OF_SCOPE_DB = os.path.join(DATA, "Step_5_out_of_scope.db")
CLOSED_DB = os.path.join(DATA, "Step_5_closed_stores.db")
SPLIT_TABLES = ("stores", "reviews", "quarantined_reviews")


# ── rules (pure) ─────────────────────────────────────────────────────────

def stalled_is_complete(have: int, target: int) -> bool:
    """Owner rule 2026-10-24: completion is measured against the target capped at 1,500 (the stored
    target stays Google's true count). Allowed missing: 10 for a target of 100 or under, 20 over 100,
    50 over 500, 100 over 1,500 (tier chosen by the true target)."""
    effective = min(target, CAP)
    missing = effective - have
    return missing <= 0 or any(target > over and missing <= allowed for over, allowed in TOLERANCE_TIERS)


def target_from_live(live: int, have: int) -> int:
    return min(max(live, have), CAP)


# ── steps ────────────────────────────────────────────────────────────────

def step_targets(con, live_counts: dict) -> int:
    n = 0
    for r in con.execute("SELECT store_id, place_id, feature_id, reviews_scraped FROM stores "
                         "WHERE target_reviews IS NULL OR target_reviews <= 0").fetchall():
        live = live_counts.get((r["place_id"] or "").lower()) or \
            live_counts.get((r["feature_id"] or "").lower())
        if not live or live <= 0:
            continue
        con.execute("UPDATE stores SET target_reviews=? WHERE store_id=?",
                    (target_from_live(live, r["reviews_scraped"] or 0), r["store_id"]))
        n += 1
    return n


def step_wrong_location(con) -> list:
    out = []
    stores = con.execute(
        "SELECT store_id, store_name, target_reviews t FROM stores WHERE status_reason IS NULL "
        "AND target_reviews >= ? AND reviews_scraped > target_reviews * ?",
        (CLASS_C_MIN_TARGET, CLASS_C_RATIO)).fetchall()
    for s in stores:
        for src, n in con.execute("SELECT source_db, COUNT(*) FROM reviews WHERE store_id=? "
                                  "GROUP BY source_db", (s["store_id"],)).fetchall():
            if n <= WRONG_SOURCE_RATIO * s["t"]:
                continue
            ids = [x[0] for x in con.execute(
                "SELECT id FROM reviews WHERE store_id=? AND source_db IS ?", (s["store_id"], src))]
            R12._quarantine(con, ids, f"{src} brought {n} reviews to a store whose live "
                                      f"count is {s['t']}: another store's page")
            out.append({"store": s["store_id"], "name": s["store_name"], "target": s["t"],
                        "source": src, "reviews_quarantined": n})
    return out


def step_stalled(con) -> list:
    out = []
    for r in con.execute("SELECT store_id, store_name, target_reviews t, reviews_scraped n "
                         "FROM stores WHERE status_reason IS NULL AND target_reviews > 0 "
                         "AND reviews_scraped > 0 AND reviews_scraped < MIN(target_reviews, ?)",
                         (CAP,)).fetchall():
        if not stalled_is_complete(r["n"], r["t"]):
            continue
        short = min(r["t"], CAP) - r["n"]
        con.execute("UPDATE stores SET scrape_status='complete', status_reason=? WHERE store_id=?",
                    (f"stalled {short} short of {min(r['t'], CAP)}; Google shows no more; "
                     f"ruled complete {RULING_DATE}", r["store_id"]))
        out.append({"store": r["store_id"], "name": r["store_name"], "target": r["t"],
                    "have": r["n"], "short": short})
    return out


def accept(con, store_ids, check_name: str, reason: str) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS accepted_flags (store_id INTEGER, "
                "check_name TEXT, reason TEXT, ruled_at TEXT, "
                "PRIMARY KEY (store_id, check_name))")
    con.executemany("INSERT OR IGNORE INTO accepted_flags VALUES (?,?,?,?)",
                    [(s, check_name, reason, RULING_DATE) for s in store_ids])


def accepted_ids(con, check_name: str) -> set:
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name='accepted_flags'").fetchone():
        return set()
    return {r[0] for r in con.execute(
        "SELECT store_id FROM accepted_flags WHERE check_name=?", (check_name,))}


QUEUE_ONLY = {"QUEUE", "BY_NAME", "LEAVE"}       # no change in master; the queue builder reads them


def step_store_rulings(con, rows, ruling_date: str = RULING_DATE) -> dict:
    """Per-store decisions from a reviewed CSV (store_id, decision, target, reason).

    OUT -> out_of_scope; DUP:<keeper> -> duplicate, reviews folded into the keeper;
    TARGET -> target from the given live count; UNRULE -> lift an earlier ruling;
    COMPLETE -> ruled complete as it stands (e.g. a shortfall the owner accepts).
    """
    counts = {}
    for r in rows:
        sid, dec, why = int(r["store_id"]), r["decision"].strip(), r["reason"].strip()
        note = f"Claude review {ruling_date}: {why}"
        if dec in ("CLOSED", "CLOSED_PERMANENT", "CLOSED_TEMPORARY"):
            # Owner ruling 2026-10-08: verified-closed stores, and their reviews, leave the
            # dataset. They move to Data/Step_5_closed_stores.db (see split_out_of_scope).
            con.execute("UPDATE stores SET business_status=?, scrape_status='closed', "
                        "status_reason=? WHERE store_id=?",
                        (dec, f"{why}; closed ruling {ruling_date}", sid))
        elif dec == "BAD_ID":
            con.execute("UPDATE stores SET scrape_status='bad_place_id', status_reason=? "
                        "WHERE store_id=?", (note, sid))
        elif dec == "OPEN":
            # Manual check: Google shows the store open. Drop the old closed flag; a count, if
            # given, becomes the target (never below what is already held, capped).
            con.execute("UPDATE stores SET business_status='OPERATIONAL' WHERE store_id=?", (sid,))
            if (r.get("target") or "").strip():
                con.execute("UPDATE stores SET target_reviews=MIN(MAX(?, COALESCE(reviews_scraped,0)), ?) "
                            "WHERE store_id=?", (int(r["target"]), CAP, sid))
        elif dec == "NO_DATA":
            # Open store whose Google page shows no reviews: complete, with no data to collect.
            con.execute("UPDATE stores SET scrape_status='complete', status_reason=? "
                        "WHERE store_id=?", (f"no data: {why}; ruled complete {ruling_date}", sid))
        elif dec == "COMPLETE":
            con.execute("UPDATE stores SET scrape_status='complete', status_reason=? "
                        "WHERE store_id=?", (f"{why}; ruled complete {ruling_date}", sid))
        elif dec == "OUT":
            con.execute("UPDATE stores SET scrape_status='out_of_scope', status_reason=? "
                        "WHERE store_id=?", (note, sid))
        elif dec.startswith("DUP:"):
            keeper = int(dec.split(":", 1)[1])
            R12._fold_duplicate(con, sid, keeper)
            con.execute("UPDATE stores SET scrape_status='duplicate', status_reason=? "
                        "WHERE store_id=?", (f"same store as {keeper}; {note}", sid))
        elif dec == "TARGET":
            con.execute("UPDATE stores SET target_reviews=MIN(MAX(?, COALESCE(reviews_scraped,0)),"
                        " ?) WHERE store_id=?", (int(r["target"]), CAP, sid))
        elif dec == "UNRULE":
            con.execute("UPDATE stores SET status_reason=NULL WHERE store_id=?", (sid,))
        elif dec not in QUEUE_ONLY:
            raise ValueError(f"store {sid}: unknown decision {dec!r}")
        key = "DUP" if dec.startswith("DUP:") else dec
        counts[key] = counts.get(key, 0) + 1
    return counts


def apply_all(con, live_counts: dict, accept_far_ids) -> dict:
    con.row_factory = sqlite3.Row
    R12.ensure_schema(con)
    targets = step_targets(con, live_counts)
    R12.recompute(con)
    wrong = step_wrong_location(con)
    R12.recompute(con)
    stalled = step_stalled(con)
    accept(con, accept_far_ids, FAR_FROM_ADDRESS,
           "accepted 2026-10-02: ZIP-centroid flaw in the check, store judged correct")
    R12.recompute(con)
    return {"targets_filled": targets, "wrong_location": wrong, "stalled_complete": stalled,
            "far_accepted": len(set(accept_far_ids))}


# ── out-of-scope split ───────────────────────────────────────────────────

def _create_like(con, table: str) -> None:
    ddl = con.execute("SELECT sql FROM main.sqlite_master WHERE type='table' AND name=?",
                      (table,)).fetchone()[0]
    ddl = ddl.replace(f'CREATE TABLE "{table}"', f"CREATE TABLE {table}", 1)
    con.execute(ddl.replace(f"CREATE TABLE {table}", f"CREATE TABLE IF NOT EXISTS oos.{table}", 1))


def split_out_of_scope(master_path: str, out_path: str, status: str = "out_of_scope") -> dict:
    """Copy stores with this scrape_status and their reviews to out_path, verify, remove from master."""
    con = sqlite3.connect(master_path, timeout=60)
    con.execute("ATTACH DATABASE ? AS oos", (out_path,))
    ids_sql = "SELECT store_id FROM main.stores WHERE scrape_status='" + status + "'"
    tables = [t for t in SPLIT_TABLES if con.execute(
        "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()]
    for t in tables:
        _create_like(con, t)
    con.execute("BEGIN IMMEDIATE")
    moved = {}
    for t in tables:
        con.execute(f"INSERT OR IGNORE INTO oos.{t} SELECT * FROM main.{t} "
                    f"WHERE store_id IN ({ids_sql})")
        want = con.execute(f"SELECT COUNT(*) FROM main.{t} WHERE store_id IN ({ids_sql})"
                           ).fetchone()[0]
        have = con.execute(f"SELECT COUNT(*) FROM oos.{t} WHERE store_id IN ({ids_sql})"
                           ).fetchone()[0]
        if have != want:
            con.rollback()
            raise RuntimeError(f"{t}: copied {have} of {want} rows; master left unchanged")
        moved[t] = want
    for t in reversed(tables):                       # child rows before their stores
        con.execute(f"DELETE FROM main.{t} WHERE store_id IN ({ids_sql})")
    con.commit()
    con.execute("DETACH DATABASE oos")
    con.close()
    return moved


# ── inputs ───────────────────────────────────────────────────────────────

def load_live_counts(path: str = PROBE_DB) -> dict:
    out = {}
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    for pid, fid, n in con.execute("SELECT g_place_id, g_feature_id, g_total_reviews FROM probe "
                                   "WHERE status='done' AND g_total_reviews > 0"):
        for k in (pid, fid):
            if k:
                out[k.lower()] = n
    con.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--out-of-scope-db", default=OUT_OF_SCOPE_DB)
    ap.add_argument("--closed-db", default=CLOSED_DB)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--ruling-date", default=RULING_DATE,
                    help="date stamped on --store-rulings decisions (YYYY-MM-DD)")
    ap.add_argument("--store-rulings", metavar="CSV", default=None,
                    help="per-store decisions (store_id,decision,target,reason), e.g. "
                         "Remediation/no_target_rulings_2026-10-02.csv")
    args = ap.parse_args()

    if args.apply:
        print("backup: " + backup(args.master, "r13"))
    # Accept what the validator reports: it measures in-scope stores only, and the
    # ZIP centroids it builds from them differ from a whole-master pass.
    ro = connect_ro(args.master)
    scope_to_study(ro)
    far = [g["store_id"] for g in geo_inconsistent(ro)]
    ro.close()
    con = connect_rw(args.master)
    con.row_factory = sqlite3.Row
    con.execute("BEGIN IMMEDIATE")
    res = apply_all(con, load_live_counts(), far)
    if args.store_rulings:
        with open(args.store_rulings, newline="", encoding="utf-8") as fh:
            print("store rulings: " + str(step_store_rulings(
                con, list(csv.DictReader(fh)), ruling_date=args.ruling_date)))
        R12.recompute(con)
    oos = con.execute("SELECT COUNT(*) FROM stores WHERE scrape_status='out_of_scope'").fetchone()[0]
    print(f"targets filled from probe     {res['targets_filled']:>6,}")
    print(f"wrong-location batches        {len(res['wrong_location']):>6,}  "
          f"({sum(w['reviews_quarantined'] for w in res['wrong_location']):,} reviews quarantined)")
    print(f"stalled stores ruled complete {len(res['stalled_complete']):>6,}")
    print(f"far-from-address accepted     {res['far_accepted']:>6,}")
    print(f"out-of-scope stores to move   {oos:>6,}")
    closed = con.execute("SELECT COUNT(*) FROM stores WHERE scrape_status='closed'").fetchone()[0]
    print(f"closed stores to move         {closed:>6,}")
    if not args.apply:
        con.rollback()
        con.close()
        print("\nDRY RUN - rolled back. Re-run with --apply.")
        return 0
    con.commit()
    con.close()
    moved = split_out_of_scope(args.master, args.out_of_scope_db)
    print("moved to " + args.out_of_scope_db + ": " + str(moved))
    print("closed stores moved to " + args.closed_db + ": "
          + str(split_out_of_scope(args.master, args.closed_db, status="closed")))
    for w in res["wrong_location"]:
        print(f"  wrong location: {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
