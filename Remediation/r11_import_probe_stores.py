"""R11 - Bring stores the Google probe found into master.

merge_results maps a batch's stores onto master by place_id and DROPS the
reviews of any store master does not know. So a store found by the probe
(Frame/build/probe.db) cannot be scraped until it has a row in master. This
script inserts those rows and, for stores master already has, replaces a
stale or capped target with Google's live review count.

WHAT GOES IN
    probe rows with status='done', not permanently closed, carrying a
    canonical ChIJ place_id OR Google's hex feature id (`0x...:0x...`, which
    the scraper navigates by via maps?cid=), whose Google category is one of
    --category. Feature-id-only stores carry the feature id in place_id (the
    column is NOT NULL) and in feature_id; the merge replaces place_id with
    the ChIJ id once the scraper has landed on the page.
    Several probe rows can resolve to one Google place (a chain listed by two
    sources); the row with the best name match wins and the rest are skipped.

WHAT IS LEFT ALONE
    unverified and failed probes, closed stores, rows with no place_id, and
    every category not named. Nothing is ever deleted from master.

USAGE
    python Remediation/r11_import_probe_stores.py --category "Grocery store"
    python Remediation/r11_import_probe_stores.py --category "Grocery store" --apply
    python Remediation/r11_import_probe_stores.py --category "Gas station" "Truck stop" --apply
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib_master import MASTER, backup, connect_ro  # noqa: E402

PROBE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "Frame", "build", "probe.db")
DEFAULT_CAP = 1500


def _clean(value) -> str | None:
    """Trim whitespace and the '.0' that pandas left on ZIP codes."""
    if value is None:
        return None
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text or None


def _status_for(target: int | None, have: int) -> str:
    if target is None or target <= 0:
        return "unknown_target"
    if have >= target:
        return "complete"
    return "not_started" if have == 0 else "incomplete"


def _identity(r) -> str | None:
    """The key a probe row (or master row) is known by: ChIJ id, else feature id."""
    pid = (r["g_place_id"] if "g_place_id" in r.keys() else r["place_id"]) or ""
    if pid.startswith("ChIJ"):
        return pid
    fid = (r["g_feature_id"] if "g_feature_id" in r.keys() else r["feature_id"]) or ""
    return fid.lower() if fid.startswith("0x") else None


def _best_per_place(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    """One probe row per identity: highest name similarity wins."""
    best: dict[str, sqlite3.Row] = {}
    for r in rows:
        key = _identity(r)
        if key and (key not in best or (r["name_sim"] or 0) > (best[key]["name_sim"] or 0)):
            best[key] = r
    return list(best.values())


IDENTITY_COLUMNS = ("google_place_id", "google_feature_id")


def identities_from_csv(path: str) -> set:
    """Google ids (place_id and feature_id, lower-cased) out of an s16 scrape list.

    A file with neither id column is a hard error: silently importing nothing looks
    exactly like "there was nothing to import".
    """
    if not os.path.exists(path):
        sys.exit(f"{path}: no such file (--place-ids-file)")
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        names = set(reader.fieldnames or ())
        if not names & set(IDENTITY_COLUMNS):
            sys.exit(f"{path}: expected one of {IDENTITY_COLUMNS}, found {sorted(names)}")
        out = set()
        for row in reader:
            for col in IDENTITY_COLUMNS:
                value = (row.get(col) or "").strip().lower()
                if value:
                    out.add(value)
    return out


def _probe_rows(probe_path: str, categories: tuple[str, ...],
                identities: set | None = None) -> list[sqlite3.Row]:
    """Open, verified, navigable probe rows, narrowed by category and/or identity.

    Both filters may be given; they intersect. Identity selection exists because the
    Dewey work is a set of individual stores that cuts across Google categories.
    """
    p = connect_ro(probe_path)
    try:
        sql = ("SELECT * FROM probe WHERE status='done' AND COALESCE(g_closed,0)=0 "
               "AND (g_place_id LIKE 'ChIJ%' OR g_feature_id LIKE '0x%')")
        params: list = []
        if categories:
            sql += " AND g_category IN (" + ",".join("?" * len(categories)) + ")"
            params.extend(categories)
        rows = p.execute(sql, tuple(params)).fetchall()
    finally:
        p.close()
    if identities is not None:
        rows = [r for r in rows
                if (r["g_place_id"] or "").lower() in identities
                or (r["g_feature_id"] or "").lower() in identities]
    return _best_per_place(rows)


def _ensure_feature_column(m: sqlite3.Connection) -> None:
    cols = {r[1] for r in m.execute("PRAGMA table_info(stores)")}
    if "feature_id" not in cols:
        m.execute("ALTER TABLE stores ADD COLUMN feature_id TEXT")
        m.commit()


def _new_store_row(store_id: int, r: sqlite3.Row, cap: int, tag: str) -> tuple:
    total = r["g_total_reviews"]
    target = min(int(total), cap) if total is not None else None
    lat = r["g_lat"] if r["g_lat"] is not None else r["lat"]
    lon = r["g_lon"] if r["g_lon"] is not None else r["lon"]
    fid = (r["g_feature_id"] or "").lower() or None
    # master.place_id is NOT NULL; a store known only by feature id carries that
    # id here until the scraper lands on it and the merge backfills the ChIJ id.
    pid = r["g_place_id"] if (r["g_place_id"] or "").startswith("ChIJ") else fid
    return (store_id, pid, fid, _clean(r["g_name"]) or _clean(r["name"]),
            _clean(r["g_address"]), _clean(r["street"]), _clean(r["city"]),
            _clean(r["state"]) or "NC", _clean(r["zip"]), lat, lon, _clean(r["g_phone"]),
            r["g_rating"], target, "OPERATIONAL", r["g_category"], 0,
            _status_for(target, 0), tag)


def import_stores(master_path: str, probe_path: str, categories: tuple[str, ...], *,
                  apply: bool = False, cap: int = DEFAULT_CAP,
                  identities: set | None = None) -> dict:
    """Insert new probe stores into master; refresh targets on known ones."""
    rows = _probe_rows(probe_path, categories, identities=identities)
    m = sqlite3.connect(master_path, timeout=60)
    m.row_factory = sqlite3.Row
    try:
        _ensure_feature_column(m)
        known = {}
        for r in m.execute("SELECT store_id, place_id, feature_id, target_reviews, "
                           "reviews_scraped, category FROM stores"):
            if r["place_id"]:
                known[r["place_id"]] = r
            if r["feature_id"]:
                known[r["feature_id"].lower()] = r
        new = [r for r in rows if _identity(r) not in known]
        old = [r for r in rows if _identity(r) in known]
        stats = {"probe_rows": len(rows), "would_insert": len(new), "would_update": len(old),
                 "inserted": 0, "updated": 0}
        if not apply:
            return stats

        stats["backup"] = backup(master_path, "r11")
        tag = "r11_probe_import_" + date.today().isoformat()
        next_id = (m.execute("SELECT COALESCE(MAX(store_id),0) FROM stores").fetchone()[0]) + 1
        m.execute("BEGIN IMMEDIATE")
        m.executemany(
            "INSERT INTO stores (store_id, place_id, feature_id, store_name, full_address, "
            "street, city, state, zip, source_lat, source_lon, phone, rating, target_reviews, "
            "business_status, category, reviews_scraped, scrape_status, qa_flags) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [_new_store_row(next_id + i, r, cap, tag) for i, r in enumerate(new)])
        stats["inserted"] = len(new)
        for r in old:
            k = known[_identity(r)]
            total = r["g_total_reviews"]
            target = min(int(total), cap) if total is not None else k["target_reviews"]
            have = k["reviews_scraped"] or 0
            m.execute(
                "UPDATE stores SET target_reviews=?, category=COALESCE(category, ?), "
                "feature_id=COALESCE(feature_id, ?), scrape_status=? WHERE store_id=?",
                (target, r["g_category"], (r["g_feature_id"] or "").lower() or None,
                 _status_for(target, have), k["store_id"]))
            stats["updated"] += 1
        m.commit()
        return stats
    finally:
        m.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--category", nargs="+", default=[],
                    help="Google categories to import, e.g. 'Grocery store'")
    ap.add_argument("--place-ids-file", metavar="CSV", default=None,
                    help="import exactly the stores in this CSV's google_place_id / "
                         "google_feature_id columns (written by Frame/s16_dewey_report.py "
                         "--scrape-list); may be combined with --category")
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--probe", default=PROBE)
    ap.add_argument("--cap", type=int, default=DEFAULT_CAP)
    ap.add_argument("--apply", action="store_true", help="write; default is a dry run")
    args = ap.parse_args()
    if not args.category and not args.place_ids_file:
        ap.error("give --category, --place-ids-file, or both")

    identities = identities_from_csv(args.place_ids_file) if args.place_ids_file else None
    stats = import_stores(args.master, args.probe, tuple(args.category),
                          apply=args.apply, cap=args.cap, identities=identities)
    mode = "APPLIED" if args.apply else "DRY RUN"
    selector = f"categories={args.category}" if args.category else ""
    if identities is not None:
        selector += f"  identities={len(identities):,} from {os.path.basename(args.place_ids_file)}"
    print(f"R11 {mode}  {selector}")
    print(f"  probe rows (open, verified, place_id)  {stats['probe_rows']:>7,}")
    print(f"  new to master                           {stats['would_insert']:>7,}"
          f"   inserted {stats['inserted']:,}")
    print(f"  already in master (target refreshed)    {stats['would_update']:>7,}"
          f"   updated {stats['updated']:,}")
    if stats.get("backup"):
        print(f"  backup: {stats['backup']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
