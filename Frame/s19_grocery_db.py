"""
s19_grocery_db.py - the grocery-store database keyed on Dewey POIs.

The ruled POI list (s18 -> dewy data/catalog/final_poi_list.parquet) becomes the store
table; every master store linked to one of those POIs (s17's link) brings its reviews
with it. Master is only read, never written.

    Data/Step_6_grocery_pois.db
      pois      one row per POI: Dewey identity, the master store it overlaps (if any),
                Google ids, review target (Google's count, else the chain's median,
                else DEFAULT_TARGET; capped at REVIEW_CAP), reviews held and still
                needed, status, and scrape_hold (why a POI is kept out of batches)
      reviews   every review, keyed on placekey, with where it came from
                ('master' or 'batch:<file>')
      metadata  build facts

Commands (run from the repo root)
  python Frame/s19_grocery_db.py build            # (re)build from the final list + master
  python Frame/s19_grocery_db.py sitrep           # -> Data/reports/grocery_sitrep_<date>.xlsx
  python Frame/s19_grocery_db.py outscope         # -> Data/reports/Out_of_Scope_Stores_and_Reviews_<date>.xlsx
  python Frame/s19_grocery_db.py batches --n 2    # untouched POIs -> Data/batches/grocery_new<i>.db
  python Frame/s19_grocery_db.py merge <batch.db> [...]   # returned batches, matched by placekey

Untouched POIs have no Google place_id; the scraper searches by name at the POI's
coordinates, refuses a page whose name does not match, and records the place_id it lands
on. Each batch row carries dewey_placekey so merge can put reviews back on the right POI.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "Scraper_v2"))
import s18_poi_rulings as s18  # noqa: E402

FINAL_PARQUET = s18.FINAL_PARQUET
MASTER_DB = ROOT / "Data" / "Step_5_master_reviews.db"
GROCERY_DB = ROOT / "Data" / "Step_6_grocery_pois.db"
BATCH_PREFIX = ROOT / "Data" / "batches" / "grocery_new"
REPORTS = ROOT / "Data" / "reports"
BACKUPS = ROOT / "Data" / "backups"

REVIEW_CAP = 1500          # the scraper's per-store cap
# Good-enough tolerance, shared with the scraper (linux_scraper.strategy.done_at).
from linux_scraper.strategy import BIG_STORE, BIG_STORE_SLACK, SMALL_STORE_SLACK  # noqa: E402
DEFAULT_TARGET = 200       # no Google count and no chain history (r09's default)
DUP_RADIUS_M = 75.0
EARTH_RADIUS_M = 6_371_000.0
ST_COMPLETE, ST_INCOMPLETE, ST_NOT_STARTED, ST_NEW = "complete", "incomplete", "not started", "new"
# SafeGraph lists these franchise supermarkets twice: once under the store's name, once as "IGA".
DUP_CHAIN_PAIRS = frozenset(frozenset(p) for p in [
    (s18.chain_key("Carlie C's"), s18.chain_key("IGA")),
    (s18.chain_key("Fresh Foods"), s18.chain_key("IGA")),
])

SCHEMA = """
CREATE TABLE pois (
    placekey         TEXT PRIMARY KEY,
    name             TEXT, brand TEXT, chain TEXT,
    top_category     TEXT, sub_category TEXT, naics_code TEXT,
    street_address   TEXT, city TEXT, postal_code TEXT, latitude REAL, longitude REAL,
    phone            TEXT, website TEXT,
    master_store_id  INTEGER,            -- the old store this POI overlaps, if any
    match_grade      TEXT,               -- s17 link grade A-D
    google_place_id  TEXT, google_feature_id TEXT,
    target_reviews   INTEGER, target_source TEXT,
    reviews_held     INTEGER DEFAULT 0, reviews_needed INTEGER,
    status           TEXT,
    scrape_hold      TEXT,               -- why this POI is kept out of scrape batches
    last_batch       TEXT
);
CREATE TABLE reviews (
    id INTEGER PRIMARY KEY,
    placekey TEXT NOT NULL REFERENCES pois(placekey),
    source TEXT NOT NULL,                -- 'master' or 'batch:<file>'
    master_review_id INTEGER, master_store_id INTEGER,
    review_id TEXT, reviewer_name TEXT, rating INTEGER, date_relative TEXT, review_text TEXT,
    has_owner_response INTEGER, reviewer_review_count INTEGER, reviewer_photo_count INTEGER,
    is_local_guide INTEGER, scraped_at TEXT, machine_id TEXT, source_db TEXT
);
CREATE INDEX idx_reviews_placekey ON reviews(placekey);
CREATE INDEX idx_reviews_rid ON reviews(placekey, review_id);
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
"""
REVIEW_COLS = ["review_id", "reviewer_name", "rating", "date_relative", "review_text", "has_owner_response",
               "reviewer_review_count", "reviewer_photo_count", "is_local_guide", "scraped_at", "machine_id"]


# --------------------------------------------------------------------------- pure helpers
def _house(addr) -> str | None:
    m = pd.Series([addr]).astype(str).str.extract(r"^\s*(\d+)")[0].iloc[0]
    return m if isinstance(m, str) else None


def probable_duplicates(pois: pd.DataFrame) -> dict[str, str]:
    """{duplicate placekey: placekey kept} for the same store listed twice.

    Same house number within DUP_RADIUS_M, and either the same chain or a known
    name/IGA pair. The POI linked to our database is kept; two linked POIs are left
    alone (both already hold reviews), and between two unlinked ones the first is kept.
    """
    from sklearn.neighbors import BallTree

    df = pois.reset_index(drop=True)
    pts = np.radians(df[["latitude", "longitude"]].to_numpy(dtype=float))
    near = BallTree(pts, metric="haversine").query_radius(pts, r=DUP_RADIUS_M / EARTH_RADIUS_M)
    chain = df.location_name.map(s18.chain_key).to_numpy()
    house = df.street_address.map(_house).to_numpy()
    linked = df.our_store_id.notna().to_numpy()
    keys = df.placekey.to_numpy()
    out: dict[str, str] = {}
    for i, js in enumerate(near):
        for j in js:
            if j <= i or house[i] is None or house[i] != house[j]:
                continue
            if not (chain[i] == chain[j] or frozenset((chain[i], chain[j])) in DUP_CHAIN_PAIRS):
                continue
            if linked[i] and linked[j]:
                continue
            keep, drop = (i, j) if linked[i] or (not linked[j] and keys[i] < keys[j]) else (j, i)
            if keys[drop] not in out and keys[keep] not in out:
                out[keys[drop]] = keys[keep]
    return out


def estimate_targets(pois: pd.DataFrame) -> pd.Series:
    """Review target per POI: Google's count where known, else the chain's median, else
    DEFAULT_TARGET; always capped at REVIEW_CAP."""
    known = pd.to_numeric(pois.our_target_reviews, errors="coerce")
    known = known.where(known > 0).clip(upper=REVIEW_CAP)
    chain = pois.location_name.map(s18.chain_key)
    median = known.groupby(chain).median()
    return known.fillna(chain.map(median)).fillna(DEFAULT_TARGET).clip(upper=REVIEW_CAP)


def _target_source(pois: pd.DataFrame) -> pd.Series:
    known = pd.to_numeric(pois.our_target_reviews, errors="coerce") > 0
    chain = pois.location_name.map(s18.chain_key)
    has_chain = chain.isin(set(chain[known]))
    return pd.Series(np.where(known, "google", np.where(has_chain, "chain median", "default")), index=pois.index)


def _balanced_split(weights: list[float], n: int) -> list[list[int]]:
    """Indices into weights, heaviest first, each to the lightest bin (as r09 does)."""
    bins: list[list[int]] = [[] for _ in range(n)]
    loads = [0.0] * n
    for i in sorted(range(len(weights)), key=lambda k: -weights[k]):
        b = loads.index(min(loads))
        bins[b].append(i)
        loads[b] += weights[i]
    return bins


# --------------------------------------------------------------------------- database
def _done_at_sql() -> str:
    """strategy.done_at in SQL: within 25 of a target over 100, within 5 otherwise, at least 1."""
    t = f"MIN(target_reviews, {REVIEW_CAP})"
    return (f"(CASE WHEN {t} <= 0 THEN 0 WHEN {t} > {BIG_STORE} THEN MAX({t} - {BIG_STORE_SLACK}, 1) "
            f"ELSE MAX({t} - {SMALL_STORE_SLACK}, 1) END)")


def _refresh(con: sqlite3.Connection) -> None:
    """Recompute reviews held / needed and status from the reviews table. 'Needed' is what
    is left to reach the good-enough mark, not the full target."""
    done = _done_at_sql()
    con.executescript(f"""
        UPDATE pois SET reviews_held = (SELECT COUNT(*) FROM reviews r WHERE r.placekey = pois.placekey);
        UPDATE pois SET reviews_needed = MAX({done} - reviews_held, 0);
        UPDATE pois SET status = CASE
            WHEN reviews_held > 0 AND reviews_held >= {done} THEN '{ST_COMPLETE}'
            WHEN target_reviews = 0 AND last_batch IS NOT NULL THEN '{ST_COMPLETE}'
            WHEN reviews_held > 0 THEN '{ST_INCOMPLETE}'
            WHEN master_store_id IS NOT NULL OR last_batch IS NOT NULL THEN '{ST_NOT_STARTED}'
            ELSE '{ST_NEW}' END;
    """)


def _refuse_if_merged(out: Path) -> None:
    if not out.exists():
        return
    con = sqlite3.connect(f"file:{out.as_posix()}?mode=ro", uri=True)
    try:
        merged = con.execute("SELECT COUNT(*) FROM reviews WHERE source <> 'master'").fetchone()[0]
    except sqlite3.DatabaseError:
        merged = 0
    finally:
        con.close()
    if merged:
        sys.exit(f"{out} already holds {merged:,} reviews merged from batches; rebuilding would drop them. "
                 "Move it aside first if a rebuild is really wanted.")


def _poi_rows(pois: pd.DataFrame, master: Path) -> list[tuple]:
    dup = probable_duplicates(pois)
    targets, sources = estimate_targets(pois), _target_source(pois)
    m = sqlite3.connect(f"file:{Path(master).as_posix()}?mode=ro", uri=True)
    gid = {sid: (pid if (pid or "").startswith("ChIJ") else None, fid)
           for sid, pid, fid in m.execute("SELECT store_id, place_id, feature_id FROM stores")}
    m.close()
    rows = []
    for i, p in pois.iterrows():
        sid = None if pd.isna(p.our_store_id) else int(p.our_store_id)
        pid, fid = gid.get(sid, (None, None))
        rows.append((p.placekey, p.location_name, p.get("brand_name"), s18.chain_key(p.location_name),
                     p.top_category, p.sub_category, p.naics_code, p.street_address, p.city, p.postal_code,
                     float(p.latitude), float(p.longitude), p.get("phone_number"), p.get("website"),
                     sid, p.get("match_grade"), pid, fid, int(round(targets[i])), sources[i],
                     f"probable duplicate of {dup[p.placekey]}" if p.placekey in dup else None))
    return rows


def build_db(final: pd.DataFrame, master: Path, out: Path) -> dict:
    """Write out from the final POI list and master (read-only). Returns build counts."""
    out = Path(out)
    _refuse_if_merged(out)
    for side in ("", "-wal", "-shm"):
        Path(str(out) + side).unlink(missing_ok=True)
    con = sqlite3.connect(f"file:{out.as_posix()}", uri=True)   # uri=True so master attaches read-only
    con.executescript(SCHEMA)
    con.executemany("INSERT INTO pois (placekey, name, brand, chain, top_category, sub_category, naics_code, "
                    "street_address, city, postal_code, latitude, longitude, phone, website, master_store_id, "
                    "match_grade, google_place_id, google_feature_id, target_reviews, target_source, scrape_hold) "
                    "VALUES (" + ",".join("?" * 21) + ")", _poi_rows(final.reset_index(drop=True), master))
    con.commit()
    con.execute(f"ATTACH DATABASE 'file:{Path(master).as_posix()}?mode=ro' AS m")
    master_cols = {r[1] for r in con.execute("PRAGMA m.table_info(reviews)")}
    pick = ", ".join(f"r.{c}" if c in master_cols else f"NULL AS {c}" for c in REVIEW_COLS)
    src = "r.source_db" if "source_db" in master_cols else "NULL"
    con.execute(f"""
        INSERT INTO reviews (placekey, source, master_review_id, master_store_id, {", ".join(REVIEW_COLS)}, source_db)
        SELECT p.placekey, 'master', r.id, r.store_id, {pick}, {src}
        FROM pois p JOIN m.reviews r ON r.store_id = p.master_store_id""")
    con.commit()
    con.execute("DETACH DATABASE m")
    _refresh(con)
    con.executemany("INSERT INTO metadata VALUES (?,?)", [
        ("built_at", datetime.now().isoformat(timespec="seconds")), ("source_pois", FINAL_PARQUET.name),
        ("source_master", Path(master).name), ("review_cap", str(REVIEW_CAP)),
        ("default_target", str(DEFAULT_TARGET))])
    con.commit()
    counts = dict(con.execute("SELECT status, COUNT(*) FROM pois GROUP BY status").fetchall())
    counts["reviews"] = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    counts["held_back_duplicates"] = con.execute("SELECT COUNT(*) FROM pois WHERE scrape_hold IS NOT NULL").fetchone()[0]
    con.close()
    return counts


# --------------------------------------------------------------------------- batches + merge
def write_batches(db: Path, prefix: Path, n: int) -> list[Path]:
    """Untouched POIs (no old store, no reviews, not held) -> n disjoint, cost-balanced batches."""
    from linux_scraper.database import make_db

    con = sqlite3.connect(f"file:{Path(db).as_posix()}?mode=ro", uri=True)
    todo = pd.read_sql("SELECT * FROM pois WHERE master_store_id IS NULL AND reviews_held = 0 "
                       "AND scrape_hold IS NULL AND last_batch IS NULL", con)
    con.close()
    paths = [Path(f"{prefix}{i}.db") for i in range(1, n + 1)]
    existing = [str(p) for p in paths if p.exists()]
    if existing:
        sys.exit(f"refusing to overwrite batch files that may hold scraped work: {existing}")
    paths[0].parent.mkdir(parents=True, exist_ok=True)
    for path, idx in zip(paths, _balanced_split(todo.target_reviews.astype(float).tolist(), n)):
        mdb = make_db(str(path))
        bcon = mdb.connect()
        mdb.init_schema()
        bcon.execute("ALTER TABLE stores ADD COLUMN dewey_placekey TEXT")
        for sid, r in enumerate(todo.iloc[idx].itertuples(), start=1):
            address = f"{r.street_address}, {r.city}, NC {r.postal_code}"
            bcon.execute(
                "INSERT INTO stores (store_id, machine_id, input_name, input_street, input_city, input_state, "
                "input_zip, input_lat, input_lon, google_place_id, google_feature_id, master_store_id, gmaps_name, "
                "gmaps_address, gmaps_category, status, target_reviews, master_reviews, reviews_scraped, attempts, "
                "dewey_placekey) VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,?,?,?,'pending',?,0,0,0,?)",
                (sid, "", r.name, r.street_address, r.city, "NC", r.postal_code, r.latitude, r.longitude,
                 r.name, address, r.sub_category, int(r.target_reviews), r.placekey))
        bcon.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('built_from', ?)", (Path(db).name,))
        bcon.commit()
        mdb.close()
    return paths


def _held_keys(con: sqlite3.Connection, placekey: str) -> tuple[set, set]:
    ids, content = set(), set()
    for rid, name, rating, text in con.execute(
            "SELECT review_id, reviewer_name, rating, review_text FROM reviews WHERE placekey=?", (placekey,)):
        if rid:
            ids.add(rid)
        content.add(((name or "").strip(), rating, (text or "").strip()))
    return ids, content


def merge_batch(batch: Path, db: Path) -> dict:
    """Returned batch -> grocery db, by dewey_placekey. Idempotent: a review already held
    for the POI (same review_id, or same reviewer + rating + text) is skipped."""
    b = sqlite3.connect(f"file:{Path(batch).as_posix()}?mode=ro", uri=True)
    b.row_factory = sqlite3.Row
    stores = {r["store_id"]: dict(r) for r in b.execute("SELECT * FROM stores WHERE dewey_placekey IS NOT NULL")}
    reviews = [dict(r) for r in b.execute("SELECT * FROM reviews")]
    b.close()
    con = sqlite3.connect(db)
    tag = f"batch:{Path(batch).name}"
    held = {pk: _held_keys(con, pk) for pk in {s["dewey_placekey"] for s in stores.values()}}
    new = dup = 0
    insert = (f"INSERT INTO reviews (placekey, source, {', '.join(REVIEW_COLS)}) "
              f"VALUES (?,?,{','.join('?' * len(REVIEW_COLS))})")
    for r in reviews:
        s = stores.get(r["store_id"])
        if not s:
            continue
        ids, content = held[s["dewey_placekey"]]
        key = ((r.get("reviewer_name") or "").strip(), r.get("rating"), (r.get("review_text") or "").strip())
        if (r.get("review_id") and r["review_id"] in ids) or key in content:
            dup += 1
            continue
        con.execute(insert, (s["dewey_placekey"], tag, *[r.get(c) for c in REVIEW_COLS]))
        if r.get("review_id"):
            ids.add(r["review_id"])
        content.add(key)
        new += 1
    for s in stores.values():
        if s["status"] == "pending" and not s.get("reviews_scraped"):
            continue                                   # never visited: leave the POI as it was
        con.execute("UPDATE pois SET last_batch=?, google_place_id=COALESCE(?, google_place_id), "
                    "google_feature_id=COALESCE(?, google_feature_id) WHERE placekey=?",
                    (tag, s.get("google_place_id"), s.get("google_feature_id"), s["dewey_placekey"]))
        live = s.get("gmaps_reviews_count")
        if live is not None and live >= 0:
            con.execute("UPDATE pois SET target_reviews=?, target_source='google' WHERE placekey=?",
                        (min(int(live), REVIEW_CAP), s["dewey_placekey"]))
    con.commit()
    _refresh(con)
    con.commit()
    con.close()
    return {"stores": len(stores), "new_reviews": new, "duplicates": dup}


# --------------------------------------------------------------------------- out of scope
IN_LIST = "in list"
REASON_CLOSED = "POI in scope but closed on SafeGraph"
REASON_UNMATCHED = "no Dewey POI matched (needs a decision)"
OUTSCOPE_COLUMNS = ["store_id", "store_name", "full_address", "city", "category", "scrape_status",
                    "target_reviews", "reviews_in_db", "reason", "placekey", "dewey_name", "dewey_top_category",
                    "dewey_sub_category", "match_grade"]


def explain_decisions(catalog: pd.DataFrame, rulings: list[dict]) -> dict[str, str]:
    """placekey -> IN_LIST or the reason it is out, for every POI linked to a master store.
    Mirrors s18.apply_rulings' precedence exactly."""
    ruled = s18._latest(rulings)
    df = s18._shape(catalog)
    df = df[df.our_store_id.notna()]
    chain_sugg = s18._shape(catalog).pipe(lambda d: d[d.chain.notna()]).groupby("chain").sub_suggestion.agg(
        s18._chain_suggestion).to_dict()
    label = {s18.YES: IN_LIST, s18.MAYBE_RULING: "left undecided"}
    out: dict[str, str] = {}
    for r in df.itertuples():
        if ("poi", r.placekey) in ruled:
            v = ruled[("poi", r.placekey)]
            out[r.placekey] = label.get(v, "store ruled out individually")
        elif ("top", r.top_category) in ruled:
            v = ruled[("top", r.top_category)]
            out[r.placekey] = label.get(v, f"top category ruled out: {r.top_category}")
        elif not r.is_food:
            out[r.placekey] = f"not food retail: {r.top_category}"
        elif isinstance(r.chain, str):
            v = ruled.get(("chain", r.chain))
            out[r.placekey] = (label.get(v, f"chain ruled out: {r.location_name}") if v else
                               f"chain out by default: {r.location_name}" if chain_sugg.get(r.chain) == s18.NO
                               else "left undecided")
        else:
            v = ruled.get(("sub", r.sub_key))
            out[r.placekey] = (label.get(v, f"independent stores ruled out: {r.sub_category}") if v else
                               f"category out by default: {r.sub_category} ({r.sub_reason})"
                               if r.sub_suggestion == s18.NO else "left undecided")
    return out


def out_of_scope(master: pd.DataFrame, catalog: pd.DataFrame, decided: dict[str, str]) -> pd.DataFrame:
    """Every master store not in the grocery list, one reason each."""
    linked = catalog[catalog.our_store_id.notna()].assign(store_id=lambda d: d.our_store_id.astype(int))
    linked = linked.rename(columns={"location_name": "dewey_name", "top_category": "dewey_top_category",
                                    "sub_category": "dewey_sub_category"})
    keep = [c for c in ["store_id", "placekey", "dewey_name", "dewey_top_category", "dewey_sub_category",
                        "match_grade", "is_closed"] if c in linked.columns]
    m = master.merge(linked[keep], on="store_id", how="left")
    decision = m.placekey.map(decided)
    closed = pd.to_numeric(m.get("is_closed"), errors="coerce").fillna(0) != 0
    m["reason"] = np.where(m.placekey.isna(), REASON_UNMATCHED,
                           np.where((decision == IN_LIST) & closed, REASON_CLOSED, decision.fillna(REASON_UNMATCHED)))
    out = m[m.reason != IN_LIST]
    return out[[c for c in OUTSCOPE_COLUMNS if c in out.columns]].reset_index(drop=True)


def outscope_report(master_db: Path, catalog: pd.DataFrame, rulings: list[dict]) -> dict[str, pd.DataFrame]:
    con = sqlite3.connect(f"file:{Path(master_db).as_posix()}?mode=ro", uri=True)
    stores = pd.read_sql("SELECT store_id, store_name, full_address, city, category, scrape_status, target_reviews "
                         "FROM stores", con)
    counts = pd.read_sql("SELECT store_id, COUNT(*) AS reviews_in_db FROM reviews GROUP BY store_id", con)
    total_reviews = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    con.close()
    master = stores.merge(counts, on="store_id", how="left").fillna({"reviews_in_db": 0})
    out = out_of_scope(master, catalog, explain_decisions(catalog, rulings))
    out["reason_group"] = out.reason.str.split(":").str[0]
    summary = out.groupby("reason_group").agg(stores=("store_id", "size"), reviews=("reviews_in_db", "sum")).sort_values(
        "reviews", ascending=False).reset_index()
    kept = len(master) - len(out)
    head = pd.DataFrame([
        {"reason_group": "MASTER TOTAL", "stores": len(master), "reviews": int(total_reviews)},
        {"reason_group": "stays in scope (in the grocery list)", "stores": kept,
         "reviews": int(master.reviews_in_db.sum() - out.reviews_in_db.sum())},
        {"reason_group": "goes out of scope (all reasons below)", "stores": len(out), "reviews": int(out.reviews_in_db.sum())},
    ])
    detail = out.groupby(["reason_group", "reason"]).agg(stores=("store_id", "size"), reviews=("reviews_in_db", "sum")) \
        .sort_values("reviews", ascending=False).reset_index()
    by_category = out.groupby(["reason_group", "category"], dropna=False).agg(
        stores=("store_id", "size"), reviews=("reviews_in_db", "sum")).sort_values("reviews", ascending=False).reset_index()
    return {"Summary": pd.concat([head, summary], ignore_index=True), "By_Reason": detail,
            "By_Google_Category": by_category, "Stores": out.sort_values(["reason_group", "reviews_in_db"],
                                                                         ascending=[True, False])}


# --------------------------------------------------------------------------- sitrep
def sitrep(db: Path) -> dict[str, pd.DataFrame]:
    con = sqlite3.connect(f"file:{Path(db).as_posix()}?mode=ro", uri=True)
    p = pd.read_sql("SELECT * FROM pois", con)
    by_source = pd.read_sql("SELECT source, COUNT(*) AS reviews FROM reviews GROUP BY source", con)
    con.close()
    p["in_batches"] = (p.status.eq(ST_NEW) & p.scrape_hold.isna()).astype(int)
    agg = dict(pois=("placekey", "size"), reviews_held=("reviews_held", "sum"),
               target=("target_reviews", "sum"), reviews_needed=("reviews_needed", "sum"))
    order = [ST_COMPLETE, ST_INCOMPLETE, ST_NOT_STARTED, ST_NEW]
    status = p.groupby("status").agg(**agg).reindex(order).fillna(0).astype(int).reset_index()
    total = status.drop(columns="status").sum().to_frame().T.assign(status="TOTAL")
    status = pd.concat([status, total], ignore_index=True)
    chain = p.groupby("chain").agg(**agg, new_pois=("in_batches", "sum")).sort_values(
        "reviews_needed", ascending=False).reset_index()
    target_src = p.groupby(["status", "target_source"]).agg(
        pois=("placekey", "size"), reviews_needed=("reviews_needed", "sum")).reset_index()
    held = p[p.scrape_hold.notna()][["placekey", "name", "street_address", "city", "scrape_hold"]]
    return {"by_status": status, "by_chain": chain, "target_source": target_src,
            "reviews_by_source": by_source, "held_back": held}


# --------------------------------------------------------------------------- cli
def _backup(db: Path) -> Path:
    BACKUPS.mkdir(parents=True, exist_ok=True)
    dest = BACKUPS / f"{db.stem}_backup_{datetime.now():%Y%m%d_%H%M%S}.db"
    con = sqlite3.connect(db)
    con.execute("VACUUM INTO ?", (str(dest),))
    con.close()
    return dest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build")
    sub.add_parser("sitrep")
    op = sub.add_parser("outscope")
    op.add_argument("--rulings", default=str(s18.s17.OUT_DIR / "rulings_2026-09-24.json"))
    bp = sub.add_parser("batches")
    bp.add_argument("--n", type=int, default=2)
    bp.add_argument("--out", default=str(BATCH_PREFIX))
    mp = sub.add_parser("merge")
    mp.add_argument("batches", nargs="+")
    args = ap.parse_args(argv)

    if args.cmd == "build":
        if not FINAL_PARQUET.exists():
            sys.exit(f"missing {FINAL_PARQUET} - run s18 apply first")
        print(f"{GROCERY_DB}: {build_db(pd.read_parquet(FINAL_PARQUET), MASTER_DB, GROCERY_DB)}")
    elif args.cmd == "sitrep":
        import s17_dewey_catalog as s17

        rep = sitrep(GROCERY_DB)
        path = REPORTS / f"grocery_sitrep_{date.today():%Y-%m-%d}.xlsx"
        s17.write_workbook(path, list(rep.items()))
        print(rep["by_status"].to_string(index=False))
        print(f"-> {path}")
    elif args.cmd == "outscope":
        import json

        import s17_dewey_catalog as s17

        rulings = json.loads(Path(args.rulings).read_text(encoding="utf-8"))
        rep = outscope_report(MASTER_DB, pd.read_parquet(s17.CATALOG_PARQUET), rulings)
        path = REPORTS / f"Out_of_Scope_Stores_and_Reviews_{date.today():%Y-%m-%d}.xlsx"
        s17.write_workbook(path, list(rep.items()))
        print(rep["Summary"].to_string(index=False))
        print(f"-> {path}")
    elif args.cmd == "batches":
        for pth in write_batches(GROCERY_DB, Path(args.out), args.n):
            c = sqlite3.connect(pth)
            n, t = c.execute("SELECT COUNT(*), SUM(target_reviews) FROM stores").fetchone()
            c.close()
            print(f"{pth}: {n:,} stores, ~{t:,} reviews of work")
    elif args.cmd == "merge":
        print(f"backup: {_backup(GROCERY_DB)}")
        for pth in args.batches:
            print(pth, merge_batch(Path(pth), GROCERY_DB))
    return 0


if __name__ == "__main__":
    sys.exit(main())
