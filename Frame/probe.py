"""
probe.py - visit each store's Google Maps page, read it, leave.

This is the pass that turns a candidate list into a real one. For every store
it captures the four things we cannot get from any open dataset:

  google_category   what Google itself calls this store. The classification the
                    research group will write rules against, and the only
                    authority that can settle whether an Overture
                    "wholesale_store" is a Costco or a bolt distributor.
  place_id          the identifier the review scraper navigates by. 25,172 of
                    the frame had none, which blocks review collection entirely.
  total_reviews     Google's live review count, which is the correct scrape
                    target. Existing targets are stale, missing, or capped.
  business_status   permanently/temporarily closed.

It deliberately does NOT open the reviews tab or scroll. That is where all the
time goes in the normal scraper; skipping it is what makes ~17k stores a
one-to-two day job instead of a month.

Resumable by construction: state lives in SQLite, every store is claimed with
BEGIN IMMEDIATE, and an interrupted store resets to pending on restart. Kill it
and rerun it as often as you like.

    python Frame/probe.py --init                     # build probe.db from probe_list.csv
    python Frame/probe.py --run --limit 500          # smoke test
    python Frame/probe.py --run                      # the full pass
    python Frame/probe.py --stats
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import re
import socket
import sqlite3
import sys
import time
from pathlib import Path

from rapidfuzz import fuzz

HERE = Path(__file__).parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "Scraper_v2"))

DB_PATH = HERE / "build" / "probe.db"
LIST_PATH = HERE / "build" / "probe_list.csv"

SCHEMA = """
CREATE TABLE IF NOT EXISTS probe (
    store_id          INTEGER PRIMARY KEY,
    name              TEXT,
    street            TEXT,
    city              TEXT,
    state             TEXT DEFAULT 'NC',
    zip               TEXT,
    county            TEXT,
    lat               REAL,
    lon               REAL,
    place_id_in       TEXT,
    food_access_type  TEXT,
    status            TEXT DEFAULT 'pending',
    attempts          INTEGER DEFAULT 0,
    -- captured from Google
    g_name            TEXT,
    g_category        TEXT,
    g_address         TEXT,
    g_phone           TEXT,
    g_website         TEXT,
    g_rating          REAL,
    g_total_reviews   INTEGER,
    g_place_id        TEXT,
    g_feature_id      TEXT,
    g_lat             REAL,
    g_lon             REAL,
    g_closed          INTEGER,
    verified          INTEGER,
    name_sim          INTEGER,
    error             TEXT,
    probed_at         TEXT,
    machine_id        TEXT
);
CREATE INDEX IF NOT EXISTS idx_probe_status ON probe(status);
CREATE INDEX IF NOT EXISTS idx_probe_cat    ON probe(g_category);
"""


def connect(path=DB_PATH):
    c = sqlite3.connect(path, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.row_factory = sqlite3.Row
    return c


def cmd_init(args):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = connect()
    c.executescript(SCHEMA)
    rows = []
    with open(LIST_PATH, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            def f(k):
                v = r.get(k, "")
                return None if v in ("", "nan", "None") else v
            rows.append((
                int(float(r["store_id"])), f("name"), f("street"), f("city"),
                "NC", f("zip"), f("county"),
                float(r["lat"]) if f("lat") else None,
                float(r["lon"]) if f("lon") else None,
                f("place_id"), f("food_access_type"),
            ))
    c.executemany("""INSERT OR IGNORE INTO probe
        (store_id,name,street,city,state,zip,county,lat,lon,place_id_in,food_access_type)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""", rows)
    c.commit()
    n = c.execute("SELECT COUNT(*) FROM probe").fetchone()[0]
    have = c.execute("SELECT COUNT(*) FROM probe WHERE place_id_in IS NOT NULL").fetchone()[0]
    print(f"probe.db ready: {n:,} stores ({have:,} already carry a place_id)")
    c.close()


def release_stranded(c, machine_id):
    """Return this machine's crashed claims to the queue.

    A store claimed by a run that was killed stays 'in_progress' forever and is
    never re-claimed, because claim() only selects 'pending'. The supervisor
    restarts on crash, so without this every crash would strand a store. Only
    rows this machine claimed are touched: two probes sharing one queue file
    must not release each other's live work.
    """
    n = c.execute(
        "UPDATE probe SET status='pending' "
        "WHERE status='in_progress' AND machine_id=?", (machine_id,)).rowcount
    c.commit()
    return n


def claim(c, machine_id, n=1):
    """Claim n pending stores atomically."""
    cur = c.cursor()
    cur.execute("BEGIN IMMEDIATE")
    rows = cur.execute(
        "SELECT * FROM probe WHERE status='pending' ORDER BY store_id LIMIT ?",
        (n,)).fetchall()
    if rows:
        cur.executemany("UPDATE probe SET status='in_progress', machine_id=?, "
                        "attempts=attempts+1 WHERE store_id=?",
                        [(machine_id, r["store_id"]) for r in rows])
    c.commit()
    return rows


def store_for_scraper(r):
    """Adapt a probe row to the shape navigate_to_store/verify_store expect.

    gmaps_address must be filled in. navigate_to_store's best text strategy is
    `name + gmaps_address`; with it empty the search degrades to name + city,
    and "Harris Teeter Raleigh NC" is genuinely ambiguous - Google ranked a
    Whole Foods first. In the first 106 stores that produced 21 unverified
    rows, 20 of which had no place_id to navigate by. Giving the search the
    street is the difference between naming a store and describing a region.
    """
    parts = [p for p in (r["street"], r["city"], "NC", r["zip"]) if p]
    full_addr = ", ".join(str(p) for p in parts)
    return {
        "store_id": r["store_id"],
        "input_name": r["name"], "gmaps_name": r["name"],
        "input_street": r["street"], "gmaps_address": full_addr,
        "input_city": r["city"], "input_state": "NC", "input_zip": r["zip"],
        "input_lat": r["lat"], "input_lon": r["lon"],
        "gmaps_lat": None, "gmaps_lon": None,
        "google_place_id": r["place_id_in"] or "",
        "target_reviews": 0, "master_reviews": 0,
    }


async def probe_one(scraper, r):
    """Visit one store. Returns a dict of what Google said."""
    out = {"verified": 0, "error": None}
    store = store_for_scraper(r)
    if not await scraper.navigate_to_store(store):
        out["error"] = "navigation_failed"
        return out
    if await scraper.check_and_handle_blocks():
        out["error"] = "blocked"
        return out
    try:
        out["verified"] = 1 if await scraper.verify_store(store) else 0
    except Exception as e:                                   # noqa: BLE001
        out["verified"] = 0
        out["error"] = f"verify:{type(e).__name__}"
    info = await scraper.extract_business_info()
    out.update({
        "g_name": info.get("name") or None,
        "g_category": (info.get("category") or "").strip() or None,
        "g_address": info.get("address") or None,
        "g_phone": info.get("phone") or None,
        "g_website": info.get("website") or None,
        "g_rating": info.get("overall_rating"),
        "g_total_reviews": info.get("total_reviews") or 0,
    })
    try:
        out["g_place_id"] = await scraper._extract_page_place_id()
    except Exception:                                        # noqa: BLE001
        out["g_place_id"] = None
    try:
        out["g_feature_id"] = await scraper._extract_page_feature_id()
    except Exception:                                        # noqa: BLE001
        out["g_feature_id"] = None
    try:
        ll = await scraper._extract_page_latlon()
        out["g_lat"], out["g_lon"] = (ll if ll else (None, None))
    except Exception:                                        # noqa: BLE001
        out["g_lat"] = out["g_lon"] = None
    try:
        out["g_closed"] = 1 if await scraper.is_permanently_closed() else 0
    except Exception:                                        # noqa: BLE001
        out["g_closed"] = None

    # Record how well the page name agrees with what we asked for, independently
    # of verify_store's overall pass/fail. verify_store also weighs distance, so
    # a real store whose coordinates are a couple of km out fails it while the
    # name plainly matches. Keeping the similarity means those rows stay usable
    # for the category census instead of being thrown away with the genuinely
    # wrong ones - which matters because a store with no place_id can only ever
    # be reached by text search.
    def _n(x):
        x = re.sub(r"[^a-z0-9 ]", " ", str(x or "").lower())
        return re.sub(r"s+", " ", x).strip()
    out["name_sim"] = int(fuzz.token_set_ratio(_n(r["name"]), _n(out.get("g_name"))))
    return out


def save(c, store_id, out, machine_id):
    """Record the visit.

    A page we landed on but could NOT verify is stored as 'unverified', never
    'done'. The smoke test showed why: "Food Lion Grocery Store of Reidsville"
    text-searched to a Whole Foods 32 km away, and "Harris Teeter" in Raleigh
    to a different Whole Foods 2.75 km away. Verification caught both. If those
    rows were marked done, their categories would enter the category census as
    though Google had called those stores Whole Foods - which is precisely the
    wrong-store defect that produced the 323 crossed place_ids in the first
    place. The data is kept for inspection; it is just not treated as evidence.
    """
    got_page = out.get("g_category") or out.get("g_name")
    if not got_page:
        status = "failed"
    elif out.get("verified") or (out.get("name_sim") or 0) >= 85:
        # Either the full verifier passed, or the page name is an unambiguous
        # match for what we asked for. The second case covers a correct store
        # whose recorded coordinates are off by more than the distance veto
        # allows, which is common for stores that only have a text search.
        status = "done"
    else:
        status = "unverified"
    c.execute("""UPDATE probe SET
        status=?, g_name=?, g_category=?, g_address=?, g_phone=?, g_website=?,
        g_rating=?, g_total_reviews=?, g_place_id=?, g_feature_id=?, g_lat=?, g_lon=?,
        g_closed=?, verified=?, name_sim=?, error=?,
        probed_at=datetime('now'), machine_id=?
        WHERE store_id=?""", (
        status,
        out.get("g_name"), out.get("g_category"), out.get("g_address"),
        out.get("g_phone"), out.get("g_website"), out.get("g_rating"),
        out.get("g_total_reviews"), out.get("g_place_id"),
        out.get("g_feature_id"), out.get("g_lat"),
        out.get("g_lon"), out.get("g_closed"), out.get("verified"),
        out.get("name_sim"), out.get("error"), machine_id, store_id))
    c.commit()


async def run(args):
    from linux_scraper.scraper import GoogleMapsScraper
    from linux_scraper.auth_manager import AuthAccountManager

    # Reuse the scraper's own config loader so the probe runs with exactly the
    # same browser, timeout and block-handling settings as the review scraper.
    from linux_scraper.config import load_config
    config = load_config()
    config.setdefault("browser", {})["headless"] = not args.headed
    config.setdefault("auth", {})

    auth_files = sorted(str(p) for p in (ROOT / "Auth").glob("google_auth_*.json"))
    config["auth"]["accounts"] = auth_files
    am = AuthAccountManager(auth_files) if auth_files else None
    print(f"auth sessions: {len(auth_files)}")

    c = connect()
    stranded = release_stranded(c, args.machine)
    if stranded:
        print(f"released {stranded} store(s) stranded by a previous run")
    todo = c.execute("SELECT COUNT(*) FROM probe WHERE status='pending'").fetchone()[0]
    limit = args.limit or todo
    print(f"pending {todo:,}; this run will attempt {min(limit, todo):,}")

    scraper = GoogleMapsScraper(config=config, db=None, worker_id=0,
                                auth_manager=am, machine_id=args.machine)
    first_auth = am.get_next_account() if am else None
    await scraper.start_browser(first_auth)

    done = failed = blocked = unverified = 0
    t0 = time.time()
    try:
        for i in range(min(limit, todo)):
            rows = claim(c, args.machine, 1)
            if not rows:
                break
            r = rows[0]
            try:
                out = await probe_one(scraper, r)
            except Exception as e:                            # noqa: BLE001
                out = {"error": f"{type(e).__name__}: {e}", "verified": 0}
            save(c, r["store_id"], out, args.machine)
            if out.get("error") == "blocked":
                blocked += 1
                print("  BLOCKED - backing off 300s", flush=True)
                await asyncio.sleep(300)
            elif (out.get("g_category") or out.get("g_name")) and out.get("verified"):
                done += 1
            elif out.get("g_category") or out.get("g_name"):
                unverified += 1
            else:
                failed += 1
            if (i + 1) % 25 == 0:
                el = time.time() - t0
                print(f"  [{i+1}/{min(limit,todo)}] ok={done} unver={unverified} "
                      f"failed={failed} blocked={blocked}  {el/(i+1):.1f}s/store  "
                      f"eta {(min(limit,todo)-i-1)*el/(i+1)/3600:.1f}h", flush=True)
            await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))
    finally:
        await scraper.close_browser()
        el = time.time() - t0
        print(f"\ndone={done} failed={failed} blocked={blocked} in {el/60:.1f}min "
              f"({el/max(done+failed,1):.1f}s/store)")
        c.close()


def cmd_stats(args):
    c = connect()
    tot = c.execute("SELECT COUNT(*) FROM probe").fetchone()[0]
    print(f"probe.db: {tot:,} stores")
    for r in c.execute("SELECT status, COUNT(*) n FROM probe GROUP BY 1 ORDER BY n DESC"):
        print(f"  {r['status']:12s} {r['n']:7,}")
    d = c.execute("SELECT COUNT(*) FROM probe WHERE status IN ('done','unverified')").fetchone()[0]
    if d:
        pid = c.execute("SELECT COUNT(*) FROM probe WHERE g_place_id IS NOT NULL").fetchone()[0]
        rev = c.execute("SELECT SUM(g_total_reviews) FROM probe WHERE status='done'").fetchone()[0]
        unv = c.execute("SELECT COUNT(*) FROM probe WHERE status='unverified'").fetchone()[0]
        clo = c.execute("SELECT COUNT(*) FROM probe WHERE g_closed=1").fetchone()[0]
        print(f"\n  place_id captured : {pid:,} ({pid/d*100:.1f}% of done)")
        print(f"  reviews available : {rev or 0:,}")
        print(f"  closed stores     : {clo:,}")
        print(f"  unverified        : {unv:,} ({unv/d*100:.1f}%) - landed on a page we could not confirm")
        print("\n  top Google categories so far:")
        # Only VERIFIED pages count toward the category census. An unverified
        # page is a store we may not have actually found.
        for r in c.execute("""SELECT g_category, COUNT(*) n FROM probe
                              WHERE g_category IS NOT NULL AND status='done'
                              GROUP BY 1 ORDER BY n DESC LIMIT 25"""):
            print(f"    {r['n']:6,}  {r['g_category']}")
    c.close()


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--init", action="store_true")
    p.add_argument("--run", action="store_true")
    p.add_argument("--stats", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    # Default to the hostname, never a shared literal: release_stranded() frees
    # this machine's claims, and two probes both called "local" on a shared
    # queue would free each other's live work.
    p.add_argument("--machine", default=socket.gethostname())
    p.add_argument("--headed", action="store_true")
    p.add_argument("--delay-min", type=float, default=1.5)
    p.add_argument("--delay-max", type=float, default=4.0)
    a = p.parse_args()
    if a.init:
        cmd_init(a)
    if a.run:
        asyncio.run(run(a))
    if a.stats:
        cmd_stats(a)
    if not (a.init or a.run or a.stats):
        p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
