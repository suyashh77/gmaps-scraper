"""R12 - Apply the 2026-10-02 rulings: duplicates, scope, contamination, no-target.

WHAT IT DOES, IN ORDER
  1  duplicate stores
     The same Google listing sits in master twice: once under its ChIJ place_id and
     once under its hex feature id (probe imports), or under two ids at one
     address. One copy is kept (ChIJ id first, then most reviews). Text reviews
     only the other copy holds move to the keeper; everything else on the copy
     goes to quarantined_reviews. The copy is marked 'duplicate'.
  2  scope
     Gas stations, truck stops and convenience stores are out of the study
     (ruling 2026-10-02). Google's category decides; Dewey's food_access_type is
     used only where Google gave no category. Marked 'out_of_scope'; rows and
     reviews stay.
  3  contamination
     Store pairs sharing 20+ identical reviews. Which side collected the other's
     page: the side holding far more than its own target (or having none), else
     the old-pipeline side (store_id <= 5226, name-search era). The wrong side's
     reviews from the batches that carried the shared ones are quarantined.
     If those batches navigated by place_id (September onward), the store's
     place_id itself opens the other store's page: marked 'bad_place_id' and
     not requeued. Otherwise it is requeued. Pairs no rule decides are listed.
  4  no target
     A store with no target that has been attempted twice or more is ruled
     'complete'.

Every ruling sets stores.status_reason. merge_results, r03 and r09 leave stores
with a status_reason alone, so a ruling survives later merges. Nothing is lost:
quarantined_reviews holds every review taken out of reviews, with the reason.

USAGE
    python Remediation/r12_rulings.py            # dry run: counts only, rolled back
    python Remediation/r12_rulings.py --apply    # back up, apply, write Excel report
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sqlite3
import sys
from collections import defaultdict
from math import asin, cos, radians, sin, sqrt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import DATA, MASTER, REPO, backup, connect_rw   # noqa: E402
from r00_validate import cross_store_contamination              # noqa: E402

RULING_DATE = "2026-10-02"
OUT_OF_SCOPE_CATEGORIES = {"Gas station", "Truck stop", "Convenience store"}
OUT_OF_SCOPE_DEWEY = {"gas_convenience", "convenience"}
OLD_PIPELINE_MAX_ID = 5226          # last store loaded before the September probe imports
OVER_TARGET_RATIO = 1.5
MIN_ATTEMPTS = 2
SAME_SPOT_KM = 0.2                  # two pins this close at one address are one store
PLACE_ID_ERA_PREFIXES = ("run1_", "run2_", "grocery", "dewey")
DEWEY_LISTS = [os.path.join(DATA, "batches", f"dewey_scrape_list_{k}.csv")
               for k in ("inscope", "heldback")]
RETURNS_GLOBS = [os.path.join(DATA, "returns", "*", "*.db"),
                 os.path.join(REPO, "Deploy", "results", "*.db")]


# ── rules (pure) ─────────────────────────────────────────────────────────

def is_out_of_scope(category, dewey_type) -> bool:
    if category:
        return category in OUT_OF_SCOPE_CATEGORIES
    return dewey_type in OUT_OF_SCOPE_DEWEY


def _norm(s) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _hex_ids(s: dict) -> set:
    return {v.lower() for v in (s.get("place_id"), s.get("feature_id"))
            if v and v.lower().startswith("0x")}


def _address_key(s: dict, loose: bool):
    addr = s.get("full_address") or ""
    num = re.match(r"\s*(\d+)\b", addr)
    zips = re.findall(r"\b(\d{5})\b", addr)
    words = re.findall(r"[a-z0-9]+", (s.get("store_name") or "").lower())
    if not (num and zips and words):
        return None
    name = words[0] if loose else _norm(s.get("store_name"))
    return (name, num.group(1), zips[-1])


def _same_spot(a: dict, b: dict) -> bool:
    """The same normalized address, or pins within SAME_SPOT_KM (one address spelt two ways).

    Old-pipeline pins can sit far from the building, so identical text is enough.
    """
    if _norm(a.get("full_address")) == _norm(b.get("full_address")):
        return True
    pts = [(s.get("source_lat"), s.get("source_lon")) for s in (a, b)]
    if any(None in p for p in pts):
        return False
    (la1, lo1), (la2, lo2) = pts
    dlat, dlon = radians(la2 - la1), radians(lo2 - lo1)
    h = sin(dlat / 2) ** 2 + cos(radians(la1)) * cos(radians(la2)) * sin(dlon / 2) ** 2
    return 2 * 6371.0 * asin(sqrt(h)) <= SAME_SPOT_KM


def same_store(a: dict, b: dict, loose: bool = False) -> bool:
    """One Google listing: a shared hex feature id, or one name at one house number and
    ZIP on the same spot (the same number recurs on different streets of one ZIP).

    loose compares only the first word of the name and skips the spot test; use it
    only on pairs that already share 20+ identical reviews.
    """
    if _hex_ids(a) & _hex_ids(b):
        return True
    ka, kb = _address_key(a, loose), _address_key(b, loose)
    return ka is not None and ka == kb and (loose or _same_spot(a, b))


def pick_keeper(stores: list) -> int:
    best = min(stores, key=lambda s: (not (s.get("place_id") or "").startswith("ChIJ"),
                                      -(s.get("reviews_scraped") or 0), s["store_id"]))
    return best["store_id"]


def _over_target(s: dict) -> bool:
    t = s.get("target_reviews") or 0
    return t <= 0 or (s.get("reviews_scraped") or 0) > OVER_TARGET_RATIO * t


def wrong_side(a: dict, b: dict):
    """store_id of the store that collected the other's page, or None if no rule decides."""
    oa, ob = _over_target(a), _over_target(b)
    if oa != ob:
        return a["store_id"] if oa else b["store_id"]
    old_a, old_b = a["store_id"] <= OLD_PIPELINE_MAX_ID, b["store_id"] <= OLD_PIPELINE_MAX_ID
    if old_a != old_b:
        return a["store_id"] if old_a else b["store_id"]
    return None


_GENERIC_NAME_WORDS = {"market", "markets", "store", "stores", "inc", "llc", "supermarket",
                       "grocery", "super", "the", "of", "and"}


def _brand(name) -> str:
    words = [w for w in re.sub(r"[^a-z0-9' ]", " ", (name or "").lower().replace("’", "'")).split()
             if w not in _GENERIC_NAME_WORDS]
    return " ".join(words[:2])


def name_wrong_side(a: dict, b: dict, shared_texts: list):
    """store_id of the store that collected the other's page, decided by name and address.

    Owner ruling 2026-10-08: when only a place-id problem is in the way, go by name and
    address. (1) Different brands: the shared reviews name the page's real brand, so the
    store of the other brand holds a page that is not its own. (2) Same brand: the store
    with no Outscraper-verified address was name-searched onto the verified store's page.
    None when neither signal decides.
    """
    ba, bb = _brand(a.get("store_name")), _brand(b.get("store_name"))
    if ba and bb and ba != bb:
        blob = " ".join(shared_texts).lower().replace("’", "'")
        ca, cb = blob.count(ba), blob.count(bb)
        if ca != cb:
            return b["store_id"] if ca > cb else a["store_id"]
        return None
    va, vb = bool(a.get("outscraper_address")), bool(b.get("outscraper_address"))
    if va != vb:
        return b["store_id"] if va else a["store_id"]
    return None


def is_place_id_era(source_db) -> bool:
    return (source_db or "").startswith(PLACE_ID_ERA_PREFIXES)


# ── database steps ───────────────────────────────────────────────────────

def ensure_schema(con) -> None:
    if not any(r[1] == "status_reason" for r in con.execute("PRAGMA table_info(stores)")):
        con.execute("ALTER TABLE stores ADD COLUMN status_reason TEXT")
    con.execute("CREATE TABLE IF NOT EXISTS quarantined_reviews AS SELECT *, "
                "CAST(NULL AS TEXT) AS quarantine_reason, CAST(NULL AS TEXT) AS quarantined_at "
                "FROM reviews WHERE 0")


def _chunks(ids: list, size: int = 500):
    for i in range(0, len(ids), size):
        chunk = ids[i:i + size]
        yield chunk, ",".join("?" * len(chunk))


def _quarantine(con, review_ids: list, reason: str) -> None:
    for chunk, marks in _chunks(review_ids):
        con.execute(f"INSERT INTO quarantined_reviews SELECT *, ?, ? FROM reviews "
                    f"WHERE id IN ({marks})", (reason, RULING_DATE, *chunk))
        con.execute(f"DELETE FROM reviews WHERE id IN ({marks})", chunk)


def _stores(con) -> dict:
    return {r["store_id"]: dict(r) for r in con.execute(
        "SELECT store_id, place_id, feature_id, store_name, full_address, category, "
        "source_lat, source_lon, target_reviews, reviews_scraped, scrape_status, "
        "status_reason, outscraper_address FROM stores")}


def _text_key(r) -> tuple:
    return (r["reviewer_name"] or "", r["rating"], (r["review_text"] or "").strip())


def duplicate_groups(stores: dict, pairs: list) -> list:
    """Groups of store_ids that are one Google listing (union-find)."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    by_hex, by_addr = defaultdict(list), defaultdict(list)
    for s in stores.values():
        for h in _hex_ids(s):
            by_hex[h].append(s["store_id"])
        k = _address_key(s, loose=False)
        if k:
            by_addr[k].append(s["store_id"])
    links = [ids for ids in by_hex.values() if len(ids) > 1]
    links += [[a, b] for ids in by_addr.values() for i, a in enumerate(ids) for b in ids[i + 1:]
              if same_store(stores[a], stores[b])]
    links += [[a, b] for a, b, _ in pairs if same_store(stores[a], stores[b], loose=True)]
    for ids in links:
        for x in ids[1:]:
            parent[find(x)] = find(ids[0])
    groups = defaultdict(list)
    for x in list(parent):
        groups[find(x)].append(x)
    return [sorted(g) for g in groups.values() if len(g) > 1]


def _fold_duplicate(con, dup: int, keeper: int) -> int:
    keys = {_text_key(r) for r in con.execute(
        "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=? "
        "AND TRIM(COALESCE(review_text,'')) <> ''", (keeper,))}
    move, hold = [], []
    for r in con.execute("SELECT id, reviewer_name, rating, review_text FROM reviews "
                         "WHERE store_id=?", (dup,)).fetchall():
        k = _text_key(r)
        if k[2] and k not in keys:
            keys.add(k)
            move.append(r["id"])
        else:
            hold.append(r["id"])
    for chunk, marks in _chunks(move):
        con.execute(f"UPDATE reviews SET store_id=? WHERE id IN ({marks})", (keeper, *chunk))
    _quarantine(con, hold, f"duplicate store {dup} of store {keeper}")
    return len(move)


def step_duplicates(con) -> list:
    stores = _stores(con)
    pairs, _ = cross_store_contamination(con)
    out = []
    for group in duplicate_groups(stores, pairs):
        keeper = pick_keeper([stores[s] for s in group])
        for dup in group:
            if dup == keeper or stores[dup]["scrape_status"] == "duplicate":
                continue
            moved = _fold_duplicate(con, dup, keeper)
            con.execute("UPDATE stores SET scrape_status='duplicate', status_reason=? "
                        "WHERE store_id=?",
                        (f"same Google listing as store {keeper}; ruled {RULING_DATE}", dup))
            out.append({"duplicate_store": dup, "kept_store": keeper,
                        "name": stores[dup]["store_name"],
                        "address": stores[dup]["full_address"],
                        "reviews_moved_to_keeper": moved})
    return out


def step_scope(con, dewey_types: dict) -> int:
    n = 0
    for s in _stores(con).values():
        if s["status_reason"]:
            continue
        dtype = dewey_types.get((s["place_id"] or "").lower())
        if not is_out_of_scope(s["category"], dtype):
            continue
        why = s["category"] or f"Dewey {dtype}"
        con.execute("UPDATE stores SET scrape_status='out_of_scope', status_reason=? "
                    "WHERE store_id=?",
                    (f"{why}: gas/convenience out of scope; ruled {RULING_DATE}", s["store_id"]))
        n += 1
    return n


def _contaminating_sources(con, wrong: int, right: int) -> set:
    theirs = {_text_key(r) for r in con.execute(
        "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=?", (right,))}
    return {r["source_db"] for r in con.execute(
        "SELECT reviewer_name, rating, review_text, source_db FROM reviews WHERE store_id=? "
        "AND TRIM(COALESCE(review_text,'')) <> ''", (wrong,)) if _text_key(r) in theirs}


def _settle_pair(con, stores: dict, w: int, r: int, shared: int, out: dict) -> None:
    sources = _contaminating_sources(con, w, r)
    ids = [x[0] for x in con.execute(
        f"SELECT id FROM reviews WHERE store_id=? AND source_db IN "
        f"({','.join('?' * len(sources))})", (w, *sources))] if sources else []
    _quarantine(con, ids, f"store {w} collected store {r}'s page ({shared} shared)")
    row = {"store": w, "name": stores[w]["store_name"], "page_of_store": r,
           "page_name": stores[r]["store_name"], "shared": shared,
           "reviews_quarantined": len(ids), "sources": ";".join(sorted(sources))}
    if any(is_place_id_era(s) for s in sources):
        con.execute("UPDATE stores SET scrape_status='bad_place_id', status_reason=? "
                    "WHERE store_id=?",
                    (f"place_id opens store {r}'s page; re-probe; ruled {RULING_DATE}", w))
        stores[w]["status_reason"] = "bad_place_id"
        out["bad_place_id"].append(row)
    else:
        # Its reviews are now its own again, so a 'ruled complete' stall ruling no longer holds.
        con.execute("UPDATE stores SET status_reason=NULL WHERE store_id=? AND status_reason "
                    "LIKE '%ruled complete%'", (w,))
        out["requeue"].append(row)


# A store in one of these statuses has been settled for good. A store that is merely
# 'ruled complete' (stalled, no target) still holds whatever pages it collected, and a
# shared page in it must be settled like any other.
SETTLED_STATUSES = ("duplicate", "bad_place_id", "out_of_scope", "closed")


def _shared_texts(con, a: int, b: int) -> list:
    return [r[0] for r in con.execute(
        "SELECT r.review_text FROM reviews r JOIN reviews q ON q.reviewer_name = r.reviewer_name "
        "AND q.review_text = r.review_text WHERE r.store_id=? AND q.store_id=? "
        "AND TRIM(COALESCE(r.review_text,'')) <> ''", (a, b))]


def step_contamination(con) -> dict:
    stores = _stores(con)
    pairs, _ = cross_store_contamination(con)
    out = {"requeue": [], "bad_place_id": [], "ambiguous": []}
    for a, b, shared in pairs:
        A, B = stores[a], stores[b]
        if A["scrape_status"] in SETTLED_STATUSES or B["scrape_status"] in SETTLED_STATUSES:
            continue
        w = wrong_side(A, B)
        if w is None:
            w = name_wrong_side(A, B, _shared_texts(con, a, b))
        if w is None:
            out["ambiguous"].append({"store_a": a, "name_a": A["store_name"],
                                     "address_a": A["full_address"], "store_b": b,
                                     "name_b": B["store_name"], "address_b": B["full_address"],
                                     "shared": shared})
            continue
        _settle_pair(con, stores, w, b if w == a else a, shared, out)
    return out


def apply_pair_rulings(con, rows: list) -> dict:
    """Settle ambiguous shared-page pairs from a person's answer.

    Each row: store_a, store_b, wrong_store - the one of the two that did NOT own the page.
    Its reviews from the batches that carried the shared ones are quarantined, exactly as
    for a pair the rules decided.
    """
    ensure_schema(con)
    stores = _stores(con)
    out = {"requeue": [], "bad_place_id": [], "ambiguous": []}
    for r in rows:
        a, b, w = int(r["store_a"]), int(r["store_b"]), int(r["wrong_store"])
        if w not in (a, b):
            raise ValueError(f"pair {a}/{b}: wrong_store {w} must be one of the two")
        shared = len(_shared_texts(con, a, b))
        _settle_pair(con, stores, w, b if w == a else a, shared, out)
    return out


_FEATURE_ID_IN_URL = re.compile(r"!1s(0x[0-9a-f]+:0x[0-9a-f]+)", re.I)
PAGE_OWNER_CAP = 1500


def feature_id_from_url(url: str):
    m = _FEATURE_ID_IN_URL.search(url or "")
    return m.group(1).lower() if m else None


def _repoint(con, sid: int, row: dict) -> None:
    """Point a store at its real Google page (taken from a manual check)."""
    fid = feature_id_from_url(row.get("real_url"))
    if not fid:
        raise ValueError(f"store {sid}: no Google feature id in real_url {row.get('real_url')!r}")
    clash = con.execute("SELECT store_id FROM stores WHERE store_id <> ? AND "
                        "(LOWER(place_id) = ? OR LOWER(COALESCE(feature_id,'')) = ?)",
                        (sid, fid, fid)).fetchone()
    if clash:
        raise ValueError(f"store {sid}: the real page {fid} is already held by store {clash[0]}")
    count = int(row.get("real_count") or 0)
    con.execute("UPDATE stores SET place_id=?, feature_id=?, full_address=COALESCE(NULLIF(?, ''), full_address), "
                "target_reviews=CASE WHEN ? > 0 THEN MIN(?, ?) ELSE target_reviews END, "
                "business_status='OPERATIONAL', status_reason=NULL, scrape_status='not_started', "
                "reviews_scraped=0 WHERE store_id=?",
                (fid, fid, row.get("real_address") or "", count, count, PAGE_OWNER_CAP, sid))


def apply_page_owner_answers(con, rows: list, ruling_date: str = None) -> dict:
    """Apply the manual check of stores that share a Google page (2026-10-08).

    Each row: store_id, opens_own (YES / NO / PAGE_FAILED: does the store's saved link open
    that store), real_exists (YES / CLOSED / NO / UNSURE), real_url, real_address, real_count,
    partners (store_ids holding the same reviews).

    A store whose saved link opens ANOTHER store's page (opens_own is not YES) holds that
    other store's reviews: all of them are set aside, and so are the reviews its partners
    hold that match them, because the page belongs to a third store neither of them is.
    The wrong store is then re-pointed at its real page, or removed if it is closed or missing.
    Nothing is deleted: set-aside reviews go to quarantined_reviews.
    """
    ruling_date = ruling_date or RULING_DATE
    ensure_schema(con)
    wrong = [r for r in rows if (r.get("opens_own") or "").strip().upper() != "YES"]
    keys = {}
    for r in wrong:
        sid = int(r["store_id"])
        keys[sid] = {_text_key(x) for x in con.execute(
            "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=? "
            "AND TRIM(COALESCE(review_text,'')) <> ''", (sid,))}
    out = {"quarantined": {}, "repointed": [], "closed": [], "out_of_scope": [], "unresolved": []}

    def set_aside(sid: int, ids: list, why: str) -> None:
        _quarantine(con, ids, why)
        out["quarantined"][sid] = out["quarantined"].get(sid, 0) + len(ids)

    for r in wrong:
        sid = int(r["store_id"])
        set_aside(sid, [x[0] for x in con.execute("SELECT id FROM reviews WHERE store_id=?", (sid,))],
                  f"store {sid}'s saved page is another store's; manual check {ruling_date}")
        for p in r.get("partners") or []:
            ids = [x["id"] for x in con.execute(
                "SELECT id, reviewer_name, rating, review_text FROM reviews WHERE store_id=?", (int(p),))
                if _text_key(x)[2] and _text_key(x) in keys[sid]]
            set_aside(int(p), ids, f"shared with store {sid}, whose page is another store's; manual check {ruling_date}")
    for r in wrong:
        sid, real = int(r["store_id"]), (r.get("real_exists") or "").strip().upper()
        note = f"saved page was another store's; manual check {ruling_date}"
        if real == "YES":
            _repoint(con, sid, r)
            out["repointed"].append(sid)
        elif real == "CLOSED":
            con.execute("UPDATE stores SET business_status='CLOSED', scrape_status='closed', status_reason=? "
                        "WHERE store_id=?", (f"{note}; real store is closed", sid))
            out["closed"].append(sid)
        elif real == "NO":
            con.execute("UPDATE stores SET scrape_status='out_of_scope', status_reason=? WHERE store_id=?",
                        (f"{note}; no such store on Google", sid))
            out["out_of_scope"].append(sid)
        else:
            con.execute("UPDATE stores SET status_reason=? WHERE store_id=?",
                        (f"{note}; real store not found", sid))
            out["unresolved"].append(sid)
    return out


def step_no_target(con, attempts: dict) -> list:
    out = []
    for s in _stores(con).values():
        if s["status_reason"] or (s["target_reviews"] or 0) > 0:
            continue
        n = attempts.get(s["store_id"], 0)
        if n < MIN_ATTEMPTS:
            continue
        con.execute("UPDATE stores SET scrape_status='complete', status_reason=? WHERE store_id=?",
                    (f"no target; attempted {n} times; ruled complete {RULING_DATE}",
                     s["store_id"]))
        out.append({"store": s["store_id"], "name": s["store_name"], "attempts": n,
                    "reviews": s["reviews_scraped"]})
    return out


def recompute(con) -> None:
    con.execute("UPDATE stores SET reviews_scraped = "
                "(SELECT COUNT(*) FROM reviews r WHERE r.store_id = stores.store_id)")
    con.execute("""UPDATE stores SET scrape_status = CASE
          WHEN target_reviews IS NULL OR target_reviews = 0 THEN 'unknown_target'
          WHEN reviews_scraped >= target_reviews            THEN 'complete'
          WHEN reviews_scraped = 0                          THEN 'not_started'
          ELSE 'incomplete' END
        WHERE status_reason IS NULL""")


def apply_all(con, dewey_types: dict, attempts: dict) -> dict:
    con.row_factory = sqlite3.Row
    ensure_schema(con)
    dups = step_duplicates(con)
    recompute(con)
    scope = step_scope(con, dewey_types)
    cont = step_contamination(con)
    no_target = step_no_target(con, attempts)
    recompute(con)
    return {"duplicates": dups, "out_of_scope": scope, "no_target_complete": no_target,
            "requeue": [r["store"] for r in cont["requeue"]], "contamination": cont,
            "ambiguous": cont["ambiguous"]}


# ── inputs and outputs ───────────────────────────────────────────────────

def load_dewey_types(paths=DEWEY_LISTS) -> dict:
    out = {}
    for p in paths:
        with open(p, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if r.get("google_place_id"):
                    out[r["google_place_id"].lower()] = r["dewey_food_access_type"]
    return out


def attempts_from_batches(paths, pid_to_sid: dict) -> dict:
    """Total scrape attempts per master store across returned batch databases.

    A file returned twice (same name in two folders) is counted once.
    """
    seen, out = set(), defaultdict(int)
    for p in paths:
        if os.path.basename(p) in seen:
            continue
        seen.add(os.path.basename(p))
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        cols = {r[1] for r in con.execute("PRAGMA table_info(stores)")}
        if "attempts" in cols:
            msid = "master_store_id" if "master_store_id" in cols else "NULL"
            for m, pid, n in con.execute(f"SELECT {msid}, google_place_id, attempts "
                                         f"FROM stores WHERE attempts > 0"):
                sid = m or pid_to_sid.get((pid or "").lower())
                if sid:
                    out[sid] += n
        con.close()
    return dict(out)


def write_report(result: dict, exports: str) -> str:
    import pandas as pd
    path = os.path.join(exports, f"R12_Rulings_{RULING_DATE}.xlsx")
    sheets = {"duplicates": result["duplicates"],
              "contaminated_requeued": result["contamination"]["requeue"],
              "bad_place_id": result["contamination"]["bad_place_id"],
              "ambiguous_pairs": result["ambiguous"],
              "no_target_complete": result["no_target_complete"]}
    with pd.ExcelWriter(path) as xw:
        for name, rows in sheets.items():
            pd.DataFrame(rows or [{"none": ""}]).to_excel(xw, sheet_name=name, index=False)
    return path


def _summary(con, result: dict) -> None:
    q = con.execute("SELECT COUNT(*) FROM quarantined_reviews").fetchone()[0]
    print(f"duplicate copies folded      {len(result['duplicates']):>7,}")
    print(f"out of scope (gas/conv)      {result['out_of_scope']:>7,}")
    print(f"contaminated, requeued       {len(result['requeue']):>7,}")
    print(f"contaminated, bad place_id   {len(result['contamination']['bad_place_id']):>7,}")
    print(f"ambiguous pairs (listed)     {len(result['ambiguous']):>7,}")
    print(f"no target, ruled complete    {len(result['no_target_complete']):>7,}")
    print(f"reviews in quarantine        {q:>7,}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--exports", default=os.path.join(DATA, "exports"))
    ap.add_argument("--pair-rulings", metavar="CSV", default=None,
                    help="store_a,store_b,wrong_store rows for pairs the rules left ambiguous")
    ap.add_argument("--ruling-date", default=RULING_DATE)
    args = ap.parse_args()
    globals()["RULING_DATE"] = args.ruling_date

    if args.apply:
        print("backup: " + backup(args.master, "r12"))
    con = connect_rw(args.master)
    con.row_factory = sqlite3.Row
    pid_to_sid = {(r[1] or "").lower(): r[0] for r in con.execute(
        "SELECT store_id, place_id FROM stores")}
    attempts = attempts_from_batches(
        [p for g in RETURNS_GLOBS for p in sorted(glob.glob(g))], pid_to_sid)

    con.execute("BEGIN IMMEDIATE")
    result = apply_all(con, load_dewey_types(), attempts)
    if args.pair_rulings:
        with open(args.pair_rulings, newline="", encoding="utf-8") as fh:
            rows = [r for r in csv.DictReader(fh) if (r.get("wrong_store") or "").strip()]
        settled = apply_pair_rulings(con, rows)
        recompute(con)
        result["ambiguous"] = [p for p in result["ambiguous"]
                               if not any({int(r["store_a"]), int(r["store_b"])}
                                          == {p["store_a"], p["store_b"]} for r in rows)]
        print(f"pair rulings applied: {len(rows)} "
              f"({len(settled['requeue'])} requeued, {len(settled['bad_place_id'])} bad place_id)")
    _summary(con, result)
    if not args.apply:
        con.rollback()
        con.close()
        print("\nDRY RUN - rolled back. Re-run with --apply.")
        return 0
    con.commit()
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    print("report: " + write_report(result, args.exports))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
