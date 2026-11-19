"""R17 - Restore reviews that were set aside as another store's page but are the store's own.

r13 set a batch's reviews aside ("store S collected store T's page") when a source brought far
more reviews than the store's live count. The merge then treats everything in quarantined_reviews
as already known, so a fresh scrape of the store's OWN saved page was dropped as duplicates.
A set-aside review that a scrape of the store's own page returns again is that store's review.

Restored: quarantined rows whose (reviewer, rating, text) is returned by a batch scrape of the
same store, and which no other store holds. Everything else stays set aside. Nothing is deleted.

USAGE
    python Remediation/r17_restore_confirmed.py            # dry run, reads every Data/returns/*/*.db
    python Remediation/r17_restore_confirmed.py --apply    # backs master up first
Idempotent. Run r15 afterwards (this script does) so near-misses are ruled complete.
"""

from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib_master import MASTER, backup, connect_rw             # noqa: E402
import r12_rulings as R12                                       # noqa: E402
import r15_tolerance as R15                                     # noqa: E402

RETURNS = os.path.join(os.path.dirname(MASTER), "returns")


def _key(name, rating, text) -> tuple:
    return ((name or "").strip(), rating, (text or "").strip())


def _batch_keys(path: str) -> dict:
    """master_store_id -> review keys that a scrape of that store's own page returned."""
    out: dict = {}
    b = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    if "master_store_id" in [r[1] for r in b.execute("PRAGMA table_info(stores)")]:
        for master_id, batch_id in b.execute("SELECT master_store_id, store_id FROM stores"):
            if master_id is None:
                continue
            out.setdefault(master_id, set()).update(
                _key(*r) for r in b.execute(
                    "SELECT reviewer_name, rating, review_text FROM reviews WHERE store_id=?", (batch_id,)))
    b.close()
    return out


def restore(con, batches: list) -> dict:
    """Move confirmed quarantined rows back into reviews. Returns {store_id: rows restored}."""
    confirmed: dict = {}
    for path in batches:
        for sid, keys in _batch_keys(path).items():
            confirmed.setdefault(sid, set()).update(keys)
    held_by: dict = {}
    for sid, n, r, t in con.execute("SELECT store_id, reviewer_name, rating, review_text FROM reviews"):
        held_by.setdefault(_key(n, r, t), set()).add(sid)
    cols = [r[1] for r in con.execute("PRAGMA table_info(reviews)")]
    qcols = {r[1] for r in con.execute("PRAGMA table_info(quarantined_reviews)")}
    cols = [c for c in cols if c in qcols]
    restored: dict = {}
    for sid, keys in confirmed.items():
        ids = []
        for qid, n, r, t in con.execute(
                "SELECT id, reviewer_name, rating, review_text FROM quarantined_reviews WHERE store_id=?", (sid,)):
            k = _key(n, r, t)
            owners = held_by.get(k, set())
            if not k[2]:                      # rating-only: names collide across stores, so ask only this store
                owners = owners & {sid}
            if k in keys and not owners:
                ids.append(qid)
                held_by.setdefault(k, set()).add(sid)
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            marks = ",".join("?" * len(chunk))
            con.execute(f"INSERT INTO reviews ({','.join(cols)}) SELECT {','.join(cols)} FROM "
                        f"quarantined_reviews WHERE id IN ({marks})", chunk)
            con.execute(f"DELETE FROM quarantined_reviews WHERE id IN ({marks})", chunk)
        if ids:
            restored[sid] = len(ids)
    return restored


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--returns", default=RETURNS)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    batches = [p for p in glob.glob(os.path.join(args.returns, "*", "*.db"))
               if "interim" not in p and "pre_timeout" not in p and "backup" not in p]
    if args.apply:
        print("backup: " + backup(args.master, "r17"))
    con = connect_rw(args.master)
    con.row_factory = sqlite3.Row
    con.execute("BEGIN IMMEDIATE")
    restored = restore(con, batches)
    R12.recompute(con)
    tolerated = R15.apply_tolerance(con)
    left = con.execute("SELECT COUNT(*), COALESCE(SUM(target_reviews - reviews_scraped),0) FROM stores "
                       "WHERE scrape_status='incomplete'").fetchone()
    print(f"restored {sum(restored.values()):,} reviews to {len(restored)} stores from {len(batches)} returned DBs")
    print(f"tolerance: {len(tolerated)} stores ruled complete")
    print(f"incomplete after: {left[0]} stores, {left[1]:,} reviews short")
    if args.apply:
        con.commit()
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    else:
        con.rollback()
        print("DRY RUN - rolled back. Re-run with --apply.")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
