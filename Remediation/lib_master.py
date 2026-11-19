"""Shared helpers for the remediation toolkit.

Every tool in this folder follows the same contract:
  * open master read-only unless --apply is passed
  * take a timestamped backup INCLUDING the WAL sidecar before any write
  * print a before/after delta
  * be idempotent (running twice changes nothing the second time)
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from datetime import datetime

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(REPO, "Data")
MASTER = os.path.join(DATA, "Step_5_master_reviews.db")


def ro_uri(path: str) -> str:
    """Read-only SQLite URI; percent-encodes #, ? and % that a raw f-string would not."""
    return Path(path).resolve().as_uri() + "?mode=ro"


def connect_ro(path: str) -> sqlite3.Connection:
    """Read-only connection. Cannot modify the file even by accident."""
    conn = sqlite3.connect(ro_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def connect_rw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def backup(path: str, tag: str) -> str:
    """Point-in-time copy of the .db via VACUUM INTO.

    A plain file copy misses committed pages still sitting in the -wal sidecar,
    and a checkpoint before the copy can complete only partially when another
    connection holds a lock. VACUUM INTO reads through the WAL and writes a
    consistent single file, so there is nothing to verify.
    """
    backups = os.path.join(os.path.dirname(os.path.abspath(path)), "backups")
    os.makedirs(backups, exist_ok=True)
    name = os.path.basename(path).replace(".db", f"_backup_{tag}_{datetime.now():%Y%m%d_%H%M%S}.db")
    dest = os.path.join(backups, name)
    if os.path.exists(dest):
        os.remove(dest)
    conn = sqlite3.connect(ro_uri(path), uri=True, timeout=60)
    try:
        conn.execute("VACUUM INTO ?", (dest,))
    finally:
        conn.close()
    return dest


def dedup_key(store_id, reviewer_name, rating, review_text, prefix: int = 50):
    """The key master was merged on. Kept for compatibility checks."""
    return (store_id, reviewer_name or "", rating, (review_text or "")[:prefix])


def strong_key(store_id, reviewer_name, rating, review_text):
    """Full-text key. Distinguishes two reviews that share a 50-char opening.

    The 50-char prefix key collapses 'I love this store, the staff are always
    so friendly...' pairs that are genuinely different reviews. Full text plus
    the reviewer's own review-count is materially more selective.
    """
    return (store_id, reviewer_name or "", rating, (review_text or "").strip())


def counts(conn) -> dict:
    c = conn.cursor()
    return {
        "stores": c.execute("SELECT COUNT(*) FROM stores").fetchone()[0],
        "reviews": c.execute("SELECT COUNT(*) FROM reviews").fetchone()[0],
        "orphans": c.execute(
            "SELECT COUNT(*) FROM reviews r LEFT JOIN stores s ON s.store_id=r.store_id "
            "WHERE s.store_id IS NULL").fetchone()[0],
    }


def print_delta(before: dict, after: dict) -> None:
    print("\n  metric      before        after       delta")
    print("  " + "-" * 46)
    for k in before:
        d = after[k] - before[k]
        print(f"  {k:<10} {before[k]:>10,}  {after[k]:>10,}  {d:>+10,}")
