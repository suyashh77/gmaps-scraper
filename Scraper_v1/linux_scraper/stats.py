"""Stats CLI: overview, failed stores, sessions, optional monitor loop.

Uses SQLite (matches the main database layer).
"""

import os
import sys
import time
import sqlite3
from datetime import datetime, timedelta

from .config import load_config


def _row_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    cols = [desc[0] for desc in cursor.description]
    return dict(zip(cols, row))


def _fmt_number(n: int | None) -> str:
    return "0" if n is None else f"{int(n):,}"


def _fmt_duration(seconds: float | None) -> str:
    if not seconds or seconds <= 0:
        return "N/A"
    hours, rem = divmod(int(seconds), 3600)
    minutes, sec = divmod(rem, 60)
    if hours >= 24:
        days, hours = divmod(hours, 24)
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def _print_header(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection."""
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = _row_factory
    return conn


def show_stats(db_path: str) -> None:
    conn = _connect(db_path)
    cur = conn.cursor()

    _print_header("REVIEW SCRAPER STATS")
    cur.execute("SELECT COUNT(*) AS cnt FROM stores")
    total_stores = cur.fetchone()["cnt"]
    cur.execute("SELECT status, COUNT(*) AS cnt FROM stores GROUP BY status")
    by_status = {r["status"]: r["cnt"] for r in cur.fetchall()}
    cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
    total_reviews = cur.fetchone()["cnt"]

    completed = by_status.get("completed", 0)
    pending = by_status.get("pending", 0)
    in_progress = by_status.get("in_progress", 0)
    failed = by_status.get("failed", 0)
    skipped = by_status.get("skipped", 0)
    pct = (completed / total_stores * 100) if total_stores else 0

    print(f"Stores Total:      {_fmt_number(total_stores)}")
    print(f"Completed:         {_fmt_number(completed)} ({pct:.1f}%)")
    print(f"Pending:           {_fmt_number(pending)}")
    print(f"In Progress:       {_fmt_number(in_progress)}")
    print(f"Failed:            {_fmt_number(failed)}")
    print(f"Skipped:           {_fmt_number(skipped)}")
    print(f"Total Reviews:     {_fmt_number(total_reviews)}")

    cur.execute("""
        SELECT SUM(stores_processed) AS sp,
               SUM(
                   CAST(strftime('%s', ended_at) AS INTEGER) -
                   CAST(strftime('%s', started_at) AS INTEGER)
               ) AS secs
        FROM scrape_sessions
        WHERE status IN ('completed', 'interrupted')
          AND ended_at IS NOT NULL AND started_at IS NOT NULL
    """)
    th = cur.fetchone()
    stores_processed = th["sp"] or 0
    total_secs = th["secs"] or 0
    if stores_processed > 0 and total_secs > 0:
        sps = total_secs / stores_processed
        remaining = pending + in_progress + failed
        eta = datetime.now() + timedelta(seconds=remaining * sps)
        print(f"Avg time/store:    {_fmt_duration(sps)}")
        print(f"ETA:               {eta.strftime('%Y-%m-%d %H:%M')}")

    conn.close()


def show_failed(db_path: str) -> None:
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT store_id, input_name, input_city, input_state, error_message, attempts
        FROM stores WHERE status='failed' ORDER BY last_attempt DESC
    """)
    rows = cur.fetchall()
    _print_header(f"FAILED STORES ({len(rows)})")
    for row in rows:
        print(
            f"#{row['store_id']:<5} {(row['input_name'] or 'Unknown')[:34]:<34} "
            f"{(row['input_city'] or '')}, {(row['input_state'] or '')} attempts={row['attempts']}"
        )
        print(f"  error: {(row['error_message'] or '')[:120]}")
    conn.close()


def show_sessions(db_path: str) -> None:
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT session_id, started_at, ended_at, stores_processed, reviews_collected, status
        FROM scrape_sessions ORDER BY session_id DESC LIMIT 30
    """)
    rows = cur.fetchall()
    _print_header("SESSION HISTORY")
    for row in rows:
        duration = "N/A"
        if row["started_at"] and row["ended_at"]:
            try:
                s = datetime.fromisoformat(row["started_at"])
                e = datetime.fromisoformat(row["ended_at"])
                duration = _fmt_duration((e - s).total_seconds())
            except Exception:
                pass
        started = str(row["started_at"])[:19] if row["started_at"] else ""
        print(
            f"{row['session_id']:<5} {started:<19} "
            f"{duration:<10} stores={row['stores_processed'] or 0:<5} "
            f"reviews={row['reviews_collected'] or 0:<7} {row['status']}"
        )
    conn.close()


def monitor_mode(db_path: str) -> None:
    try:
        while True:
            os.system("cls" if os.name == "nt" else "clear")
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            show_stats(db_path)
            time.sleep(60)
    except KeyboardInterrupt:
        pass
