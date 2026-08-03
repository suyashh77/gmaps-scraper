"""Stats CLI: overview, failed stores, sessions, optional monitor loop."""

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta

from .config import load_config


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


def show_stats(db_path: str) -> None:
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    _print_header("REVIEW SCRAPER STATS")
    cur.execute("SELECT COUNT(*) FROM stores")
    total_stores = cur.fetchone()[0]
    cur.execute("SELECT status, COUNT(*) cnt FROM stores GROUP BY status")
    by_status = {r["status"]: r["cnt"] for r in cur.fetchall()}
    cur.execute("SELECT COUNT(*) FROM reviews")
    total_reviews = cur.fetchone()[0]

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

    cur.execute(
        """
        SELECT SUM(stores_processed) sp, SUM(
            CASE WHEN ended_at IS NOT NULL AND started_at IS NOT NULL
            THEN (julianday(ended_at)-julianday(started_at))*86400 ELSE 0 END
        ) secs
        FROM scrape_sessions
        WHERE status IN ('completed', 'interrupted')
        """
    )
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
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT store_id, input_name, input_city, input_state, error_message, attempts
        FROM stores WHERE status='failed' ORDER BY last_attempt DESC
        """
    )
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
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT session_id, started_at, ended_at, stores_processed, reviews_collected, status
        FROM scrape_sessions ORDER BY session_id DESC LIMIT 30
        """
    )
    rows = cur.fetchall()
    _print_header("SESSION HISTORY")
    for row in rows:
        duration = "N/A"
        if row["started_at"] and row["ended_at"]:
            try:
                duration = _fmt_duration(
                    (datetime.fromisoformat(row["ended_at"]) - datetime.fromisoformat(row["started_at"])).total_seconds()
                )
            except Exception:
                pass
        print(
            f"{row['session_id']:<5} {(row['started_at'] or '')[:19]:<19} "
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper stats")
    parser.add_argument("--failed", action="store_true", help="Show failed stores")
    parser.add_argument("--sessions", action="store_true", help="Show session history")
    parser.add_argument("--monitor", action="store_true", help="Refresh stats every 60s")
    parser.add_argument("--db", type=str, help="Database path (default from config)")
    args = parser.parse_args()

    cfg = load_config()
    db_path = args.db or cfg["database"]["path"]
    if args.monitor:
        monitor_mode(db_path)
    elif args.failed:
        show_failed(db_path)
    elif args.sessions:
        show_sessions(db_path)
    else:
        show_stats(db_path)


if __name__ == "__main__":
    main()
