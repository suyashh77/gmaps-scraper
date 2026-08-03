"""
Continuous scraping orchestrator.

Runs the scraper on a batch DB, logs progress, and restarts the scraper
until either:
  - The batch DB has no pending stores left
  - Deadline reached (default: 2026-12-31 23:59 — run as long as possible)

The batch DB is the source of truth for "remaining work" while this loop runs.
No automatic merge into master and no batch regeneration — those are manual
steps you run between scraper sessions.

Usage:
  python run_continuous.py --batch batch1.db
  python run_continuous.py --batch batch2.db --deadline "2026-12-31 23:59"
  python run_continuous.py --batch batch1.db --no-headless
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(ROOT_DIR, "Data")
TARGET_CAP = 1500


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def batch_status(batch_db: str) -> dict:
    """Return current state of the batch DB.

    Returns dict with: pending_stores, in_progress, completed, failed, skipped,
    reviews_in_batch, reviews_remaining (sum of target-scraped on still-needed stores).
    """
    conn = sqlite3.connect(batch_db)
    cur = conn.cursor()

    cur.execute("SELECT status, COUNT(*) FROM stores GROUP BY status")
    by_status = {s: n for s, n in cur.fetchall()}

    cur.execute("SELECT COUNT(*) FROM reviews")
    reviews_in_batch = int(cur.fetchone()[0] or 0)

    # Reviews still needed = capped target − (master copy + this-run-scraped).
    # max() inline so SQLite returns 0 (not negative) for stores already over their target.
    cur.execute("""
        SELECT COALESCE(SUM(
            CASE
                WHEN target_reviews - master_reviews - reviews_scraped > 0
                    THEN target_reviews - master_reviews - reviews_scraped
                ELSE 0
            END
        ), 0)
        FROM stores
        WHERE status IN ('pending','in_progress','failed')
    """)
    reviews_remaining = int(cur.fetchone()[0] or 0)

    conn.close()
    return {
        "pending": int(by_status.get("pending", 0)),
        "in_progress": int(by_status.get("in_progress", 0)),
        "completed": int(by_status.get("completed", 0)),
        "failed": int(by_status.get("failed", 0)),
        "skipped": int(by_status.get("skipped", 0)),
        "reviews_in_batch": reviews_in_batch,
        "reviews_remaining": reviews_remaining,
    }


def run_scraper(batch_db: str, headless: bool) -> int:
    """Run the scraper as a subprocess. Returns exit code."""
    cmd = [sys.executable, "-m", "linux_scraper", "--db", batch_db]
    if not headless:
        cmd.append("--no-headless")
    log(f"Starting scraper: {' '.join(cmd)}")
    try:
        return subprocess.call(cmd, cwd=SCRIPT_DIR)
    except KeyboardInterrupt:
        log("Scraper interrupted by user")
        return 130


def parse_deadline(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M")


def main() -> int:
    parser = argparse.ArgumentParser(description="Continuous scraping orchestrator (batch-driven)")
    parser.add_argument("--batch", required=True,
                        help="Batch DB filename in Data/ (e.g. batch1.db)")
    parser.add_argument("--deadline", default="2026-12-31 23:59",
                        help="Hard stop deadline 'YYYY-MM-DD HH:MM' (default: end of 2026)")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    parser.add_argument("--max-cycles", type=int, default=200,
                        help="Safety cap on iterations (default: 200)")
    args = parser.parse_args()

    deadline = parse_deadline(args.deadline)
    batch_path = args.batch if os.path.isabs(args.batch) else os.path.join(DATA_DIR, args.batch)

    log("=" * 70)
    log(f"CONTINUOUS SCRAPER (batch-driven) — batch={os.path.basename(batch_path)}")
    log(f"deadline={deadline}")
    log("=" * 70)

    if not os.path.exists(batch_path):
        log(f"ERROR: batch DB not found: {batch_path}")
        return 1

    run_start = datetime.now()
    start = batch_status(batch_path)
    start_pending = start["pending"] + start["in_progress"] + start["failed"]
    start_reviews = start["reviews_in_batch"]
    log(f"Run start: {start_pending} stores still pending, "
        f"{start['completed']} already complete, "
        f"{start_reviews:,} reviews already in batch, "
        f"~{start['reviews_remaining']:,} reviews still needed")

    total_new_reviews = 0
    cycle = 0
    while cycle < args.max_cycles:
        cycle += 1
        if datetime.now() >= deadline:
            log(f"Deadline reached ({deadline}) — stopping")
            break

        # 1. Count remaining work in the batch DB
        before = batch_status(batch_path)
        pending_now = before["pending"] + before["in_progress"] + before["failed"]
        log(f"--- Cycle {cycle} ---")
        log(f"  batch state: {before['pending']} pending, "
            f"{before['in_progress']} in_progress, "
            f"{before['completed']} completed, "
            f"{before['failed']} failed, "
            f"{before['skipped']} skipped")
        log(f"  reviews in batch: {before['reviews_in_batch']:,} | "
            f"~{before['reviews_remaining']:,} still needed across pending stores")
        log(f"  this run so far: +{total_new_reviews:,} reviews")

        if pending_now == 0:
            log("Batch drained — no pending/failed stores left, stopping")
            break

        # 2. Run scraper (it claims stores from the batch DB until drained or interrupted)
        rc = run_scraper(batch_path, headless=not args.no_headless)
        log(f"Scraper exited with code {rc}")

        # 3. Compute progress: delta of reviews in batch
        after = batch_status(batch_path)
        cycle_new = max(0, after["reviews_in_batch"] - before["reviews_in_batch"])
        total_new_reviews += cycle_new

        elapsed_h = (datetime.now() - run_start).total_seconds() / 3600.0
        rate = total_new_reviews / elapsed_h if elapsed_h > 0 else 0
        cleared = after["completed"] - start["completed"]

        # 4. Log progress
        log(f"  Cycle {cycle}: +{cycle_new:,} reviews this cycle | "
            f"+{total_new_reviews:,} this run in {elapsed_h:.1f}h ({rate:,.0f}/hr)")
        log(f"  Stores cleared this run: {cleared} "
            f"(completed {start['completed']} → {after['completed']})")
        log(f"  Remaining: {after['pending']} pending, "
            f"{after['failed']} failed, "
            f"~{after['reviews_remaining']:,} reviews still needed")

        # 5. Restart (no merge, no regen) unless the batch is fully drained
        if after["pending"] + after["in_progress"] + after["failed"] == 0:
            log("Batch drained — done")
            break

        # If the scraper made no progress this cycle, give the network/Google a brief
        # cooldown rather than thrashing.
        if cycle_new == 0 and rc != 0:
            log("No progress this cycle and scraper non-zero exit — sleeping 60s before restart")
            time.sleep(60)

    log("=" * 70)
    final = batch_status(batch_path)
    elapsed_h = (datetime.now() - run_start).total_seconds() / 3600.0
    log(f"FINAL — batch state: {final['pending']} pending, "
        f"{final['completed']} completed, {final['failed']} failed")
    log(f"FINAL — this run added: +{total_new_reviews:,} reviews "
        f"across {cycle} cycle(s) in {elapsed_h:.1f}h "
        f"(cleared {final['completed'] - start['completed']} stores)")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
