"""
Continuous scraping orchestrator.

Runs the scraper on a batch DB, logs progress, and restarts the scraper
until either:
  - The batch DB has no pending stores left
  - Deadline reached (default: 2026-12-31 23:59 — run as long as possible)

The batch DB is the source of truth for "remaining work" while this loop runs.
No batch regeneration — that stays a manual step between sessions.

Merging into master is opt-in with --sync-master <master.db>. On a machine that
holds master (this PC), the batch is folded in after every cycle, so a local run
needs no collect_results.sh, no scp and no manual merge afterwards. The merge is
merge_results.merge_one — the same dedup and identity rules a returned batch gets
— so the two paths cannot drift. Volunteer kits leave the flag off and are
unchanged: they still collect and send batches back.

Usage:
  python run_continuous.py --batch batch1.db
  python run_continuous.py --batch batch2.db --deadline "2026-12-31 23:59"
  python run_continuous.py --batch batch1.db --no-headless
  python run_continuous.py --batch dewey1.db --sync-master ../Data/Step_5_master_reviews.db
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from linux_scraper.merge_results import (  # noqa: E402
    _acquire_lock as acquire_master_lock,
    _connect as connect_master,
    backup_master,
    merge_one,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(ROOT_DIR, "Data")
TARGET_CAP = 1500


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


class MasterSync:
    """Folds a batch into master during a run, or does nothing when disabled.

    The lock and the connection are taken per merge, not for the run. Holding
    merge_results' lock for a multi-day scrape would make the volunteer-returns merge
    (STATUS.md step 7) exit immediately for that whole time, since _acquire_lock does
    not wait. A tick costs a couple of file opens instead, and the lock is held for
    seconds.

    master is backed up once per run, not per tick: merge_one is idempotent (it dedups
    on review_id) and a 500 MB copy every 15 minutes would fill Data/backups/ for no
    gain. A failed merge rolls back — merge_one runs inside BEGIN IMMEDIATE, so leaving
    the transaction open would make every later tick fail with "cannot start a
    transaction within a transaction" and silently disable syncing for the rest of the
    run. The reviews stay in the batch either way and the next tick re-merges them.
    """

    def __init__(self, master_path: str | None) -> None:
        self.master_path = master_path
        self._backed_up = False
        self.total_new = 0

    def __enter__(self) -> "MasterSync":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @property
    def enabled(self) -> bool:
        return bool(self.master_path)

    def merge(self, batch_path: str) -> int:
        """Merge the batch into master; returns reviews added. 0 when disabled or failed."""
        if not self.enabled:
            return 0
        lock = conn = None
        try:
            lock = acquire_master_lock(self.master_path)
            if not self._backed_up:
                log(f"  master backup: {backup_master(self.master_path)}")
                self._backed_up = True
            conn = connect_master(self.master_path)
            stats = merge_one(conn, batch_path)
            conn.commit()
        except SystemExit:
            log("  WARNING: another merge holds the master lock — retrying next tick")
            return 0
        except Exception as exc:  # a merge problem must not end the scrape
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            log(f"  WARNING: master sync failed ({exc}) — reviews stay in the batch, "
                "retrying next tick")
            return 0
        finally:
            if conn is not None:
                conn.close()
            if lock is not None:
                lock.close()
        added = int(stats.get("reviews_new", 0))
        self.total_new += added
        log(f"  master sync: +{added:,} reviews, {stats.get('stores_matched', 0)} stores matched"
            + (f", {stats['stores_unmatched']} unmatched (not in master — run r11 first)"
               if stats.get("stores_unmatched") else ""))
        return added

    def close(self) -> None:
        """Nothing is held between merges; present so callers can use it as a context."""


def resolve_batch_path(batch: str) -> str:
    """Resolve a --batch argument the way a shell would.

    Order: exactly what was typed, then next to this script, then ./batches/
    (the collection-kit layout), then the legacy Data/ directory. The old code
    joined every relative path onto Data/, so `--batch batches/run1.db` typed
    from a deploy folder looked for Data/batches/run1.db and failed with a path
    the operator never wrote.
    """
    if os.path.isabs(batch):
        return batch
    candidates = (
        os.path.abspath(batch),
        os.path.join(SCRIPT_DIR, batch),
        os.path.join(SCRIPT_DIR, "batches", os.path.basename(batch)),
        os.path.join(DATA_DIR, batch),
    )
    return next((c for c in candidates if os.path.exists(c)), candidates[0])


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


def run_scraper(batch_db: str, headless: bool, on_tick=None, tick_seconds: int = 0) -> int:
    """Run the scraper as a subprocess. Returns exit code.

    The scraper claims stores until the batch is drained, so one cycle can last days.
    `on_tick` is called every `tick_seconds` while it runs — that is what gets reviews
    into master during a long run rather than only when the batch finally empties. A
    tick that raises is logged and ignored; nothing about syncing may stop the scrape.
    """
    cmd = [sys.executable, "-m", "linux_scraper", "--db", batch_db]
    if not headless:
        cmd.append("--no-headless")
    log(f"Starting scraper: {' '.join(cmd)}")
    try:
        proc = subprocess.Popen(cmd, cwd=SCRIPT_DIR)
        if not on_tick or tick_seconds <= 0:
            return proc.wait()
        while True:
            try:
                return proc.wait(timeout=tick_seconds)
            except subprocess.TimeoutExpired:
                try:
                    on_tick()
                except Exception as exc:
                    log(f"  WARNING: periodic sync failed ({exc}) — scrape continues")
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
    parser.add_argument("--sync-master", metavar="MASTER_DB", default=None,
                        help="Merge the batch into this master DB as the run goes "
                             "(local runs only — removes the collect/scp/merge step)")
    parser.add_argument("--sync-every", type=int, default=900, metavar="SECONDS",
                        help="With --sync-master: how often to merge while the scraper is "
                             "running (default 900 = every 15 min; 0 = only between cycles)")
    args = parser.parse_args()

    deadline = parse_deadline(args.deadline)
    batch_path = resolve_batch_path(args.batch)

    log("=" * 70)
    log(f"CONTINUOUS SCRAPER (batch-driven) — batch={os.path.basename(batch_path)}")
    log(f"deadline={deadline}")
    log("=" * 70)

    if not os.path.exists(batch_path):
        log(f"ERROR: batch DB not found: {batch_path}")
        log("Looked in: cwd, next to this script, ./batches/, and the legacy Data dir.")
        kit_batches = os.path.join(SCRIPT_DIR, "batches")
        if os.path.isdir(kit_batches):
            found = sorted(f for f in os.listdir(kit_batches) if f.endswith(".db"))
            if found:
                log("Available batches: " + ", ".join("batches/" + f for f in found))
        return 1

    run_start = datetime.now()
    start = batch_status(batch_path)
    start_pending = start["pending"] + start["in_progress"] + start["failed"]
    start_reviews = start["reviews_in_batch"]
    log(f"Run start: {start_pending} stores still pending, "
        f"{start['completed']} already complete, "
        f"{start_reviews:,} reviews already in batch, "
        f"~{start['reviews_remaining']:,} reviews still needed")

    if args.sync_master and not os.path.exists(args.sync_master):
        log(f"ERROR: --sync-master database not found: {args.sync_master}")
        return 1
    if args.sync_master:
        every = (f"every {args.sync_every}s while scraping and after each cycle"
                 if args.sync_every > 0 else "after each cycle")
        log(f"Master sync ON — merging into {os.path.basename(args.sync_master)} {every}")

    total_new_reviews = 0
    cycle = 0
    sync = MasterSync(args.sync_master)
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
        rc = run_scraper(batch_path, headless=not args.no_headless,
                         on_tick=(lambda: sync.merge(batch_path)) if sync.enabled else None,
                         tick_seconds=args.sync_every)
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

        # 5. Fold this cycle's reviews into master (no-op unless --sync-master)
        sync.merge(batch_path)

        # 6. Restart (no regen) unless the batch is fully drained
        if after["pending"] + after["in_progress"] + after["failed"] == 0:
            log("Batch drained — done")
            break

        # If the scraper made no progress this cycle, give the network/Google a brief
        # cooldown rather than thrashing.
        if cycle_new == 0 and rc != 0:
            log("No progress this cycle and scraper non-zero exit — sleeping 60s before restart")
            time.sleep(60)

    sync.merge(batch_path)   # last cycle's reviews, and any a failed merge left behind
    sync.close()

    log("=" * 70)
    final = batch_status(batch_path)
    elapsed_h = (datetime.now() - run_start).total_seconds() / 3600.0
    log(f"FINAL — batch state: {final['pending']} pending, "
        f"{final['completed']} completed, {final['failed']} failed")
    log(f"FINAL — this run added: +{total_new_reviews:,} reviews "
        f"across {cycle} cycle(s) in {elapsed_h:.1f}h "
        f"(cleared {final['completed'] - start['completed']} stores)")
    if sync.enabled:
        log(f"FINAL — merged into master: +{sync.total_new:,} reviews "
            "(no collect step needed)")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
