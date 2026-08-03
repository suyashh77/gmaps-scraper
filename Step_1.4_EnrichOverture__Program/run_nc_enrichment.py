"""
run_nc_enrichment.py
====================
Primary enrichment runner for North Carolina (NC) records.

Reads all NC businesses from the database (status='pending'), distributes
them across N parallel worker processes, and writes results back to the
database as they complete.

Each worker runs its own Playwright browser session and restarts it every
``--batch`` businesses to prevent memory leaks.

Usage
-----
    # Run with default 6 workers
    python run_nc_enrichment.py

    # Run with 4 workers, restart browser every 100 items
    python run_nc_enrichment.py --workers 4 --batch 100

    # Retry previously failed records
    python run_nc_enrichment.py --retry-failed

    # Skip Excel import (use data already in the database)
    python run_nc_enrichment.py --skip-import

Architecture
------------
    Main process
    ├── Fills a JoinableQueue with all pending business dicts
    ├── Spawns N EnrichmentWorker processes
    │   └── Each worker: browser → search → verify → extract → result_queue
    └── Reads result_queue, writes to DB, logs progress

The main process is the only writer to the database, which avoids
SQLite write-lock contention between workers.
"""

import asyncio
import argparse
import logging
import multiprocessing
import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from queue import Empty
from typing import Any, Dict, List, Optional, Tuple

# Ensure the project root is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.browser import BrowserManager
from src.database import (
    init_db,
    get_pending_businesses_by_state,
    update_business_status,
    import_dataframe,
)
from src.scraper import GoogleMapsScraper
from src.utils import setup_logger
from config.settings import INPUT_DIR, DB_PATH

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_FILE            = os.path.join(INPUT_DIR, "Test_File_NC.xlsx")
DEFAULT_BATCH_SIZE    = 50    # Businesses per browser session before restart
DEFAULT_WORKERS       = 6     # Parallel worker processes
MAX_CONSECUTIVE_ERRORS = 10   # Abort worker after this many consecutive errors
DELAY_BETWEEN_BUSINESSES = 1  # Seconds to wait between businesses (per worker)

logger = setup_logger("NC_Enrichment")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_stats() -> Dict[str, int]:
    """Return status counts for all NC records in the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status, COUNT(*) FROM supermarkets WHERE input_state='NC' GROUP BY status"
    )
    stats = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute("SELECT COUNT(*) FROM supermarkets WHERE input_state='NC'")
    stats["total"] = cursor.fetchone()[0]
    conn.close()
    return stats


def reset_failed_to_pending() -> int:
    """
    Reset all NC records with status='failed' back to 'pending'.

    Called when ``--retry-failed`` is passed so that previously failed
    records are re-processed in the current run.

    Returns:
        Number of records reset.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE supermarkets SET status='pending', error_message=NULL "
        "WHERE status='failed' AND input_state='NC'"
    )
    count = cursor.rowcount
    conn.commit()
    conn.close()
    return count


def log_stats(stats: Dict[str, int], header: str = "DB Status (NC)") -> None:
    """Pretty-print a stats dict to the log."""
    total   = stats.get("total", 0)
    pending = stats.get("pending", 0)
    success = stats.get("success", 0)
    failed  = stats.get("failed", 0)
    nf      = stats.get("not_found", 0)
    done    = success + failed + nf

    logger.info("─" * 60)
    logger.info(f"  {header}")
    logger.info("─" * 60)
    logger.info(f"  Total:      {total:>8,}")
    logger.info(f"  Pending:    {pending:>8,}")
    if total:
        logger.info(f"  Success:    {success:>8,}  ({success/total*100:.1f}%)")
        logger.info(f"  Not found:  {nf:>8,}  ({nf/total*100:.1f}%)")
        logger.info(f"  Failed:     {failed:>8,}  ({failed/total*100:.1f}%)")
        logger.info(f"  Progress:   {done:>8,} / {total:,}  ({done/total*100:.1f}%)")
    logger.info("─" * 60)


# ---------------------------------------------------------------------------
# Business processing (shared by all workers)
# ---------------------------------------------------------------------------

async def process_business(
    scraper: GoogleMapsScraper, business: Dict[str, Any]
) -> Tuple[str, Dict[str, float], Optional[Dict[str, Any]]]:
    """
    Run the full search → verify → extract pipeline for one business.

    Args:
        scraper:  Initialised :class:`~src.scraper.GoogleMapsScraper`.
        business: Row dict from the ``supermarkets`` table.

    Returns:
        ``(status, timing, data)`` where:
        - *status* is ``'success'``, ``'not_found'``, or ``'failed'``
        - *timing* is a dict with ``search_s``, ``verify_s``, ``extract_s``,
          ``total_s`` (all in seconds)
        - *data* is the extracted info dict (on success) or an error dict
    """
    b_id  = business["id"]
    name  = business["input_name"]
    lat   = business["input_lat"]
    lon   = business["input_lon"]
    city  = business.get("input_city", "") or ""
    state = business.get("input_state", "") or ""

    timing = {"search_s": 0.0, "verify_s": 0.0, "extract_s": 0.0, "total_s": 0.0}
    biz_start = time.time()

    try:
        # ── Search ────────────────────────────────────────────────────────
        t0 = time.time()
        has_coords = (
            lat is not None
            and lon is not None
            and str(lat).strip() not in ("", "0", "0.0")
            and str(lon).strip() not in ("", "0", "0.0")
        )

        if has_coords:
            found = await scraper.search_business(name, float(lat), float(lon))
        elif city and state:
            # Fallback: no coordinates — search by name + city + state
            found = await scraper.search_business_by_name(name, city, state)
        else:
            found = False

        timing["search_s"] = round(time.time() - t0, 2)

        if not found:
            timing["total_s"] = round(time.time() - biz_start, 2)
            return "not_found", timing, None

        # ── Verify ────────────────────────────────────────────────────────
        t0 = time.time()
        match = await scraper.verify_business(name)
        timing["verify_s"] = round(time.time() - t0, 2)

        if match != "match":
            timing["total_s"] = round(time.time() - biz_start, 2)
            return "not_found", timing, {"error_detail": f"Verification: {match}"}

        # ── Extract ───────────────────────────────────────────────────────
        t0 = time.time()
        safe_name = str(name).replace(" ", "_")[:50]
        data = await scraper.extract_business_info(debug_name=safe_name)
        timing["extract_s"] = round(time.time() - t0, 2)
        timing["total_s"]   = round(time.time() - biz_start, 2)

        if data:
            return "success", timing, data
        else:
            return "failed", timing, {"error_detail": "Extraction returned no data"}

    except Exception as exc:
        timing["total_s"] = round(time.time() - biz_start, 2)
        return "failed", timing, {"error_detail": str(exc)[:500]}


# ---------------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------------

class EnrichmentWorker(multiprocessing.Process):
    """
    A worker process that owns one browser session and processes businesses
    from the shared input queue.

    Communication:
        - Reads :class:`dict` business records from ``input_queue``.
        - Writes result dicts to ``result_queue``.
        - Receives ``None`` as a poison pill to shut down cleanly.
    """

    def __init__(
        self,
        worker_id: int,
        input_queue: multiprocessing.JoinableQueue,
        result_queue: multiprocessing.Queue,
        batch_size: int,
    ) -> None:
        super().__init__()
        self.worker_id    = worker_id
        self.input_queue  = input_queue
        self.result_queue = result_queue
        self.batch_size   = batch_size
        self.daemon       = True  # Die automatically if the main process exits

    def run(self) -> None:
        """Entry point for the worker process."""
        wlog = setup_logger(f"Worker_{self.worker_id}")
        wlog.info(f"Worker {self.worker_id} started (batch_size={self.batch_size}).")
        asyncio.run(self._loop(wlog))

    async def _loop(self, wlog: logging.Logger) -> None:
        """Async event loop: initialise browser, process queue, clean up."""
        browser_mgr = None
        scraper     = None
        processed   = 0

        try:
            browser_mgr = BrowserManager()
            scraper     = GoogleMapsScraper(browser_mgr)
            await scraper.initialize()
            await scraper.navigate_to_maps()

            while True:
                # Fetch next task (5-second timeout so we can check liveness)
                try:
                    task = self.input_queue.get(timeout=5)
                except Empty:
                    continue

                if task is None:
                    # Poison pill — shut down cleanly
                    self.input_queue.task_done()
                    break

                # Periodic browser restart to prevent memory leaks
                if processed > 0 and processed % self.batch_size == 0:
                    wlog.info(f"Restarting browser after {processed} items...")
                    try:
                        await browser_mgr.close()
                        await asyncio.sleep(2)
                        browser_mgr = BrowserManager()
                        scraper     = GoogleMapsScraper(browser_mgr)
                        await scraper.initialize()
                        await scraper.navigate_to_maps()
                    except Exception as exc:
                        wlog.error(f"Browser restart failed: {exc}")

                # Process the business
                try:
                    status, timing, data = await process_business(scraper, task)
                    self.result_queue.put({
                        "type":        "result",
                        "worker_id":   self.worker_id,
                        "business_id": task["id"],
                        "status":      status,
                        "timing":      timing,
                        "data":        data,
                    })
                except Exception as exc:
                    wlog.error(f"Unhandled exception on business {task['id']}: {exc}")
                    self.result_queue.put({
                        "type":        "error",
                        "worker_id":   self.worker_id,
                        "business_id": task["id"],
                        "error":       str(exc),
                    })

                processed += 1
                self.input_queue.task_done()
                await asyncio.sleep(DELAY_BETWEEN_BUSINESSES)

        except Exception as exc:
            wlog.error(f"Worker {self.worker_id} fatal error: {exc}\n{traceback.format_exc()}")
        finally:
            if browser_mgr:
                try:
                    await browser_mgr.close()
                except Exception:
                    pass
            wlog.info(f"Worker {self.worker_id} stopped.")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run_parallel_orchestrator(
    num_workers: int, batch_size: int, limit: int = 0
) -> None:
    """
    Coordinate all workers: fill the queue, collect results, write to DB.

    Args:
        num_workers: Number of parallel worker processes.
        batch_size:  Browser restart interval per worker.
        limit:       Max records to process (0 = all pending).
    """
    logger.info("Fetching pending NC businesses from database...")
    fetch_limit = limit if limit > 0 else 20_000
    businesses  = get_pending_businesses_by_state(state="NC", limit=fetch_limit)

    if not businesses:
        logger.info("No pending NC businesses found.")
        return

    total = len(businesses)
    logger.info(f"Queuing {total} businesses for {num_workers} worker(s)...")

    # Set up inter-process queues
    input_queue  = multiprocessing.JoinableQueue()
    result_queue = multiprocessing.Queue()

    # Fill the input queue
    for biz in businesses:
        input_queue.put(biz)
    # Poison pills — one per worker to signal shutdown
    for _ in range(num_workers):
        input_queue.put(None)

    # Start workers
    workers = []
    for i in range(num_workers):
        w = EnrichmentWorker(i + 1, input_queue, result_queue, batch_size)
        w.start()
        workers.append(w)

    # Collect results in the main process (sole DB writer)
    processed     = 0
    success_count = 0
    failed_count  = 0
    nf_count      = 0
    start_time    = time.time()

    logger.info("Processing started — waiting for results...")

    while processed < total:
        try:
            msg = result_queue.get(timeout=10)

            b_id   = msg["business_id"]
            w_id   = msg["worker_id"]
            timing = msg.get("timing", {})

            if msg["type"] == "result":
                processed += 1
                status = msg["status"]
                data   = msg.get("data")

                if status == "success":
                    success_count += 1
                    update_business_status(b_id, "success", enriched_data=data)
                    logger.info(
                        f"  [W{w_id}] ✓ {b_id} ({timing.get('total_s')}s) "
                        f"| reviews={data.get('reviews_count') if data else '?'}"
                    )
                elif status == "not_found":
                    nf_count += 1
                    err = data.get("error_detail") if data else "Not Found"
                    update_business_status(b_id, "not_found", error=err)
                    logger.warning(f"  [W{w_id}] ✗ {b_id} not found ({timing.get('total_s')}s)")
                else:
                    failed_count += 1
                    err = data.get("error_detail") if data else "Failed"
                    update_business_status(b_id, "failed", error=err)
                    logger.error(f"  [W{w_id}] ✗ {b_id} failed: {err}")

            elif msg["type"] == "error":
                processed    += 1
                failed_count += 1
                update_business_status(b_id, "failed", error=f"Worker error: {msg['error']}")
                logger.error(f"  [W{w_id}] ✗ {b_id} worker error: {msg['error']}")

            # Periodic progress summary every 10 records
            if processed % 10 == 0:
                elapsed = time.time() - start_time
                rate    = processed / elapsed if elapsed > 0 else 0
                logger.info(
                    f"  Progress: {processed}/{total} ({processed/total*100:.1f}%) "
                    f"| {rate:.2f}/s "
                    f"| ✓{success_count} ✗{failed_count} ?{nf_count}"
                )

        except Empty:
            # Check whether all workers are still alive
            alive = sum(1 for w in workers if w.is_alive())
            if alive == 0 and processed < total:
                logger.error("All workers died before finishing!")
                break
            continue

    logger.info("All tasks processed.")
    for w in workers:
        w.join()
    logger.info("Orchestrator finished.")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def main(args: argparse.Namespace) -> None:
    """Top-level async entry: import data, optionally retry failures, run."""
    start_time = time.time()

    logger.info("=" * 70)
    logger.info("  NC ENRICHMENT SCRAPER")
    logger.info(f"  Started:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Workers:  {args.workers}  |  Batch size: {args.batch}")
    logger.info("=" * 70)

    # 1. Ensure the database schema exists
    init_db()

    # 2. Import the NC Excel file (unless the user skipped it)
    if not args.skip_import:
        input_path = Path(INPUT_FILE)
        if input_path.exists():
            try:
                logger.info(f"Importing: {input_path}")
                import pandas as pd
                df = pd.read_excel(input_path)
                import_dataframe(df)
            except Exception as exc:
                logger.error(f"Excel import error: {exc}")
        else:
            logger.warning(f"Input file not found: {input_path}")

    # 3. Optionally reset failed records so they are retried
    if args.retry_failed:
        count = reset_failed_to_pending()
        logger.info(f"Reset {count} failed records to pending.")

    # 4. Log initial state and run
    log_stats(get_db_stats(), "INITIAL STATUS")

    if get_db_stats().get("pending", 0) == 0:
        logger.info("No pending records — nothing to do.")
        return

    await run_parallel_orchestrator(args.workers, args.batch)

    # 5. Final summary
    log_stats(get_db_stats(), "FINAL STATUS")
    logger.info(f"Total runtime: {timedelta(seconds=int(time.time() - start_time))}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich NC supermarket records with Google Maps data."
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Number of parallel browser workers (default: {DEFAULT_WORKERS})"
    )
    parser.add_argument(
        "--batch", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Browser restart interval per worker (default: {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="Reset failed records to pending before running"
    )
    parser.add_argument(
        "--skip-import", action="store_true",
        help="Skip Excel import (use data already in the database)"
    )
    args = parser.parse_args()
    asyncio.run(main(args))
