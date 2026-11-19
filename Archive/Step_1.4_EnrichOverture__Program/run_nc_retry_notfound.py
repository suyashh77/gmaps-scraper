"""
run_nc_retry_notfound.py
========================
Second-pass retry script for NC records that were not found on the first pass.

For every NC record with ``status='not_found'``, this script retries the
search using the street address + city + state instead of the business name
+ coordinates.  This catches businesses whose name on Google Maps differs
significantly from the Overture Maps input data.

When to run
-----------
Run this script *after* ``run_nc_enrichment.py`` has finished processing
all NC records.  It operates only on ``not_found`` rows, so it is safe to
run multiple times.

Usage
-----
    # Default: 6 workers, all not_found records
    python run_nc_retry_notfound.py

    # Fewer workers
    python run_nc_retry_notfound.py --workers 4

    # Test with a small sample
    python run_nc_retry_notfound.py --limit 200

Architecture
------------
Same producer/consumer pattern as ``run_nc_enrichment.py``:
  - Main process fills a queue with not_found business dicts
  - N RetryWorker processes each own a browser and do address-based searches
  - Main process collects results and writes to the database
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
import urllib.parse
from datetime import datetime, timedelta
from queue import Empty
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.browser import BrowserManager
from src.scraper import GoogleMapsScraper
from src.database import update_business_status
from src.utils import setup_logger
from config.settings import DB_PATH

logger = setup_logger("NC_Retry_NotFound")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_WORKERS    = 6    # Parallel worker processes
DEFAULT_BATCH_SIZE = 50   # Browser restart interval per worker
DELAY_BETWEEN_BUSINESSES = 1  # Seconds between businesses (per worker)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_not_found_nc(limit: int = 0) -> List[Dict[str, Any]]:
    """
    Fetch all NC records with ``status='not_found'``.

    Args:
        limit: Maximum records to return (0 = all).

    Returns:
        List of row dicts ordered by ``id ASC``.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = (
        "SELECT * FROM supermarkets "
        "WHERE status='not_found' AND input_state='NC' "
        "ORDER BY id ASC"
    )
    if limit > 0:
        query += f" LIMIT {limit}"

    rows = [dict(r) for r in cursor.execute(query).fetchall()]
    conn.close()
    return rows


def get_nc_stats() -> Dict[str, int]:
    """Return status counts for all NC records."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status, COUNT(*) FROM supermarkets WHERE input_state='NC' GROUP BY status"
    )
    stats = {r[0]: r[1] for r in cursor.fetchall()}
    conn.close()
    return stats


# ---------------------------------------------------------------------------
# Business processing
# ---------------------------------------------------------------------------

async def process_by_address(
    scraper: GoogleMapsScraper, business: Dict[str, Any]
) -> tuple:
    """
    Retry a single business using an address-based Google Maps search.

    Constructs a search URL from the input street + city + state, navigates
    to it, and runs the standard verify → extract pipeline.

    Args:
        scraper:  Initialised :class:`~src.scraper.GoogleMapsScraper`.
        business: Row dict from the ``supermarkets`` table.

    Returns:
        ``(status, data_or_none)`` where status is one of
        ``'success'``, ``'not_found'``, or ``'failed'``.
    """
    b_id   = business["id"]
    name   = business.get("input_name", "")
    street = business.get("input_street", "") or ""
    city   = business.get("input_city",   "") or ""
    state  = business.get("input_state",  "") or ""

    try:
        # Build the address query string (skip empty parts)
        query_parts = [p for p in [street, city, state] if p.strip()]
        if not query_parts:
            logger.warning(f"[{b_id}] No address parts available — skipping.")
            return "not_found", None

        query   = ", ".join(query_parts)
        encoded = urllib.parse.quote(query)
        url     = f"https://www.google.com/maps/search/{encoded}"

        logger.info(f"[{b_id}] Address search: {query}")
        await scraper.page.goto(url, timeout=60_000)

        # Wait for the page to settle
        try:
            await scraper.page.wait_for_selector(
                "h1, div[role='feed'], div.bad-query", timeout=15_000
            )
        except Exception:
            pass  # Continue even if the selector times out

        # Verify using the standard method (fuzzy name matching included)
        match_status = await scraper.verify_business(name)
        if match_status != "match":
            return "not_found", None

        # Extract all available fields
        safe_name = str(name).replace(" ", "_")[:50]
        data = await scraper.extract_business_info(debug_name=f"retry_{safe_name}")
        if data:
            return "success", data
        else:
            return "failed", None

    except Exception as exc:
        logger.error(f"[{b_id}] Error during address retry: {exc}")
        return "failed", None


# ---------------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------------

class RetryWorker(multiprocessing.Process):
    """
    Worker process for the address-based retry pass.

    Identical in structure to ``EnrichmentWorker`` in ``run_nc_enrichment.py``
    but calls ``process_by_address`` instead of ``process_business``.
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
        self.daemon       = True

    def run(self) -> None:
        wlog = setup_logger(f"RetryWorker_{self.worker_id}")
        wlog.info(f"Worker {self.worker_id} started.")
        asyncio.run(self._loop(wlog))

    async def _loop(self, wlog: logging.Logger) -> None:
        """Async event loop: initialise browser, drain queue, clean up."""
        browser_mgr = None
        scraper     = None
        processed   = 0

        try:
            browser_mgr = BrowserManager()
            scraper     = GoogleMapsScraper(browser_mgr)
            await scraper.initialize()
            await scraper.navigate_to_maps()

            while True:
                try:
                    task = self.input_queue.get(timeout=5)
                except Empty:
                    continue

                if task is None:
                    # Poison pill
                    self.input_queue.task_done()
                    break

                # Periodic browser restart
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

                try:
                    status, data = await process_by_address(scraper, task)
                    self.result_queue.put({
                        "type":        "result",
                        "worker_id":   self.worker_id,
                        "business_id": task["id"],
                        "status":      status,
                        "data":        data,
                    })
                except Exception as exc:
                    wlog.error(f"Worker exception on {task['id']}: {exc}")
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
            wlog.error(f"Worker {self.worker_id} fatal: {exc}\n{traceback.format_exc()}")
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

async def run(num_workers: int, limit: int) -> None:
    """
    Main orchestrator: queue not_found records, run workers, collect results.

    Args:
        num_workers: Number of parallel RetryWorker processes.
        limit:       Max records to process (0 = all not_found NC records).
    """
    logger.info("=" * 70)
    logger.info("  NC RETRY: NOT-FOUND (Address-Based Search)")
    logger.info(f"  Started:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Workers:  {num_workers}")
    logger.info("=" * 70)

    businesses = get_not_found_nc(limit=limit)
    if not businesses:
        logger.info("No not_found NC records to retry.")
        return

    total = len(businesses)
    logger.info(f"Queuing {total} not_found records...")

    input_queue  = multiprocessing.JoinableQueue()
    result_queue = multiprocessing.Queue()

    for biz in businesses:
        input_queue.put(biz)
    for _ in range(num_workers):
        input_queue.put(None)  # Poison pills

    workers = []
    for i in range(num_workers):
        w = RetryWorker(i + 1, input_queue, result_queue, DEFAULT_BATCH_SIZE)
        w.start()
        workers.append(w)

    processed     = 0
    success_count = 0
    nf_count      = 0
    failed_count  = 0
    start_time    = time.time()

    logger.info("Processing started...")

    while processed < total:
        try:
            msg  = result_queue.get(timeout=10)
            b_id = msg["business_id"]
            w_id = msg["worker_id"]

            if msg["type"] == "result":
                status = msg["status"]
                data   = msg.get("data")

                if status == "success":
                    success_count += 1
                    update_business_status(b_id, "success", enriched_data=data)
                    logger.info(
                        f"  [W{w_id}] SUCCESS {b_id} "
                        f"| reviews={data.get('reviews_count') if data else '?'}"
                    )
                elif status == "not_found":
                    nf_count += 1
                    # Leave status as not_found — no DB update needed
                    logger.info(f"  [W{w_id}] Still not found: {b_id}")
                else:
                    failed_count += 1
                    update_business_status(b_id, "failed", error="Address retry failed")
                    logger.warning(f"  [W{w_id}] FAILED {b_id}")

            elif msg["type"] == "error":
                failed_count += 1
                update_business_status(
                    b_id, "failed", error=f"Worker error: {msg['error']}"
                )
                logger.error(f"  [W{w_id}] ERROR {b_id}: {msg['error']}")

            processed += 1

            # Progress summary every 25 records
            if processed % 25 == 0:
                elapsed = time.time() - start_time
                rate    = processed / elapsed if elapsed > 0 else 0
                logger.info(
                    f"  Progress: {processed}/{total} ({processed/total*100:.1f}%) "
                    f"| {rate:.2f}/s "
                    f"| ✓{success_count} ?{nf_count} ✗{failed_count}"
                )

        except Empty:
            alive = sum(1 for w in workers if w.is_alive())
            if alive == 0 and processed < total:
                logger.error("All workers died before finishing!")
                break
            continue

    for w in workers:
        w.join()

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("  RETRY COMPLETE")
    logger.info(f"  Total processed : {processed:,}")
    logger.info(f"  Newly succeeded : {success_count:,}")
    logger.info(f"  Still not found : {nf_count:,}")
    logger.info(f"  Failed          : {failed_count:,}")
    logger.info(f"  Runtime         : {timedelta(seconds=int(elapsed))}")
    logger.info("=" * 70)

    # Print final NC stats for reference
    logger.info("Final NC stats:")
    for k, v in sorted(get_nc_stats().items()):
        logger.info(f"  {k}: {v:,}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retry not_found NC records using address-based search."
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Limit records to process (0 = all not_found NC records)"
    )
    args = parser.parse_args()
    asyncio.run(run(args.workers, args.limit))
