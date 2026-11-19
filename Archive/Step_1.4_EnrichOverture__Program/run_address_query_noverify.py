"""
run_address_query_noverify.py
=============================
Address-based Google Maps query for all not_found + failed records.

Key difference from run_nc_retry_notfound.py
---------------------------------------------
Verification is SKIPPED entirely.  After an address search, the script
simply clicks the first Google Maps result (whatever it is) and extracts
all available fields — name, place ID, rating, reviews, category, phone,
website, hours, coordinates.

The DB is NOT modified.  Everything is written to a timestamped Excel
file so you can review and decide what to keep.

Usage
-----
    python run_address_query_noverify.py
    python run_address_query_noverify.py --workers 4
    python run_address_query_noverify.py --limit 500     # test run
    python run_address_query_noverify.py --state NC      # one state only
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

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.browser import BrowserManager
from src.scraper import GoogleMapsScraper
from src.utils import setup_logger
from config.settings import DB_PATH, OUTPUT_DIR

logger = setup_logger("AddressQueryNoVerify")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_WORKERS    = 6
DEFAULT_BATCH_SIZE = 50   # browser restart interval
DELAY_PER_BUSINESS = 1    # seconds between businesses per worker


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_unsuccessful(state: str = "", limit: int = 0) -> List[Dict[str, Any]]:
    """Fetch all not_found + failed records, optionally filtered by state."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    where_parts = ["status IN ('not_found', 'failed')"]
    if state:
        where_parts.append(f"input_state = '{state.upper()}'")
    where = " AND ".join(where_parts)

    q = f"SELECT * FROM supermarkets WHERE {where} ORDER BY input_state, id ASC"
    if limit > 0:
        q += f" LIMIT {limit}"

    rows = [dict(r) for r in conn.execute(q).fetchall()]
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Core processing — NO name verification
# ---------------------------------------------------------------------------

async def query_by_address(
    scraper: GoogleMapsScraper,
    business: Dict[str, Any],
) -> tuple:
    """
    Search Google Maps by address and extract whatever shows up first.

    No name matching is performed — the caller decides whether the result
    is the right business.

    Returns
    -------
    ("found", data_dict)   — extraction succeeded
    ("not_found", None)    — no Google Maps result at this address
    ("failed", None)       — navigation / extraction error
    """
    b_id   = business["id"]
    street = (business.get("input_street") or "").strip()
    city   = (business.get("input_city")   or "").strip()
    state  = (business.get("input_state")  or "").strip()
    name   = (business.get("input_name")   or "").strip()

    try:
        parts = [p for p in [street, city, state] if p]
        if not parts:
            logger.warning(f"[{b_id}] No address available — skipping.")
            return "not_found", None

        query   = ", ".join(parts)
        encoded = urllib.parse.quote(query)
        url     = f"https://www.google.com/maps/search/{encoded}"

        logger.info(f"[{b_id}] '{name}' -> address search: {query}")
        await scraper.page.goto(url, timeout=60_000)

        # Wait for either a place page, a results list, or a bad-query banner
        try:
            await scraper.page.wait_for_selector(
                "h1, div[role='feed'], div.bad-query", timeout=15_000
            )
        except Exception:
            pass  # continue anyway

        current_url = scraper.page.url

        # ── Already landed on a place page ──────────────────────────────
        if "/maps/place/" in current_url:
            safe = name.replace(" ", "_")[:40]
            data = await scraper.extract_business_info(debug_name=f"addr_{safe}")
            if data:
                data["input_id"]     = b_id
                data["input_name"]   = name
                data["input_street"] = street
                data["input_city"]   = city
                data["input_state"]  = state
                data["input_zip"]    = business.get("input_zip", "")
                data["input_lat"]    = business.get("input_lat")
                data["input_lon"]    = business.get("input_lon")
                return "found", data
            return "failed", None

        # ── Results list — click first result without checking name ──────
        results = scraper.page.locator("a[href*='/maps/place/']")
        count   = await results.count()

        if count > 0:
            logger.info(f"[{b_id}] {count} result(s) — clicking first (no name check).")
            await results.first.click()

            # Wait for the place page URL or at minimum an h1
            try:
                await scraper.page.wait_for_url("**/maps/place/**", timeout=10_000)
                await scraper.page.wait_for_load_state("domcontentloaded", timeout=5_000)
            except Exception:
                try:
                    await scraper.page.locator("h1").first.wait_for(
                        state="visible", timeout=4_000
                    )
                except Exception:
                    logger.warning(f"[{b_id}] Could not load place page after click.")
                    return "not_found", None

            safe = name.replace(" ", "_")[:40]
            data = await scraper.extract_business_info(debug_name=f"addr_{safe}")
            if data:
                data["input_id"]     = b_id
                data["input_name"]   = name
                data["input_street"] = street
                data["input_city"]   = city
                data["input_state"]  = state
                data["input_zip"]    = business.get("input_zip", "")
                data["input_lat"]    = business.get("input_lat")
                data["input_lon"]    = business.get("input_lon")
                return "found", data
            return "failed", None

        # ── No results ───────────────────────────────────────────────────
        logger.info(f"[{b_id}] No Google Maps result for: {query}")
        return "not_found", None

    except Exception as exc:
        logger.error(f"[{b_id}] Exception: {exc}")
        return "failed", None


# ---------------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------------

class AddressWorker(multiprocessing.Process):
    def __init__(self, worker_id, input_queue, result_queue, batch_size):
        super().__init__()
        self.worker_id    = worker_id
        self.input_queue  = input_queue
        self.result_queue = result_queue
        self.batch_size   = batch_size
        self.daemon       = True

    def run(self):
        wlog = setup_logger(f"AddressWorker_{self.worker_id}")
        wlog.info(f"Worker {self.worker_id} started.")
        asyncio.run(self._loop(wlog))

    async def _loop(self, wlog):
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
                    self.input_queue.task_done()
                    break

                # Periodic browser restart to prevent memory leaks
                if processed > 0 and processed % self.batch_size == 0:
                    wlog.info(f"Worker {self.worker_id}: restarting browser after {processed} items...")
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
                    status, data = await query_by_address(scraper, task)
                    self.result_queue.put({
                        "type":      "result",
                        "worker_id": self.worker_id,
                        "id":        task["id"],
                        "status":    status,
                        "data":      data,
                    })
                except Exception as exc:
                    wlog.error(f"Worker {self.worker_id} error on {task['id']}: {exc}")
                    self.result_queue.put({
                        "type":      "error",
                        "worker_id": self.worker_id,
                        "id":        task["id"],
                        "error":     str(exc),
                    })

                processed += 1
                self.input_queue.task_done()
                await asyncio.sleep(DELAY_PER_BUSINESS)

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
# Excel export helper
# ---------------------------------------------------------------------------

COLUMN_ORDER = [
    "input_id", "input_name", "input_street", "input_city", "input_state",
    "input_zip", "input_lat", "input_lon",
    "name", "place_id", "rating", "reviews_count", "category", "address",
    "phone", "website", "price_level", "hours", "latitude", "longitude",
    "gmaps_url",
]

def export_to_excel(found_rows: List[Dict], not_found_rows: List[Dict], timestamp: str) -> str:
    out_path = os.path.join(OUTPUT_DIR, f"address_query_results_{timestamp}.xlsx")

    def _auto_fit(ws):
        for col_cells in ws.columns:
            max_len = max(
                (len(str(c.value)) for c in col_cells if c.value is not None),
                default=10,
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 60)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # Sheet 1: Found results
        if found_rows:
            df_found = pd.DataFrame(found_rows)
            # Reorder columns — put known ones first, append any extras
            ordered = [c for c in COLUMN_ORDER if c in df_found.columns]
            extras  = [c for c in df_found.columns if c not in COLUMN_ORDER]
            df_found = df_found[ordered + extras]
            df_found.to_excel(writer, index=False, sheet_name="Found")
            _auto_fit(writer.sheets["Found"])
        else:
            pd.DataFrame(columns=COLUMN_ORDER).to_excel(
                writer, index=False, sheet_name="Found"
            )

        # Sheet 2: Still not found
        if not_found_rows:
            df_nf = pd.DataFrame(not_found_rows)
            df_nf.to_excel(writer, index=False, sheet_name="Still Not Found")
            _auto_fit(writer.sheets["Still Not Found"])

    return out_path


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(num_workers: int, limit: int, state: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 70)
    logger.info("  ADDRESS QUERY — NO VERIFICATION")
    logger.info(f"  Started : {ts}")
    logger.info(f"  Workers : {num_workers}")
    if state:
        logger.info(f"  State   : {state}")
    logger.info("=" * 70)

    businesses = get_unsuccessful(state=state, limit=limit)
    if not businesses:
        logger.info("No not_found / failed records to process.")
        return

    total = len(businesses)
    logger.info(f"Queuing {total:,} records...")

    input_queue  = multiprocessing.JoinableQueue()
    result_queue = multiprocessing.Queue()

    for biz in businesses:
        input_queue.put(biz)
    for _ in range(num_workers):
        input_queue.put(None)  # poison pills

    workers = []
    for i in range(num_workers):
        w = AddressWorker(i + 1, input_queue, result_queue, DEFAULT_BATCH_SIZE)
        w.start()
        workers.append(w)

    processed      = 0
    found_count    = 0
    nf_count       = 0
    failed_count   = 0
    found_rows:    List[Dict] = []
    nf_rows:       List[Dict] = []
    start_time     = time.time()
    timestamp      = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("Processing started...")

    while processed < total:
        try:
            msg = result_queue.get(timeout=10)
        except Empty:
            alive = sum(1 for w in workers if w.is_alive())
            if alive == 0 and processed < total:
                logger.error("All workers exited before finishing!")
                break
            continue

        w_id = msg["worker_id"]
        b_id = msg["id"]

        if msg["type"] == "result":
            status = msg["status"]
            data   = msg.get("data")

            if status == "found" and data:
                found_count += 1
                found_rows.append(data)
                logger.info(
                    f"  [W{w_id}] FOUND    {b_id:>6} | "
                    f"gmaps_name='{data.get('name', '?')}' "
                    f"place_id={bool(data.get('place_id'))} "
                    f"rating={data.get('rating')} "
                    f"reviews={data.get('reviews_count')}"
                )
            elif status == "not_found":
                nf_count += 1
                # Keep original input data for reference
                biz = next((b for b in businesses if b["id"] == b_id), {})
                nf_rows.append({
                    "input_id":     b_id,
                    "input_name":   biz.get("input_name"),
                    "input_street": biz.get("input_street"),
                    "input_city":   biz.get("input_city"),
                    "input_state":  biz.get("input_state"),
                    "input_zip":    biz.get("input_zip"),
                })
                logger.info(f"  [W{w_id}] NO RESULT {b_id:>6}")
            else:
                failed_count += 1
                logger.warning(f"  [W{w_id}] FAILED   {b_id:>6}")

        elif msg["type"] == "error":
            failed_count += 1
            logger.error(f"  [W{w_id}] ERROR    {b_id:>6}: {msg.get('error')}")

        processed += 1

        if processed % 25 == 0 or processed == total:
            elapsed = time.time() - start_time
            rate    = processed / elapsed if elapsed > 0 else 0
            eta_s   = (total - processed) / rate if rate > 0 else 0
            logger.info(
                f"  Progress: {processed}/{total} ({processed/total*100:.1f}%) "
                f"| {rate:.2f}/s | ETA {timedelta(seconds=int(eta_s))} "
                f"| found={found_count} nf={nf_count} err={failed_count}"
            )

    for w in workers:
        w.join()

    # Export
    logger.info("Exporting results to Excel...")
    out_path = export_to_excel(found_rows, nf_rows, timestamp)

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("  DONE")
    logger.info(f"  Total processed : {processed:,}")
    logger.info(f"  Found           : {found_count:,}")
    logger.info(f"  Still not found : {nf_count:,}")
    logger.info(f"  Errors          : {failed_count:,}")
    logger.info(f"  Runtime         : {timedelta(seconds=int(elapsed))}")
    logger.info(f"  Output          : {os.path.abspath(out_path)}")
    logger.info("=" * 70)

    print(f"\nDone. Results saved to:")
    print(f"  {os.path.abspath(out_path)}")
    print(f"  Sheet 'Found'          : {found_count:,} records")
    print(f"  Sheet 'Still Not Found': {nf_count:,} records")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Address-based Google Maps query — no name verification."
    )
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--limit",   type=int, default=0,
                        help="Limit records to process (0 = all)")
    parser.add_argument("--state",   type=str, default="",
                        help="Filter by state abbreviation (e.g. NC)")
    args = parser.parse_args()

    run(args.workers, args.limit, args.state)
