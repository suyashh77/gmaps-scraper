"""
Multi-Pass US Supermarkets Enrichment Runner.

Designed for long, unattended runs (e.g. in tmux).
- Imports the full us_supermarkets.csv into the database (skips duplicates).
- Processes businesses in passes of BATCH_SIZE each.
- Restarts the browser between passes to avoid memory leaks / detection.
- Retries failed businesses after all pending are done.
- Gracefully handles errors at every level (business, pass, top-level).
- Logs progress + periodic status summaries.
- Tracks per-business and per-phase timing with rolling ETAs.

Usage:
    python run_us_supermarkets.py                   # Run all pending
    python run_us_supermarkets.py --batch 500       # Custom batch size
    python run_us_supermarkets.py --passes 5        # Limit number of passes
    python run_us_supermarkets.py --retry-failed    # Also retry previously failed businesses
    python run_us_supermarkets.py --skip-import     # Skip CSV import (use existing DB data)
"""
import asyncio
import argparse
import logging
import os
import sys
import time
import traceback
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database import init_db, get_pending_businesses, update_business_status, import_dataframe
from src.browser import BrowserManager
from src.scraper import GoogleMapsScraper
from src.utils import setup_logger
from config.settings import INPUT_DIR, DB_PATH

# ── Configuration ──────────────────────────────────────────────────────
INPUT_CSV = os.path.join(INPUT_DIR, "us_supermarkets.csv")
DEFAULT_BATCH_SIZE = 500          # Businesses per pass (browser restart between)
DEFAULT_MAX_PASSES = 0            # 0 = unlimited
MAX_CONSECUTIVE_ERRORS = 20       # Abort pass after N consecutive errors
DELAY_BETWEEN_BUSINESSES = 2      # Seconds between businesses
DELAY_BETWEEN_PASSES = 15         # Seconds between passes
BROWSER_RESTART_EVERY = 200       # Restart browser mid-pass every N businesses (0=disabled)
MAX_RETRIES_PER_BUSINESS = 0      # No retries — search once, extract once
BROWSER_RESTART_TIME_EST = 10     # Estimated seconds per browser restart
NC_TOTAL_STORES = 16_680          # Total NC stores for projection

logger = setup_logger('US_Supermarkets')


# ── Database helpers ───────────────────────────────────────────────────

def get_db_stats() -> Dict[str, int]:
    """Get current status counts from the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT status, COUNT(*) FROM supermarkets GROUP BY status')
    stats = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute('SELECT COUNT(*) FROM supermarkets')
    stats['total'] = cursor.fetchone()[0]
    conn.close()
    return stats


def reset_failed_to_pending() -> int:
    """Reset 'failed' businesses back to 'pending' for retry."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE supermarkets SET status = 'pending', error_message = NULL WHERE status = 'failed'")
    count = cursor.rowcount
    conn.commit()
    conn.close()
    return count


def log_stats(stats: Dict[str, int], header: str = "DB Status") -> None:
    """Log formatted database statistics."""
    total = stats.get('total', 0)
    pending = stats.get('pending', 0)
    success = stats.get('success', 0)
    failed = stats.get('failed', 0)
    not_found = stats.get('not_found', 0)
    done = success + failed + not_found

    logger.info(f"{'─' * 60}")
    logger.info(f"  {header}")
    logger.info(f"{'─' * 60}")
    logger.info(f"  Total records:  {total:>8,}")
    logger.info(f"  Pending:        {pending:>8,}")
    logger.info(f"   Success:      {success:>8,}  ({success/total*100:.1f}%)" if total else f"   Success: 0")
    logger.info(f"   Failed:       {failed:>8,}  ({failed/total*100:.1f}%)" if total else f"   Failed: 0")
    logger.info(f"    Not found:   {not_found:>8,}  ({not_found/total*100:.1f}%)" if total else f"    Not found: 0")
    logger.info(f"  Progress:       {done:>8,} / {total:,}  ({done/total*100:.1f}%)" if total else f"  Progress: 0")
    logger.info(f"{'─' * 60}")


# ── Business processing ───────────────────────────────────────────────

async def process_business(scraper: GoogleMapsScraper, business: Dict[str, Any]) -> tuple:
    """
    Process a single business — single search attempt, no retries.
    
    - Search once: by coordinates if available, otherwise by name+city+state.
    - If not found, mark not_found and move on immediately.
    - If found, extract all info. Success even without place_id (URL is enough).
    
    Returns: tuple of (status_string, timing_dict, extracted_data)
        status: 'success', 'failed', 'not_found'
        timing: {'search_s', 'verify_s', 'extract_s', 'total_s'}
        extracted_data: Dict or None
    """
    b_id = business['id']
    name = business['input_name']
    lat = business['input_lat']
    lon = business['input_lon']
    city = business.get('input_city', '') or ''
    state = business.get('input_state', '') or ''

    timing = {'search_s': 0.0, 'verify_s': 0.0, 'extract_s': 0.0, 'total_s': 0.0}
    biz_start = time.time()

    logger.info(f"Processing [{b_id}]: {name} ({lat}, {lon}) - {city}, {state}")

    try:
        # ── Search phase ──────────────────────────────────────
        search_start = time.time()
        found = False
        has_coords = (lat is not None and lon is not None
                      and str(lat).strip() not in ('', '0', '0.0')
                      and str(lon).strip() not in ('', '0', '0.0'))

        if has_coords:
            found = await scraper.search_business(name, float(lat), float(lon))
        elif city and state:
            logger.info(f"  No coords — searching by name+city+state for {name}")
            found = await scraper.search_business_by_name(name, city, state)
        timing['search_s'] = round(time.time() - search_start, 2)

        if not found:
            logger.warning(f"  Not found: {name}")
            update_business_status(b_id, 'not_found', error="Search returned no results")
            timing['total_s'] = round(time.time() - biz_start, 2)
            return 'not_found', timing, None

        # ── Verify phase ──────────────────────────────────────
        verify_start = time.time()
        match_status = await scraper.verify_business(name)
        timing['verify_s'] = round(time.time() - verify_start, 2)

        if match_status != 'match':
            logger.warning(f"  Verification failed: {match_status} for {name}")
            update_business_status(b_id, 'not_found', error=f"Verification: {match_status}")
            timing['total_s'] = round(time.time() - biz_start, 2)
            return 'not_found', timing, None

        # ── Extract phase ─────────────────────────────────────
        extract_start = time.time()
        safe_name = name.replace(" ", "_")[:50]
        data = await scraper.extract_business_info(debug_name=safe_name)
        timing['extract_s'] = round(time.time() - extract_start, 2)

        if data:
            logger.info(f"   Success: Place ID={data.get('place_id', 'N/A')}, "
                        f"Name={data.get('name', 'N/A')}, "
                        f"Reviews={data.get('reviews_count', 'N/A')}, "
                        f"Category={data.get('category', 'N/A')}, "
                        f"URL={'Yes' if data.get('gmaps_url') else 'No'}")
            update_business_status(b_id, 'success', enriched_data=data)
            timing['total_s'] = round(time.time() - biz_start, 2)
            return 'success', timing, data
        else:
            logger.warning(f"  Extraction returned no data for {name}")
            update_business_status(b_id, 'failed', error="Extraction returned no data")
            timing['total_s'] = round(time.time() - biz_start, 2)
            return 'failed', timing, None

    except Exception as e:
        logger.error(f"  Error processing {name}: {e}")
        update_business_status(b_id, 'failed', error=str(e)[:500])
        timing['total_s'] = round(time.time() - biz_start, 2)
        return 'failed', timing, None


# ── Pass execution ─────────────────────────────────────────────────────

async def run_pass(pass_num: int, batch_size: int) -> Dict[str, Any]:
    """
    Run a single pass: start browser, process batch_size businesses, close browser.
    
    Returns: dict with counts and timing:
        {'success': N, 'failed': N, 'not_found': N, 'errors': N, 'categories_found': N,
         'biz_times': [float], 'search_times': [float],
         'verify_times': [float], 'extract_times': [float],
         'pass_elapsed_s': float}
    """
    results = {'success': 0, 'failed': 0, 'not_found': 0, 'errors': 0, 'categories_found': 0}
    biz_times: List[float] = []
    search_times: List[float] = []
    verify_times: List[float] = []
    extract_times: List[float] = []
    pass_start = time.time()

    # Get pending businesses
    businesses = get_pending_businesses(limit=batch_size)
    if not businesses:
        logger.info(f"Pass {pass_num}: No pending businesses. Done!")
        return {**results, 'biz_times': biz_times, 'search_times': search_times,
                'verify_times': verify_times, 'extract_times': extract_times,
                'pass_elapsed_s': 0.0}

    logger.info(f"Pass {pass_num}: Processing {len(businesses)} businesses...")

    browser_mgr = BrowserManager()
    scraper = GoogleMapsScraper(browser_mgr)
    consecutive_errors = 0

    try:
        await scraper.initialize()
        await scraper.navigate_to_maps()

        for i, business in enumerate(businesses, 1):
            # Mid-pass browser restart to avoid memory buildup
            if BROWSER_RESTART_EVERY > 0 and i > 1 and (i - 1) % BROWSER_RESTART_EVERY == 0:
                logger.info(f"  Mid-pass browser restart ({i}/{len(businesses)})...")
                try:
                    await browser_mgr.close()
                    await asyncio.sleep(5)
                    browser_mgr = BrowserManager()
                    scraper = GoogleMapsScraper(browser_mgr)
                    await scraper.initialize()
                    await scraper.navigate_to_maps()
                    consecutive_errors = 0
                    logger.info("  Browser restarted successfully.")
                except Exception as e:
                    logger.error(f"  Browser restart failed: {e}")
                    # Try one more time
                    try:
                        await browser_mgr.close()
                    except:
                        pass
                    await asyncio.sleep(10)
                    browser_mgr = BrowserManager()
                    scraper = GoogleMapsScraper(browser_mgr)
                    await scraper.initialize()
                    await scraper.navigate_to_maps()

            # Process the business
            try:
                logger.info(f"  [{i}/{len(businesses)}] ────────────────────")
                status, timing, data = await process_business(scraper, business)
                results[status] = results.get(status, 0) + 1
                
                if data and data.get('category'):
                    results['categories_found'] += 1

                # Collect timing data
                biz_times.append(timing['total_s'])
                search_times.append(timing['search_s'])
                if timing['verify_s'] > 0:
                    verify_times.append(timing['verify_s'])
                if timing['extract_s'] > 0:
                    extract_times.append(timing['extract_s'])

                # Per-business timing log with LIVE STATS
                logger.info(f"  ⏱  {timing['total_s']:.1f}s | "
                            f"Stats: {i}/{len(businesses)} Processed | "
                            f" {results['success']} Found | "
                            f" {results['categories_found']} Cats")
                            
                if status == 'success':
                    consecutive_errors = 0
                elif status == 'failed':
                    consecutive_errors += 1
                else:
                    # not_found is not really an "error"
                    consecutive_errors = 0

            except Exception as e:
                logger.error(f"  UNHANDLED ERROR for [{business['id']}] {business['input_name']}: {e}")
                logger.error(traceback.format_exc())
                results['errors'] += 1
                consecutive_errors += 1
                # Mark as failed so we don't re-process it immediately
                try:
                    update_business_status(business['id'], 'failed', error=f"Unhandled: {str(e)[:400]}")
                except:
                    pass

            # Check consecutive error limit
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.error(f"   {MAX_CONSECUTIVE_ERRORS} consecutive errors! "
                             f"Browser may be broken. Ending pass early.")
                break

            # Delay between businesses
            await asyncio.sleep(DELAY_BETWEEN_BUSINESSES)

            # Rolling avg + ETA every 10 businesses
            if i % 10 == 0 and biz_times:
                avg = sum(biz_times) / len(biz_times)
                remaining = len(businesses) - i
                eta_s = remaining * (avg + DELAY_BETWEEN_BUSINESSES)
                logger.info(f"   Rolling avg: {avg:.1f}s/biz | "
                            f"ETA this pass: {timedelta(seconds=int(eta_s))}")

            # Periodic progress (every 50 businesses)
            if i % 50 == 0:
                logger.info(f"   Pass {pass_num} progress: {i}/{len(businesses)} "
                            f"({results['success']} {results['failed']} "
                            f"{results['not_found']})")

    except Exception as e:
        logger.error(f"Pass {pass_num} fatal error: {e}")
        logger.error(traceback.format_exc())
    finally:
        try:
            await browser_mgr.close()
        except:
            pass
        logger.info(f"Pass {pass_num} browser closed.")

    pass_elapsed = time.time() - pass_start

    # Per-pass timing summary
    if biz_times:
        avg_t = sum(biz_times) / len(biz_times)
        logger.info(f"  ⏱  Pass {pass_num} timing: {len(biz_times)} businesses in "
                    f"{timedelta(seconds=int(pass_elapsed))} "
                    f"(avg {avg_t:.1f}s, min {min(biz_times):.1f}s, max {max(biz_times):.1f}s)")

    return {**results, 'biz_times': biz_times, 'search_times': search_times,
            'verify_times': verify_times, 'extract_times': extract_times,
            'pass_elapsed_s': pass_elapsed}


# ── Main orchestrator ──────────────────────────────────────────────────

async def main(batch_size: int, max_passes: int, retry_failed: bool, skip_import: bool) -> None:
    start_time = time.time()

    logger.info("=" * 70)
    logger.info("  US SUPERMARKETS ENRICHMENT - MULTI-PASS RUNNER")
    logger.info(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Batch size: {batch_size} | Max passes: {max_passes or 'unlimited'}")
    logger.info(f"  Retry failed: {retry_failed} | Skip import: {skip_import}")
    logger.info("=" * 70)

    # 1. Initialize DB
    init_db()

    # 2. Import CSV data
    if not skip_import:
        input_path = Path(INPUT_CSV)
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            logger.info("Use --skip-import to work with existing DB data only.")
            return

        logger.info(f"Importing from {input_path} ({input_path.stat().st_size / 1024 / 1024:.0f} MB)...")
        logger.info("This may take a few minutes for large files...")

        try:
            import pandas as pd
            # Read in chunks to handle large files
            chunk_size = 50000
            total_imported = 0
            for chunk_num, chunk in enumerate(pd.read_csv(input_path, chunksize=chunk_size)):
                count = import_dataframe(chunk)
                total_imported += count
                logger.info(f"  Chunk {chunk_num + 1}: imported {count} new records "
                            f"({total_imported:,} total new)")

            logger.info(f"Import complete: {total_imported:,} new records added.")
        except Exception as e:
            logger.error(f"CSV import failed: {e}")
            logger.error(traceback.format_exc())
            logger.info("Continuing with existing DB data...")
    else:
        logger.info("Skipping CSV import (--skip-import).")

    # 3. Retry failed if requested
    if retry_failed:
        count = reset_failed_to_pending()
        logger.info(f"Reset {count} failed businesses back to pending for retry.")

    # 4. Show initial stats
    stats = get_db_stats()
    log_stats(stats, "INITIAL STATUS")

    if stats.get('pending', 0) == 0:
        logger.info("No pending businesses to process. Exiting.")
        return

    # 5. Multi-pass loop
    pass_num = 0
    total_results = {'success': 0, 'failed': 0, 'not_found': 0, 'errors': 0, 'categories_found': 0}
    all_biz_times: List[float] = []
    all_search_times: List[float] = []
    all_verify_times: List[float] = []
    all_extract_times: List[float] = []

    while True:
        pass_num += 1

        # Check pass limit
        if max_passes > 0 and pass_num > max_passes:
            logger.info(f"Reached max passes ({max_passes}). Stopping.")
            break

        # Check remaining
        stats = get_db_stats()
        pending = stats.get('pending', 0)
        if pending == 0:
            logger.info("All businesses processed! No more pending.")
            break

        logger.info("")
        logger.info(f"{'═' * 70}")
        logger.info(f"  PASS {pass_num}  |  {pending:,} pending  |  "
                     f"Elapsed: {timedelta(seconds=int(time.time() - start_time))}")
        logger.info(f"{'═' * 70}")

        # Run the pass
        pass_results = await run_pass(pass_num, batch_size)

        # Accumulate results and timing data
        for key in total_results:
            total_results[key] += pass_results.get(key, 0)
        all_biz_times.extend(pass_results.get('biz_times', []))
        all_search_times.extend(pass_results.get('search_times', []))
        all_verify_times.extend(pass_results.get('verify_times', []))
        all_extract_times.extend(pass_results.get('extract_times', []))

        # Log pass results
        pass_count_keys = ['success', 'failed', 'not_found', 'errors']
        pass_total = sum(pass_results.get(k, 0) for k in pass_count_keys)
        logger.info(f"  Pass {pass_num} results: {pass_total} processed "
                     f"({pass_results['success']} {pass_results['failed']} "
                     f"{pass_results['not_found']} {pass_results['errors']})")

        # If pass processed nothing, break to avoid infinite loop
        if pass_total == 0:
            logger.info("Pass processed 0 businesses. Stopping to avoid infinite loop.")
            break

        # Show cumulative stats
        stats = get_db_stats()
        log_stats(stats, f"STATUS AFTER PASS {pass_num}")

        # Delay between passes
        logger.info(f"Waiting {DELAY_BETWEEN_PASSES}s before next pass...")
        await asyncio.sleep(DELAY_BETWEEN_PASSES)

    # 6. Final summary
    elapsed_s = time.time() - start_time
    elapsed = timedelta(seconds=int(elapsed_s))
    stats = get_db_stats()

    logger.info("")
    logger.info("=" * 70)
    logger.info("  FINAL SUMMARY")
    logger.info("=" * 70)
    log_stats(stats, "FINAL DB STATUS")
    logger.info(f"  Passes completed:  {pass_num}")
    logger.info(f"  Total processed:   {sum(total_results.values()):,}")
    logger.info(f"     Success:       {total_results['success']:,}")
    logger.info(f"     Failed:        {total_results['failed']:,}")
    logger.info(f"      Not found:    {total_results['not_found']:,}")
    logger.info(f"     Errors:        {total_results['errors']:,}")
    logger.info(f"     Categories:    {total_results['categories_found']:,}")
    logger.info(f"  Total time:        {elapsed}")

    # ── Timing summary + 16K projection ───────────────────────────
    if all_biz_times:
        avg_t = sum(all_biz_times) / len(all_biz_times)
        min_t = min(all_biz_times)
        max_t = max(all_biz_times)

        logger.info("")
        logger.info("=" * 70)
        logger.info("  ⏱  TIMING BREAKDOWN")
        logger.info("=" * 70)
        logger.info(f"  Businesses timed:    {len(all_biz_times)}")
        logger.info(f"  Avg per business:    {avg_t:.2f}s")
        logger.info(f"  Min per business:    {min_t:.2f}s")
        logger.info(f"  Max per business:    {max_t:.2f}s")
        if all_search_times:
            logger.info(f"  Avg search time:     {sum(all_search_times)/len(all_search_times):.2f}s")
        if all_verify_times:
            logger.info(f"  Avg verify time:     {sum(all_verify_times)/len(all_verify_times):.2f}s")
        if all_extract_times:
            logger.info(f"  Avg extract time:    {sum(all_extract_times)/len(all_extract_times):.2f}s")

        # 16K NC Stores Projection
        n = NC_TOTAL_STORES
        effective_per_biz = avg_t + DELAY_BETWEEN_BUSINESSES
        num_restarts = n // BROWSER_RESTART_EVERY
        restart_overhead = num_restarts * BROWSER_RESTART_TIME_EST
        num_passes_est = (n + batch_size - 1) // batch_size
        pass_overhead = num_passes_est * DELAY_BETWEEN_PASSES

        total_best = n * (min_t + DELAY_BETWEEN_BUSINESSES) + restart_overhead + pass_overhead
        total_avg = n * effective_per_biz + restart_overhead + pass_overhead
        total_worst = n * (max_t + DELAY_BETWEEN_BUSINESSES) + restart_overhead + pass_overhead

        logger.info("")
        logger.info("=" * 70)
        logger.info(f"    PROJECTION FOR {n:,} NC STORES")
        logger.info("=" * 70)
        logger.info(f"  Effective per biz (avg): {effective_per_biz:.2f}s  "
                    f"(processing: {avg_t:.2f}s + delay: {DELAY_BETWEEN_BUSINESSES}s)")
        logger.info(f"  Browser restarts:        ~{num_restarts} × {BROWSER_RESTART_TIME_EST}s")
        logger.info(f"  Pass transitions:        ~{num_passes_est} × {DELAY_BETWEEN_PASSES}s")
        logger.info("")
        logger.info(f"  {'Scenario':<12} {'Per Biz':>10} {'Total':>14} {'Hours':>10} {'Days':>8}")
        logger.info(f"  {'─'*12} {'─'*10} {'─'*14} {'─'*10} {'─'*8}")
        for label, total_s, per_biz_s in [
            ('Best case', total_best, min_t),
            ('Average', total_avg, avg_t),
            ('Worst case', total_worst, max_t),
        ]:
            hours = total_s / 3600
            days = total_s / 86400
            logger.info(f"  {label:<12} {per_biz_s:>8.1f}s  "
                        f"{timedelta(seconds=int(total_s)):>14}  "
                        f"{hours:>8.1f}h  {days:>6.2f}d")

    logger.info("=" * 70)
    logger.info("Run complete!")


# ── CLI ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="US Supermarkets Enrichment - Multi-Pass Runner")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Businesses per pass (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--passes", type=int, default=DEFAULT_MAX_PASSES,
                        help=f"Max number of passes, 0=unlimited (default: {DEFAULT_MAX_PASSES})")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Reset failed businesses to pending before starting")
    parser.add_argument("--skip-import", action="store_true",
                        help="Skip CSV import, use existing DB data only")
    args = parser.parse_args()

    asyncio.run(main(
        batch_size=args.batch,
        max_passes=args.passes,
        retry_failed=args.retry_failed,
        skip_import=args.skip_import,
    ))
