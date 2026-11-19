"""
Runner script for Test_File_NC.xlsx
Processes NC businesses using exact name search on Google Maps.
Saves results directly to Excel (no database).

Includes per-business and per-phase timing instrumentation
to estimate how long 16,000+ NC stores would take.
"""
import asyncio
import sys
import os
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.browser import BrowserManager
from src.scraper import GoogleMapsScraper
from src.utils import setup_logger

logger = setup_logger('NC_Test')

INPUT_FILE = os.path.join('data', 'input', 'Test_File_NC.xlsx')
OUTPUT_DIR = os.path.join('data', 'output')
BATCH_SIZE = 50  # Process 50 at a time for this test run

# Overhead constants (seconds) used for projections
DELAY_BETWEEN_BUSINESSES = 2
DELAY_BETWEEN_PASSES = 15
BROWSER_RESTART_EVERY = 200
BROWSER_RESTART_TIME = 10  # estimated seconds per restart
NC_TOTAL_STORES = 16_680   # total NC rows in Test_File_NC.xlsx


async def process_business(scraper: GoogleMapsScraper, row: dict) -> dict:
    """
    Process a single business. Returns a result dict with timing data.
    Success = place_id or gmaps_url is found.
    If store can't be found, move on immediately.
    """
    name = row.get('name', '')
    lat = row.get('latitude')
    lon = row.get('longitude')
    city = row.get('city', '')
    state = row.get('state', '')

    result = {
        # Input data (pass through)
        'input_name': name,
        'input_city': city,
        'input_state': state,
        'input_lat': lat,
        'input_lon': lon,
        'input_street': row.get('street', ''),
        'input_zipcode': row.get('zipcode', ''),
        'input_category': row.get('primary_category', ''),
        # Output data (filled by scraper)
        'status': 'not_found',
        'google_place_id': '',
        'gmaps_url': '',
        'gmaps_name': '',
        'gmaps_address': '',
        'gmaps_phone': '',
        'gmaps_website': '',
        'gmaps_rating': '',
        'gmaps_reviews_count': '',
        'gmaps_category': '',
        'gmaps_price_level': '',
        'gmaps_hours': '',
        'error': '',
        # Timing data (seconds)
        'timing_search_s': 0.0,
        'timing_verify_s': 0.0,
        'timing_extract_s': 0.0,
        'timing_total_s': 0.0,
    }

    logger.info(f"Processing: {name} ({lat}, {lon}) - {city}, {state}")

    biz_start = time.time()

    try:
        # ── Search phase ──────────────────────────────────────────
        search_start = time.time()
        found = False
        has_coords = (lat is not None and lon is not None
                      and str(lat).strip() not in ('', '0', '0.0')
                      and str(lon).strip() not in ('', '0', '0.0'))

        if has_coords:
            found = await scraper.search_business(name, float(lat), float(lon))

        if not found and city and state:
            logger.info(f"Fallback search (name+city+state) for {name}")
            found = await scraper.search_business_by_name(name, city, state)

        result['timing_search_s'] = round(time.time() - search_start, 2)

        if not found:
            logger.warning(f"Not found, moving on: {name}")
            result['status'] = 'not_found'
            result['error'] = 'Search returned no results'
            result['timing_total_s'] = round(time.time() - biz_start, 2)
            return result

        # ── Verify phase ──────────────────────────────────────────
        verify_start = time.time()
        match_status = await scraper.verify_business(name)
        result['timing_verify_s'] = round(time.time() - verify_start, 2)

        if match_status != 'match':
            logger.warning(f"Verification: {match_status} for {name}, extracting anyway...")

        # ── Extract phase ─────────────────────────────────────────
        extract_start = time.time()
        safe_name = name.replace(" ", "_")[:50]
        data = await scraper.extract_business_info(debug_name=safe_name)
        result['timing_extract_s'] = round(time.time() - extract_start, 2)

        if data and (data.get('place_id') or data.get('gmaps_url')):
            # SUCCESS: we have place_id or URL
            result['status'] = 'success'
            result['google_place_id'] = data.get('place_id', '')
            result['gmaps_url'] = data.get('gmaps_url', '')
            result['gmaps_name'] = data.get('name', '')
            result['gmaps_address'] = data.get('address', '')
            result['gmaps_phone'] = data.get('phone', '')
            result['gmaps_website'] = data.get('website', '')
            result['gmaps_rating'] = data.get('rating', '')
            result['gmaps_reviews_count'] = data.get('reviews_count', '')
            result['gmaps_category'] = data.get('category', '')
            result['gmaps_price_level'] = data.get('price_level', '')
            result['gmaps_hours'] = data.get('hours', '')
            logger.info(f"SUCCESS! {name} -> Place ID: {result['google_place_id']}, "
                        f"Reviews: {result['gmaps_reviews_count']}, URL: {result['gmaps_url']}")
        else:
            result['status'] = 'failed'
            result['error'] = 'No place_id or URL extracted'
            logger.warning(f"Failed extraction for {name}")

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)[:200]
        logger.error(f"Error processing {name}: {e}")

    result['timing_total_s'] = round(time.time() - biz_start, 2)
    return result


def print_timing_summary(results: list, run_elapsed_s: float) -> None:
    """Print detailed timing summary and 16K projection."""
    timings = [r['timing_total_s'] for r in results if r['timing_total_s'] > 0]
    search_times = [r['timing_search_s'] for r in results]
    verify_times = [r['timing_verify_s'] for r in results if r['timing_verify_s'] > 0]
    extract_times = [r['timing_extract_s'] for r in results if r['timing_extract_s'] > 0]

    if not timings:
        logger.info("No timing data collected.")
        return

    avg_total = sum(timings) / len(timings)
    min_total = min(timings)
    max_total = max(timings)

    logger.info("")
    logger.info("=" * 80)
    logger.info("⏱  TIMING BREAKDOWN")
    logger.info("=" * 80)
    logger.info(f"  Businesses timed:    {len(timings)}")
    logger.info(f"  Wall-clock elapsed:  {timedelta(seconds=int(run_elapsed_s))}")
    logger.info(f"  Avg per business:    {avg_total:.2f}s")
    logger.info(f"  Min per business:    {min_total:.2f}s")
    logger.info(f"  Max per business:    {max_total:.2f}s")

    if search_times:
        logger.info(f"  Avg search time:     {sum(search_times)/len(search_times):.2f}s")
    if verify_times:
        logger.info(f"  Avg verify time:     {sum(verify_times)/len(verify_times):.2f}s")
    if extract_times:
        logger.info(f"  Avg extract time:    {sum(extract_times)/len(extract_times):.2f}s")
    logger.info(f"  Avg inter-biz delay: {DELAY_BETWEEN_BUSINESSES}s (hardcoded)")

    # ── 16K NC Stores Projection ──────────────────────────────────
    n = NC_TOTAL_STORES
    effective_per_biz = avg_total + DELAY_BETWEEN_BUSINESSES

    # Browser restart overhead
    num_restarts = n // BROWSER_RESTART_EVERY
    restart_overhead = num_restarts * BROWSER_RESTART_TIME

    # Pass overhead (assuming 500/pass as in run_us_supermarkets.py)
    pass_size = 500
    num_passes = (n + pass_size - 1) // pass_size
    pass_overhead = num_passes * DELAY_BETWEEN_PASSES

    total_best = n * (min_total + DELAY_BETWEEN_BUSINESSES) + restart_overhead + pass_overhead
    total_avg = n * effective_per_biz + restart_overhead + pass_overhead
    total_worst = n * (max_total + DELAY_BETWEEN_BUSINESSES) + restart_overhead + pass_overhead

    logger.info("")
    logger.info("=" * 80)
    logger.info(f"  PROJECTION FOR {n:,} NC STORES")
    logger.info("=" * 80)
    logger.info(f"  Effective per business (avg): {effective_per_biz:.2f}s  "
                f"(processing: {avg_total:.2f}s + delay: {DELAY_BETWEEN_BUSINESSES}s)")
    logger.info(f"  Browser restarts:            ~{num_restarts} × {BROWSER_RESTART_TIME}s = "
                f"{timedelta(seconds=int(restart_overhead))}")
    logger.info(f"  Pass transitions:            ~{num_passes} × {DELAY_BETWEEN_PASSES}s = "
                f"{timedelta(seconds=int(pass_overhead))}")
    logger.info("")
    logger.info(f"  {'Scenario':<12} {'Per Biz':>10} {'Total':>14} {'Hours':>10} {'Days':>8}")
    logger.info(f"  {'─'*12} {'─'*10} {'─'*14} {'─'*10} {'─'*8}")
    for label, total_s, per_biz_s in [
        ('Best case', total_best, min_total),
        ('Average', total_avg, avg_total),
        ('Worst case', total_worst, max_total),
    ]:
        hours = total_s / 3600
        days = total_s / 86400
        logger.info(f"  {label:<12} {per_biz_s:>8.1f}s  {timedelta(seconds=int(total_s)):>14}  "
                    f"{hours:>8.1f}h  {days:>6.2f}d")
    logger.info("=" * 80)


async def run():
    logger.info("=" * 80)
    logger.info("NC TEST FILE SCRAPER RUN (Excel output + Timing)")
    logger.info("=" * 80)

    run_start = time.time()

    # 1. Read input
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    logger.info(f"Reading {input_path}...")
    df = pd.read_excel(input_path)
    logger.info(f"Loaded {len(df)} rows from Excel")

    # Take first BATCH_SIZE rows
    batch = df.head(BATCH_SIZE)
    logger.info(f"Processing {len(batch)} businesses")

    # 2. Start browser and process
    browser_mgr = BrowserManager()
    scraper = GoogleMapsScraper(browser_mgr)
    results = []
    biz_times = []  # running list of per-business times

    try:
        await scraper.initialize()
        await scraper.navigate_to_maps()

        for i, (_, row) in enumerate(batch.iterrows(), 1):
            logger.info(f"\n--- [{i}/{len(batch)}] ---")
            result = await process_business(scraper, row)
            results.append(result)
            biz_times.append(result['timing_total_s'])

            # ── Per-business timing log ───────────────────────────
            # Calculate running stats
            running_success = sum(1 for r in results if r['status'] == 'success')
            running_cats = sum(1 for r in results if r.get('gmaps_category'))
            
            logger.info(f"⏱  {result['timing_total_s']:.1f}s | "
                        f"Stats: {i}/{len(batch)} Processed | "
                        f" {running_success} Found | "
                        f" {running_cats} Cats")

            # ── Rolling average + ETA ─────────────────────────────
            if i >= 3:
                avg = sum(biz_times) / len(biz_times)
                remaining = len(batch) - i
                eta_s = remaining * (avg + DELAY_BETWEEN_BUSINESSES)
                logger.info(f"  Rolling avg: {avg:.1f}s/biz | "
                            f"ETA for remaining {remaining}: {timedelta(seconds=int(eta_s))}")

            await asyncio.sleep(DELAY_BETWEEN_BUSINESSES)

    finally:
        await browser_mgr.close()

    run_elapsed = time.time() - run_start

    # 3. Save to Excel (including timing columns)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"NC_Test_Results_{timestamp}.xlsx")

    results_df = pd.DataFrame(results)
    results_df.to_excel(output_file, index=False, sheet_name='Results')
    logger.info(f"\nResults saved to: {output_file}")

    # 4. Print stats
    total = len(results)
    success = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'failed')
    not_found = sum(1 for r in results if r['status'] == 'not_found')
    errors = sum(1 for r in results if r['status'] == 'error')

    logger.info("\n" + "=" * 80)
    logger.info("FINAL STATS")
    logger.info("=" * 80)
    logger.info(f"Total processed: {total}")
    logger.info(f"   Success:   {success} ({success/total*100:.1f}%)")
    logger.info(f"   Failed:    {failed} ({failed/total*100:.1f}%)")
    logger.info(f"    Not found: {not_found} ({not_found/total*100:.1f}%)")
    if errors:
        logger.info(f"   Errors:    {errors} ({errors/total*100:.1f}%)")
    logger.info(f"\nOutput: {output_file}")

    # 5. Print timing summary + 16K projection
    print_timing_summary(results, run_elapsed)

    logger.info("Run complete!")


if __name__ == "__main__":
    asyncio.run(run())
