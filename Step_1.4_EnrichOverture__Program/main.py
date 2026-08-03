import asyncio
import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any
from src.database import init_db, get_pending_businesses, update_business_status, import_dataframe
from src.browser import BrowserManager
from src.scraper import GoogleMapsScraper
from config.settings import HEADLESS, INPUT_DIR, LOG_FILE

from src.utils import setup_logger

# Setup logging
logger = setup_logger('Main')

INPUT_CSV = os.path.join(INPUT_DIR, "us_supermarkets.csv")

# Configuration
TEST_LIMIT = 1000  # Process batch size

async def process_business(scraper: GoogleMapsScraper, business: Dict[str, Any], max_retries: int = 2) -> None:
    """
    Process a single business with retry logic.
    
    Args:
        scraper: The scraper instance.
        business: Dictionary containing business data.
        max_retries: Maximum number of retry attempts for failures.
    """
    b_id = business['id']
    name = business['input_name']
    lat = business['input_lat']
    lon = business['input_lon']
    city = business.get('input_city', '')
    state = business.get('input_state', '')
    
    logger.info(f"Processing [{b_id}]: {name} ({lat}, {lon}) - {city}, {state}")
    
    for attempt in range(max_retries + 1):
        if attempt > 0:
            logger.info(f"Retry attempt {attempt}/{max_retries} for {name}")
            await asyncio.sleep(3)  # Wait before retry
        
        try:
            # Search: try coordinates first, fallback to name+city+state
            found = False
            has_coords = lat is not None and lon is not None and lat != 0 and lon != 0
            
            if has_coords:
                found = await scraper.search_business(name, lat, lon)
            
            if not found and city and state:
                logger.info(f"Using fallback search (name+city+state) for {name}")
                found = await scraper.search_business_by_name(name, city, state)
            
            if not found:
                logger.warning(f"Business not found: {name}")
                update_business_status(b_id, 'not_found', error="Search returned no results")
                return 'not_found', None

            # Verify
            match_status = await scraper.verify_business(name)
            if match_status != 'match':
                logger.warning(f"Verification failed for {name}: {match_status}")
                if attempt < max_retries:
                    continue  # Retry
                update_business_status(b_id, 'failed', error=f"Verification: {match_status}")
                return 'failed', None

            # Extract
            safe_name = name.replace(" ", "_")
            data = await scraper.extract_business_info(debug_name=safe_name)
            
            if data and data.get('place_id'):
                logger.info(f"Success! Place ID: {data.get('place_id')}, Reviews: {data.get('reviews_count')}, URL: {data.get('gmaps_url', 'N/A')}")
                update_business_status(b_id, 'success', enriched_data=data)
                return 'success', data  # Success, exit retry loop
            else:
                logger.error(f"Failed to extract info for {name} (attempt {attempt + 1})")
                if attempt < max_retries:
                    continue  # Retry
                update_business_status(b_id, 'failed', error="Extraction failed - no place_id found")
                return 'failed', None
                
        except Exception as e:
            logger.error(f"Error processing {name} (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                continue  # Retry
            update_business_status(b_id, 'failed', error=str(e))
            return 'failed', None

async def run() -> None:
    logger.info("Starting Supermarket Enrichment (Lean Mode)")
    
    # 1. Initialize DB
    init_db()
    
    # 2. Import Data (Test Limit)
    # 2. Import Data (if input file exists)
    input_path = Path(INPUT_CSV)
    if not input_path.exists():
        logger.warning(f"Input file not found: {input_path}. Proceeding with existing DB data only.")
    else:
        import pandas as pd
        
        logger.info(f"Importing data from {input_path}...")
        try:
            df = pd.read_csv(input_path)
            # Note: For testing large files, you can limit rows here e.g. df = df.head(100)
            import_dataframe(df)
        except Exception as e:
            logger.error(f"Failed to read/import CSV: {e}")
    
    # 3. Process Pending
    businesses = get_pending_businesses(limit=TEST_LIMIT)
    logger.info(f"Found {len(businesses)} pending businesses.")
    
    if not businesses:
        logger.info("No work to do.")
        return

    # 4. Start Browser
    browser_mgr = BrowserManager()
    scraper = GoogleMapsScraper(browser_mgr)
    
    try:
        await scraper.initialize()
        await scraper.navigate_to_maps()
        
        # Loop
        stats = {'processed': 0, 'success': 0, 'failed': 0, 'not_found': 0, 'categories': 0}
        
        for i, business in enumerate(businesses, 1):
            b_id = business['id']
            status, data = await process_business(scraper, business)
            
            # Note: process_business handles DB updates internally.
            
            stats['processed'] += 1
            stats[status] = stats.get(status, 0) + 1
            if data and data.get('category'):
                stats['categories'] += 1
            
            logger.info(f"Stats: {i}/{len(businesses)} |  {stats['success']} Found |  {stats['categories']} Cats")
            
            await asyncio.sleep(2) # Friendly delay
            
    finally:
        await browser_mgr.close()
        logger.info("Run complete.")

if __name__ == "__main__":
    asyncio.run(run())
