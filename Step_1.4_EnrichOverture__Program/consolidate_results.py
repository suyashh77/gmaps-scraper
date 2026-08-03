
import os
import sys
import sqlite3
import pandas as pd
import logging
from config.settings import OUTPUT_DIR, DB_PATH
from src.database import update_business_status

# Setup logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Consolidator')

def consolidate_data():
    logger.info("Starting consolidation process...")
    
    # 1. Find all Excel result files
    files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith('NC_Test_Results_') and f.endswith('.xlsx')]
    
    if not files:
        logger.info("No NC Test Results files found to consolidate.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    total_updated = 0
    total_skipped = 0
    
    for filename in files:
        filepath = os.path.join(OUTPUT_DIR, filename)
        logger.info(f"Processing {filename}...")
        
        try:
            df = pd.read_excel(filepath)
            
            # Filter for successful rows only
            success_df = df[df['status'] == 'success']
            
            if success_df.empty:
                logger.info(f"  No successful records in {filename}. Skipping.")
                continue
                
            logger.info(f"  Found {len(success_df)} successful records to merge.")
            
            for _, row in success_df.iterrows():
                # We need to find the matching record in DB.
                # Matching logic: input_name + input_lat + input_lon
                # OR input_name + input_city + input_state if coords are missing
                
                input_name = row.get('input_name')
                lat = row.get('input_lat')
                lon = row.get('input_lon')
                city = row.get('input_city')
                state = row.get('input_state')
                
                # Try to find ID
                business_id = None
                
                # Strategy 1: Lat/Lon match
                if pd.notna(lat) and pd.notna(lon):
                    cursor.execute('''
                        SELECT id, status FROM supermarkets 
                        WHERE input_name = ? AND abs(input_lat - ?) < 0.0001 AND abs(input_lon - ?) < 0.0001
                    ''', (input_name, lat, lon))
                    match = cursor.fetchone()
                    if match:
                        business_id, current_status = match
                
                # Strategy 2: Name/City/State match (fallback)
                if not business_id and city and state:
                     cursor.execute('''
                        SELECT id, status FROM supermarkets 
                        WHERE input_name = ? AND input_city = ? AND input_state = ?
                    ''', (input_name, city, state))
                     match = cursor.fetchone()
                     if match:
                        business_id, current_status = match
                
                if business_id:
                    # Update if current status is NOT success, OR if we want to overwrite?
                    # User said "append data and process only ones not already processed".
                    # So if DB says 'pending' or 'failed' and Excel says 'success', we UPDATE.
                    # If DB says 'success', we skip (assuming DB is source of truth or equal).
                    
                    if current_status != 'success':
                        # Prepare data dict for update_business_status
                        enriched_data = {
                            'place_id': row.get('google_place_id'),
                            'name': row.get('gmaps_name'),
                            'address': row.get('gmaps_address'),
                            'phone': row.get('gmaps_phone'),
                            'website': row.get('gmaps_website'),
                            'rating': row.get('gmaps_rating'),
                            'reviews_count': row.get('gmaps_reviews_count'),
                            # We don't have lat/lon in excel output specifically for gmaps usually, but check
                            # The excel output from run_nc_test.py had 'gmaps_category' etc.
                            'category': row.get('gmaps_category'),
                            'price_level': row.get('gmaps_price_level'),
                            'hours': row.get('gmaps_hours'),
                            'gmaps_url': row.get('gmaps_url')
                        }
                        
                        # Use our helper to update
                        # Note: update_business_status opens its own connection, so we shouldn't use it inside this loop
                        # enabling concurrent writes or lock contention.
                        # Better to implement update logic directly here with our open cursor.
                        
                        try:
                            cursor.execute('''
                                UPDATE supermarkets SET
                                    status = 'success',
                                    google_place_id = ?,
                                    gmaps_name = ?,
                                    gmaps_address = ?,
                                    gmaps_phone = ?,
                                    gmaps_website = ?,
                                    gmaps_rating = ?,
                                    gmaps_reviews_count = ?,
                                    gmaps_category = ?,
                                    gmaps_price_level = ?,
                                    gmaps_hours = ?,
                                    gmaps_url = ?,
                                    error_message = NULL,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = ?
                            ''', (
                                enriched_data.get('place_id'),
                                enriched_data.get('name'),
                                enriched_data.get('address'),
                                enriched_data.get('phone'),
                                enriched_data.get('website'),
                                enriched_data.get('rating'),
                                enriched_data.get('reviews_count'),
                                enriched_data.get('category'),
                                enriched_data.get('price_level'),
                                enriched_data.get('hours'),
                                enriched_data.get('gmaps_url'),
                                business_id
                            ))
                            total_updated += 1
                        except sqlite3.IntegrityError:
                            logger.warning(f"  Skipping update for ID {business_id} (Place ID {enriched_data.get('place_id')} already in DB).")
                    else:
                        total_skipped += 1
                else:
                    # Record exists in Excel but not in DB?
                    # This implies Test_File_NC.xlsx has changed or our subset was different.
                    # For now just log it.
                    # logger.warning(f"Could not find DB record for {input_name} in {city}, {state}")
                    pass
            
            conn.commit()
            logger.info(f"  Merged {filename}: Updated {total_updated} records. Skipped {total_skipped} (already success).")
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            import traceback
            traceback.print_exc()

    conn.close()
    logger.info("Consolidation complete.")

if __name__ == "__main__":
    consolidate_data()
