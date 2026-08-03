import sqlite3
import pandas as pd
import os
from datetime import datetime

try:
    # Connect to the database
    db_path = 'data/output/enrichment.db'
    
    print(" Exporting enhanced test results to Excel...")
    print(f" Database: {db_path}")
    
    if not os.path.exists(db_path):
        print(f" Database not found. Please run the test first.")
        exit(1)

    conn = sqlite3.connect(db_path)

    # Query all fields including the new ones
    query = """
    SELECT 
        id,
        input_name AS business_name,
        google_place_id AS place_id,
        gmaps_url,
        gmaps_lat AS latitude,
        gmaps_lon AS longitude,
        gmaps_category AS category,
        gmaps_rating AS rating,
        gmaps_reviews_count AS reviews,
        gmaps_address AS address,
        gmaps_phone AS phone,
        gmaps_website AS website,
        gmaps_price_level AS price_level,
        gmaps_hours AS hours,
        status,
        error_message,
        created_at,
        updated_at
    FROM supermarkets 
    ORDER BY id DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Create output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'test_results_ENHANCED_{timestamp}.xlsx'
    output_path = os.path.join('data', 'output', output_file)

    # Export to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Test Results')
        
        # Add a summary sheet
        summary_data = {
            'Metric': [
                'Total Stores',
                'Successful',
                'Failed',
                'Pending',
                'Success Rate (%)'
            ],
            'Value': [
                len(df),
                len(df[df['status'] == 'success']),
                len(df[df['status'] == 'failed']),
                len(df[df['status'] == 'pending']),
                f"{(len(df[df['status'] == 'success']) / len(df) * 100):.1f}%" if len(df) > 0 else "0%"
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, index=False, sheet_name='Summary')
        
    print(f" Results exported successfully!")
    print(f" Location: {os.path.abspath(output_path)}")
    print(f"\n Summary:")
    print(f"   Total stores: {len(df)}")
    print(f"   Successful: {len(df[df['status'] == 'success'])}")
    print(f"   Failed: {len(df[df['status'] == 'failed'])}")
    print(f"   Pending: {len(df[df['status'] == 'pending'])}")
    
    if len(df) > 0:
        success_count = len(df[df['status'] == 'success'])
        success_rate = (success_count / len(df)) * 100
        print(f"   Success rate: {success_rate:.1f}%")
        
        # Show which fields were captured
        print(f"\n Fields Captured:")
        print(f"    Business Name: {df['business_name'].notna().sum()}")
        print(f"    Place ID: {df['place_id'].notna().sum()}")
        print(f"    Latitude: {df['latitude'].notna().sum()}")
        print(f"    Longitude: {df['longitude'].notna().sum()}")
        print(f"    Category: {df['category'].notna().sum()}")
        print(f"    Rating: {df['rating'].notna().sum()}")
        print(f"    Reviews: {df['reviews'].notna().sum()}")
        print(f"    Address: {df['address'].notna().sum()}")
        print(f"    Phone: {df['phone'].notna().sum()}")
        print(f"    Website: {df['website'].notna().sum()}")
        print(f"    Price Level: {df['price_level'].notna().sum()}")
        print(f"    Hours: {df['hours'].notna().sum()}")
        print(f"    GMaps URL: {df['gmaps_url'].notna().sum()}")

    print(f"\n Open this file to view all enhanced results!")
    
except Exception as e:
    print(f" Error: {e}")
    import traceback
    traceback.print_exc()
