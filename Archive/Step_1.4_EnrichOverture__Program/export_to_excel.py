import sqlite3
import pandas as pd
import os
from datetime import datetime

try:
    # Connect to the database
    db_path = 'data/output/enrichment.db'
    if not os.path.exists(db_path):
        print(f" Database not found at {db_path}")
        exit(1)

    print(" Exporting test results to Excel...")

    conn = sqlite3.connect(db_path)

    # Read all data from the database
    df = pd.read_sql_query("SELECT * FROM supermarkets ORDER BY id DESC", conn)
    conn.close()

    # Create output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'test_results_{timestamp}.xlsx'
    output_path = os.path.join('data', 'output', output_file)

    # Export to Excel with formatting
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Test Results')
        
    print(f" Results exported successfully!")
    print(f" File location: {os.path.abspath(output_path)}")
    print(f"\n Summary:")
    print(f"   Total stores: {len(df)}")
    
    if 'status' in df.columns:
        print(f"   Successful: {len(df[df['status'] == 'success'])}")
        print(f"   Failed: {len(df[df['status'] == 'failed'])}")
        print(f"   Pending: {len(df[df['status'] == 'pending'])}")
        
        if len(df) > 0:
            success_rate = (len(df[df['status'] == 'success']) / len(df)) * 100
            print(f"   Success rate: {success_rate:.1f}%")

    print(f"\n Open this file: {output_file}")
    
except Exception as e:
    print(f" Error: {e}")
    import traceback
    traceback.print_exc()
