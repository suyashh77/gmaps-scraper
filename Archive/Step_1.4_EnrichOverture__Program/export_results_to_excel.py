import sqlite3
import pandas as pd
import os
from datetime import datetime

# Connect to the database
db_path = 'data/output/enrichment.db'
if not os.path.exists(db_path):
    print(f" Database not found at {db_path}")
    exit(1)

print(" Exporting test results to Excel...")

conn = sqlite3.connect(db_path)

# Read all data from the database
query = """
SELECT 
    id,
    input_name as store_name,
    google_place_id as place_id,
    gmaps_rating as rating,
    gmaps_reviews_count as reviews_count,
    address,
    city,
    state,
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
output_file = f'test_results_{timestamp}.xlsx'
output_path = os.path.join('data', 'output', output_file)

# Export to Excel
df.to_excel(output_path, index=False, sheet_name='Test Results')

print(f" Results exported successfully!")
print(f" File location: {os.path.abspath(output_path)}")
print(f"\n Summary:")
print(f"   Total stores: {len(df)}")
print(f"   Successful: {len(df[df['status'] == 'success'])}")
print(f"   Failed: {len(df[df['status'] == 'failed'])}")
print(f"   Pending: {len(df[df['status'] == 'pending'])}")

if len(df) > 0:
    success_rate = (len(df[df['status'] == 'success']) / len(df)) * 100
    print(f"   Success rate: {success_rate:.1f}%")

print(f"\n Open the file to view all results: {output_file}")
