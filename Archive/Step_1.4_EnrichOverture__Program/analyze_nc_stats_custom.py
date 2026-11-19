import sqlite3
import pandas as pd
from config.settings import DB_PATH

def analyze_nc_stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("--- NC Statistics ---")

    # 1. Place ID Coverage
    cursor.execute("""
        SELECT
            COUNT(*) as total_nc,
            SUM(CASE WHEN google_place_id IS NOT NULL AND google_place_id != '' THEN 1 ELSE 0 END) as found_place_id
        FROM supermarkets
        WHERE input_state = 'NC';
    """)
    row = cursor.fetchone()
    total_nc = row[0]
    found_place_id = row[1]
    
    if total_nc > 0:
        coverage_pct = (found_place_id / total_nc) * 100
    else:
        coverage_pct = 0.0

    print(f"Total NC Records: {total_nc}")
    print(f"Records with Place ID: {found_place_id}")
    print(f"Place ID Coverage: {coverage_pct:.2f}%")
    print("\n")

    # 2. Top 5 Categories
    print("--- Top 5 Categories in NC ---")
    cursor.execute("""
        SELECT
            gmaps_category,
            COUNT(*) as count
        FROM supermarkets
        WHERE input_state = 'NC'
          AND gmaps_category IS NOT NULL
          AND gmaps_category != ''
        GROUP BY gmaps_category
        ORDER BY count DESC
        LIMIT 5;
    """)
    categories = cursor.fetchall()
    
    for i, (cat, count) in enumerate(categories, 1):
        cat_pct = (count / found_place_id) * 100 if found_place_id > 0 else 0
        print(f"{i}. {cat}: {count} ({cat_pct:.1f}%)")

    conn.close()

if __name__ == "__main__":
    analyze_nc_stats()
