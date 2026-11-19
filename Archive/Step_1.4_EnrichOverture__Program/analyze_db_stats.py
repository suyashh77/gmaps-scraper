import sqlite3
import pandas as pd
import os

DB_PATH = "/home/scsapre/Scraper Project/EnrichOverture/data/output/enrichment.db"

def analyze():
    print(f"Connecting to {DB_PATH}...")
    if not os.path.exists(DB_PATH):
        print("Database file not found!")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("\nTables:", tables)

    if not tables or ('supermarkets',) not in tables:
        print("No 'supermarkets' table found.")
        return

    # 2. Schema
    print("\nSchema for 'supermarkets':")
    for row in cursor.execute("PRAGMA table_info(supermarkets)"):
        print(row)

    # 3. Status Counts
    print("\nStatus Counts:")
    try:
        df_status = pd.read_sql_query("SELECT status, COUNT(*) as count FROM supermarkets GROUP BY status", conn)
        print(df_status)
    except Exception as e:
        print(f"Error querying status: {e}")

    # 4. NC Records
    print("\nNC Record Analysis:")
    try:
        # Check if 'input_state' or 'state' column exists
        cols = [info[1] for info in cursor.execute("PRAGMA table_info(supermarkets)")]
        state_col = 'input_state' if 'input_state' in cols else 'state' if 'state' in cols else None
        
        if state_col:
            count_nc = pd.read_sql_query(f"SELECT COUNT(*) as nc_count FROM supermarkets WHERE {state_col} = 'NC'", conn)
            print(f"Total NC records: {count_nc.iloc[0]['nc_count']}")
            
            df_nc_status = pd.read_sql_query(f"SELECT status, COUNT(*) as count FROM supermarkets WHERE {state_col} = 'NC' GROUP BY status", conn)
            print("NC Status Breakdown:")
            print(df_nc_status)
        else:
            print("Could not find a state column (checked 'input_state', 'state').")
            
    except Exception as e:
        print(f"Error querying NC records: {e}")

    conn.close()

if __name__ == "__main__":
    analyze()
