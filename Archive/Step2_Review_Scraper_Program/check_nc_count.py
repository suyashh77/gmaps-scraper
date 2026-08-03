import sqlite3
import pandas as pd

try:
    conn = sqlite3.connect('data/POI_Overture.db')
    
    # Let's see the columns to find the state column
    df = pd.read_sql_query('SELECT * FROM supermarkets LIMIT 1', conn)
    print("Columns in Overture supermarkets:", list(df.columns))
    
    # Usually regions/states are in 'region' or 'state'
    region_col = next((c for c in df.columns if 'region' in c.lower() or 'state' in c.lower()), None)
    
    if region_col:
        query = f"SELECT COUNT(*) as cnt FROM supermarkets WHERE {region_col} IN ('NC', 'North Carolina', 'North carolina')"
        count = pd.read_sql_query(query, conn).iloc[0]['cnt']
        print(f"\nNC stores count: {count}")
    else:
        print("Couldn't find state/region column")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()
