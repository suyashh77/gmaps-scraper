"""
Generate all reports from master_reviews.db and Overture data.

Outputs (all in data/exports/):
  - Enrichment_Successful.xlsx  — all 1269 stores with gmaps enrichment data
  - Scrape_Report.xlsx          — stores with # reviews scraped
  - Test_Store_Reviews.xlsx     — reviews for one high-review store
  - Stats printed to console (Overture + GMaps category counts)
"""

import sqlite3
import pandas as pd
import os

MASTER_DB = "data/reviews/master_reviews.db"
OVERTURE_DB = "data/POI_Overture.db"
EXPORTS = "data/exports"
os.makedirs(EXPORTS, exist_ok=True)

# ── 1. Enrichment Successful ──────────────────────────────────────────────────
print("=== 1. Enrichment_Successful.xlsx ===")
conn = sqlite3.connect(MASTER_DB)
stores_df = pd.read_sql_query("SELECT * FROM stores", conn)
print(f"Total stores in master_reviews.db: {len(stores_df)}")

# All stores here were enriched successfully (they have gmaps data)
stores_df.to_excel(os.path.join(EXPORTS, "Enrichment_Successful.xlsx"), index=False)
print(f"Saved Enrichment_Successful.xlsx ({len(stores_df)} stores)")

# ── 2. Scrape Report ──────────────────────────────────────────────────────────
print("\n=== 2. Scrape_Report.xlsx ===")
reviews_count = pd.read_sql_query(
    "SELECT store_id, COUNT(*) as reviews_in_db FROM reviews GROUP BY store_id",
    conn
)
scrape_df = stores_df.merge(reviews_count, on='store_id', how='left')
scrape_df['reviews_in_db'] = scrape_df['reviews_in_db'].fillna(0).astype(int)

# Select key report columns
report_cols = [
    'store_id', 'input_name', 'input_city', 'input_state', 'input_zip',
    'gmaps_name', 'gmaps_address', 'gmaps_rating', 'gmaps_reviews_count',
    'gmaps_category', 'status', 'reviews_scraped', 'reviews_in_db',
    'error_message'
]
report_cols = [c for c in report_cols if c in scrape_df.columns]
report_df = scrape_df[report_cols]

report_df.to_excel(os.path.join(EXPORTS, "Scrape_Report.xlsx"), index=False)
print(f"Saved Scrape_Report.xlsx ({len(report_df)} stores)")
print(f"Status breakdown:\n{scrape_df['status'].value_counts().to_string()}")

# ── 3. Test Store Export ──────────────────────────────────────────────────────
print("\n=== 3. Test_Store_Reviews.xlsx ===")
# Pick a completed store with most reviews
best = scrape_df[scrape_df['status'] == 'completed'].sort_values(
    'reviews_in_db', ascending=False
).head(1)

if not best.empty:
    test_store = best.iloc[0]
    sid = test_store['store_id']
    name = test_store['input_name']
    city = test_store.get('input_city', '')
    rev_count = int(test_store['reviews_in_db'])

    reviews_df = pd.read_sql_query(
        "SELECT * FROM reviews WHERE store_id = ?", conn, params=(sid,)
    )
    safe_name = "".join(c for c in str(name) if c.isalnum() or c in " _-")[:30].strip()
    test_path = os.path.join(EXPORTS, f"Test_Store_Reviews_{safe_name}.xlsx")
    reviews_df.to_excel(test_path, index=False)
    print(f"Test store: {name}, {city}")
    print(f"Saved {len(reviews_df)} reviews to {test_path}")
else:
    print("No completed stores found.")

conn.close()

# ── 4. GMaps Category Statistics ─────────────────────────────────────────────
print("\n=== 4. Category Statistics ===")
gmaps_cats = stores_df['gmaps_category'].dropna().unique() if 'gmaps_category' in stores_df.columns else []
print(f"GMaps categories in scraped stores: {len(gmaps_cats)}")
if len(gmaps_cats) > 0:
    print(f"  {sorted(gmaps_cats)}")

# Read unsuccessful file if it exists
unsuccessful_path = os.path.join(EXPORTS, "Enrich_Unsuccessful_6531_Stores.xlsx")
if os.path.exists(unsuccessful_path):
    df_unsucc = pd.read_excel(unsuccessful_path)
    print(f"\nUnsuccessful enrichment file: {len(df_unsucc)} stores")
    unsucc_cat_col = next((c for c in df_unsucc.columns if 'category' in c.lower()), None)
    if unsucc_cat_col:
        unsucc_cats = set(df_unsucc[unsucc_cat_col].dropna().unique())
        all_gmaps = set(gmaps_cats) | unsucc_cats
        print(f"GMaps categories (successful + unsuccessful): {len(all_gmaps)}")
    else:
        print(f"Columns in unsuccessful file: {list(df_unsucc.columns)}")

# ── 5. Overture Statistics ────────────────────────────────────────────────────
print("\n=== 5. Overture Statistics ===")
try:
    conn_ov = sqlite3.connect(OVERTURE_DB)
    cur = conn_ov.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
    tables = [r[0] for r in cur.fetchall()]
    print(f"Overture tables: {tables}")

    for tbl in tables:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        total = cur.fetchone()[0]
        print(f"  Overture # of stores ({tbl}): {total:,}")

        cur.execute(f"PRAGMA table_info({tbl})")
        cols = [r[1] for r in cur.fetchall()]
        cat_col = next((c for c in cols if 'category' in c.lower() or 'type' in c.lower()), None)
        if cat_col:
            cur.execute(f"SELECT COUNT(DISTINCT {cat_col}) FROM {tbl}")
            n_cats = cur.fetchone()[0]
            print(f"  Overture # of categories ('{cat_col}'): {n_cats:,}")
            cur.execute(f"SELECT DISTINCT {cat_col} FROM {tbl} LIMIT 20")
            sample = [r[0] for r in cur.fetchall()]
            print(f"  Sample categories: {sample}")
        else:
            print(f"  No category column found. Columns: {cols}")

    conn_ov.close()
except Exception as e:
    print(f"Could not read Overture DB: {e}")

print("\n=== DONE ===")
