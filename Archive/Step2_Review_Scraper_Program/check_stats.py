import sqlite3

conn = sqlite3.connect('data/reviews/master_reviews.db')
cur = conn.cursor()
cur.execute("SELECT DISTINCT gmaps_category FROM stores WHERE gmaps_category IS NOT NULL AND gmaps_category != ''")
cats = sorted([r[0] for r in cur.fetchall()])
print(f"GMaps categories in scraped stores ({len(cats)}):", cats)
conn.close()

conn2 = sqlite3.connect('data/POI_Overture.db')
cur2 = conn2.cursor()
cur2.execute("PRAGMA table_info(supermarkets)")
cols = [r[1] for r in cur2.fetchall()]
print("Overture columns:", cols)
cat_col = next((c for c in cols if 'category' in c.lower() or 'type' in c.lower()), None)
if cat_col:
    cur2.execute(f"SELECT COUNT(DISTINCT {cat_col}) FROM supermarkets")
    print(f"Overture # of categories ({cat_col}):", cur2.fetchone()[0])
    cur2.execute(f"SELECT DISTINCT {cat_col} FROM supermarkets LIMIT 20")
    print("Sample:", [r[0] for r in cur2.fetchall()])
else:
    print("No category column found in Overture DB")
conn2.close()
