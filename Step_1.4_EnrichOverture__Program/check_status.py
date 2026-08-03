import sqlite3

conn = sqlite3.connect('data/output/enrichment.db')
c = conn.cursor()

c.execute('SELECT status, COUNT(*) FROM supermarkets GROUP BY status')
stats = c.fetchall()
print("DB Status:")
for s, n in stats:
    print(f"  {s}: {n}")

print("\nRecent processed (non-pending):")
c.execute("""
    SELECT input_name, status, google_place_id, error_message 
    FROM supermarkets 
    WHERE status != 'pending' 
    ORDER BY updated_at DESC 
    LIMIT 30
""")
rows = c.fetchall()
for r in rows:
    pid = r[2] or "-"
    err = r[3] or "-"
    print(f"  {r[1]:10s} | {r[0]:40s} | {pid[:30]:30s} | {err[:50]}")

conn.close()
