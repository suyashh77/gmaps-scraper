import sqlite3

conn = sqlite3.connect('data/output/enrichment.db')
c = conn.cursor()

print("=" * 80)
print("NC TEST RUN - FINAL STATISTICS")
print("=" * 80)

# Overall stats
c.execute('SELECT status, COUNT(*) FROM supermarkets GROUP BY status')
stats = c.fetchall()
total = sum(n for _, n in stats)
print(f"\nTotal records in DB: {total}")
for s, n in stats:
    pct = (n / total * 100) if total > 0 else 0
    print(f"  {s:12s}: {n:6d} ({pct:.1f}%)")

# NC batch only (IDs 11 to 60)
print("\n" + "=" * 80)
print("NC BATCH (50 businesses processed this run)")
print("=" * 80)

c.execute("SELECT status, COUNT(*) FROM supermarkets WHERE id >= 11 AND id <= 60 GROUP BY status")
nc_stats = c.fetchall()
nc_total = sum(n for _, n in nc_stats)
for s, n in nc_stats:
    pct = (n / nc_total * 100) if nc_total > 0 else 0
    print(f"  {s:12s}: {n:3d} / {nc_total} ({pct:.1f}%)")

# Successful businesses
print("\n" + "=" * 80)
print("SUCCESSFUL EXTRACTIONS (NC Batch)")  
print("=" * 80)

c.execute("""
    SELECT input_name, input_city, google_place_id, gmaps_rating, 
           gmaps_reviews_count, gmaps_address, gmaps_category
    FROM supermarkets 
    WHERE id >= 11 AND id <= 60 AND status = 'success'
    ORDER BY id
""")
successes = c.fetchall()
for i, r in enumerate(successes, 1):
    name, city, pid, rating, reviews, addr, cat = r
    print(f"\n  {i}. {name}")
    print(f"     City: {city or '-'}")
    print(f"     Place ID: {pid}")
    print(f"     Rating: {rating or '-'} | Reviews: {reviews or 0}")
    print(f"     Address: {addr or '-'}")
    print(f"     Category: {cat or '-'}")

# Failed businesses
print("\n" + "=" * 80)
print("FAILED EXTRACTIONS (NC Batch)")
print("=" * 80)

c.execute("""
    SELECT input_name, input_city, input_lat, input_lon, error_message
    FROM supermarkets 
    WHERE id >= 11 AND id <= 60 AND status = 'failed'
    ORDER BY id
""")
failures = c.fetchall()
for i, r in enumerate(failures, 1):
    name, city, lat, lon, err = r
    in_nc = 33.8 <= (lat or 0) <= 36.6 and -84.4 <= (lon or 0) <= -75.4
    loc_note = "in NC" if in_nc else "OUTSIDE NC"
    print(f"  {i}. {name} ({city or '-'}) [{loc_note}] lat={lat}, lon={lon}")
    print(f"     Error: {err}")

# Data quality analysis
print("\n" + "=" * 80)
print("DATA QUALITY ANALYSIS (NC Batch)")
print("=" * 80)

c.execute("""
    SELECT COUNT(*) FROM supermarkets 
    WHERE id >= 11 AND id <= 60 
    AND input_lat BETWEEN 33.8 AND 36.6 
    AND input_lon BETWEEN -84.4 AND -75.4
""")
in_nc = c.fetchone()[0]

c.execute("""
    SELECT COUNT(*) FROM supermarkets 
    WHERE id >= 11 AND id <= 60 
    AND NOT (input_lat BETWEEN 33.8 AND 36.6 AND input_lon BETWEEN -84.4 AND -75.4)
""")
out_nc = c.fetchone()[0]

print(f"  Coordinates within NC bounds: {in_nc}")
print(f"  Coordinates OUTSIDE NC bounds: {out_nc}")

# Success rate for in-NC vs out-NC
c.execute("""
    SELECT status, COUNT(*) FROM supermarkets 
    WHERE id >= 11 AND id <= 60 
    AND input_lat BETWEEN 33.8 AND 36.6 
    AND input_lon BETWEEN -84.4 AND -75.4
    GROUP BY status
""")
in_nc_stats = dict(c.fetchall())

c.execute("""
    SELECT status, COUNT(*) FROM supermarkets 
    WHERE id >= 11 AND id <= 60 
    AND NOT (input_lat BETWEEN 33.8 AND 36.6 AND input_lon BETWEEN -84.4 AND -75.4)
    GROUP BY status
""")
out_nc_stats = dict(c.fetchall())

in_nc_success = in_nc_stats.get('success', 0)
in_nc_total = sum(in_nc_stats.values())
out_nc_success = out_nc_stats.get('success', 0)
out_nc_total = sum(out_nc_stats.values())

in_nc_rate = (in_nc_success / in_nc_total * 100) if in_nc_total > 0 else 0
out_nc_rate = (out_nc_success / out_nc_total * 100) if out_nc_total > 0 else 0

print(f"\n  In-NC success rate: {in_nc_success}/{in_nc_total} ({in_nc_rate:.1f}%)")
print(f"  Outside-NC success rate: {out_nc_success}/{out_nc_total} ({out_nc_rate:.1f}%)")

conn.close()
print("\n" + "=" * 80)
