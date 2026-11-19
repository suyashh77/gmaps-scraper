import sqlite3
import os

# Connect to the database
db_path = 'data/output/enrichment.db'
if not os.path.exists(db_path):
    print(f" Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all results
print("\n" + "="*100)
print("TEST RESULTS - ALL STORES")
print("="*100)

cursor.execute('''
    SELECT input_name, google_place_id, gmaps_rating, gmaps_reviews_count, status, error_message
    FROM supermarkets 
    ORDER BY id DESC
''')

results = cursor.fetchall()

if not results:
    print("  No results found in database")
else:
    for i, row in enumerate(results, 1):
        name, place_id, rating, reviews, status, error = row
        status_icon = "" if status == "success" else ""
        
        print(f"\n{i}. {status_icon} {name}")
        print(f"   Status: {status}")
        if place_id:
            print(f"   Place ID: {place_id}")
        if rating:
            print(f"   Rating: {rating} stars")
        if reviews:
            print(f"   Reviews: {reviews} reviews")
        if error:
            print(f"   Error: {error}")

# Get summary statistics
print("\n" + "="*100)
print("SUMMARY STATISTICS")
print("="*100)

cursor.execute('SELECT status, COUNT(*) FROM supermarkets GROUP BY status')
status_counts = cursor.fetchall()

total = sum(count for _, count in status_counts)
for status, count in status_counts:
    percentage = (count / total * 100) if total > 0 else 0
    icon = "" if status == "success" else ""
    print(f"{icon} {status.upper()}: {count} stores ({percentage:.1f}%)")

print(f"\nTotal stores processed: {total}")

# Calculate success rate
cursor.execute('''
    SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM supermarkets) 
    FROM supermarkets 
    WHERE status='success'
''')
success_rate = cursor.fetchone()[0] if cursor.fetchone() or total > 0 else 0

cursor.execute('SELECT COUNT(*) FROM supermarkets WHERE status="success"')
success_count = cursor.fetchone()[0]

if total > 0:
    actual_rate = (success_count / total) * 100
    print(f"\n Success Rate: {actual_rate:.1f}%")
    
    if actual_rate >= 80:
        print(" TEST PASSED - Success rate meets target (≥80%)")
    else:
        print("  Success rate below target")

conn.close()

print("\n" + "="*100)
print(f" Database location: {os.path.abspath(db_path)}")
print(" Logs location: logs/app.log")
print(" Screenshots location: debug_screenshots/")
print("="*100 + "\n")
