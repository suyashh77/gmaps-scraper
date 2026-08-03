"""
Comprehensive Field Coverage Analysis for NC Enrichment
Analyzes fill rates for all enriched fields in the database.
"""
import sqlite3
from config.settings import DB_PATH

def analyze_field_coverage():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get total NC records and successful enrichments
    cursor.execute("""
        SELECT COUNT(*) FROM supermarkets WHERE input_state = 'NC'
    """)
    total_nc = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(*) FROM supermarkets 
        WHERE input_state = 'NC' AND status = 'success'
    """)
    successful = cursor.fetchone()[0]
    
    print("=" * 80)
    print("NC ENRICHMENT - COMPREHENSIVE FIELD COVERAGE ANALYSIS")
    print("=" * 80)
    print(f"\nTotal NC Records: {total_nc:,}")
    print(f"Successfully Enriched: {successful:,} ({successful/total_nc*100:.1f}%)")
    print("\n" + "=" * 80)
    
    # Define all enriched fields to analyze
    enriched_fields = [
        ('google_place_id', 'Google Place ID'),
        ('gmaps_url', 'Google Maps URL'),
        ('gmaps_name', 'Business Name'),
        ('gmaps_address', 'Address'),
        ('gmaps_category', 'Category'),
        ('gmaps_rating', 'Rating'),
        ('gmaps_reviews_count', 'Reviews Count'),
        ('gmaps_phone', 'Phone Number'),
        ('gmaps_website', 'Website'),
        ('gmaps_hours', 'Hours'),
        ('gmaps_lat', 'Latitude'),
        ('gmaps_lon', 'Longitude'),
    ]
    
    print("\nFIELD COVERAGE ANALYSIS (All NC Records)")
    print("=" * 80)
    print(f"{'Field':<25} {'Filled':<12} {'Coverage':<12} {'Success Rate'}")
    print("-" * 80)
    
    coverage_data = []
    
    for field_name, display_name in enriched_fields:
        # Count non-null, non-empty values for this field
        cursor.execute(f"""
            SELECT COUNT(*) FROM supermarkets
            WHERE input_state = 'NC'
              AND {field_name} IS NOT NULL
              AND {field_name} != ''
              AND {field_name} != '0'
        """)
        filled_count = cursor.fetchone()[0]
        
        # Count filled values among successful records only
        cursor.execute(f"""
            SELECT COUNT(*) FROM supermarkets
            WHERE input_state = 'NC'
              AND status = 'success'
              AND {field_name} IS NOT NULL
              AND {field_name} != ''
              AND {field_name} != '0'
        """)
        filled_success = cursor.fetchone()[0]
        
        coverage_pct = (filled_count / total_nc * 100) if total_nc > 0 else 0
        success_rate = (filled_success / successful * 100) if successful > 0 else 0
        
        coverage_data.append({
            'field': field_name,
            'display': display_name,
            'filled': filled_count,
            'coverage': coverage_pct,
            'success_rate': success_rate
        })
        
        print(f"{display_name:<25} {filled_count:>6,}/{total_nc:<6,} "
              f"{coverage_pct:>6.1f}%     {success_rate:>6.1f}%")
    
    # Status breakdown
    print("\n" + "=" * 80)
    print("STATUS BREAKDOWN")
    print("=" * 80)
    
    cursor.execute("""
        SELECT status, COUNT(*) 
        FROM supermarkets 
        WHERE input_state = 'NC' 
        GROUP BY status
        ORDER BY COUNT(*) DESC
    """)
    
    for status, count in cursor.fetchall():
        pct = (count / total_nc * 100) if total_nc > 0 else 0
        print(f"{status:<15} {count:>6,} ({pct:>5.1f}%)")
    
    # Category distribution (top 10)
    print("\n" + "=" * 80)
    print("TOP 10 BUSINESS CATEGORIES")
    print("=" * 80)
    
    cursor.execute("""
        SELECT gmaps_category, COUNT(*) as count
        FROM supermarkets
        WHERE input_state = 'NC'
          AND gmaps_category IS NOT NULL
          AND gmaps_category != ''
        GROUP BY gmaps_category
        ORDER BY count DESC
        LIMIT 10
    """)
    
    categories = cursor.fetchall()
    total_categorized = sum(count for _, count in categories)
    
    for i, (category, count) in enumerate(categories, 1):
        pct = (count / total_categorized * 100) if total_categorized > 0 else 0
        print(f"{i:2d}. {category:<30} {count:>6,} ({pct:>5.1f}%)")
    
    # Data quality metrics
    print("\n" + "=" * 80)
    print("DATA QUALITY METRICS")
    print("=" * 80)
    
    # Records with ratings
    cursor.execute("""
        SELECT COUNT(*) FROM supermarkets
        WHERE input_state = 'NC'
          AND gmaps_rating IS NOT NULL
          AND gmaps_rating > 0
    """)
    with_rating = cursor.fetchone()[0]
    
    # Records with reviews
    cursor.execute("""
        SELECT COUNT(*) FROM supermarkets
        WHERE input_state = 'NC'
          AND gmaps_reviews_count IS NOT NULL
          AND gmaps_reviews_count > 0
    """)
    with_reviews = cursor.fetchone()[0]
    
    # Records with website
    cursor.execute("""
        SELECT COUNT(*) FROM supermarkets
        WHERE input_state = 'NC'
          AND gmaps_website IS NOT NULL
          AND gmaps_website != ''
    """)
    with_website = cursor.fetchone()[0]
    
    # Records with phone
    cursor.execute("""
        SELECT COUNT(*) FROM supermarkets
        WHERE input_state = 'NC'
          AND gmaps_phone IS NOT NULL
          AND gmaps_phone != ''
    """)
    with_phone = cursor.fetchone()[0]
    
    # Records with hours
    cursor.execute("""
        SELECT COUNT(*) FROM supermarkets
        WHERE input_state = 'NC'
          AND gmaps_hours IS NOT NULL
          AND gmaps_hours != ''
    """)
    with_hours = cursor.fetchone()[0]
    
    print(f"Records with Rating:        {with_rating:>6,} ({with_rating/total_nc*100:>5.1f}%)")
    print(f"Records with Reviews:       {with_reviews:>6,} ({with_reviews/total_nc*100:>5.1f}%)")
    print(f"Records with Website:       {with_website:>6,} ({with_website/total_nc*100:>5.1f}%)")
    print(f"Records with Phone:         {with_phone:>6,} ({with_phone/total_nc*100:>5.1f}%)")
    print(f"Records with Hours:         {with_hours:>6,} ({with_hours/total_nc*100:>5.1f}%)")
    
    # Average rating and review counts
    cursor.execute("""
        SELECT 
            AVG(gmaps_rating) as avg_rating,
            AVG(gmaps_reviews_count) as avg_reviews,
            MAX(gmaps_reviews_count) as max_reviews
        FROM supermarkets
        WHERE input_state = 'NC'
          AND status = 'success'
    """)
    avg_rating, avg_reviews, max_reviews = cursor.fetchone()
    
    print(f"\nAverage Rating:             {avg_rating:.2f}" if avg_rating else "\nAverage Rating:             N/A")
    print(f"Average Review Count:       {avg_reviews:.0f}" if avg_reviews else "Average Review Count:       N/A")
    print(f"Maximum Review Count:       {max_reviews:,}" if max_reviews else "Maximum Review Count:       N/A")
    
    print("\n" + "=" * 80)
    
    conn.close()

if __name__ == "__main__":
    analyze_field_coverage()
