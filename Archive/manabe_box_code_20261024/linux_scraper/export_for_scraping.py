"""
Export stores that need scraping from the master reviews database.

Reads Step_5_master_reviews.db and produces stores_to_scrape.xlsx
containing only pending + incomplete stores with all columns the
scraper needs.

Usage:
    python -m linux_scraper.export_for_scraping
    python -m linux_scraper.export_for_scraping --db PATH/TO/master.db
    python -m linux_scraper.export_for_scraping --db master.db --out stores.xlsx
"""

import argparse
import os
import sqlite3
import sys


def export(db_path: str, out_path: str) -> None:
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = lambda c, r: dict(zip([d[0] for d in c.description], r))
    cur = conn.cursor()

    # Get all non-complete stores
    cur.execute("""
        SELECT
            store_id,
            place_id,
            store_name,
            business_name,
            outscraper_name,
            full_address,
            street,
            city,
            state,
            zip,
            source_lat,
            source_lon,
            outscraper_address,
            gmaps_address,
            phone,
            rating,
            target_reviews,
            business_status,
            category,
            subtypes,
            type,
            reviews_scraped,
            scrape_status
        FROM stores
        WHERE scrape_status IN ('pending', 'incomplete')
        ORDER BY
            CASE WHEN reviews_scraped > 0 THEN 0 ELSE 1 END,
            (COALESCE(target_reviews, 0) - reviews_scraped) ASC,
            store_id
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No stores need scraping — all complete!")
        return

    # Write to Excel
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Stores"

    headers = list(rows[0].keys())
    ws.append(headers)
    for row in rows:
        ws.append([row[h] for h in headers])

    wb.save(out_path)

    # Stats
    incomplete = sum(1 for r in rows if (r["reviews_scraped"] or 0) > 0)
    fresh = len(rows) - incomplete
    total_target = sum(r["target_reviews"] or 0 for r in rows)
    total_master = sum(r["reviews_scraped"] or 0 for r in rows)
    reviews_needed = total_target - total_master

    print(f"Exported {len(rows)} stores to {out_path}")
    print(f"  Incomplete (have reviews): {incomplete}")
    print(f"  Fresh (no reviews):        {fresh}")
    print(f"  Total target reviews:      {total_target:,}")
    print(f"  Already in master:         {total_master:,}")
    print(f"  Reviews still needed:      {reviews_needed:,}")
    print()
    print("Next steps:")
    print("  1. Push to GitHub:  git add . && git commit && git push")
    print("  2. On each machine:")
    print(f"     python -m linux_scraper --csv {os.path.basename(out_path)} --partition 1/3")
    print(f"     python -m linux_scraper --csv {os.path.basename(out_path)} --partition 2/3")
    print(f"     python -m linux_scraper --csv {os.path.basename(out_path)} --partition 3/3")


def main():
    parser = argparse.ArgumentParser(description="Export stores needing scraping from master DB")
    parser.add_argument("--db", default="Step_5_master_reviews.db",
                        help="Path to master reviews database")
    parser.add_argument("--out", default="stores_to_scrape.xlsx",
                        help="Output Excel file path")
    args = parser.parse_args()
    export(args.db, args.out)


if __name__ == "__main__":
    main()
