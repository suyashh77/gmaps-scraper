# Project Context Export — NC Grocery Store Reviews Dataset

**Project:** ISE495_SP26 — Building a comprehensive NC grocery store reviews dataset
**Working directory:** `c:\Users\19193\Downloads Local\School\ISE495_SP26\Gmaps Scraper\Data`
**Date:** 2026-04-10

---

## Current State

**4,889 stores | 577,059 reviews** in a single SQLite database.

| Metric | Value |
|--------|-------|
| Total stores | 4,889 |
| Total reviews | 577,059 |
| Valid ChIJ place_ids | 4,822 (98.6%) |
| Non-standard place_ids | 67 |
| NULL category | 55 |
| NULL business_status | 77 |
| Scrape: complete | 2,496 |
| Scrape: incomplete | 1,775 |
| Scrape: pending | 618 |
| Overall review completion | 41.9% (577,059 / 1,376,646 target) |

---

## Pipeline Summary

```
Overture Maps (US-wide)   451,296 POIs
       |
   NC Filter               16,680 stores
       |
 EnrichOverture             7,858 successful + 6,607 unsuccessful
       |
 Outscraper Enrich          21,840 results for 16,680 queries
       |
 Fuzzy Match + Manual       4,905 matched, 1,605 rejected, 20 deduped
       |
 Category Filter            424 GMaps categories -> 13 included, 411 excluded
       |
 Exclusions                 8,199 stores excluded (category/closed/manual)
       |
 Place_id Dedup             5,169 -> 4,779
       |
 Quality Fixes              4,779 -> 4,766 (13 true dup pairs merged)
       |
 Outscraper Re-query        261 bad place_ids -> 161 fixed, 33 dups merged
       |
 Final Removal              18 stores (12 perm closed + 6 non-grocery)
       |
 ISE 408 Merge              25 student batch DBs -> +174 stores, +19,992 reviews
       |
 CURRENT                    4,889 stores, 577,059 reviews
```

---

## Master Database

**File:** `Step_5_master_reviews.db` (~90 MB SQLite)

### `stores` table (4,889 rows)

| Column | Type | Description |
|--------|------|-------------|
| store_id | INTEGER PK | Sequential ID |
| place_id | TEXT UNIQUE | Google Maps Place ID (98.6% valid ChIJ format) |
| store_name | TEXT | Canonical name (prefers GMaps over Overture) |
| business_name | TEXT | Original Overture name |
| outscraper_name | TEXT | Google Maps name via Outscraper (113 NULL) |
| full_address | TEXT | Consolidated address |
| street | TEXT | Street from Overture (16 NULL) |
| city | TEXT | City (0 NULL) |
| state | TEXT | Always NC |
| zip | TEXT | 5-digit ZIP (3 NULL) |
| source_lat | REAL | Latitude from Overture |
| source_lon | REAL | Longitude from Overture |
| outscraper_address | TEXT | Full address from Outscraper (113 NULL) |
| gmaps_address | TEXT | Address from Google Maps API (mostly NULL) |
| phone | TEXT | Phone number (1,922 NULL) |
| rating | REAL | Google Maps rating 1-5 (1,803 NULL) |
| target_reviews | INTEGER | Expected review count (162 NULL) |
| business_status | TEXT | OPERATIONAL/CLOSED_TEMPORARILY (77 NULL) |
| category | TEXT | GMaps primary category (55 NULL) |
| subtypes | TEXT | GMaps subtypes |
| type | TEXT | GMaps type |
| reviews_scraped | INTEGER | Actual review count in DB |
| scrape_status | TEXT | complete/incomplete/pending |

### `reviews` table (577,059 rows)

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| store_id | INTEGER FK | Links to stores.store_id |
| reviewer_name | TEXT | Reviewer display name |
| rating | INTEGER | Star rating 1-5 |
| date_relative | TEXT | Relative date string (e.g. "2 months ago") |
| review_text | TEXT | Review text (51.7% empty — rating-only) |
| helpful_count | INTEGER | Helpful votes |
| photo_count | INTEGER | Photos attached |
| has_owner_response | INTEGER | Owner replied (0/1) |
| reviewer_review_count | INTEGER | Reviewer's total reviews |
| reviewer_photo_count | INTEGER | Reviewer's total photos |
| is_local_guide | INTEGER | Google Local Guide (0/1) |
| service_type | TEXT | Service type |
| scraped_at | TEXT | Scrape timestamp |
| source_db | TEXT | Source batch database name |

Index: `idx_reviews_store_id` on `reviews(store_id)`

---

## Category Breakdown

| Category | Count | % |
|----------|-------|---|
| Convenience store | 1,997 | 40.8% |
| Grocery store | 1,295 | 26.5% |
| Dollar store | 860 | 17.6% |
| Supermarket | 204 | 4.2% |
| Department store | 154 | 3.1% |
| Mexican grocery store | 85 | 1.7% |
| General store | 66 | 1.4% |
| Health food store | 62 | 1.3% |
| No category | 55 | 1.1% |
| Asian grocery store | 43 | 0.9% |
| Indian grocery store | 25 | 0.5% |
| Discount store | 21 | 0.4% |
| Warehouse club | 12 | 0.2% |
| Discount supermarket | 10 | 0.2% |

---

## All Files in Data Directory

### Core Pipeline Files

| File | Description | Size |
|------|-------------|------|
| `Step_1.0_Overture_Query.txt` | DuckDB SQL query for Overture POI extraction | 2.5K |
| `Step_1.1_POI_Overture.db` | DuckDB database of 451K US POIs | 101M |
| `Step_1.1_POI_Overture_451296.csv` | 451,296 US store POIs (13 cols) | 241M |
| `Step_1.1_POI_Overture.geojson` | GeoJSON format | 33M |
| `Step_1.2_Overture_Categories_Count.xlsx` | 4,274 Overture categories with counts | 110K |
| `Step_1.3_POI_Overture_NC.xlsx` | 16,680 NC stores (filtered from 451K) | 2.3M |
| `Step_1.4_EnrichOverture__Program/` | Python program for Google Maps enrichment | dir |
| `Step_1.5_Enrichment_Results.xlsx` | 7,858 successful + 6,607 unsuccessful enrichments | 1.6M |
| `Step_1.6_Outscraper Query.xlsx` | 16,680 Outscraper query strings | 447K |
| `Step_1.7_Outscraper_Enrichment_Result.xlsx` | 21,840 Outscraper results (8 cols) | 1.8M |
| `Step_1.8_Grocery_Store_Classification.xlsx` | 424 GMaps categories: 13 included, 411 excluded | 19K |

### Excluded Stores

| File | Description | Size |
|------|-------------|------|
| `Step_1.9_Excluded_Stores.xlsx` | Raw exclusions: 5 sheets + Excluded_Quality_Fixes (6th sheet, 18 stores) | 1.3M |
| `Step_1.9_Excluded_Stores_Merged_Table.xlsx` | Canonical merged table (8,199 rows, 45 cols) | 2.1M |
| `Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx` | Deduped to 7,497 unique stores | 1.4M |
| `Step_1.9_Excluded_Stores_Analysis.md` | Analysis report | 4.8K |

### Final Dataset & Scraping

| File | Description | Size |
|------|-------------|------|
| `Step_2.0_Final_List_of_Stores.xlsx` | **4,889 stores** — master store list export | ~900K |
| `Step_3.0_Scraping_Status_Report.xlsx` | Status report (Summary + Pending/Incomplete/Complete sheets) | ~1.8M |
| `Step_3.1_Stores_Need_Requery.xlsx` | 67 stores still needing valid place_ids | 18K |
| `Step_3.2_Outscraper_Requery_Results.xlsx` | Raw Outscraper re-query results (1,077 rows) | 79K |
| `Step_5_master_reviews.db` | **Master SQLite database** (4,889 stores + 577,059 reviews) | ~90M |
| `batch_01.db` – `batch_05.db` | Scraping queue: 2,326 stores (500 per file, last file 326) | ~0.2M each |

### Supporting Files

| File | Description |
|------|-------------|
| `Data Collection Process Doccumentation.md` | Full pipeline documentation with file refs and schemas |
| `Data Quality Report.md` | All fixes applied, remaining issues, scraping status, recommendations |
| `review_matches.py` | Tkinter flashcard app for manual store match review |
| `408 batches/` | Directory with batch scraping configs |

---

## Remaining Issues (Action Items)

1. **67 non-standard place_ids** (1.4%) — can't scrape reviews for these. Exported in Step_3.1 for another Outscraper re-query
2. **55 NULL categories** — couldn't infer from chain name or GMaps data
3. **77 NULL business_status** — EnrichOverture-only stores, status unknown
4. **2,326 stores queued** in `batch_01.db`–`batch_05.db` (1,775 incomplete + 551 pending with ChIJ place_ids)
5. **67 stores with non-ChIJ place_ids** excluded from scraping queue until re-queried via Outscraper

---

## Key Design Decisions

- **Store deduplication** was done on `place_id` (Google Maps unique identifier). Some stores have different Overture names but same physical location.
- **"Grocery store"** includes 13 GMaps categories — convenience stores (40.9%) and dollar stores (16.9%) dominate. Filter by `category` for traditional groceries.
- **Review merging** used 3-tier linking: exact place_id > name+city > geo proximity (<100m). Cross-DB dedup by (store_id, reviewer_name, rating, review_text[:50]).
- **`store_name`** prefers `outscraper_name` (Google Maps canonical) over `business_name` (Overture). Use `business_name` for the original source name.
- **`full_address`** is consolidated from best available source (outscraper_address > street+city+state+zip).

---

## User Preferences

- User is a student working on ISE495_SP26 (academic project)
- Prefers direct, actionable work over excessive explanation
- Uses Excel to inspect intermediate files (files may be locked when open)
- Team has multiple students running Outscraper batch scrapes
- "same business name + street = duplicate" was the user's dedup definition
- Street number matching means the leading digits (e.g., "1024" from "1024 Hwy West"), not alphabetic words
