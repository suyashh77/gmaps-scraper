# Data Collection Process

End-to-end pipeline that produced `Step_5_master_reviews.db`. Each step's artifacts live in this folder.

```
Overture US (451,296)
  → NC filter (16,680)
    → EnrichOverture GMaps lookup (7,858 matched + 6,607 unmatched)
      → Outscraper enrichment (21,840 results)
        → Match + dedup unmatched cohort (-2,215)
          → Category filter (13 included / 411 excluded → 6,445 dropped)
            → Manual review of unmatched (-1,605)
              → Closed-store removal (-149)
                → Place_id dedup (5,169 → 4,779)
                  → Quality fixes + re-query (4,779 → 4,733 → 4,889)
                    → Review scraping → 1.20M reviews
                      → Step_5_master_reviews.db
```

## Steps

### 1.0–1.3 — Overture pull and NC filter
- `Step_1.0_Overture_Query.txt` — DuckDB query against Overture Maps POI parquet.
- `Step_1.1_POI_Overture*` — 451,296 US POIs (CSV, DuckDB, GeoJSON).
- `Step_1.2_Overture_Categories_Count.xlsx` — 4,274 categories profiled; 28 food/grocery-related.
- `Step_1.3_POI_Overture_NC.xlsx` — 16,680 NC stores.

### 1.4–1.5 — GMaps enrichment via EnrichOverture
- `Step_1.4_EnrichOverture__Program/` (in repo root) — runs each NC POI through GMaps; extracts `place_id`, address, phone, rating, category.
- `Step_1.5_Enrichment_Results.xlsx` — 7,858 matched + 6,607 unmatched.

### 1.6–1.7 — Outscraper second-pass enrichment
- `Step_1.6_Outscraper Query.xlsx` — 16,680 NC stores queried via Outscraper.
- `Step_1.7_Outscraper_Enrichment_Result.xlsx` — 21,840 results (multi-match per query).

### 1.75 — Reconcile unmatched cohort
Fuzzy-matched the 6,607 unmatched Overture rows against Outscraper results using composite scores (name, street, city, zip, geo distance). Auto-rules for chains (Walmart, Food Lion); flashcard manual review for the residual 162. Result: 4,905 confirmed matches, 1,605 unmatched, 20 same-store duplicates removed.

### 1.8 — Grocery-store category classification
- `Step_1.8_Grocery_Store_Classification.xlsx` — 424 GMaps categories evaluated. **13 included**, 411 excluded. Excluded examples: Gas station, Restaurant, Pharmacy, Farmers' market, Liquor store, Drug store, Bakery, Butcher shop.

### 1.9 — Excluded stores audit
- `Step_1.9_Excluded_Stores.xlsx` — 5 raw sheets, 8,199 rows.
- `Step_1.9_Excluded_Stores_Merged_Table.xlsx` — canonical 8,199 × 45, mojibake cleaned.
- `Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx` — 7,421 unique place_ids, 7,497 store rows.
- `Step_1.9_Excluded_Stores_Analysis.md` — exclusion analysis.

Reasons: CATEGORY_NON_GROCERY 78.6%, MANUAL_REVIEW_UNMATCHED 19.6%, CLOSED_STORE 1.8%.

### 2.0 — Final store list
- `Step_2.0_Final_List_of_Stores.xlsx` — 4,715-row frozen export. **Superseded** by master DB's `stores` table (4,889 rows after later re-queries).

### 3.0–3.2 — Review scraping setup
- `Step_3.0_Scraping_Status_Report.xlsx` — 2026-04-10 status (frozen).
- `Step_3.1_Stores_Need_Requery.xlsx` — 72 stores needing valid place_ids.
- `Step_3.2_Outscraper_Requery_Results.xlsx` — re-query results (161 fixed, 33 dupes, 67 still non-standard).

### 3.5 — Review scraping execution

Two eras:

1. **Outscraper era (Apr 5–14)** — batch review extraction via Outscraper; ingested via student/VM-submitted SQLite batches.
2. **Self-hosted Playwright era (Apr 20+)** — `Code_v2/linux_scraper/` runs headless Chromium against GMaps. Orchestrator `run_continuous.py` runs scraper → merges → regenerates next batch from remaining work → loops.

Cross-DB review dedup uses `(store_id, reviewer_name, rating, review_text[:50])`. Every review carries a `source_db` tag for full provenance (49 distinct values to date).

### Self-hosted scraper (`Code_v2/`)

| File | Purpose |
|---|---|
| `linux_scraper/scraper.py` | Per-store worker: opens GMaps, scrolls reviews, writes to per-batch SQLite |
| `linux_scraper/database.py` | Schema + worker-safe claiming (WAL + `BEGIN IMMEDIATE`) |
| `linux_scraper/merge_results.py` | Merge batch DB into master with dedup |
| `run_continuous.py` | Orchestrator: scrape → merge → regenerate → loop |
| `generate_batches.py` | Cost-balanced N-batch splitter |

### Step 5 — Master DB build
Consolidates all batch DBs into `Step_5_master_reviews.db`. 3-tier store linking: `place_id` → `name+city` → geo (<100 m). Cross-DB dedup. Quality fixes applied (see `Data Quality Report.md`).

## Schema

### `stores` (4,889)
`store_id, place_id, store_name, business_name, outscraper_name, full_address, street, city, state, zip, source_lat, source_lon, outscraper_address, gmaps_address, phone, rating, target_reviews, business_status, category, subtypes, type, reviews_scraped, scrape_status`

`place_id` is UNIQUE. 4,822 are valid `ChIJ…`; 67 are non-standard.

### `reviews` (1,201,237)
`id, store_id, reviewer_name, rating, date_relative, review_text, helpful_count, photo_count, has_owner_response, reviewer_review_count, reviewer_photo_count, is_local_guide, service_type, scraped_at, source_db`

`store_id` FK to `stores`. 0 orphan reviews. `helpful_count`, `photo_count`, `service_type` are unreliable (see Data Quality Report).
