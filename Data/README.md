# Data — lineage, schema, quality history

Everything under `Data/` except this file is gitignored (databases, spreadsheets, logs).
**For the current numbers and the plan, read [`STATUS.md`](../STATUS.md) at the repo root.**
This file is the reference behind it: how the data was produced (Part A), the exclusion
audit (Part B), the quality baseline as audited on 2026-04-30 and what remediation changed
(Part C), and the historical backlog with what has since been closed (Part D).

## Folder layout

| path | what |
|---|---|
| `Step_5_master_reviews_FROZEN_2026-10-24.db` | **the dataset (freeze v2, final)**: a read-only `VACUUM INTO` copy of master after dewey27/28/30a/30b (4,442 stores · 1,941,538 reviews). SHA-256 in `Remediation/master_freeze_manifest_v2.json`. Analysis reads this. |
| `Step_5_master_reviews_FROZEN_2026-10-11.db` | freeze v1 (4,447 stores · 1,811,845 reviews), kept for provenance. |
| `Step_5_master_reviews.db` | the working master the frozen copy was taken from — `stores` + `reviews` + `quarantined_reviews` + `accepted_flags`. Writers act on this one; re-freeze after any change. |
| `Step_5_out_of_scope.db`, `Step_5_closed_stores.db`, `Step_5_excluded_stores.db` | stores moved out of master by owner ruling, with their reviews: out of scope (4,736), closed (62), duplicates and unfixable pages (221). Same `stores`/`reviews`/`quarantined_reviews` shape. |
| `Step_6_grocery_pois.db` | the Dewey (SafeGraph) grocery POI list (3,393 POIs), snapshot of 2026-09-24, matched to master via `master_store_id`; accepted as is |
| `Step_1_NC_Food_Retail_Frame.db` | 36,820-store frame frozen 2026-09-08 (SHA in `Frame/reports/frame_manifest.json`) |
| `Step_1.0` … `Step_3.2` | January–April pipeline artifacts (Overture pull, enrichment, category sheet, exclusion audit, status reports). `Frame/s04_existing.py` still reads `Step_1.3`, `Step_1.9_…Deduplicated_Store_Level` and master. |
| `batches/` | scrape queues, all run and merged: `run1/2`, `grocery1-3`, `dewey1..9`, `dewey21..26`; `batch1/2` (May 2026, recovered by r01). `gas_station.db` was never shipped |
| `backups/` | master backups written by `Remediation/lib_master.backup()` and `merge_results`. Keep `…_r02_20260905_160506` (audited 30-Apr baseline) and the newest. |
| `returns/<YYYYMMDD>_<machine>/` | what volunteers sent back: result DBs, final log, last report. Merged into master / probe.db. |
| `reference/` | Teresa's Raleigh pilot notebook and output CSV — the analysis schema the export must match |
| `exports/` | **`NC_Grocery_POIs.xlsx`** (one row per store in the frozen master; `collection_status` = complete / complete_google_limit / no_reviews_on_google; Dewey placekey/brand/chain where matched; one store's coordinates blanked as known-wrong), the manual-check workbooks (`Manual_Check_Round4/5_*.xlsx`) and older renders |
| `reports/` | `status_report.html`, scraper xlsx reports |

The probe queue (`Frame/build/probe.db`) lives with `Frame/`.

---

# Part A — How the data was produced

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

### Steps

#### 1.0–1.3 — Overture pull and NC filter
- `Step_1.0_Overture_Query.txt` — DuckDB query against Overture Maps POI parquet.
- `Step_1.1_POI_Overture*` — 451,296 US POIs (CSV, DuckDB, GeoJSON).
- `Step_1.2_Overture_Categories_Count.xlsx` — 4,274 categories profiled; 28 food/grocery-related.
- `Step_1.3_POI_Overture_NC.xlsx` — 16,680 NC stores.

#### 1.4–1.5 — GMaps enrichment via EnrichOverture (archived)
- `Archive/Step_1.4_EnrichOverture__Program/` — historically ran each NC POI through GMaps. Superseded by `Frame/probe.py`, which performs live verification.
- `Step_1.5_Enrichment_Results.xlsx` — 7,858 matched + 6,607 unmatched (frozen 2026-04-30).

#### 1.6–1.7 — Outscraper second-pass enrichment
- `Step_1.6_Outscraper Query.xlsx` — 16,680 NC stores queried via Outscraper.
- `Step_1.7_Outscraper_Enrichment_Result.xlsx` — 21,840 results (multi-match per query).

#### 1.75 — Reconcile unmatched cohort
Fuzzy-matched the 6,607 unmatched Overture rows against Outscraper results using composite scores (name, street, city, zip, geo distance). Auto-rules for chains (Walmart, Food Lion); flashcard manual review for the residual 162. Result: 4,905 confirmed matches, 1,605 unmatched, 20 same-store duplicates removed.

#### 1.8 — Grocery-store category classification
- `Step_1.8_Grocery_Store_Classification.xlsx` — 424 GMaps categories evaluated. **13 included**, 411 excluded. Excluded examples: Gas station, Restaurant, Pharmacy, Farmers' market, Liquor store, Drug store, Bakery, Butcher shop.

#### 1.9 — Excluded stores audit
- `Step_1.9_Excluded_Stores.xlsx` — 5 raw sheets, 8,199 rows.
- `Step_1.9_Excluded_Stores_Merged_Table.xlsx` — canonical 8,199 × 45, mojibake cleaned.
- `Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx` — 7,421 unique place_ids, 7,497 store rows.
- Part B below — exclusion analysis.

Reasons: CATEGORY_NON_GROCERY 78.6%, MANUAL_REVIEW_UNMATCHED 19.6%, CLOSED_STORE 1.8%.

#### 2.0 — Final store list
- `Step_2.0_Final_List_of_Stores.xlsx` — 4,715-row frozen export. **Superseded** by master DB's `stores` table (4,889 rows after later re-queries).

#### 3.0–3.2 — Review scraping setup
- `Step_3.0_Scraping_Status_Report.xlsx` — 2026-04-10 status (frozen).
- `Step_3.1_Stores_Need_Requery.xlsx` — 72 stores needing valid place_ids.
- `Step_3.2_Outscraper_Requery_Results.xlsx` — re-query results (161 fixed, 33 dupes, 67 still non-standard).

#### 3.5 — Review scraping execution

Two eras:

1. **Outscraper era (Apr 5–14)** — batch review extraction via Outscraper; ingested via student/VM-submitted SQLite batches.
2. **Self-hosted Playwright era (Apr 20+)** — `Scraper_v2/linux_scraper/` runs headless Chromium against GMaps. Orchestrator `run_continuous.py` runs scraper → merges → regenerates next batch from remaining work → loops.

Cross-DB review dedup uses `(store_id, reviewer_name, rating, review_text[:50])`. Every review carries a `source_db` tag for full provenance (49 distinct values to date).

#### Self-hosted scraper (`Scraper_v2/`)

| File | Purpose |
|---|---|
| `linux_scraper/scraper.py` | Per-store worker: opens GMaps, scrolls reviews, writes to per-batch SQLite |
| `linux_scraper/database.py` | Schema + worker-safe claiming (WAL + `BEGIN IMMEDIATE`) |
| `linux_scraper/merge_results.py` | Merge batch DB into master with dedup |
| `run_continuous.py` | Orchestrator: scrape → merge → regenerate → loop |
| `tests/` | Unit and integration tests for scraper components |

#### Step 5 — Master DB build
Consolidates all batch DBs into `Step_5_master_reviews.db`. 3-tier store linking: `place_id` → `name+city` → geo (<100 m). Cross-DB dedup. Quality fixes applied (see Part C).

### Schema

#### `stores` (4,447 at the 2026-10-11 freeze; 5,226 when first documented)
`store_id, place_id, store_name, business_name, outscraper_name, full_address, street, city, state, zip, source_lat, source_lon, outscraper_address, gmaps_address, phone, rating, target_reviews, business_status, category, subtypes, type, reviews_scraped, scrape_status`

Also: `feature_id`, `review_cap`, `qa_flags`, `verified_at`, `status_reason`. `status_reason` is the audit trail of a ruling (stalled, within tolerance, no data, closed, duplicate…); a store that has one is never recomputed from counts. `scrape_status` is `complete` for every store at the freeze.

`place_id` is UNIQUE. Valid `ChIJ…` format preferred; non-standard IDs tracked for re-query (see Remediation/r00_validate.py).

#### `reviews` (1,811,845 at the freeze; 1,368,169 when first documented)
`id, store_id, reviewer_name, rating, date_relative, review_text, helpful_count, photo_count, has_owner_response, reviewer_review_count, reviewer_photo_count, is_local_guide, service_type, scraped_at, source_db, review_id, machine_id`

Also `review_text_sha256`. `quarantined_reviews` has the same columns plus `quarantine_reason` and `quarantined_at`: reviews set aside (wrong page, duplicate store) and never deleted. The merge treats them as already known; `Remediation/r17_restore_confirmed.py` moves back the ones a scrape of the store's own page confirms.

`store_id` FK to `stores`. 0 orphan reviews. `review_id` (stable, Google-issued) and `machine_id` (scraper host) added 2026-09-05 for improved deduplication. `helpful_count`, `photo_count`, `service_type` are UNAVAILABLE (confirmed 2026-09-13; see Remediation §9).

### Step 1 rebuilt (2026-09-08) and probed (2026-09-10 → 2026-09-15)

The NC retail frame was rebuilt from four independent sources to correct the unevenness in the original pipeline that manufactured food deserts as data artifacts. **36,820 stores** in 100 counties. The frame resolved ~11,791 place_ids via a two-week Google Maps probe, producing a scrape-ready queue. See `Frame/README.md` for the rebuild, probe process, and probe results.

Data flow: 36,820 frame → 16,751 probe-list (CORE + FRESH + SPECIALTY tiers; MARGINAL excluded) → ~11,791 place_ids resolved → scrape batch built → ready for Step 2.

### Collection kits

Volunteer and VM scraping is orchestrated via deployment kits built from canonical code. Each kit is self-contained, carries versioned dependencies, and validates its contents on first run. See `Deploy/README.md` for kit assembly, templates, and testing.

---

# Part B — Step 1.9 excluded-stores audit

> Frozen step-specific analysis from 2026-04-06. For dataset-wide quality issues, see Part C.

- Source file: `Data/Step_1.9_Excluded_Stores.xlsx`
- Merged rows: **8,199**
- Merged columns: **45**
- Deduplicated store rows: **7,497**
- Compression ratio (`merged/dedup`): **1.094**
- Category conflicts at store level: **16**
- Status conflicts at store level: **0**

### Exclusion Reason Breakdown

| exclusion_reason | rows | percent |
| --- | --- | --- |
| CATEGORY_NON_GROCERY | 6,445 | 78.61% |
| MANUAL_REVIEW_UNMATCHED | 1,605 | 19.58% |
| CLOSED_STORE | 149 | 1.82% |

### Sheet-Level Profile

| source_sheet | rows | unique_place_ids | missing_place_id | duplicate_place_id_rows |
| --- | --- | --- | --- | --- |
| Excluded_Category_Part1 | 2,671 | 2,552 | 0 | 234 |
| Excluded_Category_Part2 | 3,774 | 3,710 | 64 | 64 |
| Excluded_Closed_Part1 | 23 | 12 | 11 | 11 |
| Excluded_Closed_Part2 | 126 | 126 | 0 | 0 |
| Excluded_Manual_Review | 1,605 | 1,486 | 1 | 225 |

### Dedup Group Size Distribution

| records_merged_count | store_count | percent_of_dedup_stores |
| --- | --- | --- |
| 1 | 6,888 | 91.88% |
| 2 | 532 | 7.10% |
| 3 | 62 | 0.83% |
| 4 | 14 | 0.19% |
| 5 | 1 | 0.01% |

### Largest Dedup Groups

| store_key | records_merged_count | sample_name | source_sheets |
| --- | --- | --- | --- |
| place_id:ChIJYc_yCuf1q4kR_XaGn-bI0Q4 | 5 | Cox & Cox Farms | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJxVCmVnn1rIkRsADIqUY_PM4 | 4 | The Evening Farmers Market | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJs58kNLCPr4kRG-akd1fVD8o | 4 | Edenton Farmers Market | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJuS6yWOCZqIkR3v7_uO4k7YM | 4 | Salty Catch Seafood Company | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJv-tLYnn1rIkRK7XVyy1Aofs | 4 | Gene's Produce | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJ_ZPfky4lpYkRNC0xZYMMBHQ | 4 | Community Market in Grandy | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJ67gAF-2MWYgRpOxoXe5xnyk | 4 | Foothills Butcher Bar | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJGZ4dvNCYVogR3T3YoD4oTKs | 4 | Farmers Market | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJN6XdbQySrokRozM97zW44Sc | 4 | Chloe’s Farm Stand | Excluded_Manual_Review |
| place_id:ChIJIa6pEHn1rIkRk9xS-ZER4Jk | 4 | Quality Produce | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJqxSoJCL5WYgR66tTCqBZS7Q | 4 | Swannanoa Farmers Market | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJl2Krp3UkVIgRnA6hi2rjWdU | 4 | Biltong USA | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJsd_1Fit1r4kRNVsPwie8f1Q | 4 | Bay Brothers Seafood | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJlU93OhkoV4gRvDQ0G6b8FB0 | 4 | South Mountain Food Outlet | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJQz4hMvm1qIkRXSz-piq805E | 4 | 7 Even | Excluded_Manual_Review |
| place_id:ChIJicqTjJiGrIkRItTfLN18yMc | 3 | Carolina Meats & Etc. | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJaw_omHufVogRGfz5PKNUoGs | 3 | Carolinas Foods | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJx3JhFCXnqokRR908f3GZxXk | 3 | Sun-Do Kwik Shop | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJv6TV-wakqYkRrJID8_I1KwA | 3 | Andrew's Produce Stand | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJhTKdmRjBVogRBEkD84R9ucc | 3 | Gaston International Market | Excluded_Category_Part1 \| Excluded_Manual_Review |

### Key Null Rates

| column | null_rows | null_rate |
| --- | --- | --- |
| google_place_id | 76 | 0.93% |
| source_business_name | 14 | 0.17% |
| source_street | 139 | 1.70% |
| source_city | 41 | 0.50% |
| source_zip | 137 | 1.67% |
| google_category | 273 | 3.33% |
| google_business_status | 3,786 | 46.18% |

### Text Cleanup Impact

- Mojibake hits before cleaning: **99**
- Mojibake hits after cleaning: **0**
- Text cells normalized: **105**
- Mojibake cells repaired: **99**
- Rows touched by mojibake repair: **99**

### Deliverables

- Merged table (Excel): `Data/Step_1.9_Excluded_Stores_Merged_Table.xlsx`
- Merged table (CSV): `Data/Step_1.9_Excluded_Stores_Merged_Table.csv`
- Deduplicated table (Excel): `Data/Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx`
- Deduplicated table (CSV): `Data/Step_1.9_Excluded_Stores_Deduplicated_Store_Level.csv`
- Analysis report: Part B of this file

---

# Part C — Quality baseline (audited 2026-04-30) and what remediation changed

**Snapshot 2026-04-30**. Remediation r01–r04, r08, r10 were applied 2026-09-05/06 and the scraper was repaired 2026-09-05/09 — see `Remediation/README.md`. The baseline numbers below are kept for the record. For the current state (5,226 stores, 1,368,169 reviews), run `python Remediation/r00_validate.py`.

Known issues with the dataset. Status and remaining work: `STATUS.md` at the repo root; historical backlog in Part D.

---

### Critical issues

#### 157 over-scraped stores (wrong-location bug) — remediated 2026-09-06
`reviews_scraped > target_reviews + 5` in the Apr 30 snapshot. Remediation split into three classes resolved separately:

- **Stale target (107 stores)** — target under 10 against hundreds of real reviews. Fixed by backfilling target from live Google count (Remediation r03, r04).
- **Double scrape (10 stores)** — 1.7×–2.3× target with matching duplicate counts. Fixed by deleting exact duplicates (Remediation r03).
- **Wrong location (9 stores)** — trustworthy target 50+ with large multiple. Requires manual adjudication via place_id verification (Remediation §3, §6). See `Remediation/r06_contamination_worklist.py`.

#### 11 disputed exclusion-list cases
**0 closed-store leaks.** ✓ Of 7,421 unique excluded `place_id`s, 371 also appear in master:
- 364 with reason `MANUAL_REVIEW_UNMATCHED` — expected (per-pair exclusions, dual pathway).
- **7 with `CATEGORY_NON_GROCERY`** — disputed (Gas station, Produce market, etc. on excluded side; grocery category in master).
- **4 with NULL master category** — also in exclusion list.

Full list in Part D, P0.4.

#### 67 non-standard `place_id`s
Not in `ChIJ…` format. Cannot be re-scraped. Need fresh Outscraper/Places API queries (Part D, P0.3).

#### 55 stores with NULL `category`
Cannot be filtered by category. 1.1% of stores. Backfill or exclude (Part D, P0.5).

#### Major-chain coverage gaps (Apr 30 snapshot, remediated 2026-09-06)
| Chain | Apr 30 | Current (Sep 16) | NC locations | Notes |
|---|---:|---:|---:|---|
| Costco | **0** | **9** | 9 | Recovered by r04 (category lookup fallthrough) |
| Whole Foods | 4 | 6 | ~8+ | Partial recovery |
| Earth Fare | 3 | — | 8+ | NC HQ |
| Wegmans | 1 | — | 2+ | — |
| Sam's Club | 8 | — | 14+ | — |
| Harris Teeter | 33 | 47 | ~80–100 | Partial recovery |
| Ingles Markets | 33 | 50 | ~70+ | Partial recovery |

---

### Field-level issues

#### Stores
| Field | Null/blank | % |
|---|---:|---:|
| `gmaps_address` | 4,002 | 81.9% (use `full_address`) |
| `phone` | 2,096 | 42.9% |
| `rating` | 1,977 | 40.4% |
| `subtypes` / `type` | 300 | 6.1% |
| `outscraper_name` / `outscraper_address` | 287 | 5.9% |
| `target_reviews` | 157 | 3.2% |
| `business_status` | 77 | 1.6% |
| `category` | 55 | 1.1% |
| `street` | 17 | 0.3% |
| `zip` | 4 | 0.1% |
| `city` | 0 | — |

`scrape_status` is **stale** — drifted from `reviews_scraped` vs `target_reviews`. Don't trust; use the comparison.

#### Reviews
| Field | Null/zero | Note |
|---|---:|---|
| `service_type` | **100%** NULL | UNAVAILABLE in current Google UI (Remediation §9); confirmed not a scraper bug |
| `helpful_count` | **100%** zero | UNAVAILABLE in current Google UI (Remediation §9); confirmed not a scraper bug |
| `photo_count` | **100%** zero | UNAVAILABLE in current Google UI (Remediation §9); confirmed not a scraper bug |
| `review_text` | 593,302 (49.4%) | rating-only — typical for GMaps |
| `rating` | 569 | 0.05% |
| `date_relative` | 568 | 0.05% |
| `is_local_guide=1` | 872,427 | 72.6% — high baseline |
| `has_owner_response=1` | 90,301 | 7.5% |

#### FK integrity
0 orphan reviews. ✓

---

### Review-level issues

#### 734 within-store duplicate reviews
Same `(store_id, reviewer_name, rating, review_text[:50])` survived merge dedup. Likely 50-char prefix matches but full text differs. One-time cleanup pass needed (Part D, P1.3).

#### 2,571 cross-store same-text groups
Same reviewer + same text (≥30 chars) at 2+ stores. Mix of legitimate (chain reviewer) and wrong-location merge artifacts. Needs targeted investigation for the high-fanout cases (Part D, P1.4).

#### Reviewer-name is non-unique
"Chris" reviews 240 distinct stores; "J", "John", "Mike" each 200+. These are common first names, not stable IDs. Treat `reviewer_name` as a display label, not a key.

#### Date coverage skews old
1.07M reviews say "year(s) ago"; only 12,551 (~1%) are within the last month. `date_relative` is Google's natural-language string — no absolute dates parsed yet (Part D, P2.1).

#### Rating distribution (avg 4.07)
1★: 8.9% · 2★: 4.6% · 3★: 10.9% · 4★: 21.6% · 5★: 53.9%.

#### Text length percentiles (n=607,935)
p25=29 · p50=63 · p75=145 · p90=307 · p99=957 · max=4,096 chars.

---

### Cleanups already applied

| Fix | Impact |
|---|---|
| Place_id deduplication | 5,169 → 4,779 (-390) |
| Wrong-state Outscraper matches removed | 36 stores cleared, 764 bad reviews dropped |
| True dup-pair merging | -13 stores |
| Category inference from chain names | NULL category 354 → 55 |
| Target backfill from batch metadata | NULL-with-reviews 141 → 0 |
| Non-grocery category scrub | 26 stores cleaned |
| Address consolidation | 0 missing cities |
| Name consolidation | `store_name` from best source |
| Cross-DB review dedup | `(store_id, reviewer, rating, text[:50])` |
| Outscraper re-query | 161 place_ids fixed, 33 dupes merged |
| Mojibake repair (excluded audit) | 99 cells fixed |
| `CLOSED_PERMANENTLY` removal | -12 stores, -579 reviews |
| Non-grocery stub removal | -6 stores (5 gas stations + 1 deli), -434 reviews |

---

### Recommendations for analysts

1. Use `store_name` and `full_address` (canonical, consolidated).
2. Filter by `category`; drop the 55 NULL-category rows for category-stratified work.
3. Use `reviews_scraped >= target_reviews` for completion, not the `scrape_status` column.
4. Treat `reviewer_name` as non-unique.
5. Ignore `helpful_count`, `photo_count`, `service_type` — unreliable.
6. Spot-check stores in the over-scraped list before featuring them in top-N analyses.
7. Text corpus is ~608K reviews, not 1.20M (49.4% are rating-only).

---

# Part D — Historical backlog

**Current plan lives in `STATUS.md` at the repo root; this file archives completed work and the pre-remediation backlog.**

Original items to take `Step_5_master_reviews.db` from "usable for analysis" to "authoritative dataset". Ordered by impact. Remediation 2026-09-05/06 closed most items; remainder is below.

---

### P0 — Required for "authoritative" claim

#### ✔ P0.1  Finish scraping the remaining reviews (partial)
- Brought master from 1,206,184 to 1,368,169 reviews (+161,985 from volunteer returns 2026-09-16). Remaining: **902 stores, 105,954 reviews** (as of 2026-09-16).
- Volunteer machines (Forrester `sl9mw2`, Manabe `alodov`) still running old kit; new scraper fixes available in Deploy/ for next round.
- **Effort:** Depends on whether volunteers return or kit needs rebuilding.

#### ✔ P0.2  Reconcile the 157 over-scraped stores (split & handled)
- Remediation split into three classes (Remediation §3): stale target (107, fixed by r03/r04), double-scrape (10, fixed by r03), wrong-location (9, still needs adjudication).
- Scraper now restores place_id navigation as strategy 0, verifies place_id on landing, reads business pin instead of map camera for distance checks, and fails closed on verification (Remediation §8).
- **Still open:** 9 wrong-location stores need manual adjudication via `Remediation/r06_contamination_worklist.py`.

#### P0.3  Resolve the 67 non-standard `place_id`s
- Not in `ChIJ…` format → cannot be re-scraped.
- **Action:** Re-query via Outscraper/Places API using `name + address`. Replace with valid `ChIJ…` or move to exclusion list with reason `INVALID_PLACE_ID`.
- **Done when:** `SELECT COUNT(*) FROM stores WHERE place_id NOT LIKE 'ChIJ%'` = 0.
- **Effort:** ~2 hours.

#### P0.4  Adjudicate the 11 disputed exclusion-list cases
7 in master with reason `CATEGORY_NON_GROCERY`:
- 4855 Country Fruit Stand Number 2 (excluded as: Produce market)
- 4852 bp America (Gas station)
- 4884 Hometown Market Place (attractions)
- 4885 Joe Warren's Service Center (Gas station)
- 4883 Golden Key European Food Market (Gourmet grocery store)
- 4853 College Food Mart (Gas station)
- 2801 Battleground BP (Gas station)

4 in master with NULL category that are also in the exclusion list:
- 4626 Grandmas Country Store
- 1872 BTO Convenience Store (CLOSED_TEMPORARILY)
- 4159 The Challis Stead
- 2800 Oakboro Oil Co Inc (likely gas/oil)

- **Done when:** Each is either retained (with documented reason) or removed (with exclusion-list update).
- **Effort:** ~1 hour.

#### P0.5  Backfill the 55 NULL-category stores
- **Action:** Chain-name pass first; manual GMaps check for residuals. Move non-grocery to exclusion list.
- **Done when:** No NULL `category` rows in master.
- **Effort:** ~1 hour.

---

### P1 — High-value quality gains

#### ✔ P1.1  Audit major-chain coverage (Costco missing entirely)
- ✔ Costco: 0 → 9 (recovered by r04; all place_ids present in exclusion audit, dropped by category lookup fallthrough).
- Partial recovery from frame rebuild: Harris Teeter 33 → 47, Ingles 33 → 50, Aldi → 59, Publix → 53, Whole Foods 4 → 6.
- Gaps remain in old master but frame build has corrected the categorization logic (Remediation §4).

#### ✔ P1.2  Geographic completeness check (solved by frame rebuild)
- The frame rebuild identified the root cause: regional retention skew. Old master retained 14.4%–37.8% by ZIP3 prefix; Charlotte was 2.6× Raleigh. This was the data artifact behind false food deserts.
- New frame (36,820 stores) built from four independent sources with corroboration evidence. See `Frame/README.md` §Validation: G3 shows 1.19× regional variation (vs 2.4× in old frame).

#### ✔ P1.3  Within-store duplicate-review cleanup
- 734 duplicates in Apr 30 snapshot split: 331 text-bearing (deleted by r03), 396 rating-only (kept on purpose — "Chris" reviews 240 stores; 2 different people at one store is ordinary). r03 deletes only deterministic duplicates, leaves ambiguous cases alone.

#### P1.4  Cross-store same-text reviews investigation
- **Gap:** 2,571 (reviewer, text) pairs at 2+ stores. Mix of legitimate chain reviewers and wrong-location merge artifacts.
- **Action:** Check parent-store geographic spread per group. Groups with parents >50 km apart likely have a bad join.
- **Done when:** Each high-fanout (>5 stores) group is resolved.
- **Effort:** ~2 hours.

#### ✔ P1.5  Backfill the untargeted stores (status computed)
- Status: 493 stores carry `unknown_target`. 157 marked complete with zero reviews (r03 demoted these to `incomplete`). Remediation r00 now reports this status on every audit run.
- These stores are now reachable by the scraper. See Remediation §7 definition of done: item 2.

#### ✔ P1.6  Re-scrape `helpful_count`, `photo_count` (NOT RECOVERABLE)
- Confirmed 2026-09-13 by `probe_review_dom.py` running live against real Google Maps pages: helpful count button, photo container, and service_type elements are not present in current UI. Not a scraper bug; elements genuinely unavailable. Remediation §9 confirmed with DOM sampling.

#### P1.7  Drop or repurpose `service_type` and `gmaps_address`
- `service_type` 100% NULL. `gmaps_address` 81.9% NULL.
- **Action:** Drop or backfill from `outscraper_address` / `full_address`.
- **Effort:** ~30 minutes (drop) or ~3 hours (backfill).

---

### P2 — Polish

#### P2.1  Parse `date_relative` into absolute dates
- Compute `date_estimated_min` / `date_estimated_max` window from `(date_relative, scraped_at)`. Needed for any longitudinal analysis.
- **Effort:** ~3 hours.

#### ✔ P2.2  Refresh stale `scrape_status` column
- Remediation r03 recomputes on every audit. Was 661 stale in Apr 30; now 0. Canonical comparison is `reviews_scraped >= target_reviews`.

#### ✔ P2.3  Build `review_sources` provenance table
- `review_id` and `machine_id` now carried in every row (r02). Enables unambiguous provenance per volunteer machine. 49 distinct `source_db` values tracked since Apr 5.

#### P2.4  Address-quality pass
- 17 missing `street`, 4 missing `zip`. GMaps lookup per place_id.
- **Effort:** ~30 minutes.

#### P2.5  Normalize anonymous-reviewer rows
- 70 NULL/blank `reviewer_name`. `UPDATE … SET reviewer_name='Anonymous'`.
- **Effort:** 1 minute.

#### P2.6  Language detection on review text
- Run `langid` or `fastText lid.176` over text-bearing reviews. Store in `review_text_lang`. Spanish reviews likely a non-trivial slice (Mexican grocery stores).
- **Effort:** ~2 hours.

#### P2.7  Sentiment baseline
- VADER or small HuggingFace model → `review_sentiment` REAL. Optional topic clustering.
- **Effort:** ~6 hours.

---

### P3 — Repository hygiene

#### P3.1  Decide canonical Excel exports vs SQLite
- `Step_2.0_Final_List_of_Stores.xlsx` (4,715) ≠ master `stores` (4,889). Either regenerate on every freeze or formally deprecate the Excel.

#### P3.2  Add `make_data.py` end-to-end runner
- Single command-runner that lists every Step. Lets an external user reproduce the dataset. Bare minimum bar for "authoritative".

---

### Definition of Done — "Authoritative" (from Remediation §7)

1. `r00_validate.py --fail-on HIGH` exits 0.
2. The 16,680 NC candidates are fully reconciled: every row in master, exclusion audit with reason, or documented recovery worklist. No residual.
3. Retention by ZIP prefix is flat enough that no region is an outlier, or variation is explained and documented.
4. Every major NC chain present at its known store count.
5. Snapshot frozen with SHA256, and this report regenerated against it.

Current status: (1) not yet (9 wrong-location stores to adjudicate), (2) not yet (4,294 never reached lookup, 1,988 vanished after lookup), (3) solved by frame rebuild (1.19× variation vs 2.4× before), (4) partial recovery via remediation, (5) pending adjudication queue resolution.


---

# Part E — Final state (2026-10-11)

Freeze v1 was taken on 2026-10-11. The owner then tightened the completion rule (a store short of its verified
target is rescraped), so a last pass (`dewey27`, 629 stores) was run and merged on 2026-10-12 (1,817,972 reviews); a closing decision for the last 545 short stores and a re-freeze remain,
See [`STATUS.md`](../STATUS.md) for the current numbers, the decision log and what is left.

**Definition of Done, as met:**

1. `r00_validate.py --fail-on HIGH` exits 0 — **met** (no BLOCKER, no HIGH; worst severity MED).
2. Store list reconciled — **closed by owner ruling** (2026-10-11): the store list is complete; the
   4,447 stores in `exports/NC_Grocery_POIs.xlsx` are the list.
3. Regional retention — solved by the frame rebuild and the probe.
4. Major chains — covered through the frame (Frame gate G4).
5. Snapshot frozen with SHA-256 — **met**: `Step_5_master_reviews_FROZEN_2026-10-11.db`,
   `f9232b5f57f6e7d118c75a6475a5a73044cc8f8fea21c23ac7d8ffac925b0f82`, manifest in
   `Remediation/master_freeze_manifest.json` (also carries the side databases' SHAs and counts).

**What changed since Part C's baseline**, in order: wrong-location and duplicate cleanup (r12, r13);
five manual rounds against live Google pages (r14 for rounds 2–3, r16 for rounds 4–5); the owner's
tolerance rule (r13/r15); a retry pass that showed the "Google ceiling" was not a hard limit
(+24,481 reviews); and r17, which restored 4,529 reviews the cleanup had wrongly set aside.

**Review counts that need a label in the analysis:** 281 stores hold the 1,500-review cap
(`target_reviews` is capped at 1,500 whatever Google shows), 1,444 stores are
`complete_google_limit` (Google stopped returning reviews short of its count), and 23 stores are
`no_reviews_on_google`. Accepted as they are, by owner ruling: 403 within-store near-duplicate
reviews (50-character key), 966 reviews with no rating and 965 with no date.
