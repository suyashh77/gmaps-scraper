# Future Work — Usable → Authoritative

Items needed to take `Step_5_master_reviews.db` from "usable for analysis" to "authoritative dataset". Ordered by impact.

---

## P0 — Required for "authoritative" claim

### P0.1  Finish scraping the remaining 215,457 reviews
- 1,409 stores incomplete. Run `run_continuous.py` on both machines.
- **Done when:** `< 50` stores still have `reviews_scraped < target_reviews`.
- **Effort:** ~83 hours per machine (parallel).

### P0.2  Reconcile the 157 over-scraped stores
- `reviews_scraped > target_reviews + 5`. Worst case: store_id 3117 Food Lion (+6,541). Many recent rows have `target=4` and reviews in the hundreds → wrong place_id.
- **Action:** Verify each via `https://www.google.com/maps/place/?q=place_id:<id>`. Re-attach reviews to correct store, or delete and re-queue. Add scraper guardrail: refuse stores whose first GMaps result address differs from `master.full_address` by >1 km.
- **Done when:** Top-overshoot list shrinks to <10 stores, all explainable by Google adding reviews after target was set.
- **Effort:** ~6 hours review + ~2 hours code.

### P0.3  Resolve the 67 non-standard `place_id`s
- Not in `ChIJ…` format → cannot be re-scraped.
- **Action:** Re-query via Outscraper/Places API using `name + address`. Replace with valid `ChIJ…` or move to exclusion list with reason `INVALID_PLACE_ID`.
- **Done when:** `SELECT COUNT(*) FROM stores WHERE place_id NOT LIKE 'ChIJ%'` = 0.
- **Effort:** ~2 hours.

### P0.4  Adjudicate the 11 disputed exclusion-list cases
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

### P0.5  Backfill the 55 NULL-category stores
- **Action:** Chain-name pass first; manual GMaps check for residuals. Move non-grocery to exclusion list.
- **Done when:** No NULL `category` rows in master.
- **Effort:** ~1 hour.

---

## P1 — High-value quality gains

### P1.1  Audit major-chain coverage (Costco missing entirely)
- **Gap:** Costco 0 (NC has 9). Whole Foods 4 (vs 8+). Earth Fare 3. Wegmans 1. Sam's Club 8 (vs 14+). Harris Teeter 33 (vs ~80–100). Ingles 33 (vs 70+).
- **Action:** Pull canonical NC store list per chain from chain locators. Cross-check against master. Add missing rows via additions worklist + scrape.
- **Done when:** All known NC locations of those chains are in master.
- **Effort:** ~3–4 hours per chain.

### P1.2  Geographic completeness check vs Step 1.3
- **Action:** Compute set difference (cities/ZIPs in Step_1.3 minus in master). For each missing, check if it has a Dollar General (ubiquitous tracer).
- **Done when:** Every NC ZIP from Step 1.3 has ≥1 master row, or a documented absence.
- **Effort:** ~3–4 hours.

### P1.3  Within-store duplicate-review cleanup
- **Gap:** 734 within-store duplicates under `(store_id, reviewer, rating, text[:50])`.
- **Action:** Keep longest `review_text` (or latest `scraped_at`); delete the rest. Single SQL pass.
- **Done when:** Zero rows match the dedup-key duplicate query.
- **Effort:** ~30 minutes.

### P1.4  Cross-store same-text reviews investigation
- **Gap:** 2,571 (reviewer, text) pairs at 2+ stores. Mix of legitimate chain reviewers and wrong-location merge artifacts.
- **Action:** Check parent-store geographic spread per group. Groups with parents >50 km apart likely have a bad join.
- **Done when:** Each high-fanout (>5 stores) group is resolved.
- **Effort:** ~2 hours.

### P1.5  Backfill the 157 untargeted stores
- **Action:** Fresh GMaps lookup per place_id, set `target_reviews` from the live count (cap at 1,500), re-queue.
- **Done when:** ≤20 stores remain with NULL/0 target (legitimate "no reviews" stores).
- **Effort:** ~2 hours scripted + ~2 hours scrape.

### P1.6  Re-scrape `helpful_count`, `photo_count`
- **Gap:** Both 100% zero. Scraper doesn't extract them.
- **Action:** Add field extraction to `linux_scraper/scraper.py`. Run an enrichment pass that updates only these two fields on existing reviews.
- **Done when:** ≥80% of high-engagement reviews (rating≤2 or text>200 chars) have non-zero values.
- **Effort:** ~6 hours code + ~50 hours scrape.

### P1.7  Drop or repurpose `service_type` and `gmaps_address`
- `service_type` 100% NULL. `gmaps_address` 81.9% NULL.
- **Action:** Drop or backfill from `outscraper_address` / `full_address`.
- **Effort:** ~30 minutes (drop) or ~3 hours (backfill).

---

## P2 — Polish

### P2.1  Parse `date_relative` into absolute dates
- Compute `date_estimated_min` / `date_estimated_max` window from `(date_relative, scraped_at)`. Needed for any longitudinal analysis.
- **Effort:** ~3 hours.

### P2.2  Refresh stale `scrape_status` column
- Drop or rebuild via comparison logic. Document that the comparison is canonical.
- **Effort:** ~30 minutes.

### P2.3  Build `review_sources` provenance table
- `(source_db, machine_id, run_started_at, run_ended_at, n_reviews_added, notes)` keyed by `source_db`. 49 distinct values currently.
- **Effort:** ~3 hours.

### P2.4  Address-quality pass
- 17 missing `street`, 4 missing `zip`. GMaps lookup per place_id.
- **Effort:** ~30 minutes.

### P2.5  Normalize anonymous-reviewer rows
- 70 NULL/blank `reviewer_name`. `UPDATE … SET reviewer_name='Anonymous'`.
- **Effort:** 1 minute.

### P2.6  Language detection on review text
- Run `langid` or `fastText lid.176` over text-bearing reviews. Store in `review_text_lang`. Spanish reviews likely a non-trivial slice (Mexican grocery stores).
- **Effort:** ~2 hours.

### P2.7  Sentiment baseline
- VADER or small HuggingFace model → `review_sentiment` REAL. Optional topic clustering.
- **Effort:** ~6 hours.

---

## P3 — Repository hygiene

### P3.1  Decide canonical Excel exports vs SQLite
- `Step_2.0_Final_List_of_Stores.xlsx` (4,715) ≠ master `stores` (4,889). Either regenerate on every freeze or formally deprecate the Excel.

### P3.2  Add `make_data.py` end-to-end runner
- Single command-runner that lists every Step. Lets an external user reproduce the dataset. Bare minimum bar for "authoritative".

---

## Definition of Done — "Authoritative"

1. All P0 items completed.
2. ≥99% of stores have `reviews_scraped` ≥ `target_reviews − 1`, or are documented exceptions.
3. Major NC chains all present (P1.1).
4. Every NC ZIP from Step 1.3 present, or absence documented (P1.2).
5. Within-store and cross-store duplicate investigations complete (P1.3, P1.4).
6. Reproducible end-to-end pipeline (`make_data.py` or equivalent).
7. Frozen master snapshot with version tag and SHA256, with `Data Quality Report.md` regenerated against it.

P2 improves usability but is not a gate.
