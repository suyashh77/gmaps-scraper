# Data Quality Report

**`Step_5_master_reviews.db` · 4,889 stores · 1,201,237 reviews · 2026-04-30**

Known issues with the dataset. Status and remaining work elsewhere: `Status.md`, `Future_Work.md`.

---

## Critical issues

### 157 over-scraped stores (wrong-location bug)
`reviews_scraped > target_reviews + 5`. Top 10:

| Overshoot | store_id | Category | Name | Scraped/Target |
|---:|---:|---|---|---:|
| +6,541 | 3117 | Grocery store | Food Lion | 7,440 / 899 |
| +1,148 | 2088 | Dollar store | Dollar General | 1,383 / 235 |
| +1,008 | 2682 | Dollar store | Family Dollar | 1,037 / 29 |
| +995 | 1734 | Convenience store | The Pop Shoppe | 1,007 / 12 |
| +995 | 4941 | Grocery store | Food Lion | 999 / 4 |
| +974 | 2406 | Dollar store | Dollar General | 1,051 / 77 |
| +899 | 4528 | Dollar store | Dollar General | 1,188 / 289 |
| +897 | 4928 | Supermarket | Walmart | 900 / 3 |
| +845 | 4859 | Supermarket | Walmart | 849 / 4 |
| +789 | 2687 | Dollar store | Family Dollar | 953 / 164 |

Pattern: many recent rows (`store_id` 4800s/4900s) have `target=4` and reviews in the hundreds — strong signal the place_id resolves to a different busy store. Needs spot-check + scraper guardrail (Future_Work P0.2).

### 11 disputed exclusion-list cases
**0 closed-store leaks.** ✓ Of 7,421 unique excluded `place_id`s, 371 also appear in master:
- 364 with reason `MANUAL_REVIEW_UNMATCHED` — expected (per-pair exclusions, dual pathway).
- **7 with `CATEGORY_NON_GROCERY`** — disputed (Gas station, Produce market, etc. on excluded side; grocery category in master).
- **4 with NULL master category** — also in exclusion list.

Full list in Future_Work P0.4.

### 67 non-standard `place_id`s
Not in `ChIJ…` format. Cannot be re-scraped. Need fresh Outscraper/Places API queries (Future_Work P0.3).

### 55 stores with NULL `category`
Cannot be filtered by category. 1.1% of stores. Backfill or exclude (Future_Work P0.5).

### Major-chain coverage gaps
| Chain | Master | Notes |
|---|---:|---|
| Costco | **0** | NC has 9 locations |
| Whole Foods | 4 | NC has ~8+ |
| Earth Fare | 3 | NC HQ; 8+ NC stores |
| Wegmans | 1 | NC has 2+ |
| Sam's Club | 8 | NC has 14+ |
| Harris Teeter | 33 | Likely 80–100 in NC |
| Ingles Markets | 33 | Likely 70+ in NC |

Likely Costco was dropped at the category-filter step. Future_Work P1.1.

---

## Field-level issues

### Stores
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

### Reviews
| Field | Null/zero | Note |
|---|---:|---|
| `service_type` | **100%** NULL | column is dead |
| `helpful_count` | **100%** zero | scraper doesn't extract |
| `photo_count` | **100%** zero | scraper doesn't extract |
| `review_text` | 593,302 (49.4%) | rating-only — typical for GMaps |
| `rating` | 569 | 0.05% |
| `date_relative` | 568 | 0.05% |
| `is_local_guide=1` | 872,427 | 72.6% — high baseline |
| `has_owner_response=1` | 90,301 | 7.5% |

### FK integrity
0 orphan reviews. ✓

---

## Review-level issues

### 734 within-store duplicate reviews
Same `(store_id, reviewer_name, rating, review_text[:50])` survived merge dedup. Likely 50-char prefix matches but full text differs. One-time cleanup pass needed (Future_Work P1.3).

### 2,571 cross-store same-text groups
Same reviewer + same text (≥30 chars) at 2+ stores. Mix of legitimate (chain reviewer) and wrong-location merge artifacts. Needs targeted investigation for the high-fanout cases (Future_Work P1.4).

### Reviewer-name is non-unique
"Chris" reviews 240 distinct stores; "J", "John", "Mike" each 200+. These are common first names, not stable IDs. Treat `reviewer_name` as a display label, not a key.

### Date coverage skews old
1.07M reviews say "year(s) ago"; only 12,551 (~1%) are within the last month. `date_relative` is Google's natural-language string — no absolute dates parsed yet (Future_Work P2.1).

### Rating distribution (avg 4.07)
1★: 8.9% · 2★: 4.6% · 3★: 10.9% · 4★: 21.6% · 5★: 53.9%.

### Text length percentiles (n=607,935)
p25=29 · p50=63 · p75=145 · p90=307 · p99=957 · max=4,096 chars.

---

## Cleanups already applied

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

## Recommendations for analysts

1. Use `store_name` and `full_address` (canonical, consolidated).
2. Filter by `category`; drop the 55 NULL-category rows for category-stratified work.
3. Use `reviews_scraped >= target_reviews` for completion, not the `scrape_status` column.
4. Treat `reviewer_name` as non-unique.
5. Ignore `helpful_count`, `photo_count`, `service_type` — unreliable.
6. Spot-check stores in the over-scraped list before featuring them in top-N analyses.
7. Text corpus is ~608K reviews, not 1.20M (49.4% are rating-only).
