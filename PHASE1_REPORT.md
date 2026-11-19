# Phase 1 Project Report: NC Grocery Stores and Their Google Reviews

**Project:** Aspatial food access in North Carolina, Phase 1 (data collection)
**Team:** Suyash Sapre, Teresa Groton, Dr. Benjamin Rachunok (Operations Research, ISE, NC State)
**Status:** Complete. Data collection closed 2026-10-24. Report written 2026-11-19.

This report replaces the earlier scraper hand-off documents (`HANDOFF.md`, `STATUS.md`, the scraping docs in `Handover/`). Phase 2, the scoring pipeline, gets its own hand-off.

---

## 1. The question

Food deserts are usually measured by distance: how far a household lives from a grocery store. Distance misses most of what makes a store usable: price, what is on the shelves, opening hours, and how shoppers are treated. Teresa Groton's MS thesis (2026) measured these *aspatial* parts of access from Google review text. She used Penchansky & Thomas's five dimensions of access (availability, accessibility, accommodation, affordability, acceptability) and piloted the method on 49 stores in Raleigh (25,125 text reviews).

## 2. The goal of Phase 1

Build the statewide dataset the method needs: **every in-scope grocery store in North Carolina, and every Google review it has.**

## 3. What we delivered

| | |
|---|---|
| Stores | **4,442** in-scope grocery stores, in all **100 of 100** NC counties and 590 cities |
| Reviews | **1,941,538** Google reviews, **977,398** with text |
| Completeness | 4,400 stores complete, 42 short (955 reviews in total). 99.6% of reviews collected against targets capped at 1,500 |
| Scale vs. pilot | 91× the stores and 39× the text reviews of the Raleigh pilot |
| Final file | `03_Data/1_reviews/Step_5_master_reviews_FROZEN_2026-10-24.db` (SQLite, read-only, SHA-256 in `master_freeze_manifest_v2.json`) |
| Store table | `03_Data/4_final_store_list/NC_Grocery_POIs_v2.csv` / `.xlsx` (one row per store, 4,442 rows, rebuilt from the final freeze) |

## 4. How it was built, in six steps

1. **Store universe (the "frame").** We combined four public sources: USDA SNAP retailers, Overture Maps (two releases), OpenStreetMap and the project's earlier store list, plus an audit of previously excluded stores. Together that was 91,461 records. Records describing the same store were matched by distance and name similarity, with SNAP trusted for the address, Overture for the name and Google for the identity. That produced a frame of **36,820** stores. We checked it against Data Axle, Dewey (SafeGraph) Places and Census business counts. Every one of the 8,883 SNAP-authorised retailers is in the frame, in all 19 NC ZIP3 regions.
2. **Google probe.** For each candidate (16,751 established bricks-and-mortar food stores), a script searched Google Maps by name and address. It recorded the store's place ID, its category, its live review count and whether it was open.
3. **Labeling and scope.** We hand-ruled about 300 Google categories as in or out of scope (`category_ruleset.csv`), then ran seven rounds of manual checks.
   - In: chains with more than five locations, plus grocery stores, supermarkets, dollar stores, butchers, seafood markets, ethnic and health-food grocers.
   - Out: gas stations, convenience stores, closed stores, farmers' markets, produce stands, and restaurants that sell some groceries.
   - Out-of-scope, closed and duplicate stores were moved to side databases, never deleted.
4. **Review scraping.** A Python + Playwright scraper drove a headless Chrome browser on NC State Linux lab machines, one scraper per machine. It ran in two campaigns:
   - Spring 2026: ISE 408 student batches, scraper v1.
   - Sep–Oct 2026: lab VMs, scraper v2.

   Scraper v2 opens each store by its Google place ID and rejects any page that is not that store.
5. **Merge and repair.** Each machine's results merged into one master database, with duplicates removed on Google's review ID. Repair scripts (`Remediation/r00`–`r18`) set targets from Google's live counts, restored wrongly set-aside reviews, applied the completion rule and validated the result.
6. **Freeze.** A read-only copy of the master plus a checksum manifest. Freeze v2 (2026-10-24) is final.

## 5. Rules that still bind

- **Completion rule.** A store's target counts as at most 1,500 reviews. A store is complete when it is missing no more than 10 reviews (target 100 or under), 20 (over 100), 50 (over 500) or 100 (over 1,500). Stored targets remain Google's true counts.
- **Never delete reviews.** Set-aside reviews live in `quarantined_reviews` (29,321 rows) with a reason. Out-of-scope, closed and excluded stores live in their own side databases.
- **The frozen file is read-only.** Any change means a new freeze and a new manifest.
- **`status_reason` is a decision.** 1,587 stores carry one; it is the audit trail for every ruling.

## 6. Known limits of the data, worst first

1. **Big stores are truncated.** 257 stores have more than 1,500 reviews on Google. For those we hold the *newest* reviews, not their full history, and not a random sample. Flag them in any per-store analysis. 109 stores have a target of exactly 1,500 (an old capped number), so their true size is unknown.
2. **Rural stores are the most likely to be missing.** Small rural stores are the ones commercial maps miss. The four-source frame was built to fix this, but it is the main remaining coverage risk.
3. **Review dates are relative** ("2 years ago"). Exact dates have to be estimated from `scraped_at`.
4. **Reviewer name is not an ID** ("Chris" reviews 240 stores).
5. **Some fields are unavailable, not zero.** Google no longer shows `helpful_count`, `photo_count` or `service_type`.
6. **42 stores are short:** 40 are 21–25 reviews short after repeated passes, and 2 (Dollar General 2444, Save A Lot 3860) need a manual check.
7. **2 stores have coordinates outside NC** (Google's own pin), so they are left off the maps.
8. **Coverage vs. Google's true counts is 88%.** The 1,941,538 reviews are 99.6% of the capped targets but 87.9% of everything Google lists, because of point 1.

## 7. What we learned

- **The frame decides everything downstream.** A store that never enters the list is invisible to every later measurement. The first, Overture-only list kept 14–38% of candidates depending on region, worst in rural eastern NC. It would have reported food deserts that were pipeline artifacts.
- **Verify by identity, not by name.** The first scraper searched by name and accepted whatever came back. Sam's Club "Hendersonville" resolved to Raleigh, with 1,062 reviews. v2 navigates by place ID and rejects mismatches.
- **A silent failure looks like success.** A scroll that did nothing looked exactly like the end of a store's review list, so stores were marked complete after one screen. The scraper now checks that every scroll moved.
- **"Google won't give more" is a symptom, not a limit.** 190 stores were about to be labeled capped. One more pass on a fresh session returned 24,481 more reviews. Retry before you label.
- **Resumability is the whole game.** All scraper state lives in SQLite, so an interrupted machine picks up where it stopped.
- **Hand-copied code drifts.** Volunteer copies diverged from the repo within days. Kits are now built from the repo with a hashed manifest.

## 8. What was handed to Phase 2

- The frozen review database, the side databases and the final store list v2 (all in `03_Data/`).
- The analysis export (one row per review in the pilot's shape) is built by the Phase 2 pipeline repo, `github.com/suyashh77/nc-aspatial-food-access`.
- Open small items: the two Round 7 manual stores, a check of the final store list against Census counts (`6_validation_reports/census_validation.csv`), and Wegmans Morrisville (8259), whose stale 1,500 target trips the validator.

## 9. Where things are

| Folder | What |
|---|---|
| `01_Report/` | this report (.md and .docx) |
| `02_Code/` | the scraper code. Canonical copy on GitHub: `github.com/suyashh77/gmaps-scraper` |
| `03_Data/1_reviews/` | frozen review database (4,442 stores, 1,941,538 reviews) and its checksum manifest |
| `03_Data/2_side_databases/` | out-of-scope (4,736 stores), closed (67) and excluded (221) stores with their reviews |
| `03_Data/3_store_universe/` | the 36,820-store frame (`Step_1_NC_Food_Retail_Frame.db`, `frame.csv`) and the Google probe results (`probe.db`, 16,751 stores) |
| `03_Data/4_final_store_list/` | one row per final store (`NC_Grocery_POIs_v2`) |
| `03_Data/5_labeling_and_rulings/` | category rulings, the POI decision sheet, and manual-check rounds 4–7 |
| `03_Data/6_validation_reports/` | coverage, corroboration, Census comparison, dropped stores and frame gates |
| `04_Poster/` | 48×36 in academic poster (.pptx and print-ready .pdf) |
| `05_Walkthrough/` | slide deck and script for walking the team through this folder |

### The main tables in the review database

| Table | One row per | Key columns |
|---|---|---|
| `stores` | store | `store_id`, `place_id` (Google), `store_name`, `city`, `zip`, `source_lat/lon`, `category`, `target_reviews` (Google's count), `reviews_scraped`, `scrape_status`, `status_reason` |
| `reviews` | review | `store_id`, `review_id` (Google), `rating` (1–5), `review_text`, `date_relative`, `scraped_at`, `has_owner_response`, `is_local_guide`, `machine_id` |
| `quarantined_reviews` | set-aside review | same as `reviews`, plus `quarantine_reason` |
| `accepted_flags` | ruled exception | `store_id`, `check_name`, `reason` |

## 10. Reproducing it

The code needs Python 3.12. `pip install -r requirements.txt`, then `python -m pytest -q` (329 tests). `python Remediation/r00_validate.py --fail-on HIGH` validates a master database. It exits HIGH on freeze v2 only for the 42 documented short stores and the Wegmans false alarm. Google login files (`Auth/`) are not included and must never be shared. Scraping is closed; re-opening it starts with `Archive/manabe_box_code_20261024/` (one lab machine ran a hand-patched copy of the scraper).

## Acknowledgements

Thanks to the ISE 408 students, whose batch runs contributed reviews in the spring campaign, and to the volunteers who ran the Forrester, Manabe and Holling machines.

## References

- Groton, T. E. (2026). *Expanding the Dimensions of Food Access: Linking Experience and Geography through Data-Driven Analysis.* MS thesis, NC State University.
- Penchansky, R., & Thomas, J. W. (1981). The concept of access. *Medical Care*, 19(2), 127–140.
- Caspi, C. E., Sorensen, G., Subramanian, S. V., & Kawachi, I. (2012). The local food environment and diet: a systematic review. *Health & Place*, 18(5), 1172–1187.
