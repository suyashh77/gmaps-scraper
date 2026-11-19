# STATUS — NC grocery reviews pipeline

**Snapshot: 2026-10-24, handoff. Scraping is finished (owner, 2026-10-24). Every batch that came back is merged, and the final dataset is freeze v2.** This is the one
document to read first. It says what the project is, where everything is, what state the data is
in, what was decided and why, and what is left. Component READMEs
([Frame](Frame/README.md), [Remediation](Remediation/README.md), [Deploy](Deploy/README.md),
[Data](Data/README.md)) hold the detail; this file holds the picture. What the cleanup deleted is in
[Archive/DELETED_2026-10-24.md](Archive/DELETED_2026-10-24.md).

> **Freeze v2 (final):** `Data/Step_5_master_reviews_FROZEN_2026-10-24.db`
> SHA-256 `7a8472b7d91fc92e0b15b5c4576f7e9ed75310cdbb45610c7023e0e813d28785`
> (manifest: `Remediation/master_freeze_manifest_v2.json`). Read-only. It holds 4,442 stores and
> 1,941,538 reviews. dewey27, dewey28, dewey30a and dewey30b are merged and targets are set to Google's
> true counts. **Completion rule (owner, 2026-10-24): target capped at 1,500; complete if missing ≤ 10
> (target ≤ 100), ≤ 20 (> 100), ≤ 50 (> 500), ≤ 100 (> 1,500).** Result: 4,400 complete, 42 incomplete (§4.1).
> Freeze v1 (`..._FROZEN_2026-10-11.db`, `master_freeze_manifest.json`) is kept for provenance.

---

## 1. What this is

Research infrastructure for **Teresa Groton's dissertation on aspatial food access**
(NC State ISE, advised by Prof. Benjamin Rachunok). Food deserts are usually measured by
distance to a store. This work measures them through what people *write* about grocery
stores on Google Maps, coded against the Penchansky & Thomas five dimensions of access
(availability, accessibility, accommodation, affordability, acceptability). A Raleigh pilot
proved the method (`Data/reference/Teresa_Sample_Output.csv` is its output: one row per
review with a topic, a predicted dimension and five dimension probabilities). This repo
scales the data collection to all of North Carolina.

**The deliverables:**
1. `Data/Step_5_master_reviews_FROZEN_2026-10-24.db`: `stores` (4,442 NC food retailers, each
   with a Google `place_id`, a category and a live review count) and `reviews` (1,941,538 Google reviews).
2. `Data/exports/NC_Grocery_POIs.xlsx` — the store table (one row per store in the frozen master,
   with Dewey placekey/brand/chain where matched). This is the POI list the study goes off.

---

## 2. Where everything is

```
Gmaps Scraper/
  README.md            front door
  STATUS.md            this file — current state
  CLAUDE.md            working conventions for AI sessions
  requirements.txt     one dependency manifest (geomatch is a sibling checkout, see inside)
  pyproject.toml       pytest config: `python -m pytest -q` runs every suite (329 tests)

  Frame/               STEP 1 — the store universe.  s01..s14 build it from four sources,
                       probe.py asks Google about each store, probe_merge.py folds in
                       probe results from other machines.  build/probe.db = probe queue.
  Scraper_v2/          STEP 2 — the review scraper.  linux_scraper/ package (Playwright),
                       run_continuous.py orchestrator, tests/.  Canonical, only copy.
  Remediation/         r00..r17 — audit, repair and ruling scripts for master; r00_validate.py is
                       the definition of "authoritative".  make_manual_round4/5.py build the
                       manual-check workbooks; master_freeze_manifest.json is the freeze record.
  Deploy/              build_kit.py assembles volunteer kits from canonical code into
                       Deploy/build/ (gitignored; deleted at handoff, rebuild on demand).
  Data/                gitignored except *.md.  See §2.1.
  Auth/                google_auth_{1,2,3}.json — Google session cookies.  Only copy.
                       Never commit, never share.  Currently PC-bound (see §2.2).
  Archive/             superseded code kept for provenance; nothing imports it.
                       manabe_box_code_20261024/ = the hand-patched scraper the manabe box ran (§2.2).
                       DELETED_2026-10-24.md = what the handoff cleanup deleted.

../Gmaps Scraper - external data/   Dr.ben_data/ (9.3 GB SafeGraph Patterns) and dewy data/
                       (Dewey Places extract + catalog), moved out of the repo 2026-10-24.
                       Read by Frame/s15, s17 (and through them s18, s19); override the location
                       with the GMAPS_EXTERNAL_DATA environment variable.
```

### 2.1 Data files (all under `Data/`, none in git)

| file | what | state |
|---|---|---|
| `Step_5_master_reviews_FROZEN_2026-10-24.db` (~640 MB, read-only) | **the dataset (freeze v2, final)**: `stores` + `reviews` + `quarantined_reviews` + `accepted_flags`, a `VACUUM INTO` copy of master | 4,442 stores · 1,941,538 reviews · SHA in the banner |
| `Step_5_master_reviews_FROZEN_2026-10-11.db` | freeze v1, kept for provenance | 4,447 stores · 1,811,845 reviews |
| `Step_5_master_reviews.db` | the working master; identical to freeze v2 at handoff. Writers (`r15`, `r16`, `r17`, `merge_results`) act on this file | do not change it without re-freezing |
| `Step_5_out_of_scope.db` | gas stations, convenience stores and other out-of-scope stores, with reviews | 4,736 stores · 105,892 reviews |
| `Step_5_closed_stores.db` | stores verified closed on Google, with reviews (owner ruling: closed stores' reviews are not part of the study) | 67 stores · 7,836 reviews |
| `Step_5_excluded_stores.db` | duplicates folded into another store, and stores whose Google page could not be fixed (owner ruling 2026-10-09: excluded stores live apart from master) | 221 stores · 163 reviews (+ their quarantined rows) |
| `Step_6_grocery_pois.db` | the Dewey (SafeGraph) grocery/general-merchandise POI list, built 2026-09-24, matched to master by `master_store_id`. Owner ruling 2026-10-11: accepted as is, not refreshed | 3,393 POIs · snapshot |
| `Step_1_NC_Food_Retail_Frame.db` | the 36,820-store frame, frozen 2026-09-08, SHA in `Frame/reports/frame_manifest.json` | the universe; the store list is final (owner, 2026-10-12) |
| `batches/` | the last scrape queues: `dewey28.db` (497 stores, uncapped), `dewey30a.db` / `dewey30b.db` (the 131 stores dewey28 never reached, split in two). Every older queue is in `batches/archive/` | all run and merged |
| `backups/` | pruned at handoff to three files: `..._pre_dewey28_20261020.db`, `..._backup_r15_20261024_093607.db` (the state just before freeze v2) and the Step_6 backup | milestones only |
| `returns/` | what volunteers sent back, one folder per return. The last four: `20261019_forrester_dewey28`, `20261024_forrester_dewey30a`, `20261024_manabe_dewey30b`, and `20261024_manabe_full_pull` (the final pull of the manabe box: one copy of each distinct result DB, logs, reports, `_inventory.csv`) | all merged |
| `exports/` | **`NC_Grocery_POIs.xlsx`** (the POI table); `Manual_Check_Round4_2026-10-09.xlsx`, `Manual_Check_Round5_2026-10-11.xlsx` (the sheets sent out); older renders and the 45k-row `POI_Decision_Sheet_2026-09-25.xlsx` | derived |
| `reference/` | Teresa's Raleigh pilot: notebook + output CSV (the target analysis schema) | reference |
| `Step_1.*`, `Step_2.0`, `Step_3.*` | the January–April pipeline artifacts | historical inputs |

`Frame/build/probe.db` (the probe queue and its results, 16,751 stores) lives with Frame, not Data.

### 2.2 Machines

| machine id | host | role | what it did |
|---|---|---|---|
| `local1` / `9svarg` | this PC | dev, probe, merges | 3,521 probes; smoke runs |
| `HOLLING` (`2a479d`) | Windows box (classmate) | probe kit | 13,225 probes, 2026-09-10 → 09-15; queue drained |
| `sl9mw2` (`9b6a55`) | `forrester.ie.ncsu.edu` (Linux) | review kit | run2, grocery1, dewey6/7/8, dewey21, dewey25, dewey27, **dewey28, dewey30a** |
| `alodov` (`ce281e`) | `manabe.ie.ncsu.edu` (Linux) | review kit | run1, grocery2, dewey9, dewey22, dewey26, **dewey30b** |

**Code fork on manabe (found at handoff).** The manabe box ran a kit built 2026-09-25 (commit
`ebf8d07`) and hand-patched on the box three times: 09-27, 10-08 ("longscrape") and 10-21 ("nocap").
Its `linux_scraper/` differs from `Scraper_v2/` by about 530 added and 200 removed lines (mostly
`scraper.py`), and it has no `review_cap.py`. **`Scraper_v2/` stays canonical.** The box code, its
intermediate backups and `DIFF_vs_Scraper_v2.patch` are in `Archive/manabe_box_code_20261024/`. If
collection resumes, read that diff and port anything worth keeping (with tests) before building a kit.
The data it produced is sound: it went through the same merge, dedup key and validator as everything else.

Run a batch on a box with `./_run.sh <machine-number> <batch>.db` (e.g. `./_run.sh 1 dewey25.db`).
`run_machineN.sh` only finds a batch whose filename ends in N, so batches named otherwise need the
explicit form. One scraper per IP.

**dewey27 took about a day**, not the two weeks estimated: the box returned 55,176 reviews for 629 stores in roughly 22 hours.
**Uncapped scraping is slow.** dewey30a ran at about 350 reviews/hour and dewey30b at about 150. A
Walmart with 7,000 reviews takes longer than the per-store time budget (6–17 h), so it ends `failed`
with a partial haul (2,000–4,000 new reviews each, all kept).

**Auth:** the three files in `Auth/` are bound to this PC by Chrome's Device Bound Session
Credentials (`make_auth.py --check` reports `PC-BOUND`); they sign out on a box within ~10 minutes.
The boxes ran the last rounds on sessions of their own, and no further collection is planned.
If collection ever resumes, re-make the logins with `Scraper_v2/make_auth.py` and check them `live`.

### 2.3 External dependencies

- **geomatch** — the project's own address/POI matcher, a sibling checkout at
  `C:\Users\19193\Downloads Local\Projects\matcher` (`pip install -e` it). Used by
  `Frame/s09`, `s10`, `s13`. Not on PyPI.
- Google Maps via Playwright/Chromium, no paid API.
- Sources pulled by `Frame/s01–s04`: USDA FNS SNAP retailer list, Overture Maps
  2026-08-19.0, OpenStreetMap (Overpass), Census CBP 2023 (`Frame/sources/cbp23co.csv`).

---

## 3. How we got here (decisions that still bind)

| when | what | why it matters now |
|---|---|---|
| Jan–Apr 2026 | Overture → NC filter → Google enrichment → Outscraper → 13-category filter → 4,889 stores → review scraping (VMs + a Tkinter GUI run by ISE 408 students) → 1.2 M reviews | Produced the master. Also produced most of the defects below. |
| 2026-09-05 | **Audit** (`Remediation/README.md`). Wrong-location stores, 157 stores complete with 0 reviews, merge not idempotent, empty fields. r01–r04, r08 applied. | The April dataset would have reported food deserts in eastern NC that were pipeline artifacts. |
| 2026-09-05/09 | **Scraper repaired**: navigate and verify by `place_id`; fail-closed verification; scroll container verified to move; `review_id`/`machine_id` carried into master; merge takes a lock, backs up, verifies row count. | Re-running the old code would have reproduced every defect. |
| 2026-09-08 | **Step 1 rebuilt** as a four-source frame (`Frame/`): 36,820 stores. | Replaced the Overture-only candidate list. |
| 2026-09-09 | **Scope decision**: *established bricks-and-mortar food stores*, not stalls, farmers' markets, produce stands. `s10_select.py` cut the frame to the 16,751-store probe list. | Defines what "in scope" means. |
| 2026-09-25 → 10-07 | Dewey Places overlap work; rulings r12/r13: out-of-scope stores moved out; stalled stores ruled complete; far-from-address stores accepted; wrong-location reviews quarantined (85,899); duplicate stores folded. r10 recompute bug fixed: **`status_reason` marks a decision and is never overwritten from counts.** | Why 1,483 stores are "complete by ruling". |
| 2026-10-08 | **Manual rounds 2 and 3** (`r14`): closed stores removed; stores whose saved page was another store's re-pointed. | Closed stores' reviews leave the dataset. |
| 2026-10-09 | dewey21/22 merged. The 216 excluded stores (204 duplicates, 12 bad place_id) moved to `Step_5_excluded_stores.db`. **Manual round 4** (355 stores, `r16`): 12 closed, 27 out of scope, 4 folded into the store they duplicate, 7 re-pointed, 283 targets reset to Google's live count. | Contaminated pairs gone; validator BLOCKER cleared. |
| 2026-10-09 | **Tolerance rule (owner):** reviews get deleted on Google, so a store within **20 reviews of target when target > 1,000, within 10 when target > 100, under 10 below that** is complete (`r13.stalled_is_complete`, applied by `r15`). Targets above the 1,500 cap count as 1,500. | One rule, one place. |
| 2026-10-11 | **dewey25/26 retry** (278 stores, one pass on a fresh session): +24,481 reviews. The "Google ceiling" turned out not to be a hard limit for most stores. | Stores short after repeated passes are the only true Google-limited ones. |
| 2026-10-11 | **`r17` restored 4,529 reviews to 25 stores.** The old wrong-page cleanup had set aside reviews that a fresh scrape of the store's own page returned again (the merge treats quarantined rows as already known, so they were being dropped). Only rows confirmed by a scrape of the store's own page and held by no other store were restored. | Why the final shortfall is small. Quarantine now holds 29,323 rows that nothing contradicts. |
| 2026-10-11 | **Manual round 5** (28 no-target stores + Food Lion Monroe): 23 "no data" (open, 0 reviews), 2 closed, 2 out of scope; store 4965 found to duplicate 4011 and its 200 Target-page reviews set aside. | Validator HIGH items cleared. |
| 2026-10-11 | **Owner closed the open questions:** store list complete; Dewey grocery list accepted; 1,500-cap stores stay; the 403 within-store duplicate reviews stay; reviews with no rating/date stay; the 327 rating-only same-name pairs and 6 See-more reviews are fine. The 23 "no data" stores were verified by hand in Round 5. **Freeze v1 taken.** | Closes the questions. |
| 2026-10-11 | **Completion rule set (owner):** a store is complete when it reaches its target, or when its target is over 500 and it is missing 20 or fewer reviews, or its target is over 100 and it is missing 10 or fewer, or its target is 100 or under and it is missing 5 or fewer; anything else short of a verified target is rescraped, worst first by share of its own target missing. Anything else short of a verified target goes back in the queue (`r15.reopen_short`; old reasons kept in `reopened_rulings`). This replaced the earlier tolerance of 20 and 10. Result: 3,885 complete, **562 incomplete** (5,938 reviews short, 4.5 % of their targets). `dewey27` rebuilt to match; the scraper learned `claim_order=pct`. | The stalled stores are queued again even though earlier passes found nothing; expect low yield, and the worst-first order puts the useful stores up front. |
| 2026-10-11 | **Over-target stores investigated:** 122 stores hold more than Google shows (3,021 extra); 67 hold more than 5 over (3,293 extra). Most built up reviews over 5 to 8 scrapes across months, so some were deleted on Google since; 65 accumulated stores are accepted in `accepted_flags` (`over_target_accumulated`), which only mutes the validator's umbrella count. **Owner: delete nothing; rescrape and compare.** All 67 are in `dewey27` collected in full, and `r18_compare_rescrape.py` compares each fresh page with what we hold (CONSISTENT = keep all; anything else goes to manual). Food Lion Greensboro (1,102 vs 419) and Monroe (1,022 vs 576) hold two disjoint batches each and are the likeliest to need manual. | Nothing is moved or removed by a script; only a person decides. |
| 2026-10-11 | **Quarantine reviewed:** 29,323 set-aside reviews in 39 stores: 10,515 are held by another store (correctly out), 15,671 are rating-only with no text, 3,137 text reviews are held nowhere (27 belong to stores outside master, 3,110 were returned by no scrape). Nothing more can be restored on evidence today; `r17` after dewey27 restores whatever a fresh own-page scrape confirms. | No blind restore. |
| 2026-10-12 | **dewey27 merged** (Forrester, 622 of 629 stores completed): +6,127 new reviews, 1,817,972 in total; `r17` restored 0. **All 67 over-target stores came back CONSISTENT**: the page's reviews are already held, so the ~7,900 extra reviews those stores hold are ones the page no longer shows; kept, nothing removed (the accumulated stores, including Walmart Grocery Pickup Mount Olive 4868, are accepted in `accepted_flags`). The scraper's live count on arrival re-verified the targets: `r10` set 315 targets to Google's count of that day (raised for stores that gained reviews, lowered for 23 stale ones). 7 stores were skipped because the scraper saw them as closed; they go to manual (Round 6). 545 stores remain short, 4,735 reviews (3.78 % of their targets), median 4 missing. | Most of what is left was rescraped in full against a live-verified target and is still short: Google is not returning those last reviews. |
| 2026-10-12 | **Final completion tiers (owner):** over 500 missing 20 or fewer, over 100 missing 10 or fewer, 100 or under missing 5 or fewer are complete (`r13.TOLERANCE_TIERS`). Applied to the 540 stores still short after dewey27 and the Round 6 closures: **379 became complete, 161 remain** (2,856 reviews). Dollar General Newland (2444) and Save A Lot Winston-Salem (3860), whose saved page is a closed store in another city, were held out of the rule (`status_reason`) until Round 7. | Stores left are 11 to 58 reviews short on targets of 100 to 600, after a full fresh pass each. |
| 2026-10-12 | **Cap lifted for one batch.** Honest coverage: we hold **76.3 %** (1.74 M) of the 2.28 M reviews Google lists for our stores; 343 stores have more than 1,500 on Google and about 528,000 reviews past the cap were never collected (largest: H Mart Cary 9,646). The cap is now a per-batch setting (`linux_scraper/review_cap.py`; batch metadata `review_cap`: absent = 1,500, `0` = none), the scraper and worker honour it (8-hour per-store budget when uncapped), `r09 --cap 0 --more-than-cap --probe` queues the stores, and the completion rule no longer treats targets above 1,500 as 1,500. **Not proven live:** Google's sort orders may stop well short of the full count for the largest stores, so run the first few stores of `dewey28` (the largest) and look at the yield before committing machine time. | The 'authoritative' question turns on this: complete against a capped target is not complete against Google. After the run: merge, `r10 --cap 0` (set true targets), `r17`, `r15`, validator, re-freeze. |
| 2026-10-20 | **dewey28 merged** (Forrester, 336 of 497 stores done; 30 failed with nothing, 131 never started): +79,603 reviews. `r10 --cap 0` raised 180 targets to Google's count (e.g. Patel Brothers 1,500 → 2,961). | The cap is gone for those stores; their targets are now true counts. |
| 2026-10-20 | **dewey30a / dewey30b** built from the 131 unstarted dewey28 stores only (owner: not the 30 failed), split evenly by work left, known-keys rebuilt against the current master. | |
| 2026-10-24 | **Scraping stopped for good (owner).** Both boxes pulled. dewey30a +31,884, dewey30b +12,807, plus 3 stray reviews from an old dewey3 copy; 21 older result files on the manabe box were dry-run and held nothing new. `r10 --cap 0` raised 80 more targets. Nothing merged matched the quarantine. **Freeze v2 taken.** | The final dataset. Incomplete stores are documented, not rescraped. |
| 2026-10-24 | **Completion rule, final (owner):** keep the 1,500 cap for completion; allowed missing 10 / 20 / 50 / 100 for targets ≤ 100 / > 100 / > 500 / > 1,500 (`r13.TOLERANCE_TIERS`, tier chosen by the true target). `r15` ruled 228 more stores complete; freeze v2 re-taken with it. | 4,400 complete, 42 incomplete. Stored targets stay Google's true counts. |
| 2026-10-24 | **r03 dedup now quarantines instead of deleting** (it ran `DELETE`, against the repo rule). It found 0 exact-text duplicates, so it was **not** applied: its status step predates `r15` and would reset 1,278 statuses. | Don't run r03 on this master; use r15 for statuses. |

---

## 4. Current state of the data

### 4.1 Reviews (freeze v2, 2026-10-24, final)

| | |
|---|---:|
| stores | **4,442** (4,400 `complete`, 42 `incomplete`) |
| reviews | **1,941,538** (977,398 with text), 590 cities |
| sum of review targets | 2,207,804 (true Google counts wherever a scraper saw the page) |
| fill vs. targets capped at 1,500 | **99.6 %** (955 reviews short, in the 42 incomplete stores) |
| fill vs. Google's true counts | 87.9 % (stores past 1,500 hold their newest ~1,500–5,600) |
| stores whose target still sits at exactly 1,500 | 109 (true count never recorded; treat as censored) |
| `quarantined_reviews` | 29,321 (set aside, never deleted) |

**The 42 incomplete stores** all have targets of 101–500. 40 are 21–25 reviews short each after repeated full passes (Google is not
returning them); 2 (Dollar General 2444, Save A Lot 3860) are held for the Round 7 manual check. 955 short in all. Under the 1,500 cap every large store
counts complete (the 111 big stores hold 1,500+; their true counts are kept in `target_reviews`).

For analysis: the reviews held for a big store are its **most recent** reviews (newest-first scroll), not a random sample. Flag those stores as censored.

The freeze-v1 breakdown below is kept for history (`NC_Grocery_POIs.xlsx` column `collection_status` was built from v1).
How the 4,447 v1 stores came to be `complete`:

| `collection_status` | stores | meaning |
|---|---:|---|
| `complete` | 2,980 | reached target (or within the tolerance rule) |
| `complete_google_limit` | 1,444 | ruled complete: repeated full passes returned nothing new (1,374 "stalled"), or within tolerance (68), or could not be re-scraped |
| `no_reviews_on_google` | 23 | open stores for which Google shows 0 reviews |

1,483 stores carry a `status_reason` — that column is the audit trail for every ruling.

Rushco Markets stays ruled complete (target unverified; never found reliably on Google). The 23 "no
data" stores (verified by hand in Round 5) are complete and unaffected.

### 4.2 Side databases

| file | stores | reviews |
|---|---:|---:|
| `Step_5_out_of_scope.db` | 4,736 | 105,892 |
| `Step_5_closed_stores.db` | 67 | 7,836 |
| `Step_5_excluded_stores.db` | 221 | 163 |

### 4.3 Validator (`python Remediation/r00_validate.py --fail-on HIGH`)

Freeze v2 exits **1 (HIGH)**: 40 incomplete stores and one false alarm, expected with scraping stopped. No BLOCKER.

| check | count | decision |
|---|---:|---|
| incomplete stores | 40 | HIGH. The 42 stores in §4.1 (the validator tolerates 1 short). Documented, not rescraped. |
| over-scrape class B "double scrape" | 1 | HIGH, **false alarm**: Wegmans Morrisville (8259) holds 2,605 reviews on a 1,500 target with **0** duplicate groups. Its target is a stale capped number, not a double scrape. Fix by setting its target from a live count (or accept it in `accepted_flags`); nothing to dedupe. |
| target on the 1,500 cap / truncated at it | 109 / 348 | stays; label as capped in the analysis |
| over-scrape (all classes) | 30 | stays (accumulated over months; r18 found the 67 checked CONSISTENT) |
| within-store near-duplicate reviews (50-char key) | 1,048 | stays (owner ruling 2026-10-11) |
| rating-only same-name pairs | 327 | kept on purpose |
| reviews with no rating / no date | 983 / 979 | stays |
| review text ending in a See-more marker | 7 | stays |

Accepted flags in `accepted_flags`: 340 far-from-address stores (r13) and store 9472 (Coastal
Plains Produce) for coordinates outside NC (Google's own pin for that farm is outside NC).
`NC_Grocery_POIs.xlsx` leaves that store's coordinates blank.

### 4.4 Store universe (`Frame/build/probe.db`, 16,751 stores)

Historical context for the scope decision. All 16,751 were probed (done 15,350 · unverified 767 ·
failed 634). The store list is **final** by owner ruling (2026-10-11); the probe numbers are no
longer a to-do.

### 4.5 Known gaps against the analysis schema

Teresa's pilot rows carry things our corpus does not: an **absolute `publishedAtDate`** (we have
Google's "2 years ago" string plus `scraped_at`), a stable **`reviewerId`** (we have
`reviewer_name`, which is not a key — "Chris" reviews 240 stores), `likesCount` (unavailable in
the current Google UI), and `cid`/`fid` (captured on the page but not stored in master). They shape
the export step.

---

## 5. Definition of done

| # | criterion | state |
|---|---|---|
| 1 | Store list is authoritative and frozen | **done** — owner ruling 2026-10-11; list = the 4,447 stores in `NC_Grocery_POIs.xlsx` |
| 2 | Every review of every in-scope store is in master; `r00_validate.py --fail-on HIGH` exits 0 | **not met, by owner decision**: scraping stopped with 42 stores short under the capped rule (§4.1). Freeze v2 is final as is |
| 3 | Analysis-ready export in the pilot's shape | **to do** (§6) |
| 4 | Reproducible: `pip install -r requirements.txt`, `python -m pytest -q` (329 tests), and the commands in this file | **done** |

---

## 6. What is left

Data collection is closed. What a successor can still pick up, most useful first:

1. **Analysis export** (see 3 below). This is the step the study is actually waiting on.
2. **Label the censored stores.** Write `collection_status` / an at-cap / a "short of true target" flag onto the
   export for the stores holding fewer than their true count (every store past 1,500) and the 109 whose target still sits at 1,500. Without it, per-store
   analysis will treat a Walmart's newest 2,000 reviews as its whole history.
3. **If collection ever resumes** (not planned): the remaining big-store work is about 272,500 reviews in 111 stores.
   Read `Archive/manabe_box_code_20261024/DIFF_vs_Scraper_v2.patch` first; raise or remove the per-store time
   budget; expect the "no progress" stores (H Mart Cary, Walmart Hickory, …) to stay stuck. Google may simply not
   serve their older reviews. Pipeline after any return: `merge_results --dry-run` then for real →
   `r10 --batch <db> --cap 0 --apply` → `r17 --apply` → `r15 --apply` → `r00 --fail-on HIGH` → new freeze.
4. Small items, unchanged since 2026-10-12:
   - **Round 7 manual** (`Data/exports/Manual_Check_Round7_2026-10-12.xlsx`, 2 stores): Dollar General "Newland" (2444) and Save A Lot "Winston Salem" (3860)
     have a saved link that opens a closed store in another city, so the store we meant may still be open. The other five Round 6 stores were closed
     and moved to `Step_5_closed_stores.db`.
   - **Owner decision, pending:** what to do with stores still short of target after a full fresh pass (Google-limited). Today: 161 stores, 2,856 reviews.
   - Not yet checked: the final store list against the Census employer counts (`Frame/reports/census_validation.csv`).
   - Wegmans Morrisville (8259): stale 1,500 target (§4.3).

**Analysis export (the next real task)** (this PC, ~1 day): a new `Scraper_v2/export_analysis.py` that reads the *frozen*
database and writes one row per review joined to its store, with columns named as in
`Data/reference/Teresa_Sample_Output.csv` where we have them (`reviewId`←`review_id`, `placeId`,
`stars`, `text`, `publishAt`←`date_relative`, `scrapedAt`, `name`, `categoryName`, `location/lat,lng`).
Derive `publishedAtDate` ≈ `scraped_at − parse(date_relative)` and flag it as approximate. Carry
`collection_status` and an at-cap flag onto every row so the analysis can treat capped and
Google-limited stores as such. Write CSV/Parquet to `Data/exports/`.

Housekeeping, owner's call: commit the working tree (nothing from this project's automated sessions
is committed). Backups were pruned at handoff (see `Archive/DELETED_2026-10-24.md`).

### How to reproduce a step

```bash
python -m pytest -q                                    # 329 tests
python Remediation/r00_validate.py --fail-on HIGH      # exit 0 = authoritative
python Remediation/r15_tolerance.py [--apply]          # completion rule: reopens short stores, completes the ones inside the tiers
python Remediation/r18_compare_rescrape.py   # compare over-target stores with a full rescrape; writes the manual workbook (moves nothing)
python Remediation/r16_round4_answers.py <done.xlsx> [--apply]   # apply a completed manual round
python Remediation/r17_restore_confirmed.py [--apply]  # restore rows a scrape of the store's own page confirms
python Remediation/make_manual_round5.py [--count-check ID]       # build a manual-check workbook
cd Scraper_v2 && python -m linux_scraper.merge_results ../Data/returns/<dir>/<file>.db \
    --master ../Data/Step_5_master_reviews.db --dry-run           # then without --dry-run
```

Queues: `r09_build_scrape_queue.py --order pct [--also-ids ID...] [--ignore-held-back] [--split N]`,
`r07_build_known_keys.py --batch … --apply`, `Deploy/build_kit.py review --batches … [--with-auth]`.

---

## 7. Conventions (for people and AI sessions)

- **The frozen database is read-only.** Re-freezing means a new `VACUUM INTO` copy, a new SHA in
  `Remediation/master_freeze_manifest.json`, and the banner above updated.
- **Do not commit to git from an automated session.** The owner commits.
- **One copy of every piece of code.** Kits are outputs of `Deploy/build_kit.py`. If a fix is made
  on a volunteer machine, port it here, add a test, rebuild.
- **Tests before code**: `python -m pytest -q` from the root must stay green (329 today). New
  Python gets a test next to it.
- **Secrets**: only `Auth/`. `google_auth*.json` is gitignored everywhere.
- **Databases**: checkpoint the WAL before copying (`PRAGMA wal_checkpoint(TRUNCATE)`), or use
  `VACUUM INTO`. Every writer takes a timestamped backup into `Data/backups/` first and is
  idempotent. Never delete rows to fix scope — set a status, record a reason, or move them to a side
  database (owner rulings: out-of-scope, closed and excluded stores live in their own files).
- **`status_reason` is a decision.** Never recompute a status over it; r10/r12 recompute skip rows
  that have one.
- **Merges**: always `--dry-run` first. `merge_results` ignores any review already in
  `quarantined_reviews`; `r17` is the tool for reviews that were set aside wrongly.
- **Manual checks**: "send to manual" means an Excel workbook of everything the owner needs to
  look up. Build it with `make_manual_round*.py`, show every address field the store has (an
  earlier sheet printed only `full_address` and hid the street), and apply the `_done` file with `r16`.
- **Paths**: scripts assume they run from the repo root; `merge_results` runs from `Scraper_v2/`.
- **A field that is uniformly populated is as suspect as one uniformly empty.** Count what a
  selector matches on live pages before trusting it (`Remediation/probe_review_dom.py`).
