# Remediation — from "usable" to authoritative

Audit date: 2026-09-05. Snapshot audited: `Data/Step_5_master_reviews.db` (30 Apr 2026).

This folder turns the quality report from prose into code. `r00_validate.py` is the
definition of "authoritative": it exits non-zero while the dataset is not. Everything
else fixes one class of defect, dry-runs by default, and backs up before writing.

## Status: applied 2026-09-05, volunteer returns merged 2026-09-16

Steps r01 through r04 and r08 have been run against master. 31 tests pass
(`python -m pytest -q` from repo root runs 68 tests; 31 are in Remediation). Every step took a timestamped backup, and
those backups are in `Data/backups/`.

| | before | after |
|---|---:|---:|
| stores | 4,889 | **5,226** |
| reviews | 1,201,237 | 1,203,168 |
| reviews with text | 607,935 | 608,776 |
| cities | 578 | 585 |
| capped fill | 84.35% | 84.70% |
| failing checks | 24 | 20 |

Closed outright:

| defect | before | after |
|---|---:|---:|
| stale `scrape_status` | 661 | **0** |
| marked complete holding zero reviews | 158 | **0** |
| over-scrape from a stale target | 107 | **0** |
| exact duplicate reviews, text-bearing | 331 | **0** |
| collected reviews stranded off-master | 2,262 | **0** |
| Costco locations in North Carolina | 0 | **9** |

Chain coverage moved with the recovery: Harris Teeter 33 to 47, Ingles 33 to 50, Aldi to
59, Publix to 53, Whole Foods 4 to 6.

Store status is now honest rather than flattering: 3,343 complete, 1,346 incomplete, 380
not started, 157 with no known target. None of those 157 are pretending to be finished
any more, which means they are reachable by the scraper again.

**2026-09-16 update:** Volunteer returns merged. Forrester machine `sl9mw2` run2: 104,607 new reviews (144 dup). Manabe machine `alodov` run1: 57,378 new reviews (5 dup). Master now 1,368,169 reviews (5,226 stores). r00 after merge: 108 far-from-address, 53 shared-review pairs (71 stores), 9 wrong-location — adjudication queue needs regenerating.

**What is deliberately still open.** After 2026-09-16 merge, 108 stores sit far from their own address and 53 store pairs share 20+ identical reviews (71 distinct stores), with 9 tagged as trustworthy wrong-location. `r06` writes them to a worklist with a Google link per row. The 396 rating-only duplicate pairs are also still there on purpose; see section 2.

## Update 2026-10-11 — closed: dataset frozen

Everything above is the 2026-09 audit. The work since then is in the scripts below; the outcome is
that `r00_validate.py --fail-on HIGH` exits 0 and master is frozen
(`master_freeze_manifest.json`; numbers in [`STATUS.md`](../STATUS.md)). `python -m pytest -q`
runs 316 tests.

| script | what it does | reads / writes |
|---|---|---|
| `r10_repair_targets_from_live.py` | targets from Google's live count | master |
| `r11_import_probe_stores.py` | imports probed stores into master | master |
| `r12_rulings.py`, `r13_rulings.py` | duplicates, out-of-scope, wrong-location quarantine, stalled stores, per-store rulings (`step_store_rulings`: OUT, CLOSED_*, NO_DATA, OPEN, TARGET, COMPLETE, DUP:n). **`r13.stalled_is_complete` is the owner's completion rule (2026-10-11)**: a store is complete when it reaches its target, or when its target is over 500 and it is missing 20 or fewer reviews, or its target is over 100 and it is missing 10 or fewer, or its target is 100 or under and it is missing 5 or fewer; anything else short of a verified target is rescraped (no cap on the target: a target past 1,500 is a real count; run `r10 --cap 0` to record it). `split_out_of_scope` moves a status to a side database. | master, side DBs |
| `r14_manual_answers.py` | applies manual rounds 2 and 3 | master |
| `r15_tolerance.py` | `reopen_short`: stores ruled complete by count (stalled / within tolerance / no target) that are short of target under the rule go back to incomplete, old reason kept in `reopened_rulings`; `apply_tolerance`: rules complete the ones inside the tiers above. Skips stores with a malformed place_id. | master |
| `r16_round4_answers.py` | applies a completed manual round (sheets `place_id_check`, `location_check`, `category_check`, `no_target_check`, `target_check`; reads only the sheets present). Closed / not-on-Google → side database; a new URL → re-point, or fold into the store that already holds that page; open + count → target (a count of 0 on `no_target_check` → "no data"); `IN_NC` → coordinates from the URL; category. Unclear answers are listed, never guessed. | master, side DBs |
| `r17_restore_confirmed.py` | moves a store's quarantined reviews back when a scrape of the store's **own** page returned them again and no other store holds them; reads every `Data/returns/*/*.db`; idempotent | master |
| `r18_compare_rescrape.py` | read-only. For stores holding more than Google shows: compares a full unfiltered rescrape (queued with `r09 --also-ids`) with what we hold. CONSISTENT (the scrape returned 90 % or more of the live count and 90 % or more of it is already held) means keep everything; MISMATCH, PARTIAL, SCRAPE_TOO_SMALL and NO_FULL_SCRAPE go to a manual workbook (Round 6), together with every store still short after the rules. Moves and deletes nothing. | `Data/exports/` |
| `r09_build_scrape_queue.py` | flags added 2026-10-11/12: `--order pct` (store furthest short of its own target first; the scraper honours `claim_order=pct`), `--also-ids` (queue a store that is not short, collected in full with no skip-list, via `r07`), `--ignore-held-back` (ignore the stale r06 worklist), `--cap N` (0 = no cap; written to the batch as `review_cap`, which the scraper honours; absent means 1,500), `--more-than-cap --probe PROBE.db` (also queue every store Google shows more than 1,500 reviews for and we hold fewer, at Google's full count, whatever its earlier ruling) | batch DB |
| `make_manual_round4.py`, `make_manual_round5.py` | build the manual-check Excel workbooks (instructions sheet, drop-downs, yellow answer columns). The address column falls back to street + city + state ZIP when `full_address` is blank. | `Data/exports/` |
| `master_freeze_manifest.json` | the freeze record: SHA-256, counts, side-database SHAs | — |

Every writer dry-runs by default, backs master up first, and is idempotent. Run them in this order
after a merge: `r17` (restores wrongly set-aside reviews, then r15), `r15`, `r18` (compare the over-target
stores, build the Round 6 workbook), `r16` for a completed manual workbook, `r15` again, then `r00_validate.py --fail-on HIGH`.

**Validator changes** (each with a test in `test_r13_rulings.py`): the coordinates check skips
stores in `accepted_flags` with `check_name='coords_outside_nc'`; "stores with no target" skips
stores ruled `no data…`; "non-standard place_id" skips ruled stores; the umbrella "over-scrape" count skips stores in `accepted_flags` as `over_target_accumulated` (65 stores whose extra reviews built up over several scrapes). The BLOCKER-level wrong-location check does not. What remains at the freeze is
MED/INFO and accepted by the owner (caps, 403 within-store near-duplicates, reviews with no
rating/date).

**Lessons** (see also the root README): `merge_results` treats `quarantined_reviews` as already
known, so a fresh scrape of a store's own page was dropped when the old cleanup had set those
reviews aside — `r17` is the repair. A "Google ceiling" after four sort orders is not proof; a
retry on a fresh session recovered 24,481 reviews. A manual sheet must show every address field a
store has.

## Running it

```
python Remediation/r00_validate.py                 # where we are
python Remediation/r02_migrate_master_schema.py --apply
python Remediation/r01_recover_unmerged_batches.py --apply
python Remediation/r03_clean_deterministic.py --apply
python Remediation/r04_recover_inventory.py --apply
python Remediation/r08_repair_recovered_metadata.py --apply
python Remediation/r05_lost_store_worklist.py      # the 4,294, for re-enrichment
python Remediation/r06_contamination_worklist.py   # the 160, for adjudication
python Remediation/r07_build_known_keys.py --batch Data/batches/run1.db --apply
python Remediation/r09_build_scrape_queue.py       # build batch from current master state
python Remediation/r10_repair_targets_from_live.py # backfill targets from live Google
python Remediation/make_smoke_batch.py             # 5-store batch for smoke-testing
python Remediation/probe_review_dom.py             # count what selectors really match
python -m pytest -q                                # all tests
python Remediation/r00_validate.py                 # what changed
```

---

## 1. The finding that outranks everything else

The dataset reports **84.3% completion**. That figure is the share of *review targets*
filled for the stores that are in the dataset. It cannot see a store that never got in.

Reconciling the North Carolina candidate set against both the dataset and the exclusion
audit:

| | stores |
|---|---:|
| NC candidates (`Step_1.3`) | 16,680 |
| reached master | 4,889 |
| explained by the exclusion audit | 7,497 |
| accounted for | 12,386 |
| **no record of any kind** | **4,294** |

Roughly 2,314 never reached the Google lookup at all and 1,988 were looked up and then
vanished. `Data/README.md` Part A presents part of this as a deliberate step,
"Match + dedup unmatched cohort (−2,215)". It is not a dedup: only 113 of those rows
share a name and street with a row that was kept, and 1,375 cannot be explained as a
duplicate on name+street *or* name+city.

**The loss is not uniform.** Retention by ZIP prefix runs from 14.4% to 37.8%. The
worst-served regions are rural and eastern; Charlotte retains 2.6× what the Raleigh 276
prefix does. Dollar General is the sharpest instrument, being the best-captured chain at
80.4%: **71 NC cities have a Dollar General upstream and none in master.** An analysis of
this snapshot would report food deserts in eastern North Carolina that are artifacts of
the pipeline rather than facts about the state. For a study whose entire purpose is
locating food deserts, this is the defect that matters.

---

## 2. Correctness defects, by how they fail

### They corrupt the data (BLOCKER)

**106 stores sit more than 25 km from the address they carry**, 61 of them more than
100 km, holding 30,367 reviews. Sam's Club "Hendersonville" resolves to a Raleigh
address with 1,062 reviews; Grand Asia Market "Fletcher" to Raleigh with 1,432.

The cause is in `Step_1.4_EnrichOverture__Program`: `verify_business` computes a name
similarity, logs it, and then accepts the result **on both branches**. Both warning
strings say "accepting anyway". With verification inert the program took Google's first
search result for every query. In the enrichment output, 17.9% of "successful" rows
carry a Google address in a different city and 267 are outside North Carolina.

This cannot be caught with the recorded coordinates: 70.4% of `gmaps_lat/lon` values are
the query coordinate echoed back out of the `@lat,lon,17z` URL segment. Only the address
is an independent signal, which is why the check in `r00_validate.py` compares Overture
coordinates against a centroid built from the ZIP printed in the store's own address.

**42 store pairs share 20 or more byte-identical reviews**, 49 stores in total. One
review page was collected into several store ids. In the worst cluster the same 219
reviews sit under five different stores.

**158 stores are marked `complete` while holding zero reviews**, 152 of them because
their target is NULL. `merge_results.py` reads `target = store["target_reviews"] or 0`
and then treats `target == 0` as complete. `complete` is a one-way door: the scraping
export selects only `pending` and `incomplete`, the importer skips `complete`, and no
code path anywhere demotes a store. These 157 stores are not mislabelled, they are
**unreachable by every tool in the pipeline**.

**The completion arithmetic double-counts.** In the `prepare_machine_db` path, master's
reviews are physically copied into the batch database. `scraper.py` then computes
`total_reviews = master_reviews + final_count` where `final_count` counts *all* rows in
the batch including the copies, while the collection loop's progress counter also starts
at the copied count. With a target of 1,000 and 400 already in master the store closes
having collected 200 new reviews. Whenever `target ≤ 2 × master` it completes with zero.

**The merge is not idempotent.** Master's `reviews` table has no review identifier and
no unique index. Google's `data-review-id` is captured by the scraper, stored in the
batch database, and then dropped at the merge boundary because `merge_results.py` does
not select it. Identity is reconstructed from a lossy content key held in a Python set
that is snapshotted at the start of a run. Store 3175 holds each of its 499 real reviews
exactly twice, crossed its target of 966 on the duplicates, and was stamped `complete`
while missing 467 reviews.

### They lose data quietly (HIGH)

- **`except Exception` cannot catch `CancelledError`.** Since Python 3.8 it derives from
  `BaseException`. The per-store timeout handler at `scraper.py:1332` is meant to flush
  the buffer and does not run, discarding up to 49 reviews on every timeout — and a
  timeout is the normal ending for a large store. The dead `isinstance(exc,
  asyncio.CancelledError)` line just below proves the opposite was intended.
- **2,262 collected reviews were never merged.** `batch1.db` ran 8–10 May on machine
  `tqehk2`, collected 2,263 reviews, and its store rows still read `pending`. One of them
  is a complete 1,500-review harvest for a store the status report lists as not started.
- **`known_review_keys` has no writer anywhere in the repository.** The two batch files
  hold about 400,000 keys each. They decide which reviews the scraper skips as already
  known, and they were produced by a script that is not in version control. Nobody can
  verify which snapshot they came from or whether the format matches `make_dedup_key`.
- **Three fields are empty in all 1,201,237 rows.** `helpful_count`'s selector
  `button[aria-label*='helpful']` is case-sensitive and Google's label is "Helpful".
  `photo_count` counts `div.Iop04 img` where Google renders background-image buttons, and
  `locator().count()` returns 0 for a non-matching selector without raising, so there is
  not even an exception to swallow. `service_type` targets the wrong span. The same
  selectors are in the Archive scrapers, which is why every era is empty.

### They mislead the analyst (MED)

- **`scrape_status` is wrong for 661 stores**: 107 `complete` below target, 554
  `incomplete` at or above it.
- **The completion denominator is itself truncated.** `target_reviews` is capped at
  1,500 and 315 stores sit exactly on the cap. For those, the real review count was never
  recorded and cannot be recovered from this database. 84.3% is a ceiling on a floor.
- **The corpus skews old.** 59.6% of reviews are four or more years old and 1.4% are
  under a month. That is the fingerprint of Google's "Most relevant" ordering rather than
  "Newest", so the corpus is not a uniform sample of review history.
- **Half the corpus has no text**, and the text that exists is negatively biased:
  1-star reviews carry text 73% of the time against roughly 48% for 4- and 5-star. Any
  text method runs on ~608K reviews, not 1.20M, on a non-random half.

### Corrected during this audit

Two claims in circulation did not survive checking, and the corrected versions are in
the validator:

- **"18,047 truncated reviews"** is a false positive produced by SQLite's `LIKE`, which
  is case-insensitive for ASCII, so `LIKE '%More'` matches any review ending in the
  ordinary word "more". Under case-sensitive `GLOB` the count is 6. The 17,408 rows
  ending in an ellipsis average 100.7 characters against a corpus average of 128.8 —
  truncation would make them longer, not shorter, so they are the reviewers' own
  punctuation after an emoji.
- **"734 duplicate pairs, delete them"** would have destroyed real data. 396 of them are
  rating-only: same store, same reviewer name, same stars, no text. "Chris" appears at
  240 distinct stores in this corpus, so two different people leaving an untexted 5-star
  rating at one busy supermarket is ordinary. `r03` deletes only the 331 text-bearing
  groups and leaves the rest alone on purpose.

---

## 3. The 157 "contaminated" stores are three problems

The quality report treats them as one defect needing six hours of manual verification.
Split by the shape of the overshoot:

| class | stores | what it is | fix |
|---|---:|---|---|
| stale target | 107 | target under 10, usually 4, against hundreds of real reviews. Best-effort mode correctly distrusted it and used Google's live count. | back-fill the target, touch no reviews |
| double scrape | 10 | 1.7×–2.3× target with matching duplicate counts. Claimed twice in one batch; unstable review ids made the second pass look new. | delete exact duplicates |
| wrong location | 9 | trustworthy target of 50+ and a large multiple of it. | a human opens the place_id and rules |

Only the last class needs judgement, and it is about two hours of work. Note this is a
*different* population from the 106 geographic mismatches: a store matched to the wrong
branch of the same chain in the same city keeps a plausible review count, so the
overshoot heuristic never sees it. That is why both checks exist.

---

## 4. Recovery, in order of value per hour

| action | stores recovered | cost |
|---|---:|---|
| `r04` — wrongly excluded stores, including all 9 Costcos | **+337** | minutes |
| rule on the 162 categories nobody ever evaluated | ~+500 | a person, once |
| `r05` → re-run enrichment on the 4,294 | **+1,375 to +4,294** | the long pole |
| re-match the 1,605 unmatched against the existing Outscraper pool | +130 grocery-relevant, 417 total at 95–97% precision | no new data purchase |

**Costco is the whole category problem in one example.** `Step_1.8_Grocery_Store_Classification.xlsx`
holds three warehouse strings and rules on them inconsistently: `Warehouse club` → yes,
`Warehouse store` → No, `Warehouse` → No. Google labels Costco "Warehouse store" and
labels Sam's Club and BJ's "Warehouse club". Sam's Club and BJ's are in the dataset and
every Costco in North Carolina is not, and the only thing separating them is which
synonym Google happened to use. All nine place_ids are on file in the exclusion audit.

Two more failures of the same kind: the exclusion audit holds 573 distinct Google
categories and the classification sheet ruled on 424, so **162 were excluded by falling
through a lookup rather than by any decision** — including `Eastern European grocery
store`, `American grocery store`, `Carnicería` and `Tienda de alimentación`. And 75
stores were dropped because their category string held two values and the filter compared
the whole string for equality instead of reading the primary category.

There is also a scope question the research group has to answer rather than a script:
farmers' markets, produce markets, butcher shops and seafood markets are all excluded,
while 860 dollar stores are included. Those excluded categories also have the worst
identity-match rates, so they are lost twice.

---

## 5. Fix the scraper before scraping again

Re-running the current code would reproduce the same defects. In priority order:

1. **Restore place_id navigation and verification.** v1 navigated by place_id and verified
   by exact place_id; v2 deleted both and replaced them with a name substring test plus a
   10 km distance veto. The veto is structurally blind: `_extract_page_latlon` reads the
   map camera, and after navigating to `/maps/search/…/@lat,lon,15z` the camera *is* the
   target coordinate, so the distance is near zero for any result clicked. 686 of 705
   stores in `batch1.db` carry a real `ChIJ…` id that is currently unused.
2. **Fail closed.** Verification and block detection should return "not verified" on
   error, not "fine". Never initialise an extraction target to a valid-looking default;
   start every field at `None` so a failure is distinguishable from a real zero.
3. **Add a coverage assertion.** After each batch, assert every field is non-default in at
   least some rows. Fifteen lines would have caught the three dead fields on day one
   instead of after 1.2 million rows.
4. **Catch `BaseException` on the flush path**, and drop the flush interval from 50.
5. **Carry `review_id` into master** and put a unique index behind it, so the merge is
   idempotent by construction rather than by a Python set.
6. **Consider reading Google's own review RPC** rather than the DOM. The
   `listentitiesreviews` payload the page already fetches carries a stable id, an absolute
   timestamp, the helpful count and the photo list — one parser instead of six obfuscated
   class names, and it fixes the dead fields and the missing absolute dates together.

---

## 6. What the tools do

| file | purpose | writes? |
|---|---|---|
| `lib_master.py` | shared helpers; backup checkpoints the WAL before copying to `Data/backups/` | no |
| `r00_validate.py` | the definition of authoritative, as a pass/fail suite | no |
| `r01_recover_unmerged_batches.py` | merges the 2,262 stranded reviews | `--apply` |
| `r02_migrate_master_schema.py` | adds `review_id`, `machine_id`, hashes, qa flags, indexes | `--apply` |
| `r03_clean_deterministic.py` | duplicates, targets, counters, statuses, flags | `--apply` |
| `r04_recover_inventory.py` | puts back 337 wrongly excluded stores | `--apply` |
| `r05_lost_store_worklist.py` | exports the 4,294 for re-enrichment | no |
| `r06_contamination_worklist.py` | adjudication queue for 160 suspect stores | no |
| `r07_build_known_keys.py` | builds the skip-list, writes to batch metadata | `--apply` |
| `r08_repair_recovered_metadata.py` | fills city gaps, flags bad geocodes | `--apply` |
| `r09_build_scrape_queue.py` | builds batch from current master state, writes to `Data/batches/` | no |
| `r10_repair_targets_from_live.py` | backfills targets from live Google count | `--apply` |
| `r11_import_probe_stores.py` | inserts stores the Google probe found (by category) into master and refreshes targets from live counts | `--apply` |
| `make_smoke_batch.py` | 5-store batch for smoke-testing, writes to `Data/batches/` | no |
| `probe_review_dom.py` | counts what selectors really match on live reviews | no |
| `test_remediation.py` | 31 regression tests, one per defect | no |

Every writing tool takes a timestamped backup first and is idempotent. `r00`, `r05` and
`r06` never open master for writing at all.

### The adjudication queue

`r06` merges the three independent contamination tests and ranks by how many a store
fails:

| | stores |
|---|---:|
| coordinates over 25 km from their own address | 106 |
| sharing 20+ identical reviews with another store | 49 |
| over-scraped with a trustworthy target | 9 |
| **distinct stores to adjudicate** | **156** |
| **failing more than one test** | **8** |
| reviews at risk | 59,100 |

Start with the 8. Two independent tests agreeing is close to proof: Sam's Club
"Hendersonville" is 329 km from its address *and* shares reviews with another store, and
holds 1,062 reviews. Each row carries a `place_id` link that settles it in seconds, and a
`ruling` column to fill in. No script decides these, because the right remedy depends on
which half of the contradiction is wrong, and that is a judgement.

## 8. Scraper fixes applied

`Scraper_v2/linux_scraper/scraper.py` has been patched, because re-running the previous
code would reproduce every defect above. Changes, all compile-checked:

- **place_id navigation restored** as strategy 0, ahead of the four text searches. It is
  the only construction that cannot land on a different store, and 686 of the 705 stores
  queued in `batch1.db` carry an identifier the previous code never used.
- **place_id verification restored** as the first and decisive tier, in both directions:
  landing on a different id is now a rejection rather than an unnoticed success.
- **`_extract_page_latlon` now reads the business pin** (`!3d`/`!4d`) instead of the
  `@lat,lon` map camera. This is the fix that matters most. After navigating to
  `/maps/search/<query>/@<lat>,<lon>,15z` the camera *is* the coordinate you asked for, so
  the distance from target to page was near zero for whatever result got clicked. The 10 km
  veto was not too loose, it was disconnected.
- **Distance thresholds split by store type**: 3 km for chains, where neighbouring branches
  are the thing being confused, and 10 km for independents, where the name is genuinely
  distinguishing and Overture-to-Google coordinate disagreement needs absorbing.
- **Verification fails closed.** A chain name with no usable geometry, an untokenisable
  name, and a fuzzy match with nothing to corroborate it are all rejections now. Each was
  previously an accept, and `if not target_words: return True` accepted any page at all.
- **House-number matching is exact.** The address tier intersected every number in both
  strings, so a shared ZIP verified 7400 The Plaza against 1000 The Plaza.
- **The three dead selectors are fixed**: case-insensitive matching for the helpful
  control, background-image buttons rather than `img` elements for photos, and text
  parsing rather than a rotating class for the attribute chips.
- **`except BaseException` on the flush path.** `CancelledError` derives from
  `BaseException`, so the flush written for the timeout path never ran on it. Ordinary
  exceptions still fall through to the normal return, so a transient hiccup costs one
  store rather than aborting it.
- **`assert_field_coverage()`** logs an error when a field found no element on 95% or more
  of a store's reviews. This is the fifteen-line check whose absence cost three fields
  across 1,201,237 rows.

The sort-order ladder now selects `relevant` explicitly instead of treating it as a
no-op rung, closes the dropdown when an option is missing, and reports whether the
ordering actually changed so the caller can skip a rung that did not apply rather than
burning another 60 empty cycles in the previous ordering. `known_review_keys` has a
generator, `r07`, which imports the real key function so it cannot drift and records its
provenance in the batch metadata.

## 9. What the live run found

Running the patched scraper against real stores surfaced two defects that no amount of
reading would have produced, and confirmed the fixes that mattered.

**Confirmed working.** Navigation went to the place_id URL as strategy 1. The distance
check reported "business pin is 0.03 km from target", a real measurement; before the fix
it read the map camera, which after navigating to `/@lat,lon,15z` is the coordinate you
asked for, so it reported near zero for any store anywhere. `helpful_count` and
`review_id` populate. The first store collected 40 of 40 in four scrolls.

**Defect found: the scroll container was chosen by first match, not by function.**
`_find_scrollable_container` returned the first selector with a non-zero count from a
list ending in the very general `div.m6QErb` and `div[role='main']`, and the scroll then
ran `document.querySelector` on it, taking the first element in DOM order. When that was
not the review panel, the scroll did nothing at all: no error, no exception, just a page
that never moved, 60 empty cycles, and a store that looked like it had run out of
reviews.

Two consecutive stores on identical code made this unmissable. One collected 40 of 40.
The next collected 5 of 40 from a listing with 1,191 reviews. The container now starts
from a real review node, walks up to the nearest genuinely scrollable ancestor, and tags
it, so the scroll addresses one unambiguous element. The scroll also verifies that
`scrollTop` actually moved and counts a miss when it did not, because a scroll that
silently does nothing is indistinguishable from the end of the list, and that confusion
is the most expensive one in this scraper.

This is very likely a contributor to the corpus being short in the first place. A store
whose panel never scrolled would have been recorded as complete at whatever the first
screen held.

**Correction: helpful_count and photo_count are NOT recoverable, and an earlier claim
here that they were fixed was wrong.**

The first pass looked like a success. `photo_count` populated, and `helpful_count` showed
39 zeros and a single 1 across 40 reviews, which is exactly the distribution a real
helpful count would have. Both were measuring page furniture.

What settled it was `probe_review_dom.py`, which drives the real scraper to a live
reviews panel and counts what each candidate selector matches on each review node. A
selector returning the same count on every review is measuring a fixture; one whose count
varies is measuring content. On eight live reviews:

| selector | result |
|---|---|
| `button[aria-label*='Like' i]` | constant 1 — the Like control, present pressed or not |
| `button[aria-label*='Photo' i]` | constant 1 — the reviewer's avatar, "Photo of <name>" |
| `button[aria-label*='helpful' i]` | never matches |
| `button[jsaction*='review.thumbUp']` | never matches |
| `button.Tya61d`, `div.KtCyie button`, `[data-photo-index]` | never match |
| `div.PBK6be` and its variants | never match |

So the current Maps UI renders no helpful-count element at all, and no review-photo
container on these pages. Both furniture selectors are removed. The real selectors stay
in case a UI variant exposes them, a miss is recorded rather than a zero written, and all
three fields are registered as OPTIONAL so they report at info instead of crying wolf on
every store.

**Treat `helpful_count`, `photo_count` and `service_type` as UNAVAILABLE, not as measured
zeros.** That is the same conclusion the original quality report reached, arrived at with
evidence this time. The difference that matters is `review_id`, which is genuinely
recovered and is the field that actually fixes deduplication.

The lesson generalises past this codebase: a field that is uniformly populated is as
suspect as a field that is uniformly empty, and the only way to choose a selector against
an obfuscated DOM is to count what it matches on real pages.

---

## 7. Definition of done

1. `r00_validate.py --fail-on HIGH` exits 0.
2. The 16,680 candidates are fully reconciled: every row is in master, in the exclusion
   audit with a reason, or in a documented recovery worklist. No residual.
3. Retention by ZIP prefix is flat enough that no region is an outlier, or the variation
   is explained and documented.
4. Every major NC chain is present at its known store count.
5. The snapshot is frozen with a SHA256, and this report is regenerated against it.

Note that item 1 is achievable in a day and items 2 and 3 are the real work. A dataset
that passes the first and fails the others is exactly the dataset that exists today.
