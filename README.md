# NC Grocery Store Reviews — Data Pipeline & Scraper

Data infrastructure for research on **aspatial food access**: measuring food deserts through
the lived *experience* of grocery shopping — what people write in Google Maps reviews —
rather than distance-to-store alone. Built for Teresa Groton's MS thesis research (NC State ISE,
Prof. Benjamin Rachunok's group); the method was demonstrated for Raleigh, and this repo
scales the data collection to all of North Carolina.

**→ Read [`PHASE1_REPORT.md`](PHASE1_REPORT.md) first.** It is the Phase 1 project report: goal, method, final numbers,
known limits and where everything is. [`STATUS.md`](STATUS.md) is the full decision log behind it.
The scoring pipeline (Phase 2) lives in its own repo, `suyashh77/nc-aspatial-food-access`.

**Final, 2026-10-24 (freeze v2, scraping closed):** 4,442 stores · **1,941,538 reviews** · 99.6 % filled against
targets capped at 1,500 (about 88 % of Google's true counts). The frozen file is `Data/Step_5_master_reviews_FROZEN_2026-10-24.db`
(SHA-256 in `Remediation/master_freeze_manifest_v2.json`) and the store table is `NC_Grocery_POIs_v2` (rebuilt from freeze v2; in the Phase 1 deliverable).
Completion rule: target capped at 1,500, up to 10/20/50/100 missing for targets ≤100/>100/>500/>1,500.
4,400 stores complete, 42 short. Stores past 1,500 hold only their newest reviews. See `STATUS.md` §4.1 before any per-store analysis.

---

## Layout

| path | what |
|---|---|
| [`PHASE1_REPORT.md`](PHASE1_REPORT.md) | Phase 1 project report (start here) |
| [`STATUS.md`](STATUS.md) | full decision log, numbers and history |
| [`Frame/`](Frame/README.md) | **Step 1** — the store universe: four-source frame (SNAP · Overture · OSM · project assets), Google probe (`probe.py`), probe result merge |
| [`Scraper_v2/`](Scraper_v2/) | **Step 2** — the review scraper: `linux_scraper/` package (headless Playwright), `run_continuous.py` orchestrator, `linux_scraper.merge_results` |
| [`Remediation/`](Remediation/README.md) | audit + repair scripts for the master DB; `r00_validate.py` is the definition of "authoritative" |
| [`Deploy/`](Deploy/README.md) | `build_kit.py` assembles the volunteer kits (review kit for Linux boxes, probe kit for a Windows box) from canonical code |
| [`Data/`](Data/README.md) | data lineage, schema and quality history; the data files themselves are gitignored |
| `Auth/` | live Google session cookies (gitignored — never commit, never share) |
| [`Archive/`](Archive/README.md) | superseded scrapers and the old enrichment stage, kept for provenance |

## Run

```bash
pip install -r requirements.txt            # plus: pip install -e <path to geomatch> for Frame/s09,s10,s13
python -m pytest -q                        # 329 tests

python Frame/probe.py --stats              # probe queue state
python Remediation/r00_validate.py --fail-on HIGH   # master DB health, read-only; exit 0 = authoritative

# volunteer kits
python Deploy/build_kit.py review --batches Data/batches/run1.db Data/batches/run2.db --with-auth
python Deploy/build_kit.py probe --with-auth

# rulings and manual checks (each dry-runs unless given --apply)
python Remediation/r15_tolerance.py                      # completion rule: reopens short stores; target > 1,000 missing < 10 is complete
python Remediation/r16_round4_answers.py <done.xlsx>     # apply a completed manual-check workbook
python Remediation/r17_restore_confirmed.py              # restore reviews a scrape of the store's own page confirms
python Remediation/r18_compare_rescrape.py               # compare over-target stores with a full rescrape; writes the manual workbook

# merge what volunteers send back (dry-run first)
cd Scraper_v2 && python -m linux_scraper.merge_results ../Data/returns/<dir>/*.db --master ../Data/Step_5_master_reviews.db --dry-run
python Frame/probe_merge.py Data/returns/<dir>/probe_<machine>_<stamp>.db
```

## The pipeline

```
USDA SNAP · Overture 2026-08 · OpenStreetMap · project assets      91,461 records
  └─ Frame/s05 conflation → s06 rulings → s07 gates                36,820 stores
     └─ Frame/s10 scope cut (established bricks-and-mortar only)   16,751 candidates
        └─ Frame/probe.py on Google Maps (category, place_id,       16,751 probed
           live review count, open/closed)                          → rulings (next)
           └─ Remediation/r09 scrape queue → Deploy/build_kit.py review → volunteers
              └─ Scraper_v2 (place_id-verified, resumable, deduped on review_id)
                 └─ linux_scraper.merge_results → Data/Step_5_master_reviews.db
```

Every dropped store keeps a row and a reason; every review carries `source_db`,
`machine_id` and Google's `review_id`.

## Engineering notes — what turned out to be the real problems

**The frame decides everything downstream.** A store that never enters the candidate list
is invisible to every later measurement. The first frame retained 14–38 % of candidates
depending on ZIP prefix, worst in rural eastern NC — it would have reported food deserts
that were pipeline artifacts. Hence the four-source rebuild and the five validation gates.

**Verify by identity, not by name.** The original enrichment computed a name similarity,
logged it, and accepted on both branches. Sam's Club "Hendersonville" resolved to Raleigh
with 1,062 reviews. The scraper now navigates by `place_id` and rejects a page whose id
differs; the distance check reads the business pin, because after navigating to
`/@lat,lon` the map camera *is* the coordinate you asked for and the old check measured
nothing.

**A scroll that silently does nothing looks exactly like the end of the list.** The review
panel was chosen by first selector match; when that was not the panel, 60 empty cycles
later the store was "complete" at whatever the first screen held. The container is now
found by walking up from a real review node, and every scroll verifies `scrollTop` moved.

**Resumability is the whole game.** State lives in SQLite: stores flow
`pending → in_progress → completed`, interrupted stores reset on restart, reviews flush in
batches, and `reviews_scraped` is re-read from disk after every flush (an in-memory
counter once produced 2,263 phantom reviews).

**Two kinds of block, two responses.** A sign-in wall burns the *account* → rotate to the
next session. A CAPTCHA burns the *IP* → write `pause.flag`, sleep 30–60 min. One machine
per IP; two scrapers behind one address reach the threshold twice as fast for the same
throughput.

**Hand-copied kits drift.** Within four days the volunteers' copy had diverged from the
repo and a fix on one side never reached the other. Kits are now built by
`Deploy/build_kit.py` with a hashed manifest; the repo is the only source.

**A uniformly populated field is as suspect as a uniformly empty one.** `helpful_count`,
`photo_count`, `service_type` were "fixed" once by selectors that matched page furniture.
Counting what each selector matched on live pages (`Remediation/probe_review_dom.py`)
showed Google renders no such elements; the fields are unavailable, not zero.

**A quarantine that the merge honours can eat good data.** The wrong-page cleanup set aside
whole batches whose reviews outnumbered a store's live count, and the merge treats quarantined
rows as already known. A fresh scrape of the store's own page then came back as "duplicates" and
was dropped. 4,529 reviews across 25 stores were recovered by restoring only the rows that a
scrape of the store's own page returned again and no other store held (`Remediation/r17`).

**"Google ceiling" is a symptom, not a limit.** 190 stores stopped short after four sort orders
and were about to be labelled capped; one more pass on a fresh session returned 24,481 more
reviews. Retry before you label. What is left after repeated full passes that return nothing
new is the genuinely Google-limited set, ruled complete with the evidence in `status_reason`.

**WAL sidecars.** Copying a `.db` while committed pages sit in its `-wal` produces a copy
that is quietly missing the latest reviews. Every copy path checkpoints first or uses
`VACUUM INTO`.

## What failed, and why

`Archive/` holds the earlier architectures on purpose:

| attempt | why it died |
|---|---|
| Config-driven package (`gmaps_reviews_scraper/`) | Clean abstractions, wrong ones: a Google selector change touched three files. |
| Multiprocessing worker pool | SQLite contention, and process parallelism was never the bottleneck — Google's per-IP limit was. |
| Tkinter GUI for classmates | Shipped 25 student batches (+19,992 reviews) but leaked tabs, halted on one bad store, no checkpointing. Every complaint became a requirement. |
| Proxy rotation | Latency and launch hangs without fewer blocks; account rotation did the job. |
| Step 1.4 enrichment | Verifier inert — took Google's first result for every query. Replaced by `Frame/probe.py`. |
| Scraper v1 (name-search navigation) | No `place_id` navigation or verification; wrong-location scrapes. Replaced by v2. |
| Hand-copied deployment folders | Drifted from the repo in days. Replaced by `Deploy/build_kit.py`. |

## Data availability

`Data/Step_5_master_reviews_FROZEN_2026-10-24.db` (~640 MB, freeze v2) and the intermediate artifacts are research data
belonging to the project and are not in this repository. The repo contains the code and
the documentation needed to reproduce them; for data access, contact the research group.

## Acknowledgements

Built for **Teresa Groton**'s dissertation research on aspatial food access, advised by
**Prof. Benjamin Rachunok**, NC State ISE. Thanks to the ISE 408 students whose batch runs
contributed 19,992 reviews, and to the volunteers running the Forrester, Manabe and Holling
machines.
