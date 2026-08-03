# NC Grocery Store Reviews — Data Pipeline & Scraper

Data infrastructure for research on **aspatial food access**: measuring food deserts through the
lived *experience* of grocery shopping rather than distance-to-store alone.

Most food desert measures are geographic — how far you live from a supermarket. The research this
repo supports (NC State ISE, Prof. Benjamin Rachunok's group) instead operationalizes the
**Penchansky & Thomas five dimensions of access** — availability, accessibility, accommodation,
affordability, acceptability — by mining what people actually write in Google Maps reviews of
grocery stores. That approach was demonstrated for Raleigh. **This repo is the pipeline that scales
it to all of North Carolina.**

**Result: 4,889 stores · 1,201,237 reviews · 578 NC cities · 13 grocery categories.**

---

## What's in here

| Path | What it is |
|---|---|
| [`Data/`](Data/) | Pipeline documentation, data dictionary, and quality/limitations reports (data files themselves are not in git — see below) |
| [`Step_1.4_EnrichOverture__Program/`](Step_1.4_EnrichOverture__Program/) | Stage 1: resolves raw Overture Maps POIs to Google Maps `place_id`s |
| [`Scraper_v2/`](Scraper_v2/) | **Current** review scraper — headless Playwright, Linux/VM, continuous orchestration |
| [`Scraper_v1/`](Scraper_v1/) | Previous VM scraper generation, kept for provenance |
| [`Gmaps_Scraper_MacWindows/`](Gmaps_Scraper_MacWindows/) | Tkinter GUI build — one-double-click distribution so non-technical classmates could run batches on their own laptops |
| [`Archive/`](Archive/) | Earlier architectures that didn't survive contact with production. Kept deliberately — see [What failed](#what-failed-and-why) |

Start with [`Data/PROJECT_OVERVIEW.md`](Data/PROJECT_OVERVIEW.md), then
[`Data/Data Collection Process.md`](Data/Data%20Collection%20Process.md).

---

## The pipeline

```
Overture Maps US POIs                     451,296
  └─ NC filter                             16,680
     └─ GMaps enrichment (Step 1.4)         7,858 matched + 6,607 unmatched
        └─ Outscraper second pass          21,840 results
           └─ Fuzzy match + manual review   4,905 matched / 1,605 rejected
              └─ Category filter            424 GMaps categories → 13 in, 411 out
                 └─ Closed-store + place_id dedup
                    └─ Review scraping      1,201,237 reviews
                       └─ Step_5_master_reviews.db
```

Every dropped store is retained with a reason (`CATEGORY_NON_GROCERY`, `MANUAL_REVIEW_UNMATCHED`,
`CLOSED_STORE`) in a 7,497-row exclusion audit — a filtered-out store is a research finding, not
a deleted row. Every review carries a `source_db` provenance tag.

---

## Running the scraper

```bash
# One-time setup on a Linux VM
bash Scraper_v2/linux_scraper/setup.sh

# Continuous run: scrape → restart on crash → drain the batch
python Scraper_v2/run_continuous.py --batch batch1.db

# Monitor
python -m linux_scraper stats batch1.db --monitor
```

Full VM deployment guide: [`Scraper_v2/linux_scraper/VM_INSTRUCTIONS.md`](Scraper_v2/linux_scraper/VM_INSTRUCTIONS.md).

You supply your own Google auth sessions as Playwright `storage_state` JSON. **They are
gitignored and must never be committed** — they are live session cookies.

---

## Engineering notes

Things that turned out to be the actual problem, not the ones I expected:

**Resumability is the whole game.** An 8-hour run that loses everything to a Wi-Fi blip is worth
zero. State lives in SQLite, not memory: stores flow `pending → in_progress → completed/failed`,
interrupted stores auto-reset on restart, reviews flush to disk in batches, and each store persists
a resume marker *including which sort order it was mid-way through* so a restart re-enters the same
review ordering instead of starting over.

**`reviews_scraped` must be read back from disk.** Incrementing an in-memory counter and trusting
it produced 2,263 phantom reviews in batch1. The count is now re-read from the `reviews` table
after every flush, so the store's progress can never drift from what was actually persisted.

**Scroll stalls aren't the end of the list.** Google Maps stops yielding new reviews long before
you've seen them all. When a scroll stalls, the scraper re-sorts (`newest → oldest → relevant`) —
different sort orders surface different subsets of the same review set — and keeps collecting.

**Deduping needs two layers.** Within a run, Google's `data-review-id`. Across the ~49 batch DBs
that merged into master, `(store_id, reviewer_name, rating, review_text[:50])` — because review IDs
aren't stable across sessions. A master skip-list also lets a resumed store early-exit after 60
consecutive already-known reviews instead of scrolling a whole page of duplicates.

**Chain stores silently poison your data.** "Dollar General" matches at the wrong location and
scrapes 1,051 reviews for a store whose target was 77. `verify_store()` now cross-checks the page
name against a haversine distance from the expected coordinates, rejecting a name-match that's
>10 km off. Not a hard distance reject, though — Overture coordinates are routinely 1–2 km off
Google's even for the *correct* store, and a strict threshold wrongly rejected ~25 valid stores.

**Blocks have two different shapes, needing two different responses.** A sign-in wall means *this
account* is burned → ban it, rotate to the next in the pool. A CAPTCHA means *this IP* is burned →
write a global `pause.flag` and sleep 30–60 min. Treating both the same wastes accounts.

**Detection checks must be cheap.** CAPTCHA detection reads `page.url` + `page.title()`, never
`page.content()` — a full DOM fetch on every scroll cycle caused memory pressure that degraded
long runs.

---

## What failed, and why

`Archive/` holds four earlier architectures. They're in the repo on purpose:

| Attempt | Why it died |
|---|---|
| Config-driven package (`gmaps_reviews_scraper/`) | Clean abstractions, wrong ones. Selector changes on Google's side required edits across `selectors.py`, `models.py`, and `scraper.py`. |
| Multiprocessing worker pool | SQLite contention. Fixed with WAL + `BEGIN IMMEDIATE` store claiming, but process-level parallelism was never the bottleneck — Google's rate limits were. |
| Tkinter GUI distributed to classmates | Worked, and shipped 25 student batch DBs (+19,992 reviews). But it leaked browser tabs across 8-hour runs, halted the whole batch on one bad store, and had no checkpointing. Every one of those complaints became a design requirement for v1/v2. See [`Archive/Step2_Review_Scraper_Program/Fixes.txt`](Archive/Step2_Review_Scraper_Program/Fixes.txt) — the actual user feedback. |
| Proxy rotation | Removed in v2. Added latency and launch hangs without meaningfully reducing blocks; account rotation did the job. |

---

## Known limitations

Documented honestly in [`Data/Data Quality Report.md`](Data/Data%20Quality%20Report.md) rather than
buried. The headline ones:

- **The text corpus is ~608K reviews, not 1.20M** — 49.4% of Google Maps reviews are rating-only.
- **157 over-scraped stores** from the wrong-location bug predate the geo guardrail and still need reconciliation.
- **Costco is missing entirely** (NC has 9); Harris Teeter and Ingles are undercounted — likely dropped at the category filter.
- **`helpful_count`, `photo_count`, `service_type` are unusable** (100% zero/NULL — never extracted).
- **`date_relative` is Google's natural-language string** ("2 years ago"); no absolute dates parsed yet.
- **`reviewer_name` is not a key** — "Chris" reviews 240 distinct stores.

The path from "usable" to "authoritative" is itemized and effort-estimated in
[`Data/Future_Work.md`](Data/Future_Work.md).

---

## Data availability

The dataset (`Step_5_master_reviews.db`, ~214 MB, plus intermediate Excel/CSV artifacts) is not in
this repository — too large for git, and it's research data belonging to the project. The repo
contains the code and the full documentation needed to reproduce it. For data access, contact the
research group.

## Acknowledgements

Built for **Teresa Gorton**'s dissertation research on aspatial food access, advised by
**Prof. Benjamin Rachunok**, NC State ISE. Thanks to the ISE 408 students whose batch runs
contributed 19,992 reviews and whose bug reports made the scraper survivable.
