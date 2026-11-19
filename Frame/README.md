# Step 1 — the North Carolina food retail frame

Built 2026-09-08. Artifact: `Data/Step_1_NC_Food_Retail_Frame.db`
SHA256 `c066b79fff1a1d2a2e48f92c7a61ac7d425bff8c558821b698784cfcd29cf1a3`

> **Status 2026-10-11:** the store list is final (owner ruling). The 4,447 stores in the frozen
> master (`Data/exports/NC_Grocery_POIs.xlsx`) are the list the study goes off; the probe results
> and rulings below are the history of how it was reached. See [`STATUS.md`](../STATUS.md).

**36,820 stores · 100 of 100 counties · 1,060 cities · 5/5 validation gates pass.**

This replaces the Overture-only candidate list that produced `Step_2.0_Final_List_of_Stores`.
It is a *frame*: the store universe the review scraping in Step 2 draws from. Getting it
wrong is not recoverable downstream, because a store that never enters the frame is
invisible to every measurement made afterwards.

---

## Why it was rebuilt

The previous frame was not merely incomplete, it was **unevenly** incomplete, and the
unevenness ran along the axis the research is about.

| | |
|---|---:|
| NC candidates in `Step_1.3` | 16,680 |
| Reached the old master | 5,226 |
| Retention on grocery-relevant candidates, ZIP3 276 (Rocky Mount / eastern) | 26.1% |
| Retention on the same, ZIP3 280 (Charlotte) | 62.5% |
| SNAP-authorised NC food retailers with **no** master store within 250 m | **3,737 of 8,926 (41.9%)** |
| …of which are SNAP-classified **Supermarkets** | 286 |
| Food Lion locations upstream vs in master | 854 → 330 (38.6%) |
| NC cities with an upstream Food Lion and none in master | 62 |

A food-desert study built on that frame would report food deserts in eastern and rural
North Carolina that are artifacts of the pipeline rather than facts about the state.
That is the defect that outranks every review-count problem.

Four root causes, all confirmed against the data rather than inferred:

**The source query was too narrow.** `Step_1.0` listed 21 Overture categories and did not
include `discount_store` or `department_store` as primaries. Overture NC currently holds
2,988 and 699 of those respectively. Dollar stores and supercentres only entered the old
frame when they happened to carry a grocery string in `categories.alternate`.

**Categories were dropped by lookup miss, not by decision.** The classification sheet ruled
on 424 of 573 observed Google categories; 162 were excluded by falling through. Google
labels Costco "Warehouse store" and Sam's Club "Warehouse club"; the sheet ruled the
second in and the first out, so North Carolina's nine Costcos were absent while Sam's Club
was present.

**Identity resolution was a scraper with its verifier disabled.** `Step_1.4`'s
`verify_business` computed a name similarity, logged it, and accepted on both branches, so
the program took Google's first search result for every query. The damage is visible in
the data: **323 Google `place_id`s in master and the exclusion audit are each claimed by
two or more genuinely different stores.** "Teresita Supermarket" and "Texoma Tackle &
Convenience Store" share one id. "El Torito" in Asheville shares one with "La placita II"
in Farmville, 452 km away.

**Sub-entities were counted as stores.** Overture lists "Walmart Pharmacy", "Walmart
Bakery" and "Walmart Grocery Pickup" as separate places at the same coordinates. Left
alone the frame reports 443 Walmarts in a state with roughly 200.

---

## What it is built from

Four independent sources, deliberately chosen so no single one can dictate coverage.
91,461 records in, 49,397 clusters out, 36,820 of them ruled into the frame.

| source | rows | why it is here |
|---|---:|---|
| **USDA FNS SNAP retailer locations** | 8,926 | The administrative spine. Every row is a retailer the federal government authorised to accept SNAP, so a human verified the address and the store sells food. The only source with a legal rather than commercial basis, and the frame the food-access literature expects. All 100 counties, 732 cities, zero missing coordinates. |
| **Overture Maps 2026-08-19.0** | 46,226 | Breadth. Seven months newer than the release the old frame used, and pulled on 58 ruled-in categories rather than 21. |
| **OpenStreetMap** (Overpass) | 6,920 | Edited by people who live there, so it carries the rural corner stores and tiendas that commercial aggregators under-collect — exactly the stores whose absence manufactures a food desert. |
| **Project assets** | 29,403 | The old master (5,226), the exclusion audit (7,497) and the January candidate list (16,680). An excluded store is a candidate with a recorded decision, not a deleted row, and 337 of those decisions were already known to be wrong. |

SNAP is authoritative for the **address**, Overture for the **name** (SNAP's strings are
administrative — `DollarTree 5360`, `FOOD LION  167`), master for the **`place_id`**.

## How records become stores

Conflation is tiered, strongest evidence first, and every tier is calibrated against pairs
whose answer is known.

| tier | rule | links |
|---|---|---:|
| T0 | identical `place_id` **and** name similarity ≥ 90 **and** ≤ 1 km apart | 360 |
| T1 | ≤ 100 m and name similarity ≥ 80 | 70,677 |
| T2 | ≤ 250 m and name similarity ≥ 90 | 4,904 |
| T3 | same normalised name, house number and ZIP, ≤ 25 km | 844 |
| T4 | department name absorbed into a co-located parent of the same chain | 198 |

**T0 does not trust a `place_id` on its own**, which is the single most important choice in
this build. Calibration is clean: every correctly shared id scores 100 and sits under
100 m; every crossed id scores 86 or less or is hundreds of km away. 348 pairs were
refused and written to `build/placeid_collisions.csv` for adjudication rather than merged.

T4 knows a department from a format: `Pharmacy`, `Bakery`, `Deli`, `Grocery Pickup` are
absorbed; `Supercenter`, `Neighborhood Market` and `Express` are separate stores. 188
department-named rows (0.51%) still lack a co-located parent and are listed in
`reports/residual_subentities.csv`.

Result: **zero clusters span more than 25 km**, and 86% of multi-record clusters span
under 100 m.

## How stores are classified

`category_ruleset.csv` — 295 hand rulings across four vocabularies — plus
`category_ruleset_alt.csv`, 553 generated rulings for categories that reach the pull only
through a food string in `categories.alternate` (a furniture store with an incidental food
alternate is not a food retailer; ruled OUT as a class, with the class recorded in each
row's rationale).

Every ruling carries a tier, a `food_access_type` and a written reason. **A category string
with no ruling is a hard, reported condition, never a silent drop** — that is the whole
point of the file. `NO_CATEGORY` is kept distinct from `UNRULED`: the first means no source
supplied a category and only a person can decide (2 stores); the second means a string
exists and nobody ruled on it (0 stores).

| tier | stores | meaning |
|---|---:|---|
| CORE | 17,501 | full-line food retail: supermarket, grocery, convenience, dollar, supercentre, warehouse club |
| FRESH | 2,481 | farmers' markets, produce, butchers, seafood, farm-direct |
| SPECIALTY | 1,376 | ethnic, health and specialty grocery |
| MARGINAL | 15,462 | sells food but is not primarily a food retailer: gas stations, pharmacies, farms |

Scope was widened on purpose, per the research decision to tag rather than drop: farmers'
markets, produce stands, butchers and fishmongers are in the frame with a type tag, so
**excluding them is an analysis filter rather than data the pipeline destroyed**. Scraping
is staged by `scrape_priority` (P1 CORE, P2 FRESH+SPECIALTY, P3 MARGINAL) so Step 2 does
not have to wait on the widened scope.

## Step 1b — the Google probe

The frame is store *candidates*; the probe converts them to candidates with verified place_ids.

Running (assumes repo root context):
```
python Frame/probe.py --init              # create probe.db, 16,751 stores (CORE+FRESH+SPECIALTY)
python Frame/probe.py --run               # run headless Chromium, verify & collect place_ids
python Frame/probe.py --stats             # completion summary
python Frame/probe.py --machine holling   # filter results by machine_id
```

Steps s09–s15 post-process probe results:
- `s09_geomatch_dedupe.py` — dedup by geomatch distance
- `s10_select.py` — filter to established bricks-and-mortar food retail (research group decision, recorded in docstring)
- `s11_*.py`, `s12_*.py`, `s13_*.py` — category mapping, place_id backfill, external enrichment (dataaxle, etc.)
- `s14_*.py` — final selection and export (see docstrings)
- `s15_poi_decision_sheet.py` — one row per store with everything known (frame, probe, master,
  Dewey SafeGraph Places in `dewy data/`, Dr. Ben's SafeGraph Patterns in `Dr.ben_data/`) →
  `Data/exports/POI_Decision_Sheet_<date>.xlsx`. `--match` (geomatch + proximity, ~2 h, cached in
  `build/s15_match_*.csv`), `--proximity-only` (redo pass 2, ~1 min), `--export`.
- `s16_dewey_report.py` — every Dewey POI with its categories and whether master holds reviews
  for it (reuses the s15 join) → `Data/exports/Dewey_Overlap_Report_<date>.xlsx`.
- `s17_dewey_catalog.py` — all 740,732 NC Dewey POIs (every category), every unique top/sub
  category with a blank `is_grocery` ruling column, and each master store linked directly to a POI
  (250 m, name + house number, graded A–D) with its review status → `dewy data/catalog/`.
- `s18_poi_rulings.py` — `prep` splits the food-related POIs into chains (a store name with more
  than five POIs) and independents per subcategory, buckets every item as *maybe* or *ruled out*
  (9 Sep scope decision) → `catalog/ruling_page_data.json` + `ruling_page_pois.json`, which the
  "Grocery Scope Rulings" artifact page loads. `apply <docs.json>` takes the page's saved rulings and
  writes `catalog/Final_POI_List.xlsx` + `final_poi_list.parquet`. Nothing is in without an explicit
  In; ruled-out items stay out unless flipped; undecided POIs are listed, never included.
- `s19_grocery_db.py` — the grocery database keyed on Dewey POIs, `Data/Step_6_grocery_pois.db`
  (`pois` + `reviews` by placekey; master read-only). `build` links each POI to the master store it
  overlaps and copies that store's reviews; `sitrep` → `Data/reports/grocery_sitrep_<date>.xlsx`;
  `batches --n 2` writes untouched POIs to `Data/batches/grocery_new<i>.db` (with `dewey_placekey`,
  probable duplicates held back); `merge <batch.db>` brings returned batches back by placekey.
  Returned grocery_new batches merge with s19, **not** `merge_results` (their stores are not in master).

`Frame/probe_merge.py` merges results from multiple machines:
```
python Frame/probe_merge.py Data/returns/<dir>/probe_X.db  # merge returned probe results into probe.db
```

Distributing the probe to another machine (see `Deploy/README.md`):
```
python Deploy/build_kit.py probe --with-auth  # assemble kit, verifies sha256
```

Probe results (as of 2026-09-16): 15,350 done · 767 unverified · 634 failed (633 retryable browser crashes). 11,791 of 16,751 carry Google place_ids; 14,794 name-verified; 902 permanently closed. Top Google categories: Gas station 3,448 · Convenience store 3,234 · Grocery store 2,196 · Dollar store 1,942. Machines: HOLLING 13,225 · local1 3,521. Queue drained (all stores attempted).

Dependency: `geomatch` module (project-local, installed with `pip install -e C:/Users/.../matcher`; documented in root requirements.txt).

## Validation

`s07_validate.py` exits non-zero while the frame is not authoritative. All five pass.

| gate | result |
|---|---|
| **G1** every January candidate has a disposition | 16,680 → 15,568 clusters, **0 with no record** |
| **G2** estimated coverage of the true population | Lincoln-Petersen from SNAP × Overture overlap: **17,760** CORE stores estimated, 17,501 in frame — **98.5%** |
| **G3** evidence quality is flat across the state | CORE stores corroborated by 2+ independent sources: **64.6%–76.9% by ZIP3, ratio 1.19×** |
| **G4** no chain is missing | 28 chains, **0 below 95%** of their SNAP count |
| **G5** every store ruled by a decision | **0 UNRULED**, 0 fallthroughs |

Two of these gates were rewritten after they first passed, because they were circular.
"What share of SNAP is in the frame" returns 100% by construction once SNAP is one of the
frame's own sources, and it measures nothing. G2 now estimates the *unobserved* population
by capture-recapture, and reports the result honestly as an **upper bound** on coverage —
SNAP and Overture are positively dependent, which biases the population estimate downward.
G3 now measures independent corroboration, which can genuinely vary by region, instead of
a quantity that is flat by definition.

Against the old frame's **2.4× regional spread**, corroboration varies by **1.19×**.

## What changed

| | frame | old master |
|---|---:|---:|
| stores | **36,820** | 5,226 |
| CORE food retail | **17,501** | — |
| cities | **1,060** | 585 |
| counties | **100** | — |

31,630 stores are new to the project. Of those, 6,506 sit in the exclusion audit with a
recorded reason (5,490 `CATEGORY_NON_GROCERY`), 3,506 were in the January pull and never
reached any disposition at all, and **21,618 were found only by the new sources**.

Chain recovery, frame vs old master: Dollar General 698 → 1,253 · Food Lion 328 → 549 ·
Family Dollar 148 → 531 · Dollar Tree 22 → 412 · Harris Teeter 34 → 199 · Lowes Foods
54 → 101 · Costco 0 → 10.

## Running it

```
python Frame/s01_snap.py            # USDA FNS SNAP retailers, NC
python Frame/s02_overture.py        # Overture 2026-08-19.0, NC, ruled-in categories
python Frame/s03_osm.py             # OpenStreetMap via Overpass
python Frame/s04_existing.py        # master + exclusion audit + January candidates
python Frame/build_ruleset.py       # hand rulings, 4 vocabularies
python Frame/s05_conflate.py        # records -> physical stores
python Frame/s06_rule.py            # apply rulings, assemble the frame
python Frame/build_ruleset_alt.py   # rule anything that came back UNRULED
python Frame/s06_rule.py            # re-run until UNRULED is 0
python Frame/s07_validate.py        # the five gates
python Frame/s08_freeze.py          # SQLite + SHA256 + manifest
```

`s07` and every `build_*` script are read-only with respect to project data. `s08` is the
only writer, and it writes a new file rather than modifying anything.

## Known limitations

- **188 residual chain sub-entities** (0.51%) whose parent was not within 250 m.
  `reports/residual_subentities.csv`.
- **348 refused `place_id` collisions** are recorded, not resolved. Each needs a person to
  decide which of the two stores owns the id. `build/placeid_collisions.csv`.
- **2 stores carry no category from any source** and need a manual ruling.
  `reports/no_category_worklist.csv`.
- **After the probe (2026-09-10 to 2026-09-15), 11,791 of 16,751 stores carry a Google `place_id`** (70.4%). Remaining 4,960 (unverified, failed, or rejected by filters) await adjudication or re-query.
- **G2's 98.5% is an upper bound**, not a measurement. Capture-recapture on two positively
  dependent sources understates the population.
- The frame records **existence, not operating status**. Overture `confidence` and SNAP
  authorisation dates are carried but no closure check has been run.
