# NC Grocery Reviews Dataset — Project Overview

**Purpose:** Build an authoritative dataset of every grocery-relevant store in North Carolina, enriched with Google Maps metadata and customer reviews, for downstream research on consumer behavior, food access, and store-level analysis.

## Goals

1. **Inventory** — every operating grocery-relevant store in NC (13 included GMaps categories: Convenience store, Grocery store, Dollar store, Supermarket, Department store, Mexican grocery store, General store, Discount store, Asian grocery store, Health food store, Warehouse club, Indian grocery store, Discount supermarket).
2. **Enrich** — canonical name, address, GMaps `place_id`, rating, category, and operating status for each store.
3. **Reviews** — collect up to 1,500 customer reviews per store with text, rating, reviewer profile metadata, and date.
4. **Audit** — full provenance for every store (kept vs. excluded, with reason) and every review (source batch DB).
5. **Reproducibility** — one master SQLite DB as the source of truth, plus a documented pipeline that can be re-run end-to-end.

## Source of Truth

`Step_5_master_reviews.db` — single SQLite database containing the canonical `stores` and `reviews` tables.

## Doc Map

| Doc | Read this for |
|---|---|
| [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) | Goals, scope, doc map (you're here) |
| [Data Collection Process.md](Data%20Collection%20Process.md) | Step-by-step pipeline that produced the dataset |
| [Data Quality Report.md](Data%20Quality%20Report.md) | Known data quality issues and what's been fixed |
| [Status.md](Status.md) | Current scraping status and remaining work |
| [Future_Work.md](Future_Work.md) | Roadmap from "usable" → "authoritative" |
| [Step_1.9_Excluded_Stores_Analysis.md](Step_1.9_Excluded_Stores_Analysis.md) | Specific analysis of the 7,497 excluded stores |

## Snapshot (2026-04-30)

| Metric | Value |
|---|---:|
| Stores | 4,889 |
| Reviews | 1,201,237 |
| NC cities covered | 578 |
| Categories | 13 grocery-relevant |
| Completion (capped fill-rate) | 84.3% |
| Stores 100% complete | 3,323 |
| Reviews still to scrape | 215,457 |

## Scope Boundaries

- **Geography:** North Carolina only.
- **Time:** Reviews are point-in-time scrapes from 2026-03 through 2026-04. Google's `date_relative` strings preserved as-is; no absolute dates parsed yet.
- **Out of scope:** Restaurants, bakeries, butcher shops, liquor stores, gas stations, drug stores, farmers' markets, pharmacies (excluded per the Step 1.8 category classification).
