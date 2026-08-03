# Status

**Snapshot:** 2026-04-30 · `Step_5_master_reviews.db`

## Headline

| Metric | Value |
|---|---:|
| Stores | 4,889 |
| Reviews | 1,201,237 |
| Stores with target>0 | 4,732 |
| **Completion (capped fill-rate)** | **84.3%** (1,161,013 / 1,376,470) |
| Stores 100% complete | 3,323 (70.2%) |
| Stores in progress | 1,364 (28.8%) |
| Stores not started | 45 (1.0%) |
| Stores with no target | 157 (excluded from math) |
| **Reviews still to scrape** | **215,457** across 1,409 stores |

## Latest merges

| Date | Source | Reviews added |
|---|---|---:|
| 2026-04-30 | `batch_forrester_20260430.db` (machine `rwgugp`) | +29,431 |
| 2026-04-30 | `batch_manabe_20260430.db` (machine `h519tf`) | +30,780 |
| 2026-04-28 | `batch_forrester.db` (initial) | +25,498 |
| 2026-04-28 | `batch_manabe.db` (initial) | +83,341 |

Latest backup: `Step_5_master_reviews_backup_20260430_183056.db` (pre-2026-04-30 merge).

## Remaining work — by bucket

| Reviews remaining (per store) | Stores | Reviews |
|---|---:|---:|
| 1–50 | 682 | 8,287 |
| 51–100 | 162 | 12,195 |
| 101–250 | 232 | 36,622 |
| 251–500 | 284 | 100,626 |
| 501–1000 | 12 | 7,165 |
| 1000+ | 37 | 50,562 |

Most of the work concentrates in the **251–500** bucket (284 stores, ~100k reviews).

## Remaining work — by category

| Category | Stores incomplete | Reviews remaining |
|---|---:|---:|
| Grocery store | 683 | 120,275 |
| Supermarket | 143 | 33,615 |
| Convenience store | 97 | 24,418 |
| Department store | 71 | 10,858 |
| (no category) | 6 | 8,184 |
| Dollar store | 319 | 7,897 |
| Asian grocery store | 17 | 2,142 |
| Mexican grocery store | 15 | 2,005 |
| Warehouse club | 7 | 1,960 |
| Indian grocery store | 11 | 1,894 |
| General store | 18 | 1,484 |
| Discount store | 8 | 465 |
| Discount supermarket | 5 | 157 |
| Health food store | 9 | 103 |

## Top 10 stores by reviews remaining

| Need | Category | Name |
|---:|---|---|
| 1,500 | Supermarket | Walmart Grocery Pickup (0/1500) |
| 1,498 | Grocery store | Timeout Market (2/1500) |
| 1,497 | Convenience store | Kangaroo Express (3/1500) |
| 1,495 | Convenience store | Kangaroo Express (5/1500) |
| 1,491 | Convenience store | New Salem General Store (9/1500) |
| 1,488 | Convenience store | ShopRite (12/1500) |
| 1,483 | (none) | Family Fare (17/1500) |
| 1,481 | Dollar store | Dollar General (19/1500) |
| 1,480 | Mexican grocery store | Tienda Garcia (20/1500) |
| 1,474 | Grocery store | Community Market (26/1500) |

## How to run the next scrape cycle

From `Code_v2/`, on each machine:

```
python run_continuous.py --batch batch_forrester.db   # machine "rwgugp"
python run_continuous.py --batch batch_manabe.db      # machine "h519tf"
```

`run_continuous.py` automatically scrapes → merges to master → regenerates a fresh batch from remaining work → loops.

To rebuild the two batch files from current master state (sorted by reviews-needed DESC, cost-balanced 50/50): run the batch generator from this folder. Each batch will have ~750 stores and ~108K reviews of work (~83 hours single-machine).

## Estimated time to completion

At ~60K reviews per cycle across both machines (observed in the 2026-04-30 run), and **215,457 reviews remaining**, the dataset reaches authoritative-completion-target (≥99% of targets) in **~3–4 more parallel cycles**.

Day-level estimate at sustained two-machine throughput: ~1 week of scraping wall-clock to clear the queue, modulo Google rate limits and over-scrape guardrails.
