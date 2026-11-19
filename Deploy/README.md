# Deploy — building and receiving volunteer kits

Two kits go out to other machines: the **review kit** (Linux boxes such as
`forrester`/`manabe` scraping reviews) and the **probe kit** (a Windows box such as
`holling` looking stores up on Google Maps). Both are **built** here from the canonical
code and shipped as a folder. Nothing in a built kit is ever edited by hand: fix the
repository, rebuild, ship again.

Why: the first kits were hand-copied folders. Within four days the copy on the
volunteers' machines had diverged from the repository (`patch_batch_path.py` was the
symptom), and a fix made on one side never reached the other.

## Build

```bash
# review kit: the two scrape queues, plus the Google sessions
python Deploy/build_kit.py review --batches Data/batches/run1.db Data/batches/run2.db --with-auth

# probe kit: a fresh snapshot of Frame/build/probe.db with any stale claims released
python Deploy/build_kit.py probe --with-auth

# rebuild over an existing output
python Deploy/build_kit.py review ... --force
```

Output lands in `Deploy/build/<kit>/` (gitignored). Every file is listed with its SHA-256
in `KIT_MANIFEST.json`, together with the git commit it was built from.

`--with-auth` copies `Auth/google_auth_*.json` into the kit. Those are live Google
session cookies. Without the flag no secret is ever copied. Banned sessions
(`*.json.banned`) are never copied.

What goes in each kit:

| kit | code (from) | templates (from) | data |
|---|---|---|---|
| `review_kit` | `Scraper_v2/linux_scraper/`, `Scraper_v2/run_continuous.py` | `Deploy/review_kit/` | `--batches` snapshots into `batches/` |
| `probe_kit` | `Scraper_v2/linux_scraper/`, `Frame/probe.py` (repo layout kept, `probe.py` resolves `../Scraper_v2` and `../Auth`) | `Deploy/probe_kit/` | `Frame/build/probe.db` snapshot into `Frame/build/` |

Runtime junk (`__pycache__`, logs, `machine_id.txt`, `pause.flag`, debug screenshots) is
never copied. Database snapshots use `VACUUM INTO`, so a queue mid-write on this
machine still ships as a consistent file with no `-wal` sidecar to forget.

## Ship

Copy the built folder to the machine (scp, USB, share). The kit's own `README.md` is
the volunteer's instructions: `setup.sh` / `setup.bat` once, then `run_machine1.sh` or
`START.bat`. `run_machineN.sh` runs the batch in `batches/` whose filename ends in `N`
(`grocery1.db`, `dewey21.db`); for any other name run `./_run.sh <N> <batch>.db`
(`./_run.sh 1 dewey25.db`). Kits built without `--with-auth` are fine for machines that already
hold working logins; the files in `Auth/` are PC-bound (see STATUS.md §2.2) and sign out on a box.

## Receive

Volunteers run `collect_results.sh` / `collect.bat`, which checkpoints the database and
writes a copy named for their machine into `results/`. Put what comes back under
`Data/returns/<YYYYMMDD>_<machine>/`, then merge:

```bash
# reviews (review kit)
cd Scraper_v2
python -m linux_scraper.merge_results ../Data/returns/20260916_forrester/*.db --master ../Data/Step_5_master_reviews.db --dry-run
python -m linux_scraper.merge_results ../Data/returns/20260916_forrester/*.db --master ../Data/Step_5_master_reviews.db

# probe results (probe kit)
python Frame/probe_merge.py Data/returns/20260915_holling/probe_HOLLING_20260915_1348.db
```

Both merges are idempotent and dedupe on content; the review merge backs master up to
`Data/backups/` first, takes a lock, and verifies the row count after inserting.

Pass the result file by name (a glob such as `dewey26_*.db` also matches interim copies and
double-counts). The merge treats reviews in `quarantined_reviews` as already known, so a store whose
reviews were set aside earlier can come back with far fewer new reviews than it scraped; run
`python Remediation/r17_restore_confirmed.py` after merging (it restores only rows the store's own
page returned again), then `python Remediation/r15_tolerance.py`.

## New Dewey stores (`grocery_new<i>.db`)

The batches written by `python Frame/s19_grocery_db.py batches --n 2` hold POIs from the
Dewey grocery list that no machine has scraped. They carry no Google place_id: the
scraper searches each by name at its coordinates, refuses a page whose name does not
match, and records the place_id it lands on. Each row carries `dewey_placekey`.

```bash
# 1. build the kit (the scraper and its sort ladder ship with it)
python Deploy/build_kit.py review --batches Data/batches/grocery_new1.db Data/batches/grocery_new2.db --with-auth
# 2. ship Deploy/build/review_kit/ to two machines; each runs its own batch (1 or 2), one per IP
# 3. collect as usual (collect_results.sh / collect.bat) into Data/returns/<date>_<machine>/
# 4. merge into the grocery database by placekey - NOT merge_results (these stores are not in master)
python Frame/s19_grocery_db.py merge Data/returns/<date>_<machine>/grocery_new1_*.db
python Frame/s19_grocery_db.py sitrep
```

The s19 merge backs up `Data/Step_6_grocery_pois.db` first, is safe to repeat, and takes
Google's live review count as the store's target.

## Tests

```bash
python -m pytest Deploy/tests -q
```

`test_build_kit.py` builds both kits from a fabricated mini-repo and checks contents,
exclusions, secret handling, WAL-safe snapshots, the manifest hashes and that the
review kit's package imports.

## Review cap per batch

Batches built before 2026-10-12 stop at 1,500 reviews per store. A batch built with `r09 --cap 0` carries
`review_cap=0` in its metadata and the scraper collects up to Google's full count (up to 8 hours per store). A missing or
unreadable value means 1,500, never unlimited. The kit's scraper code supports both; no change is needed on the machines.
