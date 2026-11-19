# CLAUDE.md — working in this repo

Read **`STATUS.md`** before doing anything. It holds the current state, where every file
and database is, every decision that still binds, and the step-by-step plan. Component
detail: `Frame/README.md` (store universe + Google probe), `Remediation/README.md` (audit
and repair of the master DB), `Deploy/README.md` (volunteer kits), `Data/README.md`
(lineage, schema, quality history).

## Hard rules

- **Never commit, tag or push.** This is a local project; the owner commits. Leave work in
  the working tree.
- **Never touch `Auth/`** except to read it for a kit build. `google_auth*.json` are live
  Google sessions and are gitignored everywhere.
- **Never delete rows from `Data/Step_5_master_reviews.db` to fix scope** — set a status and
  record a reason, or move them to a side database (`Step_5_out_of_scope`, `_closed_stores`,
  `_excluded_stores`). Nothing is dropped; set-aside reviews go to `quarantined_reviews`.
- **Freeze v2 is the final dataset** (`Data/Step_5_master_reviews_FROZEN_2026-10-24.db`, SHA in
  `Remediation/master_freeze_manifest_v2.json`; v1 of 2026-10-11 deleted at the Phase 1 hand-over, 2026-11-19). Read it; do not write to it. Scraping is
  closed (owner, 2026-10-24). Re-freeze needs a new copy, a new manifest and an updated `STATUS.md`.
- **If something is missing, check `Archive/DELETED_2026-10-24.md` first.** The handoff cleanup deleted old
  backups, `Deploy/build/` and the machine-pull leftovers. `Dr.ben_data/` and `dewy data/` moved to
  `../Gmaps Scraper - external data/` (env `GMAPS_EXTERNAL_DATA`).
- **Don't run `r03_clean_deterministic.py --apply` on this master**: its status step predates `r15` and would
  reset about 1,300 statuses.
- **Completion rule (owner, 2026-10-24, final):** completion is measured against the target capped at 1,500 (stored targets stay Google's true counts); a store is complete if it is missing 10 or fewer (target 100 or under), 20 or fewer (over 100), 50 or fewer (over 500) or 100 or fewer (over 1,500). `r13.TOLERANCE_TIERS`, applied by `r15`.
- **Delete nothing, decide nothing the data cannot decide:** over-target stores are rescraped and compared (`r18`); whatever cannot be decided with accuracy goes to a manual workbook. Every writer backs up to `Data/backups/` first and is idempotent.
- **Dry-run every merge first** (`--dry-run`), and checkpoint the WAL before copying any
  SQLite file (or use `VACUUM INTO`).
- **One copy of every piece of code.** Kits under `Deploy/build/` are outputs of
  `Deploy/build_kit.py`; fix the canonical file, add a test, rebuild.
- **Tests first.** `python -m pytest -q` from the repo root must stay green. New Python gets
  a test in the component's `tests/` folder.

## Layout in one breath

`Frame/` = Step 1 (store list, probe) · `Scraper_v2/` = Step 2 (review scraper, package
`linux_scraper`) · `Remediation/` = r00–r17 audit/repair/rulings + manual-check builders · `Deploy/` = kit builder +
templates · `Data/` = gitignored data + `README.md` · `Archive/` = dead code, imported by
nothing.

## Commands you will actually use

```bash
python -m pytest -q
python Frame/probe.py --stats
python Remediation/r00_validate.py --fail-on HIGH      # exit 0 = authoritative
python Remediation/r15_tolerance.py                    # dry run; --apply writes (all writers dry-run first)
python Remediation/r16_round4_answers.py <done.xlsx>   # apply a completed manual-check workbook
python Remediation/r17_restore_confirmed.py            # restore reviews the store's own page confirms
python Remediation/r18_compare_rescrape.py             # compare over-target stores with a full rescrape (moves nothing)
python Remediation/make_manual_round5.py               # build a manual-check workbook ("send to manual")
python Deploy/build_kit.py review --batches Data/batches/run1.db Data/batches/run2.db --with-auth
python Deploy/build_kit.py probe --with-auth
cd Scraper_v2 && python -m linux_scraper.merge_results ../Data/returns/<dir>/*.db --master ../Data/Step_5_master_reviews.db --dry-run
python Frame/probe_merge.py Data/returns/<dir>/probe_<machine>_<stamp>.db
```

Scripts run from the repo root, except `merge_results`, which runs from `Scraper_v2/`.
`geomatch` (used by `Frame/s09`, `s10`, `s13`) is a sibling checkout — see
`requirements.txt`.

## Machines

Volunteer boxes: `forrester.ie.ncsu.edu` (`sl9mw2` / `9b6a55`), `manabe.ie.ncsu.edu`
(`alodov` / `ce281e`), `HOLLING` (Windows, ran the probe). Scraping is closed (2026-10-24): forrester ran dewey27/28/30a and manabe ran dewey30b, all merged. The manabe box ran a hand-patched fork of the scraper (`Archive/manabe_box_code_20261024/`); `Scraper_v2/` stays canonical. One scraper per IP. "Send to manual" means build an Excel workbook
of everything the owner must look up (`Remediation/make_manual_round*.py`).
