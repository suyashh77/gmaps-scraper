# Google Maps Review Scraper

Config-driven scraper for Google Maps business reviews: SQLite storage, single- or multi-worker runs, and optional query-only mode.

## Setup

```bash
python -m venv venv
venv\Scripts\activate   # Windows
pip install -r requirements.txt
playwright install chromium
```

Copy `.env.example` to `.env` if you want to override config via environment variables.

## Configuration

- **config.yaml** – Main config (browser, workers, DB path, input file, scraping/rate limits, logging).
- **.env** – Optional overrides: `DATABASE_PATH`, `LOG_LEVEL`, `HEADLESS`, `CONFIG_PATH`.

Input: Excel or CSV with store list. Supported column names (any of):  
`input_name`, `business_name`, `name`; `input_street`, `street`, `address`; `input_city`, `city`; `input_state`, `state`; `input_zip`, `zip`; `input_lat`/`input_lon` or `lat`/`lon`; `google_place_id`, `place_id`; and optional `gmaps_*` fields.

## Commands

All commands (except `run_workers.py`) go through the single CLI:

```bash
python run.py --config config.yaml <command> [options]
```

| Command | Description |
|--------|-------------|
| **run** | Single-process scraper: processes all pending stores from the DB. |
| **workers** | Multi-worker scraper: N processes (from config) claim stores from the DB. |
| **query** \<query\> | Scrape one place by search query; optional `--json-out file.json`, `--max-reviews N`. |
| **init** [file] | Create DB schema and optionally import stores from Excel/CSV (default from config). |
| **stats** | Show DB stats; `--failed`, `--sessions`, `--monitor` (refresh every 60s). |
| **merge** \<out.db\> \<db1.db\> [db2.db ...] | Merge multiple DBs into one. |
| **split** \<file\> \<N\> | Split input file into N parts (e.g. for parallel worker inputs). |

Multi-worker only (standalone script):

```bash
python run_workers.py [--config config.yaml]
```

## Workflow

1. **Init DB and import stores**  
   `python run.py init stores.xlsx`

2. **Run scraper**  
   - One process: `python run.py run`  
   - Multiple processes: `python run.py workers` or `python run_workers.py`

3. **Inspect**  
   `python run.py stats`  
   `python run.py stats --failed`  
   `python run.py stats --sessions`

4. **Optional: split input, run separate DBs per part, then merge**  
   `python run.py split stores.xlsx 4`  
   (Run workers with different DBs for each part, then)  
   `python run.py merge merged.db part1.db part2.db part3.db part4.db`

## Project layout

```
Review_Scraper/
  run.py              # Main CLI (run, workers, query, init, stats, merge, split)
  run_workers.py       # Multi-worker entry point
  config.yaml         # Config
  .env                # Optional env overrides
  review_scraper/     # Package
    __init__.py
    config.py         # YAML + env config
    database.py       # SQLite schema, import, claiming, reviews
    scraper.py        # GoogleMapsScraper (browser, navigation, review extraction)
    worker.py         # Multiprocessing worker loop
    merge.py          # Merge DBs
    splitter.py       # Split Excel/CSV
    stats.py          # Stats CLI
  stores.xlsx         # Your store list (or path in config)
  reviews.db          # SQLite DB (path in config)
```

## Notes

- **Captcha / blocks**: A global `pause.flag` file can be created by the scraper (e.g. on captcha). When present, workers sleep (config: `captcha_pause_min`/`max`) then restart the browser. Remove the file to resume.
- **Interrupted runs**: On startup, `run` and `workers` reset stores stuck in `in_progress` back to `pending`.
- Debug artifacts (e.g. `debug_*.png`, `*.txt` logs) in the repo can be deleted; they are not required by the app.
