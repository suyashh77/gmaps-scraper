# Google Maps Review Scraper

Scrapes Google Maps reviews for a list of store locations. Designed for mass deployment — distribute to 100+ people, collect `.db` files, merge into one master database.

## Quick Start

See the step-by-step guide for your platform:

- **Windows** → [windows/GUIDE.md](windows/GUIDE.md)
- **Mac** → [mac/GUIDE.md](mac/GUIDE.md)

### TL;DR

1. Run the setup script for your platform (`windows/setup.bat` or `mac/setup.sh`)
2. Run the scraper (`windows/START.bat` or `mac/start.sh`)
3. Select your Excel file, sign in to Google, wait for scraping to finish
4. Send the `.db` file back to the coordinator

## How It Works

1. **Select your Excel file** with store locations
2. **Sign in to Google** — Chrome opens, you log in, session is saved
3. **Scraping runs** — the tool visits each store on Google Maps and collects reviews
4. **Output** — a `.db` file (SQLite database) with all reviews

Each machine generates a unique ID (`machine_id.txt`) so files from different people don't collide.

## For Coordinators: Merging DB Files

After collecting `.db` files from all runners:

```bash
# Merge specific files
python merge_dbs.py master.db runner1.db runner2.db runner3.db

# Or merge all .db files in a folder
python merge_dbs.py master.db ./collected_dbs/
```

The merge tool:
- Deduplicates stores by `google_place_id`
- Deduplicates reviews by `review_id`
- Keeps the version with the most reviews when stores overlap
- Preserves `machine_id` on every row for audit trail

## Input File Format

Excel (`.xlsx`) or CSV with these columns (flexible naming):

| Column | Required | Example |
|---|---|---|
| `input_name` / `name` | Yes | Food Lion |
| `input_city` / `city` | Recommended | Raleigh |
| `input_state` / `state` | Recommended | NC |
| `google_place_id` / `place_id` | Best accuracy | ChIJ... |
| `input_lat` / `lat` | Helpful | 35.7796 |
| `input_lon` / `lon` | Helpful | -78.6382 |
| `gmaps_address` | Helpful | 123 Main St, Raleigh, NC |

## Output

- **SQLite database**: `{input_filename}_reviews_{machine_id}.db`
- **Excel reports**: `reports/scraper_report_{date}.xlsx` (generated every 6 hours + on completion)

### Database Tables

- **`stores`** — all store locations with status, review counts, machine_id
- **`reviews`** — individual reviews with text, rating, date, reviewer info, machine_id
- **`metadata`** — machine_id and timestamps for merge traceability

## Features

- **Sign-in-first** — Google account login required for full review access
- **Auto-retry** — failed stores retried up to 3x
- **Resume support** — restart anytime, picks up where it left off
- **Review deduplication** — never duplicates reviews in the database
- **Account rotation** — auto-switches Google accounts if one gets banned
- **CAPTCHA handling** — auto-pauses 30-60 min, then resumes
- **Live ETA tracking** — console shows progress, ETA, and review counts
- **Excel reports** — progress reports every 6 hours
- **Merge tool** — stitch 100+ DB files into one master database

## Requirements

- Python 3.10+
- Google Chrome (recommended) or Playwright Chromium
- Windows 10/11 or macOS 12+

## Project Structure

```
Gmaps Scraper/
├── run.py                  # Main entry point (wizard)
├── merge_dbs.py            # Coordinator tool: merge DB files
├── requirements.txt        # Python dependencies
├── .gitignore
├── windows/                # Windows platform files
│   ├── setup.bat           # One-time setup
│   ├── START.bat           # Double-click to run
│   └── GUIDE.md            # Step-by-step guide
├── mac/                    # Mac platform files
│   ├── setup.sh            # One-time setup
│   ├── start.sh            # Run the scraper
│   └── GUIDE.md            # Step-by-step guide
└── step2_review_extractor/         # Core package
    ├── __init__.py
    ├── config.py            # Default configuration
    ├── database.py          # SQLite database layer
    ├── scraper.py           # Browser automation + review extraction
    ├── worker.py            # Multi-process orchestration
    ├── auth_manager.py      # Google account rotation
    ├── eta_tracker.py       # ETA calculation
    ├── report_generator.py  # Excel report generation
    ├── stats.py             # CLI stats display
    └── splitter.py          # Input file splitting
```
