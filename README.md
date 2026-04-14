# Google Maps Review Scraper (Linux)

Headless multi-worker scraper for Google Maps business reviews. Runs on Linux VMs with Playwright, SQLite storage, and automatic batch management.

## Quick Start (VM Deployment)

```bash
# 1. Transfer files to VM
scp -r linux_scraper user@VM_IP:~/scraper/
scp google_auth_1.json user@VM_IP:~/scraper/
scp batch_01.db user@VM_IP:~/scraper/

# 2. Setup (run once)
ssh user@VM_IP
cd ~/scraper
bash setup.sh

# 3. Run scraper
tmux new -s scraper
python -m linux_scraper --db batch_01.db
# Detach: Ctrl-b d, re-attach: tmux attach -t scraper
```

See [VM_INSTRUCTIONS.md](linux_scraper/VM_INSTRUCTIONS.md) for complete documentation.

## Features

- **Headless browser** - Playwright + Chromium, no GUI required
- **SQLite storage** - Batched store lists with review counts
- **Resume capability** - Interrupted stores auto-reset on restart
- **Multi-account rotation** - Automatic fallback when CAPTCHA detected
- **Rate limiting** - Configurable delays to avoid detection
- **Progress monitoring** - Live stats and periodic reports

## Project Structure

```
├── linux_scraper/          # Main package
│   ├── scraper.py          # Core scraping logic
│   ├── worker.py           # Single-worker runner
│   ├── database.py         # SQLite operations
│   ├── cli.py              # Command-line interface
│   ├── auth_manager.py     # Google account rotation
│   ├── VM_INSTRUCTIONS.md  # Full VM setup guide
│   └── ...
├── setup.sh                # Linux setup script
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

## Requirements

- Linux (Ubuntu/Debian preferred)
- Python 3.8+
- Google auth JSON file (for full review access)

## Commands

```bash
# Run scraper on batch file
python -m linux_scraper --db batch_01.db

# Check stats
python -m linux_scraper stats batch_01.db

# Live monitoring
python -m linux_scraper stats batch_01.db --monitor

# See failed stores
python -m linux_scraper stats batch_01.db --failed

# Merge results into master DB
python -m linux_scraper merge batch_01_done.db --master master.db
```

## VM Deployment

Assign batch files to VMs based on capacity:

| Batch | Stores |
|-------|--------|
| batch_01.db | ~500 |
| batch_02.db | ~500 |
| ... | ... |

Each VM needs:
- `linux_scraper/` folder
- One `google_auth_*.json` file
- Assigned `batch_*.db` file(s)

**Est. time:** ~2 min/store = ~17 hours per 500-store batch

## Configuration

Edit `linux_scraper/config.py` for:
- Browser settings (headless, restart interval)
- Rate limiting delays
- Retry attempts
- Scrolling behavior

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Browser launch fails | Run `setup.sh` again |
| CAPTCHA detected | Auto-pauses 30-60 min, normal behavior |
| Process dies on disconnect | Use `tmux` - see VM_INSTRUCTIONS.md |
| PermissionError | Close other processes accessing the .db file |

## License

Internal use only.
