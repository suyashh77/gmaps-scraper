# VM Run Instructions — NC Grocery Store Review Scraper

**Date:** 2026-04-10  
**Queue:** 2,326 stores across `batch_01.db` – `batch_05.db`  
**Coordinator:** Copy the finished `.db` file back and run `merge` to import into master.

---

## What Each VM Needs

Transfer these files to each VM (in one folder, e.g. `~/scraper/`):

```
~/scraper/
├── linux_scraper/          ← the whole linux_scraper/ folder
├── google_auth_1.json      ← Google auth file (1 per VM)
└── batch_XX.db             ← the batch file(s) assigned to this VM
```

> The `google_auth_*.json` file must be in `~/scraper/` (the parent folder of `linux_scraper/`).  
> The batch `.db` file(s) can be in `~/scraper/` or any folder — just specify the path when running.

---

## Batch Assignment

| VM | Batch file(s) | Stores |
|----|---------------|--------|
| VM 1 | `batch_01.db` | 500 |
| VM 2 | `batch_02.db` | 500 |
| VM 3 | `batch_03.db` | 500 |
| VM 4 | `batch_04.db` | 500 |
| VM 5 | `batch_05.db` | 326 |

Assign batches as needed based on how many VMs you have. One VM can run multiple batches sequentially.

---

## Step 1 — Transfer Files

**From your Windows machine**, use SCP or your preferred file transfer tool:

```bash
# From your local machine (Git Bash / PowerShell with OpenSSH):
scp -r "linux_scraper" user@VM_IP:~/scraper/
scp "google_auth_1.json" user@VM_IP:~/scraper/
scp "batch_01.db" user@VM_IP:~/scraper/    # adjust batch number
```

Or use WinSCP / FileZilla if you prefer a GUI.

---

## Step 2 — First-Time Setup (run once per VM)

SSH into the VM, then:

```bash
cd ~/scraper
bash linux_scraper/setup.sh
```

This installs:
- System dependencies (libnss3, libgbm1, etc.)
- Python packages: playwright, pandas, openpyxl
- Playwright Chromium browser

Takes ~2–5 minutes. Only needed once per machine.

---

## Step 3 — Start Scraping

```bash
cd ~/scraper

# Start a tmux session so it keeps running after you disconnect
tmux new -s scraper

# Run the scraper with the pre-built batch DB
python -m linux_scraper --db batch_01.db
```

**Detach from tmux** (scraper keeps running): `Ctrl-b` then `d`  
**Re-attach later**: `tmux attach -t scraper`

### If running multiple batches on one VM:

```bash
# Run sequentially in one tmux session:
python -m linux_scraper --db batch_01.db && python -m linux_scraper --db batch_02.db
```

---

## Step 4 — Monitor Progress

**From a second terminal (or after re-attaching)**:

```bash
cd ~/scraper

# One-time snapshot
python -m linux_scraper stats batch_01.db

# Live monitor (refreshes every 60s, Ctrl-C to stop)
python -m linux_scraper stats batch_01.db --monitor

# See failed stores
python -m linux_scraper stats batch_01.db --failed

# See scrape session history
python -m linux_scraper stats batch_01.db --sessions
```

The scraper also prints timestamped progress to the terminal every 30 seconds while running.

---

## Step 5 — Retrieve Results

When the scraper finishes (or at any point to hand off partial results):

```bash
# From your local machine:
scp user@VM_IP:~/scraper/batch_01.db "C:/Users/19193/Downloads Local/School/ISE495_SP26/Gmaps Scraper/Data/batch_01_done.db"
```

Then on your local machine, merge it into the master DB:

```bash
cd "C:/Users/19193/Downloads Local/School/ISE495_SP26/Gmaps Scraper"
python -m linux_scraper merge Data/batch_01_done.db --master Data/Step_5_master_reviews.db
```

---

## Useful Details

### How the batch DB works
- Each store has `target_reviews` (total to collect) and `master_reviews` (already in master DB)
- The scraper collects `target_reviews − master_reviews` additional reviews per store
- Status flow: `pending` → `in_progress` → `completed` / `failed`
- Crashed/interrupted stores are automatically reset to `pending` on next run

### Auth file
- The scraper looks for `google_auth*.json` in the parent folder of `linux_scraper/`
- It rotates between multiple auth files if you provide more than one
- Without auth, Google Maps may show fewer reviews (especially for stores with 1000+)

### Stopping gracefully
```bash
# Inside tmux — press Ctrl-C once
# The scraper finishes the current store then stops cleanly
```

### If the scraper crashes mid-run
```bash
# Just re-run the same command — it resumes from where it left off
python -m linux_scraper --db batch_01.db
```

### Estimated time
~2 minutes per store on average. With 500 stores: **~17 hours per batch file**.  
Incomplete stores (those with `master_reviews > 0`) typically run faster since they have fewer reviews left to collect.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `playwright: command not found` | Run `setup.sh` again |
| `No module named playwright` | `pip install playwright` then `playwright install chromium` |
| Browser launch error (missing libs) | `python -m playwright install-deps chromium` |
| CAPTCHA / rate limit detected | Scraper auto-pauses 30–60 min; this is normal |
| `PermissionError` on `.db` file | Make sure no other process has the file open |
| Screen disconnect killed process | Use `tmux` — see Step 3 |
