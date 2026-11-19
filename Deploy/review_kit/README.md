# NC grocery review scraper — collection kit

One folder, one machine number. Each machine runs the batch in `batches/` whose name
ends in its number; the three batches hold disjoint stores, so nothing collides.

| machine | number | batch |
|---|---|---|
| forrester | **1** | `batches/grocery1.db` |
| manabe | **2** | `batches/grocery2.db` |
| holling | **3** | `batches/grocery3.db` |

> This folder is **built**, not hand-edited (`python Deploy/build_kit.py review …` in the
> main repository). `KIT_MANIFEST.json` lists every file with its hash. If something needs
> fixing, fix it in the repository and rebuild — never patch a copy on a machine.

## If you already have an older scraper folder on this machine

Send its results back **before** you replace it, or the reviews it collected since the last
collection are lost:

```bash
cd ~/scraper            # the OLD folder
./collect_results.sh    # (Windows: collect.bat)   -> results/*.db
# copy results/ back to the main machine, then stop the old run:
tmux kill-session -t scrape1      # or scrape2; Windows: close the window
```

Then delete or rename the old folder and put this one in its place.

## Linux (forrester, manabe)

```bash
bash setup.sh          # once per machine: Python deps + Chromium + preflight checks
./run_machine1.sh      # forrester   (./run_machine2.sh on manabe)
./collect_results.sh   # whenever you want to ship reviews back
```

`run_machine*.sh` starts the scraper inside tmux, so an SSH disconnect does not kill it.
`tmux attach -t scrape1` shows it live (Ctrl-b then d to leave).

## Windows (holling)

Double-click, in order:

1. `setup.bat` — once. Installs Python packages and Chromium. If it says Python is not on
   PATH, install Python 3.10+ from python.org and tick "Add python.exe to PATH".
2. `START.bat 3` — open a Command Prompt in this folder and type that, or make a shortcut
   with `3` as the argument. Leave the window open; Ctrl-C stops it and `START.bat 3`
   resumes exactly where it left off.
3. `collect.bat` — writes `results\grocery3_<machine>_<time>.db` to send back.

## Auth

Put the Google session files in `auth/` (`google_auth_1.json` and so on). They are live
session cookies: **do not** put this folder on a shared drive, commit it, or copy it to a
machine you do not control. Without them the scraper still runs, it is just rate-limited
sooner.

## One machine, one IP

Run **one** batch per machine and do not run two copies against one batch. The limit that
binds is Google's tolerance for a single IP address, not CPU; two workers behind one address
give the same throughput and hit the block threshold twice as fast. Two copies on one batch
is worse: both claim the same store and duplicate its reviews.

## Watching it

```bash
tail -f batches/scraper.log
python -m linux_scraper stats batches/grocery1.db --monitor     # inside the venv
```

| log line | meaning | what to do |
|---|---|---|
| `CAPTCHA` | this **IP** is burned | it pauses 30–60 min on its own |
| `sign-in wall` | this **account** is burned | it rotates to the next session on its own |
| `No network` | connectivity is gone | it waits and resumes |
| `Store progressed` | a big store advanced and was requeued | normal for large listings |
| `FIELD COVERAGE FAILURE` | a selector went stale | tell me — Google changed the page |
| `place_id mismatch` | landed on the wrong store | correct behaviour, it refused the page |

Nothing else needs a human. Failed stores are retried; if the process dies it restarts. All
state lives in the batch database, so restarting after a reboot is the whole recovery.

## What to expect

Each batch is ~552 stores and ~170,000 reviews; new stores are scraped first. Observed
throughput is roughly 5,000–10,000 reviews a day per machine, so plan on three to four weeks. Large stores do not finish in one visit
(Google exposes a bounded slice per visit); `Store progressed` is the system working.

Each batch carries a skip-list of the reviews the master database already holds, so a
store that was partly collected before recognises what it has instead of re-collecting it.

## Shipping reviews back

`collect_results.sh` / `collect.bat` writes a consistent copy to `results/`, named for your
machine. Copy that file back; the merge on the main machine dedupes on content and is safe
to repeat. You can keep scraping while the copy travels.

## Known limits

- `helpful_count`, `photo_count`, `service_type` are not collected — Google renders no
  such elements. They stay empty; treat them as unavailable, not zero.
- Review dates are Google's relative strings ("2 years ago"), not absolute dates.
