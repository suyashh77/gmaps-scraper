# POI probe — run the Google probe on another machine

> This folder is **built**, not hand-edited: `python Deploy/build_kit.py probe --with-auth`
> in the main repository assembles it from the canonical code and takes a fresh snapshot
> of the work queue. `KIT_MANIFEST.json` lists every file with its hash.

This folder is everything needed to look up North Carolina grocery stores on
Google Maps and record what Google says about each one. Copy the whole folder
to the other machine and run two files.

**16,751 stores in the queue. 3,503 are already done, so there are
13,225 to go — roughly four days of running.**

---

## Move it, then run it

### On the main machine, before you copy

Stop any probe running there, then build a fresh kit so it carries the latest
queue state:

```
python Deploy/build_kit.py probe --with-auth --force
```

Then copy the whole `Deploy/build/probe_kit` folder to the other machine. USB
drive, network share, whatever is easiest. It is about 15 MB.

### On the other machine

**1. Double-click `setup.bat`** — once, the first time only.

Installs Python packages and the Chromium browser the probe drives. Takes a few
minutes. If it says Python is not on PATH, install Python 3.10 or newer from
python.org and tick **"Add python.exe to PATH"** during the install.

**Install Google Chrome on that machine too, if it is not there already.** The
probe asks Playwright for real Chrome first and only falls back to the bundled
Chromium if it cannot find it. Both work; real Chrome looks less like a robot to
Google, so it is the better one to have.

**2. Double-click `START.bat`** — this is the one you run.

Leave the window open. It prints a line per store and restarts itself if the
browser crashes. Press **Ctrl-C** to stop whenever you like; everything is
saved, and running `START.bat` again picks up exactly where it stopped.

### When it finishes, or whenever you want the results

**Double-click `collect.bat`.** It writes one file into `results\`, named for
the machine and the time. Copy that single file back to the main machine and
run, from the project root:

```
python Frame\probe_merge.py Data\returns\<date>_<machine>\probe_BOX2_20260912_1830.db
```

That folds the results into the master queue. It is safe to run more than once,
and it never overwrites a row the master already probed more recently.

---

## The four files you actually touch

| File | What it does |
|---|---|
| `setup.bat` | One-time install. Run it once on the new machine. |
| `START.bat` | Runs the probe. This is the one you use. |
| `collect.bat` | Packages the results to carry back. |
| `stats.bat` | Shows how far along it is, any time. |
| `skip_stuck.bat` | Only if it stalls — parks stores that keep failing. |

`Deploy/build_kit.py probe` runs on the *main* machine, before you copy.
`Frame/probe_merge.py` runs on the *main* machine, after you copy back.

---

## What the probe actually does

For each store it opens the Google Maps page, checks the name on the page
matches the store it asked for, and records four things:

- **what Google calls it** — the category, which is the only authority that can
  settle whether an Overture "wholesale_store" is a Costco or a bolt distributor
- **the place ID** — the identifier the review scraper navigates by
- **the review count** — which sets the review scrape target
- **whether it is permanently closed**

It does **not** open the reviews tab or scroll. That is where all the time goes
in the review scraper, and skipping it is what makes 17,000 stores a four-day
job rather than a month.

Roughly 3,200 stores a day on one machine, at about 4 seconds a store.

---

## Things worth knowing

**One machine, one IP address.** Do not run two copies of `START.bat` on the
same machine. The limit that binds is Google's tolerance for a single address,
not CPU — two workers behind one address produce the same throughput and reach
the block threshold twice as fast. This was measured on this project.

**Two machines are fine**, as long as each has its own copy of the folder and
its own network connection. Merge both result files back; `Frame/probe_merge.py`
handles overlap correctly.

**It is safe to kill at any time.** Every store is claimed inside a database
transaction, and a store interrupted by a kill is released back to the queue
the next time the probe starts. The worst a crash costs is the one store that
was on screen.

**If Google starts blocking**, the probe detects it and backs off on its own.
If it keeps happening, stop for a few hours. There are three signed-in sessions
in `Auth\` and it rotates between them; a burned account costs a third of the
machine's capacity for the day.

**`Auth\` holds live Google session cookies.** Anyone holding one of those
files is signed in as that account. Do not put this folder on a shared drive,
do not commit it, and do not copy it to a machine you do not control. The probe
runs without them — it just gets rate limited sooner.

---

## If something goes wrong

**"Python is not on PATH"** — install Python 3.10+ from python.org, tick "Add
python.exe to PATH", rerun `setup.bat`.

**`setup.bat` fails on Playwright** — rerun it. The Chromium download is large
and sometimes times out on a slow connection.

**The window closes immediately** — you skipped `setup.bat`, or it did not
finish. Run it and read the last few lines.

**It says "queue drained"** — it is finished. Run `collect.bat`.

**It says "5 passes in a row with no progress"** — one store is crashing the
probe every time it restarts. Run `skip_stuck.bat` to park the stores that keep
failing, then `START.bat` again. Nothing is lost; parked stores keep their row
and can be retried later with `skip_stuck.bat --unpark`.

**Everything fails with a browser error** — run `stats.bat` to confirm the
queue is intact, then rerun `setup.bat`; the Chromium install may be damaged.

---

## What is in the folder

```
probe_kit\
  START.bat            run the probe            <- the one you use
  setup.bat            one-time install
  collect.bat          package results to carry back
  stats.bat            progress report
  skip_stuck.bat       park stores that keep failing (only if it stalls)
  KIT_MANIFEST.json    what was shipped, with hashes

  run_probe.py         supervisor: restarts the probe, stops when drained
  collect.py           consistent snapshot of the results
  skip_stuck.py        parks / unparks repeatedly failing stores

  Frame\
    probe.py           the probe itself
    build\
      probe.db         the work queue and every result   <- the important file
  Scraper_v2\
    linux_scraper\    browser automation the probe drives
  Auth\
    google_auth_*.json live Google sessions -- treat as passwords
  results\            collect.bat writes here
```

The one file that matters is `Frame\build\probe.db`. It is the queue and the
results in one. Everything else can be rebuilt.
