"""
Headless CLI runner for the Google Maps Review Scraper.

Replaces the Tkinter GUI for Linux/server deployments.
Progress is printed to stdout with timestamps instead of GUI labels.
Auth is loaded from a file path argument instead of a Chrome login window.
SIGINT/SIGTERM trigger graceful shutdown instead of a STOP button.
"""

from __future__ import annotations

import os
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime

from .config import load_config
from .database import DatabaseManager, make_db, get_or_create_machine_id


def _log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def run_scraper(
    csv_path: str | None = None,
    db_path: str | None = None,
    auth_path: str | None = None,
    headless: bool = True,
    partition: tuple[int, int] | None = None,
) -> int:
    """
    Run the scraper from the command line.

    Two modes:
      --db only     : use a pre-built scraper DB (from prepare_machine_db.py)
      --csv [--db]  : import stores from CSV/Excel into a new DB, then scrape

    Returns 0 on success, 1 on error.
    """
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # ── Machine ID ────────────────────────────────────────────────────────
    try:
        machine_id = get_or_create_machine_id(script_dir)
        if not machine_id:
            raise ValueError("Empty machine ID")
    except Exception as e:
        import uuid
        machine_id = uuid.uuid4().hex[:8]
        _log(f"Warning: could not load machine ID ({e}); using: {machine_id}")
    _log(f"Machine ID: {machine_id}")

    # ── Resolve DB path ───────────────────────────────────────────────────
    prebuilt_db = False
    if csv_path is None and db_path:
        # Pre-built DB mode — no CSV import needed
        prebuilt_db = True
        if not os.path.exists(db_path):
            _log(f"ERROR: Database not found: {db_path}")
            return 1
        _log(f"Using pre-built DB: {db_path}")
    elif csv_path:
        if db_path is None:
            csv_dir = os.path.dirname(os.path.abspath(csv_path))
            csv_base = os.path.splitext(os.path.basename(csv_path))[0]
            db_path = os.path.join(csv_dir, f"{csv_base}_reviews_{machine_id}.db")
        _log(f"Input:  {csv_path}")
    else:
        _log("ERROR: Provide --csv FILE or --db FILE")
        return 1

    if partition:
        _log(f"Partition: machine {partition[0]} of {partition[1]}")

    _log(f"Output: {db_path}")

    # ── Auth ──────────────────────────────────────────────────────────────
    valid_auth = []

    # If a specific auth file was passed, try it first
    if auth_path:
        if os.path.exists(auth_path):
            valid_auth.append(os.path.abspath(auth_path))
        else:
            _log(f"WARNING: Specified auth file not found: {auth_path}")

    # Auto-discover all google_auth_*.json in the deploy directory
    for fname in sorted(os.listdir(script_dir)):
        if fname.startswith("google_auth") and fname.endswith(".json"):
            p = os.path.join(script_dir, fname)
            if p not in valid_auth:
                valid_auth.append(os.path.abspath(p))

    if valid_auth:
        _log(f"Auth accounts loaded: {[os.path.basename(f) for f in valid_auth]}")
    else:
        _log("WARNING: No auth files found — some stores may show fewer reviews.")

    # ── Import or load stores ────────────────────────────────────────────
    config = load_config()

    try:
        db_exists = os.path.exists(db_path)
        with make_db(db_path) as db:
            db.init_schema()
            db.set_metadata("machine_id", machine_id)

            if prebuilt_db:
                # Pre-built DB: just show status, no import
                _log("Pre-built DB — skipping CSV import")
            else:
                db.set_metadata("created_at", time.strftime("%Y-%m-%d %H:%M:%S"))
                _log("Importing stores into database...")
                imported, skipped, type_counts = db.import_csv(
                    csv_path, machine_id=machine_id, partition=partition,
                )
                _log(f"Imported {imported} stores ({skipped} skipped/already in DB)")
                _log(f"  Fresh (0 reviews in master):    {type_counts.get('fresh', 0)}")
                _log(f"  Incomplete (has master reviews): {type_counts.get('incomplete', 0)}")

            # Show current status
            cur = db.conn.cursor()
            cur.execute("SELECT status, COUNT(*) AS cnt FROM stores GROUP BY status")
            by_status = {r["status"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
            existing_reviews = cur.fetchone()["cnt"]
            cur.execute("SELECT COUNT(*) AS cnt FROM stores WHERE master_reviews > 0 AND status='pending'")
            incomplete_pending = cur.fetchone()["cnt"]
            cur.execute("SELECT COUNT(*) AS cnt FROM stores WHERE master_reviews = 0 AND status='pending'")
            fresh_pending = cur.fetchone()["cnt"]

            completed = by_status.get("completed", 0)
            pending = by_status.get("pending", 0) + by_status.get("in_progress", 0)
            failed = by_status.get("failed", 0)
            cur.execute("SELECT COUNT(*) AS cnt FROM stores")
            total_stores = cur.fetchone()["cnt"]

            if completed > 0 or existing_reviews > 0:
                _log(
                    f"RESUMING: {completed} stores done ({existing_reviews:,} reviews in DB), "
                    f"{failed} failed, {pending} remaining"
                )
            _log(f"  Pending incomplete (have master reviews): {incomplete_pending}")
            _log(f"  Pending fresh (no reviews yet):           {fresh_pending}")

            reset = db.reset_interrupted_stores()
            if reset:
                _log(f"Reset {reset} interrupted stores back to pending")
            reset_failed = db.reset_failed_stores()
            if reset_failed:
                _log(f"Reset {reset_failed} failed stores for retry")

            pending_total = len(db.get_pending_stores())
            _log(f"{pending_total} stores pending to scrape")

    except Exception as e:
        _log(f"ERROR during setup: {e}")
        import traceback
        _log(traceback.format_exc())
        return 1

    if pending_total == 0:
        _log("All stores already scraped! Nothing to do.")
        _log(f"Output: {db_path}")
        return 0

    est_hours = pending_total * 120 / 3600
    est_str = f"{est_hours:.1f} hours" if est_hours >= 1 else f"{pending_total * 120 / 60:.0f} minutes"
    _log(f"Estimated time: ~{est_str} (~2 min/store avg)")
    _log(f"Priority: incomplete stores first (closest to done), then fresh stores")
    _log("-" * 50)

    # ── Graceful shutdown ─────────────────────────────────────────────────
    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        _log(f"Signal {signum} received — stopping after current store...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # ── Progress monitor ──────────────────────────────────────────────────
    REPORT_INTERVAL = int(config.get("report_interval_hours", 12)) * 3600
    last_report_time = [time.time()]
    monitor_stop = threading.Event()

    def _monitor():
        while not monitor_stop.wait(timeout=30):
            try:
                conn = sqlite3.connect(db_path, timeout=5)
                conn.row_factory = lambda c, r: dict(zip([d[0] for d in c.description], r))
                cur = conn.cursor()
                cur.execute("SELECT status, COUNT(*) AS cnt FROM stores GROUP BY status")
                by_status = {r["status"]: r["cnt"] for r in cur.fetchall()}
                cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
                total_reviews = cur.fetchone()["cnt"]
                conn.close()

                completed = by_status.get("completed", 0)
                failed = by_status.get("failed", 0)
                pending = by_status.get("pending", 0) + by_status.get("in_progress", 0)
                pct = (completed / total_stores * 100) if total_stores else 0
                _log(
                    f"Progress: {completed}/{total_stores} ({pct:.0f}%)  "
                    f"Reviews: {total_reviews:,}  Failed: {failed}  Remaining: {pending}"
                )
            except Exception:
                pass

            if time.time() - last_report_time[0] >= REPORT_INTERVAL:
                try:
                    from .report_generator import generate_report
                    path = generate_report(db_path)
                    _log(f"Periodic report saved: {path}")
                    last_report_time[0] = time.time()
                except Exception as e:
                    _log(f"Report generation failed: {e}")

    monitor_thread = threading.Thread(target=_monitor, daemon=True)
    monitor_thread.start()

    # ── Scrape ────────────────────────────────────────────────────────────
    config["auth"]["accounts"] = valid_auth
    config["browser"]["headless"] = headless

    try:
        from .worker import run_single
        run_single(
            db_path=db_path,
            config=config,
            auth_files=valid_auth,
            machine_id=machine_id,
            stop_event=stop_event,
        )
    except ImportError as e:
        _log(f"ERROR: Missing dependency — {e}")
        _log("Run setup.sh again, or manually run:  pip install -r requirements.txt")
        monitor_stop.set()
        return 1
    except Exception as e:
        import traceback
        _log(f"ERROR: {e}")
        _log(traceback.format_exc())
        monitor_stop.set()
        return 1
    finally:
        monitor_stop.set()

    # ── Done ──────────────────────────────────────────────────────────────
    _log("-" * 50)
    _log("SCRAPING COMPLETE!")
    _log(f"Output file: {db_path}")

    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = lambda c, r: dict(zip([d[0] for d in c.description], r))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM stores WHERE status='completed'")
        done = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
        revs = cur.fetchone()["cnt"]
        conn.close()
        _log(f"Stores completed: {done}")
        _log(f"Total reviews:    {revs:,}")
    except Exception:
        pass

    _log("Generating final report...")
    try:
        from .report_generator import generate_report
        path = generate_report(db_path)
        _log(f"Report saved: {path}")
    except Exception as e:
        _log(f"Report generation failed: {e}")

    _log(f"\nSend this file to the coordinator:\n  {db_path}")
    return 0
