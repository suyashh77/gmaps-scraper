#!/usr/bin/env python3
"""
Google Maps Review Scraper — Guided Wizard for Mass Deployment.

Usage (wizard — just double-click START.bat or run ./start.sh):
  python run.py

Advanced:
  python run.py stats DB_PATH        Show scraping stats
"""

import argparse
import asyncio
import os
import sys
import time

# Ensure package is on path when run as script
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from step2_review_extractor.config import load_config
from step2_review_extractor.database import DatabaseManager, make_db, get_or_create_machine_id


# ── Google login helper ──────────────────────────────────────────────────────

def _run_google_login(auth_file: str) -> None:
    """Opens the user's REAL Chrome browser for Google sign-in.
    
    Uses channel='chrome' to launch the locally installed Chrome instead of
    Playwright's bundled Chromium. This bypasses Google's 'unsupported browser'
    block because real Chrome is not flagged as automation software.
    """
    import shutil
    import tempfile
    from playwright.sync_api import sync_playwright

    print(f"\n  Opening your Chrome browser for Google login...")
    print(f"  Please sign in to your Google account in the browser window.")
    print(f"  Session will be saved to: {auth_file}")
    print(f"  The browser will close automatically after you sign in.\n")

    # Use a temp profile dir so login doesn't interfere with your real Chrome profile
    tmp_profile = tempfile.mkdtemp(prefix="gmaps_auth_")

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=tmp_profile,
            channel="chrome",          # <-- uses your real Chrome installation
            headless=False,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            ignore_default_args=["--enable-automation"],
            args=[
                "--disable-blink-features=AutomationControlled",
                "--lang=en-US",
            ],
        )
        
        try:
            from playwright_stealth import stealth_sync
            page = context.pages[0] if context.pages else context.new_page()
            stealth_sync(page)
        except (ImportError, Exception):
            # stealth not available — real Chrome works fine without it
            page = context.pages[0] if context.pages else context.new_page()

        page.goto(
            "https://accounts.google.com/ServiceLogin?service=local&continue=https://www.google.com/maps",
            wait_until="domcontentloaded",
        )
        print("  Waiting for you to sign in... (up to 5 minutes)")
        try:
            page.wait_for_url("**/maps**", timeout=300_000)
            print("  Sign-in detected! Saving session...")
        except Exception:
            print("  Timed out waiting for sign-in. Saving whatever session state exists...")
        context.storage_state(path=auth_file)
        print(f"  Session saved to: {auth_file}")
        context.close()

    # Cleanup temp profile
    try:
        shutil.rmtree(tmp_profile, ignore_errors=True)
    except Exception:
        pass


# ── WIZARD MODE ──────────────────────────────────────────────────────────────

def run_wizard() -> None:
    """Guided wizard: pick CSV → login Google account → scrape → output .db"""
    print()
    print("=" * 62)
    print("   GOOGLE MAPS REVIEW SCRAPER")
    print("=" * 62)
    print()

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Generate or load machine ID
    machine_id = get_or_create_machine_id(script_dir)
    print(f"  Machine ID: {machine_id}")
    print()

    # ── Step 1: Pick CSV file ────────────────────────────────────────────
    print("  STEP 1: Select your Excel file with store locations")
    print("  " + "-" * 55)

    csv_path = None

    # Try tkinter file dialog first (works on most Windows with Python)
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        csv_path = filedialog.askopenfilename(
            title="Select Excel/CSV file with store locations",
            filetypes=[
                ("Excel files", "*.xlsx;*.xls"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        root.destroy()
    except Exception:
        pass

    # Fallback: manual path entry
    if not csv_path:
        print("  (File picker not available — enter path manually)")
        csv_path = input("  Enter full path to your Excel/CSV file: ").strip().strip('"')

    if not csv_path or not os.path.exists(csv_path):
        print("\n  ERROR: No file selected or file not found. Exiting.")
        sys.exit(1)

    print(f"  Selected: {csv_path}")

    # Determine output DB path (same folder as CSV, with machine_id)
    csv_dir = os.path.dirname(os.path.abspath(csv_path))
    csv_base = os.path.splitext(os.path.basename(csv_path))[0]
    db_path = os.path.join(csv_dir, f"{csv_base}_reviews_{machine_id}.db")
    print(f"  Output will be: {db_path}")
    print()

    # ── Step 2: Login to Google account (required) ───────────────────────
    print("  STEP 2: Google Account Login")
    print("  " + "-" * 55)
    print()
    print("  You must sign in to a Google account for scraping.")
    print("  This is required to see all reviews on Google Maps.")
    print()

    auth_file = os.path.join(script_dir, "google_auth_1.json")

    if os.path.exists(auth_file):
        print(f"  Already signed in ({os.path.basename(auth_file)})")
        relogin = input("  Re-login with a different account? (y/N): ").strip().lower()
        if relogin == "y":
            print(f"\n  Opening Chrome for login...")
            try:
                _run_google_login(auth_file)
            except Exception as e:
                print(f"  WARNING: Login failed: {e}")
                if not os.path.exists(auth_file):
                    print("  ERROR: No auth session available. Exiting.")
                    sys.exit(1)
    else:
        print("  Opening Chrome for login...")
        try:
            _run_google_login(auth_file)
        except Exception as e:
            print(f"  ERROR: Login failed: {e}")
            sys.exit(1)

    if not os.path.exists(auth_file):
        print("\n  ERROR: No auth session saved. Cannot proceed without login.")
        sys.exit(1)

    valid_auth = [auth_file]
    print(f"\n  Google account ready.")
    print()

    # ── Step 3: Import & scrape ──────────────────────────────────────────
    print("  STEP 3: Importing stores and starting scraper")
    print("  " + "-" * 55)

    config = load_config()

    # Import into SQLite
    db_exists = os.path.exists(db_path)
    with make_db(db_path) as db:
        db.init_schema()

        # Store machine_id in metadata
        db.set_metadata("machine_id", machine_id)
        db.set_metadata("created_at", time.strftime("%Y-%m-%d %H:%M:%S"))

        imported, skipped = db.import_csv(csv_path, machine_id=machine_id)
        print(f"  Imported {imported} stores ({skipped} already in DB)")

        # Show resume info if DB already existed
        if db_exists:
            cur = db.conn.cursor()
            cur.execute("SELECT status, COUNT(*) AS cnt FROM stores GROUP BY status")
            by_status = {r['status']: r['cnt'] for r in cur.fetchall()}
            cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
            existing_reviews = cur.fetchone()['cnt']
            completed = by_status.get('completed', 0)
            pending = by_status.get('pending', 0) + by_status.get('in_progress', 0)
            failed = by_status.get('failed', 0)
            if completed > 0 or existing_reviews > 0:
                print(f"\n  RESUMING previous session:")
                print(f"    Already completed: {completed} stores ({existing_reviews:,} reviews)")
                print(f"    Failed (will retry): {failed} stores")
                print(f"    Remaining: {pending} stores")

        # Reset interrupted and failed stores from last run
        reset = db.reset_interrupted_stores()
        if reset:
            print(f"  Reset {reset} interrupted stores back to pending")
        reset_failed = db.reset_failed_stores()
        if reset_failed:
            print(f"  Reset {reset_failed} failed stores for retry")

        total = len(db.get_pending_stores())
        print(f"  {total} stores pending to scrape")

    if total == 0:
        print("\n  All stores already scraped! Nothing to do.")
        print(f"  Output: {db_path}")
        return

    # Show time estimates
    from step2_review_extractor.eta_tracker import ETATracker
    tracker = ETATracker(total_stores=total)
    est_per_store = 120  # rough initial estimate: 2 min/store
    est_total = total * est_per_store
    est_hours = est_total / 3600
    if est_hours >= 1:
        est_str = f"{est_hours:.1f} hours"
    else:
        est_str = f"{est_total / 60:.0f} minutes"

    print(f"\n  Mode: 1 worker (authenticated)")
    print(f"  Scraping up to 1500 reviews per store")
    print(f"  Estimated time: ~{est_str}")
    print(f"  Press Ctrl+C to stop gracefully at any time\n")

    # Set config
    config["workers"]["count"] = 1
    config["auth"]["accounts"] = valid_auth
    config["browser"]["headless"] = False

    # Launch scraping
    from step2_review_extractor.worker import main as workers_main
    workers_main(
        db_path=db_path,
        config=config,
        auth_files=valid_auth,
        machine_id=machine_id,
    )

    # ── Done ─────────────────────────────────────────────────────────────
    print()
    print("=" * 62)
    print("  SCRAPING COMPLETE!")
    print("=" * 62)
    print(f"  Output file: {db_path}")
    print()

    # Show final stats
    from step2_review_extractor.stats import show_stats
    show_stats(db_path)

    # Generate final Excel report
    try:
        from step2_review_extractor.report_generator import generate_report
        report_path = generate_report(db_path)
        print(f"\n  Final Excel report: {report_path}")
    except Exception as e:
        print(f"\n  (Could not generate Excel report: {e})")

    print(f"\n  Send this file back to the coordinator: {db_path}")
    print()


# ── STATS CLI COMMAND ─────────────────────────────────────────────────────────

def cmd_stats(args: argparse.Namespace) -> None:
    """Show DB stats."""
    from step2_review_extractor import stats as st
    if args.monitor:
        st.monitor_mode(args.db)
    elif args.failed:
        st.show_failed(args.db)
    elif args.sessions:
        st.show_sessions(args.db)
    else:
        st.show_stats(args.db)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    # No arguments = wizard mode
    if len(sys.argv) == 1:
        run_wizard()
        return 0

    parser = argparse.ArgumentParser(
        description="Google Maps review scraper",
        epilog="Run with no arguments for guided wizard mode.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # stats
    p_stats = sub.add_parser("stats", help="Show DB stats")
    p_stats.add_argument("db", help="Path to SQLite database file")
    p_stats.add_argument("--failed", action="store_true")
    p_stats.add_argument("--sessions", action="store_true")
    p_stats.add_argument("--monitor", action="store_true")
    p_stats.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
