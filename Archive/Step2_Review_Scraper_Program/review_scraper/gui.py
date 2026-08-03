"""
Tkinter GUI for the Google Maps Review Scraper.

User flow:
  1. Window opens with START button
  2. User clicks START → file picker opens
  3. Chrome opens for Google login
  4. Scraping begins with live progress in the GUI
  5. Done → shows path to .db file
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
from datetime import datetime

from .config import load_config
from .database import DatabaseManager, make_db, get_or_create_machine_id


class ReviewScraperGUI:
    """Main GUI window for the review scraper."""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Google Maps Review Scraper")
        self.root.geometry("720x560")
        self.root.resizable(True, True)
        self.root.configure(bg="#1a1a2e")

        # State
        self.csv_path: str | None = None
        self.db_path: str | None = None
        self.machine_id: str = ""
        self.scraping = False
        self._scrape_thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._monitor_stop: threading.Event = threading.Event() # Issue #10

        self._build_ui()
        self._load_machine_id()
        self._restore_last_file()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)  # Issue #11

    def _load_machine_id(self):
        """Load or generate the machine ID."""
        # Issue #12: guard against load failure
        try:
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.machine_id = get_or_create_machine_id(script_dir)
            if not self.machine_id:
                raise ValueError("Empty machine ID")
        except Exception as e:
            import uuid
            self.machine_id = uuid.uuid4().hex[:8]
            self._log(f"Warning: could not load machine ID ({e}); using: {self.machine_id}")
        self._log(f"Machine ID: {self.machine_id}")

    def _restore_last_file(self):
        """Pre-fill last used file on startup so resume is one click."""
        try:
            p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".last_file")
            if os.path.exists(p):
                val = open(p, encoding="utf-8").read().strip()
                if val and os.path.exists(val):
                    self.csv_path = val
                    csv_base = os.path.splitext(os.path.basename(val))[0]
                    csv_dir = os.path.dirname(os.path.abspath(val))
                    self.file_label.configure(text=f"File: {os.path.basename(val)}")
                    self._log(f"Last file: {val} (click START to resume)")
        except Exception:
            pass

    # ── UI Construction ──────────────────────────────────────────────────

    def _build_ui(self):
        # Colors
        bg = "#1a1a2e"
        card_bg = "#16213e"
        accent = "#0f3460"
        text_color = "#e0e0e0"
        highlight = "#e94560"
        green = "#4ecca3"

        # Title
        title_frame = tk.Frame(self.root, bg=bg, pady=12)
        title_frame.pack(fill=tk.X)

        tk.Label(
            title_frame,
            text="⭐  GOOGLE MAPS REVIEW SCRAPER",
            font=("Segoe UI", 18, "bold"),
            fg=highlight,
            bg=bg,
        ).pack()

        tk.Label(
            title_frame,
            text="Select a file, sign in to Google, collect reviews",
            font=("Segoe UI", 10),
            fg="#888",
            bg=bg,
        ).pack()

        # Status card
        status_frame = tk.Frame(self.root, bg=card_bg, padx=16, pady=10)
        status_frame.pack(fill=tk.X, padx=16, pady=(4, 8))

        self.status_label = tk.Label(
            status_frame,
            text="Status: Ready",
            font=("Segoe UI", 11, "bold"),
            fg=green,
            bg=card_bg,
            anchor="w",
        )
        self.status_label.pack(fill=tk.X)

        self.file_label = tk.Label(
            status_frame,
            text="File: (none selected)",
            font=("Segoe UI", 10),
            fg=text_color,
            bg=card_bg,
            anchor="w",
        )
        self.file_label.pack(fill=tk.X)

        self.output_label = tk.Label(
            status_frame,
            text="Output: —",
            font=("Segoe UI", 10),
            fg=text_color,
            bg=card_bg,
            anchor="w",
        )
        self.output_label.pack(fill=tk.X)

        # Progress stats
        stats_frame = tk.Frame(self.root, bg=bg)
        stats_frame.pack(fill=tk.X, padx=16, pady=(0, 4))

        self.stats_label = tk.Label(
            stats_frame,
            text="",
            font=("Consolas", 10),
            fg="#aaa",
            bg=bg,
            anchor="w",
        )
        self.stats_label.pack(fill=tk.X)

        # Log area
        log_frame = tk.Frame(self.root, bg=bg)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=(0, 8))

        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            font=("Consolas", 9),
            bg="#0d1117",
            fg="#c9d1d9",
            insertbackground="#c9d1d9",
            selectbackground="#264f78",
            relief=tk.FLAT,
            wrap=tk.WORD,
            height=14,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.configure(state=tk.DISABLED)

        # Button frame
        btn_frame = tk.Frame(self.root, bg=bg, pady=10)
        btn_frame.pack(fill=tk.X, padx=16)

        self.start_btn = tk.Button(
            btn_frame,
            text="▶  SELECT FILE & START",
            font=("Segoe UI", 13, "bold"),
            fg="white",
            bg=highlight,
            activebackground="#c0392b",
            activeforeground="white",
            relief=tk.FLAT,
            cursor="hand2",
            padx=24,
            pady=10,
            command=self._on_start,
        )
        self.start_btn.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0, 8))

        self.stop_btn = tk.Button(
            btn_frame,
            text="⏹  STOP",
            font=("Segoe UI", 13, "bold"),
            fg="white",
            bg="#555",
            activebackground="#333",
            activeforeground="white",
            relief=tk.FLAT,
            cursor="hand2",
            padx=24,
            pady=10,
            state=tk.DISABLED,
            command=self._on_stop,
        )
        self.stop_btn.pack(side=tk.RIGHT, padx=(8, 0))

        # Machine ID footer
        tk.Label(
            self.root,
            text=f"Machine ID will appear after init",
            font=("Segoe UI", 8),
            fg="#555",
            bg=bg,
        ).pack(pady=(0, 4))

    # ── Logging ──────────────────────────────────────────────────────────

    def _log(self, msg: str):
        """Thread-safe log — always routed through root.after to avoid Tcl re-entrancy."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{timestamp}] {msg}\n"

        def _insert():
            self.log_text.configure(state=tk.NORMAL)
            self.log_text.insert(tk.END, line)
            self.log_text.see(tk.END)
            self.log_text.configure(state=tk.DISABLED)

        self.root.after(0, _insert)

    def _set_status(self, text: str, color: str = "#4ecca3"):
        def _update():
            self.status_label.configure(text=f"Status: {text}", fg=color)
        self.root.after(0, _update)

    def _set_stats(self, text: str):
        def _update():
            self.stats_label.configure(text=text)
        self.root.after(0, _update)

    # ── Last-file persistence ─────────────────────────────────────────────

    def _get_last_file(self) -> str:
        """Return last used file path if it still exists on disk."""
        try:
            p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".last_file")
            if os.path.exists(p):
                val = open(p, encoding="utf-8").read().strip()
                if val and os.path.exists(val):
                    return val
        except Exception:
            pass
        return ""

    def _save_last_file(self, path: str) -> None:
        try:
            p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".last_file")
            open(p, "w", encoding="utf-8").write(path)
        except Exception:
            pass

    # ── Actions ──────────────────────────────────────────────────────────

    def _on_start(self):
        """User clicked START: pick file → login → scrape."""
        if self.scraping:
            return

        # Bring window to front before opening dialog so it doesn't hide behind other windows
        self.root.lift()
        self.root.focus_force()
        self.root.attributes("-topmost", True)
        self.root.update()

        last_file = self._get_last_file()
        initial_dir = os.path.dirname(last_file) if last_file else os.path.expanduser("~")

        csv_path = filedialog.askopenfilename(
            title="Select Excel/CSV file with store locations",
            initialdir=initial_dir,
            filetypes=[
                ("Excel files", "*.xlsx;*.xls"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        self.root.attributes("-topmost", False)
        if not csv_path:
            return

        if not os.path.exists(csv_path):
            messagebox.showerror("Error", f"File not found:\n{csv_path}")
            return

        self._save_last_file(csv_path)
        self.csv_path = csv_path
        csv_dir = os.path.dirname(os.path.abspath(csv_path))
        csv_base = os.path.splitext(os.path.basename(csv_path))[0]
        self.db_path = os.path.join(csv_dir, f"{csv_base}_reviews_{self.machine_id}.db")

        self.file_label.configure(text=f"File: {os.path.basename(csv_path)}")
        self.output_label.configure(text=f"Output: {os.path.basename(self.db_path)}")

        self._log(f"Selected: {csv_path}")
        self._log(f"Output will be: {self.db_path}")

        # Start scraping in background thread
        self.scraping = True
        self.start_btn.configure(state=tk.DISABLED, bg="#555")
        self.stop_btn.configure(state=tk.NORMAL, bg="#e94560")
        self._set_status("Starting...", "#f0ad4e")

        self._scrape_thread = threading.Thread(target=self._scrape_flow, daemon=True)
        self._scrape_thread.start()

    def _on_stop(self):
        """User clicked STOP — signals the worker subprocess to stop."""
        if not self.scraping:
            return
        self._log("Stopping gracefully... (finishing current store)")
        self._set_status("Stopping...", "#f0ad4e")
        self.scraping = False
        self._monitor_stop.set()
        if self._stop_event is not None:
            self._stop_event.set()  # Issue #8: actually signals the worker subprocess

    def _on_close(self):
        """User clicked the X button — Issue #11: prevent orphaned subprocesses."""
        if self.scraping:
            if not messagebox.askyesno("Quit", "Scraping is running. Stop and quit?"):
                return
            self._on_stop()
            time.sleep(2)  # brief wait for workers to receive the stop signal
        self.root.destroy()

    # ── Google Login ─────────────────────────────────────────────────────

    def _run_google_login(self, auth_file: str) -> bool:
        """Opens real Chrome for Google sign-in. Returns True if session saved."""
        from playwright.sync_api import sync_playwright

        self._log("Opening Chrome for Google login...")
        self._log("Please sign in to your Google account in the browser window.")
        self._set_status("Waiting for Google sign-in...", "#f0ad4e")

        tmp_profile = tempfile.mkdtemp(prefix="gmaps_auth_")

        try:
            with sync_playwright() as pw:
                context = pw.chromium.launch_persistent_context(
                    user_data_dir=tmp_profile,
                    channel="chrome",
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
                    page = context.pages[0] if context.pages else context.new_page()

                page.goto(
                    "https://accounts.google.com/ServiceLogin?service=local&continue=https://www.google.com/maps",
                    wait_until="domcontentloaded",
                )
                self._log("Waiting for sign-in... (up to 5 minutes)")
                try:
                    page.wait_for_url("**/maps**", timeout=300_000)
                    self._log("Sign-in detected! Saving session...")
                except Exception:
                    self._log("Timed out — saving whatever session state exists...")

                context.storage_state(path=auth_file)
                self._log(f"Session saved: {os.path.basename(auth_file)}")
                context.close()

            return os.path.exists(auth_file)

        except Exception as e:
            self._log(f"Login error: {e}")
            return False
        finally:
            try:
                shutil.rmtree(tmp_profile, ignore_errors=True)
            except Exception:
                pass

    # ── Main Scrape Flow ─────────────────────────────────────────────────

    def _scrape_flow(self):
        """Background thread: login → import → scrape → done."""
        try:
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            auth_file = os.path.join(script_dir, "google_auth_1.json")

            # ── Step 1: Google login ──────────────────────────────────────
            if os.path.exists(auth_file):
                self._log(f"Existing auth session found: {os.path.basename(auth_file)}")
                # Use existing — user can re-login from menu later
            else:
                success = self._run_google_login(auth_file)
                if not success or not os.path.exists(auth_file):
                    self._log("ERROR: Google login failed. Cannot proceed.")
                    self._set_status("Login failed", "#e94560")
                    self._finish()
                    return

            valid_auth = [auth_file]

            # ── Step 2: Import stores into DB ─────────────────────────────
            self._set_status("Importing stores...", "#f0ad4e")
            config = load_config()

            db_exists = os.path.exists(self.db_path)
            with make_db(self.db_path) as db:
                db.init_schema()
                db.set_metadata("machine_id", self.machine_id)
                db.set_metadata("created_at", time.strftime("%Y-%m-%d %H:%M:%S"))

                imported, skipped = db.import_csv(self.csv_path, machine_id=self.machine_id)
                self._log(f"Imported {imported} stores ({skipped} already in DB)")

                # Resume info
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
                        self._log(f"RESUMING: {completed} stores done ({existing_reviews:,} reviews), "
                                  f"{failed} failed, {pending} remaining")

                reset = db.reset_interrupted_stores()
                if reset:
                    self._log(f"Reset {reset} interrupted stores back to pending")
                reset_failed = db.reset_failed_stores()
                if reset_failed:
                    self._log(f"Reset {reset_failed} failed stores for retry")

                total = len(db.get_pending_stores())
                self._log(f"{total} stores pending to scrape")

            if total == 0:
                self._log("All stores already scraped! Nothing to do.")
                self._log(f"Output: {self.db_path}")
                self._set_status("Complete — all stores done!", "#4ecca3")
                self._finish()
                return

            # Time estimate
            est_hours = total * 120 / 3600
            est_str = f"{est_hours:.1f} hours" if est_hours >= 1 else f"{total * 120 / 60:.0f} minutes"
            self._log(f"Estimated time: ~{est_str}")
            self._log(f"Scraping up to 1500 reviews per store")
            self._log("─" * 50)

            # ── Step 3: Scrape ────────────────────────────────────────────
            self._set_status(f"Scraping {total} stores...", "#f0ad4e")

            config["auth"]["accounts"] = valid_auth
            config["browser"]["headless"] = False

            # Create a stop event the STOP button can signal
            self._stop_event = threading.Event()
            self._monitor_stop.clear()
            self._start_progress_monitor(total)

            from .worker import run_single
            run_single(
                db_path=self.db_path,
                config=config,
                auth_files=valid_auth,
                machine_id=self.machine_id,
                stop_event=self._stop_event,
            )

            # ── Done ──────────────────────────────────────────────────────
            self._log("─" * 50)
            self._log("SCRAPING COMPLETE!")
            self._log(f"Output file: {self.db_path}")

            # Final stats
            try:
                from .stats import show_stats
                # Capture stats to log
                import sqlite3
                conn = sqlite3.connect(self.db_path, timeout=10)
                conn.row_factory = lambda c, r: dict(zip([d[0] for d in c.description], r))
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) AS cnt FROM stores WHERE status='completed'")
                done = cur.fetchone()["cnt"]
                cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
                revs = cur.fetchone()["cnt"]
                conn.close()
                self._log(f"Stores completed: {done}")
                self._log(f"Total reviews: {revs:,}")
            except Exception:
                pass

            self._log(f"\nSend this file to the coordinator:\n  {self.db_path}")
            self._set_status("Complete!", "#4ecca3")

            # Pop a dialog so the student can't miss where the file is
            db_path_copy = self.db_path
            def _show_done():
                messagebox.showinfo(
                    "Scraping Complete!",
                    f"Done! Send this file to your coordinator:\n\n{db_path_copy}",
                )
            self.root.after(0, _show_done)

        except Exception as e:
            self._log(f"ERROR: {e}")
            self._set_status(f"Error: {str(e)[:60]}", "#e94560")

        finally:
            self._finish()

    def _start_progress_monitor(self, total_stores: int):
        """Start a background thread that updates progress stats in the GUI."""
        def _monitor():
            import sqlite3
            # Issue #10: use Event.wait so stop wakes us immediately, no 10s delay
            while not self._monitor_stop.wait(timeout=10):
                try:
                    conn = sqlite3.connect(self.db_path, timeout=5)
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

                    stats_text = (
                        f"Stores: {completed}/{total_stores} ({pct:.0f}%)  |  "
                        f"Reviews: {total_reviews:,}  |  "
                        f"Failed: {failed}  |  "
                        f"Remaining: {pending}"
                    )
                    self._set_stats(stats_text)
                except Exception:
                    pass

        t = threading.Thread(target=_monitor, daemon=True)
        t.start()

    def _finish(self):
        """Re-enable the start button."""
        self.scraping = False
        self._monitor_stop.set()  # wake and stop the monitor thread immediately

        def _update():
            self.start_btn.configure(state=tk.NORMAL, bg="#e94560")
            self.stop_btn.configure(state=tk.DISABLED, bg="#555")

        self.root.after(0, _update)

    # ── Run ──────────────────────────────────────────────────────────────

    def run(self):
        """Start the GUI main loop."""
        self.root.mainloop()


def launch_gui():
    """Entry point to launch the GUI."""
    app = ReviewScraperGUI()
    app.run()
