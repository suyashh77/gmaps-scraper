"""
src/orchestrator.py
===================
Legacy session-based orchestrator for the EnrichOverture project.

NOTE: This module is kept for reference but is no longer the primary
runner.  The parallel multi-worker runners (``run_nc_enrichment.py`` and
``run_nc_retry_notfound.py``) supersede it for production use.

What it does
------------
Runs ``main.py`` as a subprocess in a loop, up to ``MAX_SESSIONS`` times,
with a cooldown between sessions.  This was the original approach before
parallel processing was implemented.

When to use
-----------
Only useful for single-process sequential debugging.  For any real
workload, use ``run_nc_enrichment.py --workers N`` instead.
"""

import logging
import os
import sqlite3
import subprocess
import sys
import time
from typing import Tuple

from config.settings import DB_PATH
from src.database import init_db
from src.utils import setup_logger

logger = setup_logger("Orchestrator")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAX_SESSIONS     = 30    # Maximum number of scraping sessions to run
COOLDOWN_SECONDS = 15    # Pause between sessions (seconds)
SESSION_TIMEOUT  = 600   # Kill a session that runs longer than this (seconds)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_progress() -> Tuple[int, int]:
    """
    Query the database for overall processing progress.

    Returns:
        ``(completed, total)`` where *completed* is the count of rows
        with any status other than ``'pending'``.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM supermarkets")
        total = cursor.fetchone()[0]

        # Any status that is not 'pending' counts as done
        cursor.execute("SELECT COUNT(*) FROM supermarkets WHERE status != 'pending'")
        completed = cursor.fetchone()[0]

        conn.close()
        return completed, total

    except Exception as exc:
        logger.error(f"Could not read progress from DB: {exc}")
        return 0, 0


def run_session(session_num: int) -> bool:
    """
    Launch ``main.py`` as a subprocess and stream its output to the console.

    Args:
        session_num: Human-readable session counter (used in log messages).

    Returns:
        ``True`` if the subprocess exited with code 0, ``False`` otherwise.
    """
    logger.info(f"--- Starting Session {session_num} ---")

    cmd = [sys.executable, "main.py"]

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=os.getcwd(),
        )

        # Stream subprocess output line-by-line so it appears in real time
        if process.stdout:
            with process.stdout:
                for line in iter(process.stdout.readline, ""):
                    if line:
                        print(f"[Session {session_num}] {line.strip()}")
                    if process.poll() is not None:
                        break

        return_code = process.wait(timeout=SESSION_TIMEOUT)

        if return_code == 0:
            logger.info(f"Session {session_num} finished successfully.")
            return True
        else:
            logger.warning(f"Session {session_num} exited with code {return_code}.")
            return False

    except subprocess.TimeoutExpired:
        logger.warning(f"Session {session_num} timed out after {SESSION_TIMEOUT}s. Killing.")
        process.kill()
        return False

    except Exception as exc:
        logger.error(f"Session {session_num} failed to launch: {exc}")
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the orchestrator loop."""
    logger.info("Starting Orchestrator...")
    init_db()

    completed, total = get_progress()
    logger.info(f"Initial status: {completed}/{total} businesses completed.")

    consecutive_failures = 0

    for session_num in range(1, MAX_SESSIONS + 1):
        completed, total = get_progress()

        # Stop if everything is done
        if total > 0 and completed >= total:
            logger.info("All businesses processed. Exiting.")
            break

        success = run_session(session_num)

        if success:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                logger.error("3 consecutive failures — stopping orchestrator.")
                break

        logger.info(f"Cooling down for {COOLDOWN_SECONDS}s before next session...")
        time.sleep(COOLDOWN_SECONDS)


if __name__ == "__main__":
    main()
