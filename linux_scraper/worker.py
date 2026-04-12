"""
Single-worker scraper runner.

Runs one asyncio scraping loop in the caller's thread (no multiprocessing).
Call run_single() from a background thread — the GUI stays on the main thread.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading

from .auth_manager import AuthAccountManager
from .config import load_config
from .database import make_db


def _setup_logging(config: dict, db_path: str = "") -> None:
    log_file = config["logging"]["file"]
    # Resolve relative log path to sit next to the database file
    if db_path and not os.path.isabs(log_file):
        log_file = os.path.join(os.path.dirname(os.path.abspath(db_path)), log_file)
    handlers = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    except (OSError, PermissionError):
        pass  # can't write log file — stderr logging still works
    logging.basicConfig(
        level=getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


def run_single(
    db_path: str,
    config: dict | None = None,
    auth_files: list[str] | None = None,
    machine_id: str = "",
    stop_event: threading.Event | None = None,
) -> None:
    """
    Run one scraping worker sequentially: claim store → scrape → repeat.

    Designed to be called from a background thread. The caller's thread blocks
    until all stores are done or stop_event is set.

    stop_event: set() it from the GUI thread to stop after the current store.
    """
    if config is None:
        config = load_config()
    if stop_event is None:
        stop_event = threading.Event()

    _setup_logging(config, db_path=db_path)
    logger = logging.getLogger("worker")

    # Reset any stores left in_progress from a previous interrupted run
    with make_db(db_path) as _db:
        _db.init_schema()
        reset = _db.reset_interrupted_stores()
        if reset:
            logger.info(f"Reset {reset} interrupted stores to pending")
        reset_failed = _db.reset_failed_stores()
        if reset_failed:
            logger.info(f"Reset {reset_failed} failed stores for retry")

    db = make_db(db_path)
    db.connect()

    if auth_files is None:
        auth_files = []

    auth_manager = AuthAccountManager(auth_files) if auth_files else AuthAccountManager([])

    from .scraper import GoogleMapsScraper

    scraper = GoogleMapsScraper(
        config=config,
        db=db,
        worker_id=1,
        auth_manager=auth_manager,
        machine_id=machine_id,
    )

    restart_every = int(config["workers"]["restart_browser_every"])
    max_retries = int(config["retry"]["max_retries"])

    async def loop() -> None:
        await scraper.start_browser()
        processed = 0
        consecutive_crashes = 0

        try:
            while not stop_event.is_set():
                # Claim next store with retry on DB lock
                store = None
                for _ in range(5):
                    try:
                        store = db.claim_next_store(worker_id=1, max_retries=max_retries)
                        break
                    except sqlite3.OperationalError as e:
                        if "locked" in str(e).lower():
                            await asyncio.sleep(1)
                        else:
                            raise

                if store is None:
                    if not db.get_pending_stores():
                        logger.info("All stores complete — worker done")
                        break
                    await asyncio.sleep(3)
                    continue

                store_name = (store.get("gmaps_name") or store.get("input_name") or "Unknown")[:50]
                master_rev = int(store.get("master_reviews") or 0)
                target_rev = int(store.get("target_reviews") or 0)
                store_type = "INCOMPLETE" if master_rev > 0 else "FRESH"
                effective = max(min(1500, target_rev) - master_rev, 1)
                # Scale timeout: ~1.5s per review needed, min 5 min, max 45 min
                timeout_sec = max(300.0, min(effective * 1.5, 2700.0))
                logger.info(
                    f"Scraping [{store_type}]: {store_name} "
                    f"(target={target_rev}, capped={min(1500, target_rev)}, master={master_rev}, "
                    f"need={effective}, timeout={timeout_sec:.0f}s)"
                )

                try:
                    status = await asyncio.wait_for(
                        scraper.scrape_single_store(store),
                        timeout=timeout_sec,
                    )
                    processed += 1
                    if status in ("completed", "skipped"):
                        consecutive_crashes = 0  # only reset on clean outcomes

                    # Restart browser periodically to prevent memory bloat
                    if restart_every > 0 and processed % restart_every == 0:
                        logger.info(f"Periodic browser restart after {restart_every} stores")
                        await scraper.restart_browser()

                except asyncio.TimeoutError:
                    consecutive_crashes += 1
                    logger.warning(f"'{store_name}' timed out after {timeout_sec:.0f}s — skipping")
                    db.mark_store_failed(store["store_id"], f"Timeout: exceeded {timeout_sec:.0f}s")
                    processed += 1
                    # Force-kill browser — graceful close may hang after timeout
                    try:
                        await asyncio.wait_for(scraper.close_browser(), timeout=5.0)
                    except Exception:
                        scraper.browser = None
                        scraper.context = None
                        scraper.page = None
                        scraper._pw = None
                    await asyncio.sleep(0)  # flush event loop
                    try:
                        await scraper.start_browser()
                    except Exception as e:
                        logger.error(f"Browser restart failed after timeout: {e}")
                        break
                    continue

                except Exception as exc:
                    consecutive_crashes += 1
                    logger.warning(f"'{store_name}' crashed: {type(exc).__name__}: {str(exc)[:150]}")
                    db.mark_store_failed(store["store_id"], f"{type(exc).__name__}: {str(exc)[:300]}")
                    processed += 1

                    if consecutive_crashes >= 5:
                        # Back off for 5 minutes instead of stopping completely —
                        # transient network issues or temporary Google blocks can
                        # cause 5+ consecutive failures, and stopping would leave
                        # the remaining stores unscraped.
                        logger.warning("5 consecutive crashes — pausing 5 minutes then retrying")
                        try:
                            await scraper.close_browser()
                        except Exception:
                            pass
                        await asyncio.sleep(300)
                        consecutive_crashes = 0
                        try:
                            await scraper.start_browser()
                        except Exception as e:
                            logger.error(f"Browser restart failed after backoff: {e}")
                            break
                        continue

                    try:
                        await scraper.close_browser()
                    except Exception:
                        pass
                    await asyncio.sleep(3)
                    try:
                        await scraper.start_browser()
                    except Exception as e:
                        logger.error(f"Browser restart failed: {e}")
                        break
                    continue

        finally:
            try:
                await scraper.close_browser()
            except Exception:
                pass
            db.release_worker_stores(1)
            db.close()
            logger.info(f"Worker finished. Processed {processed} stores.")

    asyncio.run(loop())
