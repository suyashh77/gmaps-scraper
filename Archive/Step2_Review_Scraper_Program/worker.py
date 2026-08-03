"""Multi-process worker: claims stores from SQLite DB and runs scraper in a loop.

Supports authenticated mode: 1 worker per Google account.
"""

import asyncio
import logging
import multiprocessing
import os
import signal
import sys
import time

from .auth_manager import AuthAccountManager
from .config import load_config
from .database import DatabaseManager, make_db
from .eta_tracker import ETATracker

STOP_EVENT = multiprocessing.Event()

# Shared counters for cross-worker ETA tracking
_completed_counter = multiprocessing.Value('i', 0)
_reviews_counter = multiprocessing.Value('i', 0)
_failed_counter = multiprocessing.Value('i', 0)


def _configure_logging(config: dict) -> None:
    logging.basicConfig(
        level=getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO),
        format="%(asctime)s [ORCHESTRATOR] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("orchestrator.log"),
        ],
    )


def worker_process(
    worker_id: int, 
    config: dict, 
    auth_files: list[str], 
    db_path: str, 
    sys_path: list[str] | None = None,
    machine_id: str = "",
) -> None:
    """
    Single worker process: claims stores from SQLite and scrapes them.

    Each worker gets its own SQLite connection (required — SQLite connections
    cannot be shared across processes).
    """
    import sys as _sys
    if sys_path:
        for p in sys_path[::-1]:
            if p not in _sys.path:
                _sys.path.insert(0, p)
    # Import scraper here to avoid circular imports in subprocess
    from .scraper import GoogleMapsScraper

    logger = logging.getLogger(f"worker-{worker_id}")
    logger.setLevel(getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO))
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s [%(name)s] %(message)s")
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
        fh = logging.FileHandler(config["logging"]["file"])
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    db = make_db(db_path)
    db.connect()

    # Each worker gets its own auth manager (process-local)
    auth_manager = AuthAccountManager(auth_files) if auth_files else AuthAccountManager([])

    scraper = GoogleMapsScraper(
        config=config,
        db=db,
        worker_id=worker_id,
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
            while not STOP_EVENT.is_set():
                store = db.claim_next_store(worker_id=worker_id, max_retries=max_retries)
                if not store:
                    # Check if there are truly no more stores
                    if len(db.get_pending_stores()) == 0:
                        logger.info("No pending stores; worker exiting")
                        break
                    await asyncio.sleep(3)
                    continue

                store_name = (store.get("gmaps_name") or store.get("input_name") or "Unknown")[:40]
                logger.info(f"Claiming store: {store_name}")

                try:
                    status = await scraper.scrape_single_store(store)
                    consecutive_crashes = 0  # reset on success
                    processed += 1

                    # Update shared counters
                    if status == "completed":
                        with _completed_counter.get_lock():
                            _completed_counter.value += 1
                        review_count = db.get_review_count(store["store_id"])
                        with _reviews_counter.get_lock():
                            _reviews_counter.value += review_count
                    elif status == "failed":
                        with _failed_counter.get_lock():
                            _failed_counter.value += 1

                    if status == "interrupted":
                        await asyncio.sleep(5)

                    # Restart browser periodically to avoid memory leaks
                    if restart_every > 0 and processed % restart_every == 0:
                        await scraper.restart_browser()

                except Exception as exc:
                    # Browser crash, TargetClosedError, timeout, etc.
                    consecutive_crashes += 1
                    logger.warning(
                        f"Store '{store_name}' crashed: {type(exc).__name__}: {str(exc)[:120]}"
                    )
                    db.mark_store_failed(store["store_id"], f"Browser crash: {type(exc).__name__}")
                    with _failed_counter.get_lock():
                        _failed_counter.value += 1
                    processed += 1

                    if consecutive_crashes >= 5:
                        logger.error("5 consecutive crashes — worker stopping")
                        break

                    # Restart browser and continue to next store
                    logger.info("Restarting browser after crash...")
                    try:
                        await scraper.close_browser()
                    except Exception:
                        pass
                    await asyncio.sleep(3)
                    try:
                        await scraper.start_browser()
                        logger.info("Browser restarted successfully")
                    except Exception as restart_err:
                        logger.error(f"Failed to restart browser: {restart_err}")
                        break
                    continue

                # Stop if all accounts exhausted
                if auth_files and not auth_manager.has_accounts():
                    logger.error("All auth accounts exhausted — worker stopping")
                    STOP_EVENT.set()
                    break

        finally:
            try:
                await scraper.close_browser()
            except Exception:
                pass
            db.release_worker_stores(worker_id)
            db.close()

    # Note: Do NOT set WindowsSelectorEventLoopPolicy here.
    # Playwright requires ProactorEventLoop (the default on Windows) for subprocess creation.
    
    try:
        asyncio.run(loop())
    except KeyboardInterrupt:
        pass


def _eta_monitor(db_path: str, total_stores: int, num_workers: int) -> None:
    """Background process: prints live ETA and progress updates."""
    tracker = ETATracker(total_stores=total_stores)
    logger = logging.getLogger("eta")

    while not STOP_EVENT.is_set():
        time.sleep(30)  # Update every 30 seconds

        # Read shared counters
        completed = _completed_counter.value
        reviews = _reviews_counter.value
        failed = _failed_counter.value

        tracker.completed = completed
        tracker.failed = failed
        tracker.total_reviews = reviews

        if tracker.processed > 0:
            print(tracker.progress_line(num_workers=num_workers), flush=True)

        # Exit if all done
        if tracker.processed >= total_stores:
            break

    # Final summary
    print("\n" + tracker.summary(num_workers=num_workers), flush=True)


def _report_process(db_path: str, interval_hours: int = 6) -> None:
    """Background process: generates Excel reports at regular intervals."""
    from .report_generator import generate_report

    logger = logging.getLogger("report-scheduler")
    interval_secs = interval_hours * 3600

    while not STOP_EVENT.is_set():
        time.sleep(interval_secs)
        if STOP_EVENT.is_set():
            break
        try:
            path = generate_report(db_path)
            logger.info(f"Periodic report generated: {path}")
            print(f"\n  Report saved: {path}\n", flush=True)
        except Exception as e:
            logger.error(f"Report generation failed: {e}")

    # Final report on exit
    try:
        path = generate_report(db_path)
        logger.info(f"Final report generated: {path}")
        print(f"\n  Final report saved: {path}\n", flush=True)
    except Exception as e:
        logger.error(f"Final report generation failed: {e}")


def _launch_workers(
    config: dict,
    db_path: str,
    auth_files: list[str],
    num_workers: int,
    machine_id: str = "",
    logger: logging.Logger | None = None,
) -> list[multiprocessing.Process]:
    """Start N worker processes and return the list."""
    import sys as _sys

    if logger is None:
        logger = logging.getLogger("orchestrator")

    current_sys_path = _sys.path.copy()
    workers: list[multiprocessing.Process] = []

    for i in range(1, num_workers + 1):
        # Assign accounts round-robin to workers
        if auth_files:
            worker_auth = [auth_files[(i - 1) % len(auth_files)]]
        else:
            worker_auth = []

        proc = multiprocessing.Process(
            target=worker_process,
            args=(i, config, worker_auth, db_path, current_sys_path, machine_id),
            name=f"Worker-{i}",
        )
        proc.start()
        workers.append(proc)
        logger.info(f"Started Worker-{i} pid={proc.pid}")

    return workers


def _wait_for_workers(workers: list[multiprocessing.Process]) -> None:
    """Wait for all workers to finish."""
    while any(p.is_alive() for p in workers):
        time.sleep(1)


def main(
    db_path: str,
    config: dict | None = None,
    auth_files: list[str] | None = None,
    machine_id: str = "",
) -> None:
    """Launch multi-worker scraping with live ETA tracking."""
    if config is None:
        config = load_config()
    _configure_logging(config)
    logger = logging.getLogger("orchestrator")

    # Init schema and reset any interrupted stores from last run
    with make_db(db_path) as db:
        db.init_schema()
        reset = db.reset_interrupted_stores()
        if reset:
            logger.info(f"Reset {reset} interrupted stores")
        reset_failed = db.reset_failed_stores()
        if reset_failed:
            logger.info(f"Reset {reset_failed} failed stores for retry")
        total_stores = len(db.get_pending_stores())

    # Build the auth account pool
    if auth_files is None:
        auth_files = AuthAccountManager.list_auth_files(os.path.dirname(os.path.abspath(db_path)))

    auth_manager = AuthAccountManager(auth_files)
    logger.info(auth_manager.status_summary())

    # Reset shared counters
    _completed_counter.value = 0
    _reviews_counter.value = 0
    _failed_counter.value = 0

    count = int(config["workers"]["count"])

    # Start ETA monitor process
    eta_proc = multiprocessing.Process(
        target=_eta_monitor,
        args=(db_path, total_stores, count),
        name="ETA-Monitor",
        daemon=True,
    )
    eta_proc.start()

    # Start report process
    report_interval = int(config.get("report_interval_hours", 6))
    report_proc = multiprocessing.Process(
        target=_report_process,
        args=(db_path, report_interval),
        name="Report-Generator",
        daemon=True,
    )
    report_proc.start()

    # Launch workers
    workers = _launch_workers(
        config, db_path, auth_files, count,
        machine_id=machine_id, logger=logger,
    )

    def handle_signal(signum: int, _frame: object) -> None:
        logger.info(f"Signal {signum}; stopping workers")
        STOP_EVENT.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        _wait_for_workers(workers)
    finally:
        STOP_EVENT.set()
        for proc in workers:
            proc.join(timeout=6)
            if proc.is_alive():
                proc.terminate()
        eta_proc.join(timeout=3)
        if eta_proc.is_alive():
            eta_proc.terminate()
        report_proc.join(timeout=3)
        if report_proc.is_alive():
            report_proc.terminate()
        logger.info("Workers stopped")
