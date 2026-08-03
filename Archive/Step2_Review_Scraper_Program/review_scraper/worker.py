"""Multi-process worker: claims stores from DB and runs scraper in a loop."""

import asyncio
import logging
import multiprocessing
import signal
import time

from .config import load_config
from .database import DatabaseManager
from .scraper import GoogleMapsScraper

STOP_EVENT = multiprocessing.Event()


def _configure_logging(config: dict) -> None:
    logging.basicConfig(
        level=getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO),
        format="%(asctime)s [ORCHESTRATOR] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("orchestrator.log"),
        ],
    )


def worker_process(worker_id: int, config: dict) -> None:
    logger = logging.getLogger(f"worker-{worker_id}")
    db = DatabaseManager(config["database"]["path"])
    db.connect()
    scraper = GoogleMapsScraper(config=config, db=db, worker_id=worker_id)
    restart_every = int(config["workers"]["restart_browser_every"])
    max_retries = int(config["retry"]["max_retries"])

    async def loop() -> None:
        await scraper.start_browser()
        processed = 0
        try:
            while not STOP_EVENT.is_set():
                store = db.claim_next_store(worker_id=worker_id, max_retries=max_retries)
                if not store:
                    if len(db.get_pending_stores()) == 0:
                        logger.info("No pending stores; worker exiting")
                        break
                    await asyncio.sleep(3)
                    continue
                status = await scraper.scrape_single_store(store)
                processed += 1
                if status == "interrupted":
                    await asyncio.sleep(3)
                if restart_every > 0 and processed % restart_every == 0:
                    await scraper.restart_browser()
        finally:
            await scraper.close_browser()
            db.release_worker_stores(worker_id)
            db.close()

    asyncio.run(loop())


def main(config_path: str | None = None) -> None:
    config = load_config(config_path)
    _configure_logging(config)
    logger = logging.getLogger("orchestrator")

    with DatabaseManager(config["database"]["path"]) as db:
        db.init_schema()
        reset = db.reset_interrupted_stores()
        logger.info(f"Reset {reset} interrupted stores")

    workers: list[multiprocessing.Process] = []
    count = int(config["workers"]["count"])
    for i in range(1, count + 1):
        proc = multiprocessing.Process(
            target=worker_process,
            args=(i, config),
            name=f"Worker-{i}",
        )
        proc.start()
        workers.append(proc)
        logger.info(f"Started Worker-{i} pid={proc.pid}")

    def handle_signal(signum: int, _frame: object) -> None:
        logger.info(f"Signal {signum}; stopping workers")
        STOP_EVENT.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while any(p.is_alive() for p in workers):
            time.sleep(1)
    finally:
        STOP_EVENT.set()
        for proc in workers:
            proc.join(timeout=6)
            if proc.is_alive():
                proc.terminate()
        logger.info("Workers stopped")
