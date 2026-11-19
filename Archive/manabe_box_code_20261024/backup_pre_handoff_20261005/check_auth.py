"""Report whether each auth/google_auth_*.json is still signed in to Google Maps.

    .venv/bin/python check_auth.py
"""
import asyncio
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from linux_scraper.config import load_config  # noqa: E402
from linux_scraper.scraper import GoogleMapsScraper  # noqa: E402


async def check(path: str) -> bool:
    s = GoogleMapsScraper(config=load_config(), db=None, worker_id=0)
    await s.start_browser(auth_file=path)
    try:
        await s.page.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(6)
        return not await s.is_signed_out()
    finally:
        await s.close_browser()


async def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    files = sorted(glob.glob(os.path.join(here, "auth", "google_auth*.json")))
    if not files:
        print("no auth/google_auth*.json files")
    for f in files:
        ok = await check(f)
        print(f"{os.path.basename(f)}: {'SIGNED IN' if ok else 'SIGNED OUT — replace this file'}")


asyncio.run(main())
