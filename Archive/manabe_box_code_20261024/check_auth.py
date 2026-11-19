"""
Print SIGNED IN / SIGNED OUT for each auth/google_auth*.json session.

    python check_auth.py

Loads each saved session, opens Google Maps, and checks the same signal the
scraper itself checks (GoogleMapsScraper.is_signed_out): a visible "Sign in"
link. A signed-out session doesn't fail loudly — Maps just quietly serves a
degraded page where Sort opens a sign-in modal instead of a menu — so this is
meant to be run before a batch, not diagnosed after the fact from the scrape
log.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from linux_scraper.config import load_config
from linux_scraper.scraper import GoogleMapsScraper

HERE = Path(__file__).resolve().parent
AUTH_DIR = HERE / "auth"


async def check_one(path: Path) -> str:
    config = load_config()
    config["browser"]["headless"] = True
    scraper = GoogleMapsScraper(config=config)
    await scraper.start_browser(auth_file=str(path))
    try:
        await scraper.page.goto(
            "https://www.google.com/maps", wait_until="domcontentloaded", timeout=45000
        )
        await asyncio.sleep(3)
        return "SIGNED OUT" if await scraper.is_signed_out() else "SIGNED IN"
    finally:
        await scraper.close_browser()


async def main_async() -> int:
    files = sorted(AUTH_DIR.glob("google_auth_*.json"))
    if not files:
        print(f"No auth files found in {AUTH_DIR}")
        return 1
    any_signed_out = False
    for path in files:
        status = await check_one(path)
        any_signed_out = any_signed_out or status == "SIGNED OUT"
        print(f"{path.name}: {status}")
    return 1 if any_signed_out else 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
