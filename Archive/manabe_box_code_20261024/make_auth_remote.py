"""
Google login for a headless Linux box: creates auth/google_auth_N.json ON THIS PC.

    xvfb-run -a .venv/bin/python make_auth_remote.py --slot 1

Session files made on another computer get signed out the first time this PC
uses them, so each account has to be logged in from here. There is no screen,
so the browser runs on a virtual display (xvfb-run) with a DevTools port on
127.0.0.1 that you drive from your own Chrome:

  1. Forward the port to your machine: VS Code "Ports" tab → Forward 9222
     (or: ssh -L 9222:127.0.0.1:9222 <this host>).
  2. In your local Chrome open chrome://inspect/#devices → Configure… →
     add localhost:9222 → under Remote Target click "inspect" on the
     accounts.google.com tab. The page appears live; log in there (2FA too).
  3. Nothing to press here: once Google Maps shows the account signed in, the
     session is saved and the script exits.

The browser uses the same launch settings and the same fixed user agent as the
scraper, so Google sees one consistent browser from login onward.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

from linux_scraper.config import load_config
from linux_scraper.scraper import GoogleMapsScraper

HERE = Path(__file__).resolve().parent
AUTH_DIR = HERE / "auth"


async def _has_session_cookie(scraper: GoogleMapsScraper) -> bool:
    cookies = await scraper.context.cookies("https://www.google.com")
    return any(c["name"] == "__Secure-1PSID" for c in cookies)


async def main_async(slot: int, port: int, timeout_min: int) -> int:
    config = load_config()
    config["browser"]["headless"] = False
    config["browser"]["stable_user_agent"] = True
    config["browser"]["extra_args"] = [
        f"--remote-debugging-port={port}",
        "--remote-debugging-address=127.0.0.1",
    ]
    scraper = GoogleMapsScraper(config=config)
    await scraper.start_browser()
    out_path = AUTH_DIR / f"google_auth_{slot}.json"
    try:
        await scraper.page.goto(
            "https://accounts.google.com", wait_until="domcontentloaded", timeout=45000
        )
        print(f"Browser ready on 127.0.0.1:{port} — log in via chrome://inspect "
              f"(will save to {out_path.name})", flush=True)

        deadline = time.monotonic() + timeout_min * 60
        while time.monotonic() < deadline:
            await asyncio.sleep(5)
            url = scraper.page.url
            if not await _has_session_cookie(scraper):
                continue
            if "accounts.google.com" in url and ("signin" in url or "challenge" in url):
                continue  # still mid-login (2FA, "verify it's you")

            await scraper.page.goto(
                "https://www.google.com/maps", wait_until="domcontentloaded", timeout=45000
            )
            await asyncio.sleep(3)
            if await scraper.is_signed_out():
                print("Maps still shows 'Sign in' — finish logging in.", flush=True)
                await scraper.page.goto("https://accounts.google.com",
                                        wait_until="domcontentloaded", timeout=45000)
                continue

            AUTH_DIR.mkdir(exist_ok=True)
            await scraper.context.storage_state(path=str(out_path))
            print(f"Saved: {out_path}", flush=True)
            return 0

        print(f"Timed out after {timeout_min} min without a signed-in session.", flush=True)
        return 1
    finally:
        scraper.current_auth_file = None  # don't let close_browser write anywhere
        await scraper.close_browser()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--slot", type=int, required=True,
                    help="write auth/google_auth_<slot>.json (replaces it)")
    ap.add_argument("--port", type=int, default=9222)
    ap.add_argument("--timeout-min", type=int, default=30)
    args = ap.parse_args()
    return asyncio.run(main_async(args.slot, args.port, args.timeout_min))


if __name__ == "__main__":
    sys.exit(main())
