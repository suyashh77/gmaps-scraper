"""Create a signed-in Google session file for the scraper. Run on a machine with a screen.

    pip install playwright && python -m playwright install chromium
    python make_auth.py google_auth_1.json
    python make_auth.py google_auth_1.json --proxy socks5://127.0.0.1:1080

--proxy sends the login through an SSH tunnel so Google sees the scraping
server's IP, not this PC's:  ssh -N -D 1080 scsapre@manabe.ie.ncsu.edu

A browser opens on Google sign-in. Log in (use the burner account, not a personal
one), wait until Google Maps shows your avatar top-right, then come back here and
press Enter. The session is saved to the file you named.
"""
from __future__ import annotations

import argparse
import asyncio

from playwright.async_api import async_playwright


async def main(out: str, proxy: str | None) -> None:
    launch = {"headless": False, "args": ["--disable-blink-features=AutomationControlled"],
              "ignore_default_args": ["--enable-automation"]}
    if proxy:
        launch["proxy"] = {"server": proxy}
    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch(channel="chrome", **launch)
        except Exception:
            browser = await pw.chromium.launch(**launch)
        ctx = await browser.new_context(locale="en-US")
        page = await ctx.new_page()
        if proxy:
            await page.goto("https://api.ipify.org")
            print(f"Browser exit IP (should be the server's): {await page.inner_text('body')}")
        await page.goto("https://accounts.google.com/ServiceLogin?continue=https://www.google.com/maps")
        await asyncio.get_running_loop().run_in_executor(
            None, input, "\nLog in in the browser window, wait for Google Maps to load, then press Enter here... ")
        await page.goto("https://www.google.com/maps", wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)
        if await page.locator("a[aria-label='Sign in'], a[href*='ServiceLogin']").count():
            print("NOT signed in — Maps still shows 'Sign in'. Nothing saved; try again.")
        else:
            await ctx.storage_state(path=out)
            print(f"Saved signed-in session to {out}")
        await browser.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("out", nargs="?", default="google_auth_1.json")
    ap.add_argument("--proxy", help="e.g. socks5://127.0.0.1:1080 (SSH -D tunnel to the scraping server)")
    a = ap.parse_args()
    asyncio.run(main(a.out, a.proxy))
