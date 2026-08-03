"""
src/browser.py
==============
Playwright browser lifecycle management for the EnrichOverture scraper.

``BrowserManager`` launches a stealth Chromium instance, creates a
realistic browser context (user-agent, viewport, locale, geolocation),
and exposes a single ``Page`` object for the scraper to use.

Stealth mode (via ``undetected_playwright``) patches common bot-detection
signals so Google Maps does not block or rate-limit the scraper.

Typical usage
-------------
    browser_mgr = BrowserManager()
    page = await browser_mgr.start()
    # ... use page ...
    await browser_mgr.close()
"""

import asyncio
import logging
from typing import Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
)
from undetected_playwright import stealth_async

from config.settings import HEADLESS

logger = logging.getLogger(__name__)


class BrowserManager:
    """
    Manages the full lifecycle of a Playwright Chromium browser session.

    Each worker process creates its own ``BrowserManager`` instance so
    that browser sessions are fully isolated from one another.
    """

    def __init__(self) -> None:
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    async def start(self) -> Page:
        """
        Launch Chromium, create a stealth context, and return the page.

        The browser context mimics a real Windows Chrome user:
        - Realistic user-agent string
        - 1920×1080 viewport
        - US English locale and Eastern timezone
        - Geolocation set to New York (prevents Google from showing
          consent dialogs for non-US regions)

        Returns:
            The ready-to-use :class:`playwright.async_api.Page`.
        """
        logger.info("Starting browser...")
        self.playwright = await async_playwright().start()

        # Disable the AutomationControlled flag that browsers expose by default
        launch_args = ["--disable-blink-features=AutomationControlled"]
        if not HEADLESS:
            # Maximise window when running in headed (debug) mode
            launch_args.append("--start-maximized")

        self.browser = await self.playwright.chromium.launch(
            headless=HEADLESS,
            args=launch_args,
        )

        # Build a context that looks like a real desktop Chrome session
        self.context = await self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
            permissions=["geolocation"],
            geolocation={"latitude": 40.7128, "longitude": -74.0060},  # New York City
        )

        self.page = await self.context.new_page()

        # Apply stealth patches to hide Playwright fingerprints
        await stealth_async(self.page)

        logger.info("Browser started with stealth mode enabled.")
        return self.page

    async def close(self) -> None:
        """
        Gracefully close the page, context, browser, and Playwright instance.

        Safe to call even if ``start()`` was never called or failed
        partway through — each resource is checked for ``None`` before
        closing.
        """
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser closed.")

    async def restart(self) -> Page:
        """
        Close the current session and start a fresh one.

        Workers call this periodically (every N businesses) to prevent
        memory leaks and session fingerprinting.

        Returns:
            A fresh :class:`playwright.async_api.Page`.
        """
        logger.info("Restarting browser session...")
        await self.close()
        return await self.start()
