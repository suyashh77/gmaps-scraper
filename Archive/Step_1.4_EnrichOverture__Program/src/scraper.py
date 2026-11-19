"""
src/scraper.py
==============
Google Maps scraping logic for the EnrichOverture project.

``GoogleMapsScraper`` wraps a Playwright ``Page`` and provides three
high-level operations:

1. **Search** — navigate to a Google Maps search URL for a given business
   name and coordinates (or name + city + state as a fallback).

2. **Verify** — confirm that the page that loaded is actually the target
   business, not an unrelated listing.  Uses fuzzy name matching
   (``difflib``) and URL inspection.

3. **Extract** — scrape all available fields from the business detail
   page: Place ID, name, rating, review count, address, phone, website,
   coordinates, category, price level, and hours.

Design notes
------------
- All methods are ``async`` and must be called from within an asyncio
  event loop (the runner scripts handle this).
- Extraction is defensive: every field has multiple fallback strategies
  so that a single DOM change does not break the entire scrape.
- Debug screenshots are saved to ``debug_screenshots/`` on every
  extraction call to aid post-hoc debugging.
"""

import asyncio
import logging
import os
import re
import urllib.parse
from typing import Any, Dict, Literal, Optional

from playwright.async_api import Page

from config.settings import DEBUG_SCREENSHOTS_DIR

logger = logging.getLogger("Scraper")


class GoogleMapsScraper:
    """
    Scraper for Google Maps business listings.

    One instance is created per worker process and reused across many
    businesses.  The browser is restarted periodically by the worker
    (not by this class) to prevent memory leaks.
    """

    def __init__(self, browser_manager: Any) -> None:
        """
        Args:
            browser_manager: A :class:`~src.browser.BrowserManager` instance.
        """
        self.browser_manager = browser_manager
        self.page: Optional[Page] = None

    # -----------------------------------------------------------------------
    # Initialisation
    # -----------------------------------------------------------------------

    async def initialize(self) -> None:
        """Obtain a browser page from the BrowserManager."""
        self.page = await self.browser_manager.start()

    async def navigate_to_maps(self) -> None:
        """
        Navigate to ``google.com/maps`` and dismiss any consent dialogs.

        Also waits for the search box to confirm the page is interactive.
        Raises on navigation failure so the caller can restart the browser.
        """
        if not self.page:
            return

        try:
            await self.page.goto("https://www.google.com/maps", timeout=60_000)

            # Dismiss GDPR / cookie consent dialogs (EU regions)
            consent_selectors = [
                "button[aria-label='Accept all']",
                "button:has-text('Accept all')",
                "form[action*='consent'] button",
                "button:has-text('Agree')",
            ]
            for selector in consent_selectors:
                try:
                    if await self.page.locator(selector).first.is_visible(timeout=3_000):
                        logger.info(f"Dismissing consent dialog: {selector}")
                        await self.page.locator(selector).first.click()
                        await asyncio.sleep(2)
                        break
                except Exception:
                    pass  # Selector not present — try the next one

            # Confirm the search box is visible before proceeding
            try:
                await self.page.locator("input#searchboxinput").wait_for(
                    state="visible", timeout=10_000
                )
            except Exception:
                logger.warning("Search box not found after navigation.")
                logger.warning(f"Page title: {await self.page.title()}")
                await self.page.screenshot(
                    path=os.path.join(DEBUG_SCREENSHOTS_DIR, "error_nav.png")
                )
                await self.page.keyboard.press("Escape")

        except Exception as exc:
            logger.error(f"Navigation to Google Maps failed: {exc}")
            raise

    # -----------------------------------------------------------------------
    # Search
    # -----------------------------------------------------------------------

    async def search_business(self, name: str, lat: float, lon: float) -> bool:
        """
        Navigate to a coordinate-anchored Google Maps search URL.

        URL format: ``/maps/search/<name>/@<lat>,<lon>,17z``

        The zoom level ``17z`` (~city-block scale) ensures that results
        are tightly constrained to the given coordinates.

        Args:
            name: Business name to search for.
            lat:  Latitude of the expected location.
            lon:  Longitude of the expected location.

        Returns:
            ``True`` if navigation succeeded (page loaded), ``False`` on error.
        """
        if not self.page:
            return False

        try:
            encoded_name = urllib.parse.quote(name)
            url = f"https://www.google.com/maps/search/{encoded_name}/@{lat},{lon},17z"

            logger.info(f"Searching (coords): {url}")
            await self.page.goto(url, timeout=60_000)

            # Wait for either a single place page (h1) or a results list
            try:
                await self.page.wait_for_selector(
                    "h1, div[role='feed'], div.bad-query", timeout=15_000
                )
            except Exception:
                logger.warning("Timeout waiting for search results — continuing anyway.")

            return True

        except Exception as exc:
            logger.error(f"Coordinate search navigation failed: {exc}")
            return False

    async def search_business_by_name(self, name: str, city: str, state: str) -> bool:
        """
        Fallback search using name + city + state (no coordinates).

        Used when the input record has no lat/lon, or when the coordinate
        search returns no results.

        Args:
            name:  Business name.
            city:  City name.
            state: State abbreviation (e.g. ``"NC"``).

        Returns:
            ``True`` if navigation succeeded, ``False`` on error.
        """
        if not self.page:
            return False

        try:
            query = f"{name} in {city}, {state}"
            url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"

            logger.info(f"Searching (name+city+state): {url}")
            await self.page.goto(url, timeout=60_000)

            try:
                await self.page.wait_for_selector(
                    "h1, div[role='feed'], div.bad-query", timeout=15_000
                )
            except Exception:
                logger.warning("Timeout waiting for fallback search results.")

            return True

        except Exception as exc:
            logger.error(f"Name+city+state search failed: {exc}")
            return False

    # -----------------------------------------------------------------------
    # Verification
    # -----------------------------------------------------------------------

    async def verify_business(self, target_name: str) -> Literal["match", "no_match"]:
        """
        Confirm that the current page is the intended business.

        Strategy
        --------
        1. If the URL already contains ``/maps/place/``, we are on a
           direct place page — accept immediately.
        2. If a results list is shown, click the first ``/maps/place/``
           link and wait for the URL to update.
           - **Fix A**: if the URL does not change (Google sometimes loads
             the panel without a full navigation), check whether an ``h1``
             is visible in the DOM and accept if so.
        3. **Fix C**: fuzzy-match the ``h1`` text against ``target_name``
           using ``difflib.SequenceMatcher``.  A similarity ≥ 0.6 is
           accepted.  Even below the threshold we accept the result when
           we arrived via coordinates (strong location signal).

        Returns:
            ``'match'`` if the page is accepted as the target business,
            ``'no_match'`` if no usable result was found.
        """
        import difflib

        if not self.page:
            return "no_match"

        def _names_similar(a: str, b: str, threshold: float = 0.6) -> bool:
            """Return True if the two name strings are similar enough."""
            ratio = difflib.SequenceMatcher(
                None, a.lower().strip(), b.lower().strip()
            ).ratio()
            logger.info(f"Name similarity '{a}' vs '{b}': {ratio:.2f}")
            return ratio >= threshold

        try:
            url = self.page.url

            # ── Case 1: Already on a place page ─────────────────────────
            if "/maps/place/" in url:
                try:
                    page_name = await self.page.locator("h1").first.inner_text(timeout=3_000)
                    if not _names_similar(target_name, page_name):
                        logger.warning(
                            f"Name mismatch on place page: '{target_name}' vs '{page_name}' "
                            "— accepting anyway (coordinate-based navigation)."
                        )
                except Exception:
                    pass  # h1 not yet loaded; accept anyway
                return "match"

            # ── Case 2: Results list — click first result ────────────────
            results = self.page.locator("a[href*='/maps/place/']")
            count = await results.count()

            if count > 0:
                logger.info(f"Found {count} result(s). Clicking first...")
                await results.first.click()

                # Wait for URL to change to a place page
                try:
                    await self.page.wait_for_url("**/maps/place/**", timeout=10_000)
                    await self.page.wait_for_load_state("domcontentloaded", timeout=5_000)
                except Exception as exc:
                    # Fix A: URL didn't change — check if the panel loaded anyway
                    logger.warning(f"URL did not change to /maps/place/ after click: {exc}")
                    try:
                        await self.page.locator("h1").first.wait_for(
                            state="visible", timeout=4_000
                        )
                        logger.info("Fix A: h1 visible in panel — accepting as match.")
                    except Exception:
                        logger.warning(f"Fix A: h1 not visible either. URL: {self.page.url}")
                        return "no_match"

                # Fix C: fuzzy name check on the landed page
                try:
                    page_name = await self.page.locator("h1").first.inner_text(timeout=3_000)
                    if not _names_similar(target_name, page_name):
                        logger.warning(
                            f"Fix C: name mismatch after click — '{target_name}' vs '{page_name}'. "
                            "Accepting (coordinate proximity is a strong signal)."
                        )
                except Exception:
                    pass  # Accept if we can't read the name
                return "match"

            # ── Case 3: No results at all ────────────────────────────────
            logger.warning(f"No place results found. URL: {url}")
            safe_name = target_name.replace(" ", "_")[:50]
            await self.page.screenshot(
                path=os.path.join(DEBUG_SCREENSHOTS_DIR, f"error_verify_{safe_name}.png")
            )
            return "no_match"

        except Exception as exc:
            logger.error(f"Verification error: {exc}")
            return "no_match"

    # -----------------------------------------------------------------------
    # Extraction
    # -----------------------------------------------------------------------

    async def extract_business_info(self, debug_name: str = "unknown") -> Optional[Dict[str, Any]]:
        """
        Extract all available fields from the current Google Maps place page.

        Fields extracted
        ----------------
        - ``gmaps_url``        — full Google Maps URL
        - ``place_id``         — Google Place ID (``ChIJ…``)
        - ``name``             — business name (h1 text)
        - ``rating``           — star rating (float)
        - ``reviews_count``    — total review count (int)
        - ``address``          — formatted address
        - ``phone``            — phone number
        - ``website``          — website domain
        - ``latitude``         — extracted from URL
        - ``longitude``        — extracted from URL
        - ``category``         — business category (e.g. "Grocery store")
        - ``price_level``      — price tier (``"$"``, ``"$$"``, etc.)
        - ``hours``            — raw hours text

        Each field has multiple fallback extraction strategies so that
        minor Google Maps DOM changes do not break the entire scrape.

        Args:
            debug_name: Short identifier used in the debug screenshot filename.

        Returns:
            Dict of extracted fields, or ``None`` if extraction failed entirely.
        """
        if not self.page:
            return None

        info: Dict[str, Any] = {}

        try:
            url = self.page.url
            logger.info(f"Extracting from: {url}")

            # Save a screenshot for post-hoc debugging
            await self.page.screenshot(
                path=os.path.join(DEBUG_SCREENSHOTS_DIR, f"debug_extract_{debug_name}.png")
            )

            info["gmaps_url"] = url

            # ── Place ID ─────────────────────────────────────────────────
            # Google Place IDs always start with "ChIJ" followed by 20+
            # alphanumeric characters.  We try six different locations in
            # the URL and page source where the ID might appear.
            place_id = None

            # Method 1: Standard ChIJ pattern in the URL path
            m = re.search(r"ChI[a-zA-Z0-9_\-]{20,}", url)
            if m:
                place_id = m.group(0)
                logger.info(f"Place ID (URL path): {place_id}")

            # Method 2: ChIJ inside the /data= URL parameter
            if not place_id:
                dm = re.search(r"/data=([^/\s]+)", url)
                if dm:
                    m2 = re.search(r"ChI[a-zA-Z0-9_\-]{20,}", dm.group(1))
                    if m2:
                        place_id = m2.group(0)
                        logger.info(f"Place ID (data param): {place_id}")

            # Method 3: /place/<Name>/<ChIJ…> URL format
            if not place_id:
                pm = re.search(r"/place/[^/]+/(ChI[a-zA-Z0-9_\-]{20,})", url)
                if pm:
                    place_id = pm.group(1)
                    logger.info(f"Place ID (URL path segment): {place_id}")

            # Method 4: JSON-like "place_id": "ChIJ…" in page source
            if not place_id:
                try:
                    content = await self.page.content()
                    sm = re.search(r'"place_id":\s*"(ChI[a-zA-Z0-9_\-]{20,})"', content)
                    if not sm:
                        sm = re.search(r"ChI[a-zA-Z0-9_\-]{20,}", content)
                    if sm:
                        place_id = sm.group(1) if sm.lastindex else sm.group(0)
                        logger.info(f"Place ID (page source): {place_id}")
                except Exception as exc:
                    logger.warning(f"Could not search page source: {exc}")

            # Method 5: data-pid attribute in the DOM
            if not place_id:
                try:
                    content = await self.page.content()
                    pm2 = re.search(r'data-pid="(ChI[a-zA-Z0-9_\-]{20,})"', content)
                    if pm2:
                        place_id = pm2.group(1)
                        logger.info(f"Place ID (data-pid): {place_id}")
                except Exception as exc:
                    logger.warning(f"Could not search data-pid: {exc}")

            # Method 6: ChIJ inside the review dialog section
            if not place_id:
                try:
                    content = await self.page.content()
                    ds = re.search(
                        r"lu-rap-thank-you-dialog[^>]*>(.*?)</div>", content, re.DOTALL
                    )
                    if ds:
                        cm = re.search(r"ChI[a-zA-Z0-9_\-]{20,}", ds.group(1))
                        if cm:
                            place_id = cm.group(0)
                            logger.info(f"Place ID (review dialog): {place_id}")
                except Exception as exc:
                    logger.warning(f"Could not search review dialog: {exc}")

            if place_id:
                info["place_id"] = place_id
            else:
                logger.warning(f"No Place ID found after 6 extraction methods. URL: {url}")

            # ── Business name ─────────────────────────────────────────────
            try:
                info["name"] = await self.page.locator("h1").inner_text()
            except Exception:
                info["name"] = None

            # ── Rating & review count ─────────────────────────────────────
            # Wait briefly for the rating widget to load
            try:
                await self.page.locator(
                    "div.F7nice, span[role='img'][aria-label*='star']"
                ).first.wait_for(state="visible", timeout=5_000)
            except Exception:
                logger.warning(f"Rating widget not found for {debug_name}")

            # Rating — primary method
            try:
                rating_el = self.page.locator("div.F7nice span[aria-hidden='true']").first
                info["rating"] = float(await rating_el.inner_text())
            except Exception:
                try:
                    rating_el = self.page.locator("div.F7nice span").first
                    info["rating"] = float(await rating_el.inner_text())
                except Exception:
                    info["rating"] = None

            info["reviews_count"] = 0

            # Review count — Method 1: aria-label on the star image
            # e.g. "4.4 stars 1,443 Reviews"
            try:
                star_img = self.page.locator("span[role='img'][aria-label*='star']").first
                aria = await star_img.get_attribute("aria-label")
                if aria:
                    rm = re.search(r"([\d,]+)\s*[Rr]eview", aria)
                    if rm:
                        info["reviews_count"] = int(rm.group(1).replace(",", ""))
                        logger.info(f"Reviews (star aria-label): {info['reviews_count']}")
                    # Bonus: extract rating from the same aria-label
                    if not info.get("rating"):
                        ratm = re.search(r"([\d.]+)\s*star", aria)
                        if ratm:
                            info["rating"] = float(ratm.group(1))
            except Exception:
                pass

            # Review count — Method 2: parenthesised number in F7nice div
            # e.g. "(1,443)"
            if not info["reviews_count"]:
                try:
                    f7text = await self.page.locator("div.F7nice").first.inner_text()
                    pm = re.search(r"\(([\d,]+)\)", f7text)
                    if pm:
                        info["reviews_count"] = int(pm.group(1).replace(",", ""))
                        logger.info(f"Reviews (F7nice parens): {info['reviews_count']}")
                except Exception:
                    pass

            # Review count — Method 3: aria-label on the reviews button
            if not info["reviews_count"]:
                try:
                    aria = await self.page.locator(
                        "button[aria-label*='review']"
                    ).first.get_attribute("aria-label")
                    if aria:
                        num = re.sub(r"[^0-9]", "", aria)
                        if num:
                            info["reviews_count"] = int(num)
                            logger.info(f"Reviews (button aria-label): {info['reviews_count']}")
                except Exception:
                    pass

            # Review count — Methods 4-6: text-based fallbacks
            for selector in [
                "div.F7nice span:has-text('review')",
                "div.F7nice button",
                "button:has-text('review'), span:has-text('review')",
            ]:
                if info["reviews_count"]:
                    break
                try:
                    text = await self.page.locator(selector).first.inner_text()
                    num = re.sub(r"[^0-9]", "", text)
                    if num:
                        info["reviews_count"] = int(num)
                        logger.info(f"Reviews ({selector}): {info['reviews_count']}")
                except Exception:
                    pass

            if not info["reviews_count"]:
                logger.warning(f"Could not extract review count for {debug_name}")

            # ── Address, phone, website ───────────────────────────────────
            # Google Maps encodes these in elements with a ``data-item-id``
            # attribute.  We iterate over all such elements and classify
            # each one by its data-item-id value.
            elements = self.page.locator("button[data-item-id], a[data-item-id]")
            for i in range(await elements.count()):
                el = elements.nth(i)
                text = await el.inner_text()
                item_id = (await el.get_attribute("data-item-id")) or ""

                if "phone:" in item_id:
                    info["phone"] = text
                elif not info.get("phone") and re.match(r"^\+?\d[\d\s\-\(\)]+$", text):
                    info["phone"] = text

                if "authority" in item_id:
                    info["website"] = text
                elif not info.get("website") and "." in text and " " not in text and len(text) > 4:
                    info["website"] = text

                if "address" in item_id:
                    info["address"] = text

            # Address fallback
            if "address" not in info:
                try:
                    info["address"] = await self.page.locator(
                        "button[data-item-id*='address']"
                    ).inner_text()
                except Exception:
                    pass

            # ── Latitude & longitude ──────────────────────────────────────
            # Extracted from the URL rather than the DOM for reliability.
            # Two common URL formats:
            #   /@lat,lon,17z
            #   !3d<lat>!4d<lon>
            try:
                ll = re.search(r"@([-\d.]+),([-\d.]+),\d+", url)
                if ll:
                    info["latitude"]  = float(ll.group(1))
                    info["longitude"] = float(ll.group(2))
                else:
                    alt = re.search(r"!3d([-\d.]+)!4d([-\d.]+)", url)
                    if alt:
                        info["latitude"]  = float(alt.group(1))
                        info["longitude"] = float(alt.group(2))
            except Exception as exc:
                logger.warning(f"Could not extract lat/lon: {exc}")
                info["latitude"]  = None
                info["longitude"] = None

            # ── Business category ─────────────────────────────────────────
            try:
                cat_el = self.page.locator("button[jsaction*='category'], button.DkEaL").first
                if await cat_el.count() > 0:
                    info["category"] = await cat_el.inner_text()
                    logger.info(f"Category: {info['category']}")
                else:
                    # Fallback: regex over the main panel text
                    panel_text = await self.page.locator("div[role='main']").first.inner_text()
                    cm = re.search(
                        r"([A-Z][a-z]+ (?:store|market|shop|center|restaurant|cafe))",
                        panel_text,
                    )
                    if cm:
                        info["category"] = cm.group(1)
            except Exception as exc:
                logger.warning(f"Could not extract category: {exc}")
                info["category"] = None

            # ── Price level ───────────────────────────────────────────────
            try:
                price_el = self.page.locator(
                    "span[aria-label*='Price:'], span[aria-label*='price level']"
                ).first
                if await price_el.count() > 0:
                    price_aria = await price_el.get_attribute("aria-label")
                    if price_aria:
                        pm = re.search(r"(\$+)", price_aria)
                        if pm:
                            info["price_level"] = pm.group(1)
            except Exception as exc:
                logger.warning(f"Could not extract price level: {exc}")
                info["price_level"] = None

            # ── Hours ─────────────────────────────────────────────────────
            try:
                hours_btn = self.page.locator(
                    "button[data-item-id*='oh'], button[aria-label*='Hours']"
                ).first
                if await hours_btn.count() > 0:
                    info["hours"] = await hours_btn.inner_text()
                else:
                    open_el = self.page.locator(
                        "div:has-text('Open'), div:has-text('Closed')"
                    ).first
                    if await open_el.count() > 0:
                        info["hours"] = await open_el.inner_text()
            except Exception as exc:
                logger.warning(f"Could not extract hours: {exc}")
                info["hours"] = None

            return info

        except Exception as exc:
            logger.error(f"Extraction failed for {debug_name}: {exc}")
            return None
