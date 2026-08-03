"""Playwright scraper engine for Google Maps reviews."""

from __future__ import annotations

import asyncio
import logging
import random
import re
from pathlib import Path
from typing import Any

from playwright.async_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Locator,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from . import selectors
from .models import BusinessProfile, ReviewRecord, ScrapeResult
from .utils import build_maps_search_url, parse_rating_from_aria, safe_float, safe_int


class CaptchaDetectedError(RuntimeError):
    """Raised when a captcha/block page is detected."""


class QueryResolutionError(RuntimeError):
    """Raised when query search cannot reach a business details page."""


class ReviewsPanelError(RuntimeError):
    """Raised when reviews panel interaction fails."""


class GoogleMapsReviewsScraper:
    """Headless Google Maps query scraper focused on reviews extraction."""

    def __init__(self, config: dict[str, Any], logger: logging.Logger | None = None):
        self.config = config
        self.logger = logger or logging.getLogger("gmaps_reviews_scraper")
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    async def start(self) -> None:
        """Launch browser and create one context/page session."""
        if self.page:
            return
        browser_cfg = self.config["browser"]
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=bool(browser_cfg["headless"]),
            args=["--disable-blink-features=AutomationControlled"],
        )

        width = random.randint(int(browser_cfg["viewport_width_min"]), int(browser_cfg["viewport_width_max"]))
        height = random.randint(int(browser_cfg["viewport_height_min"]), int(browser_cfg["viewport_height_max"]))
        user_agent = random.choice(browser_cfg["user_agents"])

        self.context = await self.browser.new_context(
            user_agent=user_agent,
            viewport={"width": width, "height": height},
            locale=browser_cfg["locale"],
            timezone_id=browser_cfg["timezone_id"],
        )
        self.page = await self.context.new_page()
        self.page.set_default_timeout(int(browser_cfg["action_timeout_ms"]))
        self.page.set_default_navigation_timeout(int(browser_cfg["navigation_timeout_ms"]))
        self.logger.info("Browser started (headless=%s)", browser_cfg["headless"])

    async def close(self) -> None:
        """Close browser and Playwright resources."""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        finally:
            self.page = None
            self.context = None
            self.browser = None
            if self.playwright:
                await self.playwright.stop()
            self.playwright = None

    async def restart(self) -> None:
        """Restart browser session, usually after recoverable failures."""
        await self.close()
        await asyncio.sleep(random.uniform(1.0, 2.0))
        await self.start()

    async def scrape_query(self, query: str, max_reviews: int | None = None) -> ScrapeResult:
        """Scrape one query with retry and backoff.

        The query is considered successful if we can open the business page,
        open reviews, sort by newest (best effort), and collect reviews until
        one stop condition is reached.
        """
        if not self.page:
            raise RuntimeError("start() must be called before scrape_query().")

        configured_cap = int(self.config["scraping"]["max_reviews_per_query"])
        hard_cap = min(1500, configured_cap)
        effective_cap = hard_cap if max_reviews is None else min(hard_cap, int(max_reviews))
        retry_attempts = int(self.config["scraping"]["max_query_retries"]) + 1
        backoff_seconds = float(self.config["scraping"]["retry_backoff_sec"])

        for attempt in range(1, retry_attempts + 1):
            try:
                return await self._scrape_query_once(query=query, max_reviews=effective_cap)
            except CaptchaDetectedError:
                # Captcha generally does not recover quickly; bubble up immediately.
                raise
            except Exception as exc:
                if attempt >= retry_attempts:
                    raise
                delay = backoff_seconds * attempt
                self.logger.warning(
                    "Attempt %s/%s failed for query '%s': %s. Retrying in %.1fs.",
                    attempt,
                    retry_attempts,
                    query,
                    exc,
                    delay,
                )
                await self.restart()
                await asyncio.sleep(delay)

        raise RuntimeError("Unreachable retry state.")

    async def _scrape_query_once(self, query: str, max_reviews: int) -> ScrapeResult:
        assert self.page is not None
        await self._open_business_page(query)

        if await self._is_captcha_page():
            raise CaptchaDetectedError("Captcha detected after query navigation.")

        if await self._is_business_closed():
            business = await self._extract_business_profile(query=query)
            return ScrapeResult(
                query=query,
                business=business,
                reviews=[],
                target_reviews=0,
                reached_limit=False,
                stopped_reason="business_closed",
            )

        business = await self._extract_business_profile(query=query)
        if not business.name:
            raise QueryResolutionError("Could not extract business name from Google Maps page.")

        if business.total_reviews <= 0:
            return ScrapeResult(
                query=query,
                business=business,
                reviews=[],
                target_reviews=0,
                reached_limit=False,
                stopped_reason="no_reviews_on_profile",
            )

        target_reviews = min(max_reviews, business.total_reviews)
        opened_reviews = await self._open_reviews_panel()
        if not opened_reviews:
            raise ReviewsPanelError("Reviews tab could not be opened.")

        if bool(self.config["scraping"]["sort_by_newest"]):
            sorted_ok = await self._sort_by_newest()
            if not sorted_ok:
                self.logger.warning("Could not confirm sort-by-newest action. Continuing anyway.")

        reviews, stopped_reason = await self._collect_reviews(
            query=query,
            business_name=business.name,
            target_reviews=target_reviews,
        )
        return ScrapeResult(
            query=query,
            business=business,
            reviews=reviews,
            target_reviews=target_reviews,
            reached_limit=len(reviews) >= target_reviews,
            stopped_reason=stopped_reason,
        )

    async def _open_business_page(self, query: str) -> None:
        """Open search URL, handle consent, and click first result when needed."""
        assert self.page is not None
        search_url = build_maps_search_url(query)
        await self.page.goto(search_url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(2.0, 3.5))
        await self._handle_cookie_consent()

        if await self._is_business_page():
            return

        await self._wait_for_result_list()
        clicked = await self._click_first_result_if_needed()
        if not clicked:
            raise QueryResolutionError(f"No business result found for query: {query}")

        if not await self._is_business_page():
            raise QueryResolutionError(f"Query did not resolve to a business page: {query}")

    async def _handle_cookie_consent(self) -> None:
        assert self.page is not None
        for selector in selectors.COOKIE_ACCEPT_SELECTORS:
            try:
                button = self.page.locator(selector).first
                if await button.is_visible(timeout=1500):
                    await button.click()
                    await asyncio.sleep(0.5)
                    self.logger.debug("Accepted cookie dialog with selector: %s", selector)
                    return
            except PlaywrightError:
                continue

    async def _is_business_page(self) -> bool:
        assert self.page is not None
        # If a result feed is present, we are still in search-result mode.
        # This check must happen before URL checks because Google can keep
        # `/maps/place/` in the URL while still rendering a list/feed layout.
        try:
            result_cards = await self.page.locator("div[role='feed'] div[role='article']").count()
            if result_cards > 0:
                return False
        except PlaywrightError:
            pass

        if "/maps/place/" in self.page.url:
            return True

        for selector in selectors.BUSINESS_TITLE_SELECTORS:
            try:
                title_node = self.page.locator(selector).first
                if not await title_node.is_visible(timeout=1500):
                    continue
                title_text = (await title_node.inner_text(timeout=1500)).strip().lower()
                if title_text in {"results", "search results"}:
                    continue
                if title_text:
                    return True
            except PlaywrightError:
                continue
        return False

    async def _click_first_result_if_needed(self) -> bool:
        assert self.page is not None
        for attempt in range(1, 5):
            for selector in selectors.SEARCH_RESULT_LINK_SELECTORS:
                locator = self.page.locator(selector)
                try:
                    count = await locator.count()
                except PlaywrightError:
                    continue
                if count == 0:
                    continue

                first = locator.first
                try:
                    await first.scroll_into_view_if_needed(timeout=1000)
                except PlaywrightError:
                    pass

                # Prefer direct navigation when href is available.
                try:
                    href = await first.get_attribute("href")
                    if href:
                        target_url = href if href.startswith("http") else f"https://www.google.com{href}"
                        await self.page.goto(target_url, wait_until="domcontentloaded")
                        await asyncio.sleep(random.uniform(2.0, 3.0))
                        if await self._is_business_page():
                            return True
                except PlaywrightError:
                    pass

                clicked = False
                for click_mode in ("normal", "force", "js"):
                    try:
                        if click_mode == "normal":
                            await first.click(timeout=2500)
                        elif click_mode == "force":
                            await first.click(timeout=2500, force=True)
                        else:
                            handle = await first.element_handle(timeout=2500)
                            if handle is None:
                                continue
                            await self.page.evaluate("(el) => el.click()", handle)
                        clicked = True
                        break
                    except PlaywrightError:
                        continue

                if not clicked:
                    continue

                await asyncio.sleep(random.uniform(2.0, 3.0))
                if await self._is_business_page():
                    return True
            await asyncio.sleep(0.8 * attempt)
        return False

    async def _wait_for_result_list(self) -> None:
        """Wait briefly for results list hydration before first-result click attempts."""
        assert self.page is not None
        for _ in range(10):
            if await self._is_business_page():
                return
            for selector in selectors.SEARCH_RESULT_LINK_SELECTORS:
                try:
                    if await self.page.locator(selector).count() > 0:
                        return
                except PlaywrightError:
                    continue
            await asyncio.sleep(0.5)

    async def _is_captcha_page(self) -> bool:
        assert self.page is not None
        try:
            haystack = (self.page.url + " " + await self.page.content()).lower()
        except PlaywrightError:
            return False
        for token in self.config["runtime"]["captcha_keywords"]:
            if token.lower() in haystack:
                return True
        return False

    async def _is_business_closed(self) -> bool:
        assert self.page is not None
        for token in selectors.CLOSED_TOKENS:
            try:
                if await self.page.locator(f"text='{token}'").first.is_visible(timeout=800):
                    return True
            except PlaywrightError:
                continue
        return False

    async def _extract_business_profile(self, query: str) -> BusinessProfile:
        """Extract high-level metadata for the currently open business page."""
        assert self.page is not None
        name = await self._first_inner_text_from_page(selectors.BUSINESS_NAME_SELECTORS)
        rating_text = await self._first_inner_text_from_page(selectors.BUSINESS_RATING_SELECTORS)
        address_label = await self._first_attribute_from_page(selectors.BUSINESS_ADDRESS_SELECTORS, "aria-label")
        phone_label = await self._first_attribute_from_page(selectors.BUSINESS_PHONE_SELECTORS, "aria-label")
        website = await self._first_attribute_from_page(selectors.BUSINESS_WEBSITE_SELECTORS, "href")
        category = await self._first_inner_text_from_page(selectors.BUSINESS_CATEGORY_SELECTORS)
        total_reviews = await self._extract_total_reviews()

        return BusinessProfile(
            query=query,
            name="" if name.strip().lower() == "results" else name,
            address=address_label.replace("Address: ", ""),
            phone=phone_label.replace("Phone: ", ""),
            website=website,
            category=category,
            overall_rating=safe_float(rating_text),
            total_reviews=total_reviews,
            maps_url=self.page.url,
        )

    async def _extract_total_reviews(self) -> int:
        """Get total review count from aria-label or fallback text snippets."""
        assert self.page is not None
        for selector in selectors.BUSINESS_REVIEWS_BUTTON_SELECTORS:
            try:
                button = self.page.locator(selector).first
                if not await button.is_visible(timeout=1000):
                    continue
                label = await button.get_attribute("aria-label")
                if label:
                    parsed = safe_int(label, default=0)
                    if parsed > 0:
                        return parsed
            except PlaywrightError:
                continue

        highest = 0
        for selector in selectors.BUSINESS_REVIEWS_COUNT_TEXT_SELECTORS:
            try:
                texts = await self.page.locator(selector).all_inner_texts()
            except PlaywrightError:
                continue
            for text in texts:
                candidate = safe_int(text, default=0)
                if candidate > highest:
                    highest = candidate
        return highest

    async def _open_reviews_panel(self) -> bool:
        assert self.page is not None
        if await self._review_card_count() > 0:
            return True

        for selector in selectors.REVIEWS_TAB_SELECTORS:
            try:
                tab = self.page.locator(selector).first
                if not await tab.is_visible(timeout=1200):
                    continue
                await tab.click()
                await asyncio.sleep(1.5)
                if await self._review_card_count() > 0:
                    return True
            except PlaywrightError:
                continue

        for selector in selectors.REVIEWS_ENTRYPOINT_SELECTORS:
            try:
                entrypoints = self.page.locator(selector)
                count = min(await entrypoints.count(), 5)
            except PlaywrightError:
                continue

            for index in range(count):
                if await self._try_open_reviews_from_entrypoint(entrypoints.nth(index)):
                    return True

        return False

    async def _try_open_reviews_from_entrypoint(self, entrypoint: Locator) -> bool:
        """Try to open reviews from a non-tab element (rating chip, write-review button)."""
        assert self.page is not None
        try:
            if not await entrypoint.is_visible(timeout=1000):
                return False
        except PlaywrightError:
            return False

        for click_mode in ("normal", "force", "js"):
            try:
                if click_mode == "normal":
                    await entrypoint.click(timeout=2000)
                elif click_mode == "force":
                    await entrypoint.click(timeout=2000, force=True)
                else:
                    handle = await entrypoint.element_handle(timeout=2000)
                    if handle is None:
                        continue
                    await self.page.evaluate("(el) => el.click()", handle)
            except PlaywrightError:
                continue

            await asyncio.sleep(1.5)
            if await self._review_card_count() > 0:
                return True

        return False

    async def _sort_by_newest(self) -> bool:
        assert self.page is not None
        for sort_selector in selectors.SORT_BUTTON_SELECTORS:
            try:
                sort_button = self.page.locator(sort_selector).first
                if not await sort_button.is_visible(timeout=1500):
                    continue
                await sort_button.click()
                await asyncio.sleep(0.8)
            except PlaywrightError:
                continue

            for newest_selector in selectors.NEWEST_OPTION_SELECTORS:
                try:
                    newest_option = self.page.locator(newest_selector).first
                    if await newest_option.is_visible(timeout=1200):
                        await newest_option.click()
                        await asyncio.sleep(random.uniform(1.0, 1.8))
                        return True
                except PlaywrightError:
                    continue
        return False

    async def _collect_reviews(
        self,
        query: str,
        business_name: str,
        target_reviews: int,
    ) -> tuple[list[ReviewRecord], str]:
        """Scroll review panel and extract unique reviews until stopping criteria."""
        assert self.page is not None
        scraped: list[ReviewRecord] = []
        seen_review_ids: set[str] = set()
        max_scroll_attempts = int(self.config["scraping"]["max_scroll_attempts"])
        stall_threshold = int(self.config["scraping"]["stall_threshold"])
        min_delay = float(self.config["scraping"]["min_scroll_delay_sec"])
        max_delay = float(self.config["scraping"]["max_scroll_delay_sec"])
        # Performance guardrails: once the review list gets large, we scan a
        # bounded tail window plus overlap instead of rescanning every card.
        tail_scan_cap = 220
        overlap_window = 40

        stall_cycles = 0
        scroll_attempts = 0
        last_seen_card_count = 0

        while len(scraped) < target_reviews and scroll_attempts < max_scroll_attempts:
            if await self._is_captcha_page():
                raise CaptchaDetectedError("Captcha detected while collecting reviews.")

            cards = await self._get_review_cards_locator()
            card_count = await cards.count()
            new_in_cycle = 0

            if card_count <= tail_scan_cap:
                scan_start = 0
            else:
                baseline_start = max(0, card_count - tail_scan_cap)
                if card_count > last_seen_card_count:
                    # New cards were appended; start near previous high-water
                    # mark to avoid rescanning old reviews.
                    scan_start = max(baseline_start, last_seen_card_count - overlap_window)
                else:
                    # Card count unchanged (or virtualized list); rescan a tail slice.
                    scan_start = baseline_start

            for index in range(scan_start, card_count):
                if len(scraped) >= target_reviews:
                    break
                card = cards.nth(index)
                # Fast-path dedupe before expensive card parsing/expansion.
                try:
                    raw_review_id = await card.get_attribute("data-review-id")
                except PlaywrightError:
                    raw_review_id = None
                if raw_review_id and raw_review_id in seen_review_ids:
                    continue
                review = await self._extract_review_card(card, query=query, business_name=business_name)
                if not review:
                    continue
                if review.review_id in seen_review_ids:
                    continue
                seen_review_ids.add(review.review_id)
                scraped.append(review)
                new_in_cycle += 1

                if len(scraped) % 25 == 0:
                    self.logger.info(
                        "Collected %s/%s reviews for query '%s'",
                        len(scraped),
                        target_reviews,
                        query,
                    )

            last_seen_card_count = max(last_seen_card_count, card_count)

            if new_in_cycle == 0:
                stall_cycles += 1
            else:
                stall_cycles = 0

            if stall_cycles >= stall_threshold:
                return scraped, "stalled_no_new_reviews"

            await self._scroll_reviews_panel()
            scroll_attempts += 1
            await asyncio.sleep(random.uniform(min_delay, max_delay))

        if len(scraped) >= target_reviews:
            return scraped, "target_reached"
        if scroll_attempts >= max_scroll_attempts:
            return scraped, "max_scroll_attempts_reached"
        return scraped, "stopped"

    async def _scroll_reviews_panel(self) -> None:
        assert self.page is not None
        x, y = await self._review_panel_center()
        delta = random.randint(
            int(self.config["scraping"]["scroll_delta_min"]),
            int(self.config["scraping"]["scroll_delta_max"]),
        )
        await self.page.mouse.move(x, y)
        await self.page.mouse.wheel(0, delta)

    async def _review_panel_center(self) -> tuple[float, float]:
        assert self.page is not None
        for selector in selectors.REVIEW_SCROLL_CONTAINER_SELECTORS:
            try:
                panel = self.page.locator(selector).first
                box = await panel.bounding_box(timeout=1200)
                if box:
                    return box["x"] + (box["width"] / 2), box["y"] + (box["height"] / 2)
            except PlaywrightError:
                continue
        return (450.0, 500.0)

    async def _get_review_cards_locator(self) -> Locator:
        assert self.page is not None
        for selector in selectors.REVIEW_CARD_SELECTORS:
            locator = self.page.locator(selector)
            try:
                if await locator.count() > 0:
                    return locator
            except PlaywrightError:
                continue
        # Fallback locator is still returned so count() can be checked by caller.
        return self.page.locator(selectors.REVIEW_CARD_SELECTORS[0])

    async def _review_card_count(self) -> int:
        cards = await self._get_review_cards_locator()
        try:
            return await cards.count()
        except PlaywrightError:
            return 0

    async def _extract_review_card(
        self,
        card: Locator,
        query: str,
        business_name: str,
    ) -> ReviewRecord | None:
        """Parse one review card.

        Google Maps sometimes renders partial/incomplete cards while loading.
        Parsing is defensive: missing fields are accepted, but missing review_id
        skips the card because deduplication requires it.
        """
        try:
            review_id = await card.get_attribute("data-review-id")
            if not review_id:
                return None

            await self._expand_review_if_needed(card)
            reviewer_name = await self._first_inner_text_from_scope(card, selectors.REVIEWER_NAME_SELECTORS)
            rating_aria = await self._first_attribute_from_scope(card, selectors.REVIEW_RATING_SELECTORS, "aria-label")
            date_relative = await self._first_inner_text_from_scope(card, selectors.REVIEW_DATE_SELECTORS)
            review_text = await self._first_inner_text_from_scope(card, selectors.REVIEW_TEXT_SELECTORS)
            helpful_aria = await self._first_attribute_from_scope(
                card,
                selectors.REVIEW_HELPFUL_BUTTON_SELECTORS,
                "aria-label",
            )
            stats_text = await self._first_inner_text_from_scope(card, selectors.REVIEW_STATS_SELECTORS)

            photo_count = await self._count_elements_in_scope(card, selectors.REVIEW_PHOTO_SELECTORS)
            has_owner_response = await self._is_any_selector_visible_in_scope(
                card,
                selectors.REVIEW_OWNER_RESPONSE_SELECTORS,
            )
            service_type = await self._collect_joined_texts_from_scope(card, selectors.REVIEW_SERVICE_TYPE_SELECTORS)

            is_local_guide = "local guide" in stats_text.lower()
            reviewer_review_count = 0
            reviewer_photo_count = 0

            review_count_match = re.search(r"(\d[\d,]*)\s+review", stats_text, flags=re.IGNORECASE)
            if review_count_match:
                reviewer_review_count = safe_int(review_count_match.group(1), default=0)

            photo_count_match = re.search(r"(\d[\d,]*)\s+photo", stats_text, flags=re.IGNORECASE)
            if photo_count_match:
                reviewer_photo_count = safe_int(photo_count_match.group(1), default=0)

            return ReviewRecord(
                query=query,
                business_name=business_name,
                review_id=review_id,
                reviewer_name=reviewer_name or "Anonymous",
                rating=parse_rating_from_aria(rating_aria),
                date_relative=date_relative,
                review_text=review_text,
                helpful_count=safe_int(helpful_aria, default=0),
                photo_count=photo_count,
                has_owner_response=has_owner_response,
                reviewer_review_count=reviewer_review_count,
                reviewer_photo_count=reviewer_photo_count,
                is_local_guide=is_local_guide,
                service_type=service_type,
            )
        except PlaywrightTimeoutError:
            return None
        except PlaywrightError:
            return None

    async def _expand_review_if_needed(self, card: Locator) -> None:
        for selector in selectors.REVIEW_SEE_MORE_SELECTORS:
            try:
                button = card.locator(selector).first
                if await button.is_visible(timeout=200):
                    await button.click()
                    await asyncio.sleep(0.05)
                    return
            except PlaywrightError:
                continue

    async def _count_elements_in_scope(self, scope: Locator, selector_list: list[str]) -> int:
        counts: list[int] = []
        for selector in selector_list:
            try:
                counts.append(await scope.locator(selector).count())
            except PlaywrightError:
                continue
        return max(counts) if counts else 0

    async def _is_any_selector_visible_in_scope(self, scope: Locator, selector_list: list[str]) -> bool:
        for selector in selector_list:
            try:
                if await scope.locator(selector).first.is_visible(timeout=200):
                    return True
            except PlaywrightError:
                continue
        return False

    async def _collect_joined_texts_from_scope(self, scope: Locator, selector_list: list[str]) -> str:
        for selector in selector_list:
            try:
                texts = await scope.locator(selector).all_inner_texts()
            except PlaywrightError:
                continue
            cleaned = [text.strip() for text in texts if text.strip()]
            if cleaned:
                return ", ".join(cleaned)
        return ""

    async def _first_inner_text_from_page(self, selector_list: list[str]) -> str:
        assert self.page is not None
        for selector in selector_list:
            try:
                node = self.page.locator(selector).first
                if await node.is_visible(timeout=1000):
                    text = (await node.inner_text(timeout=1000)).strip()
                    if text:
                        return text
            except PlaywrightError:
                continue
        return ""

    async def _first_attribute_from_page(self, selector_list: list[str], attribute: str) -> str:
        assert self.page is not None
        for selector in selector_list:
            try:
                node = self.page.locator(selector).first
                if await node.count() == 0:
                    continue
                value = await node.get_attribute(attribute, timeout=800)
                if value:
                    return value.strip()
            except PlaywrightError:
                continue
        return ""

    async def _first_inner_text_from_scope(self, scope: Locator, selector_list: list[str]) -> str:
        for selector in selector_list:
            try:
                node = scope.locator(selector).first
                if await node.count() == 0:
                    continue
                text = (await node.inner_text(timeout=500)).strip()
                if text:
                    return text
            except PlaywrightError:
                continue
        return ""

    async def _first_attribute_from_scope(
        self,
        scope: Locator,
        selector_list: list[str],
        attribute: str,
    ) -> str:
        for selector in selector_list:
            try:
                node = scope.locator(selector).first
                if await node.count() == 0:
                    continue
                value = await node.get_attribute(attribute, timeout=500)
                if value:
                    return value.strip()
            except PlaywrightError:
                continue
        return ""

    async def save_debug_artifacts(self, output_dir: str | Path, file_stem: str) -> dict[str, str]:
        """Save screenshot and page HTML for debugging failed runs."""
        assert self.page is not None
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = out_dir / f"{file_stem}.png"
        html_path = out_dir / f"{file_stem}.html"

        await self.page.screenshot(path=str(screenshot_path), full_page=True)
        html_path.write_text(await self.page.content(), encoding="utf-8")
        return {"screenshot": str(screenshot_path), "html": str(html_path)}
