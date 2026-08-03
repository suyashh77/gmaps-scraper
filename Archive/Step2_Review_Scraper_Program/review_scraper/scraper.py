"""
Google Maps scraper: store-based (DB) and query-based modes, captcha handling, review extraction.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import signal
from datetime import datetime
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .config import load_config
from .database import DatabaseManager

PAUSE_FLAG = Path("pause.flag")
CAPTCHA_INDICATORS = [
    "unusual traffic",
    "automated queries",
    "not a robot",
    "captcha",
    "recaptcha",
    "sorry/index",
    "systems have detected",
]

USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def _logger(config: dict[str, Any], worker_id: int = 0) -> logging.Logger:
    name = "scraper" if worker_id == 0 else f"worker-{worker_id}"
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO)
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.FileHandler(config["logging"]["file"]))
    for h in logger.handlers:
        h.setFormatter(fmt)
    return logger


class GoogleMapsScraper:
    def __init__(
        self,
        config: dict[str, Any],
        db: DatabaseManager | None = None,
        worker_id: int = 0,
    ):
        self.config = config
        self.db = db
        self.worker_id = worker_id
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self._pw = None
        self._shutdown = False
        self.logger = _logger(config, worker_id)

    async def start_browser(self) -> None:
        laptop = self.config.get("laptop", {})
        ua_list = laptop.get("user_agents") or USER_AGENT_POOL
        user_agent = random.choice(ua_list)
        w = random.randint(laptop.get("viewport_width_min", 1280), laptop.get("viewport_width_max", 1920))
        h = random.randint(800, 1080)
        self._pw = await async_playwright().start()
        self.browser = await self._pw.chromium.launch(
            headless=bool(self.config["browser"]["headless"]),
            args=["--disable-blink-features=AutomationControlled"],
        )
        self.context = await self.browser.new_context(
            user_agent=user_agent,
            viewport={"width": w, "height": h},
            locale="en-US",
            timezone_id="America/New_York",
        )
        self.page = await self.context.new_page()
        self.logger.info("Browser started")

    async def close_browser(self) -> None:
        try:
            if self.browser:
                await self.browser.close()
        finally:
            self.browser = None
            self.context = None
            self.page = None
            if self._pw:
                await self._pw.stop()
            self._pw = None

    async def restart_browser(self) -> None:
        await self.close_browser()
        await asyncio.sleep(random.uniform(1.5, 3.5))
        await self.start_browser()

    async def _handle_cookies(self) -> None:
        for sel in [
            "button[aria-label*='Accept all']",
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
        ]:
            try:
                btn = self.page.locator(sel).first
                if await btn.is_visible(timeout=1800):
                    await btn.click()
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue

    async def _is_on_business_page(self) -> bool:
        for sel in ["h1.DUwDvf", "h1.fontHeadlineLarge", "div[role='main'] h1"]:
            try:
                if await self.page.locator(sel).first.is_visible(timeout=2000):
                    return True
            except Exception:
                continue
        return False

    async def _click_first_result_if_needed(self) -> None:
        if await self._is_on_business_page():
            return
        for sel in ["a.hfpxzc", "div[role='feed'] a.hfpxzc", "div.Nv2PK a"]:
            try:
                loc = self.page.locator(sel)
                if await loc.count() > 0:
                    await loc.first.click()
                    await asyncio.sleep(random.uniform(3.5, 5.5))
                    return
            except Exception:
                continue

    async def check_for_captcha(self) -> bool:
        try:
            haystack = (self.page.url + " " + (await self.page.content())).lower()
            return any(t in haystack for t in CAPTCHA_INDICATORS)
        except Exception:
            return False

    def trigger_global_pause(self, reason: str = "CAPTCHA detected") -> None:
        PAUSE_FLAG.write_text(f"{datetime.now().isoformat()} | {reason}\n", encoding="utf-8")
        self.logger.warning(f"Global pause: {reason}")

    async def wait_if_paused(self) -> None:
        if not PAUSE_FLAG.exists():
            return
        cfg = self.config["rate_limiting"]
        pause = random.uniform(cfg["captcha_pause_min"], cfg["captcha_pause_max"])
        self.logger.warning(f"Pause flag found. Sleeping {pause:.0f}s")
        await asyncio.sleep(pause)
        try:
            PAUSE_FLAG.unlink(missing_ok=True)
        except Exception:
            pass
        await self.restart_browser()

    async def _goto_query(self, query: str) -> bool:
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        await self.page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(random.uniform(3.5, 5.5))
        await self._handle_cookies()
        await self._click_first_result_if_needed()
        return await self._is_on_business_page()

    async def navigate_to_store(self, store: Any) -> bool:
        name = (store["gmaps_name"] or store["input_name"] or "").strip()
        address = (store["gmaps_address"] or store["input_street"] or "").strip()
        city = (store["input_city"] or "").strip()
        place_id = (store["google_place_id"] or "").strip()
        lat = store.get("gmaps_lat") or store.get("input_lat")
        lon = store.get("gmaps_lon") or store.get("input_lon")
        attempts = []
        if place_id and place_id.startswith("ChIJ"):
            attempts.append(f"https://www.google.com/maps/place/?q=place_id:{place_id}")
        if lat and lon:
            q = f"{name} {city}".strip().replace(" ", "+")
            attempts.append(f"https://www.google.com/maps/search/{q}/@{lat},{lon},15z")
        attempts.append(f"https://www.google.com/maps/search/{(name + ' ' + address + ' ' + city).strip().replace(' ', '+')}")
        if city and name:
            attempts.append(f"https://www.google.com/maps/search/{(name + ' ' + city).strip().replace(' ', '+')}")
        for idx, url in enumerate(attempts, start=1):
            try:
                self.logger.info(f"Navigate strategy {idx}")
                await self.page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(random.uniform(3.0, 6.0))
                await self._handle_cookies()
                await self._click_first_result_if_needed()
                if await self._is_on_business_page():
                    return True
            except Exception:
                continue
        return False

    async def _extract_place_id_from_url(self) -> str | None:
        m = re.search(r"(ChIJ[\w-]{20,})", self.page.url)
        if m:
            return m.group(1)
        m = re.search(r"!1s(0x[0-9a-f]+:0x[0-9a-f]+)", self.page.url)
        if m:
            return m.group(1)
        return None

    async def verify_store(self, store: Any) -> bool:
        target_place_id = (store.get("google_place_id") or "").strip()
        if target_place_id:
            current = await self._extract_place_id_from_url()
            if current and current == target_place_id:
                return True
        target_name = (store.get("gmaps_name") or store.get("input_name") or "").lower().strip()
        page_name = ""
        for sel in ["h1.DUwDvf", "h1.fontHeadlineLarge", "div[role='main'] h1"]:
            try:
                page_name = (await self.page.locator(sel).first.inner_text(timeout=2000)).strip()
                if page_name:
                    break
            except Exception:
                continue
        if not target_name:
            return bool(page_name)
        if not page_name:
            return False
        page_name_l = page_name.lower()
        if target_name in page_name_l or page_name_l in target_name:
            return True
        target_words = [w for w in re.findall(r"[a-z0-9]+", target_name) if len(w) >= 3]
        if not target_words:
            return True
        matches = sum(1 for w in target_words if w in page_name_l)
        return matches >= max(1, int(len(target_words) * 0.5))

    async def is_permanently_closed(self) -> bool:
        for token in ["Permanently closed", "Temporarily closed"]:
            try:
                if await self.page.locator(f"text='{token}'").first.is_visible(timeout=1500):
                    return True
            except Exception:
                continue
        return False

    async def _extract_total_reviews(self) -> int:
        try:
            btn = self.page.locator("button[aria-label*='reviews']").first
            if await btn.is_visible(timeout=2000):
                label = await btn.get_attribute("aria-label")
                if label:
                    m = re.search(r"([\d,]+)\s+reviews?", label, re.IGNORECASE)
                    if m:
                        return int(m.group(1).replace(",", ""))
        except Exception:
            pass
        try:
            texts = await self.page.locator("div.F7nice span, span.UY7F9").all_inner_texts()
            for t in texts:
                m = re.search(r"([\d,]+)", t)
                if m:
                    val = int(m.group(1).replace(",", ""))
                    if val > 1:
                        return val
        except Exception:
            pass
        return 0

    async def extract_business_info(self) -> dict[str, Any]:
        info = {
            "name": "",
            "address": "",
            "phone": "",
            "website": "",
            "overall_rating": None,
            "total_reviews": 0,
            "category": "",
        }
        try:
            info["name"] = await self.page.locator("h1.DUwDvf, h1.fontHeadlineLarge").first.inner_text(timeout=3000)
        except Exception:
            pass
        try:
            rt = await self.page.locator("div.F7nice span[aria-hidden='true']").first.inner_text(timeout=1500)
            info["overall_rating"] = float(rt.replace(",", "."))
        except Exception:
            pass
        info["total_reviews"] = await self._extract_total_reviews()
        try:
            info["address"] = (await self.page.locator("button[data-item-id='address']").get_attribute("aria-label", timeout=1200) or "").replace("Address: ", "")
        except Exception:
            pass
        try:
            info["phone"] = (await self.page.locator("button[data-item-id*='phone:tel']").get_attribute("aria-label", timeout=1200) or "").replace("Phone: ", "")
        except Exception:
            pass
        try:
            info["website"] = await self.page.locator("a[data-item-id='authority']").get_attribute("href", timeout=1200)
        except Exception:
            pass
        try:
            info["category"] = await self.page.locator("button.DkEaL").first.inner_text(timeout=1000)
        except Exception:
            pass
        return info

    async def _click_reviews_tab(self) -> bool:
        try:
            if await self.page.locator("div.jftiEf").count() > 0:
                return True
        except Exception:
            pass
        for sel in [
            "button[aria-label*='Reviews']",
            "button[role='tab']:has-text('Reviews')",
            "div[role='tab']:has-text('Reviews')",
            "button.hh2c6:has-text('Reviews')",
            "button:has(span[aria-label*='review'])",
        ]:
            try:
                btn = self.page.locator(sel).first
                if await btn.is_visible(timeout=2500):
                    await btn.click()
                    await asyncio.sleep(1.5)
                    if await self.page.locator("div.jftiEf").count() > 0:
                        return True
            except Exception:
                continue
        return False

    async def _sort_by_newest(self) -> None:
        if not self.config["scraping"].get("sort_by_newest", True):
            return
        for sort_btn in [
            "button[aria-label*='Sort reviews']",
            "button[data-value='Sort']",
            "button.g88MCb",
        ]:
            try:
                btn = self.page.locator(sort_btn).first
                if await btn.is_visible(timeout=2500):
                    await btn.click()
                    await asyncio.sleep(1.2)
                    for new_opt in [
                        "div[role='menuitemradio']:has-text('Newest')",
                        "div[role='menuitem']:has-text('Newest')",
                        "li[data-index='1']",
                    ]:
                        opt = self.page.locator(new_opt).first
                        if await opt.is_visible(timeout=2200):
                            await opt.click()
                            await asyncio.sleep(random.uniform(2.0, 3.5))
                            return
            except Exception:
                continue

    async def _get_scroll_position(self) -> tuple[float, float]:
        for sel in ["div.jftiEf", "div.m6QErb.DxyBCb", "div.m6QErb.DByNcb", "div.m6QErb"]:
            try:
                node = self.page.locator(sel).first
                box = await node.bounding_box(timeout=2000)
                if box:
                    return box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
            except Exception:
                continue
        return 400.0, 450.0

    async def _extract_review(self, el: Any) -> dict[str, Any] | None:
        try:
            rid = await el.get_attribute("data-review-id")
            if not rid:
                return None
            try:
                more_btn = el.locator("button[aria-label*='See more']").first
                if await more_btn.is_visible(timeout=200):
                    await more_btn.click()
            except Exception:
                pass
            name, rating, date_rel, review_text = "Anonymous", None, "", ""
            try:
                name = await el.locator("div.d4r55").first.inner_text(timeout=500)
            except Exception:
                pass
            try:
                aria = await el.locator("span.kvMYJc").first.get_attribute("aria-label", timeout=500)
                if aria:
                    rating = int(re.findall(r"\d+", aria)[0])
            except Exception:
                pass
            try:
                date_rel = await el.locator("span.rsqaWe").first.inner_text(timeout=500)
            except Exception:
                pass
            try:
                review_text = await el.locator("span.wiI7pd").first.inner_text(timeout=500)
            except Exception:
                pass
            helpful = photo_count = has_owner_response = reviewer_review_count = reviewer_photo_count = is_local_guide = 0
            service_type = ""
            try:
                stats = await el.locator("div.RfnDt").first.inner_text(timeout=500)
                if "Local Guide" in stats:
                    is_local_guide = 1
                m = re.search(r"(\d[\d,]*)\s+review", stats)
                if m:
                    reviewer_review_count = int(m.group(1).replace(",", ""))
                m = re.search(r"(\d[\d,]*)\s+photo", stats)
                if m:
                    reviewer_photo_count = int(m.group(1).replace(",", ""))
            except Exception:
                pass
            try:
                lbl = await el.locator("button[aria-label*='helpful']").first.get_attribute("aria-label", timeout=350)
                if lbl:
                    m = re.search(r"(\d+)", lbl)
                    if m:
                        helpful = int(m.group(1))
            except Exception:
                pass
            try:
                photo_count = await el.locator("div.Iop04 img").count()
            except Exception:
                pass
            try:
                if await el.locator("text='Response from the owner'").first.is_visible(timeout=250):
                    has_owner_response = 1
            except Exception:
                pass
            try:
                attrs = await el.locator("div.PBK6be span.pVtsbf").all_inner_texts()
                if attrs:
                    service_type = ", ".join(attrs)
            except Exception:
                pass
            return {
                "review_id": rid,
                "reviewer_name": name,
                "rating": rating,
                "date_relative": date_rel,
                "review_text": review_text,
                "helpful_count": helpful,
                "photo_count": photo_count,
                "has_owner_response": has_owner_response,
                "reviewer_review_count": reviewer_review_count,
                "reviewer_photo_count": reviewer_photo_count,
                "is_local_guide": is_local_guide,
                "service_type": service_type,
            }
        except Exception:
            return None

    async def _collect_reviews(
        self,
        target_count: int,
        store_id: int | None = None,
    ) -> int | list[dict[str, Any]]:
        if not await self._click_reviews_tab():
            return 0 if store_id else []
        await self._sort_by_newest()
        max_reviews = min(max(int(target_count), 1), int(self.config["scraping"]["max_reviews_per_store"]))
        existing_ids: set[str] = set()
        if store_id and self.db:
            existing_ids = self.db.get_existing_review_ids(store_id)
        seen_ids: set[str] = set(existing_ids)
        total_collected = len(existing_ids)
        buffer: list[dict[str, Any]] = []
        in_memory: list[dict[str, Any]] = []
        scroll_x, scroll_y = await self._get_scroll_position()
        await self.page.mouse.move(scroll_x, scroll_y)
        stall_cycles = 0
        scroll_attempts = 0
        max_scrolls = int(self.config["scraping"]["max_scroll_attempts"])
        stall_threshold = int(self.config["scraping"]["stall_threshold"])
        batch_size = int(self.config["scraping"]["batch_save_interval"])
        rl = self.config["rate_limiting"]

        while total_collected < max_reviews and scroll_attempts < max_scrolls and not self._shutdown:
            if await self.check_for_captcha():
                self.trigger_global_pause("CAPTCHA during review collection")
                break
            visible = await self.page.locator("div.jftiEf").all()
            new_in_cycle = 0
            for el in visible:
                if total_collected >= max_reviews:
                    break
                rid = await el.get_attribute("data-review-id")
                if not rid or rid in seen_ids:
                    continue
                seen_ids.add(rid)
                record = await self._extract_review(el)
                if not record:
                    continue
                new_in_cycle += 1
                total_collected += 1
                if store_id and self.db:
                    buffer.append(record)
                    if len(buffer) >= batch_size:
                        self.db.save_reviews_batch(store_id, buffer)
                        self.db.update_store_reviews_count(store_id, total_collected)
                        buffer = []
                else:
                    in_memory.append(record)
            stall_cycles = stall_cycles + 1 if new_in_cycle == 0 else 0
            if stall_cycles >= stall_threshold:
                break
            try:
                await self.page.locator("div.jftiEf").first.hover()
            except Exception:
                pass
            await self.page.mouse.wheel(0, random.randint(2800, 6200))
            await asyncio.sleep(random.uniform(rl["min_scroll_delay"], rl["max_scroll_delay"]))
            scroll_attempts += 1
            idle_every = int(rl.get("idle_pause_every_n_scrolls", 0))
            if idle_every > 0 and scroll_attempts % idle_every == 0:
                await asyncio.sleep(random.uniform(rl["idle_pause_min"], rl["idle_pause_max"]))
        if store_id and self.db and buffer:
            self.db.save_reviews_batch(store_id, buffer)
            self.db.update_store_reviews_count(store_id, total_collected)
        return total_collected if store_id else in_memory

    async def scrape_reviews(self, store_id: int, target_count: int) -> int:
        result = await self._collect_reviews(target_count=target_count, store_id=store_id)
        return int(result)

    async def scrape_query(self, query: str, max_reviews: int = 1500) -> list[dict[str, Any]]:
        await self.wait_if_paused()
        ok = await self._goto_query(query)
        if not ok:
            raise RuntimeError(f"Could not open a business page for query: {query}")
        if await self.is_permanently_closed():
            return []
        business = await self.extract_business_info()
        live = int(business.get("total_reviews") or 0)
        target = min(1500, max(max_reviews, live if live > 0 else max_reviews))
        self.logger.info(f"Query mode target: {target} reviews")
        reviews = await self._collect_reviews(target_count=target, store_id=None)
        return reviews

    async def scrape_single_store(self, store: Any) -> str:
        store_id = store["store_id"]
        await self.wait_if_paused()
        try:
            if not await self.navigate_to_store(store):
                raise RuntimeError("Failed to navigate to store page")
            if await self.check_for_captcha():
                self.trigger_global_pause("CAPTCHA after navigation")
                return "interrupted"
            if not await self.verify_store(store):
                raise RuntimeError("Store verification failed")
            if await self.is_permanently_closed():
                if self.db:
                    self.db.mark_store_skipped(store_id, "Permanently/temporarily closed")
                return "skipped"
            info = await self.extract_business_info()
            live_count = int(info.get("total_reviews") or 0)
            target = min(
                int(self.config["scraping"]["max_reviews_per_store"]),
                max(live_count, int(store.get("target_reviews") or 1500)),
            )
            collected = await self.scrape_reviews(store_id=store_id, target_count=target)
            if self.db:
                final_count = self.db.get_review_count(store_id)
                self.db.mark_store_completed(store_id, final_count)
            else:
                final_count = collected
            self.logger.info(f"Store completed: {final_count} reviews")
            return "completed"
        except Exception as exc:
            if self.db:
                self.db.mark_store_failed(store_id, str(exc))
            self.logger.error(f"Store failed: {exc}")
            return "failed"

    def _setup_signal_handlers(self) -> None:
        def handler(signum: int, _frame: Any) -> None:
            self.logger.warning(f"Signal {signum}; shutting down")
            self._shutdown = True

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    async def run(self) -> None:
        if not self.db:
            raise RuntimeError("Database manager required for store mode")
        self._setup_signal_handlers()
        self.db.reset_interrupted_stores()
        session_id = self.db.start_session()
        await self.start_browser()
        stores = self.db.get_pending_stores()
        processed = completed = failed = reviews_collected = 0
        restart_every = int(self.config["browser"]["restart_every_n_stores"])
        max_retries = int(self.config["retry"]["max_retries"])
        try:
            for idx, store in enumerate(stores):
                if self._shutdown:
                    break
                if store["attempts"] >= max_retries:
                    self.db.mark_store_failed(store["store_id"], "Max retries exceeded")
                    failed += 1
                    processed += 1
                    continue
                if idx > 0 and restart_every > 0 and idx % restart_every == 0:
                    await self.restart_browser()
                self.db.mark_store_in_progress(store["store_id"], self.worker_id or None)
                result = await self.scrape_single_store(store)
                processed += 1
                if result == "completed":
                    completed += 1
                    reviews_collected += int(self.db.get_review_count(store["store_id"]))
                elif result == "failed":
                    failed += 1
                if idx < len(stores) - 1 and not self._shutdown:
                    await asyncio.sleep(
                        random.uniform(
                            self.config["rate_limiting"]["min_delay_between_stores"],
                            self.config["rate_limiting"]["max_delay_between_stores"],
                        )
                    )
        finally:
            await self.close_browser()
            status = "interrupted" if self._shutdown else "completed"
            self.db.end_session(session_id, processed, completed, failed, reviews_collected, status)
