"""
Google Maps scraper: store-based (DB) and query-based modes, captcha handling, review extraction.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .auth_manager import AuthAccountManager
from .database import DatabaseManager

PAUSE_FLAG = Path(__file__).parent.parent / "pause.flag"
CAPTCHA_INDICATORS = [
    "unusual traffic",
    "automated queries",
    "not a robot",
    "captcha",
    "recaptcha",
    "sorry/index",
    "systems have detected",
]

# Updated user agents — Chrome 134-135 (current Q1 2026)
USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
]


def _logger(config: dict[str, Any], worker_id: int = 0) -> logging.Logger:
    name = "scraper" if worker_id == 0 else f"worker-{worker_id}"
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO)
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    try:
        log_path = Path(config["logging"]["file"])
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except (OSError, PermissionError):
        pass  # can't write log file — stderr logging still works
    return logger


class GoogleMapsScraper:
    def __init__(
        self,
        config: dict[str, Any],
        db: DatabaseManager | None = None,
        worker_id: int = 0,
        auth_manager: AuthAccountManager | None = None,
        machine_id: str = "",
    ):
        self.config = config
        self.db = db
        self.worker_id = worker_id
        self.auth_manager = auth_manager
        self.machine_id = machine_id
        self.current_auth_file: str | None = None  # track which account is active
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self._pw = None
        self._shutdown = False
        self.logger = _logger(config, worker_id)

    async def start_browser(self, auth_file: str | None = None) -> None:
        """
        Launch Chromium and load a Google auth session.

        Auth priority:
          1. auth_file argument (explicit override)
          2. auth_manager.get_next_account() (pool rotation)
          3. config browser.google_auth_file (single-account fallback)
          4. No auth (limited view — Reviews tab hidden)
        """
        laptop = self.config.get("laptop", {})
        ua_list = laptop.get("user_agents") or USER_AGENT_POOL
        user_agent = random.choice(ua_list)
        w = random.randint(laptop.get("viewport_width_min", 1280), laptop.get("viewport_width_max", 1920))
        h = random.randint(800, 1080)

        self._pw = await async_playwright().start()
        # Try real Chrome first (bypasses Google's automation detection)
        # Falls back to Playwright Chromium if Chrome is not installed
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--lang=en-US",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-background-networking",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--no-first-run",
            "--password-store=basic",
            "--use-mock-keychain",
        ]
        # On Linux, add --no-sandbox when running as root or inside a container
        # (Docker, CI) where the user namespace sandbox is not available.
        # Also apply on any Linux system to avoid pipe closed errors.
        import os as _os
        import sys as _sys
        is_linux = _sys.platform.startswith("linux")
        is_root = hasattr(_os, "geteuid") and _os.geteuid() == 0
        is_docker = _os.path.exists("/.dockerenv")
        if is_linux or is_root or is_docker:
            launch_args.append("--no-sandbox")
            launch_args.append("--disable-setuid-sandbox")
        try:
            self.browser = await self._pw.chromium.launch(
                channel="chrome",
                headless=bool(self.config["browser"]["headless"]),
                ignore_default_args=["--enable-automation"],
                args=launch_args,
            )
        except Exception as _chrome_err:
            self.logger.info(f"Chrome not found ({_chrome_err}), using Playwright Chromium")
            self.browser = await self._pw.chromium.launch(
                headless=bool(self.config["browser"]["headless"]),
                ignore_default_args=["--enable-automation"],
                args=launch_args,
            )

        # Resolve which auth file to use
        import os
        resolved_auth: str | None = None
        if auth_file and os.path.exists(auth_file):
            resolved_auth = auth_file
        elif self.auth_manager and self.auth_manager.has_accounts():
            candidate = self.auth_manager.get_next_account()
            if candidate and os.path.exists(candidate):
                resolved_auth = candidate
        else:
            fallback = self.config.get("browser", {}).get("google_auth_file", "")
            if fallback and os.path.exists(fallback):
                resolved_auth = fallback

        self.current_auth_file = resolved_auth
        if resolved_auth:
            self.logger.info(f"Auth session: {os.path.basename(resolved_auth)} "
                             f"(pool: {self.auth_manager.account_count() if self.auth_manager else 'n/a'} accounts)")
        else:
            self.logger.warning("No auth session — Maps may show limited view")

        self.context = await self.browser.new_context(
            user_agent=user_agent,
            viewport={"width": w, "height": h},
            locale="en-US",
            timezone_id="America/New_York",
            geolocation={"longitude": -80.8431, "latitude": 35.2271},
            permissions=["geolocation"],
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            storage_state=resolved_auth,
        )

        # Basic stealth: hide automation fingerprints
        self.page = await self.context.new_page()
        try:
            from playwright_stealth import stealth_async
            await stealth_async(self.page)
        except (ImportError, OSError):
            pass  # stealth not available — scraper works fine without it

        # Auto-close any extra tabs/popups (Local Guides links, window.open, etc.)
        # Handler set AFTER our page is created so it only fires for subsequent tabs
        async def _close_popup(popup):
            try:
                await popup.close()
                self.logger.debug("Closed popup/extra tab")
            except (OSError, ImportError, RuntimeError):
                pass
        self.context.on("page", lambda p: asyncio.ensure_future(_close_popup(p)))

        self.logger.info(f"Browser started (viewport={w}x{h})")

    async def _handle_account_ban(self, reason: str = "CAPTCHA/sign-in detected") -> bool:
        """
        Mark the current auth account as banned and restart the browser
        with the next available account.

        Returns True if a new account was available and browser restarted,
        False if the pool is exhausted (scraper should pause or stop).
        """
        if self.current_auth_file and self.auth_manager:
            self.logger.warning(
                f"Banning account {self.current_auth_file} — reason: {reason}"
            )
            self.auth_manager.mark_account_banned(self.current_auth_file)
            self.current_auth_file = None

        if self.auth_manager and self.auth_manager.has_accounts():
            self.logger.info(
                f"Switching to next account "
                f"({self.auth_manager.account_count()} remaining)"
            )
            await self.close_browser()
            await asyncio.sleep(random.uniform(5.0, 10.0))
            await self.start_browser()
            return True
        else:
            self.logger.error(
                "All Google accounts exhausted! "
                "Add more accounts with: python run.py add-account"
            )
            return False

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

    # ── Cookie consent ──────────────────────────────────────────────────

    async def _handle_cookies(self) -> None:
        for sel in [
            "button[aria-label*='Accept all']",
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
        ]:
            try:
                btn = self.page.locator(sel).first
                if await btn.is_visible(timeout=1800):
                    try:
                        await btn.click()
                        await asyncio.sleep(0.5)
                        return
                    except OSError:
                        continue
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

    async def _dismiss_overlays(self) -> None:
        """Dismiss any Google Maps overlay popups that may block interaction.
        
        IMPORTANT: Do NOT press Escape or click generic 'Close' buttons here,
        as those can clear the search results panel entirely.
        """
        safe_dismiss_selectors = [
            "button[aria-label='Dismiss']",
            "button:has-text('Got it')",
            "button:has-text('No thanks')",
            "button:has-text('Not now')",
        ]
        for sel in safe_dismiss_selectors:
            try:
                btn = self.page.locator(sel).first
                if await btn.is_visible(timeout=600):
                    try:
                        await btn.click()
                        self.logger.info(f"Dismissed overlay: {sel}")
                        await asyncio.sleep(0.3)
                    except OSError:
                        continue
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

    # ── Page detection ──────────────────────────────────────────────────

    async def _is_on_business_page(self) -> bool:
        """Check if we're on a single business page (not search results).
        
        Only checks for selectors specific to individual business pages.
        Do NOT match generic selectors like div[role='main'] h1 which also
        appear on search results pages (matching the 'Results' heading).
        """
        for sel in ["h1.DUwDvf", "h1.fontHeadlineLarge"]:
            try:
                if await self.page.locator(sel).first.is_visible(timeout=2000):
                    return True
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue
        return False

    async def _click_first_result_if_needed(self) -> None:
        """If we landed on search results, click the first result."""
        if await self._is_on_business_page():
            return
        self.logger.info("On search results page — clicking first result")

        # Dismiss only safe overlays
        await self._dismiss_overlays()
        await asyncio.sleep(0.3)

        # Try multiple selector strategies, with force click to bypass overlays
        result_selectors = [
            "a.hfpxzc",
            "div[role='feed'] a.hfpxzc",
            "div.Nv2PK a",
            "div.Nv2PK",  # The result card itself
        ]

        for attempt in range(2):  # Two attempts
            for sel in result_selectors:
                try:
                    loc = self.page.locator(sel)
                    count = await loc.count()
                    if count > 0:
                        self.logger.info(f"Found {count} search results ({sel}), clicking first (attempt {attempt + 1})")
                        # Use force=True to click through any overlays
                        await loc.first.click(force=True)
                        # Wait for the business page to load after SPA navigation
                        try:
                            await self.page.wait_for_selector(
                                "h1.DUwDvf, h1.fontHeadlineLarge",
                                timeout=12000,
                            )
                        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                            pass
                        await asyncio.sleep(random.uniform(2.0, 4.0))

                        # Verify we actually navigated to a business page
                        if await self._is_on_business_page():
                            self.logger.info("Successfully navigated to business page")
                            return
                        else:
                            self.logger.warning("Click didn't navigate to business page")
                except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error) as exc:
                    self.logger.debug(f"Selector {sel} failed: {exc}")
                    continue

            # If first attempt failed, wait a bit and let page settle
            if attempt == 0:
                self.logger.info("Retrying after waiting for page to settle...")
                await asyncio.sleep(3.0)

        self.logger.warning("Could not navigate to a business page from search results")

    # ── Captcha & sign-in detection ──────────────────────────────────────

    # Indicators that Google is blocking us (CAPTCHA or sign-in wall)
    SIGNIN_INDICATORS = [
        "sign in to continue",
        "sign in to google",
        "accounts.google.com/signin",
        "accounts.google.com/v3/signin",
        "you've been signed out",
        "session expired",
    ]

    async def check_for_captcha(self) -> bool:
        """Return True if a CAPTCHA or sign-in wall is detected.

        Uses URL + title only — not page.content() — to avoid a full DOM fetch
        on every scroll cycle which causes memory pressure and slows long runs.
        """
        try:
            haystack = (self.page.url + " " + (await self.page.title())).lower()
            return any(t in haystack for t in CAPTCHA_INDICATORS)
        except (OSError, ImportError):
            return False

    async def check_for_signin_wall(self) -> bool:
        """Return True if Google is showing a sign-in page (account banned/expired)."""
        try:
            haystack = (self.page.url + " " + (await self.page.title())).lower()
            return any(t in haystack for t in self.SIGNIN_INDICATORS)
        except (OSError, ImportError):
            return False

    async def check_and_handle_blocks(self) -> bool:
        """
        Check for CAPTCHA or sign-in walls and handle them.

        - Sign-in wall → ban current account, rotate to next
        - CAPTCHA → trigger global pause (wait 30-60 min)

        Returns True if a block was detected (caller should retry or abort store).
        """
        # Sign-in wall: account banned or session expired
        if await self.check_for_signin_wall():
            self.logger.warning("Sign-in wall detected — account may be banned or expired")
            switched = await self._handle_account_ban("Sign-in wall detected")
            if not switched:
                self.trigger_global_pause("All accounts exhausted")
            return True

        # CAPTCHA: IP-level block, pause and wait
        if await self.check_for_captcha():
            self.logger.warning("CAPTCHA detected")
            self.trigger_global_pause("CAPTCHA detected")
            return True

        return False

    def trigger_global_pause(self, reason: str = "CAPTCHA detected") -> None:
        PAUSE_FLAG.write_text(f"{datetime.now().isoformat()} | {reason}\n", encoding="utf-8")
        self.logger.warning(f"Global pause: {reason}")

    async def wait_if_paused(self) -> None:
        if not PAUSE_FLAG.exists():
            return
        cfg = self.config["rate_limiting"]
        # Auto-delete stale pause flags so the scraper doesn't hang indefinitely
        # if the previous run was killed mid-pause (e.g., user closed the window)
        try:
            content = PAUSE_FLAG.read_text(encoding="utf-8")
            ts_str = content.split(" | ")[0].strip()
            created_at = datetime.fromisoformat(ts_str)
            age_seconds = (datetime.now() - created_at).total_seconds()
            max_pause = cfg["captcha_pause_max"]  # e.g. 3600s
            if age_seconds > max_pause:
                self.logger.warning(f"Stale pause flag ({age_seconds/60:.0f} min old) — deleting and continuing")
                PAUSE_FLAG.unlink(missing_ok=True)
                return
        except (OSError, ValueError):
            pass  # unreadable flag — still respect it

        pause = random.uniform(cfg["captcha_pause_min"], cfg["captcha_pause_max"])
        self.logger.warning(f"Pause flag found. Sleeping {pause:.0f}s")
        await asyncio.sleep(pause)
        try:
            PAUSE_FLAG.unlink(missing_ok=True)
        except OSError:
            pass
        await self.restart_browser()

    # ── Navigation ──────────────────────────────────────────────────────

    async def _goto_query(self, query: str) -> bool:
        """Navigate to Google Maps with a search query."""
        encoded = quote_plus(query)
        url = f"https://www.google.com/maps/search/{encoded}"
        self.logger.info(f"Navigating to: {url}")
        await self.page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(random.uniform(4.0, 6.0))
        await self._handle_cookies()
        await self._dismiss_overlays()
        await self._click_first_result_if_needed()
        return await self._is_on_business_page()

    async def navigate_to_store(self, store: Any) -> bool:
        name = (store["gmaps_name"] or store["input_name"] or "").strip()
        street = (store["input_street"] or "").strip()
        gmaps_addr = (store["gmaps_address"] or "").strip()
        city = (store["input_city"] or "").strip()
        state = (store.get("input_state") or "").strip()
        place_id = (store["google_place_id"] or "").strip()
        lat = store.get("gmaps_lat") or store.get("input_lat")
        lon = store.get("gmaps_lon") or store.get("input_lon")
        attempts = []

        # Strategy 1: Direct place_id URL (most reliable)
        if place_id and place_id.startswith("ChIJ"):
            attempts.append(f"https://www.google.com/maps/place/?q=place_id:{place_id}")

        # Strategy 2: Name + gmaps_address with lat/lon zoom (best for user's CSV)
        if lat and lon and gmaps_addr:
            q = quote_plus(f"{name} {gmaps_addr}".strip())
            attempts.append(f"https://www.google.com/maps/search/{q}/@{lat},{lon},15z")

        # Strategy 3: Name + lat/lon (if no gmaps_address)
        if lat and lon and not gmaps_addr:
            q = quote_plus(f"{name} {city} {state}".strip())
            attempts.append(f"https://www.google.com/maps/search/{q}/@{lat},{lon},15z")

        # Strategy 4: Full text search with name + street + city + state
        address = gmaps_addr or street
        search_text = f"{name} {address} {city} {state}".strip()
        attempts.append(f"https://www.google.com/maps/search/{quote_plus(search_text)}")

        # Strategy 5: Just name + city + state
        if city and name:
            attempts.append(f"https://www.google.com/maps/search/{quote_plus(f'{name} {city} {state}'.strip())}")

        for idx, url in enumerate(attempts, start=1):
            try:
                self.logger.info(f"Navigate strategy {idx}: {url[:120]}")
                await self.page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(random.uniform(3.0, 6.0))
                await self._handle_cookies()
                await self._click_first_result_if_needed()
                if await self._is_on_business_page():
                    return True
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, OSError) as exc:
                self.logger.warning(f"Strategy {idx} failed: {exc}")
                continue
        return False

    # ── Place ID extraction ─────────────────────────────────────────────

    async def _extract_place_id_from_url(self) -> str | None:
        m = re.search(r"(ChIJ[\w-]{20,})", self.page.url)
        if m:
            return m.group(1)
        m = re.search(r"!1s(0x[0-9a-f]+:0x[0-9a-f]+)", self.page.url)
        if m:
            return m.group(1)
        return None

    # ── Store verification ──────────────────────────────────────────────

    async def verify_store(self, store: Any) -> bool:
        """Verify we're on the right store page.
        
        Matching priority:
          1. Place ID exact match (most reliable)
          2. Name exact/substring match
          3. Address fuzzy match (gmaps_address vs page address)
          4. Name word overlap >= 50%
        """
        # ── Place ID match ──
        target_place_id = (store.get("google_place_id") or "").strip()
        if target_place_id and target_place_id.startswith("ChIJ"):
            current = await self._extract_place_id_from_url()
            if current and current == target_place_id:
                self.logger.info(f"Verified by place_id: {target_place_id}")
                return True

        # ── Extract page name ──
        target_name = (store.get("gmaps_name") or store.get("input_name") or "").lower().strip()
        page_name = ""
        for sel in ["h1.DUwDvf", "h1.fontHeadlineLarge", "div[role='main'] h1"]:
            try:
                page_name = (await self.page.locator(sel).first.inner_text(timeout=2000)).strip()
                if page_name:
                    break
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

        if not target_name:
            return bool(page_name)
        if not page_name:
            return False

        page_name_l = page_name.lower()

        # ── Name exact/substring match ──
        if target_name in page_name_l or page_name_l in target_name:
            self.logger.info(f"Verified by name match: '{page_name}'")
            return True

        # ── Address match (gmaps_address from CSV vs page address) ──
        target_addr = (store.get("gmaps_address") or "").lower().strip()
        if target_addr:
            try:
                page_addr = ""
                addr_el = self.page.locator("button[data-item-id='address']")
                if await addr_el.count() > 0:
                    page_addr = (await addr_el.get_attribute("aria-label", timeout=2000) or "").lower()
                    page_addr = page_addr.replace("address: ", "")

                if page_addr:
                    # Extract numbers from both addresses for comparison
                    target_nums = set(re.findall(r'\d+', target_addr))
                    page_nums = set(re.findall(r'\d+', page_addr))
                    nums_match = bool(target_nums & page_nums) if target_nums else False

                    # Check if key words overlap
                    target_words = set(re.findall(r'[a-z]+', target_addr)) - {'st', 'rd', 'ave', 'dr', 'ln', 'ct', 'blvd', 'sw', 'nw', 'se', 'ne', 'n', 's', 'e', 'w'}
                    page_words = set(re.findall(r'[a-z]+', page_addr)) - {'st', 'rd', 'ave', 'dr', 'ln', 'ct', 'blvd', 'sw', 'nw', 'se', 'ne', 'n', 's', 'e', 'w', 'address'}
                    words_overlap = len(target_words & page_words)

                    if nums_match and words_overlap >= 1:
                        self.logger.info(f"Verified by address match: '{page_addr}' ~ '{target_addr}'")
                        return True
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, OSError):
                pass

        # ── Name word overlap (fuzzy) ──
        target_words = [w for w in re.findall(r"[a-z0-9]+", target_name) if len(w) >= 3]
        if not target_words:
            return True
        matches = sum(1 for w in target_words if w in page_name_l)
        if matches >= max(1, int(len(target_words) * 0.5)):
            self.logger.info(f"Verified by fuzzy name match: {matches}/{len(target_words)} words")
            return True

        self.logger.warning(f"Verification failed: target='{target_name}' page='{page_name}'")
        return False

    async def is_permanently_closed(self) -> bool:
        for token in ["Permanently closed", "Temporarily closed"]:
            try:
                if await self.page.locator(f"text='{token}'").first.is_visible(timeout=1500):
                    return True
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue
        return False

    # ── Business info extraction ────────────────────────────────────────

    async def _extract_total_reviews(self) -> int:
        """
        Extract the business-level review count.

        Only accepts numbers explicitly tied to the word "review" to avoid
        false positives like parsing a rating value (e.g. 4.3 -> 4).
        """
        counts: list[int] = []

        def _collect_from_text(text: str | None) -> None:
            if not text:
                return
            for m in re.finditer(r"(\d[\d,]*)\s+reviews?\b", text, re.IGNORECASE):
                try:
                    counts.append(int(m.group(1).replace(",", "")))
                except ValueError:
                    continue

        selectors = [
            # Primary review-count controls in Maps header
            "button[jsaction*='pane.rating.moreReviews']",
            "button[aria-label*='review']",
            "span[aria-label*='review']",
            # Fallback areas near the rating header
            "div.F7nice span",
            "div.F7nice button",
            "div.jANrlb span",
        ]

        for sel in selectors:
            try:
                loc = self.page.locator(sel)
                n = min(await loc.count(), 12)
                for i in range(n):
                    el = loc.nth(i)
                    _collect_from_text(await el.get_attribute("aria-label"))
                    try:
                        _collect_from_text(await el.inner_text(timeout=300))
                    except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                        continue
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

        # Some views expose "(1,234)" inside a header block that also contains "reviews".
        if not counts:
            try:
                header_texts = await self.page.locator("div.F7nice, div.jANrlb").all_inner_texts()
                for txt in header_texts:
                    if "review" not in (txt or "").lower():
                        continue
                    for m in re.finditer(r"\((\d[\d,]*)\)", txt):
                        try:
                            counts.append(int(m.group(1).replace(",", "")))
                        except ValueError:
                            continue
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, OSError):
                pass

        return max(counts) if counts else 0

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
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
            pass
        try:
            rt = await self.page.locator("div.F7nice span[aria-hidden='true']").first.inner_text(timeout=1500)
            info["overall_rating"] = float(rt.replace(",", "."))
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, ValueError):
            pass
        info["total_reviews"] = await self._extract_total_reviews()
        try:
            info["address"] = (await self.page.locator("button[data-item-id='address']").get_attribute("aria-label", timeout=1200) or "").replace("Address: ", "")
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
            pass
        try:
            info["phone"] = (await self.page.locator("button[data-item-id*='phone:tel']").get_attribute("aria-label", timeout=1200) or "").replace("Phone: ", "")
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
            pass
        try:
            info["website"] = await self.page.locator("a[data-item-id='authority']").get_attribute("href", timeout=1200)
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
            pass
        try:
            info["category"] = await self.page.locator("button.DkEaL").first.inner_text(timeout=1000)
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
            pass
        self.logger.info(f"Business: {info['name']} | Rating: {info['overall_rating']} | Reviews: {info['total_reviews']}")
        return info

    # ── Reviews tab ─────────────────────────────────────────────────────

    async def _click_reviews_tab(self) -> bool:
        """Click the Reviews tab on a business page. Returns True if reviews panel is loaded."""
        self.logger.info("Clicking Reviews tab...")

        # Selectors for the reviews tab button (try multiple)
        tab_selectors = [
            "button[aria-label*='Reviews']",
            "button[aria-label*='reviews']",
            "button[role='tab']:has-text('Reviews')",
            "div[role='tab']:has-text('Reviews')",
            "button.hh2c6:has-text('Reviews')",
            "button:has(span[aria-label*='review'])",
            "[aria-label*='Reviews'][role='tab']",
        ]

        for sel in tab_selectors:
            try:
                btn = self.page.locator(sel).first
                if await btn.is_visible(timeout=2500):
                    self.logger.info(f"Found Reviews tab with selector: {sel}")
                    await btn.click()
                    # Wait for the reviews panel to actually load
                    try:
                        await self.page.wait_for_selector(
                            "div.jftiEf, div[data-review-id]",
                            timeout=10000,
                        )
                        self.logger.info("Reviews panel loaded successfully")
                        return True
                    except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                        self.logger.warning("Reviews tab clicked but panel didn't load, trying next selector")
                        continue
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

        # Recovery: we may be in compact/sidebar view — click the store name
        # or review count link to force the full detail view with tabs
        self.logger.warning("No Reviews tab found — attempting to expand to full detail view")
        expand_selectors = [
            # Click review count text (e.g. "8 reviews") to go directly to reviews
            "button[aria-label*='review']",
            "span[aria-label*='review']",
            "a[aria-label*='review']",
            # Click the store name header to expand panel
            "h1.DUwDvf",
            "h1.fontHeadlineLarge",
            "h2.bwoZTb",
            # Click the rating stars area
            "div.F7nice",
            "span.ceNzKf",
        ]
        for sel in expand_selectors:
            try:
                el = self.page.locator(sel).first
                if await el.is_visible(timeout=1500):
                    self.logger.info(f"Clicking '{sel}' to expand detail view")
                    await el.click()
                    await asyncio.sleep(random.uniform(2.0, 3.5))
                    # Now retry finding the Reviews tab
                    for tab_sel in tab_selectors:
                        try:
                            btn = self.page.locator(tab_sel).first
                            if await btn.is_visible(timeout=2500):
                                self.logger.info(f"Found Reviews tab after expand: {tab_sel}")
                                await btn.click()
                                try:
                                    await self.page.wait_for_selector(
                                        "div.jftiEf, div[data-review-id]",
                                        timeout=10000,
                                    )
                                    self.logger.info("Reviews panel loaded after expand")
                                    return True
                                except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                                    continue
                        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                            continue
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

        self.logger.error("Could not find Reviews tab — even after expand attempts")

        # Save debug info
        try:
            await self.page.screenshot(path="debug_reviews_tab.png")
            self.logger.info("Debug screenshot saved: debug_reviews_tab.png")
        except (OSError, ImportError, RuntimeError):
            pass

        return False

    # ── Sort reviews ───────────────────────────────────────────────────

    async def _sort_reviews(self, sort_newest: bool = True) -> None:
        """Click the sort button and select 'Newest' or leave as 'Most relevant' (oldest/mixed).

        Alternates per attempt:
          odd attempts (1, 3) → sort by Newest
          even attempts (2, 4) → skip sort (Most relevant default = older/mixed reviews)
        """
        if not sort_newest:
            self.logger.info("Sort: using default order (Most relevant / oldest-mixed) for this attempt")
            return

        self.logger.info("Sorting reviews by newest...")
        await asyncio.sleep(random.uniform(1.0, 2.0))

        # Selectors for the sort button
        sort_btn_selectors = [
            "button[aria-label*='Sort reviews']",
            "button[aria-label='Sort']",
            "button[data-value='Sort']",
            "button.g88MCb",
            # Sort button often contains "Most relevant" text by default
        ]

        for sort_btn_sel in sort_btn_selectors:
            try:
                btn = self.page.locator(sort_btn_sel).first
                if await btn.is_visible(timeout=3000):
                    self.logger.info(f"Found sort button: {sort_btn_sel}")
                    await btn.click()
                    await asyncio.sleep(random.uniform(1.0, 2.0))

                    # Now click "Newest" in the dropdown menu
                    newest_selectors = [
                        "div[role='menuitemradio']:has-text('Newest')",
                        "div[role='menuitem']:has-text('Newest')",
                        "li[data-index='1']",
                        "div[role='menuitemradio'][data-index='1']",
                        # Fallback: second menu item
                        "div[role='menuitemradio']:nth-child(2)",
                    ]

                    for new_sel in newest_selectors:
                        try:
                            opt = self.page.locator(new_sel).first
                            if await opt.is_visible(timeout=2500):
                                self.logger.info(f"Clicking 'Newest': {new_sel}")
                                await opt.click()
                                await asyncio.sleep(random.uniform(2.5, 4.0))
                                self.logger.info("Sort by newest applied")
                                return
                        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                            continue

                    self.logger.warning("Sort button clicked but 'Newest' option not found")
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

        self.logger.warning("Could not find sort button — proceeding without sorting")

    # ── Scroll utilities ────────────────────────────────────────────────

    async def _find_scrollable_container(self) -> str | None:
        """Find the scrollable reviews container and return a JS selector for it."""
        # The reviews panel is typically a scrollable div
        container_selectors = [
            "div.m6QErb.DxyBCb",
            "div.m6QErb.DByNcb",
            "div.m6QErb",
            "div[role='main']",
        ]
        for sel in container_selectors:
            try:
                count = await self.page.locator(sel).count()
                if count > 0:
                    return sel
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue
        return None

    async def _scroll_reviews_panel(self, delta: int = 3000) -> None:
        """Scroll the reviews panel using JavaScript for reliability."""
        # Try JavaScript scroll on the container first
        container_sel = await self._find_scrollable_container()
        if container_sel:
            try:
                await self.page.evaluate(f"""
                    (() => {{
                        const el = document.querySelector('{container_sel}');
                        if (el) el.scrollTop += {delta};
                    }})()
                """)
                return
            except (OSError, ImportError):
                pass

        # Fallback: mouse wheel scroll
        try:
            scroll_x, scroll_y = await self._get_scroll_position()
            await self.page.mouse.move(scroll_x, scroll_y)
            await self.page.mouse.wheel(0, delta)
        except (OSError, ImportError):
            pass

    async def _get_scroll_position(self) -> tuple[float, float]:
        for sel in ["div.jftiEf", "div.m6QErb.DxyBCb", "div.m6QErb.DByNcb", "div.m6QErb"]:
            try:
                node = self.page.locator(sel).first
                box = await node.bounding_box(timeout=2000)
                if box:
                    return box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue
        return 400.0, 450.0

    # ── Single review extraction ────────────────────────────────────────

    async def _expand_review_text(self, el: Any) -> None:
        """Click 'More' / 'See more' button to expand truncated review text."""
        expand_selectors = [
            "button[aria-label*='See more']",
            "button.w8nwRe",
            "button:has-text('More')",
            "a.review-more-link",
        ]
        for sel in expand_selectors:
            try:
                more_btn = el.locator(sel).first
                if await more_btn.is_visible(timeout=300):
                    await more_btn.click()
                    await asyncio.sleep(0.3)
                    return
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                continue

    async def _extract_review(self, el: Any) -> dict[str, Any] | None:
        try:
            rid = await el.get_attribute("data-review-id")
            if not rid:
                return None

            # Expand full review text
            await self._expand_review_text(el)

            name, rating, date_rel, review_text = "Anonymous", None, "", ""
            try:
                name = await el.locator("div.d4r55").first.inner_text(timeout=500)
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                pass
            try:
                aria = await el.locator("span.kvMYJc").first.get_attribute("aria-label", timeout=500)
                if aria:
                    rating = int(re.findall(r"\d+", aria)[0])
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, ValueError, IndexError):
                pass
            try:
                date_rel = await el.locator("span.rsqaWe").first.inner_text(timeout=500)
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                pass
            try:
                review_text = (await el.locator("span.wiI7pd").first.inner_text(timeout=500)).replace("\x00", "")
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
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
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, ValueError):
                pass
            try:
                lbl = await el.locator("button[aria-label*='helpful']").first.get_attribute("aria-label", timeout=350)
                if lbl:
                    m = re.search(r"(\d+)", lbl)
                    if m:
                        helpful = int(m.group(1))
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, ValueError):
                pass
            try:
                photo_count = await el.locator("div.Iop04 img").count()
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                pass
            try:
                if await el.locator("text='Response from the owner'").first.is_visible(timeout=250):
                    has_owner_response = 1
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
                pass
            try:
                attrs = await el.locator("div.PBK6be span.pVtsbf").all_inner_texts()
                if attrs:
                    service_type = ", ".join(attrs)
            except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
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
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error, OSError):
            return None

    # ── Main review collection loop ─────────────────────────────────────

    async def _collect_reviews(
        self,
        target_count: int,
        store_id: int | None = None,
        sort_newest: bool = True,
    ) -> tuple[int, bool] | tuple[list[dict[str, Any]], bool]:
        if not await self._click_reviews_tab():
            self.logger.error("Could not find Reviews tab — aborting review collection")
            return (0, False) if store_id else ([], False)
        await self._sort_reviews(sort_newest=sort_newest)

        max_reviews = min(max(int(target_count), 1), int(self.config["scraping"]["max_reviews_per_store"]))
        self.logger.info(f"Starting review collection: target={max_reviews}")

        existing_ids: set[str] = set()
        if store_id and self.db:
            existing_ids = self.db.get_existing_review_ids(store_id)
            if existing_ids:
                self.logger.info(f"Dedup guard: {len(existing_ids)} reviews already in DB for store {store_id}")
        seen_ids: set[str] = set(existing_ids)
        total_collected = len(existing_ids)

        if total_collected > 0:
            self.logger.info(f"Resuming: {total_collected} reviews already in DB, collecting up to {max_reviews - total_collected} more")
        buffer: list[dict[str, Any]] = []
        in_memory: list[dict[str, Any]] = []
        stall_cycles = 0
        scroll_attempts = 0
        reached_end = False
        max_scrolls = int(self.config["scraping"]["max_scroll_attempts"])
        stall_threshold = int(self.config["scraping"].get("stall_threshold", 80))
        batch_size = int(self.config["scraping"]["batch_save_interval"])
        rl = self.config["rate_limiting"]
        last_log_count = 0

        # Faster scrolling for large targets to stay within timeout
        fast_mode = max_reviews > 300
        scroll_delay_min = 0.6 if fast_mode else rl["min_scroll_delay"]
        scroll_delay_max = 1.5 if fast_mode else rl["max_scroll_delay"]
        if fast_mode:
            self.logger.info(f"Fast-scroll mode enabled (target={max_reviews} > 300)")

        try:
            while total_collected < max_reviews and scroll_attempts < max_scrolls and not self._shutdown:
                if await self.check_for_captcha():
                    # Flush buffer before breaking so no reviews are lost
                    if store_id and self.db and buffer:
                        self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
                        self.db.update_store_reviews_count(store_id, total_collected)
                        buffer = []
                    self.trigger_global_pause("CAPTCHA during review collection")
                    break

                # Find all review elements currently in the DOM
                # Use only data-review-id — div.jftiEf can also match the outer
                # container, causing duplicate extraction
                visible = await self.page.locator("div[data-review-id]").all()
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
                            self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
                            self.db.update_store_reviews_count(store_id, total_collected)
                            buffer = []
                    else:
                        in_memory.append(record)

                # Progress logging every 25 reviews
                if total_collected - last_log_count >= 25:
                    self.logger.info(
                        f"Progress: {total_collected}/{max_reviews} reviews collected "
                        f"(scroll #{scroll_attempts}, {len(seen_ids)} seen)"
                    )
                    last_log_count = total_collected

                stall_cycles = stall_cycles + 1 if new_in_cycle == 0 else 0
                reached_end = False
                if stall_cycles >= stall_threshold:
                    # Before giving up, try a recovery scroll: jump far down + wait
                    # to let Google Maps lazy-load more reviews
                    self.logger.info(f"Stall at {stall_cycles} cycles — attempting recovery scroll")
                    await self._scroll_reviews_panel(delta=10000)
                    await asyncio.sleep(random.uniform(4.0, 6.0))
                    recovery_visible = await self.page.locator("div[data-review-id]").all()
                    recovered = 0
                    for el in recovery_visible:
                        rid = await el.get_attribute("data-review-id")
                        if rid and rid not in seen_ids:
                            recovered += 1
                    if recovered > 0:
                        self.logger.info(f"Recovery found {recovered} new reviews — continuing")
                        stall_cycles = 0
                        continue
                    self.logger.info(f"Recovery failed — end of reviews reached ({total_collected} collected)")
                    reached_end = True
                    break

                # Scroll the reviews panel
                scroll_delta = random.randint(1500, 4000)
                await self._scroll_reviews_panel(delta=scroll_delta)
                await asyncio.sleep(random.uniform(scroll_delay_min, scroll_delay_max))
                scroll_attempts += 1

                # Periodic idle pauses
                idle_every = int(rl.get("idle_pause_every_n_scrolls", 0))
                if idle_every > 0 and scroll_attempts % idle_every == 0:
                    await asyncio.sleep(random.uniform(rl["idle_pause_min"], rl["idle_pause_max"]))

        except Exception as exc:
            # Flush buffer on timeout/cancellation so partial reviews are saved
            if store_id and self.db and buffer:
                self.logger.info(f"Flushing {len(buffer)} buffered reviews before exit")
                self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
                self.db.update_store_reviews_count(store_id, total_collected)
                buffer = []
            if isinstance(exc, asyncio.CancelledError):
                raise

        # Flush remaining buffer
        if store_id and self.db and buffer:
            self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
            self.db.update_store_reviews_count(store_id, total_collected)

        # Check if we hit the target (= reached end)
        if total_collected >= max_reviews:
            reached_end = True

        self.logger.info(f"Review collection complete: {total_collected} reviews in {scroll_attempts} scrolls (reached_end={reached_end})")
        if store_id:
            return total_collected, reached_end
        return in_memory, reached_end

    # ── Public scrape methods ───────────────────────────────────────────

    async def scrape_reviews(self, store_id: int, target_count: int, sort_newest: bool = True) -> tuple[int, bool]:
        """Returns (review_count, reached_end)."""
        result, reached_end = await self._collect_reviews(target_count=target_count, store_id=store_id, sort_newest=sort_newest)
        return int(result), reached_end

    async def scrape_query(self, query: str, max_reviews: int = 1500) -> list[dict[str, Any]]:
        """Scrape reviews for a single place by search query."""
        await self.wait_if_paused()
        self.logger.info(f"Query mode: searching for '{query}'")
        ok = await self._goto_query(query)
        if not ok:
            # Save debug info before failing
            try:
                await self.page.screenshot(path="debug_query_failed.png")
                with open("debug_query_failed.html", "w", encoding="utf-8") as f:
                    f.write(await self.page.content())
                self.logger.info("Debug artifacts saved: debug_query_failed.png, debug_query_failed.html")
            except (OSError, ImportError):
                pass
            raise RuntimeError(f"Could not open a business page for query: {query}")
        if await self.is_permanently_closed():
            self.logger.info("Store is permanently/temporarily closed")
            return []
        business = await self.extract_business_info()
        live = int(business.get("total_reviews") or 0)
        target = min(1500, max(max_reviews, live if live > 0 else max_reviews))
        self.logger.info(f"Query mode target: {target} reviews (live={live}, requested={max_reviews})")
        reviews, _reached_end = await self._collect_reviews(target_count=target, store_id=None)
        return reviews

    async def scrape_url(self, url: str, max_reviews: int = 1500) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Scrape reviews from a direct Google Maps place URL.

        Returns (business_info, reviews_list).
        This bypasses all search navigation — just opens the URL directly.
        """
        await self.wait_if_paused()
        self.logger.info(f"URL mode: navigating directly to place")
        self.logger.info(f"URL: {url[:120]}...")

        # Navigate directly to the place URL
        await self.page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(random.uniform(4.0, 6.0))

        # Handle cookie consent
        await self._handle_cookies()
        await self._dismiss_overlays()

        # Wait for business page to load
        try:
            await self.page.wait_for_selector(
                "h1.DUwDvf, h1.fontHeadlineLarge",
                timeout=15000,
            )
        except (playwright._impl._api_types.TimeoutError, playwright._impl._api_types.Error):
            # Save debug info
            try:
                await self.page.screenshot(path="debug_url_failed.png")
                self.logger.info("Debug screenshot saved: debug_url_failed.png")
            except (OSError, ImportError):
                pass
            raise RuntimeError("Business page did not load from URL")

        await asyncio.sleep(random.uniform(1.0, 2.0))

        # Check if store is closed
        if await self.is_permanently_closed():
            self.logger.info("Store is permanently/temporarily closed")
            business = await self.extract_business_info()
            return business, []

        # Extract business info
        business = await self.extract_business_info()
        live = int(business.get("total_reviews") or 0)
        target = min(1500, max(max_reviews, live if live > 0 else max_reviews))
        self.logger.info(f"URL mode target: {target} reviews (live={live}, requested={max_reviews})")

        # Click reviews tab and collect
        reviews, _reached_end = await self._collect_reviews(target_count=target, store_id=None)
        return business, reviews

    async def scrape_single_store(self, store: Any) -> str:
        store_id = store["store_id"]
        master_reviews = int(store.get("master_reviews") or 0)
        raw_target = int(store.get("target_reviews") or 1000)
        target_reviews = min(1500, raw_target)
        store_type = "incomplete" if master_reviews > 0 else "fresh"

        await self.wait_if_paused()

        # Skip already-completed stores (dedup guard)
        if self.db and self.db.is_store_completed(store_id):
            self.logger.info(f"Store {store_id} already completed — skipping")
            return "skipped"

        # Effective target: how many MORE reviews this machine needs to collect
        effective_target = max(target_reviews - master_reviews, 1)
        self.logger.info(
            f"[{store_type.upper()}] target={target_reviews} (raw={raw_target}, capped=1500), "
            f"master={master_reviews}, effective_target={effective_target}"
        )

        try:
            if not await self.navigate_to_store(store):
                raise RuntimeError("Failed to navigate to store page")
            # Check for CAPTCHA or sign-in wall (banned account)
            block_result = await self.check_and_handle_blocks()
            if block_result:
                return "interrupted"
            if not await self.verify_store(store):
                raise RuntimeError("Store verification failed")
            if await self.is_permanently_closed():
                if self.db:
                    self.db.mark_store_skipped(store_id, "Permanently/temporarily closed")
                return "skipped"
            info = await self.extract_business_info()
            live_count = int(info.get("total_reviews") or 0)

            # Save live review count from Google before we start scraping
            if self.db and live_count > 0:
                self.db.update_store_live_review_count(store_id, live_count)
                self.logger.info(f"Live Google review count: {live_count}")

            # Guard against limited-view pages that incorrectly show ~4 reviews
            # for high-target stores. This usually means the session is degraded.
            limited_view_suspected = (
                live_count > 0
                and target_reviews >= 50
                and live_count <= 5
            )
            if limited_view_suspected:
                self.logger.warning(
                    "Suspiciously low live count (%s) vs target (%s) - "
                    "not trusting live-count completion shortcuts",
                    live_count,
                    target_reviews,
                )

            # Scrape only what we actually need — don't over-scrape
            scrape_target = effective_target
            # If Google shows fewer reviews than we need, cap to avoid fruitless scrolling
            if live_count > 0 and live_count < scrape_target and not limited_view_suspected:
                self.logger.info(f"Live count ({live_count}) < effective target ({scrape_target}) — capping to live count")
                scrape_target = live_count
            scrape_target = min(scrape_target, int(self.config["scraping"]["max_reviews_per_store"]))

            # Alternate sort order: odd attempts → newest, even attempts → most relevant (oldest/mixed)
            attempt = int(store.get("attempts") or 1)
            sort_newest = (attempt % 2 == 1)  # 1,3 → newest; 2,4 → most relevant
            self.logger.info(f"Attempt {attempt}: sort={'newest' if sort_newest else 'most relevant (oldest/mixed)'}, scrape_target={scrape_target}")

            collected, reached_end = await self.scrape_reviews(store_id=store_id, target_count=scrape_target, sort_newest=sort_newest)
            if self.db:
                final_count = self.db.get_review_count(store_id)
                # Total reviews = what master has + what we collected locally
                total_reviews = master_reviews + final_count

                # _collect_reviews sets reached_end=True both when:
                #  1) we truly hit the end, and
                #  2) we hit scrape_target.
                # Case (2) should not be treated as "store is complete".
                reached_true_end = reached_end and collected < scrape_target

                # Trust "target > live" completion only when live_count is plausible
                # and matches what we actually have for this store.
                # We must have collected all available reviews (total >= live_count)
                # AND the counts should match (we actually got what Google reports).
                live_shortfall_confirmed = (
                    live_count > 0
                    and live_count < target_reviews
                    and not limited_view_suspected
                    and total_reviews >= live_count
                    and abs(total_reviews - live_count) <= 1
                )

                # Mark complete if:
                #   1. Total hits target, OR
                #   2. We truly reached end of reviews and confirmed Google live
                #      count is a plausible lower ceiling for this store.
                is_done = (
                    total_reviews >= target_reviews
                    or (reached_true_end and live_shortfall_confirmed)
                )
                if is_done:
                    self.db.mark_store_completed(store_id, final_count)
                    self.logger.info(
                        f"Store completed [{store_type}]: {final_count} new + {master_reviews} master "
                        f"= {total_reviews}/{target_reviews} (live={live_count}, reached_end={reached_end})"
                    )
                    return "completed"
                else:
                    self.db.mark_store_failed(
                        store_id,
                        f"Incomplete [{store_type}]: {final_count} new + {master_reviews} master "
                        f"= {total_reviews}/{target_reviews} (live={live_count}, reached_end={reached_end}, "
                        f"limited_view={limited_view_suspected}), attempt {attempt}",
                    )
                    self.logger.warning(
                        f"Store incomplete [{store_type}]: {total_reviews}/{target_reviews} "
                        f"(live={live_count}, reached_end={reached_end}, limited_view={limited_view_suspected}), "
                        f"attempt {attempt}, will retry"
                    )
                    return "failed"
            else:
                final_count = collected
            self.logger.info(f"Store completed: {final_count} reviews")
            return "completed"
        except Exception as exc:
            if self.db:
                self.db.mark_store_failed(store_id, str(exc))
            self.logger.error(f"Store failed: {exc}")
            return "failed"

    # ── Signal handling ─────────────────────────────────────────────────

    def _setup_signal_handlers(self) -> None:
        def handler(signum: int, _frame: Any) -> None:
            self.logger.warning(f"Signal {signum}; shutting down")
            self._shutdown = True

        try:
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)
        except ValueError:
            pass  # Not in main thread (GUI mode)

    # ── Main run loop (store mode) ──────────────────────────────────────

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
