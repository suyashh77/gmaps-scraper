"""
Google Maps scraper: store-based (DB) and query-based modes, captcha handling, review extraction.
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import math
import os
import random
import re
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus


_FEATURE_ID_RE = re.compile(r"^0x[0-9a-f]+:0x([0-9a-f]+)$")


def feature_id_to_cid_url(feature_id: str | None) -> str | None:
    """Google Maps URL for a hex feature id (`0x<fid>:0x<cid>`).

    The second half is the place's CID in hex; `maps?cid=<decimal>` opens that
    place directly, the same way `?q=place_id:ChIJ...` does. It is the route for
    stores the probe identified without a ChIJ id.
    """
    m = _FEATURE_ID_RE.match((feature_id or "").strip().lower())
    return f"https://www.google.com/maps?cid={int(m.group(1), 16)}" if m else None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two (lat, lon) points in kilometers."""
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

import playwright
from playwright.async_api import Browser, BrowserContext, Page, async_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError

from .auth_manager import AuthAccountManager
from .database import DatabaseManager
from . import strategy

PAUSE_FLAG = Path(__file__).parent.parent / "pause.flag"

# Cookies Google sets for ad tracking / consent rather than the session itself.
# Dropped on a fresh_cookies session swap; clearing them along with everything
# else made Maps serve the limited view (no Reviews tab) 2/2 times in testing,
# vs 0/2 when they were kept and only the rest of the cookie jar was cleared.
TRACKING_COOKIES = {"NID", "AEC", "1P_JAR", "DV", "OTZ", "SOCS", "CONSENT", "UULE"}

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


class _WorkerPrefixFilter(logging.Filter):
    """Tags every record from a worker's logger with "[wN] " so a shared log
    file interleaved from N parallel workers can still be read per-worker."""

    def __init__(self, worker_id: int):
        super().__init__()
        self._prefix = f"[w{worker_id}] "

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = f"{self._prefix}{record.msg}"
        return True


def _logger(config: dict[str, Any], worker_id: int = 0) -> logging.Logger:
    name = "scraper" if worker_id == 0 else f"worker-{worker_id}"
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = getattr(logging, str(config["logging"]["level"]).upper(), logging.INFO)
    logger.setLevel(level)
    if worker_id != 0:
        logger.addFilter(_WorkerPrefixFilter(worker_id))
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
        self._network_down = False
        self.signed_out = False
        self.last_stop_reason = ""
        self.last_rungs_applied = 0
        self.logger = _logger(config, worker_id)
        # Per-store tally of extractions that found no element at all, as
        # distinct from ones that found an element holding zero. Without this
        # split, a selector Google renamed and a review with no thumbs-up write
        # exactly the same row, which is how three fields stayed empty across
        # 1,201,237 reviews without anyone noticing. assert_field_coverage()
        # turns the tally into a loud failure at the end of each store.
        self._extract_failures: dict[str, int] = collections.Counter()

    async def start_browser(
        self, auth_file: str | None = None, timeout: float = 60.0,
        fresh_cookies: bool = False,
    ) -> None:
        """
        Launch Chromium and load a Google auth session.

        Auth priority:
          1. auth_file argument (explicit override)
          2. auth_manager.acquire(worker_id) (pool rotation, avoids accounts
             held by other workers)
          3. config browser.google_auth_file (single-account fallback)
          4. No auth (limited view — Reviews tab hidden)

        Args:
            timeout: Maximum time to wait for browser startup (seconds)
            fresh_cookies: drop everything in the loaded storage_state except
                the auth file's own (non-tracking) cookies. Used when rotating
                to a new browser after a stalled store, so ad-tracking cookies
                from the dead session don't follow into the new one — but the
                actual sign-in cookies still do, since dropping those too
                pushed Maps into the no-Reviews-tab limited view.
        """
        self.signed_out = False
        import asyncio
        laptop = self.config.get("laptop", {})
        ua_list = laptop.get("user_agents") or USER_AGENT_POOL
        user_agent = random.choice(ua_list)
        w = random.randint(laptop.get("viewport_width_min", 1280), laptop.get("viewport_width_max", 1920))
        # Below about 850px tall Maps does not mount the review list until the panel is
        # scrolled, so a third of launches saw "panel didn't load" and gave up.
        h = random.randint(920, 1080)

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
        launch_args += self.config["browser"].get("extra_args", [])
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
            # Launch browser with timeout to prevent hanging on proxy connection
            self.browser = await asyncio.wait_for(
                self._pw.chromium.launch(
                    channel="chrome",
                    headless=bool(self.config["browser"]["headless"]),
                    ignore_default_args=["--enable-automation"],
                    args=launch_args,
                ),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            self.logger.error(f"Browser launch timed out after {timeout}s (proxy connection issue?)")
            raise
        except Exception as _chrome_err:
            self.logger.info(f"Chrome not found ({_chrome_err}), using Playwright Chromium")
            try:
                self.browser = await asyncio.wait_for(
                    self._pw.chromium.launch(
                        headless=bool(self.config["browser"]["headless"]),
                        ignore_default_args=["--enable-automation"],
                        args=launch_args,
                    ),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                self.logger.error(f"Browser launch timed out after {timeout}s (proxy connection issue?)")
                raise

        # Resolve which auth file to use
        import os
        resolved_auth: str | None = None
        if auth_file and os.path.exists(auth_file):
            resolved_auth = auth_file
        elif self.auth_manager and self.auth_manager.has_accounts():
            candidate = self.auth_manager.acquire(self.worker_id)
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
            self.logger.info("No auth session — using IP rotation mode")
        # A signed-in session must look like the same browser every time, from
        # login (stable_user_agent) through every scrape. A random Windows/Mac UA
        # per launch, on what is really Linux Chromium, got every session signed
        # out on its first use.
        if resolved_auth or self.config["browser"].get("stable_user_agent"):
            user_agent = self._session_user_agent()

        context_args = {
            "user_agent": user_agent,
            "viewport": {"width": w, "height": h},
            "locale": "en-US",
            "timezone_id": "America/New_York",
            "geolocation": {"longitude": -80.8431, "latitude": 35.2271},
            "permissions": ["geolocation"],
            "extra_http_headers": {"Accept-Language": "en-US,en;q=0.9"},
            "storage_state": resolved_auth,
        }
        self.context = await self.browser.new_context(**context_args)

        if fresh_cookies and resolved_auth:
            await self.context.clear_cookies()
            try:
                with open(resolved_auth, "r", encoding="utf-8") as fh:
                    auth_state = json.load(fh)
                auth_cookies = [
                    c for c in auth_state.get("cookies", [])
                    if c.get("name") not in TRACKING_COOKIES
                ]
                if auth_cookies:
                    await self.context.add_cookies(auth_cookies)
                self.logger.info(
                    f"fresh_cookies: cleared cookie jar, restored "
                    f"{len(auth_cookies)} session cookie(s) from the auth file"
                )
            except (OSError, json.JSONDecodeError) as exc:
                self.logger.warning(f"fresh_cookies: could not reload auth cookies: {exc}")

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

    def _session_user_agent(self) -> str:
        """The UA this machine's browser really is, minus 'Headless' — stable across launches."""
        major = (self.browser.version or "0").split(".")[0]
        if sys.platform.startswith("linux"):
            platform = "X11; Linux x86_64"
        elif sys.platform == "darwin":
            platform = "Macintosh; Intel Mac OS X 10_15_7"
        else:
            platform = "Windows NT 10.0; Win64; x64"
        return (f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 "
                f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36")

    async def save_session(self) -> bool:
        """Write the context's current cookies back to the auth file.

        Google rotates the session cookies while the browser runs. Replaying the
        file's original snapshot on every launch presents cookies Google has
        already rotated away, so keep the file current. Skipped once the session
        is signed out, so a dead jar never overwrites a good file.
        """
        path = self.current_auth_file
        # A file renamed to .banned by mark_account_banned must not be recreated.
        if not (path and self.context) or self.signed_out or not os.path.exists(path):
            return False
        try:
            state = await self.context.storage_state()
        except (PlaywrightError, OSError) as exc:
            self.logger.debug(f"save_session: could not read storage state: {exc}")
            return False
        if not any(c["name"] == "__Secure-1PSID" and c["domain"].endswith("google.com")
                   for c in state.get("cookies", [])):
            return False
        tmp = f"{path}.tmp{os.getpid()}"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh)
        os.replace(tmp, path)
        return True

    async def close_browser(self) -> None:
        await self.save_session()
        if self.auth_manager:
            self.auth_manager.release(self.worker_id)
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
            except (PlaywrightTimeoutError, PlaywrightError):
                continue

    async def _dismiss_signin_modal(self) -> bool:
        """Dismiss the "Sign in to get the best of Google Maps" modal.

        On a signed-out session, clicking Sort (or several other controls)
        opens this modal instead of the menu it would normally open. Left in
        place it sits on top of the panel and intercepts every subsequent
        Playwright click, turning each one into a full click timeout instead
        of an instant failure. Done as one page.evaluate() so the browser
        performs the click directly rather than Playwright locating and
        clicking through whatever is on top.

        Returns True if a modal was found and dismissed.
        """
        try:
            clicked = await self.page.evaluate(
                """
                (() => {
                  const dialogs = document.querySelectorAll(
                    "[role='dialog'], [role='alertdialog'], [aria-modal='true']");
                  for (const d of dialogs) {
                    if (!/sign.?in/i.test(d.textContent || '')) continue;
                    for (const b of d.querySelectorAll('button')) {
                      const t = (b.textContent || '').trim();
                      if (/^(Dismiss|Not now|No thanks|Cancel)$/i.test(t)) {
                        b.click();
                        return true;
                      }
                    }
                  }
                  return false;
                })()
                """
            )
            return bool(clicked)
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            return False

    async def is_signed_out(self) -> bool:
        """True if the page is showing Maps' signed-out chrome (a "Sign in" link)."""
        try:
            return await self.page.locator(
                "a[aria-label='Sign in'], a[href*='ServiceLogin']"
            ).count() > 0
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            return False

    async def _dismiss_overlays(self) -> None:
        """Dismiss any Google Maps overlay popups that may block interaction.

        IMPORTANT: Do NOT press Escape or click generic 'Close' buttons here,
        as those can clear the search results panel entirely.
        """
        await self._dismiss_signin_modal()
        safe_dismiss_selectors = [
            "button[aria-label='Dismiss']",
            "button:has-text('Got it')",
            "button:has-text('No thanks')",
            "button:has-text('Not now')",
        ]
        for sel in safe_dismiss_selectors:
            try:
                btn = self.page.locator(sel).first
                if await btn.is_visible(timeout=2000):
                    try:
                        await btn.click(timeout=2000)
                        self.logger.info(f"Dismissed overlay: {sel}")
                        await asyncio.sleep(0.3)
                    except OSError:
                        continue
            except (PlaywrightTimeoutError, PlaywrightError):
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
            except (PlaywrightTimeoutError, PlaywrightError):
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
                        except (PlaywrightTimeoutError, PlaywrightError):
                            pass
                        await asyncio.sleep(random.uniform(2.0, 4.0))

                        # Verify we actually navigated to a business page
                        if await self._is_on_business_page():
                            self.logger.info("Successfully navigated to business page")
                            return
                        else:
                            self.logger.warning("Click didn't navigate to business page")
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
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
        lat = store.get("gmaps_lat") or store.get("input_lat")
        lon = store.get("gmaps_lon") or store.get("input_lon")
        place_id = (store.get("google_place_id") or "").strip()
        attempts = []

        # Strategy 0: the place_id itself. Restored from v1, which navigated this
        # way before v2 replaced it with text search.
        #
        # This is the only construction that cannot land on a different store. A
        # text search ranks results, and a ranked result for "Dollar General" in a
        # town with four of them is a coin toss. 686 of the 705 stores queued in
        # batch1.db carry a real ChIJ id that the current code never uses.
        if place_id.startswith("ChIJ"):
            attempts.append(
                f"https://www.google.com/maps/place/?q=place_id:{quote_plus(place_id)}")

        # Strategy 0b: the feature id, for stores the probe found without a ChIJ
        # id. Same guarantee as the place_id URL: Google opens exactly that
        # place or nothing.
        cid_url = None if place_id.startswith("ChIJ") else feature_id_to_cid_url(
            store.get("google_feature_id"))
        if cid_url:
            attempts.append(cid_url)

        # Strategy 1: Name + gmaps_address with lat/lon zoom (best for user's CSV)
        if lat and lon and gmaps_addr:
            q = quote_plus(f"{name} {gmaps_addr}".strip())
            attempts.append(f"https://www.google.com/maps/search/{q}/@{lat},{lon},15z")

        # Strategy 2: Name + lat/lon (if no gmaps_address)
        if lat and lon and not gmaps_addr:
            q = quote_plus(f"{name} {city} {state}".strip())
            attempts.append(f"https://www.google.com/maps/search/{q}/@{lat},{lon},15z")

        # Strategy 3: Full text search with name + street + city + state
        address = gmaps_addr or street
        search_text = f"{name} {address} {city} {state}".strip()
        attempts.append(f"https://www.google.com/maps/search/{quote_plus(search_text)}")

        # Strategy 4: Just name + city + state
        if city and name:
            attempts.append(f"https://www.google.com/maps/search/{quote_plus(f'{name} {city} {state}'.strip())}")

        # Which attempt, if any, was the place_id URL. Landing on a business page
        # from that URL is itself the identity check; see verify_store.
        place_id_attempt = 1 if (attempts and (
            attempts[0].startswith("https://www.google.com/maps/place/?q=place_id:")
            or attempts[0].startswith("https://www.google.com/maps?cid="))) else None
        self._navigated_by_place_id = False

        connectivity_failures = 0
        for idx, url in enumerate(attempts, start=1):
            try:
                self.logger.info(f"Navigate strategy {idx}: {url[:120]}")
                await self.page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(random.uniform(3.0, 6.0))
                await self._handle_cookies()
                await self._click_first_result_if_needed()
                if await self._is_on_business_page():
                    self._network_down = False
                    self._navigated_by_place_id = (idx == place_id_attempt)
                    return True
            except (PlaywrightTimeoutError, PlaywrightError, OSError) as exc:
                if self._is_connectivity_error(exc):
                    connectivity_failures += 1
                self.logger.warning(f"Strategy {idx} failed: {exc}")
                continue

        # Every strategy failed for the same reason: this machine has no network.
        # That is not a fact about the store, and treating it as one is expensive.
        # During a 4.5 hour outage on the first long run the scraper burned a
        # three-hour per-store timeout and then marked three good stores failed
        # in four seconds, consuming a retry attempt on each. The block-shape
        # principle the rest of this scraper follows applies here too: the way a
        # request fails tells you what to do about it, and "the network is gone"
        # calls for waiting, not for blaming the store.
        self._network_down = connectivity_failures == len(attempts) and attempts != []
        return False

    @staticmethod
    def _is_connectivity_error(exc: BaseException) -> bool:
        text = str(exc)
        return any(tok in text for tok in (
            "ERR_INTERNET_DISCONNECTED", "ERR_NETWORK_CHANGED", "ERR_NAME_NOT_RESOLVED",
            "ERR_CONNECTION_RESET", "ERR_CONNECTION_REFUSED", "ERR_CONNECTION_TIMED_OUT",
            "ERR_ADDRESS_UNREACHABLE", "ERR_PROXY_CONNECTION_FAILED",
        ))

    async def wait_for_network(self, max_wait_s: int = 3600) -> bool:
        """Block until this machine can reach Google again.

        Polls with backoff rather than hammering. Returns True once a request
        succeeds, False if the outage outlasts max_wait_s.
        """
        waited, delay = 0, 15
        while waited < max_wait_s:
            try:
                resp = await self.page.goto("https://www.google.com/generate_204",
                                            wait_until="domcontentloaded", timeout=15000)
                if resp is not None:
                    self.logger.info(f"Network is back after {waited}s")
                    self._network_down = False
                    return True
            except (PlaywrightTimeoutError, PlaywrightError, OSError) as exc:
                if not self._is_connectivity_error(exc):
                    self._network_down = False
                    return True                      # reachable, just an odd response
            self.logger.warning(
                f"No network. Waited {waited}s, retrying in {delay}s.")
            await asyncio.sleep(delay)
            waited += delay
            delay = min(delay * 2, 300)
        return False

    # ── Store verification ──────────────────────────────────────────────

    async def _extract_page_latlon(self) -> tuple[float, float] | None:
        """Coordinates of the BUSINESS, never the map camera.

        The previous implementation read the `@lat,lon` segment. That segment is
        the camera position, and after navigating to
        `/maps/search/<query>/@<lat>,<lon>,15z` the camera is *the coordinate we
        asked for*. So the distance from the target to the "page" was near zero
        for whatever result got clicked, and the 10 km chain veto could never
        fire on strategies 1 and 2, which are the strategies that handle almost
        every store. The veto was not too loose; it was disconnected.

        `!3d<lat>!4d<lon>` is the place pin Google appends once a business panel
        is open, and it belongs to the business. The directions link carries the
        same pair and is used when the URL has not settled yet.
        """
        m = re.search(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", self.page.url)
        if m:
            try:
                return float(m.group(1)), float(m.group(2))
            except ValueError:
                pass
        try:
            href = await self.page.locator("a[href*='/maps/dir/']").first.get_attribute(
                "href", timeout=1500)
            m = re.search(r"/dir/[^/]*/(-?\d+\.\d+),(-?\d+\.\d+)", href or "")
            if m:
                return float(m.group(1)), float(m.group(2))
        except (PlaywrightTimeoutError, PlaywrightError, ValueError):
            pass
        return None

    async def _extract_page_place_id(self) -> str | None:
        """Google's own identifier for whatever page we actually landed on.

        Two things this has to get right, both learned from live pages.

        The URL does NOT carry a ChIJ id. A Maps place URL identifies the place
        with a hex feature id instead:

            /maps/place/Dollar+General/@35.917,-80.583,17z/data=...!1s0x8853c25e63664061:0x93f5682775aaa02e!8m2!...

        so the old `!1s(ChIJ...)` pattern could never match, and the function
        fell through to the HTML.

        In the HTML the ChIJ id IS present, but not wrapped in double quotes,
        so the old `"(ChIJ...)"` pattern missed it too. The result was that
        this returned None on every real page - which quietly disabled the
        place_id tier of verify_store, the strongest check we have.

        The match is done inside the browser so we never pull a 400 KB DOM
        across the wire just to run one regex over it.
        """
        url = self.page.url
        m = re.search(r"(ChIJ[\w-]{20,})", url)
        if m:
            return m.group(1)
        try:
            pid = await self.page.evaluate(
                "() => { const m = document.documentElement.innerHTML"
                ".match(/ChIJ[\\w-]{20,}/); return m ? m[0] : null; }")
            if pid:
                return pid
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            return None
        return None

    async def _extract_page_feature_id(self) -> str | None:
        """The hex feature id from the URL (`!1s0x...:0x...`).

        A second, independent identifier for the same place. It is what the URL
        actually carries, so it is available even when the ChIJ id is not, and
        two pages sharing it are certainly the same place.
        """
        m = re.search(r"!1s(0x[0-9a-f]+:0x[0-9a-f]+)", self.page.url)
        return m.group(1) if m else None

    async def _record_landed_identity(self, store: Any) -> None:
        """Write the page's Google ids back to the batch row if it lacked them."""
        if not self.db:
            return
        has_pid = (store.get("google_place_id") or "").startswith("ChIJ")
        has_fid = bool(store.get("google_feature_id"))
        if has_pid and has_fid:
            return
        try:
            page_pid = None if has_pid else await self._extract_page_place_id()
            page_fid = None if has_fid else await self._extract_page_feature_id()
        except (PlaywrightTimeoutError, PlaywrightError) as exc:
            self.logger.warning(f"Could not read page ids: {exc}")
            return
        if page_pid or page_fid:
            self.db.update_store_identity(store["store_id"], page_pid, page_fid)
            self.logger.info(f"Recorded landed ids: place_id={page_pid} feature_id={page_fid}")

    async def verify_store(self, store: Any) -> bool:
        """Verify we're on the right store page.

        Strategy: name match is the strong signal. Lat/lon is a tiebreaker for
        chain stores (Walmart, Dollar General) where a name-only match could
        otherwise pull the wrong location. We reject only when name fails AND
        coordinates are far off — Overture's input_lat/lon is often 1–2 km off
        Google's coordinates even for the right store, so a strict hard reject
        on distance wrongly rejected ~25 stores in batch1.

        Matching priority:
          1. Name exact/substring match (+ optional lat/lon sanity check for chains)
          2. Address fuzzy match (gmaps_address vs page address)
          3. Name word overlap >= 50%
        """
        # ── Tier -1: we asked Google for this exact place_id and it gave us a
        # business page. That IS the identity check, and nothing measured on the
        # page can outrank it.
        #
        # Getting this wrong cost real stores. The first long run rejected a
        # Harris Teeter whose pin sat 3.14 km from the stored coordinate and an
        # ALDI at 11.66 km, both navigated by their own place_id. The page was
        # right in both cases; the stored coordinate was stale. Overture
        # coordinates are a rooftop or parcel centroid from a conflated feed and
        # routinely disagree with Google's pin, which is exactly why the original
        # 10 km veto existed and why tightening it to 3 km for chains is only
        # safe when identity is genuinely in doubt.
        #
        # Distance is still worth recording, because a large gap means the stored
        # coordinate needs repair even though the store is correct.
        if getattr(self, "_navigated_by_place_id", False):
            coords = await self._extract_page_latlon()
            tlat = store.get("input_lat")
            tlon = store.get("input_lon")
            if coords and tlat is not None and tlon is not None:
                try:
                    d = _haversine_km(float(tlat), float(tlon), coords[0], coords[1])
                    if d > 5.0:
                        self.logger.warning(
                            f"Verified by place_id, but the stored coordinate is {d:.2f} km "
                            f"from Google's pin. Store is correct; the coordinate is stale.")
                except (TypeError, ValueError):
                    pass
            self.logger.info("Verified by place_id navigation")
            return True

        # ── Tier 0: place_id. Decisive in BOTH directions. ──
        #
        # Runs once per store, so the page.content() fallback inside
        # _extract_page_place_id is affordable here in a way it would never be
        # inside the scroll loop.
        target_pid = (store.get("google_place_id") or "").strip()
        if target_pid.startswith("ChIJ"):
            page_pid = await self._extract_page_place_id()
            if page_pid:
                if page_pid == target_pid:
                    self.logger.info(f"Verified by place_id: {target_pid}")
                    return True
                self.logger.warning(
                    f"place_id mismatch: wanted {target_pid}, landed on {page_pid} — rejecting")
                return False

        # ── Tier 0b: feature id. Decisive in both directions when the page's
        # URL carries one, which a business page reached any way normally does.
        target_fid = (store.get("google_feature_id") or "").strip().lower()
        if target_fid and not target_pid.startswith("ChIJ"):
            page_fid = (await self._extract_page_feature_id() or "").lower()
            if page_fid:
                if page_fid == target_fid:
                    self.logger.info(f"Verified by feature id: {target_fid}")
                    return True
                self.logger.warning(
                    f"feature id mismatch: wanted {target_fid}, landed on {page_fid} - rejecting")
                return False

        # ── Geometry, using the business pin rather than the map camera ──
        # input_lat/lon can legitimately be 0.0-adjacent, so test for None
        # explicitly instead of relying on truthiness.
        target_lat = store.get("input_lat")
        if target_lat is None:
            target_lat = store.get("gmaps_lat")
        target_lon = store.get("input_lon")
        if target_lon is None:
            target_lon = store.get("gmaps_lon")
        page_coords = await self._extract_page_latlon()
        dist_km: float | None = None
        if target_lat is not None and target_lon is not None and page_coords:
            try:
                dist_km = _haversine_km(float(target_lat), float(target_lon),
                                        page_coords[0], page_coords[1])
                self.logger.info(f"Business pin is {dist_km:.2f} km from target")
            except (TypeError, ValueError):
                dist_km = None

        # ── Extract page name ──
        target_name = (store.get("gmaps_name") or store.get("input_name") or "").lower().strip()
        page_name = ""
        for sel in ["h1.DUwDvf", "h1.fontHeadlineLarge", "div[role='main'] h1"]:
            try:
                page_name = (await self.page.locator(sel).first.inner_text(timeout=2000)).strip()
                if page_name:
                    break
            except (PlaywrightTimeoutError, PlaywrightError):
                continue

        if not target_name:
            return bool(page_name)
        if not page_name:
            return False

        page_name_l = page_name.lower()

        # A name match on a chain carries almost no information: every Dollar
        # General in the state matches "dollar general" exactly. For those, the
        # distance IS the identity test, and the scale that matters is the gap
        # between neighbouring branches, which is a few kilometres in town, not
        # ten. For an independent store the name is genuinely distinguishing and
        # the threshold can stay loose enough to absorb the 1-2 km disagreement
        # between an Overture centroid and a Google pin.
        CHAIN_DISTANCE_REJECT_KM = 3.0
        INDEPENDENT_DISTANCE_REJECT_KM = 10.0
        CHAIN_TOKENS = (
            "dollar general", "family dollar", "dollar tree", "food lion", "walmart",
            "circle k", "sheetz", "7-eleven", "seven eleven", "harris teeter", "aldi",
            "lidl", "ingles", "publix", "kangaroo", "speedway", "exxon", "shell",
            "bp", "cvs", "walgreens", "target", "piggly wiggly", "lowes foods",
            "sam's club", "sams club", "costco", "food city", "quality mart",
        )
        is_chain = any(tok in target_name for tok in CHAIN_TOKENS)
        limit_km = CHAIN_DISTANCE_REJECT_KM if is_chain else INDEPENDENT_DISTANCE_REJECT_KM

        # ── Name exact/substring match ──
        if target_name in page_name_l or page_name_l in target_name:
            if dist_km is not None and dist_km > limit_km:
                self.logger.warning(
                    f"Name matched ('{page_name}') but business pin is {dist_km:.2f} km off, "
                    f"limit {limit_km} km — wrong location, rejecting"
                )
                return False
            if dist_km is None and is_chain:
                # A chain name with no geometry to corroborate it is exactly the
                # case that produced the wrong-location contamination. Refusing
                # here costs a re-queue; accepting costs a silently wrong store.
                self.logger.warning(
                    f"Chain name '{page_name}' with no usable coordinates — refusing to verify")
                return False
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
                    # The house number must match, not merely share a digit run
                    # with something else in the string. The old test intersected
                    # every number in both addresses, so a common ZIP was enough
                    # to "verify" 7400 The Plaza against 1000 The Plaza.
                    def _house_number(text: str) -> str | None:
                        m = re.match(r"\s*(\d+)", text.strip())
                        return m.group(1) if m else None

                    t_house = _house_number(target_addr)
                    p_house = _house_number(page_addr)
                    nums_match = bool(t_house and p_house and t_house == p_house)

                    # Check if key words overlap
                    target_words = set(re.findall(r'[a-z]+', target_addr)) - {'st', 'rd', 'ave', 'dr', 'ln', 'ct', 'blvd', 'sw', 'nw', 'se', 'ne', 'n', 's', 'e', 'w'}
                    page_words = set(re.findall(r'[a-z]+', page_addr)) - {'st', 'rd', 'ave', 'dr', 'ln', 'ct', 'blvd', 'sw', 'nw', 'se', 'ne', 'n', 's', 'e', 'w', 'address'}
                    words_overlap = len(target_words & page_words)

                    if nums_match and words_overlap >= 1:
                        self.logger.info(f"Verified by address match: '{page_addr}' ~ '{target_addr}'")
                        return True
            except (PlaywrightTimeoutError, PlaywrightError, OSError):
                pass

        # ── Name word overlap (fuzzy) ──
        # Fail closed. The previous version returned True when the target name
        # tokenised to nothing, so "7-Eleven" and any other unparseable name were
        # accepted on whatever page happened to be open.
        target_words = [w for w in re.findall(r"[a-z0-9]+", target_name) if len(w) >= 3]
        if not target_words:
            self.logger.warning(f"Target name '{target_name}' has no usable tokens — refusing")
            return False
        matches = sum(1 for w in target_words if w in page_name_l)
        if matches >= max(1, int(len(target_words) * 0.5)):
            if dist_km is not None and dist_km > limit_km:
                self.logger.warning(
                    f"Fuzzy name match but {dist_km:.2f} km off — rejecting")
                return False
            if dist_km is None:
                self.logger.warning("Fuzzy name match with no geometry to corroborate — refusing")
                return False
            self.logger.info(f"Verified by fuzzy name match: {matches}/{len(target_words)} words")
            return True

        self.logger.warning(f"Verification failed: target='{target_name}' page='{page_name}'")
        return False

    async def is_permanently_closed(self) -> bool:
        for token in ["Permanently closed", "Temporarily closed"]:
            try:
                if await self.page.locator(f"text='{token}'").first.is_visible(timeout=1500):
                    return True
            except (PlaywrightTimeoutError, PlaywrightError):
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
                    except (PlaywrightTimeoutError, PlaywrightError):
                        continue
            except (PlaywrightTimeoutError, PlaywrightError):
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
            except (PlaywrightTimeoutError, PlaywrightError, OSError):
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
        except (PlaywrightTimeoutError, PlaywrightError):
            pass
        try:
            rt = await self.page.locator("div.F7nice span[aria-hidden='true']").first.inner_text(timeout=1500)
            info["overall_rating"] = float(rt.replace(",", "."))
        except (PlaywrightTimeoutError, PlaywrightError, ValueError):
            pass
        info["total_reviews"] = await self._extract_total_reviews()
        try:
            info["address"] = (await self.page.locator("button[data-item-id='address']").get_attribute("aria-label", timeout=1200) or "").replace("Address: ", "")
        except (PlaywrightTimeoutError, PlaywrightError):
            pass
        try:
            info["phone"] = (await self.page.locator("button[data-item-id*='phone:tel']").get_attribute("aria-label", timeout=1200) or "").replace("Phone: ", "")
        except (PlaywrightTimeoutError, PlaywrightError):
            pass
        try:
            info["website"] = await self.page.locator("a[data-item-id='authority']").get_attribute("href", timeout=1200)
        except (PlaywrightTimeoutError, PlaywrightError):
            pass
        try:
            info["category"] = await self.page.locator("button.DkEaL").first.inner_text(timeout=1000)
        except (PlaywrightTimeoutError, PlaywrightError):
            pass
        self.logger.info(f"Business: {info['name']} | Rating: {info['overall_rating']} | Reviews: {info['total_reviews']}")
        return info

    # ── Reviews tab ─────────────────────────────────────────────────────

    async def _click_reviews_tab(self) -> bool:
        """Click the Reviews tab on a business page. Returns True if reviews panel is loaded."""
        self.logger.info("Clicking Reviews tab...")

        # Give the tab strip itself a chance to mount before hunting through
        # the (larger, slower) individual selectors below.
        try:
            await self.page.locator("button[role='tab'], div[role='tab']").first.wait_for(
                state="visible", timeout=6000)
        except (PlaywrightTimeoutError, PlaywrightError):
            pass

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
                    await btn.click(timeout=3000)
                    # Wait for the reviews panel to actually load
                    try:
                        await self.page.wait_for_selector(
                            "div.jftiEf, div[data-review-id]",
                            timeout=10000,
                        )
                        self.logger.info("Reviews panel loaded successfully")
                        return True
                    except (PlaywrightTimeoutError, PlaywrightError):
                        # The list may simply not be mounted yet: nudge the panel once.
                        await self._scroll_reviews_panel(delta=700)
                        try:
                            await self.page.wait_for_selector(
                                "div.jftiEf, div[data-review-id]", timeout=5000)
                            self.logger.info("Reviews panel loaded after a nudge scroll")
                            return True
                        except (PlaywrightTimeoutError, PlaywrightError):
                            pass
                        self.logger.warning("Reviews tab clicked but panel didn't load, trying next selector")
                        continue
            except (PlaywrightTimeoutError, PlaywrightError):
                continue

        # No Reviews tab anywhere. On a signed-out session this is often
        # because the page is showing Maps' "limited view" (no tab strip at
        # all) rather than a compact layout worth clicking around in — and
        # the fallback below would just walk into a sign-in modal. Bail out
        # fast instead of burning the click-around fallback on a page that
        # was never going to have a Reviews tab.
        await self._dismiss_signin_modal()
        if await self.is_signed_out():
            self.logger.warning("No Reviews tab and session is signed out — limited view")
            self.last_stop_reason = "stalled"
            return False

        # Recovery: we may be in compact/sidebar view — click the store name
        # or review count link to force the full detail view with tabs.
        # "Write a review" is deliberately excluded: it opens a sign-in modal,
        # not the detail view, and that modal then blocks every later click.
        self.logger.warning("No Reviews tab found — attempting to expand to full detail view")
        expand_selectors = [
            # Click review count text (e.g. "8 reviews") to go directly to reviews
            "button[aria-label*='review']:not([aria-label*='Write' i])",
            "span[aria-label*='review']:not([aria-label*='Write' i])",
            "a[aria-label*='review']:not([aria-label*='Write' i])",
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
                    await el.click(timeout=3000)
                    await asyncio.sleep(random.uniform(2.0, 3.5))
                    await self._dismiss_signin_modal()
                    # Now retry finding the Reviews tab
                    for tab_sel in tab_selectors:
                        try:
                            btn = self.page.locator(tab_sel).first
                            if await btn.is_visible(timeout=2500):
                                self.logger.info(f"Found Reviews tab after expand: {tab_sel}")
                                await btn.click(timeout=3000)
                                try:
                                    await self.page.wait_for_selector(
                                        "div.jftiEf, div[data-review-id]",
                                        timeout=10000,
                                    )
                                    self.logger.info("Reviews panel loaded after expand")
                                    return True
                                except (PlaywrightTimeoutError, PlaywrightError):
                                    continue
                        except (PlaywrightTimeoutError, PlaywrightError):
                            continue
            except (PlaywrightTimeoutError, PlaywrightError):
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

    async def _sort_reviews(self, sort_order: str = "newest") -> str:
        """Click the sort button and select the desired sort order.

        Returns one of three outcomes rather than a plain bool, because
        "the option we wanted wasn't in the menu" and "there is no menu to
        speak of" call for different responses from the caller:

          "applied"           the ordering was actually changed.
          "option_missing"    the menu opened but this rung isn't in it —
                               try the next rung.
          "menu_unavailable"  the sort button/menu never opened at all
                               (usually a signed-out session) — do not
                               keep walking the ladder, every rung will
                               fail the same way.

        "relevant" used to return immediately on the theory that it is Google's
        default. Inside the rotation ladder the panel has already been re-sorted
        to newest or oldest, so returning early left it there and re-walked an
        identical list. It is now selected explicitly like any other rung.

        sort_order values (Google offers exactly these four; see strategy.SORT_LADDER):
          "newest"   → Newest (every store starts here)
          "relevant" → Most relevant (Google's default)
          "lowest"   → Lowest rating
          "highest"  → Highest rating
        """
        if self.signed_out:
            # Clicking Sort while signed out opens the sign-in modal, not the
            # menu — every rung would fail identically, so don't even try.
            self.logger.info("Signed out — skipping Sort click (it only opens the sign-in modal)")
            return "menu_unavailable"

        self.logger.info(f"Sorting reviews by {sort_order}...")
        await asyncio.sleep(random.uniform(1.0, 2.0))

        sort_btn_selectors = [
            "button[aria-label*='Sort reviews']",
            "button[aria-label='Sort']",
            "button[data-value='Sort']",
            "button.g88MCb",
        ]

        # Google Maps offers exactly four orderings, in this menu order: Most relevant,
        # Newest, Highest rating, Lowest rating. There is no "Oldest"; the rung that asked
        # for it never applied once in the 16-23 Sep forrester run.
        labels = {"relevant": "Most relevant", "newest": "Newest",
                  "highest": "Highest rating", "lowest": "Lowest rating"}
        positions = {"relevant": 1, "newest": 2, "highest": 3, "lowest": 4}
        if sort_order not in labels:
            self.logger.warning(f"Unknown sort order '{sort_order}' — Google offers {list(labels)}")
            return "option_missing"
        label = labels[sort_order]
        target_options = [
            f"div[role='menuitemradio']:has-text('{label}')",
            f"div[role='menuitem']:has-text('{label}')",
            f"li[role='menuitemradio']:has-text('{label}')",
            f"div[role='menuitemradio']:nth-child({positions[sort_order]})",
        ]
        menu_items = "[role='menuitemradio'], [role='menuitem']"

        for sort_btn_sel in sort_btn_selectors:
            try:
                btn = self.page.locator(sort_btn_sel).first
                if await btn.is_visible(timeout=3000):
                    self.logger.info(f"Found sort button: {sort_btn_sel}")
                    # The menu opens asynchronously. Looking for the option 1-2 s after
                    # the click missed it on 2,032 of 2,035 attempts in the forrester log;
                    # wait for the menu itself, and re-open it once if it never appeared.
                    menu_opened = False
                    for _open_try in range(2):
                        await btn.click(timeout=3000)
                        try:
                            await self.page.locator(menu_items).first.wait_for(state="visible", timeout=4000)
                            menu_opened = True
                            break
                        except (PlaywrightTimeoutError, PlaywrightError):
                            self.logger.info("Sort menu did not open — clicking the sort button again")
                            if await self._dismiss_signin_modal():
                                self.signed_out = True
                                self.logger.warning(
                                    "Sort opened a sign-in modal instead of the menu — "
                                    "marking session signed out")
                                return "menu_unavailable"
                            await asyncio.sleep(random.uniform(0.8, 1.6))
                    if not menu_opened:
                        self.logger.warning("Sort menu never opened")
                        return "menu_unavailable"
                    await asyncio.sleep(random.uniform(0.4, 0.9))

                    for opt_sel in target_options:
                        try:
                            opt = self.page.locator(opt_sel).first
                            if await opt.is_visible(timeout=2500):
                                self.logger.info(f"Clicking '{sort_order}' option: {opt_sel}")
                                await opt.click(timeout=3000)
                                await asyncio.sleep(random.uniform(2.5, 4.0))
                                self.logger.info(f"Sort by {sort_order} applied")
                                return "applied"
                        except (PlaywrightTimeoutError, PlaywrightError):
                            continue

                    # The menu is open and the option we wanted is not in it. It
                    # must be closed before returning: a modal dropdown left
                    # hanging over the panel swallows every subsequent scroll, so
                    # the store stalls for the rest of its run and looks exactly
                    # like it reached the end of the reviews.
                    #
                    # Escape is safe here specifically because the open element is
                    # the sort menu. The overlay dismisser elsewhere refuses
                    # Escape because on a search-results page it clears the whole
                    # results panel.
                    try:
                        await self.page.keyboard.press("Escape")
                        await asyncio.sleep(random.uniform(0.4, 0.9))
                    except (PlaywrightTimeoutError, PlaywrightError):
                        pass
                    self.logger.warning(
                        f"'{sort_order}' option not in the sort menu — menu dismissed, "
                        f"ordering unchanged")
                    return "option_missing"
            except (PlaywrightTimeoutError, PlaywrightError):
                continue

        self.logger.warning("Could not find sort button — proceeding without sorting")
        return "menu_unavailable"

    # ── Scroll utilities ────────────────────────────────────────────────

    SCROLL_MARK = "[data-scrape-scroll='1']"

    async def _find_scrollable_container(self) -> str | None:
        """Find the element that actually scrolls the review list.

        The previous version returned the first selector whose count was above
        zero, from a list ending in the very general `div.m6QErb` and
        `div[role='main']`. Those match several elements on a Maps place page,
        and the scroll then ran `document.querySelector(sel)`, which takes the
        first match in DOM order. When that first match was not the review panel
        the scroll silently did nothing: no error, no exception, just a page that
        never moved, 60 empty cycles, and a store that looked like it had run out
        of reviews.

        Observed live during the September smoke test. One store collected 40 of
        40 and the next collected 5 of 40 from a listing with 1,191 reviews, on
        identical code, because the two pages disagreed about which element came
        first.

        So: start from a real review node, walk up to the nearest ancestor that
        is genuinely scrollable, and tag it. Addressing the tag removes the
        ambiguity entirely. Falls back to the old selector list if no review node
        is mounted yet.
        """
        try:
            tagged = await self.page.evaluate(
                """
                (() => {
                  document.querySelectorAll('[data-scrape-scroll]')
                          .forEach(e => e.removeAttribute('data-scrape-scroll'));
                  const rev = document.querySelector('div[data-review-id], div.jftiEf');
                  if (!rev) return false;
                  let el = rev.parentElement;
                  while (el && el !== document.body) {
                    const st = window.getComputedStyle(el);
                    const scrolls = /(auto|scroll)/.test(st.overflowY);
                    if (scrolls && el.scrollHeight > el.clientHeight + 40) {
                      el.setAttribute('data-scrape-scroll', '1');
                      return true;
                    }
                    el = el.parentElement;
                  }
                  return false;
                })()
                """
            )
            if tagged:
                return self.SCROLL_MARK
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            pass

        for sel in ("div.m6QErb.DxyBCb", "div.m6QErb.DByNcb", "div.m6QErb",
                    "div[role='main']"):
            try:
                if await self.page.locator(sel).count() > 0:
                    self.logger.debug(f"Scroll container fallback: {sel}")
                    return sel
            except (PlaywrightTimeoutError, PlaywrightError):
                continue
        return None

    async def _scroll_reviews_panel(self, delta: int = 3000) -> None:
        """Scroll the reviews panel using JavaScript for reliability."""
        # Try JavaScript scroll on the container first
        # Verify the scroll actually moved something. The old version assumed a
        # successful evaluate meant a successful scroll, and caught only OSError
        # and ImportError, neither of which Playwright raises. A scroll that
        # silently did nothing is indistinguishable from a page with no more
        # reviews, which is the most expensive confusion in this scraper.
        container_sel = await self._find_scrollable_container()
        if container_sel:
            try:
                moved = await self.page.evaluate(
                    """
                    ((args) => {
                      const el = document.querySelector(args.sel);
                      if (!el) return null;
                      const before = el.scrollTop;
                      el.scrollTop += args.delta;
                      return {before: before, after: el.scrollTop,
                              max: el.scrollHeight - el.clientHeight};
                    })
                    """,
                    {"sel": container_sel, "delta": delta},
                )
                if moved and (moved["after"] != moved["before"]
                              or moved["after"] >= moved["max"] - 1):
                    return
                if moved is not None:
                    self._extract_failures["scroll_no_movement"] += 1
            except (PlaywrightTimeoutError, PlaywrightError, OSError):
                pass

        # Fallback: mouse wheel over the review list.
        try:
            scroll_x, scroll_y = await self._get_scroll_position()
            await self.page.mouse.move(scroll_x, scroll_y)
            await self.page.mouse.wheel(0, delta)
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            pass

    async def _get_scroll_position(self) -> tuple[float, float]:
        for sel in ["div.jftiEf", "div.m6QErb.DxyBCb", "div.m6QErb.DByNcb", "div.m6QErb"]:
            try:
                node = self.page.locator(sel).first
                box = await node.bounding_box(timeout=2000)
                if box:
                    return box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
            except (PlaywrightTimeoutError, PlaywrightError):
                continue
        return 400.0, 450.0

    # ── Single review extraction ────────────────────────────────────────

    async def _expand_all_reviews(self) -> int:
        """Click every truncated review's "More"/"See more" button in one pass.

        Used to be done per-review, inside _extract_review, with Playwright
        clicks that carry the default 30s timeout. On a signed-out session the
        sign-in modal sits on top of the panel and intercepts every one of
        those clicks, so a single review could burn 90s just expanding its
        text. Doing it as one page.evaluate() means the browser performs the
        clicks directly — a modal in front of the page can't block a click
        that never goes through Playwright's own hit-testing.

        Each button is tagged data-scrape-expanded so a later cycle doesn't
        click it again. Returns the number of buttons clicked this pass.
        """
        try:
            return await self.page.evaluate(
                """
                (() => {
                  let n = 0;
                  const sels = ["button.w8nwRe", "button[aria-label*='See more']", "a.review-more-link"];
                  for (const rev of document.querySelectorAll('div[data-review-id]')) {
                    for (const sel of sels) {
                      for (const btn of rev.querySelectorAll(sel)) {
                        if (btn.hasAttribute('data-scrape-expanded')) continue;
                        btn.setAttribute('data-scrape-expanded', '1');
                        try { btn.click(); n++; } catch (e) {}
                      }
                    }
                  }
                  return n;
                })()
                """
            )
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            return 0

    async def _extract_review(self, el: Any) -> dict[str, Any] | None:
        try:
            rid = await el.get_attribute("data-review-id")
            if not rid:
                return None

            name, rating, date_rel, review_text = "Anonymous", None, "", ""
            try:
                name = await el.locator("div.d4r55").first.inner_text(timeout=500)
            except (PlaywrightTimeoutError, PlaywrightError):
                pass
            try:
                aria = await el.locator("span.kvMYJc").first.get_attribute("aria-label", timeout=500)
                if aria:
                    rating = int(re.findall(r"\d+", aria)[0])
            except (PlaywrightTimeoutError, PlaywrightError, ValueError, IndexError):
                pass
            try:
                date_rel = await el.locator("span.rsqaWe").first.inner_text(timeout=500)
            except (PlaywrightTimeoutError, PlaywrightError):
                pass
            try:
                review_text = (await el.locator("span.wiI7pd").first.inner_text(timeout=500)).replace("\x00", "")
            except (PlaywrightTimeoutError, PlaywrightError):
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
            except (PlaywrightTimeoutError, PlaywrightError, ValueError):
                pass
            # helpful_count: the old selector was button[aria-label*='helpful'].
            # CSS attribute matching is case-sensitive and Google's label is
            # "Helpful", so it never matched once across 1.2M rows. The ' i'
            # flag makes the match case-insensitive; the alternatives cover the
            # variants where the count sits in text beside the control.
            # button[aria-label*='Like' i] is deliberately NOT here. The DOM
            # probe (Remediation/probe_review_dom.py) counted it on eight live
            # reviews and found exactly one match on every single one: it is the
            # Like control itself, which every review carries whether or not
            # anyone has pressed it. Parsing a number out of its label produced
            # a column that looked populated and meant nothing.
            #
            # On the current Maps UI none of the real helpful-count selectors
            # match at all, so this field is usually unobtainable. That is
            # recorded as a miss rather than written as a zero, and the field is
            # treated as optional so it does not cry wolf on every store.
            helpful_found = False
            for sel in ("button[aria-label*='helpful' i]",
                        "button[jsaction*='review.thumbUp']"):
                try:
                    loc = el.locator(sel).first
                    if await loc.count() == 0:
                        continue
                    lbl = (await loc.get_attribute("aria-label", timeout=350)) or ""
                    txt = (await loc.inner_text(timeout=350)) or ""
                    m = re.search(r"(\d[\d,]*)", lbl) or re.search(r"(\d[\d,]*)", txt)
                    helpful = int(m.group(1).replace(",", "")) if m else 0
                    helpful_found = True
                    break
                except (PlaywrightTimeoutError, PlaywrightError, ValueError):
                    continue
            if not helpful_found:
                self._extract_failures["helpful_count"] += 1

            # photo_count: the old selector counted div.Iop04 img. Google renders
            # review thumbnails as buttons carrying a CSS background-image, so
            # there is no img to count. locator().count() returns 0 for a
            # non-matching selector and never raises, which is why not even an
            # exception marked the failure.
            # Scope every selector to the review-photo container.
            #
            # A first version of this fix fell back to counting anything with a
            # googleusercontent URL inside the review node. That matched the
            # reviewer's avatar, which is present on every review, so the field
            # went from "always 0" to "always 1". Replacing one constant with
            # another constant is not a fix, and the smoke test caught it only
            # because the value was implausibly uniform.
            #
            # There is no fallback on purpose. If none of these match, the count
            # is unknown and is recorded as a miss, rather than being invented.
            # button[aria-label*='Photo' i] is deliberately NOT in this list.
            # Google labels the reviewer's avatar "Photo of <name>", so that
            # selector matches exactly one element on every review and turns the
            # field into a constant 1. It was in the first version of this fix
            # and the live smoke test caught it: forty reviews, every one
            # reporting exactly one photo. An implausibly uniform distribution is
            # the same defect as an all-zero column wearing a different number.
            photo_found = False
            for sel in ("button.Tya61d", "div.KtCyie button", "[data-photo-index]"):
                try:
                    n = await el.locator(sel).count()
                    if n > 0:
                        photo_count, photo_found = n, True
                        break
                except (PlaywrightTimeoutError, PlaywrightError):
                    continue
            if not photo_found:
                # Unknown, not zero. The column keeps its 0 default for schema
                # compatibility, but the miss is counted so assert_field_coverage
                # reports it rather than letting it pass as a measurement.
                self._extract_failures["photo_count"] += 1
            try:
                if await el.locator("text='Response from the owner'").first.is_visible(timeout=250):
                    has_owner_response = 1
            except (PlaywrightTimeoutError, PlaywrightError):
                pass
            # service_type: div.PBK6be is the attribute-chip container but
            # span.pVtsbf is not the value span, so this was NULL in every row.
            # Reading the container's text and parsing the labels out of it is
            # robust to the class rotating, which it does.
            try:
                block = await el.locator("div.PBK6be").first.inner_text(timeout=350)
            except (PlaywrightTimeoutError, PlaywrightError):
                block = ""
            if block:
                hits = re.findall(
                    r"(Service|Meal type|Price per person|Food|Atmosphere|Dine in|"
                    r"Take ?out|Delivery|Curbside pickup)\s*:?\s*([^\n|]{0,40})", block)
                service_type = ", ".join(
                    (k.strip() + ": " + v.strip()).strip(": ") for k, v in hits)
            if not service_type:
                self._extract_failures["service_type"] += 1
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
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            return None

    # ── Main review collection loop ─────────────────────────────────────

    def assert_field_coverage(self, collected: int, tolerance: float = 0.95) -> None:
        """Shout when a field found no element on nearly every review.

        This is the fifteen-line check that was missing. helpful_count,
        photo_count and service_type were extracted for all 1,201,237 reviews and
        every one of them is empty, because a selector that matches nothing is
        indistinguishable from a value that is genuinely zero once a try/except
        writes the default. The scraper stayed alive and produced three columns
        of nothing for two months.

        A field failing on a few reviews is normal. A field failing on 95% of
        them means Google renamed something, and that is worth a loud line in the
        log at the end of every single store rather than a discovery months later.
        """
        if collected < 20:
            self._extract_failures.clear()
            return
        # Fields whose absence is a fact about Google's UI rather than a stale
        # selector. Verified against live pages with
        # Remediation/probe_review_dom.py rather than assumed:
        #
        #   service_type   Google's service chips (Dine in, Takeout, Delivery)
        #                  are a restaurant feature. A grocery store has none.
        #
        #   helpful_count  On the current Maps UI no helpful-count element is
        #                  rendered at all. The only thing present is the Like
        #                  control itself, on every review, pressed or not.
        #                  Counting it produces a column that looks populated
        #                  and means nothing.
        #
        #   photo_count    Same story. Every candidate photo-container selector
        #                  matched zero elements across eight live reviews; the
        #                  only constant match was the reviewer's avatar.
        #
        # These stay in the extractor in case a UI variant does expose them, but
        # a miss is expected and must not read as an emergency on every store.
        # Treat all three as UNAVAILABLE in analysis, not as measured zeros.
        OPTIONAL = {"service_type", "helpful_count", "photo_count"}
        for field, misses in sorted(self._extract_failures.items()):
            rate = misses / collected
            if rate >= tolerance and field in OPTIONAL:
                self.logger.info(
                    f"Field '{field}' absent on all {collected} reviews. Expected for "
                    f"non-restaurant categories; only investigate if it is also absent "
                    f"on places that do show service attributes."
                )
            elif rate >= tolerance:
                self.logger.error(
                    f"FIELD COVERAGE FAILURE: '{field}' found no element on "
                    f"{misses}/{collected} reviews ({rate:.0%}). The selector is "
                    f"almost certainly stale — this field is being written as a "
                    f"default, not measured."
                )
            elif rate >= 0.5:
                self.logger.warning(
                    f"Field '{field}' missing on {rate:.0%} of reviews this store")
        self._extract_failures.clear()

    async def _collect_reviews(
        self,
        target_count: int,
        store_id: int | None = None,
        sort_order: str = "newest",
        resuming: bool = False,
    ) -> tuple[int, bool] | tuple[list[dict[str, Any]], bool]:
        self.last_stop_reason = ""
        self.last_rungs_applied = 0
        if not await self._click_reviews_tab():
            self.logger.error("Could not find Reviews tab — aborting review collection")
            return (0, False) if store_id else ([], False)

        await self._dismiss_overlays()
        if await self.is_signed_out():
            self.logger.warning(f"AUTH SIGNED OUT: {self.current_auth_file}")
            self.signed_out = True

        # Resume: prefer saved sort order from a prior crashed/interrupted run,
        # so seen_ids picks up where we left off in the same review ordering.
        # The saved order is only honoured when the previous run of this store
        # was actually interrupted mid-traversal. It used to be applied
        # unconditionally and never cleared, so once a store ended on 'relevant'
        # every later run started there too, permanently defeating both the
        # "incomplete stores start on oldest" rule and the newest/relevant
        # alternation across attempts. 'relevant' is also the ordering under
        # which the consecutive-known early exit is least safe, because it is not
        # monotone, so pinning a store to it is the worst of the three outcomes.
        saved_sort = None
        if store_id and self.db:
            saved_sort, _ = self.db.get_store_resume_state(store_id)
            if saved_sort and resuming:
                self.logger.info(f"Resuming interrupted run with saved sort order '{saved_sort}'")
                sort_order = saved_sort
            elif saved_sort:
                self.logger.info(
                    f"Ignoring stale saved sort '{saved_sort}' — this is a fresh attempt")

        # The sort ladder (strategy.SORT_LADDER): Newest first, then Most relevant, then
        # the two rating orders. A run of reviews we already hold, or a stall, moves to
        # the next rung; the store stops only when every rung is spent. An interrupted
        # run resumes on the rung it was on.
        sort_queue = strategy.sort_ladder(resume_from=sort_order if resuming else None)
        sort_idx = 0
        while sort_idx < len(sort_queue):
            sort_result = await self._sort_reviews(sort_order=sort_queue[sort_idx])
            if sort_result == "applied":
                self.last_rungs_applied += 1
                break
            if sort_result == "menu_unavailable":
                self.logger.warning("Sort menu unavailable — skipping remaining sort rungs")
                sort_idx = len(sort_queue)
                break
            self.logger.info(f"Sort '{sort_queue[sort_idx]}' did not apply — trying the next rung")
            sort_idx += 1
        if sort_idx >= len(sort_queue):
            self.logger.warning("No sort order applied — collecting in the page's current order")
            sort_queue, sort_idx = ["relevant"], 0
        if store_id and self.db:
            self.db.update_store_resume_state(store_id, sort_order=sort_queue[sort_idx])

        max_reviews = min(max(int(target_count), 1), int(self.config["scraping"]["max_reviews_per_store"]))
        self.logger.info(f"Starting review collection: target={max_reviews}")

        existing_ids: set[str] = set()
        known_keys: set[str] = set()
        if store_id and self.db:
            existing_ids = self.db.get_existing_review_ids(store_id)
            known_keys = self.db.get_known_review_keys(store_id)
            if existing_ids:
                self.logger.info(f"Dedup guard: {len(existing_ids)} reviews already in DB for store {store_id}")
            if known_keys:
                self.logger.info(f"Master skip-list: {len(known_keys)} known-review keys for store {store_id}")
        seen_ids: set[str] = set(existing_ids)
        total_collected = len(existing_ids)
        # New reviews collected this run (excludes pre-existing batch DB reviews)
        new_collected = 0
        # Reviews the page loaded this run that we had not seen, whether or not they
        # were new to us. Progress means the panel is still loading reviews, not that
        # new ones were saved: stopping at a run of reviews master already holds never
        # reached the missing ones on stores that already had many.
        loaded_total = 0

        if total_collected > 0:
            self.logger.info(f"Resuming: {total_collected} reviews already in DB, collecting up to {max_reviews - total_collected} more")
        # Warmup phase: while scrolling past pre-existing reviews on resume,
        # we hit lots of "already-seen" rows but no NEW data. Don't count those
        # cycles toward stall — we haven't reached fresh territory yet.
        # Once we collect our first NEW review this run, warmup ends.
        in_warmup = total_collected > 0
        max_warmup_scrolls = 200
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

        # Wall-clock watchdog. stall_threshold/stall_cycles above count *scroll
        # cycles*, which is exactly what a signed-out session's sign-in-modal
        # click storm defeats: every cycle "completes" in the usual number of
        # cycles, just each one now takes up to a 30s click timeout instead of
        # a couple of seconds. This tracks actual elapsed time instead, so a
        # store that has stopped making progress gets cut loose regardless of
        # why each cycle is slow.
        scraping_cfg = self.config["scraping"]
        no_progress_timeout_s = float(scraping_cfg.get("no_progress_timeout_s", 600))
        min_rate_window_s = float(scraping_cfg.get("min_rate_window_s", 900))
        min_rate_reviews = int(scraping_cfg.get("min_rate_reviews", 20))
        early_stuck_s = float(scraping_cfg.get("early_stuck_s", 180))
        watchdog_start = last_session_save = time.monotonic()
        last_new_at = watchdog_start

        try:
            while total_collected < max_reviews and scroll_attempts < max_scrolls and not self._shutdown:
                if time.monotonic() - last_session_save >= 600:
                    await self.save_session()
                    last_session_save = time.monotonic()
                await self._dismiss_signin_modal()
                await self._expand_all_reviews()

                if await self.check_for_captcha():
                    # Flush buffer before breaking so no reviews are lost
                    if store_id and self.db and buffer:
                        self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
                        persisted = self.db.get_review_count(store_id)
                        total_collected = persisted
                        self.db.update_store_reviews_count(store_id, persisted)
                        buffer = []
                    self.last_stop_reason = "captcha"
                    self.trigger_global_pause("CAPTCHA during review collection")
                    break

                # Find all review elements currently in the DOM
                # Use only data-review-id — div.jftiEf can also match the outer
                # container, causing duplicate extraction
                visible = await self.page.locator("div[data-review-id]").all()
                new_in_cycle = 0
                loaded_in_cycle = 0

                for el in visible:
                    if total_collected >= max_reviews:
                        break
                    rid = await el.get_attribute("data-review-id")
                    if not rid or rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    loaded_in_cycle += 1
                    loaded_total += 1
                    record = await self._extract_review(el)
                    if not record:
                        continue

                    # Skip-list check: a review master already holds is not saved again,
                    # but scrolling continues past it.
                    if known_keys:
                        from .database import DatabaseManager
                        key = DatabaseManager.make_dedup_key(
                            record.get("reviewer_name"),
                            record.get("rating"),
                            record.get("review_text"),
                        )
                        if key in known_keys:
                            continue

                    new_in_cycle += 1
                    total_collected += 1
                    new_collected += 1
                    in_warmup = False  # we're in fresh territory now
                    if store_id and self.db:
                        buffer.append(record)
                        if len(buffer) >= batch_size:
                            self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
                            # Read the persisted count back so reviews_scraped
                            # can never drift away from the actual reviews
                            # table (the bug behind batch1's 2,263 phantom reviews).
                            persisted = self.db.get_review_count(store_id)
                            total_collected = persisted
                            self.db.update_store_reviews_count(store_id, persisted)
                            # Persist resume marker: most recent review we wrote.
                            self.db.update_store_resume_state(
                                store_id, last_review_id=record.get("review_id"),
                            )
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

                # Wall-clock watchdog — applies during warmup too, since a
                # signed-out session stalls on resume exactly the same way it
                # stalls on fresh territory.
                now_mono = time.monotonic()
                if loaded_in_cycle > 0:
                    last_new_at = now_mono
                watchdog_elapsed = now_mono - watchdog_start
                if now_mono - last_new_at >= no_progress_timeout_s:
                    self.logger.warning(
                        f"Watchdog: page loaded no reviews for {no_progress_timeout_s:.0f}s — stopping store")
                    self.last_stop_reason = "stalled"
                elif watchdog_elapsed >= min_rate_window_s and loaded_total < min_rate_reviews:
                    self.logger.warning(
                        f"Watchdog: page loaded only {loaded_total} reviews after "
                        f"{min_rate_window_s:.0f}s (min {min_rate_reviews}) — stopping store")
                    self.last_stop_reason = "stalled"
                elif (len(existing_ids) <= 5 and loaded_total <= 5
                      and watchdog_elapsed >= early_stuck_s
                      and self.last_rungs_applied <= 1):
                    # Only while sorting is not progressing. Walking all four orders
                    # takes about four minutes, so applying this unconditionally cut
                    # healthy passes off on the last order and forced a login swap.
                    self.logger.warning(
                        f"Watchdog: {len(existing_ids)} existing + {loaded_total} loaded after "
                        f"{early_stuck_s:.0f}s — stopping store")
                    self.last_stop_reason = "stalled"
                if self.last_stop_reason == "stalled":
                    reached_end = False
                    break

                # During warmup, skip stall counting unless we've scrolled too long
                # without ever finding new content (in which case we've over-shot
                # the resume zone or there are no more new reviews).
                if loaded_in_cycle > 0:
                    in_warmup = False  # past what the DB already held
                if in_warmup and loaded_in_cycle == 0:
                    if scroll_attempts > max_warmup_scrolls:
                        self.logger.info(
                            f"Warmup exceeded {max_warmup_scrolls} scrolls without "
                            f"new reviews — assuming end of stream"
                        )
                        in_warmup = False  # let normal stall handling take over
                else:
                    stall_cycles = stall_cycles + 1 if loaded_in_cycle == 0 else 0
                reached_end = False
                if not in_warmup and stall_cycles >= stall_threshold:
                    # Before giving up on this ordering after a stall, try a recovery
                    # scroll: jump far down + wait to let Google Maps lazy-load more.
                    recovered = 0
                    self.logger.info(f"Stall at {stall_cycles} cycles — attempting recovery scroll")
                    await self._scroll_reviews_panel(delta=10000)
                    await asyncio.sleep(random.uniform(4.0, 6.0))
                    for el in await self.page.locator("div[data-review-id]").all():
                        rid = await el.get_attribute("data-review-id")
                        if rid and rid not in seen_ids:
                            recovered += 1
                    if recovered > 0:
                        self.logger.info(f"Recovery found {recovered} new reviews — continuing")
                        stall_cycles = 0
                        continue

                    # Recovery scroll failed — try the next sort order before giving up.
                    # Switching sort re-renders the panel and surfaces reviews we
                    # couldn't reach via the previous sort.
                    sort_idx += 1
                    # Skip rungs that do not actually apply. _sort_reviews now
                    # reports whether the ordering changed, so a missing option
                    # costs one attempt instead of another 60 empty cycles spent
                    # re-reading the ordering we were already in. A menu that
                    # won't open at all means every remaining rung will fail
                    # the same way, so stop walking the ladder immediately
                    # instead of re-trying each one.
                    while sort_idx < len(sort_queue):
                        sort_result = await self._sort_reviews(sort_order=sort_queue[sort_idx])
                        if sort_result == "applied":
                            self.last_rungs_applied += 1
                            break
                        if sort_result == "menu_unavailable":
                            self.logger.warning("Sort menu unavailable — skipping remaining sort rungs")
                            sort_idx = len(sort_queue)
                            break
                        self.logger.info(
                            f"Sort '{sort_queue[sort_idx]}' did not apply — skipping that rung")
                        sort_idx += 1
                    if sort_idx < len(sort_queue):
                        next_sort = sort_queue[sort_idx]
                        self.logger.info(
                            f"Stall — re-sorted reviews to '{next_sort}' "
                            f"and continuing ({total_collected} collected so far)"
                        )
                        if store_id and self.db:
                            self.db.update_store_resume_state(store_id, sort_order=next_sort)
                        # Reset progress trackers for the new sort order
                        stall_cycles = 0
                        last_log_count = total_collected
                        # Scroll back near the top of the panel so the new ordering takes effect
                        try:
                            container_sel = await self._find_scrollable_container()
                            if container_sel:
                                await self.page.evaluate(
                                    f"(() => {{ const el = document.querySelector('{container_sel}');"
                                    f" if (el) el.scrollTop = 0; }})()"
                                )
                        except (PlaywrightTimeoutError, PlaywrightError, OSError):
                            pass
                        await asyncio.sleep(random.uniform(2.0, 4.0))
                        continue

                    # Few reviews held and few found only means "stuck" when sorting did
                    # not work; four orders walked signed in is a real ceiling.
                    self.last_stop_reason = (
                        "stalled" if (len(existing_ids) <= 5 and loaded_total <= 5
                                      and self.last_rungs_applied < 3)
                        else "exhausted"
                    )
                    self.logger.info(
                        f"All sort orders exhausted — stopping at {total_collected} reviews "
                        f"(reason={self.last_stop_reason})"
                    )
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

        except BaseException:
            # Must be BaseException, not Exception.
            #
            # asyncio.CancelledError has derived from BaseException since Python
            # 3.8, so `except Exception` never caught it and this flush never ran
            # on the path it was written for. The per-store timeout in worker.py
            # cancels the task, and a timeout is the ordinary way a large store
            # ends, so up to batch_save_interval - 1 reviews were discarded every
            # time. The dead isinstance() check that used to sit below is the
            # evidence the opposite was intended.
            #
            # The flush is wrapped so a failed save cannot mask the original
            # exception. Ordinary exceptions keep the previous behaviour of
            # falling through to the normal return, so a transient Playwright
            # hiccup still costs only the rest of one store. Cancellation and
            # shutdown propagate, because nothing further can run anyway.
            exc = sys.exc_info()[1]
            if store_id and self.db and buffer:
                self.logger.info(f"Flushing {len(buffer)} buffered reviews before exit")
                try:
                    self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
                    persisted = self.db.get_review_count(store_id)
                    total_collected = persisted
                    self.db.update_store_reviews_count(store_id, persisted)
                except Exception as save_exc:            # noqa: BLE001
                    self.logger.error(f"Buffer flush failed during teardown: {save_exc}")
                finally:
                    buffer = []
            if not isinstance(exc, Exception):
                raise
            self.logger.error(f"Collection aborted: {exc}")

        # Flush remaining buffer
        if store_id and self.db and buffer:
            self.db.save_reviews_batch(store_id, buffer, machine_id=self.machine_id)
            persisted = self.db.get_review_count(store_id)
            total_collected = persisted
            self.db.update_store_reviews_count(store_id, persisted)

        # Check if we hit the target (= reached end)
        if total_collected >= max_reviews:
            reached_end = True

        self.logger.info(f"Review collection complete: {total_collected} reviews in {scroll_attempts} scrolls (reached_end={reached_end})")
        if store_id:
            return total_collected, reached_end
        return in_memory, reached_end

    # ── Public scrape methods ───────────────────────────────────────────

    async def scrape_reviews(self, store_id: int, target_count: int,
                             sort_order: str = "newest",
                             resuming: bool = False) -> tuple[int, bool]:
        """Returns (review_count, reached_end).

        `resuming` says this store was left in_progress by a run that died, so
        the saved sort order should be re-entered. On a fresh attempt the saved
        order is stale and following it re-walks ground already covered.
        """
        result, reached_end = await self._collect_reviews(
            target_count=target_count, store_id=store_id,
            sort_order=sort_order, resuming=resuming)
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
        target = min(int(self.config["scraping"]["max_reviews_per_store"]), max(max_reviews, live if live > 0 else max_reviews))
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
        except (PlaywrightTimeoutError, PlaywrightError):
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
        target = min(int(self.config["scraping"]["max_reviews_per_store"]), max(max_reviews, live if live > 0 else max_reviews))
        self.logger.info(f"URL mode target: {target} reviews (live={live}, requested={max_reviews})")

        # Click reviews tab and collect
        reviews, _reached_end = await self._collect_reviews(target_count=target, store_id=None)
        return business, reviews

    async def scrape_single_store(self, store: Any) -> str:
        store_id = store["store_id"]
        master_reviews = int(store.get("master_reviews") or 0)
        raw_target = int(store.get("target_reviews") or 1000)
        review_cap = int(self.config["scraping"]["max_reviews_per_store"])
        target_reviews = min(review_cap, raw_target)
        store_type = "incomplete" if master_reviews > 0 else "fresh"

        await self.wait_if_paused()

        # Skip already-completed stores (dedup guard)
        if self.db and self.db.is_store_completed(store_id):
            self.logger.info(f"Store {store_id} already completed — skipping")
            return "skipped"

        # Effective target: how many MORE reviews this machine needs to collect
        effective_target = max(target_reviews - master_reviews, 0)
        self.logger.info(
            f"[{store_type.upper()}] target={target_reviews} (raw={raw_target}, cap={review_cap}), "
            f"master={master_reviews}, effective_target={effective_target}"
        )

        try:
            if not await self.navigate_to_store(store):
                if self._network_down:
                    # Wait the outage out and give this store one clean retry
                    # before deciding anything about it. Marking it failed here
                    # would spend a retry attempt on a fact about the machine.
                    self.logger.warning(
                        "All navigation strategies failed with a connectivity error. "
                        "Pausing until the network returns rather than failing the store.")
                    if not await self.wait_for_network():
                        return "interrupted"
                    if not await self.navigate_to_store(store):
                        raise RuntimeError("Failed to navigate to store page")
                else:
                    raise RuntimeError("Failed to navigate to store page")
            # Check for CAPTCHA or sign-in wall (banned account)
            block_result = await self.check_and_handle_blocks()
            if block_result:
                return "interrupted"
            if not await self.verify_store(store):
                raise RuntimeError("Store verification failed")
            await self._record_landed_identity(store)
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

            # Go for every review Google lists, not the stored target. The stored target
            # is usually a little under the live count, and that gap was being left behind.
            if (self.config["scraping"].get("target_all_live") and live_count > target_reviews
                    and live_count > 5):
                target_reviews = min(review_cap, live_count)
                effective_target = max(target_reviews - master_reviews, 0)
                self.logger.info(
                    f"Going for all reviews: target raised to the live count {target_reviews} "
                    f"(need {effective_target})")

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

            # Best-effort mode: stores with very low target (<10) often have a
            # mismatched/stale target. Trust Google's live count and pull
            # everything available (capped at max_reviews_per_store).
            best_effort = (raw_target < 10)
            if best_effort:
                if live_count > 0 and not limited_view_suspected:
                    scrape_target = min(live_count, int(self.config["scraping"]["max_reviews_per_store"]))
                else:
                    scrape_target = int(self.config["scraping"]["max_reviews_per_store"])
                self.logger.info(
                    f"Best-effort mode: raw_target={raw_target} < 10, scraping up to "
                    f"{scrape_target} (live={live_count})"
                )
            else:
                # Scrape only what we actually need — don't over-scrape
                scrape_target = effective_target
                # If Google shows fewer reviews than we need, cap to avoid fruitless scrolling
                if live_count > 0 and live_count < scrape_target and not limited_view_suspected:
                    self.logger.info(f"Live count ({live_count}) < effective target ({scrape_target}) — capping to live count")
                    scrape_target = live_count
                scrape_target = min(scrape_target, int(self.config["scraping"]["max_reviews_per_store"]))

            # Nothing left to collect: Google itself shows no more reviews than
            # master already holds.
            #
            # Without this, such a store runs the full escalation ladder before
            # concluding the obvious: 60 empty cycles, a recovery scroll, then
            # the same again for each of three sort orders, roughly ten minutes
            # to rediscover that a store with 12 reviews has 12 reviews.
            #
            # It is common rather than rare. target_reviews is fiction for a
            # large slice of the inventory: 315 stores carry a target of exactly
            # 1,500 because the target itself was capped, and the first store of
            # the first long run was a ShopRite with a target of 1,500 that
            # Google says has 12 reviews, all 12 already in master.
            #
            # The guard is deliberately conservative. It requires a healthy
            # session (limited_view_suspected covers the degraded-render case),
            # a positive live count, and master already holding at least that
            # many. Under those conditions there is nothing on the page we do
            # not already have.
            # Held = master's reviews plus what this batch already collected on earlier
            # passes. Checking master alone missed the second case: in the forrester run
            # 1,566 visits reopened stores whose batch already held every review Google
            # shows, collected nothing, and were requeued again.
            batch_before = self.db.get_review_count(store_id) if self.db else 0
            held_before = master_reviews + batch_before
            if self.db and strategy.nothing_to_collect(held_before, live_count, limited_view_suspected):
                self.logger.info(
                    f"Already complete: we hold {held_before} ({master_reviews} master + {batch_before} batch) "
                    f"and Google shows {live_count}. Target of {raw_target} was stale. Skipping collection."
                )
                self.db.update_store_live_review_count(store_id, live_count)
                self.db.mark_store_completed(store_id, batch_before)
                return "completed"
            if live_count <= 0:
                self.logger.warning("Live review count not read from the page — completion falls back to the target")

            # Every store starts on Newest; the ladder in _collect_reviews moves on to
            # Most relevant and the rating orders when it runs into reviews we hold or stalls.
            attempt = int(store.get("attempts") or 1)
            sort_order = strategy.SORT_LADDER[0]
            self.logger.info(f"Attempt {attempt}: ladder={strategy.SORT_LADDER}, scrape_target={scrape_target}")

            # claim_next_store re-reads the row after setting status='in_progress', so
            # the status can never say whether the previous run was interrupted. It
            # used to be tested here, which made every retry start on the saved last
            # rung ("highest") and stop after a few scrolls. Interrupted stores are
            # reset to pending at startup and just restart from Newest; the dedup
            # guard keeps already-saved reviews from being collected twice.
            was_interrupted = False
            collected, reached_end = await self.scrape_reviews(
                store_id=store_id, target_count=scrape_target,
                sort_order=sort_order, resuming=was_interrupted)

            # A "stalled" store didn't run out of reviews — it ran into a
            # signed-out session or a genuinely stuck page. One clean session
            # swap (fresh browser, next account) gives it a real second look
            # before the retry ladder in scrape_single_store gets involved,
            # rather than spending a whole attempt on a session we already
            # know is broken.
            swaps_remaining = int(self.config["scraping"].get("session_swaps_per_store", 1))
            while self.last_stop_reason == "stalled" and swaps_remaining > 0:
                swaps_remaining -= 1
                old_auth = os.path.basename(self.current_auth_file) if self.current_auth_file else "none"
                await self.close_browser()
                await asyncio.sleep(random.uniform(5.0, 10.0))
                clear_tracking = bool(self.config["scraping"].get("clear_tracking_cookies_on_swap", False))
                await self.start_browser(fresh_cookies=clear_tracking)
                new_auth = os.path.basename(self.current_auth_file) if self.current_auth_file else "none"
                self.logger.warning(
                    f"Stalled — rotated auth {old_auth} → {new_auth}, fresh browser; retrying store")
                if not await self.navigate_to_store(store):
                    break
                if await self.check_and_handle_blocks():
                    return "interrupted"
                collected, reached_end = await self.scrape_reviews(
                    store_id=store_id, target_count=scrape_target,
                    sort_order=sort_order, resuming=True)

            if self.db:
                final_count = self.db.get_review_count(store_id)
                total_reviews = master_reviews + final_count
                gained = final_count - batch_before
                result = strategy.outcome(held=total_reviews, target=target_reviews, live=live_count,
                                          limited_view=limited_view_suspected, gained=gained)
                self.assert_field_coverage(final_count)
                if result == strategy.COMPLETED:
                    self.db.mark_store_completed(store_id, final_count)
                    self.logger.info(
                        f"Store completed [{store_type}]: {final_count} new + {master_reviews} master "
                        f"= {total_reviews}/{target_reviews} (live={live_count}, reached_end={reached_end})"
                    )
                    return "completed"
                elif (gained <= 10 and not self.signed_out and reached_end
                      and self.last_stop_reason == "exhausted" and self.last_rungs_applied >= 3):
                    # Every sort order was walked while signed in and the whole walk
                    # produced 10 or fewer reviews we did not hold. Google's live count includes ratings it
                    # never lists, so held < live is the ceiling, not a failure.
                    # Retrying repeats the same walk: a whole retry round added 1
                    # review in 2 hours.
                    self.db.mark_store_completed(store_id, final_count)
                    cur = self.db.conn.cursor()
                    cur.execute("UPDATE stores SET error_message=? WHERE store_id=?",
                                (f"Ceiling: {self.last_rungs_applied} sort orders exhausted, +{gained} new "
                                 f"({total_reviews} held of live {live_count})", store_id))
                    self.db.conn.commit()
                    self.logger.info(
                        f"Store completed at ceiling [{store_type}]: {total_reviews} held, live={live_count}; "
                        f"{self.last_rungs_applied} sort orders exhausted, +{gained} new")
                    return "completed"
                elif result == strategy.PROGRESSED:
                    # Progress, not failure.
                    #
                    # A big store cannot be drained in one pass. Google exposes a
                    # bounded slice of its reviews per ordering, so a listing with
                    # 2,381 reviews yields on the order of a hundred per visit and
                    # needs many visits. Marking that "failed" spent the retry
                    # budget on stores that were working: in the first long run 64
                    # of 70 failures had each collected between 81 and 149 new
                    # reviews, and were three attempts from being abandoned.
                    #
                    # Retries exist for stores that are broken. A store that moved
                    # forward goes back on the queue with its budget intact.
                    self.db.mark_store_progress(
                        store_id, final_count,
                        f"Partial [{store_type}]: +{gained} this pass, "
                        f"{total_reviews}/{target_reviews} (live={live_count})",
                    )
                    self.logger.info(
                        f"Store progressed [{store_type}]: +{gained} this pass, "
                        f"{total_reviews}/{target_reviews} (live={live_count}). "
                        f"Requeued without spending a retry."
                    )
                    return "progressed"
                else:
                    self.db.mark_store_failed(
                        store_id,
                        f"No progress [{store_type}]: {total_reviews}/{target_reviews} "
                        f"(live={live_count}, reached_end={reached_end}, "
                        f"limited_view={limited_view_suspected}), attempt {attempt}",
                    )
                    self.logger.warning(
                        f"Store made no progress [{store_type}]: {total_reviews}/{target_reviews} "
                        f"(live={live_count}), attempt {attempt}, will retry"
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
