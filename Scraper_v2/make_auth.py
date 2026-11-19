"""Create or check a Google session file for the scraper.

An auth file is a Playwright storage_state JSON. It must be made on a machine with a
visible browser — Google blocks scripted logins from a headless server — so this runs
on the Windows PC and the resulting files are copied to the volunteer machines.

    python Scraper_v2/make_auth.py Auth/google_auth_1.json     # log in, then press Enter
    python Scraper_v2/make_auth.py --check Auth/*.json         # verify existing files

Nothing is written unless the session is really signed in, and --check asks Google
rather than reading the file: on 2026-09-27 all three Auth/ files carried valid-looking
cookies with 2027 expiry dates that Google had already revoked server-side, which is
what left the scraper running logged out for two days.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# The cookies Google sets for a signed-in account. All three, on a google.com domain.
REQUIRED_COOKIES = {"SID", "HSID", "SSID"}
LOGIN_URL = "https://accounts.google.com/"
VERIFY_URL = "https://www.google.com/maps"
# Signed out, this redirects to accounts.google.com. Cookie presence cannot tell you
# this: Google revokes server-side and leaves the cookies sitting on disk.
LIVE_CHECK_URL = "https://myaccount.google.com/"
# Chrome profiles for the login windows. Data/ is gitignored, so nothing leaks into git.
PROFILE_DIR = Path(__file__).resolve().parent.parent / "Data" / "auth_profiles"
# Chrome 146+ binds a Google login to this PC's TPM (Device Bound Session Credentials).
# The bound session lives on *PSIDRTS cookies that expire in ~10 minutes and can only be
# renewed with a key that never leaves this PC, so a file made that way goes signed-out
# about 10 minutes after it reaches a volunteer box — exactly what happened 2026-09-27.
# Login windows therefore run with session binding switched off.
CHROME_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-features=EnableBoundSessionCredentials,DeviceBoundSessions",
]
DEVICE_BOUND_SUFFIX = "PSIDRTS"


def is_device_bound(state: dict) -> bool:
    """True when the session is tied to the PC it was made on and will die elsewhere."""
    return any(str(c.get("name", "")).endswith(DEVICE_BOUND_SUFFIX)
               for c in state.get("cookies", []))


def _refuse_device_bound(path: str, state: dict) -> bool:
    if not is_device_bound(state):
        return False
    print(f"  NOT saved: {path} — the session is bound to this PC (DBSC) and would sign out"
          " on the volunteer box. Delete its folder under Data/auth_profiles/ and log in again.")
    return True


def is_signed_in(state: dict) -> bool:
    """True when a storage_state carries Google's session cookies.

    Necessary, not sufficient — a revoked session keeps its cookies. Use live_check()
    to find out whether Google still honours them.
    """
    names = {c.get("name") for c in state.get("cookies", [])
             if str(c.get("domain", "")).endswith("google.com")}
    return REQUIRED_COOKIES <= names


def live_check(path: str) -> bool:
    """Load the session into a browser and see whether Google still accepts it."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(storage_state=path)
            page = context.new_page()
            page.goto(LIVE_CHECK_URL, wait_until="domcontentloaded", timeout=30000)
            return "accounts.google.com" not in page.url
        finally:
            browser.close()


def check(paths: list[str], offline: bool = False) -> int:
    bad = 0
    for p in paths:
        try:
            state = json.loads(Path(p).read_text(encoding="utf-8"))
            ok = is_signed_in(state)
        except (OSError, ValueError) as exc:
            print(f"  BROKEN      {p}  ({exc})")
            bad += 1
            continue
        if is_device_bound(state):
            # Live from this PC, dead ~10 min after it reaches another machine.
            print(f"  PC-BOUND    {p}  (DBSC session: works only on the PC that made it)")
            bad += 1
            continue
        if ok and not offline:
            try:
                live = live_check(p)
            except Exception as exc:
                print(f"  UNKNOWN     {p}  (live check failed: {exc})")
                bad += 1
                continue
            if not live:
                print(f"  REVOKED     {p}  (cookies present, Google says signed out)")
                bad += 1
                continue
            print(f"  live        {p}")
            continue
        print(f"  {'cookies ok ' if ok else 'NO COOKIES '}{p}")
        bad += not ok
    return 1 if bad else 0


def create(path: str) -> int:
    """Open real Chrome on a persistent profile and save the session once logged in.

    Google refuses sign-in from Playwright's bundled Chromium ("this browser or app may
    not be secure"). It keys on the automation flags and the Chromium build, so this
    uses the installed Chrome channel, drops --enable-automation and hides the
    AutomationControlled blink feature (navigator.webdriver then reads False). The
    profile persists per account, so a repeat run resumes the login instead of redoing it.
    """
    from playwright.sync_api import sync_playwright

    profile = PROFILE_DIR / Path(path).stem
    profile.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(profile),
            channel="chrome",
            headless=False,
            args=CHROME_ARGS,
            ignore_default_args=["--enable-automation"],
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(LOGIN_URL)
        print(f"\nChrome is open. Sign in to the account for {path}.")
        print("When Google Maps shows you signed in, come back here and press Enter.")
        input("  [Enter] when done, Ctrl-C to abandon: ")
        page.goto(VERIFY_URL)
        page.wait_for_timeout(3000)
        state = context.storage_state()
        context.close()

    if not is_signed_in(state):
        print(f"\nNOT saved: {path} — no signed-in Google session in that window.")
        return 1
    if _refuse_device_bound(path, state):
        return 1
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(state), encoding="utf-8")
    print(f"\nwrote {out}  ({len(state.get('cookies', []))} cookies)")
    return 0


def dump_profile(path: str) -> int:
    """Write the auth file from an existing login profile, without logging in again.

    The login lives in the Chrome profile, so a save that failed after a successful
    sign-in is recoverable: reopen the profile headless and take its storage_state.
    """
    from playwright.sync_api import sync_playwright

    profile = PROFILE_DIR / Path(path).stem
    if not profile.exists():
        print(f"  no profile at {profile} — run without --from-profile to log in")
        return 1
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(profile), channel="chrome", headless=True,
            args=CHROME_ARGS,
            ignore_default_args=["--enable-automation"],
        )
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(VERIFY_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)
            state = context.storage_state()
        finally:
            context.close()

    if not is_signed_in(state):
        print(f"  NOT saved: {path} — profile has no signed-in session")
        return 1
    if _refuse_device_bound(path, state):
        return 1
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(state), encoding="utf-8")
    print(f"  wrote {out}  ({len(state.get('cookies', []))} cookies)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+", help="auth file to write, or files to --check")
    ap.add_argument("--check", action="store_true", help="verify existing files, write nothing")
    ap.add_argument("--offline", action="store_true",
                    help="with --check: only look at cookies, do not ask Google")
    ap.add_argument("--from-profile", action="store_true",
                    help="write the auth file(s) from an existing login profile, no login")
    a = ap.parse_args()
    if a.check:
        return check(a.paths, offline=a.offline)
    if a.from_profile:
        return max(dump_profile(p) for p in a.paths)
    if len(a.paths) != 1:
        ap.error("give one output path when creating (use --check for many)")
    return create(a.paths[0])


if __name__ == "__main__":
    sys.exit(main())
