"""A saved auth file is only worth shipping if it actually carries a signed-in session.

The 2026-09-25 Dewey run failed because the three Auth/ files were signed out: Google
served the logged-out page, sorting was dead and stores returned nothing. The check is
on cookie names rather than page text so it cannot rot with Google's UI.
"""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import make_auth  # noqa: E402


def _state(*names: str, domain: str = ".google.com") -> dict:
    return {"cookies": [{"name": n, "domain": domain, "value": "x"} for n in names]}


class DeviceBound(unittest.TestCase):
    """Chrome 146+ on Windows binds Google sessions to the PC's TPM (DBSC). The bound
    session carries short-lived *PSIDRTS cookies that only this PC can renew, so the file
    goes signed-out about 10 minutes after it lands on a volunteer box (2026-09-27)."""

    def test_rts_cookie_marks_a_session_device_bound(self) -> None:
        self.assertTrue(make_auth.is_device_bound(
            _state("SID", "HSID", "SSID", "__Secure-1PSIDRTS")))

    def test_plain_session_is_portable(self) -> None:
        self.assertFalse(make_auth.is_device_bound(
            _state("SID", "HSID", "SSID", "__Secure-1PSID", "__Secure-3PSIDTS")))

    def test_login_browser_turns_session_binding_off(self) -> None:
        flag = [a for a in make_auth.CHROME_ARGS if a.startswith("--disable-features=")]
        self.assertTrue(flag and "EnableBoundSessionCredentials" in flag[0])


class IsSignedIn(unittest.TestCase):
    def test_google_session_cookies_mean_signed_in(self) -> None:
        self.assertTrue(make_auth.is_signed_in(_state("SID", "HSID", "SSID")))

    def test_missing_any_required_cookie_is_signed_out(self) -> None:
        self.assertFalse(make_auth.is_signed_in(_state("SID", "HSID")))

    def test_cookies_on_another_domain_do_not_count(self) -> None:
        self.assertFalse(make_auth.is_signed_in(_state("SID", "HSID", "SSID", domain=".example.com")))

    def test_empty_state_is_signed_out(self) -> None:
        self.assertFalse(make_auth.is_signed_in({"cookies": []}))
        self.assertFalse(make_auth.is_signed_in({}))


if __name__ == "__main__":
    unittest.main()
