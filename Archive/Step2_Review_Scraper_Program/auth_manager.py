"""
Google account rotation manager for burner accounts.

Maintains a pool of saved Google auth sessions (google_auth_*.json files).
When an account gets banned or triggers a CAPTCHA, it is removed from rotation
and the next account is used automatically.

Thread-safe — safe to share across multiprocessing workers via a Manager proxy.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional


logger = logging.getLogger(__name__)


class AuthAccountManager:
    """
    Round-robin rotation across a pool of Google auth session files.

    Each auth file is a Playwright storage_state JSON saved during login.
    When an account is banned or flagged, call mark_account_banned() to remove it
    from rotation. The scraper will automatically switch to the next account.

    Usage:
        manager = AuthAccountManager(["google_auth_1.json", "google_auth_2.json"])
        auth_file = manager.get_next_account()   # returns path or None
        manager.mark_account_banned(auth_file)   # removes from pool
    """

    def __init__(self, auth_files: list[str]):
        # Filter to only files that actually exist
        self._pool: list[str] = [f for f in auth_files if os.path.exists(f)]
        self._banned: list[str] = []
        self._index: int = 0
        self._lock = threading.Lock()

        missing = [f for f in auth_files if not os.path.exists(f)]
        if missing:
            logger.warning(f"Auth files not found (skipped): {missing}")
        logger.info(f"AuthAccountManager: {len(self._pool)} account(s) in pool")

    @classmethod
    def from_config(cls, config: dict) -> AuthAccountManager:
        """Build from config dict. Reads auth.accounts list or falls back to google_auth_file."""
        auth_cfg = config.get("auth", {})
        accounts = auth_cfg.get("accounts", [])

        # Fallback: single account from browser.google_auth_file
        if not accounts:
            single = config.get("browser", {}).get("google_auth_file", "")
            if single:
                accounts = [single]

        return cls(accounts)

    # ── public API ────────────────────────────────────────────────────────────

    def get_next_account(self) -> Optional[str]:
        """Return the next available auth file path, or None if pool is empty."""
        with self._lock:
            if not self._pool:
                return None
            # Round-robin: wrap index around pool size
            self._index = self._index % len(self._pool)
            path = self._pool[self._index]
            self._index += 1
            return path

    def mark_account_banned(self, auth_file: str) -> None:
        """
        Remove an account from the rotation pool.
        Renames the auth file to *.banned so it won't be auto-loaded.
        """
        with self._lock:
            if auth_file in self._pool:
                self._pool.remove(auth_file)
                self._banned.append(auth_file)
                logger.warning(
                    f"Account banned and removed from rotation: {auth_file} "
                    f"({len(self._pool)} account(s) remaining)"
                )
                # Rename the file so it's not accidentally reloaded
                banned_path = auth_file + ".banned"
                try:
                    os.rename(auth_file, banned_path)
                    logger.info(f"Renamed to: {banned_path}")
                except OSError as e:
                    logger.warning(f"Could not rename auth file: {e}")
            else:
                logger.debug(f"mark_account_banned: {auth_file} not in pool (already removed?)")

    def has_accounts(self) -> bool:
        """True if at least one account remains in the pool."""
        with self._lock:
            return len(self._pool) > 0

    def account_count(self) -> int:
        """Number of active accounts remaining."""
        with self._lock:
            return len(self._pool)

    def banned_count(self) -> int:
        """Number of accounts that have been banned this session."""
        with self._lock:
            return len(self._banned)

    def status_summary(self) -> str:
        with self._lock:
            return (
                f"Auth pool: {len(self._pool)} active, {len(self._banned)} banned"
            )

    # ── account file helpers ──────────────────────────────────────────────────

    @staticmethod
    def list_auth_files(directory: str = ".") -> list[str]:
        """Find all google_auth_*.json files in a directory."""
        files = []
        for fname in sorted(os.listdir(directory)):
            if fname.startswith("google_auth") and fname.endswith(".json"):
                files.append(os.path.join(directory, fname))
        return files
