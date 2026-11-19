"""Tests for linux_scraper.cli.discover_auth_files.

The scraper used to look for google_auth_*.json only in the directory above
the package, which is why three copies of the secrets accumulated (repo Auth/,
Scraper_v2/, and every kit). Discovery now checks, in order: an explicit path,
the package parent, its auth/ subfolder (kit layout), and the repo Auth/
folder one level up. Banned sessions (*.json.banned) are never picked up.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from linux_scraper.cli import discover_auth_files  # noqa: E402


def _touch(path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8"):
        pass
    return os.path.abspath(path)


class TestDiscoverAuthFiles(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = self.tmp.name
        self.script_dir = os.path.join(self.repo, "Scraper_v2")
        os.makedirs(self.script_dir)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_returns_empty_tuple_when_nothing_exists(self) -> None:
        self.assertEqual(discover_auth_files(self.script_dir), ())

    def test_finds_files_next_to_package(self) -> None:
        a = _touch(os.path.join(self.script_dir, "google_auth_2.json"))
        b = _touch(os.path.join(self.script_dir, "google_auth_1.json"))
        self.assertEqual(discover_auth_files(self.script_dir), (b, a))

    def test_finds_kit_auth_subfolder(self) -> None:
        a = _touch(os.path.join(self.script_dir, "auth", "google_auth_1.json"))
        self.assertEqual(discover_auth_files(self.script_dir), (a,))

    def test_finds_repo_auth_folder_one_level_up(self) -> None:
        a = _touch(os.path.join(self.repo, "Auth", "google_auth_1.json"))
        self.assertEqual(discover_auth_files(self.script_dir), (a,))

    def test_explicit_path_comes_first_and_is_not_duplicated(self) -> None:
        a = _touch(os.path.join(self.script_dir, "google_auth_1.json"))
        b = _touch(os.path.join(self.repo, "Auth", "google_auth_9.json"))
        got = discover_auth_files(self.script_dir, explicit=b)
        self.assertEqual(got, (b, a))

    def test_missing_explicit_path_is_ignored(self) -> None:
        a = _touch(os.path.join(self.script_dir, "google_auth_1.json"))
        got = discover_auth_files(self.script_dir, explicit=os.path.join(self.repo, "nope.json"))
        self.assertEqual(got, (a,))

    def test_banned_sessions_are_skipped(self) -> None:
        _touch(os.path.join(self.script_dir, "google_auth_1.json.banned"))
        self.assertEqual(discover_auth_files(self.script_dir), ())

    def test_same_file_reachable_twice_is_listed_once(self) -> None:
        # On a case-insensitive filesystem auth/ and Auth/ can be one folder;
        # whatever the platform, a path must never appear twice.
        a = _touch(os.path.join(self.script_dir, "auth", "google_auth_1.json"))
        got = discover_auth_files(self.script_dir)
        self.assertEqual(len(got), len(set(os.path.normcase(p) for p in got)))
        self.assertIn(a, got)


if __name__ == "__main__":
    unittest.main(verbosity=2)
