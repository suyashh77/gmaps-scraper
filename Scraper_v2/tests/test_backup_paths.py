"""Backups land in a backups/ folder beside the database, never beside it loose.

Ten backups of a 460 MB master accumulated next to the live file before this;
both writers (Remediation/lib_master.backup and merge_results) now share one
destination rule.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "Remediation"))

import lib_master  # noqa: E402
from linux_scraper import merge_results  # noqa: E402


class TestBackupPaths(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "master.db")
        c = sqlite3.connect(self.db)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("CREATE TABLE t (x)")
        c.execute("INSERT INTO t VALUES (1)")
        c.commit()
        c.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_lib_master_backup_goes_to_backups_dir(self) -> None:
        dest = lib_master.backup(self.db, "r99")
        self.assertEqual(os.path.basename(os.path.dirname(dest)), "backups")
        self.assertTrue(os.path.basename(dest).startswith("master_backup_r99_"))
        self.assertTrue(os.path.exists(dest))

    def test_merge_results_backup_goes_to_backups_dir(self) -> None:
        dest = merge_results.backup_master(self.db)
        self.assertEqual(os.path.basename(os.path.dirname(dest)), "backups")
        self.assertTrue(os.path.basename(dest).startswith("master_backup_"))
        c = sqlite3.connect(dest)
        try:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM t").fetchone()[0], 1)
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
