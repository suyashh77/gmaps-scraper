"""Tests for probe.release_stranded.

A store claimed by a run that was killed stays 'in_progress' forever, because
claim() only selects 'pending'. On startup a probe releases its own stranded
claims - and only its own, so two machines sharing one queue file cannot
release each other's live work.
"""
from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import probe  # noqa: E402


class TestReleaseStranded(unittest.TestCase):
    def setUp(self) -> None:
        self.c = sqlite3.connect(":memory:")
        self.c.row_factory = sqlite3.Row
        self.c.executescript(probe.SCHEMA)
        self.c.executemany(
            "INSERT INTO probe (store_id, name, status, machine_id) VALUES (?, ?, ?, ?)",
            [
                (1, "A-stranded", "in_progress", "A"),
                (2, "B-live", "in_progress", "B"),
                (3, "done", "done", "A"),
                (4, "pending", "pending", None),
            ],
        )
        self.c.commit()

    def tearDown(self) -> None:
        self.c.close()

    def _status(self, store_id: int) -> str:
        return self.c.execute(
            "SELECT status FROM probe WHERE store_id=?", (store_id,)).fetchone()[0]

    def test_releases_only_own_in_progress_rows(self) -> None:
        released = probe.release_stranded(self.c, "A")
        self.assertEqual(released, 1)
        self.assertEqual(self._status(1), "pending")
        self.assertEqual(self._status(2), "in_progress")
        self.assertEqual(self._status(3), "done")
        self.assertEqual(self._status(4), "pending")

    def test_second_call_releases_nothing(self) -> None:
        probe.release_stranded(self.c, "A")
        self.assertEqual(probe.release_stranded(self.c, "A"), 0)

    def test_unknown_machine_releases_nothing(self) -> None:
        self.assertEqual(probe.release_stranded(self.c, "Z"), 0)
        self.assertEqual(self._status(1), "in_progress")


if __name__ == "__main__":
    unittest.main(verbosity=2)
