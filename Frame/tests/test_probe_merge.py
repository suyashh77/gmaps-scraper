"""Tests for probe_merge.merge_into.

Folding a volunteer's probe results into the master queue must: take rows the
incoming file probed and the master has not; prefer the newer probed_at when
both have one; never blank a master row from an unprobed incoming row; and be
idempotent, so running it twice changes nothing the second time.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import probe  # noqa: E402
import probe_merge  # noqa: E402

ROWS = [
    # store_id, status, g_category, probed_at, machine_id
    (1, "pending", None, None, None),                         # master unprobed, incoming probed
    (2, "done", "Grocery store", "2026-09-10 10:00:00", "M"),  # master newer than incoming
    (3, "done", "Old label", "2026-09-01 10:00:00", "M"),      # incoming newer than master
    (4, "done", "Keep me", "2026-09-05 10:00:00", "M"),        # incoming never probed it
]


def _make(path: Path, rows) -> None:
    c = sqlite3.connect(path)
    c.executescript(probe.SCHEMA)
    c.executemany(
        "INSERT INTO probe (store_id, name, status, g_category, probed_at, machine_id) "
        "VALUES (?, 'n', ?, ?, ?, ?)", rows)
    c.commit()
    c.close()


class TestMergeInto(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        d = Path(self.tmp.name)
        self.master = d / "master.db"
        self.incoming = d / "incoming.db"
        _make(self.master, ROWS)
        _make(self.incoming, [
            (1, "done", "Convenience store", "2026-09-15 09:00:00", "H"),
            (2, "done", "Stale label", "2026-09-09 10:00:00", "H"),
            (3, "done", "New label", "2026-09-15 09:00:00", "H"),
            (4, "pending", None, None, None),
        ])

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _row(self, sid: int):
        c = sqlite3.connect(self.master)
        r = c.execute("SELECT status, g_category, machine_id FROM probe WHERE store_id=?",
                      (sid,)).fetchone()
        c.close()
        return r

    def test_takes_rows_master_never_probed(self) -> None:
        stats = probe_merge.merge_into(self.master, self.incoming)
        self.assertEqual(self._row(1), ("done", "Convenience store", "H"))
        self.assertEqual(stats["rows_updated"], 2)

    def test_newer_master_row_is_kept(self) -> None:
        probe_merge.merge_into(self.master, self.incoming)
        self.assertEqual(self._row(2), ("done", "Grocery store", "M"))

    def test_newer_incoming_row_wins(self) -> None:
        probe_merge.merge_into(self.master, self.incoming)
        self.assertEqual(self._row(3), ("done", "New label", "H"))

    def test_unprobed_incoming_row_never_blanks_master(self) -> None:
        probe_merge.merge_into(self.master, self.incoming)
        self.assertEqual(self._row(4), ("done", "Keep me", "M"))

    def test_idempotent(self) -> None:
        probe_merge.merge_into(self.master, self.incoming)
        again = probe_merge.merge_into(self.master, self.incoming)
        self.assertEqual(again["rows_updated"], 0)
        self.assertEqual(again["probed_before"], again["probed_after"])

    def test_incoming_file_is_not_modified(self) -> None:
        before = self.incoming.read_bytes()
        probe_merge.merge_into(self.master, self.incoming)
        self.assertEqual(self.incoming.read_bytes(), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
