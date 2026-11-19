"""Tests for r10_repair_targets_from_live: the status recompute must respect rulings.

On 2026-10-05 r10 ran after the dewey6/7 merge and its blanket status recompute
demoted 975 ruled-complete stores to incomplete and erased every 'duplicate' and
'bad_place_id' status. A store with a status_reason carries a decision, not a count.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import r10_repair_targets_from_live as R10                # noqa: E402


def _master(with_reason: bool = True) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row                         # as lib_master.connect_rw sets it
    reason = ", status_reason TEXT" if with_reason else ""
    con.execute("CREATE TABLE stores (store_id INTEGER PRIMARY KEY, target_reviews INTEGER, "
                "reviews_scraped INTEGER, scrape_status TEXT" + reason + ")")
    return con


def _status(con, sid: int) -> str:
    return con.execute("SELECT scrape_status FROM stores WHERE store_id=?", (sid,)).fetchone()[0]


class TestRecomputeStatuses(unittest.TestCase):
    def test_ruled_stores_keep_their_status(self):
        con = _master()
        con.executemany("INSERT INTO stores VALUES (?,?,?,?,?)", [
            (1, 1500, 1498, "complete", "stalled 2 short of 1500; ruled complete"),
            (2, 300, 300, "duplicate", "duplicate of 7"),
            (3, 100, 0, "bad_place_id", "id opens another store"),
        ])
        R10.recompute_statuses(con)
        self.assertEqual(_status(con, 1), "complete")
        self.assertEqual(_status(con, 2), "duplicate")
        self.assertEqual(_status(con, 3), "bad_place_id")

    def test_unruled_stores_follow_their_counts(self):
        con = _master()
        con.executemany("INSERT INTO stores VALUES (?,?,?,?,NULL)", [
            (1, 1500, 200, "complete"),       # target raised by r10: no longer complete
            (2, 100, 100, "incomplete"),
            (3, 100, 0, "incomplete"),
            (4, None, 50, "complete"),
        ])
        R10.recompute_statuses(con)
        self.assertEqual(_status(con, 1), "incomplete")
        self.assertEqual(_status(con, 2), "complete")
        self.assertEqual(_status(con, 3), "not_started")
        self.assertEqual(_status(con, 4), "unknown_target")

    def test_master_without_status_reason_still_recomputes(self):
        con = _master(with_reason=False)
        con.execute("INSERT INTO stores VALUES (1, 100, 100, 'incomplete')")
        R10.recompute_statuses(con)
        self.assertEqual(_status(con, 1), "complete")


if __name__ == "__main__":
    unittest.main()
