"""The queue's order must match the order the scraper actually claims in.

linux_scraper.database.claim_next_store hands out never-scraped stores before
partly-scraped ones (Scraper_v2/tests/test_claim_order.py). r09 used to order the
queue by size alone, so --limit kept the biggest resumed stores and dropped fresh
ones, and a --split gave one machine all the resumes. queue_order fixes both.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import r09_build_scrape_queue as r09  # noqa: E402


def _row(name: str, store_id: int) -> sqlite3.Row:
    """A real sqlite3.Row, because that is what main() passes — a dict fixture hid a
    .get() call that works on dicts and raises on Row."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    return con.execute("SELECT ? AS store_id, ? AS store_name", (store_id, name)).fetchone()


def _entry(name: str, need: int, have: int, store_id: int = 0):
    """(need, row, target, have) as main() builds it; only name/need/have matter here."""
    return (need, _row(name, store_id), need + have, have)


class QueueOrder(unittest.TestCase):
    def test_never_scraped_stores_come_before_resumed_ones(self) -> None:
        out = r09.queue_order([
            _entry("resumed-huge", 1300, 200),
            _entry("fresh-small", 40, 0),
            _entry("resumed-small", 10, 90),
            _entry("fresh-big", 900, 0),
        ])
        self.assertEqual([e[1]["store_name"] for e in out],
                         ["fresh-big", "fresh-small", "resumed-huge", "resumed-small"])

    def test_largest_need_first_within_each_group(self) -> None:
        out = r09.queue_order([_entry("a", 10, 0), _entry("b", 900, 0), _entry("c", 50, 0)])
        self.assertEqual([e[0] for e in out], [900, 50, 10])

    def test_limit_keeps_fresh_work_rather_than_the_biggest_resumes(self) -> None:
        out = r09.queue_order([_entry("resumed-huge", 5000, 100), _entry("fresh", 5, 0)])[:1]
        self.assertEqual(out[0][1]["store_name"], "fresh")

    def test_ties_break_on_store_id_like_the_scraper_does(self) -> None:
        out = r09.queue_order([_entry("c", 200, 0, store_id=9), _entry("a", 200, 0, store_id=2),
                               _entry("b", 200, 0, store_id=5)])
        self.assertEqual([e[1]["store_id"] for e in out], [2, 5, 9])

    def test_pct_order_puts_the_store_furthest_short_of_its_own_target_first(self) -> None:
        # (name, need, have): target = need + have
        out = r09.queue_order([_entry("big-gap-small-share", 300, 1200),   # 20 % missing
                               _entry("small-gap-big-share", 40, 10),      # 80 % missing
                               _entry("never-scraped", 100, 0)],           # 100 % missing
                              by_pct=True)
        self.assertEqual([e[1]["store_name"] for e in out],
                         ["never-scraped", "small-gap-big-share", "big-gap-small-share"])

    def test_empty_queue(self) -> None:
        self.assertEqual(r09.queue_order([]), [])

    def test_by_need_puts_the_biggest_shortfall_first_fresh_or_not(self) -> None:
        out = r09.queue_order([_entry("fresh-small", 40, 0), _entry("resumed-huge", 1300, 200),
                               _entry("fresh-big", 900, 0)], by_need=True)
        self.assertEqual([e[1]["store_name"] for e in out],
                         ["resumed-huge", "fresh-big", "fresh-small"])


if __name__ == "__main__":
    unittest.main()


class TestHeldBack(unittest.TestCase):
    def test_a_worklist_row_with_a_ruling_is_no_longer_held(self):
        import tempfile
        path = os.path.join(tempfile.mkdtemp(), "worklist.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            fh.write("store_id,flags,ruling\n1,OVER_SCRAPED,\n2,OVER_SCRAPED,resolved by r12\n")
        self.assertEqual({1}, r09.held_back_ids(path))


class TestByName(unittest.TestCase):
    def test_by_name_store_is_queued_without_ids_despite_its_ruling(self):
        """A store whose place_id opens another store's page is searched by name."""
        import subprocess
        import tempfile
        from test_remediation import build_master
        tmp = tempfile.mkdtemp()
        master, out, ids = (os.path.join(tmp, n) for n in ("m.db", "q.db", "ids.csv"))
        build_master(master)
        con = sqlite3.connect(master)
        for col in ("feature_id", "status_reason", "qa_flags"):
            con.execute(f"ALTER TABLE stores ADD COLUMN {col} TEXT")
        con.execute("UPDATE stores SET scrape_status='bad_place_id', status_reason='x', "
                    "place_id='ChIdBROKEN' WHERE store_id=1")
        con.commit()
        con.close()
        with open(ids, "w", encoding="utf-8") as fh:
            fh.write("store_id\n1\n")
        subprocess.run([sys.executable, os.path.join(HERE, "r09_build_scrape_queue.py"),
                        "--master", master, "--out", out, "--by-name", ids],
                       check=True, capture_output=True)
        row = sqlite3.connect(out).execute(
            "SELECT google_place_id, google_feature_id FROM stores WHERE master_store_id=1"
        ).fetchone()
        self.assertEqual((None, None), row)
