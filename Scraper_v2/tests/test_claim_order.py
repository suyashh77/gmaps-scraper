"""claim_next_store hands out never-scraped stores before partly-scraped ones.

New stores (nothing in master yet) come first regardless of size; within each
group the largest remaining need goes first so the run does not end with one
enormous store still going while everything else is idle.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from linux_scraper.database import make_db  # noqa: E402


class TestClaimOrder(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = make_db(os.path.join(self.tmp.name, "b.db"))
        self.db.connect()
        self.db.init_schema()
        self.db.conn.executemany(
            "INSERT INTO stores (store_id, input_name, status, target_reviews, master_reviews) "
            "VALUES (?,?,?,?,?)", [
                (1, "old-big", "pending", 1500, 200),   # need 1300, already partly scraped
                (2, "new-small", "pending", 40, 0),     # need 40, never scraped
                (3, "new-big", "pending", 900, 0),      # need 900, never scraped
                (4, "old-small", "pending", 100, 90),   # need 10
            ])
        self.db.conn.commit()

    def tearDown(self) -> None:
        self.db.close()
        self.tmp.cleanup()

    def test_new_stores_first_then_largest_need(self) -> None:
        order = [self.db.claim_next_store(worker_id=1)["store_id"] for _ in range(4)]
        self.assertEqual(order, [3, 2, 1, 4])

    def test_batch_marked_claim_order_need_goes_biggest_first(self) -> None:
        """Ruling 2026-10-05: re-queue big shortfalls first, fresh or not."""
        self.db.set_metadata("claim_order", "need")
        order = [self.db.claim_next_store(worker_id=1)["store_id"] for _ in range(4)]
        self.assertEqual(order, [1, 3, 2, 4])

    def test_batch_marked_claim_order_pct_goes_by_share_of_target_missing(self) -> None:
        """Ruling 2026-10-11: rescrape the stores furthest short of their own target first."""
        self.db.set_metadata("claim_order", "pct")
        order = [self.db.claim_next_store(worker_id=1)["store_id"] for _ in range(4)]
        # 3 and 2 are 100% missing (the bigger need breaks the tie), 1 is 87%, 4 is 10%
        self.assertEqual(order, [3, 2, 1, 4])

    def test_batch_without_a_metadata_table_still_claims(self) -> None:
        self.db.conn.execute("DROP TABLE metadata")
        self.assertEqual(self.db.claim_next_store(worker_id=1)["store_id"], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
