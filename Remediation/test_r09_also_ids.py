"""--also-ids queues a store that is NOT short of its target, to be collected in full with no skip-list.

Used to rescrape a store that holds more than Google shows: a skip-list scrape returns only the
reviews master lacks, so it cannot say which of the held reviews are still on the page.
"""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import r07_build_known_keys as r07            # noqa: E402
import r09_build_scrape_queue as r09          # noqa: E402

STORE_COLS = ("store_id INTEGER PRIMARY KEY, place_id, feature_id, store_name, street, city, state, zip, "
              "source_lat, source_lon, full_address, target_reviews, reviews_scraped, category, "
              "business_status, qa_flags, status_reason")


class AlsoIds(unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp()
        self.master = os.path.join(d, "m.db")
        self.out = os.path.join(d, "batch.db")
        c = sqlite3.connect(self.master)
        c.execute(f"CREATE TABLE stores ({STORE_COLS})")
        c.execute("CREATE TABLE reviews (id INTEGER PRIMARY KEY, store_id, reviewer_name, rating, review_text)")
        rows = [(1, "ChIJshort", None, "Short", "1 A St", "Cary", "NC", "27511", 35.0, -78.0, None, 100, 60, "Grocery", None, None, None),
                (2, "ChIJover", None, "Over", "2 B St", "Cary", "NC", "27511", 35.1, -78.1, None, 100, 150, "Grocery", None, None, None),
                (3, "ChIJfine", None, "Fine", "3 C St", "Cary", "NC", "27511", 35.2, -78.2, None, 100, 100, "Grocery", None, None, None)]
        c.executemany(f"INSERT INTO stores VALUES ({','.join('?' * 17)})", rows)
        c.executemany("INSERT INTO reviews (store_id, reviewer_name, rating, review_text) VALUES (?,?,?,?)",
                      [(1, "ann", 5, "short one"), (2, "bob", 4, "over one"), (2, "cat", 3, "over two")])
        c.commit()
        c.close()

    def _build(self, *extra):
        argv = ["r09", "--master", self.master, "--out", self.out, "--order", "pct", *extra]
        with mock.patch.object(sys, "argv", argv):
            self.assertEqual(0, r09.main())
        return sqlite3.connect(self.out)

    def test_without_the_flag_only_short_stores_are_queued(self):
        b = self._build()
        self.assertEqual([1], [r[0] for r in b.execute("SELECT master_store_id FROM stores")])

    def test_a_store_on_the_old_adjudication_worklist_is_held_back_unless_told_otherwise(self):
        with mock.patch.object(r09, "held_back_ids", lambda *a, **k: {1}):
            held = self._build()
            self.assertEqual([], [r[0] for r in held.execute("SELECT master_store_id FROM stores")])
            held.close()
            queued = self._build("--ignore-held-back")
            self.assertEqual([1], [r[0] for r in queued.execute("SELECT master_store_id FROM stores")])

    def test_the_flag_adds_a_store_that_is_not_short_to_be_collected_in_full(self):
        b = self._build("--also-ids", "2")
        rows = {r[0]: r[1:] for r in b.execute("SELECT master_store_id, target_reviews, master_reviews FROM stores")}
        self.assertEqual({1, 2}, set(rows))
        self.assertEqual((100, 0), rows[2])          # target in full, nothing "already held"

    def test_a_store_queued_this_way_gets_no_skip_list_but_the_others_do(self):
        self._build("--also-ids", "2")
        with mock.patch.object(r07, "backup", lambda *a, **k: "backup"), \
                mock.patch.object(sys, "argv", ["r07", "--batch", self.out, "--master", self.master, "--apply"]):
            self.assertEqual(0, r07.main())
        b = sqlite3.connect(self.out)
        by_master = dict(b.execute("SELECT master_store_id, store_id FROM stores"))
        keyed = {r[0] for r in b.execute("SELECT DISTINCT store_id FROM known_review_keys")}
        self.assertIn(by_master[1], keyed)
        self.assertNotIn(by_master[2], keyed)


if __name__ == "__main__":
    unittest.main()
