"""--cap 0 --more-than-cap queues every store Google shows more reviews for than we hold past the 1,500 cap."""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import r09_build_scrape_queue as r09          # noqa: E402

STORE_COLS = ("store_id INTEGER PRIMARY KEY, place_id, feature_id, store_name, street, city, state, zip, "
              "source_lat, source_lon, full_address, target_reviews, reviews_scraped, category, "
              "business_status, qa_flags, status_reason")


class MoreThanCap(unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp()
        self.master, self.probe, self.out = (os.path.join(d, n) for n in ("m.db", "probe.db", "batch.db"))
        c = sqlite3.connect(self.master)
        c.execute(f"CREATE TABLE stores ({STORE_COLS})")
        c.execute("CREATE TABLE reviews (id INTEGER PRIMARY KEY, store_id, reviewer_name, rating, review_text)")
        row = lambda i, pid, t, h, why=None: (i, pid, None, f"S{i}", "1 A St", "Cary", "NC", "27511", 35.0, -78.0,
                                              None, t, h, "Grocery", None, None, why)
        c.executemany(f"INSERT INTO stores VALUES ({','.join('?' * 17)})", [
            row(1, "ChIJa", 1500, 1500),                                   # Google shows 3,000
            row(2, "ChIJb", 1500, 1500),                                   # Google shows exactly 1,500: nothing beyond
            row(3, "ChIJc", 100, 90),                                      # ordinary short store
            row(4, "ChIJd", 1500, 1500, "stalled 0 short of 1500; ruled")])   # ruled complete, Google shows 4,000
        c.commit()
        c.close()
        p = sqlite3.connect(self.probe)
        p.execute("CREATE TABLE probe (status, g_place_id, g_feature_id, g_total_reviews)")
        p.executemany("INSERT INTO probe VALUES ('done', ?, NULL, ?)",
                      [("ChIJa", 3000), ("ChIJb", 1500), ("ChIJc", 100), ("ChIJd", 4000)])
        p.commit()
        p.close()

    def _build(self, *extra):
        argv = ["r09", "--master", self.master, "--probe", self.probe, "--out", self.out, "--order", "pct",
                "--ignore-held-back", *extra]
        with mock.patch.object(sys, "argv", argv):
            self.assertEqual(0, r09.main())
        return sqlite3.connect(self.out)

    def test_without_the_flag_only_the_short_store_is_queued_and_the_cap_stays_1500(self):
        b = self._build()
        self.assertEqual([3], [r[0] for r in b.execute("SELECT master_store_id FROM stores")])
        self.assertEqual("1500", b.execute("SELECT value FROM metadata WHERE key='review_cap'").fetchone()[0])

    def test_the_flag_queues_stores_with_reviews_beyond_the_cap_at_googles_full_count(self):
        b = self._build("--cap", "0", "--more-than-cap")
        rows = {r[0]: r[1:] for r in b.execute("SELECT master_store_id, target_reviews, master_reviews FROM stores")}
        self.assertEqual({1, 3, 4}, set(rows))                         # store 2 has nothing beyond the cap
        self.assertEqual((3000, 1500), rows[1])                        # full count as target, held kept (skip-list applies)
        self.assertEqual((4000, 1500), rows[4])                        # an earlier "complete by ruling" does not block it
        self.assertEqual((100, 90), rows[3])
        self.assertEqual("0", b.execute("SELECT value FROM metadata WHERE key='review_cap'").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
