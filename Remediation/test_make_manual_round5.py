"""Round 5 manual workbook: the no-target stores, plus any store named for a count check."""
import os
import sqlite3
import sys
import tempfile
import unittest

import openpyxl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import make_manual_round5 as M          # noqa: E402

COLS = ("store_id,store_name,full_address,city,state,category,place_id,source_lat,source_lon,"
        "target_reviews,reviews_scraped,scrape_status,status_reason")


class Round5(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "m.db")
        c = sqlite3.connect(self.db)
        c.execute(f"CREATE TABLE stores ({COLS})")
        c.execute("CREATE TABLE reviews (store_id, reviewer_name, rating, review_text)")
        row = lambda i, t, n: (i, f"S{i}", "1 Main St", "Raleigh", "NC", "Grocery store", "ChIJx", 35.5, -80.0, t, n,
                               "complete", None)
        c.executemany(f"INSERT INTO stores VALUES ({','.join('?' * 13)})",
                      [row(1, 100, 100), row(2, None, 0), row(3, 0, 12), row(4, 574, 1022)])
        c.commit()
        c.close()
        self.out = os.path.join(self.tmp, "r5.xlsx")

    def test_no_target_stores_and_named_stores_get_their_own_sheets(self):
        counts = M.build(self.db, self.out, count_check_ids=[4])
        self.assertEqual({"no_target_check": 2, "target_check": 1}, counts)
        wb = openpyxl.load_workbook(self.out)
        self.assertEqual(["READ FIRST", "no_target_check", "target_check"], wb.sheetnames)
        ids = [r[0] for r in wb["no_target_check"].iter_rows(min_row=2, values_only=True) if r[0]]
        self.assertEqual([2, 3], ids)

    def test_no_named_stores_means_no_target_check_sheet(self):
        self.assertEqual({"no_target_check": 2}, M.build(self.db, self.out))


if __name__ == "__main__":
    unittest.main()
