"""Round 4 manual workbook: right sheets, right rows, input columns left blank."""
import os
import sqlite3
import sys
import tempfile
import unittest

import openpyxl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import make_manual_round4 as M          # noqa: E402

COLS = ("store_id,store_name,full_address,city,state,category,place_id,source_lat,source_lon,"
        "target_reviews,reviews_scraped,scrape_status,status_reason")


def _store(i, name, pid="ChIJgood", cat="Grocery store", lat=35.5, lon=-80.0, target=100, n=100):
    return (i, name, "1 Main St", "Raleigh", "NC", cat, pid, lat, lon, target, n, "complete", None)


class Round4(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "m.db")
        c = sqlite3.connect(self.db)
        c.execute(f"CREATE TABLE stores ({COLS})")
        c.execute("CREATE TABLE reviews (store_id, reviewer_name, rating, review_text)")
        rows = [_store(1, "Fine"), _store(2, "BadId", pid="ChIGxyz"), _store(3, "Far", lat=42.0),
                _store(4, "NoCat", cat=None), _store(5, "NoTarget", target=None, n=7),
                _store(6, "Short", target=500, n=460)[:11] + ("incomplete", None)]
        c.executemany(f"INSERT INTO stores VALUES ({','.join('?' * 13)})", rows)
        c.commit()
        c.close()
        self.out = os.path.join(self.tmp, "r4.xlsx")

    def test_each_issue_lands_on_its_own_sheet_and_clean_stores_on_none(self):
        counts = M.build(self.db, self.out)
        self.assertEqual(counts, {"place_id_check": 1, "location_check": 1, "category_check": 1,
                                  "no_target_check": 1, "target_check": 1})
        wb = openpyxl.load_workbook(self.out)
        self.assertEqual(wb.sheetnames[0], "READ FIRST")
        ids = {n: [r[0] for r in wb[n].iter_rows(min_row=2, values_only=True) if r[0]]
               for n in counts}
        self.assertEqual(ids, {"place_id_check": [2], "location_check": [3], "category_check": [4],
                               "no_target_check": [5], "target_check": [6]})

    def test_target_check_lists_short_stores_with_a_good_place_id_and_how_short(self):
        M.build(self.db, self.out)
        ws = openpyxl.load_workbook(self.out)["target_check"]
        head = [c.value for c in ws[1]]
        row = dict(zip(head, [c.value for c in ws[2]]))
        self.assertEqual(40, row["short_by"])
        self.assertIsNone(row["google_review_count"])

    def test_address_falls_back_to_street_city_zip_when_full_address_is_blank(self):
        c = sqlite3.connect(self.db)
        c.execute("ALTER TABLE stores ADD COLUMN street")
        c.execute("ALTER TABLE stores ADD COLUMN zip")
        c.execute("UPDATE stores SET full_address=NULL, street='1826 E Arlington Blvd', zip='27858' WHERE store_id=2")
        c.commit()
        c.close()
        M.build(self.db, self.out)
        ws = openpyxl.load_workbook(self.out)["place_id_check"]
        row = dict(zip([c.value for c in ws[1]], [c.value for c in ws[2]]))
        self.assertEqual("1826 E Arlington Blvd, Raleigh, NC 27858", row["address"])

    def test_both_addresses_show_when_google_derived_address_is_in_a_different_city(self):
        c = sqlite3.connect(self.db)
        c.execute("ALTER TABLE stores ADD COLUMN street")
        c.execute("ALTER TABLE stores ADD COLUMN zip")
        c.execute("UPDATE stores SET full_address='832 E Chatham St, Cary, NC 27511', city='Newland', "
                  "street='117 Winters Ln', zip='28657' WHERE store_id=2")
        c.commit()
        c.close()
        M.build(self.db, self.out)
        ws = openpyxl.load_workbook(self.out)["place_id_check"]
        row = dict(zip([c.value for c in ws[1]], [c.value for c in ws[2]]))
        self.assertEqual("117 Winters Ln, Newland, NC 28657 | Google page says: 832 E Chatham St, Cary, NC 27511",
                         row["address"])

    def test_answer_columns_start_blank(self):
        M.build(self.db, self.out)
        ws = openpyxl.load_workbook(self.out)["place_id_check"]
        head = [c.value for c in ws[1]]
        row = dict(zip(head, [c.value for c in ws[2]]))
        self.assertIsNone(row["outcome"])
        self.assertIn("search", row["search_link"])


if __name__ == "__main__":
    unittest.main()
