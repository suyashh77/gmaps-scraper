"""R15: incomplete stores within the owner's tolerance are ruled complete; the rest are untouched."""
import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import r15_tolerance as R          # noqa: E402


def _con():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE stores (store_id, place_id, target_reviews, reviews_scraped, "
                "scrape_status, status_reason, business_status)")
    rows = [(1, "ChIJa", 1500, 1455, "incomplete", None, None),     # 45 missing, target over 500: complete
            (2, "ChIJb", 1500, 1440, "incomplete", None, None),     # 60 missing: rescrape
            (3, "ChIJc", 500, 482, "incomplete", None, None),       # 18 missing, target over 100: complete
            (4, "ChIGbad", 500, 495, "incomplete", None, None),     # malformed id: left for manual
            (5, "ChIJd", 500, 495, "incomplete", "ruled earlier", None),   # already has a ruling
            (6, "ChIJe", 500, 0, "not_started", None, None)]        # nothing collected: not a near-miss
    con.executemany("INSERT INTO stores VALUES (?,?,?,?,?,?,?)", rows)
    return con


class Tolerance(unittest.TestCase):
    def test_only_near_misses_with_a_good_place_id_are_ruled_complete(self):
        con = _con()
        self.assertEqual([1, 3], R.apply_tolerance(con, "2026-10-09"))
        status = dict(con.execute("SELECT store_id, scrape_status FROM stores").fetchall())
        self.assertEqual({1: "complete", 2: "incomplete", 3: "complete", 4: "incomplete",
                          5: "incomplete", 6: "not_started"}, status)

    def test_reason_records_the_rule_and_is_idempotent(self):
        con = _con()
        R.apply_tolerance(con, "2026-10-09")
        why = con.execute("SELECT status_reason FROM stores WHERE store_id=1").fetchone()[0]
        self.assertIn("2026-10-09", why)
        self.assertEqual([], R.apply_tolerance(con, "2026-10-09"))


class Reopen(unittest.TestCase):
    def _stores(self):
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        con.execute("CREATE TABLE stores (store_id, place_id, target_reviews, reviews_scraped, "
                    "scrape_status, status_reason, business_status)")
        con.executemany("INSERT INTO stores VALUES (?,?,?,?,?,?,?)", [
            (1, "ChIJa", 500, 475, "complete", "stalled 25 short of 500; 3 full passes", None),
            (2, "ChIJb", 1500, 1495, "complete", "stalled 5 short of 1500; 2 full passes", None),
            (3, "ChIJc", 0, 0, "complete", "no data: Google shows 0 reviews; ruled complete", None),
            (4, "ChIJd", 13, 4, "complete", "could not be verified on Google; holds 4 of 13", None),
            (5, "ChIJe", 300, 300, "complete", "stalled 0 short of 300", None),
            (6, "ChIJf", 200, 150, "complete", "no target; attempted 2 times; ruled complete", None),
            (7, "ChIJg", 80, 0, "complete", "within tolerance: 0 of 80", None)])
        return con

    def test_short_stores_ruled_complete_by_count_are_reopened_for_rescrape(self):
        con = self._stores()
        self.assertEqual([1, 6, 7], R.reopen_short(con))
        rows = {r["store_id"]: (r["scrape_status"], r["status_reason"])
                for r in con.execute("SELECT * FROM stores")}
        self.assertEqual(("incomplete", None), rows[1])
        self.assertEqual(("not_started", None), rows[7])                 # nothing held yet
        self.assertTrue(rows[2][1].startswith("stalled"))                # 5 missing on a 1,500 target: stays
        self.assertTrue(rows[3][1].startswith("no data"))                # nothing to collect
        self.assertTrue(rows[4][1].startswith("could not be verified"))  # unverified target
        self.assertEqual("complete", rows[5][0])                         # at target

    def test_the_old_reason_is_kept_for_audit_and_a_second_run_changes_nothing(self):
        con = self._stores()
        R.reopen_short(con)
        old = con.execute("SELECT old_reason FROM reopened_rulings WHERE store_id=1").fetchone()[0]
        self.assertTrue(old.startswith("stalled 25 short"))
        self.assertEqual([], R.reopen_short(con))


if __name__ == "__main__":
    unittest.main()
