"""R16: turn the Round 4 manual answers into per-store actions, then apply them to master."""
import os
import sqlite3
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import r16_round4_answers as R          # noqa: E402

URL = "https://www.google.com/maps/place/X/@35.5,-80.1,17z/data=!4m6!3m5!1s{fid}!8m2"
FID_NEW = "0x89aa000000000001:0x00000000000000aa"
FID_KEPT = "0x89aa000000000002:0x00000000000000bb"


def _frames():
    return {
        "place_id_check": pd.DataFrame([
            dict(master_store_id=1, outcome="OPEN_OK", google_review_count=300,
                 correct_google_url=URL.format(fid=FID_NEW), notes=None),
            dict(master_store_id=2, outcome="OPEN_OK", google_review_count=50,
                 correct_google_url="| NOTE: junk", notes=None),           # OPEN_OK but no usable URL
            dict(master_store_id=3, outcome="NOT_ON_GOOGLE", google_review_count=None,
                 correct_google_url=None, notes="no page"),
            dict(master_store_id=4, outcome="UNSURE", google_review_count=None,
                 correct_google_url=None, notes=None)]),
        "target_check": pd.DataFrame([
            dict(master_store_id=5, outcome="OPEN_OK", google_review_count=410,
                 correct_google_url="| NOTE:", notes=None),
            dict(master_store_id=3, outcome="OPEN_OK", google_review_count=9,
                 correct_google_url=None, notes=None)]),                    # also on place_id_check as OUT
        "location_check": pd.DataFrame([
            dict(master_store_id=6, outcome="IN_NC", google_review_count=4, real_address="1 Main St, Ernul, NC",
                 correct_google_url=URL.format(fid=FID_NEW), notes=None)]),
        "category_check": pd.DataFrame([
            dict(master_store_id=7, outcome="OPEN_OK", google_category="Grocery store",
                 google_review_count=None, notes=None)]),
        "no_target_check": pd.DataFrame([
            dict(master_store_id=8, outcome="OPEN_OK", google_review_count=0,
                 correct_google_url=None, notes=None)]),               # open, Google shows 0 reviews
    }


class Plan(unittest.TestCase):
    def test_each_answer_becomes_one_action_and_a_terminal_one_wins(self):
        acts = {(a["store_id"], a["action"]) for a in R.plan_actions(_frames())}
        self.assertEqual({(1, "REPOINT"), (3, "OUT"), (5, "TARGET"), (6, "LOCATION"), (7, "CATEGORY"),
                          (8, "NO_DATA")}, acts)

    def test_unusable_answers_are_listed_not_guessed(self):
        skipped = {a["store_id"] for a in R.plan_actions(_frames(), include_skipped=True)
                   if a["action"] == "SKIP"}
        self.assertEqual({2, 4}, skipped)

    def test_url_helpers(self):
        self.assertEqual((35.5, -80.1), R.latlon_from_url(URL.format(fid=FID_NEW)))
        self.assertEqual("", R.place_url("| NOTE: x"))
        self.assertEqual(300, R.count_from(300.0))
        self.assertIsNone(R.count_from(float("nan")))


class Apply(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        self.con.execute("CREATE TABLE stores (store_id INTEGER PRIMARY KEY, store_name, place_id, feature_id, "
                         "target_reviews, reviews_scraped, scrape_status, status_reason, category, "
                         "source_lat, source_lon, full_address, business_status)")
        self.con.execute("CREATE TABLE reviews (id INTEGER PRIMARY KEY, store_id, reviewer_name, rating, "
                         "review_text)")
        rows = [(1, "A", "ChIGbad", None, 1500, 40, "incomplete", None, "Grocery", 35, -80, None, None),
                (3, "C", "ChIGbad2", None, 1500, 5, "incomplete", None, "Grocery", 35, -80, None, None),
                (5, "E", "ChIJe", None, 500, 400, "incomplete", None, "Grocery", 35, -80, None, None),
                (6, "F", "ChIJf", None, 20, 20, "complete", None, "Grocery", 99.0, -190.0, None, None),
                (7, "G", "ChIJg", None, 10, 10, "complete", None, None, 35, -80, None, None),
                (8, "H", "ChIJh", None, 0, 0, "complete", "no target; attempted 13 times; ruled complete",
                 "Grocery", 35, -80, None, None)]
        self.con.executemany("INSERT INTO stores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)

    def test_apply_repoints_sets_targets_and_leaves_the_rest(self):
        R.apply_actions(self.con, R.plan_actions(_frames()), "2026-10-09")
        row = lambda i: dict(self.con.execute("SELECT * FROM stores WHERE store_id=?", (i,)).fetchone())
        self.assertEqual(FID_NEW, row(1)["place_id"])
        self.assertEqual(300, row(1)["target_reviews"])
        self.assertEqual("out_of_scope", row(3)["scrape_status"])
        self.assertEqual(410, row(5)["target_reviews"])
        self.assertEqual((35.5, -80.1), (row(6)["source_lat"], row(6)["source_lon"]))
        self.assertEqual("Grocery store", row(7)["category"])
        self.assertTrue(row(8)["status_reason"].startswith("no data"))   # Google shows 0 reviews

    def test_a_page_master_already_holds_becomes_a_duplicate_of_that_store(self):
        self.con.execute("INSERT INTO stores VALUES (9,'Keeper','ChIJkeep',?,500,500,'complete',NULL,"
                         "'Grocery',35,-80,NULL,NULL)", (FID_KEPT,))
        self.assertEqual(9, R.holder_of(self.con, 1, FID_KEPT))
        self.assertEqual(9, R.holder_of(self.con, 1, "0x89aaffffffffffff:" + FID_KEPT.split(":")[1]))
        self.assertIsNone(R.holder_of(self.con, 1, FID_NEW))


if __name__ == "__main__":
    unittest.main()
