"""R18: compare what a full unfiltered rescrape returned with what a store holds. Moves nothing."""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import r18_compare_rescrape as R          # noqa: E402


def keys(n, prefix="p"):
    return {(f"{prefix}{i}", 5, f"text {i}") for i in range(n)}


class Verdict(unittest.TestCase):
    def test_a_page_whose_reviews_we_already_hold_is_consistent_and_the_extras_are_kept(self):
        held = keys(120) | {("old", 4, "deleted since")}        # holds 121, the page shows 100 of them
        out = R.compare_store(held, keys(100), live=100)
        self.assertEqual("CONSISTENT", out["verdict"])
        self.assertEqual(21, out["held_not_in_fresh"])          # reported, not touched
        self.assertEqual(0, out["fresh_not_held"])

    def test_a_page_that_mostly_disagrees_with_what_we_hold_is_a_mismatch(self):
        held = keys(100, "a")
        fresh = keys(100, "b") | set(list(held)[:10])           # only 10 of the page's 110 are held
        self.assertEqual("MISMATCH", R.compare_store(held, fresh, live=100)["verdict"])

    def test_in_between_is_partial_and_goes_to_manual(self):
        held = keys(100)
        fresh = set(list(held)[:70]) | keys(30, "new")          # 70 % of the page is held
        self.assertEqual("PARTIAL", R.compare_store(held, fresh, live=100)["verdict"])

    def test_a_small_or_missing_scrape_cannot_decide(self):
        self.assertEqual("SCRAPE_TOO_SMALL", R.compare_store(keys(100), keys(50), live=100)["verdict"])
        self.assertEqual("NO_FULL_SCRAPE", R.compare_store(keys(100), None, live=100)["verdict"])

    def test_only_a_consistent_store_needs_no_manual_check(self):
        self.assertFalse(R.needs_manual("CONSISTENT"))
        for v in ("MISMATCH", "PARTIAL", "SCRAPE_TOO_SMALL", "NO_FULL_SCRAPE"):
            self.assertTrue(R.needs_manual(v))


class LatestFullScrape(unittest.TestCase):
    def test_only_a_completed_scrape_with_no_skip_list_counts(self):
        d = tempfile.mkdtemp()
        os.makedirs(os.path.join(d, "20261101_x"))
        b = sqlite3.connect(os.path.join(d, "20261101_x", "b.db"))
        b.execute("CREATE TABLE stores (store_id INTEGER PRIMARY KEY, master_store_id, status, updated_at)")
        b.execute("CREATE TABLE reviews (store_id, reviewer_name, rating, review_text)")
        b.execute("CREATE TABLE known_review_keys (store_id, dedup_key)")
        b.executemany("INSERT INTO stores VALUES (?,?,?,?)", [(1, 10, "completed", "t"), (2, 20, "completed", "t"),
                                                              (3, 30, "failed", "t")])
        b.executemany("INSERT INTO known_review_keys VALUES (?,?)", [(2, "k1")])       # store 2 had a skip-list
        b.executemany("INSERT INTO reviews VALUES (1,?,?,?)", [("a", 5, "x"), ("b", 4, "")])
        b.commit()
        b.close()
        self.assertEqual({("a", 5, "x"), ("b", 4, "")}, R.latest_full_scrape(d, 10))
        self.assertIsNone(R.latest_full_scrape(d, 20))         # skip-listed: cannot say what is on the page
        self.assertIsNone(R.latest_full_scrape(d, 30))         # failed
        self.assertIsNone(R.latest_full_scrape(d, 99))         # never scraped


if __name__ == "__main__":
    unittest.main()
