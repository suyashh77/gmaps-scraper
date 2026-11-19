"""The 1,500-review cap is a per-batch setting: absent means 1,500 (every existing batch), 0 means none."""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from linux_scraper.review_cap import DEFAULT_CAP, UNCAPPED, review_cap, store_timeout_minutes  # noqa: E402


class ReviewCap(unittest.TestCase):
    def test_a_batch_with_no_setting_keeps_the_cap_of_1500(self):
        self.assertEqual(DEFAULT_CAP, 1500)
        self.assertEqual(1500, review_cap(None))
        self.assertEqual(1500, review_cap(""))

    def test_zero_or_none_means_no_cap(self):
        self.assertEqual(UNCAPPED, review_cap("0"))
        self.assertEqual(UNCAPPED, review_cap("none"))
        self.assertEqual(UNCAPPED, review_cap("None"))

    def test_a_number_is_the_cap(self):
        self.assertEqual(3000, review_cap("3000"))

    def test_junk_falls_back_to_the_default_rather_than_scraping_without_limit(self):
        self.assertEqual(1500, review_cap("lots"))
        self.assertEqual(1500, review_cap("-5"))


class Timeout(unittest.TestCase):
    def test_capped_batches_keep_exactly_the_old_budget_min_180_60_plus_2_per_review(self):
        self.assertEqual(80, store_timeout_minutes(10, 1500))
        self.assertEqual(180, store_timeout_minutes(100, 1500))
        self.assertEqual(180, store_timeout_minutes(1500, 1500))

    def test_uncapped_batches_use_the_same_formula_with_an_8_hour_ceiling(self):
        self.assertEqual(80, store_timeout_minutes(10, UNCAPPED))
        self.assertEqual(260, store_timeout_minutes(100, UNCAPPED))
        self.assertEqual(480, store_timeout_minutes(3000, UNCAPPED))


if __name__ == "__main__":
    unittest.main()
