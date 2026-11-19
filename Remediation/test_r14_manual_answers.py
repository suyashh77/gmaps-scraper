"""Tests for r14_manual_answers: turning the completed manual-check workbooks into rulings.

Pure mapping only; the database steps are tested in test_r12_rulings / test_r13_rulings.
"""

from __future__ import annotations

import os
import sys
import unittest

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import r14_manual_answers as R                                # noqa: E402


def sheet(**cols) -> pd.DataFrame:
    return pd.DataFrame(cols)


class TestRulingsFromSheets(unittest.TestCase):
    def test_closed_open_and_missing_answers_become_the_right_decisions(self):
        df = sheet(master_store_id=[1, 2, 3, 4, 5],
                   outcome=["CLOSED_TEMPORARY", "OPEN_OK", "OPEN_OK", "NOT_ON_GOOGLE", "UNSURE"],
                   google_review_count=[float("nan"), 572.0, float("nan"), float("nan"), float("nan")],
                   evidence=["red banner", "hours shown", "hours shown", "", ""],
                   notes=["", "", "", "no listing", ""])
        rows, skipped = R.rulings_from_sheet(df, "closed_flag_check", "2026-10-08")
        by = {int(r["store_id"]): r for r in rows}
        self.assertEqual("CLOSED_TEMPORARY", by[1]["decision"])
        self.assertEqual(("OPEN", "572"), (by[2]["decision"], by[2]["target"]))
        self.assertEqual(("OPEN", ""), (by[3]["decision"], by[3]["target"]),
                         "open with no count shown: flag cleared, target untouched")
        self.assertEqual("OUT", by[4]["decision"])
        self.assertEqual([5], skipped, "UNSURE is reported, never guessed")

    def test_open_store_with_zero_reviews_is_complete_with_no_data(self):
        df = sheet(master_store_id=[7], outcome=["OPEN_OK"], google_review_count=[0.0],
                   google_rating=[float("nan")], notes=[""])
        rows, _ = R.rulings_from_sheet(df, "no_target_check", "2026-10-08")
        self.assertEqual("NO_DATA", rows[0]["decision"])

    def test_blank_rows_are_ignored(self):
        df = sheet(master_store_id=[8], outcome=[float("nan")], google_review_count=[float("nan")], notes=[""])
        rows, skipped = R.rulings_from_sheet(df, "failed_stores_check", "2026-10-08")
        self.assertEqual(([], [8]), (rows, skipped))


class TestPageAnswers(unittest.TestCase):
    def test_partners_are_read_from_the_shares_page_with_text(self):
        df = sheet(master_store_id=[2661], shares_page_with=["store 4570 (103 reviews), store 2659 (80 reviews)"],
                   stored_link_opens_own_store=["NO"], real_store_exists=["YES"], real_store_url=["u"],
                   real_store_address=["a"], real_store_review_count=[208.0])
        rows = R.page_answers_from_sheets([df])
        self.assertEqual([4570, 2659], rows[0]["partners"])
        self.assertEqual(("NO", "YES", "208"), (rows[0]["opens_own"], rows[0]["real_exists"], rows[0]["real_count"]))

    def test_a_store_answered_on_two_sheets_is_applied_once(self):
        a = sheet(master_store_id=[4451], shares_page_with=["store 4726 (249 reviews)"],
                  stored_link_opens_own_store=["NO"], real_store_exists=["YES"], real_store_url=["u1"],
                  real_store_address=["a"], real_store_review_count=[1079.0])
        b = a.copy()
        rows = R.page_answers_from_sheets([a, b])
        self.assertEqual(1, len(rows))

    def test_unanswered_rows_are_not_applied(self):
        df = sheet(master_store_id=[9], shares_page_with=["store 10 (5 reviews)"],
                   stored_link_opens_own_store=[float("nan")], real_store_exists=[float("nan")],
                   real_store_url=[float("nan")], real_store_address=[float("nan")],
                   real_store_review_count=[float("nan")])
        self.assertEqual([], R.page_answers_from_sheets([df]))


if __name__ == "__main__":
    unittest.main()
