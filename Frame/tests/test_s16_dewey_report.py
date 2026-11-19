"""Tests for the pure helpers in s16_dewey_report.

The report's one claim per row is "do we have reviews for this Dewey POI"; the
status label and the per-type summary are what a reader trusts, so they are tested
on a tiny synthetic sheet.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import s16_dewey_report as s16  # noqa: E402


def _sheet() -> pd.DataFrame:
    """Five Dewey rows covering every status; two share one master store (collision)."""
    return pd.DataFrame({
        "dewey_placekey": ["p1", "p2", "p3", "p4", "p5"],
        "dewey_food_access_type": ["supermarket", "supermarket", "dollar", "dollar", "produce"],
        "frame_store_id": [1, 2, 3, 4, None],
        "google_probed_at": ["2026-09-10", "2026-09-10", "2026-09-10", None, None],
        "google_total_reviews": [900, 900, 50, None, None],
        "master_store_id": [10, 10, 11, None, None],
        "master_reviews_in_db": [300, 300, 0, None, None],
        "master_reviews_with_text": [150, 150, 0, None, None],
    })


class ReviewStatus(unittest.TestCase):
    def test_every_branch_gets_a_label(self):
        out = s16.review_status(_sheet())
        self.assertEqual(out.tolist(), [
            s16.STATUS_HAVE_REVIEWS, s16.STATUS_HAVE_REVIEWS, s16.STATUS_IN_MASTER_NONE,
            s16.STATUS_FRAME_NOT_PROBED, s16.STATUS_NO_MATCH])

    def test_have_reviews_flag_is_yes_only_when_count_positive(self):
        out = s16.have_reviews(_sheet())
        self.assertEqual(out.tolist(), ["yes", "yes", "no", "no", "no"])


class BrandName(unittest.TestCase):
    def test_extracts_name_from_safegraph_json(self):
        raw = pd.Series(['[{"safegraph_brand_id":"SG_1","safegraph_brand_name":"Food Lion"}]', "[]", None,
                         '[{"safegraph_brand_id":"SG_2","safegraph_brand_name":"Circle K"},{"safegraph_brand_id":"SG_3","safegraph_brand_name":"Circle K Gas"}]'])
        self.assertEqual(s16.brand_name(raw).tolist(), ["Food Lion", "(no brand)", "(no brand)", "Circle K; Circle K Gas"])


def _candidates_sheet() -> pd.DataFrame:
    """One row per selection branch the scrape list has to get right."""
    return pd.DataFrame({
        "dewey_placekey": ["ok-new", "ok-partial", "done", "closed", "unprobed", "no-id", "failed"],
        "dewey_food_access_type": ["supermarket"] * 7,
        "dewey_location_name": ["A", "B", "C", "D", "E", "F", "G"],
        "frame_store_id": [1, 2, 3, 4, 5, 6, 7],
        "google_probed_at": ["2026-09-10"] * 6 + [None],
        "google_probe_status": ["done", "done", "done", "done", None, "done", "failed"],
        "google_closed": [0, 0, 0, 1, None, 0, 0],
        "google_place_id": ["ChIJ_a", "ChIJ_b", "ChIJ_c", "ChIJ_d", None, None, "ChIJ_g"],
        "google_feature_id": [None, None, None, None, None, None, None],
        "google_total_reviews": [400, 900, 50, 10, 10, 10, 10],
        "master_store_id": [None, 20, 30, None, None, None, None],
        "master_reviews_in_db": [None, 100, 50, None, None, None, None],
        "master_target_reviews": [None, 900, 50, None, None, None, None],
    })


class ScrapeCandidates(unittest.TestCase):
    def test_keeps_only_probed_open_identifiable_and_unfinished(self):
        out = s16.scrape_candidates(_candidates_sheet())
        self.assertEqual(out.dewey_placekey.tolist(), ["ok-new", "ok-partial"])

    def test_never_scraped_rows_are_flagged_and_sorted_first(self):
        out = s16.scrape_candidates(_candidates_sheet())
        self.assertEqual(out.never_scraped.tolist(), [1, 0])

    def test_carries_the_identity_the_importer_needs(self):
        out = s16.scrape_candidates(_candidates_sheet())
        for col in ("google_place_id", "google_feature_id", "master_store_id", "dewey_placekey"):
            self.assertIn(col, out.columns)

    def test_feature_id_only_store_is_still_scrapeable(self):
        sheet = _candidates_sheet()
        sheet.loc[sheet.dewey_placekey == "no-id", "google_feature_id"] = "0x88541:0xabc"
        out = s16.scrape_candidates(sheet)
        self.assertIn("no-id", out.dewey_placekey.tolist())


class SummaryByType(unittest.TestCase):
    def test_counts_rows_but_reviews_once_per_master_store(self):
        s = s16.summary_by(_sheet(), "dewey_food_access_type")
        sup = s.set_index("dewey_food_access_type").loc["supermarket"]
        self.assertEqual(int(sup["dewey_pois"]), 2)
        self.assertEqual(int(sup["have_reviews"]), 2)
        self.assertEqual(int(sup["distinct_master_stores"]), 1)
        self.assertEqual(int(sup["reviews_in_master"]), 300)  # not 600
        tot = s.set_index("dewey_food_access_type").loc["TOTAL"]
        self.assertEqual(int(tot["dewey_pois"]), 5)
        self.assertEqual(int(tot["matched_to_frame"]), 4)
        self.assertEqual(int(tot["probed"]), 3)


if __name__ == "__main__":
    unittest.main()
