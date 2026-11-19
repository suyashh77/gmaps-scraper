"""Tests for linux_scraper.strategy: the decisions the scraper makes per store.

The forrester log of 16-23 Sep 2026 showed what the old decisions cost: 1,566 of 1,788
store visits collected nothing because a store holding every review Google shows was never
marked complete, and the sort rung 'oldest' does not exist on Google Maps, so incomplete
stores never left Google's default ordering.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from linux_scraper import strategy as st  # noqa: E402


class SortLadder(unittest.TestCase):
    def test_every_store_starts_on_newest_then_default_then_the_rating_orders(self):
        self.assertEqual(st.sort_ladder(), ["newest", "relevant", "lowest", "highest"])

    def test_resume_starts_on_the_saved_rung_and_keeps_the_rest_in_order(self):
        self.assertEqual(st.sort_ladder(resume_from="lowest"), ["lowest", "highest"])

    def test_unknown_saved_rung_falls_back_to_the_full_ladder(self):
        self.assertEqual(st.sort_ladder(resume_from="oldest"), st.SORT_LADDER)

    def test_oldest_is_not_a_rung(self):
        self.assertNotIn("oldest", st.SORT_LADDER)


class SwitchSort(unittest.TestCase):
    def test_a_run_of_reviews_we_already_hold_moves_to_the_next_rung(self):
        self.assertTrue(st.should_switch_sort(known_run=st.KNOWN_RUN_SWITCH, stall_cycles=0))
        self.assertFalse(st.should_switch_sort(known_run=st.KNOWN_RUN_SWITCH - 1, stall_cycles=0))

    def test_a_stall_moves_to_the_next_rung(self):
        self.assertTrue(st.should_switch_sort(known_run=0, stall_cycles=st.STALL_SWITCH))


class Completion(unittest.TestCase):
    def test_holding_everything_google_shows_is_complete_even_below_a_stale_target(self):
        # store 81: 824 held, Google shows 824, target 825 - requeued 20+ times before
        self.assertEqual(st.outcome(held=824, target=825, live=824, limited_view=False, gained=0),
                         st.COMPLETED)

    def test_reaching_the_target_is_complete(self):
        self.assertEqual(st.outcome(held=1500, target=1500, live=4000, limited_view=False, gained=10),
                         st.COMPLETED)

    def test_a_degraded_page_count_is_not_trusted(self):
        self.assertEqual(st.outcome(held=4, target=900, live=4, limited_view=True, gained=4), st.PROGRESSED)

    def test_progress_without_completion_is_requeued(self):
        self.assertEqual(st.outcome(held=300, target=900, live=900, limited_view=False, gained=120),
                         st.PROGRESSED)

    def test_no_progress_is_a_failure(self):
        self.assertEqual(st.outcome(held=300, target=900, live=900, limited_view=False, gained=0), st.FAILED)


class GoodEnough(unittest.TestCase):
    def test_big_store_within_25_of_target_is_complete(self):
        self.assertEqual(st.done_at(300), 275)
        self.assertEqual(st.outcome(held=275, target=300, live=0, limited_view=False, gained=5), st.COMPLETED)
        self.assertEqual(st.outcome(held=274, target=300, live=0, limited_view=False, gained=5), st.PROGRESSED)

    def test_store_of_100_or_fewer_within_5_of_target_is_complete(self):
        self.assertEqual(st.done_at(100), 95)
        self.assertEqual(st.done_at(80), 75)
        self.assertEqual(st.outcome(held=75, target=80, live=0, limited_view=False, gained=1), st.COMPLETED)

    def test_tiny_store_still_needs_one_review(self):
        self.assertEqual(st.done_at(3), 1)
        self.assertEqual(st.done_at(0), 0)

    def test_tolerance_also_applies_to_googles_live_count(self):
        self.assertTrue(st.nothing_to_collect(held=800, live=824, limited_view=False))
        self.assertFalse(st.nothing_to_collect(held=798, live=824, limited_view=False))


class NothingToCollect(unittest.TestCase):
    def test_held_at_or_above_live_count_needs_no_scrolling(self):
        self.assertTrue(st.nothing_to_collect(held=824, live=824, limited_view=False))
        self.assertFalse(st.nothing_to_collect(held=798, live=824, limited_view=False))

    def test_unknown_or_degraded_live_count_is_never_taken_as_done(self):
        self.assertFalse(st.nothing_to_collect(held=10, live=0, limited_view=False))
        self.assertFalse(st.nothing_to_collect(held=10, live=4, limited_view=True))


if __name__ == "__main__":
    unittest.main()
