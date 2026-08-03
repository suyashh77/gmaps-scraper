"""Unit tests for small utility helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gmaps_reviews_scraper.utils import (  # noqa: E402
    build_maps_search_url,
    parse_rating_from_aria,
    safe_float,
    safe_int,
    slugify,
)


class UtilsTests(unittest.TestCase):
    def test_build_maps_search_url(self) -> None:
        url = build_maps_search_url("Food Lion Tryon Road Raleigh")
        self.assertEqual(url, "https://www.google.com/maps/search/Food+Lion+Tryon+Road+Raleigh")

    def test_build_maps_search_url_passthrough_for_direct_url(self) -> None:
        source = "https://www.google.com/maps/place/Food+Lion/@35.7660345,-78.7351835,14z"
        self.assertEqual(build_maps_search_url(source), source)

    def test_safe_int_parses_commas(self) -> None:
        self.assertEqual(safe_int("1,234 reviews"), 1234)
        self.assertEqual(safe_int("No reviews"), 0)

    def test_parse_rating_from_aria(self) -> None:
        self.assertEqual(parse_rating_from_aria("5 stars"), 5)
        self.assertEqual(parse_rating_from_aria("Rated 4 out of 5"), 4)
        self.assertIsNone(parse_rating_from_aria(""))

    def test_safe_float(self) -> None:
        self.assertEqual(safe_float("4.3"), 4.3)
        self.assertEqual(safe_float("4,7"), 4.7)
        self.assertIsNone(safe_float("not-a-number"))

    def test_slugify(self) -> None:
        self.assertEqual(slugify("Food Lion Tryon Road Raleigh"), "food_lion_tryon_road_raleigh")
        self.assertEqual(slugify("  "), "query")


if __name__ == "__main__":
    unittest.main()
