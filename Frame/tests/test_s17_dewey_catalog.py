"""Tests for the pure helpers in s17_dewey_catalog.

The catalog makes two claims a reader acts on: "this Dewey POI is (or is not) one of our
stores, and here is how far its reviews got", and "this category holds these POIs". The
matcher, the grade, the status label and the category counts are what those claims rest
on, so each is tested on a tiny synthetic set.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import s17_dewey_catalog as s17  # noqa: E402

# One degree of latitude is ~111 km, so 0.001 deg ~ 111 m.
LAT, LON = 35.7800, -78.6400


def _dewey() -> pd.DataFrame:
    return pd.DataFrame({
        "PLACEKEY": ["pk-fl", "pk-dent", "pk-fl-old", "pk-far", "pk-dg"],
        "LOCATION_NAME": ["Food Lion", "Bright Smile Dental", "Food Lion", "Food Lion", "Dollar General"],
        "STREET_ADDRESS": ["100 Main St", "100 Main St", "100 Main St", "900 Oak Ave", "55 Elm St"],
        "LATITUDE": [LAT, LAT + 0.0001, LAT + 0.0002, LAT + 0.0100, LAT + 0.0500],
        "LONGITUDE": [LON, LON, LON, LON, LON],
        "CLOSED_ON": [None, None, "2021-03-01", None, None],
    })


def _master() -> pd.DataFrame:
    return pd.DataFrame({
        "store_id": [1, 2, 3],
        "store_name": ["Food Lion #1234", "Corner Market", "Dollar General"],
        "business_name": [None, None, None],
        "outscraper_name": [None, None, None],
        "street": ["100 Main St", "7 Pine Rd", "55 Elm Street"],
        "full_address": [None, None, None],
        "source_lat": [LAT + 0.0003, LAT + 0.2, LAT + 0.0500 + 0.0016],
        "source_lon": [LON, LON, LON],
    })


class Grade(unittest.TestCase):
    def test_name_and_house_is_a(self):
        self.assertEqual(s17.grade(0.95, True, 200.0), "A")

    def test_name_near_is_b_and_name_far_is_c(self):
        self.assertEqual(s17.grade(0.90, False, 100.0), "B")
        self.assertEqual(s17.grade(0.90, False, 220.0), "C")

    def test_house_with_partial_name_is_d(self):
        self.assertEqual(s17.grade(0.60, True, 50.0), "D")

    def test_house_alone_or_too_far_is_no_link(self):
        self.assertIsNone(s17.grade(0.20, True, 10.0))       # dentist in the same building
        self.assertIsNone(s17.grade(0.60, True, 200.0))      # weak name beyond the house radius
        self.assertIsNone(s17.grade(0.95, True, 400.0))      # beyond every radius


class MatchMasterToDewey(unittest.TestCase):
    def setUp(self):
        self.links = s17.match_master_to_dewey(_master(), _dewey())

    def test_links_the_same_store_and_prefers_the_open_poi(self):
        row = self.links.set_index("master_store_id").loc[1]
        self.assertEqual(row["placekey"], "pk-fl")           # not the closed duplicate, not the dentist
        self.assertEqual(row["match_grade"], "A")

    def test_store_with_no_counterpart_is_not_linked(self):
        self.assertNotIn(2, set(self.links.master_store_id))

    def test_same_store_beyond_150m_still_links_on_name_and_house(self):
        row = self.links.set_index("master_store_id").loc[3]  # ~178 m away, 55 Elm St vs Street
        self.assertEqual(row["placekey"], "pk-dg")
        self.assertEqual(row["match_grade"], "A")

    def test_assignment_is_one_to_one(self):
        self.assertTrue(self.links.placekey.is_unique)
        self.assertTrue(self.links.master_store_id.is_unique)

    def test_candidates_are_counted_per_dewey_poi(self):
        row = self.links.set_index("placekey").loc["pk-fl"]
        self.assertEqual(int(row["n_candidates"]), 1)         # one of our stores could be this POI

    def test_unnamed_poi_never_matches_on_name(self):
        dewey = _dewey().assign(LOCATION_NAME=[None, "Bright Smile Dental", float("nan"), None, None])
        master = _master().assign(store_name=["nan", "Corner Market", "Dollar General"])
        links = s17.match_master_to_dewey(master, dewey)
        self.assertNotIn(1, set(links.master_store_id))

    def test_shared_town_name_alone_is_not_a_name_match(self):
        dewey = pd.DataFrame({"PLACEKEY": ["pk-spa", "pk-lf"], "LOCATION_NAME": ["Maggie Valley Spa", "Lowes Foods"],
                              "STREET_ADDRESS": ["9 Oak St", "1 Elm St"], "LATITUDE": [LAT, LAT + 0.0500],
                              "LONGITUDE": [LON, LON], "CLOSED_ON": [None, None]})
        master = pd.DataFrame({"store_id": [1, 2],
                               "store_name": ["Wild Market, Maggie Valley, NC", "Lowes Foods of Bermuda Run"],
                               "street": ["4 Pine Rd", "7 Ash Rd"], "full_address": [None, None],
                               "city": ["Maggie Valley", "Bermuda Run"],
                               "source_lat": [LAT + 0.0003, LAT + 0.0503], "source_lon": [LON, LON]})
        links = s17.match_master_to_dewey(master, dewey).set_index("master_store_id")
        self.assertNotIn(1, links.index)                      # only "maggie valley" in common
        self.assertEqual(links.loc[2, "placekey"], "pk-lf")   # brand survives the city strip

    def test_one_shared_word_inside_a_longer_name_is_not_a_match(self):
        self.assertLess(s17.name_similarity("Country Store", "High Country Ceramic Arts", frozenset()),
                        s17.NAME_PARTIAL)

    def test_brand_short_forms_still_match(self):
        for ours, theirs in [("Shell", "Shell Oil"), ("Exxon", "Exxon Mobil"),
                             ("Walmart Supercenter", "Walmart"), ("Food Lion #1234", "Food Lion")]:
            self.assertGreaterEqual(s17.name_similarity(ours, theirs, frozenset()), s17.NAME_STRONG, ours)

    def test_poi_without_coordinates_is_skipped_not_fatal(self):
        dewey = _dewey().assign(LATITUDE=[LAT, LAT + 0.0001, None, None, LAT + 0.0500])
        links = s17.match_master_to_dewey(_master(), dewey)
        self.assertEqual(links.set_index("master_store_id").loc[1, "placekey"], "pk-fl")


class BuildCatalog(unittest.TestCase):
    def _inputs(self):
        dewey = _dewey().assign(
            TOP_CATEGORY="Grocery Stores", SUB_CATEGORY=None, NAICS_CODE="445110", CATEGORY_TAGS="[]",
            CITY="Raleigh", POSTAL_CODE="27601", OPENED_ON=None, PHONE_NUMBER=None, WEBSITE=None,
            PARENT_PLACEKEY=None, SAFEGRAPH_PLACE_ID=None, TOP_CATEGORY_2022=None,
            SUB_CATEGORY_2022=None, NAICS_CODE_2022=None, BRANDS="[]")
        master = _master().assign(place_id=["g1", "g2", "g3"], category="Grocery store",
                                  target_reviews=[100, 5, 50], reviews_in_db=[100, 0, 10],
                                  reviews_with_text=[60, 0, 4],
                                  scrape_status=["complete", "not_started", "incomplete"])
        return dewey, master

    def test_one_row_per_poi_with_status(self):
        dewey, master = self._inputs()
        cat = s17.build_catalog(dewey, master, s17.match_master_to_dewey(master, dewey))
        self.assertEqual(len(cat), len(dewey))
        self.assertTrue(cat.placekey.is_unique)
        status = cat.set_index("placekey").review_status
        self.assertEqual(status["pk-fl"], s17.STATUS_COMPLETE)
        self.assertEqual(status["pk-dg"], s17.STATUS_INCOMPLETE)
        self.assertEqual(status["pk-dent"], s17.STATUS_NOT_IN_DB)

    def test_duplicate_master_store_id_is_refused(self):
        dewey, master = self._inputs()
        dup = pd.concat([master, master.head(1)], ignore_index=True)
        with self.assertRaises(SystemExit):
            s17.build_catalog(dewey, dup, s17.match_master_to_dewey(master, dewey))


def _geo(placekey: str, store_id: int) -> pd.DataFrame:
    """One geomatch pair, Dewey on the left (id_left = PLACEKEY, id_right = our store_id)."""
    return pd.DataFrame({"id_left": [placekey], "id_right": [store_id], "score": [0.95], "decision": ["match"],
                         "why": ["geomatch rule"], "name_sim": [100.0], "dist_km": [0.02], "n_alternatives": [1]})


EMPTY_GEO = pd.DataFrame(columns=["id_left", "id_right", "score", "decision", "why", "name_sim", "dist_km",
                                  "n_alternatives"])


class DeweyLeadsTheMatch(unittest.TestCase):
    def test_geomatch_links_win_and_proximity_only_uses_what_is_left(self):
        links = s17.link_dewey_to_master(_dewey(), _master(), _geo("pk-fl-old", 1)).set_index("placekey")
        self.assertEqual(links.loc["pk-fl-old", "master_store_id"], 1)
        self.assertEqual(links.loc["pk-fl-old", "match_grade"], "G")
        self.assertEqual(links.loc["pk-dg", "master_store_id"], 3)     # proximity still fills the rest
        self.assertTrue(links.master_store_id.is_unique)

    def test_same_address_different_business_is_rejected(self):
        # geomatch pairs the dentist with our Food Lion because they share 100 Main St
        links = s17.link_dewey_to_master(_dewey(), _master(), _geo("pk-dent", 1)).set_index("placekey")
        self.assertNotIn("pk-dent", links.index)
        self.assertEqual(links.loc["pk-fl", "master_store_id"], 1)       # the right POI gets it instead

    def test_same_address_with_the_same_phone_is_kept_despite_the_name(self):
        dewey = _dewey().assign(PHONE_NUMBER=[None, "(919) 555-0001", None, None, None])
        master = _master().assign(phone=["919-555-0001", None, None])
        links = s17.link_dewey_to_master(dewey, master, _geo("pk-dent", 1)).set_index("placekey")
        self.assertEqual(links.loc["pk-dent", "master_store_id"], 1)

    def test_no_geomatch_pairs_is_the_proximity_result(self):
        a = s17.link_dewey_to_master(_dewey(), _master(), EMPTY_GEO).sort_values("master_store_id")
        b = s17.match_master_to_dewey(_master(), _dewey()).sort_values("master_store_id")
        self.assertEqual(a.placekey.tolist(), b.placekey.tolist())


class PhonePass(unittest.TestCase):
    def _frames(self, dewey_phones):
        dewey = pd.DataFrame({"PLACEKEY": ["pk-a", "pk-b"], "LOCATION_NAME": ["Quick Stop", "Tire Barn"],
                              "STREET_ADDRESS": ["1 A St", "2 B St"], "LATITUDE": [LAT, LAT + 0.3],
                              "LONGITUDE": [LON, LON], "CLOSED_ON": [None, None], "PHONE_NUMBER": dewey_phones})
        master = pd.DataFrame({"store_id": [7], "store_name": ["Corner Market"], "street": ["9 C St"],
                               "full_address": [None], "source_lat": [LAT + 0.005], "source_lon": [LON],
                               "phone": ["+1 919-555-0101"]})
        return dewey, master

    def test_unique_shared_phone_nearby_links_a_renamed_store(self):
        links = s17.link_dewey_to_master(*self._frames(["9195550101", None]), EMPTY_GEO)
        self.assertEqual(links.set_index("placekey").loc["pk-a", "match_grade"], "P")

    def test_phone_shared_by_several_pois_links_nothing(self):
        links = s17.link_dewey_to_master(*self._frames(["9195550101", "9195550101"]), EMPTY_GEO)
        self.assertTrue(links.empty)

    def test_phone_too_far_away_links_nothing(self):
        dewey, master = self._frames(["9195550101", None])
        links = s17.link_dewey_to_master(dewey, master.assign(source_lat=LAT + 0.5), EMPTY_GEO)
        self.assertTrue(links.empty)


class ReviewStatus(unittest.TestCase):
    def test_maps_master_status_and_unmatched(self):
        out = s17.review_status(pd.Series(["complete", "incomplete", "not_started", "unknown_target", None]))
        self.assertEqual(out.tolist(), [s17.STATUS_COMPLETE, s17.STATUS_INCOMPLETE, s17.STATUS_NOT_STARTED,
                                        s17.STATUS_UNKNOWN_TARGET, s17.STATUS_NOT_IN_DB])


def _catalog() -> pd.DataFrame:
    return pd.DataFrame({
        "top_category": ["Grocery Stores"] * 3 + ["Gasoline Stations"] * 2,
        "sub_category": ["Supermarkets", "Supermarkets", None, "Gas Stations", "Gas Stations"],
        "naics_code": ["445110", "445110", "445100", "447110", "447110"],
        "brand_name": ["Food Lion", "Food Lion", "(no brand)", "Sheetz", "Circle K"],
        "location_name": ["Food Lion", "Food Lion", "Joe's", "Sheetz", "Circle K"],
        "is_closed": [0, 1, 0, 0, 0],
        "review_status": [s17.STATUS_COMPLETE, s17.STATUS_NOT_IN_DB, s17.STATUS_INCOMPLETE,
                          s17.STATUS_NOT_IN_DB, s17.STATUS_NOT_STARTED],
    })


class CategorySummary(unittest.TestCase):
    def test_counts_per_pair_with_blank_sub_labelled(self):
        s = s17.category_summary(_catalog(), ["top_category", "sub_category"])
        keyed = s.set_index(["top_category", "sub_category"])
        sup = keyed.loc[("Grocery Stores", "Supermarkets")]
        self.assertEqual(int(sup["pois"]), 2)
        self.assertEqual(int(sup["open_pois"]), 1)
        self.assertEqual(int(sup["in_our_db"]), 1)
        self.assertEqual(int(sup[s17.STATUS_COMPLETE]), 1)
        self.assertIn(("Grocery Stores", s17.NO_SUBCATEGORY), keyed.index)

    def test_top_brands_listed_by_frequency(self):
        s = s17.category_summary(_catalog(), ["top_category"]).set_index("top_category")
        self.assertTrue(s.loc["Grocery Stores", "top_brands"].startswith("Food Lion (2)"))

    def test_has_decision_column_left_blank(self):
        s = s17.category_summary(_catalog(), ["top_category"])
        self.assertIn(s17.DECISION_COLUMN, s.columns)
        self.assertTrue((s[s17.DECISION_COLUMN] == "").all())


class FoodRelevant(unittest.TestCase):
    def test_fixed_list_plus_categories_holding_our_stores(self):
        tops = pd.DataFrame({"top_category": ["Grocery Stores", "Offices of Physicians", "Florists"],
                             "in_our_db": [10, 0, s17.MIN_OUR_STORES_FOR_FILE]})
        out = s17.food_relevant(tops).set_index("top_category")["gets_file"]
        self.assertEqual(out.to_dict(), {"Grocery Stores": "food list", "Offices of Physicians": "",
                                         "Florists": "holds our stores"})


class Names(unittest.TestCase):
    def test_sheet_names_are_excel_safe_and_unique(self):
        used: set[str] = set()
        a = s17.sheet_name("Supermarkets and Other Grocery (except Convenience) Stores", used)
        b = s17.sheet_name("Supermarkets and Other Grocery (except Convenience) Stores", used)
        c = s17.sheet_name("Drugs/Sundries: [misc]?", used)
        self.assertLessEqual(max(len(a), len(b)), 31)
        self.assertNotEqual(a, b)
        self.assertFalse(set(c) & set("[]:*?/\\"))

    def test_file_slug(self):
        self.assertEqual(s17.file_slug("Beer, Wine, and Liquor Stores"), "Beer_Wine_and_Liquor_Stores")
