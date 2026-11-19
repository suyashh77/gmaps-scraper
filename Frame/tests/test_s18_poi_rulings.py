"""Tests for s18_poi_rulings: suggestions, chain extraction, the page data, and turning
saved rulings into the final POI list.

The rules a ruling relies on: nothing enters the list without an explicit In, a
suggested-out category stays out unless someone flips it, a store name with more than
five POIs is a chain and follows the chain's ruling rather than its category's, and
anything undecided is reported, never included.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import s18_poi_rulings as s18  # noqa: E402

SUP, CONV, REST = "Supermarkets", "Convenience Stores", "Full-Service Restaurants"


def _catalog() -> pd.DataFrame:
    """7 Food Lions (a chain), 2 independent grocers, 6 Circle Ks across two subcategories
    (a chain), 1 corner store, 1 diner, 1 doctor."""
    rows = (
        [("Grocery Stores", SUP, "445110", f"Food Lion #{i}", 0) for i in range(7)]
        + [("Grocery Stores", SUP, "445110", "Joe's Market", 0), ("Grocery Stores", SUP, "445110", "Old Grocer", 1)]
        + [("Grocery Stores", CONV, "445120", "Circle K", 0) for _ in range(4)]
        + [("Gasoline Stations", "Gasoline Stations with Convenience Stores", "447110", "CIRCLE K", 0)] * 2
        + [("Grocery Stores", CONV, "445120", "Quick Stop", 0)]
        + [("Restaurants and Other Eating Places", REST, "722511", "Mama's Diner", 0)]
        + [("Offices of Physicians", "Doctors", "621111", "Dr. Smith", 0)]
    )
    df = pd.DataFrame(rows, columns=["top_category", "sub_category", "naics_code", "location_name", "is_closed"])
    n = len(df)
    return df.assign(
        placekey=[f"p{i}" for i in range(n)], brand_name="(no brand)",
        street_address="1 Main St", city="Raleigh", postal_code="27601", category_tags="",
        review_status=["complete"] + ["not in our DB"] * (n - 1),
        our_store_id=[1] + [None] * (n - 1), our_reviews_in_db=[500] + [None] * (n - 1))


def _ids(df: pd.DataFrame) -> list[str]:
    return sorted(df.placekey)


class Suggest(unittest.TestCase):
    def test_core_grocery_is_suggested_in(self):
        self.assertEqual(s18.suggest("44511, 445110")[0], s18.YES)

    def test_contested_formats_are_maybe(self):
        for code in ("445120", "447110", "452319"):
            self.assertEqual(s18.suggest(code)[0], s18.MAYBE, code)

    def test_out_of_scope_formats_say_why(self):
        ruling, reason = s18.suggest("722511")
        self.assertEqual(ruling, s18.NO)
        self.assertIn("9 Sep", reason)


class Chains(unittest.TestCase):
    def test_chain_key_ignores_case_punctuation_and_store_numbers(self):
        self.assertEqual(s18.chain_key("Food Lion #1234"), s18.chain_key("FOOD LION"))
        self.assertEqual(s18.chain_key("Dollar General Store"), s18.chain_key("Dollar General"))
        self.assertEqual(s18.chain_key("7-Eleven"), "7 eleven")

    def test_more_than_five_pois_is_a_chain(self):
        chains = s18.chain_ids(_catalog())
        self.assertEqual(chains.notna().sum(), 13)                   # 7 Food Lion + 6 Circle K
        self.assertEqual(chains[_catalog().location_name == "Joe's Market"].isna().all(), True)

    def test_exactly_five_is_not_a_chain(self):
        df = pd.DataFrame({"location_name": ["Five Guys Market"] * 5})
        self.assertTrue(s18.chain_ids(df).isna().all())


class PageData(unittest.TestCase):
    def setUp(self):
        self.page = s18.page_data(_catalog())

    def test_chain_pois_leave_their_category(self):
        sup = next(s for s in self.page["subs"] if s["sub_key"] == s18.sub_key("Grocery Stores", SUP))
        self.assertEqual(sup["pois"], 2)                               # Joe's + Old Grocer only

    def test_chain_spans_subcategories(self):
        ck = next(c for c in self.page["chains"] if c["chain"] == s18.chain_key("Circle K"))
        self.assertEqual(ck["pois"], 6)
        self.assertEqual(ck["bucket"], s18.BUCKET_MAYBE)

    def test_suggested_out_goes_to_ruled_out_bucket(self):
        rest = next(s for s in self.page["subs"] if s["sub_category"] == REST)
        self.assertEqual(rest["bucket"], s18.BUCKET_OUT)
        doc = next(t for t in self.page["auto_no"] if t["top_category"] == "Offices of Physicians")
        self.assertEqual(doc["bucket"], s18.BUCKET_OUT)

    def test_poi_lists_cover_every_maybe_item(self):
        lists = s18.poi_lists(_catalog())
        self.assertEqual(len(lists["chain:" + s18.chain_key("Food Lion")]["rows"]), 7)
        self.assertEqual(len(lists["sub:" + s18.sub_key("Grocery Stores", CONV)]["rows"]), 1)
        self.assertNotIn("sub:" + s18.sub_key("Restaurants and Other Eating Places", REST), lists)


class ApplyRulings(unittest.TestCase):
    def test_nothing_is_in_without_an_explicit_in(self):
        out = s18.apply_rulings(_catalog(), [])
        self.assertEqual(len(out.final), 0)
        self.assertIn("p0", set(out.undecided.placekey))              # suggested-in Food Lion waits

    def test_suggested_out_stays_out_by_default(self):
        out = s18.apply_rulings(_catalog(), [])
        diner_and_doctor = set(_catalog().placekey[_catalog().location_name.isin(["Mama's Diner", "Dr. Smith"])])
        self.assertFalse(diner_and_doctor & set(out.undecided.placekey))
        self.assertFalse(diner_and_doctor & set(out.final.placekey))

    def test_chain_ruling_overrides_its_category(self):
        rulings = [{"kind": "sub", "key": s18.sub_key("Grocery Stores", CONV), "ruling": s18.NO},
                   {"kind": "chain", "key": s18.chain_key("Circle K"), "ruling": s18.YES}]
        out = s18.apply_rulings(_catalog(), rulings)
        names = set(out.final.location_name)
        self.assertEqual(names, {"Circle K", "CIRCLE K"})             # both subcategories, not Quick Stop

    def test_category_ruling_covers_independents_and_closed_split_out(self):
        out = s18.apply_rulings(_catalog(), [{"kind": "sub", "key": s18.sub_key("Grocery Stores", SUP),
                                              "ruling": s18.YES}])
        self.assertEqual(set(out.final.location_name), {"Joe's Market"})
        self.assertEqual(set(out.closed.location_name), {"Old Grocer"})

    def test_flipping_a_ruled_out_category_in(self):
        out = s18.apply_rulings(_catalog(), [{"kind": "top", "key": "Offices of Physicians", "ruling": s18.YES}])
        self.assertIn("Dr. Smith", set(out.final.location_name))

    def test_cleared_ruling_is_ignored_and_later_record_wins(self):
        key = s18.sub_key("Grocery Stores", SUP)
        rulings = [{"kind": "sub", "key": key, "ruling": s18.YES}, {"kind": "sub", "key": key, "ruling": None}]
        self.assertEqual(len(s18.apply_rulings(_catalog(), rulings).final), 0)

    def test_single_store_ruling_beats_its_chain_and_category(self):
        rulings = [{"kind": "chain", "key": s18.chain_key("Food Lion"), "ruling": s18.YES},
                   {"kind": "poi", "key": "p3", "ruling": s18.NO},                  # one Food Lion out
                   {"kind": "poi", "key": "p15", "ruling": s18.YES}]                # Quick Stop in
        out = s18.apply_rulings(_catalog(), rulings)
        self.assertNotIn("p3", set(out.final.placekey))
        self.assertIn("p15", set(out.final.placekey))
        self.assertEqual(len(out.final), 7)                                        # 6 Food Lions + Quick Stop

    def test_explicit_maybe_reopens_a_ruled_out_item(self):
        rulings = [{"kind": "top", "key": "Offices of Physicians", "ruling": s18.MAYBE_RULING}]
        out = s18.apply_rulings(_catalog(), rulings)
        self.assertIn("Dr. Smith", set(out.undecided.location_name))

    def test_store_lists_carry_the_placekey(self):
        lists = s18.poi_lists(_catalog())
        self.assertEqual(lists["chain:" + s18.chain_key("Food Lion")]["cols"][0], "placekey")

    def test_rejects_unknown_ruling_value(self):
        with self.assertRaises(ValueError):
            s18.apply_rulings(_catalog(), [{"kind": "sub", "key": "x", "ruling": "maybe"}])
