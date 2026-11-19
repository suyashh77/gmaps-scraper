"""Tests for the pure helpers in s15_poi_decision_sheet.

The sheet is only as good as its joins: ZIP normalisation decides the geomatch
blocks, the one-to-one assignment decides which frame row a SafeGraph record
lands on, and the coalesce decides what the reader sees in the "best" columns.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import s15_poi_decision_sheet as s15  # noqa: E402


class ExternalDataPath(unittest.TestCase):
    def test_env_var_overrides_the_default_location(self):
        import importlib
        import os
        os.environ["GMAPS_EXTERNAL_DATA"] = "/elsewhere"
        try:
            mod = importlib.reload(s15)
            self.assertEqual(Path("/elsewhere") / "dewy data" / "nc_grocery_pois.db", mod.DEWEY_DB)
        finally:
            del os.environ["GMAPS_EXTERNAL_DATA"]
            importlib.reload(s15)

    def test_default_sits_beside_the_repo_not_inside_it(self):
        self.assertEqual(s15.ROOT.parent, s15.EXTERNAL.parent)


class Zip5(unittest.TestCase):
    def test_strips_float_and_plus4(self):
        self.assertEqual(s15.zip5("27284.0"), "27284")
        self.assertEqual(s15.zip5("28105-5766"), "28105")
        self.assertEqual(s15.zip5(27284.0), "27284")

    def test_missing_is_none(self):
        self.assertIsNone(s15.zip5(None))
        self.assertIsNone(s15.zip5(float("nan")))
        self.assertIsNone(s15.zip5("abc"))


class BuildAddr(unittest.TestCase):
    def test_joins_parts_and_skips_blanks(self):
        df = pd.DataFrame({"st": ["1 Main St", None], "ct": ["Apex", "Cary"], "zp": ["27523", None]})
        out = s15.build_addr(df, "st", "ct", "zp")
        self.assertEqual(out.tolist(), ["1 Main St, Apex, NC 27523", "Cary, NC"])


class AssignOneToOne(unittest.TestCase):
    def test_best_pair_wins_and_each_side_used_once(self):
        pairs = pd.DataFrame({
            "id_left": ["A", "A", "B", "C"],
            "id_right": [1, 2, 1, 3],
            "score": [1.0, 0.9, 0.95, 0.8],
            "name_sim": [0.5, 1.0, 0.9, 0.9],
            "dist_km": [0.1, 0.1, 0.1, 0.1],
            "decision": ["match", "match", "match", "non-match"],
        })
        out = s15.assign_one_to_one(pairs)
        # A takes right 1 (higher score); B cannot reuse 1 and has no other; C not a match
        self.assertEqual(out.set_index("id_left")["id_right"].to_dict(), {"A": 1})
        self.assertEqual(int(out.loc[out.id_left == "A", "n_alternatives"].iloc[0]), 2)

    def test_tie_on_score_breaks_on_name_then_distance(self):
        pairs = pd.DataFrame({
            "id_left": ["A", "A"],
            "id_right": [1, 2],
            "score": [1.0, 1.0],
            "name_sim": [0.2, 0.9],
            "dist_km": [0.01, 0.5],
            "decision": ["match", "match"],
        })
        out = s15.assign_one_to_one(pairs)
        self.assertEqual(out.id_right.tolist(), [2])

    def test_empty_input(self):
        out = s15.assign_one_to_one(pd.DataFrame(columns=s15.PAIR_COLUMNS))
        self.assertEqual(len(out), 0)
        self.assertIn("n_alternatives", out.columns)


class NormalizePairs(unittest.TestCase):
    """geomatch v2 renamed the two features the sheet reads; v1 output must still work."""

    def test_v2_distance_in_metres_becomes_km(self):
        raw = pd.DataFrame({"id_left": ["a"], "id_right": [1], "score": [0.99], "decision": ["match"],
                            "why": ["names match"], "name_sim": [0.98], "dist_m": [2743.5]})
        out = s15.normalize_pairs(raw)
        self.assertAlmostEqual(float(out.dist_km.iloc[0]), 2.7435)
        self.assertAlmostEqual(float(out.name_sim.iloc[0]), 0.98)
        self.assertEqual(list(out.columns), s15.PAIR_COLUMNS)

    def test_v1_feature_names_still_map(self):
        raw = pd.DataFrame({"id_left": ["a"], "id_right": [1], "score": [0.9], "decision": ["match"],
                            "why": [None], "f_token_set_ratio_poi": [0.8], "f_coord_distance_km": [0.25]})
        out = s15.normalize_pairs(raw)
        self.assertAlmostEqual(float(out.dist_km.iloc[0]), 0.25)
        self.assertAlmostEqual(float(out.name_sim.iloc[0]), 0.8)

    def test_missing_feature_columns_are_blank_not_absent(self):
        raw = pd.DataFrame({"id_left": ["a"], "id_right": [1], "score": [0.9], "decision": ["match"]})
        out = s15.normalize_pairs(raw)
        self.assertEqual(list(out.columns), s15.PAIR_COLUMNS)
        self.assertTrue(pd.isna(out.dist_km.iloc[0]))

    def test_uncertain_rows_are_kept_for_the_caller_to_drop(self):
        raw = pd.DataFrame({"id_left": ["a", "b"], "id_right": [1, 2], "score": [0.9, 0.5],
                            "decision": ["match", "uncertain"], "name_sim": [0.9, 0.5], "dist_m": [10.0, 20.0]})
        self.assertEqual(len(s15.normalize_pairs(raw)), 2)
        self.assertEqual(len(s15.assign_one_to_one(s15.normalize_pairs(raw))), 1)


class Coalesce(unittest.TestCase):
    def test_first_non_blank_wins(self):
        df = pd.DataFrame({"a": [None, "x", ""], "b": ["p", "q", "r"]})
        self.assertEqual(s15.coalesce(df, ["a", "b"]).tolist(), ["p", "x", "r"])


class SourceFlags(unittest.TestCase):
    def test_lists_sources_present(self):
        df = pd.DataFrame({"frame_store_id": [1, None], "google_probed_at": ["t", None],
                           "master_store_id": [None, 7], "dewey_placekey": ["k", None],
                           "patterns_store_id": [None, None]})
        self.assertEqual(s15.source_flags(df).tolist(), ["frame|google|dewey", "master"])


class NormName(unittest.TestCase):
    def test_strips_corporate_suffixes_and_store_numbers(self):
        self.assertEqual(s15.norm_name("Food Lion,  LLC"), "food lion")
        self.assertEqual(s15.norm_name("Ingles Markets, Incorporated"), "ingles")
        self.assertEqual(s15.norm_name("BJ's Membership Club 110"), "bjs membership club")
        self.assertEqual(s15.norm_name("Harris Teeter Pharmacy"), "harris teeter pharmacy")


class MatchConfidence(unittest.TestCase):
    def test_maps_why_to_confidence_label(self):
        why = pd.Series(["accepted by rule: house number + street + ZIP/city all match",
                         "proximity pass: same house number + name agrees within 12 m",
                         "proximity pass: name agrees within 40 m",
                         "proximity pass: same house number within 90 m", None])
        self.assertEqual(s15.match_confidence(why).tolist(),
                         ["A geomatch", "B proximity house+name", "C proximity name",
                          "D proximity house only - verify", None])


class ProximityPass(unittest.TestCase):
    """Second pass for records geomatch left behind: same spot, name or house number agrees."""

    def _sides(self):
        left = pd.DataFrame({
            "_id": ["L1", "L2", "L3", "L4"],
            "name": ["Harris Teeter", "Food Lion, LLC", "Shell", "Far Away Foods"],
            "addr": ["750 West Williams Street, Apex, NC 27539", "3501 Highway 601, Concord, NC 28025",
                     "1 Main St, Apex, NC 27523", "9 Nowhere Rd, Apex, NC 27523"],
            "lat": [35.7300, 35.4000, 35.7400, 35.7500],
            "lon": [-78.8500, -80.6000, -78.8600, -78.8700]})
        right = pd.DataFrame({
            "_id": [1, 2, 3, 4],
            "name": ["Harris Teeter Pharmacy", "Food Lion", "Kangaroo Express", "Far Away Foods"],
            "addr": ["750 W Williams St, Apex, NC 27502", "3501 US Highway 601 S, Concord, NC 28025",
                     "1 Main St, Apex, NC 27523", "9 Nowhere Rd, Apex, NC 27523"],
            "lat": [35.73002, 35.40005, 35.74001, 35.7600],   # 4 is ~1.1 km away
            "lon": [-78.85001, -80.60003, -78.86001, -78.8700]})
        return left, right

    def test_accepts_name_or_house_number_within_radius(self):
        left, right = self._sides()
        out = s15.proximity_pass(left, right, taken_left=set(), taken_right=set())
        got = out.set_index("id_left")["id_right"].to_dict()
        self.assertEqual(got, {"L1": 1, "L2": 2, "L3": 3})   # L4 is too far
        self.assertTrue(out.why.str.startswith("proximity pass").all())
        self.assertTrue((out.score < 1.0).all())

    def test_respects_taken_sides(self):
        left, right = self._sides()
        out = s15.proximity_pass(left, right, taken_left={"L1"}, taken_right={2})
        self.assertEqual(out.id_left.tolist(), ["L3"])


def _pairs(rows):
    cols = s15.PAIR_COLUMNS + ["n_alternatives"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


class Assemble(unittest.TestCase):
    """Five tiny sources, one deliberate place_id collision, one leftover of every kind."""

    def setUp(self):
        self.saved = {n: getattr(s15, n) for n in
                      ("load_frame", "load_probe", "load_master", "load_dewey", "load_patterns",
                       "_read_match")}
        frame = pd.DataFrame({
            "store_id": [1, 2, 3], "name": ["A", "B", "C"], "street": ["1 x", "2 x", "3 x"],
            "city": ["Apex"] * 3, "state": ["NC"] * 3, "zip": ["27523.0"] * 3, "county": ["WAKE"] * 3,
            "lat": [35.7] * 3, "lon": [-78.8] * 3, "place_id": [None, "P_OLD", None],
            "tier": ["CORE"] * 3, "food_access_type": ["supermarket"] * 3,
            "drop_reason": [None, None, "UNCORROBORATED"], "drop_detail": [None, None, "one source"],
            "disposition": ["PROBE_LIST", "PROBE_LIST", "DROPPED_BY_S10"]})
        # stores 1 and 2 both resolved to Google place P1 (a collision); 2 also has an old id
        probe = pd.DataFrame({
            "store_id": [1, 2], "name": ["A", "B"], "street": ["1 x", "2 x"], "city": ["Apex"] * 2,
            "state": ["NC"] * 2, "zip": ["27523.0"] * 2, "county": ["WAKE"] * 2,
            "lat": [35.7] * 2, "lon": [-78.8] * 2, "place_id_in": [None, "P_OLD"],
            "food_access_type": ["supermarket"] * 2, "status": ["done"] * 2, "attempts": [1, 1],
            "g_name": ["A", "B"], "g_category": ["Grocery store"] * 2, "g_address": ["", ""],
            "g_phone": ["", ""], "g_website": ["", ""], "g_rating": [4.0, 4.1],
            "g_total_reviews": [10, 20], "g_place_id": ["P1", "P1"], "g_lat": [35.7] * 2,
            "g_lon": [-78.8] * 2, "g_closed": [0, 0], "verified": [1, 1], "error": [None, None],
            "probed_at": ["2026-09-10 10:00:00"] * 2, "machine_id": ["t"] * 2,
            "g_feature_id": [None, None], "name_sim": [None, None]})
        master = pd.DataFrame({
            "store_id": [100, 101, 102], "place_id": ["P1", "P_OLD", "P_ORPHAN"],
            "store_name": ["A", "B", "Z"], "street": ["1 x", "2 x", "9 z"], "city": ["Apex"] * 3,
            "state": ["NC"] * 3, "zip": ["27523"] * 3, "source_lat": [35.7] * 3,
            "source_lon": [-78.8] * 3, "scrape_status": ["complete"] * 3,
            "reviews_scraped": [10, 20, 5], "reviews_in_db": [10, 20, 5],
            "reviews_with_text": [5, 10, 1], "last_scraped_at": ["2026-09-01"] * 3})
        dewey = pd.DataFrame({
            "PLACEKEY": ["K1", "K_ONLY"], "LOCATION_NAME": ["A", "Dewey only"],
            "STREET_ADDRESS": ["1 x", "7 q"], "CITY": ["Apex"] * 2, "REGION": ["NC"] * 2,
            "POSTAL_CODE": ["27523"] * 2, "LATITUDE": [35.7] * 2, "LONGITUDE": [-78.8] * 2,
            "SUB_CATEGORY": ["Supermarkets"] * 2, "food_access_type": ["supermarket"] * 2,
            "operational_status": ["closed"] * 2, "CLOSED_ON": [None, None], "closed": [0, 0]})
        patterns = pd.DataFrame({
            "store_id": [900, 901, 902], "store_name": ["A", "Dewey only", "Patterns only"],
            "brand": ["A", "", ""], "address": ["1 x", "7 q", "8 r"], "city": ["Apex"] * 3,
            "state": ["NC"] * 3, "zip_code": ["27523"] * 3, "latitude": [35.7] * 3,
            "longitude": [-78.8] * 3, "sub_category": ["Supermarkets"] * 3,
            "close_date": ["2038-01-01 00:00:00"] * 3, "total_visits": [1, 2, 3],
            "avg_weekly_visits": [1.0, 2.0, 3.0], "closed": [0, 0, 0]})
        matches = {
            s15.MATCH_DEWEY_FRAME: _pairs([["K1", 1, 1.0, "match", "rule", 1.0, 0.0, 1]]),
            s15.MATCH_PATTERNS_FRAME: _pairs([[900, 1, 1.0, "match", "rule", 1.0, 0.0, 1]]),
            s15.MATCH_PATTERNS_DEWEY: _pairs([[901, "K_ONLY", 1.0, "match", "rule", 1.0, 0.0, 1]]),
        }
        s15.load_frame = lambda: frame
        s15.load_probe = lambda: probe
        s15.load_master = lambda: master
        s15.load_dewey = lambda: dewey
        s15.load_patterns = lambda: patterns
        s15._read_match = lambda path: matches[path]

    def tearDown(self):
        for n, f in self.saved.items():
            setattr(s15, n, f)

    def test_one_row_per_entity_and_leftovers_kept(self):
        out = s15.assemble().set_index("row_key")
        # 3 frame rows + Dewey-only (with its Patterns match folded in) + Patterns-only + master orphan
        self.assertEqual(sorted(out.index), ["dewey:K_ONLY", "frame:1", "frame:2", "frame:3",
                                             "master:102", "patterns:902"])
        self.assertEqual(out.loc["frame:1", "sources"], "frame|google|master|dewey|patterns")
        self.assertEqual(out.loc["frame:3", "sources"], "frame")
        self.assertEqual(out.loc["dewey:K_ONLY", "sources"], "dewey|patterns")
        self.assertEqual(out.loc["dewey:K_ONLY", "patterns_store_id"], 901)

    def test_master_join_prefers_frame_prior_id_then_probe_id(self):
        out = s15.assemble().set_index("row_key")
        self.assertEqual(out.loc["frame:1", "master_store_id"], 100)      # via probe's P1
        self.assertEqual(out.loc["frame:2", "master_store_id"], 101)      # own P_OLD beats shared P1
        self.assertEqual(out.loc["frame:1", "google_place_id_shared_by_n_frame_rows"], 2)
        self.assertTrue(pd.isna(out.loc["frame:3", "join_place_id"]))
        self.assertEqual(out.loc["frame:3", "in_probe_list"], 0)
        self.assertEqual(out.loc["frame:1", "in_probe_list"], 1)


if __name__ == "__main__":
    unittest.main()
