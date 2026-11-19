"""Tests for r13_rulings: probe targets, wrong-location sources, stalled completes,
accepted flags, and moving out-of-scope stores to their own file."""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "Scraper_v2"))

import r12_rulings as R12                     # noqa: E402
import r13_rulings as R                       # noqa: E402
from test_remediation import TempDBCase       # noqa: E402


class TestRules(unittest.TestCase):
    def test_stalled_rule(self):
        """Owner rule 2026-10-24: target capped at 1,500; allowed missing 10 (<=100), 20 (>100),
        50 (>500), 100 (>1,500)."""
        self.assertTrue(R.stalled_is_complete(have=1500, target=9724))   # cap: 1,500 held of a true 9,724
        self.assertTrue(R.stalled_is_complete(have=1400, target=3000))   # over 1,500: 100 missing of the cap
        self.assertFalse(R.stalled_is_complete(have=1399, target=3000))  # 101 missing
        self.assertTrue(R.stalled_is_complete(have=1450, target=1500))   # exactly 1,500 is not over 1,500: 50 allowed
        self.assertFalse(R.stalled_is_complete(have=1449, target=1500))
        self.assertTrue(R.stalled_is_complete(have=551, target=601))     # over 500: 50 missing
        self.assertFalse(R.stalled_is_complete(have=550, target=601))
        self.assertTrue(R.stalled_is_complete(have=480, target=500))     # exactly 500 is not over 500: 20 allowed
        self.assertFalse(R.stalled_is_complete(have=479, target=500))
        self.assertTrue(R.stalled_is_complete(have=180, target=200))     # over 100: 20 missing
        self.assertFalse(R.stalled_is_complete(have=179, target=200))
        self.assertTrue(R.stalled_is_complete(have=90, target=100))      # exactly 100 is not over 100: 10 allowed
        self.assertFalse(R.stalled_is_complete(have=89, target=100))
        self.assertTrue(R.stalled_is_complete(have=20, target=30))       # small store: 10 missing
        self.assertFalse(R.stalled_is_complete(have=19, target=30))
        self.assertTrue(R.stalled_is_complete(have=30, target=30))       # at target
        self.assertTrue(R.stalled_is_complete(have=1600, target=1500))   # over target is fine

    def test_probe_target_keeps_the_larger_count_and_caps(self):
        self.assertEqual(80, R.target_from_live(live=80, have=10))
        self.assertEqual(120, R.target_from_live(live=80, have=120))
        self.assertEqual(1500, R.target_from_live(live=4000, have=0))


class TestApply(TempDBCase):
    """build_master: store 1 target 100, store 2 target NULL, store 3 Convenience store."""

    def setUp(self):
        super().setUp()
        con = sqlite3.connect(self.master)
        con.execute("ALTER TABLE stores ADD COLUMN feature_id TEXT")
        con.execute("INSERT INTO stores (store_id, place_id, store_name, city, state, zip, "
                    "full_address, target_reviews, category) VALUES "
                    "(80,'ChIJeighty','Dollar General','Hillsborough','NC','27278',"
                    "'110 Rebecca Dr, Hillsborough, NC 27278',50,'Dollar store'),"
                    "(81,'ChIJeightyone','Big Grocer','Raleigh','NC','27601',"
                    "'9 Z St, Raleigh, NC 27601',150,'Grocery store')")
        rows = [(80, f"F{i}", 5, f"Food Lion page {i}", "master_reviews.db") for i in range(200)]
        rows += [(80, f"D{i}", 4, f"Dollar General review {i}", "list3.db") for i in range(10)]
        rows += [(81, f"G{i}", 4, f"Grocer review {i}", "run1_x.db") for i in range(140)]
        con.executemany("INSERT INTO reviews (store_id, reviewer_name, rating, review_text, "
                        "source_db) VALUES (?,?,?,?,?)", rows)
        con.execute("UPDATE stores SET reviews_scraped = (SELECT COUNT(*) FROM reviews r "
                    "WHERE r.store_id = stores.store_id)")
        con.commit()
        con.row_factory = sqlite3.Row
        R12.apply_all(con, dewey_types={}, attempts={})
        con.commit()
        con.close()
        self.out = os.path.join(self.tmp, "out_of_scope.db")

    def apply(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        res = R.apply_all(con, live_counts={"chijaaaaaaaaaaaaaaaaaaaaaa2": 40},
                          accept_far_ids=[90])
        con.commit()
        con.close()
        R.split_out_of_scope(self.master, self.out)
        return res

    def one(self, sql, params=(), db=None):
        con = sqlite3.connect(db or self.master)
        v = con.execute(sql, params).fetchone()[0]
        con.close()
        return v

    def test_no_target_store_gets_the_probe_live_count(self):
        self.apply()
        self.assertEqual(40, self.one("SELECT target_reviews FROM stores WHERE store_id=2"))

    def test_batch_that_brought_another_page_is_quarantined(self):
        self.apply()
        self.assertEqual(10, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=80"))
        self.assertEqual(200, self.one("SELECT COUNT(*) FROM quarantined_reviews "
                                       "WHERE store_id=80"))

    def test_store_over_100_missing_ten_is_ruled_complete(self):
        self.apply()
        self.assertEqual("complete", self.one("SELECT scrape_status FROM stores WHERE store_id=81"))

    def test_accepted_far_store_is_not_reported_by_the_validator(self):
        self.apply()
        con = sqlite3.connect(self.master)
        self.assertEqual({90}, R.accepted_ids(con, R.FAR_FROM_ADDRESS))
        con.close()

    def test_validator_does_not_count_a_ruled_complete_store_as_incomplete(self):
        import r00_validate as V
        self.apply()
        checks = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}
        self.assertEqual(0, checks["scrape_status disagrees with the counts"])
        incomplete = self.one("SELECT COUNT(*) FROM stores WHERE target_reviews > 0 "
                              "AND reviews_scraped < target_reviews - 1 AND status_reason IS NULL")
        self.assertEqual(incomplete, checks["incomplete stores"], "store 81 is ruled complete")

    def test_validator_skips_coordinates_a_person_accepted(self):
        import r00_validate as V
        self.apply()
        con = sqlite3.connect(self.master)
        con.execute("UPDATE stores SET source_lat=46.4, source_lon=-129.9 WHERE store_id=1")
        con.commit()
        name = "coordinates outside the NC bounding box"
        flagged = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        R.accept(con, [1], "coords_outside_nc", "manual round 4: address is in NC; Google's own pin is wrong")
        con.commit()
        con.close()
        accepted = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        self.assertEqual(1, flagged - accepted)

    def test_validator_skips_an_over_target_store_whose_extra_reviews_were_accepted(self):
        import r00_validate as V
        self.apply()
        con = sqlite3.connect(self.master)
        con.execute("UPDATE stores SET target_reviews=20, reviews_scraped=40 WHERE store_id=1")
        con.commit()
        name = "over-scrape, all classes"
        flagged = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        R.accept(con, [1], "over_target_accumulated", "extra reviews from several scrapes; some since deleted")
        con.commit()
        con.close()
        accepted = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        self.assertEqual(1, flagged - accepted)

    def test_validator_skips_a_double_scrape_flag_for_a_store_whose_page_was_compared(self):
        import r00_validate as V
        self.apply()
        con = sqlite3.connect(self.master)
        con.execute("UPDATE stores SET target_reviews=20, reviews_scraped=40 WHERE store_id=1")   # 2.0x: class B
        con.commit()
        name = "over-scrape class B: double scrape"
        flagged = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        R.accept(con, [1], "over_target_accumulated", "full rescrape compared: CONSISTENT; extras kept")
        con.commit()
        con.close()
        accepted = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        self.assertEqual(1, flagged - accepted)

    def test_validator_does_not_flag_a_ruled_store_for_a_malformed_place_id(self):
        import r00_validate as V
        self.apply()
        con = sqlite3.connect(self.master)
        con.execute("UPDATE stores SET place_id='ChIGbroken', status_reason=NULL WHERE store_id=1")
        con.commit()
        name = "non-standard place_id"
        flagged = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        con.execute("UPDATE stores SET status_reason='ruled complete: place_id malformed, not re-scrapable' WHERE store_id=1")
        con.commit()
        con.close()
        ruled = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        self.assertEqual(1, flagged - ruled)

    def test_validator_does_not_flag_a_store_ruled_no_data_as_missing_its_target(self):
        import r00_validate as V
        self.apply()
        con = sqlite3.connect(self.master)
        con.execute("UPDATE stores SET target_reviews=0, status_reason=NULL WHERE store_id=1")
        con.commit()
        name = "stores with no target"
        flagged = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        con.execute("UPDATE stores SET status_reason='no data: Google shows 0 reviews' WHERE store_id=1")
        con.commit()
        con.close()
        ruled = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}[name]
        self.assertEqual(1, flagged - ruled)

    def test_out_of_scope_store_moves_to_its_own_file_with_its_reviews(self):
        self.apply()
        self.assertEqual(0, self.one("SELECT COUNT(*) FROM stores WHERE store_id=3"))
        self.assertEqual(0, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=3"))
        self.assertEqual(1, self.one("SELECT COUNT(*) FROM stores WHERE store_id=3", db=self.out))
        self.assertEqual(30, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=3",
                                      db=self.out))

    def test_second_run_changes_nothing(self):
        self.apply()
        before = (self.one("SELECT COUNT(*) FROM reviews"),
                  self.one("SELECT COUNT(*) FROM quarantined_reviews"),
                  self.one("SELECT COUNT(*) FROM reviews", db=self.out))
        self.apply()
        self.assertEqual(before, (self.one("SELECT COUNT(*) FROM reviews"),
                                  self.one("SELECT COUNT(*) FROM quarantined_reviews"),
                                  self.one("SELECT COUNT(*) FROM reviews", db=self.out)))


class TestStoreRulings(TempDBCase):
    def test_each_decision_does_what_it_says(self):
        con = sqlite3.connect(self.master)
        con.execute("ALTER TABLE stores ADD COLUMN feature_id TEXT")
        con.row_factory = sqlite3.Row
        R12.apply_all(con, dewey_types={}, attempts={2: 2})        # store 2 ruled complete
        rows = [{"store_id": "1", "decision": "OUT", "target": "", "reason": "a farm"},
                {"store_id": "90", "decision": "DUP:91", "target": "", "reason": "same"},
                {"store_id": "4", "decision": "TARGET", "target": "60", "reason": "probe"},
                {"store_id": "2", "decision": "UNRULE", "target": "", "reason": "cap 200"},
                {"store_id": "91", "decision": "QUEUE", "target": "", "reason": "x"}]
        R.step_store_rulings(con, rows)
        R12.recompute(con)
        con.commit()
        got = {r[0]: (r[1], r[2]) for r in con.execute(
            "SELECT store_id, scrape_status, target_reviews FROM stores")}
        con.close()
        self.assertEqual("out_of_scope", got[1][0])
        self.assertEqual("duplicate", got[90][0])
        self.assertEqual(60, got[4][1])
        self.assertEqual("unknown_target", got[2][0], "ruling lifted; status from counts")

    def test_complete_decision_rules_the_store_complete_with_its_reason(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R12.ensure_schema(con)
        R.step_store_rulings(con, [{"store_id": "4", "decision": "COMPLETE", "target": "",
                                    "reason": "short by 20; owner rule <=20"}],
                             ruling_date="2026-10-05")
        R12.recompute(con)
        status, reason = con.execute(
            "SELECT scrape_status, status_reason FROM stores WHERE store_id=4").fetchone()
        con.close()
        self.assertEqual("complete", status)
        self.assertIn("2026-10-05", reason)
        self.assertIn("short by 20", reason)

    def test_manual_check_decisions(self):
        """Decisions from the 2026-10-08 manual Google check of the dewey9 failures."""
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R12.ensure_schema(con)
        con.execute("UPDATE stores SET reviews_scraped=10, scrape_status='incomplete' WHERE store_id=1")
        con.execute("UPDATE stores SET reviews_scraped=0, scrape_status='not_started' WHERE store_id=4")
        con.execute("UPDATE stores SET reviews_scraped=0, scrape_status='not_started' WHERE store_id=90")
        con.execute("UPDATE stores SET reviews_scraped=0, scrape_status='not_started' WHERE store_id=91")
        R.step_store_rulings(con, [
            {"store_id": "1", "decision": "CLOSED_PERMANENT", "target": "", "reason": "Google: permanently closed"},
            {"store_id": "4", "decision": "CLOSED_TEMPORARY", "target": "", "reason": "Google: temporarily closed"},
            {"store_id": "90", "decision": "BAD_ID", "target": "", "reason": "place_id opens another store"},
            {"store_id": "91", "decision": "NO_DATA", "target": "", "reason": "Google shows 0 reviews"},
        ], ruling_date="2026-10-08")
        R12.recompute(con)
        got = {r["store_id"]: r for r in con.execute(
            "SELECT store_id, scrape_status, business_status, status_reason FROM stores "
            "WHERE store_id IN (1,4,90,91)")}
        con.close()
        self.assertEqual(("closed", "CLOSED_PERMANENT"), (got[1][1], got[1][2]),
                         "owner ruling 2026-10-08: verified-closed stores leave the dataset")
        self.assertEqual(("closed", "CLOSED_TEMPORARY"), (got[4][1], got[4][2]))
        self.assertIn("2026-10-08", got[4][3])
        self.assertEqual("bad_place_id", got[90][1])
        self.assertEqual("complete", got[91][1], "open with 0 reviews on Google: complete, no data")
        self.assertIn("no data", got[91][3])

    def test_closed_stores_move_to_their_own_file_with_their_reviews(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R12.ensure_schema(con)
        R.step_store_rulings(con, [{"store_id": "1", "decision": "CLOSED_PERMANENT",
                                    "target": "", "reason": "x"}])
        con.commit()
        n_reviews = con.execute("SELECT COUNT(*) FROM reviews WHERE store_id=1").fetchone()[0]
        con.close()
        closed = os.path.join(os.path.dirname(self.master), "closed.db")
        moved = R.split_out_of_scope(self.master, closed, status="closed")
        self.assertEqual(1, moved["stores"])
        self.assertEqual(n_reviews, moved["reviews"])
        self.assertEqual(0, self.one("SELECT COUNT(*) FROM stores WHERE store_id=1"))
        self.assertEqual(1, sqlite3.connect(closed).execute("SELECT COUNT(*) FROM stores").fetchone()[0])

    def test_open_decision_clears_the_closed_flag_and_sets_the_live_target(self):
        """Manual check 2026-10-08: Google shows the store open, so the old closed flag goes
        and the store is measured against the count Google shows."""
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R12.ensure_schema(con)
        con.execute("UPDATE stores SET business_status='CLOSED_TEMPORARILY', target_reviews=10 "
                    "WHERE store_id=1")
        held = con.execute("SELECT reviews_scraped FROM stores WHERE store_id=1").fetchone()[0]
        R.step_store_rulings(con, [{"store_id": "1", "decision": "OPEN", "target": str(held + 40),
                                    "reason": "manual check: open"}], ruling_date="2026-10-08")
        row = con.execute("SELECT business_status, target_reviews, status_reason FROM stores "
                          "WHERE store_id=1").fetchone()
        con.close()
        self.assertEqual("OPERATIONAL", row[0])
        self.assertEqual(held + 40, row[1])
        self.assertIsNone(row[2], "not a ruling: the store is counted like any other")

    def test_unknown_decision_is_refused(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        with self.assertRaises(ValueError):
            R.step_store_rulings(con, [{"store_id": "1", "decision": "MAYBE",
                                        "target": "", "reason": ""}])
        con.close()


class TestValidatorHonoursAcceptedFlags(TempDBCase):
    def test_accepted_far_store_drops_out_of_the_geo_check(self):
        import r00_validate as V
        before = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}
        con = sqlite3.connect(self.master)
        R.accept(con, [4], R.FAR_FROM_ADDRESS, "checked by hand")
        con.commit()
        con.close()
        after = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}
        name = "stores far from their own address"
        self.assertEqual(before[name] - 1, after[name])


if __name__ == "__main__":
    unittest.main()
