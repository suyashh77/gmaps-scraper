"""Tests for r12_rulings: scope, duplicates, contamination and no-target rulings.

Run against synthetic masters in a temp directory; nothing touches the real data.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "Scraper_v2"))

import r12_rulings as R                                   # noqa: E402
from test_remediation import TempDBCase, build_batch      # noqa: E402


def store(sid, name="Food Lion", addr="100 Main St, Raleigh, NC 27601", pid=None, fid=None,
          n=0, t=100):
    return {"store_id": sid, "store_name": name, "full_address": addr,
            "place_id": pid or f"ChIJ{sid}", "feature_id": fid,
            "reviews_scraped": n, "target_reviews": t}


class TestPureRules(unittest.TestCase):
    def test_gas_and_convenience_are_out_of_scope_by_google_category(self):
        self.assertTrue(R.is_out_of_scope("Gas station", None))
        self.assertTrue(R.is_out_of_scope("Convenience store", "supermarket"))
        self.assertFalse(R.is_out_of_scope("Grocery store", "gas_convenience"))

    def test_dewey_type_decides_only_when_google_gave_no_category(self):
        self.assertTrue(R.is_out_of_scope(None, "gas_convenience"))
        self.assertFalse(R.is_out_of_scope(None, "supermarket"))

    def test_same_feature_id_is_the_same_store(self):
        a = store(1, fid="0xabc:0x1")
        b = store(2, pid="0xabc:0x1", addr="elsewhere")
        self.assertTrue(R.same_store(a, b))

    def test_same_brand_house_number_and_zip_is_the_same_store(self):
        a = store(1, "Walmart Supercenter", "197 Plz Dr Ext, Forest City, NC 28043")
        b = store(2, "Walmart Supercenter", "197 Plaza Dr, Forest City, NC, 28043")
        a.update(source_lat=35.3330, source_lon=-81.8650)
        b.update(source_lat=35.3331, source_lon=-81.8652)
        self.assertTrue(R.same_store(a, b))

    def test_same_number_and_zip_on_different_streets_far_apart_is_not_the_same_store(self):
        """7-Eleven 4800 Prosperity Church Rd vs 4800 N Graham St, both 28269."""
        a = store(1, "7-Eleven", "4800 Prosperity Church Rd, Charlotte, NC 28269")
        b = store(2, "7-Eleven", "4800 N Graham St, Charlotte, NC 28269")
        a.update(source_lat=35.3735, source_lon=-80.8170)
        b.update(source_lat=35.3115, source_lon=-80.7980)
        self.assertFalse(R.same_store(a, b))
        b.update(source_lat=35.3736, source_lon=-80.8171)
        self.assertTrue(R.same_store(a, b), "same pin: one store, address spelt two ways")

    def test_identical_address_is_the_same_store_even_with_a_stray_pin(self):
        a = store(1, "Harris Teeter", "3050 Traemoor Village Dr, Fayetteville, NC 28306")
        b = store(2, "Harris Teeter", "3050 Traemoor Village Dr, Fayetteville, NC 28306")
        a.update(source_lat=35.00, source_lon=-79.00)
        b.update(source_lat=35.10, source_lon=-79.10)
        self.assertTrue(R.same_store(a, b))

    def test_different_brand_at_same_address_is_not_the_same_store(self):
        a = store(1, "Whole Foods Market")
        b = store(2, "Food Lion")
        self.assertFalse(R.same_store(a, b))

    def test_keeper_prefers_a_chij_id_then_more_reviews(self):
        hexy = store(1, pid="0xabc:0x1", n=1500)
        chij = store(2, n=10)
        self.assertEqual(2, R.pick_keeper([hexy, chij]))
        self.assertEqual(4, R.pick_keeper([store(3, n=5), store(4, n=9)]))

    def test_wrong_side_is_the_store_holding_far_more_than_its_target(self):
        good = store(7000, n=1000, t=1000)
        bad = store(7001, n=1000, t=29)
        self.assertEqual(7001, R.wrong_side(good, bad))
        self.assertEqual(7001, R.wrong_side(good, store(7001, n=900, t=None)))

    def test_wrong_side_falls_back_to_the_old_pipeline_store(self):
        old = store(4000, n=1000, t=1000)
        new = store(8000, n=1000, t=1000)
        self.assertEqual(4000, R.wrong_side(old, new))

    def test_two_consistent_stores_of_the_same_era_are_ambiguous(self):
        self.assertIsNone(R.wrong_side(store(10, n=5, t=5), store(11, n=5, t=5)))

    def test_name_decides_when_shared_reviews_name_one_brand(self):
        """Owner ruling 2026-10-08: when only a place-id problem is in the way, go by name and
        address. A Food Lion holding reviews that say 'Whole Foods' collected Whole Foods' page."""
        whole = store(1, "Whole Foods Market", "621 Broad St, Durham, NC 27705")
        lion = store(2, "Food Lion", "5279 N Roxboro Rd, Durham, NC 27712")
        texts = ["Best Whole Foods in town", "whole foods salad bar is great", "Love this store"]
        self.assertEqual(2, R.name_wrong_side(whole, lion, texts))

    def test_verified_address_decides_between_same_brand_stores(self):
        """Two Aldis on one page: the one with no Outscraper-verified address was name-searched
        onto the other's page."""
        verified = store(1, "ALDI", "5201 Ramsey St, Fayetteville, NC 28311")
        verified["outscraper_address"] = "5201 Ramsey St, Fayetteville, NC 28311"
        unverified = store(2, "Aldi", "3447 Ramsey St, Fayetteville, NC 28311")
        unverified["outscraper_address"] = ""
        self.assertEqual(2, R.name_wrong_side(verified, unverified, ["Good prices at Aldi"]))

    def test_no_name_signal_leaves_the_pair_for_a_person(self):
        a = store(1, "Dollar Tree", "1 A St, Cary, NC 27511")
        b = store(2, "Dollar Tree", "2 B St, Apex, NC 27502")
        a["outscraper_address"] = b["outscraper_address"] = "x"
        self.assertIsNone(R.name_wrong_side(a, b, ["cheap stuff"]))

    def test_place_id_era_sources(self):
        self.assertTrue(R.is_place_id_era("run2_sl9mw2_20260916_084616.db"))
        self.assertTrue(R.is_place_id_era("grocery1_48b03a_20260923.db"))
        self.assertFalse(R.is_place_id_era("batch_04.db"))


class TestApply(TempDBCase):
    """Fixture: store 1 Food Lion (ChIJ), store 3 Corner Mart (Convenience store),
    store 4 shares 25 text reviews with store 1 (both old pipeline, consistent)."""

    def setUp(self):
        super().setUp()
        con = sqlite3.connect(self.master)
        con.execute("ALTER TABLE stores ADD COLUMN feature_id TEXT")   # real master has it
        # store 50: same Food Lion under its hex feature id, holding one review the
        # ChIJ copy lacks, one it already has, and one rating-only row
        con.execute("INSERT INTO stores (store_id, place_id, store_name, city, state, zip, "
                    "full_address, target_reviews, scrape_status, category) VALUES "
                    "(50,'0xfeed:0x1','Food Lion','Raleigh','NC','27601',"
                    "'100 Main St, Raleigh, NC 27601',100,'incomplete','Grocery store')")
        rows = [(50, "Bella", 4, "Fine.", "grocery2_x.db"),
                (50, "Nova", 5, "Only on the hex copy.", "grocery2_x.db"),
                (50, "Rae", 3, "", "grocery2_x.db")]
        # store 60: old-pipeline Family Dollar that collected store 61's page in April
        con.execute("INSERT INTO stores (store_id, place_id, store_name, city, state, zip, "
                    "full_address, target_reviews, category) VALUES "
                    "(60,'ChIJsixty','Family Dollar','Cameron','NC','28326',"
                    "'1 A St, Cameron, NC 28326',5,'Dollar store'),"
                    "(61,'ChIJsixtyone','Piggly Wiggly','Cameron','NC','28326',"
                    "'9 B St, Cameron, NC 28326',30,'Grocery store'),"
                    "(70,'ChIJseventy','Mystery Mart','Raleigh','NC','27601',"
                    "'5 C St, Raleigh, NC 27601',NULL,'Grocery store')")
        for i in range(25):
            text = f"Piggly page review {i} with its own words."
            rows.append((61, f"P{i}", 5, text, "grocery1_y.db"))
            rows.append((60, f"P{i}", 5, text, "batch_04.db"))
        rows.append((60, "Own", 4, "A genuine Family Dollar review.", "run1_z.db"))
        con.executemany("INSERT INTO reviews (store_id, reviewer_name, rating, review_text, "
                        "source_db) VALUES (?,?,?,?,?)", rows)
        con.execute("UPDATE stores SET reviews_scraped = (SELECT COUNT(*) FROM reviews r "
                    "WHERE r.store_id = stores.store_id)")
        con.commit()
        con.close()

    def apply(self, attempts=None):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        out = R.apply_all(con, dewey_types={}, attempts=attempts or {70: 2})
        con.commit()
        con.close()
        return out

    def test_convenience_store_is_marked_out_of_scope_and_keeps_its_row(self):
        self.apply()
        row = self.rows("SELECT scrape_status, status_reason FROM stores WHERE store_id=3")[0]
        self.assertEqual("out_of_scope", row[0])
        self.assertIn("Convenience store", row[1])

    def test_duplicate_copy_hands_unique_text_reviews_to_the_keeper(self):
        self.apply()
        self.assertEqual("duplicate", self.one("SELECT scrape_status FROM stores WHERE store_id=50"))
        self.assertEqual(0, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=50"))
        self.assertEqual(1, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=1 "
                                     "AND reviewer_name='Nova'"))
        self.assertEqual(1, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=1 "
                                     "AND reviewer_name='Bella'"))
        self.assertEqual(2, self.one("SELECT COUNT(*) FROM quarantined_reviews "
                                     "WHERE store_id=50"), "copy and rating-only row are kept aside")

    def test_contaminated_store_loses_only_the_reviews_from_the_bad_batch(self):
        out = self.apply()
        self.assertIn(60, out["requeue"])
        left = self.rows("SELECT reviewer_name FROM reviews WHERE store_id=60")
        self.assertEqual(["Own"], [r[0] for r in left])
        self.assertEqual(25, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=61"))

    def test_a_store_ruled_complete_is_still_settled_when_it_holds_a_shared_page(self):
        """The stall rule marks stores complete with a status_reason; that must not hide
        a store whose reviews are another store's page (2026-10-08: 20 pairs were skipped)."""
        con = sqlite3.connect(self.master)
        con.execute("ALTER TABLE stores ADD COLUMN status_reason TEXT")
        con.execute("UPDATE stores SET scrape_status='complete', "
                    "status_reason='stalled 2 short of 5; ruled complete' WHERE store_id=60")
        con.commit()
        con.close()
        out = self.apply()
        self.assertIn(60, out["requeue"])
        self.assertEqual(["Own"], [r[0] for r in self.rows(
            "SELECT reviewer_name FROM reviews WHERE store_id=60")])
        self.assertEqual("incomplete", self.one("SELECT scrape_status FROM stores WHERE store_id=60"),
                         "its count is real again, so it is counted again")

    def test_ambiguous_pairs_are_listed_not_touched(self):
        out = self.apply()
        self.assertTrue(any({1, 4} == {p["store_a"], p["store_b"]} for p in out["ambiguous"]))
        self.assertEqual(25, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=4"))

    def test_a_person_can_name_the_store_that_does_not_own_an_ambiguous_page(self):
        self.apply()
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        out = R.apply_pair_rulings(con, [{"store_a": "1", "store_b": "4", "wrong_store": "4"}])
        R.recompute(con)
        con.commit()
        con.close()
        self.assertEqual(1, len(out["requeue"]) + len(out["bad_place_id"]))
        self.assertEqual(0, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=4"),
                         "the shared page leaves the store that did not own it")
        self.assertGreater(self.one("SELECT COUNT(*) FROM reviews WHERE store_id=1"), 0)

    def test_a_pair_ruling_naming_a_third_store_is_refused(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        with self.assertRaises(ValueError):
            R.apply_pair_rulings(con, [{"store_a": "1", "store_b": "4", "wrong_store": "60"}])
        con.close()

    REAL_URL = ("https://www.google.com/maps/place/Family+Dollar/@35.4,-83.4,17z/data=!3m1!4b1!4m6!3m5"
                "!1s0x885948bd848bef67:0x792df7a71c68da4c!8m2!3d35.4!4d-83.4")

    def _answer(self, **kw):
        row = {"store_id": "60", "opens_own": "NO", "real_exists": "YES", "real_url": self.REAL_URL,
               "real_address": "7 Real St, Cameron, NC 28326", "real_count": "40", "partners": [61]}
        row.update(kw)
        return row

    def test_a_store_whose_saved_page_is_another_stores_loses_all_its_reviews_and_is_repointed(self):
        """Manual check 2026-10-08: the stored page belongs to a third store, so the shared reviews
        are removed from BOTH stores, and the wrong store is re-pointed at its real page."""
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R.apply_page_owner_answers(con, [self._answer()], ruling_date="2026-10-08")
        R.recompute(con)
        con.commit()
        self.assertEqual(0, con.execute("SELECT COUNT(*) FROM reviews WHERE store_id=60").fetchone()[0])
        self.assertEqual(0, con.execute("SELECT COUNT(*) FROM reviews WHERE store_id=61").fetchone()[0],
                         "the partner held the same page's reviews")
        self.assertEqual(26, con.execute("SELECT COUNT(*) FROM quarantined_reviews WHERE store_id=60").fetchone()[0])
        self.assertEqual(25, con.execute("SELECT COUNT(*) FROM quarantined_reviews WHERE store_id=61").fetchone()[0])
        row = con.execute("SELECT place_id, feature_id, full_address, target_reviews, scrape_status, "
                          "status_reason, business_status FROM stores WHERE store_id=60").fetchone()
        con.close()
        fid = "0x885948bd848bef67:0x792df7a71c68da4c"
        self.assertEqual((fid, fid), (row[0], row[1]), "navigable by the cid in the second half")
        self.assertEqual(("7 Real St, Cameron, NC 28326", 40, "not_started", None, "OPERATIONAL"),
                         (row[2], row[3], row[4], row[5], row[6]))

    def test_a_partner_is_only_trimmed_of_the_shared_reviews(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R.ensure_schema(con)
        con.execute("INSERT INTO reviews (store_id, reviewer_name, rating, review_text, source_db) "
                    "VALUES (61, 'Mine', 5, 'Only this store has this one.', 'run1_z.db')")
        R.apply_page_owner_answers(con, [self._answer()], ruling_date="2026-10-08")
        left = [r[0] for r in con.execute("SELECT reviewer_name FROM reviews WHERE store_id=61")]
        con.close()
        self.assertEqual(["Mine"], left)

    def test_a_store_that_opens_its_own_page_is_left_alone(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        before = con.execute("SELECT COUNT(*) FROM reviews WHERE store_id=61").fetchone()[0]
        R.apply_page_owner_answers(con, [self._answer(store_id="61", opens_own="YES", partners=[60])])
        after = con.execute("SELECT COUNT(*) FROM reviews WHERE store_id=61").fetchone()[0]
        con.close()
        self.assertEqual(before, after)

    def test_when_the_real_store_is_closed_or_missing_the_wrong_store_leaves_the_dataset(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R.apply_page_owner_answers(con, [self._answer(real_exists="CLOSED", real_url="", partners=[])],
                                   ruling_date="2026-10-08")
        status = con.execute("SELECT scrape_status, business_status FROM stores WHERE store_id=60").fetchone()
        R.apply_page_owner_answers(con, [self._answer(store_id="61", real_exists="NO", real_url="", partners=[])])
        gone = con.execute("SELECT scrape_status FROM stores WHERE store_id=61").fetchone()[0]
        con.close()
        self.assertEqual(("closed", "CLOSED"), tuple(status))
        self.assertEqual("out_of_scope", gone)

    def test_a_real_page_already_held_by_another_store_is_refused(self):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        R.ensure_schema(con)
        con.execute("UPDATE stores SET feature_id='0x885948bd848bef67:0x792df7a71c68da4c' WHERE store_id=61")
        with self.assertRaises(ValueError):
            R.apply_page_owner_answers(con, [self._answer(partners=[])])
        con.close()

    def test_no_target_store_attempted_twice_is_ruled_complete(self):
        self.apply()
        self.assertEqual("complete", self.one("SELECT scrape_status FROM stores WHERE store_id=70"))
        self.assertEqual("unknown_target",
                         self.one("SELECT scrape_status FROM stores WHERE store_id=2"))

    def test_second_run_changes_nothing(self):
        self.apply()
        before = self.rows("SELECT store_id, scrape_status, reviews_scraped FROM stores")
        q = self.one("SELECT COUNT(*) FROM quarantined_reviews")
        self.apply()
        self.assertEqual([tuple(r) for r in before], [tuple(r) for r in self.rows(
            "SELECT store_id, scrape_status, reviews_scraped FROM stores")])
        self.assertEqual(q, self.one("SELECT COUNT(*) FROM quarantined_reviews"))

    def test_validator_measures_in_scope_stores_and_accepts_ruled_completes(self):
        import r00_validate as V
        self.apply()
        summary = V.run(self.master, verbose=False)
        checks = {c["name"]: c["value"] for c in summary["checks"]}
        in_scope = self.one("SELECT COUNT(*) FROM stores WHERE scrape_status NOT IN "
                            "('out_of_scope','duplicate','bad_place_id')")
        self.assertEqual(in_scope, summary["counts"]["stores"])
        self.assertEqual(0, checks["marked complete while holding zero reviews"],
                         "store 70 was ruled complete on purpose")

    def test_a_quarantined_review_is_not_merged_back_in(self):
        self.apply()
        batch = os.path.join(self.tmp, "old.db")
        build_batch(batch, ["ChIJsixty"])
        con = sqlite3.connect(batch)
        con.execute("INSERT INTO reviews (store_id, reviewer_name, rating, review_text) "
                    "VALUES (1, 'P0', 5, 'Piggly page review 0 with its own words.')")
        con.commit()
        con.close()
        from linux_scraper import merge_results as M
        con = M._connect(self.master)
        M.merge_one(con, batch, dry_run=False)
        con.close()
        self.assertEqual(0, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=60 "
                                     "AND reviewer_name='P0'"))

    def test_validator_still_sees_orphan_reviews(self):
        import r00_validate as V
        self.apply()
        con = sqlite3.connect(self.master)
        con.execute("INSERT INTO reviews (store_id, reviewer_name, rating) VALUES (99999,'x',5)")
        con.commit()
        con.close()
        checks = {c["name"]: c["value"] for c in V.run(self.master, verbose=False)["checks"]}
        self.assertEqual(1, checks["orphan reviews"])

    def test_rulings_survive_a_merge_and_r03(self):
        self.apply()
        batch = os.path.join(self.tmp, "b.db")
        build_batch(batch, ["ChIJaaaaaaaaaaaaaaaaaaaaaa3", "ChIJseventy"])
        from linux_scraper import merge_results as M
        con = M._connect(self.master)
        M.merge_one(con, batch, dry_run=False)
        con.close()
        self.run_tool("r03_clean_deterministic.py", "--apply")
        self.assertEqual("out_of_scope", self.one("SELECT scrape_status FROM stores WHERE store_id=3"))
        self.assertEqual("complete", self.one("SELECT scrape_status FROM stores WHERE store_id=70"))


if __name__ == "__main__":
    unittest.main()
