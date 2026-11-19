"""Tests for s19_grocery_db: the POI-keyed grocery database, the review sitrep, the
scrape batches for untouched POIs, and merging returned batches back.

What a wrong answer would cost: reviews attached to the wrong POI, a store scraped twice,
or a sitrep that under-counts the work left. Each is tested on a tiny synthetic set.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import s19_grocery_db as s19  # noqa: E402

LAT, LON = 35.78, -78.64


def _final() -> pd.DataFrame:
    """Two Food Lions we hold, one new Food Lion, a Carlie C's we hold with its IGA twin,
    a new Dollar General, and a Family Dollar sharing an address with a Food Lion."""
    rows = [
        # placekey, name, street, lat, lon, our_store_id, target, status
        ("fl1", "Food Lion", "1 Main St", LAT, LON, 10, 3, "complete"),
        ("fl2", "Food Lion", "9 Oak Ave", LAT + .1, LON, 11, 1000, "incomplete"),
        ("fl3", "Food Lion", "5 Elm St", LAT + .2, LON, None, None, "not in our DB"),
        ("cc", "Carlie C's", "333 N Raleigh St", LAT + .3, LON, 12, 300, "incomplete"),
        ("iga", "IGA", "333 N Raleigh St", LAT + .3 + .0002, LON, None, None, "not in our DB"),
        ("dg", "Dollar General", "7 Pine Rd", LAT + .4, LON, None, None, "not in our DB"),
        ("fd", "Family Dollar Stores", "1 Main St", LAT + .0002, LON, None, None, "not in our DB"),
    ]
    df = pd.DataFrame(rows, columns=["placekey", "location_name", "street_address", "latitude", "longitude",
                                     "our_store_id", "our_target_reviews", "review_status"])
    return df.assign(brand_name="(no brand)", top_category="Grocery Stores", sub_category="Supermarkets",
                     naics_code="445110", city="Raleigh", postal_code="27601", phone_number=None, website=None,
                     is_closed=0, our_place_id=[f"ChIJ{i}" if s else None for i, s in enumerate(df.our_store_id)],
                     match_grade=["A" if s else None for s in df.our_store_id])


def _master(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE stores (store_id INTEGER PRIMARY KEY, place_id TEXT, feature_id TEXT, store_name TEXT,
                             target_reviews INTEGER, reviews_scraped INTEGER, scrape_status TEXT);
        CREATE TABLE reviews (id INTEGER PRIMARY KEY, store_id INTEGER, reviewer_name TEXT, rating INTEGER,
                              date_relative TEXT, review_text TEXT, helpful_count INTEGER, photo_count INTEGER,
                              has_owner_response INTEGER, reviewer_review_count INTEGER,
                              reviewer_photo_count INTEGER, is_local_guide INTEGER, service_type TEXT,
                              scraped_at TEXT, source_db TEXT, review_id TEXT, machine_id TEXT);
    """)
    con.executemany("INSERT INTO stores VALUES (?,?,?,?,?,?,?)", [
        (10, "ChIJ0", None, "Food Lion", 400, 400, "complete"),
        (11, "ChIJ1", None, "Food Lion", 1000, 2, "incomplete"),
        (12, "ChIJ3", None, "Carlie C's IGA", 300, 1, "incomplete"),
        (99, "ChIJx", None, "Shell", 50, 50, "complete"),            # not in the final list
    ])
    reviews = [(10, f"a{i}") for i in range(3)] + [(11, "b0"), (11, "b1"), (12, "c0"), (99, "z0")]
    con.executemany("INSERT INTO reviews (store_id, reviewer_name, rating, review_text, review_id) "
                    "VALUES (?,?,5,'ok',?)", [(s, "r", rid) for s, rid in reviews])
    con.commit()
    con.close()


class OutOfScope(unittest.TestCase):
    def test_every_master_store_not_in_the_list_gets_one_reason(self):
        master = pd.DataFrame({"store_id": [1, 2, 3, 4, 5], "store_name": ["Food Lion", "Shell", "Joe's", "Old FL", "Nowhere"],
                               "reviews_in_db": [10, 20, 30, 40, 50]})
        catalog = pd.DataFrame({
            "placekey": ["a", "b", "c", "d"], "our_store_id": [1, 2, 3, 4],
            "location_name": ["Food Lion", "Shell", "Joe's", "Food Lion"], "top_category": ["Grocery Stores",
                              "Gasoline Stations", "Offices of Physicians", "Grocery Stores"],
            "sub_category": ["Supermarkets", "Gas", "Doctors", "Supermarkets"], "is_closed": [0, 0, 0, 1]})
        decided = {"a": s19.IN_LIST, "b": "chain ruled out", "c": "not food retail", "d": s19.IN_LIST}
        out = s19.out_of_scope(master, catalog, decided).set_index("store_id")
        self.assertNotIn(1, out.index)                                      # in the list
        self.assertEqual(out.loc[2, "reason"], "chain ruled out")
        self.assertEqual(out.loc[3, "reason"], "not food retail")
        self.assertEqual(out.loc[4, "reason"], s19.REASON_CLOSED)             # in scope but closed
        self.assertEqual(out.loc[5, "reason"], s19.REASON_UNMATCHED)
        self.assertEqual(int(out.reviews_in_db.sum()), 140)


class Duplicates(unittest.TestCase):
    def test_iga_twin_of_a_held_store_is_flagged(self):
        dup = s19.probable_duplicates(_final())
        self.assertEqual(dup, {"iga": "cc"})

    def test_different_stores_in_one_plaza_are_not_duplicates(self):
        self.assertNotIn("fd", s19.probable_duplicates(_final()))


class Targets(unittest.TestCase):
    def test_known_target_is_capped_and_unknown_uses_chain_median(self):
        t = s19.estimate_targets(_final().assign(our_target_reviews=[400, 3000, None, 300, None, None, None]))
        self.assertEqual(t.tolist()[:3], [400, s19.REVIEW_CAP, (400 + s19.REVIEW_CAP) / 2])

    def test_no_chain_history_falls_back_to_default(self):
        t = s19.estimate_targets(_final())
        self.assertEqual(t[_final().placekey == "dg"].iloc[0], s19.DEFAULT_TARGET)


class BuildDb(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        _master(self.tmp / "master.db")
        self.db = self.tmp / "grocery.db"
        s19.build_db(_final(), self.tmp / "master.db", self.db)
        self.con = sqlite3.connect(self.db)

    def tearDown(self):
        self.con.close()

    def test_one_row_per_poi_with_link_to_old_store(self):
        rows = dict(self.con.execute("SELECT placekey, master_store_id FROM pois").fetchall())
        self.assertEqual(len(rows), 7)
        self.assertEqual(rows["fl1"], 10)
        self.assertIsNone(rows["fl3"])

    def test_reviews_follow_their_store_to_the_poi_and_nothing_else_comes(self):
        got = dict(self.con.execute("SELECT placekey, COUNT(*) FROM reviews GROUP BY placekey").fetchall())
        self.assertEqual(got, {"fl1": 3, "fl2": 2, "cc": 1})       # Shell's review stays behind

    def test_status_and_need(self):
        q = "SELECT placekey, status, reviews_held, reviews_needed, scrape_hold FROM pois"
        rows = {r[0]: r[1:] for r in self.con.execute(q)}
        self.assertEqual(rows["fl1"][:3], (s19.ST_COMPLETE, 3, 0))
        self.assertEqual(rows["fl2"][:3], (s19.ST_INCOMPLETE, 2, 973))     # target 1000, done at 975
        self.assertEqual(rows["fl3"][0], s19.ST_NEW)
        self.assertEqual(rows["iga"][3], "probable duplicate of cc")

    def test_within_tolerance_of_target_counts_complete(self):
        self.con.execute("UPDATE pois SET target_reviews = 5 WHERE placekey = 'fl2'")      # holds 2 of 5
        self.con.execute("UPDATE pois SET target_reviews = 26 WHERE placekey = 'cc'")      # holds 1 of 26
        s19._refresh(self.con)
        rows = dict(self.con.execute("SELECT placekey, status FROM pois WHERE placekey IN ('fl2','cc')"))
        self.assertEqual(rows, {"fl2": s19.ST_COMPLETE, "cc": s19.ST_INCOMPLETE})

    def test_master_is_not_modified(self):
        m = sqlite3.connect(self.tmp / "master.db")
        self.assertEqual(m.execute("SELECT COUNT(*) FROM reviews").fetchone()[0], 7)
        m.close()


class Batches(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        _master(self.tmp / "master.db")
        self.db = self.tmp / "grocery.db"
        s19.build_db(_final(), self.tmp / "master.db", self.db)

    def test_two_disjoint_batches_of_untouched_pois_only(self):
        paths = s19.write_batches(self.db, self.tmp / "new", 2)
        keys = []
        for p in paths:
            c = sqlite3.connect(p)
            keys.append({r[0] for r in c.execute("SELECT dewey_placekey FROM stores")})
            c.close()
        self.assertEqual(len(paths), 2)
        self.assertFalse(keys[0] & keys[1])
        self.assertEqual(keys[0] | keys[1], {"fl3", "dg", "fd"})    # not the IGA twin, nothing held

    def test_merge_brings_reviews_back_by_placekey_once(self):
        paths = s19.write_batches(self.db, self.tmp / "new", 1)
        b = sqlite3.connect(paths[0])
        sid = b.execute("SELECT store_id FROM stores WHERE dewey_placekey='fl3'").fetchone()[0]
        b.executemany("INSERT INTO reviews (store_id, review_id, reviewer_name, rating, review_text) "
                      "VALUES (?,?,?,?,?)", [(sid, "n1", "x", 4, "fine"), (sid, "n2", "y", 3, "meh")])
        b.execute("UPDATE stores SET status='completed', google_place_id='ChIJnew', gmaps_reviews_count=2 "
                  "WHERE store_id=?", (sid,))
        b.commit()
        b.close()
        first = s19.merge_batch(paths[0], self.db)
        again = s19.merge_batch(paths[0], self.db)
        self.assertEqual((first["new_reviews"], again["new_reviews"]), (2, 0))
        c = sqlite3.connect(self.db)
        row = c.execute("SELECT google_place_id, reviews_held, status FROM pois WHERE placekey='fl3'").fetchone()
        c.close()
        self.assertEqual(row, ("ChIJnew", 2, s19.ST_COMPLETE))
