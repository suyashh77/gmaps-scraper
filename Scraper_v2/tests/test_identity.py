"""Stores identified by Google's feature id, not only by ChIJ place_id.

665 probed grocery stores (Food Lion, Ingles, Harris Teeter...) came back with
a hex feature id (`0x...:0x...`) but no ChIJ id. Google Maps navigates to a
feature id directly (`maps?cid=<decimal>`), so they are scrapable by identity.
For that to survive the round trip the batch carries the master store_id, the
scraper writes the ids it lands on back into the batch, and the merge maps by
master_store_id first and backfills master's place_id.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from linux_scraper import merge_results  # noqa: E402
from linux_scraper.database import make_db  # noqa: E402
from linux_scraper.scraper import feature_id_to_cid_url  # noqa: E402

FID = "0x88542026e4b8472f:0x1b71ff14f3c30f11"
CHIJ = "ChIJlandedOnThisPage000000"


class TestCidUrl(unittest.TestCase):
    def test_hex_feature_id_becomes_decimal_cid_url(self) -> None:
        self.assertEqual(feature_id_to_cid_url(FID),
                         "https://www.google.com/maps?cid=" + str(int("0x1b71ff14f3c30f11", 16)))

    def test_garbage_is_rejected(self) -> None:
        for bad in ("", None, "ChIJabc", "0xzz:0x1", "0x1", "0x1:0x2:0x3"):
            self.assertIsNone(feature_id_to_cid_url(bad))


class TestBatchIdentityColumns(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "b.db")
        self.db = make_db(self.path)
        self.db.connect()
        self.db.init_schema()
        self.db.conn.execute(
            "INSERT INTO stores (store_id, input_name, google_feature_id, master_store_id, status) "
            "VALUES (1, 'Food Lion', ?, 4242, 'pending')", (FID,))
        self.db.conn.execute(
            "INSERT INTO stores (store_id, input_name, google_place_id, status) "
            "VALUES (2, 'Other', 'ChIJalreadyTaken0000000000', 'pending')")
        self.db.conn.commit()

    def tearDown(self) -> None:
        self.db.close()
        self.tmp.cleanup()

    def _row(self, sid):
        r = self.db.conn.execute(
            "SELECT google_place_id, google_feature_id FROM stores WHERE store_id=?",
            (sid,)).fetchone()
        return (r["google_place_id"], r["google_feature_id"])

    def test_schema_has_feature_and_master_columns(self) -> None:
        cols = {r["name"] for r in self.db.conn.execute("PRAGMA table_info(stores)")}
        self.assertIn("google_feature_id", cols)
        self.assertIn("master_store_id", cols)

    def test_old_batch_is_migrated(self) -> None:
        old = os.path.join(self.tmp.name, "old.db")
        first = make_db(old)
        first.connect()
        first.init_schema()
        first.conn.execute("ALTER TABLE stores DROP COLUMN google_feature_id")
        first.conn.execute("ALTER TABLE stores DROP COLUMN master_store_id")
        first.conn.commit()
        first.close()
        db = make_db(old)
        db.connect()
        db.init_schema()
        cols = {r["name"] for r in db.conn.execute("PRAGMA table_info(stores)")}
        db.close()
        self.assertTrue({"google_feature_id", "master_store_id"} <= cols)

    def test_writeback_fills_empty_ids_only(self) -> None:
        self.db.update_store_identity(1, place_id=CHIJ, feature_id="0xshouldnotoverwrite:0x1")
        self.assertEqual(tuple(self._row(1)), (CHIJ, FID))

    def test_writeback_tolerates_place_id_taken_by_another_row(self) -> None:
        self.db.update_store_identity(1, place_id="ChIJalreadyTaken0000000000", feature_id=None)
        self.assertEqual(tuple(self._row(1)), (None, FID))


MASTER_DDL = """
CREATE TABLE stores (store_id INTEGER PRIMARY KEY, place_id TEXT UNIQUE, feature_id TEXT,
  store_name TEXT, target_reviews INTEGER, reviews_scraped INTEGER DEFAULT 0, scrape_status TEXT);
CREATE TABLE reviews (id INTEGER PRIMARY KEY AUTOINCREMENT, store_id INTEGER, reviewer_name TEXT,
  rating INTEGER, date_relative TEXT, review_text TEXT, helpful_count INTEGER, photo_count INTEGER,
  has_owner_response INTEGER, reviewer_review_count INTEGER, reviewer_photo_count INTEGER,
  is_local_guide INTEGER, service_type TEXT, scraped_at TEXT, source_db TEXT, review_id TEXT,
  machine_id TEXT);
"""


class TestMergeByMasterStoreId(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.master = os.path.join(self.tmp.name, "master.db")
        m = sqlite3.connect(self.master)
        m.executescript(MASTER_DDL)
        m.executemany("INSERT INTO stores (store_id, place_id, feature_id, store_name, target_reviews) "
                      "VALUES (?,?,?,?,?)", [
                          (4242, FID, FID, "Food Lion", 100),   # feature id as placeholder place_id
                          (7, "ChIJalreadyTaken0000000000", None, "Other", 100),
                      ])
        m.commit()
        m.close()
        self.batch = os.path.join(self.tmp.name, "b.db")
        db = make_db(self.batch)
        db.connect()
        db.init_schema()
        db.conn.execute(
            "INSERT INTO stores (store_id, input_name, google_place_id, google_feature_id, "
            "master_store_id, status) VALUES (1, 'Food Lion', ?, ?, 4242, 'completed')", (CHIJ, FID))
        db.conn.execute(
            "INSERT INTO reviews (store_id, machine_id, review_id, reviewer_name, rating, "
            "review_text, scraped_at) VALUES (1, 'm', 'r1', 'Ann', 5, 'great', '2026-09-16')")
        db.conn.commit()
        db.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_reviews_land_on_master_store_without_place_id(self) -> None:
        mc = merge_results._connect(self.master)
        stats = merge_results.merge_one(mc, self.batch)
        mc.close()
        self.assertEqual(stats["stores_matched"], 1)
        self.assertEqual(stats["reviews_new"], 1)
        c = sqlite3.connect(self.master)
        self.assertEqual(c.execute("SELECT store_id FROM reviews").fetchone()[0], 4242)
        # the ChIJ id the scraper landed on is backfilled into master
        self.assertEqual(c.execute("SELECT place_id FROM stores WHERE store_id=4242").fetchone()[0], CHIJ)
        c.close()

    def test_backfill_never_steals_a_place_id_from_another_store(self) -> None:
        b = sqlite3.connect(self.batch)
        b.execute("UPDATE stores SET google_place_id='ChIJalreadyTaken0000000000' WHERE store_id=1")
        b.commit()
        b.close()
        mc = merge_results._connect(self.master)
        stats = merge_results.merge_one(mc, self.batch)
        mc.close()
        self.assertEqual(stats["reviews_new"], 1)
        c = sqlite3.connect(self.master)
        self.assertEqual(c.execute("SELECT place_id FROM stores WHERE store_id=4242").fetchone()[0], FID)
        c.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
