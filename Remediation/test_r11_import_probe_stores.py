"""Tests for r11_import_probe_stores.

New stores found by the Google probe have to exist in master before their
reviews can be merged (merge_results maps batch stores to master by place_id
and drops the rest). r11 inserts them, dedupes probe rows that resolved to the
same place, updates targets on stores master already has, and never touches
closed, unverified or place_id-less rows.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import r11_import_probe_stores as r11  # noqa: E402
from test_remediation import MASTER_DDL  # noqa: E402

PROBE_DDL = """
CREATE TABLE probe (
  store_id INTEGER PRIMARY KEY, name TEXT, street TEXT, city TEXT, state TEXT,
  zip TEXT, lat REAL, lon REAL, status TEXT, g_name TEXT, g_category TEXT,
  g_address TEXT, g_phone TEXT, g_rating REAL, g_total_reviews INTEGER,
  g_place_id TEXT, g_feature_id TEXT, g_lat REAL, g_lon REAL, g_closed INTEGER,
  verified INTEGER, name_sim REAL
);
"""

PROBE_COLS = ("store_id", "name", "street", "city", "state", "zip", "lat", "lon", "status",
              "g_name", "g_category", "g_address", "g_phone", "g_rating", "g_total_reviews",
              "g_place_id", "g_feature_id", "g_lat", "g_lon", "g_closed", "verified", "name_sim")


def _probe_row(store_id, name, pid, cat="Grocery store", total=100, closed=0, status="done",
               sim=95.0, zip_="27536.0", fid=None):
    return (store_id, name, "1 Main St", "Henderson", "NC", zip_, 36.3, -78.4, status,
            name, cat, "1 Main St, Henderson, NC 27536 ", "(252) 000-0000 ", 4.2, total,
            pid, fid, 36.31, -78.41, closed, 1, sim)


class TestImport(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.master = os.path.join(self.tmp.name, "master.db")
        self.probe = os.path.join(self.tmp.name, "probe.db")
        m = sqlite3.connect(self.master)
        m.executescript(MASTER_DDL)
        m.execute("ALTER TABLE stores ADD COLUMN qa_flags TEXT")
        m.execute("INSERT INTO stores (store_id, place_id, store_name, target_reviews, "
                  "reviews_scraped, scrape_status, category) VALUES "
                  "(7, 'ChIJexisting000000000000000', 'Old Store', 1500, 40, 'incomplete', NULL)")
        m.commit()
        m.close()
        p = sqlite3.connect(self.probe)
        p.executescript(PROBE_DDL)
        rows = [
            _probe_row(1, "Food Lion", "ChIJnew00000000000000000001", total=755),
            _probe_row(2, "Food Lion dup A", "ChIJnew00000000000000000002", sim=80.0),
            _probe_row(3, "Food Lion dup B", "ChIJnew00000000000000000002", sim=97.0),
            _probe_row(4, "Closed Mart", "ChIJnew00000000000000000004", closed=1),
            _probe_row(5, "Gas Stop", "ChIJnew00000000000000000005", cat="Gas station"),
            _probe_row(6, "No id", None),
            _probe_row(7, "Unverified", "ChIJnew00000000000000000007", status="unverified"),
            _probe_row(8, "Old Store", "ChIJexisting000000000000000", total=2200),
            _probe_row(9, "Huge", "ChIJnew00000000000000000009", total=9000),
            _probe_row(10, "Food Lion by fid", None, total=300, fid="0x88542026E4B8472F:0x1B71FF14F3C30F11"),
            _probe_row(11, "Food Lion by fid dup", None, total=300, sim=50.0,
                       fid="0x88542026e4b8472f:0x1b71ff14f3c30f11"),
        ]
        placeholders = ",".join("?" * len(PROBE_COLS))
        p.executemany(f"INSERT INTO probe ({','.join(PROBE_COLS)}) VALUES ({placeholders})", rows)
        p.commit()
        p.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _stores(self):
        c = sqlite3.connect(self.master)
        c.row_factory = sqlite3.Row
        try:
            return {r["place_id"]: dict(r) for r in c.execute("SELECT * FROM stores")}
        finally:
            c.close()

    def test_dry_run_writes_nothing(self) -> None:
        stats = r11.import_stores(self.master, self.probe, ("Grocery store",), apply=False)
        self.assertEqual(stats["would_insert"], 4)
        self.assertEqual(len(self._stores()), 1)

    def test_inserts_only_open_verified_grocery_rows_with_place_id(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True)
        s = self._stores()
        self.assertIn("ChIJnew00000000000000000001", s)
        self.assertIn("ChIJnew00000000000000000009", s)
        for absent in ("ChIJnew00000000000000000004", "ChIJnew00000000000000000005",
                       "ChIJnew00000000000000000007"):
            self.assertNotIn(absent, s)

    def test_duplicate_place_ids_collapse_to_best_name_match(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True)
        s = self._stores()["ChIJnew00000000000000000002"]
        self.assertEqual(s["store_name"], "Food Lion dup B")

    def test_new_row_fields(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True)
        s = self._stores()["ChIJnew00000000000000000001"]
        self.assertEqual(s["target_reviews"], 755)
        self.assertEqual(s["reviews_scraped"], 0)
        self.assertEqual(s["scrape_status"], "not_started")
        self.assertEqual(s["business_status"], "OPERATIONAL")
        self.assertEqual(s["category"], "Grocery store")
        self.assertEqual(s["zip"], "27536")               # float artefact stripped
        self.assertEqual(s["phone"], "(252) 000-0000")    # trailing space stripped
        self.assertEqual((s["source_lat"], s["source_lon"]), (36.31, -78.41))
        self.assertIn("r11", s["qa_flags"])

    def test_target_is_capped(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True, cap=1500)
        self.assertEqual(self._stores()["ChIJnew00000000000000000009"]["target_reviews"], 1500)

    def test_existing_store_gets_live_target_and_category(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True, cap=1500)
        s = self._stores()["ChIJexisting000000000000000"]
        self.assertEqual(s["target_reviews"], 1500)        # 2200 capped
        self.assertEqual(s["category"], "Grocery store")
        self.assertEqual(s["reviews_scraped"], 40)         # untouched
        self.assertEqual(s["scrape_status"], "incomplete")

    def test_idempotent(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True)
        again = r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True)
        self.assertEqual(again["inserted"], 0)
        self.assertEqual(len(self._stores()), 5)

    def test_feature_id_only_store_is_imported_with_null_place_id(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True)
        c = sqlite3.connect(self.master)
        c.row_factory = sqlite3.Row
        rows = c.execute("SELECT * FROM stores WHERE feature_id IS NOT NULL").fetchall()
        c.close()
        self.assertEqual(len(rows), 1)                       # the two fid rows collapse to one
        self.assertEqual(rows[0]["store_name"], "Food Lion by fid")
        self.assertEqual(rows[0]["place_id"], "0x88542026e4b8472f:0x1b71ff14f3c30f11")
        self.assertEqual(rows[0]["feature_id"], "0x88542026e4b8472f:0x1b71ff14f3c30f11")
        self.assertEqual(rows[0]["target_reviews"], 300)

    def test_store_ids_continue_after_max(self) -> None:
        r11.import_stores(self.master, self.probe, ("Grocery store",), apply=True)
        ids = sorted(v["store_id"] for v in self._stores().values())
        self.assertEqual(ids, [7, 8, 9, 10, 11])


if __name__ == "__main__":
    unittest.main(verbosity=2)
