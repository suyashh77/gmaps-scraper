"""r11 can select probe rows by identity, not only by Google category.

The Dewey work needs exactly the stores a Dewey POI matched — a set that cuts across
Google categories and is not expressible as a category list. identities_from_csv reads
that set out of the scrape list s16 writes; _probe_rows filters on it.
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

PROBE_COLUMNS = ("store_id INTEGER, name TEXT, status TEXT, g_closed INTEGER, "
                 "g_place_id TEXT, g_feature_id TEXT, g_category TEXT, g_total_reviews INTEGER, "
                 "g_name TEXT, street TEXT, city TEXT, state TEXT, zip TEXT, "
                 "lat REAL, lon REAL, g_address TEXT, g_lat REAL, g_lon REAL, name_sim REAL")

ROWS = [
    (1, "Alpha", "done", 0, "ChIJ_alpha", None, "Grocery store", 500, "Alpha", "1 A St",
     "Apex", "NC", "27502", 35.7, -78.8, "1 A St", 35.7, -78.8, 0.99),
    (2, "Beta", "done", 0, "ChIJ_beta", None, "Gas station", 60, "Beta", "2 B St",
     "Cary", "NC", "27511", 35.8, -78.7, "2 B St", 35.8, -78.7, 0.98),
    (3, "Gamma", "done", 0, None, "0x885a:0xfeed", "Seafood market", 40, "Gamma", "3 C St",
     "Durham", "NC", "27701", 36.0, -78.9, "3 C St", 36.0, -78.9, 0.97),
]


def _probe_db(path: str) -> None:
    con = sqlite3.connect(path)
    con.execute(f"CREATE TABLE probe ({PROBE_COLUMNS})")
    con.executemany(f"INSERT INTO probe VALUES ({','.join('?' * len(ROWS[0]))})", ROWS)
    con.commit()
    con.close()


class IdentitiesFromCsv(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.csv = os.path.join(self.tmp.name, "list.csv")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write(self, text: str) -> str:
        with open(self.csv, "w", encoding="utf-8") as fh:
            fh.write(text)
        return self.csv

    def test_reads_place_ids_and_feature_ids(self) -> None:
        path = self._write("google_place_id,google_feature_id\nChIJ_alpha,\n,0x885a:0xfeed\n")
        self.assertEqual(r11.identities_from_csv(path), {"chij_alpha", "0x885a:0xfeed"})

    def test_blank_and_missing_values_are_skipped(self) -> None:
        path = self._write("google_place_id,google_feature_id\nChIJ_alpha,\n,\n,\n")
        self.assertEqual(r11.identities_from_csv(path), {"chij_alpha"})

    def test_a_file_without_the_columns_is_an_error_not_an_empty_import(self) -> None:
        path = self._write("placekey,name\nabc,Alpha\n")
        with self.assertRaises(SystemExit):
            r11.identities_from_csv(path)


    def test_a_missing_file_exits_cleanly(self) -> None:
        with self.assertRaises(SystemExit):
            r11.identities_from_csv(os.path.join(self.tmp.name, "nope.csv"))


class ProbeRowsFilter(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.probe = os.path.join(self.tmp.name, "probe.db")
        _probe_db(self.probe)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_identities_select_across_categories(self) -> None:
        got = r11._probe_rows(self.probe, (), identities={"chij_alpha", "0x885a:0xfeed"})
        self.assertEqual(sorted(r["store_id"] for r in got), [1, 3])

    def test_category_still_works_on_its_own(self) -> None:
        got = r11._probe_rows(self.probe, ("Grocery store",), identities=None)
        self.assertEqual([r["store_id"] for r in got], [1])

    def test_identity_and_category_intersect(self) -> None:
        got = r11._probe_rows(self.probe, ("Grocery store",),
                              identities={"chij_alpha", "chij_beta"})
        self.assertEqual([r["store_id"] for r in got], [1])


if __name__ == "__main__":
    unittest.main()
