"""R17: reviews set aside as another store's page come back when the store's own page returns them."""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import r17_restore_confirmed as R          # noqa: E402

RCOLS = "id INTEGER PRIMARY KEY, store_id, reviewer_name, rating, review_text, source_db"


class Restore(unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp()
        self.con = sqlite3.connect(os.path.join(d, "m.db"))
        self.con.row_factory = sqlite3.Row
        self.con.execute("CREATE TABLE stores (store_id INTEGER PRIMARY KEY, target_reviews, reviews_scraped, "
                         "scrape_status, status_reason)")
        self.con.execute(f"CREATE TABLE reviews ({RCOLS})")
        self.con.execute(f"CREATE TABLE quarantined_reviews ({RCOLS}, quarantine_reason, quarantined_at)")
        self.con.execute("INSERT INTO stores VALUES (1, 4, 1, 'incomplete', NULL), (2, 10, 10, 'complete', NULL)")
        # store 1 holds A; B, C, D, E were set aside from it; store 2 also holds D
        self.con.executemany("INSERT INTO reviews VALUES (?,?,?,?,?,?)",
                             [(1, 1, "ann", 5, "A text", "x"), (9, 2, "dee", 4, "D text", "x")])
        self.con.executemany("INSERT INTO quarantined_reviews VALUES (?,?,?,?,?,?,?,?)",
                             [(2, 1, "bob", 5, "B text", "x", "wrong page", "2026-10-02"),
                              (3, 1, "cat", 3, "C text", "x", "wrong page", "2026-10-02"),
                              (4, 1, "dee", 4, "D text", "x", "wrong page", "2026-10-02"),
                              (5, 1, "eve", 2, "E text", "x", "wrong page", "2026-10-02")])
        self.batch = os.path.join(d, "batch.db")
        b = sqlite3.connect(self.batch)
        b.execute("CREATE TABLE stores (store_id INTEGER PRIMARY KEY, master_store_id)")
        b.execute("CREATE TABLE reviews (store_id, reviewer_name, rating, review_text)")
        b.execute("INSERT INTO stores VALUES (1, 1)")
        # the store's own page returned B, C and D again, but not E
        b.executemany("INSERT INTO reviews VALUES (1,?,?,?)",
                      [("bob", 5, "B text"), ("cat", 3, "C text"), ("dee", 4, "D text")])
        b.commit()
        b.close()

    def test_confirmed_rows_return_unless_another_store_holds_them(self):
        restored = R.restore(self.con, [self.batch])
        self.assertEqual({1: 2}, restored)                       # B and C; D is held by store 2
        held = {r[0] for r in self.con.execute("SELECT reviewer_name FROM reviews WHERE store_id=1")}
        self.assertEqual({"ann", "bob", "cat"}, held)
        left = {r[0] for r in self.con.execute("SELECT reviewer_name FROM quarantined_reviews WHERE store_id=1")}
        self.assertEqual({"dee", "eve"}, left)                   # D contested, E never confirmed

    def test_rating_only_reviews_are_judged_against_the_same_store_only(self):
        # "fay" 5 stars with no text is held by store 2 as well; names collide, so it must not block
        self.con.execute("INSERT INTO reviews VALUES (20, 2, 'fay', 5, '', 'x')")
        self.con.execute("INSERT INTO quarantined_reviews VALUES (21, 1, 'fay', 5, '', 'x', 'wrong page', '2026-10-02')")
        b = sqlite3.connect(self.batch)
        b.execute("INSERT INTO reviews VALUES (1, 'fay', 5, '')")
        b.commit()
        b.close()
        self.assertEqual({1: 3}, R.restore(self.con, [self.batch]))      # B, C and the rating-only one

    def test_second_run_changes_nothing(self):
        R.restore(self.con, [self.batch])
        self.assertEqual({}, R.restore(self.con, [self.batch]))


if __name__ == "__main__":
    unittest.main()
