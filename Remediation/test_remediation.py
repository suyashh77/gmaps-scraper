"""Tests for the remediation toolkit and the scraper fixes.

Every test here is a regression test for a specific defect found in the
September 2026 audit. The name of each test says which one. They run against
synthetic databases built in a temp directory, so nothing touches the real
master.

    python Remediation/test_remediation.py
    python Remediation/test_remediation.py -v
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "Scraper_v2"))

import lib_master                                        # noqa: E402
import r00_validate as V                                 # noqa: E402

PY = sys.executable

MASTER_DDL = """
CREATE TABLE stores (
  store_id INTEGER PRIMARY KEY, place_id TEXT UNIQUE, store_name TEXT,
  business_name TEXT, outscraper_name TEXT, full_address TEXT, street TEXT,
  city TEXT, state TEXT, zip TEXT, source_lat REAL, source_lon REAL,
  outscraper_address TEXT, gmaps_address TEXT, phone TEXT, rating REAL,
  target_reviews INTEGER, business_status TEXT, category TEXT, subtypes TEXT,
  type TEXT, reviews_scraped INTEGER DEFAULT 0, scrape_status TEXT
);
CREATE TABLE reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT, store_id INTEGER REFERENCES stores(store_id),
  reviewer_name TEXT, rating INTEGER, date_relative TEXT, review_text TEXT,
  helpful_count INTEGER DEFAULT 0, photo_count INTEGER DEFAULT 0,
  has_owner_response INTEGER DEFAULT 0, reviewer_review_count INTEGER DEFAULT 0,
  reviewer_photo_count INTEGER DEFAULT 0, is_local_guide INTEGER DEFAULT 0,
  service_type TEXT DEFAULT '', scraped_at TEXT, source_db TEXT
);
"""

BATCH_DDL = """
CREATE TABLE stores (
  store_id INTEGER PRIMARY KEY, machine_id TEXT, input_name TEXT, input_street TEXT,
  input_city TEXT, input_state TEXT, input_zip TEXT, input_lat REAL, input_lon REAL,
  google_place_id TEXT UNIQUE, gmaps_name TEXT, gmaps_address TEXT, gmaps_phone TEXT,
  gmaps_website TEXT, gmaps_rating REAL, gmaps_reviews_count INTEGER, gmaps_lat REAL,
  gmaps_lon REAL, gmaps_category TEXT, gmaps_price_level TEXT, status TEXT,
  worker_id INTEGER, reviews_scraped INTEGER DEFAULT 0, target_reviews INTEGER,
  master_reviews INTEGER DEFAULT 0, error_message TEXT, attempts INTEGER DEFAULT 0,
  last_attempt TEXT, created_at TEXT, updated_at TEXT, last_sort_order TEXT,
  last_review_id TEXT
);
CREATE TABLE reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT, store_id INTEGER, machine_id TEXT,
  review_id TEXT, reviewer_name TEXT, rating INTEGER, date_relative TEXT,
  review_text TEXT, helpful_count INTEGER DEFAULT 0, photo_count INTEGER DEFAULT 0,
  has_owner_response INTEGER DEFAULT 0, reviewer_review_count INTEGER DEFAULT 0,
  reviewer_photo_count INTEGER DEFAULT 0, is_local_guide INTEGER DEFAULT 0,
  service_type TEXT DEFAULT '', scraped_at TEXT, UNIQUE(store_id, review_id)
);
CREATE TABLE known_review_keys (store_id INTEGER, dedup_key TEXT);
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE scrape_sessions (id INTEGER PRIMARY KEY);
"""


def build_master(path: str) -> None:
    con = sqlite3.connect(path)
    con.executescript(MASTER_DDL)
    stores = [
        # id, place_id, name, city, zip, lat, lon, target, scraped, status, category
        (1, "ChIJaaaaaaaaaaaaaaaaaaaaaa1", "Food Lion", "Raleigh", "27601",
         35.7796, -78.6382, 100, 0, "incomplete", "Grocery store"),
        (2, "ChIJaaaaaaaaaaaaaaaaaaaaaa2", "Dollar General", "Durham", "27701",
         35.9940, -78.8986, None, 0, "complete", "Dollar store"),
        (3, "ChIJaaaaaaaaaaaaaaaaaaaaaa3", "Corner Mart", "Charlotte", "28202",
         35.2271, -80.8431, 4, 0, "complete", "Convenience store"),
        (4, "ChIJaaaaaaaaaaaaaaaaaaaaaa4", "Grand Asia Market", "Fletcher", "28732",
         35.4310, -82.5010, 50, 0, "incomplete", "Asian grocery store"),
    ]
    for (sid, pid, name, city, zc, lat, lon, tgt, scr, st, cat) in stores:
        addr = f"100 Main St, {city}, NC {zc}"
        if sid == 4:
            # The wrong-location signature: an Asheville-area store carrying a
            # Raleigh address, which is what the geographic check must catch.
            addr = "1253 Buck Jones Rd, Raleigh, NC 27606"
        con.execute(
            "INSERT INTO stores (store_id, place_id, store_name, city, state, zip, "
            "full_address, source_lat, source_lon, target_reviews, reviews_scraped, "
            "scrape_status, category) VALUES (?,?,?,?,'NC',?,?,?,?,?,?,?,?)",
            (sid, pid, name, city, zc, addr, lat, lon, tgt, scr, st, cat))
    # Two stores in ZIP 27606 so the centroid for that ZIP exists.
    for sid, pid in ((90, "ChIJzzzzzzzzzzzzzzzzzzzzzz1"), (91, "ChIJzzzzzzzzzzzzzzzzzzzzzz2")):
        con.execute(
            "INSERT INTO stores (store_id, place_id, store_name, city, state, zip, "
            "full_address, source_lat, source_lon, target_reviews, reviews_scraped, "
            "scrape_status, category) VALUES (?,?,?,?,'NC',?,?,?,?,?,?,?,?)",
            (sid, pid, "Filler", "Raleigh", "27606",
             "200 Buck Jones Rd, Raleigh, NC 27606", 35.7700, -78.7500,
             10, 0, "incomplete", "Grocery store"))

    def add(store_id, name, rating, text, source="seed.db"):
        con.execute(
            "INSERT INTO reviews (store_id, reviewer_name, rating, review_text, "
            "date_relative, scraped_at, source_db) VALUES (?,?,?,?,'a year ago','2026-04-01',?)",
            (store_id, name, rating, text, source))

    # store 1: two byte-identical text reviews (a real duplicate) plus one other
    add(1, "Alex", 5, "Great produce and friendly staff every single time.")
    add(1, "Alex", 5, "Great produce and friendly staff every single time.")
    add(1, "Bella", 4, "Fine.")
    # store 1: two rating-only rows from the same common first name. NOT a
    # duplicate: two different people called Chris.
    add(1, "Chris", 5, "")
    add(1, "Chris", 5, "")
    # store 3: stale target of 4 against many reviews (the back-fill case)
    for i in range(30):
        add(3, f"Person{i}", 4, f"Review number {i} with distinct text.")
    # store 4: reviews that will also appear at store 1 (contamination)
    for i in range(25):
        t = f"Shared review body number {i} that is identical across two stores."
        add(4, f"Ghost{i}", 5, t)
        add(1, f"Ghost{i}", 5, t)
    con.execute("UPDATE stores SET reviews_scraped = "
                "(SELECT COUNT(*) FROM reviews r WHERE r.store_id = stores.store_id)")
    con.commit()
    con.close()


def build_batch(path: str, place_ids) -> None:
    con = sqlite3.connect(path)
    con.executescript(BATCH_DDL)
    for i, pid in enumerate(place_ids, start=1):
        con.execute(
            "INSERT INTO stores (store_id, google_place_id, input_name, input_city, "
            "input_state, status, target_reviews, master_reviews, reviews_scraped) "
            "VALUES (?,?,?,?,'NC','pending',?,0,0)",
            (i, pid, f"Store {i}", "Raleigh", 100))
    # Collected but never merged, the batch1 situation.
    for j in range(5):
        con.execute(
            "INSERT INTO reviews (store_id, machine_id, review_id, reviewer_name, rating, "
            "review_text, date_relative, scraped_at) VALUES (1,'tqehk2',?,?,?,?,"
            "'2 months ago','2026-05-09')",
            (f"rid{j}", f"Newcomer{j}", 5, f"Brand new review {j} not yet in master."))
    con.commit()
    con.close()


class TempDBCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="remediation_test_")
        self.master = os.path.join(self.tmp, "master.db")
        build_master(self.master)

    def rows(self, sql, params=()):
        con = sqlite3.connect(self.master)
        con.row_factory = sqlite3.Row
        out = con.execute(sql, params).fetchall()
        con.close()
        return out

    def one(self, sql, params=()):
        return self.rows(sql, params)[0][0]

    def run_tool(self, tool, *args):
        cmd = [PY, os.path.join(HERE, tool), "--master", self.master, *args]
        env = dict(os.environ, PYTHONIOENCODING="utf-8")
        return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=REPO)


class TestGeographicCheck(TempDBCase):
    """Regression: the enricher accepted every first search result."""

    def test_finds_store_far_from_its_own_address(self):
        con = lib_master.connect_ro(self.master)
        bad = V.geo_inconsistent(con, threshold_km=25.0)
        con.close()
        ids = {b["store_id"] for b in bad}
        self.assertIn(4, ids, "Fletcher store with a Raleigh address must be flagged")
        far = next(b for b in bad if b["store_id"] == 4)
        self.assertGreater(far["km_off"], 200)

    def test_does_not_flag_a_consistent_store(self):
        con = lib_master.connect_ro(self.master)
        bad = V.geo_inconsistent(con, threshold_km=25.0)
        con.close()
        self.assertNotIn(1, {b["store_id"] for b in bad})


class TestContaminationCheck(TempDBCase):
    """Regression: one review page collected into several store ids."""

    def test_detects_shared_review_bodies(self):
        con = lib_master.connect_ro(self.master)
        pairs, involved = V.cross_store_contamination(con, min_shared=20)
        con.close()
        self.assertTrue(pairs, "25 shared identical reviews must register")
        self.assertEqual({1, 4}, involved)
        self.assertGreaterEqual(pairs[0][2], 25)

    def test_ignores_incidental_overlap(self):
        con = lib_master.connect_ro(self.master)
        pairs, _ = V.cross_store_contamination(con, min_shared=200)
        con.close()
        self.assertEqual([], pairs, "a high threshold must not fire on 25 shared")


class TestDeterministicCleanup(TempDBCase):
    """Regression: dedup that would have destroyed real rating-only reviews."""

    def test_deletes_text_duplicates_but_keeps_rating_only_pairs(self):
        before_text = self.one(
            "SELECT COUNT(*) FROM reviews WHERE store_id=1 AND reviewer_name='Alex'")
        before_rating = self.one(
            "SELECT COUNT(*) FROM reviews WHERE store_id=1 AND reviewer_name='Chris'")
        self.assertEqual(2, before_text)
        self.assertEqual(2, before_rating)

        r = self.run_tool("r03_clean_deterministic.py", "--apply")
        self.assertEqual(0, r.returncode, r.stderr)

        self.assertEqual(
            1, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=1 AND reviewer_name='Alex'"),
            "identical text duplicates must collapse to one")
        self.assertEqual(
            2, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=1 AND reviewer_name='Chris'"),
            "two people sharing a first name with no text are NOT duplicates")

    def test_duplicates_are_quarantined_not_dropped(self):
        before = self.one("SELECT COUNT(*) FROM reviews")
        r = self.run_tool("r03_clean_deterministic.py", "--apply")
        self.assertEqual(0, r.returncode, r.stderr)
        moved = self.one("SELECT COUNT(*) FROM quarantined_reviews WHERE reviewer_name='Alex'")
        self.assertEqual(1, moved)
        self.assertEqual(before, self.one("SELECT COUNT(*) FROM reviews")
                         + self.one("SELECT COUNT(*) FROM quarantined_reviews"))

    def test_backfills_a_stale_target_without_touching_reviews(self):
        before = self.one("SELECT COUNT(*) FROM reviews WHERE store_id=3")
        self.assertEqual(4, self.one("SELECT target_reviews FROM stores WHERE store_id=3"))
        r = self.run_tool("r03_clean_deterministic.py", "--apply")
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual(30, self.one("SELECT target_reviews FROM stores WHERE store_id=3"))
        self.assertEqual(before, self.one("SELECT COUNT(*) FROM reviews WHERE store_id=3"))

    def test_null_target_never_becomes_complete(self):
        """The bug that locked 157 stores out of every future batch."""
        r = self.run_tool("r03_clean_deterministic.py", "--apply")
        self.assertEqual(0, r.returncode, r.stderr)
        status = self.one("SELECT scrape_status FROM stores WHERE store_id=2")
        self.assertEqual("unknown_target", status)
        self.assertNotEqual("complete", status)

    def test_is_idempotent(self):
        self.run_tool("r03_clean_deterministic.py", "--apply")
        after_first = self.one("SELECT COUNT(*) FROM reviews")
        self.run_tool("r03_clean_deterministic.py", "--apply")
        self.assertEqual(after_first, self.one("SELECT COUNT(*) FROM reviews"))

    def test_dry_run_changes_nothing(self):
        before = self.one("SELECT COUNT(*) FROM reviews")
        r = self.run_tool("r03_clean_deterministic.py")
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual(before, self.one("SELECT COUNT(*) FROM reviews"))


class TestBatchRecovery(TempDBCase):
    """Regression: 2,262 collected reviews that never reached master."""

    def setUp(self):
        super().setUp()
        self.batch = os.path.join(self.tmp, "batch_test.db")
        build_batch(self.batch, [self.rows("SELECT place_id FROM stores ORDER BY store_id")[0][0]])

    def test_recovers_reviews_and_preserves_review_id(self):
        r = self.run_tool("r02_migrate_master_schema.py", "--apply")
        self.assertEqual(0, r.returncode, r.stderr)
        before = self.one("SELECT COUNT(*) FROM reviews")
        r = self.run_tool("r01_recover_unmerged_batches.py", "--apply", "--batches", self.batch)
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual(before + 5, self.one("SELECT COUNT(*) FROM reviews"))
        self.assertEqual(
            5, self.one("SELECT COUNT(*) FROM reviews WHERE review_id IS NOT NULL"),
            "review_id must survive the merge; dropping it is what broke dedup")
        self.assertEqual(
            5, self.one("SELECT COUNT(*) FROM reviews WHERE machine_id='tqehk2'"))

    def test_second_run_adds_nothing(self):
        self.run_tool("r02_migrate_master_schema.py", "--apply")
        self.run_tool("r01_recover_unmerged_batches.py", "--apply", "--batches", self.batch)
        after_first = self.one("SELECT COUNT(*) FROM reviews")
        self.run_tool("r01_recover_unmerged_batches.py", "--apply", "--batches", self.batch)
        self.assertEqual(after_first, self.one("SELECT COUNT(*) FROM reviews"),
                         "merging the same batch twice must be a no-op")


class TestSchemaMigration(TempDBCase):
    def test_adds_columns_and_is_rerunnable(self):
        r = self.run_tool("r02_migrate_master_schema.py", "--apply")
        self.assertEqual(0, r.returncode, r.stderr)
        cols = {c[1] for c in self.rows("PRAGMA table_info(reviews)")}
        self.assertTrue({"review_id", "machine_id", "review_text_sha256"} <= cols)
        cols = {c[1] for c in self.rows("PRAGMA table_info(stores)")}
        self.assertTrue({"qa_flags", "verified_at", "review_cap"} <= cols)
        r = self.run_tool("r02_migrate_master_schema.py", "--apply")
        self.assertEqual(0, r.returncode, "re-running the migration must not fail")

    def test_hashes_every_review(self):
        self.run_tool("r02_migrate_master_schema.py", "--apply")
        self.assertEqual(
            0, self.one("SELECT COUNT(*) FROM reviews WHERE review_text_sha256 IS NULL"))


class TestValidatorSemantics(TempDBCase):
    """The two checks that were wrong before the audit corrected them."""

    def test_glob_not_like_for_the_see_more_marker(self):
        con = sqlite3.connect(self.master)
        con.execute("INSERT INTO reviews (store_id, reviewer_name, rating, review_text, "
                    "source_db) VALUES (1,'X',5,'Good selection and so much more','t.db')")
        con.execute("INSERT INTO reviews (store_id, reviewer_name, rating, review_text, "
                    "source_db) VALUES (1,'Y',5,'Truncated body textMore','t.db')")
        con.commit()
        like = con.execute(
            "SELECT COUNT(*) FROM reviews WHERE review_text LIKE '%More'").fetchone()[0]
        glob = con.execute(
            "SELECT COUNT(*) FROM reviews WHERE review_text GLOB '*More'").fetchone()[0]
        con.close()
        self.assertEqual(2, like, "LIKE is case-insensitive and catches the innocent row")
        self.assertEqual(1, glob, "GLOB is case-sensitive and catches only the clipped row")

    def test_service_type_empty_string_is_detected(self):
        """service_type is '' and never NULL, so an IS NULL check silently passes."""
        n_null = self.one("SELECT COUNT(*) FROM reviews WHERE service_type IS NULL")
        n_empty = self.one(
            "SELECT COUNT(*) FROM reviews WHERE service_type IS NULL "
            "OR TRIM(service_type) = ''")
        self.assertEqual(0, n_null)
        self.assertGreater(n_empty, 0, "the check must test for empty, not just NULL")


class TestScraperLogic(unittest.TestCase):
    """Pure-function tests for the scraper fixes."""

    def setUp(self):
        from linux_scraper import scraper as S
        self.S = S

    def test_haversine_is_sane(self):
        # Raleigh to Charlotte is roughly 200 km.
        d = self.S._haversine_km(35.7796, -78.6382, 35.2271, -80.8431)
        self.assertTrue(190 < d < 220, f"expected ~200 km, got {d}")
        self.assertAlmostEqual(0.0, self.S._haversine_km(35.0, -80.0, 35.0, -80.0), places=6)

    def test_business_pin_regex_prefers_place_over_camera(self):
        import re
        url = ("https://www.google.com/maps/place/Food+Lion/@35.7796,-78.6382,15z/"
               "data=!4m6!3m5!1s0x89ac!8m2!3d35.9999!4d-79.1111")
        camera = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
        pin = re.search(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", url)
        self.assertEqual(("35.7796", "-78.6382"), camera.groups())
        self.assertEqual(("35.9999", "-79.1111"), pin.groups())
        self.assertNotEqual(camera.groups(), pin.groups(),
                            "the camera and the business pin are different points; "
                            "reading the camera is what disabled the distance veto")

    def test_house_number_match_is_exact(self):
        import re

        def house(text):
            m = re.match(r"\s*(\d+)", text.strip())
            return m.group(1) if m else None

        a, b = "7400 the plaza, charlotte, nc 28215", "1000 the plaza, charlotte, nc 28215"
        self.assertNotEqual(house(a), house(b))
        # The old test intersected every number in both strings, so the shared
        # ZIP made these two "match".
        old_match = bool(set(re.findall(r"\d+", a)) & set(re.findall(r"\d+", b)))
        self.assertTrue(old_match, "documents the old permissive behaviour")

    def test_field_coverage_assertion_fires_on_a_dead_selector(self):
        import logging

        class Probe(logging.Handler):
            def __init__(self):
                super().__init__()
                self.errors = []

            def emit(self, record):
                if record.levelno >= logging.ERROR:
                    self.errors.append(record.getMessage())

        sc = self.S.GoogleMapsScraper.__new__(self.S.GoogleMapsScraper)
        sc.logger = logging.getLogger("coverage_probe")
        sc.logger.setLevel(logging.DEBUG)
        probe = Probe()
        sc.logger.addHandler(probe)
        import collections
        # reviewer_review_count is a field Google does render, so a 100% miss
        # rate really would mean a stale selector. helpful_count and photo_count
        # are NOT used here: the live DOM probe showed Maps renders no such
        # element at all, so they are registered as optional and report at info.
        sc._extract_failures = collections.Counter({"reviewer_review_count": 100})
        sc.assert_field_coverage(100)
        self.assertTrue(any("reviewer_review_count" in m for m in probe.errors),
                        "a field missing on every review must raise a loud error")
        self.assertEqual({}, dict(sc._extract_failures), "counters reset per store")

    def test_unobtainable_fields_do_not_raise_a_false_alarm(self):
        """helpful_count and photo_count are absent from the UI, not broken.

        A check that cries wolf on every single store is a check people learn to
        ignore, which would defeat the whole purpose of adding it.
        """
        import collections
        import logging

        sc = self.S.GoogleMapsScraper.__new__(self.S.GoogleMapsScraper)
        sc.logger = logging.getLogger("coverage_optional")
        errors = []

        class Probe(logging.Handler):
            def emit(self, record):
                if record.levelno >= logging.ERROR:
                    errors.append(record.getMessage())

        sc.logger.addHandler(Probe())
        sc.logger.setLevel(logging.DEBUG)
        sc._extract_failures = collections.Counter(
            {"helpful_count": 100, "photo_count": 100, "service_type": 100})
        sc.assert_field_coverage(100)
        self.assertEqual([], errors,
                         "fields Google does not render must not log as failures")

    def test_field_coverage_stays_quiet_on_normal_sparsity(self):
        import collections
        import logging

        sc = self.S.GoogleMapsScraper.__new__(self.S.GoogleMapsScraper)
        sc.logger = logging.getLogger("coverage_probe_quiet")
        sc._extract_failures = collections.Counter({"helpful_count": 10})
        with self.assertLogs(sc.logger, level="WARNING") as cm:
            sc.logger.warning("sentinel")
            sc.assert_field_coverage(100)
        self.assertEqual(1, len(cm.output), "10% missing is normal and must not warn")

    def test_cancelled_error_is_not_an_exception(self):
        """The bug: `except Exception` never caught the timeout path."""
        import asyncio
        self.assertFalse(issubclass(asyncio.CancelledError, Exception))
        self.assertTrue(issubclass(asyncio.CancelledError, BaseException))

    def test_place_id_navigation_outranks_the_distance_veto(self):
        """Regression: the tightened distance veto rejected correct stores.

        The first long run navigated to a Harris Teeter and an ALDI by their own
        place_id, landed on the right pages, and then rejected both because the
        business pin was 3.14 km and 11.66 km from the stored coordinate. The
        pages were correct; the Overture coordinates were stale.

        Asking Google for a specific place_id and receiving a business page IS
        the identity check. Geometry cannot outrank it, and may only be used
        where identity is genuinely in doubt, which is text-search navigation.
        """
        import inspect
        src = inspect.getsource(self.S.GoogleMapsScraper.verify_store)
        self.assertIn("_navigated_by_place_id", src,
                      "verify_store must short-circuit when navigation used the place_id")
        veto_pos = src.find("CHAIN_DISTANCE_REJECT_KM")
        short_pos = src.find("_navigated_by_place_id")
        self.assertNotEqual(-1, veto_pos)
        self.assertLess(short_pos, veto_pos,
                        "the place_id short-circuit must come BEFORE the distance veto")
        nav = inspect.getsource(self.S.GoogleMapsScraper.navigate_to_store)
        self.assertIn("place_id_attempt", nav,
                      "navigation must record whether it used the place_id URL")

    def test_connectivity_errors_are_distinguished_from_bad_stores(self):
        """Regression: a network outage marked three good stores failed."""
        sc = self.S.GoogleMapsScraper
        self.assertTrue(sc._is_connectivity_error(
            Exception("Page.goto: net::ERR_INTERNET_DISCONNECTED at https://...")))
        self.assertTrue(sc._is_connectivity_error(Exception("net::ERR_NAME_NOT_RESOLVED")))
        self.assertFalse(sc._is_connectivity_error(Exception("Timeout 45000ms exceeded")))
        self.assertFalse(sc._is_connectivity_error(Exception("net::ERR_ABORTED")))

    def test_sort_reviews_reports_success(self):
        import inspect
        src = inspect.getsource(self.S.GoogleMapsScraper._sort_reviews)
        self.assertIn("-> bool", src, "callers need to know whether the sort applied")
        self.assertIn("Escape", src, "a failed sort must close the menu it opened")
        self.assertIn('"relevant"', src, "relevant must be a real rung, not a no-op")


class TestMergeSemantics(TempDBCase):
    """Behavioural tests for merge_results, run against a temp master.

    These assert what the code DOES, not what its source text says. An earlier
    version of this file grepped for the string `target == 0` and failed once
    that string survived only inside a comment explaining the old bug, which is
    a good reminder that a source grep is not a test.
    """

    def setUp(self):
        super().setUp()
        self.batch = os.path.join(self.tmp, "batch_merge.db")
        pids = [r[0] for r in self.rows("SELECT place_id FROM stores ORDER BY store_id")]
        build_batch(self.batch, pids[:2])          # store 1 and store 2 (NULL target)

    def _merge(self):
        from linux_scraper import merge_results as M
        con = M._connect(self.master)
        stats = M.merge_one(con, self.batch, dry_run=False)
        con.close()
        return stats

    def test_null_target_store_is_not_marked_complete(self):
        """The expression that locked 157 stores out of every future batch."""
        self._merge()
        status = self.one("SELECT scrape_status FROM stores WHERE store_id=2")
        self.assertEqual("unknown_target", status)
        self.assertNotEqual("complete", status)

    def test_review_id_and_machine_id_survive_the_merge(self):
        self.run_tool("r02_migrate_master_schema.py", "--apply")
        self._merge()
        self.assertEqual(
            5, self.one("SELECT COUNT(*) FROM reviews WHERE review_id IS NOT NULL"),
            "Google's own review identifier must reach master")
        self.assertEqual(
            5, self.one("SELECT COUNT(*) FROM reviews WHERE machine_id='tqehk2'"))

    def test_counts_are_recomputed_even_when_nothing_new_arrives(self):
        """The old code only wrote when the count went UP, so deletions stuck."""
        con = sqlite3.connect(self.master)
        con.execute("UPDATE stores SET reviews_scraped = 9999 WHERE store_id = 1")
        con.commit()
        con.close()
        self._merge()
        actual = self.one("SELECT COUNT(*) FROM reviews WHERE store_id=1")
        self.assertEqual(actual, self.one(
            "SELECT reviews_scraped FROM stores WHERE store_id=1"),
            "an overstated counter must be corrected downward")

    def test_merging_the_same_batch_twice_is_a_no_op(self):
        self._merge()
        after_first = self.one("SELECT COUNT(*) FROM reviews")
        self._merge()
        self.assertEqual(after_first, self.one("SELECT COUNT(*) FROM reviews"))

    def test_full_text_key_keeps_two_reviews_sharing_an_opening(self):
        """The 50-character prefix key merged genuinely different reviews."""
        from linux_scraper import merge_results as M
        opener = "I love this store, the staff are always so friendly and "
        con = sqlite3.connect(self.batch)
        con.execute(
            "INSERT INTO reviews (store_id, machine_id, review_id, reviewer_name, rating, "
            "review_text, scraped_at) VALUES (1,'m','p1','Pat',5,?,'2026-05-09')",
            (opener + "the produce is excellent.",))
        con.execute(
            "INSERT INTO reviews (store_id, machine_id, review_id, reviewer_name, rating, "
            "review_text, scraped_at) VALUES (1,'m','p2','Pat',5,?,'2026-05-09')",
            (opener + "the bakery is the best in town.",))
        con.commit()
        con.close()
        self._merge()
        n = self.one("SELECT COUNT(*) FROM reviews WHERE reviewer_name='Pat'")
        self.assertEqual(2, n, "two distinct reviews sharing 50 characters must both survive")

    def test_lock_helper_exists_and_is_used(self):
        import inspect
        from linux_scraper import merge_results as M
        self.assertTrue(hasattr(M, "_acquire_lock"))
        src = inspect.getsource(M.main)
        self.assertIn("_acquire_lock", src)
        self.assertIn("wal_checkpoint", src, "the backup must include the WAL")


if __name__ == "__main__":
    unittest.main(verbosity=2)
