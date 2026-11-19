"""--sync-master folds the batch into master as the run goes, so a local run needs
no collect_results.sh / scp / manual merge step.

What matters is that it reuses merge_results (one copy of the dedup and locking
logic), that it takes the lock for the whole run rather than per cycle, and that it
backs master up once — not once per cycle, which would leave a dozen 500 MB copies.
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import run_continuous  # noqa: E402


class SyncMaster(unittest.TestCase):
    def setUp(self) -> None:
        self.calls = []
        self.merge = mock.Mock(side_effect=lambda conn, path, dry_run=False: self.calls.append(path)
                               or {"reviews_new": 7, "stores_matched": 2, "stores_unmatched": 0})
        self.backup = mock.Mock(return_value="/backups/master_backup.db")
        self.lock = mock.Mock(return_value=mock.Mock())
        self.connect = mock.Mock(return_value=mock.MagicMock())
        self._p = [
            mock.patch.object(run_continuous, "merge_one", self.merge),
            mock.patch.object(run_continuous, "backup_master", self.backup),
            mock.patch.object(run_continuous, "acquire_master_lock", self.lock),
            mock.patch.object(run_continuous, "connect_master", self.connect),
        ]
        for p in self._p:
            p.start()

    def tearDown(self) -> None:
        for p in self._p:
            p.stop()

    def test_merges_the_batch_and_reports_new_reviews(self) -> None:
        with run_continuous.MasterSync("/m/master.db") as sync:
            added = sync.merge("/b/grocery1.db")
        self.assertEqual(added, 7)
        self.assertEqual(self.calls, ["/b/grocery1.db"])

    def test_backs_up_once_per_run_not_once_per_cycle(self) -> None:
        with run_continuous.MasterSync("/m/master.db") as sync:
            sync.merge("/b/b.db")
            sync.merge("/b/b.db")
            sync.merge("/b/b.db")
        self.assertEqual(self.backup.call_count, 1)
        self.assertEqual(len(self.calls), 3)

    def test_takes_the_lock_per_merge_so_a_returns_merge_can_still_run(self) -> None:
        """Holding merge.lock for a multi-day run would lock out the volunteer-returns
        merge documented in STATUS.md step 7, which exits rather than waiting."""
        with run_continuous.MasterSync("/m/master.db") as sync:
            sync.merge("/b/b.db")
            sync.merge("/b/b.db")
        self.assertEqual(self.lock.call_count, 2)
        self.assertEqual(self.lock.return_value.close.call_count, 2)

    def test_a_failed_merge_rolls_back_so_the_next_one_still_works(self) -> None:
        conn = self.connect.return_value
        self.merge.side_effect = [RuntimeError("merge verification failed"),
                                  {"reviews_new": 5, "stores_matched": 1, "stores_unmatched": 0}]
        with run_continuous.MasterSync("/m/master.db") as sync:
            self.assertEqual(sync.merge("/b/b.db"), 0)
            conn.rollback.assert_called_once()
            self.assertEqual(sync.merge("/b/b.db"), 5)

    def test_disabled_sync_is_a_no_op_so_volunteer_kits_are_unaffected(self) -> None:
        with run_continuous.MasterSync(None) as sync:
            self.assertEqual(sync.merge("/b/b.db"), 0)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.backup.call_count, 0)
        self.assertEqual(self.lock.call_count, 0)

    def test_a_merge_failure_does_not_kill_the_run(self) -> None:
        self.merge.side_effect = RuntimeError("database is locked")
        with run_continuous.MasterSync("/m/master.db") as sync:
            added = sync.merge("/b/b.db")
        self.assertEqual(added, 0)


class PeriodicSyncDuringScrape(unittest.TestCase):
    """The scraper drains a whole batch in one cycle, so a per-cycle merge on a 260k-review
    run would first reach master days later. run_scraper ticks while the subprocess runs."""

    def setUp(self) -> None:
        self.ticks = []
        self.proc = mock.Mock()
        self._popen = mock.patch.object(run_continuous.subprocess, "Popen",
                                        return_value=self.proc).start()
        self.addCleanup(mock.patch.stopall)

    def test_ticks_once_per_interval_until_the_scraper_exits(self) -> None:
        timeout = run_continuous.subprocess.TimeoutExpired("scraper", 1)
        self.proc.wait.side_effect = [timeout, timeout, 0]
        rc = run_continuous.run_scraper("/b/b.db", headless=True,
                                        on_tick=lambda: self.ticks.append(1), tick_seconds=60)
        self.assertEqual(rc, 0)
        self.assertEqual(len(self.ticks), 2)
        self.proc.wait.assert_called_with(timeout=60)

    def test_without_an_interval_it_just_waits(self) -> None:
        self.proc.wait.return_value = 0
        rc = run_continuous.run_scraper("/b/b.db", headless=True)
        self.assertEqual(rc, 0)
        self.assertEqual(self.ticks, [])

    def test_a_failing_tick_does_not_stop_the_scraper(self) -> None:
        timeout = run_continuous.subprocess.TimeoutExpired("scraper", 1)
        self.proc.wait.side_effect = [timeout, 0]

        def boom():
            raise RuntimeError("merge blew up")

        self.assertEqual(run_continuous.run_scraper("/b/b.db", headless=True,
                                                    on_tick=boom, tick_seconds=1), 0)


if __name__ == "__main__":
    unittest.main()
