"""Tests for run_continuous.py path resolution.

The orchestrator used to join every relative --batch path onto Data/, so the
collection-kit invocation `--batch batches/run1.db` failed with a path the
operator never typed. resolve_batch_path() must try the shell-like candidates
in order and fall back to the typed path when nothing exists.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import run_continuous  # noqa: E402


class TestResolveBatchPath(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.script_dir = os.path.join(self.root, "kit")
        self.data_dir = os.path.join(self.root, "Data")
        os.makedirs(os.path.join(self.script_dir, "batches"))
        os.makedirs(self.data_dir)
        self._patches = [
            mock.patch.object(run_continuous, "SCRIPT_DIR", self.script_dir),
            mock.patch.object(run_continuous, "DATA_DIR", self.data_dir),
        ]
        for p in self._patches:
            p.start()
        self.old_cwd = os.getcwd()
        os.chdir(self.root)

    def tearDown(self) -> None:
        os.chdir(self.old_cwd)
        for p in self._patches:
            p.stop()
        self.tmp.cleanup()

    @staticmethod
    def _touch(path: str) -> str:
        with open(path, "w", encoding="utf-8"):
            pass
        return path

    def test_absolute_path_is_returned_unchanged(self) -> None:
        # Arrange
        wanted = os.path.join(self.root, "anywhere.db")
        # Act
        got = run_continuous.resolve_batch_path(wanted)
        # Assert
        self.assertEqual(got, wanted)

    def test_relative_path_existing_in_cwd_wins(self) -> None:
        wanted = self._touch(os.path.join(self.root, "run1.db"))
        self._touch(os.path.join(self.script_dir, "batches", "run1.db"))
        got = run_continuous.resolve_batch_path("run1.db")
        self.assertEqual(os.path.normcase(got), os.path.normcase(wanted))

    def test_kit_batches_dir_is_found_by_basename(self) -> None:
        wanted = self._touch(os.path.join(self.script_dir, "batches", "run1.db"))
        got = run_continuous.resolve_batch_path("batches/run1.db")
        self.assertEqual(os.path.normcase(got), os.path.normcase(wanted))

    def test_legacy_data_dir_is_last_resort(self) -> None:
        wanted = self._touch(os.path.join(self.data_dir, "batch1.db"))
        got = run_continuous.resolve_batch_path("batch1.db")
        self.assertEqual(os.path.normcase(got), os.path.normcase(wanted))

    def test_missing_file_falls_back_to_typed_path(self) -> None:
        got = run_continuous.resolve_batch_path("nope.db")
        self.assertEqual(os.path.normcase(got),
                         os.path.normcase(os.path.abspath("nope.db")))
        self.assertFalse(os.path.exists(got))


if __name__ == "__main__":
    unittest.main(verbosity=2)
