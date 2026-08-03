"""Unit tests for SQLite storage behavior."""

from __future__ import annotations

import sys
import unittest
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gmaps_reviews_scraper.models import BusinessProfile, ReviewRecord  # noqa: E402
from gmaps_reviews_scraper.storage import SQLiteStorage  # noqa: E402


class StorageTests(unittest.TestCase):
    def test_run_lifecycle_and_review_dedupe(self) -> None:
        local_tmp_root = PROJECT_ROOT / ".tmp_test_artifacts"
        local_tmp_root.mkdir(parents=True, exist_ok=True)
        test_dir = local_tmp_root / f"storage_{uuid.uuid4().hex}"
        test_dir.mkdir(parents=True, exist_ok=True)
        db_path = test_dir / "reviews.db"
        with SQLiteStorage(db_path) as storage:
            run_id = storage.start_run(query="Food Lion Tryon Road Raleigh", max_reviews=1500)
            storage.save_business(
                run_id=run_id,
                business=BusinessProfile(
                    query="Food Lion Tryon Road Raleigh",
                    name="Food Lion",
                    total_reviews=321,
                ),
            )

            duplicate_review = ReviewRecord(
                query="Food Lion Tryon Road Raleigh",
                business_name="Food Lion",
                review_id="abc123",
                review_text="Great store",
            )
            inserted = storage.save_reviews_batch(run_id, [duplicate_review, duplicate_review])
            self.assertEqual(inserted, 1)
            self.assertEqual(storage.get_review_count(run_id), 1)

            storage.finish_run(run_id=run_id, status="completed")

            # Confirm final status in DB.
            row = storage.conn.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "completed")


if __name__ == "__main__":
    unittest.main()
