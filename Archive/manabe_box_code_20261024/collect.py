"""
Package this machine's batch database(s) for the trip back to the master DB.

    python collect.py            (or: collect.bat / ./collect_results.sh)

Takes a consistent copy with VACUUM INTO, which reads through the write-ahead
log. A plain file copy taken while committed pages are still in the -wal
sidecar arrives missing its most recent reviews and looks perfectly fine;
that is a documented way this project has lost data before.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _count(db: Path, sql: str) -> int:
    c = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        return c.execute(sql).fetchone()[0]
    finally:
        c.close()


def main() -> int:
    mid = (HERE / "machine_id.txt").read_text().strip() if (HERE / "machine_id.txt").exists() else "unknown"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = HERE / "results"
    out_dir.mkdir(exist_ok=True)
    batches = sorted((HERE / "batches").glob("*.db"))
    if not batches:
        print("no batches/*.db found")
        return 1
    for db in batches:
        dest = out_dir / f"{db.stem}_{mid}_{stamp}.db"
        src = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            src.execute("VACUUM INTO ?", (str(dest),))
        finally:
            src.close()
        rev = _count(dest, "SELECT COUNT(*) FROM reviews")
        done = _count(dest, "SELECT COUNT(*) FROM stores WHERE status='completed'")
        left = _count(dest, "SELECT COUNT(*) FROM stores WHERE status='pending'")
        print(f"  {dest.name}")
        print(f"    {rev:,} reviews | {done} stores completed, {left} still pending"
              f" | {dest.stat().st_size / 1e6:.0f} MB")
    print("\nCopy results/ back to the main machine (Data/returns/<date>_<machine>/), then there:")
    print("  cd Scraper_v2 && python -m linux_scraper.merge_results ../Data/returns/<dir>/*.db"
          " --master ../Data/Step_5_master_reviews.db --dry-run")
    print("You can keep scraping while the copy travels. Nothing here is destructive.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
