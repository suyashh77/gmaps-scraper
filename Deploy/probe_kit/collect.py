"""
Package the probe results to carry back to the main machine.

Writes results/probe_<machine>_<timestamp>.db -- a consistent copy taken with
VACUUM INTO, so it is safe to run while the probe is still going.
"""
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
DB = HERE / "Frame" / "build" / "probe.db"
OUT = HERE / "results"


def main():
    if not DB.exists():
        print(f"No queue found at {DB}")
        return 1
    OUT.mkdir(exist_ok=True)
    machine = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("COMPUTERNAME", "remote")
    machine = "".join(ch for ch in machine if ch.isalnum() or ch in "-_")[:32] or "remote"
    dest = OUT / f"probe_{machine}_{datetime.now():%Y%m%d_%H%M}.db"

    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    c.execute("VACUUM INTO ?", (str(dest),))
    c.close()

    v = sqlite3.connect(f"file:{dest}?mode=ro", uri=True)
    rows = dict(v.execute("SELECT status, COUNT(*) FROM probe GROUP BY 1").fetchall())
    v.close()

    print(f"\nWrote {dest}")
    print(f"  {dest.stat().st_size / 1e6:.1f} MB")
    for k, n in sorted(rows.items(), key=lambda x: -x[1]):
        print(f"  {k:<14}{n:>8,}")
    print("\nCopy that one file back to the main machine and run:")
    print(f"  python Frame\\probe_merge.py <path to {dest.name}>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
