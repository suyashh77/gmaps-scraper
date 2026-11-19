"""
Fold probe results collected on another machine back into the master queue.

Run this ON THE MAIN MACHINE, from the project root:

    python Frame/probe_merge.py Data/returns/20260915_holling/probe_HOLLING_20260915_1348.db

Safe to run more than once. A row is only taken from the incoming file when
that file actually probed it and the master has not, or probed it earlier.
Nothing in the master is ever blanked out by an unprobed incoming row.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

MASTER = Path(__file__).resolve().parent / "build" / "probe.db"

FIELDS = ("g_name", "g_category", "g_address", "g_phone", "g_website", "g_rating",
          "g_total_reviews", "g_place_id", "g_feature_id", "g_lat", "g_lon",
          "g_closed", "verified", "name_sim", "error", "probed_at", "machine_id",
          "status", "attempts")

_SETS = ", ".join(
    f"{f} = (SELECT i.{f} FROM inc.probe i WHERE i.store_id = probe.store_id)"
    for f in FIELDS)
_WHERE = """store_id IN (
    SELECT i.store_id FROM inc.probe i
    JOIN probe m ON m.store_id = i.store_id
    WHERE i.probed_at IS NOT NULL
      AND (m.probed_at IS NULL OR i.probed_at > m.probed_at))"""


def _count_probed(c: sqlite3.Connection, table: str) -> int:
    return c.execute(f"SELECT COUNT(*) FROM {table} WHERE probed_at IS NOT NULL").fetchone()[0]


def merge_into(master: Path, incoming: Path) -> dict[str, int]:
    """Merge one result file into `master`; returns before/after counts."""
    c = sqlite3.connect(master, timeout=60)
    try:
        c.execute("PRAGMA journal_mode=WAL")
        # Attach by plain path, not a file: URI. ATTACH only parses URIs when the
        # connection was opened with uri=True, and a Windows path's backslashes
        # are not legal in one either. Nothing here writes to inc.
        c.execute("ATTACH DATABASE ? AS inc", (str(Path(incoming).resolve()),))
        stats = {
            "incoming_probed": _count_probed(c, "inc.probe"),
            "probed_before": _count_probed(c, "probe"),
        }
        c.execute("BEGIN IMMEDIATE")
        stats["rows_updated"] = c.execute(f"UPDATE probe SET {_SETS} WHERE {_WHERE}").rowcount
        c.commit()
        stats["probed_after"] = _count_probed(c, "probe")
        stats["by_status"] = dict(c.execute(
            "SELECT status, COUNT(*) FROM probe GROUP BY 1 ORDER BY 2 DESC").fetchall())
        c.execute("DETACH DATABASE inc")
    finally:
        c.close()
    return stats


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 1
    incoming = Path(argv[1])
    if not incoming.exists():
        print(f"No such file: {incoming}")
        return 1
    if not MASTER.exists():
        print(f"Master queue not found at {MASTER}")
        return 1

    s = merge_into(MASTER, incoming)
    print(f"incoming file  : {s['incoming_probed']:,} probed rows")
    print(f"rows updated   : {s['rows_updated']:,}")
    print(f"master probed  : {s['probed_before']:,} -> {s['probed_after']:,}"
          f"  (+{s['probed_after'] - s['probed_before']:,} new)")
    print()
    for status, n in s["by_status"].items():
        print(f"  {status:<14}{n:>8,}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
