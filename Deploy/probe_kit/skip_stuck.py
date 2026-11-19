"""
Park stores that keep failing, so the probe can get on with the rest.

A store that crashes the probe is released back to the queue on the next start
and claimed again first, because the queue is worked in store_id order. That is
an infinite loop on one bad store. This marks anything attempted 5 times or
more as 'failed', so the queue skips past it.

Nothing is deleted. Parked stores keep their row and every value already
captured, and can be put back in the queue later with:

    python skip_stuck.py --unpark
"""
import sqlite3
import sys
from pathlib import Path

DB = Path(__file__).resolve().parent / "Frame" / "build" / "probe.db"
THRESHOLD = 5


def summary(c):
    for r in c.execute("SELECT status, COUNT(*) FROM probe GROUP BY 1 ORDER BY 2 DESC"):
        print(f"  {r[0]:<14}{r[1]:>8,}")


def main():
    if not DB.exists():
        print(f"No queue found at {DB}")
        return 1

    c = sqlite3.connect(DB, timeout=60)

    if "--unpark" in sys.argv:
        n = c.execute(
            "UPDATE probe SET status='pending', attempts=0 WHERE status='failed'"
        ).rowcount
        c.commit()
        print(f"Returned {n:,} parked store(s) to the queue, attempts reset.")
        summary(c)
        c.close()
        return 0

    stuck = c.execute(
        "SELECT store_id, name, city, attempts FROM probe "
        "WHERE status IN ('pending','in_progress') AND attempts >= ? "
        "ORDER BY attempts DESC",
        (THRESHOLD,),
    ).fetchall()

    if not stuck:
        print(f"No store has been attempted {THRESHOLD} or more times.")
        print("Nothing to park. If the probe is still stalling, read the error")
        print("it printed just before it stopped.")
        summary(c)
        c.close()
        return 0

    print(f"Parking {len(stuck):,} store(s) attempted {THRESHOLD}+ times:")
    print()
    for sid, name, city, att in stuck[:20]:
        print(f"  {sid:>7}  {att:>2} attempts  {name} - {city}")
    if len(stuck) > 20:
        print(f"  ... and {len(stuck) - 20:,} more")

    n = c.execute(
        "UPDATE probe SET status='failed' "
        "WHERE status IN ('pending','in_progress') AND attempts >= ?",
        (THRESHOLD,),
    ).rowcount
    c.commit()
    print()
    print(f"Parked {n:,}. Run START.bat again to carry on with the rest.")
    summary(c)
    c.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
