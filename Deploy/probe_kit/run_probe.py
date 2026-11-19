"""
Supervisor for the Google probe. Runs it, restarts it if it dies, stops when
the queue is empty.

The probe is resumable by construction -- every store is claimed inside a
transaction and an interrupted store is released back to pending on the next
start -- so killing this and rerunning it costs nothing but the store that was
on screen.

    python run_probe.py              # machine name = this computer's name
    python run_probe.py box2         # or name it yourself
"""
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
DB = HERE / "Frame" / "build" / "probe.db"
PROBE = HERE / "Frame" / "probe.py"
MAX_BACKOFF = 600
MAX_STALLED_PASSES = 5


def counts():
    c = sqlite3.connect(Path(DB).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        left = c.execute(
            "SELECT COUNT(*) FROM probe WHERE status IN ('pending','in_progress')"
        ).fetchone()[0]
        done = c.execute(
            "SELECT COUNT(*) FROM probe WHERE status IN ('done','unverified')"
        ).fetchone()[0]
        return left, done
    finally:
        c.close()


def stamp():
    return datetime.now().strftime("%H:%M:%S")


def main():
    if not DB.exists():
        print(f"No queue found at {DB}")
        return 1
    machine = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("COMPUTERNAME", "remote")
    machine = "".join(ch for ch in machine if ch.isalnum() or ch in "-_")[:32] or "remote"

    left, done = counts()
    print(f"[{stamp()}] machine '{machine}'  |  {done:,} probed, {left:,} to go")
    if left == 0:
        print("Queue is already drained. Nothing to do.")
        return 0

    fails = 0
    while True:
        left, done_before = counts()
        if left == 0:
            print(f"\n[{stamp()}] QUEUE DRAINED. {done_before:,} stores probed.")
            print("Run collect.bat to package the results, then copy them back.")
            return 0

        print(f"\n[{stamp()}] starting probe -- {left:,} stores left")
        rc = subprocess.run(
            [sys.executable, "-u", str(PROBE), "--run", "--machine", machine],
            cwd=str(HERE),
        ).returncode

        _, done_after = counts()
        progressed = done_after - done_before
        if progressed:
            fails = 0
        else:
            fails += 1

        if rc == 0 and progressed == 0:
            print(f"[{stamp()}] probe exited cleanly but nothing moved. Stopping so "
                  f"this does not spin. Check the output above.")
            return 0

        # A store that crashes the probe is released back to pending on the next
        # start and claimed again first, because claim() goes in store_id order.
        # Without this guard that is an infinite loop on one bad store.
        if fails >= MAX_STALLED_PASSES:
            print(f"\n[{stamp()}] {fails} passes in a row with no progress. Stopping.")
            print("One store is probably crashing the probe every time.")
            print("Run  skip_stuck.bat  to park the stores that keep failing,")
            print("then run START.bat again to carry on with the rest.")
            return 1

        wait = min(MAX_BACKOFF, 30 * max(fails, 1))
        print(f"[{stamp()}] probe exited {rc}; +{progressed:,} this pass; "
              f"restarting in {wait}s  (Ctrl-C to stop)")
        try:
            time.sleep(wait)
        except KeyboardInterrupt:
            print("\nStopped. Progress is saved -- rerun START.bat to continue.")
            return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nStopped. Progress is saved -- rerun START.bat to continue.")
        sys.exit(0)
