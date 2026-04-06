"""
Google Maps Review Scraper (Linux) — entry point.

Usage:
    python -m linux_scraper --csv FILE.csv
    python -m linux_scraper --csv FILE.csv --db OUT.db --auth auth.json
    python -m linux_scraper --csv FILE.csv --no-headless
    python -m linux_scraper stats DB_PATH
    python -m linux_scraper stats DB_PATH --failed
    python -m linux_scraper stats DB_PATH --sessions
    python -m linux_scraper stats DB_PATH --monitor
"""

import multiprocessing
import os
import sys
import argparse


def main() -> int:
    multiprocessing.freeze_support()

    parser = argparse.ArgumentParser(
        prog="python -m linux_scraper",
        description="Google Maps Review Scraper — headless Linux version",
    )
    subparsers = parser.add_subparsers(dest="command")

    # ── scrape subcommand (default) ───────────────────────────────────────
    scrape_parser = subparsers.add_parser("scrape", help="Run the scraper")
    scrape_parser.add_argument("--csv", metavar="FILE",
                               help="Input CSV or Excel file with store locations")
    scrape_parser.add_argument("--db", metavar="FILE",
                               help="SQLite database path (pre-built or output)")
    scrape_parser.add_argument("--auth", metavar="FILE",
                               help="Playwright auth JSON file (default: google_auth_1.json)")
    scrape_parser.add_argument("--no-headless", action="store_true",
                               help="Show the browser window (requires a display)")
    scrape_parser.add_argument("--partition", metavar="N/M",
                               help="Machine partition: e.g. 1/3 = machine 1 of 3")

    # ── stats subcommand ──────────────────────────────────────────────────
    stats_parser = subparsers.add_parser("stats", help="Show stats for a .db file")
    stats_parser.add_argument("db", metavar="DB_PATH", help="Path to the SQLite database")
    stats_parser.add_argument("--failed", action="store_true", help="List failed stores")
    stats_parser.add_argument("--sessions", action="store_true", help="Show session history")
    stats_parser.add_argument("--monitor", action="store_true",
                              help="Live stats (refreshes every 60s)")

    # ── export subcommand ────────────────────────────────────────────────
    export_parser = subparsers.add_parser("export", help="Export stores needing scraping from master DB")
    export_parser.add_argument("--db", default="Step_5_master_reviews.db",
                               help="Path to master reviews database")
    export_parser.add_argument("--out", default="stores_to_scrape.xlsx",
                               help="Output Excel file path")

    # ── merge subcommand ─────────────────────────────────────────────────
    merge_parser = subparsers.add_parser("merge", help="Merge scraper result DBs into master")
    merge_parser.add_argument("scraper_dbs", nargs="+", metavar="DB",
                              help="Scraper result .db files to merge")
    merge_parser.add_argument("--master", default="Step_5_master_reviews.db",
                              help="Path to master reviews database")
    merge_parser.add_argument("--dry-run", action="store_true",
                              help="Show what would be merged without writing")

    # ── prepare subcommand ───────────────────────────────────────────────
    prep_parser = subparsers.add_parser("prepare",
                                        help="Build scraper DB from store list + master reviews")
    prep_parser.add_argument("--stores", required=True,
                             help="Excel/CSV file with stores to scrape")
    prep_parser.add_argument("--master", required=True,
                             help="Path to master reviews DB (for existing reviews)")
    prep_parser.add_argument("--out", help="Output DB path (default: based on input filename)")

    # ── convenience: bare --csv / --db without subcommand ──────────────────
    # Allow: python -m linux_scraper --csv FILE  or  --db FILE
    parser.add_argument("--csv", metavar="FILE",
                        help="Shorthand: run scraper with this CSV file")
    parser.add_argument("--db", metavar="FILE",
                        help="Database path (pre-built from prepare_machine_db, or output)")
    parser.add_argument("--auth", metavar="FILE",
                        help="Auth JSON file")
    parser.add_argument("--no-headless", action="store_true",
                        help="Show browser window")
    parser.add_argument("--partition", metavar="N/M",
                        help="Machine partition: e.g. 1/3 = machine 1 of 3")

    args = parser.parse_args()

    # Resolve effective command
    if args.command == "prepare":
        from .prepare_machine_db import prepare
        out = args.out
        if not out:
            base = os.path.splitext(os.path.basename(args.stores))[0]
            out = f"{base}_scraper.db"
        prepare(args.stores, args.master, out)
        return 0

    if args.command == "export":
        from .export_for_scraping import export
        export(args.db, args.out)
        return 0

    if args.command == "merge":
        from .merge_results import main as merge_main
        # Re-parse with merge's own parser
        sys.argv = ["merge"] + args.scraper_dbs
        if args.master != "Step_5_master_reviews.db":
            sys.argv += ["--master", args.master]
        if args.dry_run:
            sys.argv += ["--dry-run"]
        merge_main()
        return 0

    if args.command == "stats":
        from . import stats as st
        if args.failed:
            st.show_failed(args.db)
        elif args.sessions:
            st.show_sessions(args.db)
        elif args.monitor:
            st.monitor_mode(args.db)
        else:
            st.show_stats(args.db)
        return 0

    # scrape subcommand or --csv/--db shorthand
    csv_file = None
    db_file = None
    auth_file = None
    no_headless = False
    partition_str = None

    if args.command == "scrape":
        csv_file = args.csv
        db_file = args.db
        auth_file = args.auth
        no_headless = args.no_headless
        partition_str = args.partition
    elif args.csv or args.db:
        csv_file = args.csv
        db_file = args.db
        auth_file = args.auth
        no_headless = args.no_headless
        partition_str = args.partition
    else:
        parser.print_help()
        return 1

    if not csv_file and not db_file:
        print("ERROR: Provide --csv FILE and/or --db FILE")
        return 1

    # Parse partition: "1/3" → (1, 3)
    partition = None
    if partition_str:
        try:
            n, m = partition_str.split("/")
            partition = (int(n), int(m))
            if not (1 <= partition[0] <= partition[1]):
                print(f"ERROR: --partition {partition_str} invalid. N must be 1..M (e.g. 1/3, 2/3, 3/3)")
                return 1
        except ValueError:
            print(f"ERROR: --partition must be N/M format (e.g. 1/3)")
            return 1

    from .cli import run_scraper
    return run_scraper(
        csv_path=csv_file,
        db_path=db_file,
        auth_path=auth_file,
        headless=not no_headless,
        partition=partition,
    )


if __name__ == "__main__":
    sys.exit(main())
