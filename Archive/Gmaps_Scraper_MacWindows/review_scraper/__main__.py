"""
Google Maps Review Scraper — entry point.

Usage:
    python -m review_scraper              Launch GUI
    python -m review_scraper stats DB     Show stats for a .db file
"""

import multiprocessing
import sys


def main() -> int:
    multiprocessing.freeze_support()

    # No args = GUI mode
    if len(sys.argv) == 1:
        from .gui import launch_gui
        launch_gui()
        return 0

    # CLI: stats command
    if len(sys.argv) >= 3 and sys.argv[1] == "stats":
        db_path = sys.argv[2]
        from . import stats as st
        if "--failed" in sys.argv:
            st.show_failed(db_path)
        elif "--sessions" in sys.argv:
            st.show_sessions(db_path)
        elif "--monitor" in sys.argv:
            st.monitor_mode(db_path)
        else:
            st.show_stats(db_path)
        return 0

    print("Usage:")
    print("  python -m review_scraper              Launch GUI")
    print("  python -m review_scraper stats DB      Show stats")
    return 1


if __name__ == "__main__":
    sys.exit(main())
