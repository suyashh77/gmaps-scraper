"""
Google Maps Review Scraper — headless Linux deployment.

Usage:
    python -m linux_scraper --csv FILE.csv
    python -m linux_scraper stats DB_PATH
"""

__version__ = "4.0.0-linux"

# Lazy imports — heavy dependencies (playwright, pandas) are only loaded when
# actually needed.  This allows the CLI to start and show a helpful error
# instead of an ImportError traceback if pip install was incomplete.

__all__ = [
    "load_config",
    "DatabaseManager",
    "make_db",
    "GoogleMapsScraper",
    "AuthAccountManager",
    "__version__",
]


def __getattr__(name: str):
    if name == "AuthAccountManager":
        from .auth_manager import AuthAccountManager
        return AuthAccountManager
    if name == "load_config":
        from .config import load_config
        return load_config
    if name == "DatabaseManager":
        from .database import DatabaseManager
        return DatabaseManager
    if name == "make_db":
        from .database import make_db
        return make_db
    if name == "GoogleMapsScraper":
        from .scraper import GoogleMapsScraper
        return GoogleMapsScraper
    raise AttributeError(f"module 'linux_scraper' has no attribute {name!r}")
