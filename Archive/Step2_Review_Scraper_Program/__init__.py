"""
Google Maps review scraper: config-driven scraping with SQLite storage,
Google account rotation, and multi-worker support.
"""

__version__ = "3.0.0"

from .auth_manager import AuthAccountManager
from .config import load_config
from .database import DatabaseManager, make_db
from .scraper import GoogleMapsScraper

__all__ = [
    "load_config",
    "DatabaseManager",
    "make_db",
    "GoogleMapsScraper",
    "AuthAccountManager",
    "__version__",
]
