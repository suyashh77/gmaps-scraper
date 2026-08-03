"""
Google Maps Review Scraper — deployed to worker machines.

Usage:
    python -m review_scraper      (GUI mode — click START)
    python -m review_scraper stats DB_PATH
"""

__version__ = "4.0.0"

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
