"""
Google Maps review scraper: config-driven scraping with SQLite storage and optional multi-worker support.
"""

__version__ = "1.0.0"

from .config import load_config
from .database import DatabaseManager
from .scraper import GoogleMapsScraper

__all__ = ["load_config", "DatabaseManager", "GoogleMapsScraper", "__version__"]
