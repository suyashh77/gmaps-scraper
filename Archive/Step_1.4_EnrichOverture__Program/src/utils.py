"""
src/utils.py
============
Shared utility helpers used across the EnrichOverture project.

Currently provides a single factory function, ``setup_logger``, which
configures a named logger that writes to both the shared log file and
stdout.  All runner scripts and worker processes call this function so
that every log line ends up in the same ``logs/app.log`` file.
"""

import logging
import sys

from config.settings import LOG_FILE


def setup_logger(name: str) -> logging.Logger:
    """
    Create (or retrieve) a named logger with file + console handlers.

    The logger is idempotent — calling this function twice with the same
    ``name`` returns the same logger without adding duplicate handlers.

    Args:
        name: Logical name for the logger (e.g. ``"NC_Enrichment"``).

    Returns:
        A fully configured :class:`logging.Logger` instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Guard: only add handlers once per logger name
    if logger.handlers:
        return logger

    # ------------------------------------------------------------------
    # File handler — DEBUG and above → logs/app.log
    # ------------------------------------------------------------------
    try:
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(file_handler)
    except Exception as exc:
        print(f"[utils] Could not open log file '{LOG_FILE}': {exc}")

    # ------------------------------------------------------------------
    # Console handler — INFO and above → stdout
    # ------------------------------------------------------------------
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console_handler)

    return logger
