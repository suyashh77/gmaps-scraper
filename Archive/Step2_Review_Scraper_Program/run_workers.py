#!/usr/bin/env python3
"""Run the multi-worker Google Maps scraper (convenience wrapper)."""

import argparse
import multiprocessing
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from step2_review_extractor.worker import main

if __name__ == "__main__":
    multiprocessing.freeze_support()
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config.yaml", help="Config file")
    args = p.parse_args()
    main(config_path=args.config)
