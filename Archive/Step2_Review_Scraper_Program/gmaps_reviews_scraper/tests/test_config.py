"""Unit tests for config loading and overrides."""

from __future__ import annotations

import os
import sys
import textwrap
import unittest
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gmaps_reviews_scraper.config import load_config  # noqa: E402


class ConfigTests(unittest.TestCase):
    def test_yaml_and_cli_override(self) -> None:
        local_tmp_root = PROJECT_ROOT / ".tmp_test_artifacts"
        local_tmp_root.mkdir(parents=True, exist_ok=True)
        test_dir = local_tmp_root / f"config_{uuid.uuid4().hex}"
        test_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = test_dir / "config.yaml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                browser:
                  headless: false
                scraping:
                  max_reviews_per_query: 800
                logging:
                  level: DEBUG
                """
            ).strip(),
            encoding="utf-8",
        )

        config = load_config(
            config_path=str(cfg_path),
            cli_overrides={"browser": {"headless": True}},
        )

        self.assertTrue(config["browser"]["headless"])
        self.assertEqual(config["scraping"]["max_reviews_per_query"], 800)
        self.assertEqual(config["logging"]["level"], "DEBUG")

    def test_env_override_headless(self) -> None:
        old = os.environ.get("GMAPS_HEADLESS")
        try:
            os.environ["GMAPS_HEADLESS"] = "false"
            config = load_config(config_path="nonexistent-config.yaml")
            self.assertFalse(config["browser"]["headless"])
        finally:
            if old is None:
                os.environ.pop("GMAPS_HEADLESS", None)
            else:
                os.environ["GMAPS_HEADLESS"] = old

    def test_validation_fails_on_invalid_cap(self) -> None:
        with self.assertRaises(ValueError):
            load_config(
                config_path="nonexistent-config.yaml",
                cli_overrides={"scraping": {"max_reviews_per_query": 5000}},
            )


if __name__ == "__main__":
    unittest.main()
