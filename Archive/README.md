# Archive — Superseded implementations

This folder holds earlier versions and approaches that have been replaced by newer pipeline stages. Nothing here is imported by live code.

## Contents

- **Scraper_v1/** — First-generation review scraper (pre-place_id era, Apr 2026). Replaced by Scraper_v2, which restores place_id navigation and verification, fixes DOM selectors, and runs idempotent deduplication.

- **Gmaps_Scraper_MacWindows/** — Tkinter GUI for classmates to run scraping locally. Early prototype for volunteer deployment; replaced by Deploy/ kits (review_kit, probe_kit) with standardized setup and validation.

- **Step2_Review_Scraper_Program/** — Four earlier review-scraper architectures (Apr 2026). Replaced by Scraper_v2 and the repair summary in Remediation §5 and §8.

- **Step_1.4_EnrichOverture__Program/** — Batch GMaps enrichment of Overture POIs (Jan 2026). Its `verify_business` function accepted matches on both branches, inert as a gate. Replaced by Frame/probe.py, which performs live verification of place_id + name + distance on both landing and navigation.
