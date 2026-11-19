"""Probe what candidate selectors actually match inside a live review node.

Written because guessing at obfuscated Google class names is how three fields
ended up empty across 1,201,237 rows, and how the first attempt at fixing
photo_count turned "always 0" into "always 1" by matching the reviewer's avatar.
The only reliable way to pick a selector is to count what it matches on a real
page.

It drives the real GoogleMapsScraper rather than a simplified copy, so getting
to the reviews panel uses the same seven-selector ladder and expand recovery the
scraper uses. A probe that reaches the page a different way is not measuring the
same thing the scraper sees.

READING THE OUTPUT
    A selector returning the same count on every review is measuring furniture,
    not content. Constant 1 is the avatar. Constant 0 never matches. A selector
    whose count VARIES across reviews is the real signal, because some reviews
    have photos and some do not.

Usage:
    python Remediation/probe_review_dom.py --place-id ChIJ...
    python Remediation/probe_review_dom.py --place-id ChIJ... --auth Scraper_v2/google_auth_1.json
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "Scraper_v2"))

from linux_scraper.config import load_config                     # noqa: E402
from linux_scraper.scraper import GoogleMapsScraper              # noqa: E402

CANDIDATES = {
    "photo": [
        "button.Tya61d",
        "div.KtCyie button",
        "[data-photo-index]",
        "button[aria-label*='Photo' i]",
        "div.KtCyie",
        "button[jsaction*='photo']",
        "[style*='googleusercontent']",
    ],
    "helpful": [
        "button[aria-label*='helpful' i]",
        "button[jsaction*='review.thumbUp']",
        "button[aria-label*='Like' i]",
    ],
    "service": [
        "div.PBK6be",
        "div.PBK6be span.pVtsbf",
    ],
}


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--place-id", required=True)
    ap.add_argument("--reviews", type=int, default=8)
    ap.add_argument("--auth", default=os.path.join(REPO, "Scraper_v2", "google_auth_1.json"))
    args = ap.parse_args()

    cfg = load_config()
    cfg["browser"]["headless"] = True
    sc = GoogleMapsScraper(cfg, db=None, worker_id=99, machine_id="probe")

    auth = args.auth if os.path.exists(args.auth) else None
    await sc.start_browser(auth_file=auth)
    try:
        store = {
            "google_place_id": args.place_id, "gmaps_name": "", "input_name": "",
            "input_street": "", "input_city": "", "input_state": "",
            "gmaps_address": "", "input_lat": None, "input_lon": None,
            "gmaps_lat": None, "gmaps_lon": None, "status": "pending",
        }
        if not await sc.navigate_to_store(store):
            print("Could not reach a business page for that place_id.")
            return 1
        if not await sc._click_reviews_tab():
            print("Reviews tab never opened.")
            return 1
        await asyncio.sleep(2)

        nodes = await sc.page.locator("div[data-review-id]").all()
        print("review nodes mounted: " + str(len(nodes)))
        print("page: " + (await sc.page.title()) + "\n")

        totals: dict[str, list] = {}
        for i, node in enumerate(nodes[:args.reviews]):
            try:
                name = (await node.locator("div.d4r55").first.inner_text(timeout=800)).strip()
            except Exception:                                    # noqa: BLE001
                name = "?"
            has_text = False
            try:
                t = await node.locator("span.wiI7pd").first.inner_text(timeout=800)
                has_text = bool(t.strip())
            except Exception:                                    # noqa: BLE001
                pass
            line = ["review " + str(i + 1).rjust(2) + "  "
                    + name[:20].ljust(20) + ("text" if has_text else "rating-only").ljust(12)]
            for group, sels in CANDIDATES.items():
                for sel in sels:
                    try:
                        n = await node.locator(sel).count()
                    except Exception:                            # noqa: BLE001
                        n = -1
                    totals.setdefault(sel, []).append(n)
                    if n > 0:
                        line.append(group + ":" + sel.split("[")[0].split(" ")[0] + "=" + str(n))
            print("  ".join(line))

        print("\n" + "=" * 72)
        print("VERDICT")
        for sel, counts in totals.items():
            uniq = sorted(set(counts))
            if uniq == [0]:
                verdict = "never matches"
            elif len(uniq) == 1:
                verdict = "CONSTANT " + str(uniq[0]) + " - furniture, not content"
            else:
                verdict = "VARIES " + str(uniq) + " - REAL SIGNAL"
            print("  " + sel.ljust(42) + verdict)
    finally:
        await sc.close_browser()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
