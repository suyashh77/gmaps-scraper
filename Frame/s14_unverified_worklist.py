"""
s14_unverified_worklist.py - the manual queue for stores the probe could not confirm.

A store lands here when the probe reached a Google page but verification said
the page is not the store we asked for. Every one comes from the ~6,648 stores
with no place_id, where navigation falls back to text search - and text search
is exactly how the original pipeline put 323 place_ids on the wrong stores.

The row keeps what Google showed AND what we asked for, side by side, plus two
links: one to the page we landed on, one to a fresh search. Fill in `ruling`:

    CORRECT   the page really is the store; the verifier was too strict
    WRONG     it is a different store; use `correct_place_id` if you found the right one
    GONE      the store does not exist any more
    UNSURE    leave for someone else

Then feed it back with --apply, which writes the accepted place_ids into
probe.db so the store counts as verified.

    python Frame/s14_unverified_worklist.py
    python Frame/s14_unverified_worklist.py --apply rulings.csv
"""
import argparse
import math
import sqlite3
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
PROBE = HERE / "build" / "probe.db"
REPORTS = HERE / "reports"


def hav(a, b, c, d):
    if any(x is None or (isinstance(x, float) and math.isnan(x)) for x in (a, b, c, d)):
        return None
    p = math.pi / 180
    return round(2 * 6371.0088 * math.asin(math.sqrt(
        math.sin((c - a) * p / 2) ** 2
        + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2)), 2)


def build(args):
    c = sqlite3.connect(PROBE)
    d = pd.read_sql("SELECT * FROM probe WHERE status='unverified'", c)
    tot = pd.read_sql("SELECT status, COUNT(*) n FROM probe GROUP BY 1", c)
    c.close()
    print(tot.to_string(index=False))
    if not len(d):
        print("\nnothing unverified yet")
        return 0

    d["km_off"] = [hav(r.lat, r.lon, r.g_lat, r.g_lon) for r in d.itertuples()]
    d["landed_on"] = ("https://www.google.com/maps/place/?q=place_id:"
                      + d.g_place_id.fillna(""))
    d.loc[d.g_place_id.isna(), "landed_on"] = ""
    q = (d["name"].fillna("") + " " + d.street.fillna("") + " "
         + d.city.fillna("") + " NC").str.strip().str.replace(r"\s+", "+", regex=True)
    d["search_again"] = "https://www.google.com/maps/search/" + q
    d["ruling"] = ""
    d["correct_place_id"] = ""
    d["note"] = ""

    cols = ["store_id", "name", "street", "city", "zip", "county",
            "food_access_type", "g_name", "g_category", "g_address",
            "g_total_reviews", "km_off", "landed_on", "search_again",
            "ruling", "correct_place_id", "note"]
    d = d[[x for x in cols if x in d.columns]]
    # worst first: a big distance is the strongest sign of a genuinely wrong store
    d = d.sort_values("km_off", ascending=False, na_position="last")

    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / "unverified_worklist.csv"
    d.to_csv(out, index=False)
    print(f"\n{len(d):,} unverified stores -> {out}")
    far = int((d.km_off > 5).sum()) if d.km_off.notna().any() else 0
    print(f"  more than 5 km from where we expected : {far:,}"
          f"   <-- almost certainly the wrong store")
    print(f"  no distance measurable                : {int(d.km_off.isna().sum()):,}")
    print("\n  worst 10:")
    for r in d.head(10).itertuples():
        print(f"    {str(r.km_off):>8s} km  asked '{str(r.name)[:26]:26s}'  "
              f"got '{str(r.g_name)[:26]:26s}'  ({str(r.g_category)[:18]})")
    return 0


def apply_rulings(args):
    r = pd.read_csv(args.apply)
    need = {"store_id", "ruling"}
    if not need <= set(r.columns):
        print(f"rulings file needs columns {need}")
        return 1
    r["ruling"] = r.ruling.astype(str).str.strip().upper()
    c = sqlite3.connect(PROBE)
    cur = c.cursor()
    n_ok = n_wrong = n_gone = n_repid = 0
    for x in r.itertuples():
        if x.ruling == "CORRECT":
            cur.execute("UPDATE probe SET status='done', verified=1 WHERE store_id=?",
                        (int(x.store_id),))
            n_ok += 1
        elif x.ruling == "WRONG":
            pid = str(getattr(x, "correct_place_id", "") or "").strip()
            if pid.startswith("ChIJ"):
                # re-queue against the id a human confirmed
                cur.execute("""UPDATE probe SET status='pending', place_id_in=?,
                               g_name=NULL, g_category=NULL, g_place_id=NULL,
                               verified=NULL, error=NULL WHERE store_id=?""",
                            (pid, int(x.store_id)))
                n_repid += 1
            else:
                cur.execute("UPDATE probe SET status='rejected_wrong_store' WHERE store_id=?",
                            (int(x.store_id),))
            n_wrong += 1
        elif x.ruling == "GONE":
            cur.execute("UPDATE probe SET status='closed_gone', g_closed=1 WHERE store_id=?",
                        (int(x.store_id),))
            n_gone += 1
    c.commit()
    c.close()
    print(f"applied {len(r):,} rulings")
    print(f"  CORRECT -> verified          {n_ok:,}")
    print(f"  WRONG   -> re-queued with id {n_repid:,}")
    print(f"  WRONG   -> rejected          {n_wrong - n_repid:,}")
    print(f"  GONE    -> closed            {n_gone:,}")
    if n_repid:
        print("\n  re-run the probe to pick up the re-queued stores:")
        print("    python Frame/probe.py --run --machine manual")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", metavar="RULINGS.CSV")
    a = p.parse_args()
    return apply_rulings(a) if a.apply else build(a)


if __name__ == "__main__":
    sys.exit(main())
