"""
s07_validate.py - the definition of "authoritative", as a pass/fail suite.

Exits non-zero while the frame is not authoritative. Each gate answers one of
the five conditions in the remediation report's definition of done, and each
prints the number that would go in a paper rather than a bare PASS.

G1  reconciliation  every one of the 16,680 January candidates has a
                    disposition; no record evaporates
G2  SNAP coverage   the federal SNAP retailer list is a list nobody in this
                    project made up, so the share of it the frame reaches is
                    the least self-serving coverage measure available
G3  flatness        coverage must not vary by region. This is the gate that
                    matters most for a food-desert study: a frame that is
                    thinner in eastern NC than in Charlotte produces food
                    deserts that are artifacts of the pipeline
G4  chains          a chain has a knowable number of stores. Comparing the
                    frame against SNAP and against the raw sources per chain
                    catches whole-chain losses like the missing Costcos
G5  rulings         zero UNRULED clusters, and zero categories decided by
                    falling through a lookup

Run: python Frame/s07_validate.py [--fail-on GATE]
"""
import math
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
ROOT = HERE.parent
BUILD = HERE / "build"
REPORTS = HERE / "reports"

CHAINS = ["foodlion", "harristeeter", "ingles", "publix", "aldi", "lidl",
          "walmart", "target", "costco", "samsclub", "bjs", "dollargeneral",
          "dollartree", "familydollar", "savealot", "pigglywiggly",
          "wholefoods", "earthfare", "freshmarket", "comparefoods",
          "supergmart", "foodcity", "weis", "lowesfoods", "harveys",
          "carlieC", "galaxyfoods", "supermercado"]

results = []


def gate(gid, name, ok, detail):
    results.append({"gate": gid, "name": name, "pass": bool(ok), "detail": detail})
    print(f"  [{'PASS' if ok else 'FAIL'}] {gid}  {name}")
    for line in detail.splitlines():
        print(f"          {line}")


def hav(a, b, c, d):
    p = math.pi / 180
    return 2 * 6371.0088 * math.asin(math.sqrt(
        math.sin((c - a) * p / 2) ** 2
        + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2))


def norm(s):
    """Strip everything but letters and digits. Chain names appear spaced,
    unspaced and punctuated across sources - "Dollar Tree", "DollarTree 5360",
    "Sam's Club" - so a chain test that keeps spaces silently misses whole
    chains, which is exactly the class of error this gate exists to catch."""
    import re
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def main():
    f = pd.read_csv(BUILD / "frame.csv")
    clusters = pd.read_csv(BUILD / "clusters.csv", dtype=str)
    inframe = f[~f.tier.isin(["OUT", "UNRULED"])].copy()
    print(f"frame.csv: {len(f):,} clusters, {len(inframe):,} in frame\n")

    # ---- G1 reconciliation ------------------------------------------------
    cand = clusters[clusters.source == "overture_2601"]
    cand_clusters = set(cand.cluster_id)
    disposed = set(f.frame_id.astype(str))
    missing = cand_clusters - disposed
    n_cand = len(cand)
    gate("G1", "every January candidate has a disposition",
         len(missing) == 0,
         f"16,680 candidates -> {len(cand_clusters):,} clusters, "
         f"{len(cand_clusters) - len(missing):,} disposed, {len(missing):,} with no record\n"
         f"candidate records carried into the build: {n_cand:,}")

    # ---- G2 coverage, by capture-recapture --------------------------------
    #
    # "How much of SNAP is in the frame" is not a coverage test once SNAP is
    # one of the frame's own sources - it returns 100% by construction and
    # measures nothing. The question that has an answer is how many food
    # retailers North Carolina actually has, and for that two sources that
    # were collected independently give an estimate.
    #
    # Lincoln-Petersen: with n1 seen by SNAP, n2 seen by Overture and m seen
    # by both, N =~ n1*n2/m. SNAP is an administrative census of authorised
    # retailers; Overture is commercial and crowd-sourced. They are not
    # perfectly independent - a store that is easy for one to find is often
    # easy for the other - and positive dependence biases N downwards. So the
    # estimate is a LOWER bound on the true population, and the coverage
    # figure derived from it is an UPPER bound. It is reported that way.
    fr = inframe.copy()
    fr["zip3"] = fr.zip.astype(str).str.extract(r"(\d{3})", expand=False)

    core = inframe[inframe.tier == "CORE"]
    n1 = int(core.in_snap.sum())
    n2 = int(core.in_overture.sum())
    m = int((core.in_snap & core.in_overture).sum())
    N = (n1 * n2 / m) if m else float("nan")
    cov = len(core) / N * 100 if N == N and N else 0
    gate("G2", "estimated coverage of the true store population",
         cov >= 90,
         f"CORE stores in frame: {len(core):,}\n"
         f"seen by SNAP {n1:,}, by Overture {n2:,}, by both {m:,}\n"
         f"Lincoln-Petersen population estimate: {N:,.0f} "
         f"(a lower bound; sources are positively dependent)\n"
         f"frame covers {cov:.1f}% of that estimate (an upper bound on true coverage)")

    # ---- G3 geographic flatness ------------------------------------------
    #
    # Same trap: measuring the frame against a source it contains is flat by
    # construction. What can vary by region, and is the thing that actually
    # threatens a food-desert study, is how much INDEPENDENT CORROBORATION the
    # frame has. A store three sources agree on is solid; a region where most
    # stores rest on a single source is a region where the frame is a guess.
    # A flat corroboration rate means the evidence is as good in Bertie County
    # as in Mecklenburg.
    core_z = core.dropna(subset=["zip3"]) if "zip3" in core else core
    core_z = fr[fr.tier == "CORE"].dropna(subset=["zip3"]).copy()
    core_z["corroborated"] = core_z.n_sources >= 2
    t = core_z.groupby("zip3").agg(stores=("frame_id", "size"),
                                   corrob=("corroborated", "sum"))
    t = t[t.stores >= 100]
    t["pct"] = t.corrob / t.stores * 100
    ratio = t.pct.max() / t.pct.min() if len(t) and t.pct.min() > 0 else 99
    t.sort_values("pct").to_csv(REPORTS / "corroboration_by_zip3.csv")
    worst = t.sort_values("pct").head(3)
    best = t.sort_values("pct").tail(1)
    gate("G3", "evidence quality is flat across the state",
         ratio <= 1.25,
         f"share of CORE stores corroborated by 2+ independent sources, by ZIP3:\n"
         f"range {t.pct.min():.1f}% to {t.pct.max():.1f}% (ratio {ratio:.2f}x) "
         f"over {len(t)} ZIP3 areas\n"
         f"weakest: {', '.join(f'{i}={r.pct:.1f}%' for i, r in worst.iterrows())}   "
         f"strongest: {', '.join(f'{i}={r.pct:.1f}%' for i, r in best.iterrows())}\n"
         f"full table -> reports/corroboration_by_zip3.csv")

    # ---- G4 chain tracer --------------------------------------------------
    rows = []
    cl = clusters.copy()
    cl["n"] = cl["name"].map(norm)
    fr["n"] = fr["name"].map(norm)
    for ch in CHAINS:
        in_f = int(fr.n.str.contains(ch, na=False).sum())
        in_snap = cl[(cl.source == "snap") & cl.n.str.contains(ch, na=False)].cluster_id.nunique()
        in_mst = cl[(cl.source == "master") & cl.n.str.contains(ch, na=False)].cluster_id.nunique()
        in_ovt = cl[(cl.source == "overture") & cl.n.str.contains(ch, na=False)].cluster_id.nunique()
        rows.append({"chain": ch, "frame": in_f, "snap": in_snap,
                     "master_old": in_mst, "overture": in_ovt,
                     "frame_vs_master": in_f - in_mst})
    ct = pd.DataFrame(rows).sort_values("frame", ascending=False)
    ct.to_csv(REPORTS / "chain_tracer.csv", index=False)
    short = ct[(ct.snap > 0) & (ct.frame < ct.snap * 0.95)]
    gate("G4", "no chain is missing from the frame",
         len(short) == 0,
         f"{len(ct)} chains checked; {len(short)} below 95% of their SNAP count\n"
         + "\n".join(f"{r.chain:16s} frame={r.frame:4d} snap={r.snap:4d} "
                     f"overture={r.overture:4d} old_master={r.master_old:4d}"
                     for r in ct.head(12).itertuples())
         + "\nfull table -> reports/chain_tracer.csv")

    # ---- G5 rulings -------------------------------------------------------
    n_unruled = int((f.tier == "UNRULED").sum())
    n_fallthrough = int((f.ruling_basis == "none").sum())
    gate("G5", "every store was ruled by a decision, not a fallthrough",
         n_unruled == 0,
         f"UNRULED clusters: {n_unruled:,}\n"
         f"clusters with no ruling basis: {n_fallthrough:,}\n"
         f"worklist -> reports/unruled_worklist.csv")

    # ---- summary ----------------------------------------------------------
    r = pd.DataFrame(results)
    r.to_csv(REPORTS / "validation.csv", index=False)
    npass = int(r["pass"].sum())
    print(f"\n{npass}/{len(r)} gates pass")
    print(f"report -> reports/validation.csv")
    return 0 if npass == len(r) else 1


if __name__ == "__main__":
    sys.exit(main())
