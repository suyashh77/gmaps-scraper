#!/usr/bin/env python3
"""
Timing Report & Projection for NC Store Enrichment.

Estimates how long it would take to enrich 16,000+ NC stores based on:
  1. Observed timing data from previous runs (Excel output files), OR
  2. User-supplied avg seconds per business (--avg flag), OR
  3. Default estimates if no data is available.

Usage:
    python timing_report.py                       # Auto-detect from latest Excel output
    python timing_report.py --avg 8.5             # Manual avg seconds per business
    python timing_report.py --file data/output/NC_Test_Results_*.xlsx  # Specific file
"""
import argparse
import glob
import os
import sys
from datetime import timedelta

# ── Constants ─────────────────────────────────────────────────────────
NC_TOTAL_STORES = 16_680
DELAY_BETWEEN_BUSINESSES = 2      # seconds
DELAY_BETWEEN_PASSES = 15         # seconds
BROWSER_RESTART_EVERY = 200
BROWSER_RESTART_TIME = 10         # estimated seconds per restart
DEFAULT_BATCH_SIZE = 500


def load_timing_from_excel(filepath: str) -> dict:
    """Load timing columns from an Excel results file."""
    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas not installed. Use --avg flag instead.")
        sys.exit(1)

    df = pd.read_excel(filepath)

    # Check for timing columns
    if 'timing_total_s' not in df.columns:
        print(f"WARNING: No timing columns in {filepath}.")
        print("  This file was generated before timing instrumentation was added.")
        print("  Use --avg to supply an estimate manually.\n")
        return {}

    timings = df['timing_total_s'].dropna()
    search = df['timing_search_s'].dropna() if 'timing_search_s' in df.columns else None
    verify = df['timing_verify_s'].dropna() if 'timing_verify_s' in df.columns else None
    extract = df['timing_extract_s'].dropna() if 'timing_extract_s' in df.columns else None

    # Filter out zeros for verify/extract (means those phases were skipped)
    if verify is not None:
        verify = verify[verify > 0]
    if extract is not None:
        extract = extract[extract > 0]

    return {
        'count': len(timings),
        'avg': timings.mean(),
        'min': timings.min(),
        'max': timings.max(),
        'median': timings.median(),
        'std': timings.std(),
        'search_avg': search.mean() if search is not None and len(search) > 0 else None,
        'verify_avg': verify.mean() if verify is not None and len(verify) > 0 else None,
        'extract_avg': extract.mean() if extract is not None and len(extract) > 0 else None,
        'source': filepath,
    }


def find_latest_output() -> str | None:
    """Find the most recent NC test results Excel file."""
    pattern = os.path.join('data', 'output', 'NC_Test_Results_*.xlsx')
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def project(n: int, per_biz_s: float, batch_size: int = DEFAULT_BATCH_SIZE) -> float:
    """Calculate total projected seconds for n businesses."""
    effective = per_biz_s + DELAY_BETWEEN_BUSINESSES
    num_restarts = n // BROWSER_RESTART_EVERY
    restart_overhead = num_restarts * BROWSER_RESTART_TIME
    num_passes = (n + batch_size - 1) // batch_size
    pass_overhead = num_passes * DELAY_BETWEEN_PASSES
    return n * effective + restart_overhead + pass_overhead


def print_report(data: dict, n: int = NC_TOTAL_STORES, batch_size: int = DEFAULT_BATCH_SIZE):
    """Print the timing report with projections."""
    avg = data['avg']
    min_t = data.get('min', avg * 0.5)
    max_t = data.get('max', avg * 2.0)
    median = data.get('median', avg)

    print("=" * 75)
    print(f"  TIMING REPORT — {n:,} NC STORES PROJECTION")
    print("=" * 75)
    print()

    # Source info
    if 'source' in data:
        print(f"  Data source:         {data['source']}")
    print(f"  Businesses sampled:  {data.get('count', 'N/A')}")
    print()

    # Per-business breakdown
    print("  ── Per-Business Timing ──────────────────────────────────")
    print(f"  Average:   {avg:.2f}s")
    print(f"  Median:    {median:.2f}s")
    print(f"  Min:       {min_t:.2f}s")
    print(f"  Max:       {max_t:.2f}s")
    if data.get('std') is not None:
        print(f"  Std Dev:   {data['std']:.2f}s")
    print()

    # Phase breakdown
    if any(data.get(k) is not None for k in ('search_avg', 'verify_avg', 'extract_avg')):
        print("  ── Phase Breakdown ──────────────────────────────────────")
        if data.get('search_avg') is not None:
            print(f"  Search:    {data['search_avg']:.2f}s avg")
        if data.get('verify_avg') is not None:
            print(f"  Verify:    {data['verify_avg']:.2f}s avg")
        if data.get('extract_avg') is not None:
            print(f"  Extract:   {data['extract_avg']:.2f}s avg")
        print()

    # Overhead
    effective = avg + DELAY_BETWEEN_BUSINESSES
    num_restarts = n // BROWSER_RESTART_EVERY
    restart_overhead = num_restarts * BROWSER_RESTART_TIME
    num_passes = (n + batch_size - 1) // batch_size
    pass_overhead = num_passes * DELAY_BETWEEN_PASSES

    print("  ── Overhead ─────────────────────────────────────────────")
    print(f"  Delay between businesses:  {DELAY_BETWEEN_BUSINESSES}s")
    print(f"  Effective per business:    {effective:.2f}s")
    print(f"  Browser restarts:          ~{num_restarts:,} × {BROWSER_RESTART_TIME}s = "
          f"{timedelta(seconds=int(restart_overhead))}")
    print(f"  Pass transitions:          ~{num_passes:,} × {DELAY_BETWEEN_PASSES}s = "
          f"{timedelta(seconds=int(pass_overhead))}")
    print()

    # Projection table
    total_best = project(n, min_t, batch_size)
    total_avg = project(n, avg, batch_size)
    total_median = project(n, median, batch_size)
    total_worst = project(n, max_t, batch_size)

    print("  ── Projection ───────────────────────────────────────────")
    print()
    header = f"  {'Scenario':<14} {'Per Biz':>10} {'Total':>16} {'Hours':>10} {'Days':>8}"
    print(header)
    print(f"  {'─'*14} {'─'*10} {'─'*16} {'─'*10} {'─'*8}")

    for label, total_s, per_biz_s in [
        ('Best case', total_best, min_t),
        ('Median', total_median, median),
        ('Average', total_avg, avg),
        ('Worst case', total_worst, max_t),
    ]:
        hours = total_s / 3600
        days = total_s / 86400
        td = timedelta(seconds=int(total_s))
        print(f"  {label:<14} {per_biz_s:>8.1f}s  {str(td):>16}  {hours:>8.1f}h  {days:>6.2f}d")

    print()

    # Running 24/7 vs 8h/day
    avg_days_24_7 = total_avg / 86400
    avg_days_8h = total_avg / (8 * 3600)

    print("  ── Practical Estimates ───────────────────────────────────")
    print(f"  Running 24/7 (average):    {avg_days_24_7:.1f} days")
    print(f"  Running 8h/day (average):  {avg_days_8h:.1f} days")
    print()

    # Parallelization estimates
    print("  ── With Parallelization ─────────────────────────────────")
    for workers in [2, 3, 4]:
        parallel_s = total_avg / workers
        print(f"  {workers} parallel workers:       ~{timedelta(seconds=int(parallel_s))} "
              f"({parallel_s/3600:.1f}h / {parallel_s/86400:.1f}d)")
    print()
    print("=" * 75)


def main():
    parser = argparse.ArgumentParser(description="NC Store Enrichment Timing Report")
    parser.add_argument("--avg", type=float, help="Average seconds per business (manual input)")
    parser.add_argument("--min", type=float, help="Min seconds per business (manual input)")
    parser.add_argument("--max", type=float, help="Max seconds per business (manual input)")
    parser.add_argument("--file", type=str, help="Path to Excel results file with timing data")
    parser.add_argument("--count", type=int, default=NC_TOTAL_STORES,
                        help=f"Total stores to project for (default: {NC_TOTAL_STORES:,})")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Batch size per pass (default: {DEFAULT_BATCH_SIZE})")
    args = parser.parse_args()

    n = args.count

    if args.avg:
        # Manual timing data
        data = {
            'count': 'manual',
            'avg': args.avg,
            'min': args.min or args.avg * 0.4,
            'max': args.max or args.avg * 2.5,
            'median': args.avg,
            'source': 'manual input',
        }
    else:
        # Try to load from Excel
        filepath = args.file or find_latest_output()
        if filepath and os.path.exists(filepath):
            print(f"Loading timing data from: {filepath}\n")
            data = load_timing_from_excel(filepath)
            if not data:
                print("No timing data found. Using default estimates.")
                print("Run the enrichment with the new timing instrumentation first,")
                print("or supply --avg manually.\n")
                data = {
                    'count': 'default estimate',
                    'avg': 8.0,
                    'min': 3.0,
                    'max': 20.0,
                    'median': 7.0,
                    'source': 'default estimates (run enrichment to get real data)',
                }
        else:
            print("No Excel output files found. Using default estimates.")
            print("Run the NC enrichment first, or supply --avg manually.\n")
            data = {
                'count': 'default estimate',
                'avg': 8.0,
                'min': 3.0,
                'max': 20.0,
                'median': 7.0,
                'source': 'default estimates (run enrichment to get real data)',
            }

    print_report(data, n=n, batch_size=args.batch)


if __name__ == "__main__":
    main()
