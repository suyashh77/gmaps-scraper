"""
export_split_results.py
=======================
Exports two Excel files from enrichment.db:
  1. successful.xlsx  — status='success' only (duplicates excluded)
  2. unsuccessful.xlsx — status IN ('not_found', 'failed') (duplicates excluded)
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_PATH = os.path.join("data", "output", "enrichment.db")
OUT_DIR = os.path.join("data", "output")

# ── column aliases ────────────────────────────────────────────────────────────

SUCCESS_QUERY = """
SELECT
    id,
    input_name          AS business_name,
    input_street        AS street,
    input_city          AS city,
    input_state         AS state,
    input_zip           AS zip,
    input_lat           AS source_lat,
    input_lon           AS source_lon,
    google_place_id     AS place_id,
    gmaps_name          AS gmaps_name,
    gmaps_address       AS gmaps_address,
    gmaps_phone         AS phone,
    gmaps_website       AS website,
    gmaps_rating        AS rating,
    gmaps_reviews_count AS reviews,
    gmaps_lat           AS gmaps_lat,
    gmaps_lon           AS gmaps_lon,
    gmaps_category      AS category,
    gmaps_price_level   AS price_level,
    gmaps_hours         AS hours,
    gmaps_url           AS gmaps_url,
    created_at,
    updated_at
FROM supermarkets
WHERE status = 'success'
ORDER BY input_state, input_city, input_name
"""

UNSUCCESSFUL_QUERY = """
SELECT
    id,
    input_name   AS business_name,
    input_street AS street,
    input_city   AS city,
    input_state  AS state,
    input_zip    AS zip,
    input_lat    AS source_lat,
    input_lon    AS source_lon,
    status,
    error_message,
    created_at,
    updated_at
FROM supermarkets
WHERE status IN ('not_found', 'failed')
ORDER BY input_state, input_city, input_name
"""


def write_excel(df: pd.DataFrame, path: str, sheet_name: str) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        # Auto-fit column widths (capped at 60)
        for col_cells in ws.columns:
            max_len = max(
                (len(str(c.value)) for c in col_cells if c.value is not None),
                default=10,
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 60)


def main() -> None:
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    print("Reading database...")
    df_success      = pd.read_sql_query(SUCCESS_QUERY,      conn)
    df_unsuccessful = pd.read_sql_query(UNSUCCESSFUL_QUERY, conn)
    conn.close()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    success_path      = os.path.join(OUT_DIR, f"successful_{timestamp}.xlsx")
    unsuccessful_path = os.path.join(OUT_DIR, f"unsuccessful_{timestamp}.xlsx")

    print(f"Writing successful ({len(df_success):,} records)...")
    write_excel(df_success, success_path, "Successful")

    print(f"Writing unsuccessful ({len(df_unsuccessful):,} records)...")
    write_excel(df_unsuccessful, unsuccessful_path, "Unsuccessful")

    # ── summary ───────────────────────────────────────────────────────────────
    print("\n=== Export complete ===")
    print(f"  Successful   -> {os.path.abspath(success_path)}")
    print(f"                  {len(df_success):,} records")
    print(f"  Unsuccessful -> {os.path.abspath(unsuccessful_path)}")
    print(f"                  {len(df_unsuccessful):,} records")

    if not df_success.empty:
        print(f"\n  Field coverage (successful records):")
        skip = {"id", "created_at", "updated_at", "business_name", "street",
                "city", "state", "zip", "source_lat", "source_lon"}
        for col in df_success.columns:
            if col in skip:
                continue
            filled = df_success[col].notna().sum()
            pct    = filled / len(df_success) * 100
            print(f"    {col:<20} {filled:>7,}  ({pct:.1f}%)")


if __name__ == "__main__":
    main()
