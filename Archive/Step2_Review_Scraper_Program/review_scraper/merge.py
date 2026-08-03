"""Merge multiple SQLite databases (e.g. from split workers) into one."""

import os
import shutil
import sqlite3
from typing import List


def merge_databases(output_path: str, input_paths: List[str]) -> None:
    if len(input_paths) < 1:
        raise ValueError("At least one input database is required")
    if not os.path.exists(input_paths[0]):
        raise FileNotFoundError(input_paths[0])

    shutil.copy2(input_paths[0], output_path)
    conn = sqlite3.connect(output_path)
    conn.execute("PRAGMA foreign_keys=OFF")
    cur = conn.cursor()

    for idx, src in enumerate(input_paths[1:], start=1):
        if not os.path.exists(src):
            print(f"Skipping missing DB: {src}")
            continue
        alias = f"db{idx}"
        cur.execute(f"ATTACH DATABASE '{src}' AS {alias}")
        cur.execute(
            f"""
            INSERT OR IGNORE INTO stores (
                input_name, input_street, input_city, input_state, input_zip,
                input_lat, input_lon, google_place_id, gmaps_name, gmaps_address,
                gmaps_phone, gmaps_website, gmaps_rating, gmaps_reviews_count,
                gmaps_lat, gmaps_lon, gmaps_category, gmaps_price_level, status,
                reviews_scraped, target_reviews, error_message
            )
            SELECT
                input_name, input_street, input_city, input_state, input_zip,
                input_lat, input_lon, google_place_id, gmaps_name, gmaps_address,
                gmaps_phone, gmaps_website, gmaps_rating, gmaps_reviews_count,
                gmaps_lat, gmaps_lon, gmaps_category, gmaps_price_level, status,
                reviews_scraped, target_reviews, error_message
            FROM {alias}.stores
            WHERE google_place_id IS NOT NULL
            """
        )
        conn.commit()

        id_map = {}
        cur.execute("SELECT google_place_id, store_id FROM stores WHERE google_place_id IS NOT NULL")
        for place_id, store_id in cur.fetchall():
            id_map[place_id] = store_id

        cur.execute(
            f"""
            SELECT s.store_id, s.google_place_id
            FROM {alias}.stores s
            JOIN {alias}.reviews r ON r.store_id = s.store_id
            GROUP BY s.store_id, s.google_place_id
            """
        )
        inserted = 0
        for src_store_id, place_id in cur.fetchall():
            dest_store_id = id_map.get(place_id)
            if not dest_store_id:
                continue
            cur.execute(
                f"""
                INSERT OR IGNORE INTO reviews (
                    store_id, review_id, reviewer_name, rating, date_relative, review_text,
                    helpful_count, photo_count, has_owner_response, reviewer_review_count,
                    reviewer_photo_count, is_local_guide, service_type
                )
                SELECT
                    ?, review_id, reviewer_name, rating, date_relative, review_text,
                    helpful_count, photo_count, has_owner_response, reviewer_review_count,
                    reviewer_photo_count, is_local_guide, service_type
                FROM {alias}.reviews WHERE store_id = ?
                """,
                (dest_store_id, src_store_id),
            )
            inserted += cur.rowcount
        conn.commit()
        print(f"Merged {inserted} review rows from {src}")
        cur.execute(f"DETACH DATABASE {alias}")

    conn.execute("PRAGMA foreign_keys=ON")
    conn.close()
