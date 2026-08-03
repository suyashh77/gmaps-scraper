# Step 1.9 — Excluded Stores Analysis

> Frozen step-specific analysis from 2026-04-06. For dataset-wide quality issues, see [Data Quality Report.md](Data%20Quality%20Report.md).

- Source file: `Data/Step_1.9_Excluded_Stores.xlsx`
- Merged rows: **8,199**
- Merged columns: **45**
- Deduplicated store rows: **7,497**
- Compression ratio (`merged/dedup`): **1.094**
- Category conflicts at store level: **16**
- Status conflicts at store level: **0**

## Exclusion Reason Breakdown

| exclusion_reason | rows | percent |
| --- | --- | --- |
| CATEGORY_NON_GROCERY | 6,445 | 78.61% |
| MANUAL_REVIEW_UNMATCHED | 1,605 | 19.58% |
| CLOSED_STORE | 149 | 1.82% |

## Sheet-Level Profile

| source_sheet | rows | unique_place_ids | missing_place_id | duplicate_place_id_rows |
| --- | --- | --- | --- | --- |
| Excluded_Category_Part1 | 2,671 | 2,552 | 0 | 234 |
| Excluded_Category_Part2 | 3,774 | 3,710 | 64 | 64 |
| Excluded_Closed_Part1 | 23 | 12 | 11 | 11 |
| Excluded_Closed_Part2 | 126 | 126 | 0 | 0 |
| Excluded_Manual_Review | 1,605 | 1,486 | 1 | 225 |

## Dedup Group Size Distribution

| records_merged_count | store_count | percent_of_dedup_stores |
| --- | --- | --- |
| 1 | 6,888 | 91.88% |
| 2 | 532 | 7.10% |
| 3 | 62 | 0.83% |
| 4 | 14 | 0.19% |
| 5 | 1 | 0.01% |

## Largest Dedup Groups

| store_key | records_merged_count | sample_name | source_sheets |
| --- | --- | --- | --- |
| place_id:ChIJYc_yCuf1q4kR_XaGn-bI0Q4 | 5 | Cox & Cox Farms | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJxVCmVnn1rIkRsADIqUY_PM4 | 4 | The Evening Farmers Market | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJs58kNLCPr4kRG-akd1fVD8o | 4 | Edenton Farmers Market | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJuS6yWOCZqIkR3v7_uO4k7YM | 4 | Salty Catch Seafood Company | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJv-tLYnn1rIkRK7XVyy1Aofs | 4 | Gene's Produce | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJ_ZPfky4lpYkRNC0xZYMMBHQ | 4 | Community Market in Grandy | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJ67gAF-2MWYgRpOxoXe5xnyk | 4 | Foothills Butcher Bar | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJGZ4dvNCYVogR3T3YoD4oTKs | 4 | Farmers Market | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJN6XdbQySrokRozM97zW44Sc | 4 | Chloe’s Farm Stand | Excluded_Manual_Review |
| place_id:ChIJIa6pEHn1rIkRk9xS-ZER4Jk | 4 | Quality Produce | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJqxSoJCL5WYgR66tTCqBZS7Q | 4 | Swannanoa Farmers Market | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJl2Krp3UkVIgRnA6hi2rjWdU | 4 | Biltong USA | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJsd_1Fit1r4kRNVsPwie8f1Q | 4 | Bay Brothers Seafood | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJlU93OhkoV4gRvDQ0G6b8FB0 | 4 | South Mountain Food Outlet | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJQz4hMvm1qIkRXSz-piq805E | 4 | 7 Even | Excluded_Manual_Review |
| place_id:ChIJicqTjJiGrIkRItTfLN18yMc | 3 | Carolina Meats & Etc. | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJaw_omHufVogRGfz5PKNUoGs | 3 | Carolinas Foods | Excluded_Category_Part1 \| Excluded_Category_Part2 \| Excluded_Manual_Review |
| place_id:ChIJx3JhFCXnqokRR908f3GZxXk | 3 | Sun-Do Kwik Shop | Excluded_Category_Part1 \| Excluded_Category_Part2 |
| place_id:ChIJv6TV-wakqYkRrJID8_I1KwA | 3 | Andrew's Produce Stand | Excluded_Category_Part1 \| Excluded_Manual_Review |
| place_id:ChIJhTKdmRjBVogRBEkD84R9ucc | 3 | Gaston International Market | Excluded_Category_Part1 \| Excluded_Manual_Review |

## Key Null Rates

| column | null_rows | null_rate |
| --- | --- | --- |
| google_place_id | 76 | 0.93% |
| source_business_name | 14 | 0.17% |
| source_street | 139 | 1.70% |
| source_city | 41 | 0.50% |
| source_zip | 137 | 1.67% |
| google_category | 273 | 3.33% |
| google_business_status | 3,786 | 46.18% |

## Text Cleanup Impact

- Mojibake hits before cleaning: **99**
- Mojibake hits after cleaning: **0**
- Text cells normalized: **105**
- Mojibake cells repaired: **99**
- Rows touched by mojibake repair: **99**

## Deliverables

- Merged table (Excel): `Data/Step_1.9_Excluded_Stores_Merged_Table.xlsx`
- Merged table (CSV): `Data/Step_1.9_Excluded_Stores_Merged_Table.csv`
- Deduplicated table (Excel): `Data/Step_1.9_Excluded_Stores_Deduplicated_Store_Level.xlsx`
- Deduplicated table (CSV): `Data/Step_1.9_Excluded_Stores_Deduplicated_Store_Level.csv`
- Analysis report: `Data/Step_1.9_Excluded_Stores_Analysis.md`
