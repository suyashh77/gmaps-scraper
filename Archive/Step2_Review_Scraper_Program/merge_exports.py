"""
Re-merges all successful enrichment spreadsheets into one deduplicated file.
Also deletes the old source files afterwards.
"""
import os
import glob
import pandas as pd

exports_dir = "data/exports"
unsuccessful_file = "Enrich_Unsuccessful_6531_Stores.xlsx"
output_file = "Enrichment_Successful.xlsx"
output_path = os.path.join(exports_dir, output_file)

all_files = glob.glob(os.path.join(exports_dir, "*.xlsx"))

# Exclude unsuccessful file, the output file itself, and temp lock files
exclude = {unsuccessful_file, output_file}
files_to_merge = [
    f for f in all_files
    if os.path.basename(f) not in exclude
    and not os.path.basename(f).startswith("~$")
]

print(f"Found {len(files_to_merge)} files to merge:")
for f in files_to_merge:
    print(f"  - {os.path.basename(f)}")

dfs = []
for f in files_to_merge:
    try:
        df = pd.read_excel(f)
        dfs.append(df)
        print(f"Read {os.path.basename(f)}: {len(df)} rows, columns: {list(df.columns[:5])}...")
    except Exception as e:
        print(f"ERROR reading {os.path.basename(f)}: {e}")

if not dfs:
    print("No data to merge.")
    exit(1)

combined = pd.concat(dfs, ignore_index=True)
initial = len(combined)
print(f"\nTotal rows before deduplication: {initial}")

# Deduplicate by google_place_id if available, else by name+address
if 'google_place_id' in combined.columns:
    combined = combined.drop_duplicates(subset=['google_place_id'], keep='first')
    print(f"After dedup by 'google_place_id': {len(combined)} rows")
elif 'input_name' in combined.columns and 'input_street' in combined.columns:
    combined = combined.drop_duplicates(subset=['input_name', 'input_street'], keep='first')
    print(f"After dedup by name+street: {len(combined)} rows")
else:
    combined = combined.drop_duplicates(keep='first')
    print(f"After generic dedup: {len(combined)} rows")

print(f"Rows removed: {initial - len(combined)}")

combined.to_excel(output_path, index=False)
print(f"\nSaved to: {output_path} ({len(combined)} rows)")
print(f"Columns: {list(combined.columns)}")

# Print category stats if possible
for cat_col in ['gmaps_category', 'category', 'type']:
    if cat_col in combined.columns:
        cats = combined[cat_col].dropna().unique()
        print(f"\nGMaps categories found in column '{cat_col}': {len(cats)}")
        break

# Now clean up old files
print("\nCleaning up old files...")
for f in files_to_merge:
    try:
        os.remove(f)
        print(f"  Deleted: {os.path.basename(f)}")
    except Exception as e:
        print(f"  Could not delete {os.path.basename(f)}: {e} (close it in Excel first)")
