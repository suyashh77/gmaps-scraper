"""Split input Excel/CSV into N parts for parallel worker input."""

import math
import os
import pandas as pd


def split_input(input_file: str, num_parts: int) -> None:
    if num_parts <= 0:
        raise ValueError("num_parts must be positive")
    if not os.path.exists(input_file):
        raise FileNotFoundError(input_file)

    ext = os.path.splitext(input_file)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(input_file)
    elif ext in (".xls", ".xlsx"):
        df = pd.read_excel(input_file)
    else:
        raise ValueError(f"Unsupported input type: {ext}")

    total_rows = len(df)
    chunk_size = math.ceil(total_rows / num_parts) if total_rows else 0
    base = os.path.splitext(os.path.basename(input_file))[0]
    folder = os.path.dirname(input_file) or "."

    for i in range(num_parts):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, total_rows)
        if start >= total_rows:
            break
        output = os.path.join(folder, f"{base}_part{i + 1}.xlsx")
        df.iloc[start:end].to_excel(output, index=False)
        print(f"Created {output}: rows {start + 1}-{end}")
