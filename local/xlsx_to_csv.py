"""Convert the Transactions workbook sheets to CSV files.

Run from the repo root:
    uv run python local/xlsx_to_csv.py

Writes one CSV per sheet into data/, named after the (lower-cased) sheet name.
Date cells are parsed as real timestamps (not Excel serial numbers).
"""

import pandas as pd

XLSX_PATH = "data/Transactions.xlsx"
OUT_DIR = "data"


def main():
    sheets = pd.read_excel(XLSX_PATH, sheet_name=None)
    for name, df in sheets.items():
        out_path = f"{OUT_DIR}/{name.strip().lower()}.csv"
        df.to_csv(out_path, index=False)
        print(f"sheet '{name}' -> {out_path} ({len(df)} data rows)")


if __name__ == "__main__":
    main()
