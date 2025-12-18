import pandas as pd
import yaml
from pathlib import Path

# Load config
CONFIG = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))

EXCEL_INPUT = Path(CONFIG["paths"]["excel_input"])
RAW_CSV = Path(CONFIG["paths"]["raw_csv"])
HEADER_ROW = int(CONFIG.get("header_row", 1))


def ingest_excel(path: Path) -> pd.DataFrame:
    """
    Read the first valid table from an Excel file.

    - Scans sheets top to bottom
    - Uses HEADER_ROW as the header line (1-based)
    - Returns the first non-empty table found
    """
    xls = pd.ExcelFile(path)

    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)

        if df.empty or len(df) < HEADER_ROW:
            continue

        header_idx = HEADER_ROW - 1
        header = df.iloc[header_idx].fillna("").astype(str).str.strip()

        data = df.iloc[header_idx + 1 :].copy()
        data.columns = header

        # Drop empty rows and columns
        data = data.dropna(how="all").dropna(axis=1, how="all")
        return data.reset_index(drop=True)

    raise ValueError("No valid table found in Excel file")


def main():
    """
    Ingest Excel input and write raw CSV output.
    """
    if not EXCEL_INPUT.exists():
        raise FileNotFoundError(f"Excel file not found: {EXCEL_INPUT}")

    RAW_CSV.parent.mkdir(parents=True, exist_ok=True)

    df = ingest_excel(EXCEL_INPUT)
    df.to_csv(RAW_CSV, index=False)

    print(f"Ingested {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"Wrote {RAW_CSV}")


if __name__ == "__main__":
    main()