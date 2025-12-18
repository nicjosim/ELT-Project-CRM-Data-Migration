import pandas as pd
import yaml
from pathlib import Path

CONFIG = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))
SCHEMA = yaml.safe_load(open("schema.yaml", "r", encoding="utf-8"))

INVESTORS_CSV = Path(CONFIG["paths"]["investors_csv"])
REGISTRY_CSV  = Path(CONFIG["paths"]["registry_csv"])

PLACEHOLDER = "NO DATA AVAILABLE"
TABLE = "registry"

schema_cols = SCHEMA["tables"][TABLE]["columns"]
REQUIRED_COLS = [c for c, meta in schema_cols.items() if meta.get("required") is True]

def apply_required_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """
    For required columns (from schema.yaml), replace empty values with PLACEHOLDER.
    """
    df = df.copy()
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = PLACEHOLDER
        else:
            df[c] = df[c].map(lambda v: v if isinstance(v, str) and v.strip() != "" else PLACEHOLDER)
    return df

def main():
    investors = pd.read_csv(INVESTORS_CSV, dtype=str, keep_default_na=False)

    registry = pd.DataFrame({
        "Fund Name": "",
        "Investor ID": investors["Account ID"],
        "Investor Name": investors["First Name"] + " " + investors["Last Name"],
        "Transaction Date": "",
        "Unit Change": "",
        "Unit Price": "",
        "Transaction Type": "",
    })

    # Apply placeholders to required fields
    registry = apply_required_placeholders(registry)

    REGISTRY_CSV.parent.mkdir(parents=True, exist_ok=True)
    registry.to_csv(REGISTRY_CSV, index=False)
    print(f"Wrote {REGISTRY_CSV} ({len(registry)} rows)")

if __name__ == "__main__":
    main()