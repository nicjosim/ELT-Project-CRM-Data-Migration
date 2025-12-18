import pandas as pd
import yaml
import re
from pathlib import Path
import rules

# Load config
CONFIG = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))
PATHS = CONFIG.get("paths", {})
IN_CSV  = Path(PATHS.get("raw_csv", "out/raw.csv"))
OUT_CSV = Path(PATHS.get("standardized_csv", "out/standardized.csv"))
COLMAP = CONFIG.get("columns", {})
DROP_LAST = CONFIG.get("drop", {}).get("strategy") == "last_row"


def clean(x, title=False):
    """
    Normalize a value to a clean string.
    - trims whitespace
    - collapses repeated spaces
    - optionally title-cases words
    """
    s = re.sub(r"\s+", " ", str(x).strip())
    if not s:
        return ""
    return " ".join(w.capitalize() for w in s.split()) if title else s


def canon(c):
    """
    Canonicalize column names for matching:
    - lowercase
    - remove punctuation
    - normalize spaces
    """
    return re.sub(
        r"\s+",
        " ",
        re.sub(r"[^a-z0-9]+", " ", str(c).strip().lower())
    ).strip()


def digits(x):
    """
    Extract digits only from a value.
    Used for phone numbers and tax identifiers.
    """
    return re.sub(r"\D+", "", clean(x))


def date_iso(x):
    """
    Parse a date and return ISO-8601 format (YYYY-MM-DD).
    Returns empty string if parsing fails.
    """
    s = clean(x)
    if not s:
        return ""
    dt = pd.to_datetime(s, errors="coerce", dayfirst=rules.DATE.get("dayfirst", True))
    return "" if pd.isna(dt) else dt.date().isoformat()


def phone_key(x):
    """
    Normalize phone numbers to a national format:
    - digits only
    - strip country prefix (61/64)
    - strip leading trunk zero
    """
    p = digits(x)
    for pref in rules.PHONE_PREFIX:
        if p.startswith(pref):
            p = p[len(pref):]
            break
    return p[1:] if p.startswith("0") and len(p) >= 9 else p


def percent(x):
    """
    Normalize percentage values:
    - accepts '0.175', '17.5%', '17.50'
    - outputs formatted percent string (e.g. '17.5%')
    """
    s = clean(x).replace(" ", "").replace("%", "")
    if not s:
        return ""
    try:
        v = float(s)
    except ValueError:
        return ""
    if rules.PERCENT.get("fraction_to_percent", True) and 0 <= v <= 1:
        v *= 100
    out = f"{v:.{rules.PERCENT.get('decimals', 2)}f}".rstrip("0").rstrip(".")
    return f"{out}%"


def country(country_val, city_val=""):
    """
    Resolve country name:
    - normalize explicit country value if present
    - otherwise infer from city using rules
    """
    c = clean(country_val)
    if c:
        m = rules.COUNTRY["map"].get(c.upper().strip())
        return m if m else " ".join(w.capitalize() for w in c.split())
    return rules.CITY_TO_COUNTRY.get(clean(city_val).lower(), "")


def address(x):
    """
    Standardize address strings:
    - normalize PO BOX
    - expand common street abbreviations
    - apply basic title-casing
    """
    t = clean(x)
    if not t:
        return ""
    for p in rules.ADDRESS["po_box_patterns"]:
        t = re.sub(p, rules.ADDRESS["po_box_replacement"], t, flags=re.I)
    ab = rules.ADDRESS["street_abbrev"]
    t = re.sub(
        r"\b(" + "|".join(map(re.escape, ab.keys())) + r")\b\.?",
        lambda m: ab.get(m.group(1).lower(), m.group(1)),
        t,
        flags=re.I,
    )
    return " ".join(
        w if any(ch.isdigit() for ch in w) else w.capitalize()
        for w in t.split()
    )


def col(df, name):
    """
    Return a column if it exists, otherwise a blank Series.
    Keeps row alignment intact.
    """
    return df[name] if name in df.columns else pd.Series([""] * len(df), index=df.index)


# --- main transforms ---
def standardize_columns(df):
    """
    Rename raw columns using config-driven canonical mapping.
    """
    return df.rename(columns={c: COLMAP.get(canon(c), str(c).strip()) for c in df.columns})


def standardize_investors(raw):
    """
    Produce a clean, standardized investors table:
    - normalize names, email, phone, dates
    - infer country and country code
    - format address and percentages
    """
    if DROP_LAST and len(raw):
        raw = raw.iloc[:-1].copy()

    city = col(raw, "City").map(lambda x: clean(x, title=True))
    ctry = col(raw, "Country").combine(city, country)

    return pd.DataFrame({
        "Account ID": "",
        "First Name": col(raw, "First Name").map(lambda x: clean(x, title=True)),
        "Last Name":  col(raw, "Last Name").map(lambda x: clean(x, title=True)),
        "Email":      col(raw, "Email").map(lambda x: clean(x).lower()),
        "Country Code": ctry.map(lambda c: rules.COUNTRY_TO_CC.get(clean(c), "")),
        "Phone Number": col(raw, "Phone Number").map(phone_key),
        "Date of Birth": col(raw, "Date of Birth").map(date_iso),
        "Address Line": col(raw, "Address Line").map(address),
        "Suburb":   col(raw, "Suburb").map(clean),
        "Postcode": col(raw, "Postcode").map(clean),
        "City":     city,
        "Country":  ctry,
        "PIR %":    col(raw, "PIR %").map(percent),
        "WHT %":    col(raw, "WHT %").map(percent),
        "Tax Identification Number": col(raw, "Tax Identification Number").map(digits),
    })


def main():
    """
    Run the standardization pipeline using paths defined in config.yaml.
    """
    if not IN_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {IN_CSV}")

    raw = pd.read_csv(IN_CSV, dtype=str, keep_default_na=False)
    out = standardize_investors(standardize_columns(raw))

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"Wrote {OUT_CSV} ({out.shape[0]} rows)")


if __name__ == "__main__":
    main()