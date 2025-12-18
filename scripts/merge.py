import pandas as pd
import yaml
import re
from pathlib import Path
from rules import OUT_COLS, MERGE_COLS

# Load config
CONFIG = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))
SCHEMA = yaml.safe_load(open("schema.yaml", "r", encoding="utf-8"))

PATHS = CONFIG.get("paths", {})
IN_CSV  = Path(PATHS.get("standardized_csv", "out/standardized.csv"))
OUT_CSV = Path(PATHS.get("merged_csv", "out/merged.csv"))

TABLE = "investors"  
schema_cols = SCHEMA.get("tables", {}).get(TABLE, {}).get("columns", {})
REQUIRED_COLS = [c for c, meta in schema_cols.items() if meta.get("required", False)]
PLACEHOLDER = "NOT AVAILABLE"

def clean(x) -> str:
    """Return a trimmed string; treat NaN/None/empty as ''."""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return "" if not s else s

def key_addr(x) -> str:
    """Address match key: lowercase, remove non-alphanumerics."""
    return re.sub(r"[^a-z0-9]+", "", clean(x).lower())

def key_tax(x) -> str:
    """Tax match key: digits only."""
    return re.sub(r"\D+", "", clean(x))

def key_email(x) -> str:
    """Email match key: lowercase."""
    return clean(x).lower()

# --- DSU (Union-Find) ---
class DSU:
    """Disjoint Set Union for clustering row indices."""
    def __init__(self, idx):
        self.p = {i: i for i in idx}

    def find(self, x):
        """Find cluster root with path compression."""
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        """Union two indices into the same cluster."""
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra

# --- merge logic ---
def first_nonempty(series: pd.Series) -> str:
    """First non-empty value in a Series, else ''."""
    for v in series.map(clean).tolist():
        if v:
            return v
    return ""

def merge_cluster(g: pd.DataFrame) -> dict:
    """
    Merge a cluster by selecting a single survivor row (most complete),
    then filling missing fields from other rows.
    """
    # survivor = most complete row
    scores = g.apply(lambda r: sum(1 for c in MERGE_COLS if clean(r.get(c, ""))), axis=1)
    survivor = g.loc[scores.idxmax()]

    out = {}
    for c in MERGE_COLS:
        sv = clean(survivor.get(c, ""))
        out[c] = sv if sv else first_nonempty(g[c]) if c in g.columns else ""
    return out

def merge_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cluster rows using 3-of-5 matching on:
    Phone, DOB, Address key, Tax key, Email key.
    Any shared triple puts rows into the same cluster (via union-find).
    """
    df = df.copy()
    df["_ak"] = df.get("Address Line", "").map(key_addr)
    df["_tx"] = df.get("Tax Identification Number", "").map(key_tax)
    df["_em"] = df.get("Email", "").map(key_email)

    dsu, blocks = DSU(df.index), {}

    for i, r in df.iterrows():
        keys = [
            ("PH", clean(r.get("Phone Number", ""))),
            ("DOB", clean(r.get("Date of Birth", ""))),
            ("ADDR", clean(r.get("_ak", ""))),
            ("TAX", clean(r.get("_tx", ""))),
            ("EMAIL", clean(r.get("_em", ""))),
        ]
        keys = [(k, v) for k, v in keys if v]  # keep non-empty only

        # index all 3-field combinations
        for a in range(len(keys)):
            for b in range(a + 1, len(keys)):
                for c in range(b + 1, len(keys)):
                    tag = (keys[a][0], keys[b][0], keys[c][0])
                    k = (tag, keys[a][1], keys[b][1], keys[c][1])
                    blocks.setdefault(k, []).append(i)

    # union all rows that share a triple key
    for ids in blocks.values():
        head = ids[0]
        for j in ids[1:]:
            dsu.union(head, j)

    # build clusters
    clusters = {}
    for i in df.index:
        clusters.setdefault(dsu.find(i), []).append(i)

    return pd.DataFrame([merge_cluster(df.loc[idxs]) for idxs in clusters.values()])

def apply_required_placeholders(out: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing required fields (from schema.yaml)
    with a clear placeholder for manual review.
    """
    out = out.copy()
    for c in REQUIRED_COLS:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].map(lambda v: v if clean(v) else PLACEHOLDER)
    return out

def main():
    """Read standardized CSV, merge duplicates, and write merged CSV."""
    if not IN_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {IN_CSV}")

    df = pd.read_csv(IN_CSV, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]

    # ensure expected columns exist
    for c in MERGE_COLS:
        if c not in df.columns:
            df[c] = ""

    out = merge_all(df)
    out.insert(0, "Account ID", range(1, len(out) + 1))

    for c in OUT_COLS:
        if c not in out.columns:
            out[c] = ""
    out = out[OUT_COLS]
    out = apply_required_placeholders(out)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"Wrote {OUT_CSV} ({out.shape[0]} investors)")

if __name__ == "__main__":
    main()