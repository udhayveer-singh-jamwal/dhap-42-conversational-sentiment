"""Small, dependency-light contract validator used by tests and DAG review."""
from pathlib import Path
import pandas as pd
import yaml

EXPECTED_COLUMNS = ["id","conversation_id","speaker","text","sentiment","created_at"]

def validate_csv(csv_path, contract_path):
    df = pd.read_csv(csv_path)
    contract = yaml.safe_load(Path(contract_path).read_text(encoding="utf-8"))
    expected = [c["name"] for c in contract["columns"]]
    if list(df.columns) != expected:
        raise ValueError("schema contract failed: columns")
    for c in contract["columns"]:
        if not c.get("nullable", True) and df[c["name"]].isna().any():
            raise ValueError(f"schema contract failed: nulls in {c['name']}")
    if not pd.api.types.is_integer_dtype(df["id"]):
        n = pd.to_numeric(df["id"], errors="coerce")
        if n.isna().any() or (n % 1 != 0).any():
            raise ValueError("schema contract failed: id")
    for c in ("conversation_id","speaker","text","sentiment"):
        if not all(isinstance(v, str) for v in df[c].dropna()):
            raise ValueError(f"schema contract failed: {c}")
    parsed = pd.to_datetime(df["created_at"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if parsed.isna().any():
        raise ValueError("schema contract failed: created_at")
    return df
