from pathlib import Path
import pandas as pd
import pytest
from tests.contract_validator import validate_csv

ROOT = Path(__file__).parents[1]
CSV = ROOT / "dags" / "extraction" / "sample.csv"
CONTRACT = ROOT / "schema_contract.yaml"

def test_source_dataset_passes_contract():
    df = validate_csv(CSV, CONTRACT)
    assert len(df) == 22
    assert list(df.columns) == ["id","conversation_id","speaker","text","sentiment","created_at"]

@pytest.mark.parametrize("bad_name", ["speaker_bad", "text_bad"])
def test_missing_or_renamed_column_fails(tmp_path, bad_name):
    df = pd.read_csv(CSV)
    original = "speaker" if bad_name == "speaker_bad" else "text"
    df = df.rename(columns={original: bad_name})
    p = tmp_path / "bad.csv"
    df.to_csv(p, index=False)
    with pytest.raises(ValueError, match="columns"):
        validate_csv(p, CONTRACT)

def test_bad_timestamp_fails(tmp_path):
    df = pd.read_csv(CSV)
    df.loc[0, "created_at"] = "not-a-timestamp"
    p = tmp_path / "bad_timestamp.csv"
    df.to_csv(p, index=False)
    with pytest.raises(ValueError, match="created_at"):
        validate_csv(p, CONTRACT)

def test_fractional_id_fails(tmp_path):
    df = pd.read_csv(CSV)
    # Cast to object dtype before assigning a float into an int64 column —
    # newer pandas versions raise a TypeError on an in-place int->float
    # upcast instead of silently coercing, which broke this test's own
    # setup (not the validator) under pandas >= 3.0.
    df["id"] = df["id"].astype(object)
    df.loc[0, "id"] = 1.5
    p = tmp_path / "bad_id.csv"
    df.to_csv(p, index=False)
    with pytest.raises(ValueError, match="id"):
        validate_csv(p, CONTRACT)
