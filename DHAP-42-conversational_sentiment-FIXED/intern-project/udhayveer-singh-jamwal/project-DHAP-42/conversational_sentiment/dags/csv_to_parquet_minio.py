"""
DHAP-42 — Local CSV -> validate -> transform -> partitioned Parquet -> MinIO.

The contract is deliberately strict on:
- exact column names and order
- required/non-null columns
- integer `id`
- string columns
- parseable `created_at` timestamps

The source dataset is reused from DHAP-34. Transformation normalizes text/sentiment
and drops rows that cannot be safely represented in the curated Parquet output.
"""

from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime

import pandas as pd
import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from minio import Minio
from minio.error import S3Error

DATA_SOURCE_DIR = "/opt/airflow/data_source"
CSV_PATH = os.path.join(DATA_SOURCE_DIR, "dags", "extraction", "sample.csv")
CONTRACT_PATH = os.path.join(DATA_SOURCE_DIR, "schema_contract.yaml")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "dhap42")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "conversational_sentiment")

EXPECTED_COLUMNS = [
    "id", "conversation_id", "speaker", "text", "sentiment", "created_at"
]


def _minio_client() -> Minio:
    # Prefer the Airflow connection when configured; env vars remain a local fallback.
    # The connection is an S3-style URI with an explicit `endpoint_url` extra
    # (aws://key:secret@?endpoint_url=http%3A%2F%2Fminio%3A9000) so MinIO,
    # not real AWS, is targeted — read it from `extra_dejson` rather than
    # `conn.host`/`conn.port`, which are empty for this URI shape.
    try:
        conn = BaseHook.get_connection("minio_default")
        extra = conn.extra_dejson or {}
        endpoint_url = extra.get("endpoint_url", "")
        if endpoint_url:
            endpoint = endpoint_url.split("://", 1)[-1]
            secure = endpoint_url.startswith("https://")
        else:
            endpoint = conn.host or MINIO_ENDPOINT.split(":", 1)[0]
            port = conn.port or (int(MINIO_ENDPOINT.split(":", 1)[1])
                                  if ":" in MINIO_ENDPOINT else 9000)
            endpoint = f"{endpoint}:{port}" if port else endpoint
            secure = MINIO_SECURE
        access_key = conn.login or MINIO_ACCESS_KEY
        secret_key = conn.password or MINIO_SECRET_KEY
    except Exception:
        endpoint = MINIO_ENDPOINT
        access_key = MINIO_ACCESS_KEY
        secret_key = MINIO_SECRET_KEY
        secure = MINIO_SECURE

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def read_csv_task(**context):
    df = pd.read_csv(CSV_PATH)
    if df.empty:
        raise ValueError("Input CSV is empty.")
    context["ti"].xcom_push(key="raw_records", value=df.to_json(orient="records"))
    print(f"Read {len(df)} rows from {CSV_PATH}")


def validate_contract_task(**context):
    raw = context["ti"].xcom_pull(key="raw_records", task_ids="read_csv")
    if not raw:
        raise ValueError("No records received from read_csv.")
    df = pd.DataFrame(json.loads(raw))

    with open(CONTRACT_PATH, encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    contract_cols = [c["name"] for c in contract["columns"]]
    actual_cols = list(df.columns)

    # Strict: extra, missing, or reordered columns are contract violations.
    if actual_cols != contract_cols:
        raise ValueError(
            "Schema contract failed: expected columns exactly "
            f"{contract_cols}, received {actual_cols}"
        )

    for col_def in contract["columns"]:
        col = col_def["name"]
        if not col_def.get("nullable", True) and df[col].isna().any():
            raise ValueError(
                f"Schema contract failed: required column '{col}' contains null values."
            )

    # Strict logical type checks. CSV strings are parsed only for timestamp fields.
    if not pd.api.types.is_integer_dtype(df["id"]):
        # Reject values that are not integer-valued; do not silently coerce arbitrary text.
        numeric = pd.to_numeric(df["id"], errors="coerce")
        if numeric.isna().any() or (numeric % 1 != 0).any():
            raise ValueError("Schema contract failed: 'id' must contain integers.")

    for col in ("conversation_id", "speaker", "text", "sentiment"):
        non_null = df[col].dropna()
        if not all(isinstance(v, str) for v in non_null):
            raise ValueError(
                f"Schema contract failed: '{col}' must contain string values."
            )

    parsed = pd.to_datetime(df["created_at"], errors="coerce")
    if parsed.isna().any():
        bad = df.loc[parsed.isna(), "created_at"].tolist()
        raise ValueError(
            "Schema contract failed: 'created_at' contains unparseable timestamps: "
            f"{bad}"
        )

    # Preserve source values for the transform stage; timestamp parse is verified above.
    context["ti"].xcom_push(key="validated_records", value=df.to_json(orient="records"))
    print("Schema contract passed.")


def transform_task(**context):
    raw = context["ti"].xcom_pull(
        key="validated_records", task_ids="validate_contract"
    )
    df = pd.DataFrame(json.loads(raw))
    before = len(df)

    df["conversation_id"] = df["conversation_id"].astype("string").str.strip()
    df["speaker"] = df["speaker"].astype("string").str.strip()
    df["text"] = df["text"].astype("string").str.strip()
    df["sentiment"] = df["sentiment"].astype("string").str.strip().str.lower()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="raise")

    # Curated sink: required text fields must be non-empty and sentiment must be valid.
    df = df[df["conversation_id"].notna() & (df["conversation_id"] != "")]
    df = df[df["speaker"].notna() & (df["speaker"] != "")]
    df = df[df["text"].notna() & (df["text"] != "")]
    df = df[df["sentiment"].isin({"positive", "negative", "neutral"})]
    df = df.drop_duplicates(subset=["id"], keep="last")

    # Partition column requested by the brief: a date derived from created_at.
    df["created_date"] = df["created_at"].dt.strftime("%Y-%m-%d")

    if df.empty:
        raise ValueError("Transform produced zero valid rows; nothing to write.")

    context["ti"].xcom_push(key="clean_records", value=df.to_json(orient="records"))
    print(
        f"Transform complete: {before} rows in, {len(df)} rows out, "
        f"{before - len(df)} dropped."
    )


def write_parquet_to_minio_task(**context):
    raw = context["ti"].xcom_pull(key="clean_records", task_ids="transform")
    df = pd.DataFrame(json.loads(raw))
    df["created_at"] = pd.to_datetime(df["created_at"])

    client = _minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    # Write one Parquet object per date partition:
    # bucket/conversational_sentiment/created_date=YYYY-MM-DD/part-*.parquet
    for partition_value, part_df in df.groupby("created_date", sort=True):
        object_name = (
            f"{MINIO_PREFIX}/created_date={partition_value}/"
            f"part-{re.sub(r'[^A-Za-z0-9_.-]', '_', context['run_id'])}.parquet"
        )
        payload = io.BytesIO()
        part_df.drop(columns=["created_date"]).to_parquet(
            payload, index=False, engine="pyarrow"
        )
        payload.seek(0)
        client.put_object(
            MINIO_BUCKET,
            object_name,
            payload,
            length=payload.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        print(f"Wrote {len(part_df)} rows -> s3://{MINIO_BUCKET}/{object_name}")

    print(
        f"MinIO write complete: {len(df)} rows across "
        f"{df['created_date'].nunique()} date partitions."
    )


default_args = {"owner": "de_intern", "retries": 1}

with DAG(
    dag_id="csv_to_parquet_minio",
    description="DHAP-42: CSV -> strict contract -> transform -> partitioned Parquet -> MinIO",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["intern-project", "DHAP-42", "minio", "parquet"],
) as dag:
    read_csv = PythonOperator(task_id="read_csv", python_callable=read_csv_task)
    validate_contract = PythonOperator(
        task_id="validate_contract", python_callable=validate_contract_task
    )
    transform = PythonOperator(task_id="transform", python_callable=transform_task)
    write_parquet = PythonOperator(
        task_id="write_parquet_to_minio",
        python_callable=write_parquet_to_minio_task,
    )

    read_csv >> validate_contract >> transform >> write_parquet
