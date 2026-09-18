# DHAP-42 — Local CSV → Partitioned Parquet → MinIO

Airflow pipeline for Project 2 (DHAP-42). It reuses the DHAP-34
`conversational_sentiment` dataset, strictly validates the schema contract,
cleans the records, and writes partitioned Parquet objects to MinIO.

## Repo path

```text
intern-project/udhayveer-singh-jamwal/project-DHAP-42/conversational_sentiment/
```

## Project structure

```text
.
├── dags/
│   ├── extraction/
│   │   └── sample.csv
│   └── csv_to_parquet_minio.py
├── tests/
│   ├── __init__.py
│   ├── contract_validator.py
│   └── test_contract.py
├── manifest.yaml
├── schema_contract.yaml
├── docker-compose.yaml
├── .env.example
├── .gitignore
└── README.md
```

## Architecture

`sample.csv → read_csv → validate_contract → transform → write_parquet_to_minio`

The contract stage fails the DAG before the MinIO write if the incoming CSV
has missing, extra, reordered, or incompatible columns, required nulls, a
non-integer `id`, non-string text fields, or an unparseable `created_at`.

The transform stage then normalizes sentiment casing and removes invalid
curated records.

## Prerequisites

- Docker Desktop / Docker Engine
- Docker Compose

## Setup

1. Copy `.env.example` to `.env` and change local credentials if desired.

```bash
cp .env.example .env
```

2. Start the stack:

```bash
docker compose up
```

3. Airflow UI: http://localhost:8080

   Log in using `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.

4. MinIO console: http://localhost:9001

   Log in using `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`.

The `minio-init` service creates the `${MINIO_BUCKET}` bucket automatically.
The DAG uses the Airflow `minio_default` connection — an S3-style URI with
an explicit `endpoint_url` extra pointing at the MinIO container, so it
targets MinIO rather than real AWS — with environment-variable fallback if
the connection isn't configured.

## Run the DAG

In Airflow, find:

`csv_to_parquet_minio`

Trigger it manually and watch:

`read_csv → validate_contract → transform → write_parquet_to_minio`

## Expected MinIO layout

```text
dhap42/
└── conversational_sentiment/
    └── created_date=YYYY-MM-DD/
        └── part-<airflow-run-id>.parquet
```

The partition column is derived from `created_at`. A separate Parquet
object is written for each date represented by the transformed data.

## Verify the Parquet output

After a successful run, open the MinIO console and browse the `dhap42`
bucket.

For local inspection, the output is standard Parquet and can be read with
pandas/pyarrow or any S3-compatible client.

## Contract-failure test

To prove the strict contract gate, make a temporary copy of
`dags/extraction/sample.csv` and rename a column (for example `speaker` to
`speaker_bad`). Point the DAG at that file or replace the sample temporarily.

The `validate_contract` task must fail with a schema-contract error and
`write_parquet_to_minio` must not run.

Restore the original dataset afterward.

## Tests

```bash
pip install pandas pyarrow pyyaml pytest
pytest tests/ -v
```

5/5 tests passing — source dataset passes the contract; renamed/missing
columns, a bad timestamp, and a non-integer `id` are all correctly
rejected.

## Notes

- `catchup=False` is enabled.
- No secrets are hardcoded in the DAG.
- The DAG performs no network/file writes at import time.
- The MinIO bucket is initialized by the Docker Compose `minio-init` service.
- The source dataset contains 22 rows; transformation may reduce the number
  of curated rows when source values are missing/invalid.
- `pandas`, `pyarrow`, `pyyaml`, and `minio` versions are pinned in
  `docker-compose.yaml` to keep the build reproducible.
