# Big Data Framework

Roy Homsi — ECE Paris

This project contains my work for the uv, S3 and DuckDB labs. I generated fictional users and orders in Python, uploaded them to the bronze layer in S3, then used DuckDB to query the data and compare CSV with Parquet.

## Labs

- [uv: Python project and dataset generation](labs/lab-01-uv.md)
- [S3: object storage and Kubernetes ingestion](labs/lab-02-s3.md)
- [DuckDB: SQL analytics and Parquet](labs/lab-03-duckdb.md)

## Usage

The project uses Python 3.13 and uv. Dependencies are recorded in `uv.lock`.

```bash
uv sync --locked
uv run dataset-users -h
uv run dataset-orders -h
uv run dataset-users -c 2 -o jsonline
uv run dataset-orders -u 2 -C 1 -c 2 -o json
```

The generators support CSV, JSON and JSON Lines. Each order references a generated user through `user_uuid`.

For S3, I generated the two CSV files and used the ingestion script:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
bash scripts/upload_bronze.sh
```

The DuckDB monthly report uses the Onyxia S3 connection and the bucket defined by `LAB_BUCKET_NAME`:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
uv run orders-report
uv run orders-report -p cookie
```
