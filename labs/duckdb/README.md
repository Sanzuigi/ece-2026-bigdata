# DuckDB lab files

Completed on 9 October 2026. See results.md and evidence/ for the verified remote run. The generated temporary S3 objects were cleaned up; both bronze datasets remain available.

Run commands from your existing Onyxia uv project root.

## Files

`analytics.sql` contains the examples, quality checks and five exercise solutions. `parquet.sql` exports to S3 and demonstrates partition pruning. `orders_report.py` is copied into the project's Python package. `run_lab.py` executes the lab, retains raw results and checks consistency. `answers.md` answers the conceptual questions. `results.md` is generated only after a successful S3 run.

## Setup

```bash
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile default || echo "https://$AWS_S3_ENDPOINT")
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"
uv add duckdb
cp labs/duckdb/orders_report.py src/ece_2026_bigdata/orders_report.py
```

Add `orders-report = "ece_2026_bigdata.orders_report:main"` under the existing `[project.scripts]` section in `pyproject.toml`. Adapt the package name if needed. Keep the two existing generator entry points.

Add these entries to `.gitignore`:

```gitignore
*.duckdb
*.duckdb.wal
*.parquet
*.csv
init.sql
init-local.sql
labs/duckdb/local-bucket/
duckdb-lab-files.zip
```

## Execute

```bash
uv run python labs/duckdb/run_lab.py
```

If S3 credentials are temporarily invalid, analytics alone can be validated on the existing local CSV files:

```bash
uv run python labs/duckdb/run_lab.py --local
```

The local run is explicitly labelled and does not complete the S3 or performance sections. Restarting the Onyxia service refreshes temporary credentials. Do not include credentials or DuckDB persistent secret files in Git.

## Completion checklist

- Environment and S3 bronze objects verified
- SQL examples executed and actual counts recorded
- Quality checks inspected
- Five exercises executed
- Conceptual answers reviewed against actual results
- Parquet metadata and partition pruning inspected
- Large CSV generated, uploaded and converted to Parquet
- Fresh-process benchmarks recorded with the external file cache disabled
- Actual row-group count and skipped groups recorded
- CSV and Parquet aggregates checked for equality
- Python report checked against SQL, with cookie and unknown parameter filters
- Source files and measured evidence committed and pushed

## Cleanup

Keep `bronze/users.csv` and `bronze/orders.csv` for the next module. The subject asks to remove its temporary `analytics/` and `large/` S3 objects and `orders_large.csv` after recording evidence. Check that these prefixes contain only this lab's outputs before deleting them. Preserve the SQL, Python, answers and evidence. The runner leaves generated objects in place for inspection; finalize_lab.py records benchmark measurements and removes the exact generated objects after a successful run.

## Measurement caveat

The subject's large-dataset description, displayed count and displayed sizes disagree. Record the actual generator count, file sizes, transfer volumes and timings; do not copy the illustrative values as measured results.
