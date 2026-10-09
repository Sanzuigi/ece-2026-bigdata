> Historical local validation before the S3 connection was repaired. The completed remote results are in results.md.

# DuckDB lab: local validation

S3 execution is pending: both the default AWS profile and DuckDB persistent secret return InvalidAccessKeyId, including after the service restart. The portal can browse the bronze objects.

All examples and five exercises passed using the existing local CSV copies: 50 users, 2829 orders, no orphan orders, no inactive users and no duplicate order IDs. Average orders per user: 56.58. Average quantity per order: 3.045245669848003.

Parquet export, partition pruning and the Python report were validated locally. CSV and Parquet aggregates match; the Python monthly report matches SQL. Product filtering and safe query parameters passed.

Large dataset: 252416 orders, CSV 29574123 bytes, Parquet 11532863 bytes. Three row groups were all excluded by the 2100 cutoff because the actual dates end in October 2048. Local aggregation time: CSV 0.225 seconds; Parquet 0.0121 seconds. These are single-run local timings and do not measure S3 transfer.

See evidence/ for raw output. Run uv run python labs/duckdb/run_lab.py after S3 credentials are repaired to complete the remote sections.
