# DuckDB lab: measured results

All SQL examples and five exercises were executed against the S3 bronze objects.

Users: 50; orders: 2829; orphan orders: 0; inactive users: 0; duplicate order IDs: 0.

Average orders per user: 56.58; average quantity per order: 3.045245669848003.

Large dataset: 252416 orders. Parquet row groups: 3; skipped by the 2100 filter: 3.

CSV and Parquet product aggregates match. Python monthly results match the SQL reference; the cookie filter and parameter handling were checked.

See evidence/ for full query results, object sizes, execution plans and HTTP transfer statistics. Timings are specific to this run.

## S3 benchmark measurements

| Query | Received | GET requests | Time (seconds) |
|---|---:|---:|---:|
| csv | 28.2 MiB | 1 | 1.18 |
| parquet | 203.5 KiB | 4 | 0.351 |
| filtered-parquet | 16.0 KiB | 1 | 0.278 |

The CSV and Parquet aggregation queries return identical totals. The 2100 filter excludes all three row groups because this generator produced dates ending in 2048. Its low transfer volume reflects footer-only exclusion, rather than a query returning later orders.

The cookie partition query scanned one of six files. Parquet was 11,532,863 bytes compared with 29,574,123 bytes for CSV. These single-run S3 timings include transfer, request latency and execution. The local comparison is recorded separately; it does not precisely isolate network and parsing times.

## Connection repair

The portal supplied a working AWS shared profile after the original service credentials returned InvalidAccessKeyId. DuckDB was refreshed using the default profile through the credential-chain provider. Credentials remain outside the project and Git.

## Cleanup

The nine exact S3 objects created by this run and the generated local large CSV were removed after recording evidence. Both bronze objects were retained. Source files, reports, SQL results, plans and benchmark logs are preserved in the repository.
