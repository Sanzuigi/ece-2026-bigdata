# DuckDB lab: explanatory answers

Source: https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/04.sql-analytics/lab-duckdb.md

## S3 secrets

A persistent secret puts credentials on disk. A process running as the same user, a compromised service, or an exposed backup could read them. Onyxia limits the exposure through the service's isolated user environment, scoped permissions and temporary credentials. Isolation reduces the risk; it does not protect against a compromised process inside the service. Do not commit the secret directory or credential files.

For a Kubernetes Job, use a dedicated identity with permission only to the required bucket prefixes and operations. Supply short-lived credentials through a Kubernetes Secret or workload identity, mount them only in the container that needs them, and restrict Secret access with RBAC. Avoid passing the interactive user's broad credentials to every Job.

## CSV inference and approximate counts

`approx_unique` estimates distinct values; it is not an exact count and can be above or below the true value. Use `count(DISTINCT uuid)` and `count(DISTINCT user_uuid)` when exact counts matter. The estimates printed in the subject are examples, not target values.

An unrepresentative CSV sample can cause incorrect type inference, such as inferring integers before encountering text, leading to conversion errors. Supply explicit column types or increase `sample_size` (use `-1` to sample the entire file). `strict_mode=false` relaxes CSV parsing rules; it does not replace schema validation.

## Parquet compression

Random UUIDs have high cardinality and little repetition, so dictionary encoding and compression help much less than for repeated product names or user IDs. Compression metadata and block overhead can make an already compact small column slightly larger after compression.

## Partitioning

Partitioning by a unique order UUID creates many tiny objects. Listing and requesting them adds latency, request overhead and metadata work, with little data per request. A growing order dataset is usually better partitioned by day or month, depending on volume and typical query filters. Choose a granularity that avoids tiny partitions; sort by timestamp within partitions when useful.

## CSV and Parquet performance

The actual row-group count and skipped groups must be measured from this run's Parquet metadata. A group can be excluded by `date >= '2100-01-01'` when its maximum timestamp is earlier than the cutoff. Shuffling dates makes most groups span a wider date range, reducing the number that can be excluded.

CSV normally transfers and parses the complete file to aggregate two columns. Parquet reads its footer and the selected compressed column chunks. This reduces network transfer and avoids parsing unused CSV fields. End-to-end timings combine transfer, request latency, parsing, decompression and execution; those timings alone cannot precisely separate network time from parsing time. A controlled local-file comparison can estimate the computation component.

## Exercise choices

Average orders per user includes users with zero orders. First and last order queries retain inactive users with NULL dates. The highest-selling hour returns ties. Month-over-month results use consecutive calendar months, with NULL for the first month or a zero previous quantity. Ordering every product is checked against the distinct product set in the actual dataset.

## Evidence status

The S3 run completed successfully. See `results.md` and `evidence/` for actual remote SQL results, benchmark measurements, partition pruning, Python checks and cleanup verification. The measured Parquet file had three row groups and the 2100 filter skipped all three, since its latest orders were in 2048.
