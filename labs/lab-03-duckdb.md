# DuckDB Lab: SQL Analytics and Parquet

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/04.sql-analytics/lab-duckdb.md)

## Environment and bronze queries

I used DuckDB in the existing Onyxia uv project to query `bronze/users.csv` and `bronze/orders.csv` in my bucket, `user-r-homsi-ece`. The CLI version was 1.5.5 and the Python library version was 1.5.6.

I inspected the CSV dialect and column types, then loaded the datasets into the `users` and `orders` tables. The quality checks returned 50 users and 2,829 orders, with no orphan orders, inactive users or duplicate order identifiers.

The SQL is in [analytics.sql](duckdb/analytics.sql) and [parquet.sql](duckdb/parquet.sql). The query results, plans and benchmark measurements are kept in [duckdb/evidence](duckdb/evidence/).

### Why does approx_unique return 40 and 3167?

My output returned 40 distinct user identifiers and 3,167 order identifiers, even though the exact counts are 50 and 2,829. `approx_unique` uses a HyperLogLog estimate, which trades exact counting for a compact probabilistic calculation. An estimate can be lower or higher than the real count. I would use `count(DISTINCT user_uuid)` and `count(DISTINCT uuid)` for exact values. [DuckDB documentation](https://duckdb.org/docs/current/sql/functions/aggregates)

### What can go wrong with CSV type inference?

If the sampled rows are not representative, DuckDB can infer the wrong type and fail when it reaches a different value later in the file. Explicit column types or a larger `sample_size` address this, with `-1` sampling the whole file.

## Analytics and exercises

I ran the product and monthly aggregations, customer join, age groups, cumulative quantities, seven-day moving average, monthly product ranking and pivot.

The five exercises use the loaded tables:

1. Average orders per user: **56.58**. Average quantity per order: **3.045245669848003**.
2. First and last order dates per user, with `date_diff` giving the number of days between them.
3. Quantity sold by hour, ranked to identify the highest-selling hour.
4. Monthly quantity per product, with `lag` giving the previous month's quantity for the percentage variation.
5. Users whose distinct product count equals the number of products in the dataset.

The complete results are in [analytics.txt](duckdb/evidence/analytics.txt).

## Parquet export and partitioning

I exported the orders to Parquet and inspected the column metadata. The random order UUIDs contain little repetition, so they compress less than the repeated user identifiers and product names. For small column chunks, compression overhead can make the compressed size slightly larger than the uncompressed size.

The Hive-style export created six product partitions. The query filtered on `cookie` read one of the six files, with the product value obtained from the path. The plan is in [parquet.txt](duckdb/evidence/parquet.txt).

Partitioning by a unique UUID would create many tiny objects and add listing and request overhead. For orders growing every day, I would partition by date, using days or months depending on the volume and query filters.

## CSV and Parquet at scale

I generated orders for 5,000 users. The dataset contained 252,416 orders, with dates ending in 2048: one order per hourly interval over roughly 28 years.

- CSV: 29,574,123 bytes.
- Parquet: 11,532,863 bytes.

Each benchmark ran in a new CLI process with the external file cache disabled:

| Query | Data received | GET requests | Time |
|---|---:|---:|---:|
| CSV product quantities | 28.2 MiB | 1 | 1.18 s |
| Parquet product quantities | 203.5 KiB | 4 | 0.351 s |
| Parquet count from 2100 | 16.0 KiB | 1 | 0.278 s |

The CSV and Parquet queries returned identical product quantities. CSV downloaded the complete file and parsed the text fields. Parquet read the footer and the compressed chunks for the two requested columns, which greatly reduced the transfer volume. Both network transfer and CSV parsing contribute to the time difference; the S3 timings do not separate them.

The file contained three row groups. The `date >= '2100-01-01'` filter skipped all three because their maximum dates were before 2100, so the query returned zero after reading the footer. The date statistics are in [row-groups.json](duckdb/evidence/row-groups.json).

Shuffling the dates would spread them across the row groups and reduce pruning for filters within the dataset's date range.

## Python report

I added the DuckDB dependency and the `orders-report` entry point. The [Python script](../src/ece_2026_bigdata/orders_report.py) runs the query inside the Python process and passes the product as a SQL parameter.

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
uv run orders-report
uv run orders-report -p cookie
```

The unfiltered result was:

| Month | Orders | Quantity |
|---|---:|---:|
| 2020-01 | 744 | 2,297 |
| 2020-02 | 696 | 2,154 |
| 2020-03 | 744 | 2,192 |
| 2020-04 | 645 | 1,972 |

These values matched the SQL query. The cookie report returned quantities of 376, 371, 340 and 278 for the same four months.

## S3 secret questions

The persistent secret is stored in the user's home directory and can be read by processes running as that user. A compromised process or exposed backup could access it. Onyxia limits the exposure through the isolated service environment, scoped permissions and temporary credentials.

For a Kubernetes Job, I would use an identity restricted to the required bucket prefixes and operations. Workload identity or a Kubernetes Secret can supply the credentials, with RBAC restricting access to the Secret.

## Cleanup

I removed the temporary objects under `analytics/` and `large/`, along with the local large CSV. The bronze users and orders remain in S3 for the next module.
