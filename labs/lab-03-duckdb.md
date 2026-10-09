# DuckDB Lab: SQL Analytics and Parquet

Author: Roy Homsi
Course: Big Data Framework
Source: [lab-duckdb.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/04.sql-analytics/lab-duckdb.md)

## Objective and environment

The aim of this lab was to query the bronze datasets directly on S3, then compare CSV and Parquet before running the same analytics through Python. I used the existing uv project in Onyxia, with `user-r-homsi-ece` being my bucket name. The execution used DuckDB CLI 1.5.5 and the Python library 1.5.6, which is recorded in `uv.lock`.

The SQL examples and five exercises are in [analytics.sql](duckdb/analytics.sql), while [parquet.sql](duckdb/parquet.sql) contains the Parquet export, metadata inspection and partition query. The monthly Python report is implemented in [orders_report.py](../src/ece_2026_bigdata/orders_report.py), with the execution evidence kept in [duckdb/evidence](duckdb/evidence/).

## Bronze queries and data quality

I loaded the two bronze objects into DuckDB tables after inspecting the CSV dialect and inferred types. The execution returned 50 users and 2,829 orders, with no orphan orders, inactive users or duplicate order identifiers. These checks allowed the following metrics to use the relationship between users and orders without leaving unmatched records out of the results.

`approx_unique` provides an estimate rather than an exact count, so its result can be above or below the number of distinct values. An exact check therefore requires `count(DISTINCT uuid)` or `count(DISTINCT user_uuid)`. The values shown in the subject are examples, rather than counts that the estimate must reproduce.

CSV inference also depends on the rows sampled. If the first rows contain only integers but later rows contain text, the inferred type can cause a conversion error. Supplying the column types explicitly or increasing `sample_size`, with `-1` reading the whole file, addresses this issue. Nevertheless, `strict_mode=false` only relaxes parsing rules and does not replace validation of the schema.

## Analytics and exercises

I executed the product and monthly aggregations, customer join, age groups, cumulative quantities, seven-day moving average, monthly product ranking and pivot. The full results are recorded in [analytics.txt](duckdb/evidence/analytics.txt).

The five exercise queries follow the examples in `analytics.sql`. The average number of orders per user was 56.58, while the average quantity per order was approximately 3.0452. The first average includes every user, using the complete user count as the denominator, so a user without orders contributes zero.

The first and last order query retains every user and returns NULL dates when there are no orders. For the best-selling hour, the query returns all hours tied for the highest quantity. The month-over-month query uses `lag` over consecutive calendar months, with NULL for the first month or when the previous quantity is zero. Finally, the users who ordered every product are identified by comparing their distinct product count with the actual product set in the dataset.

## Parquet and partitioning

I exported the orders to Parquet and inspected the compression metadata. Random order UUIDs contain little repetition, which limits the benefit of compression compared with repeated user identifiers or product names. For small column chunks, compression overhead can also make the compressed size slightly larger than the uncompressed size.

The product partition export created six files. Filtering on `cookie` read one of these six files, as shown in [parquet.txt](duckdb/evidence/parquet.txt). The partition value is obtained from the Hive-style path, allowing DuckDB to exclude the other files before reading their data.

Partitioning by a unique order UUID would create many small objects, with listing and request overhead becoming large compared with the amount of data in each object. For orders growing every day, I would choose the order date at a daily or monthly granularity, depending on the volume and usual filters. Sorting by date within those partitions would also help the row-group statistics exclude data outside a requested period.

## CSV and Parquet comparison

The large dataset generated for 5,000 users contained 252,416 orders. Its CSV object occupied 29,574,123 bytes, compared with 11,532,863 bytes for Parquet. The generator produced dates ending in 2048, which differs from the century-long range and some of the illustrative counts in the subject. I therefore used the actual measurements from this execution.

Each S3 benchmark ran in a new CLI process with the external file cache disabled:

| Query | Data received | GET requests | Time |
|---|---:|---:|---:|
| CSV product quantities | 28.2 MiB | 1 | 1.18 s |
| Parquet product quantities | 203.5 KiB | 4 | 0.351 s |
| Parquet count from 2100 | 16.0 KiB | 1 | 0.278 s |

The CSV and Parquet queries returned identical quantities by product. CSV transferred the complete file to aggregate two columns, whereas Parquet read its footer and the required column chunks. This explains the large reduction in data transferred, with CSV also requiring its text fields to be parsed.

The Parquet file contained three row groups, and the `date >= '2100-01-01'` filter skipped all three because their maximum dates were earlier than the cutoff. Its result was therefore empty, with the low transfer volume coming from footer-based exclusion. The date statistics are recorded in [row-groups.json](duckdb/evidence/row-groups.json).

If the dates were shuffled, the row groups would generally span wider date ranges, reducing pruning for a cutoff inside the dataset's range. For this particular 2100 cutoff, they would still all be excluded because no order reaches that date.

The measured times combine network transfer, request latency, parsing, decompression and execution. They show the difference for this run, but they do not provide a precise split between network and parsing time. Such a split would require a controlled comparison with local files and repeated measurements.

## Python report

The `orders-report` entry point runs DuckDB inside the Python process. It reads the bucket name from the environment and passes the product as a query parameter, allowing the filter to be applied without concatenating user input into SQL.

The unfiltered report returned:

| Month | Orders | Quantity |
|---|---:|---:|
| 2020-01 | 744 | 2,297 |
| 2020-02 | 696 | 2,154 |
| 2020-03 | 744 | 2,192 |
| 2020-04 | 645 | 1,972 |

These values matched the SQL reference. I also checked `orders-report -p cookie`, which returned monthly quantities of 376, 371, 340 and 278. An unknown product containing SQL-like text returned no rows, confirming that it was handled as a parameter value.

## S3 secret questions

A persistent secret stores credentials in the user's home directory, which means a process running as that user, a compromised service or an exposed backup could read them. Onyxia reduces the exposure through the isolated service environment, scoped permissions and temporary credentials. However, this isolation does not prevent a compromised process inside the service from accessing the same files.

For a Kubernetes Job, I would use a dedicated identity restricted to the required bucket prefixes and operations. Short-lived credentials should be supplied through workload identity or a Kubernetes Secret, with RBAC restricting access and the values mounted only in the container that needs them. The interactive user's wider access should not be passed to every Job.

## Reproduce the execution

From the project root, with a valid AWS `default` profile and a DuckDB S3 secret configured in Onyxia:

```bash
uv sync --locked
export S3_ENDPOINT_URL="$(aws configure get endpoint_url --profile default)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"
uv run python labs/duckdb/run_lab.py
uv run orders-report
uv run orders-report -p cookie
```

The runner recreates `init.sql` and the DuckDB tables, executes the SQL, records the benchmarks and checks that CSV, Parquet and Python results agree. The generated database and dataset files are excluded from Git.

After inspecting the outputs, the temporary objects created by this lab can be removed with:

```bash
uv run python labs/duckdb/cleanup.py
```

## Submission and final state

The remote execution completed on 9 October 2026. After recording the evidence, I removed the nine temporary objects created under `analytics/` and `large/`. Both `bronze/users.csv` and `bronze/orders.csv` remain available for the following modules, with the final listing and empty temporary prefixes recorded in the evidence directory. Credential files and persistent secrets remain outside the repository.
