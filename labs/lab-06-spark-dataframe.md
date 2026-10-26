# Spark DataFrame Lab: Bronze to Silver

Roy Homsi, Big Data Framework

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/07.sparksql-and-dataframes/lab-pyspark-silver.md)

## Environment

I used the existing Onyxia repository and bronze datasets. The service was increased from 2 CPU cores and 2,000 MiB of memory to the lab's required limits of 4 cores and 4,000 MiB, within the namespace quota. The runtime is Spark 4.1.1 with Scala 2.13, so the matching Iceberg package is `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`.

[dataframe_lab.py](spark/dataframe_lab.py) contains the lab and all four exercises as separate cells. It can also be run from the repository root:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
spark-submit --driver-memory 1800m \
  --conf spark.ui.showConsoleProgress=false \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  labs/spark/dataframe_lab.py
```

The default AWS profile supplies the temporary access key, secret and session token. They are passed to Hadoop's `TemporaryAWSCredentialsProvider` without being saved in the source. The endpoint uses path-style access. The Spark session also declares the `lab` Hadoop catalog with the bucket's `warehouse/` prefix as its warehouse.

Restarting the service kills a job running in its local JVM. A new session is needed afterwards, and expired temporary credentials must be refreshed. `SimpleAWSCredentialsProvider` supplies only an access key and secret; those values are insufficient for credentials issued with a session token. The temporary provider includes the token. A job which outlives static temporary credentials also needs a renewal strategy.

## Bronze and silver transformations

I read the two bronze CSVs through S3A with explicit schemas and `multiLine=True`. This avoids inference depending on a small sample and correctly reconstructs the multiline user addresses. Dates, quantities and timestamps are cast during staging, and the SQL session timezone is UTC.

The user transformation normalizes text and splits street, city, state and ZIP. A military address has no city, so a failed city match becomes NULL rather than an empty string. The state and ZIP extractions can also return empty strings on malformed addresses. Without validation, an empty state could form its own aggregate group or fail a lookup, and an empty ZIP could be counted as a populated value by `count(zip_code)`.

For orders, I append three duplicate rows to a temporary DataFrame, then use a window partitioned by order identifier and ordered by descending timestamp. I implement the same deduplication in SQL. Both results are compared with `exceptAll()` in both directions, which checks values and duplicate multiplicities rather than just counts and column names. The bronze files remain unchanged.

The state aggregation counts users per state and counts the users whose city is NULL. It checks that the military-address case remains visible instead of being silently grouped under an empty string. SQL is convenient for a one-off exploration and is easy for SQL analysts to review. A DataFrame model fits a Python pipeline with reusable functions; SQL can also be a shared model when the team maintains SQL files. The team's skills and conventions matter more than the API alone.

## Parquet output

Spark writes users and orders under `silver/spark/users.parquet/` and `silver/spark/orders.parquet/`. These are directories of part-files, unlike the dbt exports, which are single objects at `silver/dbt/users.parquet` and `silver/dbt/orders.parquet`. The handout uses several older paths, so I used the actual existing exports for the comparison.

The number of part-files depends on the executed output partitions, including adaptive partition coalescing. The evidence records the object listing rather than assuming the configured shuffle partition count is the file count. `coalesce(1)` funnels the write through one partition: it gives a simple layout but sacrifices parallelism and can make one task a bottleneck.

Reading back both exports and comparing them to their source DataFrames verifies the complete round trip. A plain Parquet directory is a collection of objects without a transaction log. If another job overwrites it while a query is reading, the reader can encounter missing files, a mixture of generations or a failed query, depending on timing and file discovery.

## Iceberg

I wrote `lab.silver.users` and `lab.silver.orders`, queried their `snapshots` and `files` metadata tables, and compared the data read through the catalog with the DataFrames. Parquet files hold the records. The metadata JSON records the schema and snapshot references, while the Avro manifests describe data files and their membership in a snapshot. The Hadoop catalog also uses a version-hint file to locate the current metadata version.

An Iceberg write stages new files and commits a new snapshot. Readers use a consistent snapshot instead of watching an in-place directory replacement. This protection still depends on a catalog and storage implementation with the required commit guarantees. With a Hadoop catalog on an object store, concurrent updates can race if the metadata-pointer update does not have suitable atomicity or locking. A catalog service can coordinate conditional commits, reject a stale update and support retries.

Only the requested Iceberg metadata is collected into `warehouse/silver/users/metadata/` and `warehouse/silver/orders/metadata/` for Git. The Parquet data remains in S3.

## Comparison with dbt

The [comparison evidence](spark/evidence/engine-comparison.txt) checks counts, total order quantity and schemas against the existing dbt exports. UUIDs are strings in Spark rather than DuckDB's native UUID type, so consumers must validate or cast them. Spark's timestamp interpretation depends on the session timezone and Parquet timestamp representation; the session is set to UTC, and the comparison normalizes timestamp types explicitly.

| Check | dbt | Spark |
| --- | ---: | ---: |
| Users | 50 | 50 |
| Orders | 2,829 | 2,829 |
| Total quantity | 8,615 | 8,615 |
| Invalid original birthdates | 5 | 5 |

Bidirectional `EXCEPT ALL` found zero different rows in either model. The final Parquet export has one part-file for users and one for orders. The live Storage tab showed both bronze inputs fully cached, using 10,048 and 227,216 bytes respectively.

A single dbt object is a straightforward input for a small consumer. Spark's directory of partitions supports distributed reads and writes at larger scale, but consumers must read the directory or part-file pattern correctly.

The lab initially uses assertions to check counts, columns and agreement between APIs. Those do not automatically provide all of dbt's data tests. I added checks for unique identifiers, allowed products, positive and non-null quantities, and order-to-user relationships. Production code would need documented validation rules, failure handling and monitoring as well.

Spark builds an execution DAG from the DataFrame expressions and their dependencies. In the Python source, variables determine which result feeds the next transformation. This is different from dbt's named model graph built from `ref()` and `source()`: notebook cell ordering and variable state can otherwise hide dependencies.

The recorded Spark time includes CSV reading, validation and writes, and the complete process also includes JVM and library startup. A dbt build has a different scope and may query views only when tests need them. Data caching, network conditions, output materialization and startup costs must be aligned before making a performance comparison.

The measured Spark silver phase took 36.94 seconds. The earlier dbt evidence records 8.24 seconds for two view models and 24 tests; these different workloads do not establish an engine speed ranking.

For a 10 GB daily batch, I would first consider dbt with DuckDB if it fits the machine and transformation workload. For 10 TB per day, distributed Spark is the more suitable starting point. For analysts who only know SQL, dbt provides a familiar model and test workflow; Spark SQL can still be an execution option where distributed processing is required.

## Exercises

### 1. Birthdate validity

I find the earliest order per user and join it to the user data. `birthdate_is_valid` is false when the original birthdate is later than that date. The original value is kept as `birthdate_original`, and the cleaned `birthdate` becomes NULL when invalid. A known birthdate with no order has no order-date contradiction. An unknown birthdate is marked invalid.

The DataFrame and SQL implementations agree in both directions. I also include `username_normalized`, matching the earlier dbt exercise, so the final silver output can be compared with that completed model without dropping its extra columns.

### 2. ZIP extraction timings

I implement extraction with the native expression, a plain Python UDF and a Pandas UDF. All three run on the complete 50-user DataFrame and return the same ZIP values. I use `timeit.repeat()` with three repetitions, the same timing approach available through IPython's `%%timeit`, after an initial evaluation.

The measured action sums the lengths of the extracted ZIPs, forcing the expression to execute. A plain `count()` could be optimized without evaluating the new column. The evidence records the actual samples. Fifty rows are too few to establish a general ranking: Python-worker startup, Arrow batches and scheduling can dominate. Native expressions remain preferable when they express the rule, because Spark can optimize them without crossing the Python boundary.

### 3. Cache and Spark UI

Both bronze DataFrames are cached and materialized with an action. Repeated counts and Storage metrics are recorded, and the live Spark UI is inspected while the application runs. Cached partitions let subsequent stages reuse the parsed input instead of reading and parsing the S3 CSVs again. A new session or evicted cache blocks still requires recomputation. Caching consumes executor memory and should be released when the input is no longer reused.

### 4. Append and time travel

I save the original orders snapshot ID, append one order with a new identifier, and verify that the current Iceberg table has one extra row. `VERSION AS OF` reads the original snapshot and agrees with the pre-append DataFrame. The snapshot listing shows both commits.

Appending the same row to the Parquet directory also adds one record, but there is no snapshot ID or equivalent `VERSION AS OF` for that directory. Recovering the earlier dataset would require maintaining separate paths or using external versioning and a manifest. After the experiment, I restore the Parquet baseline and roll the Iceberg table back to the original snapshot. The append snapshot remains part of the table history.

## Evidence and cleanup

The [DataFrame execution output](spark/evidence/dataframe-results.txt), [S3 object listing](spark/evidence/s3-outputs.txt) and engine comparison retain the main results. The source and reports are committed separately from generated runtime files. Bronze data and the earlier dbt lab are preserved, cached inputs are released, and the Spark session is stopped.
