# Spark DataFrame Lab: Bronze to Silver

Roy Homsi, Big Data Framework

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/07.sparksql-and-dataframes/lab-pyspark-silver.md)

## Environment

I used the existing Onyxia repository and bronze datasets. The service was increased from 2 CPU cores and 2,000 MiB of memory to the lab's required limits of 4 cores and 4,000 MiB, within the namespace quota. The runtime is Spark 4.1.1 with Scala 2.13, so the matching Iceberg package is `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`.

[dataframe_lab.py](spark/dataframe_lab.py) contains the transformations and all four exercises as separate cells. I executed it from the repository root with the following command:

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

For users, I normalized the text and separated the street, city, state and ZIP. A military address has no city, which means the failed city match needs to become NULL instead of remaining an empty string. The same issue can affect the state and ZIP extractions on malformed addresses, with an empty state forming its own aggregate group or failing a lookup, and an empty ZIP being counted as a populated value by `count(zip_code)`.

For orders, I append three duplicate rows to a temporary DataFrame, then use a window partitioned by order identifier and ordered by descending timestamp. I implement the same deduplication in SQL. Both results are compared with `exceptAll()` in both directions, which checks values and duplicate multiplicities rather than just counts and column names. The bronze files remain unchanged.

The state aggregation counted the users in each state and those whose city was NULL, which kept the military-address case visible. I would choose SQL for a one-off exploration because the query directly expresses the grouping and is familiar to SQL analysts. For a model reused in a Python pipeline, I would use the DataFrame API with reusable functions, although a team maintaining SQL files could also share the SQL model. The choice therefore depends on how the colleagues work as well as the API itself.

## Parquet output

Spark writes users and orders under `silver/spark/users.parquet/` and `silver/spark/orders.parquet/`. These are directories of part-files, unlike the dbt exports, which are single objects at `silver/dbt/users.parquet` and `silver/dbt/orders.parquet`. The handout uses several older paths, so I used the actual existing exports for the comparison.

The number of part-files depends on the executed output partitions, including adaptive partition coalescing. The evidence records the object listing rather than assuming the configured shuffle partition count is the file count. `coalesce(1)` funnels the write through one partition: it gives a simple layout but sacrifices parallelism and can make one task a bottleneck.

Reading back both exports and comparing them to their source DataFrames verifies the complete round trip. A plain Parquet directory is a collection of objects without a transaction log. If another job overwrites it while a query is reading, the reader can encounter missing files, a mixture of generations or a failed query, depending on timing and file discovery.

## Iceberg

I wrote `lab.silver.users` and `lab.silver.orders`, queried their `snapshots` and `files` metadata tables, then compared the data read through the catalog with the DataFrames. The Parquet files contain the records, while the metadata JSON stores the schema and snapshot references, with the Avro manifests describing which data files belong to a snapshot. The Hadoop catalog also uses a version-hint file to locate the current metadata version.

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

I found the earliest order per user and joined it to the user data, which allowed me to compare each original birthdate with the first order date. `birthdate_is_valid` is false when the birthdate is later, with the original value kept as `birthdate_original` and the cleaned `birthdate` set to NULL. A known birthdate without an order has no order-date contradiction, whereas an unknown birthdate is marked invalid.

The DataFrame and SQL implementations agree in both directions. I also include `username_normalized`, matching the earlier dbt exercise, so the final silver output can be compared with that completed model without dropping its extra columns.

### 2. ZIP extraction timings

I implemented ZIP extraction with the native expression, a plain Python UDF and a Pandas UDF, with all three returning the same values for the complete 50-user DataFrame. After an initial evaluation, I measured three repetitions with `timeit.repeat()`, using the timing mechanism behind IPython's `%%timeit` without an interactive notebook.

The measured action sums the lengths of the extracted ZIPs, forcing the expression to execute. A plain `count()` could be optimized without evaluating the new column. The evidence records the actual samples. Fifty rows are too few to establish a general ranking: Python-worker startup, Arrow batches and scheduling can dominate. Native expressions remain preferable when they express the rule, because Spark can optimize them without crossing the Python boundary.

### 3. Cache and Spark UI

I cached both bronze DataFrames and materialized them with an action, then recorded repeated counts and inspected the Storage tab while the application was running. The cached partitions allowed later stages to reuse the parsed input instead of reading and parsing the S3 CSVs again. This also occupied executor memory, which is why I released the cache after the final use. A new session or an evicted block would still require recomputation.

### 4. Append and time travel

I saved the original orders snapshot ID, appended one order with a new identifier and checked that the current Iceberg table had one extra row. Reading the original snapshot with `VERSION AS OF` returned the same records as the pre-append DataFrame, with both commits visible in the snapshot listing.

Appending the same row to the Parquet directory also adds one record, but there is no snapshot ID or equivalent `VERSION AS OF` for that directory. Recovering the earlier dataset would require maintaining separate paths or using external versioning and a manifest. After the experiment, I restore the Parquet baseline and roll the Iceberg table back to the original snapshot. The append snapshot remains part of the table history.

## Evidence and cleanup

The [DataFrame execution output](spark/evidence/dataframe-results.txt), [S3 object listing](spark/evidence/s3-outputs.txt) and engine comparison contain the measured results. I preserved the bronze data and the earlier dbt outputs, released both cached inputs and stopped the Spark session after completing the exercises.
