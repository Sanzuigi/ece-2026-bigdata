# Iceberg Lab: Snapshots and Schema Evolution

Roy Homsi, Big Data Framework

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/08.iceberg-introduction/lab.md)

## Environment

I used the Onyxia Spark service and the silver tables created in the DataFrame lab, with Spark 4.1.1, Scala 2.13 and Iceberg 1.11.0. The [source](spark/iceberg_lab.py) contains the lab operations and exercises in separate cells. Temporary S3 credentials come from the default AWS profile and are passed to S3A with `TemporaryAWSCredentialsProvider`, which includes the session token.

The `lab` catalog is a Hadoop catalog pointing to `s3a://user-r-homsi-ece/warehouse`. The users and orders tables already existed, so I reused them instead of replacing the previous lab's tables before the experiment. I ran the code with:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
spark-submit --driver-memory 1800m \
  --conf spark.ui.showConsoleProgress=false \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  labs/spark/iceberg_lab.py
```

## Files and table metadata

The actual rows are stored in the Parquet files under each table's `data/` directory. The `metadata/` directory contains the metadata JSON files, manifest lists and Avro manifests. The JSON records the schema, partition specifications and snapshot references, while the manifests describe which data files belong to a snapshot. The Hadoop catalog uses the table location and metadata versions, with a version-hint file helping it locate the current version.

These extra files are needed because a table is more than the rows in its Parquet files. Iceberg needs to know which files belong to the current state, which schema applies and which previous versions are still available. Simply listing every Parquet object would also include files which belong to older states or are no longer referenced by the current snapshot.

## Snapshots and appends

I ran the lab on 31 October 2026. The baseline orders table contained 2,829 rows with a total quantity of 8,615.

| Stage | Orders | Order snapshots | Current order data files | Users | User snapshots | Current user data files |
|---|---:|---:|---:|---:|---:|---:|
| Before this lab | 2,829 | 2 | 1 | 50 | 1 | 1 |
| After the three-row append | 2,832 | 3 | 2 | 53 | 2 | 2 |
| After the evolved-schema insert | 2,833 | 4 | 3 | 54 | 3 | 3 |

The orders table already had two snapshots because the DataFrame lab included an append followed by a rollback. The rolled-back append remained in the snapshot history, although its row was absent from the current table.

The `snapshots` metadata table records snapshot identifiers, parent identifiers, commit times and operations. These describe the table's history, whereas a Parquet file describes the rows stored in that file and does not independently identify the committed state of the whole table.

I appended three orders copied from existing records with newly generated order identifiers, which keeps the appended batch separate from the original orders. The original data files remained referenced after the append. Iceberg created a new snapshot referring to both the previous files and the new files, which allows readers already using the previous snapshot to continue reading a consistent state.

## Time travel

I saved the current snapshot identifier before the append and queried it with `VERSION AS OF`. This is important here since the table already had history from the DataFrame exercise; choosing an arbitrary oldest snapshot would not necessarily mean the state immediately before this lab's append.

For orders, snapshot `1752134345965458086` returned 2,829 rows while the table after the append contained 2,832. For users, snapshot `8067420111286679264` returned 50 rows while the table after the append contained 53.

Time travel reads the files referenced by the selected snapshot. It does not need a complete physical copy of the table, since snapshots can share unchanged data files and describe only the differences in their references. If the required metadata or data files were physically removed, the old version could no longer be read successfully. Keeping a snapshot identifier alone therefore does not preserve its data.

## Schema evolution

I added the nullable string column `source` to the orders table, then inserted one row with the value `spark`. The file set stayed identical immediately after `ALTER TABLE`, so the existing Parquet files were not rewritten by the schema change. The older rows were read with NULL for the new column.

After the insert, 2,832 orders had a NULL source and one had `source = 'spark'`. I kept the handout's SQL for this insert, which copies an existing `order_id`. Unlike the earlier batch, this example therefore introduces a duplicate order identifier. Iceberg does not enforce uniqueness automatically; a production pipeline would need to generate a new identifier or validate the constraint before committing the data.

The updated schema is recorded in the Iceberg metadata JSON, with field identifiers allowing Iceberg to track columns independently of their physical order. This is a table-level change because every reader needs the same interpretation of the table, including files written before the new column existed. Changing a single Parquet file would not establish that shared schema for the other files.

## Iceberg and plain Parquet

`silver/spark/orders.parquet/` is a directory of Parquet part-files. `warehouse/silver/orders/` is an Iceberg table location containing data files and the metadata which describes its snapshots. The plain Parquet exports stayed separate from the Iceberg appends and schema changes.

To safely replace plain Parquet output while readers are active, I would write the new generation to a separate location and publish a coordinated pointer to it, while keeping the previous generation until its readers finish. Deleting and rewriting the directory in place can leave readers with missing files or a mixture of generations, since object storage does not provide an atomic directory replacement.

Iceberg writes new files and metadata before committing the new table state, so readers can resolve one complete snapshot rather than discover a directory halfway through replacement. This atomic visibility depends on the catalog's commit mechanism. A Hadoop catalog needs suitable atomic filesystem operations or locking, which cannot simply be assumed on S3A. With several engines sharing the table, a catalog service supplies a common table reference and can coordinate conditional commits, reject conflicting updates and allow retries.

## Exercises

1. The `lab.silver.users` Iceberg table was already created in the DataFrame lab. I verified that it was available through the same catalog and inspected its baseline rows, snapshots and current data files.
2. I appended a batch of three users with new `user_id` values, then inspected the new snapshot and checked that the original data files remained part of the table.
3. I queried the saved baseline snapshot with `VERSION AS OF` and compared its row count with the current table after the append.
4. I added the nullable string column `country`, checked that the previous files were unchanged and read NULL for their country values, then appended a user with a new identifier and `country = 'US'`. The resulting table had 54 users, with 53 NULL country values and one `US` value. This value belongs to the synthetic exercise row.
5. Both S3 locations use the same structure: `data/` holds Parquet records, while `metadata/` holds metadata JSON, manifest lists and manifests. Their exact numbers of files and sizes depend on their own write histories, so an identical directory structure does not mean identical snapshots or data.
6. Parquet is the columnar file format storing the rows. Iceberg is the table format which organizes those files, tracks schemas and snapshots and supports operations such as time travel. An Iceberg catalog resolves a table name to its metadata and coordinates access to the current table state; it does not replace either the table format or the data files.

## Cleanup

I stopped the Spark session after completing the operations and checks. I saved the warehouse state, removed the warehouse prefix as requested, then restored its pre-lab contents so the tables from the DataFrame lab remained available. I downloaded the restored objects and compared them with the saved copy; their contents were identical.
