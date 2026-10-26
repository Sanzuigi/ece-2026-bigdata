# Big Data Framework

Roy Homsi, ECE Paris

This project contains my work for the uv, S3, DuckDB, dbt, Spark RDD and Spark DataFrame labs. I generated fictional users and orders in Python, uploaded them to the bronze layer in S3, then used DuckDB to query the data and compare CSV with Parquet. The dbt and Spark projects build and validate the silver layer, with Parquet exports and Iceberg tables.

## Labs

- [uv: Python project and dataset generation](labs/lab-01-uv.md)
- [S3: object storage and Kubernetes ingestion](labs/lab-02-s3.md)
- [DuckDB: SQL analytics and Parquet](labs/lab-03-duckdb.md)
- [dbt: bronze to silver, data tests and Parquet exports](labs/lab-04-dbt.md)
- [Spark RDD: partitions, shuffles, joins and exercises](labs/lab-05-spark-rdd.md)
- [Spark DataFrame: silver, Parquet, Iceberg and exercises](labs/lab-06-spark-dataframe.md)

## Usage

I used Python 3.13 and uv, with the dependencies recorded in `uv.lock`. The following commands show the help output and the small datasets used to check the generators:

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

For dbt, I used the same bucket and DuckDB S3 secret. I loaded the reference seeds, built the silver models and generated the documentation with these commands:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
cd lab_medallion
uv run dbt seed
uv run dbt build --select silver
uv run dbt docs generate --static
```

The static documentation was generated at `lab_medallion/target/static_index.html`. Model files, tests and reference provenance are in [lab_medallion](lab_medallion/), with the execution results in [labs/dbt/evidence](labs/dbt/evidence/). The final build passed 25 resources and returned one warning for the synthetic ZIP/state combinations, with birthdate validation passing after the cleaning exercise.

I completed the Spark labs with the installation supplied by Onyxia, using Spark 4.1.1, Scala 2.13, 4 CPU cores and 4,000 MiB of service memory. The [Spark sources](labs/spark/) contain the transformations and exercises in cells marked by `# %%`. The RDD lab used local copies of the bronze CSVs, while the DataFrame lab read S3 through the default AWS profile. The reports contain the answers and measured results. The Iceberg metadata is stored under `warehouse/`, as requested in the DataFrame lab, with the Parquet data kept in S3.
