# Bronze to silver dbt lab

This project reads the original S3 CSV datasets and builds `main_silver.stg_orders` and `main_silver.stg_users` as DuckDB views. It includes all five exercises from the current course subject.

Run in the existing Onyxia uv project:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
cd /home/onyxia/work/ece-2026-bigdata/lab_medallion
uv run dbt debug
uv run dbt seed
uv run dbt build --select silver
uv run dbt docs generate --static
```

The profile contains no credentials. DuckDB loads the user's persistent `s3_onyxia_connection` secret. If AWS CLI works but a model cannot read S3, check that secret's current credentials.

The final lineage is `bronze.orders → stg_orders → stg_users`, with `bronze.users → stg_users`. The users model needs order dates to validate birthdates. Tests also reference the state and ZIP reference seeds.

The final silver build passed 25 resources with one warning and zero errors. The ZIP test stores 43 findings: 18 known state/ZIP mismatches and 25 ZIPs absent from the reference. Military ZIPs are outside this reference's scope. The initial birthdate test found 288 orders; cleaning five users' birthdates makes that test pass.

The complete write-up and answers are in [the dbt lab report](../labs/lab-04-dbt.md). Real execution outputs are in [evidence](../labs/dbt/evidence/). Reference provenance and limitations are documented in [reference-data.md](reference-data.md).

Static documentation is generated at `target/static_index.html`. This generated directory and the database are excluded from Git.
