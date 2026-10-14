# dbt Lab: Bronze to Silver with DuckDB

Roy Homsi, Big Data Framework

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/1a6d9abc3a6b4d4473f47d9b55f8ae45f4b2c5bd/05.etl-elt-and-medallion-architecture/lab-dbt.md)

## Environment and scope

The project runs in the existing Onyxia vscode-pyspark service, with uv managing the Python environment and DuckDB executing dbt's compiled SQL. Before starting, I verified the existing generators, the bronze objects and both Python monthly reports. The stored S3 credentials had expired. Refreshing the default AWS profile and recreating `s3_onyxia_connection` from that profile restored access.

The current subject covers the silver layer and its Parquet exports. It differs from the earlier version that included gold aggregates. I kept the bronze files unchanged and used `lab_medallion/` for the dbt project. Runtime outputs are excluded from Git; the reference seeds are explicitly included despite the existing CSV ignore rule.

The subject still references a `states` resource without defining it. I supplied a seed containing the 62 state, territory and military postal abbreviations, including AA, AE and AP. This repairs the missing dependency without discarding valid military addresses.

## Sources, models and tests

The bronze sources point to the existing users and orders CSVs through `LAB_BUCKET_NAME`. The orders model casts identifiers to UUID, timestamps to UTC-aware timestamps and quantities to integers. It normalizes product names and keeps one row per order identifier, selecting the latest timestamp.

The users model standardizes names, email and sex, then separates street, city, state and ZIP code. Military addresses can have no city. ZIP codes stay as text so a leading zero is preserved.

Generic tests check identifiers, email, sex, state references, allowed quantities and products, and the order-to-user relationship. Singular tests check orphan orders, positive quantities, normalized usernames, birthdate cleaning and ZIP/state consistency. Each singular test returns violating rows, so an empty result means success.

I ran the initial birthdate test before the exercise changes: it failed on 288 orders. A second run demonstrates `severity='warn'`. The final exercise cleans the birthdates and restores error severity, so the final birthdate test must pass rather than merely warn.

The main dbt outputs (birthdate warning, lineage, final build, table experiment, Parquet export listing and ZIP check summary) are stored in [dbt/evidence](dbt/evidence/).

## Questions

### Why keep bronze unchanged?

The original records let me replay a corrected transformation and compare its output with earlier runs. If a bug appears six months later, I can rebuild silver without trying to reconstruct what the producer originally sent. For years of hourly ingestion, I would use compressed Parquet in immutable batches, partitioned by ingestion or event date. The partition size should avoid both full-history scans and many tiny files.

### Views and UUID storage

A view stores SQL and executes it when queried. These silver views therefore read and transform the current bronze CSVs each time. Views suit small datasets and cases where current source data matters more than repeated query cost. Tables suit repeated reads or expensive transformations, but need a refresh policy.

A UUID has 16 bytes of binary data. Its usual hyphenated representation contains 36 ASCII characters, or 36 UTF-8 payload bytes. A VARCHAR value can also incur engine-specific metadata and allocation overhead.

### Invalid birthdates and dashboards

An age dashboard should exclude or flag an invalid age rather than invent one. For sales by region, the order can remain usable if its user and region are valid; an invalid birthdate does not invalidate the sale. Keeping a quality flag makes that decision visible.

A state/ZIP test needs a real postal reference. A regular expression can validate the five-digit shape but cannot show that a ZIP belongs to a state.

## Exercises

### 1. Normalized usernames

`username_normalized` applies `lower(trim(username))`. The column is documented and checked for NULLs. A singular test also compares the value with that expression, which verifies the requested transformation directly.

### 2. Order validation

Allowed products and user references use generic tests. Quantity has a not-null check and the subject's allowed-value test, plus a singular positive-quantity test that returns NULL, zero or negative quantities.

### 3. Birthdate cleaning

The users model finds each user's earliest order from `stg_orders`. If the original birthdate is later than that order's date, the cleaned birthdate becomes NULL and `birthdate_is_valid` becomes false. `birthdate_original` is retained for audit. A user without orders has no order-date contradiction; an unknown original birthdate is marked invalid.

The order model continues to depend only on bronze. The users model depends on orders, which avoids a cycle. Tests check both the absence of orders before a cleaned birthdate and the correctness of the flag and replacement rule.

### 4. State and ZIP reference

The ZIP/state seed comes from the [GeoNames postal download](https://download.geonames.org/export/zip/), with its retrieval date, archive hash and attribution recorded in `lab_medallion/reference-data.md`. Distinct five-digit ZIP/state pairs are versioned so the result is reproducible.

The test distinguishes a known ZIP assigned to another state from a ZIP absent from the reference. Missing coverage alone does not prove that an address is invalid. Military codes are outside the geographical US mapping and are explicitly excluded. The seed is useful for this exercise but does not replace USPS address verification.

Faker generates ZIP codes independently of states. The resulting findings are kept as a warning with stored failure rows, rather than changing the raw addresses or silently accepting them. This is an explicit monitoring choice; the birthdate cleaning and structural tests retain error severity.

For production, I would obtain an approved USPS mapping or a maintained postal reference with suitable coverage and update terms. Small mappings fit dbt seeds; larger or frequently changing references should be ingested and versioned separately. Validation belongs in silver, where data-quality rules are applied. A reference update should be reviewed, loaded and followed by new tests; historical validation may require effective dates.

### 5. View/table experiment

I temporarily changed silver to tables, built them and checked both outputs against the saved view results with `EXCEPT ALL` in both directions. I then restored the views and built again.

Views store the query in the DuckDB database and execute transformations at read time. A table stores the transformed rows in that database, uses more storage and avoids rereading the CSVs for each query. A source change appears on the next view query, while a table keeps its earlier snapshot until rebuilt. Build timing alone is not a general performance benchmark because cache and network conditions vary.

## Parquet and documentation

Users and orders are exported to `silver/dbt/users.parquet` and `silver/dbt/orders.parquet` in the existing bucket. Each exported file is compared with its silver relation using `EXCEPT ALL` in both directions, preserving duplicate multiplicity as well as values. The object listing is retained as evidence.

Static documentation is generated with `uv run dbt docs generate --static`. The resulting `lab_medallion/target/static_index.html` contains the model descriptions, tests and lineage. `source()` identifies an external bronze dataset; `ref()` identifies a dbt resource and creates a dependency edge.

The silver models finish as views. No unrelated gold objects are deleted, and the existing bronze datasets remain available for later modules.

## Execution results

The final silver build completed with **PASS=25, WARN=1, ERROR=0, SKIP=0, TOTAL=26**. The warning is the documented ZIP/state check; birthdate validation passes after cleaning.

| Check | Observed result |
| --- | --- |
| Users / orders | 50 / 2,829 |
| Initial birthdate test | 288 violating orders, stored before cleaning |
| Invalid original birthdates cleaned to NULL | 5 users |
| Known state/ZIP mismatches | 18 users |
| ZIP absent from the reference | 25 users |
| Users Parquet | 50 rows, 8,186 bytes, zero rows different from the view |
| Orders Parquet | 2,829 rows, 72,466 bytes, zero rows different from the view |
| Table versus saved view results | Zero differences for both models |
| Final materialization | Views restored |
| Static documentation | Generated successfully |

The monthly order totals remained 744, 696, 744 and 645, with quantities 2,297, 2,154, 2,192 and 1,972. These match the earlier DuckDB lab. Original bronze object sizes remained 7,351 and 331,568 bytes.
