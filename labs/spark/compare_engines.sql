-- Set the DuckDB session variable bucket to s3://<your bucket> before reading.
CREATE OR REPLACE VIEW dbt_orders AS
SELECT cast(order_id AS varchar) AS order_id,
       cast(user_id AS varchar) AS user_id,
       cast(ordered_at AS timestamp) AS ordered_at, quantity, product
FROM read_parquet(getvariable('bucket') || '/silver/dbt/orders.parquet');

CREATE OR REPLACE VIEW spark_orders AS
SELECT cast(order_id AS varchar) AS order_id,
       cast(user_id AS varchar) AS user_id,
       cast(ordered_at AS timestamp) AS ordered_at, quantity, product
FROM read_parquet(getvariable('bucket') || '/silver/spark/orders.parquet/*.parquet');

CREATE OR REPLACE VIEW dbt_users AS
SELECT cast(user_id AS varchar) AS user_id, username, username_normalized,
       name, sex, email, birthdate_original, birthdate, birthdate_is_valid,
       street, city, state, zip_code
FROM read_parquet(getvariable('bucket') || '/silver/dbt/users.parquet');

CREATE OR REPLACE VIEW spark_users AS
SELECT cast(user_id AS varchar) AS user_id, username, username_normalized,
       name, sex, email, birthdate_original, birthdate, birthdate_is_valid,
       street, city, state, zip_code
FROM read_parquet(getvariable('bucket') || '/silver/spark/users.parquet/*.parquet');

SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity FROM dbt_orders
UNION ALL
SELECT 'spark', count(*), sum(quantity) FROM spark_orders;

SELECT 'dbt' AS engine, count(*) AS users FROM dbt_users
UNION ALL
SELECT 'spark', count(*) FROM spark_users;

SELECT 'orders' AS model, count(*) AS different_rows FROM (
    (SELECT * FROM dbt_orders EXCEPT ALL SELECT * FROM spark_orders)
    UNION ALL
    (SELECT * FROM spark_orders EXCEPT ALL SELECT * FROM dbt_orders)
)
UNION ALL
SELECT 'users', count(*) FROM (
    (SELECT * FROM dbt_users EXCEPT ALL SELECT * FROM spark_users)
    UNION ALL
    (SELECT * FROM spark_users EXCEPT ALL SELECT * FROM dbt_users)
);

DESCRIBE SELECT * FROM read_parquet(getvariable('bucket') || '/silver/dbt/orders.parquet');
DESCRIBE SELECT * FROM read_parquet(getvariable('bucket') || '/silver/spark/orders.parquet/*.parquet');
