-- First steps
SELECT 'Hello DuckDB' AS greeting, 21 * 2 AS answer;

SELECT getvariable('bucket') AS bucket;

-- Bronze CSV files
SELECT uuid, username, name, birthdate
FROM read_csv(
    getvariable('bucket') || '/bronze/users.csv',
    header = true,
    strict_mode = false
)
LIMIT 3;

SELECT Delimiter, Quote, HasHeader
FROM sniff_csv(getvariable('bucket') || '/bronze/users.csv');

DESCRIBE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');

SELECT column_name, column_type, approx_unique, null_percentage
FROM (SUMMARIZE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv'));

-- Load tables
CREATE OR REPLACE TABLE users AS
FROM read_csv(getvariable('bucket') || '/bronze/users.csv', strict_mode = false);

CREATE OR REPLACE TABLE orders AS
FROM read_csv(getvariable('bucket') || '/bronze/orders.csv', strict_mode = false);

SELECT
    (SELECT count(*) FROM users) AS users,
    (SELECT count(*) FROM orders) AS orders;

SELECT min(date) AS first_order, max(date) AS last_order
FROM orders;

-- Data quality
SELECT count(*) AS orphan_orders
FROM orders o
ANTI JOIN users u ON o.user_uuid = u.uuid;

SELECT count(*) AS inactive_users
FROM users u
ANTI JOIN orders o ON o.user_uuid = u.uuid;

SELECT uuid, count(*) AS occurrences
FROM orders
GROUP BY uuid
HAVING count(*) > 1;

-- Aggregations
SELECT
    product,
    count(*) AS orders,
    sum(quantity) AS quantity,
    round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct
FROM orders
GROUP BY product
ORDER BY quantity DESC;

SELECT date_trunc('month', date) AS month, count(*) AS orders, sum(quantity) AS quantity
FROM orders
GROUP BY month
ORDER BY month;

-- Customer joins
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY quantity DESC
LIMIT 5;

SELECT
    (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
    count(DISTINCT u.uuid) AS users,
    count(*) AS orders,
    round(avg(o.quantity), 2) AS avg_quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group
ORDER BY age_group;

-- Window functions
WITH daily AS (
    SELECT date::DATE AS day, sum(quantity) AS quantity
    FROM orders
    GROUP BY day
)
SELECT
    day,
    quantity,
    sum(quantity) OVER (
        ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative,
    round(avg(quantity) OVER (
        ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1) AS avg_7d
FROM daily
ORDER BY day
LIMIT 10;

SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders
GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;

-- Pivot
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product
USING sum(quantity)
ORDER BY month;

-- Exercise 1: averages
SELECT
    (SELECT count(*) FROM orders) / (SELECT count(*) FROM users) AS avg_orders_per_user,
    avg(quantity) AS avg_quantity_per_order
FROM orders;

-- Exercise 2: first and last orders
SELECT
    u.uuid,
    u.username,
    min(o.date) AS first_order,
    max(o.date) AS last_order,
    date_diff('day', min(o.date)::DATE, max(o.date)::DATE) AS days_between
FROM users u
LEFT JOIN orders o ON o.user_uuid = u.uuid
GROUP BY u.uuid, u.username
ORDER BY u.username;

-- Exercise 3: highest-selling hour
SELECT hour(date) AS hour_utc, sum(quantity) AS quantity
FROM orders
GROUP BY hour_utc
QUALIFY rank() OVER (ORDER BY sum(quantity) DESC) = 1
ORDER BY hour_utc;

-- Exercise 4: month-over-month variation per product
WITH monthly AS (
    SELECT date_trunc('month', date) AS month, product, sum(quantity) AS quantity
    FROM orders
    GROUP BY month, product
), previous AS (
    SELECT *, lag(quantity) OVER (PARTITION BY product ORDER BY month) AS previous_quantity
    FROM monthly
)
SELECT
    strftime(month, '%Y-%m') AS month,
    product,
    quantity,
    previous_quantity,
    round(100.0 * (quantity - previous_quantity) / nullif(previous_quantity, 0), 2) AS mom_pct
FROM previous
ORDER BY product, month;

-- Exercise 5: users who ordered every product
SELECT u.uuid, u.username, u.name, count(DISTINCT o.product) AS products_ordered
FROM users u
JOIN orders o ON o.user_uuid = u.uuid
GROUP BY u.uuid, u.username, u.name
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
