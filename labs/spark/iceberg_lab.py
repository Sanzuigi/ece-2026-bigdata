"""Iceberg snapshots, time travel and schema evolution."""
# %% Session and existing silver tables
import configparser
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

profile = configparser.ConfigParser()
profile.read(Path.home() / '.aws/credentials')
credentials = profile['default']
bucket = os.environ.get('LAB_BUCKET_NAME') or os.environ['KUBERNETES_NAMESPACE']
endpoint = 'https://' + os.environ.get('AWS_S3_ENDPOINT', 's3.seaweedfs.adm.adaltas.cloud').removeprefix('https://')
spark = (SparkSession.builder.appName('lab-iceberg').master('local[*]')
         .config('spark.sql.session.timeZone', 'UTC')
         .config('spark.sql.shuffle.partitions', '8')
         .config('spark.hadoop.fs.s3a.endpoint', endpoint)
         .config('spark.hadoop.fs.s3a.access.key', credentials['aws_access_key_id'])
         .config('spark.hadoop.fs.s3a.secret.key', credentials['aws_secret_access_key'])
         .config('spark.hadoop.fs.s3a.session.token', credentials['aws_session_token'])
         .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
         .config('spark.hadoop.fs.s3a.path.style.access', 'true')
         .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
         .config('spark.sql.catalog.lab', 'org.apache.iceberg.spark.SparkCatalog')
         .config('spark.sql.catalog.lab.type', 'hadoop')
         .config('spark.sql.catalog.lab.warehouse', f's3a://{bucket}/warehouse')
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
print('Spark:', spark.version, flush=True)
spark.sql('SHOW NAMESPACES IN lab').show()


def inspect(name, stage):
    table = f'lab.silver.{name}'
    print(f'\n{name}: {stage}', flush=True)
    spark.sql(f'SELECT count(*) AS rows FROM {table}').show()
    spark.sql(f'''SELECT snapshot_id, parent_id, operation, committed_at
                  FROM {table}.snapshots ORDER BY committed_at''').show(truncate=False)
    spark.sql(f'''SELECT file_path, file_format, record_count, file_size_in_bytes
                  FROM {table}.files ORDER BY file_path''').show(truncate=False)
    return {
        'rows': spark.table(table).count(),
        'snapshot': spark.sql(f'SELECT snapshot_id FROM {table}.history WHERE is_current_ancestor ORDER BY made_current_at DESC').first()[0],
        'files': {row.file_path for row in spark.table(f'{table}.files').select('file_path').collect()},
    }


# %% Orders: baseline and append three distinct new orders
orders_before = inspect('orders', 'baseline')
spark.sql('SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders').show()
new_orders = (spark.table('lab.silver.orders').orderBy('order_id').limit(3)
              .withColumn('order_id', F.expr('uuid()')).cache())
assert new_orders.count() == 3
assert not new_orders.join(spark.table('lab.silver.orders'), 'order_id').take(1)
new_orders.writeTo('lab.silver.orders').append()
orders_after = inspect('orders', 'after append')
assert orders_after['rows'] == orders_before['rows'] + 3
assert orders_before['files'] <= orders_after['files']

# %% Orders: time travel from the current baseline snapshot
orders_old = spark.sql(f"SELECT * FROM lab.silver.orders VERSION AS OF {orders_before['snapshot']}")
assert orders_old.count() == orders_before['rows']
print('Orders time travel:', orders_old.count(), 'current:', orders_after['rows'], flush=True)

# %% Orders: evolve the schema without rewriting existing data files
spark.sql('ALTER TABLE lab.silver.orders ADD COLUMN source STRING')
assert {r.file_path for r in spark.table('lab.silver.orders.files').select('file_path').collect()} == orders_after['files']
assert spark.table('lab.silver.orders').filter(F.col('source').isNotNull()).count() == 0
spark.sql('DESCRIBE lab.silver.orders').show(truncate=False)
spark.sql('''INSERT INTO lab.silver.orders
             SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
             FROM lab.silver.orders LIMIT 1''')
orders_evolved = inspect('orders', 'evolved schema and insert')
assert orders_evolved['rows'] == orders_before['rows'] + 4
assert spark.table('lab.silver.orders').filter("source = 'spark'").count() == 1
spark.sql('SELECT source, count(*) AS rows FROM lab.silver.orders GROUP BY source').show()
new_orders.unpersist()

# %% Exercises 1 to 3: users table, new batch and time travel
# The users table already exists from the DataFrame lab.
assert spark.catalog.tableExists('lab.silver.users')
users_before = inspect('users', 'baseline')
new_users = (spark.table('lab.silver.users').orderBy('user_id').limit(3)
             .withColumn('user_id', F.expr('uuid()')).cache())
assert new_users.count() == 3
assert not new_users.join(spark.table('lab.silver.users'), 'user_id').take(1)
new_users.writeTo('lab.silver.users').append()
users_after = inspect('users', 'after append')
assert users_after['rows'] == users_before['rows'] + 3
assert users_before['files'] <= users_after['files']
users_old = spark.sql(f"SELECT * FROM lab.silver.users VERSION AS OF {users_before['snapshot']}")
assert users_old.count() == users_before['rows']
print('Users time travel:', users_old.count(), 'current:', users_after['rows'], flush=True)

# %% Exercise 4: country and data written with the new schema
spark.sql('ALTER TABLE lab.silver.users ADD COLUMN country STRING')
assert {r.file_path for r in spark.table('lab.silver.users.files').select('file_path').collect()} == users_after['files']
assert spark.table('lab.silver.users').filter(F.col('country').isNotNull()).count() == 0
country_user = (spark.table('lab.silver.users').orderBy('user_id').limit(1)
                .withColumn('user_id', F.expr('uuid()')).withColumn('country', F.lit('US')))
country_user.writeTo('lab.silver.users').append()
users_evolved = inspect('users', 'evolved schema and insert')
assert users_evolved['rows'] == users_before['rows'] + 4
assert spark.table('lab.silver.users').filter("country = 'US'").count() == 1
spark.sql('DESCRIBE lab.silver.users').show(truncate=False)
spark.sql('SELECT country, count(*) AS rows FROM lab.silver.users GROUP BY country').show()
new_users.unpersist()

# %% Exercise 5: current metadata and unchanged plain Parquet output
for name in ('users', 'orders'):
    spark.sql(f'''SELECT content, count(*) AS manifests FROM lab.silver.{name}.manifests
                  GROUP BY content''').show()
    print(name, 'plain Parquet rows:', spark.read.parquet(f's3a://{bucket}/silver/spark/{name}.parquet/').count(), flush=True)

spark.stop()
print('ICEBERG_LAB_OK', flush=True)
