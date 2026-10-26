"""Bronze-to-silver DataFrame lab and all four exercises.

Spark 4.1.1, Scala 2.13 and Iceberg 1.11.0 in the Onyxia service.
S3 credentials are read from the default shared profile.
"""
# %% Session with S3A and an Iceberg Hadoop catalog
import configparser
import json
import os
from pathlib import Path
import re
import time
import timeit
from urllib.request import urlopen

import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

profile = configparser.ConfigParser()
profile.read(Path.home() / '.aws/credentials')
credentials = profile['default']
bucket = os.environ.get('LAB_BUCKET_NAME') or os.environ['KUBERNETES_NAMESPACE']
endpoint = 'https://' + os.environ.get('AWS_S3_ENDPOINT', 's3.seaweedfs.adm.adaltas.cloud').removeprefix('https://')
spark = (SparkSession.builder.appName('lab-silver-pyspark').master('local[*]')
         .config('spark.sql.session.timeZone', 'UTC')
         .config('spark.sql.shuffle.partitions', '8')
         .config('spark.hadoop.fs.s3a.endpoint', endpoint)
         .config('spark.hadoop.fs.s3a.access.key', credentials['aws_access_key_id'])
         .config('spark.hadoop.fs.s3a.secret.key', credentials['aws_secret_access_key'])
         .config('spark.hadoop.fs.s3a.session.token', credentials['aws_session_token'])
         .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
         .config('spark.hadoop.fs.s3a.path.style.access', 'true')
         .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
         .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
         .config('spark.sql.catalog.lab', 'org.apache.iceberg.spark.SparkCatalog')
         .config('spark.sql.catalog.lab.type', 'hadoop')
         .config('spark.sql.catalog.lab.warehouse', f's3a://{bucket}/warehouse')
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel('ERROR')
print('Spark:', spark.version, 'parallelism:', sc.defaultParallelism, flush=True)
started = time.perf_counter()


def ui(resource):
    with urlopen(f'{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{resource}', timeout=15) as response:
        return json.load(response)


def agree(left, right, label):
    right = right.select(*left.columns)
    assert not left.exceptAll(right).take(1), label + ': left-only rows'
    assert not right.exceptAll(left).take(1), label + ': right-only rows'
    print(label + ': identical rows', flush=True)


# %% Bronze, explicit schemas and the multiline address
users_schema = T.StructType([T.StructField(name, T.StringType()) for name in
                            ('uuid', 'username', 'name', 'sex', 'address', 'mail', 'birthdate')])
orders_schema = 'uuid string, user_uuid string, date string, quantity int, product string'
users_bronze = spark.read.option('header', True).option('multiLine', True).schema(users_schema).csv(f's3a://{bucket}/bronze/users.csv')
orders_bronze = spark.read.option('header', True).option('multiLine', True).schema(orders_schema).csv(f's3a://{bucket}/bronze/orders.csv')
users_bronze.printSchema()
users_bronze.select('address').show(3, truncate=False)
print('Bronze counts:', users_bronze.count(), orders_bronze.count(), flush=True)

# %% Exercise 3: cache materialization and inspect Storage metrics
users_bronze.cache()
orders_bronze.cache()
users_bronze.count()
orders_bronze.count()
print('UI Storage cached bronze:', [{key: item.get(key) for key in
      ('id', 'name', 'numCachedPartitions', 'memoryUsed')} for item in ui('storage/rdd')], flush=True)
for attempt in range(2):
    before = time.perf_counter()
    print('Cached bronze repeat', attempt + 1, users_bronze.count(), orders_bronze.count(),
          'seconds=', round(time.perf_counter() - before, 4), flush=True)

# %% Silver users through the DataFrame API
users_typed = users_bronze.select(
    F.col('uuid').alias('user_id'), F.trim('username').alias('username'),
    F.trim('name').alias('name'), F.upper(F.trim('sex')).alias('sex'),
    F.lower(F.trim('mail')).alias('email'), F.to_date('birthdate').alias('birthdate'),
    F.split('address', '\n').alias('address_lines'))
users_typed = (users_typed.withColumn('street', F.element_at('address_lines', 1))
               .withColumn('address_line_2', F.element_at('address_lines', 2)))
stg_users_base = users_typed.select(
    'user_id', 'username', 'name', 'sex', 'email', 'birthdate', 'street',
    F.nullif(F.regexp_extract('address_line_2', r'^(.+), [A-Z]{2} [0-9]{5}$', 1), F.lit('')).alias('city'),
    F.regexp_extract('address_line_2', r'([A-Z]{2}) ([0-9]{5})$', 1).alias('state'),
    F.regexp_extract('address_line_2', r'([A-Z]{2}) ([0-9]{5})$', 2).alias('zip_code'))
stg_users_base.show(5, truncate=False)

# %% Orders: deliberate duplicates, DataFrame windows and equivalent SQL
orders_typed = orders_bronze.select(
    F.col('uuid').alias('order_id'), F.col('user_uuid').alias('user_id'),
    F.to_timestamp('date').alias('ordered_at'), F.col('quantity').cast('int').alias('quantity'),
    F.lower(F.trim('product')).alias('product'))
orders_with_duplicates = orders_typed.unionByName(orders_typed.orderBy('order_id').limit(3))
window = Window.partitionBy('order_id').orderBy(F.col('ordered_at').desc())
stg_orders = (orders_with_duplicates.withColumn('row_number', F.row_number().over(window))
              .filter('row_number = 1').drop('row_number'))
orders_with_duplicates.createOrReplaceTempView('orders_with_duplicates')
stg_orders_sql = spark.sql('''
    select order_id, user_id, ordered_at, quantity, product
    from (select *, row_number() over (partition by order_id order by ordered_at desc) as rn
          from orders_with_duplicates) where rn = 1
''')
print('Orders original / with duplicates / deduplicated:', orders_typed.count(),
      orders_with_duplicates.count(), stg_orders.count(), flush=True)
agree(stg_orders, stg_orders_sql, 'Orders API versus SQL')
stg_users_base.createOrReplaceTempView('users_base')
stg_orders.createOrReplaceTempView('orders_silver')
spark.sql('''select state, count(*) as users,
             sum(case when city is null then 1 else 0 end) as missing_city
             from users_base group by state order by users desc,state''').show(10)

# %% Exercise 1: validate and clean birthdates, API and SQL
first_orders = stg_orders.groupBy('user_id').agg(F.min('ordered_at').cast('date').alias('first_order_date'))
joined = stg_users_base.join(first_orders, 'user_id', 'left')
valid = F.col('birthdate').isNotNull() & (F.col('first_order_date').isNull() | (F.col('birthdate') <= F.col('first_order_date')))
stg_users = (joined.withColumn('username_normalized', F.lower(F.trim('username')))
             .withColumn('birthdate_original', F.col('birthdate'))
             .withColumn('birthdate_is_valid', valid)
             .withColumn('birthdate', F.when(F.col('birthdate_is_valid'), F.col('birthdate')))
             .drop('first_order_date'))
stg_users_sql = spark.sql('''
    with first_orders as (select user_id, cast(min(ordered_at) as date) as first_order_date
                          from orders_silver group by user_id)
    select u.user_id, username, name, sex, email,
           case when first_order_date is not null and birthdate > first_order_date
                then null else birthdate end as birthdate,
           street, city, state, zip_code, lower(trim(username)) as username_normalized,
           birthdate as birthdate_original,
           birthdate is not null and (first_order_date is null or birthdate <= first_order_date) as birthdate_is_valid
    from users_base u left join first_orders f on u.user_id=f.user_id
''')
agree(stg_users, stg_users_sql, 'Birthdate API versus SQL')
print('Invalid birthdate users:', stg_users.filter(~F.col('birthdate_is_valid')).count(), flush=True)
assert stg_orders.filter('quantity is null or quantity <= 0 or product not in (\'bread\',\'brioche\',\'cookie\',\'croissant\',\'donut\',\'drink\')').count() == 0
assert stg_orders.join(stg_users, 'user_id', 'left_anti').count() == 0
assert stg_orders.select('order_id').distinct().count() == stg_orders.count()
assert stg_users.select('user_id').distinct().count() == stg_users.count()

# %% Parquet exports and exact round-trip checks
for name, frame in [('users', stg_users), ('orders', stg_orders)]:
    path = f's3a://{bucket}/silver/spark/{name}.parquet'
    frame.write.mode('overwrite').parquet(path)
    agree(frame, spark.read.parquet(path), name + ' Parquet round trip')
    print(name, 'output partitions:', frame.rdd.getNumPartitions(), flush=True)
print('SILVER_BUILD_SECONDS', round(time.perf_counter() - started, 4), flush=True)

# %% Iceberg snapshots and files
spark.sql('create namespace if not exists lab.silver')
stg_users.writeTo('lab.silver.users').createOrReplace()
stg_orders.writeTo('lab.silver.orders').createOrReplace()
agree(stg_users, spark.table('lab.silver.users'), 'Users Iceberg round trip')
agree(stg_orders, spark.table('lab.silver.orders'), 'Orders Iceberg round trip')
spark.sql('select snapshot_id, operation, committed_at from lab.silver.orders.snapshots').show(truncate=False)
spark.sql('select file_path,file_format,record_count,file_size_in_bytes from lab.silver.orders.files').show(truncate=False)

# %% Exercise 4: append, time travel, then restore the baseline
snapshot = spark.sql('select snapshot_id from lab.silver.orders.snapshots order by committed_at desc').first()[0]
new_order = stg_orders.orderBy('order_id').limit(1).withColumn('order_id', F.lit('00000000-0000-0000-0000-000000000099'))
new_order.writeTo('lab.silver.orders').append()
assert spark.table('lab.silver.orders').count() == stg_orders.count() + 1
previous = spark.sql(f'select * from lab.silver.orders version as of {snapshot}')
agree(stg_orders, previous, 'Iceberg pre-append snapshot')
spark.sql('select snapshot_id,operation,committed_at from lab.silver.orders.snapshots order by committed_at').show(truncate=False)
parquet_path = f's3a://{bucket}/silver/spark/orders.parquet'
new_order.write.mode('append').parquet(parquet_path)
assert spark.read.parquet(parquet_path).count() == stg_orders.count() + 1
print('Parquet after append:', spark.read.parquet(parquet_path).count(), 'rows; no snapshot catalogue.', flush=True)
stg_orders.write.mode('overwrite').parquet(parquet_path)
spark.sql(f"call lab.system.rollback_to_snapshot(table => 'silver.orders', snapshot_id => {snapshot})")
agree(stg_orders, spark.table('lab.silver.orders'), 'Restored Iceberg baseline')
agree(stg_orders, spark.read.parquet(parquet_path), 'Restored Parquet baseline')

# %% Exercise 2: native expression, Python UDF and Pandas UDF
def extract_zip(address):
    match = re.search(r'([A-Z]{2}) ([0-9]{5})$', address or '')
    return match.group(2) if match else ''


python_zip = F.udf(extract_zip, T.StringType())


@F.pandas_udf('string')
def pandas_zip(addresses: pd.Series) -> pd.Series:
    return addresses.fillna('').str.extract(r'([0-9]{5})$', expand=False).fillna('')


expressions = {
    'native regexp_extract': F.regexp_extract('address', r'([A-Z]{2}) ([0-9]{5})$', 2),
    'Python UDF': python_zip('address'),
    'Pandas UDF': pandas_zip('address')}
expected = None
for label, expression in expressions.items():
    frame = users_bronze.select(expression.alias('zip_code'))
    values = sorted(row.zip_code for row in frame.collect())
    if expected is None:
        expected = values
    assert values == expected
    # Force evaluation of the expression: count() alone could be optimized away.
    def evaluate():
        return frame.agg(F.sum(F.length('zip_code'))).first()[0]
    assert evaluate() == 250
    samples = timeit.repeat(evaluate, number=1, repeat=3)
    print('ZIP timing', label, 'seconds=', [round(value, 4) for value in samples], flush=True)
print('Final UI Storage:', [{key: item.get(key) for key in
      ('id', 'name', 'numCachedPartitions', 'memoryUsed')} for item in ui('storage/rdd')], flush=True)
print('DATAFRAME_LAB_OK', flush=True)
users_bronze.unpersist()
orders_bronze.unpersist()
spark.stop()
