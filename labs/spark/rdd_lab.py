"""Spark RDD lab and exercises. Run with spark-submit from the repo root.

The cells use the local bronze CSV copies downloaded with AWS CLI.
"""
# %% SparkContext and partitioning
import csv
import json
from operator import add
from pathlib import Path
import re
import time
from urllib.request import urlopen

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('lab-rdd').master('local[*]').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')
print('Spark:', spark.version, 'default parallelism:', sc.defaultParallelism)


def ui(resource):
    with urlopen(f'{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{resource}', timeout=15) as response:
        return json.load(response)


def timed(label, action):
    started = time.perf_counter()
    result = action()
    print(label, 'result=', result, 'seconds=', round(time.perf_counter() - started, 4))
    return result


users_lines = sc.textFile('users.csv')
orders_lines = sc.textFile('orders.csv')
print('Initial partitions:', users_lines.getNumPartitions(), orders_lines.getNumPartitions())
print('Repartitioned orders:', orders_lines.repartition(8).getNumPartitions())
header = orders_lines.first()  # first() is already an action.
orders_data = orders_lines.filter(lambda line: line != header and bool(line.strip()))
print('Lazy filter lineage:\n', orders_data.toDebugString().decode())
before = len(ui('jobs'))
timed('Uncached count 1', orders_data.count)
between = len(ui('jobs'))
timed('Uncached count 2', orders_data.count)
after = len(ui('jobs'))
print('UI jobs before / after each count:', before, between, after)

# %% Multiline CSV limitation
print('First six physical user lines:')
for line in users_lines.take(6):
    print(line)
uuid_at_start = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
users_records = users_lines.filter(lambda line: uuid_at_start.match(line) is not None)
print('Physical user lines / UUID-leading records:', users_lines.count(), users_records.count())
with Path('users.csv').open(newline='') as source:
    expected_users = len(list(csv.DictReader(source)))
assert users_records.count() == expected_users


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(',')
    return dict(order_id=order_id, user_id=user_id, date=date,
                quantity=int(quantity), product=product.strip().lower())


# %% reduceByKey versus groupByKey
orders_rdd = orders_data.map(parse_order).cache()
print('Sample orders:', orders_rdd.take(3))
quantities = orders_rdd.map(lambda order: (order['user_id'], order['quantity']))
user_quantity = quantities.reduceByKey(add)
sc.setJobGroup('reduceByKey', 'Per-user quantity with map-side combining')
reduced = dict(user_quantity.collect())  # Only 50 aggregate rows reach the driver.
sc.setJobGroup('groupByKey', 'Per-user quantity without map-side combining')
grouped = dict(quantities.groupByKey().mapValues(sum).collect())
assert reduced == grouped
print('Aggregation agreement:', len(reduced), 'users; total quantity:', sum(reduced.values()))
print('First five sorted aggregates:', sorted(reduced.items())[:5])
print('Completed UI stage shuffle metrics:')
for stage in sorted(ui('stages?status=complete'), key=lambda item: item['stageId']):
    print({key: stage.get(key) for key in ('stageId', 'name', 'numTasks', 'shuffleReadBytes', 'shuffleWriteBytes')})

# %% Deduplication and joins
def most_recent(first, second):
    return first if first['date'] > second['date'] else second


orders_dedup = orders_rdd.map(lambda order: (order['order_id'], order)).reduceByKey(most_recent).values()
print('Orders before / after deduplication:', orders_rdd.count(), orders_dedup.count())
# Also exercise the combiner with an older and newer copy without modifying bronze.
original = orders_rdd.first()
older = dict(original, date='2019-01-01 00:00:00+00:00')
fixture = orders_rdd.union(sc.parallelize([older, original], 1))
fixture_dedup = fixture.map(lambda order: (order['order_id'], order)).reduceByKey(most_recent)
assert fixture_dedup.count() == orders_rdd.count()
assert fixture_dedup.lookup(original['order_id'])[0]['date'] == original['date']
print('Duplicate replay check passed.')

users_kv = users_records.map(lambda line: (line.split(',')[0], line.split(',')[1]))
orders_kv = orders_rdd.map(lambda order: (order['user_id'], order))
enriched = orders_kv.join(users_kv)
print('Joined orders:', enriched.count(), 'sample:', enriched.take(1))
assert enriched.count() == orders_rdd.count()

# %% Exercise 1: count, sum and average in one aggregateByKey
def add_order(acc, quantity):
    return acc[0] + 1, acc[1] + quantity


def combine(acc_a, acc_b):
    return acc_a[0] + acc_b[0], acc_a[1] + acc_b[1]


stats = quantities.aggregateByKey((0, 0), add_order, combine)
averages = stats.mapValues(lambda value: (value[0], value[1], value[1] / value[0]))
print('Exercise 1: user_id, order_count, quantity, average')
for user_id, (count, quantity, average) in sorted(averages.collect()):
    print(user_id, count, quantity, round(average, 4))

# %% Exercise 2: one unknown user, visible in a left outer join
fake = dict(original, order_id='00000000-0000-0000-0000-000000000001',
            user_id='00000000-0000-0000-0000-000000000000')
with_unknown = orders_rdd.union(sc.parallelize([fake], 1))
orphans = (with_unknown.map(lambda order: (order['user_id'], order))
           .leftOuterJoin(users_kv).filter(lambda item: item[1][1] is None))
assert orphans.count() == 1
print('Exercise 2: unmatched orders:', orphans.count(), 'record:', orphans.collect())

# %% Exercise 3: DataFrame aggregation and plans
orders_df = spark.createDataFrame(orders_rdd)
quantity_df = orders_df.groupBy('user_id').agg(F.sum('quantity').alias('quantity'))
assert {row.user_id: row.quantity for row in quantity_df.collect()} == reduced
print('Exercise 3: DataFrame and RDD aggregates agree.')
quantity_df.explain(mode='formatted')
print('RDD lineage:\n', user_quantity.toDebugString().decode())

# %% Exercise 4: cached and uncached actions, plus the Storage tab API
orders_rdd.count()
print('Storage with cached parsed orders:', [
    {key: item.get(key) for key in ('id', 'name', 'numCachedPartitions', 'memoryUsed')}
    for item in ui('storage/rdd')])
for repetition in range(3):
    timed(f'Cached orders count {repetition+1}', orders_rdd.count)
    timed(f'Cached user_quantity count {repetition+1}', user_quantity.count)
orders_rdd.unpersist(blocking=True)
print('Storage after unpersist:', ui('storage/rdd'))
for repetition in range(3):
    timed(f'Uncached orders count {repetition+1}', orders_rdd.count)
    timed(f'Uncached user_quantity count {repetition+1}', user_quantity.count)
print('RDD_LAB_OK')
spark.stop()
