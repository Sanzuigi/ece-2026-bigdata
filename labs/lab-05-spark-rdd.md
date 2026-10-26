# Spark RDD Lab

Roy Homsi, Big Data Framework

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/06.spark-introduction/lab-pyspark-rdd.md)

## Environment and execution

I used the existing Onyxia project, downloaded the two bronze CSVs with the default AWS profile and ran [rdd_lab.py](spark/rdd_lab.py) with Spark 4.1.1. The source is divided into cells so each part can also be explored in VS Code. Results are in [rdd-results.txt](spark/evidence/rdd-results.txt).

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv
aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv
spark-submit --conf spark.ui.showConsoleProgress=false labs/spark/rdd_lab.py
```

The calculations use local copies. The original bronze objects are unchanged. RDDs can use Hadoop input formats and S3A paths when the connector is configured; downloading the files is this lab's chosen setup, rather than a general limitation of RDDs.

## Partitions, lazy evaluation and multiline CSV

For these small local files, `textFile()` uses the Hadoop input splits and Spark's minimum partition setting, which is normally based on `min(defaultParallelism, 2)`. The size and number of files can change the result. Repartitioning to eight creates a shuffle. Reducing partitions with the usual `coalesce(n)` avoids a shuffle but may produce less balanced partitions; `coalesce(n, shuffle=True)` requests one explicitly.

The filter builds a lineage until an action needs its result. There is already an action in `orders_lines.first()`, which obtains the header. Each subsequent uncached count creates another job and computes the filter again. Calling `cache()` on the filtered or parsed RDD before its first action allows later reads to reuse the materialized partitions, provided those partitions remain available. It still creates jobs; caching does not make an action disappear from the UI.

The users file contains 50 records, but each address contains a newline. `textFile()` treats physical lines independently. Selecting UUID-leading lines recovers the record starts needed for UUID and username, but it does not reconstruct the address. A CSV reader with `multiLine=True` follows CSV quoting and reconstructs complete records. A line reader is suitable only when record boundaries coincide with line boundaries, or a separate reconstruction step is supplied.

## Parsing, aggregations and deduplication

Splitting orders on commas is sufficient for these generated records. An extra comma inside a product would produce too many values to unpack and fail when an action evaluates that row. Spark's CSV reader instead applies quoting, the chosen schema and its malformed-record policy. A DataFrame is also lazy, so data errors may surface during execution; schema inference can itself cause an earlier scan. `FAILFAST` would reject malformed input rather than silently accepting the permissive default.

Both aggregations returned the same quantities for 50 users, totalling 8,615. I inspected the completed stages in the live Spark UI. The `reduceByKey` shuffle wrote 2,513 bytes, while `groupByKey` wrote 6,230 bytes. The first combines quantities within each input partition before transferring partial sums. The second transfers the individual quantities and groups them afterwards. Timing on this small dataset includes scheduling overhead and is not a useful general speed benchmark.

Some operations need the values themselves, such as preserving every observation for arbitrary per-key processing. `groupByKey` can express that. A mean does not require it: a count and sum can be combined. Even some more complex statistics have specialized mergeable summaries, so grouping every value should not be the default choice.

For deduplication, mapping each order to `(order_id, order)` and reducing with `most_recent` brings equal identifiers together. This key partitioning and shuffle play the role of `PARTITION BY order_id` in the DataFrame window. The 2,829 original orders remain 2,829. I also replayed an order and added an older version in a temporary RDD; the result still had 2,829 identifiers and retained the original later timestamp. The window expression states the intent more directly, while the RDD version requires a custom combiner. Ties need an additional deterministic rule if equal timestamps can contain conflicting payloads.

## Joining users and orders

The inner join returns 2,829 enriched orders. Spark can avoid an additional join shuffle when both pair RDDs already have compatible partitioners placing the same keys on the same partitions. Simply having equal partition counts is insufficient, and the default inputs here do not meet that condition.

A left outer join retains orders whose user is missing and supplies `None` for the username. The inner join would discard those orders. Calling `collect()` on the entire enriched dataset would bring all results into driver memory, which can exhaust it on a large dataset. `take()` is suitable for inspection; distributed writes or aggregations are suitable for larger results.

## Exercises

### 1. Count, total and average per user

`aggregateByKey` starts from `(0, 0)`. Its sequence function adds one to the count and the order's quantity to the sum. Its combiner adds two partial counts and sums. Dividing the final sum by its count gives the average order quantity for each user. The evidence contains all 50 aggregate rows, and the quantities agree with the earlier `reduceByKey` result.

### 2. Missing user

I copied one order into a temporary RDD and gave it a new order UUID and an unknown user UUID. A left outer join followed by a check for `None` found exactly one unmatched order. This does not alter the source CSV or S3 bronze layer.

### 3. DataFrame equivalent

The DataFrame aggregation uses `groupBy('user_id').agg(sum('quantity'))`, with no custom value-pair mapping or Python reduction function. The result agrees with the RDD aggregation. The RDD aggregate expression and DataFrame expression are each one executable line in the submitted source; the RDD version additionally depends on its pair-RDD definition. Line count alone does not measure maintainability.

`toDebugString()` describes RDD dependencies and shuffle boundaries. The DataFrame's formatted `explain()` also shows the relational plan, partial/final aggregations and exchange, with optimization handled by Catalyst. This is useful when the computation can be expressed relationally.

### 4. Cache experiment

I measured three repetitions of both counts with `time.perf_counter()`, then called `unpersist(blocking=True)` and repeated them. I inspected the Spark Storage page after removal and verified that it listed no cached RDD. The cached partitions and their memory use were also recorded through the UI's REST API before removal.

The timings are retained in the evidence rather than replaced by a single claimed speedup. The dataset is small, the operating system can cache the local files, and Spark can reuse shuffle outputs from a previous aggregate even when its parent RDD is no longer persisted. Those effects explain why removing `cache()` does not necessarily make every later action slower.

## Completion

All four exercises ran successfully. The live Spark UI showed completed jobs and no failed job. I stopped the session after inspecting the stages and storage. The temporary local CSVs are excluded from Git and removed after the Spark work.
