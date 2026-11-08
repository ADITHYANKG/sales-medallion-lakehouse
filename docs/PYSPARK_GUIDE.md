# PySpark guide — Spark internals through this pipeline

All examples refer to `src/transformations/spark_jobs.py` (transformations) and
`src/transformations/spark_session.py` (configuration).

## 1. Runtime architecture

| Concept | What it is | In this project |
|---|---|---|
| **Driver** | The process that runs your Python program, builds the logical plan, asks for resources and schedules tasks. | The Airflow task process (or `docker compose run app ...`). `build_spark()` creates the `SparkSession`; `SPARK_DRIVER_MEMORY` sizes its JVM. |
| **Executor** | JVM processes that run tasks and hold cached data. | With `local[*]` the executors live *inside* the driver JVM, one task thread per core. With the `spark-cluster` profile, `spark-worker` launches executor JVMs. |
| **Cluster manager** | Allocates executors (Standalone, YARN, Kubernetes). | None for `local[*]`; the optional **Standalone** master (`spark://spark-master:7077`). |
| **Job** | Triggered by every *action* (`count`, `write`, `collect`, `first`). | `transform_silver` triggers several: write Silver, write quarantine, the stats `agg().first()`, `silver.count()`. |
| **Stage** | Part of a job between shuffles. | Reading + validating Bronze is one stage; the dedup window needs a shuffle by `order_item_id`, which starts a new stage. |
| **Task** | One unit of work on one partition in a stage. | A Bronze file < 128 MB is one input partition -> one task; after the shuffle there are `spark.sql.shuffle.partitions` tasks (8 by default, `SPARK_SHUFFLE_PARTITIONS`). |
| **Partition** | A chunk of a DataFrame processed by one task. | Input partitions come from files/splits; `repartition(n, "order_month")` before the Silver write controls output files. |

Spark UI: while a job runs, `http://localhost:4040` inside the running container (publish the port if
needed), or the standalone master UI at http://localhost:8081.

## 2. Lazy evaluation

Transformations (`select`, `filter`, `withColumn`, `join`, `groupBy`) only build a plan; nothing runs
until an action. `validate_bronze_df()` defines ~40 column expressions, yet no data is read until
`write_silver()` is called. Spark's Catalyst optimizer can then push filters down, prune unused
columns and fuse operators into one stage (*whole-stage code generation*).

Consequence used in this project: the annotated DataFrame is used by **three** actions (Silver write,
quarantine write, statistics). Without caching, Bronze would be re-read and re-validated three
times, so `bronze_to_silver()` calls `.cache()` and `transform_silver` calls `.unpersist()` at the end.

## 3. Narrow vs wide transformations

| Narrow (no data movement, same stage) | Wide (shuffle, new stage) |
|---|---|
| `select`, `withColumn`, `filter`, `cast`, `regexp_replace`, `sha2`, `when` in `validate_bronze_df` | `Window.partitionBy("order_item_id")` in `deduplicate` |
| `coalesce(1)` (merges partitions without a shuffle) | `groupBy(...).agg(...)` in every Gold table |
| | `join(latest_product, "product_id")`, `repartition(n, "order_month")`, `orderBy` |

```python
# narrow: each output row depends on one input row
annotated = raw.withColumn("source_row_hash", F.sha2(F.concat_ws(SEP, *cols), 256))

# wide: all versions of an order_item_id must meet in one partition
w = Window.partitionBy("order_item_id").orderBy(F.col("updated_at").desc(), ...)
silver = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
```

## 4. Shuffle

A shuffle writes map-side output to local disk, then each reducer task fetches its keys over the
network (or local disk in `local[*]`). It is the main cost driver for Spark jobs.

* `spark.sql.shuffle.partitions` (here 8) sets the number of reduce partitions. The Spark default of
  200 would create 200 tiny tasks for a few thousand rows.
* **Adaptive Query Execution** (`spark.sql.adaptive.enabled=true`) coalesces small shuffle partitions
  and can switch join strategies at runtime.
* The Gold `product_sales` join is small on both sides, so Spark broadcasts one side instead of
  shuffling both (verified: the executed plan of `product_sales` contains `BroadcastHashJoin`;
  inspect it with `frame._jdf.queryExecution().executedPlan()` or `frame.explain("formatted")`).
* Windows **without** `partitionBy` (`Window.orderBy("order_date")` for the 7-day moving average)
  move all rows to one partition. This is acceptable only because the input is already aggregated to
  one row per day; on raw data it would be a bottleneck (Spark logs a warning).

## 5. `repartition` vs `coalesce`

```python
# Silver: one output file per order_month partition directory
silver.withColumn("order_month", F.date_format("order_date", "yyyy-MM")) \
      .repartition(partitions, "order_month") \          # full shuffle, hash by month
      .sortWithinPartitions("order_month", "order_item_id") \
      .write.partitionBy("order_month").parquet(uri)

# Gold: tiny tables -> a single file, no shuffle
frame.coalesce(1).write.parquet(uri)
```

`repartition` can increase or decrease partitions and balances data (shuffle). `coalesce` only
decreases by merging existing partitions (no shuffle) — cheap, but it can create uneven tasks and
reduces upstream parallelism if used too early.

**Small-files problem:** writing `partitionBy("order_month")` from 8 random partitions would create
up to 8 files per month. Repartitioning by the same column first gives one file per month.

## 6. Explicit schemas instead of inference

```python
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in S.SOURCE_COLUMNS])
spark.read.schema(RAW_SCHEMA).option("header", "true").option("enforceSchema", "false").csv(uri)
```

* Inference reads the data twice and guesses types per file — a batch where `quantity` contains
  `"abc"` would silently become a string column in one file and an integer in another.
* Reading everything as **string** keeps Bronze lossless; typing happens in Silver with explicit,
  failure-tolerant casts.
* `enforceSchema=false` makes Spark check the CSV header names against the schema instead of
  blindly mapping by position.
* Partition columns (`ingestion_date`, `batch_id`) come from directory names; type inference for them
  is disabled (`spark.sql.sources.partitionColumnTypeInference.enabled=false`) so a batch id like
  `b0123...` is never turned into a number.

## 7. ANSI mode (Spark 4 default) and safe casts

Spark 4 enables ANSI SQL semantics: an invalid cast raises an error instead of returning NULL.
A single bad row would fail the whole job, so validation uses:

```python
quantity = F.when(matches("quantity", S.RE_INT), t["quantity"].try_cast("long"))
order_date = F.to_date(F.try_to_timestamp(text, F.lit("yyyy-MM-dd")))
```

`try_cast` / `try_to_timestamp` return NULL for bad input, which the rule then reports as
`invalid_quantity` / `invalid_order_date`.

## 8. Window functions used

| Function | Where | Purpose |
|---|---|---|
| `row_number()` over `partitionBy(order_item_id).orderBy(updated_at desc, ...)` | Silver | Deterministic de-duplication (latest version wins) |
| `avg(...)` over `rowsBetween(-6, 0)` | `daily_sales` | 7-day moving average |
| `sum(...)` over `rowsBetween(unboundedPreceding, currentRow)` | `daily_sales` | Cumulative revenue |
| `lag(net_revenue)` | `monthly_sales` | Month-over-month growth |
| `sum(...)` over `partitionBy()` | `category_sales` | Share of total |
| `row_number()` over `partitionBy(category)` | `product_sales` | Rank within category |

Deterministic tie-breakers are always included in `orderBy` (e.g. `product_id`), otherwise rank order
between equal values could differ between runs.

## 9. Caching

```python
annotated = validate_bronze_df(raw, currencies).cache()   # reused by 3 actions
silver = SJ.read_silver(...).cache()                      # Gold: 6 aggregations + reconciliation
```

`cache()` = `persist(MEMORY_AND_DISK)` for DataFrames; it is lazy (filled by the first action) and
must be released with `unpersist()`. Cache only what is reused, and never "just in case": it
consumes executor memory that joins and aggregations need.

## 10. Fault tolerance

* Every DataFrame is described by its **lineage** (the plan). If an executor dies, Spark recomputes
  only the lost partitions from the source (Bronze files are immutable, so recomputation is exact).
* Failed tasks are retried (`spark.task.maxFailures`, default 4 on a cluster).
* Shuffle files allow a failed downstream stage to restart without recomputing upstream stages.
* At pipeline level, failures are handled by Airflow retries plus **write-then-publish**: each attempt
  writes `version=<run>-t<try>` and only a successful, validated attempt moves the `_CURRENT.json`
  pointer. S3 has no atomic directory rename, so this pointer pattern replaces the
  "write to `_temporary` + rename" approach Hadoop committers use on HDFS.

## 11. Data skew

Skew = a few keys hold most of the rows, so one task runs far longer than the others.

* Here: `groupBy("category")` has only 8 keys — fine for small data, but with billions of rows the
  "Electronics" task would dominate. `groupBy("customer_id")` is naturally well distributed.
* Detection: the Spark UI stage page (task duration / shuffle read max vs median).
* Remedies: AQE skew-join handling (`spark.sql.adaptive.skewJoin.enabled`), **salting**
  (add `rand() % N` to the key, aggregate twice), broadcasting the small side of a join, or
  pre-aggregating before the wide operation (the Gold windows run on already aggregated data).

## 12. Spark + MinIO (S3A) configuration

```properties
spark.jars                                   hadoop-aws-3.4.2.jar, bundle-2.29.52.jar (AWS SDK v2), postgresql-42.7.14.jar
spark.hadoop.fs.s3a.endpoint                 http://minio:9000
spark.hadoop.fs.s3a.path.style.access        true      # MinIO serves buckets as paths, not subdomains
spark.hadoop.fs.s3a.connection.ssl.enabled   false
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.sql.session.timeZone                   UTC
```

The `hadoop-aws` version must equal the Hadoop version bundled with Spark (`hadoop-client-api-3.4.2.jar`
inside PySpark 4.1.1), and the AWS SDK bundle must be the one `hadoop-aws` was built against
(declared in the `hadoop-project` POM). Mixing versions produces `NoSuchMethodError` /
`ClassNotFoundException` at the first S3 access.

## 13. JDBC writes to PostgreSQL

```python
frame.write.format("jdbc").options(url=..., dbtable="staging.sales_clean", driver="org.postgresql.Driver")
     .option("truncate", "true").option("batchsize", "10000").option("numPartitions", "4").mode("overwrite").save()
```

* Executors write in parallel (`numPartitions` connections) — data never passes through the driver
  (no `collect()`/`toPandas()` of large data).
* `mode=overwrite` + `truncate=true` keeps the table definition (types, constraints) and only empties it.
* Spark JDBC writes are not one transaction across partitions, so they target **staging** tables only;
  the mart is changed by a single PostgreSQL transaction afterwards, under an advisory lock.

## 14. Measured behaviour

See [benchmark_results.md](benchmark_results.md) for measured Pandas vs Spark timings on this project's
workload, including JVM start-up cost and why Spark is slower than pandas for small inputs on a
single machine.
