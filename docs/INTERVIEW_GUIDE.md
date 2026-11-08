# Interview guide — 50 questions answered with this project

Answers are short and concrete; each points to where the project implements the idea, so you can
open the code during an interview walkthrough.

## Python

**1. How does the generator handle 1 million rows without running out of memory?**
It streams: `orders_for_day()` is a generator that yields one order at a time and rows are written
immediately with `csv.DictWriter`. Only one day of orders exists in memory (`src/ingestion/generator.py`).

**2. How do you make synthetic data reproducible?**
Every random stream is a `random.Random(f"{seed}:{purpose}:{day}")`. String seeds are hashed
deterministically, and separate streams mean the *clean* rows don't change when the dirty-data rate
changes (a unit test asserts byte-identical output for identical arguments).

**3. Why integer cents instead of floats or `Decimal` in pandas?**
Floats can't represent 0.1 exactly, and Python `Decimal` objects are slow in vectorised code.
`int64` cents/basis points are exact and fast:
`net_cents = (q * price_cents * (10000 - discount_bp) + 5000) // 10000` (half-up rounding).

**4. How is configuration separated from code?**
A frozen `Settings` dataclass is built from environment variables (optionally a `.env` file), with
safe local defaults and no secrets in code (`src/config/settings.py`). Compose passes container-specific
values (hostnames) while `.env` holds secrets.

**5. How do you avoid hiding the original error when monitoring fails?**
`track_task()` records the task result in a `finally` block wrapped in its own `try/except` that only
logs, then the original exception propagates (`src/monitoring/metrics.py`).

## SQL

**6. Write the query for average order value.**
`SUM(net_amount) FILTER (WHERE order_status='completed') / NULLIF(COUNT(DISTINCT order_id) FILTER (WHERE order_status='completed'), 0)`.
`NULLIF` avoids division by zero; `DISTINCT order_id` because the fact grain is the line item.

**7. How do you compute month-over-month growth?**
`LAG(revenue) OVER (ORDER BY year_month)` and `(revenue - prev) / NULLIF(prev,0) * 100`
(`sql/analytics/sales_kpis.sql`, query `monthly_sales`).

**8. `ROW_NUMBER`, `RANK` and `DENSE_RANK`?**
`ROW_NUMBER` is always unique (used for deterministic de-duplication); `RANK` leaves gaps after ties
(1,1,3); `DENSE_RANK` does not (1,1,2). Product rankings use `ROW_NUMBER` with an id tie-breaker so
results are stable between runs.

**9. What does `DISTINCT ON` do?**
PostgreSQL keeps the first row per key according to `ORDER BY`. The merge picks the latest customer
version with `DISTINCT ON (customer_id) ... ORDER BY customer_id, updated_at DESC`.

**10. How do you find orphaned fact rows?**
`LEFT JOIN dim ... WHERE dim.key IS NULL` — part of `WAREHOUSE_CHECKS` (`src/validation/quality.py`).

**11. What is `IS DISTINCT FROM` and why use it in an upsert?**
A NULL-safe inequality. `ON CONFLICT DO UPDATE ... WHERE (old cols) IS DISTINCT FROM (new cols)` skips
no-op updates, so a re-run touches 0 rows (no table bloat, honest `updated_at`).

## PostgreSQL

**12. How is the load made idempotent?**
Raw rows: `ON CONFLICT (batch_id, source_row) DO NOTHING`. Facts: `ON CONFLICT (order_item_id) DO UPDATE`
guarded by `source_updated_at <= EXCLUDED.source_updated_at` and change detection. Batches are
content-addressed (sha256), so the same file is the same batch.

**13. What happens if the load fails half-way?**
All mart changes for a run are one transaction; any error rolls everything back. The warehouse-only
path also rolls back raw/staging, while the quarantine CSV and failure metrics are written outside
the transaction so they survive.

**14. How do you prevent two pipelines writing at once?**
`pg_advisory_xact_lock` (transaction-scoped) for merges, and a session-level `pg_advisory_lock` held
across the Spark JDBC staging load + merge. Airflow additionally uses `max_active_runs=1` and a
1-slot `pipeline_writes` pool (backfills ignore the DAG's `max_active_runs`, the pool does not).

**15. Why `COPY` instead of row-by-row `INSERT`?**
`COPY` streams many rows in one command with minimal per-row overhead (orders of magnitude faster).
It loads into a temp table first, then `INSERT ... SELECT ... ON CONFLICT` provides idempotency.

**16. How are schema changes managed?**
Versioned migration files `sql/migrations/V###__name.sql`, applied once each in a transaction and
recorded with a checksum in `public.schema_migrations`; editing an applied migration is detected.
V001 moved the Phase-1 tables to a `legacy` schema instead of dropping them.

**17. How is least privilege implemented for BI?**
`metabase_reader`: `LOGIN`, `SELECT` on `mart` and `monitoring` only, default privileges for future
tables, no access to `raw`/`staging`, and `default_transaction_read_only = on`. Tests assert that
`DELETE` fails.

## Data engineering

**18. ETL vs ELT — which is this?**
Both. Bronze and `raw.sales_records` are loaded untransformed (EL); typed transformations happen
afterwards in Spark (Silver/Gold) and SQL (the merge). Keeping raw data enables reprocessing.

**19. What is idempotency and how do you test it?**
Running the same input twice yields the same state. The integration tests run the same batch twice
and assert identical fact counts and revenue, `new_batch == False` and 0 upserted rows.

**20. How do you handle late-arriving or updated records?**
Every row carries `updated_at`. Silver keeps the latest version per `order_item_id`; the merge only
accepts versions at least as new as the stored one, so late *older* data can't overwrite newer data.

**21. How do you handle invalid records?**
Row-level quarantine with reasons (`staging.sales_rejected`, `data/rejected/*.csv`,
`silver/sales_rejected/`) plus a reject-rate gate: if more than `MAX_REJECT_RATE` of a batch is bad,
the batch is quarantined and the run fails — a sign of an upstream problem, not bad luck.

**22. Duplicates — exact vs logical?**
Exact duplicates have the same row hash (identical content); logical duplicates share
`order_item_id` with different content (an update). Both are counted separately
(`exact_duplicates`, `superseded_versions`).

**23. Event date vs ingestion date vs logical date?**
*Event date* = `order_date` (when the sale happened). *Ingestion date* = the Bronze partition the batch
was stored under (`ingestion_date=`; set to the run's logical date so backfills are reproducible; the
real wall-clock time is in object metadata). *Logical date* = the data interval an Airflow run
represents; the 2026-03-01 run executes after midnight on 2026-03-02.

**24. What data-quality checks run?**
Bronze: existence, checksum, header contract, batch reject rate. Silver: uniqueness, null keys,
negative values, revenue formula, record reconciliation, reject rate. Gold: revenue reconciliation per
table. Warehouse: duplicates, orphans, revenue formula, invalid status, snapshot coverage, Gold =
fact. All results are stored in `monitoring.data_quality_results`.

## Data lake

**25. Explain the medallion architecture.**
Bronze = raw, immutable, as received (CSV/JSONL + metadata). Silver = validated, typed,
de-duplicated, conformed (Parquet). Gold = business aggregates (Parquet). Each layer can be rebuilt
from the previous one.

**26. Why Parquet?**
Columnar (read only needed columns), compressed, typed schema embedded, supports predicate pushdown
and partition pruning; the standard for Spark/warehouses.

**27. How is Bronze organised and why?**
`bronze/sales/ingestion_date=YYYY-MM-DD/batch_id=<sha>/sales.csv` + `_metadata.json` (source,
checksum, run id, timestamps). Hive-style partitions are discovered by Spark as columns, and the
content-addressed batch id makes duplicates detectable.

**28. How do you avoid readers seeing half-written data on S3?**
Write to an immutable `version=<id>/` prefix, validate, then atomically overwrite a small
`_CURRENT.json` pointer. Old versions are pruned after `LAKE_VERSIONS_TO_KEEP`. (S3 has no atomic
directory rename; this replaces the HDFS rename-commit pattern.)

**29. How do you avoid the small-files problem?**
Silver is `repartition(n, "order_month")` before `partitionBy("order_month")` → one file per month;
Gold tables are tiny and written with `coalesce(1)`.

**30. MinIO vs S3?**
MinIO implements the S3 API, so boto3 and Hadoop S3A work unchanged; differences are mainly
configuration (`path.style.access=true`, custom endpoint, no SSL locally).

## Data warehouse

**31. What is the grain of your fact table?**
One row per order line item (`order_item_id`). Order-level metrics use `COUNT(DISTINCT order_id)`.
Declaring the grain first prevents double counting.

**32. Why surrogate keys?**
Stable integer join keys independent of source ids; allow SCD Type 2 (several rows per natural key);
smaller indexes and faster joins.

**33. SCD Type 1 vs Type 2?**
Type 1 overwrites (current view only; implemented). Type 2 adds a new row with validity dates for
each change (history as it was; design documented in `DATA_MODEL.md`).

**34. Why keep cancelled orders in the fact table?**
Cancellation rate and total orders need them, and auditability. Revenue is defined as completed
`net_amount`, applied consistently in views, Gold and dashboards.

**35. What is a degenerate dimension?**
A dimension key with no attributes table — `order_id` on the fact table.

## PySpark

**36. Narrow vs wide transformations?**
Narrow: each output partition depends on one input partition (`withColumn`, `filter`). Wide: needs a
shuffle (`groupBy`, window `partitionBy`, joins). See `PYSPARK_GUIDE.md`.

**37. Why might Spark be slower than pandas here?**
JVM start-up, task scheduling and shuffle overhead dominate small inputs on one machine. Spark's
advantage is scaling beyond one machine's memory and parallelising across nodes. The benchmark
reports measured numbers (`docs/benchmark_results.md`).

**38. Why explicit schemas?**
No inference pass, no type drift between files, lossless string Bronze, failures handled explicitly
in validation.

**39. How do you de-duplicate deterministically in Spark?**
`row_number()` over `partitionBy("order_item_id").orderBy(updated_at desc, ingestion_date desc,
batch_id desc, source_row_hash desc)`, keep 1. The final tie-breaker is content-based, so the
result is independent of input order and partitioning.

**40. How do you load Spark output into PostgreSQL safely?**
JDBC writes from executors into staging tables (`truncate=true` keeps DDL), then one PostgreSQL
transaction merges staging into the mart under an advisory lock. Never `collect()` large data.

**41. What does ANSI mode change in Spark 4?**
Invalid casts/arithmetic raise errors instead of returning NULL; validation uses `try_cast` and
`try_to_timestamp` so one bad row cannot fail the job.

## Apache Airflow

**42. Describe the DAG.**
`check_source → generate_or_fetch_sales → upload_bronze → validate_bronze → transform_silver →
validate_silver → transform_gold → load_data_warehouse → verify_warehouse → record_pipeline_metrics`,
each a TaskFlow task calling one function in `lake_pipeline.py` (no logic in the DAG file).

**43. How are retries made safe?**
Every step is idempotent: deterministic extracts, content-addressed Bronze, per-attempt output
versions, pointer publish after validation, guarded merge. Quality gates have `retries=0`.

**44. How do backfills work?**
`airflow backfill create --dag-id sales_data_pipeline --from-date ... --to-date ...` creates one run per
logical date; the generator/API fetch use that date. Pass `--max-active-runs 1`: Airflow 3 backfills do
not inherit the DAG's `max_active_runs`. The `pipeline_writes` pool and the Silver publish guard keep
concurrent runs safe anyway (observed during verification: 7 backfill runs started at once).

**45. How are failures surfaced?**
`on_failure_callback` marks the run failed in `monitoring.pipeline_runs` with the error and posts to
`ALERT_WEBHOOK_URL`; task logs are in the Airflow UI; the Metabase monitoring dashboard shows
success rate and failed checks.

**46. Why `CronDataIntervalTimetable`?**
In Airflow 3 the default cron timetable sets the logical date to the trigger time. Data-interval
semantics make the logical date the *start of the day being processed*, which is what a daily batch
ETL needs.

## Docker

**47. Why one image for Airflow, Spark and the CLI?**
Driver/executor/Airflow-worker version mismatches (Python, Java, Hadoop JARs) are the most common
PySpark-in-Docker failure. One image built with the Airflow constraint file removes that class of bugs.

**48. How are services started in the right order?**
`depends_on` with `condition: service_healthy` (healthchecks for PostgreSQL, MinIO, Metabase,
Airflow) and `service_completed_successfully` for the `init` and `airflow-init` jobs.

## System design

**49. How would you scale this to 1 TB/day?**
Incremental Silver (process only new batches and `MERGE` into a table format such as Delta
Lake/Iceberg instead of full recompute), partition by event date, Spark on Kubernetes/EMR,
CeleryExecutor or KubernetesExecutor for Airflow, a columnar warehouse (or Postgres partitioning +
BRIN indexes), and file compaction.

**50. What would you change for production?**
Secrets manager instead of `.env`, TLS everywhere, an Iceberg/Delta table format for ACID upserts in
the lake, data contracts with the source team, OpenLineage, alerting to PagerDuty/Slack, CI running
the test suite on every change, infrastructure as code, and SCD Type 2 where history matters.
