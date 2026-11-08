"""Lake pipeline steps (Bronze -> Silver -> Gold -> Warehouse).

Every Airflow task in ``airflow/dags/sales_data_pipeline.py`` calls exactly one
function from this module, and ``run_lake_pipeline`` calls them in sequence for
local CLI runs. Functions return small JSON-serialisable dicts (Airflow XComs).

Idempotency strategy
--------------------
* Extract: generated data is deterministic per logical date; API responses are
  canonicalised; so a retry produces identical bytes.
* Bronze: content-addressed batch id (sha256) + batch registry => a re-sent file
  is detected and not uploaded/landed twice.
* Silver/Gold: full recompute from all Bronze batches into a new immutable
  version prefix; published by swapping a pointer only after quality checks.
* Warehouse: staging snapshot + guarded upsert (newest ``updated_at`` wins)
  in one transaction under an advisory lock.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from src.config import Settings
from src.ingestion.api_source import fetch_sales
from src.ingestion.generator import generate_daily_batch
from src.monitoring import metrics
from src.storage.object_store import ObjectStore
from src.utils.ids import batch_id_for_checksum, safe_token, sha256_file
from src.utils.logging import get_logger
from src.validation import schema as S
from src.validation.quality import (GOLD_RECONCILIATION_CHECKS, WAREHOUSE_CHECKS, CheckResult,
                                    assert_passed, check, reject_rate_check, run_sql_checks)
from src.warehouse import loader
from src.warehouse.db import WAREHOUSE_LOCK_ID, acquire_warehouse_lock, connect, jdbc_options, ping

log = get_logger(__name__)
BRONZE_DATASET = "sales"
SILVER_DATASET = "sales"
QUARANTINE_DATASET = "sales_rejected"


@dataclass
class RunContext:
    settings: Settings
    run_id: str
    logical_date: date
    try_number: int = 1
    extras: dict = field(default_factory=dict)

    @property
    def version(self) -> str:
        """Immutable output version id for Silver/Gold written by this run attempt."""
        return f"{safe_token(self.run_id)}-t{self.try_number}"

    def store(self) -> ObjectStore:
        return ObjectStore(self.settings)


def _uri(bucket: str, prefix: str) -> str:
    return f"s3a://{bucket}/{prefix}"


def _record(ctx: RunContext, results: list[CheckResult]) -> None:
    metrics.record_quality_results(ctx.settings, ctx.run_id, results)
    assert_passed(results)


# ------------------------------------------------------------------ 1. check_source
def check_source(ctx: RunContext, source_mode: str = "auto") -> dict:
    """Verify dependencies are reachable and decide where today's data comes from."""
    settings = ctx.settings
    ping(settings)
    created = ctx.store().ensure_lake_buckets()
    metrics.start_pipeline_run(settings, ctx.run_id, "lake", ctx.logical_date,
                               {"source_mode": source_mode})
    ds = ctx.logical_date.isoformat()
    candidates = [settings.incoming_dir / f"sales_{ds}{ext}" for ext in (".csv", ".jsonl")]
    incoming = next((p for p in candidates if p.is_file()), None)
    if source_mode in ("auto", "file") and incoming:
        mode = "file"
    elif source_mode == "file":
        raise FileNotFoundError(f"no incoming file for {ds}: expected one of {[str(c) for c in candidates]}")
    elif source_mode == "api" or (source_mode == "auto" and settings.sales_api_url):
        if not settings.sales_api_url:
            raise ValueError("source_mode=api requires SALES_API_URL")
        mode = "api"
    else:
        mode = "generate"
    log.info("source for %s: %s", ds, mode)
    return {"mode": mode, "path": str(incoming) if mode == "file" else None, "buckets_created": created}


# -------------------------------------------------------- 2. generate_or_fetch_sales
def generate_or_fetch_sales(ctx: RunContext, source: dict) -> dict:
    settings, ds = ctx.settings, ctx.logical_date.isoformat()
    mode = source["mode"]
    if mode == "file":
        path = Path(source["path"])
    elif mode == "api":
        path = settings.generated_dir / "api" / f"sales_{ds}.jsonl"
        fetch_sales(settings.sales_api_url, ctx.logical_date, path)
    else:
        path = settings.generated_dir / "daily" / f"sales_{ds}.csv"
        stats = generate_daily_batch(path, ctx.logical_date, records=settings.daily_records,
                                     seed=settings.generator_seed, dirty_rate=settings.dirty_rate)
        log.info("generated %s", stats.as_dict())
    return {"mode": mode, "path": str(path), "bytes": path.stat().st_size,
            "format": "jsonl" if path.suffix in (".json", ".jsonl") else "csv"}


# ------------------------------------------------------------------ 3. upload_bronze
def upload_bronze(ctx: RunContext, extract: dict) -> dict:
    """Store the untouched source file in Bronze and land its rows in raw.sales_records."""
    from src.ingestion.csv_ingest import read_source
    from src.validation.pandas_rules import normalize_columns, row_hashes

    settings, store = ctx.settings, ctx.store()
    path = Path(extract["path"])
    checksum = sha256_file(path)
    batch_id = batch_id_for_checksum(checksum)
    existing = metrics.lookup_batch(settings, checksum)
    ext = "jsonl" if extract["format"] == "jsonl" else "csv"
    if existing and existing["object_key"] and store.exists(settings.bronze_bucket, existing["object_key"]):
        key, ingestion_date, reused = existing["object_key"], existing["ingestion_date"], True
        log.info("batch %s already in bronze at %s; not re-uploading", batch_id, key)
    else:
        ingestion_date, reused = ctx.logical_date, False
        key = f"{BRONZE_DATASET}/ingestion_date={ingestion_date.isoformat()}/batch_id={batch_id}/sales.{ext}"
        meta = {"source": extract["mode"], "checksum-sha256": checksum, "batch-id": batch_id,
                "ingested-at": datetime.now(timezone.utc).isoformat(), "run-id": ctx.run_id,
                "original-name": path.name}
        store.upload_file(path, settings.bronze_bucket, key, meta)
        store.put_json({**meta, "bytes": path.stat().st_size, "object_key": key},
                       settings.bronze_bucket, key.rsplit("/", 1)[0] + "/_metadata.json")

    # Land rows in raw.sales_records. A file that violates the column contract is
    # still kept in Bronze (never lose source data); validate_bronze quarantines it.
    from src.validation.pandas_rules import SchemaError

    records, raw_inserted, schema_error = 0, 0, None
    with connect(settings) as conn, conn.transaction():
        cur = conn.cursor()
        acquire_warehouse_lock(cur)
        first_row = 2 if ext == "csv" else 1
        try:
            with conn.transaction():  # savepoint: a bad file must not abort the registry write
                for chunk in read_source(path):
                    frame, _ = normalize_columns(chunk)
                    raw_inserted += loader.load_raw_records(cur, batch_id, frame, row_hashes(frame), first_row)
                    first_row += len(frame)
                    records += len(frame)
        except SchemaError as exc:
            schema_error, records, raw_inserted = str(exc), 0, 0
            log.error("batch %s violates the column contract: %s", batch_id, exc)
        new = metrics.register_batch(cur, batch_id, checksum, extract["mode"], str(path), key,
                                     path.stat().st_size, records, ingestion_date)
    return {"batch_id": batch_id, "bucket": settings.bronze_bucket, "key": key, "checksum": checksum,
            "records": records, "new_batch": new, "reused_object": reused, "raw_rows_inserted": raw_inserted,
            "local_path": str(path), "schema_error": schema_error}


# ---------------------------------------------------------------- 4. validate_bronze
def _quarantine_batch(ctx: RunContext, bronze: dict, reason: str) -> str:
    """Move a failed batch out of the active Bronze prefix (idempotent)."""
    store, bucket, key = ctx.store(), bronze["bucket"], bronze["key"]
    if key.startswith("quarantine/"):
        target = key
    else:
        target = f"quarantine/{key}"
        folder = key.rsplit("/", 1)[0] + "/"
        for obj in list(store.list_objects(bucket, folder)):
            store.client.copy_object(Bucket=bucket, Key=f"quarantine/{obj.key}",
                                     CopySource={"Bucket": bucket, "Key": obj.key})
            store.client.delete_object(Bucket=bucket, Key=obj.key)
    with connect(ctx.settings, autocommit=True) as conn:
        conn.execute("""UPDATE monitoring.ingestion_batches
                        SET status = 'quarantined', quarantine_reason = %s, object_key = %s
                        WHERE batch_id = %s""", (reason[:2000], target, bronze["batch_id"]))
    log.error("batch %s quarantined at s3://%s/%s: %s", bronze["batch_id"], bucket, target, reason)
    return target


def validate_bronze(ctx: RunContext, bronze: dict) -> dict:
    """Bronze gate: integrity (exists, checksum), contract (header) and batch reject rate.

    A failing batch is quarantined (moved to ``quarantine/``) and the run fails, so
    the bad batch is excluded from every later Silver recompute.
    """
    from src.ingestion.csv_ingest import validate_file
    from src.validation.pandas_rules import SchemaError

    settings, store = ctx.settings, ctx.store()
    bucket, key = bronze["bucket"], bronze["key"]
    exists = store.exists(bucket, key)
    remote_checksum = store.checksum(bucket, key) if exists else None
    results = [
        check("bronze", "object_exists", int(exists), exists, "= 1"),
        check("bronze", "checksum_matches", int(remote_checksum == bronze["checksum"]),
              remote_checksum == bronze["checksum"], "= 1", details=key),
        check("bronze", "column_contract", int(not bronze.get("schema_error")),
              not bronze.get("schema_error"), "= 1", details=bronze.get("schema_error")),
        check("bronze", "record_count_positive", bronze["records"], bronze["records"] > 0, "> 0"),
    ]
    if exists and key.endswith(".csv"):
        head = store.client.get_object(Bucket=bucket, Key=key, Range="bytes=0-8191")["Body"].read()
        header = head.decode("utf-8-sig", "replace").splitlines()[0].strip().split(",")
        ok = header == S.SOURCE_COLUMNS
        results.append(check("bronze", "header_matches_contract", int(ok), ok, "= 1",
                             details=None if ok else f"got {header}"))
    if not bronze.get("schema_error") and bronze["records"] > 0:
        try:  # per-batch reject rate, using the same rules as Silver
            local = Path(bronze.get("local_path") or "")
            if not local.is_file():
                local = store.download_file(bucket, key, settings.generated_dir / "_bronze_check" / Path(key).name)
            _, _, _, stats, _ = validate_file(local, settings, bronze["batch_id"], ctx.logical_date.isoformat())
            results.append(reject_rate_check("bronze", stats["source_records"], stats["rejected_records"],
                                             settings.max_reject_rate))
        except SchemaError as exc:
            results.append(check("bronze", "column_contract", 0, False, "= 1", details=str(exc)))
    metrics.record_quality_results(settings, ctx.run_id, results)
    failures = [r for r in results if not r.passed and r.severity == "error"]
    if failures:
        reason = "; ".join(f"{r.name} (observed={r.observed}, expected {r.expected})" for r in failures)
        if exists:
            _quarantine_batch(ctx, bronze, reason)
        assert_passed(results)
    return {"checks": len(results), "passed": True,
            "batch_reject_rate": next((r.observed for r in results if r.name == "reject_rate"), None)}


# --------------------------------------------------------------- 5. transform_silver
def _bronze_formats(ctx: RunContext) -> tuple[bool, bool]:
    keys = [o.key for o in ctx.store().list_objects(ctx.settings.bronze_bucket, f"{BRONZE_DATASET}/")]
    return any(k.endswith(".csv") for k in keys), any(k.endswith(".jsonl") for k in keys)


def transform_silver(ctx: RunContext) -> dict:
    from src.transformations import spark_jobs as SJ
    from src.transformations.spark_session import build_spark

    settings = ctx.settings
    has_csv, has_json = _bronze_formats(ctx)
    if not (has_csv or has_json):
        raise FileNotFoundError("bronze layer is empty")
    spark = build_spark(settings, f"bronze_to_silver[{ctx.run_id}]")
    started = time.monotonic()
    raw = SJ.read_bronze(spark, _uri(settings.bronze_bucket, f"{BRONZE_DATASET}/"), has_csv, has_json)
    silver, rejected, _valid, annotated = SJ.bronze_to_silver(raw, settings.allowed_currencies)
    silver_prefix = f"{SILVER_DATASET}/version={ctx.version}/"
    quarantine_prefix = f"{QUARANTINE_DATASET}/version={ctx.version}/"
    SJ.write_silver(silver, _uri(settings.silver_bucket, silver_prefix), settings.spark_shuffle_partitions)
    SJ.write_small(rejected.orderBy("batch_id", "source_row_hash"),
                   _uri(settings.silver_bucket, quarantine_prefix))
    stats = SJ.silver_stats(annotated, silver)
    stats["reject_reasons"] = SJ.reject_reason_counts(rejected)
    # Which Bronze batches this snapshot contains (used for optimistic publish checks).
    stats["bronze_batches"] = sorted(r[0] for r in annotated.select("batch_id").distinct().collect())
    annotated.unpersist()
    stats.update({"version": ctx.version, "silver_prefix": silver_prefix,
                  "quarantine_prefix": quarantine_prefix,
                  "seconds": round(time.monotonic() - started, 2)})
    log.info("silver candidate %s: %s", ctx.version, stats)
    return stats


# ---------------------------------------------------------------- 6. validate_silver
def validate_silver(ctx: RunContext, silver: dict) -> dict:
    """Quality gate on the candidate version; publishes it only if all checks pass."""
    from pyspark.sql import functions as F

    from src.transformations import spark_jobs as SJ
    from src.transformations.spark_session import build_spark

    settings = ctx.settings
    spark = build_spark(settings, f"validate_silver[{ctx.run_id}]")
    df = SJ.read_silver(spark, _uri(settings.silver_bucket, silver["silver_prefix"]))
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count_distinct("order_item_id").alias("distinct_items"),
        F.sum(F.when(F.col("order_item_id").isNull() | F.col("order_id").isNull()
                     | F.col("customer_id").isNull() | F.col("product_id").isNull()
                     | F.col("order_date").isNull(), 1).otherwise(0)).alias("null_keys"),
        F.sum(F.when((F.col("net_amount") < 0) | (F.col("unit_price") < 0) | (F.col("quantity") <= 0), 1)
              .otherwise(0)).alias("negative_values"),
        F.sum(F.when(F.col("net_amount") != F.round(
            F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount")), 2), 1)
              .otherwise(0)).alias("bad_revenue"),
        F.sum(F.when(~F.col("order_status").isin(*S.ORDER_STATUSES), 1).otherwise(0)).alias("bad_status"),
    ).first()
    rows = int(row["rows"])
    results = [
        check("silver", "row_count_matches_job", rows, rows == silver["silver_records"],
              f"= {silver['silver_records']}"),
        check("silver", "has_rows", rows, rows > 0, "> 0"),
        check("silver", "unique_order_item_id", rows - int(row["distinct_items"]),
              rows == int(row["distinct_items"]), "= 0"),
        check("silver", "null_keys", row["null_keys"] or 0, not row["null_keys"], "= 0"),
        check("silver", "negative_values", row["negative_values"] or 0, not row["negative_values"], "= 0"),
        check("silver", "revenue_formula", row["bad_revenue"] or 0, not row["bad_revenue"], "= 0"),
        check("silver", "valid_order_status", row["bad_status"] or 0, not row["bad_status"], "= 0"),
        check("silver", "records_reconcile",
              silver["valid_records"] + silver["rejected_records"],
              silver["valid_records"] + silver["rejected_records"] == silver["source_records"],
              f"= {silver['source_records']}"),
        reject_rate_check("silver", silver["source_records"], silver["rejected_records"],
                          settings.max_reject_rate),
    ]
    store = ctx.store()
    # Optimistic concurrency: never replace a published snapshot that contains Bronze
    # batches this candidate does not (a concurrent run published newer data first).
    current = store.current_version(settings.silver_bucket, SILVER_DATASET)
    candidate_batches = set(silver.get("bronze_batches") or [])
    missing = sorted(set((current or {}).get("bronze_batches") or []) - candidate_batches)
    results.append(check("silver", "candidate_not_older_than_published", len(missing), not missing, "= 0",
                         severity="warn", details=f"missing batches {missing[:5]}" if missing else None))
    _record(ctx, results)
    if missing:
        log.warning("not publishing %s: published version %s already contains newer batches %s",
                    silver["version"], current["version"], missing[:5])
        return {"published_version": current["version"], "records": current.get("records"),
                "skipped_stale_candidate": silver["version"]}
    info = {"records": rows, "run_id": ctx.run_id, "bronze_batches": sorted(candidate_batches)}
    store.publish_version(settings.silver_bucket, SILVER_DATASET, silver["version"], info)
    store.publish_version(settings.silver_bucket, QUARANTINE_DATASET, silver["version"],
                          {"records": silver["rejected_records"], "run_id": ctx.run_id})
    pruned = store.prune_versions(settings.silver_bucket, SILVER_DATASET, settings.versions_to_keep)
    store.prune_versions(settings.silver_bucket, QUARANTINE_DATASET, settings.versions_to_keep)
    return {"published_version": silver["version"], "records": rows, "pruned_versions": pruned}


# ----------------------------------------------------------------- 7. transform_gold
def _current_prefix(ctx: RunContext, bucket: str, dataset: str) -> str:
    pointer = ctx.store().current_version(bucket, dataset)
    if not pointer:
        raise FileNotFoundError(f"no published version of s3://{bucket}/{dataset}")
    return pointer["prefix"]


def transform_gold(ctx: RunContext) -> dict:
    from pyspark.sql import functions as F

    from src.transformations import spark_jobs as SJ
    from src.transformations.spark_session import build_spark

    settings = ctx.settings
    spark = build_spark(settings, f"silver_to_gold[{ctx.run_id}]")
    silver = SJ.read_silver(spark, _uri(settings.silver_bucket,
                                        _current_prefix(ctx, settings.silver_bucket, SILVER_DATASET))).cache()
    expected_revenue = silver.filter(F.col("order_status") == "completed").agg(
        F.coalesce(F.sum("net_amount"), F.lit(0))).first()[0]
    counts, results = {}, []
    for name, frame in SJ.build_gold(silver).items():
        SJ.write_small(frame.orderBy(*frame.columns[:1]), _uri(settings.gold_bucket, f"{name}/version={ctx.version}/"))
    for name in loader.GOLD_TABLES:  # re-read what was written and reconcile
        written = spark.read.parquet(_uri(settings.gold_bucket, f"{name}/version={ctx.version}/"))
        counts[name] = written.count()
        revenue_col = "lifetime_revenue" if name == "customer_sales" else "net_revenue"
        total = written.agg(F.coalesce(F.sum(revenue_col), F.lit(0))).first()[0]
        results.append(check("gold", f"{name}_revenue_reconciles", abs(total - expected_revenue),
                             total == expected_revenue, "= 0"))
    silver.unpersist()
    _record(ctx, results)
    store = ctx.store()
    for name in loader.GOLD_TABLES:
        store.publish_version(settings.gold_bucket, name, ctx.version, {"records": counts[name]})
        store.prune_versions(settings.gold_bucket, name, settings.versions_to_keep)
    return {"version": ctx.version, "tables": counts, "completed_revenue": float(expected_revenue)}


# ------------------------------------------------------------ 8. load_data_warehouse
def load_data_warehouse(ctx: RunContext) -> dict:
    """Spark JDBC -> staging tables, then one transactional merge into the mart.

    A session-level advisory lock is held for the whole step so no other writer
    can touch the staging tables between the JDBC load and the merge.
    """
    from src.transformations import spark_jobs as SJ
    from src.transformations.spark_session import build_spark

    settings = ctx.settings
    spark = build_spark(settings, f"load_warehouse[{ctx.run_id}]")
    jdbc = jdbc_options(settings)
    silver = SJ.read_silver(spark, _uri(settings.silver_bucket,
                                        _current_prefix(ctx, settings.silver_bucket, SILVER_DATASET)))

    def write(frame, table: str) -> None:
        (frame.write.format("jdbc").options(**jdbc).option("dbtable", table)
         .option("truncate", "true").option("batchsize", "10000")
         .option("numPartitions", str(min(4, settings.spark_shuffle_partitions)))
         .mode("overwrite").save())

    with connect(settings, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (WAREHOUSE_LOCK_ID,))
        try:
            write(silver.select(*S.CLEAN_COLUMNS), "staging.sales_clean")
            for name in loader.GOLD_TABLES:
                gold = spark.read.parquet(_uri(settings.gold_bucket, _current_prefix(ctx, settings.gold_bucket, name)))
                write(gold, f"staging.gold_{name}")
            with conn.transaction():
                cur = conn.cursor()
                acquire_warehouse_lock(cur)  # re-entrant for this session
                merged = loader.merge_mart(cur)
                swapped = loader.swap_gold_tables(cur)
                totals = loader.warehouse_totals(cur)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (WAREHOUSE_LOCK_ID,))
    return {**merged, **swapped, **totals}


# --------------------------------------------------------------- 9. verify_warehouse
def verify_warehouse(ctx: RunContext) -> dict:
    with connect(ctx.settings) as conn:
        cur = conn.cursor()
        results = run_sql_checks(cur, WAREHOUSE_CHECKS) + run_sql_checks(cur, GOLD_RECONCILIATION_CHECKS)
        totals = loader.warehouse_totals(cur)
    _record(ctx, results)
    return {"checks": len(results), **totals}


# -------------------------------------------------------- 10. record_pipeline_metrics
def record_pipeline_metrics(ctx: RunContext, silver: dict, warehouse: dict, bronze: dict | None = None) -> dict:
    counts = {
        "source_records": silver.get("source_records"),
        "valid_records": silver.get("valid_records"),
        "rejected_records": silver.get("rejected_records"),
        "duplicate_records": silver.get("duplicate_records"),
        "output_records": silver.get("silver_records"),
        "warehouse_records": warehouse.get("fact_rows"),
    }
    details = {"bronze": bronze or {}, "silver_version": silver.get("version"),
               "reject_reasons": silver.get("reject_reasons"), "warehouse": warehouse}
    metrics.finish_pipeline_run(ctx.settings, ctx.run_id, "success", counts, details=details)
    return counts


def mark_failed(settings: Settings, run_id: str, error: str) -> None:
    try:
        metrics.finish_pipeline_run(settings, run_id, "failed", error=error)
    except Exception:  # never mask the original failure
        log.exception("could not mark run %s as failed", run_id)


# ------------------------------------------------------------------ sequential run
TASKS = ("check_source", "generate_or_fetch_sales", "upload_bronze", "validate_bronze", "transform_silver",
         "validate_silver", "transform_gold", "load_data_warehouse", "verify_warehouse",
         "record_pipeline_metrics")


def run_lake_pipeline(settings: Settings, logical_date: date, run_id: str,
                      source_mode: str = "auto") -> dict:
    """Run all DAG steps in-process (same functions Airflow calls)."""
    ctx = RunContext(settings, run_id, logical_date)
    out: dict[str, dict] = {}

    def step(name, fn, *args):
        with metrics.track_task(settings, run_id, name) as task_metrics:
            result = fn(ctx, *args)
            task_metrics.update(result if isinstance(result, dict) else {})
        out[name] = result
        log.info("step %s done", name)
        return result

    try:
        source = step("check_source", check_source, source_mode)
        extract = step("generate_or_fetch_sales", generate_or_fetch_sales, source)
        bronze = step("upload_bronze", upload_bronze, extract)
        step("validate_bronze", validate_bronze, bronze)
        silver = step("transform_silver", transform_silver)
        step("validate_silver", validate_silver, silver)
        step("transform_gold", transform_gold)
        warehouse = step("load_data_warehouse", load_data_warehouse)
        step("verify_warehouse", verify_warehouse)
        step("record_pipeline_metrics", record_pipeline_metrics, silver, warehouse, bronze)
        return out
    except Exception as exc:
        mark_failed(settings, run_id, f"{type(exc).__name__}: {exc}")
        raise
