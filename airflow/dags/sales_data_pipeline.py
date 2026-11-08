"""
### sales_data_pipeline

Daily sales lake pipeline: source -> **Bronze** (raw CSV/JSON in MinIO) -> **Silver**
(validated, de-duplicated Parquet) -> **Gold** (aggregates) -> **PostgreSQL** star
schema, with data-quality gates and run metrics in `monitoring.*`.

* **Logical date** = the business day being processed (data interval start). A run
  for 2026-03-01 is scheduled at 2026-03-02 00:00 UTC once the day is complete.
* **Manual run**: *Trigger DAG* (optionally choose a logical date / `source_mode`).
* **Backfill**: `airflow backfill create --dag-id sales_data_pipeline --from-date 2026-03-01 --to-date 2026-03-07 --max-active-runs 1`
* Every task calls one function in `src/orchestration/lake_pipeline.py`; retries are
  safe because each step is idempotent (see that module's docstring).
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

log = logging.getLogger("sales_data_pipeline")
# 1-slot pool: lake/warehouse writers never overlap, even across backfill runs
# (Airflow 3 backfills use their own --max-active-runs, not the DAG setting).
PIPELINE_POOL = "pipeline_writes"


def _context():
    """Build the RunContext for the current task instance."""
    from src.config import load_settings
    from src.orchestration.lake_pipeline import RunContext

    context = get_current_context()
    override = (context["params"] or {}).get("business_date")
    if override:
        logical_day = date.fromisoformat(override)
    else:
        logical = context.get("logical_date") or context["dag_run"].run_after
        logical_day = pendulum.instance(logical).in_timezone("UTC").date()
    ti = context["ti"]
    return RunContext(load_settings(), context["run_id"], logical_day, ti.try_number)


def _run(step_name: str, *args):
    """Execute a pipeline step with task-level monitoring."""
    from src.monitoring.metrics import track_task
    from src.orchestration import lake_pipeline
    from src.utils.logging import configure_logging

    configure_logging()
    ctx = _context()
    with track_task(ctx.settings, ctx.run_id, step_name, ctx.try_number) as task_metrics:
        result = getattr(lake_pipeline, step_name)(ctx, *args)
        task_metrics.update(result or {})
    log.info("%s -> %s", step_name, json.dumps(result, default=str)[:2000])
    return result


def notify_failure(context) -> None:
    """Final-failure callback: mark the run failed and send an optional webhook."""
    from src.config import load_settings
    from src.orchestration.lake_pipeline import mark_failed

    settings = load_settings()
    ti = context["ti"]
    error = f"{ti.task_id} failed: {context.get('exception')}"
    log.error("PIPELINE FAILURE run_id=%s %s", context["run_id"], error)
    mark_failed(settings, context["run_id"], error)
    if settings.alert_webhook_url:
        try:
            import requests

            requests.post(settings.alert_webhook_url, timeout=10, json={
                "text": f":red_circle: sales_data_pipeline {context['run_id']} - {error}"})
        except Exception:  # alerting must never raise
            log.exception("failure webhook could not be delivered")


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="sales_data_pipeline",
    description="CSV/API -> MinIO bronze/silver/gold (PySpark) -> PostgreSQL star schema",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,          # no concurrent writers to the lake or the warehouse
    dagrun_timeout=timedelta(hours=3),
    default_args=DEFAULT_ARGS,
    params={
        "source_mode": Param("auto", enum=["auto", "generate", "file", "api"],
                             description="auto: incoming file > API (if SALES_API_URL) > generator"),
        "business_date": Param(None, type=["null", "string"], format="date",
                               description="Optional override of the processed business date"),
    },
    tags=["sales", "lake", "spark", "warehouse"],
    doc_md=__doc__,
)
def sales_data_pipeline():
    @task(retries=3)
    def check_source() -> dict:
        return _run("check_source", get_current_context()["params"]["source_mode"])

    @task
    def generate_or_fetch_sales(source: dict) -> dict:
        return _run("generate_or_fetch_sales", source)

    @task(pool=PIPELINE_POOL)
    def upload_bronze(extract: dict) -> dict:
        return _run("upload_bronze", extract)

    @task(retries=0)  # quality gates fail fast: retrying does not fix bad data
    def validate_bronze(bronze: dict) -> dict:
        return _run("validate_bronze", bronze)

    @task(pool=PIPELINE_POOL)
    def transform_silver() -> dict:
        return _run("transform_silver")

    @task(retries=0, pool=PIPELINE_POOL)
    def validate_silver(silver: dict) -> dict:
        return _run("validate_silver", silver)

    @task(pool=PIPELINE_POOL)
    def transform_gold() -> dict:
        return _run("transform_gold")

    @task(pool=PIPELINE_POOL)
    def load_data_warehouse() -> dict:
        return _run("load_data_warehouse")

    @task(retries=0)
    def verify_warehouse() -> dict:
        return _run("verify_warehouse")

    @task
    def record_pipeline_metrics(silver: dict, warehouse: dict, bronze: dict) -> dict:
        return _run("record_pipeline_metrics", silver, warehouse, bronze)

    source = check_source()
    extract = generate_or_fetch_sales(source)
    bronze = upload_bronze(extract)
    bronze_ok = validate_bronze(bronze)
    silver = transform_silver()
    bronze_ok >> silver
    silver_ok = validate_silver(silver)
    gold = transform_gold()
    silver_ok >> gold
    warehouse = load_data_warehouse()
    gold >> warehouse
    verified = verify_warehouse()
    warehouse >> verified
    metrics = record_pipeline_metrics(silver, warehouse, bronze)
    verified >> metrics


sales_data_pipeline()
