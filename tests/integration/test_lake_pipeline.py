"""End-to-end lake pipeline (MinIO + Spark + PostgreSQL) in isolated test resources."""
from datetime import date

import psycopg
import pytest

from src.orchestration.lake_pipeline import run_lake_pipeline
from src.validation.quality import DataQualityError

pytestmark = pytest.mark.integration


def one(settings, sql):
    with psycopg.connect(settings.database_url) as conn:
        return conn.execute(sql).fetchone()


@pytest.fixture()
def lake_env(clean_warehouse, clean_lake, spark):
    return clean_warehouse


def test_end_to_end_idempotent_with_updates_and_quarantine(lake_env):
    settings = lake_env
    day1 = run_lake_pipeline(settings, date(2026, 3, 1), "test_day1")
    totals1 = day1["load_data_warehouse"]
    assert totals1["fact_rows"] == day1["transform_silver"]["silver_records"]

    # 1. Re-run the same day: identical business result, no new batch, no fact changes.
    rerun = run_lake_pipeline(settings, date(2026, 3, 1), "test_day1_rerun")
    assert rerun["upload_bronze"]["new_batch"] is False
    assert rerun["load_data_warehouse"]["fact_sales_upserted"] == 0
    for key in ("fact_rows", "completed_rows", "completed_revenue"):
        assert rerun["load_data_warehouse"][key] == totals1[key]

    # 2. Next day carries status changes for day-1 orders -> superseded + cancellations applied.
    day2 = run_lake_pipeline(settings, date(2026, 3, 2), "test_day2")
    assert day2["transform_silver"]["superseded_versions"] > 0
    stale = one(settings, """
        SELECT COUNT(*) FROM mart.fact_sales f JOIN staging.sales_clean s USING (order_item_id)
        WHERE f.order_status <> s.order_status OR f.source_updated_at <> s.updated_at""")[0]
    assert stale == 0

    # 3. SQL on the star schema equals the Spark-built Gold aggregates.
    fact_revenue, gold_revenue = one(settings, """
        SELECT (SELECT SUM(net_amount) FROM mart.fact_sales WHERE order_status = 'completed'),
               (SELECT SUM(net_revenue) FROM mart.agg_daily_sales)""")
    assert fact_revenue == gold_revenue
    sql_orders, gold_orders = one(settings, """
        SELECT (SELECT COUNT(DISTINCT order_id) FROM mart.fact_sales WHERE order_status = 'completed'),
               (SELECT SUM(completed_orders) FROM mart.agg_daily_sales)""")
    assert sql_orders == gold_orders

    # 4. A broken batch fails the run, is quarantined, and does not poison later runs.
    settings.incoming_dir.mkdir(parents=True, exist_ok=True)
    (settings.incoming_dir / "sales_2026-03-03.csv").write_text("foo,bar\n1,2\n", encoding="utf-8")
    with pytest.raises(DataQualityError):
        run_lake_pipeline(settings, date(2026, 3, 3), "test_bad_batch")
    assert one(settings, "SELECT status FROM monitoring.pipeline_runs WHERE run_id = 'test_bad_batch'")[0] == "failed"
    assert one(settings, "SELECT COUNT(*) FROM monitoring.ingestion_batches WHERE status = 'quarantined'")[0] == 1
    (settings.incoming_dir / "sales_2026-03-03.csv").unlink()
    day4 = run_lake_pipeline(settings, date(2026, 3, 4), "test_day4")
    assert day4["record_pipeline_metrics"]["warehouse_records"] > totals1["fact_rows"]

    runs = one(settings, "SELECT COUNT(*) FILTER (WHERE status='success'), COUNT(*) FROM monitoring.pipeline_runs")
    assert runs == (4, 5)
    failed_checks = one(settings, "SELECT COUNT(*) FROM monitoring.data_quality_results WHERE NOT passed")[0]
    assert failed_checks >= 1
