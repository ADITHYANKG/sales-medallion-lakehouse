"""End-to-end verification report for a running stack.

    docker compose run --rm app python scripts/verify_pipeline.py

Checks (each prints PASS/FAIL; exit code 1 if anything fails):
* PostgreSQL reachable, migrations applied, warehouse data-quality checks
* Gold aggregates reconcile with the fact table
* MinIO buckets exist; Bronze has batches; Silver/Gold have published versions
* Latest pipeline run status; read-only reporting role cannot write
* Airflow API and Metabase health endpoints (if reachable)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import get_settings  # noqa: E402
from src.validation.quality import GOLD_RECONCILIATION_CHECKS, WAREHOUSE_CHECKS, run_sql_checks  # noqa: E402
from src.warehouse.db import connect  # noqa: E402

results: list[tuple[bool, str, str]] = []


def record(ok: bool, name: str, detail: str = "") -> None:
    results.append((ok, name, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name:<45} {detail}")


def main() -> int:
    settings = get_settings()
    with connect(settings) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM public.schema_migrations")
        record(True, "postgres reachable / migrations applied", f"{cur.fetchone()[0]} migrations")
        for r in run_sql_checks(cur, WAREHOUSE_CHECKS + GOLD_RECONCILIATION_CHECKS):
            record(r.passed, f"warehouse.{r.name}", f"observed={r.observed:g}")
        cur.execute("""SELECT COUNT(*), COUNT(*) FILTER (WHERE order_status='completed'),
                              COALESCE(SUM(net_amount) FILTER (WHERE order_status='completed'),0)
                       FROM mart.fact_sales""")
        rows, completed, revenue = cur.fetchone()
        record(rows > 0, "fact_sales populated", f"{rows:,} rows, {completed:,} completed, revenue {revenue:,.2f}")
        cur.execute("SELECT run_id, status, error_message FROM monitoring.pipeline_runs ORDER BY started_at DESC LIMIT 1")
        latest = cur.fetchone()
        record(bool(latest) and latest[1] == "success", "latest pipeline run succeeded",
               f"{latest[0]} -> {latest[1]}" if latest else "no runs yet")

    if settings.reader_password:
        import psycopg

        url = settings.database_url.replace(settings.database_url.split("//")[1].split("@")[0],
                                            f"{settings.reader_user}:{settings.reader_password}")
        try:
            with psycopg.connect(url) as conn:
                conn.execute("DELETE FROM mart.fact_sales WHERE false")
            record(False, "reporting role is read-only", "DELETE was allowed!")
        except psycopg.Error as exc:
            record(True, "reporting role is read-only", type(exc).__name__)

    if settings.s3_configured:
        from src.storage.object_store import ObjectStore

        store = ObjectStore(settings)
        bronze = [o for o in store.list_objects(settings.bronze_bucket, "sales/") if o.key.endswith((".csv", ".jsonl"))]
        record(bool(bronze), "bronze has source batches", f"{len(bronze)} files")
        pointer = store.current_version(settings.silver_bucket, "sales")
        record(bool(pointer), "silver/sales published", pointer["version"] if pointer else "missing")
        for table in ("daily_sales", "monthly_sales", "category_sales", "product_sales",
                      "customer_sales", "location_sales"):
            p = store.current_version(settings.gold_bucket, table)
            record(bool(p), f"gold/{table} published", f"{p.get('records')} rows" if p else "missing")

    import requests

    for name, url in (("airflow api-server", os.environ.get("AIRFLOW_HEALTH_URL", "http://airflow-apiserver:8080/api/v2/monitor/health")),
                      ("metabase", os.environ.get("METABASE_URL", "http://metabase:3000") + "/api/health")):
        try:
            response = requests.get(url, timeout=5)
            record(response.ok, f"{name} healthy", f"HTTP {response.status_code}")
        except requests.RequestException as exc:
            print(f"SKIP  {name:<45} not reachable ({type(exc).__name__})")

    failed = [r for r in results if not r[0]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
