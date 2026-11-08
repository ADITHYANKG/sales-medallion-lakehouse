"""Warehouse-only path against a real PostgreSQL (isolated *_test database)."""
import psycopg
import pytest

from src.ingestion.csv_ingest import ingest_file
from src.ingestion.generator import generate_dataset
from src.validation.quality import DataQualityError
from tests.conftest import write_csv

pytestmark = pytest.mark.integration


def q(settings, sql, *params):
    with psycopg.connect(settings.database_url) as conn:
        return conn.execute(sql, params).fetchall()


def fact(settings, item):
    return q(settings, "SELECT order_status, net_amount, customer_key FROM mart.fact_sales WHERE order_item_id = %s",
             item)[0]


def test_ingest_is_idempotent(clean_warehouse, tmp_path):
    path = tmp_path / "sales.csv"
    generate_dataset(path, records=1500, dirty_rate=0.03)
    first = ingest_file(path, clean_warehouse)
    second = ingest_file(path, clean_warehouse)
    assert first["new_batch"] and not second["new_batch"]
    assert second["raw_rows_inserted"] == 0 and second["fact_sales_upserted"] == 0
    for key in ("fact_rows", "completed_rows", "completed_revenue"):
        assert first[key] == second[key]
    raw_rows = q(clean_warehouse, "SELECT COUNT(*) FROM raw.sales_records")[0][0]
    assert raw_rows == first["source_records"]
    assert first["rejected_records"] > 0
    assert q(clean_warehouse, "SELECT COUNT(*) FROM staging.sales_rejected")[0][0] == first["rejected_records"]
    runs = q(clean_warehouse, "SELECT status FROM monitoring.pipeline_runs")
    assert [r[0] for r in runs] == ["success", "success"]


def test_cancellation_and_reinstatement(clean_warehouse, tmp_path, valid_row):
    ingest_file(write_csv(tmp_path / "v1.csv", [valid_row]), clean_warehouse)
    assert fact(clean_warehouse, "ORD-1-01")[0] == "completed"
    revenue = "SELECT COALESCE(SUM(net_amount),0) FROM mart.v_completed_sales"
    assert float(q(clean_warehouse, revenue)[0][0]) == 180.90

    cancelled = {**valid_row, "order_status": "cancelled", "updated_at": "2026-01-06T08:00:00Z"}
    ingest_file(write_csv(tmp_path / "v2.csv", [cancelled]), clean_warehouse)
    assert fact(clean_warehouse, "ORD-1-01")[0] == "cancelled"
    assert float(q(clean_warehouse, revenue)[0][0]) == 0.0

    back = {**valid_row, "order_status": "completed", "updated_at": "2026-01-07T08:00:00Z"}
    ingest_file(write_csv(tmp_path / "v3.csv", [back]), clean_warehouse)
    assert float(q(clean_warehouse, revenue)[0][0]) == 180.90
    assert q(clean_warehouse, "SELECT COUNT(*) FROM mart.fact_sales")[0][0] == 1


def test_older_version_never_overwrites_newer(clean_warehouse, tmp_path, valid_row):
    newer = {**valid_row, "order_status": "cancelled", "updated_at": "2026-01-08T00:00:00Z"}
    ingest_file(write_csv(tmp_path / "new.csv", [newer]), clean_warehouse)
    ingest_file(write_csv(tmp_path / "old.csv", [valid_row]), clean_warehouse)  # late, older replay
    assert fact(clean_warehouse, "ORD-1-01")[0] == "cancelled"


def test_scd1_customer_update_keeps_surrogate_key(clean_warehouse, tmp_path, valid_row):
    ingest_file(write_csv(tmp_path / "a.csv", [valid_row]), clean_warehouse)
    key_before = fact(clean_warehouse, "ORD-1-01")[2]
    renamed = {**valid_row, "order_id": "ORD-2", "order_item_id": "ORD-2-01", "customer_name": "Asha R. Iyer",
               "order_date": "2026-01-09", "updated_at": "2026-01-09T10:00:00Z"}
    ingest_file(write_csv(tmp_path / "b.csv", [renamed]), clean_warehouse)
    rows = q(clean_warehouse, "SELECT customer_key, customer_name FROM mart.dim_customer WHERE customer_id = 'C1'")
    assert rows == [(key_before, "Asha R. Iyer")]
    assert fact(clean_warehouse, "ORD-2-01")[2] == key_before


def test_reject_rate_gate_rolls_back_everything(clean_warehouse, tmp_path, valid_row):
    good = [{**valid_row, "order_item_id": f"ORD-1-{i:02d}"} for i in range(5)]
    bad = [{**valid_row, "order_item_id": f"BAD-{i}", "quantity": "-1"} for i in range(5)]
    with pytest.raises(DataQualityError, match="reject rate"):
        ingest_file(write_csv(tmp_path / "bad.csv", good + bad), clean_warehouse)
    assert q(clean_warehouse, "SELECT COUNT(*) FROM mart.fact_sales")[0][0] == 0
    assert q(clean_warehouse, "SELECT COUNT(*) FROM raw.sales_records")[0][0] == 0
    status, error = q(clean_warehouse, "SELECT status, error_message FROM monitoring.pipeline_runs")[0]
    assert status == "failed" and "reject rate" in error
    assert list(clean_warehouse.rejected_dir.glob("*_rejected.csv"))  # quarantine file still written


def test_legacy_phase1_file_still_loads(clean_warehouse, tmp_path):
    path = tmp_path / "legacy.csv"
    path.write_text("transaction_id,transaction_date,customer_id,customer_name,product_id,product_name,"
                    "quantity,unit_price,status\n"
                    "TXN1,2026-02-05,C002,Customer 002,P001,Laptop,2,55000,completed\n"
                    "TXN2,2026-02-06,C003,Customer 003,P002,Mouse,1,750,cancelled\n", encoding="utf-8")
    summary = ingest_file(path, clean_warehouse)
    assert summary["legacy_format"] and summary["completed_revenue"] == 110000.0


def test_reader_role_is_read_only(clean_warehouse):
    from src.warehouse.migrate import ensure_reader_role

    ensure_reader_role(clean_warehouse)
    url = clean_warehouse.database_url.replace(
        clean_warehouse.database_url.split("//")[1].split("@")[0],
        f"{clean_warehouse.reader_user}:{clean_warehouse.reader_password}")
    with psycopg.connect(url) as conn:
        conn.execute("SELECT COUNT(*) FROM mart.fact_sales")
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("DELETE FROM mart.fact_sales")
    with psycopg.connect(url) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("SELECT * FROM raw.sales_records")
