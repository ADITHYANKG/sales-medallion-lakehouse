"""Shared fixtures.

Integration tests never touch the real warehouse or lake: they use a separate
database (``<db>_test``) and ``test-*`` buckets, and skip automatically when the
services (or Java for Spark) are not reachable.
"""
from __future__ import annotations

import dataclasses
import logging
import shutil
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pytest

from src.config import load_settings

logging.getLogger("py4j").setLevel(logging.WARNING)  # quiet JVM gateway shutdown messages


def _with_db(url: str, dbname: str) -> str:
    parts = urlparse(url)
    return urlunparse(parts._replace(path=f"/{dbname}"))


@pytest.fixture(scope="session")
def base_settings():
    return load_settings()


@pytest.fixture(scope="session")
def test_settings(base_settings, tmp_path_factory):
    dbname = urlparse(base_settings.database_url).path.lstrip("/") + "_test"
    return dataclasses.replace(
        base_settings,
        database_url=_with_db(base_settings.database_url, dbname),
        bronze_bucket="test-bronze", silver_bucket="test-silver", gold_bucket="test-gold",
        data_dir=tmp_path_factory.mktemp("data"),
        daily_records=300, dirty_rate=0.02, spark_shuffle_partitions=4,
        reader_password=base_settings.reader_password or "test_reader_pw",
    )


def _postgres_available(settings) -> bool:
    try:
        import psycopg

        with psycopg.connect(settings.database_url, connect_timeout=3):
            return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres(base_settings, test_settings):
    if not _postgres_available(base_settings):
        pytest.skip("PostgreSQL not reachable (start: docker compose up -d postgres)")
    import psycopg

    dbname = urlparse(test_settings.database_url).path.lstrip("/")
    with psycopg.connect(base_settings.database_url, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone():
            conn.execute(f'CREATE DATABASE "{dbname}"')
    return test_settings


@pytest.fixture()
def clean_warehouse(postgres):
    """Fresh schema for each test that needs one."""
    import psycopg

    from src.warehouse.migrate import migrate

    with psycopg.connect(postgres.database_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS raw, staging, mart, monitoring, legacy CASCADE")
        conn.execute("DROP TABLE IF EXISTS public.schema_migrations")
    migrate(postgres)
    shutil.rmtree(postgres.rejected_dir, ignore_errors=True)
    return postgres


@pytest.fixture(scope="session")
def object_store(test_settings):
    if not test_settings.s3_configured:
        pytest.skip("S3/MinIO credentials not configured")
    from src.storage.object_store import ObjectStore

    store = ObjectStore(test_settings)
    try:
        store.ensure_lake_buckets()
    except Exception as exc:
        pytest.skip(f"MinIO not reachable: {exc}")
    return store


@pytest.fixture()
def clean_lake(object_store, test_settings):
    for bucket in (test_settings.bronze_bucket, test_settings.silver_bucket, test_settings.gold_bucket):
        object_store.delete_prefix(bucket, "")
    return object_store


@pytest.fixture(scope="session")
def spark(test_settings):
    if shutil.which("java") is None and not Path("/opt/java/bin/java").exists():
        pytest.skip("Java is not installed (run the tests inside the Docker image)")
    pytest.importorskip("pyspark")
    from src.transformations.spark_session import build_spark

    session = build_spark(test_settings, "pytest", master="local[2]")
    yield session
    session.stop()


def write_csv(path: Path, rows: list[dict]) -> Path:
    import csv

    from src.validation.schema import SOURCE_COLUMNS

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SOURCE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


VALID_ROW = {
    "order_id": "ORD-1", "order_item_id": "ORD-1-01", "order_date": "2026-01-05",
    "customer_id": "C1", "customer_name": "Asha Rao", "customer_email": "asha@example.com",
    "product_id": "P1", "product_name": "Laptop", "category": "Electronics",
    "quantity": "2", "unit_price": "100.50", "discount": "0.10", "total_amount": "180.90",
    "currency": "INR", "payment_method": "upi", "order_status": "completed",
    "city": "Pune", "state": "Maharashtra", "country": "India", "updated_at": "2026-01-05T10:00:00Z",
}


@pytest.fixture()
def valid_row():
    return dict(VALID_ROW)
