"""PostgreSQL connection helpers."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
from urllib.parse import unquote, urlparse

from src.config import Settings

# Arbitrary constant: serialises warehouse writers (Airflow tasks, CLI runs).
WAREHOUSE_LOCK_ID = 7_220_301


def _psycopg():
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - environment problem
        raise SystemExit("Install dependencies: pip install -r requirements.txt") from exc
    return psycopg


@contextmanager
def connect(settings: Settings, autocommit: bool = False) -> Iterator["psycopg.Connection"]:
    psycopg = _psycopg()
    conn = psycopg.connect(settings.database_url, autocommit=autocommit, connect_timeout=10)
    try:
        yield conn
    finally:
        conn.close()


def acquire_warehouse_lock(cursor) -> None:
    """Transaction-scoped advisory lock; released automatically on commit/rollback."""
    cursor.execute("SELECT pg_advisory_xact_lock(%s)", (WAREHOUSE_LOCK_ID,))


def jdbc_options(settings: Settings) -> dict[str, str]:
    """Translate DATABASE_URL into Spark JDBC connection options."""
    url = urlparse(settings.database_url)
    return {
        "url": f"jdbc:postgresql://{url.hostname}:{url.port or 5432}{url.path}",
        "user": unquote(url.username or ""),
        "password": unquote(url.password or ""),
        "driver": "org.postgresql.Driver",
    }


def ping(settings: Settings) -> bool:
    with connect(settings, autocommit=True) as conn:
        return conn.execute("SELECT 1").fetchone()[0] == 1
