"""Pipeline run / task metrics persisted in the ``monitoring`` schema.

These writes use their own autocommit connection so that failure information is
kept even when the data transaction of the failing step is rolled back.
"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator

from src.config import Settings
from src.utils.logging import get_logger
from src.warehouse.db import connect

log = get_logger(__name__)
COUNT_FIELDS = ("source_records", "valid_records", "rejected_records", "duplicate_records",
                "output_records", "warehouse_records")


def start_pipeline_run(settings: Settings, run_id: str, pipeline: str,
                       logical_date: date | None = None, details: dict | None = None) -> None:
    with connect(settings, autocommit=True) as conn:
        conn.execute(
            """INSERT INTO monitoring.pipeline_runs (run_id, pipeline, logical_date, status, details)
               VALUES (%s, %s, %s, 'running', %s)
               ON CONFLICT (run_id) DO UPDATE SET status = 'running', error_message = NULL,
                   finished_at = NULL, duration_seconds = NULL,
                   details = monitoring.pipeline_runs.details || EXCLUDED.details""",
            (run_id, pipeline, logical_date, json.dumps(details or {})),
        )


def finish_pipeline_run(settings: Settings, run_id: str, status: str,
                        counts: dict[str, Any] | None = None, error: str | None = None,
                        details: dict | None = None) -> None:
    counts = counts or {}
    with connect(settings, autocommit=True) as conn:
        conn.execute(
            f"""UPDATE monitoring.pipeline_runs SET
                    status = %s, finished_at = NOW(),
                    duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
                    {', '.join(f'{f} = COALESCE(%s, {f})' for f in COUNT_FIELDS)},
                    error_message = %s,
                    details = details || %s::JSONB
                WHERE run_id = %s""",
            (status, *[counts.get(f) for f in COUNT_FIELDS], (error or None) and error[:4000],
             json.dumps(details or {}, default=str), run_id),
        )


@contextmanager
def track_task(settings: Settings, run_id: str, task_name: str,
               try_number: int = 1) -> Iterator[dict]:
    """Record a task attempt. Yields a dict the task fills with its metrics."""
    metrics: dict[str, Any] = {}
    started = time.monotonic()
    with connect(settings, autocommit=True) as conn:
        conn.execute(
            """INSERT INTO monitoring.task_runs (run_id, task_name, try_number, status)
               VALUES (%s, %s, %s, 'running')
               ON CONFLICT (run_id, task_name, try_number) DO UPDATE
               SET status = 'running', started_at = NOW(), finished_at = NULL, error_message = NULL""",
            (run_id, task_name, try_number),
        )
    status, error = "success", None
    try:
        yield metrics
    except BaseException as exc:
        status, error = "failed", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            with connect(settings, autocommit=True) as conn:
                conn.execute(
                    """UPDATE monitoring.task_runs SET status = %s, finished_at = NOW(),
                           duration_seconds = %s, metrics = %s, error_message = %s
                       WHERE run_id = %s AND task_name = %s AND try_number = %s""",
                    (status, round(time.monotonic() - started, 3), json.dumps(metrics, default=str),
                     error and error[:4000], run_id, task_name, try_number),
                )
        except Exception:  # monitoring must never hide the original error
            log.exception("could not record task metrics for %s/%s", run_id, task_name)


def record_quality_results(settings: Settings, run_id: str, results: list) -> None:
    if not results:
        return
    with connect(settings, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO monitoring.data_quality_results
                       (run_id, layer, check_name, passed, severity, observed, expected, details)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                [(run_id, r.layer, r.name, r.passed, r.severity, r.observed, r.expected, r.details)
                 for r in results],
            )


def register_batch(cur, batch_id: str, checksum: str, source: str, source_uri: str,
                   object_key: str | None, file_bytes: int, record_count: int | None,
                   ingestion_date: date) -> bool:
    """Upsert into the batch registry. Returns True when the batch is new."""
    cur.execute(
        """INSERT INTO monitoring.ingestion_batches
               (batch_id, checksum, source, source_uri, object_key, file_bytes, record_count, ingestion_date)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (batch_id) DO UPDATE SET
               last_seen_at = NOW(),
               times_received = monitoring.ingestion_batches.times_received + 1,
               object_key = COALESCE(monitoring.ingestion_batches.object_key, EXCLUDED.object_key),
               record_count = COALESCE(EXCLUDED.record_count, monitoring.ingestion_batches.record_count)
           RETURNING (xmax = 0) AS inserted""",
        (batch_id, checksum, source, source_uri, object_key, file_bytes, record_count, ingestion_date),
    )
    return bool(cur.fetchone()[0])


def lookup_batch(settings: Settings, checksum: str) -> dict | None:
    with connect(settings, autocommit=True) as conn:
        row = conn.execute(
            "SELECT batch_id, object_key, ingestion_date FROM monitoring.ingestion_batches WHERE checksum = %s",
            (checksum,),
        ).fetchone()
    return None if row is None else {"batch_id": row[0], "object_key": row[1], "ingestion_date": row[2]}
