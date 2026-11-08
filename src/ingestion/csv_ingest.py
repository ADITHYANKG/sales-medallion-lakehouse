"""Warehouse-only ingestion (Phase 1 path): CSV -> raw -> staging -> mart with pandas.

Flow (all data changes inside ONE PostgreSQL transaction, guarded by an
advisory lock so concurrent runs cannot interleave):

1. checksum the file -> content-addressed ``batch_id`` and batch registry entry
2. read in chunks (streaming), check the column contract
3. land every row untouched in ``raw.sales_records``
4. validate rows; quarantine failures in ``staging.sales_rejected`` + a CSV file
5. de-duplicate (latest ``updated_at`` wins) -> ``staging.sales_clean``
6. reject-rate quality gate, then merge into the star schema

Re-running the same file is a no-op for raw rows and leaves the mart unchanged.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import Settings
from src.monitoring import metrics
from src.utils.ids import batch_id_for_checksum, new_run_id, sha256_file
from src.utils.logging import get_logger
from src.validation import pandas_rules as R
from src.validation.quality import (CheckResult, DataQualityError, WAREHOUSE_CHECKS, assert_passed,
                                    reject_rate_check, run_sql_checks)
from src.warehouse import loader
from src.warehouse.db import acquire_warehouse_lock, connect

log = get_logger(__name__)
CHUNK_ROWS = 200_000


def read_source(path: Path, chunk_rows: int = CHUNK_ROWS):
    """Stream a CSV or JSON-Lines file as chunks of string columns."""
    if path.suffix.lower() in (".json", ".jsonl", ".ndjson"):
        reader = pd.read_json(path, lines=True, dtype=str, chunksize=chunk_rows)
    else:
        reader = pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=chunk_rows,
                             encoding="utf-8-sig")
    for chunk in reader:
        yield chunk.fillna("")


def validate_file(path: Path, settings: Settings, batch_id: str, ingestion_date: str):
    """Validate a whole file chunk by chunk. Returns (raw_chunks, valid, rejected, stats, legacy)."""
    raw_chunks, valid_parts, rejected_parts = [], [], []
    totals = {"source_records": 0, "valid_records": 0, "rejected_records": 0, "reject_reasons": {}}
    legacy = False
    first_row = 2  # line 1 is the header
    for chunk in read_source(path):
        frame, legacy = R.normalize_columns(chunk)
        result = R.validate(frame, batch_id, ingestion_date, settings.allowed_currencies,
                            derive_missing_total=legacy, source_row_offset=first_row)
        raw_chunks.append((frame, R.row_hashes(frame), first_row))
        valid_parts.append(result.valid)
        rejected_parts.append(result.rejected)
        for key in ("source_records", "valid_records", "rejected_records"):
            totals[key] += result.stats[key]
        for reason, count in result.stats["reject_reasons"].items():
            totals["reject_reasons"][reason] = totals["reject_reasons"].get(reason, 0) + count
        first_row += len(frame)
    if totals["source_records"] == 0:
        raise R.SchemaError("Input file contains no records")
    valid = pd.concat(valid_parts, ignore_index=True)
    rejected = pd.concat(rejected_parts, ignore_index=True)
    return raw_chunks, valid, rejected, totals, legacy


def ingest_file(path: Path, settings: Settings, run_id: str | None = None) -> dict:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(path)
    run_id = run_id or new_run_id("warehouse")
    checksum = sha256_file(path)
    batch_id = batch_id_for_checksum(checksum)
    ingestion_date = datetime.now(timezone.utc).date()
    metrics.start_pipeline_run(settings, run_id, "warehouse", ingestion_date,
                               {"input": str(path), "batch_id": batch_id})
    try:
        raw_chunks, valid, rejected, stats, legacy = validate_file(
            path, settings, batch_id, ingestion_date.isoformat())
        current, dup_stats = R.deduplicate(valid)
        stats.update(dup_stats)
        gate = reject_rate_check("ingestion", stats["source_records"], stats["rejected_records"],
                                 settings.max_reject_rate)
        # Quarantine file is written first so it exists even if the gate fails.
        rejected_file = write_rejected_file(rejected, settings, batch_id)

        with connect(settings) as conn, conn.transaction():
            cur = conn.cursor()
            acquire_warehouse_lock(cur)
            new_batch = metrics.register_batch(
                cur, batch_id, checksum, "file", str(path), None, path.stat().st_size,
                stats["source_records"], ingestion_date)
            raw_inserted = sum(loader.load_raw_records(cur, batch_id, frame, hashes, first)
                               for frame, hashes, first in raw_chunks)
            rejected_inserted = loader.load_rejected(cur, rejected)
            if not gate.passed:
                raise DataQualityError(
                    f"reject rate {gate.observed:.2%} exceeds MAX_REJECT_RATE={settings.max_reject_rate:.2%}; "
                    "nothing was loaded")
            staged = loader.replace_staging_clean(cur, current)
            merged = loader.merge_mart(cur)
            checks: list[CheckResult] = [gate, *run_sql_checks(cur, WAREHOUSE_CHECKS)]
            assert_passed(checks)
            totals = loader.warehouse_totals(cur)

        metrics.record_quality_results(settings, run_id, checks)
        summary = {
            "run_id": run_id, "batch_id": batch_id, "new_batch": new_batch, "legacy_format": legacy,
            **stats, "raw_rows_inserted": raw_inserted, "rejected_rows_inserted": rejected_inserted,
            "staged_records": staged, **merged, **totals,
            "rejected_file": str(rejected_file) if rejected_file else None,
        }
        metrics.finish_pipeline_run(settings, run_id, "success", {
            **stats, "output_records": staged, "warehouse_records": totals["fact_rows"]}, details=summary)
        log.info("ingested %s: %s", path.name, {k: summary[k] for k in (
            "source_records", "valid_records", "rejected_records", "duplicate_records", "fact_rows")})
        return summary
    except Exception as exc:
        metrics.finish_pipeline_run(settings, run_id, "failed", error=f"{type(exc).__name__}: {exc}")
        raise


def write_rejected_file(rejected: pd.DataFrame, settings: Settings, batch_id: str) -> Path | None:
    if rejected.empty:
        return None
    settings.rejected_dir.mkdir(parents=True, exist_ok=True)
    target = settings.rejected_dir / f"{batch_id}_rejected.csv"
    rejected.to_csv(target, index=False)
    return target
