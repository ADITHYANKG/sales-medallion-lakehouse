"""Transactional loading of raw / staging tables and the mart merge."""
from __future__ import annotations

import json
import re
from typing import Iterable

import pandas as pd

from src.config import PROJECT_ROOT
from src.utils.logging import get_logger
from src.validation import schema as S
from src.validation.pandas_rules import cents_to_str

log = get_logger(__name__)
MERGE_SQL = PROJECT_ROOT / "sql" / "warehouse" / "merge_mart.sql"
GOLD_TABLES = ("daily_sales", "monthly_sales", "category_sales",
               "product_sales", "customer_sales", "location_sales")
_STATEMENT_SPLIT = re.compile(r";\s*(?:\n|$)")


def _clean(value):
    if value is None or value is pd.NA:
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None
    return value


def _rows(frame: pd.DataFrame, columns: list[str]) -> Iterable[tuple]:
    for record in frame[columns].itertuples(index=False, name=None):
        yield tuple(_clean(v) for v in record)


def load_raw_records(cur, batch_id: str, raw: pd.DataFrame, hashes: pd.Series,
                     first_row: int) -> int:
    """Append raw source rows; idempotent per (batch_id, source_row)."""
    frame = raw.copy()
    frame["batch_id"] = batch_id
    frame["source_row"] = range(first_row, first_row + len(frame))
    frame["source_row_hash"] = hashes.values
    columns = ["batch_id", "source_row", *S.SOURCE_COLUMNS, "source_row_hash"]
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_raw_sales (LIKE raw.sales_records INCLUDING DEFAULTS) ON COMMIT DROP")
    cur.execute("TRUNCATE tmp_raw_sales")
    with cur.copy(f"COPY tmp_raw_sales ({', '.join(columns)}) FROM STDIN") as copy:
        for row in _rows(frame, columns):
            copy.write_row(row)
    cur.execute(f"""
        INSERT INTO raw.sales_records ({', '.join(columns)})
        SELECT {', '.join(columns)} FROM tmp_raw_sales
        ON CONFLICT (batch_id, source_row) DO NOTHING
    """)
    return cur.rowcount


def load_rejected(cur, rejected: pd.DataFrame) -> int:
    if rejected.empty:
        return 0
    inserted = 0
    for record in rejected.to_dict("records"):
        raw_record = {c: _clean(record.get(c)) for c in S.SOURCE_COLUMNS}
        cur.execute(
            """INSERT INTO staging.sales_rejected
                   (batch_id, source_row, order_item_id, reject_reasons, raw_record, source_row_hash)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (batch_id, source_row) DO NOTHING""",
            (record["batch_id"], int(record["source_row"]), raw_record.get("order_item_id") or None,
             record["reject_reasons"], json.dumps(raw_record), record["source_row_hash"]),
        )
        inserted += cur.rowcount
    return inserted


def replace_staging_clean(cur, clean: pd.DataFrame) -> int:
    """Replace the staging snapshot with pandas-cleaned rows (exact decimal text)."""
    frame = clean.copy()
    frame["unit_price"] = cents_to_str(frame["unit_price_cents"])
    frame["discount"] = (frame["discount_bp"] / 10_000).map(lambda d: f"{d:.4f}")
    frame["gross_amount"] = cents_to_str(frame["gross_cents"])
    frame["discount_amount"] = cents_to_str(frame["discount_cents"])
    frame["net_amount"] = cents_to_str(frame["net_cents"])
    columns = S.CLEAN_COLUMNS
    cur.execute("TRUNCATE staging.sales_clean")
    with cur.copy(f"COPY staging.sales_clean ({', '.join(columns)}) FROM STDIN") as copy:
        for row in _rows(frame, columns):
            copy.write_row(row)
    return len(frame)


def merge_mart(cur) -> dict[str, int]:
    """Run the star-schema merge; returns affected-row counts per statement."""
    statements = [s.strip() for s in _STATEMENT_SPLIT.split(MERGE_SQL.read_text(encoding="utf-8"))]
    labels = ["dim_date", "dim_customer", "dim_product", "dim_location", "fact_sales"]
    statements = [s for s in statements if s and not all(
        line.strip().startswith("--") or not line.strip() for line in s.splitlines())]
    if len(statements) != len(labels):
        raise RuntimeError(f"merge_mart.sql: expected {len(labels)} statements, found {len(statements)}")
    counts = {}
    for label, statement in zip(labels, statements):
        cur.execute(statement)
        counts[f"{label}_upserted"] = cur.rowcount
    return counts


def swap_gold_tables(cur) -> dict[str, int]:
    """Atomically replace mart.agg_* from staging.gold_* (call inside a transaction)."""
    counts = {}
    for name in GOLD_TABLES:
        cur.execute(f"DELETE FROM mart.agg_{name}")
        cur.execute(f"INSERT INTO mart.agg_{name} SELECT * FROM staging.gold_{name}")
        counts[f"agg_{name}"] = cur.rowcount
    return counts


def warehouse_totals(cur) -> dict[str, float | int]:
    cur.execute("""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE order_status = 'completed'),
               COALESCE(SUM(net_amount) FILTER (WHERE order_status = 'completed'), 0)
        FROM mart.fact_sales
    """)
    rows, completed, revenue = cur.fetchone()
    return {"fact_rows": rows, "completed_rows": completed, "completed_revenue": float(revenue)}
