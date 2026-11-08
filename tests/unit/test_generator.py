import csv
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from src.ingestion.generator import generate_daily_batch, generate_dataset, line_total
from src.utils.ids import sha256_file
from src.validation import pandas_rules as R
from src.validation.schema import SOURCE_COLUMNS


def read(path):
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_generation_is_reproducible(tmp_path):
    a = generate_dataset(tmp_path / "a.csv", records=500, seed=7, dirty_rate=0.05)
    b = generate_dataset(tmp_path / "b.csv", records=500, seed=7, dirty_rate=0.05)
    assert sha256_file(tmp_path / "a.csv") == sha256_file(tmp_path / "b.csv")
    assert a.as_dict() == b.as_dict()
    generate_dataset(tmp_path / "c.csv", records=500, seed=8)
    assert sha256_file(tmp_path / "a.csv") != sha256_file(tmp_path / "c.csv")


def test_exact_record_count_and_contract(tmp_path):
    stats = generate_dataset(tmp_path / "s.csv", records=1234, days=10, update_rate=0)
    rows = read(tmp_path / "s.csv")
    assert stats.clean_rows == 1234 == len(rows) == stats.rows_written
    assert list(rows[0]) == SOURCE_COLUMNS
    assert len({r["order_item_id"] for r in rows}) == 1234


def test_clean_rows_are_internally_consistent(tmp_path):
    generate_dataset(tmp_path / "s.csv", records=800, update_rate=0)
    for row in read(tmp_path / "s.csv"):
        expected = line_total(int(row["quantity"]), Decimal(row["unit_price"]), Decimal(row["discount"]))
        assert Decimal(row["total_amount"]) == expected
        assert row["order_item_id"].startswith(row["order_id"])


def test_clean_data_is_independent_of_dirty_rate(tmp_path):
    generate_dataset(tmp_path / "clean.csv", records=600, dirty_rate=0.0, update_rate=0)
    stats = generate_dataset(tmp_path / "dirty.csv", records=600, dirty_rate=0.2, update_rate=0)
    clean_ids = {r["order_item_id"] for r in read(tmp_path / "clean.csv")}
    dirty_ids = {r["order_item_id"] for r in read(tmp_path / "dirty.csv")}
    assert dirty_ids <= clean_ids | {""}
    assert sum(stats.dirty.values()) > 50


def test_dirty_rows_are_caught_by_validation(tmp_path):
    stats = generate_dataset(tmp_path / "d.csv", records=2000, dirty_rate=0.1)
    frame, _ = R.normalize_columns(pd.read_csv(tmp_path / "d.csv", dtype=str, keep_default_na=False))
    result = R.validate(frame, "b", "2026-01-01")
    bad_kinds = sum(v for k, v in stats.dirty.items() if k not in ("duplicate", "missing_email", "messy_format"))
    assert result.stats["rejected_records"] == bad_kinds
    _, dup = R.deduplicate(result.valid)
    assert dup["exact_duplicates"] >= stats.dirty["duplicate"]


def test_daily_batch_contains_updates_for_previous_day(tmp_path):
    prev = date(2026, 3, 1)
    generate_daily_batch(tmp_path / "d1.csv", prev, records=400, update_rate=0)
    stats = generate_daily_batch(tmp_path / "d2.csv", date(2026, 3, 2), records=400, update_rate=0.2)
    day1 = {r["order_item_id"]: r for r in read(tmp_path / "d1.csv")}
    updates = [r for r in read(tmp_path / "d2.csv") if r["order_date"] == "2026-03-01"]
    assert stats.update_rows == len(updates) > 0
    for row in updates:
        original = day1[row["order_item_id"]]
        assert row["updated_at"] > original["updated_at"]
        assert row["order_status"] != original["order_status"]
        assert row["total_amount"] == original["total_amount"]


def test_jsonl_output(tmp_path):
    generate_dataset(tmp_path / "s.jsonl", records=50, fmt="jsonl", update_rate=0)
    frame = pd.read_json(tmp_path / "s.jsonl", lines=True, dtype=str)
    assert len(frame) == 50 and list(frame.columns) == SOURCE_COLUMNS


def test_invalid_arguments(tmp_path):
    with pytest.raises(ValueError):
        generate_dataset(tmp_path / "x.csv", records=0)
