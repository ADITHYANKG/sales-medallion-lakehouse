import pandas as pd
import pytest

from src.validation import pandas_rules as R
from src.validation.schema import SOURCE_COLUMNS


def run(rows, **kwargs):
    frame, legacy = R.normalize_columns(pd.DataFrame(rows, dtype=str))
    return R.validate(frame, "b1", "2026-01-06", derive_missing_total=legacy, **kwargs)


def reasons(result):
    return list(result.rejected["reject_reasons"])


def test_valid_row_is_cleaned_with_exact_amounts(valid_row):
    result = run([valid_row])
    assert result.stats == {"source_records": 1, "valid_records": 1, "rejected_records": 0, "reject_reasons": {}}
    row = R.with_decimal_columns(result.valid).iloc[0]
    assert row["quantity"] == 2
    assert row["unit_price"] == 100.50
    assert row["gross_amount"] == 201.00
    assert row["net_amount"] == 180.90
    assert row["discount_amount"] == pytest.approx(20.10)


def test_net_amount_rounds_half_up(valid_row):
    # 1 x 0.30 x (1 - 0.05) = 0.285 -> 0.29 (half-up), banker's rounding would give 0.28
    result = run([{**valid_row, "quantity": "1", "unit_price": "0.30", "discount": "0.05", "total_amount": "0.29"}])
    assert result.valid.iloc[0]["net_cents"] == 29


@pytest.mark.parametrize("field,value,reason", [
    ("quantity", "-1", "invalid_quantity"),          # starter test case
    ("quantity", "0", "invalid_quantity"),
    ("quantity", "abc", "invalid_quantity"),
    ("order_status", "unknown", "invalid_order_status"),  # starter test case
    ("order_date", "2026-99-99", "invalid_order_date"),   # starter test case
    ("order_date", "2026-02-30", "invalid_order_date"),
    ("unit_price", "NaN", "invalid_unit_price"),          # starter test case
    ("unit_price", "-5.00", "invalid_unit_price"),
    ("unit_price", "1.999", "invalid_unit_price"),
    ("discount", "0.95", "invalid_discount"),
    ("total_amount", "999.99", "total_amount_mismatch"),
    ("currency", "USD", "unsupported_currency"),
    ("updated_at", "yesterday", "invalid_updated_at"),
    ("updated_at", "2026-01-01T00:00:00Z", "updated_before_order_date"),
    ("customer_id", "", "missing_required_field"),
    ("product_name", "   ", "missing_required_field"),
])
def test_invalid_values_are_rejected_with_reason(valid_row, field, value, reason):
    result = run([{**valid_row, field: value}])
    assert result.stats["rejected_records"] == 1
    assert reason in reasons(result)[0].split(";")


def test_multiple_reasons_are_listed_in_rule_order(valid_row):
    result = run([{**valid_row, "quantity": "0", "order_status": "shipped", "customer_id": ""}])
    assert reasons(result) == ["missing_required_field;invalid_quantity;invalid_order_status"]


def test_rejected_rows_keep_raw_values_and_line_numbers(valid_row):
    result = run([valid_row, {**valid_row, "order_item_id": "X", "quantity": "-3"}])
    rejected = result.rejected.iloc[0]
    assert rejected["quantity"] == "-3"
    assert rejected["source_row"] == 3  # header is line 1


def test_messy_formats_are_normalised_not_rejected(valid_row):
    messy = {**valid_row, "order_status": " COMPLETED ", "order_date": "2026/01/05",
             "customer_name": "  Asha Rao ", "currency": "inr", "payment_method": "UPI"}
    result = run([messy])
    assert result.stats["rejected_records"] == 0
    row = result.valid.iloc[0]
    assert (row["order_status"], row["customer_name"], row["currency"], row["payment_method"]) == \
        ("completed", "Asha Rao", "INR", "upi")
    assert str(row["order_date"]) == "2026-01-05"


def test_optional_fields_get_defaults_and_bad_email_becomes_null(valid_row):
    result = run([{**valid_row, "customer_email": "not-an-email", "category": "", "discount": "",
                   "total_amount": "201.00", "city": "", "payment_method": "barter"}])
    row = result.valid.iloc[0]
    assert pd.isna(row["customer_email"])
    assert row["category"] == "Uncategorized" and row["city"] == "Unknown"
    assert row["payment_method"] == "unknown" and row["discount_bp"] == 0


def test_missing_required_column_raises_schema_error(valid_row):
    frame = pd.DataFrame([valid_row]).drop(columns=["order_status"])
    with pytest.raises(R.SchemaError, match="order_status"):
        R.normalize_columns(frame)


def test_legacy_phase1_layout_is_accepted():
    legacy = {"transaction_id": "TXN1", "transaction_date": "2026-02-05", "customer_id": "C002",
              "customer_name": "Customer 002", "product_id": "P001", "product_name": "Laptop",
              "quantity": "2", "unit_price": "55000", "status": "completed"}
    result = run([legacy])
    assert result.stats["valid_records"] == 1
    row = result.valid.iloc[0]
    assert row["order_item_id"] == "TXN1" and row["net_cents"] == 11_000_000


def test_row_hash_is_stable_and_content_based(valid_row):
    frame, _ = R.normalize_columns(pd.DataFrame([valid_row, valid_row, {**valid_row, "quantity": "3"}]))
    hashes = R.row_hashes(frame)
    assert hashes[0] == hashes[1] != hashes[2]
    assert len(hashes[0]) == 64
    assert list(frame.columns) == SOURCE_COLUMNS


class TestDeduplicate:
    def _clean(self, rows, batch="b1", ingestion="2026-01-06"):
        frame, _ = R.normalize_columns(pd.DataFrame(rows))
        return R.validate(frame, batch, ingestion).valid

    def test_latest_updated_at_wins_and_counts(self, valid_row):
        newer = {**valid_row, "order_status": "cancelled", "updated_at": "2026-01-06T09:00:00Z"}
        valid = self._clean([valid_row, valid_row, newer])
        current, stats = R.deduplicate(valid)
        assert len(current) == 1 and current.iloc[0]["order_status"] == "cancelled"
        assert stats == {"exact_duplicates": 1, "superseded_versions": 1, "duplicate_records": 2}

    def test_resolution_does_not_depend_on_input_order(self, valid_row):
        a = {**valid_row, "payment_method": "upi"}
        b = {**valid_row, "payment_method": "wallet"}  # same updated_at -> tie-break on hash
        first, _ = R.deduplicate(self._clean([a, b]))
        second, _ = R.deduplicate(self._clean([b, a]))
        assert first.iloc[0]["source_row_hash"] == second.iloc[0]["source_row_hash"]

    def test_later_batch_wins_on_equal_timestamps(self, valid_row):
        old = self._clean([valid_row], batch="b1", ingestion="2026-01-06")
        new = self._clean([{**valid_row, "payment_method": "wallet"}], batch="b2", ingestion="2026-01-07")
        current, _ = R.deduplicate(pd.concat([new, old], ignore_index=True))
        assert current.iloc[0]["batch_id"] == "b2"
