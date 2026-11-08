"""Single source of truth for the sales data contract.

Both validation engines (pandas in ``pandas_rules`` and PySpark in
``src.transformations.spark_jobs``) implement exactly these rules, in exactly this
order, so their outputs can be compared row-for-row in tests.

Grain: one row per order line item (``order_item_id``). An order (``order_id``)
can contain several line items; order-level attributes (date, customer,
payment, status, location) are repeated on each line.
"""
from __future__ import annotations

SOURCE_COLUMNS: list[str] = [
    "order_id",
    "order_item_id",
    "order_date",
    "customer_id",
    "customer_name",
    "customer_email",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount",
    "total_amount",
    "currency",
    "payment_method",
    "order_status",
    "city",
    "state",
    "country",
    "updated_at",
]

REQUIRED_COLUMNS: list[str] = [
    "order_id",
    "order_item_id",
    "order_date",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "quantity",
    "unit_price",
    "total_amount",
    "order_status",
    "updated_at",
]

# Optional columns and the value used when they are blank.
DEFAULTS: dict[str, str | None] = {
    "customer_email": None,
    "category": "Uncategorized",
    "discount": "0",
    "currency": "INR",
    "payment_method": "unknown",
    "city": "Unknown",
    "state": "Unknown",
    "country": "Unknown",
}

ORDER_STATUSES: tuple[str, ...] = ("completed", "pending", "cancelled")
PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card",
    "debit_card",
    "upi",
    "net_banking",
    "wallet",
    "cash_on_delivery",
    "unknown",
)

MAX_QUANTITY = 10_000
MAX_DISCOUNT = 0.9
# |source total_amount - recomputed amount| must be <= 1 cent.
TOTAL_TOLERANCE_CENTS = 1

# Regular expressions shared by both engines (Java and Python regex compatible).
RE_DATE = r"^\d{4}[-/]\d{2}[-/]\d{2}$"
RE_TIMESTAMP = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}Z?$"
# Digit limits keep exact integer-cent arithmetic inside int64 and match the
# PostgreSQL NUMERIC precisions (unit price <= 99,999,999.99).
RE_INT = r"^-?\d{1,9}$"
RE_MONEY = r"^-?\d{1,8}(\.\d{1,2})?$"
RE_TOTAL = r"^-?\d{1,12}(\.\d{1,2})?$"
RE_DISCOUNT = r"^\d{1,2}(\.\d{1,4})?$"
RE_EMAIL = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# Rejection reason codes, in evaluation order. A rejected row lists every rule it
# fails, joined by ';' in this order (deterministic across engines).
REJECT_RULES: list[str] = [
    "missing_required_field",
    "invalid_order_date",
    "invalid_updated_at",
    "updated_before_order_date",
    "invalid_quantity",
    "invalid_unit_price",
    "invalid_discount",
    "invalid_total_amount",
    "total_amount_mismatch",
    "invalid_order_status",
    "unsupported_currency",
]

# Columns of the cleaned (Silver / staging) dataset.
CLEAN_COLUMNS: list[str] = [
    "order_item_id",
    "order_id",
    "order_date",
    "customer_id",
    "customer_name",
    "customer_email",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount",
    "gross_amount",
    "discount_amount",
    "net_amount",
    "currency",
    "payment_method",
    "order_status",
    "city",
    "state",
    "country",
    "updated_at",
    "batch_id",
    "ingestion_date",
    "source_row_hash",
]

# Legacy Phase-1 starter CSV layout -> current layout (kept for backwards compatibility).
LEGACY_COLUMNS: set[str] = {
    "transaction_id",
    "transaction_date",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "quantity",
    "unit_price",
    "status",
}
