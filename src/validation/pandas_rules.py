"""Vectorised pandas implementation of the data contract in ``schema.py``.

Money is handled as exact integer cents (and discounts as integer basis points),
so revenue is computed without floating-point drift:

    net_cents = round_half_up(quantity * unit_price_cents * (10000 - discount_bp) / 10000)
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.validation import schema as S

SEP = "\x1f"  # unit separator used when hashing a row
STRING_DTYPE = "string[pyarrow]"


class SchemaError(ValueError):
    """The file does not satisfy the column contract (whole batch rejected)."""


@dataclass
class ValidationResult:
    valid: pd.DataFrame
    rejected: pd.DataFrame
    stats: dict = field(default_factory=dict)


def normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """Return a frame with exactly ``SOURCE_COLUMNS`` (as strings).

    Accepts the legacy Phase-1 layout (transaction_id/status/...) and maps it to
    the current contract. Raises ``SchemaError`` if required columns are missing.
    """
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    legacy = S.LEGACY_COLUMNS.issubset(df.columns) and "order_item_id" not in df.columns
    if legacy:
        df = df.rename(columns={"transaction_date": "order_date", "status": "order_status"})
        df["order_id"] = df["transaction_id"]
        df["order_item_id"] = df["transaction_id"]
        df["updated_at"] = df["order_date"].astype(str).str.strip() + "T00:00:00Z"
        df["total_amount"] = ""  # derived during validation
    missing = [c for c in S.REQUIRED_COLUMNS if c not in df.columns and not (legacy and c == "total_amount")]
    if missing:
        raise SchemaError(f"missing required columns: {', '.join(missing)}")
    for column in S.SOURCE_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    # Arrow-backed strings: vectorised (C++) .str operations instead of Python objects.
    out = df[S.SOURCE_COLUMNS].astype(STRING_DTYPE).fillna("")
    return out, legacy


def row_hashes(raw: pd.DataFrame) -> pd.Series:
    """sha256 of the raw (un-normalised) source values; identical rows => identical hash."""
    import pyarrow as pa
    import pyarrow.compute as pc

    arrays = [pa.array(raw[c].astype(STRING_DTYPE).fillna("")).cast(pa.large_string())
              for c in S.SOURCE_COLUMNS]
    separator = pa.scalar(SEP, type=pa.large_string())
    joined = pc.binary_join_element_wise(*arrays, separator).to_pylist()  # vectorised concat (C++)
    return pd.Series([hashlib.sha256(text.encode("utf-8")).hexdigest() for text in joined],
                     index=raw.index, dtype=object)


def _matches(series: pd.Series, pattern: str) -> pd.Series:
    return series.str.fullmatch(pattern.strip("^$")).fillna(False).astype(bool)


def _to_units(series: pd.Series, mask: pd.Series, scale_digits: int) -> pd.Series:
    """Decimal string -> integer units (e.g. cents) for rows where ``mask``.

    Exact because the regexes cap magnitudes (<= 12 integer digits, <= 4 decimals):
    every value * 10**scale stays far below 2**53, so float parsing + rounding to the
    nearest integer recovers the exact decimal.
    """
    result = pd.Series(0, index=series.index, dtype="int64")
    if not mask.any():
        return result
    values = pd.to_numeric(series[mask].astype(object), errors="coerce").to_numpy(dtype="float64")
    result[mask] = np.rint(values * (10 ** scale_digits)).astype("int64")
    return result


def cents_to_str(cents: pd.Series) -> pd.Series:
    """Exact 2-decimal text for integer cents (non-negative)."""
    cents = cents.astype("int64")
    return (cents // 100).astype(str) + "." + (cents % 100).astype(str).str.zfill(2)


def validate(raw: pd.DataFrame, batch_id: str, ingestion_date: str,
             allowed_currencies: tuple[str, ...] = ("INR",),
             derive_missing_total: bool = False,
             source_row_offset: int = 2) -> ValidationResult:
    """Validate and clean a frame of raw string columns (``normalize_columns`` output)."""
    raw = raw.reset_index(drop=True)
    hashes = row_hashes(raw)
    v = raw.apply(lambda col: col.str.strip())

    # ---- normalisation -------------------------------------------------------
    v["order_status"] = v["order_status"].str.lower()
    v["currency"] = v["currency"].str.upper()
    v["payment_method"] = v["payment_method"].str.lower()
    for column, default in S.DEFAULTS.items():
        if default is not None:
            v.loc[v[column] == "", column] = default
    v["customer_email"] = v["customer_email"].str.lower()

    blank = {c: v[c] == "" for c in S.SOURCE_COLUMNS}
    required = [c for c in S.REQUIRED_COLUMNS if not (derive_missing_total and c == "total_amount")]

    date_text = v["order_date"].str.replace("/", "-", regex=False)
    date_ok_fmt = _matches(v["order_date"], S.RE_DATE)
    order_date = pd.to_datetime(date_text.where(date_ok_fmt), format="%Y-%m-%d", errors="coerce")
    ts_ok_fmt = _matches(v["updated_at"], S.RE_TIMESTAMP)
    ts_text = v["updated_at"].str.replace("T", " ", regex=False).str.rstrip("Z")
    updated_at = pd.to_datetime(ts_text.where(ts_ok_fmt), format="%Y-%m-%d %H:%M:%S", errors="coerce")

    qty_fmt = _matches(v["quantity"], S.RE_INT)
    quantity = pd.to_numeric(v["quantity"].where(qty_fmt), errors="coerce")
    price_fmt = _matches(v["unit_price"], S.RE_MONEY)
    price_cents = _to_units(v["unit_price"], price_fmt, 2)
    disc_fmt = _matches(v["discount"], S.RE_DISCOUNT)
    disc_bp = _to_units(v["discount"], disc_fmt, 4)
    total_fmt = _matches(v["total_amount"], S.RE_TOTAL)
    total_cents = _to_units(v["total_amount"], total_fmt, 2)

    bad = {
        "missing_required_field": pd.concat([blank[c] for c in required], axis=1).any(axis=1),
        "invalid_order_date": ~blank["order_date"] & order_date.isna(),
        "invalid_updated_at": ~blank["updated_at"] & updated_at.isna(),
        "updated_before_order_date": (order_date.notna() & updated_at.notna()
                                      & (updated_at.dt.normalize() < order_date)),
        "invalid_quantity": ~blank["quantity"] & ~(quantity.notna() & (quantity > 0) & (quantity <= S.MAX_QUANTITY)),
        "invalid_unit_price": ~blank["unit_price"] & ~(price_fmt & (price_cents >= 0)),
        "invalid_discount": ~(disc_fmt & (disc_bp <= int(S.MAX_DISCOUNT * 10_000))),
        "invalid_total_amount": ~blank["total_amount"] & ~(total_fmt & (total_cents >= 0)),
    }
    numeric_ok = ~(bad["invalid_quantity"] | bad["invalid_unit_price"] | bad["invalid_discount"]
                   | blank["quantity"] | blank["unit_price"])
    q = quantity.where(numeric_ok, 0).fillna(0).astype("int64")
    gross_cents = q * price_cents.where(numeric_ok, 0)
    net_cents = (gross_cents * (10_000 - disc_bp.where(numeric_ok, 0)) + 5_000) // 10_000
    has_total = ~blank["total_amount"] & ~bad["invalid_total_amount"]
    bad["total_amount_mismatch"] = numeric_ok & has_total & ((total_cents - net_cents).abs() > S.TOTAL_TOLERANCE_CENTS)
    bad["invalid_order_status"] = ~blank["order_status"] & ~v["order_status"].isin(S.ORDER_STATUSES)
    bad["unsupported_currency"] = ~v["currency"].isin(allowed_currencies)

    flags = pd.DataFrame({rule: bad[rule].astype(bool) for rule in S.REJECT_RULES})
    is_bad = flags.any(axis=1)
    # Vectorised "rule1;rule2" strings, built only for the (few) failing rows.
    bad_flags = flags[is_bad]
    joined = np.full(len(bad_flags), "", dtype=object)
    for rule in S.REJECT_RULES:
        hit = bad_flags[rule].to_numpy()
        joined = np.where(hit, joined + ";" + rule, joined)
    reasons = pd.Series(joined, index=bad_flags.index, dtype=object).str.lstrip(";")

    # ---- rejected rows (raw values preserved for investigation) ---------------
    rejected = raw[is_bad].copy()
    rejected["reject_reasons"] = reasons[is_bad]
    rejected["batch_id"] = batch_id
    rejected["ingestion_date"] = ingestion_date
    rejected["source_row"] = (raw.index[is_bad] + source_row_offset).astype("int64")
    rejected["source_row_hash"] = hashes[is_bad]

    # ---- cleaned rows -------------------------------------------------------------
    ok = ~is_bad
    email = v["customer_email"].where(_matches(v["customer_email"], S.RE_EMAIL))
    clean = pd.DataFrame({
        "order_item_id": v["order_item_id"],
        "order_id": v["order_id"],
        "order_date": order_date.dt.date,
        "customer_id": v["customer_id"],
        "customer_name": v["customer_name"],
        "customer_email": email.replace("", pd.NA),
        "product_id": v["product_id"],
        "product_name": v["product_name"],
        "category": v["category"],
        "quantity": q,
        "unit_price_cents": price_cents,
        "discount_bp": disc_bp,
        "gross_cents": gross_cents,
        "discount_cents": gross_cents - net_cents,
        "net_cents": net_cents,
        "currency": v["currency"],
        "payment_method": v["payment_method"].where(v["payment_method"].isin(S.PAYMENT_METHODS), "unknown"),
        "order_status": v["order_status"],
        "city": v["city"],
        "state": v["state"],
        "country": v["country"],
        "updated_at": updated_at,
        "batch_id": batch_id,
        "ingestion_date": ingestion_date,
        "source_row_hash": hashes,
    })[ok].reset_index(drop=True)

    stats = {
        "source_records": int(len(raw)),
        "valid_records": int(ok.sum()),
        "rejected_records": int(is_bad.sum()),
        "reject_reasons": {rule: int(flags[rule].sum()) for rule in S.REJECT_RULES if flags[rule].any()},
    }
    return ValidationResult(valid=clean, rejected=rejected.reset_index(drop=True), stats=stats)


def deduplicate(valid: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Keep the latest version of each order_item_id.

    Deterministic conflict resolution: updated_at DESC, ingestion_date DESC,
    batch_id DESC, source_row_hash DESC (identical to the Spark window).
    """
    if valid.empty:
        return valid.copy(), {"exact_duplicates": 0, "superseded_versions": 0, "duplicate_records": 0}
    ordered = valid.sort_values(
        ["order_item_id", "updated_at", "ingestion_date", "batch_id", "source_row_hash"],
        ascending=[True, False, False, False, False], kind="mergesort",
    )
    distinct = ordered.drop_duplicates(subset=["order_item_id", "source_row_hash"], keep="first")
    exact = len(valid) - len(distinct)
    current = distinct.drop_duplicates(subset=["order_item_id"], keep="first")
    superseded = len(distinct) - len(current)
    current = current.sort_values("order_item_id", kind="mergesort").reset_index(drop=True)
    return current, {
        "exact_duplicates": int(exact),
        "superseded_versions": int(superseded),
        "duplicate_records": int(exact + superseded),
    }


def with_decimal_columns(clean: pd.DataFrame) -> pd.DataFrame:
    """Add human-readable decimal amount columns (float) derived from exact cents."""
    out = clean.copy()
    out["unit_price"] = out["unit_price_cents"] / 100
    out["discount"] = out["discount_bp"] / 10_000
    out["gross_amount"] = out["gross_cents"] / 100
    out["discount_amount"] = out["discount_cents"] / 100
    out["net_amount"] = out["net_cents"] / 100
    return out
