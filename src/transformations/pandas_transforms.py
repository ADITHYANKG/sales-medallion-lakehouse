"""Pandas implementation of Bronze -> Silver -> Gold.

Used by the warehouse-only path, by the Pandas-vs-PySpark benchmark and as an
independent reference implementation in the Spark parity tests. Business rules
are identical to ``spark_jobs.py`` (see docs/DATA_MODEL.md for definitions).
"""
from __future__ import annotations

import pandas as pd

from src.validation import pandas_rules as R


def bronze_to_silver(raw_frames: list[tuple[pd.DataFrame, str, str]],
                     allowed_currencies: tuple[str, ...] = ("INR",)):
    """raw_frames: (string frame, batch_id, ingestion_date). Returns (silver, rejected, stats)."""
    valid, rejected, stats = [], [], {"source_records": 0, "valid_records": 0, "rejected_records": 0}
    for frame, batch_id, ingestion_date in raw_frames:
        normalized, legacy = R.normalize_columns(frame)
        result = R.validate(normalized, batch_id, ingestion_date, allowed_currencies,
                            derive_missing_total=legacy)
        valid.append(result.valid)
        rejected.append(result.rejected)
        for key in stats:
            stats[key] += result.stats[key]
    silver, dup = R.deduplicate(pd.concat(valid, ignore_index=True))
    stats.update(dup)
    stats["silver_records"] = int(len(silver))
    return silver, pd.concat(rejected, ignore_index=True), stats


def _money(cents: pd.Series) -> pd.Series:
    return (cents / 100).round(2)


def _orders(frame: pd.DataFrame, keys: list[str], status: str | None = None, name: str = "orders"):
    subset = frame if status is None else frame[frame["order_status"] == status]
    return subset.groupby(keys)["order_id"].nunique().rename(name)


def _rank(frame: pd.DataFrame, value: str, tie: list[str], partition: list[str] | None = None) -> pd.Series:
    ordered = frame.sort_values([*(partition or []), value, *tie],
                                ascending=[*([True] * len(partition or [])), False, *([True] * len(tie))],
                                kind="mergesort")
    if partition:
        ranks = ordered.groupby(partition).cumcount() + 1
    else:
        ranks = pd.Series(range(1, len(ordered) + 1), index=ordered.index)
    return ranks.reindex(frame.index).astype("int64")


def _latest(frame: pd.DataFrame, key: str, columns: list[str]) -> pd.DataFrame:
    ordered = frame.sort_values([key, "updated_at", *columns], kind="mergesort")
    return ordered.groupby(key)[columns].last()


def build_gold(silver: pd.DataFrame) -> dict[str, pd.DataFrame]:
    s = silver.copy()
    s["order_date"] = pd.to_datetime(s["order_date"])
    done = s[s["order_status"] == "completed"]

    # ---- daily -------------------------------------------------------------------------
    daily = pd.concat([
        _orders(s, ["order_date"], name="total_orders"),
        _orders(s, ["order_date"], "completed", "completed_orders"),
        _orders(s, ["order_date"], "cancelled", "cancelled_orders"),
        _orders(s, ["order_date"], "pending", "pending_orders"),
        done.groupby("order_date")[["quantity", "gross_cents", "discount_cents", "net_cents"]].sum(),
    ], axis=1).fillna(0).sort_index()
    daily = daily.reset_index()
    daily["revenue_7d_avg"] = (daily["net_cents"].rolling(7, min_periods=1).mean() / 100).round(2)
    daily["cumulative_revenue"] = _money(daily["net_cents"].cumsum())
    daily["avg_order_value"] = (daily["net_cents"] / 100 / daily["completed_orders"].where(
        daily["completed_orders"] > 0)).round(2)
    daily = daily.rename(columns={"quantity": "units_sold"})
    daily["gross_revenue"] = _money(daily.pop("gross_cents"))
    daily["discount_amount"] = _money(daily.pop("discount_cents"))
    daily["net_revenue"] = _money(daily.pop("net_cents"))
    daily["order_date"] = daily["order_date"].dt.date
    daily = daily[["order_date", "total_orders", "completed_orders", "cancelled_orders", "pending_orders",
                   "units_sold", "gross_revenue", "discount_amount", "net_revenue", "avg_order_value",
                   "revenue_7d_avg", "cumulative_revenue"]]

    # ---- monthly -----------------------------------------------------------------------
    s["order_month"] = s["order_date"].dt.to_period("M").dt.to_timestamp()
    done = s[s["order_status"] == "completed"]
    monthly = pd.concat([
        _orders(s, ["order_month"], name="total_orders"),
        _orders(s, ["order_month"], "completed", "completed_orders"),
        _orders(s, ["order_month"], "cancelled", "cancelled_orders"),
        done.groupby("order_month")["net_cents"].sum(),
    ], axis=1).fillna(0).sort_index().reset_index()
    monthly["net_revenue"] = _money(monthly.pop("net_cents"))
    monthly["avg_order_value"] = (monthly["net_revenue"] / monthly["completed_orders"].where(
        monthly["completed_orders"] > 0)).round(2)
    monthly["prev_month_revenue"] = monthly["net_revenue"].shift(1)
    prev = monthly["prev_month_revenue"]
    monthly["revenue_growth_pct"] = ((monthly["net_revenue"] - prev) / prev.where(prev > 0) * 100).round(2)
    monthly["cancelled_order_pct"] = (monthly["cancelled_orders"] / monthly["total_orders"] * 100).round(2)
    monthly["order_month"] = monthly["order_month"].dt.date

    # ---- category ----------------------------------------------------------------------
    category = pd.concat([
        _orders(done, ["category"], name="completed_orders"),
        done.groupby("category")[["quantity", "net_cents"]].sum(),
    ], axis=1).reset_index().rename(columns={"quantity": "units_sold"})
    total = category["net_cents"].sum()
    category["net_revenue"] = _money(category["net_cents"])
    category["revenue_share_pct"] = (category["net_cents"] / total * 100).round(2) if total else 0.0
    category["revenue_rank"] = _rank(category, "net_cents", ["category"])
    category = category.drop(columns="net_cents")

    # ---- product -----------------------------------------------------------------------
    product = pd.concat([
        _latest(s, "product_id", ["product_name", "category"]),
        _orders(done, ["product_id"], name="completed_orders"),
        done.groupby("product_id")[["quantity", "net_cents"]].sum(),
    ], axis=1, join="inner").reset_index().rename(columns={"quantity": "units_sold"})
    product["net_revenue"] = _money(product["net_cents"])
    product["revenue_rank"] = _rank(product, "net_cents", ["product_id"])
    product["category_rank"] = _rank(product, "net_cents", ["product_id"], ["category"])
    product = product.drop(columns="net_cents")

    # ---- customer ----------------------------------------------------------------------
    customer = pd.concat([
        _latest(s, "customer_id", ["customer_name"]),
        _orders(s, ["customer_id"], name="total_orders"),
        _orders(done, ["customer_id"], name="completed_orders"),
        done.groupby("customer_id")["net_cents"].sum(),
        s.groupby("customer_id")["order_date"].agg(first_order_date="min", last_order_date="max"),
    ], axis=1).fillna({"completed_orders": 0, "net_cents": 0}).reset_index()
    customer["lifetime_revenue"] = _money(customer["net_cents"])
    customer["revenue_rank"] = _rank(customer, "net_cents", ["customer_id"])
    customer["first_order_date"] = customer["first_order_date"].dt.date
    customer["last_order_date"] = customer["last_order_date"].dt.date
    customer = customer[["customer_id", "customer_name", "total_orders", "completed_orders",
                         "lifetime_revenue", "first_order_date", "last_order_date", "revenue_rank"]]

    # ---- location ----------------------------------------------------------------------
    keys = ["country", "state", "city"]
    location = pd.concat([
        _orders(done, keys, name="completed_orders"),
        done.groupby(keys)["customer_id"].nunique().rename("customers"),
        done.groupby(keys)["net_cents"].sum(),
    ], axis=1).reset_index()
    location["net_revenue"] = _money(location["net_cents"])
    location["revenue_rank"] = _rank(location, "net_cents", keys)
    location = location.drop(columns="net_cents")

    int_cols = ["total_orders", "completed_orders", "cancelled_orders", "pending_orders", "units_sold", "customers"]
    out = {"daily_sales": daily, "monthly_sales": monthly, "category_sales": category,
           "product_sales": product, "customer_sales": customer, "location_sales": location}
    for frame in out.values():
        for col in int_cols:
            if col in frame:
                frame[col] = frame[col].astype("int64")
    return out
