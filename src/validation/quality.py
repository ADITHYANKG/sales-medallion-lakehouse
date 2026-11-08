"""Data-quality checks and gates for each layer."""
from __future__ import annotations

from dataclasses import dataclass


class DataQualityError(RuntimeError):
    """Raised when an error-severity check fails; stops the pipeline (quality gate)."""


@dataclass
class CheckResult:
    layer: str
    name: str
    passed: bool
    observed: float | None = None
    expected: str | None = None
    severity: str = "error"
    details: str | None = None

    def as_dict(self) -> dict:
        return {"layer": self.layer, "name": self.name, "passed": self.passed,
                "observed": self.observed, "expected": self.expected,
                "severity": self.severity, "details": self.details}


def check(layer: str, name: str, observed, ok: bool, expected: str,
          severity: str = "error", details: str | None = None) -> CheckResult:
    value = None if observed is None else float(observed)
    return CheckResult(layer, name, bool(ok), value, expected, severity, details)


def assert_passed(results: list[CheckResult]) -> None:
    failures = [r for r in results if not r.passed and r.severity == "error"]
    if failures:
        summary = "; ".join(f"{r.layer}.{r.name} observed={r.observed} expected {r.expected}"
                            for r in failures)
        raise DataQualityError(f"{len(failures)} data-quality check(s) failed: {summary}")


def reject_rate_check(layer: str, source: int, rejected: int, max_rate: float) -> CheckResult:
    rate = (rejected / source) if source else 0.0
    return check(layer, "reject_rate", round(rate, 6), rate <= max_rate, f"<= {max_rate}")


# SQL checks against the warehouse: (name, query returning one number, expected, comparator)
WAREHOUSE_CHECKS: list[tuple[str, str, str]] = [
    ("fact_duplicate_order_items",
     "SELECT COUNT(*) - COUNT(DISTINCT order_item_id) FROM mart.fact_sales", "= 0"),
    ("fact_orphan_customers",
     "SELECT COUNT(*) FROM mart.fact_sales f LEFT JOIN mart.dim_customer d USING (customer_key) "
     "WHERE d.customer_key IS NULL", "= 0"),
    ("fact_orphan_products",
     "SELECT COUNT(*) FROM mart.fact_sales f LEFT JOIN mart.dim_product d USING (product_key) "
     "WHERE d.product_key IS NULL", "= 0"),
    ("fact_orphan_locations",
     "SELECT COUNT(*) FROM mart.fact_sales f LEFT JOIN mart.dim_location d USING (location_key) "
     "WHERE d.location_key IS NULL", "= 0"),
    ("fact_orphan_dates",
     "SELECT COUNT(*) FROM mart.fact_sales f LEFT JOIN mart.dim_date d USING (date_key) "
     "WHERE d.date_key IS NULL", "= 0"),
    ("fact_revenue_formula",
     "SELECT COUNT(*) FROM mart.fact_sales "
     "WHERE net_amount <> ROUND(quantity * unit_price * (1 - discount_rate), 2)", "= 0"),
    ("fact_negative_amounts",
     "SELECT COUNT(*) FROM mart.fact_sales WHERE net_amount < 0 OR unit_price < 0 OR quantity <= 0", "= 0"),
    ("fact_invalid_status",
     "SELECT COUNT(*) FROM mart.fact_sales WHERE order_status NOT IN ('completed','pending','cancelled')", "= 0"),
    ("dim_customer_duplicate_ids",
     "SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM mart.dim_customer", "= 0"),
    ("fact_matches_staging_snapshot",
     # every staged row must be in the mart with a version at least as new
     "SELECT COUNT(*) FROM staging.sales_clean s LEFT JOIN mart.fact_sales f USING (order_item_id) "
     "WHERE f.order_item_id IS NULL OR f.source_updated_at < s.updated_at", "= 0"),
]

# Gold aggregates (lake mode) must reconcile with the fact rows of the same Silver
# snapshot (= staging.sales_clean after a lake load). Restricting to the snapshot
# keeps the check valid even if the warehouse-only path loaded other batches.
_SNAPSHOT_FACTS = ("FROM mart.fact_sales f JOIN staging.sales_clean s USING (order_item_id) "
                   "WHERE f.order_status = 'completed'")
GOLD_RECONCILIATION_CHECKS: list[tuple[str, str, str]] = [
    ("gold_daily_revenue_matches_fact",
     "SELECT ABS((SELECT COALESCE(SUM(net_revenue),0) FROM mart.agg_daily_sales) - "
     f"(SELECT COALESCE(SUM(f.net_amount),0) {_SNAPSHOT_FACTS}))", "= 0"),
    ("gold_category_revenue_matches_fact",
     "SELECT ABS((SELECT COALESCE(SUM(net_revenue),0) FROM mart.agg_category_sales) - "
     f"(SELECT COALESCE(SUM(f.net_amount),0) {_SNAPSHOT_FACTS}))", "= 0"),
    ("gold_completed_orders_match_fact",
     "SELECT ABS((SELECT COALESCE(SUM(completed_orders),0) FROM mart.agg_daily_sales) - "
     f"(SELECT COUNT(DISTINCT f.order_id) {_SNAPSHOT_FACTS}))", "= 0"),
]


def run_sql_checks(cur, checks: list[tuple[str, str, str]], layer: str = "warehouse") -> list[CheckResult]:
    results = []
    for name, query, expected in checks:
        cur.execute(query)
        value = cur.fetchone()[0]
        target = float(expected.split()[-1])
        ok = float(value) == target if expected.startswith("=") else float(value) <= target
        results.append(check(layer, name, value, ok, expected))
    return results
