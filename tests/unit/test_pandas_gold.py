import pandas as pd
import pytest

from src.transformations import pandas_transforms as PT


def silver_rows(rows):
    """Build a silver-like frame through the real validation path."""
    frame = pd.DataFrame(rows, dtype=str)
    silver, _, _ = PT.bronze_to_silver([(frame, "b1", "2026-03-10")])
    return silver


def item(order, line, day, status, qty, price, customer="C1", product="P1", category="Electronics",
         discount="0", city="Pune"):
    from decimal import Decimal

    from src.ingestion.generator import line_total

    return {"order_id": order, "order_item_id": f"{order}-{line}", "order_date": day,
            "customer_id": customer, "customer_name": f"Name {customer}", "customer_email": "",
            "product_id": product, "product_name": f"Product {product}", "category": category,
            "quantity": str(qty), "unit_price": price, "discount": discount,
            "total_amount": str(line_total(qty, Decimal(price), Decimal(discount))), "currency": "INR",
            "payment_method": "upi", "order_status": status, "city": city, "state": "MH",
            "country": "India", "updated_at": f"{day}T10:00:00Z"}


@pytest.fixture()
def gold():
    rows = [
        item("O1", 1, "2026-01-10", "completed", 2, "100.00"),
        item("O1", 2, "2026-01-10", "completed", 1, "50.00", product="P2", category="Audio"),
        item("O2", 1, "2026-01-10", "cancelled", 1, "999.00", customer="C2"),
        item("O3", 1, "2026-02-01", "completed", 1, "300.00", customer="C2", discount="0.10", city="Mumbai"),
        item("O4", 1, "2026-02-02", "pending", 1, "10.00", customer="C3"),
    ]
    return PT.build_gold(silver_rows(rows))


def test_only_completed_revenue_counts(gold):
    daily = gold["daily_sales"].set_index("order_date")
    jan10 = daily.loc[pd.Timestamp("2026-01-10").date()]
    assert jan10["net_revenue"] == 250.00  # cancelled 999 excluded
    assert (jan10["total_orders"], jan10["completed_orders"], jan10["cancelled_orders"]) == (2, 1, 1)
    assert jan10["avg_order_value"] == 250.00
    assert gold["daily_sales"]["cumulative_revenue"].iloc[-1] == 520.00


def test_monthly_growth_and_cancel_rate(gold):
    monthly = gold["monthly_sales"].reset_index(drop=True)
    assert list(monthly["net_revenue"]) == [250.00, 270.00]
    assert monthly.loc[1, "revenue_growth_pct"] == 8.00
    assert monthly.loc[0, "cancelled_order_pct"] == 50.00
    assert pd.isna(monthly.loc[0, "revenue_growth_pct"])


def test_rankings_and_shares(gold):
    category = gold["category_sales"].set_index("category")
    assert category.loc["Electronics", "net_revenue"] == 470.00
    assert category.loc["Electronics", "revenue_rank"] == 1
    assert category["revenue_share_pct"].sum() == pytest.approx(100.0, abs=0.02)
    customers = gold["customer_sales"].set_index("customer_id")
    assert customers.loc["C2", "lifetime_revenue"] == 270.00 and customers.loc["C2", "total_orders"] == 2
    assert customers.loc["C3", "lifetime_revenue"] == 0.00
    assert list(gold["customer_sales"].sort_values("revenue_rank")["customer_id"]) == ["C2", "C1", "C3"]


def test_location_and_product_totals_reconcile(gold):
    total = gold["daily_sales"]["net_revenue"].sum()
    for name, column in [("location_sales", "net_revenue"), ("product_sales", "net_revenue"),
                         ("category_sales", "net_revenue"), ("customer_sales", "lifetime_revenue")]:
        assert gold[name][column].sum() == pytest.approx(total)
