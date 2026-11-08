"""Deterministic synthetic sales data generator.

* Streaming: rows are written as they are produced (one day of orders in memory
  at a time), so 1M+ row files do not need to fit in memory.
* Reproducible: every random stream is seeded from ``(seed, purpose, day)``, so
  the same arguments always yield byte-identical files, and the *clean* data does
  not change when the dirty-data rate changes.
* Realistic shape: orders with 1-4 line items, catalogue prices, discounts,
  payment methods, customer home locations and order-status lifecycle updates.
* Optional data-quality test cases: duplicates, missing values, invalid
  quantities, incorrect prices/totals, bad dates, bad statuses, messy formats.
"""
from __future__ import annotations

import csv
import json
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Iterator

from src.validation.schema import SOURCE_COLUMNS

# (product_id, product_name, category, list price in INR)
PRODUCTS: list[tuple[str, str, str, str]] = [
    ("P001", "Laptop Pro 14", "Electronics", "85000.00"),
    ("P002", "Laptop Air 13", "Electronics", "55000.00"),
    ("P003", "Wireless Mouse", "Accessories", "750.00"),
    ("P004", "Mechanical Keyboard", "Accessories", "2200.00"),
    ("P005", "27in Monitor", "Electronics", "12000.00"),
    ("P006", "Noise Cancelling Headphones", "Audio", "3000.00"),
    ("P007", "Bluetooth Speaker", "Audio", "1899.00"),
    ("P008", "USB-C Hub", "Accessories", "1499.00"),
    ("P009", "Smartphone X", "Mobiles", "29999.00"),
    ("P010", "Smartphone Lite", "Mobiles", "14999.00"),
    ("P011", "Tablet 11", "Mobiles", "24999.00"),
    ("P012", "Smart Watch", "Wearables", "4999.00"),
    ("P013", "Fitness Band", "Wearables", "1999.00"),
    ("P014", "Office Chair", "Furniture", "8999.00"),
    ("P015", "Standing Desk", "Furniture", "18999.00"),
    ("P016", "Webcam HD", "Accessories", "2499.00"),
    ("P017", "External SSD 1TB", "Storage", "6999.00"),
    ("P018", "Pen Drive 64GB", "Storage", "499.00"),
    ("P019", "Wi-Fi Router", "Networking", "3499.00"),
    ("P020", "Printer", "Electronics", "9999.00"),
]

LOCATIONS: list[tuple[str, str, str]] = [
    ("Mumbai", "Maharashtra", "India"),
    ("Pune", "Maharashtra", "India"),
    ("Delhi", "Delhi", "India"),
    ("Bengaluru", "Karnataka", "India"),
    ("Mysuru", "Karnataka", "India"),
    ("Chennai", "Tamil Nadu", "India"),
    ("Coimbatore", "Tamil Nadu", "India"),
    ("Hyderabad", "Telangana", "India"),
    ("Kolkata", "West Bengal", "India"),
    ("Ahmedabad", "Gujarat", "India"),
    ("Surat", "Gujarat", "India"),
    ("Jaipur", "Rajasthan", "India"),
    ("Lucknow", "Uttar Pradesh", "India"),
    ("Kochi", "Kerala", "India"),
    ("Thiruvananthapuram", "Kerala", "India"),
    ("Chandigarh", "Chandigarh", "India"),
    ("Bhopal", "Madhya Pradesh", "India"),
    ("Indore", "Madhya Pradesh", "India"),
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan",
    "Rohan", "Ananya", "Diya", "Aadhya", "Saanvi", "Myra", "Anika", "Kavya", "Priya",
    "Meera", "Neha", "Rahul", "Karan", "Nikhil", "Pooja", "Sneha", "Arnav", "Tara", "Zoya",
    "Farhan", "Imran", "Gurpreet", "Harleen", "Joseph", "Maria", "Deepak", "Lakshmi",
]
LAST_NAMES = [
    "Sharma", "Verma", "Iyer", "Nair", "Reddy", "Patel", "Shah", "Gupta", "Singh", "Kumar",
    "Das", "Bose", "Menon", "Pillai", "Rao", "Joshi", "Kulkarni", "Mehta", "Khan", "Fernandes",
    "Chatterjee", "Banerjee", "Agarwal", "Mishra", "Pandey", "Naidu", "Hegde", "Kapoor",
]

PAYMENT_WEIGHTS = {
    "upi": 35, "credit_card": 22, "debit_card": 18, "net_banking": 8,
    "wallet": 7, "cash_on_delivery": 10,
}
STATUS_WEIGHTS = {"completed": 85, "pending": 8, "cancelled": 7}
DISCOUNTS = (("0", 60), ("0.05", 15), ("0.10", 12), ("0.15", 8), ("0.20", 5))
DIRTY_KINDS = (
    "duplicate", "missing_customer_id", "invalid_quantity", "negative_price",
    "total_mismatch", "invalid_date", "invalid_status", "missing_email", "messy_format",
)
CENT = Decimal("0.01")


@dataclass
class GenerationStats:
    rows_written: int = 0
    clean_rows: int = 0
    update_rows: int = 0
    orders: int = 0
    dirty: Counter = field(default_factory=Counter)

    def as_dict(self) -> dict:
        return {
            "rows_written": self.rows_written,
            "clean_rows": self.clean_rows,
            "update_rows": self.update_rows,
            "orders": self.orders,
            "dirty": dict(sorted(self.dirty.items())),
        }


def build_customers(count: int, seed: int) -> list[dict[str, str]]:
    rng = random.Random(f"{seed}:customers")
    customers = []
    for i in range(1, count + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city, state, country = rng.choice(LOCATIONS)
        customers.append({
            "customer_id": f"C{i:06d}",
            "customer_name": f"{first} {last}",
            "customer_email": f"{first}.{last}{i}@example.com".lower(),
            "city": city, "state": state, "country": country,
        })
    return customers


def _weighted(rng: random.Random, weights: dict[str, int]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def line_total(quantity: int, unit_price: Decimal, discount: Decimal) -> Decimal:
    return (quantity * unit_price * (1 - discount)).quantize(CENT, rounding=ROUND_HALF_UP)


def orders_for_day(day: date, n_rows: int, seed: int,
                   customers: list[dict[str, str]]) -> Iterator[list[dict[str, str]]]:
    """Yield the orders (each a list of line-item rows) placed on ``day``."""
    rng = random.Random(f"{seed}:orders:{day.isoformat()}")
    produced, order_no = 0, 0
    while produced < n_rows:
        order_no += 1
        n_lines = min(rng.choices((1, 2, 3, 4), weights=(55, 25, 15, 5))[0], n_rows - produced)
        customer = rng.choice(customers)
        placed_at = datetime.combine(day, time()) + timedelta(seconds=rng.randrange(86_400))
        status = _weighted(rng, STATUS_WEIGHTS)
        payment = _weighted(rng, PAYMENT_WEIGHTS)
        order_id = f"ORD-{day:%Y%m%d}-{order_no:06d}"
        rows = []
        for line_no, product in enumerate(rng.sample(PRODUCTS, n_lines), start=1):
            product_id, product_name, category, list_price = product
            quantity = rng.choices((1, 2, 3, 4, 5), weights=(50, 25, 12, 8, 5))[0]
            unit_price = Decimal(list_price)
            discount = Decimal(rng.choices([d for d, _ in DISCOUNTS],
                                           weights=[w for _, w in DISCOUNTS])[0])
            rows.append({
                "order_id": order_id,
                "order_item_id": f"{order_id}-{line_no:02d}",
                "order_date": day.isoformat(),
                "customer_id": customer["customer_id"],
                "customer_name": customer["customer_name"],
                "customer_email": customer["customer_email"],
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "quantity": str(quantity),
                "unit_price": f"{unit_price:.2f}",
                "discount": f"{discount:.2f}",
                "total_amount": f"{line_total(quantity, unit_price, discount):.2f}",
                "currency": "INR",
                "payment_method": payment,
                "order_status": status,
                "city": customer["city"],
                "state": customer["state"],
                "country": customer["country"],
                "updated_at": placed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
        produced += n_lines
        yield rows


def lifecycle_update(order: list[dict[str, str]], rng: random.Random,
                     updated_at: datetime) -> list[dict[str, str]] | None:
    """Status change for an existing order (pending->completed/cancelled, completed->cancelled)."""
    current = order[0]["order_status"]
    if current == "pending":
        new_status = "completed" if rng.random() < 0.7 else "cancelled"
    elif current == "completed":
        new_status = "cancelled"
    else:
        return None
    stamp = updated_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    return [{**row, "order_status": new_status, "updated_at": stamp} for row in order]


def _make_dirty(row: dict[str, str], rng: random.Random, stats: GenerationStats) -> list[dict[str, str]]:
    kind = rng.choice(DIRTY_KINDS)
    stats.dirty[kind] += 1
    bad = dict(row)
    if kind == "duplicate":
        return [row, dict(row)]
    if kind == "missing_customer_id":
        bad["customer_id"] = ""
    elif kind == "invalid_quantity":
        bad["quantity"] = rng.choice(("0", "-2", "abc"))
    elif kind == "negative_price":
        bad["unit_price"] = "-" + bad["unit_price"]
    elif kind == "total_mismatch":
        bad["total_amount"] = f"{Decimal(bad['total_amount']) + 100:.2f}"
    elif kind == "invalid_date":
        bad["order_date"] = "2026-13-45"
    elif kind == "invalid_status":
        bad["order_status"] = "shipped??"
    elif kind == "missing_email":
        bad["customer_email"] = ""
    elif kind == "messy_format":  # valid after normalisation
        bad["order_status"] = bad["order_status"].upper()
        bad["order_date"] = bad["order_date"].replace("-", "/")
        bad["customer_name"] = f"  {bad['customer_name']}  "
    return [bad]


class _Writer:
    def __init__(self, path: Path, fmt: str):
        self.path, self.fmt = path, fmt
        path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = path.open("w", newline="", encoding="utf-8")
        if fmt == "csv":
            self.writer = csv.DictWriter(self.handle, fieldnames=SOURCE_COLUMNS)
            self.writer.writeheader()

    def write(self, row: dict[str, str]) -> None:
        if self.fmt == "csv":
            self.writer.writerow(row)
        else:  # JSON Lines
            self.handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    def close(self) -> None:
        self.handle.close()


def _emit(writer: _Writer, rows: list[dict[str, str]], dirty_rng: random.Random,
          dirty_rate: float, stats: GenerationStats) -> None:
    for row in rows:
        out = _make_dirty(row, dirty_rng, stats) if dirty_rate and dirty_rng.random() < dirty_rate else [row]
        for item in out:
            writer.write(item)
            stats.rows_written += 1


def generate_dataset(path: Path, records: int = 1000, seed: int = 42,
                     start_date: date = date(2026, 1, 1), days: int = 90,
                     dirty_rate: float = 0.0, update_rate: float = 0.03,
                     customers: int = 2000, fmt: str = "csv") -> GenerationStats:
    """Write ``records`` clean line items spread over ``days`` days, plus optional
    lifecycle updates and dirty test rows (which are written in addition)."""
    if records < 1 or days < 1:
        raise ValueError("records and days must be positive")
    pool = build_customers(customers, seed)
    stats = GenerationStats()
    writer = _Writer(Path(path), fmt)
    try:
        base, extra = divmod(records, days)
        for offset in range(days):
            day = start_date + timedelta(days=offset)
            n_rows = base + (1 if offset < extra else 0)
            if n_rows == 0:
                continue
            dirty_rng = random.Random(f"{seed}:dirty:{day.isoformat()}")
            update_rng = random.Random(f"{seed}:updates:{day.isoformat()}")
            for order in orders_for_day(day, n_rows, seed, pool):
                stats.orders += 1
                stats.clean_rows += len(order)
                _emit(writer, order, dirty_rng, dirty_rate, stats)
                if update_rate and update_rng.random() < update_rate:
                    changed = lifecycle_update(
                        order, update_rng,
                        datetime.combine(day + timedelta(days=1), time()) + timedelta(seconds=update_rng.randrange(86_400)),
                    )
                    if changed:
                        stats.update_rows += len(changed)
                        _emit(writer, changed, dirty_rng, 0.0, stats)
    finally:
        writer.close()
    return stats


def generate_daily_batch(path: Path, logical_date: date, records: int = 1000, seed: int = 42,
                         dirty_rate: float = 0.0, update_rate: float = 0.05,
                         customers: int = 2000, fmt: str = "csv") -> GenerationStats:
    """One day's extract for the orchestrated pipeline.

    Contains new orders placed on ``logical_date`` and status updates (made on
    ``logical_date``) for orders placed the previous day, which are regenerated
    deterministically from the same seed.
    """
    pool = build_customers(customers, seed)
    stats = GenerationStats()
    writer = _Writer(Path(path), fmt)
    try:
        dirty_rng = random.Random(f"{seed}:dirty:{logical_date.isoformat()}")
        for order in orders_for_day(logical_date, records, seed, pool):
            stats.orders += 1
            stats.clean_rows += len(order)
            _emit(writer, order, dirty_rng, dirty_rate, stats)
        update_rng = random.Random(f"{seed}:daily-updates:{logical_date.isoformat()}")
        previous = logical_date - timedelta(days=1)
        for order in orders_for_day(previous, records, seed, pool):
            if update_rng.random() < update_rate:
                changed = lifecycle_update(
                    order, update_rng,
                    datetime.combine(logical_date, time()) + timedelta(seconds=update_rng.randrange(86_400)),
                )
                if changed:
                    stats.update_rows += len(changed)
                    _emit(writer, changed, dirty_rng, 0.0, stats)
    finally:
        writer.close()
    return stats
