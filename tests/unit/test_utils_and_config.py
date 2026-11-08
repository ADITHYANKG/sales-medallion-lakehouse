import re

from src.analytics.dashboards import DASHBOARDS
from src.analytics.metabase_provision import GRID_WIDTH, _layout
from src.cli import build_parser, load_named_queries
from src.config.settings import load_settings
from src.utils.ids import batch_id_for_checksum, safe_token, sha256_bytes, sha256_file
from src.warehouse.migrate import discover


def test_batch_id_is_content_addressed(tmp_path):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    a.write_bytes(b"x,y\n1,2\n")
    b.write_bytes(b"x,y\n1,2\n")
    assert sha256_file(a) == sha256_file(b) == sha256_bytes(b"x,y\n1,2\n")
    assert batch_id_for_checksum(sha256_file(a)) == batch_id_for_checksum(sha256_file(b))


def test_safe_token():
    assert safe_token("manual__2026-03-01T00:00:00+00:00") == "manual__2026-03-01T00-00-00-00-00"


def test_settings_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("SPARK_SHUFFLE_PARTITIONS", "3")
    monkeypatch.setenv("ALLOWED_CURRENCIES", "inr, usd")
    settings = load_settings(env_file=tmp_path / "missing.env")
    assert settings.spark_shuffle_partitions == 3
    assert settings.allowed_currencies == ("INR", "USD")


def test_migrations_are_ordered_and_unique():
    versions = [v for v, _, _ in discover()]
    assert versions == sorted(versions) == list(range(1, len(versions) + 1))


def test_named_analytics_queries():
    queries = load_named_queries()
    for required in ("headline_kpis", "top_products", "revenue_by_category", "monthly_sales", "daily_sales",
                     "revenue_by_location", "customer_lifetime_revenue", "order_status_breakdown"):
        assert required in queries and queries[required].upper().lstrip().startswith(("SELECT", "WITH"))


def test_dashboard_specs_are_consistent():
    names = set()
    for dash in DASHBOARDS:
        params = {p["slug"] for p in dash["parameters"]}
        for card in dash["cards"]:
            assert card["name"] not in names
            names.add(card["name"])
            used = set(re.findall(r"\{\{(\w+)\}\}", card["sql"]))
            assert used == set(card.get("tags", ())), card["name"]
            assert used <= params
        for row, col in _layout(dash["cards"]):
            assert 0 <= col < GRID_WIDTH
    assert len(DASHBOARDS[0]["cards"]) >= 12


def test_cli_parser_has_all_commands():
    parser = build_parser()
    commands = parser._subparsers._group_actions[0].choices
    assert {"init", "generate", "ingest", "lake-run", "verify", "report", "benchmark",
            "provision-metabase", "lake-inspect"} <= set(commands)
