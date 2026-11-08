"""PySpark must produce exactly the same Silver and Gold results as pandas."""
import numpy as np
import pandas as pd
import pytest

from src.ingestion.generator import generate_dataset
from src.transformations import pandas_transforms as PT

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def results(spark, tmp_path_factory):
    from src.transformations import spark_jobs as SJ

    root = tmp_path_factory.mktemp("bronze")
    batches = []
    for i, (seed, ingestion) in enumerate([(1, "2026-01-01"), (2, "2026-01-02")]):
        path = root / f"ingestion_date={ingestion}" / f"batch_id=b{i}" / "sales.csv"
        generate_dataset(path, records=1500, seed=1, days=20, dirty_rate=0.05, update_rate=0.1 * seed)
        batches.append((pd.read_csv(path, dtype=str, keep_default_na=False), f"b{i}", ingestion))
    silver_p, rejected_p, stats_p = PT.bronze_to_silver(batches)
    raw = SJ.read_bronze(spark, str(root))
    silver_s, rejected_s, _valid, annotated = SJ.bronze_to_silver(raw, ("INR",))
    stats_s = SJ.silver_stats(annotated, silver_s)
    gold_s = {k: v.toPandas() for k, v in SJ.build_gold(silver_s).items()}
    out = {"silver_p": silver_p, "silver_s": silver_s.toPandas(), "stats_p": stats_p, "stats_s": stats_s,
           "rejected_p": rejected_p, "rejected_s": rejected_s.toPandas(),
           "gold_p": PT.build_gold(silver_p), "gold_s": gold_s}
    annotated.unpersist()
    return out


def test_counts_match(results):
    for key in ("source_records", "valid_records", "rejected_records", "exact_duplicates",
                "superseded_versions", "silver_records"):
        assert results["stats_p"][key] == results["stats_s"][key], key


def test_silver_rows_match(results):
    p = PT.R.with_decimal_columns(results["silver_p"]).set_index("order_item_id").sort_index()
    s = results["silver_s"].set_index("order_item_id").sort_index()
    assert list(p.index) == list(s.index)
    for column in ("order_status", "customer_id", "product_id", "source_row_hash", "batch_id", "quantity"):
        assert (p[column].astype(str).values == s[column].astype(str).values).all(), column
    for column in ("unit_price", "gross_amount", "discount_amount", "net_amount"):
        assert np.allclose(p[column].values, s[column].astype(float).values, atol=1e-9), column


def test_rejection_reasons_match(results):
    p = results["rejected_p"].set_index(["batch_id", "source_row_hash"])["reject_reasons"].sort_index()
    s = results["rejected_s"].set_index(["batch_id", "source_row_hash"])["reject_reasons"].sort_index()
    pd.testing.assert_series_equal(p, s, check_names=False)


@pytest.mark.parametrize("table", ["daily_sales", "monthly_sales", "category_sales", "product_sales",
                                   "customer_sales", "location_sales"])
def test_gold_tables_match(results, table):
    p, s = results["gold_p"][table], results["gold_s"][table]
    assert list(p.columns) == list(s.columns)
    keys = ["country", "state", "city"] if table == "location_sales" else [p.columns[0]]
    p = p.sort_values(keys).reset_index(drop=True)
    s = s.sort_values(keys).reset_index(drop=True)
    for column in p.columns:
        try:
            a = pd.to_numeric(p[column]).astype(float).fillna(-1)
            b = pd.to_numeric(s[column]).astype(float).fillna(-1)
            assert np.allclose(a, b, atol=0.011), column
        except (ValueError, TypeError):
            assert (p[column].astype(str).values == s[column].astype(str).values).all(), column
