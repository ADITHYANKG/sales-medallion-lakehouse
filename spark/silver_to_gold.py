"""Standalone PySpark job: Silver (clean Parquet) -> Gold (business aggregates).

    python spark/silver_to_gold.py --silver s3a://silver/sales/version=<v>/ --gold s3a://gold --version manual

Writes ``<gold>/<table>/version=<version>/`` for daily_sales, monthly_sales,
category_sales, product_sales, customer_sales and location_sales.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import load_settings  # noqa: E402
from src.transformations import spark_jobs as SJ  # noqa: E402
from src.transformations.spark_session import build_spark  # noqa: E402
from src.utils.logging import configure_logging  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--silver", required=True)
    parser.add_argument("--gold", required=True, help="base URI, e.g. s3a://gold")
    parser.add_argument("--version", default="manual")
    parser.add_argument("--master", default=None)
    parser.add_argument("--show", action="store_true", help="print the top rows of each table")
    args = parser.parse_args()
    configure_logging()
    spark = build_spark(load_settings(), "silver_to_gold", master=args.master)
    silver = SJ.read_silver(spark, args.silver).cache()
    counts = {}
    for name, frame in SJ.build_gold(silver).items():
        target = f"{args.gold.rstrip('/')}/{name}/version={args.version}/"
        SJ.write_small(frame, target)
        counts[name] = spark.read.parquet(target).count()
        if args.show:
            frame.show(5, truncate=False)
    print(json.dumps(counts, indent=2))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
