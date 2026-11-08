"""Standalone PySpark job: Bronze (raw CSV) -> Silver (clean Parquet) + quarantine.

The orchestrated pipeline calls the same functions via
``src.orchestration.lake_pipeline.transform_silver``. This script exists so the job
can be run on its own, e.g. with spark-submit against the standalone cluster:

    docker compose run --rm app spark-submit --master spark://spark-master:7077 \
        spark/bronze_to_silver.py --bronze s3a://bronze/sales/ \
        --silver s3a://silver/sales/version=manual/ --quarantine s3a://silver/sales_rejected/version=manual/

or locally on any paths:  python spark/bronze_to_silver.py --bronze ./bronze --silver ./silver ...
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
    parser.add_argument("--bronze", required=True, help="base URI containing ingestion_date=*/batch_id=*/")
    parser.add_argument("--silver", required=True)
    parser.add_argument("--quarantine", required=True)
    parser.add_argument("--json", action="store_true", help="also read *.jsonl bronze files")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()
    configure_logging()
    settings = load_settings()
    spark = build_spark(settings, "bronze_to_silver", master=args.master)
    raw = SJ.read_bronze(spark, args.bronze, include_csv=True, include_json=args.json)
    silver, rejected, _valid, annotated = SJ.bronze_to_silver(raw, settings.allowed_currencies)
    SJ.write_silver(silver, args.silver, settings.spark_shuffle_partitions)
    SJ.write_small(rejected, args.quarantine)
    stats = SJ.silver_stats(annotated, silver)
    stats["reject_reasons"] = SJ.reject_reason_counts(rejected)
    print(json.dumps(stats, indent=2))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
