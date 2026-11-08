"""Backwards-compatible Phase-1 entry point.

    python -m src.pipeline --input data/sample/sales_sample.csv

Equivalent to ``python -m src.cli ingest``: CSV -> validation -> raw -> staging ->
star schema, in one transaction, idempotent on re-run. Files in the original
starter layout (transaction_id, status, ...) are still accepted.
"""
import argparse
import sys
from pathlib import Path

from src.cli import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/sample/sales_sample.csv"))
    args = parser.parse_args()
    sys.exit(main(["ingest", "--input", str(args.input)]))
