"""Backwards-compatible Phase-1 generator entry point.

    python -m src.generate_data --count 1000 [--output data/generated/sales.csv]

Delegates to ``python -m src.cli generate`` (see that command for all options:
dirty test cases, seeds, daily batches, JSON Lines output).
"""
import argparse
import sys

from src.cli import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--output", default="data/generated/sales.csv")
    parser.add_argument("--dirty-rate", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    sys.exit(main(["generate", "--records", str(args.count), "--output", args.output,
                   "--dirty-rate", str(args.dirty_rate), "--seed", str(args.seed)]))
