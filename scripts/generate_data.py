"""Generate synthetic sales data (thin wrapper around ``python -m src.cli generate``).

    python scripts/generate_data.py --records 100000 --dirty-rate 0.02 --output data/generated/sales_100k.csv
    python scripts/generate_data.py --records 1000000 --output data/generated/sales_1m.csv
    python scripts/generate_data.py --daily 2026-03-01 --records 1000 --output data/incoming/sales_2026-03-01.csv
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(["generate", *sys.argv[1:]]))
