#!/usr/bin/env bash
# Run the lake pipeline (Bronze -> Silver -> Gold -> Warehouse) without Airflow.
#   ./scripts/run_pipeline.sh [YYYY-MM-DD] [days] [source]
set -euo pipefail
cd "$(dirname "$0")/.."
DATE="${1:-$(date +%F)}"; DAYS="${2:-1}"; SOURCE="${3:-auto}"
docker compose run --rm app python -m src.cli lake-run --date "$DATE" --days "$DAYS" --source "$SOURCE"
docker compose run --rm app python scripts/verify_pipeline.py
