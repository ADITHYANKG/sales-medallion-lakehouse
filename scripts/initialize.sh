#!/usr/bin/env bash
# One-time setup on macOS/Linux (or Git Bash):  ./scripts/initialize.sh
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || python3 scripts/setup_env.py
docker compose build
docker compose up -d
docker compose run --rm app python -m src.cli generate --records 1000 --dirty-rate 0.03 --output data/sample/sales_sample.csv
cat <<'EOF'

Stack is starting. Next steps:
  ./scripts/run_pipeline.sh                    # lake pipeline for one day (no Airflow)
  docker compose run --rm app python -m src.cli provision-metabase
  Airflow http://localhost:8080 | MinIO http://localhost:9001 | Metabase http://localhost:3000
EOF
