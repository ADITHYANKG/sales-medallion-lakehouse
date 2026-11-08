# One-time setup on Windows PowerShell:  .\scripts\initialize.ps1
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path .env)) { python scripts/setup_env.py }
docker compose build
docker compose up -d
docker compose run --rm app python -m src.cli generate --records 1000 --dirty-rate 0.03 --output data/sample/sales_sample.csv
Write-Host ""
Write-Host "Stack is starting. Next steps:"
Write-Host "  .\scripts\run_pipeline.ps1                 # lake pipeline for one day (no Airflow)"
Write-Host "  docker compose run --rm app python -m src.cli provision-metabase"
Write-Host "  Airflow   http://localhost:8080   MinIO   http://localhost:9001   Metabase http://localhost:3000"
