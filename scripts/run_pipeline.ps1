# Run the lake pipeline (Bronze -> Silver -> Gold -> Warehouse) without Airflow.
#   .\scripts\run_pipeline.ps1                       # today
#   .\scripts\run_pipeline.ps1 -Date 2026-03-01 -Days 7
param([string]$Date = (Get-Date -Format "yyyy-MM-dd"), [int]$Days = 1, [string]$Source = "auto")
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose run --rm app python -m src.cli lake-run --date $Date --days $Days --source $Source
docker compose run --rm app python scripts/verify_pipeline.py
