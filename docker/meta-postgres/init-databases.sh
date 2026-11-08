#!/bin/bash
# Creates separate databases + owners for Airflow and Metabase metadata.
# Runs once, on first initialisation of the meta_postgres_data volume.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" -v metabase_pw="$METABASE_APP_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;
CREATE ROLE metabase_app LOGIN PASSWORD :'metabase_pw';
CREATE DATABASE metabase OWNER metabase_app;
SQL
