# Implementation and verification report

Environment used for verification: Windows 11, Docker Desktop 29.5 (WSL2 VM with 16 CPUs and
11.5 GiB RAM), project image `sales-pipeline:latest` (Python 3.12.13, OpenJDK 17.0.21,
PySpark 4.1.1, Airflow 3.1.8, pandas 2.3.3, pyarrow 18.1.0). Date: 2026-11-08.

Everything below marked **tested** was executed in this environment, and the result was
observed. Nothing is claimed from reasoning alone.

**Final demo state** (after a reset of the non-legacy schemas and buckets): 69 batches for
2026-01-01 to 2026-03-10. March was processed by Airflow (10 runs: backfills plus manual runs);
January and February by `lake-run` (59 runs). 68 runs succeeded and 1 failed on purpose (malformed file,
quarantined). The warehouse holds 71,266 raw rows and 67,276 fact rows. `verify_pipeline.py`:
27/27 checks passed. `pytest`: 70 passed.

## 1. Implemented and tested

| Area | What was verified | How |
|---|---|---|
| Docker stack | postgres, meta-postgres, minio, init, airflow-apiserver/scheduler/dag-processor, metabase, mock-api all healthy | `docker compose up -d`, `docker compose ps` |
| Phase-1 preservation | Existing volume kept; starter tables moved to `legacy` (786 facts, ₹37,263,300 unchanged); original CSV layout still loads; `python -m src.pipeline` and `src.generate_data` still work | migration V001 + ingest of `data/sample/legacy_phase1_sales.csv` (completed revenue matched exactly) |
| Generator | Reproducible (byte-identical), exact counts, 10k / 100k / 1M rows streamed, defects caught by validation, daily batches with updates | unit tests; benchmark inputs (1,029,722 rows, 222 MB) |
| Validation | 11 rules incl. half-up rounding, normalisation, defaults, legacy mapping, schema errors | 34 unit tests (incl. the 4 original starter cases) |
| Warehouse-only path | Idempotent re-run (0 raw inserts, 0 fact changes); cancellation then reinstatement; late older version ignored; SCD1 keeps surrogate key; reject-rate gate rolls back everything; quarantine file written | integration tests on an isolated `sales_dw_test` DB, plus manual runs |
| MinIO utilities | upload/download/exists/metadata/checksum/list, Parquet round-trip, versioned publish and prune | integration tests (`test-*` buckets) |
| Bronze | Partitioned keys, `_metadata.json`, content-addressed batch ids, duplicate delivery detected (`times_received=2`, no re-upload) | lake runs + `monitoring.ingestion_batches` |
| Silver / Gold (Spark) | Explicit schemas, ANSI-safe casts, dedup window, quarantine, month-partitioned Parquet, 6 Gold tables, reconciliation before publish | end-to-end runs, `lake-inspect`, integration tests |
| pandas vs Spark parity | Identical counts, every Silver row, every reject reason, all 6 Gold tables (on 2 batches with 5% defects) | `tests/integration/test_spark_parity.py` |
| Warehouse load from Spark | JDBC to staging, transactional merge under advisory lock, Gold swap; re-run changes 0 rows; SQL on the fact table equals the Gold aggregates | integration test + `verify_pipeline.py` |
| Batch quarantine | Malformed file → `validate_bronze` fails, object moved to `bronze/quarantine/`, run marked failed, later runs unaffected | Airflow run `manual__malformed_file_2026-03-08` + integration test |
| Airflow DAG | Parses with no import errors; 10 tasks in order; manual trigger; scheduled run; backfills (7 runs, then 2 serialised runs); retry after simulated DB outage (attempt 1 failed, attempt 2 succeeded); quality-gate failure with `upstream_failed` downstream; reprocessing a failed run after fixing the source; task logs present | Airflow CLI/UI, `monitoring.task_runs` |
| Monitoring | `pipeline_runs`, `task_runs`, `ingestion_batches`, `data_quality_results` populated for success and failure | SQL queries, Metabase monitoring dashboard |
| Optional Spark cluster | Full lake run on `spark://spark-master:7077` with executors in `spark-worker` | `--profile spark-cluster`; found and documented the "no underscore in driver hostname" requirement |
| Metabase | Admin setup, read-only DB connection, collection, 21 native questions, 2 dashboards with 3 filters; every card executed through the API without errors; filtered result equals direct SQL (₹74,458,419.85 at the time of the test) | `provision-metabase` + API execution of each dashcard |
| Least privilege | `metabase_reader`: SELECT works; DELETE → `ReadOnlySqlTransaction`; `raw.*` → permission denied | manual check + integration test + `verify_pipeline.py` |
| Benchmark | Pandas vs PySpark at 10k / 100k / 1M rows, 3 repeats, peak RSS, partitions, identical outputs | `docs/benchmark_results.md` |
| Test suite | **70 passed** (unit + integration), no skips, inside the project image | `docker compose run --rm app python -m pytest -q` |
| End-to-end verification | **27/27** checks passed on the demo dataset | `scripts/verify_pipeline.py` |
| Screenshots | Metabase (both dashboards), Airflow DAG runs, MinIO Bronze | `scripts/capture_screenshots.py` → `docs/images/` |

### Problems found during verification and fixed

1. The MinIO healthcheck needed `curl`/`grep`, which the minimal image lacks, and newer MinIO rejects
   HTTP/1.0 requests without a `Host` header. Replaced with a bash `/dev/tcp` HTTP/1.1 probe.
2. The `raw.sales_records` temp table lacked column defaults (`LIKE ... INCLUDING DEFAULTS`).
3. A single bad batch would have failed every later Silver recompute (cumulative reject rate).
   Added batch-level quarantine (migration V006).
4. Airflow 3 backfills ignore the DAG's `max_active_runs` (7 runs started at once). Added the
   `pipeline_writes` pool and an optimistic "don't publish an older Silver snapshot" guard.
5. pandas was a strawman in the first benchmark (row-wise `apply`, Python-object strings).
   Vectorised the code and used Arrow-backed strings: 1M-row warm time dropped from 78 s to 40 s,
   with outputs unchanged (parity tests).
6. Spark executors reject driver hostnames containing `_`. Documented.
7. Dashboard growth KPI compared a partial month with a full month. Replaced with like-for-like
   month-to-date growth.

## 2. Implemented but not tested

* `ALERT_WEBHOOK_URL` failure notification POST (the code path runs inside the failure callback, but
  no webhook endpoint was configured during verification).
* REST API ingestion **through the Airflow DAG** with `SALES_API_URL=http://mock-api:8000/sales`. The
  mock API service is running and the fetch code is written; a full API-sourced DAG run was not
  executed.
* JSON Lines Bronze files in the Spark path (`include_json`); the generator and pandas JSONL
  paths are unit-tested.
* `scripts/initialize.ps1|.sh` and `scripts/run_pipeline.ps1|.sh` as complete scripts. The
  individual commands they contain were all run.
* Running the unit tests in a host virtualenv (verification used the Docker image).

## 3. Not implemented

* SCD Type 2 (design documented in `DATA_MODEL.md`).
* Incremental Silver processing / ACID table format (Iceberg/Delta). Silver is a full recompute.
* Multi-currency conversion (only `INR` is accepted).
* CI pipeline, and a Git repository: the project directory is not a Git repo, so nothing was
  initialised or committed.

## 4. Known limitations

* Full Silver recompute: run time grows with history (about 145 s per Airflow run at about 10k
  historical rows, mostly JVM start-up per task).
* Spark JDBC writes to staging are not atomic across partitions; correctness relies on the
  subsequent single-transaction merge and the advisory lock.
* The Silver publish guard compares batch sets at publish time. A tiny race window remains
  between reading and writing the pointer; the 1-slot pool makes it practically unreachable.
* Demo data starts mid-stream: the first processed day contains updates for orders from the day
  before, which was never loaded, so December 2025 shows a handful of (mostly cancelled) orders.
* Spark `local[*]` and Airflow LocalExecutor on one host; no HA. Credentials live in a local `.env`.
* MinIO comes from a community-built image (Chainguard) because upstream images were discontinued.
