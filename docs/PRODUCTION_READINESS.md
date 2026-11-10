# Production readiness — what exists and what is left

This project runs locally on one machine with Docker Compose. This document lists what would
need to change to run it in production, grouped by area, with an honest status for each item.

Status: ✅ done · ⚠️ partly done · ❌ not done

---

## 1. Summary: what to do first

| Priority | Item | Area |
|---|---|---|
| 1 | Secrets manager instead of `.env`; TLS everywhere; SSO for the UIs | Security |
| 2 | Managed services: S3, managed PostgreSQL/warehouse, managed Airflow or Kubernetes | Infrastructure |
| 3 | CI/CD pipeline that runs the tests and deploys | Delivery |
| 4 | Alerting that reaches a human (Slack/PagerDuty/email), freshness and volume alerts | Observability |
| 5 | Backups + tested restore for the warehouse and the lake | Reliability |
| 6 | Incremental Silver (Iceberg/Delta) instead of a full recompute | Scalability |
| 7 | Anomaly detection and data contracts with the source team | Data quality |

---

## 2. Security

| Item | Status | Now | Production |
|---|---|---|---|
| Secrets | ⚠️ | `.env` on disk (git-ignored), random values from `scripts/setup_env.py` | AWS Secrets Manager / Azure Key Vault / HashiCorp Vault; Airflow secrets backend; automatic rotation |
| Database accounts | ⚠️ | `sales_user` is the **superuser** created by the postgres image and is also used by the pipeline; `metabase_reader` is read-only | Separate roles: migrations owner, pipeline writer (no DDL), BI reader, analysts; no shared admin |
| Encryption in transit | ❌ | Plain HTTP/PostgreSQL on localhost | TLS for PostgreSQL (`sslmode=verify-full`), S3 over HTTPS, HTTPS for Airflow/Metabase behind a load balancer |
| Encryption at rest | ❌ | Docker volumes | Encrypted disks/RDS, S3 server-side encryption (SSE-KMS) |
| Network | ⚠️ | All ports bound to `127.0.0.1` | Private subnets/VPC; only the BI and Airflow UIs reachable, via VPN/SSO proxy; security groups |
| UI authentication | ⚠️ | Local admin users (Airflow FAB, Metabase) | SSO (Okta/Azure AD/Google) with roles; MFA |
| Personal data (PII) | ❌ | Customer name and email stored in clear text | Classify columns; mask or hash email in `mart`; row/column-level access; retention and deletion (GDPR/DPDP Act) |
| Container images | ⚠️ | Pinned versions; MinIO pinned by digest | Image scanning (Trivy/Grype) in CI; minimal base images; private registry; regular patching |
| SQL injection | ✅ | Parameterised queries throughout | Keep; add static analysis (Bandit) in CI |
| Audit | ⚠️ | `raw` keeps every received row; run history in `monitoring` | Database audit logs, S3 access logs, who-changed-what for Airflow/Metabase |

---

## 3. Infrastructure and deployment

| Item | Status | Now | Production |
|---|---|---|---|
| Hosting | ❌ | One PC, Docker Compose | Cloud (AWS/Azure/GCP) or Kubernetes |
| Object storage | ⚠️ | MinIO locally; **Amazon S3 with an IAM role supported** (`LAKE_STORAGE=aws`, [DEPLOY_AWS.md](DEPLOY_AWS.md)) | S3 with lifecycle rules, versioning, SSE-KMS |
| Warehouse | ⚠️ | PostgreSQL container | Managed PostgreSQL (RDS/Cloud SQL) at this size; Snowflake/BigQuery/Redshift when large |
| Airflow | ⚠️ | LocalExecutor, one scheduler | Managed (MWAA, Cloud Composer, Astronomer) or Helm chart on Kubernetes with CeleryExecutor/KubernetesExecutor, 2+ schedulers |
| Spark | ⚠️ | `local[*]` inside the Airflow task (optional standalone cluster tested) | EMR / Dataproc / Databricks / Spark on Kubernetes; tasks submit jobs instead of running Spark in the worker |
| Metabase | ⚠️ | Single container | Metabase Cloud or 2+ instances behind a load balancer, metadata DB on managed PostgreSQL |
| Infrastructure as code | ❌ | `compose.yaml` only | Terraform / Pulumi for all resources |
| Environments | ❌ | One | dev → staging → prod, with separate data and credentials |

---

## 4. CI/CD and testing

| Item | Status | Now | Production |
|---|---|---|---|
| Unit + integration tests | ✅ | 70 pytest tests (real PostgreSQL, MinIO, Spark) | Keep; run on every pull request |
| CI pipeline | ❌ | Tests run manually | GitHub Actions: lint (ruff), type check (mypy), tests, image build, image scan |
| DAG tests | ✅ | DAG import + structure test | Also run `airflow dags test` in CI |
| Deployment | ❌ | Manual | Automated deploy on merge (image to registry, DAGs to Airflow) with rollback |
| Migrations | ✅ | Versioned SQL V001–V006, applied once | Run in CI against a copy of prod schema; reviewed; backwards-compatible changes |
| Data tests on real-like data | ⚠️ | Generator with injected defects; parity tests | Staging environment with a masked copy of production data |

---

## 5. Observability (monitoring, logging, alerting)

| Item | Status | Now | Production |
|---|---|---|---|
| Run / task / batch metrics | ✅ | `monitoring.*` tables + Metabase monitoring dashboard | Keep; also export to Prometheus/Datadog/CloudWatch |
| Alerts | ⚠️ | Failure callback can POST to `ALERT_WEBHOOK_URL` (not tested with a real endpoint) | Slack/Teams/PagerDuty/email with on-call rotation; alert on failure, on retry storms, on long runs |
| Freshness | ❌ | Visible on dashboard only | Alert if no successful run by e.g. 06:00 (Airflow 3.1 Deadline Alerts, or a check on `monitoring.pipeline_runs`) |
| Volume / anomaly detection | ❌ | Fixed thresholds (reject rate ≤ 10%) | Alert when row count, revenue or reject rate deviates from the recent average (e.g. ±3 standard deviations) |
| Logs | ⚠️ | Airflow task logs in a Docker volume | Central logs (CloudWatch/ELK/Loki), remote Airflow logging to S3, retention policy |
| Infrastructure metrics | ❌ | `docker stats` | CPU/memory/disk dashboards and alerts (Grafana, cloud monitoring) |
| Lineage | ⚠️ | `batch_id` on every Silver/fact row; versioned outputs | OpenLineage → Marquez/DataHub: which job produced which table from which inputs |
| Data catalog | ❌ | Docs in `docs/` | DataHub / OpenMetadata / AWS Glue Catalog with owners and descriptions |

---

## 6. Data quality and evaluation

"Evaluation" for a data pipeline means: *how do we know the numbers are right, every day?*

| Item | Status | Now | Production |
|---|---|---|---|
| Row-level rules | ✅ | 11 rules, reasons recorded | Keep |
| File-level gate | ✅ | Batch quarantine | Keep |
| Reconciliation | ✅ | Gold = Silver totals; warehouse = Gold | Also reconcile with the **source system** (e.g. daily totals from the shop's finance report) |
| pandas/Spark parity | ✅ | Tests compare every row | Keep in CI |
| Declarative checks | ❌ | Python checks in `quality.py` | Great Expectations / Soda / dbt tests, readable by analysts |
| Data contract with source | ⚠️ | `schema.py` (our side only) | A versioned contract agreed with the source team; schema-change alerts |
| Anomaly detection | ❌ | — | Statistical checks on volume, revenue, reject rate, null rates, new categories |
| Dashboard correctness | ⚠️ | Cards executed via API; filtered result = direct SQL (tested once) | Automated tests of key KPIs after each run; a "metrics layer" with one definition per KPI |
| SLAs / SLOs | ❌ | — | e.g. "data for yesterday available by 06:00 IST on 99% of days", measured and reported |

---

## 7. Reliability and scalability

| Item | Status | Now | Production |
|---|---|---|---|
| Idempotency / safe re-runs | ✅ | Content-addressed batches, versioned outputs, guarded merge | Keep |
| Retries | ✅ | 2 retries with backoff; quality gates fail fast | Keep |
| Concurrency safety | ✅ | 1 active run, 1-slot pool, advisory lock | Keep |
| Backups | ❌ | Docker volumes only | Automated PostgreSQL backups + point-in-time recovery; S3 versioning and cross-region replication; **tested restores** |
| Disaster recovery | ❌ | — | Documented RPO/RTO, runbook, rebuild from Bronze |
| Incremental processing | ❌ | Silver is a full recompute (grows with history) | Apache Iceberg / Delta Lake `MERGE`, process only new batches |
| Late / out-of-order data | ✅ | Latest `updated_at` wins | Keep; add a policy for very late corrections |
| Retention | ❌ | Old Silver/Gold versions pruned; Bronze kept forever | Lifecycle rules (e.g. Bronze to cold storage after 90 days); raw PII deletion |
| High availability | ❌ | Single instance of everything | Multi-AZ database, 2+ schedulers, managed services |

---

## 8. Operations and governance

| Item | Status | Production |
|---|---|---|
| Runbooks | ⚠️ (README, HOW_IT_WORKS) | Per-alert runbook: "if X fails, check Y, do Z" |
| Ownership | ❌ | Named owner per pipeline, table and dashboard |
| Access requests | ❌ | Process for granting data access; periodic reviews |
| Cost monitoring | ❌ | Budgets and alerts on cloud spend (Spark clusters, storage) |
| Change management | ❌ | Pull requests with review; changelog; communicating schema changes to dashboard users |

---

## 9. What is already production-grade

These patterns are what production pipelines use and need no redesign:

* Medallion layers with an untouched Bronze copy (re-processable).
* Idempotent, content-addressed ingestion and guarded upserts.
* Versioned Silver/Gold published by pointer swap (no half-written reads).
* Quality gates at every layer, row and file quarantine.
* Staging + single-transaction merge into the star schema.
* Least-privilege, read-only BI account; parameterised SQL; secrets outside code.
* Run, task, batch and quality monitoring tables.
* Automated tests including integration tests against real services.
