# How it works — plain-language guide and FAQ

A beginner-friendly explanation of the whole application: what it does, which tools it uses,
where the data comes from, how the Airflow DAG works, how to start/stop it, and where to see
the results. Organised as the questions you are likely to have when you open this repo again.

For deeper detail see [ARCHITECTURE.md](ARCHITECTURE.md), [DATA_MODEL.md](DATA_MODEL.md),
[PYSPARK_GUIDE.md](PYSPARK_GUIDE.md) and [INTERVIEW_GUIDE.md](INTERVIEW_GUIDE.md).

---

## 1. What does this application do?

A shop sells things every day. This application, **every day**:

1. **collects** the sales data (a file, an API, or fake data from a generator),
2. **cleans** it (removes bad rows and duplicates, keeps the latest version of each order),
3. **summarises** it (daily/monthly revenue, top products, customers, cities, categories),
4. **stores** it in a warehouse built for reporting (a PostgreSQL star schema),
5. **shows** it on dashboards, and
6. **records** whether each step worked (a monitoring dashboard for the pipeline itself).

> One sentence: Airflow runs a daily job that saves raw sales data in MinIO, cleans and
> summarises it with Spark, loads it into a PostgreSQL star schema, and Metabase shows the
> results and the pipeline's health on dashboards.

All data is **synthetic** (fake but realistic). No real customer data is used.

---

## 2. The flow

```
 Daily sales file / API / fake-data generator
                │
                ▼
   ① INGEST ──► BRONZE  (raw copy, never changed)            [MinIO]
                │
                ▼
   ② CLEAN  ──► SILVER  (valid, de-duplicated rows)          [MinIO, via Spark]
                │       bad rows ──► quarantine, with a reason
                ▼
   ③ SUMMARISE ► GOLD   (daily/monthly totals, rankings)     [MinIO, via Spark]
                │
                ▼
   ④ LOAD   ──► WAREHOUSE (star schema tables)               [PostgreSQL]
                │
                ▼
   ⑤ SHOW   ──► DASHBOARDS (sales + pipeline health)         [Metabase]

   Airflow runs steps ①–⑤ every day, in order, and retries on failure.
   Every step writes "how did it go" records into the monitoring schema.
```

**Bronze / Silver / Gold** is called the *medallion architecture*:

| Layer | Contains | Purpose |
|---|---|---|
| Bronze | Raw CSV exactly as received | Proof of what arrived; you can always re-process from it |
| Silver | Clean, de-duplicated Parquet | The trustworthy version of each order line |
| Gold | Ready-made summaries | Fast answers for reporting |

---

## 2b. Could this be done without Airflow, Spark and a data lake?

**Yes. At this data size (~1,000 rows/day) a plain Python script + PostgreSQL + a scheduler
(cron / Windows Task Scheduler) would produce the same dashboards.** The project even contains
two simpler paths:

| Path | Command | Uses |
|---|---|---|
| Warehouse-only (Phase 1 style) | `python -m src.cli ingest --input <file.csv>` | pandas → PostgreSQL. No lake, no Spark, no Airflow |
| Lake without orchestration | `python -m src.cli lake-run --date YYYY-MM-DD --days N` | Same lake + Spark steps, run in order by a Python loop |
| Full | Airflow DAG | Everything |

What each heavy tool adds, and when it starts to matter:

| Tool | Without it | What it adds | Worth it when |
|---|---|---|---|
| **Airflow** | cron runs a script | Run history UI, retries, task-level restart, backfills, dependencies, alerts, logs per task | Many pipelines/teams, failures must be visible, re-runs of past dates |
| **Spark** | pandas | Processing data larger than one machine's memory, scaling out to a cluster | Tens of GB+ (the benchmark here: pandas was faster at 1M rows on one machine) |
| **Data lake (MinIO/S3)** | Load straight into PostgreSQL | Cheap storage of raw history, re-processing from originals, other consumers (ML, data science) reading the same files | Large or many data sources, unstructured data, several teams |
| **Separate warehouse model** | Query the raw table | Clean, fast, consistent numbers for BI | Almost always, even when small |

**Why build it this way, then?** It is a **portfolio project**: the data is small on purpose, but
the architecture is the one used at companies where data is large, sources are many and failures
are expensive. The design scales without being rewritten (e.g. point Spark at a cluster, MinIO →
AWS S3).

**Interview tip:** say this yourself: *"For this volume a script and cron would work; I used
Airflow, Spark and a lake to demonstrate the production pattern, and my benchmark shows pandas is
faster on a single machine up to at least 1M rows."* Knowing when a tool is **overkill** is a
senior skill.

---

## 3. Which tools are used, and why?

| Tool | Type | Job here | In simple words |
|---|---|---|---|
| **Python 3.12** | Language | All pipeline code in `src/` | The glue that does everything |
| **pandas** | Python data library | Small-data path, file checks, comparison with Spark | Excel, but in code |
| **PySpark** | Distributed data engine | Bronze → Silver → Gold | pandas for big data; can spread over many machines |
| **MinIO** | Object storage (S3-compatible) | The data lake (Bronze/Silver/Gold files) | A local Amazon S3; a big folder for files |
| **Parquet** | File format | How Silver and Gold are stored | Compressed, column-based CSV; fast to read |
| **PostgreSQL 16** | Relational database | The warehouse | The final, organised tables for reporting |
| **Apache Airflow 3** | Workflow scheduler | Runs the daily pipeline | Alarm clock + to-do list: runs steps in order, retries failures |
| **Metabase** | BI / dashboard tool | Sales and monitoring dashboards | Turns SQL into charts |
| **Docker Compose** | Container runner | Starts everything with one command (`compose.yaml`) | Each tool in its own box, started together |
| **pytest** | Test framework | 70 automated tests in `tests/` | Proves the code does what it claims |

---

## 3b. Which Docker images and containers are used?

**4 images** are used by the app (plus 1 optional helper):

| Image | Where it comes from | Size | Used for |
|---|---|---|---|
| `sales-pipeline:latest` | **Built by us** from the `Dockerfile` (base: `apache/airflow:3.1.8-python3.12`, plus Java 17, PySpark 4.1.1, S3/JDBC jars and our Python packages) | ~5.5 GB | Airflow, Spark, the CLI, the mock API, setup jobs |
| `postgres:16` | Docker Hub (official) | ~0.6 GB | The warehouse **and** the metadata database |
| `chainguard/minio@sha256:…` | Chainguard (pinned by digest; the official MinIO images were discontinued) | ~0.2 GB | The data lake |
| `metabase/metabase:v0.64.5.5` | Docker Hub (official) | ~1.9 GB | Dashboards |
| `mcr.microsoft.com/playwright/python:v1.63.0-noble` | Microsoft (optional) | ~3.5 GB | Only `scripts/capture_screenshots.py`; not part of the app |

Why one custom image for many services: Airflow tasks, the Spark driver, Spark workers and the
CLI must all have **identical** Python, Java and Spark versions, otherwise Spark breaks.

**Containers** (defined in `compose.yaml`):

| Container (service) | Image | Runs | Port (localhost only) | Purpose |
|---|---|---|---|---|
| `postgres` | postgres:16 | always | 5434 | **Sales warehouse** (`raw`, `staging`, `mart`, `monitoring`, `legacy`) |
| `meta-postgres` | postgres:16 | always | – | Airflow's and Metabase's **own** databases (run history, users, dashboards) |
| `minio` | chainguard/minio | always | 9000 (API), 9001 (UI) | **Data lake**: Bronze / Silver / Gold buckets |
| `init` | sales-pipeline | once, then exits | – | Runs migrations (creates tables, read-only user) and creates the buckets |
| `airflow-init` | sales-pipeline | once, then exits | – | Prepares Airflow's database, creates the admin user and the `pipeline_writes` pool |
| `airflow-apiserver` | sales-pipeline | always | 8080 | Airflow **web UI** and API |
| `airflow-scheduler` | sales-pipeline | always | – | Starts the daily runs and **executes the tasks** (including Spark in `local[*]` mode) |
| `airflow-dag-processor` | sales-pipeline | always | – | Reads and parses the DAG file |
| `metabase` | metabase | always | 3000 | **Dashboards** |
| `mock-api` | sales-pipeline | always | 8000 | Fake REST API with sales data, to test the API source |
| `app` | sales-pipeline | on demand (`--profile tools`) | – | Runs CLI commands: `docker compose run --rm app python -m src.cli …` |
| `spark-master`, `spark-worker` | sales-pipeline | optional (`--profile spark-cluster`) | 8081 | A real Spark cluster, to show scale-out; normally Spark runs inside the task |

`init` and `airflow-init` showing **Exited (0)** is normal: exit code 0 means they finished
successfully.

**Volumes** (where container data survives restarts; removed only by `docker compose down -v`):

| Volume | Holds |
|---|---|
| `postgres_data` | The sales warehouse |
| `meta_postgres_data` | Airflow history + Metabase setup and dashboards |
| `minio_data` | All lake files |
| `airflow_logs` | Task logs shown in the Airflow UI |

Your project folder is also mounted into the custom containers at `/opt/project`, so code and
`data/` changes are visible inside them immediately.

---

## 4. Where does the data live?

### On disk: the `data/` folder (where data starts)

| Folder | Contents | Comes from |
|---|---|---|
| `data/generated/daily/` | **Main source.** One CSV per day: `sales_<YYYY-MM-DD>.csv` (~1,000 rows) | Written by the generator during each run |
| `data/incoming/` | Empty by default | **Drop folder for your own real files** (see section 8) |
| `data/sample/` | `sales_sample.csv` (example of the format), `legacy_phase1_sales.csv` (original starter data) | Shipped with the project |
| `data/generated/benchmark/` | 10k / 100k / 1M-row files | Only for the pandas-vs-Spark benchmark |
| `data/rejected/` | Bad rows with the reason they were rejected | Written by validation |
| `data/processed/` | Empty | Left over from the original starter |

`data/generated/`, `data/rejected/` and `data/incoming/*` are git-ignored (they are recreated).

### In MinIO (the data lake, http://localhost:9001)

```
bronze/sales/ingestion_date=2026-01-01/batch_id=b…/sales.csv  (+ _metadata.json)
bronze/quarantine/…            ← files that failed validation
silver/sales/version=…/        ← Parquet, partitioned by month; _CURRENT.json points to the live version
gold/<dataset>/version=…/      ← 6 summary datasets
```

### In PostgreSQL (the warehouse, `localhost:5434`, database `sales_dw`)

| Schema | Contains | Purpose |
|---|---|---|
| `raw` | Every row ever received | Audit trail |
| `staging` | Temporary landing tables | Safe area before the final merge |
| `mart` | **Star schema** + Gold `agg_*` tables | What the dashboards query |
| `monitoring` | Pipeline runs, task runs, batches, quality results | Pipeline health dashboard |
| `legacy` | The original starter's tables | Kept so the old version still works |

**Why separate schemas?** A schema is a folder inside the database. Splitting by *stage* and
*purpose* gives:

* **Clear data flow**: `raw` → `staging` → `mart`, the same idea as Bronze → Silver → Gold.
* **Security**: permissions are granted per schema. `metabase_reader` can read only `mart` and
  `monitoring`, never `raw` or `staging`.
* **Safe loading**: new data lands in `staging` first; the dashboards (reading `mart`) never see
  a half-loaded table, because the merge into `mart` is one transaction.
* **Same table names, no clash**: `legacy.fact_sales` (old starter) and `mart.fact_sales` (new)
  live side by side.
* **Easy clean-up**: `staging` can be emptied anytime; `raw` is never edited (audit trail).

`public.schema_migrations` records which SQL migrations (V001–V006) have been applied, so
`init` never runs one twice.

Where to look in pgAdmin: start with `mart` (the business data), then `monitoring` (pipeline
health). `raw` and `staging` are for debugging.

**Star schema**: one central table of facts (`fact_sales`, one row per order line) and four
tables that describe them:

```
             dim_customer
                  │
 dim_product ── fact_sales ── dim_date
                  │
             dim_location
```

---

## 4b. How is the data transformed? (input columns → output)

Code: rules in [`src/validation/schema.py`](../src/validation/schema.py), Spark logic in
[`src/transformations/spark_jobs.py`](../src/transformations/spark_jobs.py), warehouse merge in
[`sql/warehouse/merge_mart.sql`](../sql/warehouse/merge_mart.sql).

```
INPUT CSV (20 text columns, messy)
   │ ① clean text  ② fill defaults  ③ convert types  ④ 11 checks  ⑤ compute amounts
   ▼
valid rows ─────────────► ⑥ de-duplicate (latest version wins) ─► SILVER (25 columns)
bad rows ──► quarantine (raw values + reject_reasons)                     │
                                                    ┌─────────────────────┴───────────┐
                                                    ▼                                 ▼
                                          ⑦ GOLD: 6 summary tables        ⑧ STAR SCHEMA: fact + 4 dims
                                                    └──────────► mart.* ◄─────────────┘ ─► dashboards
```

### Stage 0: input (Bronze), 20 columns, all read as text

`order_id, order_item_id, order_date, customer_id, customer_name, customer_email, product_id,
product_name, category, quantity, unit_price, discount, total_amount, currency, payment_method,
order_status, city, state, country, updated_at`

One row = **one line of an order** (an order with 2 products = 2 rows, same `order_id`).
`updated_at` says when this version of the row was produced: the same `order_item_id` can come
again later with a new status.

Example rows:

```
ORD-20260101-000001, ORD-20260101-000001-01, 2026-01-01, C000637, Joseph Pillai, …, P009, Smartphone X, Mobiles, 1, 29999.00, 0.00, 29999.00, INR, cash_on_delivery, completed, Coimbatore, Tamil Nadu, India, 2026-01-01T10:39:58Z
ORD-20260101-000001, ORD-20260101-000001-02, 2026-01-01, C000637, Joseph Pillai, …, P003, Wireless Mouse, Accessories, 1, 750.00, 0.10, 675.00, INR, …
```

The generator deliberately adds dirty rows (duplicates, missing customer id, invalid quantity,
negative price, wrong total, invalid date, invalid status, missing email, messy formatting such
as upper-case status and extra spaces) to test the cleaning.

### Stage 1: Bronze → Silver (Spark, `transform_silver`)

| Step | What happens | Example |
|---|---|---|
| ① Clean text | Trim spaces; `order_status`, `payment_method`, `customer_email` → lower case; `currency` → upper case | `" Completed "` → `completed`, `inr` → `INR` |
| ② Defaults for blank optional fields | `category`→`Uncategorized`, `discount`→`0`, `currency`→`INR`, `payment_method`→`unknown`, `city/state/country`→`Unknown` | blank city → `Unknown` |
| ③ Convert types (safely) | Dates `yyyy-MM-dd` or `yyyy/MM/dd` → DATE; `updated_at` → TIMESTAMP; quantity → INT; money → DECIMAL. Uses `try_cast`, so a bad value becomes NULL instead of crashing the job | `2026/01/05` → `2026-01-05` |
| ④ 11 checks | See below; a row can fail several | |
| ⑤ Compute amounts | `gross = quantity × unit_price`; `net = ROUND(quantity × unit_price × (1 − discount), 2)`; `discount_amount = gross − net` | mouse: gross 750.00, net 675.00, discount 75.00 |
| Split | Rows with no failed check → valid; others → quarantine with `reject_reasons` | |
| ⑥ De-duplicate | Per `order_item_id`, keep the newest version: highest `updated_at`, then latest `ingestion_date`, `batch_id`, `source_row_hash` | pending (Jan 1) + completed (Jan 3) → keep **completed** |
| Write | Parquet, partitioned by `order_month` | `silver/sales/version=…/order_month=2026-01/` |

**The 11 checks (reject reasons), in order:**

| # | Reason | Rejects when |
|---|---|---|
| 1 | `missing_required_field` | Any of order_id, order_item_id, order_date, customer_id, customer_name, product_id, product_name, quantity, unit_price, total_amount, order_status, updated_at is blank |
| 2 | `invalid_order_date` | Not a real date |
| 3 | `invalid_updated_at` | Not a real timestamp |
| 4 | `updated_before_order_date` | Updated before the order existed |
| 5 | `invalid_quantity` | Not a whole number, or not 1–10,000 |
| 6 | `invalid_unit_price` | Not money, or negative |
| 7 | `invalid_discount` | Not a number, or above 0.9 (90%) |
| 8 | `invalid_total_amount` | Not money, or negative |
| 9 | `total_amount_mismatch` | Source `total_amount` differs from the recomputed `net` by more than 1 paisa |
| 10 | `invalid_order_status` | Not `completed` / `pending` / `cancelled` |
| 11 | `unsupported_currency` | Not `INR` |

A rejected row keeps its **original raw values** plus e.g.
`reject_reasons = "invalid_quantity;total_amount_mismatch"`. In the lake/Airflow path these go to
MinIO `silver/sales_rejected/` (Parquet); in the warehouse-only `ingest` path they go to
`staging.sales_rejected` and `data/rejected/`. Also: an invalid email is set to NULL (row kept); an unknown payment method
becomes `unknown` (row kept).

**Silver output: 25 typed columns.** The 19 business columns (with `discount` as a rate, and
`total_amount` *dropped*: it was only used for checking, because the recomputed `net_amount` is
trusted instead), plus:

| Added column | Meaning |
|---|---|
| `gross_amount`, `discount_amount`, `net_amount` | Computed money (DECIMAL, rounded half-up) |
| `batch_id` | Which Bronze file the row came from (lineage) |
| `ingestion_date` | When that file arrived |
| `source_row_hash` | Fingerprint of the raw row (dedup tie-break and change detection) |

### Stage 2: Silver → Gold (Spark, `transform_gold`), 6 summary tables

Revenue always means **`SUM(net_amount)` of completed rows**. Cancelled and pending orders are
counted as orders but add no revenue.

| Gold table (→ `mart.agg_*`) | One row per | Main columns |
|---|---|---|
| `daily_sales` | day | total/completed/cancelled/pending orders, units sold, gross revenue, discount, **net revenue**, avg order value, **7-day moving average**, cumulative revenue |
| `monthly_sales` | month | orders, net revenue, avg order value, previous month revenue, **growth %**, cancelled % |
| `category_sales` | category | completed orders, units, net revenue, **share %**, rank |
| `product_sales` | product | name and category (latest), orders, units, revenue, overall rank, rank within category |
| `customer_sales` | customer | name (latest), total/completed orders, **lifetime revenue**, first and last order date, rank |
| `location_sales` | city/state/country | completed orders, distinct customers, net revenue, rank |

Before publishing, Gold totals are reconciled against Silver (e.g. sum of daily revenue = sum of
Silver revenue), otherwise the run fails.

### Stage 3: Silver → star schema (`load_data_warehouse`)

Silver is copied to `staging.sales_clean`, then one SQL transaction (`merge_mart.sql`) splits
each wide row into the star schema:

| Table | Columns | Built from |
|---|---|---|
| `dim_date` | `date_key` (20260101), full_date, year, quarter, month, month_name, year_month, day, day_of_week, day_name, is_weekend | Pre-filled calendar 2020–2035 |
| `dim_customer` | `customer_key` (surrogate), customer_id, customer_name, customer_email | Latest version per customer (SCD Type 1 = overwrite) |
| `dim_product` | `product_key`, product_id, product_name, category | Latest version per product (SCD 1) |
| `dim_location` | `location_key`, city, state, country | One row per distinct place |
| **`fact_sales`** | `order_item_id` (unique), order_id, **date_key, customer_key, product_key, location_key**, payment_method, order_status, currency, quantity, unit_price, discount_rate, gross_amount, discount_amount, net_amount, source_updated_at, batch_id | One row per order line |

Text like "Joseph Pillai / Coimbatore" is stored **once** in a dimension; the fact row holds only
the numeric keys plus the numbers. The database itself enforces the maths (a `CHECK` constraint
requires `net_amount = ROUND(quantity × unit_price × (1 − discount_rate), 2)`).

Merge rules: insert new lines; update a line only if the incoming `updated_at` is newer **and**
something actually changed; never let an older version overwrite a newer one. The 6 Gold tables
replace `mart.agg_*` in the same transaction.

### Stage 4: final output

* `mart.fact_sales` + 4 dims: detailed, queryable sales history.
* `mart.agg_*`: ready-made summaries.
* `mart.v_sales`: fact joined with all dimensions (one wide, readable row);
  `mart.v_completed_sales`: same, only completed.
* `monitoring.*`: counts of rows read, rejected, duplicates and each quality check result.
* **Metabase** queries these tables to draw the dashboards.

### Rejected vs quarantine

* **Rejected** = the *decision*: this data failed validation and will not be used.
* **Quarantine** = the *place*: where failed data is kept, isolated but **not deleted**, so it
  can be inspected, fixed and re-processed. (Like a hospital isolation ward.)

The project has quarantine at **two levels**:

| | Row-level (rejected rows) | File-level (quarantined batch) |
|---|---|---|
| What fails | Individual rows break one of the 11 rules | The **whole file** is unusable |
| Triggers | Blank customer, negative price, wrong total, … | Wrong/missing columns, checksum mismatch, empty file, or more than `MAX_REJECT_RATE` (10%) of its rows rejected |
| Detected in | `transform_silver` (Spark) | `validate_bronze` |
| Kept in | MinIO `silver/sales_rejected/` (lake path); `staging.sales_rejected` + `data/rejected/` (warehouse-only `ingest`) | MinIO `bronze/quarantine/…`, status `quarantined` + reason in `monitoring.ingestion_batches` |
| Pipeline | **Continues**: good rows from the same file are loaded | **Run fails**; the file is excluded from all later Silver rebuilds |
| Analogy | Throw out the few rotten apples, keep the box | The whole box is rotten: send it back |

Why both: a few bad rows are normal and shouldn't stop the business; a broken file signals a
source problem a human must look at, and loading its partial data would give misleading numbers.
Example in the demo data: the 2026-03-08 file was deliberately broken and is in `bronze/quarantine/`.

To fix a quarantined file: correct it, put it in `data/incoming/sales_<date>.csv`, and re-run that
date (it gets a new `batch_id` because its content changed).

---

## 4c. What are fact and dimension tables, and why use them?

This design is called **dimensional modelling** (Ralph Kimball, 1990s), the standard way to
organise a reporting warehouse.

* **Fact table** = *what happened*, the **measurable events**. Each row is one event (here: one
  order line) with **numbers you add up**: quantity, gross/discount/net amount. Named "fact"
  because each row records a fact: *"this item was sold, for this much"*.
* **Dimension table** = *the context*: **who, what, when, where**. Descriptive text you
  **filter and group by**: customer name, product category, month, city. Named "dimension"
  because these are the directions you can slice the numbers along ("revenue *by month*, *by
  category*, *by city*").

| Question part | Type | Table |
|---|---|---|
| "How much revenue / how many units" | measure | `fact_sales` (net_amount, quantity) |
| "…per month / on weekends" | when | `dim_date` |
| "…per category / product" | what | `dim_product` |
| "…per customer" | who | `dim_customer` |
| "…per city / state" | where | `dim_location` |

**Why "star schema"?** Draw it: the fact table in the middle, each dimension connected to it by
a key. It looks like a star.

```
              dim_customer (who)
                     │
 dim_product ─── fact_sales ─── dim_date (when)
   (what)            │
              dim_location (where)
```

**Why not keep one big table (like Silver)?**

| Problem with one wide table | How the star schema fixes it |
|---|---|
| Same text repeated on every row ("Joseph Pillai, Coimbatore, Tamil Nadu…" × every purchase) | Stored once in a dimension; facts hold a small number key |
| A customer changes name / a product changes category → thousands of rows to update, and it is easy to miss some | Change one row in the dimension |
| Every report must re-derive "month", "quarter", "weekend" from a date | `dim_date` has them ready (`month_name`, `quarter`, `is_weekend`) |
| Unclear which columns are numbers to sum vs labels to group by | Facts = numbers, dims = labels; BI tools (Metabase) map this directly to filters and charts |

**Why not the fully normalised design of an app database (many small tables)?** Apps
(OLTP) optimise for writing one order at a time; reports (OLAP) read millions of rows. A star
schema keeps every query to *one fact + a few direct joins*: simple to write, fast to run.

**Keys:** each dimension has a **surrogate key** (`customer_key`, a warehouse-generated number)
next to the **natural key** from the source (`customer_id = C000637`). The fact table stores the
surrogate keys. This keeps joins fast and makes history tracking (SCD Type 2) possible later.

**SCD (Slowly Changing Dimension):** how changes to dimension data are handled. Here **Type 1**:
overwrite with the latest value (no history). Type 2 would keep old versions with valid-from/to
dates (design in [DATA_MODEL.md](DATA_MODEL.md)).

**Grain:** the most important decision: what *one fact row* means. Here: **one order line**
(`order_item_id`). Finer grain = more flexible questions; all summaries (`agg_*`) are built from it.

Typical query: numbers from the fact, labels from dimensions:

```sql
SELECT d.year_month, p.category, SUM(f.net_amount) AS revenue
FROM mart.fact_sales f
JOIN mart.dim_date    d ON d.date_key    = f.date_key
JOIN mart.dim_product p ON p.product_key = f.product_key
WHERE f.order_status = 'completed'
GROUP BY d.year_month, p.category
ORDER BY d.year_month, revenue DESC;
```

---

## 5. The Airflow DAG

**DAG** (Directed Acyclic Graph) = Airflow's word for a pipeline: tasks plus the order they
run in, with no loops.

### Is there only one DAG?

**Yes**: `sales_data_pipeline` in [`airflow/dags/sales_data_pipeline.py`](../airflow/dags/sales_data_pipeline.py).
The job is one straight chain where every step depends on the previous one, so one DAG is the
right size. One-off jobs are deliberately *not* DAGs:

| Job | Command |
|---|---|
| Create tables (migrations) | `python -m src.cli init` (the `init` container does this) |
| Build Metabase dashboards | `python -m src.cli provision-metabase` |
| Pandas vs Spark benchmark | `python -m src.cli benchmark` |
| End-to-end check | `python scripts/verify_pipeline.py` |

Possible future DAGs: weekly `lake_maintenance` (clean old versions), a separate data-quality
DAG, a manual `provision_metabase` DAG, or splitting ingest and processing linked by an
Airflow **Asset** (event-driven).

### Three layers of code

```
 airflow/dags/sales_data_pipeline.py       ← LAYER 1: the DAG ("what runs, in what order")
        │  each task calls ONE function with the same name
        ▼
 src/orchestration/lake_pipeline.py        ← LAYER 2: the steps ("what each task does")
        │
        ▼
 src/ingestion/  src/validation/  src/storage/      ← LAYER 3: the real work
 src/transformations/  src/warehouse/  src/monitoring/
```

The DAG file has no business logic. That means the same steps also run **without Airflow**
(`python -m src.cli lake-run`) and can be tested on their own.

### How Airflow finds the code (in `compose.yaml`)

| Setting | Meaning |
|---|---|
| Volume `./:/opt/project` | Your project folder is mounted into the containers; edits apply without rebuilding |
| `AIRFLOW__CORE__DAGS_FOLDER=/opt/project/airflow/dags` | Where Airflow looks for DAGs |
| `PYTHONPATH=/opt/project` | Lets the DAG do `from src... import ...` |

Airflow containers: `airflow-dag-processor` (parses the DAG file), `airflow-scheduler`
(starts runs and tasks), `airflow-apiserver` (web UI), `meta-postgres` (Airflow's own
history database, separate from the sales warehouse), `airflow-init` (one-time setup).

### The 10 tasks

| # | Task | What it does | Main code | Reads → writes |
|---|---|---|---|---|
| 1 | `check_source` | Picks the source, starts a run record, checks DB and MinIO are up | `lake_pipeline.check_source`, `monitoring/metrics.py` | → `monitoring.pipeline_runs` |
| 2 | `generate_or_fetch_sales` | Gets the day's data | `ingestion/generator.py` or `ingestion/api_source.py` | → `data/generated/daily/sales_<date>.csv` |
| 3 | `upload_bronze` | Hashes the file, skips it if already seen, uploads it, saves every row | `storage/object_store.py`, `ingestion/csv_ingest.py`, `warehouse/loader.py` | → MinIO `bronze/`, `raw.sales_records`, `monitoring.ingestion_batches` |
| 4 | `validate_bronze` | File-level checks; a bad file goes to `bronze/quarantine/` | `ingestion/csv_ingest.py`, `validation/quality.py` | → `monitoring.data_quality_results` |
| 5 | `transform_silver` | **Spark**: reads all Bronze, applies 11 rules, de-duplicates, rejects bad rows | `transformations/spark_jobs.py`, `validation/schema.py` | `bronze/` → `silver/` |
| 6 | `validate_silver` | Reject rate and duplicate checks, then publishes the new Silver version | `validation/quality.py` | updates `silver/_CURRENT.json` |
| 7 | `transform_gold` | **Spark**: daily/monthly, product, customer, category, location summaries | `transformations/spark_jobs.py` | `silver/` → `gold/` |
| 8 | `load_data_warehouse` | Spark writes staging tables, then one SQL transaction merges into the star schema | `warehouse/loader.py`, `sql/warehouse/merge_mart.sql` | → `staging.*` → `mart.*` |
| 9 | `verify_warehouse` | SQL checks on the final tables (e.g. fact totals = Gold totals) | `validation/quality.py` | → `monitoring.data_quality_results` |
| 10 | `record_pipeline_metrics` | Marks the run successful, saves counts | `monitoring/metrics.py` | → `monitoring.pipeline_runs` |

### How the tasks connect

At the bottom of the DAG file:

```python
source    = check_source()
extract   = generate_or_fetch_sales(source)   # passes data along
bronze    = upload_bronze(extract)
bronze_ok = validate_bronze(bronze)
silver    = transform_silver();  bronze_ok >> silver   # ">>" = "run after"
...
```

* **Passing data**: a task returns a small dict (e.g. `{"batch_id": ..., "rows": ...}`);
  Airflow stores it as an **XCom** and gives it to the next task as an argument.
* **Order only (`>>`)**: e.g. `transform_silver` reads Bronze directly from MinIO; it just must
  wait until validation passed.
* Big data never passes through Airflow. It moves through MinIO and PostgreSQL; Airflow only
  passes small summaries and decides what runs when.

### Rules built into the DAG

| Setting | Value | Why |
|---|---|---|
| Schedule | Daily at 00:00 UTC (05:30 India time), processing the **previous** day | The day is complete before it is processed |
| Retries | 2, with growing delay (1–10 min); quality-check tasks: 0 | Retrying helps with outages, not bad data |
| `max_active_runs=1` + `pipeline_writes` pool | One run, one writer at a time | No two runs write together |
| `execution_timeout` | 30 min per task | A stuck task fails instead of hanging |
| `on_failure_callback` | `notify_failure` | Marks the run failed; optional webhook alert |
| Params | `source_mode`, `business_date` | Shown as a form when you click **Trigger** |

If a task fails, all later tasks show `upstream_failed` and are skipped.

---

## 6. What happens every day? (the whole working)

**Yes, the DAG processes one file per day: the previous day's.**

```
00:00 UTC each day
   ① take ONE file: yesterday's (sales_<yesterday>.csv)
   ② save it in Bronze (MinIO) + raw rows in PostgreSQL
   ③ rebuild Silver and Gold
   ④ merge into the warehouse
   ⑤ dashboards show the new numbers
```

Three details that are easy to miss:

**A. One new file in, but all history is rebuilt.** `transform_silver` reads *all* Bronze
files (day 1 → yesterday) and rebuilds Silver and Gold, because a new file can contain changes
to older orders (pending → completed, completed → cancelled). Downside: each run gets a little
slower as history grows (production fix: Iceberg/Delta incremental tables).

**B. The warehouse is updated, not just appended.**

| Row type | What happens |
|---|---|
| New order line | **Inserted** |
| Existing order line that changed (e.g. cancelled) | **Updated in place**, so revenue drops |
| Existing order line, unchanged | Not touched |
| New customer / product | Added to the `dim_*` table |

The Gold summary tables (`mart.agg_*`) are replaced with the new version.

**C. Dashboards are not "added to"; they re-query.** Each card is a SQL query that Metabase runs
every time the page is opened or refreshed. Same cards, new values: revenue goes up, the trend
gets one more point, rankings may change. The monitoring dashboard gets one more run. You do
**not** re-run `provision-metabase`; that is only for changing the dashboard design.

**If something fails**, nothing is half-written: the warehouse keeps the previous day's data,
the sales dashboard shows the old numbers, and the monitoring dashboard shows the failed run.

**Re-running is safe (idempotent).** Running the same day again changes 0 rows: the batch is
recognised by its hash, and the merge only changes rows that really changed.

---

## 7. Where does each day's data come from? (the generator fallback)

Logic in `check_source` ([`src/orchestration/lake_pipeline.py`](../src/orchestration/lake_pipeline.py)),
in the default `auto` mode:

```
Is there data/incoming/sales_<yesterday>.csv (or .jsonl)?
   YES → use that file
   NO  → Is SALES_API_URL set in .env?
            YES → fetch from the API
            NO  → run the generator   ← default setup (SALES_API_URL is empty)
```

So **yes: if yesterday's data is not there, the generator creates it** in
`data/generated/daily/sales_<yesterday>.csv`, and the rest of the run processes it.

* The generator is **deterministic**: the same date always gives the same file, so retries
  never duplicate data.
* Nothing watches folders in the background. The **DAG run itself** fetches or generates the
  data at run time.
* You can force a source when triggering manually with `source_mode`:
  * `generate`: always fake data
  * `file`: **fail** if `data/incoming/sales_<date>.csv` is missing (use this for real data,
    so a missing file is an error instead of quietly replaced with fake data)
  * `api`: always call the API

---

## 8. How do I use my own data?

1. Use the same columns as [`data/sample/sales_sample.csv`](../data/sample/sales_sample.csv).
2. Name the file after the business day: **`sales_YYYY-MM-DD.csv`**, e.g. `sales_2026-11-08.csv`.
   The name **must** match the date, or it is ignored (and fake data is generated instead).
3. Put it in `data/incoming/` **before** that day's run.
4. Either wait for the scheduled run, or in the Airflow UI click **Trigger** and set the
   logical date (or the `business_date` param) to that day.

For a real deployment, change the scheduled default from `auto` to `file` in the DAG, or set
`SALES_API_URL` to a real API.

---

## 9. Does it run automatically? Only when everything is up?

* The DAG is **unpaused**, so it runs every day at 00:00 UTC **by itself**.
* It only runs if the PC is on, **Docker Desktop is running**, and the containers are up.

| Situation | What happens |
|---|---|
| PC on, Docker running, containers up | Run starts at 00:00 UTC automatically |
| PC off/asleep or Docker closed at 00:00 UTC | Nothing runs at that moment |
| You start it again later | Airflow runs the **latest** missed day once (`catchup=False`); older missed days are skipped, so backfill them if needed |

Containers have `restart: unless-stopped`: after a reboot they come back by themselves when
Docker Desktop starts, unless you stopped them yourself.

### What has already run (demo data in this repo)

69 days, 2026-01-01 to 2026-03-10: January and February with the CLI (`lake-run`), March
with Airflow (manual runs, backfills, a retry test, and one deliberately broken file for
2026-03-08 that was quarantined). Plus the automatic scheduled runs after the DAG was unpaused.
See [VERIFICATION_REPORT.md](VERIFICATION_REPORT.md).

---

## 10. How do I start and stop the application?

Run from the project folder (Docker Desktop must be open first):

| Action | Command | Data kept? |
|---|---|---|
| **Start everything** | `docker compose up -d` | yes |
| **Pause** (stop containers) | `docker compose stop` | yes |
| **Resume** | `docker compose start` | yes |
| **Shut down** (remove containers) | `docker compose down` | yes (data is in volumes) |
| **Status** | `docker compose ps` | – |
| ⚠️ **Wipe everything** | `docker compose down -v` | **NO.** Deletes warehouse, lake, Airflow and Metabase data |

Wait 1–2 minutes after starting for Airflow and Metabase to become healthy.

**Stop only the daily schedule** (keep the app running): toggle **pause** on the DAG in the
Airflow UI, or:

```
docker compose exec airflow-scheduler airflow dags pause sales_data_pipeline
docker compose exec airflow-scheduler airflow dags unpause sales_data_pipeline
```

**Other useful commands**

```
# run one day without Airflow
docker compose run --rm app python -m src.cli lake-run --days 1

# backfill a date range through Airflow (one run at a time)
docker compose exec airflow-scheduler airflow backfill create --dag-id sales_data_pipeline --from-date 2026-03-01 --to-date 2026-03-07 --max-active-runs 1

# print a warehouse summary
docker compose run --rm app python -m src.cli report

# rebuild the Metabase dashboards
docker compose run --rm app python -m src.cli provision-metabase

# run the tests
docker compose run --rm app python -m pytest -q
```

---

## 11. Where can I see the dashboards and other UIs?

| What | URL | Login (values are in `.env`) |
|---|---|---|
| **Sales dashboard** | http://localhost:3000/dashboard/2 | `METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD` |
| **Pipeline monitoring dashboard** | http://localhost:3000/dashboard/3 | same |
| Airflow (pipeline runs, logs) | http://localhost:8080 | `admin` / `AIRFLOW_ADMIN_PASSWORD` |
| MinIO (lake files) | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Warehouse (SQL client) | `localhost:5434`, db `sales_dw` | `POSTGRES_USER` / `POSTGRES_PASSWORD` |

Both dashboards are also in the **Sales Analytics** collection on Metabase's home page.
Screenshots are in [`docs/images/`](images/). Dashboard ids can differ on a fresh install;
`provision-metabase` prints the real URLs.

* **Sales Analytics**: revenue, orders, average order value, customers, cancelled %,
  month-to-date growth, monthly/daily trends, top products, categories, locations, customer
  ranking. Filters at the top (Start date, End date, Category) apply to every card.
* **Pipeline Monitoring**: success rate, latest run, durations, records processed,
  invalid %, duplicates, failed quality checks.

---

## 12. Did we build the dashboard UI ourselves?

**No.** The charts, filters and page layout are drawn by **Metabase**, an open-source BI tool
running in its own container. No front-end code was written. Using a BI tool is the normal
approach in data teams.

What *was* built is **the dashboards as code**:

| Part | Where | What it does |
|---|---|---|
| Dashboard definitions | [`src/analytics/dashboards.py`](../src/analytics/dashboards.py) | The 21 cards: SQL, chart type, size, filters |
| Provisioning | [`src/analytics/metabase_provision.py`](../src/analytics/metabase_provision.py) | Sets up Metabase through its REST API; safe to re-run |
| Read-only access | `metabase_reader` role (`src/warehouse/migrate.py`) | Metabase can only read `mart` and `monitoring` |
| The data behind it | `mart` star schema, `agg_*` tables, `monitoring` tables | What the cards query |

If Metabase is wiped, `provision-metabase` rebuilds both dashboards exactly.

**How to describe it** (CV / interview): *"Built the data platform and defined the BI
dashboards as code, provisioned into Metabase via its REST API, with a least-privilege
read-only connection."* Don't say "built a dashboard UI".

A custom front end (e.g. Streamlit, or FastAPI + charts) could be added as an extra container
using the same read-only user, if you want to show UI work.

---

## 13. Safety and quality features

* **Idempotent**: re-running a day gives the same result, with no duplicates.
* **Bad data doesn't stop the pipeline**: bad rows and bad files are quarantined with reasons.
* **11 validation rules** in one place ([`src/validation/schema.py`](../src/validation/schema.py)),
  implemented identically in pandas and Spark (tested for equality).
* **Least privilege**: Metabase's account can only read; it can't delete or see `raw`.
* **No passwords in code**: secrets live in `.env` (git-ignored). [`.env.example`](../.env.example)
  shows the format, and `python scripts/setup_env.py` generates a `.env` with random secrets.
* **Local-only ports**: every service listens on `127.0.0.1`.

---

## 14. Folder map (where is the code?)

```
sales-data-engineering-starter/
├── airflow/dags/sales_data_pipeline.py   ← the one DAG
├── src/
│   ├── orchestration/lake_pipeline.py    ← one function per DAG task
│   ├── ingestion/        generator.py, api_source.py, csv_ingest.py, mock_api.py
│   ├── storage/          object_store.py        (MinIO upload/download)
│   ├── validation/       schema.py (11 rules), pandas_rules.py, quality.py
│   ├── transformations/  spark_jobs.py, spark_session.py, pandas_transforms.py
│   ├── warehouse/        db.py, migrate.py, loader.py
│   ├── monitoring/       metrics.py             (writes the monitoring tables)
│   ├── analytics/        dashboards.py, metabase_provision.py
│   ├── benchmarks/       pandas_vs_spark.py
│   ├── config/           settings.py            (reads .env)
│   └── cli.py                                   (run without Airflow)
├── sql/migrations/       V001–V006              (creates all tables)
├── sql/warehouse/merge_mart.sql                 (staging → star schema)
├── sql/analytics/        example KPI queries
├── spark/                standalone Spark entry points
├── scripts/              setup, run, verify, screenshots
├── tests/                unit + integration tests
├── data/                 CSV files (section 4)
├── docs/                 documentation (you are here)
├── Dockerfile            one image for Airflow, Spark and the CLI
└── compose.yaml          starts everything
```

**Reading order for the code:** `airflow/dags/sales_data_pipeline.py` →
`src/orchestration/lake_pipeline.py` → whichever module a step uses.

---

## 14b. Running it on AWS

The same stack runs on one EC2 server with Amazon S3 as the lake (no MinIO, no AWS keys: an
IAM role is used). Switch with `LAKE_STORAGE=aws` plus `compose.aws.yaml`; both are set up by
`python3 scripts/setup_env.py --aws --region ap-south-1 --bucket-prefix <prefix>`.
Step-by-step guide, costs and clean-up: [DEPLOY_AWS.md](DEPLOY_AWS.md).

## 15. Git notes

* The repo is ready to push: `.env`, generated data and caches are git-ignored, and
  `.gitattributes` keeps Linux line endings for files that run inside containers (needed
  on Windows).
* Suggested repository name: **`sales-data-pipeline`**.
* Never commit `.env`. Anyone cloning the repo runs `python scripts/setup_env.py` (or copies
  `.env.example`) to create their own.
