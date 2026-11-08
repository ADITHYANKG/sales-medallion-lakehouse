-- V004: pipeline execution metadata and data-quality results.

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    pipeline          TEXT        NOT NULL,          -- 'lake' (Airflow/Spark) or 'warehouse' (pandas)
    logical_date      DATE,
    status            TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    duration_seconds  NUMERIC(12,3),
    source_records    BIGINT,
    valid_records     BIGINT,
    rejected_records  BIGINT,
    duplicate_records BIGINT,
    output_records    BIGINT,
    warehouse_records BIGINT,
    error_message     TEXT,
    details           JSONB       NOT NULL DEFAULT '{}'::JSONB
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON monitoring.pipeline_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS monitoring.task_runs (
    run_id           TEXT        NOT NULL,
    task_name        TEXT        NOT NULL,
    try_number       INTEGER     NOT NULL DEFAULT 1,
    status           TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    duration_seconds NUMERIC(12,3),
    metrics          JSONB       NOT NULL DEFAULT '{}'::JSONB,
    error_message    TEXT,
    PRIMARY KEY (run_id, task_name, try_number)
);

-- Registry of every received source batch (content-addressed => idempotent).
CREATE TABLE IF NOT EXISTS monitoring.ingestion_batches (
    batch_id       TEXT PRIMARY KEY,
    checksum       TEXT        NOT NULL UNIQUE,
    source         TEXT        NOT NULL,
    source_uri     TEXT,
    object_key     TEXT,
    file_bytes     BIGINT,
    record_count   BIGINT,
    ingestion_date DATE        NOT NULL,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    times_received INTEGER     NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS monitoring.data_quality_results (
    result_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id     TEXT        NOT NULL,
    layer      TEXT        NOT NULL,   -- bronze / silver / gold / warehouse
    check_name TEXT        NOT NULL,
    passed     BOOLEAN     NOT NULL,
    severity   TEXT        NOT NULL DEFAULT 'error' CHECK (severity IN ('error', 'warn')),
    observed   NUMERIC,
    expected   TEXT,
    details    TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dq_results_run ON monitoring.data_quality_results (run_id);
