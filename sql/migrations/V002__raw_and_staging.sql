-- V002: raw landing + staging tables.

-- Every source row exactly as received (all TEXT), keyed by batch + line number.
-- Re-ingesting the same batch is a no-op (ON CONFLICT DO NOTHING).
CREATE TABLE IF NOT EXISTS raw.sales_records (
    batch_id        TEXT        NOT NULL,
    source_row      INTEGER     NOT NULL,
    order_id        TEXT,
    order_item_id   TEXT,
    order_date      TEXT,
    customer_id     TEXT,
    customer_name   TEXT,
    customer_email  TEXT,
    product_id      TEXT,
    product_name    TEXT,
    category        TEXT,
    quantity        TEXT,
    unit_price      TEXT,
    discount        TEXT,
    total_amount    TEXT,
    currency        TEXT,
    payment_method  TEXT,
    order_status    TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    updated_at      TEXT,
    source_row_hash TEXT        NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, source_row)
);
CREATE INDEX IF NOT EXISTS idx_raw_sales_records_item ON raw.sales_records (order_item_id);

-- Validated, typed, de-duplicated snapshot that feeds the mart merge.
-- Filled by either engine: pandas (warehouse-only mode) or Spark JDBC (lake mode).
CREATE TABLE IF NOT EXISTS staging.sales_clean (
    order_item_id   TEXT          PRIMARY KEY,
    order_id        TEXT          NOT NULL,
    order_date      DATE          NOT NULL,
    customer_id     TEXT          NOT NULL,
    customer_name   TEXT          NOT NULL,
    customer_email  TEXT,
    product_id      TEXT          NOT NULL,
    product_name    TEXT          NOT NULL,
    category        TEXT          NOT NULL,
    quantity        INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    discount        NUMERIC(6,4)  NOT NULL CHECK (discount >= 0 AND discount <= 0.9),
    gross_amount    NUMERIC(16,2) NOT NULL CHECK (gross_amount >= 0),
    discount_amount NUMERIC(16,2) NOT NULL CHECK (discount_amount >= 0),
    net_amount      NUMERIC(16,2) NOT NULL CHECK (net_amount >= 0),
    currency        CHAR(3)       NOT NULL,
    payment_method  TEXT          NOT NULL,
    order_status    TEXT          NOT NULL CHECK (order_status IN ('completed', 'pending', 'cancelled')),
    city            TEXT          NOT NULL,
    state           TEXT          NOT NULL,
    country         TEXT          NOT NULL,
    updated_at      TIMESTAMP     NOT NULL,
    batch_id        TEXT          NOT NULL,
    ingestion_date  DATE          NOT NULL,
    source_row_hash TEXT          NOT NULL
);

-- Quarantined rows with the rule(s) they failed.
CREATE TABLE IF NOT EXISTS staging.sales_rejected (
    batch_id        TEXT        NOT NULL,
    source_row      INTEGER     NOT NULL,
    order_item_id   TEXT,
    reject_reasons  TEXT        NOT NULL,
    raw_record      JSONB       NOT NULL,
    source_row_hash TEXT        NOT NULL,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, source_row)
);
