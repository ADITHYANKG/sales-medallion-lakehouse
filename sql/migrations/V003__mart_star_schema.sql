-- V003: star schema. Grain of mart.fact_sales = one row per order line item.

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key     INTEGER PRIMARY KEY,            -- YYYYMMDD
    full_date    DATE    NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    month_name   TEXT    NOT NULL,
    year_month   CHAR(7) NOT NULL,               -- YYYY-MM
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,              -- ISO: 1 = Monday
    day_name     TEXT    NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

-- Pre-populate a calendar so every fact date resolves (more are added on demand).
INSERT INTO mart.dim_date
SELECT TO_CHAR(d, 'YYYYMMDD')::INTEGER, d::DATE,
       EXTRACT(YEAR FROM d), EXTRACT(QUARTER FROM d), EXTRACT(MONTH FROM d),
       TRIM(TO_CHAR(d, 'Month')), TO_CHAR(d, 'YYYY-MM'), EXTRACT(DAY FROM d),
       EXTRACT(ISODOW FROM d), TRIM(TO_CHAR(d, 'Day')), EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(DATE '2020-01-01', DATE '2035-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- SCD Type 1: attributes are overwritten in place, surrogate key is stable.
CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_key      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id       TEXT        NOT NULL UNIQUE,
    customer_name     TEXT        NOT NULL,
    customer_email    TEXT,
    source_updated_at TIMESTAMP   NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_key       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id        TEXT        NOT NULL UNIQUE,
    product_name      TEXT        NOT NULL,
    category          TEXT        NOT NULL,
    source_updated_at TIMESTAMP   NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart.dim_location (
    location_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city         TEXT        NOT NULL,
    state        TEXT        NOT NULL,
    country      TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (country, state, city)
);

CREATE TABLE IF NOT EXISTS mart.fact_sales (
    sales_key         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_item_id     TEXT          NOT NULL UNIQUE,  -- natural key of the grain
    order_id          TEXT          NOT NULL,         -- degenerate dimension
    date_key          INTEGER       NOT NULL REFERENCES mart.dim_date (date_key),
    customer_key      BIGINT        NOT NULL REFERENCES mart.dim_customer (customer_key),
    product_key       BIGINT        NOT NULL REFERENCES mart.dim_product (product_key),
    location_key      BIGINT        NOT NULL REFERENCES mart.dim_location (location_key),
    payment_method    TEXT          NOT NULL,
    order_status      TEXT          NOT NULL CHECK (order_status IN ('completed', 'pending', 'cancelled')),
    currency          CHAR(3)       NOT NULL,
    quantity          INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price        NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    discount_rate     NUMERIC(6,4)  NOT NULL CHECK (discount_rate BETWEEN 0 AND 0.9),
    gross_amount      NUMERIC(16,2) NOT NULL,
    discount_amount   NUMERIC(16,2) NOT NULL,
    net_amount        NUMERIC(16,2) NOT NULL,
    source_updated_at TIMESTAMP     NOT NULL,
    batch_id          TEXT          NOT NULL,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_fact_amounts CHECK (
        gross_amount = ROUND(quantity * unit_price, 2)
        AND net_amount = ROUND(quantity * unit_price * (1 - discount_rate), 2)
        AND discount_amount = gross_amount - net_amount
    )
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON mart.fact_sales (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON mart.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON mart.fact_sales (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_location ON mart.fact_sales (location_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order    ON mart.fact_sales (order_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_status   ON mart.fact_sales (order_status);
