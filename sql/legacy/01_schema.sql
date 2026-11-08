-- Raw landing table: retains the most recently received version of each transaction.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.sales (
    transaction_id TEXT PRIMARY KEY,
    transaction_date DATE NOT NULL,
    customer_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    status TEXT NOT NULL CHECK (status IN ('completed', 'pending', 'cancelled')),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE,
    customer_name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mart.fact_sales (
    transaction_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES mart.dim_date(date_key),
    customer_key BIGINT NOT NULL REFERENCES mart.dim_customer(customer_key),
    product_key BIGINT NOT NULL REFERENCES mart.dim_product(product_key),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    revenue NUMERIC(14,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON mart.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON mart.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON mart.fact_sales(customer_key);
