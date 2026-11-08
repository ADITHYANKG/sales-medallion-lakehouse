-- V005: Gold-layer aggregates (loaded from the lake by Spark) and reporting views.
-- Revenue = net_amount of COMPLETED line items. Order counts are distinct order_id.

CREATE TABLE IF NOT EXISTS mart.agg_daily_sales (
    order_date         DATE PRIMARY KEY,
    total_orders       BIGINT        NOT NULL,
    completed_orders   BIGINT        NOT NULL,
    cancelled_orders   BIGINT        NOT NULL,
    pending_orders     BIGINT        NOT NULL,
    units_sold         BIGINT        NOT NULL,
    gross_revenue      NUMERIC(18,2) NOT NULL,
    discount_amount    NUMERIC(18,2) NOT NULL,
    net_revenue        NUMERIC(18,2) NOT NULL,
    avg_order_value    NUMERIC(18,2),
    revenue_7d_avg     NUMERIC(18,2),
    cumulative_revenue NUMERIC(20,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.agg_monthly_sales (
    order_month         DATE PRIMARY KEY,           -- first day of month
    total_orders        BIGINT        NOT NULL,
    completed_orders    BIGINT        NOT NULL,
    cancelled_orders    BIGINT        NOT NULL,
    net_revenue         NUMERIC(18,2) NOT NULL,
    avg_order_value     NUMERIC(18,2),
    prev_month_revenue  NUMERIC(18,2),
    revenue_growth_pct  NUMERIC(9,2),
    cancelled_order_pct NUMERIC(7,2)  NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.agg_category_sales (
    category          TEXT PRIMARY KEY,
    completed_orders  BIGINT        NOT NULL,
    units_sold        BIGINT        NOT NULL,
    net_revenue       NUMERIC(18,2) NOT NULL,
    revenue_share_pct NUMERIC(7,2)  NOT NULL,
    revenue_rank      INTEGER       NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.agg_product_sales (
    product_id       TEXT PRIMARY KEY,
    product_name     TEXT          NOT NULL,
    category         TEXT          NOT NULL,
    completed_orders BIGINT        NOT NULL,
    units_sold       BIGINT        NOT NULL,
    net_revenue      NUMERIC(18,2) NOT NULL,
    revenue_rank     INTEGER       NOT NULL,
    category_rank    INTEGER       NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.agg_customer_sales (
    customer_id      TEXT PRIMARY KEY,
    customer_name    TEXT          NOT NULL,
    total_orders     BIGINT        NOT NULL,
    completed_orders BIGINT        NOT NULL,
    lifetime_revenue NUMERIC(18,2) NOT NULL,
    first_order_date DATE          NOT NULL,
    last_order_date  DATE          NOT NULL,
    revenue_rank     INTEGER       NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.agg_location_sales (
    country          TEXT          NOT NULL,
    state            TEXT          NOT NULL,
    city             TEXT          NOT NULL,
    completed_orders BIGINT        NOT NULL,
    customers        BIGINT        NOT NULL,
    net_revenue      NUMERIC(18,2) NOT NULL,
    revenue_rank     INTEGER       NOT NULL,
    PRIMARY KEY (country, state, city)
);

-- Staging copies receive Spark JDBC writes before an atomic swap into mart.
CREATE TABLE IF NOT EXISTS staging.gold_daily_sales    (LIKE mart.agg_daily_sales    INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.gold_monthly_sales  (LIKE mart.agg_monthly_sales  INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.gold_category_sales (LIKE mart.agg_category_sales INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.gold_product_sales  (LIKE mart.agg_product_sales  INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.gold_customer_sales (LIKE mart.agg_customer_sales INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.gold_location_sales (LIKE mart.agg_location_sales INCLUDING ALL);

-- Denormalised view used by ad-hoc SQL and Metabase.
CREATE OR REPLACE VIEW mart.v_sales AS
SELECT
    f.order_item_id, f.order_id, d.full_date AS order_date, d.year, d.quarter, d.month,
    d.year_month, d.day_name, d.is_weekend,
    c.customer_id, c.customer_name, c.customer_email,
    p.product_id, p.product_name, p.category,
    l.city, l.state, l.country,
    f.payment_method, f.order_status, f.currency, f.quantity, f.unit_price, f.discount_rate,
    f.gross_amount, f.discount_amount, f.net_amount,
    (f.order_status = 'completed') AS is_completed,
    f.source_updated_at, f.batch_id
FROM mart.fact_sales f
JOIN mart.dim_date d     ON d.date_key = f.date_key
JOIN mart.dim_customer c ON c.customer_key = f.customer_key
JOIN mart.dim_product p  ON p.product_key = f.product_key
JOIN mart.dim_location l ON l.location_key = f.location_key;

CREATE OR REPLACE VIEW mart.v_completed_sales AS
SELECT * FROM mart.v_sales WHERE order_status = 'completed';
