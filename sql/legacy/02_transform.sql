-- Idempotent warehouse refresh from the raw landing table.
-- Type 1 dimensions: current names replace older names.
INSERT INTO mart.dim_customer (customer_id, customer_name)
SELECT DISTINCT ON (customer_id) customer_id, customer_name
FROM raw.sales
ORDER BY customer_id, ingested_at DESC, transaction_id DESC
ON CONFLICT (customer_id) DO UPDATE SET customer_name = EXCLUDED.customer_name;

INSERT INTO mart.dim_product (product_id, product_name)
SELECT DISTINCT ON (product_id) product_id, product_name
FROM raw.sales
ORDER BY product_id, ingested_at DESC, transaction_id DESC
ON CONFLICT (product_id) DO UPDATE SET product_name = EXCLUDED.product_name;

INSERT INTO mart.dim_date (date_key, full_date, year, month, day)
SELECT DISTINCT
    TO_CHAR(transaction_date, 'YYYYMMDD')::INTEGER,
    transaction_date,
    EXTRACT(YEAR FROM transaction_date)::INTEGER,
    EXTRACT(MONTH FROM transaction_date)::INTEGER,
    EXTRACT(DAY FROM transaction_date)::INTEGER
FROM raw.sales
ON CONFLICT (date_key) DO NOTHING;

-- Only completed transactions contribute revenue.
-- Delete records that changed from completed to another status.
DELETE FROM mart.fact_sales f
USING raw.sales r
WHERE f.transaction_id = r.transaction_id AND r.status <> 'completed';

INSERT INTO mart.fact_sales (
    transaction_id, date_key, customer_key, product_key,
    quantity, unit_price, revenue
)
SELECT
    r.transaction_id,
    TO_CHAR(r.transaction_date, 'YYYYMMDD')::INTEGER,
    c.customer_key,
    p.product_key,
    r.quantity,
    r.unit_price,
    (r.quantity * r.unit_price)::NUMERIC(14,2)
FROM raw.sales r
JOIN mart.dim_customer c ON c.customer_id = r.customer_id
JOIN mart.dim_product p ON p.product_id = r.product_id
WHERE r.status = 'completed'
ON CONFLICT (transaction_id) DO UPDATE SET
    date_key = EXCLUDED.date_key,
    customer_key = EXCLUDED.customer_key,
    product_key = EXCLUDED.product_key,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    revenue = EXCLUDED.revenue,
    updated_at = NOW();
