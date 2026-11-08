-- Idempotent merge: staging.sales_clean -> star schema.
-- Executed inside one transaction while holding an advisory lock (see loader.py).
--
-- Change handling:
--  * Dimensions are SCD Type 1 - the most recent source version overwrites attributes.
--  * Facts are upserted on order_item_id. The guard `source_updated_at <= EXCLUDED`
--    means an older snapshot can never overwrite a newer version (out-of-order or
--    replayed batches are harmless), while re-running the same snapshot is a no-op.
--  * Cancellations are status updates: the row stays in the fact table with
--    order_status = 'cancelled' and drops out of completed-revenue reporting.

-- 1. Calendar rows for any date outside the pre-populated range.
INSERT INTO mart.dim_date
SELECT DISTINCT TO_CHAR(s.order_date, 'YYYYMMDD')::INTEGER, s.order_date,
       EXTRACT(YEAR FROM s.order_date), EXTRACT(QUARTER FROM s.order_date),
       EXTRACT(MONTH FROM s.order_date), TRIM(TO_CHAR(s.order_date, 'Month')),
       TO_CHAR(s.order_date, 'YYYY-MM'), EXTRACT(DAY FROM s.order_date),
       EXTRACT(ISODOW FROM s.order_date), TRIM(TO_CHAR(s.order_date, 'Day')),
       EXTRACT(ISODOW FROM s.order_date) IN (6, 7)
FROM staging.sales_clean s
ON CONFLICT (date_key) DO NOTHING;

-- 2. Customers (SCD1, latest version wins).
INSERT INTO mart.dim_customer (customer_id, customer_name, customer_email, source_updated_at)
SELECT DISTINCT ON (customer_id) customer_id, customer_name, customer_email, updated_at
FROM staging.sales_clean
ORDER BY customer_id, updated_at DESC, source_row_hash DESC
ON CONFLICT (customer_id) DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    customer_email = COALESCE(EXCLUDED.customer_email, mart.dim_customer.customer_email),
    source_updated_at = EXCLUDED.source_updated_at,
    updated_at = NOW()
WHERE mart.dim_customer.source_updated_at <= EXCLUDED.source_updated_at
  AND (mart.dim_customer.customer_name, mart.dim_customer.customer_email)
      IS DISTINCT FROM (EXCLUDED.customer_name, COALESCE(EXCLUDED.customer_email, mart.dim_customer.customer_email));

-- 3. Products (SCD1).
INSERT INTO mart.dim_product (product_id, product_name, category, source_updated_at)
SELECT DISTINCT ON (product_id) product_id, product_name, category, updated_at
FROM staging.sales_clean
ORDER BY product_id, updated_at DESC, source_row_hash DESC
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    source_updated_at = EXCLUDED.source_updated_at,
    updated_at = NOW()
WHERE mart.dim_product.source_updated_at <= EXCLUDED.source_updated_at
  AND (mart.dim_product.product_name, mart.dim_product.category)
      IS DISTINCT FROM (EXCLUDED.product_name, EXCLUDED.category);

-- 4. Locations (static attributes, insert-only).
INSERT INTO mart.dim_location (city, state, country)
SELECT DISTINCT city, state, country FROM staging.sales_clean
ON CONFLICT (country, state, city) DO NOTHING;

-- 5. Facts.
INSERT INTO mart.fact_sales (
    order_item_id, order_id, date_key, customer_key, product_key, location_key,
    payment_method, order_status, currency, quantity, unit_price, discount_rate,
    gross_amount, discount_amount, net_amount, source_updated_at, batch_id
)
SELECT
    s.order_item_id, s.order_id, TO_CHAR(s.order_date, 'YYYYMMDD')::INTEGER,
    c.customer_key, p.product_key, l.location_key,
    s.payment_method, s.order_status, s.currency, s.quantity, s.unit_price, s.discount,
    s.gross_amount, s.discount_amount, s.net_amount, s.updated_at, s.batch_id
FROM staging.sales_clean s
JOIN mart.dim_customer c ON c.customer_id = s.customer_id
JOIN mart.dim_product p  ON p.product_id = s.product_id
JOIN mart.dim_location l ON l.city = s.city AND l.state = s.state AND l.country = s.country
ON CONFLICT (order_item_id) DO UPDATE SET
    order_id = EXCLUDED.order_id,
    date_key = EXCLUDED.date_key,
    customer_key = EXCLUDED.customer_key,
    product_key = EXCLUDED.product_key,
    location_key = EXCLUDED.location_key,
    payment_method = EXCLUDED.payment_method,
    order_status = EXCLUDED.order_status,
    currency = EXCLUDED.currency,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    discount_rate = EXCLUDED.discount_rate,
    gross_amount = EXCLUDED.gross_amount,
    discount_amount = EXCLUDED.discount_amount,
    net_amount = EXCLUDED.net_amount,
    source_updated_at = EXCLUDED.source_updated_at,
    batch_id = EXCLUDED.batch_id,
    updated_at = NOW()
WHERE mart.fact_sales.source_updated_at <= EXCLUDED.source_updated_at
  AND (mart.fact_sales.order_status, mart.fact_sales.quantity, mart.fact_sales.unit_price,
       mart.fact_sales.discount_rate, mart.fact_sales.net_amount, mart.fact_sales.customer_key,
       mart.fact_sales.product_key, mart.fact_sales.location_key, mart.fact_sales.date_key,
       mart.fact_sales.payment_method, mart.fact_sales.source_updated_at)
      IS DISTINCT FROM
      (EXCLUDED.order_status, EXCLUDED.quantity, EXCLUDED.unit_price, EXCLUDED.discount_rate,
       EXCLUDED.net_amount, EXCLUDED.customer_key, EXCLUDED.product_key, EXCLUDED.location_key,
       EXCLUDED.date_key, EXCLUDED.payment_method, EXCLUDED.source_updated_at);
