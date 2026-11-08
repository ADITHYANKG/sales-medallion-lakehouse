-- Sales analytics queries against the star schema.
-- Revenue always = SUM(net_amount) of COMPLETED line items (after discount).
-- Each query is preceded by a "-- name: <id>" marker used by `python -m src.cli report`.

-- name: headline_kpis
SELECT
    COALESCE(SUM(f.net_amount) FILTER (WHERE f.order_status = 'completed'), 0)          AS total_revenue,
    COUNT(DISTINCT f.order_id)                                                         AS total_orders,
    COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'completed')            AS completed_orders,
    ROUND(COALESCE(SUM(f.net_amount) FILTER (WHERE f.order_status = 'completed'), 0)
          / NULLIF(COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'completed'), 0), 2)
                                                                                       AS average_order_value,
    COUNT(DISTINCT f.customer_key)                                                     AS total_customers,
    ROUND(100.0 * COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'cancelled')
          / NULLIF(COUNT(DISTINCT f.order_id), 0), 2)                                  AS cancelled_order_pct
FROM mart.fact_sales f;

-- name: top_products
SELECT p.product_id, p.product_name, p.category,
       SUM(f.quantity) AS units_sold, SUM(f.net_amount) AS revenue
FROM mart.fact_sales f
JOIN mart.dim_product p ON p.product_key = f.product_key
WHERE f.order_status = 'completed'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC, p.product_id
LIMIT 10;

-- name: revenue_by_category
SELECT p.category, COUNT(DISTINCT f.order_id) AS completed_orders,
       SUM(f.net_amount) AS revenue,
       ROUND(100.0 * SUM(f.net_amount) / SUM(SUM(f.net_amount)) OVER (), 2) AS revenue_share_pct
FROM mart.fact_sales f
JOIN mart.dim_product p ON p.product_key = f.product_key
WHERE f.order_status = 'completed'
GROUP BY p.category
ORDER BY revenue DESC;

-- name: monthly_sales
WITH m AS (
    SELECT d.year_month, SUM(f.net_amount) AS revenue, COUNT(DISTINCT f.order_id) AS completed_orders
    FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.date_key
    WHERE f.order_status = 'completed'
    GROUP BY d.year_month
)
SELECT year_month, completed_orders, revenue,
       LAG(revenue) OVER (ORDER BY year_month) AS prev_month_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY year_month))
             / NULLIF(LAG(revenue) OVER (ORDER BY year_month), 0), 2) AS growth_pct
FROM m ORDER BY year_month;

-- name: daily_sales
SELECT d.full_date, COUNT(DISTINCT f.order_id) AS completed_orders, SUM(f.net_amount) AS revenue,
       ROUND(AVG(SUM(f.net_amount)) OVER (ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
           AS revenue_7d_avg
FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.date_key
WHERE f.order_status = 'completed'
GROUP BY d.full_date
ORDER BY d.full_date;

-- name: revenue_by_location
SELECT l.country, l.state, l.city, COUNT(DISTINCT f.order_id) AS completed_orders,
       COUNT(DISTINCT f.customer_key) AS customers, SUM(f.net_amount) AS revenue
FROM mart.fact_sales f JOIN mart.dim_location l ON l.location_key = f.location_key
WHERE f.order_status = 'completed'
GROUP BY l.country, l.state, l.city
ORDER BY revenue DESC;

-- name: customer_lifetime_revenue
SELECT c.customer_id, c.customer_name,
       COUNT(DISTINCT f.order_id) AS total_orders,
       COALESCE(SUM(f.net_amount) FILTER (WHERE f.order_status = 'completed'), 0) AS lifetime_revenue,
       RANK() OVER (ORDER BY COALESCE(SUM(f.net_amount) FILTER (WHERE f.order_status = 'completed'), 0) DESC)
           AS revenue_rank
FROM mart.fact_sales f JOIN mart.dim_customer c ON c.customer_key = f.customer_key
GROUP BY c.customer_id, c.customer_name
ORDER BY lifetime_revenue DESC, c.customer_id
LIMIT 20;

-- name: order_status_breakdown
SELECT f.order_status, COUNT(DISTINCT f.order_id) AS orders,
       ROUND(100.0 * COUNT(DISTINCT f.order_id) / SUM(COUNT(DISTINCT f.order_id)) OVER (), 2) AS pct_of_orders,
       SUM(f.net_amount) AS order_value
FROM mart.fact_sales f
GROUP BY f.order_status
ORDER BY orders DESC;

-- name: data_quality_fact_vs_completed_view
-- Must return zero rows: completed revenue in the view equals the fact table.
SELECT 'mismatch' AS problem
WHERE (SELECT COALESCE(SUM(net_amount), 0) FROM mart.v_completed_sales)
   <> (SELECT COALESCE(SUM(net_amount), 0) FROM mart.fact_sales WHERE order_status = 'completed');

-- name: pipeline_runs
SELECT run_id, pipeline, logical_date, status, started_at, duration_seconds,
       source_records, valid_records, rejected_records, duplicate_records, warehouse_records
FROM monitoring.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
