-- Daily completed revenue
SELECT d.full_date, SUM(f.revenue) AS daily_revenue
FROM mart.fact_sales f
JOIN mart.dim_date d ON d.date_key = f.date_key
GROUP BY d.full_date
ORDER BY d.full_date;

-- Top 5 products by completed revenue
SELECT p.product_name, SUM(f.quantity) AS units_sold, SUM(f.revenue) AS total_revenue
FROM mart.fact_sales f
JOIN mart.dim_product p ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;

-- Data quality check: zero rows should be returned.
SELECT f.transaction_id
FROM mart.fact_sales f
LEFT JOIN raw.sales r ON r.transaction_id = f.transaction_id
WHERE r.transaction_id IS NULL OR r.status <> 'completed';
