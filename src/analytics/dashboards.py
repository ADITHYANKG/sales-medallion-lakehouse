"""Dashboard definitions (native SQL questions) provisioned into Metabase.

Sales cards read ``mart.v_sales`` and accept optional filters through Metabase
template tags: ``[[ ... {{tag}} ]]`` blocks are dropped when a filter is empty.
"""
from __future__ import annotations

SALES_FILTERS = """
WHERE 1 = 1
  [[AND order_date >= {{start_date}}]]
  [[AND order_date <= {{end_date}}]]
  [[AND category = {{category}}]]
"""
SALES_TAGS = ("start_date", "end_date", "category")


def _sales(sql: str) -> str:
    return sql.replace("{filters}", SALES_FILTERS)


SALES_CARDS: list[dict] = [
    {"name": "Total revenue", "display": "scalar", "size": (4, 3), "tags": SALES_TAGS,
     "sql": _sales("SELECT COALESCE(SUM(net_amount) FILTER (WHERE is_completed), 0) AS total_revenue "
                   "FROM mart.v_sales {filters}")},
    {"name": "Completed orders", "display": "scalar", "size": (4, 3), "tags": SALES_TAGS,
     "sql": _sales("SELECT COUNT(DISTINCT order_id) FILTER (WHERE is_completed) AS completed_orders "
                   "FROM mart.v_sales {filters}")},
    {"name": "Average order value", "display": "scalar", "size": (4, 3), "tags": SALES_TAGS,
     "sql": _sales("SELECT ROUND(SUM(net_amount) FILTER (WHERE is_completed) / "
                   "NULLIF(COUNT(DISTINCT order_id) FILTER (WHERE is_completed), 0), 2) AS average_order_value "
                   "FROM mart.v_sales {filters}")},
    {"name": "Total customers", "display": "scalar", "size": (4, 3), "tags": SALES_TAGS,
     "sql": _sales("SELECT COUNT(DISTINCT customer_id) AS total_customers FROM mart.v_sales {filters}")},
    {"name": "Cancelled order %", "display": "scalar", "size": (4, 3), "tags": SALES_TAGS,
     "sql": _sales("SELECT ROUND(100.0 * COUNT(DISTINCT order_id) FILTER (WHERE order_status = 'cancelled') / "
                   "NULLIF(COUNT(DISTINCT order_id), 0), 2) AS cancelled_order_pct FROM mart.v_sales {filters}")},
    # Like-for-like growth: month-to-date revenue vs the same days of the previous month,
    # so a partially loaded current month is not compared with a full month.
    {"name": "Sales growth % (MTD vs same days last month)", "display": "scalar", "size": (4, 3),
     "tags": SALES_TAGS,
     "sql": _sales("""WITH s AS (SELECT order_date, net_amount FROM mart.v_sales {filters} AND is_completed),
l AS (SELECT MAX(order_date) AS d FROM s),
p AS (
  SELECT SUM(s.net_amount) FILTER (WHERE s.order_date BETWEEN DATE_TRUNC('month', l.d)::date AND l.d) AS mtd,
         SUM(s.net_amount) FILTER (WHERE s.order_date BETWEEN (DATE_TRUNC('month', l.d) - INTERVAL '1 month')::date
                                                       AND (l.d - INTERVAL '1 month')::date) AS prev_mtd
  FROM s CROSS JOIN l)
SELECT ROUND(100.0 * (mtd - prev_mtd) / NULLIF(prev_mtd, 0), 2) AS sales_growth_pct FROM p""")},
    {"name": "Monthly revenue trend", "display": "bar", "size": (12, 6), "tags": SALES_TAGS,
     "viz": {"graph.dimensions": ["year_month"], "graph.metrics": ["revenue"]},
     "sql": _sales("SELECT year_month, SUM(net_amount) FILTER (WHERE is_completed) AS revenue "
                   "FROM mart.v_sales {filters} GROUP BY year_month ORDER BY year_month")},
    {"name": "Daily sales trend", "display": "line", "size": (12, 6), "tags": SALES_TAGS,
     "viz": {"graph.dimensions": ["order_date"], "graph.metrics": ["revenue", "revenue_7d_avg"]},
     "sql": _sales("""SELECT order_date, SUM(net_amount) FILTER (WHERE is_completed) AS revenue,
  ROUND(AVG(SUM(net_amount) FILTER (WHERE is_completed))
        OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7d_avg
FROM mart.v_sales {filters} GROUP BY order_date ORDER BY order_date""")},
    {"name": "Top 10 products", "display": "row", "size": (12, 7), "tags": SALES_TAGS,
     "viz": {"graph.dimensions": ["product_name"], "graph.metrics": ["revenue"]},
     "sql": _sales("SELECT product_name, SUM(net_amount) AS revenue, SUM(quantity) AS units_sold "
                   "FROM mart.v_sales {filters} AND is_completed "
                   "GROUP BY product_name ORDER BY revenue DESC LIMIT 10")},
    {"name": "Revenue by category", "display": "pie", "size": (12, 7), "tags": SALES_TAGS,
     "viz": {"pie.dimension": "category", "pie.metric": "revenue"},
     "sql": _sales("SELECT category, SUM(net_amount) AS revenue FROM mart.v_sales {filters} AND is_completed "
                   "GROUP BY category ORDER BY revenue DESC")},
    {"name": "Revenue by location", "display": "bar", "size": (12, 7), "tags": SALES_TAGS,
     "viz": {"graph.dimensions": ["state"], "graph.metrics": ["revenue"]},
     "sql": _sales("SELECT state, SUM(net_amount) AS revenue, COUNT(DISTINCT customer_id) AS customers "
                   "FROM mart.v_sales {filters} AND is_completed GROUP BY state ORDER BY revenue DESC")},
    {"name": "Customer sales ranking", "display": "table", "size": (12, 7), "tags": SALES_TAGS,
     "sql": _sales("""SELECT RANK() OVER (ORDER BY SUM(net_amount) FILTER (WHERE is_completed) DESC NULLS LAST) AS rank,
  customer_id, customer_name, COUNT(DISTINCT order_id) AS orders,
  COALESCE(SUM(net_amount) FILTER (WHERE is_completed), 0) AS lifetime_revenue
FROM mart.v_sales {filters} GROUP BY customer_id, customer_name ORDER BY rank LIMIT 25""")},
]

# Run sequence number gives evenly spaced charts even when runs are minutes apart.
RUNS = "FROM (SELECT ROW_NUMBER() OVER (ORDER BY started_at) AS run_no, * FROM monitoring.pipeline_runs) r "

MONITORING_CARDS: list[dict] = [
    {"name": "Pipeline success rate %", "display": "scalar", "size": (6, 3),
     "sql": "SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success') / NULLIF(COUNT(*), 0), 1) "
            "AS success_rate_pct FROM monitoring.pipeline_runs WHERE status <> 'running'"},
    {"name": "Latest execution status", "display": "table", "size": (18, 3),
     "sql": "SELECT run_id, pipeline, logical_date, status, started_at, ROUND(duration_seconds, 1) AS seconds, "
            "LEFT(error_message, 120) AS error FROM monitoring.pipeline_runs ORDER BY started_at DESC LIMIT 1"},
    {"name": "Processing duration (seconds)", "display": "line", "size": (12, 6),
     "viz": {"graph.dimensions": ["run_no"], "graph.metrics": ["duration_seconds"]},
     "sql": "SELECT run_no, ROUND(duration_seconds, 1) AS duration_seconds " + RUNS +
            "WHERE status = 'success' ORDER BY run_no"},
    {"name": "Records processed per run", "display": "bar", "size": (12, 6),
     "viz": {"graph.dimensions": ["run_no"], "graph.metrics": ["source_records", "valid_records"]},
     "sql": "SELECT run_no, source_records, valid_records " + RUNS + "WHERE status = 'success' ORDER BY run_no"},
    {"name": "Invalid record %", "display": "line", "size": (12, 6),
     "viz": {"graph.dimensions": ["run_no"], "graph.metrics": ["invalid_pct"]},
     "sql": "SELECT run_no, ROUND(100.0 * rejected_records / NULLIF(source_records, 0), 2) AS invalid_pct "
            + RUNS + "WHERE status = 'success' ORDER BY run_no"},
    {"name": "Duplicate records per run", "display": "bar", "size": (12, 6),
     "viz": {"graph.dimensions": ["run_no"], "graph.metrics": ["duplicate_records"]},
     "sql": "SELECT run_no, duplicate_records " + RUNS + "WHERE status = 'success' ORDER BY run_no"},
    {"name": "Warehouse load count", "display": "line", "size": (12, 6),
     "viz": {"graph.dimensions": ["run_no"], "graph.metrics": ["warehouse_records"]},
     "sql": "SELECT run_no, warehouse_records " + RUNS + "WHERE status = 'success' ORDER BY run_no"},
    {"name": "Recent pipeline runs", "display": "table", "size": (12, 6),
     "sql": "SELECT run_id, pipeline, logical_date, status, ROUND(duration_seconds, 1) AS seconds, "
            "source_records, rejected_records, duplicate_records, warehouse_records "
            "FROM monitoring.pipeline_runs ORDER BY started_at DESC LIMIT 20"},
    {"name": "Failed data-quality checks", "display": "table", "size": (24, 5),
     "sql": "SELECT checked_at, run_id, layer, check_name, observed, expected, LEFT(details, 150) AS details "
            "FROM monitoring.data_quality_results WHERE NOT passed ORDER BY checked_at DESC LIMIT 50"},
]

DASHBOARDS = [
    {"name": "Sales Analytics", "description": "Completed-sales revenue, orders, products, categories, "
     "locations and customers from the PostgreSQL star schema.", "cards": SALES_CARDS,
     "parameters": [
         {"id": "start_date", "name": "Start date", "slug": "start_date", "type": "date/single"},
         {"id": "end_date", "name": "End date", "slug": "end_date", "type": "date/single"},
         {"id": "category", "name": "Category", "slug": "category", "type": "string/="},
     ]},
    {"name": "Pipeline Monitoring", "description": "Run history, durations, volumes and data-quality "
     "results recorded in the monitoring schema.", "cards": MONITORING_CARDS, "parameters": []},
]
