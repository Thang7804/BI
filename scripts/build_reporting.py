"""
build_reporting.py
Bước xây dựng các bảng tổng hợp phục vụ dashboard Metabase (reporting schema) từ dw schema.
"""

import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist123@localhost:5434/olist_dw")

AGG_MONTHLY_REVENUE = """
DROP TABLE IF EXISTS reporting.agg_monthly_revenue;

CREATE TABLE reporting.agg_monthly_revenue AS
SELECT
    DATE_TRUNC('month', purchased_at)           AS month,
    COUNT(DISTINCT order_id)                    AS total_orders,
    COUNT(*)                                    AS total_items,
    ROUND(SUM(price)::numeric,         2)       AS revenue,
    ROUND(SUM(freight_value)::numeric, 2)       AS freight_revenue,
    ROUND(SUM(total_value)::numeric,   2)       AS gross_revenue,
    ROUND(AVG(NULLIF(review_score, 0))::numeric,  2)       AS avg_review_score,
    ROUND(AVG(delivery_days)::numeric, 1)       AS avg_delivery_days,
    ROUND(
        100.0 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
        1
    )                                           AS on_time_pct
FROM dw.fact_sales
GROUP BY DATE_TRUNC('month', purchased_at)
ORDER BY month;
"""

AGG_CATEGORY_PERFORMANCE = """
DROP TABLE IF EXISTS reporting.agg_category_performance;

CREATE TABLE reporting.agg_category_performance AS
SELECT
    category_en                                           AS category,
    COUNT(DISTINCT order_id)                              AS total_orders,
    COUNT(*)                                              AS total_items,
    ROUND(SUM(price)::numeric, 2)                         AS revenue,
    ROUND(AVG(NULLIF(review_score, 0))::numeric, 2)                  AS avg_review_score,
    ROUND(AVG(delivery_days)::numeric, 1)                 AS avg_delivery_days
FROM dw.fact_sales
GROUP BY category_en
ORDER BY revenue DESC;
"""

AGG_STATE_REVENUE = """
DROP TABLE IF EXISTS reporting.agg_state_revenue;

CREATE TABLE reporting.agg_state_revenue AS
SELECT
    f.customer_state                        AS state,
    r.region,
    COUNT(DISTINCT f.customer_id)           AS total_customers,
    COUNT(DISTINCT f.order_id)              AS total_orders,
    ROUND(SUM(f.price)::numeric, 2)         AS total_revenue,
    ROUND(AVG(f.delivery_days)::numeric, 1) AS avg_delivery_days
FROM dw.fact_sales f
LEFT JOIN dw.dim_region r ON f.customer_state = r.state
GROUP BY f.customer_state, r.region
ORDER BY total_revenue DESC;
"""

AGG_SELLER_PERFORMANCE = """
DROP TABLE IF EXISTS reporting.agg_seller_performance;

CREATE TABLE reporting.agg_seller_performance AS
SELECT
    seller_id,
    seller_state,
    COUNT(DISTINCT order_id)                AS total_orders,
    COUNT(*)                                AS total_items,
    ROUND(SUM(price)::numeric,        2)    AS total_revenue,
    ROUND(AVG(NULLIF(review_score, 0))::numeric, 2)    AS avg_review_score,
    ROUND(AVG(delivery_days)::numeric,1)    AS avg_delivery_days,
    ROUND(
        100.0 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
        1
    )                                       AS on_time_pct
FROM dw.fact_sales
GROUP BY seller_id, seller_state
ORDER BY total_revenue DESC;
"""

AGG_PAYMENT_ANALYSIS = """
DROP TABLE IF EXISTS reporting.agg_payment_analysis;

CREATE TABLE reporting.agg_payment_analysis AS
WITH order_payments AS (
    SELECT DISTINCT
        order_id,
        payment_type,
        total_payment,
        max_installments
    FROM dw.fact_sales
    WHERE payment_type IS NOT NULL
)
SELECT
    payment_type,
    COUNT(order_id)                              AS total_orders,
    ROUND(SUM(total_payment)::numeric, 2)        AS total_payment_value,
    ROUND(AVG(max_installments)::numeric, 1)     AS avg_installments,
    ROUND(
        100.0 * COUNT(order_id)::numeric
        / NULLIF(SUM(COUNT(order_id)) OVER (), 0),
        1
    )                                           AS pct_orders
FROM order_payments
GROUP BY payment_type
ORDER BY total_orders DESC;
"""

AGG_DELIVERY_ANALYSIS = """
DROP TABLE IF EXISTS reporting.agg_delivery_analysis;

CREATE TABLE reporting.agg_delivery_analysis AS
SELECT
    customer_state                                      AS state,
    ROUND(AVG(delivery_days)::numeric, 1)               AS avg_delivery_days,
    ROUND(MIN(delivery_days)::numeric, 1)               AS min_delivery_days,
    ROUND(MAX(delivery_days)::numeric, 1)               AS max_delivery_days,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
          (ORDER BY delivery_days)::numeric, 1)         AS median_delivery_days,
    ROUND(
        100.0 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1
    )                                                   AS on_time_pct,
    COUNT(DISTINCT order_id)                            AS total_orders
FROM dw.fact_sales
WHERE delivery_days IS NOT NULL
GROUP BY customer_state
ORDER BY avg_delivery_days;
"""


def run_sql(engine, label: str, sql: str):
    log.info(f"  → Building: {label}")
    with engine.begin() as conn:
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
    log.info(f"  ✓ {label} done")


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)

    # Đảm bảo schema reporting tồn tại
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS reporting"))

    log.info("━━━ Building Reporting Aggregates ━━━")
    run_sql(engine, "reporting.agg_monthly_revenue",     AGG_MONTHLY_REVENUE)
    run_sql(engine, "reporting.agg_category_performance",AGG_CATEGORY_PERFORMANCE)
    run_sql(engine, "reporting.agg_state_revenue",       AGG_STATE_REVENUE)
    run_sql(engine, "reporting.agg_seller_performance",  AGG_SELLER_PERFORMANCE)
    run_sql(engine, "reporting.agg_payment_analysis",    AGG_PAYMENT_ANALYSIS)
    run_sql(engine, "reporting.agg_delivery_analysis",   AGG_DELIVERY_ANALYSIS)

    log.info("build_reporting.py hoàn thành! reporting (aggregates) sẵn sàng.")


if __name__ == "__main__":
    main()
