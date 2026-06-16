
import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist123@localhost:5434/olist_dw")

# ── SQL transforms ──────────────────────────────────────────

STAGING_ORDERS = """
CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    purchased_at TIMESTAMP,
    approved_at TIMESTAMP,
    delivered_at TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    delivery_days DOUBLE PRECISION,
    is_on_time BOOLEAN,
    customer_city TEXT,
    customer_state TEXT
);

TRUNCATE TABLE staging.stg_orders;

INSERT INTO staging.stg_orders
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp::timestamp                         AS purchased_at,
    o.order_approved_at::timestamp                               AS approved_at,
    o.order_delivered_customer_date::timestamp                   AS delivered_at,
    o.order_estimated_delivery_date::timestamp                   AS estimated_delivery_at,
    -- Tính số ngày giao hàng thực tế
    EXTRACT(EPOCH FROM (
        o.order_delivered_customer_date::timestamp
        - o.order_purchase_timestamp::timestamp
    )) / 86400.0                                                  AS delivery_days,
    -- So sánh với dự kiến
    CASE
        WHEN o.order_delivered_customer_date::timestamp
             <= o.order_estimated_delivery_date::timestamp
        THEN TRUE ELSE FALSE
    END                                                           AS is_on_time,
    c.customer_city,
    c.customer_state
FROM raw.orders o
JOIN raw.customers c USING (customer_id)
WHERE o.order_status = 'delivered'
  AND o.order_purchase_timestamp IS NOT NULL;
"""

STAGING_ORDER_ITEMS = """
DROP TABLE IF EXISTS staging.stg_order_items;

CREATE TABLE staging.stg_order_items AS
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value                                   AS total_value,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS category,
    s.seller_city,
    s.seller_state
FROM raw.order_items oi
JOIN raw.products p USING (product_id)
JOIN raw.sellers s USING (seller_id)
LEFT JOIN raw.product_category_name_translation t
       ON p.product_category_name = t.product_category_name;
"""

STAGING_REVIEWS = """
DROP TABLE IF EXISTS staging.stg_reviews;

CREATE TABLE staging.stg_reviews AS
SELECT
    review_id,
    order_id,
    review_score,
    review_creation_date::timestamp AS reviewed_at
FROM raw.order_reviews
WHERE review_score IS NOT NULL;
"""

STAGING_PAYMENTS = """
DROP TABLE IF EXISTS staging.stg_payments;

CREATE TABLE staging.stg_payments AS
SELECT
    order_id,
    payment_type,
    SUM(payment_value)     AS total_payment,
    MAX(payment_installments) AS max_installments
FROM raw.order_payments
GROUP BY order_id, payment_type;
"""

def run_sql(engine, label: str, sql: str):
    log.info(f"Running: {label}")
    with engine.begin() as conn:
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
    log.info(f"  ✓ {label} done")

def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))

    run_sql(engine, "stg_orders",      STAGING_ORDERS)
    run_sql(engine, "stg_order_items", STAGING_ORDER_ITEMS)
    run_sql(engine, "stg_reviews",     STAGING_REVIEWS)
    run_sql(engine, "stg_payments",    STAGING_PAYMENTS)

    log.info("✅ Transform hoàn thành!")

if __name__ == "__main__":
    main()
