
import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist123@localhost:5434/olist_dw")

# ── SQL transforms ──────────────────────────────────────────

STAGING_CUSTOMERS = """
DROP TABLE IF EXISTS staging.stg_customers;

CREATE TABLE staging.stg_customers AS
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix AS zip_code_prefix,
    LOWER(TRIM(customer_city)) AS city,
    UPPER(TRIM(customer_state)) AS state
FROM raw.customers
WHERE customer_id IS NOT NULL;
"""

STAGING_SELLERS = """
DROP TABLE IF EXISTS staging.stg_sellers;

CREATE TABLE staging.stg_sellers AS
SELECT DISTINCT
    seller_id,
    seller_zip_code_prefix AS zip_code_prefix,
    LOWER(TRIM(seller_city)) AS city,
    UPPER(TRIM(seller_state)) AS state
FROM raw.sellers
WHERE seller_id IS NOT NULL;
"""

STAGING_PRODUCTS = """
DROP TABLE IF EXISTS staging.stg_products;

CREATE TABLE staging.stg_products AS
SELECT DISTINCT
    p.product_id,
    p.product_category_name AS category_pt,
    COALESCE(
        t.product_category_name_english,
        p.product_category_name,
        'unknown'
    ) AS category_en,
    p.product_weight_g AS weight_g,
    p.product_length_cm AS length_cm,
    p.product_height_cm AS height_cm,
    p.product_width_cm AS width_cm,
    p.product_photos_qty AS photos_qty
FROM raw.products p
LEFT JOIN raw.product_category_name_translation t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL;
"""

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
    c.city                                                        AS customer_city,
    c.state                                                       AS customer_state
FROM raw.orders o
JOIN staging.stg_customers c USING (customer_id)
WHERE o.order_status = 'delivered'
  AND o.order_purchase_timestamp IS NOT NULL
  AND o.order_delivered_customer_date IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL;
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
    p.category_en                                                 AS category,
    s.city                                                        AS seller_city,
    s.state                                                       AS seller_state
FROM raw.order_items oi
JOIN staging.stg_products p USING (product_id)
JOIN staging.stg_sellers s USING (seller_id);
"""

STAGING_REVIEWS = """
DROP TABLE IF EXISTS staging.stg_reviews;

CREATE TABLE staging.stg_reviews AS
SELECT DISTINCT ON (order_id)
    review_id,
    order_id,
    review_score,
    review_creation_date::timestamp AS reviewed_at
FROM raw.order_reviews
WHERE review_score IS NOT NULL
ORDER BY order_id, review_creation_date DESC;
"""

STAGING_PAYMENTS = """
DROP TABLE IF EXISTS staging.stg_payments;

CREATE TABLE staging.stg_payments AS
SELECT
    order_id,
    STRING_AGG(DISTINCT payment_type, '+') AS payment_type,
    SUM(payment_value) AS total_payment,
    MAX(payment_installments) AS max_installments
FROM raw.order_payments
GROUP BY order_id;
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

    run_sql(engine, "stg_customers",   STAGING_CUSTOMERS)
    run_sql(engine, "stg_sellers",     STAGING_SELLERS)
    run_sql(engine, "stg_products",    STAGING_PRODUCTS)
    run_sql(engine, "stg_orders",      STAGING_ORDERS)
    run_sql(engine, "stg_order_items", STAGING_ORDER_ITEMS)
    run_sql(engine, "stg_reviews",     STAGING_REVIEWS)
    run_sql(engine, "stg_payments",    STAGING_PAYMENTS)

    log.info("Transform hoàn thành!")

if __name__ == "__main__":
    main()
