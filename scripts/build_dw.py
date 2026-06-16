"""
build_dw.py
Bước cuối của ETL pipeline: staging → dw (Star Schema) → reporting (aggregates)

Thứ tự chạy:
  1. Tạo/seed dim tables (dim_customer, dim_seller, dim_product, dim_time, dim_region)
  2. Build fact_sales (JOIN tất cả staging + dim_time)
  3. Build reporting.agg_* (aggregate tables phục vụ Metabase dashboard)
"""

import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist123@localhost:5434/olist_dw")

# ══════════════════════════════════════════════════════════════
# STEP 1 – DIMENSION TABLES (Star Schema)
# ══════════════════════════════════════════════════════════════

DIM_CUSTOMER = """
TRUNCATE TABLE dw.dim_customer;

INSERT INTO dw.dim_customer (customer_id, city, state, zip_code_prefix)
SELECT DISTINCT
    customer_id,
    city,
    state,
    zip_code_prefix
FROM staging.stg_customers
ON CONFLICT (customer_id) DO NOTHING;
"""

DIM_SELLER = """
TRUNCATE TABLE dw.dim_seller;

INSERT INTO dw.dim_seller (seller_id, city, state)
SELECT DISTINCT
    seller_id,
    city,
    state
FROM staging.stg_sellers
ON CONFLICT (seller_id) DO NOTHING;
"""

DIM_PRODUCT = """
TRUNCATE TABLE dw.dim_product;

INSERT INTO dw.dim_product
    (product_id, category_pt, category_en, weight_g, length_cm, height_cm, width_cm, photos_qty)
SELECT DISTINCT
    product_id,
    category_pt,
    category_en,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    photos_qty
FROM staging.stg_products
ON CONFLICT (product_id) DO NOTHING;
"""

# Tạo dim_time từ toàn bộ ngày mua hàng trong staging.stg_orders
DIM_TIME = """
TRUNCATE TABLE dw.dim_time;

INSERT INTO dw.dim_time (time_id, year, quarter, month, week, day_of_week)
SELECT DISTINCT
    d::date                               AS time_id,
    EXTRACT(YEAR    FROM d)::INT          AS year,
    EXTRACT(QUARTER FROM d)::INT          AS quarter,
    EXTRACT(MONTH   FROM d)::INT          AS month,
    EXTRACT(WEEK    FROM d)::INT          AS week,
    EXTRACT(DOW     FROM d)::INT          AS day_of_week  -- 0=Sun..6=Sat
FROM (
    SELECT DISTINCT purchased_at::date AS d
    FROM staging.stg_orders
    WHERE purchased_at IS NOT NULL
) t
ON CONFLICT (time_id) DO NOTHING;
"""

# 27 bang Brazil với mapping region
DIM_REGION_SEED = """
TRUNCATE TABLE dw.dim_region;

INSERT INTO dw.dim_region (state, region, country) VALUES
('AC', 'Norte',           'Brazil'),
('AL', 'Nordeste',        'Brazil'),
('AM', 'Norte',           'Brazil'),
('AP', 'Norte',           'Brazil'),
('BA', 'Nordeste',        'Brazil'),
('CE', 'Nordeste',        'Brazil'),
('DF', 'Centro-Oeste',    'Brazil'),
('ES', 'Sudeste',         'Brazil'),
('GO', 'Centro-Oeste',    'Brazil'),
('MA', 'Nordeste',        'Brazil'),
('MG', 'Sudeste',         'Brazil'),
('MS', 'Centro-Oeste',    'Brazil'),
('MT', 'Centro-Oeste',    'Brazil'),
('PA', 'Norte',           'Brazil'),
('PB', 'Nordeste',        'Brazil'),
('PE', 'Nordeste',        'Brazil'),
('PI', 'Nordeste',        'Brazil'),
('PR', 'Sul',             'Brazil'),
('RJ', 'Sudeste',         'Brazil'),
('RN', 'Nordeste',        'Brazil'),
('RO', 'Norte',           'Brazil'),
('RR', 'Norte',           'Brazil'),
('RS', 'Sul',             'Brazil'),
('SC', 'Sul',             'Brazil'),
('SE', 'Nordeste',        'Brazil'),
('SP', 'Sudeste',         'Brazil'),
('TO', 'Norte',           'Brazil')
ON CONFLICT (state) DO NOTHING;
"""

# ══════════════════════════════════════════════════════════════
# STEP 2 – FACT TABLE  (1 row = 1 order_item)
# ══════════════════════════════════════════════════════════════

FACT_SALES = """
TRUNCATE TABLE dw.fact_sales;

INSERT INTO dw.fact_sales (
    order_id, order_item_id,
    customer_id, product_id, seller_id,
    time_id, customer_state,
    purchased_at,
    price, freight_value, total_value,
    delivery_days, is_on_time,
    review_score,
    payment_type, total_payment, max_installments,
    category_en, seller_state
)
SELECT
    oi.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    o.purchased_at::date                                             AS time_id,
    o.customer_state,
    o.purchased_at,
    oi.price,
    oi.freight_value,
    oi.total_value,
    o.delivery_days,
    o.is_on_time,
    COALESCE(r.review_score, 0)                                      AS review_score,
    p.payment_type,
    p.total_payment,
    p.max_installments,
    oi.category                                                      AS category_en,
    s.state                                                          AS seller_state
FROM staging.stg_order_items oi
JOIN staging.stg_orders      o  USING (order_id)
JOIN staging.stg_sellers     s  USING (seller_id)
LEFT JOIN staging.stg_reviews    r  USING (order_id)
LEFT JOIN staging.stg_payments   p  USING (order_id)
ON CONFLICT (order_id, order_item_id) DO NOTHING;
"""




def run_sql(engine, label: str, sql: str):
    log.info(f"  → Building: {label}")
    with engine.begin() as conn:
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
    log.info(f"  ✓ {label} done")



# DDL – tạo dim/fact tables nếu chưa tồn tại (chạy trước TRUNCATE)
CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_id      VARCHAR(32) PRIMARY KEY,
    city             VARCHAR(100),
    state            CHAR(2),
    zip_code_prefix  VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dw.dim_seller (
    seller_id VARCHAR(32) PRIMARY KEY,
    city      VARCHAR(100),
    state     CHAR(2)
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_id  VARCHAR(32) PRIMARY KEY,
    category_pt VARCHAR(100),
    category_en VARCHAR(100),
    weight_g    NUMERIC,
    length_cm   NUMERIC,
    height_cm   NUMERIC,
    width_cm    NUMERIC,
    photos_qty  INT
);

CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_id     DATE PRIMARY KEY,
    year        INT,
    quarter     INT,
    month       INT,
    week        INT,
    day_of_week INT
);

CREATE TABLE IF NOT EXISTS dw.dim_region (
    state   CHAR(2) PRIMARY KEY,
    region  VARCHAR(50),
    country VARCHAR(50) DEFAULT 'Brazil'
);

CREATE TABLE IF NOT EXISTS dw.fact_sales (
    order_id         VARCHAR(32),
    order_item_id    INT,
    customer_id      VARCHAR(32),
    product_id       VARCHAR(32),
    seller_id        VARCHAR(32),
    time_id          DATE,
    customer_state   CHAR(2),
    purchased_at     TIMESTAMP,
    price            NUMERIC(10,2),
    freight_value    NUMERIC(10,2),
    total_value      NUMERIC(10,2),
    delivery_days    DOUBLE PRECISION,
    is_on_time       BOOLEAN,
    review_score     INT,
    payment_type     VARCHAR(50),
    total_payment    NUMERIC(10,2),
    max_installments INT,
    category_en      VARCHAR(100),
    seller_state     CHAR(2),
    PRIMARY KEY (order_id, order_item_id)
);
"""


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)

    # Đảm bảo schemas tồn tại
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw"))

    # Tạo dim/fact tables nếu chưa có (idempotent — an toàn khi chạy lại)
    log.info("Ensuring dim/fact tables exist...")
    run_sql(engine, "create_tables", CREATE_TABLES)

    log.info("━━━ STEP 1: Building Dimension Tables ━━━")
    run_sql(engine, "dim_customer", DIM_CUSTOMER)
    run_sql(engine, "dim_seller",   DIM_SELLER)
    run_sql(engine, "dim_product",  DIM_PRODUCT)
    run_sql(engine, "dim_time",     DIM_TIME)
    run_sql(engine, "dim_region",   DIM_REGION_SEED)

    log.info("━━━ STEP 2: Building Fact Table ━━━")
    run_sql(engine, "fact_sales",   FACT_SALES)

    log.info("✅ build_dw.py hoàn thành! dw (Star Schema) sẵn sàng.")


if __name__ == "__main__":
    main()

