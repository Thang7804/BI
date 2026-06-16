-- ============================================================
-- init_db.sql  –  Olist Data Warehouse initialization
-- Chạy tự động khi PostgreSQL container khởi động lần đầu
-- Tạo: raw (dữ liệu gốc) + staging (placeholder) +
--       dw (Star Schema) + reporting (aggregate)
-- ============================================================

-- ── Schemas ────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS reporting;

-- ============================================================
-- RAW LAYER – dữ liệu gốc từ CSV, không transform
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                      VARCHAR(32),
    customer_id                   VARCHAR(32),
    order_status                  VARCHAR(50),
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id            VARCHAR(32),
    order_item_id       INT,
    product_id          VARCHAR(32),
    seller_id           VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id              VARCHAR(32),
    customer_unique_id       VARCHAR(32),
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           CHAR(2)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id                 VARCHAR(32),
    product_category_name      VARCHAR(100),
    product_name_lenght        INT,
    product_description_lenght INT,
    product_photos_qty         INT,
    product_weight_g           NUMERIC,
    product_length_cm          NUMERIC,
    product_height_cm          NUMERIC,
    product_width_cm           NUMERIC
);

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id              VARCHAR(32),
    seller_zip_code_prefix VARCHAR(10),
    seller_city            VARCHAR(100),
    seller_state           CHAR(2)
);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id               VARCHAR(32),
    order_id                VARCHAR(32),
    review_score            INT,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id             VARCHAR(32),
    payment_sequential   INT,
    payment_type         VARCHAR(50),
    payment_installments INT,
    payment_value        NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS raw.product_category_name_translation (
    product_category_name         VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

-- ============================================================
-- DW LAYER – Star Schema (Dimension + Fact tables)
-- Populated by build_dw.py
-- ============================================================

-- ── Dimension: Customer ────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_id      VARCHAR(32) PRIMARY KEY,
    city             VARCHAR(100),
    state            CHAR(2),
    zip_code_prefix  VARCHAR(10)
);

-- ── Dimension: Seller ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_seller (
    seller_id VARCHAR(32) PRIMARY KEY,
    city      VARCHAR(100),
    state     CHAR(2)
);

-- ── Dimension: Product ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_id    VARCHAR(32) PRIMARY KEY,
    category_pt   VARCHAR(100),
    category_en   VARCHAR(100),
    weight_g      NUMERIC,
    length_cm     NUMERIC,
    height_cm     NUMERIC,
    width_cm      NUMERIC,
    photos_qty    INT
);

-- ── Dimension: Time ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_id      DATE PRIMARY KEY,
    year         INT,
    quarter      INT,
    month        INT,
    week         INT,
    day_of_week  INT   -- 0=Sunday … 6=Saturday (ISO: 1=Mon)
);

-- ── Dimension: Region (27 bang Brazil, seed trong build_dw.py) ──
CREATE TABLE IF NOT EXISTS dw.dim_region (
    state   CHAR(2) PRIMARY KEY,
    region  VARCHAR(50),
    country VARCHAR(50) DEFAULT 'Brazil'
);

-- ── Fact: Sales (1 row = 1 order_item) ────────────────────
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    -- Surrogate / degenerate keys
    order_id        VARCHAR(32),
    order_item_id   INT,
    -- Foreign keys → Dimensions
    customer_id     VARCHAR(32),   -- → dim_customer
    product_id      VARCHAR(32),   -- → dim_product
    seller_id       VARCHAR(32),   -- → dim_seller
    time_id         DATE,          -- → dim_time  (= purchased_at::date)
    customer_state  CHAR(2),       -- → dim_region (via state)
    -- Measures
    purchased_at    TIMESTAMP,
    price           NUMERIC(10,2),
    freight_value   NUMERIC(10,2),
    total_value     NUMERIC(10,2),
    delivery_days   DOUBLE PRECISION,
    is_on_time      BOOLEAN,
    review_score    INT,
    payment_type    VARCHAR(50),
    total_payment   NUMERIC(10,2),
    max_installments INT,
    -- Denormalized convenience columns (tránh JOIN thêm)
    category_en     VARCHAR(100),
    seller_state    CHAR(2),
    PRIMARY KEY (order_id, order_item_id)
);
