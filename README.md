# Olist E-Commerce Business Intelligence Pipeline

End-to-end **Data Warehouse + ETL pipeline** phân tích **~112k orders** từ dataset thương mại điện tử Brazil (Olist), orchestrated bởi **Kestra**, lưu trữ trên **PostgreSQL**, visualize bằng **Metabase**.

---

## Giới thiệu bài toán

Olist là nền tảng thương mại điện tử lớn tại Brazil, kết nối hàng nghìn seller với hàng triệu khách hàng trên toàn quốc. Dataset này chứa dữ liệu thực từ 2016–2018, bao gồm đơn hàng, sản phẩm, seller, khách hàng, đánh giá và thanh toán.

**Mục tiêu phân tích:**
- Doanh thu biến động theo thời gian như thế nào?
- Danh mục sản phẩm nào đóng góp doanh thu lớn nhất?
- Khu vực (bang) nào có hiệu suất mua sắm cao nhất?
- Seller nào đang hoạt động tốt nhất?
- Chất lượng giao hàng ảnh hưởng thế nào đến review score?

---

## Mô tả Dataset

| Bảng CSV | Số dòng (xấp xỉ) | Mô tả |
|---|---|---|
| `olist_orders_dataset.csv` | ~99.441 | Thông tin đơn hàng |
| `olist_order_items_dataset.csv` | ~112.650 | Chi tiết sản phẩm trong đơn |
| `olist_customers_dataset.csv` | ~99.441 | Thông tin khách hàng |
| `olist_products_dataset.csv` | ~32.951 | Danh mục sản phẩm |
| `olist_sellers_dataset.csv` | ~3.095 | Thông tin seller |
| `olist_order_reviews_dataset.csv` | ~100.000 | Đánh giá của khách |
| `olist_order_payments_dataset.csv` | ~103.886 | Thanh toán |
| `olist_geolocation_dataset.csv` | ~1M+ | Tọa độ ZIP code |
| `product_category_name_translation.csv` | ~71 | Dịch tên danh mục PT→EN |

- **Nguồn**: [Kaggle – Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Thời gian**: 09/2016 – 10/2018
- **Phạm vi**: 27 bang Brazil, 73 danh mục sản phẩm

---

## Kiến trúc Data Warehouse

### Thiết kế 4 tầng (Layer Design)

```
[Kaggle CSV]
     │
     ▼  extract_load.py
┌─────────────────────────────────────────┐
│  raw.*  – Dữ liệu gốc từ CSV           │
│  raw.orders, raw.order_items,           │
│  raw.customers, raw.products,           │
│  raw.sellers, raw.order_reviews,        │
│  raw.order_payments, raw.sellers        │
└─────────────────────────────────────────┘
     │
     ▼  transform.py
┌─────────────────────────────────────────┐
│  staging.stg_*  – Cleaned, cast type   │
│  stg_orders (delivery_days, is_on_time) │
│  stg_order_items (category_en)          │
│  stg_reviews, stg_payments              │
└─────────────────────────────────────────┘
     │
     ▼  build_dw.py  (Step 1 + 2)
┌─────────────────────────────────────────┐
│  dw.*  – Star Schema                   │
│  dim_customer, dim_seller, dim_product  │
│  dim_time, dim_region                   │
│  fact_sales                             │
└─────────────────────────────────────────┘
     │
     ▼  build_dw.py  (Step 3)
┌─────────────────────────────────────────┐
│  reporting.*  – Aggregates (Dashboard) │
│  agg_monthly_revenue                    │
│  agg_category_performance               │
│  agg_state_revenue                      │
│  agg_seller_performance                 │
│  agg_payment_analysis                   │
│  agg_delivery_analysis                  │
└─────────────────────────────────────────┘
     │
     ▼
[Metabase Dashboard]
```

### Star Schema (dw layer)

```
                    ┌──────────────┐
                    │  dim_time    │
                    │  time_id(PK) │
                    │  year        │
                    │  quarter     │
                    │  month       │
                    │  week        │
                    │  day_of_week │
                    └──────┬───────┘
                           │
 ┌──────────────┐    ┌─────┴──────────────────────────────┐    ┌──────────────────┐
 │ dim_customer │    │            fact_sales              │    │   dim_product    │
 │ customer_id  │◄───┤  order_id + order_item_id (PK)     ├───►│  product_id (PK) │
 │ city         │    │  customer_id  → dim_customer       │    │  category_pt     │
 │ state        │    │  product_id   → dim_product        │    │  category_en     │
 │ zip_code_pfx │    │  seller_id    → dim_seller         │    │  weight_g        │
 └──────────────┘    │  time_id      → dim_time           │    │  length_cm       │
                     │  customer_state → dim_region       │    │  height_cm       │
 ┌──────────────┐    │  ─────── Measures ──────────────   │    │  width_cm        │
 │  dim_seller  │◄───┤  price, freight_value, total_value │    └──────────────────┘
 │  seller_id   │    │  delivery_days, is_on_time         │
 │  city        │    │  review_score                      │    ┌──────────────────┐
 │  state       │    │  payment_type, total_payment       │    │   dim_region     │
 └──────────────┘    └────────────────┬───────────────────┘    │  state (PK)      │
                                      │                        │  region          │
                                      └───────────────────────►│  country         │
                                                               └──────────────────┘
```

---

## Tech Stack

| Thành phần | Công nghệ |
|---|---|
| Orchestration | Kestra (YAML-based, self-hosted) |
| Language | Python 3.11, SQL |
| Database | PostgreSQL 15 |
| Visualization | Metabase |
| Containerization | Docker Compose |
| Dataset | Olist Brazilian E-Commerce (~112k order items, 9 bảng) |

---

## Cấu trúc Project

```
olist-de-pipeline/
├── docker-compose.yml              # Kestra + PostgreSQL + Metabase
├── requirements.txt
├── .env                            # Cấu hình biến môi trường (DATABASE_URL, DATA_DIR)
├── scripts/
│   ├── init_db.sql                 # Tạo schemas + raw tables + dw DDL
│   ├── create_kestra_db.sh         # Tạo kestra_db cho Kestra backend
│   ├── extract_load.py             # Download CSV từ Kaggle → raw schema
│   ├── transform.py                # raw → staging (clean, cast type)
│   └── build_dw.py                 # staging → dw (dim+fact) → reporting (agg)
└── kestra/
    └── flows/
        └── olist_etl_pipeline.yml  # Main Kestra flow (orchestrates toàn bộ)
```

---

## Cách chạy project

### 1. Chuẩn bị

```bash
git clone <repo>
cd olist-de-pipeline

# Cấu hình file .env với DATABASE_URL và các biến môi trường cần thiết
# (Nếu chạy Kaggle API cần thêm cấu hình Kaggle token hoặc điền vào Kestra Secrets)
```

### 2. Khởi động services

```bash
docker compose up -d
```

Chờ ~30 giây rồi kiểm tra:

| Service | URL |
|---|---|
| **Kestra UI** | http://localhost:8080 |
| **Metabase** | http://localhost:3000 |
| **PostgreSQL** | `localhost:5434` (host port map → container 5432) |

> **Lưu ý**: PostgreSQL được map ra host port **5434** (không phải 5432) để tránh xung đột với PostgreSQL local.

### 3. Chạy ETL pipeline

**Cách A – Chạy thủ công qua Python (dev/test):**

```bash
pip install -r requirements.txt

# Bước 1: Download Kaggle dataset → load vào raw schema
python scripts/extract_load.py

# Bước 2: raw → staging (clean, cast type, tính delivery_days)
python scripts/transform.py

# Bước 3: staging → dw (dim+fact) → reporting (agg)
python scripts/build_dw.py
```

**Cách B – Chạy qua Kestra UI (production/scheduled):**

1. Vào http://localhost:8080 → **Flows** → Import flow
2. Upload file `kestra/flows/olist_etl_pipeline.yml`
3. Thêm secrets tại **Settings → Secrets**: `KAGGLE_USERNAME`, `KAGGLE_KEY`
4. Click **Execute** để chạy manual, hoặc enable trigger `daily_schedule`

### 4. Setup Metabase Dashboard

1. Vào http://localhost:3000 → tạo account admin
2. **Add Database**:
   - Type: `PostgreSQL`
   - Host: `postgres` *(hostname trong Docker network)*
   - Port: `5432`
   - Database: `olist_dw`
   - Username: `olist`
   - Password: `olist123`
3. Tạo các chart từ schema **`reporting`**:

| Chart | Bảng | Gợi ý |
|---|---|---|
| Line chart – Doanh thu theo tháng | `reporting.agg_monthly_revenue` | `month` vs `revenue` |
| Bar chart – Top category | `reporting.agg_category_performance` | `category` vs `revenue` (top 10) |
| Table – Top sellers | `reporting.agg_seller_performance` | sort by `total_revenue` |
| Map – Doanh thu theo bang | `reporting.agg_state_revenue` | `state` vs `total_revenue` |
| Pie – Phương thức thanh toán | `reporting.agg_payment_analysis` | `payment_type` vs `pct_orders` |
| Bar – Thời gian giao hàng | `reporting.agg_delivery_analysis` | `state` vs `avg_delivery_days` |

---

## Quy trình ETL chi tiết

```
extract_load.py
  └─ Download từ Kaggle (hoặc đọc /data nếu đã có)
  └─ pd.read_csv → raw.orders, raw.order_items, raw.customers,
                   raw.products, raw.sellers, raw.order_reviews,
                   raw.order_payments, raw.product_category_name_translation

transform.py
  └─ raw.orders + raw.customers → staging.stg_orders
       (cast timestamp, tính delivery_days, is_on_time, JOIN city/state)
  └─ raw.order_items + raw.products + raw.sellers → staging.stg_order_items
       (JOIN translation → category_en)
  └─ raw.order_reviews → staging.stg_reviews
  └─ raw.order_payments → staging.stg_payments (GROUP BY order_id)

build_dw.py
  STEP 1 – Dimension tables (Star Schema)
    └─ dw.dim_customer  ← raw.customers
    └─ dw.dim_seller    ← raw.sellers
    └─ dw.dim_product   ← raw.products + translation
    └─ dw.dim_time      ← dates từ raw.orders
    └─ dw.dim_region    ← seed 27 bang Brazil hardcoded

  STEP 2 – Fact table
    └─ dw.fact_sales ← JOIN stg_order_items + stg_orders
                            + stg_reviews + stg_payments

  STEP 3 – Reporting aggregates
    └─ reporting.agg_monthly_revenue
    └─ reporting.agg_category_performance
    └─ reporting.agg_state_revenue
    └─ reporting.agg_seller_performance
    └─ reporting.agg_payment_analysis
    └─ reporting.agg_delivery_analysis
```

---

## Kết quả pipeline

Sau khi chạy thành công:

### dw schema (Star Schema)
| Bảng | Mô tả |
|---|---|
| `dw.dim_customer` | ~99k customers |
| `dw.dim_seller` | ~3k sellers |
| `dw.dim_product` | ~33k products |
| `dw.dim_time` | ~700+ ngày giao dịch |
| `dw.dim_region` | 27 bang Brazil (seeded) |
| `dw.fact_sales` | ~112k rows – 1 row = 1 order item |

### reporting schema (Aggregates cho Dashboard)
| Bảng | Mô tả |
|---|---|
| `reporting.agg_monthly_revenue` | Doanh thu, đơn hàng, review score theo tháng |
| `reporting.agg_category_performance` | Hiệu suất từng danh mục sản phẩm |
| `reporting.agg_state_revenue` | Doanh thu + region theo bang |
| `reporting.agg_seller_performance` | Top sellers theo doanh thu |
| `reporting.agg_payment_analysis` | Phân tích phương thức thanh toán |
| `reporting.agg_delivery_analysis` | Thống kê giao hàng theo bang |

---

## Insight chính từ dữ liệu

1. **Tăng trưởng doanh thu mạnh**: Doanh thu tăng liên tục từ cuối 2016 đến Q1 2018, đỉnh điểm vào tháng 11/2017 (Black Friday Brazil).

2. **Top category**: `bed_bath_table`, `health_beauty`, `sports_leisure` là 3 danh mục dẫn đầu doanh thu, chiếm ~30% tổng GMV.

3. **SP dẫn đầu tuyệt đối**: Bang São Paulo chiếm ~40% doanh thu cả nước, tiếp theo là RJ và MG. Khu vực Sudeste chiếm >60% tổng doanh thu.

4. **Giao hàng là điểm yếu**: Thời gian giao hàng trung bình ~12 ngày. Các bang vùng Norte (AC, AM, RR) có thời gian giao hàng dài nhất (>20 ngày), on-time rate thấp.

5. **Thanh toán**: Credit card chiếm ~74% giao dịch. Trả góp phổ biến với trung bình ~3.7 kỳ hạn.

6. **Review score**: Trung bình 4.07/5. Đơn hàng giao đúng hạn có score trung bình cao hơn 0.5 điểm so với giao trễ.

---

## Data Quality Checks

Pipeline có DQ check tự động qua Kestra:

- `fact_sales` phải có > 90.000 rows
- Không có `order_id` null trong fact_sales
- Doanh thu tháng cao nhất phải > 0

---

## Mở rộng tiếp theo

- [ ] Incremental load (chỉ load data mới thay vì full refresh)
- [ ] Tích hợp dbt cho transform layer
- [ ] Deploy lên GCP (Cloud Run + Cloud SQL + Looker Studio)
- [ ] Alert email khi DQ check fail
- [ ] Thêm geolocation map với tọa độ thực tế
