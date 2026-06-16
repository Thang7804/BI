
import os
import zipfile
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://olist:olist123@localhost:5434/olist_dw"
)
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATASET  = "olistbr/brazilian-ecommerce"

# Map tên file CSV → tên bảng raw
CSV_TABLE_MAP = {
    "olist_orders_dataset.csv":                    "orders",
    "olist_order_items_dataset.csv":               "order_items",
    "olist_customers_dataset.csv":                 "customers",
    "olist_products_dataset.csv":                  "products",
    "olist_sellers_dataset.csv":                   "sellers",
    "olist_order_reviews_dataset.csv":             "order_reviews",
    "olist_order_payments_dataset.csv":            "order_payments",
    "product_category_name_translation.csv":       "product_category_name_translation",
}

def download_dataset():
    """Download dataset từ Kaggle nếu chưa có."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DATA_DIR / "brazilian-ecommerce.zip"

    if any(DATA_DIR.glob("*.csv")):
        log.info("Dataset đã tồn tại, bỏ qua bước download.")
        return

    log.info("Đang download dataset từ Kaggle...")
    import kaggle  # import muộn để tránh lỗi nếu không cần download
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(DATASET, path=str(DATA_DIR), quiet=False)

    log.info("Đang giải nén...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DATA_DIR)
    zip_path.unlink()
    log.info("Giải nén xong.")

def truncate_raw(engine, table: str):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE raw.{table}"))

def load_csv_to_raw(engine):
    """Load từng CSV vào bảng raw tương ứng."""
    for csv_file, table_name in CSV_TABLE_MAP.items():
        csv_path = DATA_DIR / csv_file
        if not csv_path.exists():
            log.warning(f"Không tìm thấy file: {csv_path}, bỏ qua.")
            continue

        log.info(f"Loading {csv_file} → raw.{table_name}")
        df = pd.read_csv(csv_path, low_memory=False)
        log.info(f"  Rows: {len(df):,} | Cols: {list(df.columns)}")

        # Parse các cột timestamp nếu có
        ts_cols = [c for c in df.columns if "timestamp" in c or "date" in c]
        for col in ts_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        truncate_raw(engine, table_name)
        df.to_sql(
            table_name,
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )
        log.info(f"  ✓ Done: {len(df):,} rows → raw.{table_name}")

def main():
    download_dataset()

    log.info(f"Kết nối database: {DB_URL}")
    engine = create_engine(DB_URL, pool_pre_ping=True)

    load_csv_to_raw(engine)
    log.info("✅ Extract & Load hoàn thành!")

if __name__ == "__main__":
    main()
