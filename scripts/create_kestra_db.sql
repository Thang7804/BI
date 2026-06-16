-- create_kestra_db.sql
-- Chạy tự động trong docker-entrypoint-initdb.d (prefix 00_ -> trước init_db.sql)
-- Tạo database kestra_db cho Kestra backend
-- NOTE: Script này chỉ chạy khi data directory trống (fresh start)
CREATE DATABASE kestra_db;
GRANT ALL PRIVILEGES ON DATABASE kestra_db TO olist;
