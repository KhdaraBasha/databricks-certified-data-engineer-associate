
-- Selecting the catalog
USE CATALOG `lakeflow_pipeline`;

-- Create a streaming orders table in bronze layer.
CREATE OR REFRESH STREAMING TABLE `1_bronze_db`.tb_stre_orders_bronze_sql 
AS 
SELECT 
    *,
    _metadata.file_name as source_file_name,
    _metadata.file_path as source_file_path,
    _metadata.file_modification_time as source_file_modification_time,
    current_timestamp() as ingestion_ts
FROM STREAM read_files(
    '/Volumes/lakeflow_pipeline/default/data/orders/',
    format => 'json'
);

-- Create a streaming orders silver table in silver layer.
CREATE OR REFRESH STREAMING TABLE `2_silver_db`.tb_stre_orders_silver_sql 
AS 
SELECT 
    "order_id",
    timestamp("order_ts") as order_ts, 
    "customer_id",
    "product_id", 
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount_pct",
    "order_amount",
    "final_status",
    "channel"
FROM STREAM(`1_bronze_db`.tb_stre_orders_bronze_sql)
WHERE order_id is not null
;

-- Create the Materialized view for orders gold aggregations.
CREATE OR REFRESH MATERIALIZED VIEW `2_silver_db`.mv_orders_gold_sql
AS 
SELECT 
    order_ts,
    channel,
    count(distinct order_id) as total_orders,
    sum(order_amount) as total_amount,
    sum(order_amount * discount_pct) as total_discount,
    sum(order_amount * (1 - discount_pct)) as total_paid,
    avg(discount_pct) as avg_discount,
    avg(order_amount) as avg_order_amount,
    avg(quantity) as avg_quantity,
    avg(unit_price) as avg_unit_price
FROM `2_silver_db`.tb_stre_orders_silver_sql
GROUP BY order_ts, channel
;
