-- =====================================================================
-- 01: AMAZON ATHENA DDL (EXTERNAL TABLES & PARTITION MANAGEMENT)
-- =====================================================================

-- 1. Create External Table for Raw JSON logs with Hive Partitioning
CREATE EXTERNAL TABLE IF NOT EXISTS aws_serverless_analytics_db_prod.raw_logs_json (
    request_id STRING,
    timestamp STRING,
    service_name STRING,
    endpoint STRING,
    http_method STRING,
    status_code INT,
    response_time_ms DOUBLE,
    user_id_hash STRING,
    client_ip STRING,
    aws_region STRING,
    device_type STRING,
    bytes_sent INT,
    error_message STRING
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'TRUE'
)
LOCATION 's3://aws-serverless-analytics-lake-prod-123456789012/raw_logs/'
TBLPROPERTIES ('has_encrypted_data'='true');

-- 2. Repair Partitions (Discovers all S3 partition prefixes automatically)
MSCK REPAIR TABLE aws_serverless_analytics_db_prod.raw_logs_json;

-- 3. Create Optimized Parquet Lakehouse Table (Snappy Compressed)
CREATE EXTERNAL TABLE IF NOT EXISTS aws_serverless_analytics_db_prod.processed_logs_parquet (
    request_id STRING,
    timestamp STRING,
    service_name STRING,
    endpoint STRING,
    http_method STRING,
    status_code INT,
    response_time_ms DOUBLE,
    user_id_hash STRING,
    client_ip STRING,
    aws_region STRING,
    device_type STRING,
    bytes_sent INT,
    error_message STRING,
    is_error INT,
    latency_category STRING
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
STORED AS PARQUET
LOCATION 's3://aws-serverless-analytics-lake-prod-123456789012/processed_parquet/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE aws_serverless_analytics_db_prod.processed_logs_parquet;
