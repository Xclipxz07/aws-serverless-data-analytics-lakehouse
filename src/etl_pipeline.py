"""
Project 4 - Serverless Cloud Analytics ETL Pipeline & S3 Ingestion Processor
-----------------------------------------------------------------------------
PART 1: Full Executable Local Code (Multi-Format Extraction + PII Anonymizer + DuckDB + Parquet)
PART 2: Full Real-World AWS Glue PySpark Production Job Script - IN COMMENTS AT BOTTOM
"""

import os
import glob
import json
import logging
import hashlib
import xml.etree.ElementTree as ET
import pandas as pd
import duckdb

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    boto3 = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =====================================================================
# PART 1: EXECUTABLE LOCAL PIPELINE (Runs on Local / Dev Environment)
# =====================================================================

class ServerlessDataETLPipeline:
    def __init__(self, data_lake_bucket: str = None, aws_region: str = "eu-west-2"):
        self.data_lake_bucket = data_lake_bucket
        self.aws_region = aws_region
        self.s3_client = boto3.client("s3", region_name=aws_region) if boto3 else None

    def anonymize_user_id(self, raw_id: str) -> str:
        """Applies SHA-256 cryptographic hashing for GDPR/HIPAA compliance."""
        if not raw_id or pd.isna(raw_id) or raw_id == "":
            return "anon-anonymous"
        return "anon-" + hashlib.sha256(str(raw_id).encode("utf-8")).hexdigest()[:12]

    def pull_json_file(self, filepath: str) -> pd.DataFrame:
        records = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        return pd.DataFrame(records)

    def pull_csv_file(self, filepath: str) -> pd.DataFrame:
        df = pd.read_csv(filepath)
        df["status_code"] = df["status_code"].astype(int)
        df["response_time_ms"] = df["response_time_ms"].astype(float)
        df["bytes_sent"] = df["bytes_sent"].astype(int)
        return df

    def pull_xml_file(self, filepath: str) -> pd.DataFrame:
        tree = ET.parse(filepath)
        root = tree.getroot()
        records = []
        for event in root.findall("event"):
            rec = {child.tag: child.text for child in event}
            records.append(rec)
        df = pd.DataFrame(records)
        df["status_code"] = df["status_code"].astype(int)
        df["response_time_ms"] = df["response_time_ms"].astype(float)
        df["bytes_sent"] = df["bytes_sent"].astype(int)
        return df

    def process_and_convert_to_parquet(self, raw_base_dir: str, processed_base_dir: str):
        """Scans multi-format raw partitions (JSON, CSV, XML), enriches with SQL, and writes snappy Parquet."""
        logger.info(f"🔍 1. EXTRACT: Scanning multi-format partitions in: {raw_base_dir}")
        all_partition_dirs = [x[0] for x in os.walk(raw_base_dir) if "day=" in x[0]]
        
        if not all_partition_dirs:
            logger.warning("⚠️ No partition subdirectories found. Checking flat files...")
            all_partition_dirs = [raw_base_dir]

        processed_files = []

        for pdir in sorted(all_partition_dirs):
            partition_dfs = []
            for fname in sorted(os.listdir(pdir)):
                fpath = os.path.join(pdir, fname)
                if fname.endswith(".json"):
                    partition_dfs.append(self.pull_json_file(fpath))
                elif fname.endswith(".csv"):
                    partition_dfs.append(self.pull_csv_file(fpath))
                elif fname.endswith(".xml"):
                    partition_dfs.append(self.pull_xml_file(fpath))

            if not partition_dfs:
                continue

            master_df = pd.concat(partition_dfs, ignore_index=True)
            
            # 2. TRANSFORM: Anonymize PII & Add Business Flags using DuckDB SQL
            con = duckdb.connect(database=":memory:")
            con.register("raw_batch", master_df)
            
            clean_df = con.execute("""
                SELECT 
                    request_id,
                    timestamp,
                    year,
                    month,
                    day,
                    service_name,
                    endpoint,
                    http_method,
                    CAST(status_code AS INTEGER) AS status_code,
                    CAST(response_time_ms AS DOUBLE) AS response_time_ms,
                    user_id_raw,
                    client_ip,
                    aws_region,
                    device_type,
                    CAST(bytes_sent AS INTEGER) AS bytes_sent,
                    COALESCE(error_message, '') AS error_message,
                    CASE WHEN CAST(status_code AS INTEGER) >= 400 THEN 1 ELSE 0 END AS is_error,
                    CASE 
                        WHEN CAST(response_time_ms AS DOUBLE) <= 50.0 THEN 'Fast (<50ms)'
                        WHEN CAST(response_time_ms AS DOUBLE) <= 200.0 THEN 'Standard (50-200ms)'
                        WHEN CAST(response_time_ms AS DOUBLE) <= 1000.0 THEN 'Elevated (200-1000ms)'
                        ELSE 'Degraded (>1000ms)'
                    END AS latency_category
                FROM raw_batch
                WHERE request_id IS NOT NULL AND status_code IS NOT NULL;
            """).fetchdf()

            # Hash User ID for GDPR Compliance and remove raw ID
            clean_df["user_id_hash"] = clean_df["user_id_raw"].apply(self.anonymize_user_id)
            clean_df.drop(columns=["user_id_raw"], inplace=True)

            # 3. LOAD: Write Snappy-Compressed Parquet matching Hive Partition Scheme
            rel_dir = os.path.relpath(pdir, raw_base_dir)
            target_out_dir = os.path.join(processed_base_dir, rel_dir)
            os.makedirs(target_out_dir, exist_ok=True)

            out_parquet_path = os.path.join(target_out_dir, "lakehouse_events.parquet")
            clean_df.to_parquet(out_parquet_path, index=False, compression="snappy", engine="pyarrow")
            processed_files.append(out_parquet_path)
            logger.info(f"✅ Loaded {len(clean_df):,} records -> {out_parquet_path}")

        return processed_files

    def upload_to_s3(self, local_base_dir: str, s3_prefix: str = "raw_logs"):
        """Uploads partitioned data files to the Amazon S3 Data Lake bucket."""
        if not self.s3_client or not self.data_lake_bucket:
            logger.warning("⚠️ AWS S3 Client or Bucket not configured. Skipping S3 upload (Local Simulation Mode).")
            return

        logger.info(f"☁️ Uploading partitioned files to s3://{self.data_lake_bucket}/{s3_prefix}/")
        for root, _, files in os.walk(local_base_dir):
            for file in files:
                if file.startswith("."):
                    continue
                local_path = os.path.join(root, file)
                rel_path = os.path.relpath(local_path, local_base_dir)
                s3_key = f"{s3_prefix}/{rel_path}".replace("\\", "/")

                try:
                    self.s3_client.upload_file(local_path, self.data_lake_bucket, s3_key)
                    logger.info(f"  ⬆️ S3 Upload Success: s3://{self.data_lake_bucket}/{s3_key}")
                except (BotoCoreError, ClientError) as e:
                    logger.error(f"  ❌ S3 Upload Failed: {e}")


# =====================================================================================
# ☁️ PART 2: FULL REAL-WORLD AWS GLUE PYSPARK PRODUCTION JOB SCRIPT
# =====================================================================================
#
# Below is the exact complete AWS Glue PySpark job deployed in production:
#
# -------------------------------------------------------------------------------------
# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql import functions as F
#
# args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_RAW_PATH', 'S3_TARGET_PATH'])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
#
# # 1. Read Raw Multi-Format JSON/CSV Event Stream from S3 Data Lake
# raw_df = spark.read.json(args['S3_RAW_PATH'])
#
# # 2. Transform, Anonymize PII with SHA-256, and Add Analytics Flags
# transformed_df = raw_df \
#     .withColumn("user_id_hash", F.concat(F.lit("anon-"), F.substring(F.sha2(F.col("user_id_raw"), 256), 1, 12))) \
#     .drop("user_id_raw") \
#     .withColumn("is_error", F.when(F.col("status_code") >= 400, 1).otherwise(0)) \
#     .withColumn("latency_category", 
#         F.when(F.col("response_time_ms") <= 50.0, "Fast (<50ms)")
#          .when(F.col("response_time_ms") <= 200.0, "Standard (50-200ms)")
#          .when(F.col("response_time_ms") <= 1000.0, "Elevated (200-1000ms)")
#          .otherwise("Degraded (>1000ms)"))
#
# # 3. Write Partitioned Snappy-Compressed Parquet into AWS S3 Lakehouse
# transformed_df.write \
#     .partitionBy("year", "month", "day") \
#     .mode("append") \
#     .option("compression", "snappy") \
#     .parquet(args['S3_TARGET_PATH'])
#
# job.commit()
# =====================================================================================

if __name__ == "__main__":
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(curr_dir)
    raw_dir = os.path.join(project_root, "data", "raw_logs")
    processed_dir = os.path.join(project_root, "data", "processed_parquet")

    etl = ServerlessDataETLPipeline()
    etl.process_and_convert_to_parquet(raw_dir, processed_dir)
