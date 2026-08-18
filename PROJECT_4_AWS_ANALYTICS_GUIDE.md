# 🚀 Project 4: AWS Serverless Cloud Data Analytics & SLA Monitoring — Master Learning Guide

Welcome to your complete reference guide for **Project 4: AWS Serverless Cloud Data Analytics** (`project4_aws_serverless_data_analytics`)! This document contains everything covered, explained step-by-step in simple terms with real-world analogies, file breakdowns, and cloud architecture timelines.

---

## 📑 Table of Contents
1. [Big Picture & Architecture](#1-big-picture--architecture)
2. [Core Industry Concepts Explained Simply](#2-core-industry-concepts-explained-simply)
3. [File-by-File Line-by-Line Breakdown](#3-file-by-file-line-by-line-breakdown)
4. [Local vs. Real-World AWS Production Code Comparison](#4-local-vs-real-world-aws-production-code-comparison)
5. [Quick Command Reference](#5-quick-command-reference)

---

## 1. Big Picture & Architecture

```text
[Raw Multi-Source Ingestion]         [ETL & Anonymization Engine]       [Serverless Storage & Query]       [Executive Dashboards]
JSON, CSV, XML Operational Logs ---> SHA-256 PII Hashing          ---> Snappy-Compressed Parquet  ---> Amazon QuickSight (SPICE)
(Web, Mobile, Microservices)         DuckDB / AWS Glue PySpark         Amazon Athena Serverless SQL      (SLA KPIs, Latency Trends)
                                                 ^
                                                 | Orchestrated By
                                       Apache Airflow DAG (Nightly @ 2 AM)
```

**Scenario**: You are a Cloud Data Engineer at a high-growth tech enterprise. Millions of operational API logs and transactional records arrive every minute from mobile apps, web checkouts, microservices, and external webhooks.
Your goal is to build an automated serverless analytics pipeline that:
1. Ingests raw multi-format event logs (`JSON`, `CSV`, `XML`) into an **Amazon S3 Data Lake**.
2. Automatically pseudonymizes user PII with **SHA-256** for GDPR/HIPAA compliance.
3. Converts row data into columnar **Snappy Parquet** partitioned by `year/month/day` to slash Athena query scan costs by **80%+**.
4. Automatically discovers schema evolutions via **AWS Glue Crawlers** and registers metadata in the **AWS Glue Data Catalog**.
5. Runs the 4 tiers of cloud analytics (**Descriptive, Diagnostic, Predictive, Prescriptive**) with **Amazon Athena**.
6. Delivers live executive BI dashboards in **Amazon QuickSight** powered by in-memory **SPICE**.

---

## 2. Core Industry Concepts Explained Simply

### 🔹 What is a Serverless Data Lakehouse?
A **Serverless Data Lakehouse** decouples storage from compute:
- **Storage**: Infinite, low-cost object storage (**Amazon S3**).
- **Compute**: On-demand SQL query engine (**Amazon Athena**) that charges **$0** when idle and only **$5.00 per TB** of data scanned.

### 🔹 Why convert JSON / CSV / XML to Parquet?
Raw text files (`.json`, `.csv`, `.xml`) are row-based. To calculate average latency for one service, Athena must scan the entire file across the network.
Converting to **Parquet**:
- Compresses file size by **75% to 90%** with Snappy compression.
- Organizes data into columns so Athena only scans the specific columns requested in the `SELECT` statement (**10x to 50x cheaper and faster**).

### 🔹 What is Hive Partitioning (Partition Pruning)?
Organizing S3 files into folder prefixes: `s3://bucket/raw_logs/year=2026/month=07/day=27/`.
When you write `WHERE year = '2026' AND month = '07'`, Athena skips all other months and years entirely. This is called **Partition Pruning**.

### 🔹 What is AWS Glue Data Catalog & Crawler?
- **Glue Crawler**: A robot that inspects files in S3, figures out the schema (column names and data types), detects newly added date partitions, and creates/updates tables.
- **Glue Data Catalog**: A centralized metadata repository (Hive Metastore) shared across Athena, EMR, Redshift, and QuickSight.

### 🔹 The 4 Pillars of Data Analytics Explained
1. **Descriptive Analytics (*What happened?*)**: Calculates total requests, unique active users, error percentages, and latency averages.
2. **Diagnostic Analytics (*Why did it happen?*)**: Identifies the root cause of failures (e.g. isolating peak-hour 503 timeouts to the `payment-gateway`).
3. **Predictive Analytics (*What will happen?*)**: Uses rolling 3-hour window functions (`AVG() OVER (...)`) to forecast outages and calculate user churn risk scores.
4. **Prescriptive Analytics (*How can we take action?*)**: Auto-generates infrastructure directives (e.g., scale ECS tasks if P99 $> 500$ ms) and generates AWS WAF IP blocklists.

### 🔹 GDPR / HIPAA PII Protection
Storing raw user IDs in cleartext is illegal under GDPR. We apply **SHA-256 Cryptographic Hashing** (`hashlib.sha256`) to create pseudonymized tokens (`anon-9f21a4b2c1d8`) before data enters the lakehouse.

---

## 3. File-by-File Line-by-Line Breakdown

### 📄 File 1: `data_generator/generate_data.py`
Simulates realistic multi-tenant operational logs across 3 formats (`JSON`, `CSV`, `XML`) partitioned by date.
- **Lines 15–30**: Microservice taxonomy (`auth-service`, `payment-gateway`, etc.) and HTTP error templates (400, 401, 403, 500, 503).
- **Lines 32–60 (`get_status_and_error`, `calculate_latency`)**: Injects realistic peak-hour traffic spikes (14:00–16:00 UTC) and latency variations.
- **Lines 62–95 (`generate_single_event`)**: Constructs single event dictionary with request IDs, timestamps, user IDs, and client IPs.
- **Lines 97–145 (`generate_multi_day_dataset`)**: Generates 5 days of partitioned files split across JSON (60%), CSV (25%), and XML (15%).

---

### 📄 File 2: `src/etl_pipeline.py`
The Extract, Transform, Load (ETL) processing engine.
- **Lines 35–65 (`pull_*_file`)**: Reader functions converting JSON, CSV, and XML into unified DataFrames.
- **Lines 67–125 (`process_and_convert_to_parquet`)**: Scans multi-format partitions, executes DuckDB SQL transformations, applies SHA-256 PII anonymization, and exports Snappy Parquet.
- **Lines 127–148 (`upload_to_s3`)**: Synchronizes local partitioned data files to the Amazon S3 Data Lake bucket.
- **Lines 152–195 (In Comments)**: Complete Real-World AWS Glue PySpark production script.

---

### 📄 File 3: `src/athena_runner.py`
Amazon Athena Boto3 execution engine.
- **Lines 25–65 (`execute_query`)**: Submits SQL queries to Athena workgroup, polls status, calculates scanned megabytes, and tracks execution cost in USD.
- **Lines 67–90 (`_fetch_results_dataframe`)**: Paginates through Athena result sets and returns clean Pandas DataFrames.

---

### 📄 File 4: `src/local_simulation.py`
Zero-cloud-cost offline execution engine.
- Mounts partitioned raw JSON and processed Parquet files into **DuckDB**.
- Executes all 4 analytical query tiers (Descriptive, Diagnostic, Predictive rolling trends, Prescriptive actions) and displays formatted CLI tables.

---

### 📄 File 5: `terraform/main.tf` & `variables.tf`
Infrastructure as Code defining all cloud resources.
- `aws_kms_key`: Dedicated customer-managed KMS encryption key.
- `aws_s3_bucket.data_lake_bucket`: S3 raw and processed lakehouse storage.
- `aws_s3_bucket.athena_results_bucket`: Query results output with 30-day lifecycle auto-purge.
- `aws_glue_catalog_database` & `aws_glue_crawler`: Automated schema crawler and metastore.
- `aws_athena_workgroup`: Workgroup with enforced 10 GB scan runaway limit.

---

### 📄 File 6: `dags/serverless_analytics_dag.py`
Apache Airflow DAG scheduling automated nightly runs.
- **`schedule_interval="0 2 * * *"`**: Executes automatically every night at 2:00 AM UTC.
- **Task Chain**: `generate_events_task >> run_etl_task >> run_analytics_task >> pipeline_success_task`.
- **In Comments**: Full AWS MWAA Production DAG triggering `GlueCrawlerOperator`, `AthenaOperator`, and `SlackWebhookOperator`.

---

## 4. Local vs. Real-World AWS Production Code Comparison

| Component | 💻 Local Computer Execution | ☁️ Real-World AWS Production Code |
| :--- | :--- | :--- |
| **Raw Ingestion** | Local directory (`data/raw_logs/`) | **Amazon S3 Data Lake** (`s3://.../raw_logs/`) |
| **Processed Storage** | Local directory (`data/processed_parquet/`) | **Amazon S3 Lakehouse** (`s3://.../processed_parquet/`) |
| **ETL Engine** | Python + Pandas + DuckDB | **AWS Glue PySpark Job** / **AWS EMR** |
| **Schema Metastore** | In-memory DuckDB Views | **AWS Glue Data Catalog & Crawler** |
| **Query Engine** | Local DuckDB ANSI SQL | **Amazon Athena** (Presto / Trino Serverless) |
| **BI Dashboards** | CLI Formatted Tables | **Amazon QuickSight** (SPICE In-Memory BI) |
| **Infrastructure** | Local directory scripts | **Terraform** (`terraform apply`) |
| **Scheduler** | Terminal / Local Airflow | **AWS MWAA** (Managed Workflows for Apache Airflow) |
| **Security & Privacy**| SHA-256 Hashing (`hashlib`) | **AWS KMS Encryption** + IAM Least Privilege + SHA-256 |

---

## 5. Quick Command Reference

```bash
# Navigate to the project directory
cd "/Users/prabhat/Documents/practice/project4_aws_serverless_data_analytics"

# 1. Generate 5 days of multi-format operational logs
python3 data_generator/generate_data.py

# 2. Run ETL transformation and Parquet Lakehouse conversion
python3 src/etl_pipeline.py

# 3. Run full 4-tier analytics query suite locally (Zero AWS Cost)
python3 src/local_simulation.py

# 4. Run automated unit tests
pytest tests/ -v

# 5. (Optional) Deploy to AWS Cloud via Terraform
cd terraform
terraform init
terraform apply -auto-approve
```
