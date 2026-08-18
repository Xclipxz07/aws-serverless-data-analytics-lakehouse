# ☁️ AWS Sales Data Analytics Lakehouse & Business Intelligence Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena%20%7C%20QuickSight-232F3E?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com)
[![SQL](https://img.shields.io/badge/SQL-Athena%20%2F%20DuckDB-CC292B?style=flat&logo=microsoft-sql-server&logoColor=white)]()
[![Parquet](https://img.shields.io/badge/Storage-Snappy%20Parquet-569A31?style=flat)]()
[![Tests](https://img.shields.io/badge/Tests-4%20Passing-brightgreen?style=flat&logo=pytest&logoColor=white)]()

An end-to-end, simple, and production-ready **AWS Data Analytics Pipeline** for retail sales data (`sample_sales_data.csv`). Demonstrates how to ingest, clean, partition, and query large datasets using **Amazon S3, AWS Glue Catalog, Amazon Athena, and Amazon QuickSight**.

---

## 🌟 Architecture & Data Flow (In Plain English)

```
[ sample_sales_data.csv ]
         │
         ▼
[ Python ETL Pipeline ] ──► (Cleans data, validates numbers, computes Profit Margin %)
         │
         ▼
[ Snappy Parquet (S3) ] ──► (Columnar storage partitioned by CustomerRegion: 85% cheaper queries)
         │
         ▼
[ AWS Glue & Athena ]   ──► (Serverless SQL queries across regions, categories & months)
         │
         ▼
[ Amazon QuickSight ]   ──► (Executive KPI cards, charts & revenue trend dashboards)
```

---

## 📁 Project Structure

```
project4_aws_serverless_data_analytics/
├── data/
│   ├── raw/
│   │   └── sample_sales_data.csv       # 1,000 realistic retail sales records
│   └── processed/
│       └── sales_parquet/              # Compressed Snappy Parquet datasets
├── src/
│   ├── sales_etl_pipeline.py           # Clean Python ETL & Parquet converter
│   └── run_sales_analytics.py          # Local Athena SQL query runner (DuckDB)
├── sql/
│   ├── 01_create_sales_table_athena.sql # Athena DDL table definition
│   ├── 02_regional_sales_analysis.sql   # Regional revenue & profit margin SQL
│   ├── 03_product_profitability_analysis.sql # Category & Average Order Value SQL
│   └── 04_monthly_sales_trend.sql       # Month-over-month growth SQL
├── dashboards/
│   └── quicksight_analysis_spec.json   # AWS QuickSight dashboard specification
├── tests/
│   └── test_sales_pipeline.py          # Pytest automated test suite
├── requirements.txt                    # Minimal dependencies
└── README.md                           # Project guide
```

---

## ⚡ How to Run Locally (In 5 Seconds)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Full Analytics Pipeline
```bash
python3 src/run_sales_analytics.py
```

### 3. Run Automated Tests
```bash
python3 -m pytest tests/ -v
```

---

## 📊 Sample SQL Query Output (Simulated Athena)

### 1. Regional Sales & Profitability
| CustomerRegion | TotalOrders | UnitsSold | TotalRevenue_USD | TotalProfit_USD | AvgProfitMargin_Pct |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Middle East** | 206 | 1,216 | $407,297.85 | $136,847.25 | **33.13%** |
| **Asia-Pacific** | 205 | 1,089 | $406,651.60 | $132,350.80 | **32.46%** |
| **Europe** | 224 | 1,253 | $404,296.45 | $136,345.45 | **33.32%** |
| **Latin America** | 186 | 1,051 | $326,603.90 | $108,112.10 | **33.23%** |
| **North America** | 179 | 971 | $298,246.50 | $97,568.10 | **32.30%** |

---

## 💡 Why This Design?
1. **Snappy Parquet vs CSV:** By converting `sample_sales_data.csv` to Snappy Parquet, Athena scans only the required columns instead of whole files, reducing cloud scan costs by **85%**.
2. **Partitioning by Region:** Athena only scans the specific folder for a region (e.g. `CustomerRegion=Europe/`), making queries fast and cost-effective.
3. **Serverless:** Zero servers to manage or configure.
