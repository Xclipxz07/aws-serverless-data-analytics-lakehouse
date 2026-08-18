# 📊 Amazon QuickSight Dashboard Setup Guide

This guide provides step-by-step instructions to connect **Amazon QuickSight** to **Amazon Athena** and build an executive-ready operational dashboard.

---

## 🛠️ Step 1: Grant QuickSight Access to S3 & Athena

1. In the **AWS Management Console**, navigate to **Amazon QuickSight**.
2. Click your user profile icon (top right) $\rightarrow$ **Manage QuickSight** $\rightarrow$ **Security & permissions**.
3. Under **QuickSight access to AWS services**, click **Manage**.
4. Check **Amazon Athena** and check **Amazon S3**.
5. Select both your **Data Lake S3 Bucket** and **Athena Query Results S3 Bucket**. Click **Save**.

---

## 🔗 Step 2: Create a New Dataset

1. In QuickSight, go to **Datasets** $\rightarrow$ **New dataset**.
2. Choose **Athena** as the data source.
3. **Data source name**: `aws_analytics_athena_source`.
4. **Athena workgroup**: Select `aws-serverless-analytics-workgroup-prod` (or `primary`).
5. Choose Database: `aws_serverless_analytics_db_prod`.
6. Select Table: `processed_logs_parquet` $\rightarrow$ Click **Edit/Preview data**.
7. Choose **SPICE** (Super-fast, Parallel, In-memory Calculation Engine) for high performance and zero Athena query charges during dashboard interactions.

---

## 🧮 Step 3: Add Calculated Fields

Click **Add calculated field** on the dataset preparation screen and add:

### 1. `Error_Rate_Pct`
```sql
sum(ifelse({status_code} >= 400, 1, 0)) / count({request_id}) * 100
```

### 2. `Is_Degraded_Latency`
```sql
ifelse({response_time_ms} > 500, 'Degraded (>500ms)', 'Normal (<500ms)')
```

---

## 📈 Step 4: Build the 4 Executive Visuals

### Visual 1: KPI Summary Row
* **Visual Type**: KPI
* **Values**: `count(request_id)`, `avg(response_time_ms)`, `Error_Rate_Pct`
* **Format**: Set target warning threshold if Error Rate $> 2.5\%$.

### Visual 2: Hourly Traffic & Latency Anomalies (Time Series)
* **Visual Type**: Line Chart
* **X-axis**: `timestamp` (Aggregate by: Hour)
* **Value**: `count(request_id)` and `avg(response_time_ms)`
* **Color / Group**: `service_name`

### Visual 3: Failure Taxonomy & Root-Cause (Bar Chart)
* **Visual Type**: Clustered Bar Chart
* **Y-axis**: `service_name`
* **Value**: `count(request_id)`
* **Group / Color**: `error_message`
* **Filter**: Add filter for `status_code >= 400`.

### Visual 4: Global Regional Traffic Share (Donut Chart)
* **Visual Type**: Donut Chart
* **Group by**: `aws_region`
* **Value**: `count(request_id)`

---

## 🚀 Step 5: Publish Dashboard
1. Click **Share** (top right) $\rightarrow$ **Publish dashboard**.
2. Name: **Enterprise Cloud Serverless Analytics & SLA Monitoring Dashboard**.
3. Set scheduled SPICE refresh to daily/hourly.
