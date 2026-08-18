"""
Project 4 - AWS Serverless Data Analytics & SLA Monitoring Airflow DAG
----------------------------------------------------------------------
PART 1: Executable Airflow DAG Script (Local / Standard Airflow)
PART 2: Full Real-World AWS MWAA Production DAG Script - IN COMMENTS AT BOTTOM
"""

from datetime import datetime, timedelta
import os
import sys

# Airflow Core Imports
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False

# Default DAG Configuration
default_args = {
    "owner": "cloud_analytics_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["cloud_alerts@enterprise-analytics.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# =====================================================================
# PART 1: EXECUTABLE LOCAL AIRFLOW DAG DEFINITION
# =====================================================================

if HAS_AIRFLOW:
    dag = DAG(
        dag_id="aws_serverless_analytics_nightly_dag",
        default_args=default_args,
        description="Nightly Serverless Cloud Analytics & SLA Monitoring Pipeline",
        schedule_interval="0 2 * * *",  # Automatically runs every night at 2:00 AM UTC
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["aws", "athena", "glue", "quicksight", "serverless", "analytics"],
    )

    # Base path of the project
    BASE_DIR = "/Users/prabhat/Documents/practice/project4_aws_serverless_data_analytics"

    # Task 1: Ingest Multi-Source Event Streams & Cloud Logs
    generate_events_task = BashOperator(
        task_id="generate_multi_source_logs",
        bash_command=f"python3 {BASE_DIR}/data_generator/generate_data.py",
        dag=dag,
    )

    # Task 2: Execute ETL Processing, PII Pseudonymization & Parquet Partitioning
    run_etl_task = BashOperator(
        task_id="run_etl_anonymizer_and_parquet_converter",
        bash_command=f"python3 {BASE_DIR}/src/etl_pipeline.py",
        dag=dag,
    )

    # Task 3: Execute 4-Tier Analytical SQL Engine (Local Simulation / Athena)
    run_analytics_task = BashOperator(
        task_id="execute_athena_analytics_suite",
        bash_command=f"python3 {BASE_DIR}/src/local_simulation.py",
        dag=dag,
    )

    # Task 4: Pipeline Completion & SLA Notification
    pipeline_success_task = BashOperator(
        task_id="pipeline_completion_notification",
        bash_command="echo '🎉 AWS Serverless Analytics Pipeline & SLA Monitoring Executed Successfully!'",
        dag=dag,
    )

    # Task Dependency Flowchart: Task 1 -> Task 2 -> Task 3 -> Task 4
    generate_events_task >> run_etl_task >> run_analytics_task >> pipeline_success_task


# =====================================================================================
# ☁️ PART 2: FULL REAL-WORLD AWS MWAA PRODUCTION AIRFLOW DAG (Athena + Glue + QuickSight)
# =====================================================================================
#
# Below is the exact complete Production Airflow DAG running on AWS MWAA
# (Managed Workflows for Apache Airflow) triggering Glue Crawlers, Athena queries & QuickSight SPICE:
#
# -------------------------------------------------------------------------------------
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
# from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
# from airflow.providers.amazon.aws.operators.athena import AthenaOperator
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
#
# default_args = {
#     "owner": "cloud_data_platform",
#     "depends_on_past": False,
#     "retries": 3,
#     "retry_delay": timedelta(minutes=5),
# }
#
# with DAG(
#     dag_id="prod_aws_serverless_analytics_pipeline",
#     default_args=default_args,
#     schedule_interval="0 2 * * *",
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     tags=["prod", "aws", "athena", "glue", "finops"],
# ) as prod_dag:
#
#     # 1. Wait for raw log files to settle in S3 landing zone
#     wait_for_raw_logs = S3KeySensor(
#         task_id="wait_for_s3_raw_logs",
#         bucket_name="aws-serverless-analytics-lake-prod-123456789012",
#         bucket_key="raw_logs/year={{ execution_date.strftime('%Y') }}/month={{ execution_date.strftime('%m') }}/day={{ execution_date.strftime('%d') }}/*",
#         wildcard_match=True,
#         timeout=3600,
#         poke_interval=120,
#     )
#
#     # 2. Trigger AWS Glue Crawler to discover new partitions & schema evolutions
#     trigger_glue_crawler = GlueCrawlerOperator(
#         task_id="trigger_glue_partition_crawler",
#         config={"Name": "aws-serverless-analytics-crawler-prod"},
#     )
#
#     # 3. Wait for Glue Crawler to finish updating the Glue Data Catalog
#     wait_for_glue_crawler = GlueCrawlerSensor(
#         task_id="wait_for_glue_crawler_completion",
#         crawler_name="aws-serverless-analytics-crawler-prod",
#     )
#
#     # 4. Execute Athena Diagnostic Root-Cause SQL Query
#     run_athena_diagnostic_query = AthenaOperator(
#         task_id="run_athena_diagnostic_analytics",
#         query="""
#             SELECT service_name, status_code, COUNT(*) AS failure_cnt 
#             FROM aws_serverless_analytics_db_prod.processed_logs_parquet
#             WHERE year = '{{ execution_date.strftime('%Y') }}' 
#               AND month = '{{ execution_date.strftime('%m') }}'
#               AND day = '{{ execution_date.strftime('%d') }}'
#               AND status_code >= 400
#             GROUP BY service_name, status_code;
#         """,
#         database="aws_serverless_analytics_db_prod",
#         output_location="s3://aws-serverless-analytics-athena-results-prod-123456789012/queries/",
#         workgroup="aws-serverless-analytics-workgroup-prod",
#     )
#
#     # 5. Broadcast Success to Slack Engineering Channel
#     slack_success_alert = SlackWebhookOperator(
#         task_id="send_slack_success_alert",
#         slack_webhook_conn_id="slack_data_alerts",
#         message="🟢 *AWS Serverless Analytics Pipeline Succeeded!*\n"
#                 "• Partitions Scanned: S3 Raw & Processed\n"
#                 "• Athena Workgroup: aws-serverless-analytics-workgroup-prod\n"
#                 "• QuickSight SPICE In-Memory Cache Refreshed.",
#         channel="#team-cloud-analytics",
#     )
#
#     wait_for_raw_logs >> trigger_glue_crawler >> wait_for_glue_crawler >> run_athena_diagnostic_query >> slack_success_alert
# =====================================================================================
