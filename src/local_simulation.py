"""
Local SQL Simulation Engine (Offline Athena Analytics)
Executes analytical SQL queries locally using DuckDB across partitioned JSON and Parquet datasets without requiring AWS cloud infrastructure.
"""

import os
import glob
import duckdb
from tabulate import tabulate

def run_local_analytics_suite(data_dir: str):
    """Initializes DuckDB in-memory engine, mounts partitioned files, and executes the 4 analytics query suites."""
    print("=" * 80)
    print("🏛️  LOCAL OFFLINE AWS ATHENA & GLUE DATA ANALYTICS SIMULATION")
    print("=" * 80)

    con = duckdb.connect(database=":memory:")
    
    # Path to partitioned files
    json_glob = os.path.join(data_dir, "raw_logs", "*", "*", "*", "*.json").replace("\\", "/")
    parquet_glob = os.path.join(data_dir, "processed_parquet", "*", "*", "*", "*.parquet").replace("\\", "/")

    # Check if files exist
    found_json = glob.glob(os.path.join(data_dir, "raw_logs", "**", "*.json"), recursive=True)
    if not found_json:
        print("⚠️ No raw data found. Please run data_generator/generate_data.py first.")
        return

    print(f"📦 Found {len(found_json)} partitioned raw event files.")
    
    # Register views matching AWS Glue Catalog Table schema
    con.execute(f"""
        CREATE VIEW raw_logs AS 
        SELECT * FROM read_json_auto('{json_glob}', hive_partitioning=true);
    """)

    # -------------------------------------------------------------
    # 1. Descriptive Analytics (What Happened?)
    # -------------------------------------------------------------
    print("\n" + "#" * 60)
    print("📊 1. DESCRIPTIVE ANALYTICS: Traffic, Users & Error Breakdown")
    print("#" * 60)
    q1 = """
    SELECT 
        year, month, day,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT user_id_hash) AS unique_users,
        SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_requests,
        ROUND(SUM(CASE WHEN status_code >= 400 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS error_rate_pct,
        ROUND(AVG(response_time_ms), 2) AS avg_latency_ms
    FROM raw_logs
    GROUP BY year, month, day
    ORDER BY year, month, day;
    """
    df1 = con.execute(q1).fetchdf()
    print(tabulate(df1, headers="keys", tablefmt="fancy_grid", showindex=False))

    # -------------------------------------------------------------
    # 2. Diagnostic Analytics (Why Did It Happen?)
    # -------------------------------------------------------------
    print("\n" + "#" * 60)
    print("🔍 2. DIAGNOSTIC ANALYTICS: Microservice & Error Root Cause Analysis")
    print("#" * 60)
    q2 = """
    SELECT 
        service_name,
        status_code,
        COALESCE(error_message, 'None (Success 200)') AS root_cause,
        COUNT(*) AS failure_count,
        ROUND(AVG(response_time_ms), 2) AS avg_spike_latency_ms
    FROM raw_logs
    WHERE status_code >= 400
    GROUP BY service_name, status_code, error_message
    ORDER BY failure_count DESC
    LIMIT 6;
    """
    df2 = con.execute(q2).fetchdf()
    print(tabulate(df2, headers="keys", tablefmt="fancy_grid", showindex=False))

    # -------------------------------------------------------------
    # 3. Predictive Analytics Prep (What Will Happen?)
    # -------------------------------------------------------------
    print("\n" + "#" * 60)
    print("🔮 3. PREDICTIVE PREPARATION: Hourly Moving Averages & Outage Risk Trend")
    print("#" * 60)
    q3 = """
    WITH hourly_stats AS (
        SELECT 
            service_name,
            CAST(SUBSTRING(timestamp, 12, 2) AS INTEGER) AS hour_of_day,
            COUNT(*) AS request_vol,
            AVG(response_time_ms) AS avg_lat,
            SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_errors
        FROM raw_logs
        GROUP BY service_name, CAST(SUBSTRING(timestamp, 12, 2) AS INTEGER)
    )
    SELECT 
        service_name,
        hour_of_day,
        request_vol,
        ROUND(avg_lat, 2) AS avg_latency_ms,
        server_errors,
        ROUND(AVG(avg_lat) OVER(
            PARTITION BY service_name 
            ORDER BY hour_of_day 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_3h_latency_trend
    FROM hourly_stats
    WHERE service_name IN ('payment-gateway', 'order-service') AND hour_of_day BETWEEN 12 AND 18
    ORDER BY service_name, hour_of_day;
    """
    df3 = con.execute(q3).fetchdf()
    print(tabulate(df3, headers="keys", tablefmt="fancy_grid", showindex=False))

    # -------------------------------------------------------------
    # 4. Prescriptive Analytics (How to Make It Happen / Take Action?)
    # -------------------------------------------------------------
    print("\n" + "#" * 60)
    print("⚡ 4. PRESCRIPTIVE INSIGHTS: Automated Throttling & Auto-Scale Directives")
    print("#" * 60)
    q4 = """
    SELECT 
        service_name,
        aws_region,
        COUNT(*) AS total_calls,
        ROUND(AVG(response_time_ms), 2) AS avg_latency_ms,
        SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS critical_errors,
        CASE 
            WHEN AVG(response_time_ms) > 300 OR SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) > 20
                THEN '⚠️ ACTION REQUIRED: Scale ECS/Lambda Tasks + Alert SRE Team'
            WHEN AVG(response_time_ms) > 100 
                THEN '🔔 ADVISORY: Enable Provisioned Concurrency & ElastiCache Caching'
            ELSE '✅ OPTIMAL: Maintain current cluster configuration'
        END AS architectural_recommendation
    FROM raw_logs
    GROUP BY service_name, aws_region
    ORDER BY critical_errors DESC, avg_latency_ms DESC
    LIMIT 6;
    """
    df4 = con.execute(q4).fetchdf()
    print(tabulate(df4, headers="keys", tablefmt="fancy_grid", showindex=False))
    print("\n✅ Local simulation complete! All 4 analytical tiers verified successfully.\n")

if __name__ == "__main__":
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(curr_dir)
    data_directory = os.path.join(project_root, "data")
    run_local_analytics_suite(data_directory)
