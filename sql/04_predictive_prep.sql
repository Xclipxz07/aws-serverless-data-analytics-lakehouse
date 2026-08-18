-- =====================================================================
-- 04: PREDICTIVE ANALYTICS PREPARATION (WHAT WILL HAPPEN?)
-- Feature extraction, rolling time-window calculations, and trend forecasting signals
-- =====================================================================

-- Query 1: Hourly Rolling Latency and Error Trends (3-Hour Window)
WITH hourly_aggregates AS (
    SELECT 
        service_name,
        SUBSTR(timestamp, 1, 13) AS hour_bucket,
        COUNT(*) AS total_volume,
        AVG(response_time_ms) AS avg_latency,
        SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_errors
    FROM aws_serverless_analytics_db_prod.raw_logs_json
    GROUP BY service_name, SUBSTR(timestamp, 1, 13)
)
SELECT 
    service_name,
    hour_bucket,
    total_volume,
    ROUND(avg_latency, 2) AS current_avg_latency_ms,
    server_errors,
    ROUND(AVG(avg_latency) OVER (
        PARTITION BY service_name 
        ORDER BY hour_bucket 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_3h_latency_trend,
    ROUND(AVG(total_volume) OVER (
        PARTITION BY service_name 
        ORDER BY hour_bucket 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 1) AS rolling_3h_volume_trend
FROM hourly_aggregates
ORDER BY service_name, hour_bucket;

-- Query 2: Predictive Outage Risk Indicator Feature Table (For ML/SageMaker Input)
SELECT 
    user_id_hash,
    COUNT(*) AS lifetime_api_interactions,
    ROUND(AVG(response_time_ms), 2) AS user_experienced_latency_ms,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS user_experienced_errors,
    ROUND(SUM(CASE WHEN status_code >= 400 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS user_churn_risk_score
FROM aws_serverless_analytics_db_prod.raw_logs_json
GROUP BY user_id_hash
HAVING COUNT(*) >= 5
ORDER BY user_churn_risk_score DESC;
