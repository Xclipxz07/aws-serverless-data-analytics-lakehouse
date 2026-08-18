-- =====================================================================
-- 02: DESCRIPTIVE ANALYTICS (WHAT HAPPENED?)
-- High-level operational metrics, request volume, error rates, and user counts
-- =====================================================================

-- Query 1: Daily Request Volume, Unique Users, and Overall Error Rates
SELECT 
    year, 
    month, 
    day,
    COUNT(*) AS total_requests,
    COUNT(DISTINCT user_id_hash) AS unique_active_users,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS total_errors,
    ROUND(SUM(CASE WHEN status_code >= 400 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS error_rate_pct,
    ROUND(AVG(response_time_ms), 2) AS avg_response_time_ms
FROM aws_serverless_analytics_db_prod.raw_logs_json
WHERE year = '2026' AND month = '07'
GROUP BY year, month, day
ORDER BY year, month, day;

-- Query 2: Regional Traffic Distribution & Latency Benchmarks
SELECT 
    aws_region,
    COUNT(*) AS total_requests,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS traffic_share_pct,
    ROUND(AVG(response_time_ms), 2) AS avg_latency_ms,
    ROUND(APPROX_PERCENTILE(response_time_ms, 0.95), 2) AS p95_latency_ms,
    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_5xx_errors
FROM aws_serverless_analytics_db_prod.raw_logs_json
GROUP BY aws_region
ORDER BY total_requests DESC;

-- Query 3: Top Endpoints by Consumption (Bandwidth & Calls)
SELECT 
    service_name,
    endpoint,
    http_method,
    COUNT(*) AS invocation_count,
    ROUND(SUM(bytes_sent) / (1024.0 * 1024.0), 2) AS total_megabytes_served,
    ROUND(AVG(response_time_ms), 2) AS avg_latency_ms
FROM aws_serverless_analytics_db_prod.raw_logs_json
GROUP BY service_name, endpoint, http_method
ORDER BY invocation_count DESC;
