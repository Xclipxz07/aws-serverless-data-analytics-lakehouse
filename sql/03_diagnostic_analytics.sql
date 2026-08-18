-- =====================================================================
-- 03: DIAGNOSTIC ANALYTICS (WHY DID IT HAPPEN?)
-- Root cause analysis, error spike investigations, and service bottleneck identification
-- =====================================================================

-- Query 1: Microservice Failure Attribution & Root Cause Taxonomy
SELECT 
    service_name,
    status_code,
    COALESCE(error_message, 'Unknown Exception') AS error_root_cause,
    COUNT(*) AS incident_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY service_name), 2) AS pct_of_service_failures,
    ROUND(AVG(response_time_ms), 2) AS avg_failure_latency_ms
FROM aws_serverless_analytics_db_prod.raw_logs_json
WHERE status_code >= 400
GROUP BY service_name, status_code, error_message
ORDER BY incident_count DESC;

-- Query 2: Peak Hour Degradation Analysis (Hourly Latency vs Failure Correlation)
SELECT 
    service_name,
    SUBSTR(timestamp, 12, 2) AS hour_utc,
    COUNT(*) AS request_count,
    ROUND(AVG(response_time_ms), 2) AS avg_latency_ms,
    SUM(CASE WHEN status_code = 503 THEN 1 ELSE 0 END) AS count_503_service_unavailable,
    SUM(CASE WHEN status_code = 500 THEN 1 ELSE 0 END) AS count_500_internal_error
FROM aws_serverless_analytics_db_prod.raw_logs_json
WHERE service_name = 'payment-gateway'
GROUP BY service_name, SUBSTR(timestamp, 12, 2)
ORDER BY hour_utc;

-- Query 3: Suspicious Client IP & Bot/Abuse Traffic Detection (Security Diagnostic)
SELECT 
    client_ip,
    aws_region,
    COUNT(*) AS total_calls,
    SUM(CASE WHEN status_code = 401 THEN 1 ELSE 0 END) AS auth_failures,
    SUM(CASE WHEN status_code = 403 THEN 1 ELSE 0 END) AS forbidden_attempts,
    ROUND(SUM(CASE WHEN status_code IN (401, 403) THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS abuse_rate_pct
FROM aws_serverless_analytics_db_prod.raw_logs_json
GROUP BY client_ip, aws_region
HAVING COUNT(*) > 20 AND SUM(CASE WHEN status_code IN (401, 403) THEN 1 ELSE 0 END) > 5
ORDER BY abuse_rate_pct DESC, total_calls DESC;
