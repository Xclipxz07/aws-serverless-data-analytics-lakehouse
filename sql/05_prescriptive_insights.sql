-- =====================================================================
-- 05: PRESCRIPTIVE ANALYTICS (HOW CAN WE MAKE IT HAPPEN / TAKE ACTION?)
-- Automated operational actions, auto-scaling triggers, and WAF rate-limiting directives
-- =====================================================================

-- Query 1: Auto-Scaling and Concurrency Allocation Directives
SELECT 
    service_name,
    aws_region,
    COUNT(*) AS request_count,
    ROUND(AVG(response_time_ms), 2) AS avg_latency_ms,
    ROUND(APPROX_PERCENTILE(response_time_ms, 0.99), 2) AS p99_latency_ms,
    SUM(CASE WHEN status_code = 503 THEN 1 ELSE 0 END) AS count_503_throttled,
    CASE 
        WHEN SUM(CASE WHEN status_code = 503 THEN 1 ELSE 0 END) > 10 OR AVG(response_time_ms) > 500
            THEN 'CRITICAL: Increase ECS Task Replicas + Configure AWS Application Auto Scaling Target to 60% CPU'
        WHEN AVG(response_time_ms) BETWEEN 150 AND 500
            THEN 'WARNING: Provision AWS ElastiCache Redis layer for caching frequent read queries'
        ELSE 'STABLE: Maintain standard baseline capacity'
    END AS operational_action_plan
FROM aws_serverless_analytics_db_prod.raw_logs_json
GROUP BY service_name, aws_region
ORDER BY p99_latency_ms DESC;

-- Query 2: AWS WAF (Web Application Firewall) Automated IP Blocklist Generation
SELECT 
    client_ip,
    COUNT(*) AS total_failed_attempts,
    'AWS_WAF_BLOCK_RULE_IP_SET' AS recommended_security_action,
    'Block all incoming traffic for 24 hours' AS policy_enforcement
FROM aws_serverless_analytics_db_prod.raw_logs_json
WHERE status_code IN (401, 403)
GROUP BY client_ip
HAVING COUNT(*) > 15
ORDER BY total_failed_attempts DESC;
