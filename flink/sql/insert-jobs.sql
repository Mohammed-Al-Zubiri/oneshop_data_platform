-- ============================================================
-- Flink SQL — Stream Processing Jobs (Chapter 8)
-- Run after create-tables.sql to start the streaming jobs.
-- ============================================================

-- Job 1: Enrich login events with user profile
INSERT INTO enriched_logins
SELECT
    le.user_id,
    up.email AS user_email,
    CONCAT(up.first_name, ' ', up.last_name) AS full_name,
    le.login_ts,
    le.ip_address,
    le.device,
    le.is_success
FROM login_events /*+ OPTIONS('properties.group.id'='flink-enrich-consumer') */ AS le
LEFT JOIN user_profiles FOR SYSTEM_TIME AS OF le.proctime AS up
ON le.user_id = up.id;

-- Job 2: Detect suspicious login patterns
-- Alert when a user has > 5 logins from > 2 devices in a 1-minute window
INSERT INTO login_alerts
SELECT
    le.user_id,
    up.email AS user_email,
    TUMBLE_START(le.event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(le.event_time, INTERVAL '1' MINUTE) AS window_end,
    COUNT(*) AS login_count,
    COUNT(DISTINCT le.device) AS distinct_devices,
    'SUSPICIOUS_MULTI_DEVICE' AS alert_type
FROM login_events /*+ OPTIONS('properties.group.id'='flink-alert-consumer') */ AS le
LEFT JOIN user_profiles FOR SYSTEM_TIME AS OF le.proctime AS up
ON le.user_id = up.id
GROUP BY
    le.user_id,
    up.email,
    TUMBLE(le.event_time, INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5 AND COUNT(DISTINCT le.device) > 2;
