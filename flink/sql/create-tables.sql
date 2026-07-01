-- ============================================================
-- Flink SQL — Table Definitions (Chapter 8)
-- Run this in Flink SQL Client to create source/sink tables.
-- ============================================================

-- 1. Login events source from Kafka
CREATE TABLE login_events (
    user_id INT,
    login_ts STRING,
    ip_address STRING,
    device STRING,
    is_success BOOLEAN,
    event_time AS TO_TIMESTAMP(login_ts),
    proctime AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'oneshop.logins',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- 2. User profile lookup from Postgres (for enrichment)
CREATE TABLE user_profiles (
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/oneshop',
    'table-name' = 'users',
    'username' = 'postgresuser',
    -- Local development default — matches POSTGRES_PASSWORD in .env.example
    -- In production, use Flink's secret store or pass via environment variable
    'password' = 'postgrespw'
);

-- 3. Enriched logins sink back to Kafka
CREATE TABLE enriched_logins (
    user_id INT,
    user_email STRING,
    full_name STRING,
    login_ts STRING,
    ip_address STRING,
    device STRING,
    is_success BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = 'oneshop.enriched-logins',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- 4. Suspicious login alerts sink
CREATE TABLE login_alerts (
    user_id INT,
    user_email STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    login_count BIGINT,
    distinct_devices BIGINT,
    alert_type STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'oneshop.login-alerts',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);
