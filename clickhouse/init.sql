CREATE DATABASE IF NOT EXISTS oneshop;

CREATE TABLE IF NOT EXISTS oneshop.purchases_raw (
    id              Int32,
    user_id         Int64,
    item_id         Int64,
    campaign_id     String,
    status          Int16,
    quantity        Int32,
    purchase_price  Decimal(12, 2),
    deleted         Bool,
    created_at      DateTime64(6),
    updated_at      DateTime64(6)
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'oneshop.public.purchases',
    kafka_group_name = 'clickhouse-consumer-group-v3',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS oneshop.purchases (
    id              Int32,
    user_id         Int64,
    item_id         Int64,
    campaign_id     String,
    status          Int16,
    quantity        Int32,
    purchase_price  Decimal(12, 2),
    deleted         Bool,
    created_at      DateTime64(6),
    updated_at      DateTime64(6)
) ENGINE = MergeTree()
ORDER BY (item_id, created_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS oneshop.mv_purchases TO oneshop.purchases AS
SELECT * FROM oneshop.purchases_raw;
