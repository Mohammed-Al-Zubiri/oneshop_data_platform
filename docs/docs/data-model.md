# Data Model

This page documents the data structures across all three storage tiers: the Postgres OLTP source, the Iceberg Lakehouse (Bronze → Silver → Gold), and the ClickHouse OLAP sink.

---

## Postgres OLTP

The source of truth. Seeded by `make seed-batch` and updated in real-time by CDC generators.

### Schema

```sql
-- Users: registered customers
CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    first_name VARCHAR(50)  NOT NULL,
    last_name  VARCHAR(50)  NOT NULL,
    email      VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Items: product catalog
CREATE TABLE items (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200)   NOT NULL,
    category    VARCHAR(100)   NOT NULL,
    price       DECIMAL(10, 2) NOT NULL,
    stock       INT            NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- Purchases: transaction history (implicit feedback for ML)
CREATE TABLE purchases (
    id          SERIAL PRIMARY KEY,
    user_id     INT            REFERENCES users(id),
    item_id     INT            REFERENCES items(id),
    amount      DECIMAL(10, 2) NOT NULL,
    purchased_at TIMESTAMP DEFAULT NOW()
);

-- Reviews: text reviews with star ratings
CREATE TABLE reviews (
    id          SERIAL PRIMARY KEY,
    user_id     INT REFERENCES users(id),
    item_id     INT REFERENCES items(id),
    rating      INT CHECK (rating BETWEEN 1 AND 5),
    review_text TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ML outputs: pre-computed ALS recommendations (written by Airflow @weekly)
CREATE TABLE user_recommendations (
    user_id     INT  NOT NULL,
    item_id     INT  NOT NULL,
    score       FLOAT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, item_id)
);
```

### CDC-Monitored Tables

Debezium monitors `items` and `purchases`. Any `INSERT`, `UPDATE`, or `DELETE` on these tables is immediately captured and published to Kafka.

!!! note "pgvector extension"
    The Postgres instance also has the `pgvector` extension enabled. Product embeddings are stored as `vector(384)` columns alongside the reviews table. These are generated offline by the `ml_training_pipeline` DAG (runs weekly) and queried at search time via cosine similarity.

---

## Iceberg Lakehouse

Three namespaces on MinIO at `s3://warehouse/`. Initialised by `make init-lakehouse`.

### Bronze Layer — Raw Ingestion

Data is loaded in its original form — no cleaning, no type coercion. The Bronze layer is the audit trail: if a downstream transformation bug corrupts Silver data, Bronze is the recovery point.

| Table | Source | Key Columns |
|:------|:-------|:------------|
| `bronze.users` | Postgres `users` | All columns + `_ingest_ts` |
| `bronze.items` | Postgres `items` | All columns + `_ingest_ts` |
| `bronze.purchases` | Postgres `purchases` | All columns + `_ingest_ts` |
| `bronze.reviews` | Postgres `reviews` | All columns + `_ingest_ts` |
| `bronze.pageviews` | MinIO JSON files | `user_id`, `item_id`, `page_url`, `session_id`, `utm_source`, `utm_medium`, `ts` |

Every Bronze table includes `_ingest_ts TIMESTAMP` — the time the record was written to the lakehouse, separate from the source system's own timestamps.

### Silver Layer — Cleaned & Typed

Type-safe, deduplicated, referentially consistent data ready for analytics and ML. This is the layer that Airflow's `user_engagement_segments` DAG and the ML training pipeline read from.

| Table | Key Transformations from Bronze | Business Purpose |
|:------|:-------------------------------|:----------------|
| `silver.users` | Deduplicated on `email`, normalised to lowercase, timestamps cast to `TIMESTAMP WITH TIME ZONE` | Clean customer master data for segmentation and ML features |
| `silver.items` | Deduplicated on `id`, `price` cast to `DECIMAL`, `category` normalised to `UPPER CASE` | Consistent product catalog for analytics and recommendation scoring |
| `silver.purchases` | Deduplicated, orphaned `user_id`/`item_id` records removed | Reliable transaction history for revenue reports and ALS training |
| `silver.reviews` | Deduplicated, `review_text` stripped of leading/trailing whitespace | Clean text input for embedding generation |
| `silver.pageviews` | JSON parsed, UTM parameters extracted into dedicated columns | Marketing attribution — which channels drive traffic to which products |
| `silver.user_engagement_segments` | RFV segmentation — `segment` column: `Champion`, `At-Risk`, `New`, `Inactive` | Marketing cohorts for targeted email campaigns |

### Gold Layer — Business Aggregates

Pre-aggregated, query-optimised tables served to Superset via Trino. These tables are designed to answer the specific KPI questions raised by the IT and Marketing departments — no expensive joins at query time.

| Table | Grain | Business Question It Answers |
|:------|:------|:-----------------------------|
| `gold.top_selling_items` | Item × day | Which items generated the most revenue in the last 30 days? |
| `gold.top_converting_items` | Item | Which items have the best purchase-to-view conversion ratio? |
| `gold.sales_performance_24h` | Hour | How is hourly revenue trending over the last 24 hours? |
| `gold.pageviews_by_channel` | UTM source × day | Which marketing channels drive the most traffic? |

---

## ClickHouse OLAP

ClickHouse ingests CDC events from Kafka using native **Kafka Engine** tables. Data is inserted into `MergeTree` tables for persistent storage and fast analytical queries.

> **Business driver:** Marketing needs flash sale analytics with <3-second data freshness. The Kafka Engine → MaterialisedView → MergeTree pattern delivers sub-second query latency on a continuously updating dataset — something the Iceberg Lakehouse (batch-refreshed) cannot provide during a 2-hour sale window.

### Tables

| Table | Engine | Source |
|:------|:-------|:-------|
| `items_kafka` | KafkaEngine | `oneshop.oneshop.items` Kafka topic (CDC feed) |
| `items` | MergeTree | Materialised from `items_kafka` via a Materialised View |
| `purchases_kafka` | KafkaEngine | `oneshop.oneshop.purchases` Kafka topic |
| `purchases` | MergeTree | Materialised from `purchases_kafka` |

!!! tip "Kafka Engine pattern"
    ClickHouse's Kafka Engine tables act as consumers — they read from Kafka on insert and cannot be queried directly. Materialised views automatically pipe data from the Kafka Engine table into the underlying MergeTree tables, which are what you query.

### Sample Queries

```sql
-- Connect: http://localhost:8123 or clickhouse-client
-- (user: default, password: mysecret)

-- Revenue by category, last 24 hours
SELECT
    i.category,
    count()       AS purchase_count,
    sum(p.amount) AS total_revenue
FROM purchases AS p
JOIN items AS i ON p.item_id = i.id
WHERE p.purchased_at > now() - INTERVAL 24 HOUR
GROUP BY i.category
ORDER BY total_revenue DESC;

-- Items with most recent stock updates
SELECT id, name, stock, updated_at
FROM items
ORDER BY updated_at DESC
LIMIT 20;
```

---

## Data Quality (Great Expectations)

Great Expectations runs checkpoint validations on Bronze tables before Silver transformation. Validation reports are written to `great-expectations/uncommitted/`.

Key expectations on `bronze.purchases`:

- `expect_column_values_to_not_be_null` on `user_id`, `item_id`, `amount`
- `expect_column_values_to_be_between` on `amount` (min=0, max=100000)
- `expect_column_values_to_be_of_type` on `purchased_at` (must be timestamp)
- `expect_table_row_count_to_be_between` (at least 1 row)

If any expectation fails, the `bronze_to_silver` Airflow DAG task fails and downstream Silver/Gold processing is halted. This protects BI dashboards and ML training from ingesting invalid data.

---

## Data Lineage Summary

The diagram below shows how data flows from source to consumer and which business outcome each layer serves:

```
Postgres OLTP ──── Debezium CDC ────► Kafka ──► ClickHouse ──────────────────► Flash Sale Dashboard
     │                                            (purchases, items)                 (Marketing)
     │
     ▼ Daily (Airflow)
 Bronze Iceberg ── GE Gate ──► Silver Iceberg ──► Gold Iceberg ──► Superset ──► BI Dashboards
 (raw, auditable)               (clean, typed)    (aggregated)    via Trino      (IT / Marketing)
                                     │
                                     ├──► user_engagement_segments ──► CSV ──► Marketing Email
                                     │
                                     └──► ML Training (weekly) ──► Postgres ──► Flask Rec API
                                                                └──► pgvector ──► Semantic Search
```
