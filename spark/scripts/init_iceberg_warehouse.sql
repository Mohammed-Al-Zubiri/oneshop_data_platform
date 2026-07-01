-- ============================================================
-- OneShop Data Platform
-- Idempotent Iceberg Lakehouse Initialization Script
-- ============================================================

-- Bronze: Raw or slightly cleaned data directly from sources
CREATE NAMESPACE IF NOT EXISTS bronze;

-- Silver: Enriched, deduplicated, and typed data
CREATE NAMESPACE IF NOT EXISTS silver;

-- Gold: Aggregated business-level tables for reporting
CREATE NAMESPACE IF NOT EXISTS gold;

-- ============================================================
-- Bronze Layer Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.users (
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Dimension table for user information'
);

CREATE TABLE IF NOT EXISTS bronze.items (
    id BIGINT,
    name STRING,
    category STRING,
    price DECIMAL(7,2),
    inventory INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Dimension table for product items'
);

CREATE TABLE IF NOT EXISTS bronze.purchases (
    id BIGINT,
    user_id BIGINT,
    item_id BIGINT,
    quantity INT,
    purchase_price DECIMAL(12,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Fact table for purchase transactions'
);

CREATE TABLE IF NOT EXISTS bronze.pageviews (
    user_id BIGINT,
    url STRING,
    channel STRING,
    received_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(received_at))
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Fact table for pageview events'
);

-- ============================================================
-- Silver Layer Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.users (
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    valid_email BOOLEAN,
    full_name STRING
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Validated dimension table for user information'
);

CREATE TABLE IF NOT EXISTS silver.items (
    id BIGINT,
    name STRING,
    category STRING,
    price DECIMAL(7,2),
    inventory INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Dimension table for product items'
);

CREATE TABLE IF NOT EXISTS silver.purchases_enriched (
    id BIGINT,
    user_id BIGINT,
    item_id BIGINT,
    quantity INT,
    purchase_price DECIMAL(12,2),
    total_price DECIMAL(14,2),         
    user_email STRING,                
    item_name STRING,
    item_category STRING, 
    purchase_date DATE,  
    purchase_hour INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Validated and enriched fact table for purchase transactions'
);

CREATE TABLE IF NOT EXISTS silver.pageviews_by_items (
    user_id BIGINT,
    item_id BIGINT,
    page STRING,
    item_name STRING,
    item_category STRING,
    channel STRING,
    received_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(received_at))
TBLPROPERTIES (
    'format-version' = '2',
    'comment' = 'Fact table for pageview events enriched with items'
);
