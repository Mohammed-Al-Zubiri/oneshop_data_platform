-- ============================================================
-- OneShop Data Platform — Database Bootstrap
-- Combines all table definitions from Chapters 2-10
-- ============================================================

-- ============================================================
-- 1. CORE OLTP TABLES (Chapters 3, 6, 7, 9)
-- ============================================================

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS items (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(7,2),
    inventory INT,
    inventory_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchases (
    id SERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    item_id BIGINT REFERENCES items(id),
    campaign_id VARCHAR(50),
    status SMALLINT DEFAULT 1,
    quantity INT DEFAULT 1,
    purchase_price DECIMAL(12,2),
    deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. ML SERVING TABLE (Chapter 9)
-- ============================================================

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id INT,
    item_id INT,
    score FLOAT,
    model_version TEXT,
    generated_at TIMESTAMP,
    PRIMARY KEY (user_id, item_id)
);

-- ============================================================
-- 3. VECTOR SEARCH TABLE (Chapter 10)
-- ============================================================

-- Enable pgvector extension (requires pgvector image)
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS reviews (
    review_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    date DATE,
    review TEXT,
    review_embedding vector(384)
);

-- ============================================================
-- 4. ETL USER (least-privilege for batch pipelines)
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'etluser') THEN
        CREATE USER etluser WITH PASSWORD 'etlpassword';
    END IF;
END
$$;

-- Create a readonly role
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'readonly') THEN
        CREATE ROLE readonly;
    END IF;
END
$$;

-- Grant privileges to the readonly role
GRANT CONNECT ON DATABASE oneshop TO readonly;
GRANT USAGE ON SCHEMA public TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;

-- Ensure future tables also grant SELECT to readonly
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;

-- Assign the readonly role to etluser
GRANT readonly TO etluser;

-- Grant SELECT on all existing tables
GRANT SELECT ON TABLE users TO readonly;
GRANT SELECT ON TABLE items TO readonly;
GRANT SELECT ON TABLE purchases TO readonly;
GRANT SELECT ON TABLE user_recommendations TO readonly;
GRANT SELECT ON TABLE reviews TO readonly;

-- ============================================================
-- 5. LOGICAL REPLICATION (for Debezium CDC)
-- ============================================================
-- The debezium/postgres image already has wal_level=logical.
-- We just need to set the replica identity for CDC tables.

ALTER TABLE items REPLICA IDENTITY FULL;
ALTER TABLE purchases REPLICA IDENTITY FULL;
