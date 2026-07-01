#!/bin/bash
set -e

# ============================================================
# OneShop Real-Time Streaming Setup Script
# Idempotent initialization for CDC and Flink components
# ============================================================

ACTION=$1

if [ -z "$ACTION" ]; then
    echo "Usage: ./scripts/setup_realtime.sh [cdc|flink|all]"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "⚠️  The tool 'jq' is required to parse JSON API responses."
    read -p "Would you like to install it now using apt-get? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        sudo apt-get update && sudo apt-get install -y jq
    else
        echo "❌ Cannot proceed without jq. Please install manually."
        exit 1
    fi
fi

echo "🚀 Starting Real-Time Setup: $ACTION"

# ------------------------------------------------------------
# CDC SETUP (Debezium + Kafka Connect)
# ------------------------------------------------------------
setup_cdc() {
    echo "========================================"
    echo "📦 Initializing CDC (Kafka Connect)..."
    echo "========================================"

    # 1. Check if Postgres is seeded
    echo "🔍 Checking Postgres data..."
    ROWS=$(docker exec postgres psql -U postgresuser -d oneshop -tAc "SELECT count(*) FROM items;" 2>/dev/null || echo "0")
    
    if [ "$ROWS" -eq "0" ]; then
        echo "⚠️  Postgres tables are empty! Seeding batch data..."
        make seed-batch
    else
        echo "✅ Postgres is already seeded ($ROWS items found)."
    fi

    # 2. Wait for Kafka Connect API
    echo "⏳ Waiting for Kafka Connect API..."
    until curl -s http://localhost:8083/connectors > /dev/null; do
        sleep 2
    done

    # 3. Check and Register Connectors
    echo "🔍 Checking existing connectors..."
    CONNECTORS=$(curl -s http://localhost:8083/connectors)
    
    if echo "$CONNECTORS" | grep -q "oneshop-postgres-items-connector"; then
        echo "✅ Connectors are already registered: $CONNECTORS"
    else
        echo "⚠️  Connectors not found. Registering..."
        make connectors
    fi
}

# ------------------------------------------------------------
# FLINK SETUP (Stream Processing)
# ------------------------------------------------------------
setup_flink() {
    echo "========================================"
    echo "🌊 Initializing Flink Stream Processing..."
    echo "========================================"

    # 1. Pre-create Kafka Topics
    echo "📁 Pre-creating Kafka topics to prevent crash loops..."
    docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic oneshop.public.purchases --if-not-exists 2>/dev/null || true
    docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic oneshop.logins --if-not-exists 2>/dev/null || true
    echo "✅ Kafka topics verified."

    # 2. Wait for Flink JobManager API
    echo "⏳ Waiting for Flink JobManager API..."
    until curl -s http://localhost:8081/config > /dev/null; do
        sleep 2
    done

    # 3. Create Tables (Idempotent)
    echo "📊 Initializing Flink Tables..."
    make stream-tables

    # 4. Check and Submit Flink Jobs
    echo "🔍 Checking for RUNNING Flink jobs..."
    RUNNING_JOBS=$(curl -s http://localhost:8084/jobs/overview | jq -r '.jobs[]? | select(.state=="RUNNING") | .name' || echo "")

    # Note: When we run insert-jobs.sql, Flink spawns jobs.
    if [ -n "$RUNNING_JOBS" ]; then
        echo "✅ Streaming jobs are already RUNNING:"
        echo "$RUNNING_JOBS"
    else
        echo "⚠️  No active streaming jobs found. Submitting..."
        make stream-jobs
        
        # Trigger a short burst of data so the user sees immediate results
        echo "💦 Triggering initial stream events..."
        make seed-cdc CDC_COUNT=50 > /dev/null 2>&1 &
        make seed-logins LOGIN_COUNT=50 > /dev/null 2>&1 &
    fi
}

# ------------------------------------------------------------
# CLICKHOUSE SETUP (OLAP)
# ------------------------------------------------------------
setup_clickhouse() {
    echo "========================================"
    echo "⚡ Initializing ClickHouse OLAP Analytics..."
    echo "========================================"
    echo "⏳ Waiting for ClickHouse API..."
    until curl -s http://localhost:8123/ > /dev/null; do
        sleep 2
    done
    echo "📊 Initializing ClickHouse Tables..."
    docker exec -i clickhouse clickhouse-client --user default --password mysecret --multiquery < clickhouse/init.sql
    echo "✅ ClickHouse initialized."
}

# ------------------------------------------------------------
# ROUTER
# ------------------------------------------------------------
if [ "$ACTION" == "cdc" ]; then
    setup_cdc
elif [ "$ACTION" == "flink" ]; then
    setup_flink
elif [ "$ACTION" == "all" ]; then
    setup_cdc
    setup_clickhouse
    setup_flink
else
    echo "❌ Unknown action: $ACTION"
    exit 1
fi

echo "🎉 Real-Time setup complete!"
