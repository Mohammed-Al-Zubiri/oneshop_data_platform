# ============================================================
# OneShop Unified Data Platform — Makefile
# Modular Docker Compose with dependency chains
# ============================================================
#
# Architecture:
#   up-core ← up-batch / up-realtime / up-all
#
# Profiles:
#   Fine-grained : compute, query, bi, orchestration, cdc,
#                  stream-processing, olap, search,
#                  observability, devtools
#   Composite    : batch (compute+query+bi),
#                  realtime (cdc+stream-processing+olap+search)
#
# ============================================================

.DEFAULT_GOAL := help

.PHONY: help \
        up-core \
        up-cdc up-compute up-query up-orchestration up-bi \
        up-stream-processing up-olap up-search up-aiml up-observability up-devtools \
        up-recommendation up-semantic-search \
        up-batch up-realtime up-all \
        down down-volumes \
        seed-batch seed-cdc seed-purchases seed-logins seed-reviews \
        connectors connectors-status \
        spark-submit spark-shell \
        init-lakehouse unpause-dags setup-batch etl-trigger etl-features etl-train \
        setup-cdc setup-flink setup-realtime stream-tables stream-jobs \
        test-core test-aiml test-query test test-ci \
        logs status clean docs

# --- Configuration -----------------------------------------------------------
COMPOSE := docker compose --env-file .env
GENERATOR := docker compose run --rm data-generator python cli.py

# Helper to check if a container is running before executing commands
define check_running
	@if [ -z "$$(docker ps -q -f name=$(1) -f status=running)" ]; then \
		echo "❌ Error: The '$(1)' container is not running."; \
		echo "   Please start it first using: make up-$(2)"; \
		exit 1; \
	fi
endef

# --- Seeding Configuration Defaults (Override via CLI, e.g. 'make seed-batch USERS=500')
USERS          ?= 100
ITEMS          ?= 1000
PURCHASES      ?= 5000
PAGEVIEWS      ?= 10000
CDC_COUNT      ?= 1000
CDC_INTERVAL   ?= 0.5
TX_COUNT       ?= 500
TX_INTERVAL    ?= 0.3
LOGIN_COUNT    ?= 1000
LOGIN_INTERVAL ?= 0.2
REVIEWS_COUNT  ?= 200

# All profiles — used for global operations (down, status, logs, clean)
ALL_PROFILES := --profile orchestration --profile batch --profile realtime --profile ai-ml --profile observability --profile devtools

# ============================================================
# HELP
# ============================================================
.PHONY: help
help: ## Display this help menu
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

# ============================================================
# FOUNDATION & CORE
# ============================================================
##@ Foundation & Core
download-jmx:
	@if [ ! -f ./monitoring/prometheus/jmx_prometheus_javaagent.jar ]; then \
		echo "⬇️  Downloading JMX Prometheus Java Agent..."; \
		wget -q -O ./monitoring/prometheus/jmx_prometheus_javaagent.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar; \
	fi

up-core: download-jmx ## Start minimal infrastructure (Postgres, Kafka, MinIO, Iceberg, MailHog)
	@echo "🏗️  Starting core infrastructure..."
	$(COMPOSE) up -d --wait postgres kafka schema-registry minio mc iceberg-catalog mailhog

# ============================================================
# FINE-GRAINED MODULES
# Dependencies encode functional relationships — not just startup
# order, but what each service needs to be *useful*.
# ============================================================
##@ Fine-Grained Modules

# Compute — Spark only needs core (MinIO + Iceberg)
up-compute: up-core ## Spark + Iceberg ELT engine
	@echo "🔧 Starting Spark compute engine..."
	$(COMPOSE) --profile compute up -d --wait

# Query — Trino only needs Iceberg catalog (in core)
up-query: up-core ## Trino federated SQL (usable standalone with DBeaver/SQL clients)
	@echo "🔍 Starting Trino query engine..."
	$(COMPOSE) --profile query up -d --wait

# BI — Superset queries Trino → auto-boots Trino first
up-bi: up-query ## Superset dashboards (auto-starts Trino)
	@echo "📊 Starting Superset BI dashboards..."
	$(COMPOSE) --profile bi up -d --wait

# CDC Ingestion — foundation for all real-time consumers
up-cdc: up-core ## CDC: Debezium / Kafka Connect
	@echo "📥 Starting CDC ingestion (Connect/Debezium)..."
	$(COMPOSE) --profile cdc up -d --wait

# Orchestration (standalone) — Airflow orchestrates Spark + Trino
# → auto-boots both so DAGs are ready to run on arrival
up-orchestration: up-compute up-query ## Airflow + dependencies (Spark + Trino auto-started)
	@echo "⚙️  Starting Airflow (with Spark + Trino ready)..."
	$(COMPOSE) --profile orchestration up -d --wait

# Stream Processing — Flink enriches CDC streams → needs Connect running
up-stream-processing: up-cdc ## Flink stream processing (auto-starts CDC)
	@echo "🌊 Starting Flink stream processing..."
	$(COMPOSE) --profile stream-processing up -d --wait

# OLAP — ClickHouse is a Kafka sink fed by CDC → needs Connect running
up-olap: up-cdc ## ClickHouse + Flash Sale dashboard (auto-starts CDC)
	@echo "⚡ Starting ClickHouse OLAP analytics..."
	$(COMPOSE) --profile olap up -d --wait

# Search — OpenSearch is a CDC sink (Debezium → Kafka → OpenSearch connector)
up-search: up-cdc ## OpenSearch full-text search (auto-starts CDC)
	@echo "🔎 Starting OpenSearch..."
	$(COMPOSE) --profile search up -d --wait

# AI/ML — Granular AI services reading from Postgres (in core)
up-recommendation: up-core ## Flask Recommendation API
	@echo "🤖 Starting Flask Recommendation API..."
	$(COMPOSE) --profile ai-ml up -d --wait flask-recommendation

up-semantic-search: up-core ## Streamlit Semantic Search (pgvector + sentence-transformers)
	@echo "🧠 Starting Streamlit Semantic Search..."
	$(COMPOSE) --profile ai-ml up -d --wait streamlit-search

# Observability — scrapes metrics from whatever is running
up-observability: up-core ## Prometheus + Grafana + Alertmanager (scrapes whatever is running)
	@echo "📈 Starting observability stack..."
	$(COMPOSE) --profile observability up -d --wait

# Dev Tools — Redpanda Console (Kafka UI); MailHog is started as part of up-core
up-devtools: up-core ## Redpanda Console (Kafka topic browser)
	@echo "🛠️  Starting developer tools..."
	$(COMPOSE) --profile devtools up -d --wait

# ============================================================
# COMPOSITE PROFILES
# Convenience groupings
# ============================================================
##@ Composite Profiles
up-batch: up-core ## Batch stack: Airflow + Spark + Trino + Superset
	@echo "📦 Starting full batch stack (Spark + Trino + Superset + Airflow)..."
	$(COMPOSE) --profile batch up -d --wait

up-realtime: up-core ## Realtime stack: CDC + Flink + ClickHouse + OpenSearch
	@echo "⚡ Starting full realtime stack (CDC + Flink + ClickHouse + OpenSearch)..."
	$(COMPOSE) --profile realtime up -d --wait

# AI/ML — Flask serves a pre-trained model stored in Postgres (in core)
#          Streamlit Search reads pgvector from Postgres (in core)
#          Model training happens offline via Spark; no runtime dependency.
up-aiml: up-core ## AI/ML stack: Flask Recommendation API + Streamlit Semantic Search
	@echo "🤖 Starting full AI/ML stack..."
	$(COMPOSE) --profile ai-ml up -d --wait

up-all: up-core ## Full platform — all modules
	@echo "🚀 Starting the full OneShop platform..."
	$(COMPOSE) $(ALL_PROFILES) up -d --wait

# ============================================================
# LIFECYCLE
# ============================================================
##@ Lifecycle
down: ## Stop all running services
	@echo "🛑 Stopping all services..."
	$(COMPOSE) $(ALL_PROFILES) down

down-volumes: ## Stop all services and remove volumes
	@echo "🛑 Stopping all services and removing volumes..."
	$(COMPOSE) $(ALL_PROFILES) down -v

# ============================================================
# DATA & ELT
# ============================================================
##@ Data & ELT
seed-batch: ## Seed Postgres OLTP tables + MinIO pageviews
	$(call check_running,postgres,core)
	$(GENERATOR) batch --users $(USERS) --items $(ITEMS) --purchases $(PURCHASES) --pageviews $(PAGEVIEWS)

seed-cdc: ## Simulate inventory CDC updates
	$(GENERATOR) cdc --count $(CDC_COUNT) --interval $(CDC_INTERVAL)

seed-purchases: ## Stream flash sale purchase events to Kafka
	$(GENERATOR) purchases --count $(TX_COUNT) --interval $(TX_INTERVAL)

seed-logins: ## Stream login events to Kafka
	$(GENERATOR) logins --count $(LOGIN_COUNT) --interval $(LOGIN_INTERVAL)

seed-reviews: ## Seed customer reviews for pgvector
	$(GENERATOR) reviews --count $(REVIEWS_COUNT)

# ============================================================
# KAFKA CONNECT — Register Connectors
# ============================================================
# (grouped under ##@ Data & ETL above)
connectors: ## Register all Kafka Connect connectors
	$(call check_running,connect,cdc)
	@if ! curl -s -f http://localhost:8083/ > /dev/null; then \
		echo "⏳ Waiting for Kafka Connect API (port 8083) to open..."; \
		until curl -s -f http://localhost:8083/ > /dev/null; do sleep 2; done; \
	fi
	@echo "Registering Debezium items connector..."
	curl -s -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @kafka-connect/connectors/items-connector.json | jq .
	@echo "Registering Debezium purchases connector..."
	curl -s -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @kafka-connect/connectors/purchases-connector.json | jq .
	@echo "Registering OpenSearch sink connector..."
	curl -s -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @kafka-connect/connectors/opensearch-sink.json | jq .
	@echo "✅ All connectors registered."

connectors-status: ## Check health of registered Kafka Connect connectors
	$(call check_running,connect,cdc)
	@curl -s http://localhost:8083/connectors | jq .

# ============================================================
# SPARK
# ============================================================
# (grouped under ##@ Data & ETL above)
spark-submit: ## Run a Spark script (usage: make spark-submit SCRIPT=postgres_loader.py)
	$(call check_running,spark-lab,compute)
	docker exec spark-lab /opt/spark/bin/spark-submit /home/iceberg/pyspark/scripts/$(SCRIPT)

spark-shell: ## Open interactive PySpark console
	$(call check_running,spark-lab,compute)
	docker exec -it spark-lab /opt/spark/bin/pyspark

# ============================================================
# ELT PIPELINE
# ============================================================
# (grouped under ##@ Data & ELT above)
init-lakehouse: ## Initialize Iceberg namespaces (bronze, silver, gold) and baseline tables
	$(call check_running,spark-lab,compute)
	@echo "🧊 Initializing Iceberg Lakehouse namespaces..."
	docker exec spark-lab /opt/spark/bin/spark-sql -f /home/iceberg/pyspark/scripts/init_iceberg_warehouse.sql

unpause-dags: init-lakehouse ## Unpause all Airflow DAGs so they can run on schedule
	$(call check_running,airflow-scheduler,orchestration)
	@echo "▶️  Unpausing Airflow DAGs..."
	docker exec airflow-scheduler bash -c "\
		airflow dags unpause lakehouse_hydration && \
		airflow dags unpause bronze_to_silver && \
		airflow dags unpause gold_table_refresh && \
		airflow dags unpause user_engagement_segments && \
		airflow dags unpause ml_training_pipeline && \
		airflow dags unpause iceberg_maintenance \
	"

setup-batch: ## Seed data and initialize lakehouse + DAGs for a fresh batch environment
	@echo "🔍 Checking Postgres data..."
	@ITEM_COUNT=$$(docker exec postgres psql -U postgresuser -d oneshop -t -c "SELECT COUNT(*) FROM items;" 2>/dev/null | tr -d ' ' || echo 0); \
	if [ "$$ITEM_COUNT" -eq 0 ]; then \
		echo "⚠️  Postgres tables are empty! Seeding batch data..."; \
		$(MAKE) seed-batch; \
	else \
		echo "✅ Postgres is already seeded ($$ITEM_COUNT items found)."; \
	fi
	$(MAKE) unpause-dags
	@echo "✅ Batch platform is fully initialized and pipelines are unpaused!"

etl-trigger: ## Trigger the Airflow lakehouse_hydration DAG to kick off the ELT pipeline
	$(call check_running,airflow-webserver,orchestration)
	docker exec airflow-webserver airflow dags trigger lakehouse_hydration

etl-features: ## Compute and store ML feature vectors (runs compute_features.py on Spark)
	$(MAKE) spark-submit SCRIPT=compute_features.py

etl-train: ## Train ALS Recommendation model using Spark MLlib (runs train_als.py)
	$(MAKE) spark-submit SCRIPT=train_als.py

# ============================================================
# STREAMING PIPELINE
# ============================================================
##@ Streaming Pipeline
setup-cdc: ## Setup Kafka Connect / Debezium and open-search sinks
	./scripts/setup_realtime.sh cdc

setup-flink: ## Setup Flink Stream Processing (topics, tables, jobs)
	./scripts/setup_realtime.sh flink

setup-realtime: ## Setup full real-time streaming stack (CDC + Flink)
	./scripts/setup_realtime.sh all

stream-tables: ## Initialize Flink tables in the JobManager
	$(call check_running,flink-jobmanager,stream-processing)
	@echo "🌊 Creating Flink stream processing tables..."
	@docker exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/create-tables.sql 2>&1 | grep -v "WARNING: Unknown module: jdk.compiler" | grep -v "WARNING: Unable to create a system terminal" || true

stream-jobs: ## Start Flink streaming jobs
	$(call check_running,flink-jobmanager,stream-processing)
	@echo "🚀 Starting Flink streaming jobs..."
	@docker exec flink-jobmanager ./bin/sql-client.sh -i /opt/flink/sql/create-tables.sql -f /opt/flink/sql/insert-jobs.sql 2>&1 | grep -v "WARNING: Unknown module: jdk.compiler" | grep -v "WARNING: Unable to create a system terminal" || true

# ============================================================
# TESTING
# ============================================================
##@ Testing
test-core: ## Test always-on services: Postgres, Kafka, MinIO, Schema Registry
	@echo "🧪 Running infrastructure integration tests (@core)..."
	$(COMPOSE) --profile test run --rm test-runner pytest -m core

test-aiml: ## Test Flask Recommendation API (requires: make up-aiml)
	$(call check_running,flask-recommendation,up-aiml)
	@echo "🤖 Running AI/ML integration tests (@aiml)..."
	$(COMPOSE) --profile test run --rm test-runner pytest -m aiml

test-query: ## Test Trino query engine (requires: make up-query)
	$(call check_running,trino,query)
	@echo "🔍 Running query integration tests (@query)..."
	$(COMPOSE) --profile test run --rm test-runner pytest -m query

test-orchestration: ## Test Airflow orchestration (requires: make up-orchestration)
	$(call check_running,airflow-webserver,orchestration)
	@echo "⚙️  Running orchestration integration tests (@orchestration)..."
	$(COMPOSE) --profile test run --rm test-runner pytest -m orchestration

test-olap: ## Test ClickHouse OLAP engine (requires: make up-olap)
	$(call check_running,clickhouse,olap)
	@echo "⚡ Running OLAP integration tests (@olap)..."
	$(COMPOSE) --profile test run --rm test-runner pytest -m olap

test: ## Run full integration test suite (requires make up-core)
	@echo "🧪 Running full integration test suite..."
	@echo "💡 Tip: use make test-core / test-aiml / test-query / test-orchestration / test-olap for targeted runs"
	$(COMPOSE) --profile test run --rm test-runner pytest

test-ci: download-jmx ## CI-safe full test run — auto-teardown with exit-code propagation
	@echo "🚦 CI test run (abort-on-exit, exit-code from test-runner)..."
	$(COMPOSE) --profile ai-ml --profile query --profile orchestration --profile olap --profile test \
		up --abort-on-container-exit --exit-code-from test-runner

# ============================================================
# UTILITIES
# ============================================================
##@ Utilities
logs: ## Tail container logs for active services
	$(COMPOSE) $(ALL_PROFILES) logs -f --tail=50

status: ## Check health status of all running services
	$(COMPOSE) $(ALL_PROFILES) ps

clean: ## Remove all containers, volumes, and build cache
	$(COMPOSE) $(ALL_PROFILES) down -v --rmi local
	docker system prune -f

docs: ## Serve the documentation site locally at http://localhost:8000
	@command -v mkdocs >/dev/null 2>&1 || pip install mkdocs-material --break-system-packages
	mkdocs serve --dev-addr=0.0.0.0:8000
