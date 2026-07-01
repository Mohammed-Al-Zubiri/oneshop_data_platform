# Make Commands Reference

Complete reference for every `make` target. Run `make help` at any time for a live summary.

---

## Foundation & Core

| Command | What it does | Auto-starts |
|:--------|:-------------|:------------|
| `make up-core` | Starts Postgres, Kafka (KRaft), Schema Registry, MinIO, Iceberg REST Catalog, MailHog | — |

---

## Fine-Grained Modules

Each module target also starts `up-core` automatically.

| Command | What it starts | Auto-starts |
|:--------|:---------------|:------------|
| `make up-compute` | Spark Master + Worker + Lab (Jupyter) | core |
| `make up-query` | Trino query engine | core |
| `make up-bi` | Apache Superset | core + Trino |
| `make up-cdc` | Kafka Connect + Debezium | core |
| `make up-orchestration` | Airflow Webserver + Scheduler + DB | core + Spark + Trino |
| `make up-stream-processing` | Flink JobManager + TaskManager | core + CDC |
| `make up-olap` | ClickHouse + Streamlit Flash Sale | core + CDC |
| `make up-search` | OpenSearch | core + CDC |
| `make up-recommendation` | Flask Recommendation API only | core |
| `make up-semantic-search` | Streamlit Semantic Search only | core |
| `make up-observability` | Prometheus + Grafana + Alertmanager + exporters | core |
| `make up-devtools` | Redpanda Console (Kafka topic browser) | core |

---

## Composite Profiles

| Command | What it starts |
|:--------|:---------------|
| `make up-batch` | core + Airflow + Spark + Trino + Superset |
| `make up-realtime` | core + CDC + Flink + ClickHouse + OpenSearch |
| `make up-aiml` | core + Flask Rec API + Streamlit Semantic Search |
| `make up-all` | Everything |

---

## Lifecycle

| Command | What it does |
|:--------|:-------------|
| `make down` | Stop all running containers (volumes preserved) |
| `make down-volumes` | Stop all containers and delete all volumes/data |
| `make status` | Show health status of all running containers |
| `make logs` | Tail last 50 lines from all containers (live) |
| `make clean` | Full wipe: containers + volumes + local images + Docker cache |

---

## Data & Seeding

| Command | What it does | Override variables |
|:--------|:-------------|:------------------|
| `make seed-batch` | Seed Postgres with users/items/purchases/pageviews | `USERS`, `ITEMS`, `PURCHASES`, `PAGEVIEWS` |
| `make seed-cdc` | Simulate inventory item updates as a CDC batch | `CDC_COUNT`, `CDC_INTERVAL` |
| `make seed-purchases` | Stream purchase events to Kafka in real-time | `TX_COUNT`, `TX_INTERVAL` |
| `make seed-logins` | Stream login events to Kafka in real-time | `LOGIN_COUNT`, `LOGIN_INTERVAL` |
| `make seed-reviews` | Seed product reviews (for pgvector embeddings) | `REVIEWS_COUNT` |

**Examples:**
```bash
make seed-batch USERS=500 ITEMS=5000 PURCHASES=25000
make seed-cdc CDC_COUNT=10000 CDC_INTERVAL=0.05   # Fast burst
make seed-purchases TX_COUNT=1000 TX_INTERVAL=0.2  # Realistic stream
```

---

## Kafka Connect

| Command | What it does |
|:--------|:-------------|
| `make connectors` | Register all Debezium + OpenSearch sink connectors |
| `make connectors-status` | Check health of all registered connectors |

---

## Spark

| Command | What it does |
|:--------|:-------------|
| `make spark-submit SCRIPT=<name>.py` | Submit a Spark script to the running cluster |
| `make spark-shell` | Open an interactive PySpark REPL |

**Available scripts:**
```bash
make spark-submit SCRIPT=postgres_loader.py    # Load Postgres → Bronze
make spark-submit SCRIPT=minio_loader.py       # Load MinIO → Bronze
make spark-submit SCRIPT=bronze_to_silver.py   # Bronze → Silver
make spark-submit SCRIPT=compute_features.py   # Compute ML features
make spark-submit SCRIPT=train_als.py          # Train ALS model
make spark-submit SCRIPT=validate_bronze.py    # Run Great Expectations
```

---

## ELT Pipeline

| Command | What it does |
|:--------|:-------------|
| `make init-lakehouse` | Create Iceberg namespaces (bronze, silver, gold) and baseline tables |
| `make setup-batch` | Guard-checks Postgres seed data → runs `init-lakehouse` → unpauses DAGs |
| `make unpause-dags` | Unpause all 6 Airflow DAGs so they run on schedule |
| `make etl-trigger` | Manually trigger the `lakehouse_hydration` Airflow DAG |
| `make etl-features` | Run `compute_features.py` on Spark (ML feature engineering) |
| `make etl-train` | Run `train_als.py` on Spark (ALS model training) |

---

## Streaming Pipeline

| Command | What it does |
|:--------|:-------------|
| `make setup-cdc` | Register Debezium + OpenSearch connectors via `setup_realtime.sh cdc` |
| `make setup-flink` | Create Flink SQL tables + start jobs via `setup_realtime.sh flink` |
| `make setup-realtime` | Full setup: CDC + Flink via `setup_realtime.sh all` |
| `make stream-tables` | Create Flink SQL tables only |
| `make stream-jobs` | Start Flink streaming jobs only |

---

## Testing

| Command | What it does | Requires |
|:--------|:-------------|:---------|
| `make test-core` | Run `@core` marked tests | `make up-core` |
| `make test-aiml` | Run `@aiml` marked tests | `make up-aiml` |
| `make test-query` | Run `@query` marked tests | `make up-query` |
| `make test` | Run full suite (graceful skip for missing services) | `make up-core` |
| `make test-ci` | CI mode: auto-start services, run tests, auto-teardown | — |

---

## Documentation

| Command | What it does |
|:--------|:-------------|
| `make docs` | Serve the documentation site at [http://localhost:8000](http://localhost:8000) |

`mkdocs-material` is installed automatically if not found. Press `Ctrl-C` to stop.

---

## Seeding Variable Reference

| Variable | Default | Used by |
|:---------|:--------|:--------|
| `USERS` | 100 | `seed-batch` |
| `ITEMS` | 1000 | `seed-batch` |
| `PURCHASES` | 5000 | `seed-batch` |
| `PAGEVIEWS` | 10000 | `seed-batch` |
| `CDC_COUNT` | 1000 | `seed-cdc` |
| `CDC_INTERVAL` | 0.5 | `seed-cdc` (seconds between events) |
| `TX_COUNT` | 500 | `seed-purchases` |
| `TX_INTERVAL` | 0.3 | `seed-purchases` |
| `LOGIN_COUNT` | 1000 | `seed-logins` |
| `LOGIN_INTERVAL` | 0.2 | `seed-logins` |
| `REVIEWS_COUNT` | 200 | `seed-reviews` |
