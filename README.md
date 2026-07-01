# 🏪 OneShop Data Platform

> A **production-grade data engineering platform** built on Apache open-source projects, implementing batch processing, real-time streaming, and ML/AI pipelines on a unified Lakehouse architecture.

[![CI Pipeline](https://github.com/Mohammed-Al-Zubiri/oneshop_data_platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Mohammed-Al-Zubiri/oneshop_data_platform/actions)

---

## Architecture Overview

This platform implements a complete data lifecycle for **OneShop**, a fictional e-commerce company, demonstrating production data engineering patterns across four capability tiers:

| Layer | Technologies | Purpose |
|:------|:-------------|:--------|
| **CDC Ingestion** | Kafka (KRaft), Debezium, Schema Registry | Event backbone with schema enforcement |
| **Lakehouse** | Apache Iceberg, MinIO (S3), REST Catalog | Open table format with time travel and multi-engine access |
| **Batch Processing** | Apache Spark, Airflow, Trino, Superset | Medallion ELT (Bronze → Silver → Gold) with orchestration and BI |
| **Stream Processing** | Apache Flink SQL, ClickHouse, OpenSearch | Real-time enrichment, anomaly detection, OLAP analytics |
| **ML / AI** | Spark MLlib (ALS), sentence-transformers, pgvector | Collaborative filtering + semantic vector search |
| **Observability** | Prometheus, Grafana, Alertmanager | Unified metrics across infrastructure, queues, and runtimes |

```
┌─────────────────────────────────────────────────────────────────────┐
│                      OneShop Data Platform                          │
│                                                                     │
│  ┌──────────┐    ┌───────────┐ ┌──────────────────────────────────┐ │
│  │ Postgres │──▶│ Debezium  │─▶│  Apache Kafka (KRaft + Schema   │ │
│  │ (OLTP)   │    │ (CDC)     │ │  Registry)                       │ │
│  └──────────┘    └───────────┘ └───────┬──────┬───────┬───────────┘ │
│                                        │      │       │             │
│                                        ▼      ▼       ▼             │
│  ┌──────────┐   ┌───────────┐  ┌──────────┐ ┌──────┐ ┌──────────┐   │
│  │  MinIO   │◀▶│   Spark   │  │  Flink   │ │Click-│ │OpenSearch│   │
│  │  (S3)    │   │ (PySpark) │  │  (SQL)   │ │House │ │          │   │
│  └────┬─────┘   └───────────┘  └──────────┘ └──────┘ └──────────┘   │
│       ▼                                                             │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │       Apache Iceberg (Bronze → Silver → Gold)               │    │
│  │       REST Catalog · Time Travel · Schema Evolution         │    │
│  └────────────────────────┬────────────────────────────────────┘    │
│                           ▼                                         │
│  ┌──────────┐   ┌──────────────┐  ┌──────────┐  ┌──────────────┐    │
│  │  Trino   │─▶│   Superset   │  │  Flask   │  │  Streamlit   │    │
│  │ (Query)  │   │ (Dashboards) │  │ (Rec API)│  │ (Search UI)  │    │
│  └──────────┘   └──────────────┘  └──────────┘  └──────────────┘    │
│                                                                     │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐     │
│  │ Airflow (Orchestration)  │  │   Prometheus + Grafana       │     │
│  └──────────────────────────┘  └──────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

> [!TIP]
> **Full documentation is available as a local site.**
> ```bash
> make docs
> ```
> Then open [http://localhost:8000](http://localhost:8000). The docs cover architecture decisions, every module, the data model, and a full `make` command reference.

---

## Quick Start

### Prerequisites

| Requirement | Minimum | Notes |
|:------------|:--------|:------|
| Docker Engine | 24+ with Compose V2 | `docker compose version` must return v2.x |
| RAM | 8 GB free | For the core tier alone |
| Disk | 20 GB free | Images + Iceberg data + Spark JARs |
| Tools | `make`, `curl`, `jq` | Pre-installed on most Linux/macOS |

### 1. Clone & Configure

```bash
git clone https://github.com/Mohammed-Al-Zubiri/oneshop_data_platform.git
cd oneshop_data_platform
cp .env.example .env
```

### 2. Start Core Infrastructure

```bash
make up-core    # Postgres · Kafka (KRaft) · MinIO · Iceberg Catalog · MailHog
```

> [!NOTE]
> On first boot, `make up-core` downloads the JMX Prometheus Java Agent JAR (~2.8 MB) used for Kafka metrics — a one-time operation cached in `monitoring/prometheus/`.

### 3. Seed Data

```bash
make seed-batch   # 100 users · 1 000 items · 5 000 purchases · 10 000 pageviews
```

Override any default: `make seed-batch USERS=500 PURCHASES=20000`

> [!IMPORTANT]
> Run `make seed-batch` **before** starting real-time event generators (`seed-purchases`, `seed-logins`). Streaming jobs join against Postgres records — missing rows will cause downstream failures.

### 4. Start a Module

Each `make up-*` target is self-contained and starts exactly what it needs:

```bash
# Composite stacks
make up-batch        # Airflow + Spark + Trino + Superset
make up-realtime     # CDC + Flink + ClickHouse + OpenSearch
make up-aiml         # Flask Recommendation API + Streamlit Search
make up-observability # Prometheus + Grafana + Alertmanager
make up-all          # Full platform

# Fine-grained modules
make up-compute      # Spark + Iceberg only
make up-query        # Trino (usable with any SQL client)
make up-bi           # Superset (auto-starts Trino)
make up-cdc          # Kafka Connect / Debezium
make up-stream-processing  # Flink (auto-starts CDC)
make up-olap         # ClickHouse (auto-starts CDC)
make up-search       # OpenSearch (auto-starts CDC)
make up-devtools     # Redpanda Console (Kafka topic browser)
```

### 5. Initialize Pipelines

```bash
# Batch
make setup-batch     # Init Iceberg namespaces, unpause Airflow DAGs
make etl-trigger     # Trigger the lakehouse_hydration DAG

# Real-Time
make setup-realtime  # Register CDC connectors, create Flink tables, start jobs
```

---

## Service Endpoints

| Service | URL | Credentials |
|:--------|:----|:------------|
| Airflow | http://localhost:8082 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / password |
| Spark Notebooks | http://localhost:8888 | — |
| Trino | http://localhost:9090 | test |
| Flink Dashboard | http://localhost:8084 | — |
| ClickHouse HTTP | http://localhost:8123 | default / mysecret |
| Flask Rec API | http://localhost:5050 | — |
| Streamlit Flash Sale | http://localhost:8501 | — |
| Streamlit Semantic Search | http://localhost:8502 | — |
| Redpanda Console | http://localhost:8085 | — |
| Schema Registry | http://localhost:8081 | — |
| Kafka Connect | http://localhost:8083 | — |
| Prometheus | http://localhost:9091 | — |
| MailHog | http://localhost:8025 | — |

---

## Project Structure

```
oneshop_data_platform/
├── docker-compose.yml                  # Core (always-on): Postgres, Kafka, MinIO, Iceberg, MailHog
├── docker-compose/                     # Module-level compose files (12 total)
│   ├── docker-compose.orchestration.yml
│   ├── docker-compose.compute.yml
│   ├── docker-compose.query.yml
│   ├── docker-compose.bi.yml
│   ├── docker-compose.cdc.yml
│   ├── docker-compose.streaming.yml
│   ├── docker-compose.olap.yml
│   ├── docker-compose.search.yml
│   ├── docker-compose.aiml.yml
│   ├── docker-compose.observability.yml
│   ├── docker-compose.devtools.yml
│   └── docker-compose.test.yml
├── Makefile                            # All commands with dependency chains
├── .env.example                        # Environment variable template
├── .github/workflows/ci.yml           # CI/CD pipeline
│
├── postgres/                           # OLTP schema + Dockerfile (WAL-enabled)
├── spark/scripts/                      # PySpark ELT scripts (Bronze/Silver/Gold/ML)
├── airflow/dags/                       # Workflow DAGs
├── trino/etc/                          # Trino config + Iceberg catalog
├── flink/sql/                          # Flink SQL table definitions and jobs
├── kafka-connect/connectors/           # Debezium + OpenSearch connector configs
├── clickhouse/                         # ClickHouse DDL and config
├── flask/                              # Recommendation REST API
├── streamlit/                          # Flash sale + semantic search dashboards
├── data-generator/                     # Unified CLI data generator
├── monitoring/                         # Prometheus + Grafana + Alertmanager configs
├── great-expectations/                 # Data quality checkpoints
├── tests/                              # Integration test suite (pytest)
└── docs/                               # Documentation site (mkdocs-material)
```

---

## Testing

The platform includes a containerized integration test suite (`pytest`) that runs inside a dedicated `test-runner` container on the internal Docker network.

```bash
make test-core    # Core infrastructure: Postgres, Kafka, MinIO, Schema Registry
make test-aiml    # Flask Recommendation API  (requires: make up-aiml)
make test-query   # Trino federated SQL        (requires: make up-query)
make test         # Full suite
make test-ci      # CI mode — auto teardown with exit-code propagation
```

Tests use `pytest` markers to skip gracefully when a targeted service isn't running, so you never need the full 30+ GB stack online to run a subset.

See [`docs/docs/testing.md`](docs/docs/testing.md) for the full testing guide.

---

## Documentation

All detailed documentation lives in [`./docs`](docs/docs/) and is served via MkDocs Material:

```bash
make docs   # Serves at http://localhost:8000
```

| Section | Path |
|:--------|:-----|
| Getting Started | `docs/docs/getting-started.md` |
| Architecture & Design Decisions | `docs/docs/architecture.md` |
| Batch Pipeline | `docs/docs/modules/batch.md` |
| Real-Time Pipeline | `docs/docs/modules/realtime.md` |
| ML & AI | `docs/docs/modules/ml.md` |
| Observability | `docs/docs/modules/observability.md` |
| Data Model | `docs/docs/data-model.md` |
| Testing | `docs/docs/testing.md` |
| Make Command Reference | `docs/docs/reference/make-commands.md` |
| Service Endpoints | `docs/docs/reference/service-endpoints.md` |

---

## References

Based on [*Practical Data Engineering with Apache Projects*](https://link.springer.com/book/10.1007/979-8-8688-2142-4) by Dunith Danushka (Apress, 2025).

---

## License

MIT License — See [LICENSE](LICENSE) for details.
