# 🏪 OneShop Data Platform

> A **local reference implementation of production data engineering patterns** — batch processing, real-time streaming, and ML/AI pipelines on a unified Lakehouse architecture, built entirely on Apache open-source projects.

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

## 📚 Based On — And What I Built Beyond It

This platform is grounded in the reference architecture from
[*Practical Data Engineering with Apache Projects*](https://link.springer.com/book/10.1007/979-8-8688-2142-4)
by Dunith Danushka (Apress, 2025), which walks through ten standalone data
engineering pipelines for the fictional OneShop e-commerce company using
Apache Spark, Iceberg, Kafka, Flink, Airflow, and more.

**The book structures each pipeline as an independent, chapter-scoped Docker
Compose project.** I took those ten isolated environments and re-engineered
them into a single, cohesive platform — sharing the same source database,
Kafka backbone, and Iceberg Lakehouse across every module. Beyond the
consolidation itself, I designed and implemented the following enhancements
that are not covered in the book:

- **Modular 12-File Docker Compose with composite profiles.** The book's
  approach requires booting a separate, full Docker environment per chapter.
  I refactored the infrastructure into 12 fine-grained Compose files activated
  via named profiles, coordinated by a dependency-aware `Makefile`. A developer
  working on the batch pipeline starts only what batch needs — Flink, ClickHouse,
  and OpenSearch stay down, preserving RAM.

- **Airflow Dataset-driven DAG chaining.** The book implements a single
  Airflow DAG (`user_engagement_segments`) on a `@daily` cron schedule. I
  extended this into a full orchestration layer of six DAGs — five of which use
  [Airflow Datasets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
  to chain automatically on data-readiness events rather than fixed clock
  intervals. `bronze_to_silver` fires when `lakehouse_hydration` writes new
  Bronze data; Gold refresh fires when Silver updates — no wasted Spark jobs
  on unchanged data.

- **Automated ML retraining pipeline.** The book runs ALS feature engineering
  and model training as manually executed Spark scripts. I wired both jobs into
  a dedicated `ml_training_pipeline` Airflow DAG on a `@weekly` schedule,
  with a `PythonSensor` that blocks training until Silver data is confirmed
  available, and runs embedding generation in parallel with ALS training.

- **Great Expectations data quality gate.** The book does not include a data
  quality layer. I added a Great Expectations checkpoint that runs as the first
  task in the `bronze_to_silver` DAG. If any expectation fails (null IDs, out-
  of-range amounts, type errors), the task raises a failure and the DAG halts —
  Silver transformation never runs, protecting downstream Gold tables and ML
  features from bad source data.

- **Prometheus + Grafana observability stack.** Not covered in the book. I built
  a unified monitoring layer that scrapes seven targets — Kafka JVM (via JMX
  agent delivered through a named Docker volume), consumer group lag, Flink
  JobManager and TaskManager, Airflow (bridged from UDP StatsD via
  `statsd-exporter`), ClickHouse, and Flask. Four auto-provisioned Grafana
  dashboards and Alertmanager alert rules round out the stack.

- **Containerized integration test suite and CI pipeline.** The book has no
  testing framework. I implemented a `pytest` suite that runs inside a dedicated
  container on the internal Docker network, with graceful-skip fixtures so any
  module subset can be tested without booting the full platform. A GitHub
  Actions workflow runs lint, Compose validation across all profiles, Prometheus
  rule checking, and the integration tests on every push to `main`.

- **Presigned MinIO download URL in marketing email.** The book's
  `EmailOperator` sends a plain file path string to the marketing team. I replaced
  this with a 7-day presigned MinIO object URL embedded as a styled button in
  the HTML email body — so the marketing team can download the segments CSV
  directly without needing access to the data platform.

---

## License

MIT License — See [LICENSE](LICENSE) for details.
