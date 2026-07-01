# Getting Started

This guide takes you from a fresh clone to a running platform in under 10 minutes for the core tier, and explains every module you can layer on top.

---

## Prerequisites

| Requirement | Minimum | Notes |
|:------------|:--------|:------|
| **Docker Engine** | 24+ with Compose V2 | `docker compose version` must return v2.x |
| **RAM** | 8 GB free | Core tier; more modules need more headroom |
| **Disk** | 20 GB free | Images + Iceberg data + Spark jars |
| **OS** | Linux, macOS, WSL2 | Windows native Docker not tested |
| **Tools** | `make`, `curl`, `jq` | Pre-installed on most Linux/macOS |

---

## Step 1 — Clone & Configure

```bash
git clone https://github.com/Mohammed-Al-Zubiri/oneshop_data_platform.git
cd oneshop_data_platform

# Copy the environment template — review it, don't change defaults for a first run
cp .env.example .env
```

The `.env` file contains passwords and ports. The defaults work out of the box locally; nothing is exposed to the internet.

---

## Step 2 — Start Core Infrastructure

```bash
make up-core
```

This single command starts:

| Container | Role |
|:----------|:-----|
| `postgres` | OLTP source database (WAL replication enabled for CDC) |
| `kafka` | Event backbone — KRaft mode (no Zookeeper) |
| `schema-registry` | Avro schema enforcement for Kafka topics |
| `minio` + `mc` | S3-compatible object store + bucket initialiser |
| `iceberg-catalog` | REST catalog for Iceberg table metadata |
| `mailhog` | Local SMTP sink for alert emails |

!!! note "JMX Agent download"
    On first run, `make up-core` also downloads the JMX Prometheus Java Agent JAR (~2.8 MB) needed for Kafka metrics. This is a one-time download cached in `monitoring/prometheus/`.

**Verify it's healthy:**

```bash
make status                                        # All containers should show "healthy"
docker exec postgres pg_isready -U postgresuser    # Postgres ready
curl -s http://localhost:9000/minio/health/ready   # MinIO ready
curl -s http://localhost:8181/v1/config | jq .     # Iceberg catalog ready
```

---

## Step 3 — Seed Data

```bash
make seed-batch
```

This populates:

- **100 users**, **1 000 items**, **5 000 purchases**, **10 000 pageviews** into Postgres
- Raw pageview JSON files into MinIO (the Bronze source for the batch pipeline)

You can override any default:

```bash
make seed-batch USERS=500 ITEMS=5000 PURCHASES=20000 PAGEVIEWS=50000
```

!!! important "Seed before streaming"
    The real-time event generators (`make seed-purchases`, `make seed-logins`) produce random User IDs (1–100) and Item IDs (1–1000). You **must** run `make seed-batch` at least once first — downstream Flink and ClickHouse joins will fail without matching records in Postgres.

!!! important "Seed reviews before using semantic search"
    The pgvector semantic search UI requires review embeddings. After seeding batch data, run:
    ```bash
    make seed-reviews   # Generates 200 reviews + embeddings by default
    ```
    Without this step, the Streamlit Semantic Search UI at [http://localhost:8502](http://localhost:8502) will return empty results.

---

## Step 4 — Pick Your Module(s)

Each `make up-*` target is self-contained and starts exactly what it needs — you don't need to run prerequisite commands manually.

=== "Batch Pipeline"

    ```bash
    make up-batch      # Airflow + Spark + Trino + Superset (includes core)
    make setup-batch   # Initialize Iceberg namespaces + unpause DAGs
    ```

    - **Airflow UI** → [http://localhost:8082](http://localhost:8082) (admin / admin)
    - **Superset** → [http://localhost:8088](http://localhost:8088) (admin / admin)
    - **Trino** → [http://localhost:9090](http://localhost:9090)

    See the [Batch Pipeline guide](modules/batch.md) for a full walkthrough.

=== "Real-Time Pipeline"

    ```bash
    make up-realtime    # CDC + Flink + ClickHouse + OpenSearch (includes core)
    make setup-realtime # Register connectors + create Flink tables + start jobs
    ```

    - **Flink Dashboard** → [http://localhost:8084](http://localhost:8084)
    - **ClickHouse HTTP** → [http://localhost:8123](http://localhost:8123)
    - **Redpanda Console** → [http://localhost:8085](http://localhost:8085)

    See the [Real-Time Pipeline guide](modules/realtime.md).

=== "AI / ML"

    ```bash
    make up-aiml        # Flask Rec API + Streamlit Search (includes core)
    make up-compute     # Spark (needed for training)
    make etl-features   # Compute feature vectors (requires Silver data)
    make etl-train      # Train ALS model (requires Silver data)
    ```

    - **Flask Rec API** → [http://localhost:5050](http://localhost:5050)
    - **Streamlit Semantic Search** → [http://localhost:8502](http://localhost:8502)

    See the [ML & AI guide](modules/ml.md).

=== "Observability"

    ```bash
    make up-observability   # Prometheus + Grafana + Alertmanager (includes core)
    ```

    - **Grafana** → [http://localhost:3000](http://localhost:3000) (admin / admin)
    - **Prometheus** → [http://localhost:9091](http://localhost:9091)
    - **MailHog** → [http://localhost:8025](http://localhost:8025)

    See the [Observability guide](modules/observability.md).

=== "Full Platform"

    ```bash
    make up-all         # Every module
    make setup-batch    # Initialize and unpause batch pipelines
    make setup-realtime # Initialize real-time connectors + Flink
    ```

---

## Step 5 — Verify Everything

```bash
make status    # Docker health summary for all running services
make logs      # Tail logs from everything (Ctrl-C to stop)
```

---

## Tearing Down

```bash
make down           # Stop all containers (volumes preserved)
make down-volumes   # Stop all containers AND delete all volumes/data
make clean          # Full wipe: containers + volumes + images + Docker cache
```

---

## Getting Help

```bash
make help    # Formatted menu of every available target
```

Every `make` target has a description. The output is grouped by section — Foundation, Modules, Composite Profiles, ELT, Streaming, Testing, and Utilities.
