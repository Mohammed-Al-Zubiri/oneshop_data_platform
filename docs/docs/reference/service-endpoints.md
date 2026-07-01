# Service Endpoints

All services run on `localhost` when started. Default credentials match the values in `.env.example`.

---

## Core Infrastructure

| Service | URL | Credentials | Started by |
|:--------|:----|:------------|:-----------|
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | admin / password | `make up-core` |
| **MinIO API** | [http://localhost:9000](http://localhost:9000) | admin / password | `make up-core` |
| **Iceberg REST Catalog** | [http://localhost:8181](http://localhost:8181) | — | `make up-core` |
| **Schema Registry** | [http://localhost:8081](http://localhost:8081) | — | `make up-core` |
| **MailHog UI** | [http://localhost:8025](http://localhost:8025) | — | `make up-core` |
| **MailHog SMTP** | `localhost:1025` | — | `make up-core` |

---

## Batch Processing

| Service | URL | Credentials | Started by |
|:--------|:----|:------------|:-----------|
| **Airflow UI** | [http://localhost:8082](http://localhost:8082) | admin / admin | `make up-batch` / `make up-orchestration` |
| **Spark Notebooks** | [http://localhost:8888](http://localhost:8888) | — | `make up-batch` / `make up-compute` |
| **Trino** | [http://localhost:9090](http://localhost:9090) | user: `test` (no password) | `make up-batch` / `make up-query` |
| **Superset** | [http://localhost:8088](http://localhost:8088) | admin / admin | `make up-batch` / `make up-bi` |

---

## Real-Time Processing

| Service | URL | Credentials | Started by |
|:--------|:----|:------------|:-----------|
| **Kafka Connect API** | [http://localhost:8083](http://localhost:8083) | — | `make up-realtime` / `make up-cdc` |
| **Flink Dashboard** | [http://localhost:8084](http://localhost:8084) | — | `make up-realtime` / `make up-stream-processing` |
| **ClickHouse HTTP** | [http://localhost:8123](http://localhost:8123) | default / mysecret | `make up-realtime` / `make up-olap` |
| **ClickHouse Native** | `localhost:9000` (native TCP) | default / mysecret | `make up-realtime` / `make up-olap` |
| **OpenSearch** | [http://localhost:9200](http://localhost:9200) | — | `make up-realtime` / `make up-search` |
| **Streamlit Flash Sale** | [http://localhost:8501](http://localhost:8501) | — | `make up-realtime` / `make up-olap` |
| **Redpanda Console** | [http://localhost:8085](http://localhost:8085) | — | `make up-devtools` |

---

## ML & AI

| Service | URL | Started by |
|:--------|:----|:-----------|
| **Flask Recommendation API** | [http://localhost:5050](http://localhost:5050) | `make up-aiml` / `make up-recommendation` |
| **Streamlit Semantic Search** | [http://localhost:8502](http://localhost:8502) | `make up-aiml` / `make up-semantic-search` |

---

## Observability

| Service | URL | Credentials | Started by |
|:--------|:----|:------------|:-----------|
| **Grafana** | [http://localhost:3000](http://localhost:3000) | admin / admin | `make up-observability` |
| **Prometheus** | [http://localhost:9091](http://localhost:9091) | — | `make up-observability` |
| **Alertmanager** | [http://localhost:9093](http://localhost:9093) | — | `make up-observability` |
| **StatsD Exporter metrics** | [http://localhost:9102/metrics](http://localhost:9102/metrics) | — | `make up-observability` |
| **Kafka Exporter metrics** | [http://localhost:9308/metrics](http://localhost:9308/metrics) | — | `make up-observability` |
| **Kafka JMX metrics** | [http://localhost:7071/metrics](http://localhost:7071/metrics) | — | `make up-core` (auto) |

---

## Internal Docker Network

All containers communicate on the `oneshop-network` Docker bridge network using their **container hostnames** (not `localhost`). These are used internally by test fixtures and service configs:

| Container Name | Internal Hostname | Key Internal Port |
|:---------------|:------------------|:------------------|
| `postgres` | `postgres` | 5432 |
| `kafka` | `kafka` | 9092 |
| `schema-registry` | `schema-registry` | 8081 |
| `minio` | `minio` | 9000 |
| `iceberg-catalog` | `iceberg-catalog` | 8181 |
| `mailhog` | `mailhog` | 1025 (SMTP) |
| `connect` | `connect` | 8083 |
| `flink-jobmanager` | `flink-jobmanager` | 8081, 9249 |
| `clickhouse` | `clickhouse` | 8123, 9000, 9363 |
| `statsd-exporter` | `statsd-exporter` | 8125 (UDP), 9102 |
| `airflow-webserver` | `airflow-webserver` | 8080 |
| `airflow-scheduler` | `airflow-scheduler` | — |
| `trino` | `trino` | 8080 |
| `flask-recommendation` | `flask-recommendation` | 5050 |

---

## Quick Health Checks

```bash
# Postgres
docker exec postgres pg_isready -U postgresuser

# MinIO
curl -s http://localhost:9000/minio/health/ready

# Iceberg Catalog
curl -s http://localhost:8181/v1/config | jq .

# Kafka (via AdminClient, requires kafka-python or confluent-kafka)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Prometheus targets
curl -s http://localhost:9091/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# ClickHouse
curl -s "http://localhost:8123/ping"

# Flask API
curl -s http://localhost:5050/health | jq .
```
