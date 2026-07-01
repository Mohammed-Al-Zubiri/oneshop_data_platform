# Testing

The platform includes a containerized integration test suite built with `pytest`. Tests run inside a dedicated `test-runner` container attached to the platform's internal Docker network, ensuring authentic service connectivity.

---

## Design Principles

**Graceful skipping, not hard failures.** Every test fixture checks if its target service is reachable before running. If you run `make test` without `make up-aiml`, the AI/ML tests skip — they don't crash the whole suite.

**Layered execution.** Tests are grouped by `pytest` marker, matching the platform's module tiers. You only need to boot what you want to test.

**Self-contained CI.** `make test-ci` uses Docker Compose's `--abort-on-container-exit` and `--exit-code-from` flags so pipelines accurately report pass/fail status and auto-teardown.

---

## Running Tests

### By Module

=== "Core (always-on)"

    Tests Postgres, Kafka, MinIO, and Schema Registry.

    **Requires:** `make up-core`

    ```bash
    make test-core
    ```

=== "AI/ML"

    Tests the Flask Recommendation API endpoints.

    **Requires:** `make up-aiml`

    ```bash
    make test-aiml
    ```

=== "Query"

    Tests Trino federated SQL connectivity and catalog access.

    **Requires:** `make up-query`

    ```bash
    make test-query
    ```

=== "Orchestration"

    Tests Airflow webserver and scheduler health.

    **Requires:** `make up-orchestration`

    ```bash
    make test-orchestration
    ```

=== "OLAP"

    Tests ClickHouse HTTP interface availability.

    **Requires:** `make up-olap`

    ```bash
    make test-olap
    ```

=== "Full Suite"

    Runs all markers. Services that aren't running are skipped gracefully.

    **Requires:** `make up-core` at minimum

    ```bash
    make test
    ```

=== "CI Mode"

    Spins up required services, runs tests, and tears down automatically. Exits non-zero on any failure.

    ```bash
    make test-ci
    ```

---

## What's Tested

### `test_postgres.py` — marker: `core`

| Test | What it verifies |
|:-----|:----------------|
| `test_postgres_connection` | Can connect and execute `SELECT 1` |
| `test_postgres_schema_tables_exist` | All 5 core tables exist in the `public` schema |
| `test_postgres_write_read_delete` | Full write → read → delete round-trip with `RETURNING id` |

### `test_kafka.py` — marker: `core`

| Test | What it verifies |
|:-----|:----------------|
| `test_kafka_broker_reachable` | AdminClient can connect and retrieve cluster metadata |
| `test_kafka_expected_topics_exist` | After seeding, `oneshop.logins` and `oneshop.purchases` topics exist |

### `test_minio.py` — marker: `core`

| Test | What it verifies |
|:-----|:----------------|
| `test_minio_connection` | MinIO client connects successfully |
| `test_minio_warehouse_bucket_exists` | `warehouse` bucket created by `mc` init container |
| `test_minio_put_get_delete` | Upload → download → delete round-trip |

### `test_schema_registry.py` — marker: `core`

| Test | What it verifies |
|:-----|:----------------|
| `test_schema_registry_reachable` | HTTP 200 from Schema Registry root |
| `test_schema_registry_subjects` | Can list registered subjects |

### `test_flask.py` — marker: `aiml`

| Test | What it verifies |
|:-----|:----------------|
| `test_flask_health` | `/health` returns 200 with healthy status |
| `test_flask_recommend_valid_user` | `/recommend/<id>` returns a list of recommendations |
| `test_flask_recommend_invalid_user` | Non-existent user returns 404 or empty list |

### `test_trino.py` — marker: `query`

| Test | What it verifies |
|:-----|:----------------|
| `test_trino_connection` | Can connect to Trino and run `SELECT 1` |
| `test_trino_iceberg_catalog` | Iceberg catalog is registered and accessible |
| `test_trino_show_schemas` | Can list Iceberg namespaces |

### `test_airflow.py` — marker: `orchestration`

| Test | What it verifies |
|:-----|:----------------|
| `test_airflow_webserver_health` | Webserver and scheduler statuses are healthy via `/health` endpoint |

### `test_clickhouse.py` — marker: `olap`

| Test | What it verifies |
|:-----|:----------------|
| `test_clickhouse_http_interface` | Can connect via HTTP interface and execute `SELECT 1` |

---

## Test Infrastructure

### Configuration (`pytest.ini`)

```ini
[pytest]
addopts = -v --tb=short
markers =
    core:  Always-on core services (Postgres, Kafka, MinIO, Schema Registry)
    aiml:  AI/ML profile services (Flask Recommendation API)
    query: Query profile services (Trino federated SQL)
    orchestration: Orchestration profile services (Airflow)
    olap:  OLAP profile services (ClickHouse)
```

### Shared Fixtures (`conftest.py`)

All fixtures are `scope="session"` — connections are established once per test run, not per test.

Each fixture calls `_is_reachable(host, port)` (TCP probe) before yielding. If the service is down, it calls `pytest.skip()` with a descriptive message pointing to the right `make up-*` command.

```python
@pytest.fixture(scope="session")
def postgres_conn():
    if not _is_reachable("postgres", 5432):
        pytest.skip("PostgreSQL not reachable. Run: make up-core")
    # ... yield connection
```

### Test Runner Container

Tests run inside a `test-runner` Docker container that is:

- Attached to `oneshop-network` (same network as all platform services)
- Uses internal Docker hostnames (`postgres`, `kafka`, `minio`, etc.) — no `localhost` port forwarding needed
- Defined in the `test` Docker Compose profile

---

## CI Pipeline

The GitHub Actions workflow at `.github/workflows/ci.yml` runs three jobs on every push and PR to `main`:

### Job 1: `lint` — Code Quality
- Runs `ruff check .` (Python linter)
- Runs `black --check .` (formatter check)

### Job 2: `validate-compose` — Compose Validation
- Runs `docker compose config --quiet` for every profile (core, batch, streaming, observability, etc.)
- Catches YAML syntax errors and image reference mistakes before they reach any runner

### Job 3: `integration-test` — Automated Testing
- Copies `.env.example` → `.env`
- Executes `make test-ci`
- Automatically spins up the required profiles (core, query, orchestration, olap, aiml)
- Runs the Pytest suite through the `test-runner` container
- Always tears down ephemeral resources on completion

---

## Adding a New Test

1. Create `tests/test_<service>.py`
2. Add a fixture in `conftest.py` with the TCP reachability probe
3. Mark tests with an existing or new `pytest` marker
4. Add the marker to `pytest.ini`
5. Add a `make test-<name>` target in the Makefile that calls `check_running` before the test runner

```python
# tests/test_clickhouse.py
import pytest
import requests

@pytest.mark.clickhouse
def test_clickhouse_ping(clickhouse_host):
    """ClickHouse HTTP interface responds to ping."""
    r = requests.get(f"{clickhouse_host}/ping")
    assert r.status_code == 200
    assert r.text.strip() == "Ok."
```
