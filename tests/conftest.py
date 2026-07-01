"""
OneShop Data Platform — Shared pytest fixtures.

Hosts are read from environment variables (injected by docker-compose.test.yml)
with sensible defaults matching the internal Docker service aliases.

Each profile-gated fixture performs a TCP probe before yielding. If the
target service is unreachable it calls pytest.skip() so the rest of the
suite continues unaffected.
"""

import os
import socket

import psycopg2
import pytest
from minio import Minio

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _is_reachable(host: str, port: int, timeout: float = 3.0) -> bool:
    """Return True if a TCP connection to host:port succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _parse_hostport(hostport: str) -> tuple[str, int]:
    """Split 'host:port' into (host, int(port))."""
    h, p = hostport.rsplit(":", 1)
    return h, int(p)


# ─────────────────────────────────────────────────────────────────────────────
# Always-on fixtures  (core marker — Postgres, Kafka, MinIO, Schema Registry)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def postgres_conn():
    """Session-scoped psycopg2 connection to the oneshop database."""
    host = os.getenv("TEST_POSTGRES_HOST", "postgres")
    port = int(os.getenv("TEST_POSTGRES_PORT", "5432"))

    if not _is_reachable(host, port):
        pytest.skip(
            f"PostgreSQL not reachable at {host}:{port}. Is `make up-core` running?"
        )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=os.getenv("POSTGRES_DB", "oneshop"),
        user=os.getenv("POSTGRES_USER", "postgresuser"),
        password=os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    )
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def minio_client():
    """Session-scoped MinIO client."""
    hostport = os.getenv("TEST_MINIO_HOST", "minio:9000")
    h, p = _parse_hostport(hostport)

    if not _is_reachable(h, p):
        pytest.skip(f"MinIO not reachable at {hostport}. Is `make up-core` running?")

    yield Minio(
        hostport,
        access_key=os.getenv("MINIO_ROOT_USER", "admin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
        secure=False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Profile-gated fixtures  (graceful skip when profile is not active)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def flask_host():
    """Base URL for the Flask Recommendation API (ai-ml profile)."""
    hostport = os.getenv("TEST_FLASK_HOST", "flask-recommendation:5050")
    h, p = _parse_hostport(hostport)

    if not _is_reachable(h, p):
        pytest.skip(
            f"Flask API not reachable at {hostport}. "
            "Run `make up-aiml` before executing aiml tests."
        )
    return f"http://{hostport}"


@pytest.fixture(scope="session")
def trino_host():
    """Base URL for the Trino query engine (query profile)."""
    hostport = os.getenv("TEST_TRINO_HOST", "trino:8080")
    h, p = _parse_hostport(hostport)

    if not _is_reachable(h, p):
        pytest.skip(
            f"Trino not reachable at {hostport}. "
            "Run `make up-query` before executing query tests."
        )
    return f"http://{hostport}"


@pytest.fixture(scope="session")
def airflow_host():
    """Base URL for the Airflow webserver (orchestration profile)."""
    hostport = os.getenv("TEST_AIRFLOW_HOST", "airflow-webserver:8080")
    h, p = _parse_hostport(hostport)

    if not _is_reachable(h, p, timeout=5.0):
        pytest.skip(
            f"Airflow not reachable at {hostport}. "
            "Run `make up-orchestration` before executing orchestration tests."
        )
    return f"http://{hostport}"


@pytest.fixture(scope="session")
def clickhouse_host():
    """Base URL for the ClickHouse HTTP interface (olap profile)."""
    hostport = os.getenv("TEST_CLICKHOUSE_HOST", "clickhouse:8123")
    h, p = _parse_hostport(hostport)

    if not _is_reachable(h, p):
        pytest.skip(
            f"ClickHouse not reachable at {hostport}. "
            "Run `make up-olap` before executing olap tests."
        )
    return f"http://{hostport}"


@pytest.fixture(scope="session")
def kafka_bootstrap():
    """Bootstrap server string for the Kafka broker (always-on core)."""
    hostport = os.getenv("TEST_KAFKA_HOST", "kafka:9092")
    h, p = _parse_hostport(hostport)

    if not _is_reachable(h, p):
        pytest.skip(f"Kafka not reachable at {hostport}. Is `make up-core` running?")

    return hostport


@pytest.fixture(scope="session")
def schema_registry_host():
    """Base URL for the Schema Registry (always-on core)."""
    hostport = os.getenv("TEST_SCHEMA_REGISTRY_HOST", "schema-registry:8081")
    h, p = _parse_hostport(hostport)

    if not _is_reachable(h, p):
        pytest.skip(
            f"Schema Registry not reachable at {hostport}. Is `make up-core` running?"
        )
    return f"http://{hostport}"
