"""
OneShop Integration Tests — Confluent Schema Registry
Marker: core

Covers:
  - Registry connectivity via GET /subjects (always-on core service)
  - Compatible response structure (valid JSON list)
"""

import pytest
import requests

# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.core
def test_schema_registry_is_reachable(schema_registry_host):
    """
    GET /subjects returns HTTP 200 and a JSON list.
    On a fresh platform (no schemas registered yet) the list will be empty —
    that is valid and expected.
    """
    resp = requests.get(f"{schema_registry_host}/subjects", timeout=10)
    assert (
        resp.status_code == 200
    ), f"Schema Registry /subjects returned {resp.status_code}, expected 200"

    subjects = resp.json()
    assert isinstance(
        subjects, list
    ), f"Expected a list from /subjects, got: {type(subjects).__name__}"


@pytest.mark.core
def test_schema_registry_config_endpoint(schema_registry_host):
    """
    GET /config returns the global compatibility level, confirming the
    registry is fully initialised and serving configuration requests.
    """
    resp = requests.get(f"{schema_registry_host}/config", timeout=10)
    assert (
        resp.status_code == 200
    ), f"Schema Registry /config returned {resp.status_code}, expected 200"

    config = resp.json()
    assert (
        "compatibilityLevel" in config
    ), f"'compatibilityLevel' key missing from /config response: {config}"
