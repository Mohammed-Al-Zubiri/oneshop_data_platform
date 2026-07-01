"""
OneShop Integration Tests — ClickHouse
Marker: olap

Covers:
  - Connection liveness and HTTP interface availability
"""

import pytest
import requests


@pytest.mark.olap
def test_clickhouse_http_interface(clickhouse_host):
    """Verify that the ClickHouse HTTP interface is reachable and returns results."""
    import os

    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "mysecret")
    response = requests.get(
        f"{clickhouse_host}/?query=SELECT%201", auth=(user, password)
    )

    assert response.status_code == 200, f"Expected 200 OK, got {response.status_code}"
    assert response.text.strip() == "1", f"Expected '1', got '{response.text.strip()}'"
