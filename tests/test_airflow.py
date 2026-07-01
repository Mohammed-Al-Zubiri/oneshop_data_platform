"""
OneShop Integration Tests — Airflow
Marker: orchestration

Covers:
  - Webserver liveness via health endpoint
"""

import pytest
import requests


@pytest.mark.orchestration
def test_airflow_webserver_health(airflow_host):
    """Verify that the Airflow webserver and scheduler are healthy."""
    response = requests.get(f"{airflow_host}/health")

    assert response.status_code == 200, f"Expected 200 OK, got {response.status_code}"

    data = response.json()
    assert (
        data.get("metadatabase", {}).get("status") == "healthy"
    ), "Metadatabase is not healthy"
    assert (
        data.get("scheduler", {}).get("status") == "healthy"
    ), "Scheduler is not healthy"
