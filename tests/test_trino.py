"""
OneShop Integration Tests — Trino Federated SQL Engine
Marker: query

Requires: make up-query  (trino service, profile query)

Covers:
  - Trino cluster health via GET /v1/info (synchronous, no polling needed)
  - SHOW CATALOGS via the async /v1/statement REST API with a poll loop
    (the first response is always QUEUED — must follow nextUri until FINISHED)
"""

import time

import pytest
import requests

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

TRINO_USER = "test"  # matches TRINO_USER in .env
POLL_INTERVAL = 0.5  # seconds between status polls
MAX_POLLS = 20  # give up after 10 s


def _poll_statement(trino_base: str, sql: str) -> list[list]:
    """
    Submit a SQL statement to Trino's /v1/statement endpoint and follow
    nextUri until the query reaches a terminal state (FINISHED / FAILED).

    Returns the collected rows as a list of lists.
    Raises AssertionError if the query fails on the Trino side.
    """
    headers = {"X-Trino-User": TRINO_USER}
    resp = requests.post(
        f"{trino_base}/v1/statement",
        data=sql,
        headers=headers,
        timeout=10,
    )
    assert resp.status_code == 200, f"Trino statement POST failed: {resp.status_code}"

    body = resp.json()
    rows: list[list] = []
    polls = 0

    while True:
        state = body.get("stats", {}).get("state", "UNKNOWN")

        if "data" in body:
            rows.extend(body["data"])

        if state in ("FINISHED", "FAILED", "CANCELED"):
            if state != "FINISHED":
                error = body.get("error", {}).get("message", "unknown error")
                raise AssertionError(f"Trino query ended with state={state}: {error}")
            break

        next_uri = body.get("nextUri")
        if not next_uri:
            break  # no more pages

        polls += 1
        if polls > MAX_POLLS:
            raise AssertionError("Trino query did not finish within the polling limit")

        time.sleep(POLL_INTERVAL)
        resp = requests.get(next_uri, headers=headers, timeout=10)
        assert resp.status_code == 200, f"Trino nextUri poll failed: {resp.status_code}"
        body = resp.json()

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.query
def test_trino_cluster_is_healthy(trino_host):
    """
    GET /v1/info returns HTTP 200 and reports the cluster is no longer
    in the 'starting' state — i.e., it is fully operational.
    """
    for _ in range(30):
        resp = requests.get(f"{trino_host}/v1/info", timeout=10)
        assert (
            resp.status_code == 200
        ), f"Expected 200 from /v1/info, got {resp.status_code}"

        info = resp.json()
        if info.get("starting") is False:
            break
        time.sleep(1)

    assert (
        info.get("starting") is False
    ), "Trino cluster is still starting after 30 seconds."


@pytest.mark.query
def test_trino_show_catalogs(trino_host):
    """
    SHOW CATALOGS via the async /v1/statement API returns at least one
    catalog, confirming Trino can process queries end-to-end.
    """
    rows = _poll_statement(trino_host, "SHOW CATALOGS")
    catalog_names = [r[0] for r in rows]

    assert (
        len(catalog_names) >= 1
    ), "SHOW CATALOGS returned no rows — no catalogs configured"
    # 'system' is always present in every Trino deployment
    assert (
        "system" in catalog_names
    ), f"'system' catalog missing from Trino catalog list: {catalog_names}"
