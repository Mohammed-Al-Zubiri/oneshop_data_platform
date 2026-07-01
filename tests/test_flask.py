"""
OneShop Integration Tests — Flask Recommendation API
Marker: aiml

Requires: make up-aiml  (flask-recommendation service, profile ai-ml)

Covers:
  - GET /health → {"status": "healthy"}
  - GET /recommend/<user_id> → end-to-end recommendation retrieval

Setup / teardown for the E2E test inserts and removes a sentinel row from
user_recommendations directly via psycopg2 so the Flask API has data to serve.
"""

import os

import psycopg2
import pytest
import requests

# ─────────────────────────────────────────────────────────────────────────────
# Sentinel recommendation used in the E2E test
# ─────────────────────────────────────────────────────────────────────────────

SENTINEL_USER_ID = 999999
SENTINEL_ITEM_ID = 123
SENTINEL_SCORE = 0.99


def _pg_conn():
    """Open a fresh psycopg2 connection for fixture setup/teardown."""
    return psycopg2.connect(
        host=os.getenv("TEST_POSTGRES_HOST", "postgres"),
        port=int(os.getenv("TEST_POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "oneshop"),
        user=os.getenv("POSTGRES_USER", "postgresuser"),
        password=os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    )


@pytest.fixture()
def sentinel_recommendation():
    """
    Insert a known recommendation row before the test, yield control,
    then delete it unconditionally on teardown.
    """
    conn = _pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user_recommendations (user_id, item_id, score, model_version, generated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (user_id, item_id) DO UPDATE
            SET score         = EXCLUDED.score,
                model_version = EXCLUDED.model_version,
                generated_at  = EXCLUDED.generated_at;
        """,
        (SENTINEL_USER_ID, SENTINEL_ITEM_ID, SENTINEL_SCORE, "integration-test"),
    )
    conn.commit()

    yield  # ── test body runs here ──────────────────────────────────────────

    cur.execute(
        "DELETE FROM user_recommendations WHERE user_id = %s AND item_id = %s;",
        (SENTINEL_USER_ID, SENTINEL_ITEM_ID),
    )
    conn.commit()
    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.aiml
def test_flask_health(flask_host):
    """Health endpoint returns HTTP 200 and the expected JSON payload."""
    resp = requests.get(f"{flask_host}/health", timeout=10)
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert resp.json() == {"status": "healthy"}, f"Unexpected payload: {resp.json()}"


@pytest.mark.aiml
def test_flask_recommend_returns_correct_data(flask_host, sentinel_recommendation):
    """
    End-to-end test: a known recommendation row in Postgres is served
    correctly by the Flask /recommend/<user_id> endpoint.
    """
    resp = requests.get(f"{flask_host}/recommend/{SENTINEL_USER_ID}", timeout=10)
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"

    body = resp.json()
    assert (
        body["user_id"] == SENTINEL_USER_ID
    ), f"user_id mismatch: expected {SENTINEL_USER_ID}, got {body.get('user_id')}"

    recommendations = body.get("recommendations", [])
    assert len(recommendations) >= 1, "Expected at least one recommendation in response"

    # Find our sentinel item in the returned list
    sentinel = next(
        (r for r in recommendations if r["item_id"] == SENTINEL_ITEM_ID), None
    )
    assert (
        sentinel is not None
    ), f"Sentinel item_id={SENTINEL_ITEM_ID} not found in recommendations: {recommendations}"
    assert (
        abs(sentinel["score"] - SENTINEL_SCORE) < 1e-6
    ), f"Score mismatch: expected {SENTINEL_SCORE}, got {sentinel['score']}"
