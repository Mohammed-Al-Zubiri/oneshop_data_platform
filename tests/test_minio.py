"""
OneShop Integration Tests — MinIO (S3-compatible Object Storage)
Marker: core

Covers:
  - All three platform buckets exist (warehouse, pageviews, customer-segments)
  - Object write / read / delete round-trip using the existing 'warehouse' bucket
"""

import io

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Expected buckets (matches mc init in docker-compose.yml)
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_BUCKETS = {"warehouse", "pageviews", "customer-segments"}

# Use an existing bucket with a namespaced key — no extra bucket creation needed
TEST_BUCKET = "warehouse"
TEST_OBJECT_KEY = "integration-test/probe.txt"
TEST_OBJECT_CONTENT = b"oneshop-integration-probe"


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.core
def test_minio_buckets_exist(minio_client):
    """All buckets initialised by the mc service are present."""
    actual_buckets = {b.name for b in minio_client.list_buckets()}
    missing = EXPECTED_BUCKETS - actual_buckets
    assert not missing, f"MinIO buckets not found: {missing}"


@pytest.mark.core
def test_minio_object_write_read_delete(minio_client):
    """
    Write a sentinel object into the 'warehouse' bucket, read it back to
    verify content integrity, then delete it — leaving storage clean.
    """
    data = io.BytesIO(TEST_OBJECT_CONTENT)

    # ── Write ─────────────────────────────────────────────────────────────────
    minio_client.put_object(
        TEST_BUCKET,
        TEST_OBJECT_KEY,
        data,
        length=len(TEST_OBJECT_CONTENT),
        content_type="text/plain",
    )

    try:
        # ── Read ──────────────────────────────────────────────────────────────
        response = minio_client.get_object(TEST_BUCKET, TEST_OBJECT_KEY)
        content = response.read()
        response.close()
        response.release_conn()

        assert (
            content == TEST_OBJECT_CONTENT
        ), f"Object content mismatch: expected {TEST_OBJECT_CONTENT!r}, got {content!r}"

    finally:
        # ── Teardown (always runs) ─────────────────────────────────────────────
        minio_client.remove_object(TEST_BUCKET, TEST_OBJECT_KEY)
