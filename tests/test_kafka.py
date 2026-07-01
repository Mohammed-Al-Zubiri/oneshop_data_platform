"""
OneShop Integration Tests — Apache Kafka (KRaft)
Marker: core

Covers:
  - Broker connectivity via AdminClient metadata
  - Core platform topics exist after data generator seeding
"""

import pytest
from confluent_kafka.admin import AdminClient

# ─────────────────────────────────────────────────────────────────────────────
# Topics auto-created by the data generator / Kafka Connect
# ─────────────────────────────────────────────────────────────────────────────

# These are the minimum topics we expect once `make seed-purchases` and
# `make seed-logins` have been run. The test is lenient: it skips the topic
# check if neither topic exists yet (fresh platform with no seed data).
SEEDED_TOPICS = {"oneshop.logins", "oneshop.purchases"}


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.core
def test_kafka_broker_reachable(kafka_bootstrap):
    """
    AdminClient can connect to the broker and retrieve cluster metadata.
    Verifies the broker list is non-empty.
    """
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    metadata = admin.list_topics(timeout=10)

    assert metadata.brokers, "No brokers returned — Kafka cluster is unreachable"


@pytest.mark.core
def test_kafka_expected_topics_exist(kafka_bootstrap):
    """
    If the platform has been seeded (make seed-purchases / make seed-logins),
    the corresponding Kafka topics must exist. The test is skipped — not
    failed — when neither topic is present, to stay friendly to fresh stacks.
    """
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    metadata = admin.list_topics(timeout=10)
    actual_topics = set(metadata.topics.keys())
    if not SEEDED_TOPICS.intersection(actual_topics):
        pytest.skip(
            f"Seeded topics {SEEDED_TOPICS} not present yet. "
            "Run `make seed-purchases` and `make seed-logins` first."
        )

    missing = SEEDED_TOPICS - actual_topics
    assert not missing, f"Expected Kafka topics are missing: {missing}"
