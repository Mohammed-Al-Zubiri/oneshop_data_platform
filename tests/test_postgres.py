"""
OneShop Integration Tests — PostgreSQL
Marker: core

Covers:
  - Connection liveness
  - Schema completeness (all five core tables present)
  - Write / Read / Delete round-trip (uses RETURNING id to handle SERIAL PK)
"""

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Expected schema
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_TABLES = {"users", "items", "purchases", "user_recommendations", "reviews"}


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.core
def test_postgres_connection(postgres_conn):
    """Database connection is healthy and a simple query returns a result."""
    cur = postgres_conn.cursor()
    cur.execute("SELECT 1;")
    result = cur.fetchone()
    cur.close()
    assert result == (1,), "Expected (1,) from SELECT 1"


@pytest.mark.core
def test_postgres_schema_tables_exist(postgres_conn):
    """All core platform tables are present in the public schema."""
    cur = postgres_conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type   = 'BASE TABLE';
        """)
    actual_tables = {row[0] for row in cur.fetchall()}
    cur.close()

    missing = EXPECTED_TABLES - actual_tables
    assert not missing, f"Missing tables in public schema: {missing}"


@pytest.mark.core
def test_postgres_write_read_delete(postgres_conn):
    """
    Insert a sentinel user row, verify it can be read back, then delete it.

    Uses RETURNING id because the 'id' column is SERIAL — the database assigns
    the value; we must capture it to guarantee a targeted delete on teardown.
    """
    cur = postgres_conn.cursor()

    # ── Insert ────────────────────────────────────────────────────────────────
    cur.execute(
        """
        INSERT INTO users (first_name, last_name, email)
        VALUES (%s, %s, %s)
        RETURNING id;
        """,
        ("Integration", "TestUser", "integration-test@oneshop.internal"),
    )
    test_user_id = cur.fetchone()[0]
    postgres_conn.commit()

    try:
        # ── Read ──────────────────────────────────────────────────────────────
        cur.execute(
            "SELECT first_name, last_name, email FROM users WHERE id = %s;",
            (test_user_id,),
        )
        row = cur.fetchone()
        assert row is not None, f"Inserted user (id={test_user_id}) not found"
        assert row[0] == "Integration"
        assert row[1] == "TestUser"
        assert row[2] == "integration-test@oneshop.internal"

    finally:
        # ── Teardown (always runs) ─────────────────────────────────────────────
        cur.execute("DELETE FROM users WHERE id = %s;", (test_user_id,))
        postgres_conn.commit()
        cur.close()
