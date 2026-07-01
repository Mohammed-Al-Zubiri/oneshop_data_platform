"""
CDC Inventory updater — Simulate inventory changes for Debezium CDC (Ch 6).
Directly updates Postgres items.inventory to trigger WAL events.
"""

import os
import time
import random
import psycopg2

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "oneshop"),
    "user": os.getenv("POSTGRES_USER", "postgresuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}


def run_cdc_inventory(count: int, interval: float):
    """Update random item inventory levels to trigger Debezium CDC events."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get all item IDs
    cur.execute("SELECT id FROM items")
    item_ids = [row[0] for row in cur.fetchall()]
    if not item_ids:
        print("  ❌ No items found in database. Run 'batch' mode first.")
        return

    for i in range(count):
        item_id = random.choice(item_ids)
        new_inventory = random.randint(0, 500)
        cur.execute(
            """UPDATE items SET inventory = %s, inventory_updated_at = NOW(), updated_at = NOW()
               WHERE id = %s""",
            (new_inventory, item_id),
        )
        conn.commit()
        if (i + 1) % 100 == 0:
            print(f"  Updated {i + 1}/{count} items")
        time.sleep(interval)

    cur.close()
    conn.close()
