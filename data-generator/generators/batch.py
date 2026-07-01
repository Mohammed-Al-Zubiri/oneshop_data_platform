"""
Batch data generator — Seed Postgres OLTP tables + MinIO pageview events.
Corresponds to Chapter 3 (loadgen).
"""

import os
import io
import json
import random
from datetime import datetime, timedelta
import psycopg2
from faker import Faker
from minio import Minio

fake = Faker()

# Postgres connection
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "oneshop"),
    "user": os.getenv("POSTGRES_USER", "postgresuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# MinIO connection
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Toys",
    "Beauty",
    "Automotive",
    "Food",
    "Health",
]

CHANNELS = ["web", "mobile", "tablet", "api"]

ITEM_PAGES = ["items", "products", "catalog"]


def run_batch_seed(
    num_users: int, num_items: int, num_purchases: int, num_pageviews: int
):
    """Generate and insert batch seed data."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # --- Generate Users ---
    print(f"  Generating {num_users} users...")
    for _ in range(num_users):
        cur.execute(
            "INSERT INTO users (first_name, last_name, email) VALUES (%s, %s, %s)",
            (fake.first_name(), fake.last_name(), fake.email()),
        )
    conn.commit()

    # Get user IDs
    cur.execute("SELECT id FROM users")
    user_ids = [row[0] for row in cur.fetchall()]

    # --- Generate Items ---
    print(f"  Generating {num_items} items...")
    for _ in range(num_items):
        cur.execute(
            "INSERT INTO items (name, category, price, inventory) VALUES (%s, %s, %s, %s)",
            (
                fake.catch_phrase(),
                random.choice(CATEGORIES),
                round(random.uniform(5.99, 499.99), 2),
                random.randint(0, 500),
            ),
        )
    conn.commit()

    # Get item IDs and prices
    cur.execute("SELECT id, price FROM items")
    items = [(row[0], float(row[1])) for row in cur.fetchall()]
    item_ids = [i[0] for i in items]
    item_prices = {i[0]: i[1] for i in items}

    # --- Generate Purchases ---
    print(f"  Generating {num_purchases} purchases...")
    for _ in range(num_purchases):
        user_id = random.choice(user_ids)
        item_id = random.choice(item_ids)
        quantity = random.randint(1, 5)
        purchase_price = item_prices[item_id]
        created_at = fake.date_time_between(start_date="-30d", end_date="now")
        cur.execute(
            """INSERT INTO purchases (user_id, item_id, quantity, purchase_price, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (user_id, item_id, quantity, purchase_price, created_at, created_at),
        )
    conn.commit()

    cur.close()
    conn.close()
    print(
        f"  ✅ Postgres seeded: {num_users} users, {num_items} items, {num_purchases} purchases"
    )

    # --- Generate Pageviews to MinIO ---
    print(f"  Generating {num_pageviews} pageview events to MinIO...")
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # Batch pageviews into files
    batch_size = 1000
    for batch_start in range(0, num_pageviews, batch_size):
        events = []
        for _ in range(min(batch_size, num_pageviews - batch_start)):
            events.append(
                {
                    "user_id": random.choice(user_ids),
                    "url": f"/{random.choice(ITEM_PAGES)}/{random.choice(item_ids)}",
                    "channel": random.choice(CHANNELS),
                    "received_at": (
                        datetime.utcnow() - timedelta(days=random.randint(0, 30))
                    ).isoformat(),
                }
            )

        data = "\n".join(json.dumps(e) for e in events)
        data_bytes = data.encode("utf-8")
        filename = f"batch_{batch_start}_{batch_start + len(events)}.jsonl"

        client.put_object(
            "pageviews",
            filename,
            io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/jsonl",
        )

    print(f"  ✅ MinIO seeded: {num_pageviews} pageview events")
