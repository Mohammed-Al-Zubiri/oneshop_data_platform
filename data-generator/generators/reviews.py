"""
Customer reviews generator — Seed reviews into Postgres for pgvector (Ch 10).
"""

import os
import random
import psycopg2
from faker import Faker

fake = Faker()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "oneshop"),
    "user": os.getenv("POSTGRES_USER", "postgresuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

REVIEW_TEMPLATES = [
    "Great product! The {adj} quality exceeded my expectations. {extra}",
    "Terrible experience. The product was {adj} and the customer service was unhelpful. {extra}",
    "Decent value for the price. The {adj} design is nice but could be improved. {extra}",
    "Amazing! I love the {adj} features. Would definitely buy again. {extra}",
    "Not worth the money. Disappointed with the {adj} build quality. {extra}",
    "Perfect gift! The {adj} packaging was beautiful and delivery was fast. {extra}",
    "Average product, nothing special. The {adj} performance is okay. {extra}",
    "Exceeded all expectations. The {adj} craftsmanship is outstanding. {extra}",
    "Broke after one week. Very {adj} quality control. Do not recommend. {extra}",
    "Solid purchase. The {adj} materials feel premium. {extra}",
]

ADJECTIVES = [
    "premium",
    "excellent",
    "outstanding",
    "poor",
    "mediocre",
    "fantastic",
    "durable",
    "flimsy",
    "elegant",
    "sturdy",
    "lightweight",
    "compact",
]

EXTRAS = [
    "Shipping was fast.",
    "Would recommend to friends.",
    "The packaging was eco-friendly.",
    "Customer support was responsive.",
    "I'll be a repeat customer.",
    "Expected better for the price.",
    "Great for everyday use.",
    "The color matched the photos exactly.",
    "",
]


def run_reviews_seed(count: int):
    """Generate and insert customer reviews into Postgres."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _ in range(count):
        review_text = random.choice(REVIEW_TEMPLATES).format(
            adj=random.choice(ADJECTIVES), extra=random.choice(EXTRAS)
        )
        cur.execute(
            "INSERT INTO reviews (customer_name, customer_email, date, review) VALUES (%s, %s, %s, %s)",
            (
                fake.name(),
                fake.email(),
                fake.date_between(start_date="-90d", end_date="today"),
                review_text,
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"  ✅ {count} customer reviews seeded into Postgres.")
