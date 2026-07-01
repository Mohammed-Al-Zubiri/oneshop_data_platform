"""
Flash sale purchase generator — Stream purchase events via Kafka (Ch 7).
Simulates high-frequency purchases during a flash sale campaign.
"""

import os
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "oneshop.public.purchases"


def delivery_callback(err, msg):
    if err:
        print(f"  ❌ Delivery failed: {err}")


def run_cdc_purchases(count: int, interval: float, campaign_id: str):
    """Produce flash sale purchase events to Kafka."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    for i in range(count):
        event = {
            "user_id": random.randint(1, 100),
            "item_id": random.randint(1, 1000),
            "quantity": random.randint(1, 5),
            "purchase_price": round(random.uniform(9.99, 299.99), 2),
            "campaign_id": campaign_id,
            "status": 1,
            "deleted": False,
            "created_at": datetime.utcnow().isoformat(),
        }
        producer.produce(
            TOPIC,
            key=str(event["user_id"]),
            value=json.dumps(event),
            callback=delivery_callback,
        )
        producer.poll(0)
        if (i + 1) % 100 == 0:
            print(f"  Produced {i + 1}/{count} purchase events")
        time.sleep(interval)

    producer.flush()
