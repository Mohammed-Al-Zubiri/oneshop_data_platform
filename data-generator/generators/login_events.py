"""
Login event generator — Stream login events to Kafka for Flink processing (Ch 8).
Includes some suspicious login patterns for anomaly detection.
"""

import os
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "oneshop.logins"

DEVICES = [
    "iPhone",
    "Android",
    "Desktop Chrome",
    "Desktop Firefox",
    "iPad",
    "MacOS Safari",
]
IPS_NORMAL = [f"192.168.1.{i}" for i in range(1, 255)]
IPS_SUSPICIOUS = ["185.220.101.1", "185.220.101.2", "23.129.64.100"]  # Tor exit nodes


def delivery_callback(err, msg):
    if err:
        print(f"  ❌ Delivery failed: {err}")


def run_login_events(count: int, interval: float):
    """Produce login events to Kafka, including some anomalous patterns."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    for i in range(count):
        # 10% chance of suspicious login
        is_suspicious = random.random() < 0.10
        user_id = random.randint(1, 100)

        if is_suspicious:
            ip = random.choice(IPS_SUSPICIOUS)
            # Suspicious: rapid logins from different devices
            for _ in range(random.randint(3, 8)):
                event = {
                    "user_id": user_id,
                    "login_ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "ip_address": ip,
                    "device": random.choice(DEVICES),
                    "is_success": random.choice([True, True, True, False]),
                }
                producer.produce(
                    TOPIC,
                    key=str(user_id),
                    value=json.dumps(event),
                    callback=delivery_callback,
                )
                producer.poll(0)
        else:
            event = {
                "user_id": user_id,
                "login_ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "ip_address": random.choice(IPS_NORMAL),
                "device": random.choice(DEVICES),
                "is_success": True,
            }
            producer.produce(
                TOPIC,
                key=str(user_id),
                value=json.dumps(event),
                callback=delivery_callback,
            )
            producer.poll(0)

        if (i + 1) % 100 == 0:
            print(f"  Produced {i + 1}/{count} login events")
        time.sleep(interval)

    producer.flush()
