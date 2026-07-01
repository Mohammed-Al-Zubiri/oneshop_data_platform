"""
OneShop ETL — Load Postgres tables into Iceberg Bronze layer.
Source: PostgreSQL (users, items, purchases)
Target: bronze.users, bronze.items, bronze.purchases
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuration from environment or defaults
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/oneshop")
USERNAME = os.getenv("ETL_USER", "etluser")
PASSWORD = os.getenv("ETL_PASSWORD", "etlpassword")

try:
    spark = SparkSession.builder.appName("postgres-to-iceberg-bronze").getOrCreate()
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    sys.exit(1)

print("SparkSession created successfully.")


def load_table(table_name: str, target: str, columns: list):
    """Load a Postgres table into an Iceberg Bronze table."""
    print(f"Processing '{table_name}' table...")
    try:
        df = (
            spark.read.format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", POSTGRES_URL)
            .option("dbtable", table_name)
            .option("user", USERNAME)
            .option("password", PASSWORD)
            .load()
        )

        # Cast columns to match Iceberg schema
        select_exprs = [col(c["name"]).cast(c["type"]) for c in columns]
        df = df.select(*select_exprs)

        # Write to Iceberg (overwrite partitions dynamically to prevent duplicates)
        df.writeTo(target).overwritePartitions()

        count = spark.read.format("iceberg").load(target).count()
        print(f"  ✅ '{table_name}' → {target} ({count} rows)")
    except Exception as e:
        print(f"  ❌ Error loading '{table_name}': {e}")
        raise


# --- Load Users ---
load_table(
    "users",
    "bronze.users",
    [
        {"name": "id", "type": "long"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "created_at", "type": "timestamp"},
        {"name": "updated_at", "type": "timestamp"},
    ],
)

# --- Load Items ---
load_table(
    "items",
    "bronze.items",
    [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "category", "type": "string"},
        {"name": "price", "type": "decimal(7,2)"},
        {"name": "inventory", "type": "int"},
        {"name": "created_at", "type": "timestamp"},
        {"name": "updated_at", "type": "timestamp"},
    ],
)

# --- Load Purchases ---
load_table(
    "purchases",
    "bronze.purchases",
    [
        {"name": "id", "type": "long"},
        {"name": "user_id", "type": "long"},
        {"name": "item_id", "type": "long"},
        {"name": "quantity", "type": "int"},
        {"name": "purchase_price", "type": "decimal(12,2)"},
        {"name": "created_at", "type": "timestamp"},
        {"name": "updated_at", "type": "timestamp"},
    ],
)

print("\n✅ All Postgres tables loaded into Bronze layer.")
spark.stop()
