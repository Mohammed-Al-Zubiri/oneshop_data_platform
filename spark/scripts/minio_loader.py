"""
OneShop ETL — Load MinIO pageview events into Iceberg Bronze layer.
Source: MinIO pageviews bucket (JSON files)
Target: bronze.pageviews
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = "pageviews"

try:
    spark = (
        SparkSession.builder.appName("minio-to-iceberg-bronze")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    sys.exit(1)

print("SparkSession created successfully.")

try:
    print("Reading pageview events from MinIO...")
    pageviews_df = spark.read.json(f"s3a://{MINIO_BUCKET}/")

    pageviews_df = pageviews_df.select(
        col("user_id").cast("long"),
        col("url").cast("string"),
        col("channel").cast("string"),
        col("received_at").cast("timestamp"),
    )

    # Write to Iceberg Bronze layer (overwrite partitions dynamically to prevent duplicates)
    pageviews_df.writeTo("bronze.pageviews").overwritePartitions()

    count = spark.read.format("iceberg").load("bronze.pageviews").count()
    print(f"  ✅ pageviews → bronze.pageviews ({count} rows)")

except Exception as e:
    print(f"  ❌ Error processing pageview events: {e}")
    sys.exit(1)

print("\n✅ MinIO pageviews loaded into Bronze layer.")
spark.stop()
