"""
OneShop — Feature Engineering for ALS Model (Chapter 9)
Creates user-item interaction features from Silver layer data.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum, avg

try:
    spark = SparkSession.builder.appName("compute-features").getOrCreate()
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    sys.exit(1)

print("Computing user-item interaction features...")

# Load Silver purchases
purchases = spark.table("silver.purchases_enriched")

# Compute user-item interaction matrix
user_item_interactions = purchases.groupBy("user_id", "item_id").agg(
    count("*").alias("purchase_count"),
    spark_sum("quantity").alias("total_quantity"),
    spark_sum("total_price").alias("total_spent"),
    avg("purchase_price").alias("avg_price"),
)

# Create the features namespace if needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS features")

# Write features to Iceberg
user_item_interactions.write.format("iceberg").mode("overwrite").option(
    "format-version", "2"
).saveAsTable("features.user_item_interactions")

rows_count = (
    spark.read.format("iceberg").load("features.user_item_interactions").count()
)
print(f"  ✅ User-item interaction features computed: {rows_count} rows")

# Also compute user-level features
user_features = purchases.groupBy("user_id").agg(
    count("*").alias("total_orders"),
    spark_sum("total_price").alias("lifetime_value"),
    avg("total_price").alias("avg_order_value"),
)

user_features.write.format("iceberg").mode("overwrite").option(
    "format-version", "2"
).saveAsTable("features.user_features")

print("  ✅ User-level features computed.")
spark.stop()
