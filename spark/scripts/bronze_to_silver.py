"""
OneShop ETL — Transform Bronze to Silver layer.
Applies cleaning, validation, denormalization, and enrichment.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    upper,
    regexp_extract,
    lit,
    when,
    hour,
    to_date,
)

try:
    spark = SparkSession.builder.appName("bronze-to-silver-transformer").getOrCreate()
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    sys.exit(1)

print("SparkSession created. Loading Bronze tables...")

# Load Bronze tables
bronze_users = spark.table("bronze.users")
bronze_items = spark.table("bronze.items")
bronze_purchases = spark.table("bronze.purchases")
bronze_pageviews = spark.table("bronze.pageviews")

# ============================================================
# 1. Silver Users — email validation + full name
# ============================================================
print("Transforming users...")
email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

silver_users = bronze_users.withColumn(
    "valid_email", col("email").rlike(email_regex)
).withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

silver_users.select(
    "id",
    "first_name",
    "last_name",
    "email",
    "created_at",
    "updated_at",
    "valid_email",
    "full_name",
).writeTo("silver.users").overwritePartitions()

print("  ✅ silver.users written")

# ============================================================
# 2. Silver Items — price validation + category normalization
# ============================================================
print("Transforming items...")
silver_items = bronze_items.withColumn(
    "price", when(col("price") < 0, lit(0)).otherwise(col("price"))
).withColumn("category", upper(col("category")))

silver_items.select(
    "id", "name", "category", "price", "inventory", "created_at", "updated_at"
).writeTo("silver.items").overwritePartitions()

print("  ✅ silver.items written")

# ============================================================
# 3. Silver Purchases Enriched — denormalized with user + item info
# ============================================================
print("Transforming purchases (enriched)...")
silver_purchases = (
    bronze_purchases.join(
        bronze_users, bronze_purchases.user_id == bronze_users.id, "left"
    )
    .join(bronze_items, bronze_purchases.item_id == bronze_items.id, "left")
    .select(
        bronze_purchases.id,
        bronze_purchases.user_id,
        bronze_purchases.item_id,
        bronze_purchases.quantity,
        bronze_purchases.purchase_price,
        (col("quantity") * col("purchase_price")).alias("total_price"),
        bronze_users.email.alias("user_email"),
        bronze_items.name.alias("item_name"),
        bronze_items.category.alias("item_category"),
        to_date(bronze_purchases.created_at).alias("purchase_date"),
        hour(bronze_purchases.created_at).alias("purchase_hour"),
        bronze_purchases.created_at,
        bronze_purchases.updated_at,
    )
)

silver_purchases.writeTo("silver.purchases_enriched").overwritePartitions()
print("  ✅ silver.purchases_enriched written")

# ============================================================
# 4. Silver Pageviews by Items — URL parsing + item join
# ============================================================
print("Transforming pageviews...")
pageviews_with_item = (
    bronze_pageviews.withColumn(
        "page", regexp_extract(col("url"), r"^/([^/]+)/\d+$", 1)
    )
    .withColumn("item_id", regexp_extract(col("url"), r"/(\d+)$", 1).cast("bigint"))
    .filter(col("item_id").isNotNull())
)

silver_pageviews_by_items = pageviews_with_item.join(
    bronze_items, pageviews_with_item.item_id == bronze_items.id, "left"
).select(
    pageviews_with_item.user_id,
    pageviews_with_item.item_id,
    pageviews_with_item.page,
    bronze_items.name.alias("item_name"),
    bronze_items.category.alias("item_category"),
    pageviews_with_item.channel,
    pageviews_with_item.received_at,
)

silver_pageviews_by_items.writeTo("silver.pageviews_by_items").overwritePartitions()
print("  ✅ silver.pageviews_by_items written")

print("\n✅ All Bronze → Silver transformations complete.")
spark.stop()
