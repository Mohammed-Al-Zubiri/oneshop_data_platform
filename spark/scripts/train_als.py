"""
OneShop — ALS Recommendation Model Training (Chapter 9)
Trains a collaborative filtering model and writes predictions to Postgres.
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import explode, col, current_timestamp, lit

# Postgres config
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/oneshop")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgresuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespw")

try:
    spark = SparkSession.builder.appName("train-als-model").getOrCreate()
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    sys.exit(1)

print("Loading user-item interaction features...")

# Load features from Iceberg
interactions = spark.read.format("iceberg").load("features.user_item_interactions")

# Prepare data for ALS: user_id, item_id, rating (using purchase_count as implicit rating)
als_data = interactions.select(
    interactions.user_id.cast("int"),
    interactions.item_id.cast("int"),
    interactions.purchase_count.cast("float").alias("rating"),
)

# Split data
train, test = als_data.randomSplit([0.8, 0.2], seed=42)

# Train ALS model
print("Training ALS model...")
als = ALS(
    maxIter=10,
    regParam=0.1,
    rank=10,
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    coldStartStrategy="drop",
    implicitPrefs=True,
)

model = als.fit(train)

# Evaluate
evaluator = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction"
)
predictions = model.transform(test)
rmse = evaluator.evaluate(predictions)
print(f"  RMSE on test set: {rmse:.4f}")

# Generate top-10 recommendations for all users
print("Generating recommendations for all users...")
user_recs = model.recommendForAllUsers(10)

# Flatten recommendations and write to Postgres

recs_flat = user_recs.select("user_id", explode("recommendations").alias("rec")).select(
    "user_id",
    col("rec.item_id").alias("item_id"),
    col("rec.rating").alias("score"),
    lit("als_v1").alias("model_version"),
    current_timestamp().alias("generated_at"),
)

# Write to Postgres
recs_flat.write.format("jdbc").option("driver", "org.postgresql.Driver").option(
    "url", POSTGRES_URL
).option("dbtable", "user_recommendations").option("user", POSTGRES_USER).option(
    "password", POSTGRES_PASSWORD
).option(
    "truncate", "true"
).mode(
    "overwrite"
).save()

rec_count = recs_flat.count()
print(f"  ✅ ALS model trained. {rec_count} recommendations written to Postgres.")
spark.stop()
