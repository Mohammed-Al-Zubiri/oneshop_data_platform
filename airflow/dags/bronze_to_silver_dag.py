"""
OneShop Airflow DAG — Bronze to Silver Transformation
Transforms raw Bronze data into cleaned, enriched Silver layer.

A Great Expectations data-quality gate runs first and blocks the Silver
transform if any expectation suite fails.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

postgres_bronze = Dataset("s3://warehouse/bronze/postgres")
minio_bronze = Dataset("s3://warehouse/bronze/pageviews")
silver_dataset = Dataset("s3://warehouse/silver")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@oneshop.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver",
    default_args=default_args,
    description="Transform Bronze to Silver layer with cleaning and enrichment",
    schedule=[postgres_bronze, minio_bronze],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["batch", "silver", "etl"],
) as dag:

    # ------------------------------------------------------------------
    # Step 1 — Great Expectations data-quality gate
    # Exits 1 on any suite failure → Airflow marks task as FAILED and
    # stops the DAG immediately (bronze_to_silver_transform never runs).
    # ------------------------------------------------------------------
    validate_bronze = SparkSubmitOperator(
        task_id="validate_bronze",
        application="/opt/airflow/spark_scripts/validate_bronze.py",
        conn_id="spark_default",
    )

    # ------------------------------------------------------------------
    # Step 2 — Silver transformation (only reached if gate passes)
    # ------------------------------------------------------------------
    bronze_to_silver_transform = SparkSubmitOperator(
        task_id="bronze_to_silver_transform",
        application="/opt/airflow/spark_scripts/bronze_to_silver.py",
        conn_id="spark_default",
        outlets=[silver_dataset],
    )

    # Gate MUST pass before transform is allowed to run
    validate_bronze >> bronze_to_silver_transform
