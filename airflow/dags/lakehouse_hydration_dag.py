"""
OneShop Airflow DAG — Lakehouse Hydration Pipeline
Loads data from Postgres and MinIO into Iceberg Bronze layer.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

postgres_bronze = Dataset("s3://warehouse/bronze/postgres")
minio_bronze = Dataset("s3://warehouse/bronze/pageviews")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["admin@oneshop.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_hydration",
    default_args=default_args,
    description="Load Postgres OLTP tables + MinIO pageviews into Iceberg Bronze layer",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["batch", "bronze", "etl"],
) as dag:

    postgres_to_bronze = SparkSubmitOperator(
        task_id="postgres_to_bronze",
        application="/opt/airflow/spark_scripts/postgres_loader.py",
        conn_id="spark_default",
        outlets=[postgres_bronze],
    )

    minio_to_bronze = SparkSubmitOperator(
        task_id="minio_to_bronze",
        application="/opt/airflow/spark_scripts/minio_loader.py",
        conn_id="spark_default",
        outlets=[minio_bronze],
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to="admin@oneshop.com",
        subject="✅ Lakehouse Hydration Complete",
        html_content="Bronze layer successfully hydrated from Postgres and MinIO.",
    )

    [postgres_to_bronze, minio_to_bronze] >> notify_success
