"""
OneShop Airflow DAG — ML Training Pipeline
Trains ALS recommendation model and writes results to Postgres.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.trino.hooks.trino import TrinoHook


def wait_for_silver_data():
    try:
        hook = TrinoHook(trino_conn_id="trino_default")
        records = hook.get_records(
            "SELECT count(*) FROM iceberg.silver.purchases_enriched"
        )
        return records and len(records) > 0 and records[0][0] > 0
    except Exception as e:
        print(f"Error checking table: {e}")
        return False


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@oneshop.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="Train ALS recommendation model and generate embeddings",
    schedule="@weekly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ai-ml", "training"],
) as dag:

    compute_features = SparkSubmitOperator(
        task_id="compute_features",
        application="/opt/airflow/spark_scripts/compute_features.py",
        conn_id="spark_default",
    )

    train_als = SparkSubmitOperator(
        task_id="train_als_model",
        application="/opt/airflow/spark_scripts/train_als.py",
        conn_id="spark_default",
    )

    generate_embeddings = DockerOperator(
        task_id="generate_embeddings",
        image="oneshop-platform-streamlit-search:latest",
        command="python /app/embedding_generator.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="oneshop-platform_oneshop-network",
        auto_remove="force",
        mount_tmp_dir=False,
    )

    wait_for_data_task = PythonSensor(
        task_id="wait_for_silver_data",
        python_callable=wait_for_silver_data,
        mode="reschedule",
        poke_interval=300,  # 5 minutes
        timeout=86400,  # 24 hours
    )

    wait_for_data_task >> compute_features >> train_als
    # Embeddings can run in parallel with ML training
    wait_for_data_task >> generate_embeddings
