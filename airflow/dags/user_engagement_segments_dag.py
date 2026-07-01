from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
from minio import Minio

silver_dataset = Dataset("s3://warehouse/silver")
gold_dataset = Dataset("s3://warehouse/gold")
segments_dataset = Dataset("s3://customer-segments/segment_users")


def upload_csv_to_minio(**context):
    ds = context["ds"]
    local_file_path = f"/tmp/segmented_users_{ds}.csv"
    object_name = f"segment_users/segmented_users_{ds}.csv"
    bucket_name = "customer-segments"

    # Initialize MinIO client
    client = Minio(
        "minio:9000", access_key="admin", secret_key="password", secure=False
    )

    # Create the bucket if it does not exist
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)

    # Upload the file
    client.fput_object(bucket_name, object_name, local_file_path)
    print(f"✅ Uploaded {local_file_path} to s3://{bucket_name}/{object_name}")

    # Generate presigned URL for external access
    # We must use 'localhost:9000' here so the signature is generated with 'Host: localhost:9000'
    # We pass 'region' explicitly so MinIO doesn't attempt to ping localhost over the Docker network
    presigned_client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password",
        secure=False,
        region="us-east-1",
    )
    download_url = presigned_client.presigned_get_object(
        bucket_name, object_name, expires=timedelta(days=7)
    )

    # Push path to XCom if needed downstream
    context["ti"].xcom_push(key="s3_path", value=f"s3://{bucket_name}/{object_name}")
    context["ti"].xcom_push(key="download_url", value=download_url)


def export_segmented_users_to_csv(**context):
    ds = context["ds"]  # Execution date
    output_path = f"/tmp/segmented_users_{ds}.csv"

    query = """
        SELECT user_id, email, full_name, total_pageviews, active_days,
               last_active_date, days_since_last_active, engagement_segment
        FROM iceberg.gold.user_engagement_segments
    """

    trino_hook = TrinoHook(trino_conn_id="trino_default")
    records = trino_hook.get_records(sql=query)

    headers = [
        "user_id",
        "email",
        "full_name",
        "total_pageviews",
        "active_days",
        "last_active_date",
        "days_since_last_active",
        "engagement_segment",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(records)

    context["ti"].xcom_push(key="csv_path", value=output_path)


with DAG(
    dag_id="user_engagement_segments",
    start_date=datetime(2023, 1, 1),
    schedule=[silver_dataset],
    catchup=False,
    template_searchpath=[os.path.join(os.path.dirname(__file__), "sql")],
    tags=["batch", "gold", "marketing", "export"],
) as dag:

    segment_users = TrinoOperator(
        task_id="segment_users",
        sql="gold_user_engagement_segments.sql",
        trino_conn_id="trino_default",
        outlets=[gold_dataset],
    )

    export_csv = PythonOperator(
        task_id="export_to_csv",
        python_callable=export_segmented_users_to_csv,
        provide_context=True,
    )

    upload_to_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_csv_to_minio,
        provide_context=True,
        outlets=[segments_dataset],
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to="marketing-team@example.com",
        subject="[Airflow] User Engagement Segments Exported",
        html_content="""
            <p>Hello Team,</p>
            <p>The user engagement segments have been refreshed and exported to a CSV file.</p>
            <p>
                <a href="{{ ti.xcom_pull(task_ids='upload_to_minio', key='download_url') }}" style="display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px; font-weight: bold;">
                    Download Segments CSV
                </a>
            </p>
            <p><em>Note: This secure direct download link will expire in 7 days.</em></p>
            <p>– Airflow Bot</p>
        """,
    )

    segment_users >> export_csv >> upload_to_minio >> notify_success
