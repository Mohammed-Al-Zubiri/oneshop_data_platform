from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from datetime import datetime, timedelta


def check_bronze_data():
    try:
        hook = TrinoHook(trino_conn_id="trino_default")
        records = hook.get_records("SELECT count(*) FROM iceberg.bronze.pageviews")
        return records and len(records) > 0 and records[0][0] > 0
    except Exception as e:
        print(f"Error checking table: {e}")
        return False


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "iceberg_maintenance",
    default_args=default_args,
    description="Automated Iceberg Table Maintenance (Compaction, Expiration, Orphan Removal)",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance", "iceberg", "spark"],
) as dag:

    # Note: In a production environment with many tables,
    # we would dynamically generate these tasks for each active table.
    # For Chapter 2 parity, we demonstrate on the bronze.pageviews table.

    compact_pageviews = SparkSqlOperator(
        task_id="compact_bronze_pageviews",
        sql="CALL demo.system.rewrite_data_files(table => 'bronze.pageviews');",
        conn_id="spark_default",
        name="iceberg_compact_pageviews",
    )

    expire_snapshots_pageviews = SparkSqlOperator(
        task_id="expire_snapshots_bronze_pageviews",
        sql="CALL demo.system.expire_snapshots(table => 'bronze.pageviews', older_than => TIMESTAMP '{{ macros.ds_add(ds, -7) }} 00:00:00.000');",
        conn_id="spark_default",
        name="iceberg_expire_snapshots_pageviews",
    )

    check_data_task = ShortCircuitOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data,
    )

    remove_orphans_pageviews = SparkSqlOperator(
        task_id="remove_orphans_bronze_pageviews",
        sql="CALL demo.system.remove_orphan_files(table => 'bronze.pageviews');",
        conn_id="spark_default",
        name="iceberg_remove_orphans_pageviews",
    )

    (
        check_data_task
        >> compact_pageviews
        >> expire_snapshots_pageviews
        >> remove_orphans_pageviews
    )
