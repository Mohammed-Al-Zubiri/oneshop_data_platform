"""
OneShop Airflow DAG — Gold Table Refresh
Creates aggregated Gold tables using Trino for BI consumption.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.trino.operators.trino import TrinoOperator

silver_dataset = Dataset("s3://warehouse/silver")
gold_dataset = Dataset("s3://warehouse/gold")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@oneshop.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gold_table_refresh",
    default_args=default_args,
    description="Refresh Gold aggregation tables using Trino for Superset BI",
    schedule=[silver_dataset],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["batch", "gold", "trino"],
    template_searchpath=[os.path.join(os.path.dirname(__file__), "sql")],
) as dag:

    create_gold_schema = TrinoOperator(
        task_id="create_gold_schema",
        trino_conn_id="trino_default",
        sql="CREATE SCHEMA IF NOT EXISTS iceberg.gold",
    )

    # Gold: Top Selling Items
    gold_top_selling_items = TrinoOperator(
        task_id="gold_top_selling_items",
        trino_conn_id="trino_default",
        sql="gold_top_selling_items.sql",
        outlets=[gold_dataset],
    )

    # Gold: 24h Sales Performance
    gold_sales_performance_24h = TrinoOperator(
        task_id="gold_sales_performance_24h",
        trino_conn_id="trino_default",
        sql="gold_sales_performance_24h.sql",
        outlets=[gold_dataset],
    )

    # Gold: Top Converting Items
    gold_top_converting_items = TrinoOperator(
        task_id="gold_top_converting_items",
        trino_conn_id="trino_default",
        sql="gold_top_converting_items.sql",
        outlets=[gold_dataset],
    )

    # Gold: Pageviews by Channel
    gold_pageviews_by_channel = TrinoOperator(
        task_id="gold_pageviews_by_channel",
        trino_conn_id="trino_default",
        sql="gold_pageviews_by_channel.sql",
        outlets=[gold_dataset],
    )

    create_gold_schema >> [
        gold_top_selling_items,
        gold_sales_performance_24h,
        gold_top_converting_items,
        gold_pageviews_by_channel,
    ]
