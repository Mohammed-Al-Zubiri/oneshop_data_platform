"""
OneShop — Flash Sale Performance Dashboard (Chapter 7)
Real-time OLAP visualization powered by ClickHouse.
"""

import os
import time
import streamlit as st
import plotly.express as px
from clickhouse_connect import get_client

# ClickHouse connection
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "mysecret")

client = get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD)

st.set_page_config(page_title="Flash Sale Performance Dashboard", layout="wide")
st.title("🛒 Flash Sale [FLASH2025] Performance")

# --- 1. Hourly Sales Volume (Last 24 Hours) ---
st.header("📊 Hourly Sales Volume (Last 24 Hours)")

hourly_query = """
SELECT
    toStartOfHour(created_at) AS hour,
    SUM(quantity) AS total_sales
FROM oneshop.purchases
WHERE deleted = 0
  AND created_at >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour ASC
"""

try:
    hourly_df = client.query_df(hourly_query)
    if not hourly_df.empty:
        fig = px.bar(
            hourly_df,
            x="hour",
            y="total_sales",
            title="Hourly Sales Volume (Past 24 Hours)",
            labels={"hour": "Hour", "total_sales": "Quantity Sold"},
            color_discrete_sequence=["#667eea"],
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No sales data available yet. Wait for purchase events to flow in.")
except Exception as e:
    st.error(f"Error querying ClickHouse: {e}")

# --- 2. Top 10 Selling Products by Revenue ---
st.header("🏆 Top 10 Selling Products by Revenue")

top_products_query = """
SELECT
    item_id,
    SUM(quantity) AS total_quantity,
    SUM(quantity * purchase_price) AS total_revenue
FROM oneshop.purchases
GROUP BY item_id
ORDER BY total_revenue DESC
LIMIT 10
"""

try:
    top_df = client.query_df(top_products_query)
    if not top_df.empty:
        st.dataframe(top_df, use_container_width=True)
    else:
        st.info("No product data available yet.")
except Exception as e:
    st.error(f"Error querying ClickHouse: {e}")

# --- Auto-refresh every 30 seconds ---
st.markdown("---")
st.caption("Dashboard auto-refreshes every 30 seconds.")
st.empty()
time.sleep(30)
st.rerun()
