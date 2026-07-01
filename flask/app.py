"""
OneShop — Product Recommendation API (Chapter 9)
Serves pre-computed ALS recommendations from Postgres.
"""

import os
from flask import Flask, jsonify
import psycopg2
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)
metrics = PrometheusMetrics(app)

# static information as metric
metrics.info("app_info", "Recommendation API", version="1.0.0")

# Postgres connection from environment
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "oneshop")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgresuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespw")


def get_recommendations(user_id: int, top_n: int = 10):
    """Fetch top-N recommendations for a user from Postgres."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT item_id, score
        FROM user_recommendations
        WHERE user_id = %s
        ORDER BY score DESC
        LIMIT %s
        """,
        (user_id, top_n),
    )
    results = [{"item_id": row[0], "score": row[1]} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return results


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


@app.route("/recommend/<int:user_id>")
def recommend(user_id):
    """Get product recommendations for a user."""
    recommendations = get_recommendations(user_id)
    return jsonify({"user_id": user_id, "recommendations": recommendations})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=False)
