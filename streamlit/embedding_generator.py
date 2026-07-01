"""
OneShop — Vector Embedding Generator (Chapter 10)
Generates embeddings for customer reviews using sentence-transformers
and stores them in Postgres pgvector.
"""

import os
import psycopg2
from sentence_transformers import SentenceTransformer

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "oneshop"),
    "user": os.getenv("POSTGRES_USER", "postgresuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# Load model
print("Loading sentence-transformers model...")
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
print("Model loaded.")

conn = None
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT review_id, review FROM public.reviews WHERE review_embedding IS NULL"
    )
    reviews = cur.fetchall()

    if not reviews:
        print("All reviews already have embeddings. Nothing to do.")
    else:
        print(f"Generating embeddings for {len(reviews)} reviews...")
        for review_id, review_text in reviews:
            embedding = model.encode(review_text).tolist()
            cur.execute(
                "UPDATE public.reviews SET review_embedding = %s WHERE review_id = %s;",
                (embedding, review_id),
            )
        conn.commit()
        print(f"  ✅ Embeddings generated for {len(reviews)} reviews.")

finally:
    if conn:
        conn.close()
