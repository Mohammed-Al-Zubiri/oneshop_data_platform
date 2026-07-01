"""
OneShop — Semantic Search for Customer Reviews (Chapter 10)
Uses pgvector for similarity search with sentence-transformers.
"""

import os
import streamlit as st
import psycopg2
import pandas as pd
from sentence_transformers import SentenceTransformer

# Postgres connection
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "oneshop"),
    "user": os.getenv("POSTGRES_USER", "postgresuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgrespw"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}


@st.cache_resource
def load_model():
    """Load the sentence transformer model (cached)."""
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def find_similar_reviews(query_text: str, top_n: int = 5):
    """Find reviews most similar to the query using pgvector cosine distance."""
    model = load_model()
    query_embedding = model.encode(query_text).tolist()

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT customer_name, review, (review_embedding <=> %s::vector) AS similarity
        FROM public.reviews
        WHERE review_embedding IS NOT NULL
        ORDER BY similarity ASC
        LIMIT %s;
        """,
        (query_embedding, top_n),
    )
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


st.set_page_config(page_title="Customer Review Search", layout="wide")
st.title("🔍 Search Customer Reviews by Similarity")
st.markdown(
    "Find reviews by meaning, not just keywords. Powered by **pgvector** + **sentence-transformers**."
)

query = st.text_area(
    "Enter your search text:",
    "",
    placeholder="e.g., The customer service was very helpful and kind.",
)
top_n = st.slider(
    "Number of similar reviews to show", min_value=1, max_value=20, value=5
)

if st.button("Search") and query.strip():
    with st.spinner("Searching for similar reviews..."):
        results = find_similar_reviews(query, top_n)
        if results:
            df = pd.DataFrame(results, columns=["Customer Name", "Review", "Distance"])
            df["Similarity"] = df["Distance"].apply(lambda x: f"{1 - x:.4f}")
            st.dataframe(
                df[["Customer Name", "Review", "Similarity"]], use_container_width=True
            )
        else:
            st.info("No similar reviews found. Have you generated embeddings yet?")
