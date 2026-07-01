FROM debezium/postgres:17

# Install pgvector extension on top of the Debezium image
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-17-pgvector && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
