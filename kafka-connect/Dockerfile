# Kafka Connect with Debezium PostgreSQL + OpenSearch connectors
FROM debezium/connect:2.5

# Download OpenSearch Sink Connector
RUN cd /kafka/connect && \
    mkdir -p opensearch-sink && \
    curl --retry 5 --retry-all-errors -sL -o opensearch.tar https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v3.0.0/opensearch-connector-for-apache-kafka-3.0.0.tar && \
    tar xf opensearch.tar -C opensearch-sink --strip-components=1 && \
    rm opensearch.tar
