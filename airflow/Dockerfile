FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_VERSION=3.5.2
ENV SPARK_HOME=/opt/spark

RUN mkdir -p ${SPARK_HOME} && \
    curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar xz -C /opt/spark --strip-components 1

ENV PATH=$PATH:${SPARK_HOME}/bin

USER airflow

# Install providers for Trino, Postgres, and HTTP
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-trino \
    apache-airflow-providers-postgres \
    apache-airflow-providers-http \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-docker \
    minio \
    pyspark \
    great_expectations==0.18.21 \
    airflow-provider-great-expectations \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
