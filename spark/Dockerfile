FROM python:3.11-bullseye

# Install system dependencies + Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo curl vim unzip openjdk-11-jdk \
      build-essential software-properties-common ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Spark setup
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$PATH

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.5.2
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.6.0

# Download Spark
RUN mkdir -p ${SPARK_HOME} && \
    curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar xz -C /opt/spark --strip-components 1

# Download Iceberg Spark runtime
RUN curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download Postgres JDBC driver
RUN curl -sL https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o /opt/spark/jars/postgresql-42.7.4.jar

# Download AWS SDK bundle (for MinIO S3 access via Hadoop)
RUN curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar

# Download Hadoop AWS (for s3a:// protocol)
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar

# Download Iceberg AWS bundle (for S3FileIO AWS SDK v2 support)
RUN curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Spark configuration
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties

# PyIceberg configuration
COPY .pyiceberg.yaml /root/.pyiceberg.yaml

# Notebook startup script
RUN echo '#!/bin/sh' >> /bin/notebook && \
    echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/notebook && \
    echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook && \
    echo "pyspark" >> /bin/notebook && \
    chmod u+x /bin/notebook

# Working directory for scripts
RUN mkdir -p /home/iceberg/notebooks/notebooks \
             /home/iceberg/pyspark/scripts \
             /home/iceberg/pyspark/jars \
             /home/iceberg/warehouse

WORKDIR /home/iceberg

CMD ["notebook"]
