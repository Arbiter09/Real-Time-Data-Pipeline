# Spark 3.5.3 + the exact connector jars this pipeline needs, baked in.
#
# Jars are downloaded at BUILD time rather than resolved by `spark-submit
# --packages` at run time. Two reasons: benchmark runs must not include Ivy
# resolution in their timings, and a pinned jar set means a reviewer who clones
# this repo six months from now gets the same bytes.
FROM apache/spark:3.5.3-python3

USER root

ARG SPARK_V=3.5.3
ARG SCALA_V=2.12
ARG KAFKA_CLIENTS_V=3.4.1
ARG COMMONS_POOL_V=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl procps netcat-openbsd libev4 libev-dev gcc python3-dev; \
    rm -rf /var/lib/apt/lists/*

# --- Structured Streaming Kafka source -------------------------------------
RUN set -eux; cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_V}/${SPARK_V}/spark-sql-kafka-0-10_${SCALA_V}-${SPARK_V}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_V}/${SPARK_V}/spark-token-provider-kafka-0-10_${SCALA_V}-${SPARK_V}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_V}/kafka-clients-${KAFKA_CLIENTS_V}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL_V}/commons-pool2-${COMMONS_POOL_V}.jar

# --- Python runtime ---------------------------------------------------------
# NOTE: Cassandra writes go through the Python driver inside mapPartitions, NOT
# through the DataStax Spark connector. That is deliberate: the reliability
# layer (per-record retry counting, configurable backoff, DLQ on exhaustion)
# needs control the connector does not expose, and Section 7.3 asks for
# "average retries to success" as a measured number.
#
# This base image ships Python 3.8, which has no aarch64 wheel for
# confluent-kafka - hence the DLQ going through Spark's Kafka sink instead of a
# second Python client. Pinned accordingly; do not bump blindly.
COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && python3 -c "import cassandra, prometheus_client; print('cassandra-driver', cassandra.__version__)"

RUN mkdir -p /opt/checkpoints /opt/app/results && chmod -R 777 /opt/checkpoints /opt/app

ENV PYTHONPATH=/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
WORKDIR /opt/app

USER 185
