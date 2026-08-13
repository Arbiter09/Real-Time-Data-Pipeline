FROM apache/airflow:2.10.3-python3.11

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl netcat-openbsd postgresql-client; \
    rm -rf /var/lib/apt/lists/*
USER airflow

# The DAGs talk to Cassandra, Kafka and Postgres directly through PythonOperator
# rather than shelling out to `docker exec`. Shelling out would make every task
# depend on the Docker socket and would report the exec client's exit code
# rather than the work's, which is a poor signal for retries and SLAs.
RUN pip install --no-cache-dir \
      "cassandra-driver==3.29.2" \
      "confluent-kafka==2.6.0" \
      "psycopg2-binary==2.9.10"

ENV PYTHONPATH=/opt/app:/opt/airflow
