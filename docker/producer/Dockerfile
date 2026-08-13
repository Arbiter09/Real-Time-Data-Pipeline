# Producer + benchmark-harness runner. Kept separate from the Spark image so
# load generation never competes with the driver for the same container's CPU
# budget - a producer that starves its own consumer would corrupt every
# throughput number in Section 7.1.
FROM python:3.11-slim-bookworm

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl procps netcat-openbsd postgresql-client; \
    rm -rf /var/lib/apt/lists/*

COPY docker/producer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/app/results
ENV PYTHONPATH=/opt/app
ENV PYTHONUNBUFFERED=1
WORKDIR /opt/app
