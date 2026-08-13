#!/usr/bin/env bash
# One-shot Airflow bootstrap: create its metadata DB inside the shared Postgres,
# migrate, and create the admin user.
set -euo pipefail

echo "[airflow-init] waiting for postgres"
for _ in $(seq 1 60); do
  if nc -z postgres 5432; then break; fi
  sleep 2
done

# Airflow gets its own database inside the same Postgres instance rather than a
# second container: on a 12 GiB budget, a dedicated metadata Postgres would cost
# ~300MB that the Cassandra ring needs more.
export PGPASSWORD="${POSTGRES_PASSWORD}"
psql -h postgres -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tc \
  "SELECT 1 FROM pg_database WHERE datname='airflow'" | grep -q 1 \
  || psql -h postgres -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "CREATE DATABASE airflow"

echo "[airflow-init] db migrate"
airflow db migrate

echo "[airflow-init] creating admin user"
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com 2>/dev/null || echo "  user already exists"

echo "[airflow-init] done"
