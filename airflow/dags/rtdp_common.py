"""Shared helpers for the RTDP DAGs.

Kept in the dags folder (not in `common/`) because Airflow parses this
directory directly and a DAG that cannot import its helpers is a DAG that
fails at parse time, which is far harder to debug than an import error at task
time.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

log = logging.getLogger(__name__)

CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3").split(",")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "rtdp")
CASSANDRA_READ_CL = os.environ.get("CASSANDRA_READ_CONSISTENCY", "QUORUM")
CASSANDRA_LOCAL_DC = os.environ.get("CASSANDRA_DC", "dc1")

PG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "user": os.environ.get("POSTGRES_USER", "rtdp"),
    "password": os.environ.get("POSTGRES_PASSWORD", "rtdp"),
    "dbname": os.environ.get("POSTGRES_DB", "rtdp"),
}

CITIES = ["nyc", "chi", "sfo", "lax", "bos", "sea", "aus", "mia"]

# Alerting knobs. Both default off so a clone of this repo does not try to
# reach a webhook that does not exist; the DAGs log the alert either way.
ALERT_WEBHOOK = os.environ.get("RTDP_ALERT_WEBHOOK", "")
ALERT_LOG_PATH = os.environ.get("RTDP_ALERT_LOG", "/opt/airflow/logs/rtdp_alerts.ndjson")


def cassandra_session():
    """Session for the batch DAGs.

    Both settings below silence warnings the driver emitted on the first real
    run, and both are correctness knobs rather than cosmetics:

      * An ExecutionProfile built with contact_points but no load-balancing
        policy is deprecated and will raise in a future driver major. Naming
        the local DC also stops the driver guessing it from whichever node
        answered first.
      * Without an explicit protocol_version the driver negotiates downward
        (66 -> 65 -> 5) on every single connection, which is wasted round
        trips on a task that connects, reads one hour and disconnects.
    """
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc=CASSANDRA_LOCAL_DC)),
        consistency_level=getattr(ConsistencyLevel, CASSANDRA_READ_CL),
        request_timeout=60.0)
    cluster = Cluster(
        CASSANDRA_HOSTS,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=5,
    )
    return cluster, cluster.connect(CASSANDRA_KEYSPACE)


def pg_conn():
    import psycopg2
    conn = psycopg2.connect(**PG)
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


def emit_alert(kind: str, payload: Dict[str, Any]) -> None:
    """Record an alert. Written to a file always, posted to a webhook if set.

    Deliberately never raises: an alerting path that can fail the task it is
    reporting on turns one problem into two.
    """
    record = {"kind": kind, "at": datetime.now(timezone.utc).isoformat(), **payload}
    log.warning("ALERT %s: %s", kind, json.dumps(record))
    try:
        os.makedirs(os.path.dirname(ALERT_LOG_PATH), exist_ok=True)
        with open(ALERT_LOG_PATH, "a") as fh:
            fh.write(json.dumps(record) + "\n")
    except Exception as exc:
        log.error("could not write alert log: %s", exc)
    if ALERT_WEBHOOK:
        try:
            import urllib.request
            req = urllib.request.Request(
                ALERT_WEBHOOK, data=json.dumps(record).encode(),
                headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:
            log.error("alert webhook failed: %s", exc)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Fires when a task blows its SLA while still running or after it finishes.

    This is the distinction Section 9 asks for: alerting on SLA miss rather
    than only on failure. A rollup that succeeds 40 minutes late has not
    failed, and no failure callback will ever fire for it - but downstream
    consumers reading that table are looking at stale data, which is the thing
    someone actually needs to be told about.
    """
    emit_alert("sla_miss", {
        "dag_id": getattr(dag, "dag_id", str(dag)),
        "tasks": [str(getattr(s, "task_id", s)) for s in (slas or [])],
        "blocking": [str(getattr(t, "task_id", t)) for t in (blocking_task_list or [])],
    })


def failure_callback(context) -> None:
    ti = context.get("task_instance")
    emit_alert("task_failure", {
        "dag_id": context.get("dag").dag_id if context.get("dag") else None,
        "task_id": getattr(ti, "task_id", None),
        "try_number": getattr(ti, "try_number", None),
        "logical_date": str(context.get("logical_date") or context.get("execution_date")),
        "exception": str(context.get("exception"))[:500],
    })


DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "rtdp",
    "depends_on_past": False,
    # Task-level retries with exponential backoff, mirroring the pipeline's own
    # 1s/2s/4s policy in shape. The floor is higher because an Airflow task
    # failure usually means a dependency is down, and one second is not long
    # enough for anything to have recovered.
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
    "email_on_failure": False,
    "email_on_retry": False,
}


def hour_bounds(logical_date: datetime) -> Tuple[datetime, datetime]:
    start = logical_date.replace(minute=0, second=0, microsecond=0)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    return start, start + timedelta(hours=1)
