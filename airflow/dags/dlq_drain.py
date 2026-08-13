"""DLQ drain, scheduled as its own DAG (Section 9).

Its own DAG rather than a task on the rollup for two reasons:

  * Different cadence. Quarantined records should be recovered within minutes;
    the rollup runs hourly. Coupling them would leave events sitting in the DLQ
    for up to an hour for no reason.
  * Different blast radius. A drain that fails must not mark the hourly rollup
    as failed - they share no data dependency, and conflating them means one
    noisy alert channel for two unrelated concerns.

The drain is intentionally conservative: it inspects first, refuses to run when
the DLQ is larger than a sane ceiling (which means something systemic is wrong
and re-injecting thousands of records will just re-break the thing that broke),
and parks anything it cannot automatically recover.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import timedelta
from typing import Any, Dict

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

from rtdp_common import DEFAULT_ARGS, emit_alert, sla_miss_callback

log = logging.getLogger(__name__)

# If the DLQ is bigger than this, a human should look before anything replays.
# Draining a 50k-record DLQ caused by a dead Cassandra node just replays 50k
# failures into a still-dead node.
DLQ_CIRCUIT_BREAKER = int(os.environ.get("DLQ_DRAIN_MAX", "20000"))


def _run_dlq_tool(*tool_args: str) -> Dict[str, Any]:
    """Invoke the drain inside the producer container.

    The DLQ tooling lives with the producer because that is where the Kafka
    client is; running it there rather than reimplementing it here keeps one
    implementation of the routing rules.
    """
    cmd = ["docker", "exec", "rtdp-producer", "python", "-m",
           "replay.dlq_tools", *tool_args]
    log.info("running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        raise RuntimeError(f"dlq_tools failed ({proc.returncode}): "
                           f"{proc.stderr[-1000:]}")
    out = proc.stdout
    return json.loads(out[out.index("{"):])


with DAG(
    dag_id="dlq_drain",
    description="Inspect and drain the dead letter queue back into the pipeline",
    default_args=DEFAULT_ARGS,
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    tags=["rtdp", "reliability", "dlq"],
) as dag:

    @task(task_id="inspect_dlq")
    def inspect_dlq() -> Dict[str, Any]:
        result = _run_dlq_tool("inspect", "--idle-timeout", "5")
        log.info("DLQ holds %d records (%d distinct events): %s",
                 result.get("records", 0), result.get("distinct_event_ids", 0),
                 result.get("by_stage"))
        if result.get("records", 0) > DLQ_CIRCUIT_BREAKER:
            emit_alert("dlq_circuit_breaker", {
                "records": result["records"], "ceiling": DLQ_CIRCUIT_BREAKER,
                "by_stage": result.get("by_stage"),
                "action": "drain skipped; investigate before replaying"})
            raise RuntimeError(
                f"DLQ holds {result['records']} records, above the "
                f"{DLQ_CIRCUIT_BREAKER} ceiling - refusing to auto-drain")
        return result

    @task(task_id="drain_dlq", sla=timedelta(minutes=10))
    def drain_dlq(inspection: Dict[str, Any]) -> Dict[str, Any]:
        if inspection.get("records", 0) == 0:
            log.info("DLQ empty, nothing to drain")
            return {"records_read": 0, "reinjected": 0, "direct_written": 0,
                    "parked": 0, "unaccounted": 0}
        result = _run_dlq_tool("drain", "--idle-timeout", "5",
                               "--report", "/opt/app/results/raw/dlq_drain_latest.json")
        if result.get("unaccounted", 0) != 0:
            emit_alert("dlq_drain_unaccounted", result)
            raise RuntimeError(
                f"drain lost {result['unaccounted']} records: read "
                f"{result['records_read']}, accounted {result['accounted_for']}")
        log.info("drained: reinjected=%s direct=%s parked=%s",
                 result.get("reinjected"), result.get("direct_written"),
                 result.get("parked"))
        return result

    @task(task_id="report_drain")
    def report_drain(drained: Dict[str, Any]) -> Dict[str, Any]:
        # Parked records are the ones no automation can recover. They are the
        # only category that should ever page a person, so they get their own
        # alert rather than being buried in a success log line.
        if drained.get("parked", 0) > 0:
            emit_alert("dlq_records_parked", {
                "parked": drained["parked"],
                "file": drained.get("parked_file"),
                "note": "permanently un-replayable; needs a human decision"})
        return drained

    report_drain(drain_dlq(inspect_dlq()))
