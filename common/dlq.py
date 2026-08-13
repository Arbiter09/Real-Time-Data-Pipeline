"""Dead letter queue envelope.

One shared envelope format for both producers of DLQ records (the Kafka
producer and the Spark Cassandra sink), because the replay path has to drain
both and should not need to know which stage quarantined a record.

An envelope that does not carry the ORIGINAL payload is a log line, not a DLQ -
you cannot replay from it. The payload is stored verbatim as a string so that a
record which failed *because* it was unparseable still round-trips.
"""
from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

DLQ_SCHEMA_VERSION = 1

# Stage constants - which part of the pipeline gave up on this record.
STAGE_PRODUCER = "producer"
STAGE_CASSANDRA_SINK = "cassandra_sink"
STAGE_PARSE = "parse"
STAGE_LATE = "late_event"


@dataclass
class DLQRecord:
    original_payload: str      # verbatim bytes-as-text of the failed event
    failure_reason: str        # exception type + message
    failure_stage: str         # one of the STAGE_* constants
    retry_count: int           # attempts already spent before giving up
    first_failed_at_ms: int
    quarantined_at_ms: int
    event_id: Optional[str] = None      # extracted when parseable, for tracing
    source_topic: Optional[str] = None
    source_partition: Optional[int] = None
    source_offset: Optional[int] = None
    replay_count: int = 0      # incremented each time the drain re-injects it
    host: str = ""
    dlq_schema_version: int = DLQ_SCHEMA_VERSION

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @staticmethod
    def from_json(raw: str) -> "DLQRecord":
        d = json.loads(raw)
        d.pop("dlq_schema_version", None)
        known = DLQRecord.__dataclass_fields__.keys()
        return DLQRecord(**{k: v for k, v in d.items() if k in known})


def build(
    payload: str,
    reason: str,
    stage: str,
    retry_count: int,
    *,
    first_failed_at_ms: Optional[int] = None,
    source_topic: Optional[str] = None,
    source_partition: Optional[int] = None,
    source_offset: Optional[int] = None,
    replay_count: int = 0,
) -> DLQRecord:
    now = int(time.time() * 1000)
    event_id = None
    try:
        event_id = json.loads(payload).get("event_id")
    except Exception:
        pass  # unparseable payloads are exactly what the DLQ is for
    return DLQRecord(
        original_payload=payload,
        failure_reason=reason[:2000],
        failure_stage=stage,
        retry_count=retry_count,
        first_failed_at_ms=first_failed_at_ms or now,
        quarantined_at_ms=now,
        event_id=event_id,
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        replay_count=replay_count,
        host=socket.gethostname(),
    )


def summarize(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    """Group a drained DLQ by stage and reason - used by the drain report."""
    by_stage: Dict[str, int] = {}
    by_reason: Dict[str, int] = {}
    for r in records:
        by_stage[r.get("failure_stage", "?")] = by_stage.get(r.get("failure_stage", "?"), 0) + 1
        reason = (r.get("failure_reason") or "?").split(":")[0]
        by_reason[reason] = by_reason.get(reason, 0) + 1
    return {"total": len(records), "by_stage": by_stage, "by_reason": by_reason}
