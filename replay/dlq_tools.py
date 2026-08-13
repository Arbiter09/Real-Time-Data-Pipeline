"""DLQ inspection and the replay drain.

A DLQ without a drain is a landfill. This module is what turns "N events
quarantined" into "N events quarantined AND recoverable", which is the
difference between a completeness guarantee and an excuse.

Routing rules, and why each one is what it is:

  cassandra_sink  Transient: the write exhausted its retry budget because a
                  node was down or a coordinator timed out. Once the ring is
                  healthy the event will write fine. -> RE-INJECT to trips.raw.

  late_event      The event is valid; it simply arrived beyond the allowed
                  lateness. Re-injecting it into the same stream would make it
                  late again and quarantine it again, forever. This is a real
                  trap and the reason a naive "just replay everything" drain
                  livelocks. -> WRITE DIRECTLY to Cassandra, bypassing the
                  watermark. The watermark protects the streaming aggregation,
                  not the serving table, and the serving table's keys are
                  idempotent so a direct write is safe.

  parse           Permanent: the payload does not satisfy the schema. Retrying
                  changes nothing. -> PARK to a file for human inspection.
                  Nothing is deleted.

  producer        The producer could not hand the event to Kafka at all.
                  -> RE-INJECT.

A replay_count ceiling stops a record cycling between the DLQ and the topic
forever; anything over the ceiling is parked with that reason recorded.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from common import dlq as dlqmod
from common.config import CassandraConfig, KafkaConfig

REINJECT_STAGES = {dlqmod.STAGE_CASSANDRA_SINK, dlqmod.STAGE_PRODUCER}
DIRECT_WRITE_STAGES = {dlqmod.STAGE_LATE}
PARK_STAGES = {dlqmod.STAGE_PARSE}


def _consumer(group: str, bootstrap: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 30000,
    })


def read_all(topic: str, bootstrap: str, group: str,
             idle_timeout: float = 5.0, limit: int = 0) -> List[Dict[str, Any]]:
    """Read a topic from the beginning until it goes quiet.

    Reads from the earliest offset every time and never commits, so inspection
    is repeatable and cannot advance a real consumer's position.
    """
    c = _consumer(group, bootstrap)
    records: List[Dict[str, Any]] = []
    assigned: List[TopicPartition] = []

    def on_assign(consumer, partitions):
        for p in partitions:
            p.offset = 0          # always from the start
        consumer.assign(partitions)
        assigned.extend(partitions)

    c.subscribe([topic], on_assign=on_assign)
    last_msg = time.time()
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                if time.time() - last_msg > idle_timeout:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            last_msg = time.time()
            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except Exception:
                rec = {"failure_stage": "unreadable_dlq_record",
                       "original_payload": msg.value().decode("utf-8", "replace")}
            rec["_partition"] = msg.partition()
            rec["_offset"] = msg.offset()
            records.append(rec)
            if limit and len(records) >= limit:
                break
    finally:
        c.close()
    return records


def inspect(args: argparse.Namespace) -> Dict[str, Any]:
    kcfg = KafkaConfig()
    recs = read_all(args.topic or kcfg.topic_dlq, kcfg.bootstrap,
                    f"rtdp-dlq-inspect-{uuid.uuid4().hex[:8]}",
                    idle_timeout=args.idle_timeout)

    distinct = {r.get("event_id") for r in recs if r.get("event_id")}
    by_stage = Counter(r.get("failure_stage", "?") for r in recs)
    by_reason = Counter((r.get("failure_reason") or "?").split(":")[0] for r in recs)
    retry_counts = [r.get("retry_count", 0) for r in recs if r.get("retry_count")]

    result = {
        "topic": args.topic or kcfg.topic_dlq,
        "records": len(recs),
        "distinct_event_ids": len(distinct),
        "records_without_event_id": sum(1 for r in recs if not r.get("event_id")),
        "by_stage": dict(by_stage),
        "by_reason": dict(by_reason),
        "mean_retry_count_when_retried": (
            round(sum(retry_counts) / len(retry_counts), 3) if retry_counts else 0.0),
        "routing_preview": {
            "reinject": sum(1 for r in recs if r.get("failure_stage") in REINJECT_STAGES),
            "direct_write": sum(1 for r in recs if r.get("failure_stage") in DIRECT_WRITE_STAGES),
            "park": sum(1 for r in recs if r.get("failure_stage") in PARK_STAGES),
        },
        "sample": recs[:3],
    }
    print(json.dumps(result, indent=2, default=str))
    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(result, fh, indent=2, default=str)
    return result


# ---------------------------------------------------------------------------
# drain
# ---------------------------------------------------------------------------
def _direct_write(records: List[Dict[str, Any]], ccfg: CassandraConfig) -> Tuple[int, int]:
    """Write late events straight to the serving tables.

    Safe because every primary key is derived from the event, so this is an
    upsert whether or not the stream already saw the record.
    """
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster

    if not records:
        return 0, 0

    sys.path.insert(0, "/opt/app")
    from streaming.cassandra_sink import CQL, build_statements, ALL_TABLES

    cluster = Cluster(ccfg.hosts)
    written = failed = 0
    try:
        session = cluster.connect(ccfg.keyspace)
        prepared = {}
        for name in ("trips_by_id", "trips_by_driver_day", "trips_by_city_hour"):
            st = session.prepare(CQL[name].format(ks=ccfg.keyspace))
            st.consistency_level = getattr(ConsistencyLevel, ccfg.write_consistency)
            prepared[name] = st

        now_ms = int(time.time() * 1000)
        for rec in records:
            try:
                payload = json.loads(rec["original_payload"])
                payload["event_time"] = datetime.fromisoformat(
                    payload["event_time"].replace("Z", "+00:00"))
                for key, params in build_statements(payload, ALL_TABLES, now_ms):
                    session.execute(prepared[key], params)
                written += 1
            except Exception as exc:
                print(f"  direct-write failed for {rec.get('event_id')}: "
                      f"{type(exc).__name__}: {exc}", file=sys.stderr)
                failed += 1
    finally:
        cluster.shutdown()
    return written, failed


def drain(args: argparse.Namespace) -> Dict[str, Any]:
    kcfg = KafkaConfig()
    ccfg = CassandraConfig()
    topic = args.topic or kcfg.topic_dlq

    recs = read_all(topic, kcfg.bootstrap,
                    f"rtdp-dlq-drain-{uuid.uuid4().hex[:8]}",
                    idle_timeout=args.idle_timeout, limit=args.limit)
    if not recs:
        result = {"drained": 0, "reinjected": 0, "direct_written": 0, "parked": 0,
                  "note": "DLQ empty"}
        print(json.dumps(result, indent=2))
        return result

    reinject: List[Dict[str, Any]] = []
    direct: List[Dict[str, Any]] = []
    park: List[Dict[str, Any]] = []

    for r in recs:
        stage = r.get("failure_stage", "?")
        if int(r.get("replay_count", 0)) >= args.max_replays:
            r["_park_reason"] = f"replay_count >= {args.max_replays}"
            park.append(r)
        elif stage in REINJECT_STAGES:
            reinject.append(r)
        elif stage in DIRECT_WRITE_STAGES:
            direct.append(r)
        else:
            r["_park_reason"] = f"stage {stage!r} is not automatically recoverable"
            park.append(r)

    print(f"[drain] {len(recs)} records: reinject={len(reinject)} "
          f"direct={len(direct)} park={len(park)}", flush=True)

    if args.dry_run:
        result = {"dry_run": True, "records": len(recs),
                  "would_reinject": len(reinject),
                  "would_direct_write": len(direct),
                  "would_park": len(park)}
        print(json.dumps(result, indent=2))
        return result

    # ---- re-inject -------------------------------------------------------
    reinjected = reinject_failed = 0
    if reinject:
        p = Producer({"bootstrap.servers": kcfg.bootstrap,
                      "enable.idempotence": True, "acks": "all",
                      "linger.ms": 20})
        errors: List[str] = []

        def cb(err, _msg):
            if err is not None:
                errors.append(str(err))

        for r in reinject:
            try:
                payload = json.loads(r["original_payload"])
                key = payload.get("trip_id") or r.get("event_id") or "unknown"
                p.produce(kcfg.topic_trips, key=key,
                          value=r["original_payload"].encode(), on_delivery=cb)
                reinjected += 1
            except Exception as exc:
                print(f"  reinject failed: {exc}", file=sys.stderr)
                reinject_failed += 1
        p.flush(60)
        reinject_failed += len(errors)
        reinjected -= len(errors)

    # ---- direct write ----------------------------------------------------
    direct_written, direct_failed = _direct_write(direct, ccfg)

    # ---- park ------------------------------------------------------------
    parked_path = None
    if park:
        os.makedirs(args.park_dir, exist_ok=True)
        parked_path = os.path.join(
            args.park_dir,
            f"parked_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.ndjson")
        with open(parked_path, "w") as fh:
            for r in park:
                fh.write(json.dumps(r, default=str) + "\n")

    result = {
        "drained_at": datetime.now(timezone.utc).isoformat(),
        "records_read": len(recs),
        "reinjected": reinjected,
        "reinject_failed": reinject_failed,
        "direct_written": direct_written,
        "direct_write_failed": direct_failed,
        "parked": len(park),
        "parked_file": parked_path,
        "accounted_for": reinjected + direct_written + len(park),
        "unaccounted": len(recs) - (reinjected + direct_written + len(park)),
    }
    print(json.dumps(result, indent=2))
    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(result, fh, indent=2)
    return result


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="DLQ inspection and replay drain")
    sub = p.add_subparsers(dest="cmd", required=True)

    common_args = argparse.ArgumentParser(add_help=False)
    common_args.add_argument("--topic", default=None)
    common_args.add_argument("--idle-timeout", type=float, default=5.0)
    common_args.add_argument("--report", default=None)

    i = sub.add_parser("inspect", parents=[common_args])
    i.set_defaults(func=inspect)

    d = sub.add_parser("drain", parents=[common_args])
    d.add_argument("--limit", type=int, default=0)
    d.add_argument("--max-replays", type=int, default=3)
    d.add_argument("--park-dir", default="/opt/app/results/parked")
    d.add_argument("--dry-run", action="store_true")
    d.set_defaults(func=drain)

    args = p.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
