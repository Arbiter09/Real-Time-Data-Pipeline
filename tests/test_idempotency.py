"""Section 5's required test: ingest the same event twice, assert no new rows.

Two levels, because they prove different things:

  test_schema_upsert_is_idempotent
      Writes the identical event twice through the real prepared statements and
      asserts every table's row count is unchanged. This proves the SCHEMA
      property - that primary keys are derived entirely from event fields - and
      it is the property everything else leans on.

  test_replay_of_a_full_batch_is_idempotent
      Writes a batch, records counts, replays the whole batch, asserts counts
      are identical. This is what a DLQ drain or a Spark restart actually does,
      so it is worth asserting at batch granularity rather than trusting that
      one row's behaviour generalises.

Run inside the producer container:
    docker exec rtdp-producer python -m pytest tests/test_idempotency.py -v
"""
from __future__ import annotations

import os
import sys
import time
import uuid

import pytest

sys.path.insert(0, "/opt/app")

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

from common.config import CassandraConfig
from producer.events import TripGenerator, duplicate_of
from streaming.cassandra_sink import ALL_TABLES, CQL, build_statements

TABLES = ["trips_by_id", "trips_by_driver_day", "trips_by_city_hour"]


@pytest.fixture(scope="module")
def session():
    cfg = CassandraConfig()
    cluster = Cluster(cfg.hosts)
    sess = cluster.connect(cfg.keyspace)
    yield sess
    cluster.shutdown()


@pytest.fixture(scope="module")
def prepared(session):
    cfg = CassandraConfig()
    out = {}
    for name in TABLES:
        st = session.prepare(CQL[name].format(ks=cfg.keyspace))
        st.consistency_level = getattr(ConsistencyLevel, cfg.write_consistency)
        out[name] = st
    return out


def counts(session) -> dict:
    """Count at consistency ALL.

    The driver's default read consistency is LOCAL_ONE. On a ring that has
    recently had a node bounce, two LOCAL_ONE counts taken seconds apart can
    legitimately differ while hinted handoff catches up - which shows up as
    phantom rows and makes this test flap for a reason that has nothing to do
    with idempotency. ALL forces every replica to agree before answering.
    """
    from cassandra.query import SimpleStatement
    out = {}
    for t in TABLES:
        stmt = SimpleStatement(f"SELECT COUNT(*) AS c FROM {t}",
                               consistency_level=ConsistencyLevel.ALL,
                               fetch_size=None)
        out[t] = session.execute(stmt, timeout=120).one().c
    return out


def write_event(session, prepared, event, ingested_at_ms=None):
    ingested_at_ms = ingested_at_ms or int(time.time() * 1000)
    for key, params in build_statements(event, ALL_TABLES, ingested_at_ms):
        session.execute(prepared[key], params)


def _materialize(event: dict) -> dict:
    """Convert the producer's ISO event_time into the datetime the sink expects.

    The streaming path gets this conversion from Spark's schema; a direct write
    has to do it here.
    """
    from datetime import datetime
    ev = dict(event)
    ev["event_time"] = datetime.fromisoformat(
        ev["event_time"].replace("Z", "+00:00"))
    return ev


def test_schema_upsert_is_idempotent(session, prepared):
    gen = TripGenerator(seed=1234)
    wire_event = gen.next_event()

    write_event(session, prepared, _materialize(wire_event))
    time.sleep(0.3)
    before = counts(session)

    # Byte-identical replay, taken through `duplicate_of` so this test exercises
    # the same copy the producer emits rather than a hand-rolled equivalent.
    write_event(session, prepared, _materialize(duplicate_of(wire_event)),
                ingested_at_ms=int(time.time() * 1000) + 5000)
    time.sleep(0.3)
    after = counts(session)

    assert before == after, (
        f"replaying one event changed row counts: {before} -> {after}. "
        f"A primary key is not fully derived from the event.")


def test_replay_of_a_full_batch_is_idempotent(session, prepared):
    gen = TripGenerator(seed=98765)
    batch = [_materialize(gen.next_event()) for _ in range(200)]

    for ev in batch:
        write_event(session, prepared, ev)
    time.sleep(0.5)
    before = counts(session)

    # Replay the entire batch, with a DIFFERENT ingestion timestamp so the
    # only thing held constant is the event itself. If ingested_at_ms leaked
    # into any primary key this would create a second copy of every row.
    for ev in batch:
        write_event(session, prepared, ev, ingested_at_ms=int(time.time() * 1000) + 999)
    time.sleep(0.5)
    after = counts(session)

    assert before == after, (
        f"replaying a 200-event batch changed row counts: {before} -> {after}")


def test_distinct_event_produces_new_rows(session, prepared):
    """Control case.

    If the two tests above passed because writes were silently failing, this
    would pass too - so assert that a genuinely NEW event does increase counts.
    Without this, the idempotency tests prove nothing.
    """
    gen = TripGenerator(seed=555)
    before = counts(session)
    write_event(session, prepared, _materialize(gen.next_event()))
    time.sleep(0.3)
    after = counts(session)

    for table in TABLES:
        assert after[table] == before[table] + 1, (
            f"{table}: a new event should add exactly one row "
            f"({before[table]} -> {after[table]})")
