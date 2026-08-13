"""Typed access to the topology constants in .env.

Everything that appears in the README as a number is read from here, so a
reviewer can grep one file to see what the running system was configured with.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _env(key: str, default: str | None = None) -> str:
    val = os.environ.get(key, default)
    if val is None:
        raise KeyError(f"required environment variable {key!r} is not set")
    return val


def _int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


def _float(key: str, default: float) -> float:
    return float(os.environ.get(key, default))


def _ms_list(raw: str) -> List[int]:
    """Parse '1000,2000,4000' -> [1000, 2000, 4000]. Empty string -> []."""
    raw = (raw or "").strip()
    if not raw:
        return []
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap: str = field(default_factory=lambda: os.environ.get(
        "KAFKA_BOOTSTRAP", _env("KAFKA_BOOTSTRAP_INTERNAL", "kafka1:9092,kafka2:9092,kafka3:9092")))
    topic_trips: str = field(default_factory=lambda: _env("TOPIC_TRIPS", "trips.raw"))
    topic_dlq: str = field(default_factory=lambda: _env("TOPIC_DLQ", "trips.dlq"))
    partitions: int = field(default_factory=lambda: _int("TOPIC_PARTITIONS", 6))
    replication: int = field(default_factory=lambda: _int("TOPIC_REPLICATION", 3))
    min_isr: int = field(default_factory=lambda: _int("TOPIC_MIN_ISR", 2))
    consumer_group: str = field(default_factory=lambda: _env("CONSUMER_GROUP", "rtdp-spark-trips"))


@dataclass(frozen=True)
class CassandraConfig:
    hosts: List[str] = field(default_factory=lambda: _env(
        "CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3").split(","))
    keyspace: str = field(default_factory=lambda: _env("CASSANDRA_KEYSPACE", "rtdp"))
    replication_factor: int = field(default_factory=lambda: _int("CASSANDRA_REPLICATION_FACTOR", 3))
    write_consistency: str = field(default_factory=lambda: _env("CASSANDRA_WRITE_CONSISTENCY", "QUORUM"))
    read_consistency: str = field(default_factory=lambda: _env("CASSANDRA_READ_CONSISTENCY", "QUORUM"))


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "postgres"))
    port: int = field(default_factory=lambda: _int("POSTGRES_PORT", 5432))
    user: str = field(default_factory=lambda: _env("POSTGRES_USER", "rtdp"))
    password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "rtdp"))
    database: str = field(default_factory=lambda: _env("POSTGRES_DB", "rtdp"))

    def dsn(self) -> str:
        return (f"host={self.host} port={self.port} user={self.user} "
                f"password={self.password} dbname={self.database}")


@dataclass(frozen=True)
class RetryConfig:
    """The Section 6 reliability knob.

    `backoff_schedule_ms` empty means backoff is DISABLED - attempts are retried
    immediately with no wait. That is the control arm of the Section 7.3 A/B.
    """
    backoff_schedule_ms: List[int] = field(default_factory=lambda: _ms_list(
        os.environ.get("RETRY_BACKOFF_SCHEDULE_MS", "1000,2000,4000")))
    max_attempts: int = field(default_factory=lambda: _int("RETRY_MAX_ATTEMPTS", 4))
    jitter_ratio: float = field(default_factory=lambda: _float("RETRY_JITTER_RATIO", 0.1))

    @property
    def enabled(self) -> bool:
        return bool(self.backoff_schedule_ms)

    def describe(self) -> str:
        if not self.backoff_schedule_ms:
            return f"backoff=DISABLED max_attempts={self.max_attempts}"
        sched = " -> ".join(f"{ms/1000:g}s" for ms in self.backoff_schedule_ms)
        return (f"backoff={sched} max_attempts={self.max_attempts} "
                f"jitter=+/-{self.jitter_ratio:.0%}")


@dataclass(frozen=True)
class StreamConfig:
    master: str = field(default_factory=lambda: _env("SPARK_MASTER_URL", "spark://spark-master:7077"))
    checkpoint_dir: str = field(default_factory=lambda: _env("SPARK_CHECKPOINT_DIR", "/opt/checkpoints/trips"))
    max_offsets_per_trigger: int = field(default_factory=lambda: _int("SPARK_MAX_OFFSETS_PER_TRIGGER", 200000))
    watermark_delay: str = field(default_factory=lambda: _env("WATERMARK_DELAY", "2 minutes"))
    late_event_policy: str = field(default_factory=lambda: _env("LATE_EVENT_POLICY", "dlq"))
    latency_sample_rate: float = field(default_factory=lambda: _float("LATENCY_SAMPLE_RATE", 0.05))
    run_id: str = field(default_factory=lambda: os.environ.get("RUN_ID", "adhoc"))


KAFKA = KafkaConfig
CASSANDRA = CassandraConfig
POSTGRES = PostgresConfig
RETRY = RetryConfig
STREAM = StreamConfig
