"""Scenario runner - the single instrument behind every measured number.

Runs on the HOST (it needs `docker` to inject faults) and coordinates one
complete measurement:

    truncate -> start stream -> wait for it to be listening -> produce at a
    target rate -> optionally break something -> wait for the backlog to drain
    -> stop -> reconcile what went in against what came out

The reconciliation at the end is the part that matters. Every event acked by
Kafka must be accounted for as either:

    written to Cassandra   (distinct trip_id rows)
  + quarantined in the DLQ (replayable, not lost)
  + still in flight        (should be 0 after drain)
  ------------------------------------------------
  = acked by Kafka

Anything left over is silent loss, and the report names it as such rather than
rounding it into a percentage.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(REPO, "results", "raw")

KAFKA_CONTAINERS = ["rtdp-kafka1", "rtdp-kafka2", "rtdp-kafka3"]
CASSANDRA_CONTAINERS = ["rtdp-cassandra1", "rtdp-cassandra2", "rtdp-cassandra3"]
WORKER_CONTAINERS = ["rtdp-spark-worker-1", "rtdp-spark-worker-2"]

TRIP_TABLES = ["trips_by_id", "trips_by_driver_day", "trips_by_city_hour",
               "latency_samples", "city_minute_rollup"]


# ---------------------------------------------------------------------------
# shell helpers
# ---------------------------------------------------------------------------
def sh(cmd: List[str], timeout: int = 120, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout, check=check)


def dexec(container: str, cmd: List[str], timeout: int = 120,
          env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
    full = ["docker", "exec"]
    for k, v in (env or {}).items():
        full += ["-e", f"{k}={v}"]
    full += [container] + cmd
    return sh(full, timeout=timeout)


def cqlsh(query: str, timeout: int = 300) -> str:
    r = dexec("rtdp-cassandra1",
              ["cqlsh", "--request-timeout", str(timeout), "-e", query],
              timeout=timeout + 30)
    if r.returncode != 0:
        raise RuntimeError(f"cqlsh failed: {r.stderr.strip()[:400]}")
    return r.stdout


def topic_end_offsets(topic: str) -> Dict[int, int]:
    """Sum of latest offsets per partition - the topic's total written count."""
    # kafka-get-offsets.sh, not the old kafka.tools.GetOffsetShell class -
    # that class was removed in Kafka 3.x and calling it fails silently enough
    # to be read as "the topic is empty", which is exactly the wrong answer for
    # a loss reconciliation.
    r = dexec("rtdp-kafka1", [
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "kafka1:9092", "--topic", topic, "--time", "-1"])
    if r.returncode != 0:
        raise RuntimeError(f"kafka-get-offsets failed: {r.stderr.strip()[:300]}")
    out: Dict[int, int] = {}
    for line in r.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == topic:
            out[int(parts[1])] = int(parts[2])
    return out


def topic_count(topic: str) -> int:
    return sum(topic_end_offsets(topic).values())


def purge_topic(topic: str) -> Dict[int, int]:
    """Logically empty a topic by advancing its low watermark to the end.

    Runs must be independent. Without this, a DLQ left over from the previous
    scenario is counted as this scenario's quarantine volume and every loss
    number inherits the last run's failures.
    """
    ends = topic_end_offsets(topic)
    spec = {"partitions": [{"topic": topic, "partition": p, "offset": o}
                           for p, o in ends.items()]}
    dexec("rtdp-kafka1", ["bash", "-lc",
          f"cat > /tmp/purge.json <<'EOF'\n{json.dumps(spec)}\nEOF"])
    dexec("rtdp-kafka1", [
        "/opt/kafka/bin/kafka-delete-records.sh",
        "--bootstrap-server", "kafka1:9092",
        "--offset-json-file", "/tmp/purge.json"], timeout=120)
    return ends


def dlq_distinct_events() -> Dict[str, Any]:
    """Count DISTINCT quarantined events, not DLQ messages.

    The producer deliberately emits duplicates, so a malformed event can be
    quarantined more than once. Reconciling messages against distinct events
    would show phantom over-accounting.
    """
    r = dexec("rtdp-producer",
              ["python", "-m", "replay.dlq_tools", "inspect", "--idle-timeout", "4"],
              timeout=180)
    if r.returncode != 0:
        return {"records": -1, "distinct_event_ids": -1,
                "error": r.stderr.strip()[:300]}
    try:
        return json.loads(r.stdout[r.stdout.index("{"):])
    except (ValueError, json.JSONDecodeError) as exc:
        return {"records": -1, "distinct_event_ids": -1, "error": str(exc)}


# ---------------------------------------------------------------------------
# scenario
# ---------------------------------------------------------------------------
class Scenario:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_id = args.run_id
        os.makedirs(RESULTS, exist_ok=True)
        self.p_progress = os.path.join(RESULTS, f"{self.run_id}_batches.ndjson")
        self.p_lag = os.path.join(RESULTS, f"{self.run_id}_lag.ndjson")
        self.p_ready = os.path.join(RESULTS, f"{self.run_id}.ready")
        self.p_stop = os.path.join(RESULTS, f"{self.run_id}.stop")
        self.p_stream = os.path.join(RESULTS, f"{self.run_id}_stream.json")
        self.p_producer = os.path.join(RESULTS, f"{self.run_id}_producer.json")
        self.p_report = os.path.join(RESULTS, f"{self.run_id}_scenario.json")
        self.stream_log = os.path.join(RESULTS, f"{self.run_id}_stream.log")
        self.stream_proc: Optional[subprocess.Popen] = None
        self.events: List[Dict[str, Any]] = []
        self.dlq_before = 0
        self.raw_before = 0
        self.trips_before = 0

    def log(self, msg: str) -> None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

    def mark(self, name: str, **extra: Any) -> Dict[str, Any]:
        ev = {"event": name, "ts": time.time(), **extra}
        self.events.append(ev)
        return ev

    # -- setup ---------------------------------------------------------------
    def truncate(self) -> None:
        self.log("truncating Cassandra tables")
        for t in TRIP_TABLES:
            try:
                cqlsh(f"TRUNCATE rtdp.{t};")
            except RuntimeError as exc:
                self.log(f"  truncate {t}: {exc}")
        self.log("purging Kafka topics")
        for t in ("trips.raw", "trips.dlq"):
            try:
                purge_topic(t)
            except Exception as exc:
                self.log(f"  purge {t}: {exc}")

    def clean_checkpoint(self) -> None:
        """Remove the checkpoint so a run starts from a known offset state.

        Only for fresh measurement runs. The executor-kill scenario deliberately
        does NOT do this between kill and recovery - the whole point there is
        that the checkpoint survives.
        """
        self.log("clearing checkpoint")
        dexec("rtdp-spark-driver", ["bash", "-lc",
              "rm -rf /opt/checkpoints/* || true"])

    def health_gate(self) -> Dict[str, Any]:
        state: Dict[str, Any] = {}
        r = sh(["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"])
        running = {}
        for line in r.stdout.splitlines():
            if "\t" in line:
                n, s = line.split("\t", 1)
                running[n] = s
        state["containers"] = running
        missing = [c for c in KAFKA_CONTAINERS + CASSANDRA_CONTAINERS
                   if c not in running]
        if missing:
            raise RuntimeError(f"required containers not running: {missing}")
        nodes = cqlsh("SELECT peer FROM system.peers;")
        state["cassandra_peers"] = nodes.count("\n")
        return state

    # -- stream --------------------------------------------------------------
    def start_stream(self) -> None:
        a = self.args
        for p in (self.p_progress, self.p_lag, self.p_ready, self.p_stop):
            if os.path.exists(p):
                os.remove(p)

        cmd = ["docker", "exec",
               "-e", f"RUN_ID={self.run_id}",
               "-e", f"RETRY_BACKOFF_SCHEDULE_MS={a.backoff}",
               "-e", f"RETRY_MAX_ATTEMPTS={a.max_attempts}",
               "-e", f"LATE_EVENT_POLICY={a.late_policy}",
               "-e", f"WATERMARK_DELAY={a.watermark}",
               "rtdp-spark-driver", "bash", "/opt/app/scripts/submit-stream.sh",
               "--run-id", self.run_id,
               "--duration", "0",
               "--tables", a.tables,
               "--latency-sample-rate", str(a.latency_sample_rate),
               "--total-cores", str(a.total_cores),
               "--executor-cores", str(a.executor_cores),
               "--executor-memory", a.executor_memory,
               "--max-offsets-per-trigger", str(a.max_offsets_per_trigger),
               "--write-concurrency", str(a.write_concurrency),
               "--summary-file", f"/opt/app/results/raw/{self.run_id}_stream.json",
               "--progress-file", f"/opt/app/results/raw/{self.run_id}_batches.ndjson",
               "--lag-file", f"/opt/app/results/raw/{self.run_id}_lag.ndjson",
               "--ready-file", f"/opt/app/results/raw/{self.run_id}.ready",
               "--stop-file", f"/opt/app/results/raw/{self.run_id}.stop"]
        if a.trigger_interval:
            cmd += ["--trigger-interval", a.trigger_interval]
        if a.with_rollup:
            cmd += ["--with-rollup"]

        self.log(f"starting stream (backoff={a.backoff or 'DISABLED'})")
        logfh = open(self.stream_log, "w")
        self.stream_proc = subprocess.Popen(cmd, stdout=logfh, stderr=subprocess.STDOUT)
        self.mark("stream_start")

    def wait_ready(self, timeout: int = 240) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if os.path.exists(self.p_ready):
                self.log("stream is listening")
                self.mark("stream_ready")
                # The readiness file is written when the query starts; give the
                # Kafka source one trigger to actually claim its offsets before
                # the producer opens the tap.
                time.sleep(self.args.ready_grace)
                return
            if self.stream_proc and self.stream_proc.poll() is not None:
                raise RuntimeError(
                    f"stream exited early (code {self.stream_proc.returncode}); "
                    f"see {self.stream_log}")
            time.sleep(1)
        raise RuntimeError(f"stream not ready after {timeout}s; see {self.stream_log}")

    def stop_stream(self) -> None:
        if not self.stream_proc:
            return
        self.log("stopping stream")
        self.mark("stream_stop")
        # SIGTERM reaches the docker exec client; the job's handler stops the
        # query gracefully so the final summary is written.
        # Touch the stop file the job polls for; it then stops the query
        # gracefully and writes its summary before exiting.
        with open(self.p_stop, "w") as fh:
            fh.write(str(time.time()))
        try:
            self.stream_proc.wait(timeout=180)
        except subprocess.TimeoutExpired:
            self.log("stream did not stop in 180s, killing")
            dexec("rtdp-spark-driver", ["pkill", "-f", "spark-submit"], timeout=30)
            try:
                self.stream_proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.stream_proc.kill()

    # -- producer ------------------------------------------------------------
    def run_producer(self) -> None:
        a = self.args
        cmd = ["docker", "exec",
               "-e", f"RUN_ID={self.run_id}",
               "-e", f"RETRY_BACKOFF_SCHEDULE_MS={a.backoff}",
               "-e", f"RETRY_MAX_ATTEMPTS={a.max_attempts}",
               "rtdp-producer", "python", "-m", "producer.produce",
               "--rate", str(a.rate),
               "--duration", str(a.duration),
               "--run-id", self.run_id,
               "--late-ratio", str(a.late_ratio),
               "--malformed-ratio", str(a.malformed_ratio),
               "--duplicate-ratio", str(a.duplicate_ratio),
               "--report", f"/opt/app/results/raw/{self.run_id}_producer.json"]
        self.log(f"producing at target {a.rate}/s for {a.duration}s")
        self.mark("producer_start", target_rate=a.rate)
        r = sh(cmd, timeout=int(a.duration) + 300)
        self.mark("producer_stop")
        if r.returncode not in (0, 3):
            self.log(f"producer exited {r.returncode}: {r.stderr[-500:]}")

    # -- drain ---------------------------------------------------------------
    def wait_for_drain(self, timeout: int = 300) -> Dict[str, Any]:
        """Wait until the stream has consumed every message the producer wrote.

        The drain condition is a DIRECT comparison of Kafka's end offsets
        against the row count the stream reports having read - not Spark's
        reported lag.

        Why not lag: `query.lastProgress` describes the last COMPLETED batch.
        When the producer stops, the most recent progress can still be several
        seconds old and report zero lag from before the final burst. Polling it
        five times just reads the same stale zero five times, and the stream
        gets stopped with messages still unread. That mistake silently
        manufactured ~900 events of phantom "loss" on the first run of this
        harness; the loss was the measurement, not the pipeline.
        """
        target = max(0, topic_count("trips.raw") - self.raw_before)
        self.log(f"waiting for backlog to drain ({target} messages to consume)")
        deadline = time.time() + timeout
        t0 = time.time()
        stable = 0
        consumed = 0
        while time.time() < deadline:
            consumed = sum(b["input_rows"] for b in self._read_batches())
            if consumed >= target:
                stable += 1
                # A couple of extra polls so the final batch's Cassandra writes
                # land before the reconciliation counts rows.
                if stable >= self.args.drain_confirm_polls:
                    secs = round(time.time() - t0, 2)
                    self.log(f"drained in {secs}s ({consumed}/{target} consumed)")
                    self.mark("drained", seconds=secs)
                    return {"drained": True, "drain_seconds": secs,
                            "messages_to_consume": target,
                            "messages_consumed": consumed,
                            "final_lag": self._latest_lag()}
            else:
                stable = 0
            time.sleep(1)
        self.log(f"drain TIMEOUT after {timeout}s ({consumed}/{target} consumed)")
        self.mark("drain_timeout", consumed=consumed, target=target)
        return {"drained": False, "drain_seconds": timeout,
                "messages_to_consume": target, "messages_consumed": consumed,
                "final_lag": self._latest_lag()}

    def _latest_lag(self) -> Optional[int]:
        try:
            with open(self.p_lag) as fh:
                lines = fh.readlines()
            for line in reversed(lines):
                line = line.strip()
                if line:
                    return int(json.loads(line).get("max_offsets_behind", 0))
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            return None
        return None

    # -- chaos ---------------------------------------------------------------
    def inject_fault(self) -> Optional[Dict[str, Any]]:
        f = self.args.fault
        if f == "none":
            return None
        time.sleep(self.args.fault_at)
        target, action = None, f

        if f == "kill_broker":
            target = self.args.fault_target or "rtdp-kafka2"
        elif f == "kill_executor":
            target = self.args.fault_target or "rtdp-spark-worker-2"
        elif f == "kill_cassandra":
            target = self.args.fault_target or "rtdp-cassandra3"
        elif f == "kill_cassandra_quorum":
            # TWO nodes. With RF=3 and CL=QUORUM, killing one node changes
            # nothing observable - quorum is still 2 of 3, which is the point
            # of the single-node test. To force writes to actually FAIL and
            # exercise the retry/DLQ path, quorum has to become unreachable.
            target = self.args.fault_target or "rtdp-cassandra2,rtdp-cassandra3"
        else:
            raise ValueError(f"unknown fault {f}")

        targets = [t.strip() for t in target.split(",") if t.strip()]
        self.log(f"INJECTING FAULT: kill {targets}")
        pre_lag = self._latest_lag()
        t_kill = time.time()
        # SIGKILL, not `docker stop`. A graceful shutdown lets Kafka hand off
        # leadership cleanly, which tests an orderly restart rather than the
        # crash this is supposed to simulate.
        for t in targets:
            sh(["docker", "kill", "--signal=KILL", t], timeout=60)
        self.mark("fault_injected", target=target, targets=targets,
                  action=action, pre_lag=pre_lag)

        recovery = self._await_recovery(t_kill)

        if self.args.restart_after > 0:
            time.sleep(self.args.restart_after)
            self.log(f"restarting {targets}")
            for t in targets:
                sh(["docker", "start", t], timeout=120)
                self.mark("fault_target_restarted", target=t)
            restored = max((self._await_container_health(t) or 0) for t in targets)
            recovery["target_healthy_seconds"] = restored

        return {"fault": f, "target": target, "targets": targets,
                "killed_at": t_kill, "dead_for_sec": self.args.restart_after,
                **recovery}

    def _await_recovery(self, t_kill: float, timeout: int = 240) -> Dict[str, Any]:
        """First successful non-empty batch after the kill = pipeline recovered.

        This is the honest definition: not "the container came back" and not
        "no error was logged", but "the pipeline demonstrably processed data
        again". Lag recovery is reported separately because a pipeline can
        resume processing while still carrying a backlog.
        """
        deadline = time.time() + timeout
        first_batch_after = None
        while time.time() < deadline:
            for rec in self._read_batches():
                if rec["wall_ts"] > t_kill and rec["input_rows"] > 0 \
                        and rec.get("rows_written", 0) > 0:
                    first_batch_after = rec
                    break
            if first_batch_after:
                secs = first_batch_after["wall_ts"] - t_kill
                self.log(f"pipeline processing again after {secs:.2f}s")
                self.mark("recovered", seconds=round(secs, 2))
                return {
                    "recovery_seconds": round(secs, 3),
                    "recovery_batch_id": first_batch_after["batch_id"],
                    "recovered": True,
                }
            time.sleep(0.5)
        self.log("NO RECOVERY within timeout")
        self.mark("recovery_timeout")
        return {"recovery_seconds": None, "recovered": False}

    def _await_container_health(self, name: str, timeout: int = 300) -> Optional[float]:
        t0 = time.time()
        while time.time() - t0 < timeout:
            r = sh(["docker", "inspect", "-f",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                    name], timeout=30)
            if r.stdout.strip() in ("healthy", "running"):
                return round(time.time() - t0, 2)
            time.sleep(2)
        return None

    def _read_batches(self) -> List[Dict[str, Any]]:
        out = []
        try:
            with open(self.p_progress) as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            out.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
        except FileNotFoundError:
            pass
        return out

    # -- reconciliation ------------------------------------------------------
    def count_cassandra(self) -> Dict[str, int]:
        counts = {}
        for t in ("trips_by_id", "trips_by_driver_day", "trips_by_city_hour"):
            try:
                # CONSISTENCY ALL, not cqlsh's default ONE. A count taken at ONE
                # on a ring that recently bounced a node can disagree with
                # itself between tables while hinted handoff catches up, and
                # that disagreement reads as loss in the reconciliation. This
                # bug produced a fake 400-row gap between two tables holding
                # identical data before it was caught.
                out = cqlsh(f"CONSISTENCY ALL; SELECT COUNT(*) FROM rtdp.{t};",
                            timeout=600)
                num = 0
                for line in out.splitlines():
                    s = line.strip()
                    if s.isdigit():
                        num = int(s)
                        break
                counts[t] = num
            except RuntimeError as exc:
                self.log(f"count {t} failed: {exc}")
                counts[t] = -1
        return counts

    def build_report(self, fault: Optional[Dict[str, Any]],
                     drain: Dict[str, Any], health: Dict[str, Any]) -> Dict[str, Any]:
        producer = _load_json(self.p_producer) or {}
        stream = _load_json(self.p_stream) or {}
        batches = self._read_batches()
        lag = _read_ndjson(self.p_lag)

        dlq_msgs = max(0, topic_count("trips.dlq") - self.dlq_before)
        dlq_info = dlq_distinct_events()
        dlq_run = dlq_info.get("distinct_event_ids", 0)
        if dlq_run < 0:
            dlq_run = dlq_msgs   # inspection failed; fall back to raw count
        cass = self.count_cassandra()

        # AUTHORITATIVE BASE: what Kafka actually persisted, from its own end
        # offsets - not the producer's `acked` counter.
        #
        # `acked` counts delivery CALLBACKS that fired client-side. When a
        # broker is killed mid-run, some callbacks have not fired by the time
        # flush() times out even though Kafka durably holds the message. Using
        # `acked` as the denominator then makes the pipeline look like it
        # invented rows: a chaos run reported -46 "silent loss" (more rows in
        # Cassandra than the producer thought it sent) before this was fixed.
        kafka_messages = max(0, topic_count("trips.raw") - self.raw_before)
        acked = producer.get("acked", 0)
        written_distinct = cass.get("trips_by_id", 0) - self.trips_before
        # Duplicates are emitted deliberately and MUST collapse via the primary
        # key, so the denominator is distinct events, not messages sent.
        dupes = producer.get("duplicates_emitted", 0)
        expected_distinct = kafka_messages - dupes
        callback_shortfall = kafka_messages - acked

        accounted = written_distinct + dlq_run
        unaccounted = expected_distinct - accounted

        lag_series = [s.get("max_offsets_behind", 0) for s in lag]
        monotonic_growth = _longest_monotonic_run(lag_series)

        report = {
            "run_id": self.run_id,
            "scenario": self.args.scenario,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "config": {
                "target_rate": self.args.rate,
                "duration_sec": self.args.duration,
                "backoff": self.args.backoff or "DISABLED",
                "max_attempts": self.args.max_attempts,
                "tables": self.args.tables,
                "partitions": int(os.environ.get("TOPIC_PARTITIONS", 6)),
                "total_cores": self.args.total_cores,
                "executor_cores": self.args.executor_cores,
                "executor_memory": self.args.executor_memory,
                "consistency": os.environ.get("CASSANDRA_WRITE_CONSISTENCY", "QUORUM"),
                "malformed_ratio": self.args.malformed_ratio,
                "late_ratio": self.args.late_ratio,
                "duplicate_ratio": self.args.duplicate_ratio,
                "fault": self.args.fault,
            },
            "producer": producer,
            "stream": stream,
            "cassandra_counts": cass,
            "dlq_detail": dlq_info,
            "reconciliation": {
                "kafka_messages_persisted": kafka_messages,
                "producer_acked_callbacks": acked,
                "callback_shortfall": callback_shortfall,
                "kafka_acked": acked,
                "duplicates_emitted": dupes,
                "expected_distinct_events": expected_distinct,
                "written_distinct_trips": written_distinct,
                "dlq_messages": dlq_msgs,
                "dlq_distinct_events": dlq_run,
                "accounted_for": accounted,
                "unaccounted_silent_loss": unaccounted,
                "silent_loss_pct": (round(100 * unaccounted / expected_distinct, 4)
                                    if expected_distinct else None),
                "note": ("unaccounted = expected_distinct - (written + dlq), "
                         "where expected_distinct comes from Kafka's end "
                         "offsets. A positive value is real, silent loss. "
                         "callback_shortfall > 0 just means some producer "
                         "delivery callbacks had not fired at flush time; "
                         "those messages were still persisted by Kafka."),
            },
            "lag": {
                "samples": len(lag),
                "max_observed": max(lag_series) if lag_series else 0,
                "final": lag_series[-1] if lag_series else None,
                "longest_monotonic_growth_run": monotonic_growth,
                "drained": drain.get("drained"),
                "drain_seconds": drain.get("drain_seconds"),
                "messages_to_consume": drain.get("messages_to_consume"),
                "messages_consumed": drain.get("messages_consumed"),
            },
            "batches": {
                "count": len(batches),
                "non_empty": sum(1 for b in batches if b["input_rows"] > 0),
            },
            "fault": fault,
            "health_gate": health,
        }
        return report

    # -- main ----------------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        health = self.health_gate()
        if self.args.truncate:
            self.truncate()
        if self.args.clean_checkpoint:
            self.clean_checkpoint()

        self.dlq_before = topic_count("trips.dlq")
        self.raw_before = topic_count("trips.raw")
        self.trips_before = 0 if self.args.truncate else self.count_cassandra().get("trips_by_id", 0)

        self.start_stream()
        try:
            self.wait_ready()

            fault_result: Optional[Dict[str, Any]] = None
            if self.args.fault != "none":
                import threading
                holder: Dict[str, Any] = {}

                def _chaos():
                    try:
                        holder["r"] = self.inject_fault()
                    except Exception as exc:
                        holder["error"] = str(exc)
                t = threading.Thread(target=_chaos, daemon=True)
                t.start()
                self.run_producer()
                t.join(timeout=300)
                fault_result = holder.get("r") or ({"error": holder["error"]}
                                                   if "error" in holder else None)
            else:
                self.run_producer()

            drain = self.wait_for_drain(timeout=self.args.drain_timeout)
        finally:
            self.stop_stream()

        report = self.build_report(fault_result, drain, health)
        with open(self.p_report, "w") as fh:
            json.dump(report, fh, indent=2)
        self.log(f"report -> {self.p_report}")
        _print_summary(report)
        return report


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path) as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _read_ndjson(path: str) -> List[Dict[str, Any]]:
    out = []
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except FileNotFoundError:
        pass
    return out


def _longest_monotonic_run(series: List[int]) -> int:
    """Longest strictly-increasing run - the Section 7.4 unhealthy signal.

    One high lag reading means a burst. Lag that increases across N consecutive
    intervals means the consumer is losing to the producer and will never catch
    up on its own. That distinction is the whole point of alerting on lag.
    """
    best = run = 0
    for i in range(1, len(series)):
        if series[i] > series[i - 1]:
            run += 1
            best = max(best, run)
        else:
            run = 0
    return best


def _print_summary(r: Dict[str, Any]) -> None:
    rec = r["reconciliation"]
    print("\n" + "=" * 68)
    print(f"  {r['run_id']}  ({r['scenario']})")
    print("=" * 68)
    p = r.get("producer") or {}
    s = r.get("stream") or {}
    print(f"  target rate           {r['config']['target_rate']}/s")
    print(f"  achieved rate         {p.get('achieved_rate')}/s "
          f"({p.get('rate_attainment_pct')}% of target)")
    print(f"  kafka persisted       {rec['kafka_messages_persisted']} "
          f"(producer callbacks: {rec['producer_acked_callbacks']})")
    print(f"  expected distinct     {rec['expected_distinct_events']}")
    print(f"  written to Cassandra  {rec['written_distinct_trips']}")
    print(f"  DLQ (replayable)      {rec['dlq_distinct_events']} distinct "
          f"({rec['dlq_messages']} messages)")
    print(f"  SILENT LOSS           {rec['unaccounted_silent_loss']} "
          f"({rec['silent_loss_pct']}%)")
    print(f"  lag max / final       {r['lag']['max_observed']} / {r['lag']['final']}")
    print(f"  drained               {r['lag']['drained']} in {r['lag']['drain_seconds']}s")
    print(f"  avg retries/success   {s.get('avg_retries_to_success_all_writes')}")
    print(f"  writes needing retry  {s.get('writes_needing_retry')} "
          f"(avg {s.get('avg_retries_among_retried_writes')} retries)")
    if r.get("fault"):
        f = r["fault"]
        print(f"  fault                 {f.get('fault')} -> {f.get('target')}")
        print(f"  recovery              {f.get('recovery_seconds')}s")
    print("=" * 68 + "\n")


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Pipeline measurement scenario runner")
    p.add_argument("--run-id", required=True)
    p.add_argument("--scenario", default="adhoc")
    p.add_argument("--rate", type=float, default=1000)
    p.add_argument("--duration", type=float, default=120)
    p.add_argument("--tables", choices=["all", "core"], default="all")
    p.add_argument("--backoff", default="1000,2000,4000",
                   help="empty string disables backoff (A/B control arm)")
    p.add_argument("--max-attempts", type=int, default=4)
    p.add_argument("--late-policy", default="dlq")
    p.add_argument("--watermark", default="2 minutes")
    p.add_argument("--late-ratio", type=float, default=0.0)
    p.add_argument("--malformed-ratio", type=float, default=0.0)
    p.add_argument("--duplicate-ratio", type=float, default=0.0)
    p.add_argument("--latency-sample-rate", type=float, default=0.05)
    p.add_argument("--total-cores", type=int, default=6)
    p.add_argument("--executor-cores", type=int, default=3)
    p.add_argument("--executor-memory", default="1g")
    p.add_argument("--max-offsets-per-trigger", type=int, default=200000)
    p.add_argument("--write-concurrency", type=int, default=48)
    p.add_argument("--trigger-interval", default="")
    p.add_argument("--with-rollup", action="store_true")
    p.add_argument("--fault", default="none",
                   choices=["none", "kill_broker", "kill_executor",
                            "kill_cassandra", "kill_cassandra_quorum"])
    p.add_argument("--fault-at", type=float, default=30,
                   help="seconds after producer start to inject the fault")
    p.add_argument("--fault-target", default=None)
    p.add_argument("--restart-after", type=float, default=30,
                   help="seconds to leave the target dead; 0 = never restart")
    p.add_argument("--drain-timeout", type=int, default=300)
    p.add_argument("--drain-confirm-polls", type=int, default=5)
    p.add_argument("--ready-grace", type=float, default=8)
    p.add_argument("--truncate", action="store_true", default=True)
    p.add_argument("--no-truncate", dest="truncate", action="store_false")
    p.add_argument("--clean-checkpoint", action="store_true", default=True)
    p.add_argument("--keep-checkpoint", dest="clean_checkpoint", action="store_false")
    args = p.parse_args(argv)

    try:
        Scenario(args).run()
    except Exception as exc:
        print(f"SCENARIO FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
