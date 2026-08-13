"""Section 7.3: induced failure, recovery, and the backoff A/B.

Two distinct experiments live here.

FAILURE SCENARIOS - what breaks, and how fast does it come back?
    kill_broker      SIGKILL one of three Kafka brokers mid-stream.
    kill_executor    SIGKILL a Spark worker mid-batch. The driver runs in its
                     own container, so this removes an executor without
                     restarting the job - which is what makes it a test of
                     checkpoint recovery rather than of process supervision.
    kill_cassandra   SIGKILL one of three Cassandra nodes. With RF=3 and
                     CL=QUORUM this should be a non-event: quorum is still
                     2 of 3. The measurement is whether writes actually
                     continue, not whether the node dies.

    Each is repeated `--repeats` times, because a single run is an anecdote.
    Reported per scenario: recovery time, events lost, events quarantined.

BACKOFF A/B - does the backoff schedule actually buy anything?
    Identical event volume, identical injected fault, identical timing. The
    ONLY difference between arms is RETRY_BACKOFF_SCHEDULE_MS: "1000,2000,4000"
    versus "" (retry immediately, no spacing).

    The fault is kill_cassandra_quorum - TWO nodes - because that is what it
    takes to make QUORUM writes actually fail. Killing one node would produce
    two identical arms and a meaningless comparison.

    WHAT THE COMPARISON MEASURES, precisely: the reduction in events forced
    into QUARANTINE, not a reduction in permanent loss. Permanent loss is zero
    in both arms - that is what the DLQ is for. An honest headline is
    "backoff cut quarantined events by X%", never "backoff cut data loss by X%",
    because no data was lost in either arm.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(REPO, "results", "raw")


def run_scenario(run_id: str, extra: List[str], args: argparse.Namespace) -> Dict[str, Any]:
    cmd = [
        sys.executable, os.path.join(REPO, "bench", "run_scenario.py"),
        "--run-id", run_id,
        "--rate", str(args.rate),
        "--duration", str(args.duration),
        "--total-cores", str(args.total_cores),
        "--executor-cores", str(args.executor_cores),
        "--executor-memory", args.executor_memory,
        "--latency-sample-rate", str(args.latency_sample_rate),
        "--drain-timeout", str(args.drain_timeout),
    ] + extra
    subprocess.run(cmd, timeout=int(args.duration) + args.drain_timeout + 1200)
    try:
        with open(os.path.join(RESULTS, f"{run_id}_scenario.json")) as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        return {"error": str(exc), "run_id": run_id}


def extract(report: Dict[str, Any]) -> Dict[str, Any]:
    rec = report.get("reconciliation") or {}
    lag = report.get("lag") or {}
    stream = report.get("stream") or {}
    fault = report.get("fault") or {}
    prod = report.get("producer") or {}
    return {
        "run_id": report.get("run_id"),
        "acked": rec.get("kafka_acked"),
        "expected_distinct": rec.get("expected_distinct_events"),
        "written": rec.get("written_distinct_trips"),
        "quarantined": rec.get("dlq_distinct_events"),
        "silent_loss": rec.get("unaccounted_silent_loss"),
        "silent_loss_pct": rec.get("silent_loss_pct"),
        "recovery_seconds": fault.get("recovery_seconds"),
        "recovered": fault.get("recovered"),
        "target_healthy_seconds": fault.get("target_healthy_seconds"),
        "lag_max": lag.get("max_observed"),
        "drain_seconds": lag.get("drain_seconds"),
        "avg_retries_all_writes": stream.get("avg_retries_to_success_all_writes"),
        "writes_needing_retry": stream.get("writes_needing_retry"),
        "avg_retries_among_retried": stream.get("avg_retries_among_retried_writes"),
        "cassandra_retries": stream.get("cassandra_write_retries"),
        "failure_reasons": stream.get("failure_reasons"),
        "producer_txretries": prod.get("librdkafka_txretries"),
        "producer_req_timeouts": prod.get("librdkafka_req_timeouts"),
        "achieved_rate": prod.get("achieved_rate"),
    }


def agg(values: List[Optional[float]]) -> Optional[Dict[str, float]]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return {
        "n": len(vals),
        "mean": round(statistics.fmean(vals), 3),
        "median": round(statistics.median(vals), 3),
        "min": round(min(vals), 3),
        "max": round(max(vals), 3),
        "stdev": round(statistics.stdev(vals), 3) if len(vals) > 1 else 0.0,
    }


def wait_for_stack(seconds: float) -> None:
    """Let the ring settle before the next run.

    A Cassandra node that just rejoined is still streaming hints and compacting.
    Starting the next measurement into that is measuring the recovery of the
    previous one.
    """
    print(f"  settling for {seconds}s...")
    time.sleep(seconds)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Chaos suite and backoff A/B")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--rate", type=float, default=800)
    p.add_argument("--duration", type=float, default=120)
    p.add_argument("--fault-at", type=float, default=35)
    p.add_argument("--restart-after", type=float, default=40)
    p.add_argument("--total-cores", type=int, default=6)
    p.add_argument("--executor-cores", type=int, default=3)
    p.add_argument("--executor-memory", default="1g")
    p.add_argument("--latency-sample-rate", type=float, default=0.05)
    p.add_argument("--drain-timeout", type=int, default=420)
    p.add_argument("--settle", type=float, default=45)
    p.add_argument("--scenarios",
                   default="kill_broker,kill_executor,kill_cassandra",
                   help="comma-separated; empty to skip the failure scenarios")
    p.add_argument("--skip-ab", action="store_true")
    p.add_argument("--ab-fault", default="kill_cassandra_quorum")
    p.add_argument("--prefix", default="chaos")
    p.add_argument("--report", default=os.path.join(RESULTS, "chaos_suite.json"))
    args = p.parse_args(argv)

    out: Dict[str, Any] = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "config": vars(args),
        "scenarios": {},
        "backoff_ab": None,
    }

    # ---------------- failure scenarios ------------------------------------
    scenarios = [s for s in args.scenarios.split(",") if s.strip()]
    for scenario in scenarios:
        runs = []
        for i in range(1, args.repeats + 1):
            run_id = f"{args.prefix}_{scenario}_{i}"
            print(f"\n{'#' * 72}\n#  {scenario}  run {i}/{args.repeats}\n{'#' * 72}")
            report = run_scenario(run_id, [
                "--scenario", scenario,
                "--fault", scenario,
                "--fault-at", str(args.fault_at),
                "--restart-after", str(args.restart_after),
            ], args)
            runs.append(extract(report))
            wait_for_stack(args.settle)

        out["scenarios"][scenario] = {
            "runs": runs,
            "recovery_seconds": agg([r["recovery_seconds"] for r in runs]),
            "silent_loss": agg([r["silent_loss"] for r in runs]),
            "quarantined": agg([r["quarantined"] for r in runs]),
            "lag_max": agg([r["lag_max"] for r in runs]),
            "avg_retries_among_retried": agg(
                [r["avg_retries_among_retried"] for r in runs]),
            "all_recovered": all(r.get("recovered") for r in runs),
            "zero_silent_loss_every_run": all(
                (r.get("silent_loss") or 0) <= 0 for r in runs),
        }

    # ---------------- backoff A/B ------------------------------------------
    if not args.skip_ab:
        arms: Dict[str, List[Dict[str, Any]]] = {"with_backoff": [], "no_backoff": []}
        for i in range(1, args.repeats + 1):
            for arm, backoff in (("with_backoff", "1000,2000,4000"),
                                 ("no_backoff", "")):
                run_id = f"{args.prefix}_ab_{arm}_{i}"
                print(f"\n{'#' * 72}\n#  BACKOFF A/B  {arm}  run {i}/{args.repeats}"
                      f"\n{'#' * 72}")
                report = run_scenario(run_id, [
                    "--scenario", f"backoff_ab_{arm}",
                    "--fault", args.ab_fault,
                    "--fault-at", str(args.fault_at),
                    "--restart-after", str(args.restart_after),
                    "--backoff", backoff,
                ], args)
                arms[arm].append(extract(report))
                wait_for_stack(args.settle)

        wb = arms["with_backoff"]
        nb = arms["no_backoff"]
        q_wb = agg([r["quarantined"] for r in wb])
        q_nb = agg([r["quarantined"] for r in nb])

        reduction = None
        if q_wb and q_nb and q_nb["mean"] > 0:
            reduction = round(100 * (q_nb["mean"] - q_wb["mean"]) / q_nb["mean"], 2)

        out["backoff_ab"] = {
            "fault": args.ab_fault,
            "note": ("Identical volume, identical fault, identical timing. The "
                     "only variable is RETRY_BACKOFF_SCHEDULE_MS."),
            "with_backoff": {
                "runs": wb,
                "quarantined": q_wb,
                "silent_loss": agg([r["silent_loss"] for r in wb]),
                "avg_retries_among_retried": agg(
                    [r["avg_retries_among_retried"] for r in wb]),
                "recovery_seconds": agg([r["recovery_seconds"] for r in wb]),
            },
            "no_backoff": {
                "runs": nb,
                "quarantined": q_nb,
                "silent_loss": agg([r["silent_loss"] for r in nb]),
                "avg_retries_among_retried": agg(
                    [r["avg_retries_among_retried"] for r in nb]),
                "recovery_seconds": agg([r["recovery_seconds"] for r in nb]),
            },
            "quarantine_reduction_pct": reduction,
            "interpretation": (
                "This is a reduction in events forced into QUARANTINE, not a "
                "reduction in permanent loss. Permanent loss is zero in both "
                "arms because the DLQ catches everything that exhausts its "
                "retry budget. Quoting this as 'reduced data loss' would be "
                "false."),
            "silent_loss_zero_in_both_arms": all(
                (r.get("silent_loss") or 0) <= 0 for r in wb + nb),
        }

    # ---------------- summary ----------------------------------------------
    print("\n" + "=" * 72)
    print("  CHAOS SUITE")
    print("=" * 72)
    for name, data in out["scenarios"].items():
        rec = data["recovery_seconds"]
        loss = data["silent_loss"]
        print(f"  {name:<18} recovery {rec['median'] if rec else '?':>7}s median "
              f"(n={rec['n'] if rec else 0})  "
              f"silent loss {loss['max'] if loss else '?'} max  "
              f"{'ALL RECOVERED' if data['all_recovered'] else 'RECOVERY FAILURE'}")
    if out["backoff_ab"]:
        ab = out["backoff_ab"]
        print("-" * 72)
        print(f"  backoff A/B ({ab['fault']}):")
        print(f"    quarantined with backoff : "
              f"{ab['with_backoff']['quarantined']['mean'] if ab['with_backoff']['quarantined'] else '?'}")
        print(f"    quarantined no backoff   : "
              f"{ab['no_backoff']['quarantined']['mean'] if ab['no_backoff']['quarantined'] else '?'}")
        print(f"    quarantine reduction     : {ab['quarantine_reduction_pct']}%")
        print(f"    silent loss zero in both : {ab['silent_loss_zero_in_both_arms']}")
    print("=" * 72)

    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(out, fh, indent=2)
        print(f"report -> {args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
