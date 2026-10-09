#!/usr/bin/env python3
"""Fixed offered-load sample adapter.

The native fixture schedules asks at a constant rate. Latency starts at the
scheduled instant, so a late issue keeps that delay. An offer still waiting
after the declared lateness bound is a drop and fails this adapter. This is
not a closed-loop saturation measurement.
"""
import argparse
import json
import os
import re
import subprocess
import sys

TEST = "integration::throughput_benchmarks::test_actor_ask_offered_load_measurement"


def parse(output, rate, seconds, max_inflight):
    summaries = re.findall(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output, re.M
    )
    if summaries != [("1", "0", "0")]:
        raise ValueError("exactly one selected and executed passing test required")
    lines = re.findall(r"AB_METRICS (\{[^\n]+)", output)
    if len(lines) != 1:
        raise ValueError("exactly one native record required")
    record = json.loads(lines[0])
    if record.get("workload") != f"ask_offered{rate}" or record.get("completed", 0) <= 0:
        raise ValueError("wrong workload identity or no completed work")
    if record.get("offered") != rate * seconds:
        raise ValueError("offer count does not match the preregistered rate and window")
    if record.get("completed") != record.get("offered"):
        raise ValueError("every scheduled offer must complete")
    if record.get("errors") != 0 or record.get("drops") != 0:
        raise ValueError("errors and drops must be zero")
    metrics = record["metrics"]
    if metrics["offer_window_ns"] != seconds * 1_000_000_000:
        raise ValueError("offer window does not match the requested duration")
    # The last offer is one interval before the nominal window ends.
    last_scheduled_ns = (record["offered"] - 1) * 1_000_000_000 // rate
    if metrics["duration_ns"] < last_scheduled_ns:
        raise ValueError("measurement ended before the last scheduled offer")
    if not 0 < metrics["p50_ns"] <= metrics["p95_ns"] <= metrics["p99_ns"]:
        raise ValueError("invalid latency percentiles")
    if not 1 <= metrics["max_backlog"] <= max_inflight:
        raise ValueError("backlog outside the declared admission bound")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, required=True)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    parser.add_argument("--max-inflight", type=int, default=64)
    parser.add_argument("--max-lateness-ms", type=int, default=1000)
    args = parser.parse_args()
    if args.rate <= 0 or args.seconds < 10 or args.warmup_seconds < 5:
        parser.error("positive rate, >=10 measured seconds and >=5 warmup seconds required")
    if args.max_inflight <= 0 or args.max_lateness_ms <= 0:
        parser.error("positive admission bound and lateness bound required")
    env = dict(
        os.environ,
        CARGO_NET_OFFLINE="true",
        ICANACT_MEASURE_OFFER_RATE=str(args.rate),
        ICANACT_MEASURE_MS=str(args.seconds * 1000),
        ICANACT_MEASURE_WARMUP_MS=str(args.warmup_seconds * 1000),
        ICANACT_MEASURE_MAX_INFLIGHT=str(args.max_inflight),
        ICANACT_MEASURE_MAX_LATENESS_MS=str(args.max_lateness_ms),
    )
    base = [
        "cargo",
        "test",
        "--offline",
        "--locked",
        "--release",
        "--test",
        "integration",
        "--features",
        "test-helpers",
        TEST,
        "--",
    ]
    listing = subprocess.run(
        base + ["--ignored", "--exact", "--list"], env=env, capture_output=True, text=True
    )
    sys.stdout.write(listing.stdout)
    sys.stderr.write(listing.stderr)
    if listing.returncode or re.findall(r"^(.+): test$", listing.stdout, re.M) != [TEST]:
        print("BLOCKED: expected exactly one benchmark selection", file=sys.stderr)
        return 1
    result = subprocess.run(
        base + ["--ignored", "--exact", "--test-threads=1", "--nocapture"],
        env=env,
        capture_output=True,
        text=True,
    )
    sys.stdout.write(result.stdout.replace("AB_METRICS ", "NATIVE_METRICS "))
    sys.stderr.write(result.stderr)
    if result.returncode:
        return result.returncode
    try:
        record = parse(result.stdout, args.rate, args.seconds, args.max_inflight)
    except (ValueError, KeyError) as error:
        print(f"BLOCKED: {error}", file=sys.stderr)
        return 1
    print("AB_METRICS " + json.dumps(record))
    return 0


if __name__ == "__main__":
    sys.exit(main())
