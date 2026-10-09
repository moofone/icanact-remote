#!/usr/bin/env python3
"""Long concurrency sample adapter; saturation/closed-loop, not open-loop latency."""
import argparse
import json
import os
import re
import subprocess
import sys

TEST = "integration::throughput_benchmarks::test_actor_ask_concurrency_measurement"

def parse(output, inflight, seconds):
    summaries = re.findall(r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output, re.M)
    if summaries != [("1", "0", "0")]:
        raise ValueError("exactly one selected and executed passing test required")
    lines = re.findall(r"AB_METRICS (\{[^\n]+)", output)
    if len(lines) != 1:
        raise ValueError("exactly one native record required")
    record = json.loads(lines[0])
    if record.get("workload") != f"ask_inflight{inflight}" or record.get("completed", 0) <= 0:
        raise ValueError("wrong workload identity or no completed work")
    if record.get("errors") != 0 or record.get("drops") != 0:
        raise ValueError("errors/drops must be zero")
    metrics = record["metrics"]
    if metrics["duration_ns"] < seconds * 1e9:
        raise ValueError("short timing window")
    if not 0 < metrics["p50_ns"] <= metrics["p95_ns"] <= metrics["p99_ns"]:
        raise ValueError("invalid latency percentiles")
    rate = record["completed"] * 1e9 / metrics["duration_ns"]
    if abs(rate - metrics["throughput"]) / rate > 1e-6:
        raise ValueError("inconsistent throughput/completion count")
    return record

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inflight", type=int, choices=[1, 8, 64, 512], required=True)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    args = parser.parse_args()
    if args.seconds < 10 or args.warmup_seconds < 5:
        parser.error(">=10 measured seconds and >=5 warmup seconds required")
    env = dict(os.environ, CARGO_NET_OFFLINE="true",
               ICANACT_MEASURE_INFLIGHT=str(args.inflight),
               ICANACT_MEASURE_MS=str(args.seconds * 1000),
               ICANACT_MEASURE_WARMUP_MS=str(args.warmup_seconds * 1000))
    base = ["cargo", "test", "--offline", "--locked", "--release", "--test", "integration",
            "--features", "test-helpers", TEST, "--"]
    listing = subprocess.run(base + ["--ignored", "--exact", "--list"],
                             env=env, capture_output=True, text=True)
    sys.stdout.write(listing.stdout)
    sys.stderr.write(listing.stderr)
    if listing.returncode or re.findall(r"^(.+): test$", listing.stdout, re.M) != [TEST]:
        print("BLOCKED: expected exactly one benchmark selection", file=sys.stderr)
        return 1
    result = subprocess.run(base + ["--ignored", "--exact", "--test-threads=1", "--nocapture"],
                            env=env, capture_output=True, text=True)
    # Keep the native record without emitting two recognized AB_METRICS lines.
    sys.stdout.write(result.stdout.replace("AB_METRICS ", "NATIVE_METRICS "))
    sys.stderr.write(result.stderr)
    if result.returncode:
        return result.returncode
    try:
        record = parse(result.stdout, args.inflight, args.seconds)
    except (ValueError, KeyError) as error:
        print(f"BLOCKED: {error}", file=sys.stderr)
        return 1
    print("AB_METRICS " + json.dumps(record))
    return 0

if __name__ == "__main__":
    sys.exit(main())
