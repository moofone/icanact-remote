#!/usr/bin/env python3
"""Adapt one exact, existing transport benchmark to a strict metric record.

These are aggregate throughput samples, NOT per-message p99 or allocation
measurements. Use --minimum-seconds for duration gates; short runs are pilots.
"""
import argparse
import json
import math
import re
import subprocess
import sys

PREFIX = "integration::throughput_benchmarks::"
TESTS = {
    "tell_delivered": PREFIX + "test_tell_actor_frame_delivered_throughput",
    "ask_single_flight": PREFIX + "test_ask_actor_frame_no_timeout_throughput",
    "ask_inflight512": PREFIX + "test_ask_actor_frame_no_timeout_inflight512_throughput",
}


def parse(output, workload, minimum_seconds):
    summaries = re.findall(r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output, re.M)
    if summaries != [("1", "0", "0")]:
        raise ValueError("exactly one executed, passed benchmark required")
    # With --nocapture, libtest may print the test-name prefix on the same
    # line as the benchmark's first println. Still require exactly one marker.
    lines = re.findall(r"\[throughput_benchmarks::[^\n]+", output)
    if len(lines) != 1:
        raise ValueError("exactly one benchmark result required")
    count = re.search(r"(?:messages|requests)=(\d+)", lines[0])
    elapsed = re.search(r"elapsed=([0-9.]+)s", lines[0])
    rate = re.search(r" throughput=([0-9.]+) ", lines[0])
    if not count or not elapsed or not rate:
        raise ValueError("missing completion/time/throughput metrics")
    count, elapsed, rate = int(count[1]), float(elapsed[1]), float(rate[1])
    if count <= 0 or elapsed <= 0 or elapsed < minimum_seconds or not math.isfinite(rate) or rate <= 0:
        raise ValueError("invalid/too-short benchmark sample")
    if abs(rate - count / elapsed) / rate > 0.01:
        raise ValueError("reported throughput inconsistent with completed work/time")
    return {"workload": workload, "completed": count, "errors": 0, "drops": 0,
            "metrics": {"throughput": rate, "ns_per_op": elapsed * 1e9 / count}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=TESTS, required=True)
    parser.add_argument("--minimum-seconds", type=float, default=10)
    args = parser.parse_args()
    command = ["cargo", "test", "--offline", "--locked", "--release", "--test", "integration",
               "--features", "test-helpers", TESTS[args.workload], "--"]
    # List-mode guards both an incorrect namespace and an absent selector.
    listing = subprocess.run(command + ["--ignored", "--exact", "--list"], capture_output=True, text=True)
    sys.stdout.write(listing.stdout)
    sys.stderr.write(listing.stderr)
    if listing.returncode or re.findall(r"^(.+): test$", listing.stdout, re.M) != [TESTS[args.workload]]:
        print("BLOCKED: expected exactly one benchmark selection", file=sys.stderr)
        return 1
    result = subprocess.run(command + ["--ignored", "--exact", "--test-threads=1", "--nocapture"],
                            capture_output=True, text=True)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode:
        return result.returncode
    try:
        record = parse(result.stdout, args.workload, args.minimum_seconds)
    except ValueError as error:
        print(f"BLOCKED: {error}", file=sys.stderr)
        return 1
    print("AB_METRICS " + json.dumps(record))
    return 0


if __name__ == "__main__":
    sys.exit(main())
