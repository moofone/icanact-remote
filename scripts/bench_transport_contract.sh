#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TARGET_DIR="${CARGO_TARGET_DIR:-target/codex-remote-transport-bench}"
FEATURES="${ICANACT_REMOTE_BENCH_FEATURES:-test-helpers}"
RUNS="${ICANACT_REMOTE_BENCH_RUNS:-5}"

TESTS=(
  "throughput_benchmarks::test_tell_actor_frame_delivered_throughput"
  "throughput_benchmarks::test_ask_actor_frame_no_timeout_throughput"
  "throughput_benchmarks::test_ask_actor_frame_no_timeout_inflight512_throughput"
  "throughput_benchmarks::test_ask_direct_no_timeout_throughput"
  "throughput_benchmarks::test_ask_actor_frame_no_timeout_split_inflight512_throughput"
  "throughput_benchmarks::test_ask_actor_frame_deferred_inflight512_throughput"
  "throughput_benchmarks::test_ask_actor_frame_deferred_split_inflight512_throughput"
  "throughput_benchmarks::test_ask_actor_frame_split_single_flight_throughput"
  "throughput_benchmarks::test_ask_actor_frame_proxy_split_single_flight_throughput"
  "throughput_benchmarks::test_ask_actor_frame_proxy_split_inflight64_throughput"
  "throughput_benchmarks::test_ask_actor_frame_timeout_proxy_inflight64_throughput"
  "throughput_benchmarks::test_ask_actor_frame_aligned_timeout_proxy_inflight64_throughput"
  "throughput_benchmarks::test_ask_actor_frame_outer_timeout_proxy_inflight64_throughput"
  "throughput_benchmarks::test_ask_actor_frame_deferred_timeout_proxy_inflight64_throughput"
  "throughput_benchmarks::test_connect_to_peer_contention_throughput"
)

extract_throughput() {
  grep -o 'throughput=[0-9.]*' | tail -n 1 | cut -d= -f2
}

median_of() {
  printf '%s\n' "$@" | sort -n | awk '
    {
      vals[NR] = $1
    }
    END {
      if (NR == 0) {
        exit 1
      }
      mid = int((NR + 1) / 2)
      if (NR % 2 == 1) {
        print vals[mid]
      } else {
        printf "%.2f\n", (vals[mid] + vals[mid + 1]) / 2
      }
    }
  '
}

for test_name in "${TESTS[@]}"; do
  printf '== %s ==\n' "$test_name"
  values=()
  for run_idx in $(seq 1 "$RUNS"); do
    output="$(
      CARGO_TARGET_DIR="$TARGET_DIR" \
        cargo test --test integration --features "$FEATURES" "$test_name" -- --ignored --nocapture
    )"
    printf '%s\n' "$output"
    value="$(printf '%s\n' "$output" | extract_throughput)"
    values+=("$value")
    printf 'run=%s throughput=%s\n' "$run_idx" "$value"
  done
  median="$(median_of "${values[@]}")"
  printf 'median_throughput=%s\n\n' "$median"
done
