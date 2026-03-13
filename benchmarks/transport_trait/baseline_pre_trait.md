# Transport Trait Benchmark Baseline (Pre-Trait TLS)

- Captured from pre-trait mainline artifacts in `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/baseline_pre_trait`.
- Methodology: 3 trials (median), `--release`, messages=20000, warmup=2000, payload=128B.
- Ask methodology: inflight window = 32.
- Baseline is an immutable historical capture (pre-trait code snapshot is not available in current git history).

## Routed Median Metrics (icanact-remote)

| Metric | Value |
|---|---:|
| Tell enqueue throughput (msg/s, enqueue-only) | 46251757.57 |
| Tell delivery-verified throughput (msg/s, sender e2e) | 3151943.20 |
| Tell delivery-verified throughput (msg/s, cross-process timestamp) | 2198043.74 |
| Tell send-loop throughput in delivery-verified run (msg/s) | 46674445.74 |
| Ask throughput (req/s) | 297170.20 |
| Ask delivery-verified throughput (req/s, cross-process timestamp) | 260994.39 |

## Required Reporting Table (Section 10.7)

| Transport | Tell msg/s | Ask req/s | p99 latency | Gossip convergence | Link detect p99 | Probe overhead | Copy/alloc delta | Pass/Fail |
|---|---:|---:|---|---|---|---|---|---|
| pre-trait TLS baseline | 3151943.20 | 297170.20 | N/A (not emitted by current harness) | N/A (not emitted by current harness) | N/A (not emitted by current harness) | N/A (not emitted by current harness) | N/A (baseline copy/alloc instrumentation not captured) | baseline |

## Raw Artifacts

- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/baseline_pre_trait/remote_routed_tell_enqueue_baseline.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/baseline_pre_trait/remote_routed_tell_delivered_baseline.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/baseline_pre_trait/remote_routed_ask_baseline.md`
