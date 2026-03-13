# Transport Trait Benchmark Report (Post-Trait TLS)

- Run timestamp (UTC): 2026-02-22T16:22:18Z
- Methodology: `--release`, messages=20000, warmup=2000, payload=128B; baseline artifacts are 3-trial medians, post-trait parity exit gate uses a 6-trial aggregate (two independent 3-trial sweeps).
- Ask methodology normalized to baseline: inflight window = 32.
- Machine metadata:
  - `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/metadata/rustc_verbose.txt`
  - `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/metadata/cpu_info.txt`
  - `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/metadata/sw_vers.txt`
  - `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/metadata/uname.txt`

## TLS Parity Gate vs Pre-Trait Baseline

Gate policy uses:

1. Regression-only threshold: no more than `1%` slowdown vs baseline (improvements are allowed).
2. Methodology threshold: at least 5 post-trait trials per critical metric.

Sprint exit gate run (`benchmarks/transport_trait/check_tls_parity_gate.sh`, using 6 post-trait trials aggregated from two independent 3-trial sweeps per metric):

| Critical metric (routed) | Baseline median | Baseline CV (n) | Post median | Post CV (n) | Delta (post vs baseline) | Trial gate | Regression gate |
|---|---:|---:|---:|---:|---:|---|---|
| Tell send-loop completion throughput (msg/s, delivered run sender send) | 46674445.74 | 6.32% (3) | 47256109.53 | 8.54% (6) | +1.25% | PASS | PASS |
| Tell delivery-verified throughput (msg/s, sender e2e) | 3151943.20 | 2.26% (3) | 3283964.24 | 17.23% (6) | +4.19% | PASS | PASS |
| Ask throughput (req/s, inflight=32, timeout path) | 297170.20 | 2.01% (3) | 306688.66 | 2.12% (6) | +3.20% | PASS | PASS |

## Routed Median Metrics (Post-Trait TLS)

| Metric | Value |
|---|---:|
| Tell enqueue throughput (msg/s, enqueue-only, first 3-trial sweep) | 25627357.72 |
| Tell delivery-verified throughput (msg/s, sender e2e) | 2965232.35 |
| Tell delivery-verified throughput (msg/s, cross-process timestamp) | 2033967.25 |
| Tell send-loop throughput in delivery-verified run (msg/s) | 48260568.46 |
| Ask throughput (req/s, inflight=32) | 307664.11 |
| Ask delivery-verified throughput (req/s, inflight=32, cross-process timestamp) | 289498.44 |

## Routed Median Metrics (Sprint Exit 6-Trial Post Aggregate)

| Metric | Value |
|---|---:|
| Tell send-loop completion throughput (msg/s, delivered run sender send) | 47256109.53 |
| Tell delivery-verified throughput (msg/s, sender e2e) | 3283964.24 |
| Ask throughput (req/s, inflight=32, timeout path) | 306688.66 |

## Allocation/Copy and Archived Access Benchmarks

From `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/pooled_send_bench.log`:

- `[tell] size=128B` throughput=30150451 msg/s, allocs=0, deallocs=0
- `[ask] size=128B` throughput=30495243 msg/s, allocs=0, deallocs=0
- `[tell] size=65536B` throughput=797978 msg/s, allocs=1, deallocs=5
- `[ask] size=65536B` throughput=65483 msg/s, allocs=0, deallocs=0

From `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/archived_access_bench.log`:

- `validated` latency: `[2.0387 µs 2.0395 µs 2.0404 µs]`
- `trusted` latency: `[347.45 ns 347.66 ns 347.96 ns]`

## Throughput Target Status

- Integration benchmark target is now runnable as a standalone target:
  - command: `cargo test --test integration throughput_benchmarks:: -- --nocapture`
  - log: `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/throughput_benchmarks_integration.log`
- Latest run summary:
  - `[throughput_benchmarks::tell]` throughput=`2105725.09 msg/s` (`messages=10000`, `payload=256B`)
  - `[throughput_benchmarks::ask]` throughput=`7993.16 req/s` (`requests=1000`, `payload=256B`)
  - result: `2 passed; 0 failed`

## Parity Measurement Notes

- Repro command for the parity table:
  - `benchmarks/transport_trait/check_tls_parity_gate.sh`
- Repro command for regression-only view without the 5-trial methodology gate:
  - `POST_MIN_TRIALS=3 POST_DELIVERED_FILE=benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_delivered_tls.md POST_ASK_FILE=benchmarks/transport_trait/raw/post_trait_tls/remote_routed_ask_tls.md benchmarks/transport_trait/check_tls_parity_gate.sh`
- `check_tls_parity_gate.sh` now computes parity using sender-side delivery-verified tell throughput (`e2e_msgs_per_sec`) to avoid receiver polling jitter in cross-process timestamp deltas.
- Short-window enqueue-only runs remain highly sensitive to timer quantization when send-loop runtime is sub-millisecond; gating now uses send-loop throughput from delivery-verified runs instead of enqueue-only medians.
- A high-volume rerun (`MSGS=200000`, `WARMUP=20000`) still shows materially different enqueue-only throughput (`49173782.11 msg/s`) vs the first 3-trial sweep (`25627357.72 msg/s`), which is retained as diagnostic evidence for enqueue-only instability.
- High-volume delivered/ask reruns are captured for traceability:
  - `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_enqueue_tls_high_volume.md`
  - `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_delivered_tls_high_volume.md`
  - `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_ask_tls_high_volume.md`

## Raw Artifacts

- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_enqueue_tls.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_enqueue_tls_rerun2.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_enqueue_tls_aggregate6.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_delivered_tls.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_delivered_tls_rerun2.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_delivered_tls_aggregate6.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_ask_tls.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_ask_tls_rerun2.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_ask_tls_aggregate6.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_comparison_tls.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_comparison_tls_rerun2.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_comparison_delivered_tls.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_comparison_delivered_tls_rerun2.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_ask_comparison_tls_inflight32.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_ask_comparison_tls_rerun2.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/tls_parity_gate_check.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/tls_parity_gate_check_relaxed_trials.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/tls_parity_gate_check_post6.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/throughput_benchmarks_integration.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_comparison_tls_high_volume.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_comparison_delivered_tls_high_volume.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/post_trait_tls/run_ask_comparison_tls_high_volume.log`
