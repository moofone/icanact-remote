# Native QUIC vs Iroh QUIC (Sprint 7)

- Run timestamp (UTC): 2026-02-22T16:49:13Z
- Methodology: 3 trials (median), `--release`, messages=20000, warmup=2000, payload=128B.
- Ask methodology: inflight window = 32.
- Delta formula: `(native_quic / iroh_quic - 1) * 100`.

## Throughput Comparison (Routed)

| Metric | `IrohQuicStack` | `NativeQuicStack` | Delta |
|---|---:|---:|---:|
| Tell enqueue throughput (msg/s, enqueue-only) | 45377197.96 | 36739380.02 | -19.04% |
| Tell delivery-verified throughput (msg/s) | 1673360.11 | 1414527.19 | -15.47% |
| Ask throughput (req/s, timeout path, inflight=32) | 286223.69 | 102183.80 | -64.30% |

## Mixed-Topology Functional Validation (TLS + NativeQUIC)

| Scenario | Test suite | Result | Test-finished latency | Wall-clock latency |
|---|---|---|---:|---:|
| `A(TLS) <-> B(NativeQUIC) <-> C(TLS)` routed convergence | `mixed_transport_native_quic_routing_e2e` | pass | 12.25s | 12.55s |
| `A(TLS) <-> B(NativeQUIC) <-> C(TLS)` fan-out survives segment failure | `mixed_transport_native_quic_fanout_e2e` | pass | 0.23s | 0.54s |
| `A(TLS) <-> B(NativeQUIC) <-> C(TLS)` failure isolation | `mixed_transport_native_quic_failure_isolation_e2e` | pass | 8.60s | 8.94s |

## Commands Used

```bash
ICANACT_TRANSPORT=native-quic RESULTS_FILE=/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/remote_routed_tell_enqueue_native_quic.md \
  bash /Users/greg/dev/icanact-remote/benchmarks/remote_routed_tell_compare/run_comparison.sh

ICANACT_TRANSPORT=native-quic RESULTS_FILE=/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/remote_routed_tell_delivered_native_quic.md \
  bash /Users/greg/dev/icanact-remote/benchmarks/remote_routed_tell_compare/run_comparison_delivered.sh

ICANACT_TRANSPORT=native-quic INFLIGHT=32 RESULTS_FILE=/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/remote_routed_ask_native_quic.md \
  bash /Users/greg/dev/icanact-remote/benchmarks/remote_routed_tell_compare/run_ask_comparison.sh

/usr/bin/time -p cargo test --test mixed_transport_native_quic_routing_e2e -- --nocapture
/usr/bin/time -p cargo test --test mixed_transport_native_quic_fanout_e2e -- --nocapture
/usr/bin/time -p cargo test --test mixed_transport_native_quic_failure_isolation_e2e -- --nocapture
```

## Raw Artifacts

- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/remote_routed_tell_enqueue_native_quic.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/remote_routed_tell_delivered_native_quic.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/remote_routed_ask_native_quic.md`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/run_comparison_native_quic.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/run_comparison_delivered_native_quic.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/per_transport/run_ask_comparison_native_quic_inflight32.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_native_quic_routing_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_native_quic_fanout_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_native_quic_failure_isolation_e2e.log`

## Caveat

`NativeQuicStack` is currently comparison-focused and non-default. In this sprint, its registry bootstrap uses the existing secure stream dataplane so stack-level behavior can be compared without changing the default transport path.
