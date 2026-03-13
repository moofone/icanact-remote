# Mixed Topology Matrix

- Run timestamp (UTC): 2026-02-22T15:42:29Z
- Command shape: `/usr/bin/time -p cargo test --test <suite> -- --nocapture`
- Success rate basis: test-level pass/fail per required mixed-topology suite.

## Mixed-Topology Routing/Fan-out Results

| Scenario | Test suite | Result | Success rate | Test-finished latency | Wall-clock latency |
|---|---|---|---:|---:|---:|
| `A(TLS) <-> B(QUIC) <-> C(TLS)` routed convergence | `mixed_transport_routing_e2e` | pass | 100% (1/1) | 12.47s | 15.56s |
| `A(TLS) <-> B(QUIC) <-> C(TLS)` fan-out survives segment failure | `mixed_transport_fanout_e2e` | pass | 100% (1/1) | 0.23s | 1.49s |
| `A(TLS) <-> B(QUIC) <-> C(TLS)` failure isolation | `mixed_transport_failure_isolation_e2e` | pass | 100% (1/1) | 0.23s | 1.29s |
| `A(UDP) <-> B(UDP) <-> C(UDP)` routed convergence | `mixed_transport_udp_routing_e2e` | pass | 100% (1/1) | 0.22s | 0.22s |
| `A(UDP) <-> B(UDP) <-> C(UDP)` fan-out survives segment failure | `mixed_transport_udp_fanout_e2e` | pass | 100% (1/1) | 0.11s | 0.11s |
| `A(UDP) <-> B(UDP) <-> C(UDP)` failure isolation | `mixed_transport_udp_failure_isolation_e2e` | pass | 100% (1/1) | 0.11s | 0.11s |

Aggregate mixed-topology success rate: `100% (6/6)`.

Aggregate wall-clock latency across required suites: recompute pending after next full mixed-topology sweep.

## Hardening Gate

Additional hardening suite run in same pass:

| Suite | Result | Success rate | Test-finished latency | Wall-clock latency |
|---|---|---:|---:|---:|
| `transport_edge_cases` | pass | 100% (6/6) | 1.95s | 3.12s |

## Sprint 7 Addendum: TLS + NativeQUIC

- Addendum timestamp (UTC): 2026-02-22T16:49:13Z
- Command shape: `/usr/bin/time -p cargo test --test <suite> -- --nocapture`

| Scenario | Test suite | Result | Success rate | Test-finished latency | Wall-clock latency |
|---|---|---|---:|---:|---:|
| `A(TLS) <-> B(NativeQUIC) <-> C(TLS)` routed convergence | `mixed_transport_native_quic_routing_e2e` | pass | 100% (1/1) | 12.25s | 12.55s |
| `A(TLS) <-> B(NativeQUIC) <-> C(TLS)` fan-out survives segment failure | `mixed_transport_native_quic_fanout_e2e` | pass | 100% (1/1) | 0.23s | 0.54s |
| `A(TLS) <-> B(NativeQUIC) <-> C(TLS)` failure isolation | `mixed_transport_native_quic_failure_isolation_e2e` | pass | 100% (1/1) | 8.60s | 8.94s |

## Raw Artifacts

- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_routing_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_fanout_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_failure_isolation_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_udp_routing_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_udp_fanout_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_udp_failure_isolation_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_native_quic_routing_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_native_quic_fanout_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/mixed_transport_native_quic_failure_isolation_e2e.log`
- `/Users/greg/dev/icanact-remote/benchmarks/transport_trait/raw/mixed_topology/transport_edge_cases.log`
