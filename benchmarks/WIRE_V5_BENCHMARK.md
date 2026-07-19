# Wire V5 local release verification

Date: 2026-07-19

The same release streaming suite was run against these worktrees on this host:

| Variant | Commit / tree | Command | Result |
|---|---|---|---|
| V4 baseline | detached `9602510` | `cargo test --release --test streaming_tests` | 10 passed |
| V5 | `feat/wire-v5` worktree | `cargo test --release --test streaming_tests` | 10 passed |

The historical test had a debug-only access to `RemoteActorRef.connection`.
For the detached V4 benchmark worktree only, its test harness was changed to
the existing public `connection_ref()` accessor; production V4 source was not
changed. The V5 test now uses that public accessor as well, so release testing
is a supported verification path.

This is a correctness and repeatability gate, not a throughput claim. The
wire reduction and direct-read changes must not be assigned a percentage gain
until repeated per-workload measurements, allocator counters, and CPU samples
are captured on a quiet host.

## Repeatable raw-run protocol

The checked-in integration harness has warm-up, elapsed-time, checksum, and
throughput output. Capture each command five times with `--nocapture`, retain
the unedited stdout as `benchmarks/raw/<commit>/<workload>-<run>.log`, and
record median plus min/max in the PR. Do not compare a V4 run with a V5 run
unless the toolchain, release profile, host, TLS configuration, worker count,
and workload command are identical.

```text
# V5 ActorAsk/Response, 256-byte payload, 64 in-flight requests
cargo test --release --test integration test_ask_actor_frame_inflight64_throughput -- --ignored --nocapture

# V5 release stream correctness/provenance smoke
cargo test --release --test wire_v5_e2e_zero_copy_proof -- --nocapture

# V4 baseline uses the same first command in a detached 9602510 worktree.
# Adapt only the benchmark test's old private connection access to the existing
# public connection_ref() accessor; production V4 code remains untouched.
```

The current release run completed successfully on this worktree. It is an
artifact-execution check only: no throughput delta is claimed here because the
five-run V4/V5 raw logs and allocator/CPU counters have not yet been retained.

Reproduce:

```text
git worktree add --detach /tmp/icanact-wire-v4-baseline 9602510
# adapt only tests/streaming_tests.rs to connection_ref() for release visibility
(cd /tmp/icanact-wire-v4-baseline && cargo test --release --test streaming_tests)
(cd /path/to/feat-wire-v5 && cargo test --release --test streaming_tests)
```
