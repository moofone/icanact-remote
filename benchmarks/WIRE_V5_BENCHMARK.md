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

Reproduce:

```text
git worktree add --detach /tmp/icanact-wire-v4-baseline 9602510
# adapt only tests/streaming_tests.rs to connection_ref() for release visibility
(cd /tmp/icanact-wire-v4-baseline && cargo test --release --test streaming_tests)
(cd /path/to/feat-wire-v5 && cargo test --release --test streaming_tests)
```
