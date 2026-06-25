# Remediation: asymmetric peer-bootstrap stall in icanact-remote

## Failure (observed at devnet 2026-05-14)

Stratum on `stratum-devnet-a` could not gossip with coin-proxy:

```
gossip: failed to gossip to peer peer=10.77.0.61:9301
  error=network error: timed out waiting for preferred inbound connection
```

coin-proxy on `coin-devnet-a` was completely silent — no peer activity,
no errors. Xelis jobs never propagated. The cluster appeared healthy
from systemd but the mining pipeline was dead.

## Root cause

`src/registry.rs:6152` (`should_keep_connection`) and
`src/connection_pool/transport_stream.rs:124-181` together implement a
**pre-TCP** asymmetric tie-break:

| `local_id` vs `remote_id` | Behavior |
| ------------------------- | -------- |
| `local_id < remote_id`    | Keep outbound — this side dials. |
| `local_id > remote_id`    | Keep inbound  — this side **suppresses outbound entirely** and parks in `wait_for_preferred_connection` until an inbound arrives, else returns `timed out waiting for preferred inbound connection`. |

This makes the protocol's correctness depend on a **second** invariant
that lives outside the protocol itself: *the lower-ID side must already
know the higher-ID side's address before any traffic flows.*

In production, the seeding for `stratum ↔ coin-proxy` only carried one
direction (stratum had coin-proxy in its peer set; coin-proxy did not
have stratum). Stratum was the higher-ID side, so it suppressed and
waited. coin-proxy never dialed because it never knew stratum existed.
Permanent stall, with the deafening silence on one side and a tight
warn-log loop on the other.

This is reproduced as a failing test in
`tests/preferred_inbound_deadlock.rs` —
`higher_id_side_stalls_when_lower_id_side_is_unseeded`.

## Why ops-side fixes are insufficient

Asking topology config to always seed both directions is fragile:

- Any future asymmetric topology (one-way ingress, gateway-style peers,
  late-joiners) breaks the same way.
- A node restart that loses persisted peers re-introduces the stall.
- Discovery via gossip can't bootstrap the relationship that gossip
  itself depends on.

The protocol must converge under any **non-empty unidirectional
bootstrap**: if *either* side knows the other's address, they end up
with a usable connection within a bounded time. That property is
checkable and should be a permanent invariant guarded by the test in
this PR.

## Remediation options

| # | Approach | Pros | Cons |
| - | -------- | ---- | ---- |
| 1 | **Always dial; resolve duplicates after handshake.** Drop the pre-TCP suppression in `transport_stream.rs:124-182`. Let both sides establish, then the existing post-handshake tie-break (`wrong_direction_evicted` in `pool_connect.rs:104`) drops the loser. | One small structural change. Symmetric. Guarantees convergence from any non-empty seeding. | Briefly double-handshakes when both sides race; cost is one extra TLS handshake at startup per pair. |
| 2 | **Fallback to outbound after `wait_for_preferred_connection` times out.** Keep the current optimistic suppression but after timeout, dial anyway. | Minimal change. | Adds full `connect_timeout` of latency to bootstrap in the asymmetric case; the asymmetric case is the *normal* case during cold-start. |
| 3 | **One-shot hint dial.** Higher-ID side opens a TLS connection just long enough to send `{my_addr, please_dial_me}` then closes; lower-ID side records and dials. | Preserves the existing direction preference. | New bootstrap-only message type, new lifecycle stage, extra code path. |
| 4 | **Topology-side mutual seeding.** Make ansible / config guarantee both directions are seeded. | No protocol change. | Doesn't actually fix the protocol; the next operator who misconfigures, or any peer learned over gossip whose reverse-seed is missed, hits the same wall. Test in this PR will not pass under this option. |

**Recommendation after broader scripted-network scrutiny: option 2.**
The naive option 1 does fix one-sided bootstrap, but it also publishes
extra outbound sessions during collision/reconnect scenarios and can
turn ordinary actor timeouts into `ConnectionDropped` / `ConnectionClosed`.
That is a regression. Option 2 preserves the existing preferred-inbound
fast path when the lower-ID side is correctly seeded, but removes the
permanent stall by falling back to an outbound dial after the bounded
preferred-inbound wait expires.

## Concrete change (option 2)

In `src/connection_pool/transport_stream.rs`, keep the
`!registry_arc.should_keep_connection(&remote_peer_id, true)` gate and
`wait_for_preferred_connection`, but change the timeout branch. It must
record `OutboundSuppressedInboundTimeout` and then continue into the
normal outbound TCP/TLS dial instead of returning:

```text
network error: timed out waiting for preferred inbound connection
```

The lifecycle events `OutboundSuppressedWaitInbound`,
`OutboundSuppressedInboundReady`, and `OutboundSuppressedInboundTimeout`
remain meaningful:

- `WaitInbound`: this side prefers inbound and is giving the lower-ID
  owner a chance to dial.
- `InboundReady`: the preferred direction arrived and was used.
- `InboundTimeout`: the preferred direction did not arrive; this side is
  now entering fallback dial to avoid a permanent one-sided-bootstrap
  stall.

In `src/lib.rs::Peer::connect`, do not return early only because the
tie-break prefers inbound. Configure the peer, evict any existing
wrong-direction live connection, then call `connect_to_peer`; the stream
layer owns the bounded wait plus fallback.

## Acceptance signal (mandatory)

The fix is not accepted on the absence of a warning. The signal is
**positive end-to-end propagation**, measured at two layers.

### Layer 1 — icanact-remote (this repo)

`tests/preferred_inbound_deadlock.rs` asserts, after asymmetric
seeding:

1. `has_connection_to_peer` returns true on at least one side.
2. A locally-registered actor on the higher-ID side is **visible via
   `lookup_actor` on the lower-ID side** within `bootstrap_timeout`.

Today the test fails. Post-fix it must pass. Just silencing the
"timed out waiting for preferred inbound connection" log would not
move signal #2.

### Layer 2 — icemining (downstream acceptance)

Two production-shaped counters must move on devnet after the fix
deploys, and the gates must be wired into the `--mode full` post-deploy
verification (currently the script skips `icemining_skip_post_deploy_gates=true`
across the board — see `devnet-deploy.sh` invocations; that escape
hatch should not apply to these two gates):

| Counter | Source | Required behavior |
| ------- | ------ | ----------------- |
| `coin_proxy_job_remote_enqueued_total{coin="xelis"}` | coin-proxy | must strictly increase from zero within `gossip_interval × 4` of a fresh boot of either coin-proxy or stratum. |
| `stratum_installed_broadcast_job_total{coin="xelis"}` | stratum | must strictly increase, lagging the coin-proxy counter by at most one gossip interval. |

The fix is rejected if either counter remains at zero, regardless of
how clean the logs look. Conversely, if both counters advance under
the asymmetric-seeding chaos drill (see below), the fix is accepted
even if some lifecycle warn-logs remain — logs are diagnostic; job
flow is the product.

### Layer 3 — shared-sync/auth TLS identity acceptance

The same class of silent transport failure must be rejected on
service-to-consensus paths. A client-side generic timeout is not sufficient
evidence when the consensus side logs TLS accept failures.

For any fix touching gossip, peer identity, TLS bootstrap, or service
client routing, acceptance must include a two-sided correlation:

1. Capture the service-client peer IDs, advertised bind addresses, and
   loaded peer/seed configuration from the auth binary at startup.
2. Capture the consensus-side accepted/rejected TLS peer evidence for the
   same client IP/time window.
3. If the consensus service logs `TLS accept failed` / `HandshakeFailure`
   while auth only reports `cluster-api remote error: connection timeout`,
   the fix is rejected until the mismatch is explained by exact peer ID,
   node ID, certificate identity, or config state.
4. After remediation, auth must complete lease RPCs against the consensus
   quorum without corresponding consensus-side TLS `HandshakeFailure` logs
   from the service-client IPs.

This prevents the transport from passing acceptance on a client-side
"timeout disappeared" signal while the server is still rejecting the
real identity/config loaded by the client.

### Chaos drill that gates accept

Spawn N pairs of registries with random ID orderings and random
non-empty subset of seedings (A→B only, B→A only, both). For each
pair, register a probe actor on one side and assert it becomes
visible on the other within `bootstrap_timeout`. This pins the
invariant in code so a future refactor of the connection pool cannot
silently regress it.

## Invariant to enforce going forward

> **Connection-convergence invariant.** For any pair of registries
> `(A, B)` where at least one of `A` or `B` has the other in its
> peer set, the pair will reach a state where both can route gossip
> messages to each other within `bootstrap_timeout`, regardless of
> NodeId ordering, regardless of which side was seeded, and
> regardless of TCP-level dial order.

This invariant is owned by `icanact-remote` and verified by the test
in this PR. If the upstream (`icemining`, `icanact-core`) wants to
defend the invariant at the *contract* layer, the appropriate place
is a small acceptance harness that spawns two `GossipRegistryHandle`
under all four (seeded × ordering) configurations and asserts
convergence — the same harness can run in CI of both repos.

## Scope

This branch now includes the failing reproducer, the option-2 transport
fallback implementation, connection ownership/readiness hardening, gossip
tombstone and authenticated-sender fixes, PubSub interest-state cleanup, and
the associated regression coverage. The original reproducer remains the
primary proof that the inbound-stall failure mode stays fixed.
