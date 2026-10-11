# Registry transaction admission and cancellation

This describes the candidate control-plane implementation. It does not establish
whole-tree conformance; `REMOTE_INVARIANTS.md` remains UNPROVED.

## Inbound speculative ownership (REMOTE-1, REMOTE-3, REMOTE-4, REMOTE-5)

An inbound candidate admits a journal before taking its preclaim snapshot.
Gossip exclusion remains owned by the submitted owner command through its claim
commit. The command validates address history for both the attributed address
and observed-source alias. The owner installs compensation before delivering the
claim receipt. The caller holds only a decision token: dropping it, including
while awaiting gossip, wakes compensation without creating an unbounded release
queue. A preclaim rejected as stale, expired, or saturated publishes no ownership.

The speculative publication phase is synchronous: its decision transition
excludes expiry until the tie-break and index operations finish. Later session
health updates use exact committed receipts. Lease expiration never evicts a
healthy transport that has committed its decision token.

Rollback distinguishes `Restored`, `Superseded`, and `RestorationFailed`. Exact
owner/session release history permits compensation when the candidate's own IO
exit ran first; intervening claim/release ABA or a successor generation forbids
it. Secondary aliases additionally require unchanged address history and physical
connection identity. The journal restores the captured fallback route independently
of a session-preferred route, and does not create a missing PeerSession. This is
necessary for sessionless restoration when unrelated identities fill admission.

### Budgets and failure behavior

- Pending plus compensating journals: `RegistryOwnerLimits.reservations` (512 by
  default), admitted without waiting. This is separate from the bounded reap lane.
- Per journal: at most 8 MiB conservative retained-data charge before cloning.
  Charge includes sparse actor-name set capacity, repeated names, metadata/address/
  DNS capacities, and 4096 bytes per live vector-clock entry. This is an explicit
  conservative accounting model, not an allocator measurement or a proof about
  the standard library BTreeMap allocator internals. Location payloads are
  captured under the same map read guard as charging, and a bounded VectorClock
  helper charges and deep-copies one immutable clock snapshot, so concurrent
  clock growth cannot cross the admission decision.
- Aggregate journal charge per registry owner: 32 MiB. Charged capacity remains
  held through cleanup. History uses at most two small epoch receipts, not a
  retained routing-map snapshot. Physical stream witnesses are Weak references,
  so journals do not retain retired stream buffers. Existing PeerSession holds
  remain charged against the pool identity count; full retained-allocation
  plateau evidence is still required.
- Pending lifetime: `RegistryOwnerLimits.pending_reap_lifetime` (30 seconds by
  default), applied to gossip acquisition, mailbox submission, and owner-side
  claim admission. Saturation is `GossipError::WriteQueueFull`; expired or stale
  owner receipts are typed claim rejections.
- Committed compensation does not expire while waiting for gossip. Expiring it
  would lose cleanup. An externally held gossip guard can therefore delay cleanup
  indefinitely while retaining its bounded count/byte charge.
- Unexpected restoration failure keeps the same journal and permits in a parked
  fail-closed task. `RegistryOwnerHandle::speculative_cleanup_failures()` exposes
  the number of such retained failures; each emits one warning. There is no blind
  retry or automatic recovery path. This remains an engineering lifetime/recovery
  residual, and may retain a registry until runtime shutdown. It must not be
  reported as complete remediation.

Lock order after acquiring asynchronous gossip state is owner publication,
disconnect publication, then pool session publication. Compensation never awaits
while holding the synchronous publication guards. A blocked cleanup task cannot
block unrelated owner commands.

## DNS route migration (REMOTE-3, REMOTE-4, REMOTE-5)

Resolution and source-generation capture still precede migration. After resolution,
DNS takes owned gossip exclusion, validates the source and destination projections,
and transfers the guard into an owner command. Owner migration and the derived
peer/actor-address/known-peer moves run in that one synchronous command. Canceling
the caller after submission cannot leave ownership moved with gossip state at the
old address. No reverse migration is needed. Physical streams, capabilities and
clock samples are not moved by DNS.

The historical release/all-feature ConnectionRefused failure is not established
as fixed by static inspection. The existing integration fixture, full feature
matrix, and new deterministic cancellation/ABA/resource tests require host execution.
