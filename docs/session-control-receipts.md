# Authenticated physical-session control receipts

`GossipClient::capture_session(&RemoteConnection)` captures evidence of the
**actual observed TLS stream**, not a peer/address cache guess. Capture can fail
if the address was reindexed or there is no certificate-owned stream identity.
The mutable public `RemoteConnection::addr` hint cannot redirect capture.
`capture_session_at_address` is an explicit fresh control-plane observation,
not a way to recover the authority of a previously lost observation.

Receipts are opaque and equality-scoped internally. They do not expose an
ordinal with which callers can fabricate, order, or reconstruct authority.

- `disconnect_session` delegates to transport's native exact-instance CAS.
  A delayed S1 cleanup cannot remove S2. Foreign pool evidence is rejected.
- `set_peer_dns_for_session` requires a matching certificate identity in the
  existing peer record, exact ownership of the requested address index, and
  exact ownership of the peer's current slot. A synchronous address read guard
  plus a per-peer **control-only** mutation guard fences the check/apply boundary.
  The asynchronous gossip-state lock is acquired first; no synchronous guard
  spans an await. Native publication releases its peer guard before acquiring
  address-index writers. Missing/reassigned records are a typed stale outcome.
- `receipt.closed().await` observes this stream's own terminal admission signal,
  including closure before the future is first polled. There is no peer-wide
  callback registration, periodic polling, or implicit reconnect. Canceling a
  wait does not terminate its stream or another wait.

`SessionMutationOutcome::{Applied, Stale, ForeignOwner}` makes cleanup outcomes
visible. Applied teardown is a retirement request, **not proof that IO tasks,
external handles, storage operations, or their resource ownership have ended**.
A closed receipt is likewise not a physical-retirement fence.

Receipt clones retain weak pool/connection identities and small closure-signal
owners. Weak allocation backing and signal backing can survive owner shutdown;
they are not a claim that all metadata has been freed. Clones do not retain
connection payload queues, IO tasks, or keep the pool's strong owner alive.
Caller/framework owners must budget pending observations and closure waiters.
This API does not fix aggregate transport admission or pending-task budgets.

Only verified TLS construction sets the immutable certificate identity field.
A cached/embedded identity, configured peer, signed gossip row, or address hint
is not sufficient. Receipt authority is transport-local, never membership
liveness, supervision/application generation, or registry actor authority.
The existing peer-wide disconnect API remains an administrative operation;
delayed observation-owned cleanup must use the receipt-qualified API instead.

No new mutex is acquired by established current-slot reads or ordinary message
operations. The per-peer mutex is used only by publication/removal and scoped
metadata commit. No background sweeper, additional connection population,
retry queue, serialization format, or dependencies are introduced.

## Regression evidence

`tests/core_session_contract.rs` uses real authenticated S1/S2 sessions and
preserves both legacy behavioral reds in the external TDD evidence archive:
old cleanup closed S2, and delayed old DNS work overwrote S2's hint. Fixture
failures are recorded separately. Current-session DNS and teardown are positive
controls, so rejecting every mutation is not a passing implementation.

`tests/session_receipt_controls.rs` covers foreign owners, altered public
address hints, wrong-address mutation, pending/already-terminal notification,
and cancellation. Private unit controls cover cached/embedded-identity
rejection and publication ordering while current-slot readers still progress.
Synthetic unit state establishes the latter ownership/lock properties, not
certificate authenticity; the authenticated integration tests establish that
production boundary. These controls are not broad endurance/performance or
whole-architecture acceptance.
