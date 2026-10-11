# Admission and session-provenance migrations

These are source changes awaiting the unchanged full host validation matrix.
Architecture remains **UNPROVED**. No benchmark, allocator, cross-crate SWIM or
sustained-overload acceptance is claimed by this document.

## Actor timeout and cancellation

Application timeout and cancellation never retire or reclassify a healthy
transport. `ConnectionRecoveryPolicy.evict_peer_on_ask_timeout`,
`evict_peer_on_ask_cancel`, and `consecutive_timeout_threshold` remain accepted
compatibility fields but are inert. `note_peer_ask_streak_timeout` returns false.
A hard-fault notification without an exact physical instance is also a no-op.
Explicit operator disconnect and shutdown retain their existing lifecycle role.

`retry_actor_ask_once_after_timeout` is an explicit potentially duplicate replay
opt-in for idempotent operations. It retries once, with one fresh application
budget, on the original transport; it does not evict, redial or create a peer
liveness observation. Leave it disabled for potentially non-idempotent requests.
Healthy cached sends avoid recovery-watch subscription; disabled timeout replay
also avoids a speculative payload clone. Remaining shared connection-reference
work has not been certified allocation/refcount-free.

## Bootstrap admission

`bootstrap_non_blocking(Vec<SocketAddr>)` now returns `Result<()>`.
`GossipConfig.max_bootstrap_dials` defaults to 64 simultaneous dial tasks shared
across all calls on the handle, clamped to the platform semaphore maximum.
Zero disables nonempty batches. A batch reserves its entire count before spawning;
insufficient capacity returns `WriteQueueFull`, starts no task, and retains no
background seed backlog. Each admitted task holds one address and one permit,
and its connection attempt ends within `connection_timeout`. Cancellation/error
releases admission. Authenticated session finalization owns connection status.

## Registry wire admission

Vector-clock compaction is not wire admission. The separate published constants
`MAX_INGRESS_VECTOR_CLOCK_ENTRIES` (4096 per clock) and
`MAX_INGRESS_REGISTRY_CLOCK_ENTRIES` (65536 summed across one registry frame)
are checked on archived Delta/FullSync variants before actor/change vectors or
clock Vec/BTreeMap materialization. Standalone VectorClock deserialization also
rejects over-limit archived clocks before materialization. Existing raw-ask decode
fallback behavior remains: data that is not admitted as a registry message can
still be handled as raw application payload under that API's existing rules.

## Session provenance

`GossipResult` includes `session: Option<SessionReceipt>`. Capture it using
`SessionReceipt::from_handle` from the authenticated handle actually used before
an asynchronous send. Missing/stale/foreign receipts cannot mutate sequence or
failure state. A supplied receiving-stream correlation tracker is authoritative;
missing provenance and rejected correlations never fall back to mutable addresses.
Shared trackers continue to support bidirectional replies with unique non-reissued
wire IDs. Physical session ownership independently fences exit cancellation.

Production registry merges and discovery completions retain physical receipts;
local administrative merge APIs are deliberate trusted local operations, not wire
fallbacks. DNS is a dial-route update under the serialized owner boundary, not a
migration of a physical stream or another connection's negotiated capabilities.
Third-party DNS names remain untrusted discovery hints; explicit operator DNS
configuration remains supported. First-frame processing remains behind existing
publication/identify handoffs, without an extra message-path timer or task.

## Response owner provenance

`ActorResponse::Bytes` and `AskDisposition::ImmediateBytes` now carry
`ForwardPayload`. Construct it from a whole Vec, boxed slice, static bytes or an
accounted pooled owner; opaque `Bytes` requires an explicit caller copy. Aligned
pooled buffers and aligned vectors preserve backing ownership and capacity through
response enqueue, including small slices. An opaque aligned Bytes owner is rejected
rather than silently normalized. Immediate responses and NACKs use the captured
receiving stream, never a fresh address lookup. Pooled prefix responses retain the
prefix separately and stream the original owner when oversized.

## Resource API details

- [Pooled and forwarding admission](POOLED_ADMISSION.md) documents checked aligned
  checkout, bounded byte encoders, `ForwardPayload`, and explicit serializer and
  forwarding hot-path residuals.
- [Pubsub and registry owner admission](PUBSUB_OWNER_ADMISSION.md) documents typed
  installation/subscription rejection, retained-owner publication, bounded refresh,
  guaranteed reservation release, and cooperative callback contracts and
  remaining liveness-evidence limits.
- [Session correlation admission](architecture/SESSION_CORRELATION_ADMISSION.md)
  documents exact cleanup, bounded route publication, reply ownership, and the
  mandatory receiver-enforced terminal outcome window.

Mutable pool index maps are internal. Use admitted route setters and read-only
snapshots; a read snapshot does not confer authenticated session authority.
Treat all new typed backpressure as a caller-visible outcome rather than panicking,
silently dropping an ask, or replacing rejection with emergency allocation.

## Mandatory terminal ownership compatibility

Every Hello now advertises `Feature::TerminalOutcomeWindow`. Connections to an
older peer, including an otherwise compatible V6 peer with the same schema hash,
are rejected during Hello if that feature is absent. Upgrade both endpoints;
there is no permissive fallback or assumption that the old `ask_window` setting
provides receiver-enforced admission.

Each physical stream admits at most 64 request outcomes, one per full correlation
ID's modulo-64 cell. Nonblocking admission returns `WriteQueueFull` for a busy
cell. Async admission waits on a preallocated borrowed cell permit under the
existing enclosing deadline; it does not require high-concurrency callers to
retry. Multi-cell batches use a global acquisition order and return all unsent
reservations on cancellation. Stream close wakes blocked admission. Application
timeout, waiter cancellation and correlation-slot recycling do not return
transport credit. Full terminal reception returns sender credit; full
terminal transmission returns receiver credit. Partial replies, stale IDs and
foreign streams cannot return it. A fully serialized request-stream Abort returns
its incomplete request ownership in byte-stream order; a response-stream Abort
still waits for the following terminal response. Receiver overrun is an explicit
protocol error on the offending physical stream, without evicting an accepted
outcome. Full NACK storage no longer prevents reading control or reverse replies.

Generic `write_owned_chunks` validates complete framing across chunk boundaries
and acquires all ask credits before publication. Invalid/truncated framing is a
local error; application bodies retain their original owners. Nonblocking batches
that exceed available cells must be retried after terminal consumption. A single
batch larger than 64 requests must be split; ordinary concurrent async asks wait
under their existing deadlines.

## Acceptance

The four host commands and their strict formatter/build/Clippy/rustdoc/default,
test-helper/all-feature debug/release lanes are unchanged. New deterministic tests
are authored as intended fail-first regressions, not recorded remote executions.
Remaining confirmed engineering defects or missing required measurements mean
**INCOMPLETE** remediation even if a subset of functional checks passes.
