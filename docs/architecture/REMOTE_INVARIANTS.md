# Remote Architecture Invariants

This file is the canonical architecture contract for `icanact-remote`. It is
intentionally limited to five invariants. Plans, specifications, and narrower
design documents may add implementation detail, but they may not add a second
mechanism, weaken an invariant, or create an unstated exception.

**Implementation status: `UNPROVED`.** Adoption of this contract does not
declare the current tree conformant. Conformance requires a whole-tree review
that maps every invariant to current, non-vacuous evidence. Existing code is
not exempt because it predates this document.

If an implementation cannot preserve an invariant, change this contract in a
separate architecture-only PR before changing production behavior.

## Invariants

### REMOTE-1 — One authoritative owner per plane

`icanact-remote` owns authenticated transport sessions, route lookup,
connection deduplication, registry convergence, and exact socket/stream
health. It does not own peer-liveness consensus, actor supervision, or
application authority.

- `icanact-core` SWIM is the sole framework peer-liveness classifier. Remote
  transport carries SWIM frames but does not interpret them or create an
  alternate timeout, heartbeat, response-asymmetry, or failure-vote system.
- Registry `DeltaGossip`, `FullSync`, and `FullSyncResponse` converge
  actor and route state. Their cadence and absence are not peer-liveness
  evidence.
- A fire-and-forget `tell` has no reply premise. `Ok(None)` or the absence
  of a response is neutral and cannot increment a liveness failure count.
- Application leases, write authority, fencing, durability, and promotion are
  outside this crate. Connectivity never grants or revokes them.

A hard authentication, EOF, read, or write failure may remove the exact
session that produced it. Automatic fault handling must carry the observed
session generation and conditionally remove only that generation. A SWIM
observation may cause `icanact-core` to request transport reconciliation,
but it cannot authorize an unqualified `PeerId` teardown of whichever
session is current. Explicit operator disconnect and whole-node shutdown are
the only peer-wide lifecycle exceptions.

If SWIM is absent, this crate reports transport facts only. It does not fill
the gap with another peer-liveness detector.

### REMOTE-2 — Messaging hot paths pay no incidental tax

Established steady-state remote `tell`, `try_tell`, `ask`, reply,
framing, enqueue, write, read, route dispatch, streaming, and correlation
paths remain bounded `O(1)` and pay only for the selected API semantics.

No change may add an incidental per-message:

- registry, DNS, address, or peer lookup after a destination is resolved;
- blocking lock, blocking call, or awaited control-plane operation;
- task or timer spawn;
- payload clone, copy, heap allocation, or reference-count operation;
- dynamic dispatch where the path is currently statically dispatched;
- log formatting or metric-label construction; or
- full-table scan, unbounded retry, or control-plane side effect.

Documented pooled and byte-oriented entrypoints remain allocation-free after
warmup and preserve buffer ownership through enqueue and write. Typed
convenience APIs may own the one serialization buffer required by their
declared contract, but transport machinery may not add another allocation or
copy. Pool or queue exhaustion follows explicit backpressure; an
allocation-free path cannot silently allocate a fallback buffer.

Features not participating in a message add zero work to that message.
Hot-path changes require allocator and copy counters plus repeated, same-host
release benchmarks against a checked-in per-API baseline. A statistically
defensible latency, throughput, allocation, or copy regression fails the
change unless this invariant is amended first.

### REMOTE-3 — Evidence never widens in scope

Every remote observation carries explicit provenance and may affect only the
scope it proves. The following remain distinct:

- `SocketAddr`: mutable endpoint metadata or a dial hint;
- `PeerId`: cryptographically authenticated transport identity;
- boot ID: one remote process generation;
- session generation: one physical authenticated stream;
- actor location: an identity-routed actor record; and
- correlation ID plus generation: one in-flight request.

A peer is its cryptographic identity, not its address. Addresses may index or
hint a route, but they never establish identity, authorize a connection,
choose a connection winner, or justify teardown. Every address-index lookup
revalidates the authenticated identity before use.

A `SocketAddr` failure may update that address's dial state. A stream failure
may affect that exact stream. Neither may be resolved to a `PeerId` and then
applied to the identity's current session. Third-party registry claims cannot
establish or overwrite another peer's authenticated binding unless they carry
the subject-owned proof required by that claim.

Malformed and oversized wire data is rejected or canonicalized within
declared bounds before allocation or state mutation. APIs encode provenance
and scope with distinct types and opaque receipts rather than relying on
comments or call-site discipline.

### REMOTE-4 — Stale work cannot mutate successor state

Anything that can outlive the connection, route, request, or registry state it
observed carries that state's opaque generation and revalidates it at the
mutation boundary.

This includes:

- delayed disconnect and cleanup work;
- outbound dial completion;
- simultaneous inbound/outbound connection resolution;
- stream reader and writer exit;
- route and address reindexing;
- ask timeout, cancellation, response, and correlation-slot reuse;
- registry retry and gossip result application; and
- spawned reconnect or supervision work.

Connection removal is compare-and-disconnect against the exact session
generation or source receipt. A stale stream exit cannot remove its
replacement. A delayed address result cannot resolve to a `PeerId` and
disconnect the identity's current connection. A recycled correlation slot
accepts completion only for its full current identity.

Checks occur where current state changes, after every asynchronous gap. If a
successor is current, stale work returns a typed stale result or becomes an
observable no-op; it never mutates the successor.

### REMOTE-5 — All asynchronous work is bounded and overload remains local

Every connection queue, control queue, reply lane, correlation table, buffer
pool, stream assembly, registry delta, full sync, vector clock, retry loop,
timer set, cache, handshake task, reconnect task, and per-peer diagnostic
structure has explicit count, byte, and lifetime bounds.

Saturation returns the documented typed backpressure outcome: queue-full,
NACK, timeout, closed, rejection, or an explicitly lossy drop. It does not
allocate an emergency backlog, create a hidden fallback queue, start an
unbounded retry, or spawn unbounded work.

Ordinary data-plane saturation must not starve transport control or recovery.
SWIM frames, ask replies and NACKs, cancellation, connection teardown, and
shutdown retain bounded admission and complete within documented budgets while
normal writer queues are full. One peer, stream, actor, topic, or payload
cannot consume an unbounded share of another's resources.

Memory reaches a configured plateau under sustained overload. Increasing a
capacity changes a declared bound; it does not remove backpressure.

## Required proof

Architecture is enforced by executable evidence, not document intent.

- Every production change names the affected `REMOTE-*` invariants.
- A behavior change starts with a deterministic fail-first test that fails for
  the intended reason and passes after the implementation.
- Architecture guards contain positive, negative, and non-vacuity assertions;
  source-token scans alone are not proof of runtime behavior.
- Cross-crate changes prove the boundary independently in `icanact-remote`
  and `icanact-core`, then in a production-shaped end-to-end test.
- REMOTE-1 proof includes a quiescent converged registry, fire-and-forget
  success, real socket failure, SWIM failure, and false-suspicion cases.
- REMOTE-2 proof includes allocation/copy counters and repeated release
  benchmarks.
- REMOTE-3 and REMOTE-4 proof includes aliases, mismatched identities, boot
  changes, delayed cleanup, correlation reuse, and session replacement races.
- REMOTE-5 proof saturates every bounded lane, demonstrates a memory plateau,
  and proves control-plane progress during data-plane overload.

Whole-tree conformance reviews report every production-reachable violation,
including pre-existing ones. A known violation is never an implicit exception.
