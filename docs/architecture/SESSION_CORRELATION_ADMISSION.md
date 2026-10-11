# Session correlation and inline reply admission

Status: source changes awaiting the unchanged host validation plan. This does not
establish whole-tree REMOTE-1 through REMOTE-5 conformance or a performance pass.

## Correlation

A request records the physical stream instance that admits it. Stream exit and
synchronous abort cancel only that instance's requests, even when successor and
predecessor transports share the correlation tracker. Pool/address observations
never grant broader cancellation authority. A reply already exclusively claimed
by a completion producer may finish while exit runs; cancellation never exposes
its payload cell for reuse before the owner releases it.

Each tracker has 8,192 slots. Admission performs at most 32 identity/slot probes
and returns `CorrelationTrackerExhausted` on contention, occupied probe window,
or permanent wire-ID exhaustion. Thus backpressure can occur before every slot
is occupied. Zero permanently ends the u32 ID namespace; release never resets
it. Application timeout/cancellation releases its request without classifying
transport or peer liveness. Existing caller timeout/no-timeout semantics remain.

Waker registration uses a nonblocking atomic gate. A contending poll reschedules
itself instead of blocking the executor. Cancellation of exclusive publication
sets an atomic cancellation request; the producer owns destruction and release.

## Route publication

A connection retains at most 65,536 bindings. Each used direction owns 131,072 immutable atomic publication cells,
with eight probes per lookup. The byte budget is 131,072 times the target
`size_of::<PublishedRoute>()` per used direction. Allocation happens once at
first cold binding; unused route tables retain no publication array. Cold route binding uses the existing serialization domain; warm
outbound asks and inbound resolution do not take that lock or wait on the bind
gate. The bind enqueue commits the outbound receipt. Cancellation before that
point removes only its unpublished route; published cells are never reused.

Outbound cache probe exhaustion or route/ID exhaustion sends the existing
uncompact ActorAsk representation. Incoming bindings that exceed count or probe
capacity are rejected as invalid route binding, without overwriting another
binding. Tables and all publication cells live only for their physical stream.

## Pooled replies

Inline pooled replies retain the original PooledPayload and optional 16-byte
prefix as separate objects. Serialization writer/arena capacity or pooled byte
capacity, rather than the visible slice length, supplies the retained-owner
charge. The existing `RESPONSE_BATCH_BYTE_CAP` bounds pooled batch admission;
a response that exceeds available admission becomes a Backpressure NACK.

Moving a batch to a partial writer preserves its full retained charge until all
owners are released. A fixed 64-entry stack iovec array writes headers, prefixes,
and bodies without copying them. Two reusable frame vectors avoid allocating a
new vector for each warm batch. Public actor byte replies now require
ForwardPayload owner provenance. Vec, Box, pooled and known aligned owners move
without copying; arbitrary Bytes require an explicit copy at the producer
boundary. The production immediate pipeline no longer normalizes replies.
Pre-dispatch admission reserves one STREAMING_THRESHOLD of headroom; unusually
large backing owners still undergo exact post-handler admission.

Leased normal-reply publication moves ownership through a one-entry queue rather
than allocating an additional Arc around each publication. A close racing a
publisher marks close requested; the publisher releases its payload and permits
before finishing cleanup. Only the unique resource reclaimer publishes the
terminal state after dropping permits. Resources use an exclusive-claim cell,
with no recycling mutex. Close does not spin waiting for a preempted producer.
The last active reservation completes a requested queue drain.

## Receiver-enforced terminal ownership

The mandatory `TerminalOutcomeWindow` Hello feature binds both peers to 64
outcome cells per physical stream, indexed by full correlation ID modulo 64.
Nonblocking enqueue admission performs one cell claim and rolls back unsent
batches on failure. Async admission waits on preallocated borrowed semaphores,
within the existing caller deadline. Batches acquire cells in a global order to
avoid circular waits, and cancellation releases every unsent prefix reservation.
Stream close wakes admission waiters even when stale handles remain alive.
Once published, ownership belongs to the transport independently of the waiter.
A full-ID transmitted bit prevents an unsolicited response to a queued-but-unsent
request from recycling it. Only the single IO owner reads and writes its framed
observer, so terminal reception cannot race ahead of its own send-commit callback.

The receiver enforces the same bound before dispatch. Every admitted request owns
its outcome cell until its entire terminal frame reaches the underlying writer.
A NACK queue of exactly 64 therefore retains every possible admitted NACK without
blocking unrelated reads. Over-window or colliding raw peers fail with an explicit
protocol error, rather than expanding the lane or evicting an existing outcome.
First-frame ask and stream receipts cross the accept handoff once, preserving this
invariant even when the first frame preceded IO-task construction.

Inline, vectored, batched, leased and streamed responses return credit only after
full terminal bytes. Stream receipts use preallocated radix indexes with at most
32 direct child steps; no active-stream table sweep occurs per chunk. Contiguous
fixed metadata is decoded directly from borrowed headers. Only fragmented headers
use small scratch copies, and tell bodies are skipped without copying payload or
retaining it. Generic scatter framing validation gathers fixed metadata only.
Request-stream cancellation is an explicit ordered Abort; cancelling an application
waiter alone does not free credit. Response-stream Abort does not replace its
following terminal reply.

Host acceptance still requires deterministic regression execution, saturation
and owner-release counters, and repeated same-host release allocation/copy and
latency/throughput measurements. No remote build, test, formatter, or benchmark
was run to produce this source patch.
