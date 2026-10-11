# Pubsub and registry-owner admission

Status: implemented candidate, host verification pending. The canonical REMOTE-1
through REMOTE-5 contract remains unchanged and architecture status is UNPROVED.

## Pubsub API migration

`subscribe_bytes`, `subscribe_borrowed_bytes`,
`subscribe_borrowed_bytes_with_metadata`, and `subscribe_type_bytes` now return
`Result<PubSubSubscription, PubSubAdmissionError>`. Handle the error before using
or storing the RAII handle. Errors distinguish global worker capacity, topic
capacity, per-topic subscriber capacity and fallback worker creation failure.
No rejected subscription publishes a subscriber map entry or borrowed fast cache.
If creating a fallback interest worker fails after a delivery worker was prepared,
its unpublished owner is canceled and retains its permit until teardown completes.

All four APIs now require `CooperativeCallback<F>` rather than an arbitrary
synchronous callback, and `F` returns a `Send + 'static` future yielding `()`.
For example:

```rust,ignore
let subscription = pubsub.subscribe_bytes(
    topic,
    type_hash,
    CooperativeCallback::new(|payload: bytes::Bytes| async move {
        consume_with_bounded_async_work(payload).await;
    }),
)?;
```

Callback invocation, every future poll, and cancellation destructors must perform
bounded work, return promptly, and never block a thread or start detached work.
Finite synchronous operations can return `std::future::ready(())`. Borrowed forms
can inspect their slice during invocation; pending work must own any needed input.
The engine owns the active future directly, including on its fallback runtime.
Unsubscribe or engine drop cancels it. An active delivery has a 30-second deadline;
expiration drops that future, increments `callback_expirations`, and continues with
the next queued delivery. The permit is held through future/callback destruction.
There is no `spawn_blocking` escape hatch. Arbitrary blocking code is outside the
explicit cooperative API contract and cannot be made preemptible by a Rust timeout.

`RoutedPubSub::install` keeps the defaults. `install_with_limits` accepts
`PubSubLimits` and returns `Result<Arc<RoutedPubSub>>`; excessively large semaphore
capacities are rejected as `InvalidConfig`. Defaults are:

- 256 live delivery-worker permits per engine, including cancellation teardown;
- 64 subscriptions per topic across owned/borrowed forms and all type hashes;
  wildcard type subscriptions have the same per-type limit;
- 256 topic interest records, including outstanding unregister work;
- 4096 inspected actor-owner records and total retained destination records;
- 64 queued deliveries plus one active callback per worker;
- at most 1 MiB retained allocation capacity per admitted local payload;
- 4096 interest lifecycle receipts per registry, shared across engines, covering
  live registrations, queued cancellation, and pending removal gossip.

The resulting conservative local-delivery envelope is
`subscribers * 65 * 1 MiB`, excluding user-owned callback captures and payloads a
callback deliberately retains outside the library. This is a bound, not a claim
that that default envelope has been exercised or is suitable for every deployment.
Use smaller subscriber counts where a lower envelope is required.

`publish_bytes` now accepts `ForwardPayload`, an ownership proof shared with
forwarding admission. Move a `Vec<u8>`, boxed slice or pooled payload into it, or
use `ForwardPayload::from_static`. Arbitrary `Bytes` views do not prove their
backing allocation size; converting them requires the explicit copying convenience
`ForwardPayload::copy_from_slice`. Retained-capacity oversize returns
`GossipError::WriteQueueFull` before local queue admission or remote forwarding.
Typed publication retains its existing serialization/type-prefix semantics and
moves its final owned buffer directly into the proof. Remote-only publication
continues accepting Bytes because it serializes synchronously into the bounded
pooled transport frame rather than retaining the input in subscriber queues.

Best-effort subscriber queue saturation remains explicitly lossy: the newest
delivery for that subscriber is dropped and `subscriber_queue_drops` advances.
Borrowed local ingress rejects oversize before copying. Subscription permits are
owned by the worker, so removing a subscription cannot free admission before its
active cooperative future and callback captures are destroyed.

Refresh requests coalesce rather than queue behind an in-flight refresh. A refresh
makes at most four revision-stable snapshot attempts, yielding between attempts.
Actor names longer than 256 bytes or more than the configured record budget reject
that rebuild before cloning unbounded metadata. Only actor names and PeerIds are
copied, never actor metadata/vector clocks. Provider destination amplification is
rejected before route publication. Budget rejection increments
`refresh_budget_rejections`, retains the last complete snapshot without falsely
acknowledging a revision, and applies a one-second cooldown. Interest dispatch
remains one flight per admitted topic and yields on churn.

`PubSubRouteProvider::group_destinations` now receives a library-owned
`&mut PubSubRouteSink` and returns `Result<(), PubSubRouteError>`. It no longer
returns an unbounded `HashMap`. The library admits the destination count before
creating the sink. Route each input index exactly once with `routes.route(index,
next_hop)?`; missing, repeated, or invalid indices fail the whole result. Errors
are sticky even if a provider ignores them. Variable provider traversal must call
`routes.step()?` for every bounded unit of work. Each invocation has eight steps
per destination (minimum one) and a five-millisecond cooperative deadline. Provider
code must not block, allocate unbounded temporary output, or spawn detached work.
Every publish, ingress, and refresh provider call uses this checked path. Failed
refreshes retain the last complete snapshot; explicit publish and ingress surface
local overload as `WriteQueueFull`. No partial route map is published.

Interest dispatch is single-flight per topic with an engine-owned cancellation
handle and an exact registry publication receipt. Subscription churn cancels the
old receipt synchronously, before a waiting registration can commit. Canceling an
already published receipt removes its exact local/reflected actor immediately,
even while the async gossip lock is unavailable. A prior receipt cannot remove a
successor. A pre-reserved cleanup slot carries the causal removal into ordinary
lossless gossip; cleanup never depends on a fallible best-effort send or detached
task. The same publication lock fences cancellation/queueing and successor clock
merging. Failed registration remains pending and retries at a bounded rate rather
than falsely acknowledging success.

Cleanup receipts retain their admission through the pending/urgent gossip stage.
Capacity reopens only when a normal gossip round moves those changes to bounded
`max_delta_history` retention. Draining the cleanup queue alone cannot recycle
permits into ever-growing pending vectors. If gossip is stalled, further interest
registration returns local overload and retries within the admitted topic worker;
existing subscriptions and removals retain ownership. This bounds interest input
per round, not unrelated registry producers or application callback memory.
Immediate fanout futures are scoped to their caller, so cancellation drops them;
the regular pending copy remains the lossless source if best-latency propagation
fails. Data already sent cannot be recalled, and causal successor clocks remain
necessary for remote convergence.

Deterministic candidate regressions cover cooperative cancellation/deadline queue
progress, complete/sticky provider output and step saturation, oversized request
pre-admission, engine drop while waiting for the registry lock, exact reflected
actor removal, stale-receipt successor protection, pending-gossip receipt
saturation/recovery, and preservation of prior state on failed registration.
These regressions have not been compiled or executed in this static-only pass.
Runtime memory, cancellation-latency, allocator/copy, and whole-stack acceptance
remain unproved.

## Registry owner

`RegistryOwnerHandle::with_limits` takes `RegistryOwnerLimits` and validates
representable channel capacity/lifetime. Defaults are 65536 ownership entries,
65536 session receipts, 65536 permanent configuration-generation fences, 512 reap
reservation/cleanup slots and a 30-second pending-reap lease. Identity/address/
generation records are fixed-size; configure fences intentionally persist for the
owner lifetime rather than expiring into stale-retry ABA. New entries return
`ClaimRejection::Capacity`; refresh of an admitted entry still proceeds.

Every reap guard reserves its cleanup-channel slot before publication. Both explicit
release and Drop synchronously fill that owned slot without awaiting or fallible
try_send. Guard plus queued-release cardinality cannot exceed the configured
reservation count. `try_reserve_for_reap` distinguishes typed Capacity/Closed errors
from `Ok(None)` stale/conflicting evidence; the existing Option method remains a
compatibility skip-on-rejection wrapper. Pending authority expires and is reclaimed
on the owner's one-second control tick. Release carries exact guard identity, so a
late expired guard cannot remove a successor. Production destructive work acquires
its owned gossip guard before the reservation deadline, then submits one
owner-owned synchronous mutation. Exact consumption, owner release, destructive
projection and reservation removal finish in that non-awaiting owner turn, even
if the response is canceled. Active authority is never expired while an earlier
mutation can still execute.

Custom RoutingPublisher implementations must return Result<()> from
set_configured_peer_addr; optional reserve_peer_session returns an erased RAII
admission guard held through the owner command. set_discovered_peer_addr also
returns Result<()>; its default is a no-op for publishers without a dial index.
Capacity failure must precede route mutation.

Owner configure/migrate retain bounded pool admission through route publication.
Failure precedes ownership/pin mutation and returns a typed capacity outcome.
Registry projections can hold `projection_guard` while revalidating their exact
receipt and mutating under the gossip lock; no owner await is permitted while
holding it. This shares the publication boundary with all owner commands and does
not add a messaging hot-path lock.

Historical address-generation receipts now use fixed-capacity, permanent rows;
lifetime distinct addresses consume this capacity until owner shutdown. Rows are
never silently evicted or recycled, so absent/claimed/released ABA remains fenced.
At saturation, refresh/reuse of admitted addresses works and unseen addresses fail
closed. This requires deployment capacity planning.

Remaining engineering: cancellation before rollback command submission, including
its gossip-lock wait or earlier speculative claim/projection, can still lose
compensation. Complete repair requires moving speculative claim admission and
projection into the same owner-owned transaction. The later submitted rollback is
cancellation-owned and returns the actual compensation result. Legacy index-only
routes lacking an existing pool peer-session can still fail restoration admission;
the failure is no longer masked as success. Full memory-plateau, latency,
allocation/copy, feature-build and cross-crate conformance evidence remains pending.
