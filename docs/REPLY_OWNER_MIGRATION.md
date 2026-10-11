# Scoped reply owners and exact-session delivery

The reply APIs use complete payload owners, scoped claims, and explicitly bound
connection sessions. This is an authorized source API migration; wire framing
and at-most-once correlation semantics are unchanged.

## Direct replies

`AskContext::responder()` now returns `AskResponder<'_>`. It borrows the context's
single-use claim and physical response sink. Creating and cloning an unobserved
responder does not allocate a claim object or increment a connection reference
count. A responder cannot outlive the context. Its direct owned and typed reply
methods preserve the original allocation and account its complete retained
capacity, including unused Vec and pooled-buffer capacity.

`ResponseWriter` binds once. A delayed claim cannot resolve an address to a newer
stream. An immediate enqueue failure still consumes the claim unless the caller
uses a `*_with_fallback` method. That fallback retains the same exclusive claim,
observer notification state, and exact stream.

## Deferred replies

Create a `ReplySession` once for an exact physical source connection, using
`AskContext::reply_session()` or `AskResponder::reply_session_with_capacity(n)`.
Retain that session in the queue/worker owner. Move the scoped responder into a
`ForwardReplyToken` using `into_forward_reply(&session)`, then use
`session.reply_owned(token, payload).await` or the nonblocking reply methods.

Tokens are opaque numeric capabilities. Copying a token does not create another
reply claim: the endpoint atomically claims its full generation exactly once.
A foreign endpoint, completed token, or stale generation cannot mutate a newer
claim. Generation exhaustion permanently retires that cell, rather than
reissuing an identity. A reply session itself also has a non-reissued identity.

A queue that might reject admission uses `prepare_forward_reply(&session)`.
The returned scoped admission exposes `token()` for queue storage. Commit it
immediately after publication; dropping an uncommitted admission cancels that
exact unpublished token and restores the original context's claim. The queue
must cancel accepted tokens on rejection, drain, timeout, task cancellation,
and panic. `AskForwarder` implements this ownership transfer internally.

A default reply session has 64 entries. Explicit capacities must be between 1
and 65,552 inclusive. Saturation returns `GossipError::WriteQueueFull` before
claiming the source context. Tokens retain fixed metadata until completion,
cancellation, or session destruction; a queue's configured job deadline bounds
its deferred work. Lost numeric tokens cannot allocate a backlog, but occupy
capacity until the session is dropped. Sessions never follow replacement
connections and must be recreated for a new physical source stream.

`try_reply_owned_immediate` uses the reserved immediate response lane.
`try_reply_owned_with_fallback` returns `ReplySessionTryError::Enqueue` with a
`ReplySessionFallback` when enqueue fails after claiming a token. That fallback
preserves the exclusive generation for a retry. A duplicate/foreign token is a
separate claim-unavailable error and never grants retry authority.

## Leased delivery

Register a delivery budget once against the retained reply session:

```rust,ignore
let reply_session = context.reply_session()?;
let budget = ReplyDeliveryBudget::new(job_limit, byte_limit, cancellation_owner)?;
let delivery = budget.bind(&reply_session)?;
let lease = context.responder().try_reply_lease(&delivery, max_retained_reply_bytes)?;
lease.reply_bytes(reply_owner).await?;
```

`ReplyLease<'_>` borrows `ReplyDeliverySession<'_>`, independently of the original
context borrow. Do not create a fresh registration per reply. A connection owns
at most 64 delivery registrations and 16 MiB of complete cancellation payload
allocations. Duplicate registrations are conservatively charged again.
Registration entries and cancellation owners remain connection-owned until
teardown. Exhaustion returns typed backpressure; there is no registration
replacement or unbounded lookup table.

A connection has 64 fixed reply cells. The configured job and byte limits bound
live delivery reservations independently. Normal reply admission charges the
complete retained payload allocation against its requested reservation; the
connection separately accounts its cold cancellation-owner storage. Admission
uses bounded atomic attempts and returns `WriteQueueFull` on contention or
saturation. It does not create semaphore-owned permits, waiter nodes, timers,
or per-reply shared owners.

The ready queue and IO command carry a non-cloneable numeric writer token. Its
exact arena, stream instance, and generation identify the immutable record.
The sole IO owner retains the connection table once. Scoped writer views borrow
that owner, so close cannot recycle their records. The writer drops staged
payload owners before acknowledging completion and returning the job/byte
charge. A retained caller lease pins its own completed cell until it drops.

Ordinary close stops publication and requests completion. An active writer's
payload remains charged until the writer acknowledges it or exits. Writer exit
coordinates with the final admission/activation publisher and the sole close
drainer; it does not race the ready queue for ownership, spin, or release a
successor's generation. The IO cancellation path drops staged payloads before
its cold owner reclaims abandoned writer tokens.

## Explicit payload sharing

`ReplyPayload` no longer implements implicit `Clone`. Ordinary replies move a
unique native owner. For deliberate fanout, call `into_shared()` once, then
`shared_clone()` on that explicitly promoted value. This operation preserves
alias pointers and complete retained capacity; each transport reservation still
charges the full owner. Copying is available only through the explicitly named
`copy_from_slice` convenience constructor. Observers borrow the original owner
while publication is protected; no observer-only payload clone is needed.

## Qualification

Tests cover direct owner-pointer/capacity preservation, explicit fanout controls,
zero-allocation warm reservation/token cycles, saturation and rollback, foreign
and stale tokens, permanent generation exhaustion, close during publication,
staged-payload accounting, and writer exit while the final publisher owns close.
These tests are authored for the unchanged host validation matrix. Remote work
has not executed Cargo, builds, tests, lint, formatting, or performance runs.
