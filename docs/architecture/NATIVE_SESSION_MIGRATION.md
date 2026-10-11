# Scoped exact-session requests

Status: source implementation and regression candidates; host execution and
whole-path performance acceptance remain pending. REMOTE-1 through REMOTE-5
remain unchanged and architecture conformance remains **UNPROVED**.

## Canonical repeated-operation contract

`RemoteActorRef` is a reconnectable locator. Call `remote.session().await?` once
before a batch or worker starts. `RemoteActorSession` retains one exact physical
connection generation and borrows its locator. Requests borrow that endpoint;
no healthy operation reloads `ArcSwap`, acquires another connection owner, or
upgrades the weak registry. A separately acquired session may observe a successor.
An existing endpoint and its deferred requests never change transport generation.
`RemoteConnection` and `ConnectionHandle` likewise are persistent exact-generation
owners: retain one per worker rather than cloning them per request.

Use `ForwardPayload` with the native endpoint methods:

- `tell_owned`, `try_tell_owned`, `tell_actor_frame_owned`, `try_tell_actor_frame_owned`
- `ask_owned(request, timeout)`, `ask_no_timeout_owned(request)`
- `ask_direct_owned`, `ask_direct_no_timeout_owned`, `ask_direct_owned_with_id`
- `ask_actor_frame_owned`, `ask_actor_frame_no_timeout_owned`
- `ask_deferred_owned`, `ask_actor_frame_deferred_owned` with a wait borrowed from the endpoint

Owned asks return `AlignedBytes`. Their complete request owner survives writer
admission, and their response owner is returned without `Bytes::from_owner`.
Timed requests retain the original one-deadline admission-plus-response behavior;
no-timeout requests add no timer. Raw legacy `ask` has a 30-second default: its
native equivalent is `ask_owned(request, Duration::from_secs(30))`, not the
no-timeout variant. Nonzero explicit direct request IDs retain replay identity.
Actor timeout retry still requires the existing explicit configuration opt-in.
Only that opt-in shares its request owner to allow exactly one replay on the same
session with the original fresh retry budget. It never evicts or dials because
of timeout. Hard errors repair the locator for later acquisition; ambiguous
requests are never replayed by recovery.

Deferred asks borrow their exact endpoint and tracker and cannot be moved into an
unrelated `'static` task. Own an endpoint once inside the task, then issue and wait
there, or poll borrowed requests concurrently within their endpoint's scope.
Cancellation releases only the original correlation reservation. No endpoint
lifetime expiry grants authority over a successor.

## Bounds and scope

An endpoint adds one cold retained connection owner and no request queue, task,
sidecar allocation, or additional correlation ring. Request count/byte admission
and retained-byte budgets remain the configured existing writer and correlation
limits. Saturation retains the existing typed errors; it neither allocates a
backlog nor discards a live cleanup obligation. Session lifetime is caller-owned
and bounded by its locator borrow, while request lifetime is bounded by its
endpoint borrow and the selected timeout or explicit cancellation. No timeout is
invented for APIs whose contract is unbounded waiting.

## Explicit compatibility costs

The `RemoteActorRef` tell/try-tell/ask/typed/streaming convenience methods remain
**acquisition adapters**, each selecting and retaining a connection owner per
call. `connection_ref` is also cold owner acquisition. They support one-off use
and tests specifically exercising locator recovery. They are not the canonical
steady-state path. The former locator-level `ask_deferred` is removed: acquire a
session first, because a borrowed deferred handle cannot escape a temporary
connection owner.

On persistent endpoints, Bytes-oriented `ask`, `ask_bytes`,
`ask_with_timeout_bytes`, `ask_direct`, `ask_direct_with_id`,
`ask_direct_no_timeout`, actor-frame Bytes methods and Bytes streaming methods
remain compatibility APIs. They may normalize/copy arbitrary Bytes views and/or
promote aligned response owners into reference-counted Bytes metadata. The
`*_aligned` Bytes-input actor variants still normalize request storage. Native
`*_owned` methods avoid those conversions. Typed requests use their declared
serialization owner and native aligned responses; explicit `ArchivedBytes::into_bytes`
and `decode_typed_archived(Bytes)` remain compatibility boundaries.

Authentication-attributed actor requests and caller-supplied actor request-ID
Bytes adapters preserve their existing identity/replay semantics. They are not
silently replaced with unattributed native requests. Tests exercising those
boundaries intentionally retain the adapter calls.

## Fixture migration and acceptance

The steady-state throughput/probe and proxy-round-trip fixtures now retain one
endpoint per batch and use complete static native owners or uniquely owned
per-request identity payloads. Concurrent/offered-load helpers acquire once
before their measurement clock. Console workers own a connection once per worker;
request loops borrow it. The realistic correlation and lookup-demo fixtures use
scoped requests; batched concurrency uses joined borrowed futures instead of
per-request endpoint-owning tasks. Reconnect measurement reacquires only at an
observed reconnect boundary. Cold-connect benchmarks, one-off identity/error
fixtures and locator self-heal tests intentionally keep acquisition adapters.

These are changed measurement contracts. Re-run the same scoped/native scheduling
and ownership on both baseline and candidate before comparing performance. Prior
measurements with per-job ref clones, Bytes promotion, or spawned task ownership
are not equivalent baselines. No throughput, latency, copy or full-path allocation
pass is asserted here.

`exact_session_endpoint_tests::scoped_raw_and_direct_asks_preserve_owners_and_exact_generation`
authors timed/no-timeout success and cancellation controls, physical wire output,
request pointer/capacity, response pointer, foreign tracker rejection, successor
publication, exact endpoint/stream/tracker owner counts, and nonvacuous allocator
and clone-positive controls. Its allocation scope is producer enqueue/completion
only; background writer/read work and cold setup are excluded and require
separate host evidence. Existing cancellation, timeout and successor regressions
remain enabled. All runtime/build/formatter validation is delegated to the host.
