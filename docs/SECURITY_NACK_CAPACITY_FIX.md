# S13 — bound pending terminal NACKs

## Finding and disposition

User-supplied Codex Security finding `commit:cb0f1d4108348191ac0697f6e2bebb67`, **High**, “Read-gate change permits unbounded NACK queue growth,” reported against commit `541254e8e54208fe8449da2878fbaab6a6d69dcc`.

The issue was still present after integrating main's cleanup #237. Both batch-read guards allowed unused deferred actor-ask capacity to bypass the pending-NACK cap. Valid DirectAsk frames do not enter that deque, but each queues a NoDispatcher NACK. Prior backpressure corrections therefore did not establish this particular bound.

The finding was reproduced and corrected locally. Complete combined-tree validation now passes; the update is prepared for draft PR #238. The dashboard's finding status has not been changed; the affected main branch must not be described as fixed before integration.

## Scope of correction

- Both primary and idle batch-read guards require pending-NACK room, including occupancy of an already-popped partial NACK. Deferred actor-ask capacity is no longer an alternative admission condition.
- The idle select's first read is entered only with no ordinary pending write, no deferred asks and no queued NACKs; subsequent batching uses the same hard capacity gate.
- Queue insertion independently rejects capacity exhaustion without allocating another entry or evicting an existing terminal outcome.
- Capacity failures use a private typed error, distinct from transient streaming admission's WouldBlock. All six fast/generic dispatch error boundaries fail closed on that invariant violation, rather than consuming an ask and swallowing its failed NACK as ordinary streaming pressure.
- Deferred-hold exhaustion also reports an explicit error, rather than silently discarding an already-read ask. Existing capacity and preservation assertions remain; the formerly ignored final overflow now requires rejection.
- Keep the existing NACK write owner, partial offsets, eight-per-turn drain budget, shutdown/cancellation and wire format. No actor handler, TLS policy or public API was changed.

Evidence paths in source: `src/connection_pool/stream_writer.rs` (admission and error boundaries), `src/connection_pool/writer_commands.rs` (checked insertion), `src/connection_pool/read_pipeline.rs` (error propagation), and `src/connection_pool/tests/qa_nack_capacity.rs`.

## Controlled regression evidence

The passive peak observer is compiled only into library tests. It measures actual queue length after insertion; it does not infer capacity from replies or expose a production API. Finding-specific tests share a lock when resetting the observer. Ordinary queue traffic can also contribute to the observed upper bound; it cannot make an over-cap peak appear smaller unless reset by a finding-specific test, which is serialized.

1. Initial fixture compilation referred to a nonexistent crate-root message-size constant. That attempt is retained as a fixture compile failure, not product RED. It was corrected to the configured frame-size limit.
2. Corrected fixture, before the fix: 20,000 valid DirectAsk requests and all ordered NoDispatcher replies completed through the real parser/I/O owner on an in-memory full-duplex transport. **Observed pending peak 17,335; intended cap 64.** The regression failed on the bound assertion.
3. After correction: primary and idle paths each receive 20,000 requests with concurrent reply draining and exact correlation/reason/header checks. With a 1 MiB transport buffer the observed peak is **64**; with an eight-byte buffer forcing fragmented reads/partial NACK writes it is **1**. All 80,000 replies complete in order.
4. Actual TCP/TLS test: self-generated client identity, certificate verification, ALPN/Hello/schema negotiation and identifying FullSync, then 20,000 DirectAsk requests with concurrent reply draining. **Peak 64; every reply verified in order.** No actor dispatcher is registered.
5. Direct insertion test fills 64 entries, requires typed rejection of the next insertion, and verifies every prior correlation remains ordered. It also verifies an active, already-popped NACK occupies the last reserved admission slot and that capacity failure is not classified as streaming backpressure.

All three focused tests pass. The subsequent complete feature/release matrix also passes (20 script steps, 7,059 passing records across repeated lanes/doctests, finished 2026-10-09 12:07:14 UTC). It covers these library regressions in default, test-helper, all-feature and release profiles; ignored tests are not counted as passes. Builds, strict lint, docs, formatting and copy guards pass too. See `REMEDIATION_STATUS.md` for counts and limitations.

Retained local evidence under `/Users/greg/dev/icanact-remediation-evidence/`:

- `security-finding-cb0f1d41.txt`: browser-extracted finding text, treated as untrusted review input.
- `nack-capacity-red.log`: fixture compilation failure.
- `nack-capacity-red-corrected-fixture.log`: controlled over-cap RED.
- `nack-capacity-green.log`, `nack-capacity-hardened-green.log`, `nack-capacity-partial-write-green.log`: success after scoped admission, insertion/error and partial-write coverage changes respectively.
- `conflict-full-validation.log`: initial combined-tree timeout-fixture failure; later lanes did not run.
- `d07-scripted-timeout-gate.log`: success after a controlled reply-gate fixture correction, with unchanged timeout/session/reply oracles.
- `conflict-security-full-validation.log` and worktree `logs/validation_20261009_075321_zfQuNV/`: complete renewed matrix; the failed run was not automatically retried unchanged.

No process OOM or deployed-service attack was attempted. These local loopback/synthetic tests establish bounded queue behavior and preserved replies, not Internet exposure, time-to-exhaustion, allocation/RSS savings or network throughput. Logical queued-header count is bounded; allocator slack and aggregate process retention have not been measured. This is a correctness/resource-isolation fix and stays regardless of cost. **No performance A/B cost or speedup is claimed.** Historical A/A results predate both main's cleanup and this correction.
