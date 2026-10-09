# Discovery capability and identify-order remediation

## Scope and disposition

This continuation investigates the historical fixed-seed failure in `test_version_negotiation_v3_capabilities` and later random identifying-FullSync failures after merged PR #238. It does not reinterpret earlier passing runs or retry failures into acceptance. Every failed bounded series remains under `/Users/greg/dev/icanact-remediation-evidence/`.

Two independent production defects were demonstrated and corrected:

1. **D08 — live capability ownership was lost with address projections.** Negotiated capabilities existed only in address/identity side maps. A superseded sibling's delayed cleanup could clear both while the physical winner remained active, and a verified inbound can legitimately retain only its observed ephemeral address alias while callers query the advertised bind address.
2. **D09 — published outbound traffic could precede identification.** `finalize_new_outbound_connection` armed only routed actor asks. The IO owner started immediately, and other discovery/control traffic could be queued or read after publication but before the finalizer queued FullSync. The acceptor could therefore parse a different registry message as its first identity frame and reject the connection.

Two validation follow-ups were also required:

3. **D10 — contention fixture omitted durable configuration.** The correctness test wrote a legacy address cache directly, then expected peer-wide disconnect to preserve it across six rounds. D09's write-confirmation made all first-round finalizers settle before the next disconnect, exposing the missing precondition deterministically (80 later operations failed). The test now uses production `configure_peer` and asserts the durable mapping; the ignored benchmark's explicit raw-cache restoration treatment remains unchanged.
4. **D11 — failed physical peer left an active alias.** Failure accounting marked only the resolved bind address. An inbound-source alias for the same verified node remained at `failures=0`, so stats simultaneously reported one active and one failed entry after the sole connection died. When no replacement instance is current, accounting now marks every alias of the authenticated node failed under the same gossip-state lock. Instance fencing and replacement protection remain unchanged. The focused test asserts `active_peers == 0` only after shutdown. On its GREEN run, before shutdown, node B already reported 1 active and 1 failed; that healthy-session state is not asserted and is not claimed to be quiet.
5. **D12 — successor address route removed with the retired instance.** Release validation stopped in `publisher_recovers_from_every_round_of_connection_churn` with `ActorNotFound("No peer ID found for <addr>")` after `get_connection` had already returned. Instance retirement deleted `addr_to_peer_id` after removing the old connection and after a successor had published into that address. Address publication now indexes the connection before its identity route, and retirement clears that route only while the address is still empty or still holds the retired connection. `lookup_address` also accepts the live connection's embedded identity when the route row is already gone.

These are correctness/lifecycle fixes, not accepted performance optimizations. No speedup is claimed. Connection-setup cost and retained capability field cost have not received individual A/B estimates; steady-state transport A/A results predate both changes.

## D08 — connection-scoped capability authority

`LockFreeConnection` now retains the full immutable `PeerCapabilities` negotiated by that physical TLS/Hello session, not only `remote_boot_id`. Inbound and outbound construction copy the negotiated value before publication.

Capability queries now prefer the currently live physical connection:

- first by the requested address alias;
- then, if that alias is absent, through the verified address→node projection and the current identity-indexed session.

Address and identity side maps remain useful projections/fallbacks, but cleanup races cannot override an active session's immutable negotiation. Clock-calibration queries use the same authority rule as peer-list capability queries.

Regression `live_session_capabilities_survive_stale_projection_cleanup`:

- creates a real live stream owner with PeerListGossip support;
- publishes it by identity;
- removes its configured-address alias while retaining the peer session;
- clears capability projections and writes a stale projection without peer-list support;
- proves peer-list support still follows the live session;
- retires the session and proves the stale projection is not upgraded.

## D09 — identify-first physical owner

A newly created outbound `LockFreeStreamHandle` now starts in identify-gated mode before its IO task can run. While gated, the owner does not read from the peer or drain any shared traffic queue. The first identifying FullSync is enqueued through the bounded immediate lane, then the gate is released. Ordinary traffic may accumulate while publication is visible, but it cannot reach the wire or cause inbound responses first.

Finalization now waits for the owner's byte counter to reach the end of the first identifying frame, or for the exact stream to exit/timeout. Write progress uses a subscribed-before-check notification loop, so dead streams still fail finalization and no notification race can strand the wait. Remaining split FullSync frames retain normal framing/order after the first identity-bearing frame.

Controlled regression `outbound_owner_emits_identify_before_racing_ordinary_traffic`:

- parks the real finalizer while it constructs identify;
- observes the candidate through its production publication index;
- enqueues a StreamAbort through the real ordinary control path before releasing identify construction;
- verifies the first physical frame is Gossip (the identifying FullSync), followed by StreamAbort.

Existing regressions continue to cover dead peer streams, registry disappearance, cancellation mid-identify, restart sequence-reset arming and a racing routed ask.

## Failure and evidence ledger

- Historical failure: `/Users/greg/dev/icanact-remediation-evidence/discovery-diagnostic/target-39.log`. It showed the fixed identities at full-target indexes 33/34, simultaneous known/unknown dials, missing send paths and capability assertion failure. It did not by itself prove one cause.
- First focused compile after D08 was blocked by host `ENOSPC`; this is retained as an unavailable check, not product RED. Only generated `target/` build outputs in the remediation and completed cleanup worktrees were deleted, reclaiming about 200 GiB. Source, dependency caches, logs, evidence and planning files were preserved.
- `post-merge-capability-stress/`: fixed-seed series stopped at run 40 with the original capability assertion (40 passes, then failure).
- `post-merge-capability-traced/`: traced series stopped on a separate identifying-FullSync failure in `test_local_connection_wins`; capability test passed in that process.
- `post-merge-capability-classification/`: 80 fresh traced full-target processes had no capability failure, but retained two `test_mesh_formation_3_nodes` failures.
- `post-merge-capability-diagnostic-fields/`: 160-run maximum series retained `test_partition_heal_behavior` failure at run 123 and stopped on capability failure at run 125. Timeout state proved: one active peer, verified address→node mapping present, but configured-address connection alias and both capability projections absent.
- `post-merge-capability-identity-fallback/`: after D08, **160/160** fresh fixed-seed full-target processes passed.
- `post-merge-capability-random/`: normal random-identity series stopped at process 19 on `test_mesh_formation_3_nodes`, identifying-FullSync `ConnectionAborted`; capability test passed.
- `post-merge-random-identify-traced/`: traced random series stopped at process 18 on `test_failure_recovery_backoff`. The acceptor logged a non-identity registry message as the first message (`node_id` rendered as a socket address), invalid PeerId parsing, stream closure, then identifying-FullSync failure. This supplied direct evidence for D09.
- `post-identify-barrier-stress/`: after D08+D09, **100/100 fixed historical-seed and 100/100 normal random-identity fresh full-target processes passed**, 3,800 test executions in total. Policy was sequential, stop on first failure, no retries.
- `post-merge-continuation-full-validation.log`: first complete matrix attempt passed formatting/build/lint/docs/isolated TLS, then stopped in default workspace step 10. `test_node_a_killed_b_detects_immediately` exposed D11; `test_connect_to_peer_contention_has_no_errors` exposed D10. Later lanes did not run and are not passes.
- `post-identify-connection-state.log`: deterministic D11 RED showed one verified node under two aliases: configured/outbound alias failures=2 and observed/inbound alias failures=0.
- Focused D10/D11 tests pass after scoped fixes.
- `post-merge-continuation-full-validation-v2.log`: renewed matrix stopped at strict Clippy (`nonminimal_bool`) before any test lane. The predicate was rewritten to the equivalent form Clippy requested. That failure is retained.
- `post-merge-continuation-full-validation-v3.log`: steps 1–16 passed on the same tree, including release/default. The process was aborted externally during step 17's release all-features compile. No command status was recorded for step 17. Later lanes are not passes, and this incomplete run is not a product failure.
- `post-merge-continuation-full-validation-v4.log`: release/default step 16 failed `publisher_recovers_from_every_round_of_connection_churn` with the D12 identity error. Debug lanes in that run had passed. Twenty quiet reruns of the old binary did not reproduce it; the gap is pinned by `retired_instance_does_not_clear_successor_address_route` instead of being retried into acceptance.
- After D12, the successor-route regression, the embedded-identity lookup regression, strict all-feature Clippy, and five release runs of the previously failing publisher test pass.
- `post-merge-continuation-full-validation-v5.log`: default workspace step 10 failed `superseded_mid_identify_reports_existing_survivor_not_peer_failure` with `connection_count` 0 against 1. The test accepted the first address-index snapshot. That snapshot can be the provisional alias before the session slot is installed, and `connection_count` counts connected sessions only. The wait now requires that slot and a count of 1, still before identify. The count assertion is unchanged. Later lanes did not run. v5 is retained and is not a pass.
- `post-merge-continuation-full-validation-v6.log`: after that observation fix, the 20-step matrix completed at 2026-10-09 16:18:05 UTC with status 0. Worktree log `logs/validation_20261009_122559_6ka5vb/`. Pre-run diff SHA-256 `373f183802ac37669b116fe07a71cda727b1c00cc6ad3c32cc0d4f3abd5c2ac2`; `Cargo.lock` SHA-256 `19c5ebe1eeb2cc3015e6dd6eb317fc306b58b13c47ad3135ad93b0c28b47f208`. Executed passing records: isolated TLS 4/4, default/debug 1,385/1,431, test-helpers/debug 1,435/1,482, all-features/debug 1,435/1,482, default/release 1,385/1,429, all-features/release 1,435/1,482. **7,079 passing records** across repeated lanes and doctests, not unique tests. Ignored tests are not passes. This documentation edit is after that run.

Passing bounded stress and this matrix substantially improve confidence but do not prove every scheduling interleaving or causally attribute every historical failure. The earlier failures remain part of the result.

## Remaining limitations

- Linux/other-platform CI and Miri are not local passes.
- The address→node projection used for identity fallback is authenticated and ownership-controlled. The local matrix passed; other-platform CI and a separate compatibility review are still open.
- The connection-failure test does not assert that a healthy session is quiet. Its focused GREEN run already showed 1 active and 1 failed before shutdown.
- A `DISCONNECT_STATE` diagnostic remains in `tests/integration/connection_failure.rs` and prints only when `active_peers` is still nonzero after shutdown. It was present in the v6 tree. Removing it would be a later, unvalidated test edit.
- No fixed-offered-load latency, allocator/RSS, connection-setup A/B, or fairness result is inferred from this correctness matrix.
