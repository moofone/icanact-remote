# Remediation execution status

## Location and disposition

- Isolated worktree: `/Users/greg/dev/icanact-remote-remediation`; validation was performed on the remediation snapshot based on `da560aa1f1874bdbf4ae5cb7a459ffaaaa68555b`.
- Source checkout: `/Users/greg/dev/icanact-remote`, preserved; its two existing untracked planning documents were copied to the worktree, not replaced in source.
- During the original validation: no commits, pushes, PRs, merges, deployments, dependency installs or fetches. The subsequent PR request authorized publication; the later conflict-fix request authorizes local integration with canonical main, not a GitHub PR merge or deployment.
- Publication/integration note: draft PR #238 was published at `072c9d4a959c98422da283a86f368b90acac3a8a`. Conflict resolution incorporates canonical main `d846d69924d4d8282006cabbad5f915a4a3a3d55` (cleanup #237), preserving its deleted private paths, canonical reader/writer, dependency narrowing and regression tests. Combined-tree validation now passes; its new evidence is recorded separately below. The original counts still apply only to the pre-integration snapshot.
- **Partial implementation; required acceptance gates blocked.** See [PERFORMANCE_REPORT.md](PERFORMANCE_REPORT.md) for all measurements, limits and failure evidence.

## Implemented groundwork

### F01 / S0 — receive-buffer safety

- The canonical pooled reader (used by both wait policies) and initial TLS reader use initialized `with_len` storage. Main's duplicated-reader removal is preserved.
- `with_len_uninit` retains its public unsafe signature for compatibility, but delegates to initialized storage because ordinary byte-slice exposure cannot represent uninitialized bytes safely.
- Pool regression covers fresh fallback allocation, growth, reuse and the compatibility constructor.
- Shared legal AsyncRead probe inspects initialized storage before writing; it fragments reads to one byte, alternates Pending and checks complete/truncated frames in all three pooled read modes and initial reader.
- The complete mandatory local matrix now passes (default, test-helper, all-feature, release/default and release/all-feature), including observing-reader regressions. Miri remains unavailable; no installation attempted. Overall optimization acceptance is still held, rather than treating a single matrix run as proof that every historical discovery failure is resolved.

### F02 / S1 — fail-first evidence

- Full-validation script now records commands, complete output, exit/capture status, inventory and positive executed counts.
- Removed unclassified retries, nonexistent named checks, substring skips and empty rollout/telemetry headings.
- Added explicit default, test-helper, all-feature and release feature lanes; preserved isolated TLS coverage and optional coverage gates (missing requested plan now fails).
- CI adds default/test-helper/all-feature matrix and both Python and shell tooling tests. Remote CI outcomes are not claimed here.
- Fake command fixtures test first-fail/then-pass, persistent/environment errors, zero selected/executed tests, compile/missing-tool and capture failures.
- Real full validation correctly stopped on connection/fixture failures and a hang. Each failed run remains intact; subsequent validation followed scoped remediation, not automatic failed-run retries. The latest 20-step script completes with positive counts in every lane.

### E0 — paired experiment infrastructure (not yet accepted)

- `scripts/run_ab.py`: freezes configuration, captures source/lock provenance, enforces >=10 pairs in >=2 sessions, balanced seeded order, independent-process paired bootstrap analysis, source stability and raw evidence retention. It never retries/overwrites a run. Metric-gate success requires further correctness/workload review.
- `scripts/sample_transport.py`: exact inventory and completion checks for selected historical transport benchmarks; aggregate throughput only.
- `scripts/sample_concurrent.py` plus a common Rust fixture: 1/8/64/512 concurrent asks with identical four-worker runtimes, 5-second warmup, >=10-second measurements, full echoed nonce/payload equality, and per-operation saturation p50/p95/p99. Fixed-duration policy is explicit, rather than incorrectly requiring equal completed counts from throughput samples. A normal smoke test checks all four depths.
- Historical five-run transport summary replaced by explicit `--config`/`--output` paired-runner wrapper.
- 19 deterministic Python regressions passed. Bash syntax, format/diff checks and both copy guards passed. Follow-up strict all-target/all-feature Clippy also passes.
- Historical short A/A pilot completed 80 samples. The follow-up long concurrent A/A completed 160 samples and 198,757,493 requests with zero errors/full reply identity checks. Median throughput is ~20.9k/94.2k/204.2k/206.6k requests/s at 1/8/64/512 in flight. Several per-session control CIs still fail the declared precision gate; no optimization is accepted. A first adapter-format error is retained separately and has a regression test.

### F12 — specification drift corrected independently

- Updated `spec/WIRE_V5.md` to distinguish V6 TLS/Hello from packed V5 data layout, document all 15 kinds/current metadata, and preserve intentional compatibility boundaries.
- Added a canonical table/header-size/ALPN/version check. All 20 framing and 15 handshake tests pass. No transport code or wire contract changed; no runtime speedup claim.

## Connection/correctness continuation

- **D01 — confirmed admission race:** a real finalizer, paused while building
  identify, is superseded by a preferred inbound. It incorrectly returned the
  same identifying-FullSync `ConnectionAborted` seen in the initial failure.
  The narrow production change now reports `ConnectionExists` only when a
  different usable current instance survives. The abandoned candidate is never
  returned as a handle and its cleanup guard remains armed. A dead replacement
  still fails. Both deterministic cases pass. This confirms an error mechanism;
  it does not prove causality for the original untraced full-suite failure.
- **D03 — invalid survivor fixtures:** three inbound-rejection/ownership tests
  installed a wrong-direction survivor despite asserting local identity
  ordering that permits its legitimate replacement. They now use a preferred
  survivor and explicitly assert that precondition. Existing rejection,
  ownership, pointer, alias and sequence-reset oracles remain unchanged.
- **D04 — test callback scheduler starvation:** a retained native stack and a
  deterministic RED test show a synchronous Condvar gate stranding a queued
  worker-local task. Test-only `block_in_place` hands off that scheduling core
  while preserving the pinned callback. Gate cleanup releases workers on
  controller drop; the teardown pin is armed only for the settled target
  instance. All nine lifecycle-target tests pass, including the starvation
  regression.
- **D05 — probabilistic missing-peer inventory:** a random client can sort above
  all 100 fixed missing-peer candidates. Use a fixed, separately checked client
  identity while leaving the TLS server randomized. The outbound-preference
  precondition and absent-peer failure assertion remain intact.
- **D06 — unasserted strict clock ordering:** a release-only causal-fence test
  assumed back-to-back reads were strictly ordered. Its old/failure source
  history is now explicit, with accepted-claim and strict-order assertions;
  the fresh reconnect still uses the real clock and the delayed release keeps
  its original ownership/proven-alive assertions. No production comparison
  changed. The original failed run did not capture the exact timestamp values.

## Conflict integration and new scoped corrections

- **S13 — pending-NACK resource bound:** the user-supplied High finding remained present after main integration. Controlled RED observed 17,335 queued headers against a 64-entry cap. Both read loops now require room, counting an active partial NACK; insertion is independently checked and typed capacity failures close explicitly. Three focused regressions pass: real primary/idle owners with large/eight-byte transports, actual authenticated TCP/TLS, and preserved outcomes on rejected insertion. All 100,000 requests across transport scenarios receive ordered NoDispatcher replies; observed peaks are 64 (large/TLS) and 1 (eight-byte). See [SECURITY_NACK_CAPACITY_FIX.md](SECURITY_NACK_CAPACITY_FIX.md). Cost is unmeasured; no speedup claim.
- **D07 — sleep-based timeout fixture ordering:** initial combined-tree validation stopped at helper-lane step 12 because the 50 ms ask returned the delayed reply instead of Timeout. The original log is retained as `conflict-full-validation.log`. Tokio timeouts are not preemptive; the original run did not capture precise scheduling, so no definitive original-cause claim is made. A test-only reply gate now retains the actual request until the caller observes Timeout, using `block_in_place` and unwind release. The unchanged timeout/healthy-session/subsequent-payload assertions and all nine scripted-network tests pass. A one-worker regression proves timer progress while the callback is pinned and release on controller unwind. No production timeout policy changed.
- Python tooling: 20 regressions pass, including retained main-compatible `--focus` selection/execution checks; the shell fake-Cargo harness passes and is also included in CI. Separate Rustfmt checks cover selected changed include files in addition to Cargo formatting.
- Offline lock resolution pruned only main's removed test dependencies and their unused transitive crates; retained package versions/checksums did not change. New lock hash: `19c5ebe1eeb2cc3015e6dd6eb317fc306b58b13c47ad3135ad93b0c28b47f208`. The old ignored lock is preserved outside the repository.
- Full combined-tree feature/release validation passes after these scoped changes. The first failed run was not retried unchanged.

## Latest combined-tree validation

Completed **2026-10-09 12:07:14 UTC**, 20 script steps, with no source/lock drift during execution.

| Lane | Selected | Passing records |
|---|---:|---:|
| Isolated TLS ask/reply | 4 | 4 |
| Workspace default/debug | 1,427 | 1,381 |
| Workspace test-helpers/debug | 1,478 | 1,431 |
| Workspace all-features/debug | 1,478 | 1,431 |
| Workspace default/release | 1,425 | 1,381 |
| Workspace all-features/release | 1,478 | 1,431 |

**7,059 passing records across repeated lanes/doctests, not unique tests; ignored tests are not passes.** No-default/default/all-feature builds, strict all-target/all-feature Clippy, rustdoc, Cargo formatting and both copy guards pass. Separate no-default-library strict Clippy, selected include-file Rustfmt checks, 20 Python regressions and the shell fake-Cargo harness pass.

Evidence: `/Users/greg/dev/icanact-remediation-evidence/conflict-security-full-validation.log`, worktree `logs/validation_20261009_075321_zfQuNV/`, `conflict-security-tooling-validation.log` and `conflict-security-no-default-clippy.log`. Pre-run staged-diff hash `e50ec43dfbc3063b64bbbd1c3888c60935c18b2d5454c31191f69c4b07f344fc` and lock hash `19c5ebe1eeb2cc3015e6dd6eb317fc306b58b13c47ad3135ad93b0c28b47f208` match after the run. Subsequent edits record these results in documentation only.

The initial combined-tree failure and controlled RED/prototype failures remain retained. This success follows scoped corrections, not an unchanged automatic retry. Linux/other-platform CI and Miri are not claimed as passes; historical discovery risk and performance acceptance remain open.

## Post-merge continuation — discovery lifecycle

- **D08 — live capability authority:** negotiated `PeerCapabilities` now belongs to the physical `LockFreeConnection`; address/identity side maps are projections. Queries prefer a usable address-indexed session, then the verified address→node→current-peer session. A deterministic regression removes the configured alias, clears/poisons projections and proves the live session remains authoritative for peer-list and clock features.
- **D09 — identify-first outbound owner:** outbound IO starts gated before task scheduling. It cannot read peer traffic or drain shared queues until the first FullSync is in the priority lane. Finalization waits for physical write progress or exact stream exit/timeout. A controlled test queues ordinary traffic through the published connection while identify construction is parked and verifies Gossip is the first frame on the wire. Existing dead-stream, cancellation, registry-drop, restart-exemption and racing-ask tests pass.
- Diagnostic state from a fresh fixed-seed full-target failure showed `active_peers=1` and a verified address→node mapping while the configured-address alias and both capability projections were absent. A later traced random failure showed another registry message arriving as the acceptor's first message before identify, invalid PeerId parsing and identifying-FullSync failure.
- After D08: 160/160 fresh fixed-seed full-target processes passed. Random identities still exposed D09. After D08+D09: 100/100 fixed-seed and 100/100 random-identity fresh full-target processes pass (3,800 test executions), sequential and stop-on-first-failure. Earlier capability, mesh, partition and identify failures remain retained.
- First D08 compilation was unavailable because generated build output filled the host filesystem. Only generated remediation/cleanup `target/` trees were removed; source, evidence, dependency caches and plans were preserved. The subsequent focused checks pass.
- **D10 fixture precondition:** the connect-contention correctness test now installs its expected persistent peer through `configure_peer`; direct cache writes remain only in the ignored benchmark's explicit restoration treatment. This fixes a deterministic 80-error RED exposed when D09 made first-round finalizers settle before disconnect.
- **D11 alias failure accounting:** when no replacement is current, a physical peer failure marks every gossip alias carrying the authenticated node identity failed under one lock. The deterministic RED had a configured alias at failures=2 and an observed inbound alias at failures=0, producing active=1/failed=1 after shutdown. The focused connection-failure test now reaches active=0 while instance fencing remains intact. That assertion is only after shutdown. The focused GREEN run already showed 1 active and 1 failed before shutdown, so a healthy session is not shown to be quiet.
- **D12 successor address route:** instance retirement no longer deletes `addr_to_peer_id` once a successor owns the address, and `lookup_address` can name a live connection by its embedded identity when that route row is missing. Release validation v4 stopped on `publisher_recovers_from_every_round_of_connection_churn` with `No peer ID found` after a successful connect. The gap regression passes. v2 Clippy and the externally aborted v3 run remain retained; v3 steps 1–16 are not a complete matrix.
- Validation v5 then stopped in default workspace step 10: the identify-supersession test sampled the provisional address alias before the session slot existed, so `connection_count` was 0. The observation now waits for that slot and a count of 1. The assertion is unchanged. v5 is retained and is not a pass.
- See [DISCOVERY_CAPABILITY_REMEDIATION.md](DISCOVERY_CAPABILITY_REMEDIATION.md) for the full failure/evidence ledger.

## Post-merge continuation complete local validation

Completed 2026-10-09 16:18:05 UTC on the D08–D12 tree after the identify-supersession observation fix. Log `/Users/greg/dev/icanact-remediation-evidence/post-merge-continuation-full-validation-v6.log` and worktree `logs/validation_20261009_122559_6ka5vb/`. Pre-run diff SHA-256 `373f183802ac37669b116fe07a71cda727b1c00cc6ad3c32cc0d4f3abd5c2ac2`. Lock SHA-256 `19c5ebe1eeb2cc3015e6dd6eb317fc306b58b13c47ad3135ad93b0c28b47f208`. This documentation edit is after that run.

| Lane | Selected | Passed records |
|---|---:|---:|
| Isolated TLS ask/reply | 4 | 4 |
| Workspace default/debug | 1,431 | 1,385 |
| Workspace test-helpers/debug | 1,482 | 1,435 |
| Workspace all-features/debug | 1,482 | 1,435 |
| Workspace default/release | 1,429 | 1,385 |
| Workspace all-features/release | 1,482 | 1,435 |

**7,079 passing records** across repeated lanes and doctests, not unique tests. Ignored tests are not passes. Formatting, no-default/default/all-feature builds, strict all-target/all-feature Clippy, rustdoc and both copy guards passed in the same 20 steps.

Earlier continuation attempts stay retained and are not passes: v1 stopped at default step 10 (D10 and D11), v2 stopped at Clippy step 5, v3 was aborted during step 17 with no command status, v4 failed the publisher flap test in release/default, and v5 failed the provisional-alias `connection_count` sample. v6 follows those scoped fixes.

## Historical pre-integration complete local validation

Completed 2026-10-09 00:02:47 UTC; no source modifications during this run.

| Lane | Selected | Passed records |
|---|---:|---:|
| Isolated TLS ask/reply | 4 | 4 |
| Workspace default/debug | 1,416 | 1,371 |
| Workspace test-helpers/debug | 1,467 | 1,421 |
| Workspace all-features/debug | 1,467 | 1,421 |
| Workspace default/release | 1,414 | 1,371 |
| Workspace all-features/release | 1,467 | 1,421 |

Total **7,009 passing records**, including repeated feature/profile coverage
and doctests; not 7,009 unique tests. Ignored tests are not counted as passes.
No-default library and default/all-feature all-target builds, strict Clippy,
rustdoc, format and both copy guards also pass. Nineteen Python tooling
regressions pass separately.

Evidence: `/Users/greg/dev/icanact-remediation-evidence/post-lifecycle-fixture-full-validation.log`
and worktree `logs/validation_20261008_201118_v0fKQP/`.
Earlier failed/timeout runs, red regressions and native stack capture remain
under the evidence root; see the performance report for dispositions.

## Blocking findings and next action

1. **Historical discovery risk:** D01 reproduces and fixes an identifying-FullSync error mechanism; the complete local matrix now passes. The original untraced failure's exact cause and the separate seed-39 capability-negotiation failure are not conclusively attributed. Keep the latter open; passing isolated replays or the latest matrix does not erase it.
2. **Measurement:** long concurrent windows and full reply identity are established. That A/A matrix still cannot establish <=3% non-regression (notably p99 and some depths). The offered-load A/A at 1,000/s (40 release processes, 2026-10-09 18:07–18:17 UTC) also misses the 3% latency gates; pooled p99 upper bound was +33.4%, and one session reached +287.7%. Every sample completed 10,000/10,000 with zero drops. Later 100/s clock probes are in the performance report. Spinning waits of 20 ms or less brought three samples to p50 0.241–0.254 ms and p99 0.392–0.446 ms, backlog 1. That is not an A/A, not host isolation, and not unloaded service time. Allocation, fairness, and retention evidence are still missing.
3. **Coverage:** the post-merge continuation matrix v6 passes, 7,079 records. Supported-platform/Linux CI has not run and Miri is unavailable on the installed toolchain. Keep those limits explicit. v1–v5 remain retained failures or an aborted run.

## Remaining plan

Main's #237 independently implemented changes overlapping F03–F07 and F11 (obsolete writer/test migration, canonical readers/writers, empty direct batching/speculative-state removal and dependency/features). Conflict resolution retains those changes, but does not establish their individual measured A/B acceptance under this plan. F08–F10 are deferred and are not part of this branch: the chunk writer still rescans, release refs still keep the private connection snapshot, and PubSub still copies per subscriber category. The inherited public-alias delegation is retained as main's existing behavior, not a newly expanded remediation. F12 documentation/table coverage is implemented independently; overall acceptance remains blocked. No per-optimization A/B acceptance is claimed. The source classification is [LEGACY_INVENTORY.md](LEGACY_INVENTORY.md). The disabled `get_connection` example stays uncompiled; the method remains `pub(crate)`. Public/compatibility-sensitive legacy decisions remain open. No additional public-alias change is introduced here.

The local S0/S1 matrix is complete. Resolve the remaining diagnostic risk and establish E0 precision/path-specific measurement before checking off downstream optimizer milestones. The safety correction remains necessary even if later timing work finds a cost. Revert optional failed optimizations individually; never weaken assertions or retries to disguise a regression.
