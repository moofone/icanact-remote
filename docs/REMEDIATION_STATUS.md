# Remediation execution status

## Location and disposition

- Isolated worktree: `/Users/greg/dev/icanact-remote-remediation`; validation was performed on the remediation snapshot based on `da560aa1f1874bdbf4ae5cb7a459ffaaaa68555b`.
- Source checkout: `/Users/greg/dev/icanact-remote`, preserved; its two existing untracked planning documents were copied to the worktree, not replaced in source.
- During validation: no commits, pushes, PRs, merges, deployments, dependency installs or fetches. The user's subsequent PR request separately authorizes publishing this snapshot; dependency installation, deployment and merge are not authorized.
- Publication note: canonical main advanced to `d846d69924d4d8282006cabbad5f915a4a3a3d55` (cleanup #237) after the validated base. The draft preserves the tested snapshot; integration with that overlapping cleanup needs revalidation. The counts below do not claim validation of a combined tree.
- **Partial implementation; required acceptance gates blocked.** See [PERFORMANCE_REPORT.md](PERFORMANCE_REPORT.md) for all measurements, limits and failure evidence.

## Implemented groundwork

### F01 / S0 — receive-buffer safety

- Both pooled readers and initial TLS reader use initialized `with_len` storage.
- `with_len_uninit` retains its public unsafe signature for compatibility, but delegates to initialized storage because ordinary byte-slice exposure cannot represent uninitialized bytes safely.
- Pool regression covers fresh fallback allocation, growth, reuse and the compatibility constructor.
- Shared legal AsyncRead probe inspects initialized storage before writing; it fragments reads to one byte, alternates Pending and checks complete/truncated frames in all three pooled read modes and initial reader.
- The complete mandatory local matrix now passes (default, test-helper, all-feature, release/default and release/all-feature), including observing-reader regressions. Miri remains unavailable; no installation attempted. Overall optimization acceptance is still held, rather than treating a single matrix run as proof that every historical discovery failure is resolved.

### F02 / S1 — fail-first evidence

- Full-validation script now records commands, complete output, exit/capture status, inventory and positive executed counts.
- Removed unclassified retries, nonexistent named checks, substring skips and empty rollout/telemetry headings.
- Added explicit default, test-helper, all-feature and release feature lanes; preserved isolated TLS coverage and optional coverage gates (missing requested plan now fails).
- CI adds default/test-helper/all-feature matrix and deterministic tooling tests. Remote CI has not run.
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

## Latest complete local validation

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
2. **Measurement:** long concurrent windows and full reply identity are now established, with greatly improved precision. The complete A/A matrix still cannot establish <=3% non-regression (notably p99 and some depths). Improve isolation; add fixed-offered-load/fairness, allocation/copy and retention evidence before optimizer acceptance.
3. **Coverage:** all mandatory local feature/release lanes now pass. Supported-platform/Linux CI has not run and Miri is unavailable on the installed toolchain. Keep those limits explicit.

## Remaining plan

F03–F11 remain unimplemented, including dead writer removal/test migration, reader/writer consolidation, empty direct batching, speculative parser states, chunk progress, actor-ref ownership, PubSub shared backing and dependency/features. F12 documentation/table coverage is implemented independently; overall acceptance remains blocked. No per-optimization A/B acceptance is claimed. Public/compatibility-sensitive legacy decisions remain open. The earlier public-alias idea remains out of scope.

The local S0/S1 matrix is complete. Resolve the remaining diagnostic risk and establish E0 precision/path-specific measurement before checking off downstream optimizer milestones. The safety correction remains necessary even if later timing work finds a cost. Revert optional failed optimizations individually; never weaken assertions or retries to disguise a regression.
