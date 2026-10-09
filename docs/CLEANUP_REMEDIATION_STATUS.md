# Cleanup remediation execution report

## Result

**Local remediation complete for R1–R7 and public alias consolidation. Required local acceptance checks pass.** The chunk-write experiment ended with an explicit no-change decision: do not introduce cursor/cache state without stronger real-transport evidence. This report records the remediation/validation phase before publication. The user subsequently authorized committing, pushing, and creating a PR. Remote Linux CI and any later merge remain pending; no merge, deployment, or global tooling installation is part of this work.

Implementation worktree: `/Users/greg/dev/icanact-remote-cleanup`

Branch: `cleanup-remediation`

Base revision: `da560aa1f1874bdbf4ae5cb7a459ffaaaa68555b`

The source checkout `/Users/greg/dev/icanact-remote` is unchanged, including its pre-existing untracked plan. Implementation/test changes are prepared on the isolated branch for the separately authorized PR. Evidence is retained outside the repository at `/Users/greg/dev/icanact-remote-cleanup-evidence/`.

The initial GPT Pro handoff remains artifact-incomplete because of the nested graph-receipt filename. This execution did not repair that manifest or spend another consultation. Findings were checked against current source and host validation instead.

## Finding dispositions

| ID | Disposition | Preserved behavior / evidence |
|---|---|---|
| R1 | Removed the complete obsolete write-all family, including chunk writers and byte-cap wrappers | The zero-write assertion now exercises pending response chunks. Response byte-cap/frame-order/cumulative committed-byte assertions exercise live parking/pending writes. `ResponseBatch`, real framing, and resumable ownership remain. No references to the deleted family remain in source. |
| R2 | Deleted the copied nonblocking reader body | Kept a small named wrapper delegating to canonical polling with `false`. Existing EOF tests cover all parser states/modes. New byte-by-byte actor-frame tests cancel genuinely waiting reads at every boundary, resume with exact payload/IDs, and require busy-mode calls to return without waiting. |
| R3 | Deleted `DirectResponseBatch`, dedicated park helpers, constructor/capacity requests, and empty budget/handler/owner plumbing | Current-source reference/population checks showed no production producer after R1 test migration. Ordinary response/pending-write budgeting remains. Direct asks still validate IDs and queue `NoDispatcher` NACKs; direct-response correlation and public wire kinds remain. |
| R4 | Removed always-unhandled probe, `FastReadOutcome`, impossible specialized results, and the `ReadIoResult` wrapper | The reader/owner/deferred queue pass real `MessageReadResult` directly. Live generic fast dispatch remains. Deferred/NACK-cap assertions now receive actual parsed actor-ask frames rather than an impossible synthetic result. |
| R5 | Nowait ordinary writes use one-poll canonical adapter; copied helper family deleted | Four deterministic regression tests exercise all five pending variants with vectored/non-vectored writers, `Pending`, short writes, empty chunks, zero/errors after committed bytes, cancellation/resumption, offsets, waker registration, and exact wire bytes. Existing live-owner overload/NACK/shutdown tests remain. |
| R6 | Replaced retries, stale/repeated filters, and substring skips with one authoritative default lane and one all-features lane | Fake-Cargo tests prove compile/assertion/missing-tool failures cannot retry into success, zero-selection focus cannot claim coverage, and both full lanes run. Copy guards and optional coverage gates remain. Repaired real script passes both full lanes once, including socket/TLS tests. |
| R7 | Removed duplicate dev `bytes`, unused `tokio-test`/`tracing-test`; narrowed normal Tokio features | `fs`, `io-std`, `process`, and `signal` removed from normal declaration. Development Tokio explicitly enables example-only `signal` and paused-clock `test-util`. Feature-tree inspection confirms the isolated consumer has none of these five features; project dev builds retain signal/test-util only. All-target builds and isolated consumer pass. |

Additional bounded cleanup:

- `register_urgent` delegates to `register_with_priority` with its original caller-provided priority. Both APIs remain. New runtime tests cover both priority values/location/peer identity, and an isolated consumer typechecks both generic signatures.
- Removed production-only unused-variable warnings revealed by the isolated consumer: feature-gated the registry's evidence-only `applied` flag and explicitly named an unused atomic-swap result in reply slots. Atomic ordering and ownership behavior are unchanged.
- Updated stale writer/budget comments and script documentation.

## Reduction accounting

Current tracked-file diff: **315 inserted/changed lines, 1,601 deleted lines**, net **1,286 removed**.

New runtime-regression/profiling test module: 335 lines. New validation-harness tests: 52 lines. Including these 387 new test lines yields **899 net source/script lines removed**, excluding plan/status documentation. This explicitly includes the retained profiling fixture; it is not an additional production path.

No double-counting of R1's immediately removable subset, R2's removed probe copy, or R1's direct writer as part of R3. No measured heap/RSS, binary-size, build-time, or network-throughput improvement is claimed. R3 removes two explicit per-owner vector-capacity requests from source, not a independently measured allocator result.

## Baseline and dependency policy

Installed toolchain: Rust/Cargo 1.96.1 on the local macOS host. The plan executes trusted project code on the host, not in a security sandbox.

Initial offline resolution passed, but build checking was blocked by uncached `criterion 0.7.0`. After explicit user permission, `cargo fetch --locked` fetched missing crates. All later work used offline dependencies. The ignored/untracked worktree lockfile pins checks; it was not added to version control. A copy is retained as `validated-Cargo.lock` in the evidence directory. Pruning unused dependencies updated the local resolved graph without upgrading retained packages.

Baseline default/all-features tests, all-target checks, formatting, strict all-feature lint, docs, and both copy guards passed before implementation. A separate initially pristine worktree also passed the all-features library suite during failure investigation. That diagnostic worktree later received only the identical profiling/regression harness; its production code remains at the baseline revision.

## Failure ledger — preserved, not reclassified as passes

1. Fake-Cargo harness exposed Bash 3 empty-array/nounset behavior. Fixed the harness to avoid empty-array expansion; all scenarios now pass.
2. Initial post-consolidation suite failed in `handle::tests::rejected_same_identity_refresh_does_not_rollback_concurrent_creator` (1,113 passed, 1 failed, 7 ignored).
3. After test migration, library suite failed in that same ownership test and the new zero-write test (1,112 passed, 2 failed, 7 ignored).
4. The new zero-write mock did not advertise vectored capability. Live code correctly chose scalar writes, leaving the mock's vectored counter zero. Fixed the mock capability; its focused test passes with one selected test.
5. Ownership test passed in isolation and on the pristine baseline. Installing diagnostic tracing changed reproduction. A delayed injection also passed. These observations alone do not prove an original production root cause.
6. The ownership fixture installed its own competing ownership command while the handle's unrelated periodic gossip/supervisor task could run against synthetic addresses. It now aborts and joins that timer before the fixture starts. The actual ownership injection and all incumbent/rollback assertions remain unchanged; production ownership/timer behavior is unchanged. The whole library suite, later default/all-feature suites, test-helper lane, and release suite pass after this fixture-isolation change. This establishes acceptance of the isolated fixture, not a proof of every earlier scheduling interleaving; preserve that residual limitation.
7. Removal of `DirectResponseBatch` initially left a derive attribute on a following `impl`; corrected before acceptance.
8. Removing `tokio-test` exposed missing paused-clock support inherited from its features. Added explicit development-only `test-util`; this dependency-unification hazard is now visible in the manifest.
9. Strict lint caught whitespace left after helper deletion; corrected. Isolated consumer revealed three production-only unused-variable warnings; corrected as described above.

Failure transcripts remain in the evidence directory, including `icanact-cleanup-stage1.log`, `icanact-cleanup-stage2-lib.log`, the ownership diagnostics, and baseline results. No automatic retry policy was used to obtain the final authoritative result.

## Acceptance matrix — passed

- `cargo fmt --all -- --check` plus direct Rustfmt checks on changed `include!` files (Cargo fmt does not traverse those includes).
- `git diff --check`.
- `cargo check --offline --locked --lib --no-default-features`.
- `cargo check --offline --locked --all-targets` and `--all-features` (library, tests, examples, benches compiled).
- `cargo clippy --offline --locked --all-targets --all-features -- -D warnings`.
- `cargo clippy --offline --locked --lib --no-default-features -- -D warnings`.
- `cargo rustdoc --offline --locked --all-features -- -D warnings`.
- `bash scripts/tests/full_validation_test.sh`.
- `./scripts/full_validation.sh`: default and all-features **workspace suites**, then both copy guards. Default library: **1,119 passed, 8 ignored**. All-features library: **1,120 passed, 8 ignored**. Integration tests and doctests in both lanes also pass. Transcript: `icanact-cleanup-final-harness.log`.
- `cargo test --offline --locked --release --workspace --all-features -- --test-threads=1`: library **1,120 passed, 8 ignored**; integration tests and doctests pass. Transcript: `icanact-cleanup-final-release.log`.
- `cargo test --offline --locked --lib --features test-helpers -- --test-threads=1`: **1,120 passed, 8 ignored**. Transcript: `icanact-cleanup-final-testhelpers.log`.
- Isolated downstream package under ignored `target/cleanup-consumer`, with only the path library dependency and default features disabled: offline check and strict Clippy pass. Its feature tree confirms no dev-only Tokio signal/test-util leakage. Evidence: `consumer-check.log`, `consumer-clippy.log`, feature-tree text files.
- Cargo test list inventory retained in `final-test-inventory.log`. Ordinary suites execute doctests; all-target compilation does not execute Criterion benchmarks.

Eight ignored tests include seven pre-existing profiling/benchmark fixtures plus the new explicit chunk profile, not skipped correctness regressions. The new profile was executed separately against both production snapshots.

## M10 performance experiment and decision

The same ignored, test-only chunk profile ran in optimized mode against baseline and remediated snapshots, 500 frames per case. Each frame is 512 bytes; scripted vectored writes cap commits at 3 or 1,024 bytes and insert `Pending` between attempts. Every case asserts exact bytes/completion and records poll count and elapsed distributions. Harness setup is outside per-frame timing.

Representative p50 elapsed nanoseconds (single run per snapshot):

| Workload | Write cap | Baseline | Remediated |
|---|---:|---:|---:|
| One chunk | 3 | 9,208 | 8,250 |
| 64 small chunks | 3 | 27,541 | 25,333 |
| Empty-interleaved small chunks | 3 | 38,667 | 36,417 |
| One chunk | 1,024 | 42 | 42 |
| 64 small chunks | 1,024 | 250 | 209 |
| Empty-interleaved small chunks | 1,024 | 375 | 333 |

Poll counts match across snapshots: 170,500 per 500-frame short-write case and 500 per large-write case. Both harness runs pass. Evidence: `icanact-cleanup-profile-before.log`, `icanact-cleanup-profile-after.log`.

**Decision: no cursor/cache optimization in this remediation.** These synthetic measurements support the chunk-count/backpressure cost lead but do not establish repeatable real-socket latency, allocation savings, or fairness/tail-latency improvement. Small observed differences may be noise/order/compiler/layout effects. Adding retained cursor/cache state without those stronger gates would undermine the deletion-first objective. No network-speedup claim or percentage is inferred from this experiment. A later optimization requires a separate measured proposal with real duplex fairness and allocator evidence.

## Residual limits / decisions

- Linux CI, the declared Rust 1.88 minimum, other targets, independent external review, and real-network performance were not exercised here. Require existing CI before any later merge.
- Cargo-machete was not installed locally, so its supplemental check was not run. Existing CI policy is unchanged. No audit database/tool installation or coverage tooling installation occurred; optional coverage gates were not invoked.
- Keep public transport/bootstrap APIs, enum compatibility, generic Clone bounds, shared ownership fences/maps, copy-vs-retention policies, and distinct interleaving tests. They are explicitly deferred design decisions, not proven dead code.
- Ownership fixture isolation removes an uncontrolled test participant; it does not justify weakening production timer/ownership tests or treating prior failures as infrastructure passes.
- Returned GPT Pro receipt claims are not independent acceptance evidence; current-source checks and host results above are the basis of this handoff.

## Handoff

All requested local implementation work is on the isolated branch. Review the diff and this failure/acceptance ledger as part of the separately authorized PR; transferring or merging changes into the source checkout has not been performed. The baseline diagnostic worktree `/Users/greg/dev/icanact-remote-cleanup-baseline` and its build cache under the evidence directory remain available for inspection; no production changes were made there.
