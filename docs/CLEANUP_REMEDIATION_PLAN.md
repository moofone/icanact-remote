# Deletion-first remediation plan

## Objective and status

Reduce implementation paths, impossible states, allocations, and validation ambiguity while preserving public API, wire compatibility, ownership, bounded resources, and duplex progress. Prefer deletion and delegation to existing implementations over new abstraction layers.

**Status: planning only.** No remediation, builds, tests, benchmarks, or dependency installation have been performed by creating this plan. Execution needs a separate go-ahead. Build/test commands execute project code on the host; they are not sandboxed by this plan.

Basis: GPT Pro static review of snapshot `da560aa1f1874bdbf4ae5cb7a459ffaaaa68555b`, findings R1–R7. The downloaded report is available locally at `/Users/greg/.pi/agent/pro-responses/19edb80d5374/artifacts/review-report.md`. The handoff is artifact-incomplete: the graph receipt downloaded under a nested filename, but the integration rejected the missing required root receipt. The receipt reports successful indexing; that is remote evidence, not independent local verification. Do not treat the recommendations as established acceptance results or silently mark the handoff complete.

Repository checks performed for this plan: inspected `Cargo.toml`, CI, validation script, and script documentation; source checkout was clean before adding this document. Runtime findings still require current-source verification. Report line numbers are navigation hints and may drift.

## Guardrails

- Preserve public signatures, wire kinds/framing, direct-response correlation, TLS behavior, queue caps, shutdown semantics, and ownership fences.
- Preserve production `NoDispatcher` behavior for direct asks; removing empty batching state must not remove validation or NACKs.
- Never change a nonblocking branch into a readiness wait. Keep parked/blocking behavior explicit.
- Do not replace resumable pending writes with write-all loops.
- Move useful assertions from obsolete implementations onto live paths before deleting those implementations/tests.
- Do not equate similar tests with duplicate coverage: preserve different schedules and interleavings.
- No dependency/tool installation, network resolution, PR, push, merge, or deployment is part of this plan. Any eventual GitHub action must target `moofone/icanact-remote`, never `tqwewe`.
- Prefer one isolated implementation worktree, one writer, and small independently reviewable patches. Stop on unavailable validation or unexplained failures; never retry until green and call it a clean pass.

## Execution order and dependency map

1. **M0:** Establish current-source evidence and a trustworthy baseline.
2. **M1 / R6:** Make validation reporting truthful before accepting runtime changes.
3. **M2 / R1 + R7:** Delete the unreferenced helper subset and duplicate dependency declaration.
4. **M3 / R2:** Consolidate readers.
5. **M4 / R5:** Consolidate ordinary writers.
6. **M5 / R1:** Retarget obsolete-writer tests and delete the remaining write-all family.
7. **M6 / R3:** Delete production-empty direct-response batching (depends on M5).
8. **M7 / R4:** Delete speculative fast-read states (depends on M3; coordinate with M6).
9. **M8 / R7:** Narrow dependencies/features and validate isolated consumer behavior.
10. **M9:** Consolidate public alias implementation without removing APIs.
11. **M10:** Measure and decide chunk-write optimization separately.
12. **M11:** Final acceptance and documentation.

M3/M4 can be reviewed independently, but do not run concurrent writers in the same worktree. Keep baseline failures separate from new regressions. Each milestone stays pending until its exit evidence exists.

## M0 — Verify the findings and establish baseline

- Record actual revision, working-tree state, toolchain, operating system, enabled features, and available dependency cache/tools.
- Search all source, examples, benches, tests, scripts, and `include!` files for R1 helper references, `DirectResponseBatch` population/field writes, fast-read variant constructors, nowait helpers, and dependency uses.
- Read enclosing bodies and callers. Confirm reader equivalence for each `Pending` decision and writer equivalence for every pending variant. Do not rely on graph degree alone.
- Inventory tests using Cargo's list mode for default and all-features lanes. Map each reported legacy assertion to a live-path test or a test to add.
- Run the baseline matrix below once after execution is authorized. Record failures as failures or infrastructure blockers, not passes. Preserve current guards and isolated TLS coverage.
- For before/after comparisons, resolve one compatible dependency graph from the existing offline cache and retain the generated, untracked lockfile in the worktree. This library intentionally does not track `Cargo.lock`; do not add one to version control or change its policy.

**Exit:** verified caller/constructor inventory, assertion migration map, reproducible baseline results, and explicit blocker list. If a reported path is live, revise the proposed deletion before proceeding.

## M1 — R6: Simplify and harden validation

**Files:** `scripts/full_validation.sh`, `scripts/README.md`, `.github/workflows/ci.yml` if matrix alignment requires it; add focused shell harness tests under `scripts/tests/`.

- Replace broad automatic retries with fail-first authoritative validation. Recovery runs are explicitly requested, retain attempt outcomes, and never overwrite a failed result with an unqualified pass.
- Remove the nonexistent `gossip_frame_uses_zero_copy_buffer` filter after current-source confirmation. Any retained focused selector must prove a nonzero selected-test count before claiming success.
- Define default and all-features lanes explicitly. `--all`/`--workspace` selects packages, not all features; feature-required integration targets must be included in the all-features lane.
- Remove repeated pointer/streaming runs from the normal full flow only after inventory proves canonical-lane coverage. Retain focused troubleshooting mode if useful.
- Preserve isolated TLS coverage if still needed. Avoid substring skips that could exclude unrelated tests; prove skipped tests are covered elsewhere in the same feature lane.
- Preserve both copy guards and optional coverage gates. Update stale rollout banners, step numbering, retry descriptions, and command documentation.
- Honor existing macOS output/socket constraints: log command, feature lane, exit code, attempt identity, and timestamps without introducing mandatory piping that destabilizes tests. Require preserved native output/transcripts for any recovery claim; otherwise stop rather than classify an unknown failure as infrastructure.
- Test with a fake Cargo executable in a temporary PATH: zero selected tests, first failure then success, assertion failure, compile failure, missing tooling, and successful explicit lanes. Verify exit statuses and that all required lane invocations occur. Keep harness fixtures out of runtime code.

**Exit:** failures cannot become clean passes, zero-match selectors cannot claim coverage, test inventory is preserved, and script/docs agree.

## M2 — R1/R7: Low-risk deletion subset

**Files:** `src/connection_pool/constants.rs`, `Cargo.toml`.

- Delete `write_chunks_batched`, its unreachable `write_remaining_chunks` callee, and the unused direct-response byte-cap flush wrapper after proving no callers.
- Remove duplicate development `bytes = "1.0"`; retain the normal dependency.
- Remove only allowances/comments made obsolete by these deletions. Preserve unrelated batch structures and live flush policy.

**Exit:** no dangling references; baseline quality/build/test gates pass. Reported reduction: 91 helper lines, a subset of R1's 444-line block—not an additional 91 lines.

## M3 — R2: One reader, explicit wait policy

**Files:** `src/connection_pool/read_pipeline.rs`, `src/connection_pool/stream_writer.rs`.

- Route the two busy-I/O nonblocking call sites to `read_message_step_poll(..., false)` and delete the duplicate parser body.
- Optionally retain a small delegating nonblocking wrapper to prevent a boolean-policy mistake; do not introduce a new parser or enum framework.
- Preserve parked `true` mode. Update test helpers to cover actual wait policies rather than two equivalent bodies.
- Cover fragmented prefixes/bodies/stream metadata/payloads, EOF at each parser state, invalid lengths/IDs before allocation, cancellation/resumption, stream discard/reaping, and `progressed` reporting.
- Exercise inbound `Pending` with outbound work queued; assert outbound and shutdown progress.

**Exit:** identical framing/state transitions with no wait in busy service. Estimated removal: 266–278 net lines.

## M4 — R5: One ordinary-write state machine

**File:** `src/connection_pool/stream_writer.rs`.

- Replace `poll_pending_ordinary_nowait`'s copied implementation with a one-poll adapter around `poll_pending_ordinary`; map `Pending` to ready `Partial(0)`.
- Remove orphaned nowait vectored, header/payload, and generic-buffer helpers only after confirming all callers.
- Keep waiting and timeout/teardown paths with their existing policies.
- Test every pending variant using deterministic scripted writers: vectored/non-vectored capability, short writes at boundaries, empty chunks, `Pending`, zero, errors, and canceled waiting futures.
- Assert committed bytes, offsets, counters, `Buf::advance`, frame order, waker registration, bounded NACK draining, and read/shutdown responsiveness under backpressure.
- Do not optimize chunk cursor/allocation behavior in this patch.

**Exit:** one canonical state machine; nowait never waits for readiness. Estimated reduction: 151–158 net lines.

## M5 — R1: Migrate tests, retire remaining write-all island

**Files:** `src/connection_pool/constants.rs`, `src/connection_pool/stream_writer.rs` and associated tests.

- Retarget the old direct-writer zero-write and simulated read-turn tests to live response parking and pending writes.
- Preserve byte-cap, flush-order, short-write, failure, and counter assertions. Tests must not reintroduce a synthetic production path solely to retain an old fixture.
- Delete remaining obsolete writers/cap helpers, their dead-code allowances, and stale comments that call them the real production writers.
- Keep `ResponseBatch`, `take_wire_chunks`, real response framing, and pending-write ownership intact.

**Exit:** assertion migration table has no unexplained coverage loss; no production or test reference remains to deleted helpers. Total R1 removal estimate: 444 source lines including M2, excluding replacement tests.

## M6 — R3: Remove empty direct-response batching

**Files:** `constants.rs`, `stream_writer.rs`, `read_pipeline.rs` under `src/connection_pool/`.

- Reconfirm the only population call was in the migrated legacy test; check direct field writes as well as method calls.
- Remove `DirectResponseBatch`, its per-owner instance/capacity requests, dedicated park helpers, and empty-state arguments, budget calculations, clearing, and branches.
- Preserve ordinary actor-response budgets, outbound direct asks, request validation, `NoDispatcher` NACKs, and inbound direct-response decoding/correlation.
- Validate direct requests with valid/invalid IDs, correlated replies/NACKs, and ordinary response budget exhaustion in debug, release, and test-helper configurations.

**Exit:** no unused direct batch plumbing; no direct-protocol behavior change. Reported reduction: 80+ state/helper lines plus plumbing. Do not count R1's old direct writer again.

## M7 — R4: Remove speculative fast-read alternatives

**Files:** `read_pipeline.rs`, `connection_pool/tests/mod.rs`, affected reader/I/O call sites.

- Replace the always-unhandled pooled-buffer probe with the existing generic parse path.
- Delete `FastReadOutcome` and production-unconstructed `ReadIoResult::DirectAsk`/`ActorAsk` states and their dispatch arms.
- Remove a resulting single-variant wrapper if direct `MessageReadResult` use stays clear and does not broaden the patch unnecessarily.
- Retarget the synthetic deferred-ask-cap test to bytes parsed into a real generic actor request. Preserve deferred/NACK saturation assertions.
- Cover valid/invalid direct IDs, known/unknown actors, handler failures, response correlation, pooled ownership/pointer expectations, and queue caps.
- Keep the live generic `try_handle_fast_io` handling; its name alone is not evidence of dead code.

**Exit:** tests enter through constructible states, dispatch/error behavior is preserved, and no impossible-state plumbing remains. Count reductions after editing, excluding reader regions already removed in M3.

## M8 — R7: Narrow manifest surface

**Files:** `Cargo.toml`; existing dependency-policy tests as appropriate.

- Confirm absence of `tokio_test`, `tracing_test`, and macro uses; remove unused development crates separately from feature changes.
- Trial removal of normal Tokio `fs`, `io-std`, and `process` features. Move example-only `signal` to a development Tokio declaration if the all-target matrix confirms it is needed only there.
- Inspect Cargo feature closure before/after; build success alone may reflect transitive feature enablement. State whether a feature was removed from this crate's declaration or from the resolved graph.
- Build an isolated temporary downstream consumer with only the library as a dependency, default features disabled, and without this package's dev-dependency unification. Keep the generated fixture outside the runtime API.
- Validate library, examples, benches, tests, and docs under default and all features. Preserve serde, crypto, TLS, and public serialization contracts.

**Exit:** all target lanes and isolated consumer pass; no accidental dependency on dev-only feature unification. No compile-time/binary-size improvement claim without measurement.

## M9 — Preserve APIs, consolidate alias implementation

**File:** `src/handle.rs` (`register_urgent` and `register_with_priority`).

- Verify equivalent setup and delegation; make one public method delegate to the other with the existing urgent priority.
- Preserve both signatures, return values, validation, and generic bounds. Test both entry points.
- Do not remove transport/bootstrap generics, public enums, ownership maps, or custom generic `Clone` implementations as part of this remediation. Record these as deferred compatibility/design decisions, not unfinished dead-code cleanup.
- Review similar tests only for reusable deterministic setup. Do not require deletion unless schedule/assertion equivalence is proven.

**Exit:** shared implementation with unchanged public behavior; deferred API decisions documented.

## M10 — Separate performance experiment: chunk-write progress

**File:** `src/connection_pool/stream_writer.rs`; existing applicable benches or a focused benchmark fixture.

- Establish before/after measurements for many small chunks, empty chunks, partial writes at varying offsets, and sustained backpressure. Record allocations, throughput, latency distribution, and read/shutdown fairness on the same toolchain/dependency graph.
- Compare a retained cursor or bounded I/O-slice strategy against canonical current behavior. Avoid caching allocations that worsen retained memory or adding more state than justified.
- Preserve exact byte counts, ordering, memory bounds, cancellation/error behavior, and vectored/non-vectored support.
- Set workload-specific acceptance thresholds before implementing the optimization; require repeatable improvement and no meaningful fairness/tail-latency regression.

**Exit:** either a separately tested, measured optimization or a documented no-change decision. Unmeasured speedups are not a deliverable requirement.

## Validation matrix (future execution)

Commands derive from the manifest and existing CI/scripts. Use installed tools/cache only. The repository does not track a lockfile: resolve once offline in the isolated worktree, then use its untracked lockfile for repeatability. If offline resolution, a tool, or a dependency is unavailable, report blocked and request permission rather than fetching/installing.

```bash
cargo generate-lockfile --offline
cargo fmt --all -- --check
cargo check --offline --locked --lib --no-default-features
cargo check --offline --locked --all-targets
cargo check --offline --locked --all-targets --all-features
cargo clippy --offline --locked --all-targets --all-features -- -D warnings
cargo rustdoc --offline --locked --all-features -- -D warnings
cargo test --offline --locked --workspace -- --test-threads=1
cargo test --offline --locked --workspace --all-features -- --test-threads=1
./scripts/check_no_rkyv_from_bytes.sh
./scripts/check_forbidden_copy_patterns.sh
```

- Run focused new/migrated regressions for each milestone, then default and all-features suites; run format, all-target build/check, lint, docs, and guards before accepting each patch.
- Compile tests/examples/benches using the all-target checks; execute tests via test commands, not `cargo test --all-targets`, which can execute benchmark targets. Run doctests through ordinary Cargo test lanes.
- For M6/M7, additionally run targeted protocol/I/O regressions and the all-features suite in release mode (`cargo test --offline --locked --release --workspace --all-features -- --test-threads=1`).
- Exercise default and `test-helpers` configurations for direct/actor behavior, including a targeted `--features test-helpers` lane; all-features alone can mask configuration-specific behavior.
- Use test list output to verify feature-required targets and nonzero focused selections. Do not silently skip failing socket tests. Classify an environmental failure as blocked, not successful validation.
- Run the repaired full-validation script after its fake-Cargo harness tests and at final acceptance, with Cargo offline mode inherited (`CARGO_NET_OFFLINE=true`); update script commands to preserve the resolved graph where appropriate.
- Run `cargo machete` if already installed; otherwise mark supplemental dependency evidence unavailable. No local `cargo install` or audit-database fetch is authorized by this plan. Existing CI audit policy remains unchanged.
- Coverage gates are optional and require already-installed tooling; any claimed preservation of specific legacy assertions must still have executable tests, not just a coverage percentage.

## Acceptance, evidence, and rollback

For each milestone, record: issue IDs; changed files; current-source proof; preserved contracts; tests added/migrated; exact commands/environment; pass/fail/blocked results; line/state/dependency reductions; and residual risks. Review the diff for deleted assertions, waits introduced into nowait paths, altered generic bounds, and resource-policy changes.

**Final acceptance:**

- R1–R7 have verified dispositions, passing required gates, or explicit unresolved blockers; blockers prevent claiming completion.
- No obsolete writer family, duplicate parser/writer body, empty direct batch, or speculative fast outcome remains where current-source evidence supports removal.
- API and wire contracts are unchanged, duplex/shutdown behavior remains bounded, and validation cannot disguise zero-test or retry outcomes as clean passes.
- Docs/scripts reflect actual feature lanes and behavior. Stale references are removed, not merely renamed.
- The full regression matrix passes on the supported local host, with CI Linux results required before any later merge; baseline environment-specific failures remain explicit.
- Report actual net reductions without double counting. Separate source reduction, allocation requests removed, resolved feature changes, and measured performance.
- No tracked lockfile, generated evidence, temporary consumer, benchmark output, or dependency installation is accidentally included.

If a milestone regresses behavior, revert only that milestone in the isolated worktree, retain its tests/evidence for diagnosis, and keep earlier accepted milestones intact. Do not compensate with new fallback paths or loosened assertions. M10 is independently reversible.

## Tracking checklist

- [ ] M0: Findings verified; baseline and assertion map recorded
- [ ] M1: R6 validation harness hardened and simplified
- [ ] M2: R1 unused subset and R7 duplicate dependency removed
- [ ] M3: R2 readers consolidated
- [ ] M4: R5 writers consolidated
- [ ] M5: R1 tests migrated; remaining obsolete writers removed
- [ ] M6: R3 empty batching removed
- [ ] M7: R4 speculative parser/dispatch states removed
- [ ] M8: R7 dependency/features narrowed and consumer validated
- [ ] M9: Public alias delegation consolidated; API decisions deferred explicitly
- [ ] M10: Chunk-write experiment completed or no-change decision recorded
- [ ] M11: Final matrix, evidence, documentation, and reduction report complete
