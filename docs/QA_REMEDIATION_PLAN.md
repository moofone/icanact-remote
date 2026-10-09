# QA remediation plan — safety, deletion, and measured A/B optimization

## Status and source of truth

**Execution started; acceptance blocked.** The user authorized implementation and requested a performance report. Safety, validation/tooling and a narrow connection-admission correction are in the isolated remediation worktree. The pre-integration mandatory local matrix passed; combined-tree validation now passes separately (7,059 passing records); historical capability-negotiation risk, inadequate A/A precision and missing path-specific measurements still hold optimizer acceptance. See [REMEDIATION_STATUS.md](REMEDIATION_STATUS.md) and [PERFORMANCE_REPORT.md](PERFORMANCE_REPORT.md). No optimization is accepted. Host execution is not a security sandbox. Only installed tools and offline cached dependencies were used; no installs or network fetches. This validation plan does not itself authorize publication. The user's later PR request separately authorizes a branch, commit, push and draft PR; it does not authorize deployment or GitHub PR merge. The later conflict-fix request authorizes local main integration. Canonical main advanced with overlapping cleanup #237; those inherited F03–F07/F11 changes are retained but are not accepted as measured optimizations under this plan. Historical snapshot validation must not be represented as combined-tree validation.

This plan covers **F01–F12** from the latest GPT Pro static review of snapshot `da560aa1f1874bdbf4ae5cb7a459ffaaaa68555b`, request `15c48db93e8eee7008b14e41fd3526f0e5399cb8a9d6faf1e4c21365cdf84962`.

Local review: `/Users/greg/.pi/agent/pro-responses/1dd16f497eca/artifacts/review-report.md`. The artifact manifest is complete; the code-graph receipt reports availability (6,835 nodes, 53,614 edges), not independently verified local execution. The review was static and targeted, not exhaustive.

This is the current proposed execution plan for the latest review. [CLEANUP_REMEDIATION_PLAN.md](CLEANUP_REMEDIATION_PLAN.md) remains unchanged as an earlier planning record; its R1–R7/M0–M11 identifiers and estimates are not acceptance evidence for this review. Historical public-alias consolidation is outside F01–F12; if pursued, apply the same A/B rules to any claimed optimization.

**Central rule: every optimization gets its own measured A/B comparison.** Runtime cleanup also gets A/B non-regression evidence. Source deletion, allocation requests, retained heap, RSS, build time, throughput, and latency are distinct metrics; never substitute one for another.

## Invariants and boundaries

- Preserve public APIs/generic bounds, TLS negotiation, packed framing, type hashes/alignment, request identity, correlation, queue caps, timeout/shutdown semantics, ownership fences, and duplex fairness.
- Preserve DirectAsk/DirectResponse protocol support and valid `NoDispatcher` NACK behavior when deleting empty local batching state.
- Keep busy-I/O paths nonwaiting. A canonical poller may replace copied logic, but a write-all loop or readiness wait may not replace resumable progress.
- Move useful assertions to live production paths before deleting test-sustained implementations. Similar tests may cover different schedules.
- Retain the safety fix even if initialization costs time. Do not benchmark undefined behavior as a valid competing implementation.
- Work in an isolated implementation worktree, with one writer and independently reversible changes. Preserve existing source changes and planning documents.
- No PR, commit, push, merge, deployment, dependency installation, or new paid model consultation is part of this plan. Any later GitHub target must be `https://github.com/moofone/icanact-remote`, never `tqwewe`.

## 1. Mandatory A/B experiment contract

### 1.1 Prepare the comparison before changing production code

For each change, create an experiment record containing the finding ID, hypothesis, exact A/B source identities and patch hashes, primary metric, workloads, correctness oracle, practical improvement threshold, regression limits, sample count, and measurement boundaries. Freeze these before examining B's results.

- **A:** immediately preceding accepted, safe implementation, plus the benchmark instrumentation.
- **B:** exactly the proposed change on A, with identical instrumentation and regression tests wherever applicable.
- Apply a common harness-only patch to both arms first. Verify it reaches the real path and does not implement B's optimization in A. Do not compare two differently instrumented harnesses.
- Compare each change incrementally, then compare final safe baseline versus the accumulated result. A favorable combined result cannot excuse an individually failed experiment.
- Record Rust/Cargo versions, OS/CPU, power mode, allocator, worker count, feature flags, build profile, RUSTFLAGS/LTO, TLS/transport settings, payloads, peers, inflight counts, seeds and timeouts.
- Use the same offline-resolved dependency lock for runtime comparisons. This library intentionally does not track `Cargo.lock`; retain an untracked experiment lock, not a repository policy change.
- For F11, dependency declarations/features are the treatment. Record both resolved graphs; pin common package versions and explain unavoidable graph differences. Do not mix unrelated upgrades into the experiment.
- Build each arm in its own target directory with equivalent settings. Prebuild runtime benchmarks; exclude compilation from runtime timing. Build-time experiments use fresh, matched target directories instead.

### 1.2 Repetitions and noise control

- Run serially on the same otherwise idle host; no parallel A/B processes. Use identical warmup and fixed workloads. Record thermal/power/load changes.
- Default: **10 independent paired process runs per workload in each of two sessions**. Balance order with a seeded shuffle of AB and BA pairs. One process run is the independent sample; Criterion iterations or individual messages are not independent process samples.
- Pilot A/A runs first to establish measurement noise and confirm the harness. If noise exceeds the practical threshold, improve isolation or predeclare more samples before the A/B run. Do not loosen the acceptance threshold after seeing B.
- For steady-state timing, warm up each arm equally (default at least 5 seconds where suitable) and measure at least 10 seconds or a predeclared completed-operation count with adequate precision. Record exceptions for lifecycle and deterministic counter workloads.
- Preserve every attempt, including crashes/timeouts. Do not discard slow runs or retry until green. A preregistered environment-invalid pair may be rerun in full, with its original logs and reason retained; correctness failures invalidate acceptance.
- Compute paired ratios, medians, and a 95% confidence interval by resampling independent pairs. Analyze sessions separately as well as together. Use a fixed analysis seed and retain raw samples; do not hide disagreement between sessions in the aggregate.
- p50/p95/p99 require per-completed-operation observations, not percentiles of aggregate throughput. Use fixed offered-load latency tests as well as saturation-throughput tests, and report offered/completed counts, errors, drops and backlog to expose overload/coordinated omission.

### 1.3 Default acceptance rules

Thresholds below are proposed gates, not promises of improvement. Any workload-specific adjustment needs a written decision before the experiment.

| Change class | Primary gate | Universal guardrails |
|---|---|---|
| CPU/throughput optimization | At least 5% median improvement in a named target workload in both sessions; paired 95% CI excludes no benefit | Correctness passes; upper confidence bound on cost regression <= 3% for control-workload time/op, p99 and CPU/op; throughput-loss bound <= 3% |
| Allocation/copy optimization | Deterministic reduction in allocations/requested bytes/copied bytes per completed operation, confirmed on the actual path | Same timing guardrails; no unintended retained-heap/RSS increase > 5%; no drop/error/backlog increase |
| Ownership/lifetime optimization | Old objects become reclaimable after legitimate owners release them; measured reduction in retained objects/heap under repair workload | Same timing guardrails; distinguish object ownership from open sockets/task lifetime |
| Runtime simplification/deletion | Quantified implementation/state reduction and A/B non-regression; no speedup required | Same timing/memory guardrails and mapped assertion coverage |
| Dependency/build optimization | Measured clean-build wall time/CPU/peak memory or fixed-consumer binary reduction; default meaningful timing benefit >= 5%, CI excludes no benefit | Default/minimal-consumer behavior passes; no unrelated dependency upgrades; report incremental build separately |
| Safety/validation/documentation fix | Correctness/acceptance/specification gates, not a speedup requirement | Record measured cost where runtime changes; correctness overrides performance |

For lower-is-better metrics, define regression as `B/A - 1`; for throughput use `1 - B/A`. Record the definition and units explicitly. Insufficient precision is **inconclusive**, not non-regression. Predeclare critical primary/control workloads and evaluate all of them; exploratory findings require a separately registered confirmation, not cherry-picked acceptance.

Allowed outcomes: **accepted optimization**, **accepted cleanup with no measured speedup**, **required correctness fix with measured cost**, **rejected/reverted**, **inconclusive**, or **blocked**. An inconclusive optimization is not shipped as a proven gain. Pure documentation or proven non-executing dead-code deletion may record runtime A/B as not applicable only with current-source proof and no runtime benefit claim; build/binary claims still require A/B.

### 1.4 Instrumentation requirements

- Count allocated/requested bytes separately from live/peak heap and RSS. Track reallocations and deallocations if deriving outstanding bytes. Attribute setup, pool warmup and per-operation work separately.
- `benches/pubsub_job_pattern.rs` has thread-local allocation counters: these cannot establish process-wide asynchronous allocations. Add equivalent scoped/process-wide accounting on both arms where worker allocations matter; document contamination from unrelated tasks.
- Run instrumented allocation/lifetime experiments separately from uninstrumented timing. Keep instrumentation identical across arms.
- Validate completion counts, payload checksums, ordering, queue drops, error classes, and shutdown bounds. Enqueue throughput is not delivered throughput.
- For deterministic mock I/O, record offsets, poll counts, wakes, bytes and exact frame outputs. Complement microbenchmarks with real TLS/loopback transport and duplex backpressure workloads.

## 2. Benchmark infrastructure milestone (E0)

Existing entry points are `benches/pubsub_hotpath.rs`, `benches/pubsub_job_pattern.rs`, `benches/connect_paths.rs`, and ignored tests in `tests/integration/throughput_benchmarks.rs`, orchestrated in part by `scripts/bench_transport_contract.sh`.

Before relying on them:

1. Verify each selected benchmark invokes the changed path. The existing PubSub hotpath benchmark publishes owned bytes and does not prove mixed borrowed-ingress copy savings; add a borrowed-ingress fixture to both arms.
2. Add benchmark-only fixtures for pending reads/writes, partial chunk progress, empty direct batches, and repeated ref repair. Keep mock implementations out of production paths.
3. Verify every Cargo filter selects and executes exactly the intended benchmark(s). Inventory using list mode first; a zero-test success is invalid.
4. Harden or replace the transport comparison runner: its current five-run, last-throughput-token summary is not sufficient for paired A/B, p99 or error/drop evidence. Parse structured per-workload records, reject missing/ambiguous metrics, and retain stdout/stderr and exit codes.
5. Ensure direct-ask tests measure their intended behavior, not just successful NACK/refusal handling. Keep explicit refusal benchmarks separate from successful-response benchmarks.
6. Implement paired-order orchestration and a reproducible analysis report using existing tools; do not install a profiler as an implicit prerequisite. Missing mandatory metrics block the experiment.
7. Run A/A pilots and fake-runner tests for failed commands, zero matches, malformed metrics, missing samples and mismatched environment/workload identities.

**Exit:** a reproducible A/B harness, path coverage map, A/A noise report, and preregistered experiment records. No optimizer work is accepted before this milestone.

## 3. Finding-by-finding remediation and measurement matrix

Each row requires source/caller verification against the implementation worktree. Review line numbers may drift.

| Finding / priority | Scoped remediation and correctness gates | Required A/B measurement |
|---|---|---|
| **F01 / P1** receive initialization | Use initialized `with_len` on all affected production receive paths (`aligned.rs`, `read_pipeline.rs`, `handle.rs`). Preserve safe public exposure. Test fresh allocation, growth, reuse, partial reads, Pending, truncation, cancellation and parse failures with an observing AsyncRead; use a focused Miri harness if tooling is already available. | Unsafe original is not a valid performance baseline. Establish corrected initialized implementation as safe A. Any later zero-fill elision is a separate B with MaybeUninit-safe storage, initialized-length tracking and proof before slice exposure. Measure CPU/throughput, allocations and latency across fresh/growth/warm-pool cases and 64 B/1 KiB/64 KiB/1 MiB valid frames. No performance veto of the safety fix. |
| **F02 / P1** truthful validation | Fail ordinary failures immediately; preserve all attempts/logs; replace both absent selectors; reject zero executed tests; align default/test-helper/all-feature lanes and telemetry labels. Fake-Cargo harness covers first-fail/then-pass, persistent failure, missing tools, capture failure and zero matches. | Not a throughput optimization. If claiming faster validation, A/B the same coverage inventory using identical deterministic command fixtures, then equivalent real gates. Measure wall time and invocation count. Less coverage is not a speedup. |
| **F03 / P2** obsolete writer island | Prove no production callers. Remove isolated helpers first. Migrate zero-write, ordering, byte-cap and partial-write assertions to live pending writes before deleting remaining old writers and stale comments. | Dead-only subset: reference/source reduction; no runtime gain claim. If build/size gains are claimed, measure them. Test migration/live-path edits: runtime non-regression A/B over chunked/vectored/non-vectored responses and duplex pressure; count migrated assertions and mutation-sensitive failures. |
| **F04 / P2** duplicate readers | One canonical read state machine, thin explicit wait-policy wrappers. Preserve partial prefixes/body/metadata/payload, stream discard, EOF and progress flags. Verify outbound/shutdown progress while inbound is Pending. | Fragmentation at each boundary, frame-size grid, streaming/discard and idle-to-burst loads. Measure parser CPU/time per frame, delivered throughput, p99 duplex latency and polls/wakes. Accept source simplification only with measured non-regression, not an assumed speedup. |
| **F05 / P2** empty direct batch | Remove constructor, private type/helpers/parameters and empty budget branches after test migration. Preserve ordinary response budgets, direct wire kinds, correlation, valid/invalid IDs and NoDispatcher. | Measure per-owner allocation requests/bytes and retained heap at 1/32/256 owners; separate setup from steady state. Real actor traffic plus explicit direct refusal and valid receive-side completion workloads. Require removal of the identified two vector capacity requests per owner (confirm actual allocator behavior), no budget/fairness regression. |
| **F06 / P2** speculative parser states | Delete always-unhandled seam and impossible private variants; retarget deferred-ask overflow test to a parsed generic frame. Retain live generic fast dispatch, NACK/queue caps and buffer ownership. | Generic actor/direct/response/PubSub parsing and deferred-queue saturation. Measure frame time/CPU, throughput, p99 and allocations. Record removed states/branches; no inferred fast-path gain from deleting a stub. |
| **F07 / P2** duplicate writers | One canonical ordinary poller; nowait adapter polls once and maps Pending without waiting. Cover every variant, zero/errors, short header/payload, offsets, Buf advancement, cancellation and wakers. | Identical scripted vectored/non-vectored writer sequences for every variant plus real duplex transport. Measure polls, CPU/time per completed frame, throughput, p99 and NACK order. Non-regression cleanup gate. Keep separate from F08. |
| **F08 / P2** chunk progress | Add cursor/cached totals/bounded descriptors only after F07. Preserve retained-byte accounting, lifetimes, empty-chunk handling, exact ordering and non-vectored fallback. | Grid: 1/8/64/2048 chunks; 64 B/1 KiB/64 KiB total where feasible; empty chunks; 1-byte/64-byte/full writes; Pending bursts; vectored/non-vectored. Measure descriptor allocations, CPU/op, throughput, retained heap, p99 and shutdown/read fairness. >= 5% CPU/throughput benefit in preregistered partial-write workload OR deterministic allocation reduction with non-regression; no tiny-message tradeoff hidden. |
| **F09 / P2** stale connection ownership | Remove private release snapshot while retaining shared live slot; make separate compatibility decision for public debug/test/helper field. Test clones, ambiguous recovery and replacement. | Release-profile refs/clones under 1/10/100 repairs; weak probes and retained heap after legitimate owners release old connections. Record live tracker/handle counts and time to reclaim; supplement with connect/ask latency and steady-state controls. Do not equate reclamation with socket closure. |
| **F10 / P2** PubSub copies | One lazily owned backing Bytes shared among matching categories. Preserve no-subscriber/single-subscriber paths, queue isolation/drop counters, callback lifetime and order. | Actual borrowed ingress; all 8 combinations of three subscriber categories; 64 B/1 KiB/64 KiB payloads; normal/full queues. Measure payload copies/allocated bytes, delivered throughput and callback-completion p99. Target mixed three-category path from three backing copies to one; zero subscribers remains zero. Test unsubscribe races and backing identity/lifetime. |
| **F11 / P2** dependencies/features | Separate redundant bytes declaration, unused test crates, and Tokio feature narrowing. Verify macros/generated references and resolved features. Compile minimal consumer without dev unification, all target kinds and supported features. | Compare matched cold builds, warm incremental no-op and fixed edit rebuilds; wall/CPU time, peak memory, resolved package/features and fixed release-consumer binary size. Duplicate bytes declaration removal has no presumed build gain. Feature removal must reduce the actual resolved graph if claiming that benefit. Runtime A/B only if runtime code/features change. |
| **F12 / P2** wire specification | Document V6 negotiation separately from V5-named packed frames; current kinds, request IDs, NACK metadata, route binding and schema rules. Preserve fail-closed negotiation. Golden-byte/table checks cover accepted V6/rejected V5 and metadata sentinels. | Documentation-only: no performance claim, A/B not applicable. Any optional constant deduplication/code change is separate, with golden-byte equality and A/B non-regression if it affects runtime. |

Additional profiling leads are **not established defects**: owner-drain batching, per-callback spawn_blocking, discovery scans, full-sync allocation and public alias delegation. Do not silently expand the patch. If adopted, assign new experiment IDs and preregister A/B workloads measuring scheduling latency, queue pressure, snapshot/release ordering and callback isolation as applicable.

## 4. Execution order and exits

1. **S0 — Verify and fix safety (F01):** confirm current unsafe boundaries, add regression tests, correct initialization. Safety work is not delayed for a performance baseline. Exit: safe receive paths and correctness evidence; remaining tooling blockers explicit.
2. **S1 — Reliable evidence (F02 + E0):** truthful validation, benchmark selection/count checks, common harness and A/A pilots. Exit: no false-green outcomes and frozen experiment contracts.
3. **S2 — Private dead subset (F03) and redundant manifest line (F11):** verify no callers, delete only proven subset. Exit: reference/coverage inventory and mandatory checks; measured build claims only if tested.
4. **S3 — Reader consolidation (F04):** semantic regressions plus A/B non-regression; independently reversible.
5. **S4 — Writer consolidation (F07):** semantic regressions plus A/B non-regression; no chunk optimization yet.
6. **S5 — Test migration and deletion (remaining F03, then F05/F06):** migrate old writer assertions before F05; generic parser overflow test before F06. Separate patches and A/B arms for each finding. Exit: real-path coverage and measured allocation/non-regression results.
7. **S6 — Ownership/copy improvements (F09, F10):** separate patches, compatibility decisions, independent allocation/lifetime and timing experiments.
8. **S7 — Chunk experiment (F08):** use accepted canonical writer as A; accept or revert based on preregistered evidence.
9. **S8 — Dependency graph and spec (remaining F11, F12):** separate dependency treatments; fixed-consumer builds; golden documentation checks.
10. **S9 — Cumulative acceptance:** safe post-S0 baseline versus accepted final implementation, full matrix, legacy inventory and evidence index. No aggregate gain rescues a failing individual gate.

At each exit record accepted/rejected/inconclusive/blocked status. No milestone is complete because a patch exists or a test passes once.

## 5. Repository-derived validation and benchmark commands (future execution)

Commands are based on `Cargo.toml`, `.github/workflows/ci.yml`, the earlier plan, and existing benchmark targets. **The attempted subset and results are recorded in PERFORMANCE_REPORT.md; the complete matrix has not passed.** Generate a worktree-only lock offline, then freeze it. Unavailable tools/cache, unresolved dependencies or socket restrictions are blockers, not permission to fetch or silently skip.

```bash
cargo generate-lockfile --offline
cargo fmt --all -- --check
cargo build --offline --locked --lib --no-default-features
cargo build --offline --locked --all-targets
cargo build --offline --locked --all-targets --all-features
cargo clippy --offline --locked --all-targets --all-features -- -D warnings
cargo rustdoc --offline --locked --all-features -- -D warnings
cargo test --offline --locked --workspace -- --test-threads=1
cargo test --offline --locked --workspace --features test-helpers -- --test-threads=1
cargo test --offline --locked --workspace --all-features -- --test-threads=1
cargo test --offline --locked --release --workspace -- --test-threads=1
cargo test --offline --locked --release --workspace --all-features -- --test-threads=1
./scripts/check_no_rkyv_from_bytes.sh
./scripts/check_forbidden_copy_patterns.sh
```

- Run focused regressions first and all mandatory quality/test lanes after each runtime patch. Inventory feature-gated targets; default and all-feature coverage are not interchangeable.
- Run new fake-validation/benchmark-runner harness tests, the repaired full-validation script offline/locked, and isolated minimal-consumer checks. Determine exact commands from their implementation before execution.
- Miri/profilers/dependency tooling may supplement evidence only if already installed. If a required safety proof or metric cannot be established, block the affected milestone; optional tool absence is reported, not called a pass.
- Supported-platform CI results are required before any later merge. This document does not authorize triggering remote actions.

Existing benchmark commands to run **inside each A/B worktree**, with paired orchestration, after inventory/path verification:

```bash
cargo bench --offline --locked --bench pubsub_hotpath
cargo bench --offline --locked --bench pubsub_job_pattern
cargo bench --offline --locked --bench connect_paths

# First confirm the fully qualified test name and exactly one selected test.
cargo test --offline --locked --release --test integration --features test-helpers \
  throughput_benchmarks::test_tell_actor_frame_delivered_throughput \
  -- --ignored --exact --test-threads=1 --nocapture
```

Use relevant existing actor-ask/split/deferred tests similarly after list-mode confirmation. Extend the harness for p99, allocation and treatment-specific workloads; existing aggregate throughput output alone does not satisfy the measurement contract. Do not use `--all-features` as the sole performance profile: `trace-correlation` can change logging cost. Measure default production features and each affected supported profile explicitly.

## 6. Evidence, legacy closure, and rollback

Store raw experiment artifacts outside tracked source, for example under a separately chosen evidence directory organized as `<experiment>/<session>/<pair>/<arm>/`. Each experiment must contain:

- `experiment.json`: source/patch/lock/harness hashes, environment, feature/profile/workload, metrics, thresholds, seed and planned sample count;
- raw stdout/stderr, exit status, timestamps and correctness counts for every attempt;
- `samples.csv` or JSONL: arm/pair/session, units, completed operations, time, CPU, latency distribution, allocations/requested/live bytes, errors/drops/backlog;
- `analysis.md`: paired ratios/CIs, per-session results, target and control workload outcomes, limitations and disposition;
- assertion migration map, before/after references, API/wire decision log, and rollback patch identity.

Final legacy inventory must classify every review item as **removed with evidence**, **retained for a documented live contract**, **requires owner/API decision**, or **blocked**. Re-scan source, tests, examples, benches, docs and scripts for obsolete names/comments and real callers, including include/macro-generated seams where applicable. Do not claim “no legacy remains” while compatibility decisions or uninspected paths remain unresolved. The disabled negative-API example should become a real compile-fail test or receive an explicit documented disposition.

Reject/revert an optimization independently if correctness fails or a preregistered gate fails. Preserve its harness and evidence for diagnosis. Never add a hidden fallback, weaken a test, or increase a queue limit to conceal a regression. Required safety fixes remain; redesign the optional optimization instead.

## Additional user-authorized safety scope

The later linked security finding adds **S13**, pending-NACK resource isolation, without renumbering F01–F12. It is a correctness fix, not an accepted optimization: restore admission capacity at both read loops, count active partial NACK occupancy, independently check insertion and propagate invariant failure explicitly. Controlled RED/GREEN, sustained authenticated TCP/TLS, partial writes and no-eviction tests are recorded in [SECURITY_NACK_CAPACITY_FIX.md](SECURITY_NACK_CAPACITY_FIX.md). No OOM or performance-cost experiment was authorized or claimed. D07's controlled timeout-reply fixture addresses the first combined-tree validation failure without changing production timeout policy. Full combined-tree checks finished successfully before publishing the conflict-fix update; see the separate 7,059-record matrix and retained failure ledger.

## 7. Completion checklist

- [x] F01 safe initialization and regressions (complete local matrix; Miri unavailable); any later zero-fill optimization separately measured
- [x] F02 truthful failure/selection/feature reporting and fake-command tests (20-step local matrix and 19 Python regressions)
- [ ] E0 common A/B harness, path coverage, A/A noise and frozen experiment records
- [ ] F03 obsolete helper/writer removal with assertion migration and evidence
- [ ] F04 one reader with measured non-regression
- [ ] F05 empty batch removed with allocation and duplex evidence
- [ ] F06 speculative states removed with real-path tests and non-regression evidence
- [ ] F07 one writer with measured non-regression
- [ ] F08 chunk experiment accepted with A/B evidence or explicitly rejected/inconclusive
- [ ] F09 ownership optimization measured; diagnostic API decision recorded
- [ ] F10 shared PubSub backing measured across subscriber combinations
- [ ] F11 dependency/feature decisions validated; all optimization claims measured
- [x] F12 current wire specification and golden/table checks (20 framing + 15 handshake tests; independent documentation work, not full matrix acceptance)
- [ ] Every optimization has an individual A/B result, controls and explicit disposition
- [ ] Final safe-baseline/final comparison, complete validation, legacy inventory and rollback evidence

**Current result: safety and evidence tooling implemented; final acceptance blocked. A/A noise was measured in short and long concurrent fixtures; F12 specification/table coverage is corrected. The pre-integration mandatory local matrix passed after scoped connection/fixture fixes; combined-tree validation now passes, while inherited cleanup still needs plan-compliant measurement evidence; historical capability diagnostics, measurement precision and overall acceptance remain unresolved. Zero optimizations have been accepted or credited with a speedup.**
