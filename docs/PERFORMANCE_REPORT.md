# Performance report — safe baseline, concurrent asks, and A/A stability

## Executive result

**Status: partial implementation, blocked acceptance. No optimization has been accepted and no speedup is claimed.**

The receive-buffer safety correction and fail-first validation/A/B tooling were implemented in the isolated worktree `/Users/greg/dev/icanact-remote-remediation`, originally based on `da560aa1f1874bdbf4ae5cb7a459ffaaaa68555b`. The source checkout was preserved. That validated snapshot did not implement F03–F11. Following publication as draft PR #238, conflict resolution now incorporates main's independent #237 cleanup, overlapping F03–F07 and F11. Its production changes and dependency graph mean the historical measurements below are not measurements of the combined tree; no individual A/B acceptance is inferred. F12's wire documentation and a canonical table/ALPN check have now been addressed independently, without changing production transport behavior.

The historical pre-integration mandatory local matrix passed after a narrow connection-admission fix and verified fixture corrections: **7,009 passing records across feature/profile lanes**, plus builds, strict Clippy, rustdoc and copy guards. Failed runs were retained; no outcome assertions or thresholds were weakened. Combined-tree correctness validation now passes separately (Phase 4: 7,059 passing records); it does not substitute for performance A/B evidence.

Optimization acceptance is still held: closed-loop and offered-load A/A controls both miss the <=3% latency gates, allocation/retention evidence is still absent, and the historical seed-39 capability-negotiation failure has not been conclusively attributed. A deterministic finalizer test reproduces the identifying-FullSync error mechanism, but cannot prove it caused the original untraced run. Supported-platform CI/Miri limits remain explicit. The new runtime correctness fix has not received a performance A/B cost estimate.

Safety is not optional: the previous uninitialized-memory implementation was not run as a valid performance competitor. The corrected initialized version is the baseline for future A/B work. There is no numerical estimate of the safety fix's performance cost versus a valid alternate design yet.

## Phase 1 — historical short-pilot protocol and provenance

- Hardware: Apple M1 Max, 64 GiB RAM, arm64, Darwin 27.0.0.
- Toolchain: rustc 1.96.1 (`31fca3adb`, 2026-06-26), Cargo 1.96.1 (`356927216`, 2026-06-26).
- Profile/features: release, `test-helpers`, `trace-correlation` disabled; existing TLS/loopback integration fixtures; 256-byte payloads.
- Power: observed `pmset powermode=0`; host-wide idle isolation was not guaranteed.
- Arms: A and B intentionally point to the identical safe worktree. These are **A/A noise measurements, not before/after optimization results**.
- Two consecutive sessions, ten independent paired process samples per workload per session; seeded balanced AB/BA order (seed `8102026`); 40 samples per workload, 80 total.
- Every sample verified exactly one selected and passing ignored test, positive completions and matching paired counts. Original stdout/stderr, status, config and source/lock hashes were retained.
- Delivered tells: 10,000 completed deliveries/sample, 400,000 measured deliveries total; fixture waits for receiver count.
- Single-flight asks: 1,000 completed reply waits/sample, 40,000 total; existing fixture asserts reply length. Independent payload checksum verification is still needed for comprehensive acceptance.
- Native fixture warmup remained unchanged. Timed regions were only 31.9–157.0 ms for tells and 29.7–844.2 ms for asks. The preregistered pilot explicitly allowed short durations; the acceptance duration gate was **not** satisfied.
- Bootstrap: 5,000 resamples of independent paired observations; median paired throughput loss `1 - B/A`, 95% CI; analyzed sessions separately and pooled. Positive values denote an apparent B slowdown despite identical code.
- p50/p95/p99 operation latency, allocation counts, CPU time, retained heap and RSS were **not measured**. Reciprocal throughput/time-per-operation is not a latency percentile. No result below establishes those metrics.

Raw evidence directory: `/Users/greg/dev/icanact-remediation-evidence/aa-pilot-parser-corrected/`.

| Provenance item | SHA-256 |
|---|---|
| Frozen experiment configuration | `d74fc0b711823f8e228d492f1ef438c26b46b700aeea2b9ec4201ccbae510b8a` |
| Runner used for pilot | `ca4d4cc0e85bf49c6469cfc0d8e78feae69829f093286683ec82db5500e5cfc7` |
| Runtime dependency lock, both arms | `227225c05742b4b2a006e1db79ee697f5b8eba0e2c8d5a07ed23b674f4f0bffa` |
| Tracked worktree diff during pilot, both arms | `0455cbfc54f9ed50ed32c467cc1ff62af95b240427b4b0ef1d47ca610a62ac28` |

`provenance.json` additionally records each included source file's hash; both snapshots were verified unchanged during the run. Subsequent documentation/runner regression-cap hardening is separately tested and does not change the saved pilot observations or their original provenance.

## Phase 1 — historical short-pilot results

### Descriptive safe-baseline throughput (all 40 samples per workload)

| Workload | Median throughput | Observed min–max | Measured operations | Interpretation |
|---|---:|---:|---:|---|
| Delivered actor tells | 250,679 messages/s | 63,708–313,560 messages/s | 400,000 | Short-run descriptive baseline only |
| Single-flight actor asks, no timeout wrapper | 5,928 requests/s | 1,184–33,643 requests/s | 40,000 | Large variation; not a stable acceptance baseline |

### Paired A/A noise and proposed non-regression gate

| Workload / session | A median | B median | Median paired apparent throughput loss | Paired 95% CI | Upper CI <= 3%? |
|---|---:|---:|---:|---:|---|
| Tell / session 0 | 252,165 msg/s | 239,276 msg/s | +3.50% | −6.68% to +24.01% | No |
| Tell / session 1 | 258,203 msg/s | 257,520 msg/s | +0.60% | −32.25% to +8.37% | No |
| Tell / pooled | 252,165 msg/s | 249,267 msg/s | +1.81% | −1.16% to +8.04% | No |
| Ask / session 0 | 6,339 req/s | 4,377 req/s | +21.40% | +2.26% to +47.03% | No |
| Ask / session 1 | 5,507 req/s | 7,279 req/s | −38.50% | −263.80% to +54.08% | No |
| Ask / pooled | 6,027 req/s | 5,748 req/s | +7.08% | −29.18% to +37.42% | No |

Median paired ratios are not ratios of aggregate medians. A throughput improvement can exceed 100% under `1 - B/A`, hence the large negative bound in ask session 1 is mathematically possible; it is not evidence of an optimization.

**Disposition: pilot-only; acceptance precision inadequate.** Do not interpret an apparent A/A gain/loss as a source change, pool sessions to hide disagreement, or widen thresholds after observing these results.

## Phase 2 — long concurrent-ask baseline (latest measured results)

The follow-up measurement fixes the single-flight-only scope and short timing
windows. It uses one common fixture at **1, 8, 64 and 512 concurrent asks**, with
the **same four-worker Tokio runtime**, public RemoteActorRef ask API, one
preferred TLS connection, 256-byte nonce-bearing payloads, profile/features,
and full reply-content/correlation identity validation at every depth.

Each independent process has a five-second warmup and a ten-second measured
window, followed by draining every admitted request. Setup/shutdown and
percentile sorting are outside timing. Throughput includes client scheduling,
payload construction, reply validation and draining; per-ask latency starts
immediately before the ask call and ends on reply return. The fixture stores
latency observations, so these are instrumented baseline measurements, not
unmodified production/client CPU or allocation results.

**Two sessions × ten paired observations × four depths = 160 process samples.**
A and B still use identical safe source: this is A/A baseline/stability, **not
an optimization A/B result**. Every command passed with zero errors/drops, all
admitted requests completed, and every echoed nonce-bearing payload matched.
Source/lock hashes remained stable throughout the run.

The experiment preregisters **fixed-duration** completion policy: completed
counts may differ because that difference is the throughput observation.
Fixed-count workloads in the runner still require equal paired counts.
Both policies require declared correctness and positive completion; duration
experiments additionally reject samples below their frozen minimum.

### Concurrent baseline summary

Values are medians over 40 independent process samples at each depth.
Latency columns are **medians of each run's per-operation percentile**,
not percentiles reconstructed from throughput and not a pooled request histogram.

| Concurrent asks | Throughput (requests/s) | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|
| 1 | 20,932 | 42.29 µs | 77.08 µs | 104.69 µs |
| 8 | 94,161 | 74.81 µs | 120.73 µs | 150.08 µs |
| 64 | 204,214 | 267.48 µs | 495.88 µs | 621.04 µs |
| 512 | 206,564 | 2.435 ms | 3.491 ms | 4.271 ms |

In this measured closed-loop workload, throughput roughly plateaus between
64 and 512 in flight while queueing/tail latency grows substantially. This is
**not** a 64-request implementation cap or a recommendation to silently change
the ask window. Concurrent asks are supported, and neither these results nor
the historical single-flight result is a universal service ceiling.

This is **closed-loop saturation latency**, not a fixed-offered-load test.
It does not correct coordinated omission. Payload construction/verification,
latency collection, workload duration and worker settings differ from the
historical short pilot, so its throughput must not be presented as a speedup
over Phase 1.

### Paired A/A precision

Positive throughput loss = apparent slowdown in B despite identical code.
CIs resample independent pairs. Both sessions remain visible in analysis.json;
pooled precision cannot substitute for each-session gates.

| Depth | Pooled median paired throughput loss | 95% CI | Throughput gate in both sessions? | Pooled p99 cost change / 95% CI |
|---:|---:|---:|---|---|
| 1 | −0.69% | −3.04% to +4.23% | No | +0.27% / −5.13% to +7.23% |
| 8 | −1.03% | −2.80% to +2.45% | No (session 1 upper bound +3.98%) | +0.05% / −3.14% to +3.47% |
| 64 | −2.65% | −5.90% to +0.41% | Yes | −0.59% / −9.42% to +2.43% |
| 512 | +2.84% | −4.88% to +6.45% | No | +1.52% / −8.44% to +8.68% |

Long windows improved precision, and the 64-in-flight throughput control meets
the 3% bound in both sessions. **The complete control matrix still does not**:
p99 upper bounds at depth 64 are +7.08% and +5.15% in the separate sessions,
and other depths also fail required precision gates. No threshold was widened
after observing results. Host isolation was not guaranteed, and no specific
cause of the remaining variation has been proved.

Disposition remains **pilot-only; no optimization accepted**. Allocation/CPU/
retention evidence and fixed-offered-load/fairness controls are still outstanding.

### Phase 2 provenance and evidence

- Config: `/Users/greg/dev/icanact-remediation-evidence/concurrent-aa-config.json`.
- Raw samples/logs/statuses/provenance/analysis:
  `/Users/greg/dev/icanact-remediation-evidence/concurrent-aa/`.
- Config SHA-256: `66c7d06848de06b04e5a32db5bb5aeda4c6ee733eccf10697b82578118cb05e5`.
- Runner SHA-256 during measurement: `be1c900fbf86f5e59a83cc178c2ed7e996d53c26cd44d0179b6b480c1bd77b53`.
- Tracked diff SHA-256 during measurement:
  `5cc4be8ec274bcdd428f4c44bf72c4f3b22b2df86fd53f5f7eda957c082b9f1d`.
- Existing offline runtime lock remained the same as Phase 1.
- Measurement command window: 2026-10-08 19:20:18 to 20:01:41 UTC.
- Total measured completions: **198,757,493 requests** (warmup excluded).
  Each measured window was between 10.0000 and 10.0140 seconds.
- One 512-in-flight preflight passed before the matrix (~225k requests/s);
  it is **not** included in the 160 matrix samples or used as a selected result.
- The fixture's initial compile type mismatch and premature inbound lookup
  failures are retained in separate preflight logs. The type was corrected,
  and setup now waits for the exact inbound publication precondition rather
  than sleeping/retrying a failed command. No product test assertion was weakened.
- Follow-up smoke test exercises all four depths; strict all-target/all-feature
  Clippy passes. Python runner/adapter regressions now total 19 passing tests.

## Phase 2 — historical connection-failure diagnosis

Opt-in diagnostic tracing and seeded peer identities were added to the existing
test helper. Normal test execution retains randomized identities and unchanged
assertions. Bounded diagnostic evidence is under
`/Users/greg/dev/icanact-remediation-evidence/discovery-diagnostic/`.

- 70 isolated seeded executions of `test_failure_recovery_backoff` passed.
  These diagnostic observations **do not erase the original full-suite failure**.
- 20 seeded complete-target runs were attempted; the 20th (seed
  `diagnostic-39`) failed `test_version_negotiation_v3_capabilities` on
  "Node A should negotiate peer discovery with node B".
- That trace records simultaneous known/unknown-identity dials, an inbound
  acceptance, connection-not-found sends and a peer reset. It is a concrete
  second correctness failure, not proof of the original identifying-FullSync
  failure's cause.
- The diagnostic batch stopped at its first failed target; no failed run was
  skipped or converted into an acceptance pass. Neither test was weakened.
- Public-key inventory mapped the failed capability case to seed
  `diagnostic-39`, node indexes 33/34. The diagnostic helper now supports an
  explicit offset to replay those identities. Eighty bounded isolated replays
  (two tracing levels) passed; they did not reproduce or erase the failed
  complete-target run. No deterministic scheduling reproducer exists yet.
- Root cause remains unresolved. No transport-lifecycle/negotiation fix is
  claimed, and default/full/platform acceptance remains blocked.

F12 documentation now describes V6 negotiation versus packed V5 framing,
all 15 kinds, active request-ID/NACK metadata and schema boundaries. A canonical
table/header-size/ALPN regression and all 20 framing tests pass; 15 handshake
tests also pass. This documentation/test change is not a performance
optimization and does not resolve the concurrent-discovery failures.

## Phase 3 — correctness recovery and complete local matrix

The mandatory local script completes all 20 steps with positive execution
counts. These are repeated feature/profile records and doctests, not unique tests.

| Lane | Selected | Passed records |
|---|---:|---:|
| Isolated real-TLS ask/reply | 4 | 4 |
| Workspace default/debug | 1,416 | 1,371 |
| Workspace test-helpers/debug | 1,467 | 1,421 |
| Workspace all-features/debug | 1,467 | 1,421 |
| Workspace default/release | 1,414 | 1,371 |
| Workspace all-features/release | 1,467 | 1,421 |

Total **7,009 passing records**; ignored tests were not counted as passing.
No-default library and default/all-feature all-target builds, strict Clippy,
rustdoc, formatting and both copy guards passed. Nineteen Python harness tests
passed separately. Completed 2026-10-09 00:02:47 UTC.

Main evidence:
`/Users/greg/dev/icanact-remediation-evidence/post-lifecycle-fixture-full-validation.log`;
per-command logs: worktree `logs/validation_20261008_201118_v0fKQP/`.

### Issue-keyed remediation

- **D01:** deterministic RED drives the real finalizer, parks identify, and
  installs a preferred replacement through production publication primitives.
  A usable survivor incorrectly resulted in identifying-FullSync
  `ConnectionAborted`. The narrow fix reports the existing-connection outcome
  only for a different usable current instance; its candidate cleanup guard
  remains armed. A dead replacement still fails. Both controlled cases pass,
  checking survivor identity/liveness, candidate alias removal and exact counts.
  The earlier prototype used an unconditional legacy publication helper and
  failed before the intended oracle; that is fixture-development evidence,
  not product RED.
- **D03:** three related rejection/ownership fixtures installed a wrong-direction
  survivor. They now use a preferred survivor and assert that precondition.
  Rejection, ownership, pointer/alias, sequence-exemption and later-message
  oracles remain intact.
- **D04:** native stacks and a deterministic one-worker RED prove a synchronous
  recorder gate strands a worker-local publication task. Test-only
  `block_in_place` hands off the scheduling core without releasing the pinned
  callback. Drop cleanup and an armed instance-specific pin prevent leaked
  workers/unrelated setup capture. All nine lifecycle-target tests pass.
- **D05:** a random client could sort above every member of its finite 100-key
  missing-peer inventory. A fixed, separately checked client removes this
  probabilistic setup failure. The TLS server remains random; the original
  outbound-preference and missing-peer failure oracles remain.
- **D06:** the release causal-fence fixture lacked an explicit strict source-time
  ordering guarantee. It now declares ordered historical source evidence and
  checks accepted claims, then uses actual fresh-reconnect time and its original
  delayed-release ownership assertions. Production's strict fence is unchanged.
  Exact timestamp collision was not captured in the original failure.

See [REMEDIATION_STATUS.md](REMEDIATION_STATUS.md) for details. These are
correctness/fixture changes, **not performance optimizations**. Phase 1/2
measurements predate D01 and are not post-fix measurements or an A/B cost claim.

### Retained failed evidence

All below are under `/Users/greg/dev/icanact-remediation-evidence/`:

| Evidence | Disposition |
|---|---|
| `d01-full-validation.log` | Default-library survivor fixture failure |
| `d01-d03-full-validation.log` | Default passed; helper lane interrupted at the host's 2,400-second deadline, no terminal cargo status captured |
| `d04-lifecycle-target.log` | Bounded target timeout |
| `d04-bounded-diagnostics/run-0.log`, status and native stacks | Diagnostic deadline 124; batch stopped at first failure |
| `d01-d03-d04-full-validation.log` | Isolated e2e missing-key setup failure |
| `d01-d03-d04-d05-full-validation.log` | Release causal-fence fixture failure |
| `d01-d03-d04-d05-d06-full-validation.log` | Related duplicate-survivor fixture failure |
| `d01-identify-red-corrected-fixture.log` | Controlled product RED, separate from prototype failure |
| `d03-ownership-fixture-red.log` | Invalid preference precondition demonstrated |
| `d04-lifo-starvation-red.log` | Controlled scheduling RED; corresponding GREEN retained |

No failed command was automatically retried into acceptance. Later full runs
followed scoped fixes; the older errors/timeouts remain. Historical capability
negotiation is still an open diagnostic, not declared fixed by the latest pass.

## Failed attempts retained

The first pilot stopped on a **metric adapter parsing error**: libtest printed its test-name prefix on the same line as the benchmark record. The underlying tell test passed, but no valid paired observation was accepted. Evidence is under `/Users/greg/dev/icanact-remediation-evidence/aa-pilot/`, including `blocked.json`.

The adapter was corrected to recognize that format while still requiring exactly one metric record. A regression test was added. The complete subsequent run used a fresh evidence directory; no partial samples from the failed attempt were combined with it. This was a tooling correction, not a product-failure retry into green.

## Phase 1 — historical correctness/validation evidence

| Gate | Observed result |
|---|---|
| Focused pool exhaustion/growth/reuse initialization regression | Passed |
| Observing AsyncRead tests: one-byte reads, Pending and truncated bodies in initial TLS reader and all three pooled read modes | Passed (2 tests) |
| Initial safety patch, full all-feature workspace suite | Passed: 1,411 passed-test records, including doctests; run preceded addition of the observing-reader tests |
| Format, library no-default build, all-target default/all-feature builds | Passed |
| Strict all-target/all-feature Clippy and warning-free rustdoc | Passed |
| Isolated default TLS e2e | Passed: 3 tests |
| Expanded default workspace suite | **Failed** on `test_failure_recovery_backoff`; validation stopped, not complete |
| Remaining full-script test-helper/all-feature/release lanes | Not reached in the full script after the failure; earlier all-feature run is not a replacement for these gates |
| Both copy guards | Passed separately |
| Python validation/A/B adapter/orchestration regressions | Passed: 16 tests, including fail-first/zero-selection/capture/timeout/source-drift/lock-error checks |
| Bash syntax and diff whitespace | Passed |
| Miri | Unavailable: component is not installed for `1.96.1-aarch64-apple-darwin`; no install attempted |
| Linux/supported-platform CI | Not run; CI matrix updated but no remote action triggered |

Full failed validation log: `/Users/greg/dev/icanact-remediation-evidence/s1-full-validation.log`; command/capture status was `101/0`, so output capture succeeded and the test command failed. The error was:

```text
Network(ConnectionAborted): Failed to connect to peer ...:
failed to send identifying FullSync to 127.0.0.1:64854
```

Source inspection locates the failed publication/identify path in `src/connection_pool/pool_connect.rs` and the explicit preferred dial in `tests/peer_discovery_tests.rs`. Concurrent discovery/connection ownership is a hypothesis only; no root cause has been established. Changing that behavior or loosening the test is not justified by this run.

Default builds also expose two existing unused-variable/assignment warnings for `applied` in `registry.rs`, whose consumption is feature-gated. Those were not suppressed or presented as fixed; all-feature strict Clippy passed.

## Phase 4 — main integration, resource correction and renewed validation

Conflict resolution retains main #237's canonical readers/writers, deleted private writer/parser/batch paths, regression coverage, dependency narrowing and existing alias behavior. The validation script retains the stronger feature/release/output evidence lanes plus main's `--focus` interface; focus cannot claim full validation. Both Python and shell harnesses pass (20 Python regressions).

Initial combined-tree matrix: formatting, no-default/default/all-feature builds, strict Clippy, rustdoc, isolated TLS (4/4) and default workspace (1,423 selected / 1,377 passing records) passed. Helper lane stopped at step 12 on `actor_timeout_does_not_destroy_healthy_session`, which received `Ok(remote:slow)` rather than Timeout. The run is retained in `conflict-full-validation.log`; later lanes were not run or called passes. D07 replaces timing-based response ordering with a worker-safe controlled reply gate and unwind cleanup, preserving the Timeout, healthy-session and later-reply assertions. All nine scripted-network tests pass after that scoped fixture change; the failed run's exact scheduling remains unproven.

The user's additional High security finding was not already fixed. **S13**: DirectAsk bypassed the intended pending-NACK cap through deferred actor-ask capacity. Controlled product RED measured a peak of **17,335** against a cap of **64** with every reply concurrently drained. Both read gates now require actual NACK capacity (including an active partial header), insertion is independently checked, and typed capacity failures cannot be swallowed as transient streaming pressure. Focused GREEN verifies 100,000 ordered replies across primary/idle, large/eight-byte and actual authenticated TCP/TLS scenarios, with peaks of 64 or 1. The insertion test preserves all prior outcomes on rejection. See [SECURITY_NACK_CAPACITY_FIX.md](SECURITY_NACK_CAPACITY_FIX.md) for retained attempts and limitations.

The ignored local lock was regenerated offline for main's manifest changes; diff inspection showed only removed unused test packages/edges, with no retained-package upgrades. SHA-256: `19c5ebe1eeb2cc3015e6dd6eb317fc306b58b13c47ad3135ad93b0c28b47f208`.

### Complete renewed matrix

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

This is correctness/resource-isolation evidence, **not a performance experiment**. Historical A/A measurements do not measure main's cleanup or S13. Their individual/cumulative costs, allocation/RSS and fixed-load fairness still require separate experiments. No OOM attack, speedup or allocator-savings claim is made.

## Phase 5 — post-merge discovery correctness continuation

Merged main became the new safe baseline for future performance work. The historical capability failure was still reproducible: an initial fixed-seed series passed 40 fresh full-target processes and failed on process 41; a later instrumented maximum-160 series stopped on capability failure at run 126. State at timeout showed one active peer and verified address→node attribution, but no configured-address alias and no capability projections. D08 retains immutable negotiated capabilities on the physical connection and resolves the live owner through either address alias or verified identity.

After D08, 160/160 fresh fixed-seed full-target processes passed, but random identities exposed identifying-FullSync failures in mesh/recovery scenarios. A traced failure showed an ordinary registry message being processed as the acceptor's first message before local identify, producing invalid identity parsing and connection teardown. D09 starts outbound IO behind an identify barrier, sends the first FullSync through the priority lane, and waits for physical write progress. A controlled wire test proves identify precedes ordinary traffic queued through the already-published connection.

After D08+D09, 100 fixed-seed and 100 random-identity fresh full-target processes all pass (3,800 test executions). Existing dead-stream/cancellation/restart/racing-ask checks pass. Earlier failures are retained and this bounded stress does not prove every interleaving.

The first complete continuation matrix then exposed two deterministic integration issues and stopped at default workspace step 10. D10 corrects the contention fixture's missing durable configured-peer precondition; D11 marks every authenticated alias failed when the physical peer has no replacement, instead of leaving an observed-source alias active. Both focused tests pass.

The renewed matrix then stopped twice more before completion. v2 failed strict Clippy on a non-minimal boolean, which was rewritten equivalently. v3 passed steps 1–16 and was aborted externally during the release all-features compile; that incomplete run is retained and is not a pass. v4 reached release/default and failed `publisher_recovers_from_every_round_of_connection_churn` because instance retirement cleared the successor's address-to-peer route. D12 publishes the connection index before that route and clears the route only while the retired instance still owns the address. Lookup also uses the live connection's embedded identity. The gap regression and five post-fix release runs of that publisher test pass. Validation v5 then stopped in the default workspace lane because the identify-supersession test treated a provisional address alias as the installed session and observed `connection_count` 0. The test now waits for the session slot and a count of 1 before identify; the assertion is unchanged. v5 is retained.

Validation v6 then completed the 20-step matrix at 2026-10-09 16:18:05 UTC, status 0: **7,079 passing records** (isolated TLS 4; default/debug 1,385 of 1,431; test-helpers/debug 1,435 of 1,482; all-features/debug 1,435 of 1,482; default/release 1,385 of 1,429; all-features/release 1,435 of 1,482). Repeated lanes and doctests, not unique tests; ignored tests are not passes. Evidence: `/Users/greg/dev/icanact-remediation-evidence/post-merge-continuation-full-validation-v6.log` and `logs/validation_20261009_122559_6ka5vb/`. Pre-run diff SHA-256 `373f183802ac37669b116fe07a71cda727b1c00cc6ad3c32cc0d4f3abd5c2ac2`; lock SHA-256 unchanged. v1–v5 remain retained. This paragraph is a documentation edit after that run. Detailed evidence: [DISCOVERY_CAPABILITY_REMEDIATION.md](DISCOVERY_CAPABILITY_REMEDIATION.md).

These changes are correctness/lifecycle remediation, **not performance optimizations**. Their connection-setup latency and retained-field cost are unmeasured. No improvement is claimed, and all previous throughput/A/A measurements predate them. Future optimization A/B work must branch from merged main including these safety fixes; an unsafe or misidentified session is not an admissible faster baseline.

## Phase 6 — fixed offered-load harness

`measure_offered_asks` schedules the same TLS ask path at a constant rate. Latency starts at the scheduled instant, and an offer still unissued after the lateness bound is a drop. `scripts/sample_offered.py` rejects a short window, a drop, or a backlog above the declared cap. The 50 ms smoke and the schedule unit test pass. This fixture is not in validation v6.

Two single release proofs were run at 1,000 offers/s, 5 s warmup, 10 s window, cap 64, and 1 s lateness. Neither is an A/A or a service-time baseline.

- `offered-load-harness-proof.log`: 10,000/10,000 completed, 0 drops. p50 1.352 ms, p95 8.491 ms, p99 19.267 ms, max backlog 64. The issuer could enqueue a full cap of unpolled asks, so this backlog is a fixture defect.
- `offered-load-harness-proof-v2.log`: after polling ready completions before the next issue, 10,000/10,000 completed, 0 drops. p50 1.303 ms, p95 2.884 ms, p99 6.263 ms, max backlog 22. The cap is no longer saturated. A 1 ms offer interval is at host timer granularity, so this one sample still does not establish unloaded service latency.

Two more single release samples were then run, before the A/A config was frozen, and were not used to change the planned 1,000/s rate. `offered-load-rate-100.log`: 1,000/1,000, 0 drops, p50 2.066 ms, p99 4.625 ms, max backlog 2. `offered-load-rate-10000.log`: 100,000/100,000, 0 drops, p50 0.268 ms, p99 1.307 ms, max backlog 36. The low-rate backlog is small while its latency stays in milliseconds, which is consistent with sleep lateness being part of intended-start latency. Neither sample is an A/A.

The frozen pilot is `offered-aa-config.json`, experiment `E0-AA-offered-asks-1000`: identical source in both arms, 10 pairs × 2 sessions, seed 20261009, 1,000/s, 5 s warmup, 10 s window, cap 64, 1 s lateness. Primary metrics are p50/p95/p99. `offer_window_ns` is the duration contract. `achieved_rate` and `max_backlog` are not in the 3% gate. It ran 2026-10-09 18:07:08–18:17:31 UTC, 40/40 processes exit 0, every sample 10,000/10,000 with 0 drops. Evidence: `/Users/greg/dev/icanact-remediation-evidence/offered-aa/`. Config SHA-256 `ab45ecdb012f346d42cce6683cbe4dea84a9740bdc23f3e33e1b930eb3a7c38f`; runner SHA-256 unchanged from the concurrent pilot; measured diff SHA-256 `a6f9bbf5888ce068baa48acf05efbe9617eac5da70b418b9faa73ca7935db035`; lock SHA-256 unchanged. Across the 40 samples, p50 was 0.741–8.707 ms (median 1.013 ms), p99 was 1.629–184.1 ms (median 2.311 ms), and max backlog was 2–64 (median 17). Three samples sat on the cap; the largest p99 was 184.1 ms.

Pooled paired cost change and 95% CI, lower-is-better: p50 +8.03% [−8.34%, +31.85%]; p95 +11.50% [−6.64%, +32.98%]; p99 +3.73% [−9.90%, +33.38%]. Every latency gate failed. Session 1 p99’s upper bound was +287.7%. The offer window was exactly 10 s in every sample, so that control passes without measuring noise. Runner disposition is `pilot-only` because the config mode is pilot; that exit is not a passed precision gate.

Allocation, CPU, ownership, and duplex-fairness counters are still absent. No optimization is accepted. The 3% threshold was not widened.

The issuer clock was then changed, after that failed A/A, and was not used to retarget it. Three single release samples at 100/s, same warmup, window, cap, and lateness, compared clocks. None is an A/A or a service-time baseline. Closed-loop inflight-1 median run p50 was 42.29 µs, so a 0.25 ms offered-load median is still several times the saturated single-flight figure and still includes issue delay.

- Relative sleep, waking 1 ms early (`offered-load-spin100-1.log` through `-3`, 18:28–18:30 UTC): 1,000/1,000, 0 drops, backlog 1–2. p50 1.260–1.287 ms. p99 3.549–11.053 ms (max/min 3.11). The sleep was still the median.
- `mach_wait_until`, spinning the last 200 µs (`offered-load-mach100-1.log` through `-3`, 19:41–19:42 UTC): 1,000/1,000, 0 drops, backlog 2–4. p50 2.976–3.230 ms. p99 6.829–8.306 ms. Worse than the relative sleep. That clock was removed.
- Spin for any wait of 20 ms or less (`offered-load-spin20-100-1.log` through `-3`, 19:44–19:45 UTC): 1,000/1,000, 0 drops, backlog 1. p50 0.241–0.254 ms. p99 0.392–0.446 ms (max/min 1.14). This is the issuer left in the tree. A 100/s interval is 10 ms, so these samples do not park.

The 20 ms spin’s three-sample p99 ratio is under 3×. That is not host isolation and not a reason to open another preregistered A/A or to move the frozen 1,000/s experiment. The failed 1,000/s matrix still stands.

## Every proposed optimization: disposition

| Finding | Implemented? | Individual optimization A/B | Current disposition |
|---|---|---|---|
| F01 initialized receive storage | Yes, safety correction | Unsafe old arm deliberately excluded; future safe zero-fill-elision comparison pending | Required correctness fix, local matrix passed; cost unmeasured, no speedup claim |
| F02 validation reporting | Yes; CI lanes and harness tests added | No validation-time improvement claimed | Reporting logic tested; latest 20-step matrix passed, earlier failures retained |
| F03 obsolete writers/helpers | Inherited from main #237 | Pending plan-compliant individual evidence | Retained; not accepted as a measured optimization |
| F04 reader consolidation | Inherited from main #237 | Pending | Canonical reader retained with initialized storage; no timing acceptance |
| F05 empty direct batch | Inherited from main #237 | Pending allocation + runtime comparison | Removal retained; no measured allocator result |
| F06 speculative parser states | Inherited from main #237 | Pending | Real parsed-result path retained; no inferred fast-path gain |
| F07 writer consolidation | Inherited from main #237 | Pending | Canonical poller retained; no timing acceptance |
| F08 chunk progress optimization | No | Not run | Blocked. `skip_written_chunks` still rescans. F07 has no measured non-regression. Cleanup already recorded no-change for a cursor. Not accepted and not freshly rejected |
| F09 connection ownership | No. A cfg-gated prototype was restored before this branch was published | Not run | Deferred. Release builds still keep the private original snapshot. `connection_ref` remains the live slot |
| F10 PubSub shared backing | No. A shared-buffer prototype and its pointer test were restored before this branch was published | Not run | Deferred. Each matching category still builds its own `Bytes` |
| F11 dependencies/features | Inherited from main #237 | Pending matched graph/build/consumer performance comparison | Narrowing retained; no build/runtime gain claimed |
| F12 wire documentation | Yes, plus canonical table/ALPN regression | Documentation-only, no performance claim | 20 framing and 15 handshake tests pass; overall acceptance still blocked |
| S13 pending-NACK resource isolation | Yes, user-authorized additional safety scope | Cost unmeasured; no performance claim | Controlled RED/GREEN, actual TLS and insertion preservation pass; full renewed matrix passed; performance acceptance still held |
| D08 live capability ownership | Yes, correctness continuation | Connection/retention cost unmeasured; no performance claim | Deterministic projection-loss regression and 160 fixed-seed full targets pass |
| D09 identify-first outbound owner | Yes, correctness continuation | Connection-setup cost unmeasured; no performance claim | Controlled wire-order regression plus 100 fixed/100 random full targets pass; continuation matrix v6 passes |
| D10 contention fixture configuration | Test-only correction | No runtime claim | Production configuration precondition explicit; focused target and continuation matrix v6 pass |
| D11 authenticated-alias failure accounting | Yes, correctness continuation | Failure-path cost unmeasured; no performance claim | Deterministic alias-state RED and focused integration GREEN; continuation matrix v6 passes. Healthy-session quietness is not asserted |
| D12 successor address route | Yes, correctness continuation | No performance claim | Release lookup RED and v5 provisional-alias sample retained; gap regression passes; continuation matrix v6 passes |

## Required next steps

1. D01's identifying-FullSync mechanism is reproduced/fixed and the local matrix completes. Keep the historical capability-negotiation finding open; do not infer its cause from those passes.
2. The post-merge continuation matrix v6 passes (7,079 records). Supported-platform CI remains unrun and Miri unavailable; do not call them passes. v1–v5 stay retained.
3. Long concurrent windows, full reply identity, and one offered-load A/A at 1,000/s are measured. The offered-load latency gates failed, as recorded above. Add allocation/copy counters, ownership probes and fairness controls. Verify the actual changed paths, especially mixed borrowed PubSub ingress.
4. Improve host isolation before another preregistered A/A pilot. Do not widen the 3% gate. Do not retarget the frozen 1,000/s run because three 100/s spin samples were tighter. Preserve the closed-loop matrix, the failed offered-load A/A, and the later clock probes.
5. F08–F10 are deferred. They are not in this branch. Inherited F03–F07/F11 still need individual A/B results. The current classification is [LEGACY_INVENTORY.md](LEGACY_INVENTORY.md). The cumulative safe-baseline comparison and rollback evidence are still open.

This report is the current measured handoff, **not the final optimization-success report**. The complete remediation plan remains open.
