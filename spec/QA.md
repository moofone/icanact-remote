# QA Plan (Deterministic Gossip + Safety/Perf Guardrails)

**Repo:** `/Users/greg/Dev/git/icanact_remote`

**Primary goal:** Keep the gossip/registry correct and deterministic under chaos (drop/dup/reorder/partition/churn) while protecting performance.

**Secondary goal (audit readiness):** Maintain a continuously-updated inventory of “rubric” primitives (locks/channels/async_trait) and avoid introducing new ones in hot paths. Removal of existing locks/channels is a separate, explicitly-scoped refactor because it is high-risk for correctness and performance.

**Primary quality targets (in addition to rubric):**
- Deterministic, convergent gossip/registry behavior under concurrency and reordering.
- Bounded resource usage over time (no untracked background tasks; bounded queues/histories; no leaks).

This document is written as an external-audit plan: each step has a concrete scope, gating checks, and artifacts to prove correctness.

---

## 1. Scope And Non-Goals

**In scope**
- Production crate code in `src/` and test code in `tests/` and `examples/` if required for rubric enforcement.
- Gossip protocol determinism and convergence.
- Resource bounding and shutdown determinism.

**Non-goals**
- Rewriting network framing or IO unless needed to remove locks/channels or fix determinism/resource issues.
- “Performance at all costs.” We will avoid hot-path regressions, but correctness + bounded resources are non-negotiable.

---

## 2. Definition Of Done (DoD)

This effort is **DONE** only if all are true:

1. **Determinism**:
   - Concurrent/conflicting updates converge deterministically with stable tie-break rules.
   - Any iteration-dependent logic that affects protocol-visible output is made stable (sorting or deterministic containers).
2. **Bounded resources**:
   - No untracked `tokio::spawn` loops that survive shutdown.
   - Bounded histories/queues (delta history, pending changes, pending asks/acks).
3. **No performance regressions without evidence**:
   - Any change that touches the gossip hot path, the ask/response path, or per-message allocation must include a before/after measurement (bench or harness) and a short explanation if it regresses.
4. **Evidence artifacts exist**:
   - `cargo test` is green (local and, ideally, Linux CI).
   - Targeted integration/e2e tests exist for gossip convergence and deterministic tie-breaks (including chaos/partition invariants).
5. **Rubric inventory is current (non-blocking unless explicitly scoped)**:
   - A scan report exists classifying hits as CODE vs COMMENT/DOC.
   - No new uses of locks/channels/async_trait are introduced in hot paths without explicit justification and perf evidence.

---

## 3. Deterministic Gossip: Required Invariants

These are the concrete, protocol-visible invariants we protect (and must regression-test):

1. **Order independence for concurrent or “equal-clock” updates**:
   - If two updates for the same actor are causally `Concurrent` or compare as `Equal` (common at initial zero vector clocks), applying them in different arrival orders must converge to the same winner.
2. **Stable, protocol-visible ordering**:
   - Full-sync and bootstrap delta snapshots must not leak hash iteration order into on-wire message content.
3. **Full-sync omission safety**:
   - A peer omitting an actor from a full sync must not cause deletion unless the actor is still attributed to that peer (prevents stale peers from deleting actors that moved).
4. **Safe fallback under truncation**:
   - When delta history is insufficient for a peer’s requested sequence, the system must fall back to full sync (no silent partial replay).

Evidence (tests):
- `src/registry.rs` internal deterministic tests cover order independence, omission safety, stale full-sync rejection, and fallback.
- `tests/gossip_chaos_scheduler.rs` covers deterministic convergence under reorder/dup across seeds.
- `tests/gossip_partition_invariant_e2e.rs` asserts “no propagation across cut” without `lookup()`-triggered dials.

---

## 4. Baseline Evidence (What We Know Right Now)

### 4.1 Edition
- `Cargo.toml` has `edition = "2024"` (already satisfies Rust 2024).

### 4.2 `ask()` Does Not Require `oneshot`

The `ask()` request/response path is currently backed by a per-connection correlation tracker with an `AtomicWaker`, not by `tokio::sync::oneshot`.

Evidence in code:
- `src/connection_pool.rs`: `CorrelationTracker` is a fixed-size pending-slot table with an `AtomicWaker` per slot and delivers responses by correlation id.
- `src/connection_pool.rs`: `ConnectionHandle` ask paths allocate a correlation id, write an ask frame, then await `CorrelationTracker::wait_for_response(...)`.
- `src/connection_pool.rs`: `PendingAsk` exists for “defer the wait” use cases and cancels on drop to keep resources bounded.
- `src/remote_actor_ref.rs`: `RemoteActorRef::ask()` delegates to `ConnectionHandle::ask()`.

Therefore: removing `tokio::sync::oneshot` from tests/other code will not break `ask()` as long as the correlation tracker remains intact.

### 4.3 Known Rubric Primitive Uses (Inventory)

At time of writing, the following exist in-tree. This list must be regenerated by the Phase A scan whenever we take on an audit or “remove primitives” task.

Important: this section is an **inventory**, not an automatic “must remove now” list. Removing core synchronization primitives is risky; it must be explicitly scoped and measured.

**Locks**
- `src/registry.rs`: `gossip_state: Arc<tokio::sync::Mutex<GossipState>>` and widespread `gossip_state.lock().await`.
- `src/registry.rs` internal tests: `tokio::sync::Mutex` used as a test signal wrapper.
- `src/test_helpers.rs`: `std::sync::Mutex` used for raw payload capture (compiled under `cfg(any(test, feature = "test-helpers", debug_assertions))`).
- `src/connection_pool.rs` (`#[cfg(test)]`): `std::sync::Mutex` used for the recording writer buffer.
- `tests/peer_discovery_tests.rs`: `std::sync::Mutex` used to serialize multi-socket tests.
- `tests/ask_reply_end_to_end.rs`: `std::sync::Mutex` used to serialize E2E tests.
- `tests/ask_with_lookup_demo.rs`: `tokio::sync::Mutex` used as a global test guard.
- `tests/streaming_tests.rs`: `tokio::sync::Mutex` used for cross-task result aggregation.
- `examples/bad_client_stress.rs`: `tokio::sync::Mutex` used for a stress-test handler state table.
- `examples/console_tell_ask_client.rs`: `std::sync::Mutex` used for warmup error reporting.

**Channels**
- `src/registry.rs` internal tests: `tokio::sync::oneshot`.
- `examples/console_tell_ask_client.rs`: `tokio::sync::watch`.
- `tests/reconnect_bench_e2e.rs`: `tokio::sync::watch`.

---

## 5. Risk Management And Safe Refactor Rules

To avoid breaking production behavior, we enforce these rules during migration:

1. **Small steps, always-compiling**: no “big bang” refactor. Each step must compile (`cargo test --no-run`) before proceeding.
2. **Protocol-visible behavior changes must be tested**:
   - Any change that can affect the contents/order of gossiped messages must have a test that asserts stability and convergence.
3. **No dual-path conflict resolution**:
   - One canonical “apply delta” implementation; no fast-path divergence (already addressed for delta apply).
4. **All background tasks must be trackable and cancellable**:
   - Every `tokio::spawn` must register an abort handle and be aborted on shutdown (or be proven bounded and self-terminating).
5. **Bounded data structures**:
   - Pending change buffers, delta history, and pending asks/acks must have explicit size bounds and cleanup.
6. **No new unsafe in production by default**:
   - Prefer safe lock-free primitives (`Atomic*`, `AtomicWaker`, `arc-swap`, `scc`) and deterministic single-owner state.
   - If unsafe is unavoidable, document invariants in-code and add targeted tests that would fail under reordering/tearing/ABA-style bugs.
7. **Eviction/overflow must preserve correctness**:
   - If a bounded structure overflows or evicts required history, the protocol must fall back to a safe full-sync path (never silently drop and “hope gossip catches up”).
8. **Reproducible evidence**:
   - Evidence runs must not depend on `rust:latest`; pin an image tag (or digest) and record `rustc -Vv`.

---

## 6. Plan Of Work (Phased)

### Phase A: Audit Scans (No Behavior Changes)

**Goal:** produce a reproducible rubric scan and determinism risk list.

Actions:
1. Add/maintain documented scan commands in this doc (no new scripts required). The scan MUST catch:
   - fully-qualified uses (`tokio::sync::Mutex`)
   - brace imports (`use tokio::sync::{Mutex, ...}`)
   - unqualified generics (`Arc<Mutex<T>>`)

   Recommended commands (exclude build dirs, scan only relevant code):
   - `rg -nS --glob '!target/**' --glob '!target-linux/**' --glob '!.git/**' "async-trait|async_trait" Cargo.toml Cargo.lock src tests examples`
   - `rg -nS --glob '!target/**' --glob '!target-linux/**' --glob '!.git/**' "tokio::sync::(Mutex|RwLock)|std::sync::(Mutex|RwLock)|tokio::sync::\\{[^}]*\\b(Mutex|RwLock)\\b|std::sync::\\{[^}]*\\b(Mutex|RwLock)\\b|\\bMutex<|\\bRwLock<|parking_lot|DashMap|dashmap" src tests examples`
   - `rg -nS --glob '!target/**' --glob '!target-linux/**' --glob '!.git/**' "tokio::sync::(mpsc|oneshot|watch|broadcast)|std::sync::mpsc|tokio::sync::\\{[^}]*\\b(mpsc|oneshot|watch|broadcast)\\b|\\b(mpsc|oneshot|watch|broadcast)::|crossbeam_channel|flume" src tests examples`
2. These scans are intentionally broad and will match comment/doc mentions. In the report:
   - classify each hit as CODE vs COMMENT/DOC
   - do not sign off until all CODE hits are eliminated
3. Confirm remaining violations are limited to the Known Violations list (Section 3.3); if not, update 3.3 before proceeding.
4. Audit `tokio::spawn` and `unsafe` usage:
   - `rg -nS --glob '!target/**' --glob '!target-linux/**' --glob '!.git/**' "tokio::spawn\\(" src`
   - `rg -nS --glob '!target/**' --glob '!target-linux/**' --glob '!.git/**' "\\bunsafe\\b" src`
5. Identify any protocol-visible iteration over unordered maps/sets and enumerate call sites (anything affecting message content/order must be stabilized).

Gating checks:
- `cargo test --no-run` on Linux Docker (see Phase F).

Artifacts:
- A `reports/qa_rubric_scan_<date>.md` file containing the scan output and the list of remaining violations.

---

### Phase B: Remove Prohibited Primitives From Tests/Examples (No Protocol Changes)

**Goal:** tests/examples/debug helpers must not depend on prohibited locks or ad-hoc channels.

Targets (minimum):
- `src/registry.rs` internal tests that use `tokio::sync::oneshot` + `tokio::sync::Mutex` as a “disconnect handler signal”.
- `src/test_helpers.rs` (raw payload capture) `std::sync::Mutex`.
- `src/connection_pool.rs` (`#[cfg(test)]`) `std::sync::Mutex`.
- Any remaining `Mutex`/`RwLock` usage in `tests/` and `examples/` (see Section 3.3).
- Any remaining `tokio::sync::{oneshot,mpsc,watch,broadcast}` usage in `tests/` and `examples/` (see Section 3.3).

Approach:
- Prefer SAFE lock-free patterns (no new unsafe):
  - `TestSignal<T>` built from `arc_swap::ArcSwapOption<T>` + `futures::task::AtomicWaker`:
    - `set(T)` stores `Some(Arc<T>)` then wakes.
    - `take()` uses `swap(None)` for one-shot consumption.
    - `wait(timeout)` uses `poll_fn` + `tokio::time::timeout`.
  - Replace result aggregation `Mutex<Vec<_>>` patterns with “return value from JoinHandle” patterns (collect results from `join_all` / sequential `await`).
  - Replace `tokio::sync::watch` with `ArcSwapOption<T>` + `Notify`/`AtomicWaker` for “latest value” state propagation.
  - Replace global test-serialization `std::sync::Mutex` with an atomic spin-guard (CAS + `yield_now`) or, better, make tests independent (unique ports, idempotent crypto init).
- For debug/test capture (`src/test_helpers.rs`), use a bounded, lock-free buffer:
  - e.g. `crossbeam_queue::ArrayQueue<Bytes>` (fixed capacity), plus `Notify` to wake waiters.
  - define overflow behavior (drop-oldest or drop-newest) and expose counters for diagnostics.

Gating checks:
- `cargo test --no-run`
- run the affected tests/examples that previously referenced prohibited primitives.

Artifacts:
- Diff clearly showing removal of prohibited primitives from the targeted files.
- Updated rubric scan report (Phase A) showing the delta.

---

### Phase C: Eliminate `gossip_state: Arc<Mutex<GossipState>>` (Core Rubric Blocker)

**Goal:** remove the last major lock violation without breaking gossip correctness.

Constraints:
- No new ad-hoc channels (`mpsc`/`oneshot`) to replace the lock.
- Avoid clone-heavy “copy whole state” snapshots on hot paths.

Plan (incremental extraction, not a rewrite):
0. **Baseline first (required for safety/perf)**:
   - Capture pre-change perf snapshots for at least one representative harness (see Phase G).
   - Capture a pre-change rubric scan report (Phase A).
1. **State invariants (write down before refactor)**:
   - Enumerate cross-field invariants currently “implicitly atomic” due to the mutex (e.g., peer addr migration updates peers + known_peers + actor mapping).
   - For each invariant, decide: “single-structure atomic update” (preferred) vs “explicit two-phase update + compensating check”.
2. **Reduce multi-structure invariants by grouping**:
   - Prefer a single concurrent map keyed by `SocketAddr` that stores all per-peer state (info, actor set, health reports, pending failure state) so that one entry update preserves invariants.
   - Avoid “N separate maps” unless invariants can be proven independent.
3. **Replace `known_peers: LruCache` with a bounded, deterministic structure**:
   - LRU semantics are optional; boundedness and safety are mandatory.
   - Example approach: `scc::HashMap<SocketAddr, KnownPeerEntry { peer_info, last_seen_ms: AtomicU64 }>` plus periodic prune to capacity:
     - choose removal candidates deterministically (sort by `(last_seen_ms, addr)` before removal)
     - never re-gossip unsafe/bogon entries (preserve current security filters)
4. **Pending change buffers become bounded queues (with correctness-preserving overflow)**:
   - Use `crossbeam_queue::ArrayQueue<RegistryChange>` for `pending_changes` and `urgent_changes`.
   - Overflow MUST trigger a safe fallback (e.g., set a `needs_full_sync` flag so peers perform a full sync) instead of silently dropping changes.
5. **Delta history becomes a bounded ring (with correctness-preserving eviction)**:
   - Implement a bounded ring buffer (slot state machine like the correlation tracker is acceptable).
   - If a peer requests a delta sequence that has been evicted, force a full sync (do not return partial/inconsistent deltas).
   - Ensure deterministic replay order by sorting by `sequence` before encoding protocol-visible output.
6. **Stabilize protocol-visible iteration**:
   - Whenever selecting peers/changes for a gossip round, sort the addresses/keys before forming a message.
   - Never rely on iteration order of `HashMap`, `scc::HashMap`, or sets for message content.

Gating checks (per sub-step):
- `cargo test --no-run`
- targeted tests:
  - deterministic peer selection and gossip round formation
  - delta history boundedness under churn
  - shutdown clears state and terminates tasks
  - overflow/eviction triggers full-sync fallback (explicit tests)

Artifacts:
- A short “before/after” description in `reports/` citing the exact removed lock and the replacement structures.

---

### Phase D: Task Lifecycle And Bounded Shutdown (P2 Resource Concerns)

**Goal:** no untracked background tasks; shutdown must be deterministic.

Actions:
1. Introduce a small lock-free task tracker (pattern already exists as `DiscoveryTaskTracker`).
2. Replace any “spawn and forget” tasks with tracked abort handles:
   - bootstrap-per-seed tasks must be registered and aborted on shutdown
   - delayed consensus query tasks must be registered and aborted if superseded or shutdown triggers
3. Add tests that:
   - set shutdown flag
   - assert tasks stop making progress (or counters stop changing)

Gating checks:
- `cargo test --no-run`
- run targeted integration tests that exercise churn/shutdown.

Artifacts:
- A checklist entry in PR description mapping each previously-untracked spawn site to its tracking mechanism.

---

### Phase E: Determinism Hardening (Gossip Out-Of-Sync Risks)

**Goal:** prove convergence and deterministic outcomes even under reorderings.

Actions:
1. Add/extend a “gossip matrix” test (or similar) that:
   - applies concurrent updates from multiple peers
   - permutes delivery order
   - asserts final registry state matches across nodes
2. Add a test that ensures tie-breakers are stable:
   - concurrent add/remove or add/add with same vector clock produces same winner across nodes
3. Remove all fixed sleeps in tests; use explicit waits with timeouts (a pattern already started in peer discovery tests).

Gating checks:
- all tests pass on Linux Docker.

Artifacts:
- test output summary and the new/updated tests referenced by filename.

---

### Phase F: Evidence Runs (Linux Docker)

**Goal:** reproducible compile/test evidence on Linux.

Commands (baseline):
```bash
# Pin a specific tag (or digest); DO NOT use rust:latest.
# Example: match the repo toolchain used for development/evidence.
RUST_IMAGE="rust:1.91.1"

docker run --rm -e CARGO_INCREMENTAL=0 \
  -v "$PWD:/work" -w /work \
  -v "$HOME/.cargo/registry:/usr/local/cargo/registry" \
  -v "$HOME/.cargo/git:/usr/local/cargo/git" \
  -v "$PWD/target-linux:/work/target" \
  "$RUST_IMAGE" sh -lc 'rustc -Vv && cargo test -j 1 --locked --no-run'
```

If compilation is clean, run full tests:
```bash
docker run --rm -e CARGO_INCREMENTAL=0 \
  -v "$PWD:/work" -w /work \
  -v "$HOME/.cargo/registry:/usr/local/cargo/registry" \
  -v "$HOME/.cargo/git:/usr/local/cargo/git" \
  -v "$PWD/target-linux:/work/target" \
  "$RUST_IMAGE" sh -lc 'rustc -Vv && cargo test -j 1 --locked'
```

Artifacts:
- Copy/paste of the summarized output into `reports/qa_linux_test_<date>.md` (or PR body).

---

### Phase G: Performance Evidence (Regression Guard)

**Goal:** avoid correctness-fixing refactors that quietly regress throughput/latency.

Actions:
1. Capture a baseline before Phase C (and any other hot-path change):
   - `cargo bench`
   - ask/response harness (example): run `examples/console_tell_ask_server.rs` + `examples/console_tell_ask_client.rs` with a fixed configuration and record QPS + allocations.
2. Repeat the same measurement after the change.
3. If there is a regression, document why and either:
   - optimize it back, or
   - justify it as acceptable with compensating evidence (bounded resources, determinism fixes, etc.).

Artifacts:
- `reports/qa_perf_<date>.md` with:
  - exact command lines
  - machine/OS info (brief)
  - before/after numbers

---

## 6. Sign-Off Criteria

**PASS** only when:
- Rubric scan is clean for `src/`, `tests/`, and `examples/` (or explicitly documented exceptions removed).
- Gossip convergence tests demonstrate deterministic final state across nodes.
- Bounded resources tests show no leaked/lingering tasks and bounded state growth.
- Linux Docker evidence is captured.
- Performance evidence exists for any hot-path touching change (Phase G), with no unexplained regressions.

Otherwise: **BLOCK**.
