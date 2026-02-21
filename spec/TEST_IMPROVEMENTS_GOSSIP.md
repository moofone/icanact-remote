# Gossip Test Improvements (Chaotic Network + Deterministic Proofs)

**Repo:** `/Users/greg/Dev/git/icanact_remote`  
**Goal:** materially improve gossip-specific coverage to prove deterministic convergence and correctness under chaotic network conditions (drop/dup/reorder/partition/churn), without introducing flake or performance regressions.

This spec is intentionally concrete: it lists the exact kinds of scenarios we will implement, what we will assert, and what small test-harness capabilities we may add to make the tests deterministic and non-invasive.

---

## 1. Current Coverage Snapshot

We already have good “happy path” and some chaos-adjacent coverage:
- Convergence on a simple topology (line): `tests/gossip_matrix_e2e.rs`
- Full sync vs delta + urgent propagation: `tests/gossip_e2e_tests.rs`
- Removal propagation: `tests/gossip_removal_e2e.rs`
- Partition heal test exists but explicitly cannot assert “no propagation during partition” due to lookup-triggered direct connections: `tests/gossip_partition_e2e.rs`
- Vector clock behavior (unit-level): `tests/vector_clock_test.rs`

There is also a directory of tests under `tests/unit/`, but **Cargo does not automatically compile integration tests from subdirectories**. Those files are currently **not executed** unless wired in by a top-level integration test crate entry.

---

## 2. Gaps We Must Close (What “Chaotic Network” Really Means)

### 2.1. Deterministic conflict resolution is not directly tested
The registry has deterministic tie-break logic for concurrent vector clocks (e.g. stable ordering by NodeId), but there are no direct tests that:
- deliver concurrent updates in multiple arrival orders, and
- prove the final state is identical.

This is the core “prove deterministic flows/state updates” requirement.

### 2.2. Message-level chaos (drop/dup/reorder) is not represented
TCP won’t reorder frames. If we only use real sockets, we can’t exercise reordering or duplication meaningfully.

We need a deterministic *message scheduler* at the gossip message layer that can:
- permute delivery order
- duplicate delivery
- drop messages
- delay delivery

### 2.3. Partition tests are currently bypassed by `lookup()` behavior
Calling `lookup()` can cause new connections to be established, collapsing the intended network topology and invalidating “partition blocks propagation” assertions.

Partition assertions must be expressed without triggering dials.

### 2.4. Bounds/eviction/overflow correctness is not proven end-to-end
We have some bounds tests, but we need to prove the protocol’s behavior remains correct when:
- delta history has been truncated
- pending buffers overflow
- a peer requests data that is no longer available

The required behavior is “safe fallback” (usually full sync), not silent loss.

---

## 3. Definition Of Done (DoD)

The “gossip improvements” work is DONE only when all of the following are true:

1. **Determinism proofs exist**:
   - For each core conflict pattern (add/add concurrent, add/remove concurrent, stale message arrival), we have at least one test that delivers events in multiple orders and asserts identical final state.
2. **Chaos semantics are covered deterministically**:
   - At least one test suite uses a seeded message scheduler that can permute/dup/drop gossip messages and still converges.
3. **Partition tests assert the real invariant**:
   - During a forced partition, updates do not propagate across the cut.
   - After heal, convergence occurs.
   - Tests must not rely on `lookup()` during the “partition active” phase.
4. **Bounds/eviction is safe**:
   - When history is evicted or buffers overflow, the system deterministically triggers safe fallback (full sync) and converges.
5. **No new flake**:
   - All new integration/E2E tests must use `wait_for_condition`-style polling with explicit timeouts and should avoid fixed sleeps except for minimal “settle” windows.
6. **Performance guardrails**:
   - Any hot-path behavior changes required to enable these tests must include a before/after measurement (bench/harness) and should not regress without explanation.

---

## 4. Work Items (We Will Do All Of These)

### GOSSIP-T0: Wire `tests/unit/*` so they actually run

**Problem:** `tests/unit/*.rs` is currently not compiled/executed by Cargo.

**Change:**
- Add a top-level integration test entrypoint, e.g. `tests/unit.rs`, that does:
  - `mod unit;` (and `tests/unit/mod.rs` re-exports the submodules), or
  - move the submodule files into `tests/` root.

**Acceptance:**
- `cargo test` reports executing the tests formerly living in `tests/unit/*`.

---

### GOSSIP-T1: Deterministic conflict-resolution tests (order independence)

**Focus:** prove deterministic winner selection for concurrent vector clocks and deterministic application under reorderings.

**New tests (unit-level, no sockets):**
1. **Concurrent add/add**:
   - Same actor name, two different host peers, vector clocks concurrent.
   - Deliver update A then B vs B then A into a receiver.
   - Assert final `known_actors["actor"]` is identical (same peer_id, same address).
2. **Concurrent add/remove**:
   - Receiver sees an add and a remove with concurrent clocks.
   - Deliver in both orders.
   - Assert final state matches deterministic tie-break rules.
3. **Stale self-announcement suppression**:
   - Ensure remote updates that claim `peer_id == self.peer_id` do not resurrect a locally unregistered actor.
4. **Idempotency / duplicate delivery**:
   - Deliver the same delta twice (or more) and assert final state unchanged.

**Where:**
- Prefer internal tests in `src/registry.rs` for direct access to helpers (no public API required).
- Add minimal “black box” variants in `tests/` only if required.

**Acceptance:**
- Each scenario has a “permutations” style test (at least 2 orders) and asserts identical state snapshot.

---

### GOSSIP-T2: Message scheduler for chaos (drop/dup/reorder) at gossip layer

**Why:** to test chaos semantics that TCP cannot produce.

**Design:**
- A deterministic scheduler that holds a queue of “deliverable” events:
  - `DeliverDelta { from, to, delta }`
  - `DeliverFullSync { from, to, full_sync }`
  - optionally `Disconnect { a, b }` and `Reconnect { a, b }` for E2E variants
- The scheduler supports:
  - `permute(seed)` for delivery ordering
  - `duplicate(probability or exact count)`
  - `drop(probability or predicate)`
  - `delay(k steps)`

**State assertions:**
- Use a stable snapshot function (see GOSSIP-T3) to compare entire actor maps and any relevant peer metadata.

**Acceptance:**
- At least one test that runs the same scenario across multiple seeds and asserts all seeds converge to the same final state.

---

### GOSSIP-T3: Snapshot/assert helpers that do not trigger network dials

**Problem:** `lookup()` can create connections, altering topology and invalidating partition assertions.

**Change:**
- Add test helper(s) to snapshot registry state without dialing:
  - `known_actors` keys and per-actor resolved location (peer_id, address string, vector clock summary)
  - local actor set
  - optional: per-peer metadata relevant to gossip correctness (sequence tracking)

**Where:**
- Preferred: implement as `#[cfg(any(test, feature = "test-helpers"))]` methods on `GossipRegistry` / `GossipRegistryHandle`.
- Alternate: tests can directly read `registry.actor_state.local_actors` / `known_actors` via `iter_sync` and avoid `lookup()`.

**Acceptance:**
- Partition and chaos tests assert state via snapshot, and do not call `lookup()` during the partition-active phase.

---

### GOSSIP-T4: Partition tests that actually prove partition invariants

**New E2E test scenario (3 nodes, line topology A-B-C):**
1. Connect A<->B and B<->C.
2. Register actor on C.
3. Wait until A sees the actor via gossip (state snapshot, not lookup dialing).
4. Force disconnect B<->C and suppress automatic reconnect long enough to test.
5. Register a new actor on C.
6. Assert A does NOT learn about the new actor while partition is active.
7. Heal by reconnecting B<->C.
8. Assert A learns about the new actor after heal.

**Important rule:**
- No `lookup()` calls on A during steps 4-6.

**Acceptance:**
- Test fails if the actor appears on the wrong side of the cut before heal.
- Test is resilient (no fixed sleeps beyond minimal settle; uses `wait_for_condition`).

---

### GOSSIP-T5: FullSync/Delta interleavings (stale/duplicate/out-of-order)

**New tests:**
1. **Stale full sync ignored**:
   - Ensure older `sequence` full syncs do not roll back state.
2. **Delta after full sync / full sync after delta**:
   - Deliver permutations and assert final state identical.
3. **Peer omission safety**:
   - `merge_full_sync` currently removes actors omitted by a peer’s full sync.
   - Add a test proving we do not erroneously remove an actor that has moved to a different peer when the old peer sends a stale/incomplete full sync.

**Acceptance:**
- At least one test per interleaving class asserts identical final snapshots across permutations.

---

### GOSSIP-T6: Bounds/eviction/overflow correctness proofs

**New tests:**
1. **Delta history eviction triggers safe fallback**:
   - Create a situation where a receiver would need a delta older than the sender’s retained history.
   - Assert the sender returns/forces a full sync and convergence occurs.
2. **Pending buffer overflow does not silently lose correctness**:
   - Overload pending changes and assert fallback behavior remains correct.

**Acceptance:**
- Tests must prove convergence, not only that an error enum exists.

---

### GOSSIP-T7: Deterministic message content / stable encoding assertions

**Goal:** ensure protocol-visible ordering is stable so that two nodes given the same logical state produce the same message bytes.

**Tests:**
- Construct equivalent registry states via different insertion orders.
- Generate gossip messages and assert:
  - semantic equality (decoded payload equivalent)
  - and, where intended, byte-for-byte equality (if message formation is intended to be stable).

**Acceptance:**
- At least one test that fails if unordered iteration leaks into protocol-visible output.

---

## 5. Implementation Notes (Keeping Tests Deterministic and Safe)

- Prefer message-layer scheduling tests over socket-level chaos when testing reorder/dup/drop.
- Avoid `sleep()`-based timing where possible; use `wait_for_condition(timeout, ...)`.
- Keep runtime bounded:
  - limit permutations per test (e.g. a small fixed set of orderings + a small set of seeds).
  - prefer unit-style scheduler tests for exhaustive permutations.
- If we add test-only snapshot helpers, they must be behind `cfg(test)` or `feature = "test-helpers"` to avoid production surface area changes.

---

## 6. Evidence

## 7. Status (Implemented)

Implemented in-tree on 2026-02-10.

**Key behavior hardening (to make determinism provable and safe):**
- Deterministic conflict resolution now handles vector-clock `Equal` as a tie-break case (prevents “first writer wins” divergence when clocks are initially zero).
- Protocol-visible ordering is stabilized (sorted actor lists for full sync messages; stable dedup output ordering).
- Full-sync omission safety: omitted actors are only removed if they are still attributed to the sending peer (prevents stale peers from deleting actors that moved).

**New/updated tests (gossip-specific):**
- `tests/gossip_chaos_scheduler.rs`: seeded reorder/dup delivery of deltas and full-sync interleavings; asserts convergence across seeds.
- `tests/gossip_partition_invariant_e2e.rs`: asserts partition blocks propagation until heal without `lookup()`-triggered dials.
- `src/registry.rs` internal tests: order-independence, idempotency, stale full sync rejection, omission safety, fallback to full sync when delta history is insufficient, and stable encoding/ordering assertions.

**Unit test wiring (so `tests/unit/*` actually runs):**
- `tests/unit_tests.rs` + `tests/unit/mod.rs` now compile a small, modern unit test set aligned to the current rkyv wire format.
- `tests/unit/gossip_wire.rs`: rkyv roundtrip tests for `RegistryDelta` and `RegistryMessage::DeltaGossip`.

**Verification:**
- `cargo test --no-run` clean
- `cargo test` clean

When we land these improvements, attach:
- A short “new test matrix” in `reports/` listing each scenario and its test file/function.
- `cargo test` output summary (local + Linux docker, if required by QA plan).
- If any hot-path code changes were needed, include a before/after perf note (bench/harness) per `spec/QA.md`.
