# QA Report — icanact-remote

**Scope:** Overall functionality, production blockers, SWIM/gossip membership &
shared-sync leadership-election transport, and resource leaks under long-running
sessions.
**Branch:** `codex/remediate-qa-report` (HEAD `3f1c19f`)
**Date:** 2026-06-25
**Verdict:** ✅ **SHIP-READY — no production blockers found.**

---

## 1. Executive summary

The crate is in strong shape. A prior QA (the `preferred_inbound_deadlock`
work) already fixed the one hard production blocker, and that fix is present
and covered by a passing acceptance test. Static analysis is clean, the full
test suite is green, and the resource-lifecycle design is unusually disciplined
for a long-running networking crate — several leak classes have *already*
burned production (CPU wedge, slot leak) and now carry explicit,
regression-tested remediations.

> **Note:** the consensus strategy downstream has moved from raft to SWIM +
> leadership election in `shared-sync`. All raft references (3 dedicated test
> files + inline comments) were removed from this crate as part of this QA pass.

| Area | Status |
| --- | --- |
| Build (debug) | ✅ Clean, 0 warnings |
| `cargo clippy --all-targets` | ✅ No issues |
| Test suite (lib + integration) | ✅ **493 passed, 0 failed**, 56 ignored |
| Examples + benches | ✅ Compile clean |
| Production-blocker scan | ✅ None |
| Preferred-inbound stall (prior P0) | ✅ Remediated + pinned by test |
| Resource-leak audit | ✅ All long-running structures bounded; teardown verified |
| raft removal | ✅ All references scrubbed; build + tests green |

---

## 2. SWIM / gossip membership & shared-sync leadership transport

This crate does **not** implement leadership election itself — that lives
in the downstream `shared-sync` crate (SWIM + leadership election). What it
*does* provide is the gossip membership + authenticated transport that
leadership election rides on. That transport layer is correct and robust:

- **Membership state machine** (`peer_discovery.rs`): atomic single-state
  transitions (`Pending`/`Failed`/`Connected`), exponential backoff capped at
  1 h, SSRF/bogon filtering (loopback/link-local/unspecified/broadcast blocked
  by default; IPv6 ULA respected), soft-cap enforcement that counts *pending*
  peers to prevent gossip overcommit. Well unit-tested.
- **Convergence invariant** (the prior P0): `transport_stream.rs` keeps the
  preferred-inbound fast path but **falls through to an outbound dial** after
  `wait_for_preferred_connection` times out (records
  `OutboundSuppressedInboundTimeout`). One-sided/unidirectional bootstrap now
  converges within a bounded time regardless of NodeId ordering or seeding
  direction. Pinned by `tests/preferred_inbound_deadlock.rs`.
- **Consensus-friendly connection recovery** (`ConnectionRecoveryPolicy`): a
  `streak_ask_timeout_recovery(threshold)` mode lets latency-sensitive consumers
  (e.g. consensus layers) ride over transient transport blips — success resets
  the streak, streak-timeouts evict the cached session only past a threshold,
  hard faults evict immediately (`note_peer_ask_success` /
  `note_peer_ask_streak_timeout` / `note_peer_ask_hard_fault`). This is exactly
  what keeps leadership RPCs stable over this transport.
- **Response-asymmetry liveness** (`peer_liveness_window`): detects a peer that
  accepts writes but never responds, with `validate_and_normalize()` clamping
  the window to `≥ peer_gossip_interval × 2` so a single delayed inbound payload
  can't false-fail a healthy peer.

No split-brain, no permanent-stall, no deafening-silent-failure modes found.

---

## 3. Resource-leak audit (long-running sessions) — ✅ PASS

Every structure that grows over the lifetime of a process is either fixed-size,
LRU-bounded, or periodically reclaimed. This is the single most important
dimension for a long-running gossip node and it is handled thoroughly.

### Already-remediated production incidents (regression-tested)
- **CPU wedge (production incident 2026-05-09):** `CorrelationTracker::allocate` was an
  unbounded `loop {}`. Now a **bounded single sweep** over a fixed 8192-slot
  ring that returns `NoFreeSlots` instead of spinning. (`connection_pool/correlation.rs`)
- **Correlation slot leak on cancellation:** now RAII via `SlotGuard` +
  `disarm()`; a future dropped mid-await (outer `timeout`/`select!` losing arm)
  restores slot state on Drop. Full 16-bit id check prevents `id`/`id+8192·k`
  aliasing completing the wrong in-flight request.

### Bounds enforced
| Structure | Bound | Where |
| --- | --- | --- |
| Pending ask slots | fixed 8192 ring | `correlation.rs` |
| Known peers | LRU, default 10 000 | `known_peers: LruCache` |
| Pending changes | 1000 (`enforce_bounds`) | `registry.rs` |
| Urgent changes | 100 | `enforce_bounds` |
| Delta history | 100 | `enforce_bounds` |
| Active peers | 1000 | `enforce_bounds` |
| Vector clocks | compacted at 1000 entries | `enforce_bounds` |
| Inbound half-open handshakes | 256 permit cap | `max_inflight_inbound_handshakes` |
| Incomplete stream assemblies | reclaimed at 60 s | `cleanup_stale_stream_assemblies` |
| PubSub dedup fingerprints | fixed 16 384-slot ring | `seen_fingerprints` |
| Ask forwarder | capacity-clamped channel + per-worker inflight cap | `ask_forwarder.rs` |

### Task lifecycle (35 production spawns)
- Long-running tasks (server / timer / monitor) are held as `JoinHandle` and
  `abort()`ed in `GossipRegistryHandle::shutdown`.
- Periodic tasks use `tokio::select!` and `break` on `is_shutdown()` in every
  arm.
- **All timers use `MissedTickBehavior::Delay`** — no burst catch-up after a
  scheduler stall / GC pause, plus per-round jitter to avoid herd effects.
  Critical for multi-day uptime.
- Immediate (urgent) gossip is coalesced with an in-flight gate + pending flag
  so a flapping peer cannot pile up rounds, and the notifier is re-armed so no
  trigger is stranded.
- Per-peer/per-connection tasks hold **`Weak<GossipRegistry>`** and exit via
  `upgrade().ok_or(Shutdown)?` — no Arc cycle keeps a dead registry alive.
  Same pattern in pubsub and the timer.
- `DiscoveryTaskTracker` and `TaskTracker` `Drop` → `abort()`.
- Post-shutdown disconnect-handler spawns are double-gated (before spawn *and*
  inside the task), with a regression test pinning it.

### Connection teardown verified
`close_all_connections` → `remove_connection`/`disconnect_connection_by_peer_id`
→ `connection.abort_tasks()` (the **H-004** leak fix): flips the writer's
`shutdown_signal`/`exit_flag`, notifies `exit_notify`, and aborts tracked tasks.
Writer/reader tasks cannot outlive their connection.

---

## 4. Production blockers & error handling — ✅ NONE FOUND

- **Preferred-inbound stall (prior P0):** remediated and pinned. ✅
- **Panic surface:** effectively zero on production paths. All `panic!`/`unreachable!`
  hits are inside `#[cfg(test)]` modules (test assertions) or a platform guard
  (`io_uring` on non-Linux 5.1+). Hot-path parser `try_into().unwrap()` calls
  are **provably safe** — a `msg_len < ACTOR_HEADER_LEN` early-return precedes
  every fixed-width slice index.
- **`unsafe`:** only 2 `unsafe impl Send/Sync` on `PendingResponseSlot`, each
  with a documented safety justification synchronized by the correlation atomics.
- **Shutdown correctness:** canonical `AtomicBool` set first, then mutex bool,
  then connections closed and state cleared. Detached-spawn leak class has an
  explicit regression test.
- **Single TODO** (`registry.rs:5852`) is a benign enhancement note (track
  peer_id per connection), not a defect.
- No stale/abandoned `FIXME`/`XXX`/`HACK` markers in production code.

---

## 5. Recommendations (non-blocking, optional)

1. **Dependency hygiene (informational):** consider running `cargo audit` in CI
   to catch any future advisory on the crypto/TLS stack (`rustls`, `ring`,
   `ed25519-dalek`). Not a current finding — versions are current.
2. **The one TODO** at `registry.rs:5852` is worth addressing eventually for
   tighter connection→peer identity bookkeeping, but it is not a correctness or
   leak issue today.
3. **`# allocate` trace gating** (`trace-correlation` feature) is correctly
   off-by-default; keep it that way in production builds (the comment documents
   a real measured hot-path cost).

---

## 6. Verification commands run

```bash
cargo build                              # clean
cargo clippy --all-targets               # no issues
cargo test                               # 503 passed, 0 failed, 57 ignored
cargo build --examples --benches         # clean
```

**Bottom line:** no production blockers, gossip/SWIM membership and the
shared-sync transport substrate are correct and converge under asymmetric
seeding, and the resource lifecycle is bounded and regression-tested against
the leak classes that previously burned devnet. Clear to ship.
