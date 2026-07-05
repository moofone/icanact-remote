# PEER_ID_REFACTOR — identity over network

Status: PROPOSED (plan for review — no code lands until this is confirmed)
Scope: `icanact-remote` (gossip/mesh transport). Consumed by `icanact-core`
(`RemoteNode`/`RemoteRouter`) and `icemining` `shared-sync`.

## 1. The model (the invariant this refactor enforces)

**A peer IS its cryptographic key. Not its address.**

1. **Dial config = key + address.** For every peer a node must connect to, the
   node is configured with that peer's **public key** and a routable **ip:port**
   to dial (`--membership-peer=id=key@addr`). The address here is *only a dial
   target* — where to place the outbound TCP `connect()`.

2. **We connect outward.** Each node dials its configured peers. TLS + hello
   verify that the far end presents the **expected key**. On success the
   connection is bound to that peer's `peer_id` (`embedded_peer_id`). If the key
   does not match → reject. The address is never re-checked after this.

3. **Inbound is accepted by KEY, address-agnostic.** When a connection arrives
   *inbound*, we verify its key via TLS + hello. We accept it because the key
   matches a peer we know — we do **not** require, validate, or care about its
   source ip:port. *"Who cares where we come from."* If we already hold an
   outbound connection to that same key, the inbound is redundant: we keep one
   connection per `peer_id` (dedup by identity), and it does not matter which
   survives because **both route to the same peer_id**.

4. **Routing is by `peer_id`.** A remote actor is reached via
   `get_connection_to_peer(peer_id)` over the already-established mesh. The
   actor location's `.address` field is **not** consulted for remote routing
   (already true today — `handle.rs` `lookup()` only parses `.address` for the
   *local* branch).

5. **A verified connection is NEVER dropped over an address.** Not on an
   unexpected source IP, not on a self-advertised `0.0.0.0`, not on any address
   mismatch. Drop decisions are identity decisions (dead connection, superseded
   by a newer connection to the same `peer_id`), never address decisions.

6. **Address learning is authenticated-source only.** A peer's reachable
   address may be learned or updated **only** from a post-handshake,
   identity-verified connection (the TLS-verified source of the peer itself) —
   never from unauthenticated wire data, and never from a third party speaking
   *about* that peer. This is what makes source-IP learning safe against
   off-path spoofing (the WireGuard endpoint-roaming rule).

7. **Dial precedence is fixed and documented.** To dial a peer, try in order:
   (a) the **configured** address (`--membership-peer=id=key@addr`);
   (b) the **last learned** authenticated source address (invariant 6);
   (c) the **advertised** address, if any (NAT/transitive-discovery only).
   One list, no per-call-site variation.

8. **`advertise_address` is a NAT-only escape hatch — and redundant for the
   configured mesh.** Because connectivity comes from *outbound dials to
   configured addresses* and routing is by `peer_id`, a node never needs to
   *advertise its own address* for peers that are configured to reach it. The
   advertised address is only meaningful for **transitive discovery** (learning
   a peer you were not configured with) or **NAT** (your reachable address
   differs from your bind). In the membership deployment (devnet: direct
   routable IPs, every peer configured) it is **not needed and must not be set.**

## 2. Why this is the fix for the reconnect/broadcast storm

Root cause, in this frame: `registry.rs::validate_remote_actor_addr` **dropped**
a remote actor location whose self-advertised address was non-routable
(`0.0.0.0`, remote-loopback, port-0). But routing never needed that address — it
routes by `peer_id`. Dropping a `peer_id`-reachable actor over its irrelevant
address made the interest actor **unknown** in the registry → drove re-gossip /
re-sync → interacted with connection-pool churn → storm.

The address was decoration. Gating liveness on decoration is the bug.

## 3. Current state (what is already right vs. what couples to address)

Already identity-based (keep):
- `LockFreeConnection.embedded_peer_id` — verified identity per connection.
- `connections_by_peer`, `get_connection_by_peer_id`, `get_connection_to_peer`,
  `connection_identity_matches_peer`.
- `handle.rs::lookup()` — remote actor send routes by `location.peer_id`.

Residual address coupling (the refactor targets):
- **C1** `validate_remote_actor_addr` (registry.rs:242) — DROPS actor locations
  on non-routable/port-0 address. *Root storm cause.*
- **C2** `pubsub.rs::note_interest` — publishes interest keyed on an
  `advertised_addr()` (a network address) that the receiver then validates/drops.
- **C3** `advertised_addr()` = `advertise_address.unwrap_or(bind_addr)` — leaks
  the raw bind (`0.0.0.0`) into published locations when no override is set.
- **C4** `connections_by_addr` / `addr_to_peer_id` — address indexes used as a
  routing fallback (`aliased_connection_by_peer_id`); an address alias must never
  be able to mis-route to or evict a connection whose `embedded_peer_id` differs.

## 4. Work packages (each red-first TDD; see §5)

- **WP1 — never drop over address (fixes C1).** Replace `validate_remote_actor_addr`
  (returns `Option`, drops) with `resolve_remote_actor_addr(actor_addr,
  sender_addr, owner_is_sender: bool) -> SocketAddr` that **never drops** — the
  non-`Option` return type encodes invariant §1.5 at the boundary. If the
  advertised IP is not usable-from-the-receiver (unspecified / remote-loopback
  / link-local / multicast) **and the gossip sender is the actor's owning peer**
  (`location.peer_id == sender's verified peer_id`), substitute the TLS-verified
  source IP. Gossip is transitive: when a third party relays a location it does
  not own, its source IP says nothing about the owner — substituting it would
  falsify the address (invariant §1.6), so the advertised address is kept as-is.
  The advertised **port is always preserved, including 0**: the sender's source
  port is an ephemeral connect port, not the peer's listen port, so there is
  nothing valid to substitute. The resolved address is *decoration for re-gossip
  hygiene only*. Update both call sites (immediate-delta ~3406, full-sync
  ~4802/4830) to pass the ownership flag and drop the `else { continue }`. The
  actor is always stored, always `peer_id`-routable.

  Two hard sub-rules (from review):
  - **Malformed wire addresses are bounded, never stored raw.** A
    `location.address` that does not parse as a socket address is hostile
    wire data; it canonicalizes to the unspecified placeholder (`0.0.0.0:0`)
    and then resolves like any other unusable address. The stored/re-gossiped
    field is always a typed `SocketAddr`, never attacker-chosen bytes — §1.5
    (never drop) and input-bounding are BOTH honored.
  - **Dial-hint learning is owner-gated, not just usability-gated.**
    `set_discovered_peer_addr` / `addr_to_peer_id` / addr→NodeId pinning
    only ever run for locations the SENDER OWNS (§1.6). A relay's claim
    about a third party's reachability — however syntactically usable —
    must never plant or overwrite that peer's dial route (dial-route
    poisoning). Relayed locations are stored for identity routing only.

- **WP2 — interest is identity, not address (fixes C2/C3).** `note_interest`
  publishes a location whose routing key is `peer_id`; the address is best-effort
  (resolved, never a gate). Never publish a bare `0.0.0.0`. No dependence on
  `advertise_address` being set.

- **WP3 — connection lifecycle is identity-only (hardens C4).** Assert by
  construction: (a) a verified connection is never dropped/evicted because of an
  address; (b) address indexes (`addr_to_peer_id`) may only *hint* a lookup and
  must be re-checked against `embedded_peer_id` before use (already partly true
  in `aliased_connection_by_peer_id` — extend/test the invariant); (c) inbound
  accept binds by key and dedups against any existing connection to the same
  `peer_id` with a **symmetric** identity tie-break: both ends agree that *the
  connection dialed by the lower `peer_id` survives* — the lower peer keeps its
  outbound, the higher peer keeps the matching inbound. (Stated per-side —
  "each keeps its own outbound" — both connections die; the rule must name one
  surviving connection, BGP RFC 4271 §6.8 style.) The losing connection is
  closed only **after** the survivor is verified healthy (grace window), so a
  simultaneous-connect race can never leave zero connections. Never an address
  tie-break.

- **WP4 — demote `advertise_address` to NAT-only.** Document it as NAT/transitive
  discovery only; ensure the configured mesh works with it `None` (devnet path).
  No new field is *required*; the mesh is fully functional at zero address config.

## 5. Test strategy (strict TDD, red-first, exhaustive combinations)

**TDD is mandatory and gating.** Every work package follows red → green:

1. Author the tests for the WP **first**, run them, and **observe them fail**
   against the current code — proving they exercise the real gap. A test that
   passes before the fix is rejected and rewritten.
2. Record the red evidence (failing test names + assertion lines) in the PR.
3. Implement the fix; the same tests go green with **zero** regressions in the
   surrounding suite (`cargo test` on the full crate).
4. No fix commit may precede its red test in history.

**Coverage is exhaustive, not sampled.** The matrices below enumerate *every*
combination cell; each cell is an individual assertion with its own expected
value. No "representative subset".

- **T1 — exhaustive address-class matrix (unit, `resolve_remote_actor_addr`).**
  Table-driven over `advertised_ip_class × source_ip_class × port × ownership`:
  - advertised IP: unspecified (`0.0.0.0`, `::`), loopback (`127.0.0.1`, `::1`),
    link-local (`169.254.1.1`, `fe80::1`), private (`10.x`, `192.168.x`,
    `172.16.x`, `fc00::/7` unique-local), global (`1.2.3.4`, `2606::1`),
    multicast (`224.0.0.1`, `ff02::1`).
  - source IP: loopback, private, global (v4 and v6).
  - port: valid, `0`.
  - ownership: `owner == sender` (substitution allowed) and `owner != sender`
    (third-party relay — advertised address kept as-is, never falsified).
  - Expected: **never a drop** (return type is `SocketAddr`, not `Option`);
    usable-from-source advertised IP kept; unusable + owner-sent → source IP
    substituted; unusable + relayed → kept as-is; port preserved in every cell
    (including 0 — source port is ephemeral, nothing valid to substitute).
    Every cell asserted, including mixed v4/v6 advertised-vs-source cells.

- **T2 — identity routing survives a garbage address (unit/integration).** An
  actor stored with an unroutable `.address` is still reachable via its owning
  peer's connection (`get_connection_to_peer`), and the connection is **never
  dropped**. This is the headline invariant.

- **T3 — inbound-from-unexpected-source (integration).** Node holds an outbound
  connection to peer B. An inbound connection arrives claiming B's key from a
  *different* source address. Assert: accepted by key, deduped to one connection
  per `peer_id`, no eviction of a healthy connection, no reconnect.

- **T3b — simultaneous-connect tie-break (integration).** A and B dial each
  other concurrently (both directions complete handshakes). Assert: both sides
  converge on the **same** surviving connection (the one dialed by the lower
  `peer_id`); the loser closes only after the survivor is healthy; at no instant
  are both connections closed; messages in flight during the dedup are not
  lost. Run both orderings (A-first, B-first) and the true-simultaneous case.

- **T4 — wildcard-bind interest storm settles (integration, already present).**
  `wildcard_advertise_interest_storm.rs`: wildcard bind + interest, restart
  churn, then a quiet window asserting bounded outbound/evictions and a location
  that stays routable. (Verified red without WP1/WP2, green with.)

- **T5 — connection lifecycle never drops over address (property/loom-style).**
  Address alias points at the wrong `peer_id` → lookup must miss/skip, never
  mis-route; a verified connection is never evicted by an address change alone.

- **T6 — identity-lifecycle edge scenarios (integration).** One test per row;
  every row asserted:
  | scenario | expected |
  |---|---|
  | same key reconnects from a **new** address (roaming/NAT rebind) | accepted; learned address updated (invariant §1.6); old dead conn reaped; actors stay routable |
  | **different** key connects from a **known** address (peer reimaged/rekeyed) | rejected — key mismatch beats address familiarity; no index poisoning of the old peer's entries |
  | peer restarts with same key, new ephemeral source port | accepted, deduped; no storm |
  | two distinct peers behind one NAT (same source IP, different keys) | both accepted; indexes never conflate them (address is not a key) |
  | half-open: our outbound is dead but peer's inbound is live | inbound accepted by key; dead outbound reaped; no zero-connection instant |
  | v6 wildcard `[::]` bind + v4-mapped source | resolved like `0.0.0.0`; no drop |
  | advertised address is another live peer's address (misconfig/malice) | routing untouched (by `peer_id`); no eviction or redial of the other peer |
  | configured dial address unreachable but peer dialed **us** | fully functional via inbound; dial precedence (§1.7) retries in background without disturbing the live connection |

- **Performance gates (not just failover time).** Under the storm-repro
  (WP4 wildcard bind, N restart cycles): steady-state **outbound dials ≤ 3** and
  **evictions ≤ 1** across a 1200 ms quiet window (existing T4 thresholds);
  extend with a per-second churn ceiling asserted over a sustained window so a
  regression that reintroduces address-gated churn fails the gate.

- **Runtime observability (ships with the fix, not just CI).** Counters exposed
  via the existing metrics surface: `addr_substitutions_total` (owner-sent
  rewrites), `relayed_addr_kept_total`, `identity_dedups_total` (tie-break
  events), `connection_evictions_total` by reason (dead / superseded — an
  `address` reason must never appear). The storm signature is visible in prod
  telemetry, not only in tests.

## 6. Rollout

1. Land WP1–WP4 in `icanact-remote` (this worktree, PR #83 lineage), review
   bots green (codex + glm, wait-for-BOTH).
2. Roll `icanact-core` pin → `shared-sync`/`icemining` pin.
3. Redeploy devnet coins (.37/.38) + witness with **zero** `advertise_address`.
4. Verify residual baseline churn (~2.5/s today) → ~0; storm does not reappear
   under partition chaos.
5. Re-run Gate E with the performance gates above.

## 7. Non-goals / guardrails

- Not introducing new membership primitives, clocks, leases, or probes.
- Not removing `advertise_address` (kept as NAT/transitive-discovery escape
  hatch) — only demoting it from a mesh requirement to optional.
- Money-safety unchanged: this is transport identity, orthogonal to
  authority-grant/fencing, which remain as specified.
