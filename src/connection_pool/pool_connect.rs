/// Outcome of a connection keep/drop/dedup conflict for a single peer identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a connection conflict decision must be acted on explicitly by the \
              caller (accept/replace/reject/evict) — silently discarding it and \
              falling back to ad hoc (often address-keyed) logic is exactly the \
              regression this chokepoint exists to prevent"]
pub(crate) enum ConnectionConflictDecision {
    /// No live rival (none exists, or the existing entry is stale/dead) and the
    /// incoming candidate is identity-preferred — take incoming as the session.
    AcceptIncoming,
    /// A live rival exists but either the tie-break prefers the incoming
    /// candidate's direction, or both use the preferred direction and the
    /// incoming candidate has the newer authenticated session epoch. Evict
    /// the rival and take incoming as the session.
    ReplaceExisting,
    /// A live rival exists and either the incoming direction is not preferred,
    /// or both candidates use the preferred direction but the incumbent has
    /// the newer authenticated session epoch. Keep the rival, reject incoming.
    RejectIncoming,
    /// The existing entry is stale/dead *and* the incoming candidate is not
    /// identity-preferred either — evict the stale entry, but do not accept
    /// incoming as the session either (neither survives as "the" session).
    EvictStaleRejectIncoming,
}

/// Socket-level decision authority for connection keep/drop/dedup/replace
/// outcomes within one authenticated process incarnation. Production call
/// sites route through [`resolve_authenticated_connection_conflict`], which
/// first rejects a different live boot and delegates same-boot sockets here.
///
/// Direction remains *purely* identity-derived: `keep_existing` /
/// `keep_incoming` are the results of
/// [`GossipRegistry::should_keep_connection`], a pure function of verified
/// peer NodeId ordering plus connection direction. Within that single valid
/// direction, `incoming_session_is_newer` orders two fully authenticated
/// physical sessions from the same process by their local, monotonic
/// stream-instance epoch. That makes ordinary simultaneous-open races
/// deterministic without treating another live process as a newer socket.
///
/// There is deliberately **no `SocketAddr` parameter** — this is enforced
/// structurally (at the type/
/// signature level, not by a runtime check): a keep/drop/dedup outcome can
/// never be a function of where a peer happens to be dialing from, only of
/// its cryptographic identity, direction, and authenticated session epoch. A
/// changed socket address is handled as
/// reindex-only metadata (see [`ConnectionPool::reindex_connection_addr`]) and
/// never reaches this function. This structural absence — no caller can even
/// attempt to pass an address in — is the invariant that prevents the
/// address-keyed teardown that caused the single-node-restart reconnect
/// thrash. The `#[must_use]` on [`ConnectionConflictDecision`] is a second,
/// compiler-enforced guard: a call site cannot silently ignore the decision
/// and fall back to inline address-conditioned logic.
///
/// `resolve_connection_conflict_matches_all_routed_call_sites`
/// (`src/connection_pool/tests/mod.rs`) pins the exact (existing_usable,
/// keep_existing, keep_incoming) → decision contract each routed call site
/// below relies on, so a future change to this function's logic that breaks
/// any one site's assumption fails loudly there, not silently at runtime.
///
/// ## Routed call sites
///
/// - **Outbound finalize** (`ConnectionPool::finalize_new_outbound_connection`,
///   `pool_connect.rs` — the publish-gate call, see `resolve_connection_conflict(...)`
///   a few hundred lines below): a freshly-dialed outbound socket already
///   exists; `keep_incoming = should_keep_connection(peer, true)`.
/// - **Inbound accept** (`handle_incoming_connection_tls`, `src/handle.rs`,
///   the `keep_connection` block once `pool.get_connection_by_peer_id` returns
///   `Some`): a freshly-accepted inbound socket already exists;
///   `keep_incoming = should_keep_connection(peer, false)`. The "no existing
///   connection at all" fast path is intentionally **not** routed through
///   this function (see exception below).
/// - **Outbound top-of-dial, stale-rival branch** (`ConnectionPool::connect_via_stream`,
///   `src/connection_pool/transport_stream.rs`, the `!alive` arm): the
///   existing entry is dead; `keep_incoming = should_keep_connection(peer, true)`
///   (the same value the caller reuses immediately afterward for its
///   preferred-inbound-wait decision).
///
/// ## Explicitly-justified exceptions (not routed)
///
/// - **Outbound top-of-dial, alive-rival branch** (same function, the `alive`
///   arm): this decides "is the connection I already hold still the
///   tie-break-correct one to keep" — a genuinely **one-input** question
///   (`keep_existing` alone), asked *before* any concrete incoming candidate
///   exists (the actual TCP/TLS dial for a replacement has not even started
///   yet at this point in the code; whether to dial at all, or instead wait
///   for a preferred inbound, is a *separate* decision made further down via
///   `keep_outbound_dial`). Feeding that same `keep_outbound_dial` value into
///   this function as a synthetic `keep_incoming` would be dishonest: it can
///   legitimately be `false` (this side is the higher-NodeId side, which
///   never wants to keep an outbound) while the live existing outbound must
///   *still* be evicted as wrong-direction regardless of that fact — this
///   function's `!existing_usable`-adjacent formula (`!keep_existing &&
///   keep_incoming ⇒ Replace`) would then wrongly return `RejectIncoming`
///   (reuse) instead of `ReplaceExisting` (evict). Synthesizing a `true` to
///   force the correct branch would misrepresent `should_keep_connection`'s
///   real output at that call site, defeating the "purely identity-derived
///   inputs" contract this function exists to guarantee. The correct rule
///   there really is the strict one-input reduction `if keep_existing { keep
///   } else { evict }`, which this function cannot express without either a
///   third parameter carrying the existing connection's own direction
///   (over-generalizing a chokepoint whose entire value is a small, auditable
///   surface) or a dishonest second input. It remains a direct
///   `should_keep_connection` call, cross-referenced in a code comment at the
///   call site back to this doc section.
pub(crate) fn resolve_connection_conflict(
    existing_usable: bool,
    keep_existing: bool,
    keep_incoming: bool,
    incoming_session_is_newer: bool,
) -> ConnectionConflictDecision {
    if !existing_usable {
        return if keep_incoming {
            ConnectionConflictDecision::AcceptIncoming
        } else {
            ConnectionConflictDecision::EvictStaleRejectIncoming
        };
    }
    if keep_incoming && (!keep_existing || incoming_session_is_newer) {
        return ConnectionConflictDecision::ReplaceExisting;
    }
    // Covers "the incoming direction is not preferred", "the incumbent has
    // the newer same-direction session epoch", and the equal/degenerate
    // inputs. In all three cases the live rival survives.
    ConnectionConflictDecision::RejectIncoming
}

/// Boot-aware admission wrapper around the socket-level tie-break.
///
/// Stream epochs are only comparable inside one running process. A later
/// stream from another process that reused the same long-lived PeerId is a
/// configuration conflict, not evidence of a restart, while the incumbent
/// stream remains live.
pub(crate) fn resolve_authenticated_connection_conflict(
    existing: &Arc<LockFreeConnection>,
    incoming: &Arc<LockFreeConnection>,
    keep_existing: bool,
    keep_incoming: bool,
    incoming_session_is_newer: bool,
) -> ConnectionConflictDecision {
    if existing.has_live_stream()
        && matches!(
            (existing.remote_boot_id, incoming.remote_boot_id),
            (Some(existing_boot), Some(incoming_boot)) if existing_boot != incoming_boot
        )
    {
        return ConnectionConflictDecision::RejectIncoming;
    }
    resolve_connection_conflict(
        existing.has_live_stream(),
        keep_existing,
        keep_incoming,
        incoming_session_is_newer,
    )
}

pub(crate) fn has_conflicting_remote_boot(
    existing: &Arc<LockFreeConnection>,
    incoming: &Arc<LockFreeConnection>,
) -> bool {
    existing.has_live_stream()
        && matches!(
            (existing.remote_boot_id, incoming.remote_boot_id),
            (Some(existing_boot), Some(incoming_boot)) if existing_boot != incoming_boot
        )
}

/// Compare the local authenticated-session epochs of two physical
/// connections. `LockFreeStreamHandle::instance_id` is process-global,
/// monotonic, and allocated once for each completed transport stream.
///
/// A missing stream handle is never evidence that a candidate is newer. Real
/// TLS candidates and live incumbents always have handles; the conservative
/// fallback keeps synthetic/incomplete connections from displacing a live
/// session.
pub(crate) fn incoming_session_is_newer(
    incoming: &Arc<LockFreeConnection>,
    existing: &Arc<LockFreeConnection>,
) -> bool {
    match (
        incoming
            .stream_handle
            .as_ref()
            .map(|handle| handle.instance_id()),
        existing
            .stream_handle
            .as_ref()
            .map(|handle| handle.instance_id()),
    ) {
        (Some(incoming_epoch), Some(existing_epoch)) => incoming_epoch > existing_epoch,
        _ => false,
    }
}

/// RAII guard for the window between a fresh outbound candidate being
/// published/counted and its identify gate (`LockFreeStreamHandle::mark_identified`)
/// being resolved one way or the other.
///
/// `finalize_new_outbound_connection` is itself awaited inside a
/// `tokio::time::timeout` at its only call site, so it can be cancelled out
/// from under it at any await point -- including while parked building or
/// sending the identify. Without this guard, a cancellation (or any early
/// return added later between publish/count and a successful identify)
/// would leave the candidate published and counted but never identified:
/// any `write_routed_actor_ask` caller parked in `wait_until_identified`
/// would then hang forever, since nothing would ever call `mark_identified`
/// or tear the candidate down.
///
/// [`Self::disarm`] on a clean, successful identify is the only way to
/// suppress the `Drop` cleanup; every other path -- an explicit early
/// return or the whole future being dropped by an external cancellation --
/// runs it, retiring the candidate exactly like a failed identify send
/// would.
struct IdentifyGateGuard<'a, T> {
    pool: &'a ConnectionPool<T>,
    addr: SocketAddr,
    connection: Arc<LockFreeConnection>,
    peer_id: Option<crate::PeerId>,
    armed: bool,
}

impl<'a, T> IdentifyGateGuard<'a, T> {
    fn new(
        pool: &'a ConnectionPool<T>,
        addr: SocketAddr,
        connection: Arc<LockFreeConnection>,
        peer_id: Option<crate::PeerId>,
    ) -> Self {
        Self {
            pool,
            addr,
            connection,
            peer_id,
            armed: true,
        }
    }

    /// The candidate identified successfully and is live -- nothing to
    /// unwind.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl<'a, T> Drop for IdentifyGateGuard<'a, T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(peer_id) = self.peer_id.as_ref() {
            // Re-derives whether this candidate is still the peer's current
            // connection via an atomic compare-and-clear, right now -- not
            // from any earlier decision-time snapshot, which a sibling
            // published while this candidate was still mid-flight would
            // have made stale. If a sibling HAS already superseded this
            // candidate, that sibling's own publish already retired this
            // candidate in full (aliases, counted instance, and a
            // correlation-tracker-aware abort via
            // `retire_displaced_expected`), so this call correctly declines
            // rather than duplicating that teardown or cancelling the
            // sibling's shared correlation tracker.
            self.pool
                .disconnect_connection_instance(peer_id, &self.connection);
        } else {
            // No `addr_to_peer_id`/peer-session entry was ever created for
            // an unidentified candidate; only the address index and its own
            // correlation tracker (never shared, since none was known) need
            // cleanup.
            let _ = self
                .pool
                .connections_by_addr
                .remove_if_sync(&self.addr, |v| Arc::ptr_eq(v, &self.connection));
            self.pool.release_counted_connection(&self.connection);
            self.connection.abort_tasks();
        }
    }
}

impl<T> ConnectionPool<T> {
    pub fn new(max_connections: usize, connection_timeout: Duration) -> Self {
        Self::new_with_aligned_pool_size(
            max_connections,
            connection_timeout,
            crate::aligned::DEFAULT_ALIGNED_POOL_SIZE,
        )
    }

    pub fn new_with_aligned_pool_size(
        max_connections: usize,
        connection_timeout: Duration,
        aligned_pool_size: usize,
    ) -> Self {
        let pool = Self {
            connections_by_peer: SccHashMap::default(),
            addr_to_peer_id: SccHashMap::default(),
            peer_id_to_addr: SccHashMap::default(),
            connections_by_addr: SccHashMap::default(),
            peer_sessions: SccHashMap::default(),
            counted_instances: SccHashMap::default(),
            outbound_dial_gates: SccHashMap::default(),
            max_connections,
            connection_timeout,
            registry: ArcSwapWeak::new(std::sync::Weak::new()),
            aligned_bytes_pool: Arc::new(crate::AlignedBytesPool::new(
                aligned_pool_size.max(crate::aligned::DEFAULT_ALIGNED_POOL_SIZE),
            )),
            connection_counter: AtomicIsize::new(0),
            routing_revision: AtomicU64::new(0),
            routing_change_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            preferred_connection_checks: AtomicU64::new(0),
            _marker: PhantomData,
        };

        // Log the pool's address for debugging
        debug!(
            "CONNECTION POOL: Created new pool at {:p}",
            &pool as *const _
        );
        pool
    }

    #[inline]
    pub(crate) fn routing_revision(&self) -> u64 {
        self.routing_revision.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_for_routing_change(&self, after: u64) -> u64 {
        loop {
            let notified = self.routing_change_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let current = self.routing_revision();
            if current != after {
                return current;
            }
            notified.await;
        }
    }

    #[inline]
    pub(crate) fn mark_routing_changed(&self) {
        self.routing_revision.fetch_add(1, Ordering::AcqRel);
        self.routing_change_notify.notify_waiters();
    }

    /// Set the registry reference for handling incoming messages
    pub fn set_registry(&self, registry: std::sync::Arc<GossipRegistry>) {
        self.registry.store(std::sync::Arc::downgrade(&registry));
    }

    fn handle_correlation(
        &self,
        addr: SocketAddr,
        conn: &Arc<LockFreeConnection>,
    ) -> Arc<CorrelationTracker> {
        if let Some(correlation) = conn.correlation.clone() {
            return correlation;
        }

        if let Some(peer_id) = conn.embedded_peer_id.as_ref() {
            return self.get_or_create_correlation_tracker(peer_id);
        }

        if let Some(peer_id) = self.addr_to_peer_id.read_sync(&addr, |_, v| v.clone()) {
            return self.get_or_create_correlation_tracker(&peer_id);
        }

        CorrelationTracker::new()
    }

    fn make_connection_handle(
        &self,
        addr: SocketAddr,
        conn: &Arc<LockFreeConnection>,
    ) -> Option<ConnectionHandle<T>> {
        let correlation = self.handle_correlation(addr, conn);
        if let Some(stream_handle) = conn.stream_handle.as_ref() {
            if !conn.is_connected() || stream_handle.exit_flag.load(Ordering::Acquire) {
                return None;
            }
            return Some(ConnectionHandle::new_stream(
                addr,
                conn.direction,
                stream_handle.clone(),
                correlation,
            ));
        }

        None
    }

    fn reuse_published_connection(
        &self,
        session: &Arc<PeerSession>,
    ) -> Option<ConnectionHandle<T>> {
        let connection = session.current_connection()?;
        connection.update_last_used();
        self.make_connection_handle(connection.addr, &connection)
    }

    fn reuse_published_connection_after_retry_claim(
        &self,
        session: &Arc<PeerSession>,
        attempt: OutboundDialAttempt,
    ) -> Option<ConnectionHandle<T>> {
        let handle = self.reuse_published_connection(session)?;
        session.outbound_dial_retry.record_success(attempt);
        Some(handle)
    }

    fn get_or_create_peer_session(&self, peer_id: &crate::PeerId) -> Arc<PeerSession> {
        let session = self
            .peer_sessions
            .entry_sync(peer_id.clone())
            .or_insert_with(|| {
                debug!(
                    "CONNECTION POOL: Creating new peer session for peer {}",
                    peer_id
                );
                Arc::new(PeerSession::new())
            })
            .get()
            .clone();
        session.touch();
        session
    }

    pub(crate) fn get_configured_peer_addr(&self, peer_id: &crate::PeerId) -> Option<SocketAddr> {
        self.peer_sessions
            .read_sync(peer_id, |_, session| session.configured_addr())
            .flatten()
            .or_else(|| self.peer_id_to_addr.read_sync(peer_id, |_, v| *v))
    }

    pub(crate) fn get_required_peer_addr(&self, peer_id: &crate::PeerId) -> Option<SocketAddr> {
        self.peer_sessions
            .read_sync(peer_id, |_, session| session.required_addr())
            .flatten()
    }

    fn set_session_route_addr(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        self.get_or_create_peer_session(peer_id)
            .set_configured_addr(addr);
        let _ = self.peer_id_to_addr.upsert_sync(peer_id.clone(), addr);
    }

    pub(crate) fn set_configured_peer_addr(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        let session = self.get_or_create_peer_session(peer_id);
        session.set_required_addr(addr);
        session.set_configured_addr(addr);
        session.mark_required_peer();
        let _ = self.peer_id_to_addr.upsert_sync(peer_id.clone(), addr);
    }

    pub(crate) fn set_discovered_peer_addr(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        // PEER_ID_REFACTOR §1.7 dial precedence: configured → learned →
        // advertised. A REQUIRED peer's operator-configured session route
        // must never be displaced by a learned hint (the hint may be stale
        // or NAT-only while the configured address is the routable target).
        // The hint is still recorded in the fallback index, which
        // `get_configured_peer_addr` only consults when no session route
        // is configured.
        if self.is_required_peer(peer_id) {
            let _ = self.peer_id_to_addr.upsert_sync(peer_id.clone(), addr);
            return;
        }
        self.set_session_route_addr(peer_id, addr);
    }

    /// Remove only the displaced identity's derived route for an address.
    /// Required/operator configuration is retained; verified ownership cannot
    /// be displaced by arbitration, while provisional learned state must not
    /// survive a verified takeover.
    pub(crate) fn clear_displaced_peer_addr(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        if let Some(session) = self
            .peer_sessions
            .read_sync(peer_id, |_, session| session.clone())
        {
            session.clear_route_addr_if(addr);
        }
        let _ = self
            .peer_id_to_addr
            .remove_if_sync(peer_id, |mapped| *mapped == addr);

        let current = self
            .connections_by_peer
            .read_sync(peer_id, |_, connection| connection.clone());
        let _ = self
            .connections_by_addr
            .remove_if_sync(&addr, |connection| {
                connection.embedded_peer_id.as_ref() == Some(peer_id)
                    || current
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(connection, current))
            });
    }

    /// Evict `evicted_addr`'s `connections_by_addr` alias for `peer_id`,
    /// called synchronously from `RoutingPublisher::
    /// set_configured_peer_addr` when a same-command pin decision moves
    /// `peer_id`'s pin away from `evicted_addr` -- otherwise traffic to a
    /// different identity that later claims `evicted_addr` would be
    /// delivered over this peer's still-live connection.
    ///
    /// Removes the alias unconditionally, with no exception for
    /// `evicted_addr == connection.addr` (the connection's own dial
    /// target): the correct question is not "is this a transport-source
    /// entry" but "may an address-keyed lookup still reach this connection
    /// after the address changed hands" -- and once evicted, the same
    /// atomic owner transaction already released `peer_id`'s ownership of
    /// `evicted_addr` too, so the answer is always no. `peer_id`'s
    /// connection remains reachable through the identity-aware path
    /// (`connections_by_peer`, untouched here).
    pub(crate) fn evict_pin_alias(&self, peer_id: &crate::PeerId, evicted_addr: SocketAddr) {
        let current = self
            .connections_by_peer
            .read_sync(peer_id, |_, connection| connection.clone());
        let _ = self
            .connections_by_addr
            .remove_if_sync(&evicted_addr, |connection| {
                connection.embedded_peer_id.as_ref() == Some(peer_id)
                    || current
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(connection, current))
            });
    }

    pub(crate) fn is_required_peer(&self, peer_id: &crate::PeerId) -> bool {
        self.peer_sessions
            .read_sync(peer_id, |_, session| session.is_required_peer())
            .unwrap_or(false)
    }

    /// Every peer that has a configured (desired) address — i.e. the "required
    /// peers" the p2p configured-peer supervisor must keep a direct connection
    /// to. Read-only snapshot; generates no network traffic.
    pub(crate) fn list_configured_peers(&self) -> Vec<(crate::PeerId, SocketAddr)> {
        let mut out = Vec::new();
        self.peer_sessions.iter_sync(|peer_id, session| {
            if session.is_required_peer()
                && let Some(addr) = session.required_addr()
            {
                out.push((peer_id.clone(), addr));
            }
            true
        });
        out
    }

    pub(crate) fn publish_current_peer_connection(
        &self,
        peer_id: &crate::PeerId,
        connection: Arc<LockFreeConnection>,
    ) {
        let session = self.get_or_create_peer_session(peer_id);
        let stream_instance_id = connection
            .stream_handle
            .as_ref()
            .map(|handle| handle.instance_id());
        info!(
            peer_id = %peer_id,
            addr = %connection.addr,
            direction = ?connection.direction,
            stream_instance_id = ?stream_instance_id,
            "transport_session_published"
        );
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::SessionPublished {
                peer: peer_id.clone(),
                addr: connection.addr,
                direction: match connection.direction {
                    ConnectionDirection::Inbound => crate::lifecycle::TransportDirection::Inbound,
                    ConnectionDirection::Outbound => crate::lifecycle::TransportDirection::Outbound,
                },
            },
        );
        // Publish before releasing the peer's retry reservation. Otherwise a
        // concurrent caller can observe neither a current connection nor an
        // active retry floor and start a redundant socket attempt.
        session.set_current_connection(Some(connection.clone()));
        session.outbound_dial_retry.record_published_connection();
        let _ = self
            .connections_by_peer
            .upsert_sync(peer_id.clone(), connection);
        self.mark_routing_changed();
    }

    /// Compare-and-publish counterpart to `publish_current_peer_connection`:
    /// install `connection` as the peer's current session iff the session
    /// slot still holds exactly `expected` — the snapshot a caller's
    /// tie-break decision was computed against — via
    /// `PeerSession::compare_and_set_current_connection`.
    ///
    /// On success this performs the identical side effects as
    /// `publish_current_peer_connection` (lifecycle event + logging +
    /// `connections_by_peer` mirror). On failure it performs NONE of them
    /// and returns whatever is actually installed now, so the caller can
    /// re-resolve its conflict decision against reality instead of
    /// clobbering a concurrently published rival (e.g. a preferred inbound
    /// that landed between the caller's snapshot and this call).
    pub(crate) fn compare_and_publish_peer_connection(
        &self,
        peer_id: &crate::PeerId,
        expected: Option<&Arc<LockFreeConnection>>,
        connection: Arc<LockFreeConnection>,
    ) -> std::result::Result<(), Option<Arc<LockFreeConnection>>> {
        let session = self.get_or_create_peer_session(peer_id);
        if let Err(current) =
            session.compare_and_set_current_connection(expected, connection.clone())
        {
            // ACTOR_REM_2 R10: the CAS failed because the slot already holds
            // exactly THIS connection — it was published out of band (e.g.
            // `get_connection_by_peer_id`'s address fallback adopting this
            // connection while it was provisionally indexed at its addr, before
            // this finalize decided its fate). The publish is already done, so
            // report idempotent success instead of handing our OWN candidate
            // back to the caller as a "rival", which made outbound finalize
            // re-resolve and abort its own uncontested connection. We do NOT
            // retire `expected` here — we did not displace it, whoever installed
            // `connection` did. Keep the `connections_by_peer` mirror consistent
            // (a plain idempotent upsert).
            if current
                .as_ref()
                .is_some_and(|cur| Arc::ptr_eq(cur, &connection))
            {
                session
                    .outbound_dial_retry
                    .record_published_connection();
                let _ = self
                    .connections_by_peer
                    .upsert_sync(peer_id.clone(), connection);
                self.mark_routing_changed();
                return Ok(());
            }
            return Err(current);
        }

        session
            .outbound_dial_retry
            .record_published_connection();

        let stream_instance_id = connection
            .stream_handle
            .as_ref()
            .map(|handle| handle.instance_id());
        info!(
            peer_id = %peer_id,
            addr = %connection.addr,
            direction = ?connection.direction,
            stream_instance_id = ?stream_instance_id,
            "transport_session_published"
        );
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::SessionPublished {
                peer: peer_id.clone(),
                addr: connection.addr,
                direction: match connection.direction {
                    ConnectionDirection::Inbound => crate::lifecycle::TransportDirection::Inbound,
                    ConnectionDirection::Outbound => crate::lifecycle::TransportDirection::Outbound,
                },
            },
        );
        let _ = self
            .connections_by_peer
            .upsert_sync(peer_id.clone(), connection.clone());

        // The CAS above just displaced `expected` (if `Some`) from the
        // peer's current-connection slot in favor of `connection`. The
        // `ReplaceExisting` call sites (`evict_before_replace`) already
        // fully retire a live rival's address aliases + `counted_instances`
        // marker via `disconnect_connection_instance` BEFORE calling here,
        // and pass `expected = None` once that succeeds — so there is
        // nothing left for this to do for them, and if that eviction
        // instead DECLINED (the rival was already superseded by some other
        // concurrent publish), the CAS above cannot succeed against a
        // stale `Some(rival)` either, so this is never reached in that
        // case. The AcceptIncoming shape — a known stale/dead `expected`
        // that was never separately evicted first (e.g.
        // `finalize_new_outbound_connection`'s eager `existing_before`
        // rival, or the nested re-resolve retries' `Some(rival)`) — relies
        // on THIS CAS success being the one place that retires it: without
        // this, the old session survived being displaced from the peer
        // slot but kept its address aliases and its `connection_counter`
        // contribution forever outstanding (reviewer finding). Keying the
        // retire on "the CAS just displaced a known, non-`None` `expected`"
        // closes every such call site through this single primitive
        // instead of a fix scattered across each one, and can never
        // double-retire for the reasons above.
        if let Some(expected) = expected {
            self.retire_displaced_expected(expected, &connection);
        }
        self.mark_routing_changed();
        Ok(())
    }

    /// After [`Self::compare_and_publish_peer_connection`] succeeds
    /// displacing a known, live `expected` from the peer session slot in
    /// favor of `winner`, fully retire `expected` BY IDENTITY: sweep every
    /// `connections_by_addr`/`addr_to_peer_id` alias that still points at
    /// `expected` (never `winner`, which this must never touch) and release
    /// `expected`'s `counted_instances` marker exactly once — mirroring what
    /// [`Self::disconnect_connection_instance`] already does for the
    /// `ReplaceExisting` path via [`Self::evict_before_replace`], for the
    /// AcceptIncoming shape where the caller never separately evicted
    /// `expected` first.
    ///
    /// The peer_sessions/`connections_by_peer` slot itself needs no cleanup
    /// here: the compare-and-publish that already succeeded is what
    /// overwrote both of those with `winner`, atomically. Only `expected`'s
    /// OWN address aliases and `connection_counter` contribution are left
    /// outstanding, which is exactly what this closes.
    fn retire_displaced_expected(
        &self,
        expected: &Arc<LockFreeConnection>,
        winner: &Arc<LockFreeConnection>,
    ) {
        debug_assert!(
            !Arc::ptr_eq(expected, winner),
            "retire_displaced_expected must never be asked to retire the connection it was just \
             asked to publish"
        );
        let mut alias_addrs: Vec<SocketAddr> = Vec::new();
        self.connections_by_addr.iter_sync(|addr, v| {
            if Arc::ptr_eq(v, expected) {
                alias_addrs.push(*addr);
            }
            true
        });
        let mut routing_changed = false;
        for addr in alias_addrs {
            let removed = self
                .connections_by_addr
                .remove_if_sync(&addr, |v| Arc::ptr_eq(v, expected))
                .is_some();
            if removed {
                routing_changed = true;
                let _ = self.addr_to_peer_id.remove_sync(&addr);
                self.clear_capabilities_for_addr(&addr);
            }
        }
        self.release_counted_connection(expected);
        // `expected.correlation` is a SESSION-level
        // tracker shared BY POINTER across reconnect instances for this
        // peer. `winner` is already published as the peer's current session
        // at this point, so if it shares that exact tracker Arc with
        // `expected` (the common case — both went through
        // `get_or_create_correlation_tracker`/`add_connection_by_peer_id`
        // for the same peer), an unconditional `expected.abort_tasks()`
        // would `cancel_all()` the tracker `winner` is actively using,
        // spuriously failing the winner's in-flight asks. Only cancel the
        // shared tracker when `winner` does NOT depend on it.
        if expected.shares_correlation_tracker(winner) {
            expected.abort_tasks_keep_correlation();
        } else {
            expected.abort_tasks();
        }
        if routing_changed {
            self.mark_routing_changed();
        }
    }

    /// Evict `rival` for a `ReplaceExisting` decision, and return the
    /// correct `expected` snapshot the FOLLOW-UP compare-and-publish must
    /// use to enact that decision — never `Some(rival)` unconditionally.
    ///
    /// `disconnect_connection_instance` is itself a self-validating,
    /// idempotent CAS: it either finds `rival` still installed and
    /// atomically clears + tears it down (`true`), or finds it already
    /// superseded by a concurrent publish and declines untouched (`false`).
    /// That boolean tells the caller exactly what the peer session slot
    /// looks like right now, without a second read that could itself race:
    ///   - eviction succeeded: the slot is now provably `None` (we just
    ///     cleared it ourselves) — `expected` for the follow-up publish
    ///     must be `None`, never the stale `Some(rival)`, or the publish
    ///     would spuriously "lose" to a clear that was actually our own.
    ///   - eviction declined: `rival` was already superseded by whatever is
    ///     actually current now, so `rival` remains the honest `expected`
    ///     snapshot — the follow-up compare-and-publish will (correctly)
    ///     also fail to match it and report the REAL current occupant for
    ///     re-resolution, exactly like the `AcceptIncoming` arm's own CAS
    ///     loss.
    ///
    /// This is the single evict-then-choose-expected step shared by every
    /// `ReplaceExisting` call site (outbound-finalize's eager decision, its
    /// own nested re-resolve, and inbound-accept) so none of them can revert
    /// to the "evict, then unconditionally publish" shape that clobbers a
    /// concurrently published fresh session.
    fn evict_before_replace(
        &self,
        peer_id: &crate::PeerId,
        rival: &Arc<LockFreeConnection>,
    ) -> Option<Arc<LockFreeConnection>> {
        if self.disconnect_connection_instance(peer_id, rival) {
            None
        } else {
            Some(rival.clone())
        }
    }

    /// Enact an `AcceptIncoming` outbound-finalize decision via
    /// compare-and-publish against the exact snapshot (`expected`) the
    /// decision was computed from, re-resolving against reality if a
    /// concurrent publish beat this call to the peer session slot.
    ///
    /// See the module-level `finalize_new_outbound_connection` commentary
    /// on the `existing_before` snapshot for why the decision and the
    /// publish that enacts it can never be a single non-atomic step: a
    /// PREFERRED inbound can be published for this peer in the gap between
    /// that snapshot and this call, and this is the fallback outbound's own
    /// publish attempt — it must never unconditionally overwrite whatever a
    /// concurrent publish already installed.
    ///
    /// Returns `true` if `connection_arc` ended up published/kept as a
    /// session candidate the caller should finalize as a live handle
    /// (`AcceptIncoming`'s happy path, `AcceptIncoming`'s bounded retry
    /// against a stale rival, or `ReplaceExisting`), and `false` when the
    /// re-resolve concluded the candidate must be rejected
    /// (`RejectIncoming` / `EvictStaleRejectIncoming`). On `false` the
    /// candidate has NOT been unpublished here — that is address-keyed
    /// cleanup the caller performs via `unpublish_rejected_outbound_candidate`,
    /// identical to the eager-reject call sites in
    /// `finalize_new_outbound_connection`, so a `false` return must never be
    /// treated as "safe to finalize" the way the earlier version of this
    /// function silently was.
    #[must_use = "a `false` return means the candidate lost its re-resolved \
                  tie-break and the caller MUST unpublish it and reject the \
                  finalize, exactly like the eager RejectIncoming/\
                  EvictStaleRejectIncoming call sites — silently discarding \
                  this result reproduces the reviewer finding where a \
                  rejected outbound candidate stayed indexed/counted and was \
                  handed back as a live Ok handle"]
    fn publish_outbound_or_reresolve(
        &self,
        peer_id: &crate::PeerId,
        connection_arc: &Arc<LockFreeConnection>,
        expected: Option<&Arc<LockFreeConnection>>,
        registry_weak: &std::sync::Weak<GossipRegistry>,
    ) -> bool {
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                peer: peer_id.clone(),
                addr: connection_arc.addr,
            },
        );
        let Err(rival) =
            self.compare_and_publish_peer_connection(peer_id, expected, connection_arc.clone())
        else {
            return true;
        };
        let rival = match rival {
            Some(rival) => rival,
            None => {
                // The CAS failed but the slot is empty now too: a concurrent
                // CLEAR (not a publish) raced us. Retry once against the
                // now-empty slot — instrumented so a test can deterministically
                // pin a further concurrent publish into this exact retry gap.
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::OutboundFinalizeClearRaceRetry {
                        peer: peer_id.clone(),
                        addr: connection_arc.addr,
                    },
                );
                match self.compare_and_publish_peer_connection(
                    peer_id,
                    None,
                    connection_arc.clone(),
                ) {
                    Ok(()) => return true,
                    Err(Some(retry_rival)) => retry_rival,
                    Err(None) => {
                        // A second concurrent CLEAR raced the retry itself.
                        // Nested races beyond this single bounded retry are
                        // out of scope, matching the tolerance already
                        // documented elsewhere in this function — reject
                        // rather than loop indefinitely. The candidate was
                        // never actually installed as the session, so it
                        // must never be finalized as one.
                        debug!(
                            peer_id = %peer_id,
                            "outbound finalize compare-and-publish retry also lost to a second \
                             concurrent clear; rejecting our own candidate rather than retrying \
                             indefinitely"
                        );
                        return false;
                    }
                }
            }
        };
        // A rival is actually installed now (e.g. a fresh preferred inbound
        // published concurrently, either in the original CAS window or in
        // the clear-race retry window above). Re-resolve the identical,
        // address-blind tie-break the caller already computed, this time
        // against reality.
        self.resolve_and_act_on_outbound_rival(peer_id, connection_arc, &rival, registry_weak)
    }

    /// Re-resolve the address-blind tie-break for `connection_arc` against an
    /// actually-installed `rival` and act on the outcome exactly like the
    /// eager decision arms in `finalize_new_outbound_connection` do. Shared
    /// by both `publish_outbound_or_reresolve` CAS-loss shapes: a rival
    /// observed directly by the primary compare-and-publish, and a rival
    /// observed only after its bounded clear-race retry.
    ///
    /// Entry point for a bounded total of ONE nested re-resolve beyond this
    /// call's own `AcceptIncoming` retry (see
    /// `resolve_and_act_on_outbound_rival_bounded`) — nested races beyond
    /// that are rejected rather than chased indefinitely.
    fn resolve_and_act_on_outbound_rival(
        &self,
        peer_id: &crate::PeerId,
        connection_arc: &Arc<LockFreeConnection>,
        rival: &Arc<LockFreeConnection>,
        registry_weak: &std::sync::Weak<GossipRegistry>,
    ) -> bool {
        self.resolve_and_act_on_outbound_rival_bounded(
            peer_id,
            connection_arc,
            rival,
            registry_weak,
            1,
        )
    }

    /// Implementation of `resolve_and_act_on_outbound_rival` with an explicit
    /// `nested_retries_remaining` budget. The `AcceptIncoming` arm's own
    /// retry against `rival` can itself lose to a THIRD session — another
    /// publish landing in the window between this re-resolved decision and
    /// the retry's compare-and-publish. That loss must never be silently
    /// treated as success (the reviewer finding this closes): on
    /// `Err(Some(new_rival))` this re-resolves once more against the new
    /// rival (consuming the budget), and on exhaustion — or on `Err(None)`'s
    /// own single empty-slot retry also failing — rejects the candidate
    /// (`false`) instead of looping. The bool contract from
    /// `resolve_and_act_on_outbound_rival` holds on every path: `true` only
    /// when `connection_arc` is actually installed as the peer's current
    /// session.
    fn resolve_and_act_on_outbound_rival_bounded(
        &self,
        peer_id: &crate::PeerId,
        connection_arc: &Arc<LockFreeConnection>,
        rival: &Arc<LockFreeConnection>,
        registry_weak: &std::sync::Weak<GossipRegistry>,
        nested_retries_remaining: u8,
    ) -> bool {
        let decision = registry_weak
            .upgrade()
            .map(|registry| {
                let keep_existing = registry.should_keep_connection(
                    peer_id,
                    rival.direction == ConnectionDirection::Outbound,
                );
                let keep_incoming = registry.should_keep_connection(peer_id, true);
                resolve_authenticated_connection_conflict(
                    rival,
                    connection_arc,
                    keep_existing,
                    keep_incoming,
                    // `rival` was observed only after our candidate lost a
                    // compare-and-publish against its original snapshot. Its
                    // later publication is the newer session-generation
                    // boundary, regardless of which physical stream object
                    // happened to be allocated first. Never let a stale
                    // snapshot clobber that concurrent publication.
                    false,
                )
            })
            .unwrap_or(ConnectionConflictDecision::RejectIncoming);
        match decision {
            ConnectionConflictDecision::AcceptIncoming => {
                // The rival is stale/dead by the time we re-resolved and our
                // outbound is still preferred — retry the compare-and-publish
                // against it. This retry's own result MUST be checked: a
                // further concurrent publish can land in this exact window
                // and make the retry itself lose, in which case our
                // candidate was never installed and must not be reported as
                // if it were.
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::OutboundFinalizeAcceptIncomingRetryAttempt {
                        peer: peer_id.clone(),
                        addr: connection_arc.addr,
                    },
                );
                match self.compare_and_publish_peer_connection(
                    peer_id,
                    Some(rival),
                    connection_arc.clone(),
                ) {
                    Ok(()) => true,
                    Err(Some(new_rival)) => {
                        // A THIRD session was published into the retry's own
                        // window. Re-resolve once more against reality
                        // (bounded by `nested_retries_remaining`) rather than
                        // reporting the stale-rival retry as a success it
                        // never was.
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "outbound finalize AcceptIncoming retry lost to yet another \
                                 concurrently published rival and the bounded nested-re-resolve \
                                 budget is exhausted; rejecting our own candidate rather than \
                                 retrying indefinitely"
                            );
                            return false;
                        }
                        self.resolve_and_act_on_outbound_rival_bounded(
                            peer_id,
                            connection_arc,
                            &new_rival,
                            registry_weak,
                            nested_retries_remaining - 1,
                        )
                    }
                    Err(None) => {
                        // A concurrent CLEAR raced this retry. One bounded
                        // retry against the now-empty slot; if that also
                        // fails, reject rather than loop.
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "outbound finalize AcceptIncoming retry lost to a concurrent \
                                 clear and the bounded nested-re-resolve budget is exhausted; \
                                 rejecting our own candidate rather than retrying indefinitely"
                            );
                            return false;
                        }
                        match self.compare_and_publish_peer_connection(
                            peer_id,
                            None,
                            connection_arc.clone(),
                        ) {
                            Ok(()) => true,
                            Err(_) => {
                                debug!(
                                    peer_id = %peer_id,
                                    "outbound finalize AcceptIncoming retry's own empty-slot \
                                     retry also lost; rejecting our own candidate rather than \
                                     retrying indefinitely"
                                );
                                false
                            }
                        }
                    }
                }
            }
            ConnectionConflictDecision::ReplaceExisting => {
                // Same defect, one level deeper: the re-resolved decision
                // against the ACTUALLY-installed `rival` can itself be
                // `ReplaceExisting` (a live but non-preferred rival). Evict
                // it, then compare-and-publish against the correct `expected`
                // `evict_before_replace` derives from that eviction's own
                // outcome — never an unconditional publish — and, on a CAS
                // loss to yet another concurrently published session, bound
                // the re-resolve exactly like the `AcceptIncoming` arm above
                // rather than looping indefinitely.
                let expected = self.evict_before_replace(peer_id, rival);
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::OutboundFinalizeReplaceExistingRetryAttempt {
                        peer: peer_id.clone(),
                        addr: connection_arc.addr,
                    },
                );
                match self.compare_and_publish_peer_connection(
                    peer_id,
                    expected.as_ref(),
                    connection_arc.clone(),
                ) {
                    Ok(()) => true,
                    Err(Some(new_rival)) => {
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "outbound finalize ReplaceExisting retry lost to yet another \
                                 concurrently published rival and the bounded nested-re-resolve \
                                 budget is exhausted; rejecting our own candidate rather than \
                                 retrying indefinitely"
                            );
                            return false;
                        }
                        self.resolve_and_act_on_outbound_rival_bounded(
                            peer_id,
                            connection_arc,
                            &new_rival,
                            registry_weak,
                            nested_retries_remaining - 1,
                        )
                    }
                    Err(None) => {
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "outbound finalize ReplaceExisting retry lost to a concurrent \
                                 clear and the bounded nested-re-resolve budget is exhausted; \
                                 rejecting our own candidate rather than retrying indefinitely"
                            );
                            return false;
                        }
                        match self.compare_and_publish_peer_connection(
                            peer_id,
                            None,
                            connection_arc.clone(),
                        ) {
                            Ok(()) => true,
                            Err(_) => {
                                debug!(
                                    peer_id = %peer_id,
                                    "outbound finalize ReplaceExisting retry's own empty-slot \
                                     retry also lost; rejecting our own candidate rather than \
                                     retrying indefinitely"
                                );
                                false
                            }
                        }
                    }
                }
            }
            ConnectionConflictDecision::EvictStaleRejectIncoming => {
                let _ = self.disconnect_connection_instance(peer_id, rival);
                debug!(
                    peer_id = %peer_id,
                    "outbound finalize compare-and-publish lost to a concurrently published \
                     rival; evicted the now-stale rival and rejecting our own candidate — \
                     it was not the tie-break-preferred direction either"
                );
                false
            }
            ConnectionConflictDecision::RejectIncoming => {
                debug!(
                    peer_id = %peer_id,
                    "outbound finalize compare-and-publish lost to a concurrently published, \
                     tie-break-preferred rival; rejecting our own candidate"
                );
                false
            }
        }
    }

    /// Finish indexing + counting a connection that has ALREADY been
    /// successfully installed as the peer's current session via
    /// `compare_and_publish_peer_connection` (directly, or through
    /// `publish_inbound_or_reresolve`/`publish_outbound_or_reresolve`) — the
    /// address-index / `connection_counter` side effects
    /// `add_connection_by_peer_id` otherwise bundles together with its own
    /// unconditional publish. Callers that route their publish through
    /// compare-and-publish call this exactly once, and only after that
    /// compare-and-publish actually succeeded — never for a candidate that
    /// lost its tie-break re-resolution, mirroring
    /// `finalize_new_outbound_connection`'s own non-reject-path counting.
    ///
    /// Owns ALL address-alias writes for an accepted inbound connection:
    /// both `peer_state_addr` (the peer's configured/advertised bind
    /// address) and, when it differs, the ephemeral TCP source address
    /// (`ephemeral_addr`) that `handle_response_message` looks connections
    /// up by. Callers must never index either address themselves outside
    /// this function — see the review finding this closes below.
    ///
    /// The compare-and-publish this follows only proves `connection` won the
    /// peer-session slot AT THAT INSTANT; a concurrent evict/supersede of
    /// this exact instance (e.g. another accept/finalize's
    /// `disconnect_connection_instance` or a further compare-and-publish)
    /// can land in the window between that CAS win and this call, or even
    /// between this function's own two alias writes. Any such eviction's own
    /// `connections_by_addr` alias-sweep only finds whichever of this
    /// candidate's aliases has been written so far — a candidate that used
    /// to write the ephemeral alias as a separate, unconditional step
    /// *outside* this function (the reviewer finding) would have that
    /// eviction's sweep clean up `peer_state_addr` while the not-yet-written
    /// ephemeral alias survives the sweep, then get durably (and stalely)
    /// written moments later regardless of the eviction. Folding both writes
    /// into this one function closes that: without a recheck here, the
    /// writes below would durably index and count an already-evicted,
    /// already-aborted connection under one or both aliases (a stale address
    /// alias plus a zombie `connection_counter` contribution).
    ///
    /// After performing ALL the writes, this therefore RE-VALIDATES that the
    /// peer session slot still holds exactly `connection` (`Arc::ptr_eq`).
    /// If it no longer does — evicted or superseded by something else in the
    /// window above — every write just performed is undone across BOTH
    /// addresses: every `connections_by_addr`/`addr_to_peer_id` alias that is
    /// still `connection`'s own (via `remove_connection_instance_by_id`'s
    /// identity-scoped compare-and-remove and full-map alias sweep, so a
    /// different, already-current instance's alias is never touched) is
    /// removed, the `connection_counter` contribution just marked above is
    /// released exactly once through the same `counted_instances` ownership
    /// table every other teardown path uses, and `connection`'s tasks are
    /// aborted. Returns `false` in that case so the caller treats this
    /// exactly like a re-resolved tie-break rejection: never indexed,
    /// counted, or reported as the accepted session. Returns `true` when
    /// `connection` is still genuinely the peer's current session after the
    /// writes, in which case they are all durable.
    #[must_use = "a `false` return means a concurrent evict raced this connection out of the \
                  peer session in the window before it was durably indexed, and this function \
                  has already undone all of its own writes (both addresses) — the caller MUST \
                  treat the candidate as rejected exactly like a re-resolved tie-break loss, \
                  never report it as the accepted session, and must NEVER separately index \
                  either address itself"]
    pub(crate) fn finish_indexing_accepted_connection(
        &self,
        peer_id: &crate::PeerId,
        peer_state_addr: SocketAddr,
        ephemeral_addr: Option<SocketAddr>,
        connection: &Arc<LockFreeConnection>,
    ) -> bool {
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::InboundAcceptIndexAttempt {
                peer: peer_id.clone(),
                addr: peer_state_addr,
            },
        );
        self.set_discovered_peer_addr(peer_id, peer_state_addr);
        let _ = self
            .addr_to_peer_id
            .upsert_sync(peer_state_addr, peer_id.clone());
        let _ = self
            .connections_by_addr
            .upsert_sync(peer_state_addr, connection.clone());

        // Dedupe: the ephemeral TCP source address and the peer's
        // configured/advertised bind address are frequently identical
        // (direct dial, no NAT/reverse-proxy in between) — never write the
        // same address twice.
        let distinct_ephemeral = ephemeral_addr.filter(|addr| *addr != peer_state_addr);
        if let Some(ephemeral_addr) = distinct_ephemeral {
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::InboundAcceptEphemeralAliasAttempt {
                    peer: peer_id.clone(),
                    addr: ephemeral_addr,
                },
            );
            let _ = self
                .addr_to_peer_id
                .upsert_sync(ephemeral_addr, peer_id.clone());
            let _ = self
                .connections_by_addr
                .upsert_sync(ephemeral_addr, connection.clone());
        }

        // Paired, insert-gated: `count_in_new_instance` only bumps
        // `connection_counter` if it is the call that newly creates this
        // instance's `counted_instances` marker, so a concurrent teardown
        // racing anywhere around this call nets to the correct total
        // regardless of interleaving — see that function's own comment. A
        // connection with no stream handle is never counted at all (nothing
        // to mark it by), mirroring the `None` arm's cleanup below.
        let instance_id = connection.stream_handle.as_ref().map(|h| h.instance_id());
        if let Some(instance_id) = instance_id {
            self.count_in_new_instance(instance_id);
        }

        let still_current = self
            .peer_sessions
            .read_sync(peer_id, |_, session| {
                session
                    .current_connection()
                    .is_some_and(|current| Arc::ptr_eq(&current, connection))
            })
            .unwrap_or(false);
        if still_current {
            return true;
        }

        debug!(
            peer_id = %peer_id,
            peer_state_addr = %peer_state_addr,
            ephemeral_addr = ?distinct_ephemeral,
            "finish_indexing_accepted_connection: a concurrent evict/supersede raced this \
             connection out of the peer session before it was durably indexed; undoing every \
             address alias and the counter contribution just written and rejecting the \
             candidate"
        );
        match instance_id {
            Some(instance_id) => {
                // Whichever alias a concurrent mid-window evict's own sweep
                // missed (it ran before this candidate had that alias
                // written yet) is still present under `connection`'s
                // identity; try both addresses in turn so
                // `remove_connection_instance_by_id`'s identity-scoped
                // removal plus its full-map alias sweep runs regardless of
                // which one survived, guaranteeing neither is left stale.
                let removed_via_peer_state_addr = self
                    .remove_connection_instance_by_id(peer_state_addr, instance_id)
                    .is_some();
                let removed_via_ephemeral_addr = !removed_via_peer_state_addr
                    && distinct_ephemeral.is_some_and(|ephemeral_addr| {
                        self.remove_connection_instance_by_id(ephemeral_addr, instance_id)
                            .is_some()
                    });
                if !removed_via_peer_state_addr && !removed_via_ephemeral_addr {
                    // Neither address-keyed removal found this instance
                    // still indexed at all — a concurrent teardown (e.g.
                    // `disconnect_connection_instance` racing in the window
                    // between the counter/marker pairing above and this
                    // revalidation) already swept both aliases itself. That
                    // teardown's own `release_counted_connection` call can
                    // only have run BEFORE `count_in_new_instance` inserted
                    // the marker above (this candidate's aliases were still
                    // present for it to find at all, which is only possible
                    // pre-insert — see `count_in_new_instance`'s own
                    // comment), so it found no marker and released nothing.
                    // Fall back to a direct, instance-identity-scoped release
                    // so the marker/counter pairing just established above is
                    // never left permanently orphaned — the same
                    // `release_displaced_connection_count` idiom
                    // `retire_lost_cas_matched_instance` and the IO-exit
                    // superseded-exit fallback use for this exact shape. Safe
                    // even if some OTHER path already released it (e.g. one
                    // that raced AFTER the insert): `counted_instances`
                    // removal is idempotent, so this is then a no-op.
                    self.release_displaced_connection_count(instance_id);
                }
            }
            None => {
                // No stream handle to identify an instance by — fall back to
                // a direct identity-scoped removal at every address this
                // call wrote. Never counted above either (no `instance_id`
                // to mark), so there is nothing to release.
                for addr in std::iter::once(peer_state_addr).chain(distinct_ephemeral) {
                    let removed = self
                        .connections_by_addr
                        .remove_if_sync(&addr, |v| Arc::ptr_eq(v, connection))
                        .is_some();
                    if removed {
                        let _ = self.addr_to_peer_id.remove_sync(&addr);
                        self.clear_capabilities_for_addr(&addr);
                    }
                }
                connection.abort_tasks();
            }
        }
        false
    }

    /// Inbound-accept counterpart of `publish_outbound_or_reresolve`: enact
    /// an `AcceptIncoming`/`ReplaceExisting` inbound-accept decision via
    /// compare-and-publish against the exact snapshot (`expected`) the
    /// decision was computed from, re-resolving against reality if a
    /// concurrent publish (outbound OR inbound) beat this call to the peer
    /// session slot. Closes the inbound-side counterpart of the
    /// outbound-finalize publish gap: `handle_incoming_connection_tls`'s
    /// `existing_conn` snapshot can be superseded by a concurrent
    /// accept/finalize between that snapshot and this call, and this
    /// candidate's own publish must never unconditionally overwrite
    /// whatever that concurrent publish already installed.
    ///
    /// Returns `true` if `connection_arc` ended up published/kept as the
    /// peer's current session, `false` when the re-resolve concluded the
    /// candidate must be rejected. On `false` the candidate has NOT been
    /// indexed by address or counted here — the caller must not do so
    /// either, mirroring the pre-existing `RejectIncoming`/
    /// `EvictStaleRejectIncoming` arms that never called
    /// `add_connection_by_peer_id` in the first place.
    #[must_use = "a `false` return means the candidate lost its re-resolved \
                  tie-break and must never be indexed by address, counted, or \
                  reported as the accepted session — silently discarding this \
                  result reproduces the reviewer finding where a displaced \
                  fresh session was clobbered by an unconditional publish"]
    pub(crate) fn publish_inbound_or_reresolve(
        &self,
        peer_id: &crate::PeerId,
        connection_arc: &Arc<LockFreeConnection>,
        expected: Option<&Arc<LockFreeConnection>>,
        registry_weak: &std::sync::Weak<GossipRegistry>,
    ) -> bool {
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::InboundAcceptPublishAttempt {
                peer: peer_id.clone(),
                addr: connection_arc.addr,
            },
        );
        let Err(rival) =
            self.compare_and_publish_peer_connection(peer_id, expected, connection_arc.clone())
        else {
            return true;
        };
        let rival = match rival {
            Some(rival) => rival,
            None => {
                // The CAS failed but the slot is empty now too: a concurrent
                // CLEAR (not a publish) raced us. One bounded retry against
                // the now-empty slot, instrumented so a test can
                // deterministically pin a further concurrent publish into
                // this exact retry gap.
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::InboundAcceptClearRaceRetry {
                        peer: peer_id.clone(),
                        addr: connection_arc.addr,
                    },
                );
                match self.compare_and_publish_peer_connection(
                    peer_id,
                    None,
                    connection_arc.clone(),
                ) {
                    Ok(()) => return true,
                    Err(Some(retry_rival)) => retry_rival,
                    Err(None) => {
                        debug!(
                            peer_id = %peer_id,
                            "inbound accept compare-and-publish retry also lost to a second \
                             concurrent clear; rejecting our own candidate rather than retrying \
                             indefinitely"
                        );
                        return false;
                    }
                }
            }
        };
        self.resolve_and_act_on_inbound_rival(peer_id, connection_arc, &rival, registry_weak)
    }

    /// Re-resolve the address-blind tie-break for an inbound `connection_arc`
    /// against an actually-installed `rival` and act on the outcome exactly
    /// like `handle_incoming_connection_tls`'s eager decision arms do — the
    /// inbound counterpart of `resolve_and_act_on_outbound_rival`.
    fn resolve_and_act_on_inbound_rival(
        &self,
        peer_id: &crate::PeerId,
        connection_arc: &Arc<LockFreeConnection>,
        rival: &Arc<LockFreeConnection>,
        registry_weak: &std::sync::Weak<GossipRegistry>,
    ) -> bool {
        self.resolve_and_act_on_inbound_rival_bounded(
            peer_id,
            connection_arc,
            rival,
            registry_weak,
            1,
        )
    }

    /// Implementation of `resolve_and_act_on_inbound_rival` with an explicit
    /// `nested_retries_remaining` budget — the inbound counterpart of
    /// `resolve_and_act_on_outbound_rival_bounded`, bounding nested re-resolve
    /// retries identically so a chain of concurrent publishes is rejected
    /// rather than chased indefinitely.
    fn resolve_and_act_on_inbound_rival_bounded(
        &self,
        peer_id: &crate::PeerId,
        connection_arc: &Arc<LockFreeConnection>,
        rival: &Arc<LockFreeConnection>,
        registry_weak: &std::sync::Weak<GossipRegistry>,
        nested_retries_remaining: u8,
    ) -> bool {
        let decision = registry_weak
            .upgrade()
            .map(|registry| {
                let keep_existing = registry.should_keep_connection(
                    peer_id,
                    rival.direction == ConnectionDirection::Outbound,
                );
                // The candidate here is always the freshly-accepted INBOUND
                // connection — `is_outbound == false`, mirroring
                // `handle_incoming_connection_tls`'s own `keep_new_inbound`.
                let keep_incoming = registry.should_keep_connection(peer_id, false);
                resolve_authenticated_connection_conflict(
                    rival,
                    connection_arc,
                    keep_existing,
                    keep_incoming,
                    // Same publication-order rule as outbound finalize: a
                    // rival discovered only after this candidate lost its
                    // original CAS is the newer generation and must win over
                    // the stale candidate snapshot.
                    false,
                )
            })
            .unwrap_or(ConnectionConflictDecision::RejectIncoming);
        match decision {
            ConnectionConflictDecision::AcceptIncoming => {
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::InboundAcceptAcceptIncomingRetryAttempt {
                        peer: peer_id.clone(),
                        addr: connection_arc.addr,
                    },
                );
                match self.compare_and_publish_peer_connection(
                    peer_id,
                    Some(rival),
                    connection_arc.clone(),
                ) {
                    Ok(()) => true,
                    Err(Some(new_rival)) => {
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "inbound accept AcceptIncoming retry lost to yet another \
                                 concurrently published rival and the bounded nested-re-resolve \
                                 budget is exhausted; rejecting our own candidate rather than \
                                 retrying indefinitely"
                            );
                            return false;
                        }
                        self.resolve_and_act_on_inbound_rival_bounded(
                            peer_id,
                            connection_arc,
                            &new_rival,
                            registry_weak,
                            nested_retries_remaining - 1,
                        )
                    }
                    Err(None) => {
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "inbound accept AcceptIncoming retry lost to a concurrent clear \
                                 and the bounded nested-re-resolve budget is exhausted; rejecting \
                                 our own candidate rather than retrying indefinitely"
                            );
                            return false;
                        }
                        match self.compare_and_publish_peer_connection(
                            peer_id,
                            None,
                            connection_arc.clone(),
                        ) {
                            Ok(()) => true,
                            Err(_) => {
                                debug!(
                                    peer_id = %peer_id,
                                    "inbound accept AcceptIncoming retry's own empty-slot retry \
                                     also lost; rejecting our own candidate rather than retrying \
                                     indefinitely"
                                );
                                false
                            }
                        }
                    }
                }
            }
            ConnectionConflictDecision::ReplaceExisting => {
                let expected = self.evict_before_replace(peer_id, rival);
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::InboundAcceptReplaceExistingRetryAttempt {
                        peer: peer_id.clone(),
                        addr: connection_arc.addr,
                    },
                );
                match self.compare_and_publish_peer_connection(
                    peer_id,
                    expected.as_ref(),
                    connection_arc.clone(),
                ) {
                    Ok(()) => true,
                    Err(Some(new_rival)) => {
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "inbound accept ReplaceExisting retry lost to yet another \
                                 concurrently published rival and the bounded nested-re-resolve \
                                 budget is exhausted; rejecting our own candidate rather than \
                                 retrying indefinitely"
                            );
                            return false;
                        }
                        self.resolve_and_act_on_inbound_rival_bounded(
                            peer_id,
                            connection_arc,
                            &new_rival,
                            registry_weak,
                            nested_retries_remaining - 1,
                        )
                    }
                    Err(None) => {
                        if nested_retries_remaining == 0 {
                            debug!(
                                peer_id = %peer_id,
                                "inbound accept ReplaceExisting retry lost to a concurrent clear \
                                 and the bounded nested-re-resolve budget is exhausted; rejecting \
                                 our own candidate rather than retrying indefinitely"
                            );
                            return false;
                        }
                        match self.compare_and_publish_peer_connection(
                            peer_id,
                            None,
                            connection_arc.clone(),
                        ) {
                            Ok(()) => true,
                            Err(_) => {
                                debug!(
                                    peer_id = %peer_id,
                                    "inbound accept ReplaceExisting retry's own empty-slot retry \
                                     also lost; rejecting our own candidate rather than retrying \
                                     indefinitely"
                                );
                                false
                            }
                        }
                    }
                }
            }
            ConnectionConflictDecision::EvictStaleRejectIncoming => {
                let _ = self.disconnect_connection_instance(peer_id, rival);
                debug!(
                    peer_id = %peer_id,
                    "inbound accept compare-and-publish lost to a concurrently published rival; \
                     evicted the now-stale rival and rejecting our own candidate — it was not \
                     the tie-break-preferred direction either"
                );
                false
            }
            ConnectionConflictDecision::RejectIncoming => {
                debug!(
                    peer_id = %peer_id,
                    "inbound accept compare-and-publish lost to a concurrently published, \
                     tie-break-preferred rival; rejecting our own candidate"
                );
                false
            }
        }
    }

    pub(crate) fn clear_current_peer_connection(&self, peer_id: &crate::PeerId) {
        let primary_removed = self
            .get_or_create_peer_session(peer_id)
            .take_current_connection()
            .is_some();
        let alias_removed = self.connections_by_peer.remove_sync(peer_id).is_some();
        if primary_removed || alias_removed {
            self.mark_routing_changed();
        }
    }

    /// Check-then-unconditional-clear: reads `current_connection`,
    /// `ptr_eq`-compares it to `candidate`, and — ONLY afterward, past a log
    /// line and a lifecycle-event construction, a real gap — unconditionally
    /// clears the slot. A concurrent `publish_current_peer_connection`
    /// landing in that gap (e.g. a fresh preferred inbound for this exact
    /// peer) is silently clobbered.
    ///
    /// `get_connection_by_peer_id`'s own internal self-heal no longer uses
    /// this (see [`Self::compare_and_clear_current_peer_connection`]) —
    /// this remains in use only by callers that are themselves already
    /// acting on a connection they just pulled out of an index as part of
    /// tearing it down (`remove_connection`'s per-alias peer cleanup,
    /// `GossipRegistry`'s DNS-refresh dead-connection cleanup), where the
    /// `candidate` was obtained from the SAME removal/observation this call
    /// is reacting to rather than from an earlier decision snapshot. Prefer
    /// [`Self::compare_and_clear_current_peer_connection`] for any new
    /// caller.
    pub(crate) fn clear_current_peer_connection_if_matches(
        &self,
        peer_id: &crate::PeerId,
        candidate: &Arc<LockFreeConnection>,
    ) {
        let should_clear = self
            .peer_sessions
            .read_sync(peer_id, |_, session| {
                session
                    .current_connection()
                    .map(|current| Arc::ptr_eq(&current, candidate))
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        if should_clear {
            let stream_instance_id = candidate
                .stream_handle
                .as_ref()
                .map(|handle| handle.instance_id());
            info!(
                peer_id = %peer_id,
                addr = %candidate.addr,
                direction = ?candidate.direction,
                stream_instance_id = ?stream_instance_id,
                reason = "current_connection_cleared",
                "transport_session_removed"
            );
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::SessionRemoved {
                    peer: peer_id.clone(),
                    addr: candidate.addr,
                    direction: match candidate.direction {
                        ConnectionDirection::Inbound => {
                            crate::lifecycle::TransportDirection::Inbound
                        }
                        ConnectionDirection::Outbound => {
                            crate::lifecycle::TransportDirection::Outbound
                        }
                    },
                    reason: crate::lifecycle::SessionRemovalReason::CurrentConnectionCleared,
                },
            );
            self.clear_current_peer_connection(peer_id);
        }
    }

    /// Atomic counterpart to [`Self::clear_current_peer_connection_if_matches`]:
    /// clears the peer's current-connection slot iff it still holds exactly
    /// `candidate` (`Arc::ptr_eq`), via a single CAS
    /// (`PeerSession::compare_and_clear_current_connection`) with no
    /// observable check-then-act gap. Used by `get_connection_by_peer_id`'s
    /// internal self-heal so a concurrent publish landing between "this
    /// session looks stale" and "clear it" can never be clobbered — it
    /// either finds `candidate` still installed and clears it, or finds the
    /// concurrent publish already there and declines, untouched.
    ///
    /// Mirrors `clear_current_peer_connection_if_matches`'s scope exactly:
    /// only the primary session slot and its `connections_by_peer` mirror
    /// are touched — no `connections_by_addr`/`addr_to_peer_id` cleanup, no
    /// counter release, no task abort. Callers that need full teardown of a
    /// specific instance want `disconnect_connection_instance` instead.
    fn compare_and_clear_current_peer_connection(
        &self,
        peer_id: &crate::PeerId,
        candidate: &Arc<LockFreeConnection>,
    ) -> bool {
        let cleared = self
            .peer_sessions
            .read_sync(peer_id, |_, session| {
                session.compare_and_clear_current_connection(candidate)
            })
            .unwrap_or(false);
        if cleared {
            let stream_instance_id = candidate
                .stream_handle
                .as_ref()
                .map(|handle| handle.instance_id());
            info!(
                peer_id = %peer_id,
                addr = %candidate.addr,
                direction = ?candidate.direction,
                stream_instance_id = ?stream_instance_id,
                reason = "current_connection_cleared",
                "transport_session_removed"
            );
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::SessionRemoved {
                    peer: peer_id.clone(),
                    addr: candidate.addr,
                    direction: match candidate.direction {
                        ConnectionDirection::Inbound => {
                            crate::lifecycle::TransportDirection::Inbound
                        }
                        ConnectionDirection::Outbound => {
                            crate::lifecycle::TransportDirection::Outbound
                        }
                    },
                    reason: crate::lifecycle::SessionRemovalReason::CurrentConnectionCleared,
                },
            );
            let _ = self
                .connections_by_peer
                .remove_if_sync(peer_id, |v| Arc::ptr_eq(v, candidate));
            self.mark_routing_changed();
        }
        cleared
    }

    fn is_usable_connection(&self, conn: &LockFreeConnection) -> bool {
        conn.has_live_stream()
    }

    /// PURE, non-mutating read of "what connection does this peer currently
    /// have" — the primary session slot, then (as fallbacks, purely as
    /// reads) the secondary `connections_by_peer` mirror, the
    /// configured-address index, and the ephemeral-address alias index.
    ///
    /// This performs NO self-heal clear of a stale/unusable session and NO
    /// publish of a fallback match into the peer session slot — unlike
    /// `get_connection_by_peer_id`, which does both as side effects of a
    /// primary-slot miss/staleness. That distinction is load-bearing for any
    /// caller using the result ONLY to compute a tie-break/keep-drop
    /// DECISION: taking this snapshot can never itself mutate pool state, so
    /// a fresh, concurrently-published preferred session can never be
    /// erased merely because something asked "what do you have for this
    /// peer right now?" (reviewer finding P1, the outbound-finalize
    /// `existing_before` snapshot calling the self-healing
    /// `get_connection_by_peer_id` instead of a pure lookup).
    ///
    /// Callers that need the self-healing "give me the usable current
    /// connection, repairing the index if needed" behavior for actual
    /// message routing/dialing must keep using `get_connection_by_peer_id`
    /// — that behavior is intentional there and is now itself race-free via
    /// a CAS-based self-heal (see its own doc).
    pub(crate) fn peer_current_connection_snapshot(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        self.peer_sessions
            .read_sync(peer_id, |_, session| session.current_connection())
            .flatten()
            .or_else(|| {
                self.connections_by_peer
                    .read_sync(peer_id, |_, v| v.clone())
            })
            .or_else(|| {
                self.get_configured_peer_addr(peer_id)
                    .and_then(|addr| self.connections_by_addr.read_sync(&addr, |_, v| v.clone()))
            })
            .or_else(|| self.aliased_connection_by_peer_id(peer_id))
    }

    fn connection_identity_matches_peer(
        &self,
        conn: &LockFreeConnection,
        peer_id: &crate::PeerId,
    ) -> bool {
        conn.embedded_peer_id
            .as_ref()
            .is_some_and(|embedded_peer_id| embedded_peer_id == peer_id)
    }

    fn aliased_connection_by_peer_id(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        let registry = self.registry.load().upgrade();
        let mut fallback = None;

        self.addr_to_peer_id.iter_sync(|addr, mapped_peer_id| {
            if mapped_peer_id != peer_id {
                return true;
            }

            let Some(conn) = self.connections_by_addr.read_sync(addr, |_, v| v.clone()) else {
                return true;
            };
            if !self.is_usable_connection(&conn) {
                return true;
            }
            if !self.connection_identity_matches_peer(&conn, peer_id) {
                warn!(
                    requested_peer_id = %peer_id,
                    actual_peer_id = ?conn.embedded_peer_id,
                    addr = %addr,
                    "CONNECTION POOL: Ignoring address alias with mismatched connection identity"
                );
                return true;
            }

            let is_preferred = registry
                .as_ref()
                .map(|registry| {
                    registry.should_keep_connection(
                        peer_id,
                        conn.direction == ConnectionDirection::Outbound,
                    )
                })
                .unwrap_or(true);

            if is_preferred {
                fallback = Some(conn);
                false
            } else {
                if fallback.is_none() {
                    fallback = Some(conn);
                }
                true
            }
        });

        fallback
    }

    fn session_peer_ids(&self) -> Vec<crate::PeerId> {
        let mut peer_ids = Vec::new();
        self.peer_sessions.iter_sync(|peer_id, _| {
            peer_ids.push(peer_id.clone());
            true
        });
        peer_ids
    }

    fn acquire_outbound_dial_gate(&self, addr: SocketAddr) -> OutboundDialLease {
        let candidate = Arc::new(OutboundDialGate::new());
        let gate = self
            .outbound_dial_gates
            .entry_sync(addr)
            .or_insert_with(|| candidate.clone())
            .get()
            .clone();
        if Arc::ptr_eq(&candidate, &gate) {
            OutboundDialLease::Leader(gate)
        } else {
            OutboundDialLease::Follower(gate)
        }
    }

    fn finish_outbound_dial_gate(
        &self,
        addr: SocketAddr,
        gate: &Arc<OutboundDialGate>,
        succeeded: bool,
    ) {
        gate.finish(succeeded);
        let should_remove = self
            .outbound_dial_gates
            .read_sync(&addr, |_, current| Arc::ptr_eq(current, gate))
            .unwrap_or(false);
        if should_remove {
            let _ = self.outbound_dial_gates.remove_sync(&addr);
        }
    }

    /// Shared pool for aligned receive buffers.
    pub fn aligned_bytes_pool(&self) -> Arc<crate::AlignedBytesPool> {
        self.aligned_bytes_pool.clone()
    }

    /// Allocate a pooled aligned buffer for streaming assembly.
    pub fn make_pooled_aligned_buffer(&self, len: usize) -> crate::PooledAlignedBuffer {
        crate::PooledAlignedBuffer::with_len(len, self.aligned_bytes_pool.clone())
    }

    fn clear_capabilities_for_addr(&self, addr: &SocketAddr) {
        if let Some(registry) = self.registry.load().upgrade() {
            registry.clear_peer_capabilities(addr);
        }
    }

    /// Reindex an existing connection under a new logical address for the peer.
    ///
    /// This is needed when a peer connects FROM an ephemeral TCP port but advertises
    /// a different bind address in gossip. We need to update `connections_by_addr` so
    /// that lookups by the advertised address find the connection.
    ///
    /// Called from `RoutingPublisher::set_configured_peer_addr`'s trait
    /// impl synchronously, from INSIDE the owner's serialized
    /// `install_pin`/`migrate` commands -- as well as directly by ordinary
    /// connection-establishment paths. A caller-side read of the pin
    /// followed by a separate call here can never be truly atomic with a
    /// concurrent owner command (the owner runs as its own task;
    /// `ConnectionPool`'s maps aren't protected by one lock spanning a
    /// whole owner command); doing the reindex in the SAME synchronous
    /// call that decides the pin removes that gap entirely.
    pub fn reindex_connection_addr(&self, peer_id: &crate::PeerId, new_addr: SocketAddr) {
        // First, check if this peer still has an active connection
        // This guards against race conditions where disconnect happens between checks
        let Some(connection) = self.get_connection_by_peer_id(peer_id) else {
            // Peer was disconnected, nothing to reindex.
            return;
        };

        // Reindexing is a derived connection projection, never an ownership
        // decision.  The caller must have published the address route first
        // (through the registry owner for gossip claims).  A delayed losing
        // FullSync must not remove or overwrite the successor's route.
        let published_owner = self
            .addr_to_peer_id
            .read_sync(&new_addr, |_, owner| owner.clone());
        if published_owner.as_ref() != Some(peer_id) {
            debug!(
                peer_id = %peer_id,
                new_addr = %new_addr,
                published_owner = ?published_owner,
                "skipping connection reindex for an address owned by another identity"
            );
            return;
        }

        let old_addr = connection.addr;

        // Double-check peer still exists (guard against concurrent disconnect)
        if !self.has_connection_by_peer_id(peer_id) {
            return;
        }

        // Insert the connection under the new (advertised) address
        let _ = self
            .connections_by_addr
            .upsert_sync(new_addr, connection.clone());
        // Also update peer_id_to_addr so disconnect uses the correct address
        self.set_discovered_peer_addr(peer_id, new_addr);

        // IMPORTANT: Keep the old (ephemeral) address entry as well!
        // Inbound messages still arrive with the TCP source address (old_addr),
        // so we need both addresses to point to the same connection.
        // The old entry is NOT removed - both addresses are valid for this peer.
        if old_addr != new_addr {
            // Preserve an already-published transport alias, but never steal
            // an old socket address from another identity.
            let old_addr_is_this_peer = self
                .addr_to_peer_id
                .read_sync(&old_addr, |_, owner| owner == peer_id)
                .unwrap_or(false);
            if old_addr_is_this_peer {
                let _ = self.connections_by_addr.upsert_sync(old_addr, connection);
                debug!(
                    old_addr = %old_addr,
                    new_addr = %new_addr,
                    peer_id = %peer_id,
                    "📍 Preserved authenticated transport address mapping"
                );
            }
        }

        info!(
            old_addr = %old_addr,
            new_addr = %new_addr,
            peer_id = %peer_id,
            "📍 Reindexed connection from ephemeral port to bind address"
        );
    }

    /// Get a connection by peer ID
    ///
    /// This is a SELF-HEALING lookup, not a pure one: when the primary
    /// session slot holds a connection that is no longer usable, it clears
    /// that slot, and when a fallback (address/alias) lookup finds a live
    /// match, it publishes that match back into the primary slot. Both are
    /// intentional for callers that want "the live connection to route a
    /// message through / dial against, repairing the index along the way" —
    /// see `send_to_peer_id*`, `get_connection_to_peer`, and the various
    /// consumers listed at the bottom of this function.
    ///
    /// It must never be used to decide a tie-break/keep-drop conflict: the
    /// self-heal clear below is a real mutation, and callers that used it
    /// only to compute a decision (e.g. `finalize_new_outbound_connection`'s
    /// `existing_before`) could have that mutation race a concurrent publish
    /// and erase a fresh session before the decision even ran. Use the pure
    /// [`Self::peer_current_connection_snapshot`] for decision snapshots
    /// instead.
    pub(crate) fn get_connection_by_peer_id(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        // PRIMARY: Look up live connection through the peer session.
        if let Some(conn) = self
            .peer_sessions
            .read_sync(peer_id, |_, session| session.current_connection())
            .flatten()
        {
            if self.is_usable_connection(&conn) {
                debug!("CONNECTION POOL: Found connection for peer '{}'", peer_id);
                return Some(conn);
            }
            warn!(
                "CONNECTION POOL: Connection for peer '{}' is not usable",
                peer_id
            );
            // Self-heal the stale slot, but atomically at the source: a
            // check-then-unconditional-clear (the old
            // `clear_current_peer_connection_if_matches` shape) has a real
            // gap between the ptr_eq check above and the clear below, in
            // which a concurrent `publish_current_peer_connection` (e.g. a
            // fresh preferred inbound landing for this exact peer) can
            // install a new session that the unconditional clear would then
            // silently erase — this primitive being called from a
            // decision-only context is precisely how that reproduced the
            // tie-break reconnect thrash (reviewer finding P1). The
            // instrumentation event below fires unconditionally,
            // immediately before the attempt, so tests can pin a concurrent
            // publish into this exact gap; the CAS then either finds `conn`
            // still installed and clears it, or finds something else — the
            // concurrent publish — and declines untouched.
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::GetConnectionSelfHealClearAttempt {
                    peer: peer_id.clone(),
                    addr: conn.addr,
                },
            );
            let _ = self.compare_and_clear_current_peer_connection(peer_id, &conn);
        }

        // FALLBACK: Outbound connections may only be indexed by address.
        // Look up the configured address via peer session, then get the connection by address.
        if let Some(addr) = self.get_configured_peer_addr(peer_id) {
            if let Some(conn) = self.connections_by_addr.read_sync(&addr, |_, v| v.clone()) {
                if self.is_usable_connection(&conn) {
                    debug!(
                        "CONNECTION POOL: Found connection for peer '{}' via address fallback ({})",
                        peer_id, addr
                    );
                    // Index by peer_id for future lookups
                    self.publish_current_peer_connection(peer_id, conn.clone());
                    return Some(conn);
                }
            }
        }

        // FALLBACK: inbound connections are also indexed by the peer's ephemeral
        // socket address. If a non-preferred outbound connection overwrote the
        // current peer slot and then closed, this alias is still the live stream.
        if let Some(conn) = self.aliased_connection_by_peer_id(peer_id) {
            debug!(
                "CONNECTION POOL: Found connection for peer '{}' via address alias ({})",
                peer_id, conn.addr
            );
            self.publish_current_peer_connection(peer_id, conn.clone());
            return Some(conn);
        }

        // `get_connection_by_peer_id` is a pure lookup primitive: a
        // miss returns `None` and is *not* an error condition. It is
        // called from many hot paths (`send_to_peer_id*`,
        // `get_connection_to_peer`, control-plane refresh loops, gossip
        // FullSync responses, worker-stats delivery, network ingress
        // resolve, ...) and a miss is the normal case any time we are
        // asked about a peer we have never connected to or whose
        // connection has just dropped.
        //
        // The previous implementation logged a `warn!` pair on every
        // miss AND did an O(n) scan of `connections_by_peer` to build
        // the "Available node connections" list. On a stratum where one
        // configured backend was offline and gossip kept re-announcing
        // its interest entries, that fired at ~40 Hz × 2 lines = 80
        // log lines/sec and drowned every real warning in the system
        // (observed on stratum-devnet-a 2026-05-11).
        //
        // Diagnosing "peer X is down" belongs at the caller layer that
        // knows whether the absence is expected (refresh tick, gossip
        // sync) or actionable (a configured backend stayed down across
        // N retries). This primitive just returns `None`.
        None
    }

    /// Get a connection by socket address
    pub(crate) fn get_connection_by_addr(
        &self,
        addr: &SocketAddr,
    ) -> Option<Arc<LockFreeConnection>> {
        let conn = self.connections_by_addr.read_sync(addr, |_, v| v.clone())?;
        self.is_usable_connection(&conn).then_some(conn)
    }

    /// Get the peer ID for a given socket address
    pub fn get_peer_id_by_addr(&self, addr: &SocketAddr) -> Option<crate::PeerId> {
        self.addr_to_peer_id.read_sync(addr, |_, v| v.clone())
    }

    /// Reverse-lookup a configured peer id by its configured dial address.
    ///
    /// Unlike [`get_peer_id_by_addr`](Self::get_peer_id_by_addr) — which only
    /// sees addresses we have already connected to (`addr_to_peer_id`) — this
    /// consults the *configured* peer map (`peer_id_to_addr`). It lets the very
    /// first TLS dial to a configured peer pin the expected GossipNodeId in its SNI
    /// rather than falling back to an unauthenticated placeholder.
    pub(crate) fn configured_peer_id_for_addr(&self, addr: &SocketAddr) -> Option<crate::PeerId> {
        let mut found = None;
        self.peer_sessions.iter_sync(|peer_id, session| {
            if session.required_addr().as_ref() == Some(addr) {
                found = Some(peer_id.clone());
                return false;
            }
            true
        });
        found
    }

    /// Add an additional address mapping for a peer ID.
    /// Used when a peer connects from an ephemeral port that differs from their bind address.
    pub fn add_addr_to_peer_id(&self, addr: SocketAddr, peer_id: crate::PeerId) {
        debug!(
            "CONNECTION POOL: Adding additional address {} -> peer_id {}",
            addr, peer_id
        );
        let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id);
        // An address-indexed connection is not peer-routable until this
        // ownership mapping is visible. Publish another routing revision after
        // the mapping so a waiter that observed the earlier address-index
        // notification cannot park with an unresolved alias.
        self.mark_routing_changed();
    }

    /// Get the shared correlation tracker for a peer ID
    pub(crate) fn get_shared_correlation_tracker(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<CorrelationTracker>> {
        self.peer_sessions
            .read_sync(peer_id, |_, session| session.correlation.clone())
    }

    /// Get or create a correlation tracker for a peer
    pub(crate) fn get_or_create_correlation_tracker(
        &self,
        peer_id: &crate::PeerId,
    ) -> Arc<CorrelationTracker> {
        let tracker = self.get_or_create_peer_session(peer_id).correlation.clone();
        debug!(
            "CONNECTION POOL: Got correlation tracker for peer {}",
            peer_id
        );
        tracker
    }

    /// Add a connection indexed by peer ID
    pub fn add_connection_by_peer_id(
        &self,
        peer_id: crate::PeerId,
        addr: SocketAddr,
        mut connection: Arc<LockFreeConnection>,
    ) -> bool {
        // Only set correlation tracker if the connection doesn't already have one
        if connection.correlation.is_none() {
            // Get or create shared correlation tracker for this peer
            let correlation_tracker = self.get_or_create_correlation_tracker(&peer_id);

            // Set the correlation tracker on the connection
            // We need to make the connection mutable
            if let Some(conn_mut) = Arc::get_mut(&mut connection) {
                conn_mut.correlation = Some(correlation_tracker);
            } else {
                warn!(
                    "CONNECTION POOL: Cannot set correlation tracker - Arc has multiple references"
                );
            }
        } else {
            let _ = self.get_or_create_peer_session(&peer_id);
        }

        // Update the address mappings
        self.set_discovered_peer_addr(&peer_id, addr);
        let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());

        debug!(
            "CONNECTION POOL: Added connection for peer '{}' (address: {})",
            peer_id, addr
        );

        self.publish_current_peer_connection(&peer_id, connection.clone());

        let instance_id = connection.stream_handle.as_ref().map(|h| h.instance_id());

        // Also index by address for direct lookups
        let _ = self.connections_by_addr.upsert_sync(addr, connection);

        // Paired, insert-gated via `count_in_new_instance` — see its comment.
        // A connection with no stream handle is never counted (nothing to
        // mark it by).
        if let Some(instance_id) = instance_id {
            self.count_in_new_instance(instance_id);
        }
        true
    }

    /// Index an existing connection by an additional address.
    ///
    /// This is useful for incoming connections where the ephemeral TCP address
    /// differs from the peer's configured bind address. By indexing both addresses,
    /// response delivery can find the connection by the ephemeral address.
    pub fn index_connection_by_addr(&self, addr: SocketAddr, connection: Arc<LockFreeConnection>) {
        debug!(
            "CONNECTION POOL: Indexing connection by additional address {}",
            addr
        );
        let _ = self.connections_by_addr.upsert_sync(addr, connection);
        // Address-only connections are a route-visible fallback as soon as
        // their address index is installed. Wake consumers for this mutation
        // itself; `add_addr_to_peer_id` emits a second revision when the
        // authenticated owner alias is installed, so a waiter that observes
        // this intermediate state cannot acknowledge the completed pair.
        self.mark_routing_changed();
    }

    /// Send header + payload to a peer by ID without concatenating payload bytes.
    pub fn send_to_peer_id_parts(
        &self,
        peer_id: &crate::PeerId,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        debug!(
            "CONNECTION POOL: send_to_peer_id_parts called for peer '{}', pool has {} peer connections",
            peer_id,
            self.connections_by_peer.len()
        );
        if let Some(connection) = self.get_connection_by_peer_id(peer_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                return stream_handle.write_header_and_payload_nonblocking_checked(header, payload);
            } else {
                warn!(peer_id = %peer_id, "Connection found but no stream handle");
            }
        } else {
            // Caller already gets a `GossipError::Network(NotFound)`
            // below; logging here was redundant and turned every send
            // to an offline peer into a per-call warning (#root-cause).
            debug!(peer_id = %peer_id, "send: no connection for peer");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Connection not found for peer {}", peer_id),
        )))
    }

    /// Get or create a lock-free connection - NO MUTEX NEEDED
    pub fn get_lock_free_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        self.connections_by_addr.read_sync(&addr, |_, v| v.clone())
    }

    /// Send header + payload without copying the payload.
    pub fn send_lock_free_parts(
        &self,
        addr: SocketAddr,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                stream_handle.write_header_and_payload_nonblocking_checked(header, payload)?;
                return Ok(());
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found",
        )))
    }

    /// Remove a connection from the pool - lock-free operation
    pub fn remove_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        // First remove from address-based map
        if let Some((_, connection)) = self.connections_by_addr.remove_sync(&addr) {
            debug!(
                "CONNECTION POOL: Removed connection to {} - pool now has {} connections",
                addr,
                self.connections_by_addr.len()
            );

            let mut alias_addrs = Vec::new();
            self.connections_by_addr
                .iter_sync(|alias_addr, alias_conn| {
                    if Arc::ptr_eq(alias_conn, &connection) {
                        alias_addrs.push(*alias_addr);
                    }
                    true
                });

            let mut peer_ids = Vec::new();
            if let Some((_, node_id)) = self.addr_to_peer_id.remove_sync(&addr) {
                peer_ids.push(node_id);
            }
            for alias_addr in alias_addrs {
                let _ = self.connections_by_addr.remove_sync(&alias_addr);
                if let Some((_, node_id)) = self.addr_to_peer_id.remove_sync(&alias_addr)
                    && !peer_ids.contains(&node_id)
                {
                    peer_ids.push(node_id);
                }
                self.clear_capabilities_for_addr(&alias_addr);
            }

            for peer_id in peer_ids {
                self.clear_current_peer_connection_if_matches(&peer_id, &connection);
                debug!(
                    "CONNECTION POOL: Also removed connection by node ID '{}'",
                    peer_id
                );
            }

            self.release_counted_connection(&connection);
            self.clear_capabilities_for_addr(&addr);

            // H-004: Abort background tasks (writer, reader) to prevent resource leaks
            connection.abort_tasks();

            // `clear_current_peer_connection_if_matches` may have notified
            // before the alias sweep above. Publish once more after every
            // address/index projection is gone so consumers cannot park on a
            // partially torn-down route.
            self.mark_routing_changed();

            Some(connection)
        } else {
            None
        }
    }

    /// Instance id of the live transport session currently indexed for a peer.
    ///
    /// Consumers capture this *before* an ask so they can later request an
    /// instance-guarded eviction — see [`Self::note_peer_ask_streak_timeout`]
    /// and [`Self::note_peer_ask_hard_fault`].
    pub(crate) fn current_peer_connection_instance(&self, peer_id: &crate::PeerId) -> Option<u64> {
        self.get_connection_by_peer_id(peer_id).and_then(|conn| {
            conn.stream_handle
                .as_ref()
                .map(|handle| handle.instance_id())
        })
    }

    /// Evict the peer's cached session, but only if the session currently
    /// indexed is the same instance the caller's failing ask used. A `None`
    /// expectation evicts unconditionally (caller had no instance to pin).
    /// This is the instance guard that stops a timeout on an already-replaced
    /// session from tearing down the freshly reconnected, healthy session.
    ///
    /// The eviction itself is instance-scoped (`disconnect_connection_instance`),
    /// never `disconnect_connection_by_peer_id`: fetching the current
    /// connection to compare its instance id, then separately calling a
    /// peer-wide disconnect, would reopen exactly the check/act gap this
    /// guard exists to close — a fresh publish landing between the match and
    /// the peer-wide sweep would be collaterally destroyed. Re-validating by
    /// `Arc` identity via a single atomic compare-and-clear is the only way
    /// to make the guard itself race-free.
    fn evict_peer_session_if_instance(
        &self,
        peer_id: &crate::PeerId,
        expected_instance: Option<u64>,
    ) -> bool {
        match expected_instance {
            Some(expected) => {
                let Some(current) = self.get_connection_by_peer_id(peer_id) else {
                    return false;
                };
                let current_instance = current
                    .stream_handle
                    .as_ref()
                    .map(|handle| handle.instance_id());
                if current_instance != Some(expected) {
                    return false;
                }
                self.disconnect_connection_instance(peer_id, &current)
            }
            None => self.disconnect_connection_by_peer_id(peer_id).is_some(),
        }
    }

    /// Consumer-classified healthy ask outcome: reset the peer's streak.
    pub(crate) fn note_peer_ask_success(&self, peer_id: &crate::PeerId) {
        let _ = self
            .peer_sessions
            .read_sync(peer_id, |_, session| session.reset_ask_timeout_streak());
    }

    /// Consumer-classified streak-timeout: accrue toward the threshold and
    /// evict (instance-guarded) once it is reached. Returns whether a session
    /// was evicted. `threshold == 0` disables the mechanism.
    pub(crate) fn note_peer_ask_streak_timeout(
        &self,
        peer_id: &crate::PeerId,
        threshold: u8,
        expected_instance: Option<u64>,
    ) -> bool {
        if threshold == 0 {
            return false;
        }
        let session = self.get_or_create_peer_session(peer_id);
        let count = session.record_ask_timeout();
        if count < threshold {
            return false;
        }
        let evicted = self.evict_peer_session_if_instance(peer_id, expected_instance);
        if evicted {
            // Only clear the streak once we actually tore the session down. If
            // the instance guard blocked (the session was already replaced),
            // leave the count so the next timeout on the live session still
            // trips immediately.
            session.reset_ask_timeout_streak();
            warn!(
                target: "icanact_remote_lifecycle",
                peer_id = %peer_id,
                consecutive_timeouts = count,
                "ask_timeout_streak_evicting_peer_session"
            );
        }
        evicted
    }

    /// Consumer-classified hard transport fault: evict immediately
    /// (instance-guarded), bypassing the streak, and clear the counter.
    pub(crate) fn note_peer_ask_hard_fault(
        &self,
        peer_id: &crate::PeerId,
        expected_instance: Option<u64>,
    ) -> bool {
        let _ = self
            .peer_sessions
            .read_sync(peer_id, |_, session| session.reset_ask_timeout_streak());
        self.evict_peer_session_if_instance(peer_id, expected_instance)
    }

    /// Disconnect and remove a connection by peer ID
    pub fn disconnect_connection_by_peer_id(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        if let Some(connection) = self.peer_current_connection_snapshot(peer_id) {
            let stream_instance_id = connection
                .stream_handle
                .as_ref()
                .map(|handle| handle.instance_id());
            info!(
                peer_id = %peer_id,
                addr = %connection.addr,
                direction = ?connection.direction,
                stream_instance_id = ?stream_instance_id,
                reason = "disconnect_by_peer_id",
                "transport_session_removed"
            );
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::SessionRemoved {
                    peer: peer_id.clone(),
                    addr: connection.addr,
                    direction: match connection.direction {
                        ConnectionDirection::Inbound => {
                            crate::lifecycle::TransportDirection::Inbound
                        }
                        ConnectionDirection::Outbound => {
                            crate::lifecycle::TransportDirection::Outbound
                        }
                    },
                    reason: crate::lifecycle::SessionRemovalReason::DisconnectByPeerId,
                },
            );
            self.clear_current_peer_connection(peer_id);
            // Preserve the configured peer address so reconnect logic keeps a stable destination.

            // Remove every address alias for this peer. Do not rely only
            // on `addr_to_peer_id`: older/reindexed sessions may still be
            // reachable through `connections_by_addr` at the configured
            // bind address even if that alias row is missing.
            let mut addrs_to_remove: Vec<SocketAddr> = Vec::new();
            self.addr_to_peer_id.iter_sync(|addr, pid| {
                if pid == peer_id {
                    addrs_to_remove.push(*addr);
                }
                true
            });
            if !addrs_to_remove.contains(&connection.addr) {
                addrs_to_remove.push(connection.addr);
            }
            if let Some(configured_addr) = self.get_configured_peer_addr(peer_id)
                && !addrs_to_remove.contains(&configured_addr)
            {
                addrs_to_remove.push(configured_addr);
            }
            if let Some(required_addr) = self.get_required_peer_addr(peer_id)
                && !addrs_to_remove.contains(&required_addr)
            {
                addrs_to_remove.push(required_addr);
            }

            for addr in &addrs_to_remove {
                let _ = self.addr_to_peer_id.remove_sync(addr);
                let _ = self.connections_by_addr.remove_sync(addr);
                self.clear_capabilities_for_addr(addr);
            }

            self.release_counted_connection(&connection);

            // H-004: Abort background tasks (writer, reader) to prevent resource leaks
            connection.abort_tasks();
            // The primary-slot clear above can notify before these aliases
            // are removed. Publish the final revision only after the
            // connection is closed so route consumers cannot retain it.
            self.mark_routing_changed();

            Some(connection)
        } else {
            None
        }
    }

    /// Fully un-publish and tear down an outbound candidate that
    /// `resolve_connection_conflict` rejected in `finalize_new_outbound_connection`
    /// (`RejectIncoming` / `EvictStaleRejectIncoming`).
    ///
    /// The candidate is provisionally inserted into `connections_by_addr` /
    /// `addr_to_peer_id` *before* the tie-break decision is known (so
    /// `existing_before` can be snapshotted without racing the insert). When
    /// the decision comes back "reject", that provisional indexing must be
    /// fully undone by the exact `Arc` identity of the rejected candidate —
    /// never by a plain address removal, which could delete a different,
    /// legitimate connection a concurrent operation has since published at
    /// the same address. The rejected candidate was never published as
    /// anyone's current session (`publish_current_peer_connection` only runs
    /// on `AcceptIncoming`/`ReplaceExisting`), so there is no `peer_sessions`
    /// entry to clear here. This also deliberately never touches
    /// `connection_counter`: the counter increment for a finalized outbound
    /// happens later, only on the non-reject paths, so a rejected candidate
    /// has nothing to decrement.
    ///
    /// `existing_before` is the rival this candidate's caller snapshotted
    /// *before* the provisional upsert — i.e. whatever was legitimately
    /// indexed at `addr` immediately prior to that upsert, PROVIDED
    /// `existing_before` was actually indexed at THIS `addr` itself
    /// (`existing_before.addr == addr`). That last condition matters: the
    /// far more common reject shape is a rival that lives at some OTHER
    /// address entirely (this candidate dialed a different address than the
    /// rival's own), in which case the provisional upsert never displaced
    /// anything at `addr` and restoring `existing_before` here would
    /// incorrectly plant it at an address it was never indexed under. Only
    /// when the candidate's dial address is the exact SAME address the rival
    /// already owns (a concurrent preferred inbound at that address) did the
    /// provisional upsert genuinely displace it — and only then would
    /// clearing the slot on reject leave `connections_by_addr[addr]` /
    /// `addr_to_peer_id[addr]` empty even though the peer session still
    /// points at the live `existing_before`, letting address lookups and
    /// failure-canonicalization miss it and redial a duplicate. So when the
    /// slot still holds the candidate AND the rival was genuinely displaced
    /// from this exact address, restore its mapping instead of erasing it.
    /// Only ever restores a still-live rival: a dead/aborted
    /// `existing_before`, or one that never actually lived at `addr`, is
    /// discarded exactly as before (empty slot), never resurrected.
    fn unpublish_rejected_outbound_candidate(
        &self,
        addr: SocketAddr,
        candidate: &Arc<LockFreeConnection>,
        peer_id: &crate::PeerId,
        existing_before: Option<&Arc<LockFreeConnection>>,
    ) {
        let removed = self
            .connections_by_addr
            .remove_if_sync(&addr, |v| Arc::ptr_eq(v, candidate))
            .is_some();
        // `candidate` and a still-live `existing_before`
        // for the SAME peer share their `correlation` tracker BY POINTER
        // (both resolve through `get_or_create_correlation_tracker(peer_id)`
        // for this peer session) — restoring `existing` below does not stop
        // `candidate.abort_tasks()` from also cancelling every in-flight ask
        // on that live, restored connection. Only cancel the shared tracker
        // when `existing` does not depend on it.
        //
        // Compute this independently of `removed`: a concurrent publisher
        // can replace/reindex `addr` before the identity-scoped removal, so
        // the removal may lose even while the live sibling still shares the
        // session tracker. Basing this only on the successful-removal branch
        // would cancel that sibling's in-flight asks.
        let keep_correlation = existing_before.is_some_and(|existing| {
            existing.has_live_stream() && candidate.shares_correlation_tracker(existing)
        });
        if removed {
            match existing_before {
                Some(existing) if existing.addr == addr && existing.has_live_stream() => {
                    let _ = self.connections_by_addr.upsert_sync(addr, existing.clone());
                    let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
                }
                _ => {
                    let _ = self.addr_to_peer_id.remove_sync(&addr);
                    self.clear_capabilities_for_addr(&addr);
                    // The far more common reject shape: a rival that lives at
                    // some OTHER address entirely (see this function's doc),
                    // so there is no index row to restore here — but it may
                    // still be alive and share `candidate`'s SESSION-level
                    // correlation tracker (both resolve through
                    // `get_or_create_correlation_tracker(peer_id)`).
                    // Discarding this slot must not silently cancel that
                    // still-live sibling's in-flight asks.
                }
            }
        }
        // Abort the writer/reader tasks regardless of whether the address
        // removal above found this exact instance still indexed — the
        // candidate is being discarded either way and its tasks must not be
        // left running unaccounted for.
        if keep_correlation {
            candidate.abort_tasks_keep_correlation();
        } else {
            candidate.abort_tasks();
        }
        if removed {
            self.mark_routing_changed();
        }
    }

    /// Disconnect a specific connection instance for `peer_id`, but only if
    /// it is still the instance actually indexed for that peer — matched by
    /// `Arc` identity, never merely by `peer_id`. A concurrent publish that
    /// has replaced the indexed connection since the caller captured
    /// `target` (e.g. `handle_incoming_connection_tls` publishing a fresh
    /// preferred inbound between an outbound-finalize tie-break decision and
    /// this call) is left untouched.
    ///
    /// This is the instance-scoped counterpart to
    /// [`Self::disconnect_connection_by_peer_id`], which tears down
    /// "whatever is currently indexed" for the peer and must never be called
    /// from a decision computed earlier without re-validating the target —
    /// see the outbound-finalize `EvictStaleRejectIncoming`/`ReplaceExisting`
    /// call sites, which is exactly the gap that reproduced the tie-break
    /// reconnect thrash from the outbound side.
    ///
    /// `target` need not have ever been the peer's "current" session:
    /// `peer_current_connection_snapshot` (the pure decision-snapshot
    /// lookup `finalize_new_outbound_connection`'s `existing_before` and
    /// `handle_incoming_connection_tls`'s inbound-accept decision use) can
    /// find a rival purely via the configured-address or ephemeral-alias
    /// fallback without ever promoting it to "current" — unlike the old
    /// self-healing `get_connection_by_peer_id`, which used to publish such
    /// a fallback match as a side effect of being read. So this
    /// distinguishes two DIFFERENT reasons the primary-slot CAS can decline:
    /// a genuinely different connection installed there (a real concurrent
    /// supersession — must decline, untouched) versus the slot being
    /// already empty (`target` simply was never promoted to "current" in
    /// the first place — nothing is being protected by declining, so the
    /// instance-scoped teardown below may still safely proceed by `target`'s
    /// own identity).
    pub(crate) fn disconnect_connection_instance(
        &self,
        peer_id: &crate::PeerId,
        target: &Arc<LockFreeConnection>,
    ) -> bool {
        // Atomic compare-and-take on the peer's PRIMARY current-connection
        // slot, by `Arc` identity, via `PeerSession::compare_and_take_current_connection`
        // (a single lock-free CAS on the underlying `ArcSwapOption`). This
        // IS the entire re-validation: it either finds `target` still
        // installed and clears it right here, atomically, or it finds
        // something else — a concurrent publish (e.g. a fresh preferred
        // inbound) has already superseded `target` — and declines. There is
        // deliberately no separate check-then-act pair here: a read
        // followed by an unconditional clear has a gap in which exactly
        // that concurrent publish can land and be clobbered.
        let mut routing_changed = match self
            .peer_sessions
            .read_sync(peer_id, |_, session| {
                session.compare_and_take_current_connection(target)
            })
            .unwrap_or(Err(None))
        {
            Ok(()) => true,
            Err(Some(_other)) => {
                debug!(
                    peer_id = %peer_id,
                    "declined instance-scoped disconnect: the connection currently indexed for \
                     this peer is no longer the expected instance (superseded by a concurrent \
                     publish)"
                );
                return false;
            }
            Err(None) => {
                // The primary slot is genuinely empty — `target` was never
                // promoted to "current" (e.g. found only via
                // `peer_current_connection_snapshot`'s address/alias
                // fallback). No concurrent publish superseded it (that would
                // show up as `Some(other)` above), so there is nothing to
                // protect by declining: proceed with `target`'s own
                // instance-scoped teardown below.
                debug!(
                    peer_id = %peer_id,
                    addr = %target.addr,
                    "instance-scoped disconnect target was never the peer's \"current\" \
                     session (found only via an address/alias fallback); proceeding with its \
                     own instance-scoped teardown"
                );
                false
            }
        };

        let stream_instance_id = target
            .stream_handle
            .as_ref()
            .map(|handle| handle.instance_id());
        info!(
            peer_id = %peer_id,
            addr = %target.addr,
            direction = ?target.direction,
            stream_instance_id = ?stream_instance_id,
            reason = "disconnect_by_peer_id",
            "transport_session_removed"
        );
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::SessionRemoved {
                peer: peer_id.clone(),
                addr: target.addr,
                direction: match target.direction {
                    ConnectionDirection::Inbound => crate::lifecycle::TransportDirection::Inbound,
                    ConnectionDirection::Outbound => crate::lifecycle::TransportDirection::Outbound,
                },
                reason: crate::lifecycle::SessionRemovalReason::DisconnectByPeerId,
            },
        );

        // Mirror the clear onto the secondary `connections_by_peer` index —
        // again instance-scoped: only the entry that still points at
        // `target` is removed, via `remove_if_sync`'s single-bucket-lock
        // compare-and-remove, never a blanket peer-id-keyed removal that
        // could delete a newer instance already reinserted under the same
        // `peer_id`.
        routing_changed |= self
            .connections_by_peer
            .remove_if_sync(peer_id, |v| Arc::ptr_eq(v, target))
            .is_some();

        // Remove EVERY `connections_by_addr` alias of THIS instance. An
        // accepted inbound is commonly indexed under both its
        // advertised/bind address and its ephemeral socket address, and
        // both must go — leaving one behind is a zombie alias: a later
        // socket-failure cleanup that lands on it double-decrements
        // accounting, and direct lookups by that address observe a dead
        // connection. Each removal is its own atomic, per-key
        // compare-and-remove (`remove_if_sync` holds one bucket lock across
        // the identity check and the removal); a blanket peer-id-keyed
        // sweep (as `disconnect_connection_by_peer_id` does) is exactly
        // wrong here, since `target` and a brand-new candidate that has
        // already superseded it often share the same configured/dial
        // address, and a sweep keyed on peer identity would collaterally
        // delete the NEW connection's own, already-correct address entry.
        let mut candidate_addrs: Vec<SocketAddr> = Vec::new();
        self.connections_by_addr.iter_sync(|addr, v| {
            if Arc::ptr_eq(v, target) {
                candidate_addrs.push(*addr);
            }
            true
        });
        for addr in candidate_addrs {
            let removed = self
                .connections_by_addr
                .remove_if_sync(&addr, |v| Arc::ptr_eq(v, target))
                .is_some();
            if removed {
                routing_changed = true;
                let _ = self.addr_to_peer_id.remove_sync(&addr);
                self.clear_capabilities_for_addr(&addr);
            }
        }

        self.release_counted_connection(target);

        // H-004: Abort background tasks (writer, reader) to prevent resource leaks.
        target.abort_tasks();
        if routing_changed {
            self.mark_routing_changed();
        }
        true
    }

    /// Retire a specific, address-indexed connection INSTANCE identified by
    /// its stream-handle `instance_id`, without touching whatever is
    /// currently indexed for the peer.
    ///
    /// This exists for the socket-failure path
    /// (`GossipRegistry::handle_peer_connection_failure`): once a fresh
    /// connection has been reindexed under the same bind address as an older,
    /// now-superseded link, the caller cannot safely name the failed
    /// connection by address alone — a plain address relookup would return
    /// the NEW instance, not the one whose IO task actually exited. The
    /// caller instead threads through the `instance_id` captured directly
    /// from the failing stream handle, and this does a single atomic
    /// compare-and-remove keyed on that id: `connections_by_addr` is only
    /// cleared at `addr` if the entry found there is *still* the failed
    /// instance. If a newer connection has already taken that slot, the
    /// removal is declined and the newer connection is left completely
    /// untouched.
    pub(crate) fn remove_connection_instance_by_id(
        &self,
        addr: SocketAddr,
        instance_id: u64,
    ) -> Option<Arc<LockFreeConnection>> {
        let peer_id_at_addr = self.addr_to_peer_id.read_sync(&addr, |_, v| v.clone());

        let removed = self.connections_by_addr.remove_if_sync(&addr, |v| {
            v.stream_handle.as_ref().map(|handle| handle.instance_id()) == Some(instance_id)
        });
        let (_, connection) = removed?;

        let _ = self.addr_to_peer_id.remove_sync(&addr);
        self.clear_capabilities_for_addr(&addr);

        // The same instance may also be indexed under other aliases (e.g. an
        // inbound's ephemeral socket address alongside its bind address).
        // Sweep and remove every alias that still points at THIS Arc.
        let mut alias_addrs: Vec<SocketAddr> = Vec::new();
        self.connections_by_addr.iter_sync(|alias_addr, v| {
            if Arc::ptr_eq(v, &connection) {
                alias_addrs.push(*alias_addr);
            }
            true
        });
        for alias_addr in alias_addrs {
            let removed_alias = self
                .connections_by_addr
                .remove_if_sync(&alias_addr, |v| Arc::ptr_eq(v, &connection))
                .is_some();
            if removed_alias {
                let _ = self.addr_to_peer_id.remove_sync(&alias_addr);
                self.clear_capabilities_for_addr(&alias_addr);
            }
        }

        // This instance is, by the caller's contract, already known to be
        // superseded — never the peer session's current connection. Clear
        // the session's current-connection slot ONLY in the defensive case
        // where it still literally points at this exact Arc. This MUST be a
        // single atomic compare-and-clear (the same
        // `PeerSession::compare_and_clear_current_connection` primitive
        // `disconnect_connection_instance` uses), never the
        // check-then-unconditional-clear idiom `clear_current_peer_connection_if_matches`
        // used to perform here: that idiom reads current, ptr_eq-compares it
        // to `connection`, and only afterward (past a log line and a
        // lifecycle-event construction — a real gap) stores `None`
        // unconditionally. A fresh publish for this peer landing in that gap
        // was clobbered — a collateral-teardown/reconnect-thrash race
        // reopened through this one call site. The CAS
        // closes the gap completely: it either finds `connection` still
        // installed and atomically clears it, or finds something else — a
        // concurrent publish already superseded it — and leaves the slot
        // untouched. The `connections_by_peer` removal is likewise mirrored
        // from `disconnect_connection_instance`: a conditional,
        // identity-scoped `remove_if_sync`, performed ONLY when the CAS
        // actually cleared the slot, never an unconditional
        // peer-id-keyed removal that could delete a newer instance already
        // reinserted under the same `peer_id`.
        if let Some(peer_id) = peer_id_at_addr {
            let cleared = self
                .peer_sessions
                .read_sync(&peer_id, |_, session| {
                    session.compare_and_clear_current_connection(&connection)
                })
                .unwrap_or(false);
            if cleared {
                let stream_instance_id = connection
                    .stream_handle
                    .as_ref()
                    .map(|handle| handle.instance_id());
                info!(
                    peer_id = %peer_id,
                    addr = %connection.addr,
                    direction = ?connection.direction,
                    stream_instance_id = ?stream_instance_id,
                    reason = "current_connection_cleared",
                    "transport_session_removed"
                );
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::SessionRemoved {
                        peer: peer_id.clone(),
                        addr: connection.addr,
                        direction: match connection.direction {
                            ConnectionDirection::Inbound => {
                                crate::lifecycle::TransportDirection::Inbound
                            }
                            ConnectionDirection::Outbound => {
                                crate::lifecycle::TransportDirection::Outbound
                            }
                        },
                        reason: crate::lifecycle::SessionRemovalReason::CurrentConnectionCleared,
                    },
                );
                let _ = self
                    .connections_by_peer
                    .remove_if_sync(&peer_id, |v| Arc::ptr_eq(v, &connection));
            }
        }

        self.release_counted_connection(&connection);
        connection.abort_tasks();
        self.mark_routing_changed();
        Some(connection)
    }

    /// Peer-identity-aware variant of [`Self::remove_connection_instance_by_id`]
    /// for callers (ask-timeout recovery, ask-cancellation eviction) that
    /// already know which peer the failing instance belongs to, and so must
    /// not depend on `addr_to_peer_id[addr]` still holding that peer's alias
    /// to clear its current session.
    ///
    /// `remove_connection_instance_by_id` derives the peer id to clear
    /// `peer_sessions`/`connections_by_peer` for from `addr_to_peer_id[addr]`
    /// — read BEFORE the address-indexed removal even runs. If that alias
    /// row is missing or already stale (e.g. raced by an unrelated reindex,
    /// or simply never re-added after some earlier cleanup), current-session
    /// cleanup was silently skipped even when this instance genuinely was
    /// the peer's live session — leaving a dead current session published
    /// (`peer_sessions`/`connections_by_peer` still pointing at a
    /// disconnected/exited stream). Threading the caller's own `peer_id`
    /// closes that gap without reopening the address-vs-identity hole this
    /// whole file exists to close: it never trusts `peer_id` blindly to
    /// evict "whatever is currently indexed" — it only ever acts on the
    /// connection instance actually identified by `instance_id`.
    ///
    /// Resolution of the target `Arc` is itself alias-independent: it is
    /// whatever is indexed at `addr` if that still matches `instance_id`,
    /// OR — if `addr` no longer matches (e.g. the alias metadata is stale) —
    /// whatever is the peer's current session, but ONLY if THAT also matches
    /// `instance_id`. A concurrently reconnected FRESH session for the same
    /// peer is a different `Arc`/instance id and is therefore never matched,
    /// never touched, regardless of which branch resolves the target.
    ///
    /// Once resolved, teardown prefers the fully identity-scoped
    /// [`Self::disconnect_connection_instance`] (CAS-clears `peer_sessions`
    /// by `Arc` identity, mirrors into `connections_by_peer`, and sweeps
    /// every `connections_by_addr` alias by `Arc` identity — no
    /// `addr_to_peer_id` dependency at all). If the target was NOT (or is no
    /// longer) the peer's current session — e.g. already superseded before
    /// this call — that CAS declines harmlessly, and this falls back to the
    /// address-keyed [`Self::remove_connection_instance_by_id`] to retire
    /// the still address-indexed, non-current instance without touching the
    /// peer's actual current session.
    pub(crate) fn remove_connection_instance_for_peer(
        &self,
        peer_id: &crate::PeerId,
        addr: SocketAddr,
        instance_id: u64,
    ) -> Option<Arc<LockFreeConnection>> {
        let matches_instance = |conn: &Arc<LockFreeConnection>| {
            conn.stream_handle.as_ref().map(|h| h.instance_id()) == Some(instance_id)
        };

        let by_addr = self
            .connections_by_addr
            .read_sync(&addr, |_, v| v.clone())
            .filter(matches_instance);
        let target = match by_addr.or_else(|| {
            self.peer_sessions
                .read_sync(peer_id, |_, session| session.current_connection())
                .flatten()
                .filter(matches_instance)
        }) {
            Some(target) => target,
            None => {
                // `instance_id` is no longer reachable by `Arc` through
                // EITHER the addr index or the peer's current slot — it was
                // already displaced from both (e.g. a same-address reconnect
                // reindexed `addr` under a fresh instance and concurrently
                // published over the peer's current slot before this
                // ask-timeout/hard-fault eviction got here). Nothing above
                // can find it to run its own release, so its
                // `counted_instances` marker would otherwise be permanently
                // orphaned — a capacity leak. Release it directly by id,
                // mirroring the exact `release_displaced_connection_count`
                // idiom `retire_lost_cas_matched_instance` and the IO-exit
                // superseded-exit fallback already use for this shape. Safe
                // / idempotent: `release_counted_instance`'s
                // `counted_instances` removal only decrements if the marker
                // is still actually present, so this is a no-op if some
                // other path already released it, and never
                // double-decrements a live/current instance's own marker
                // (only `instance_id`'s own is ever touched here).
                self.release_displaced_connection_count(instance_id);
                return None;
            }
        };

        if self.disconnect_connection_instance(peer_id, &target) {
            return Some(target);
        }

        // `target` was not (or no longer) the current session for `peer_id`
        // — already superseded by the time we got here. It may still be
        // address-indexed (matched purely by `instance_id`, never merely by
        // `peer_id`); retire it there without touching whatever the peer's
        // actual current session now is.
        self.remove_connection_instance_by_id(addr, instance_id)
    }

    /// Release the `connection_counter` contribution of `instance_id`, an
    /// instance the caller has confirmed is superseded/displaced, in the one
    /// case `remove_connection_instance_by_id` itself cannot decrement for:
    /// when that call returns `None` because the instance is no longer the
    /// entry indexed at `addr` at all (e.g. a fresh reconnect already
    /// reindexed the same bind address before the failed instance's
    /// teardown could run).
    ///
    /// Routed through [`Self::release_counted_instance`] rather than an
    /// unconditional decrement: `instance_id` here is frequently ALSO
    /// reachable through a completely different teardown path racing this
    /// one (e.g. a `ReplaceExisting` tie-break already retired the very same
    /// instance via `disconnect_connection_instance` moments earlier, or a
    /// superseded IO-task exit is deciding this concurrently). The
    /// `counted_instances` table makes whichever caller notices first the
    /// one that actually decrements — a caller for an instance already
    /// released (or, for a rejected candidate, never counted at all) safely
    /// observes nothing to release.
    pub(crate) fn release_displaced_connection_count(&self, instance_id: u64) {
        self.release_counted_instance(instance_id);
    }

    /// Retire a FAILED connection instance identified directly by its own
    /// `Arc` (`current`) after `disconnect_connection_instance(peer_id,
    /// current)` has already declined (CAS loss): a fresh session for this
    /// peer was published between the caller's snapshot and that CAS, so
    /// `peer_sessions`/`connections_by_peer` already correctly point at the
    /// fresh winner and must NOT be touched here. `current` — the failed
    /// instance — was never retired by anything else, though: its address
    /// aliases and `connection_counter` contribution are still outstanding
    /// and must be released here, by `current`'s own identity, without going
    /// anywhere near the peer-keyed indices the fresh winner now occupies.
    ///
    /// First tries the address-keyed, single-alias-plus-sweep path
    /// (`remove_connection_instance_by_id`), which also decrements the
    /// counter when it succeeds. That call's own internal sweep can only run
    /// once it has found `current` still indexed at `current.addr` in the
    /// first place, though — if the fresh publish's own reindexing already
    /// displaced `current` from `current.addr` too (e.g. a same-bind-address
    /// takeover), that lookup finds nothing and returns `None` before ever
    /// reaching its alias sweep. In that case this falls back to an
    /// identity-only sweep of every `connections_by_addr` entry that still
    /// points at `current` by `Arc::ptr_eq`, then performs the compensating
    /// `release_displaced_connection_count(failed_instance_id)` release: if
    /// no other path has raced this one and already released `current`'s
    /// count, this is the one that does; if one has (e.g. a concurrent
    /// `ReplaceExisting` already retired the same instance by
    /// `disconnect_connection_instance`), this observes the count already
    /// released and is a safe no-op.
    ///
    /// Callers must invoke this exactly once per lost
    /// `disconnect_connection_instance` CAS — never when that CAS succeeded
    /// (it already released the counter and swept aliases itself).
    pub(crate) fn retire_lost_cas_matched_instance(
        &self,
        current: &Arc<LockFreeConnection>,
        failed_instance_id: u64,
    ) {
        if self
            .remove_connection_instance_by_id(current.addr, failed_instance_id)
            .is_some()
        {
            return;
        }

        let mut alias_addrs: Vec<SocketAddr> = Vec::new();
        self.connections_by_addr.iter_sync(|addr, v| {
            if Arc::ptr_eq(v, current) {
                alias_addrs.push(*addr);
            }
            true
        });
        let mut routing_changed = false;
        for addr in alias_addrs {
            let removed = self
                .connections_by_addr
                .remove_if_sync(&addr, |v| Arc::ptr_eq(v, current))
                .is_some();
            if removed {
                routing_changed = true;
                let _ = self.addr_to_peer_id.remove_sync(&addr);
                self.clear_capabilities_for_addr(&addr);
            }
        }
        self.release_displaced_connection_count(failed_instance_id);
        current.abort_tasks();
        if routing_changed {
            self.mark_routing_changed();
        }
    }

    /// Choose the least-recently-used connection eligible for eviction when
    /// the pool is at capacity.
    ///
    /// Connections belonging to a configured/required peer (one we hold a
    /// stable dial address for) are never selected: evicting a live cluster
    /// member to admit a new — often transient or discovered — dial would
    /// disconnect the cluster. When every connection is a configured peer this
    /// returns `None` and the soft pool cap is allowed to flex rather than
    /// dropping a required link.
    fn select_lru_eviction_victim(&self) -> Option<SocketAddr> {
        let mut oldest: Option<(SocketAddr, usize)> = None;
        self.connections_by_addr.iter_sync(|addr, conn| {
            if let Some(peer_id) = self.addr_to_peer_id.read_sync(addr, |_, pid| pid.clone())
                && self.is_required_peer(&peer_id)
            {
                // Required cluster peer — not an eviction candidate.
                return true;
            }
            let last_used = conn.last_used.load(Ordering::Acquire);
            match oldest {
                None => oldest = Some((*addr, last_used)),
                Some((_, best_last_used)) => {
                    if last_used < best_last_used {
                        oldest = Some((*addr, last_used));
                    }
                }
            }
            true
        });
        oldest.map(|(addr, _)| addr)
    }

    /// Decrement the connection counter by exactly one.
    ///
    /// Deliberately a plain, unconditional `fetch_sub` — NOT a `saturating`
    /// clamp at zero. Every decrement here is paired with exactly one prior
    /// or still-pending increment (see [`Self::count_in_new_instance`] /
    /// [`Self::release_counted_instance`]), but the increment and decrement
    /// of a given pair can observably land in either order under a
    /// concurrent teardown racing a fresh count-in for the same instance: a
    /// release that wins the race decrements before its counterpart's
    /// increment lands. At a baseline of zero that means this call must be
    /// able to drive the counter transiently negative — clamping that dip to
    /// zero (the previous behavior) silently swallows the paired decrement,
    /// so the counterpart's later increment leaks a permanent phantom count
    /// with no marker left to ever release it again. A plain `fetch_sub`
    /// nets the pair back to the correct total regardless of ordering,
    /// because addition and subtraction on `AtomicIsize` commute; the
    /// transient negative read is safe here because the only consumer of
    /// this counter (`add_lock_free_connection`'s `>= max_connections`
    /// admission check) only ever cares about the count being at or over
    /// capacity, never about it dipping below zero.
    fn decrement_connection_counter(&self) {
        self.connection_counter.fetch_sub(1, Ordering::AcqRel);
    }

    /// Claim `instance_id`'s ownership marker in `counted_instances`.
    /// Returns `true` only when this call is the one that newly created the
    /// entry (a plain `insert`, never an `upsert`) — the single
    /// linearization point every count-in site gates its
    /// `connection_counter` increment on, via [`Self::count_in_new_instance`].
    /// A caller that observes `false` (the entry already existed) must NEVER
    /// also increment: doing so would be the double-count this insert-gated
    /// design exists to prevent. In production every `instance_id` is
    /// generated fresh per stream handle, so this only ever returns `false`
    /// if this exact instance is (incorrectly) counted twice — a bug in the
    /// caller, not a race this function needs to tolerate silently.
    fn mark_instance_counted(&self, instance_id: u64) -> bool {
        self.counted_instances.insert_sync(instance_id, ()).is_ok()
    }

    /// Pair a newly-created instance's `connection_counter` contribution
    /// with its `counted_instances` ownership marker, race-free under any
    /// interleaving with a concurrent release.
    ///
    /// This is the ONLY way `connection_counter` is ever incremented in
    /// production (`add_connection_by_peer_id`, `finish_indexing_accepted_connection`,
    /// and `finalize_new_outbound_connection`'s non-reject paths all funnel
    /// through this): the marker insert is the linearization point, and the
    /// increment happens if-and-only-if this call's insert is the one that
    /// newly created it. `add_lock_free_connection` is the one exception —
    /// its increment is an eager admission-gate reservation taken before an
    /// `instance_id` even exists, so it cannot go through this helper; see
    /// its own comment for why that early reservation is still race-free.
    ///
    /// Fires [`crate::lifecycle::TransportLifecycleEvent::ConnectionCountMarkerAttempt`]
    /// immediately before the marker insert so tests can deterministically
    /// pin a concurrent release into the exact pairing point.
    ///
    /// Closes the review finding at its root: previously, sites bumped
    /// `connection_counter` first and inserted the marker afterward, so a
    /// concurrent teardown landing in that gap found no marker to release,
    /// and the marker inserted moments later over an already-evicted
    /// connection was orphaned — a permanent leak. With the marker as the
    /// gate, EVERY increment is paired with a `counted_instances` entry at
    /// the instant it happens: a release racing anywhere before or after
    /// this call either finds nothing yet (and correctly declines to
    /// decrement, since this call's `fetch_add` — which always follows a
    /// successful insert — has not landed yet either) or removes the entry
    /// this call just inserted and decrements to match. Either ordering
    /// nets to the same correct total, because `fetch_add`/`fetch_sub` are
    /// commutative and each is strictly gated on its own map mutation
    /// succeeding.
    fn count_in_new_instance(&self, instance_id: u64) {
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::ConnectionCountMarkerAttempt { instance_id },
        );
        if self.mark_instance_counted(instance_id) {
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::ConnectionCountIncrementAttempt {
                    instance_id,
                },
            );
            self.connection_counter.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Release `instance_id`'s `connection_counter` contribution exactly
    /// once, no matter which teardown path ends up calling this for a given
    /// instance, and no matter whether that instance is still reachable
    /// through any index (`connections_by_addr`, `connections_by_peer`,
    /// `peer_sessions`) at all.
    ///
    /// A single `remove_sync` on `counted_instances` is the entire
    /// mechanism: it is one atomic remove-and-test-existence per key, so of
    /// any number of concurrent or sequential callers passing the same
    /// `instance_id` — a normal teardown path that found-and-removed the
    /// instance from its index, a compensating release for one that was
    /// already displaced from its index by something else, and a
    /// superseded-exit fallback that raced either of those — EXACTLY ONE
    /// observes the entry present and performs the compensating
    /// `decrement_connection_counter`; every other caller, including one for
    /// an instance that was never counted in the first place (a rejected
    /// outbound candidate), observes it already gone and does nothing. This
    /// is what makes releases exactly-once and closes both failure classes
    /// at once: no leaked count (whichever teardown path notices a counted
    /// instance first releases it) and no underflow (an instance that was
    /// never counted, or whose count was already released by a different
    /// path, can never be decremented a second — or first — time).
    pub(crate) fn release_counted_instance(&self, instance_id: u64) {
        if self.counted_instances.remove_sync(&instance_id).is_some() {
            self.decrement_connection_counter();
        }
    }

    /// Convenience wrapper over [`Self::release_counted_instance`] for
    /// connection-Arc-shaped callers (most pool-internal teardown paths):
    /// releases the exact instance identified by `connection`'s own stream
    /// handle, or does nothing if `connection` never had one (never counted).
    fn release_counted_connection(&self, connection: &Arc<LockFreeConnection>) {
        if let Some(instance_id) = connection.stream_handle.as_ref().map(|h| h.instance_id()) {
            self.release_counted_instance(instance_id);
        }
    }

    /// Raw admission-gate counter (`add_lock_free_connection`'s
    /// `connection_count >= max_connections` check), exposed read-only for
    /// tests that must observe it staying balanced across
    /// publish/teardown/failover cycles — see
    /// `superseded_same_addr_failover_does_not_leak_connection_counter`.
    ///
    /// Clamped to zero at the read boundary: the underlying counter is
    /// signed (see its field doc) so a concurrent, still-in-flight
    /// increment/decrement pairing can be transiently negative, but every
    /// steady-state read a test cares about — after all racing operations in
    /// that test have completed — is never negative in a correct
    /// implementation, so this conversion never masks a real bug in
    /// steady-state assertions.
    #[cfg(test)]
    pub(crate) fn raw_connection_counter(&self) -> usize {
        self.connection_counter.load(Ordering::Acquire).max(0) as usize
    }

    /// Unclamped, signed view of the same counter as
    /// [`Self::raw_connection_counter`], for tests whose steady-state
    /// baseline is itself zero (or otherwise cannot be distinguished from an
    /// unfixed underflow by the clamped `usize` view above): a genuine
    /// double-decrement/undercount regression leaves the counter at a
    /// negative steady-state value, which `raw_connection_counter`'s
    /// `.max(0)` clamp would silently present as an indistinguishable `0`.
    /// Tests asserting an exact steady-state count — especially a baseline
    /// of `0` — must assert against this signed value instead, so that a
    /// negative regression fails loudly rather than reading as a correct
    /// zero balance.
    #[cfg(test)]
    pub(crate) fn raw_connection_counter_signed(&self) -> isize {
        self.connection_counter.load(Ordering::Acquire)
    }

    /// Get connection count - lock-free operation
    pub fn connection_count(&self) -> usize {
        let mut count = 0usize;
        self.peer_sessions.iter_sync(|_, session| {
            if session
                .current_connection()
                .map(|conn| conn.is_connected())
                .unwrap_or(false)
            {
                count += 1;
            }
            true
        });
        count
    }

    /// Get all connected peers - lock-free operation
    pub fn get_connected_peers(&self) -> Vec<SocketAddr> {
        let mut peers: Vec<SocketAddr> = Vec::new();
        self.connections_by_addr.iter_sync(|addr, conn| {
            if conn.is_connected() {
                peers.push(*addr);
            }
            true
        });
        peers
    }

    /// Get or create a persistent connection to a peer
    /// Fast path: Check for existing connection without creating new ones
    pub fn get_existing_connection(&self, addr: SocketAddr) -> Option<ConnectionHandle<T>> {
        let _current_time = current_timestamp();

        let conn = self
            .connections_by_addr
            .read_sync(&addr, |_, v| v.clone())?;
        if !conn.is_connected() {
            debug!(addr = %addr, "removing disconnected connection");
            if self.connections_by_addr.remove_sync(&addr).is_some() {
                self.mark_routing_changed();
            }
            return None;
        }

        conn.update_last_used();
        debug!(addr = %addr, "using existing persistent connection (fast path)");

        // Look up peer_id to get shared correlation tracker.
        self.make_connection_handle(addr, &conn)
    }

    /// Get or create a connection to a peer by its ID
    pub(crate) async fn get_connection_to_peer(
        &self,
        peer_id: &crate::PeerId,
    ) -> Result<ConnectionHandle<T>> {
        debug!(
            "CONNECTION POOL: get_connection_to_peer called for peer '{}'",
            peer_id
        );

        let addr = self.get_configured_peer_addr(peer_id).ok_or_else(|| {
            crate::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No address configured for peer '{}'", peer_id),
            ))
        })?;

        self.get_connection_to_peer_at(peer_id, addr).await
    }

    /// Resolves the peer's required/configured address and dials/reuses a
    /// connection to it, atomically -- both happen against the SAME lookup,
    /// with no `.await` between resolving `addr` and using it. The
    /// attempted address is always returned alongside the dial result,
    /// including on failure, so a caller never needs its own separate,
    /// independently re-resolved read of the same pool state to learn what
    /// was attempted: a concurrent repin between a caller's own pre-read
    /// and this call actually running used to leave the caller
    /// attributing the outcome to a since-superseded address. `None` only
    /// when no required/configured address exists for this peer at all --
    /// nothing was resolved, and nothing was attempted.
    pub(crate) async fn get_connection_to_required_peer(
        &self,
        peer_id: &crate::PeerId,
    ) -> (
        Option<crate::registry::AttemptedRoute>,
        Result<ConnectionHandle<T>>,
    ) {
        let Some(addr) = self
            .get_required_peer_addr(peer_id)
            .or_else(|| self.get_configured_peer_addr(peer_id))
        else {
            return (
                None,
                Err(crate::GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No required address configured for peer '{}'", peer_id),
                ))),
            );
        };

        let result = self.get_connection_to_peer_at(peer_id, addr).await;
        (Some(crate::registry::AttemptedRoute::new(addr)), result)
    }

    async fn get_connection_to_peer_at(
        &self,
        peer_id: &crate::PeerId,
        addr: SocketAddr,
    ) -> Result<ConnectionHandle<T>> {
        // First check if we already have any usable stream for this peer. This includes
        // inbound alias addresses, which are the preferred side for higher node IDs.
        if let Some(conn) = self.get_connection_by_peer_id(peer_id) {
            conn.update_last_used();
            let addr = conn.addr;
            if let Some(handle) = self.make_connection_handle(addr, &conn) {
                return Ok(handle);
            }
            return Err(crate::GossipError::Network(std::io::Error::other(
                "Connection exists but has no usable writer handle",
            )));
        }

        debug!(
            "CONNECTION POOL: Creating new connection to peer '{}' at {}",
            peer_id, addr
        );

        // Convert PeerId to GossipNodeId for TLS
        let node_id_for_tls = Some(peer_id.to_node_id());

        // Create the connection and store it by node ID
        // Pass the GossipNodeId so TLS can work even if gossip state doesn't have it yet
        let handle = self
            .get_connection_with_node_id(addr, node_id_for_tls)
            .await?;

        // After successful connection, ensure it's indexed by node ID
        if let Some(conn) = self.connections_by_addr.read_sync(&addr, |_, v| v.clone()) {
            self.publish_current_peer_connection(peer_id, conn);
            let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
            debug!(
                "CONNECTION POOL: Indexed new connection under peer ID '{}'",
                peer_id
            );
        }

        Ok(handle)
    }

    pub(crate) fn get_connected_connection_to_peer(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<ConnectionHandle<T>> {
        let conn = self.get_connection_by_peer_id(peer_id)?;
        let addr = conn.addr;
        conn.update_last_used();
        self.make_connection_handle(addr, &conn)
    }

    pub(crate) async fn get_connection(&self, addr: SocketAddr) -> Result<ConnectionHandle<T>> {
        self.get_connection_with_node_id(addr, None).await
    }

    pub(crate) async fn get_connection_with_node_id(
        &self,
        addr: SocketAddr,
        node_id: Option<crate::GossipNodeId>,
    ) -> Result<ConnectionHandle<T>> {
        let _current_time = current_timestamp();
        // Debug logging removed for performance - these logs were too verbose
        // debug!("CONNECTION POOL: get_connection called on pool at {:p} for {}", self as *const _, addr);
        // debug!("CONNECTION POOL: This pool instance has {} connections stored", self.connections_by_addr.len());

        // Extract what we need before any await points to avoid Send issues
        let max_connections = self.max_connections;
        let connection_timeout = self.connection_timeout;
        let registry_weak = self.registry.load_full();

        let mut resolved_node_id = node_id;
        loop {
            if let Some(node_id) = resolved_node_id.as_ref() {
                let peer_id = crate::PeerId::from(node_id);
                if let Some(conn) = self.get_connection_by_peer_id(&peer_id) {
                    conn.update_last_used();
                    let addr = conn.addr;
                    if let Some(handle) = self.make_connection_handle(addr, &conn) {
                        return Ok(handle);
                    }
                    return Err(crate::GossipError::Network(std::io::Error::other(
                        "Connection exists but has no usable writer handle",
                    )));
                }
            }

            if let Some(conn) = self.connections_by_addr.read_sync(&addr, |_, v| v.clone()) {
                if conn.is_connected() {
                    if let Some(stream_handle) = conn.stream_handle.as_ref() {
                        if stream_handle.exit_flag.load(Ordering::Acquire) {
                            debug!(addr = %addr, "found closed stream handle, removing stale connection");
                            conn.set_state(ConnectionState::Disconnected);
                            if self.connections_by_addr.remove_sync(&addr).is_some() {
                                self.mark_routing_changed();
                            }
                        } else {
                            conn.update_last_used();
                            debug!(addr = %addr, "found existing lock-free connection, reusing handle");

                            if let Some(handle) = self.make_connection_handle(addr, &conn) {
                                return Ok(handle);
                            }
                            return Err(crate::GossipError::Network(std::io::Error::other(
                                "Connection exists but has no usable writer handle",
                            )));
                        }
                    } else {
                        return Err(crate::GossipError::Network(std::io::Error::other(
                            "Connection exists but no stream handle",
                        )));
                    }
                } else {
                    debug!(addr = %addr, "removing disconnected connection");
                    if self.connections_by_addr.remove_sync(&addr).is_some() {
                        self.mark_routing_changed();
                    }
                }
            }

            if resolved_node_id.is_none() {
                resolved_node_id = if let Some(registry_arc) = registry_weak.upgrade() {
                    registry_arc.lookup_node_id(&addr).await.or_else(|| {
                        registry_arc
                            .peer_capability_addr_to_node
                            .read_sync(&addr, |_, v| *v)
                    })
                } else {
                    None
                };
            }

            match self.acquire_outbound_dial_gate(addr) {
                OutboundDialLease::Leader(gate) => {
                    let mut gate_completion =
                        OutboundDialGateCompletion::new(self, addr, gate.clone());
                    let retry_session = resolved_node_id.as_ref().map(|node_id| {
                        self.get_or_create_peer_session(&crate::PeerId::from(node_id))
                    });
                    let retry_attempt = retry_session
                        .as_ref()
                        .map(|session| session.outbound_dial_retry.try_claim_attempt());
                    if retry_attempt.as_ref().is_some_and(Option::is_none) {
                        if let Some(handle) = retry_session
                            .as_ref()
                            .and_then(|session| self.reuse_published_connection(session))
                        {
                            gate_completion.finish(true);
                            return Ok(handle);
                        }
                        // This caller did not attempt a socket, so release the
                        // address ownership gate without extending the peer's
                        // failure streak/deadline.
                        gate_completion.finish(true);
                        return Err(crate::GossipError::Network(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "outbound retry floor active",
                        )));
                    }
                    if let (Some(session), Some(attempt)) = (
                        retry_session.as_ref(),
                        retry_attempt.as_ref().copied().flatten(),
                    ) {
                        if let Some(handle) =
                            self.reuse_published_connection_after_retry_claim(session, attempt)
                        {
                            gate_completion.finish(true);
                            return Ok(handle);
                        }
                    }
                    let result = self
                        .connect_via_stream(
                            addr,
                            resolved_node_id,
                            max_connections,
                            connection_timeout,
                            registry_weak.clone(),
                        )
                        .await;
                    if let (Some(session), Some(attempt)) = (retry_session, retry_attempt.flatten())
                    {
                        match &result {
                            Ok(_) => session.outbound_dial_retry.record_success(attempt),
                            Err(crate::GossipError::Network(error))
                                if matches!(
                                    error.kind(),
                                    std::io::ErrorKind::WouldBlock
                                        | std::io::ErrorKind::InvalidInput
                                ) => session.outbound_dial_retry.record_neutral(attempt),
                            Err(crate::GossipError::Shutdown) => {
                                session.outbound_dial_retry.record_neutral(attempt)
                            }
                            Err(_) => session.outbound_dial_retry.record_failure(attempt),
                        }
                    }
                    gate_completion.finish(result.is_ok());
                    return result;
                }
                OutboundDialLease::Follower(gate) => {
                    gate.wait().await;
                }
            }
        }
    }

    async fn finalize_new_outbound_connection<S>(
        &self,
        addr: SocketAddr,
        stream: S,
        registry_weak: std::sync::Weak<GossipRegistry>,
        tofu_node_id: Option<crate::GossipNodeId>,
        // R-11: this specific outbound socket's own local ephemeral port --
        // unique per connection, unlike `addr` (the peer's fixed listening
        // port, shared by every connection we ever make to it). Threaded
        // into this connection's `ReadContext` so the receive path can tell
        // a redial's new connection apart from an old one still draining.
        local_session_addr: SocketAddr,
        // R-11: the identity this exact TLS handshake cryptographically
        // proved (derived from the live connection's own peer certificate
        // by the caller, before the stream was moved in here). `None` if
        // the handshake didn't yield one. Used to arm the sequence-reset
        // exemption, but ONLY once this candidate is confirmed below to be
        // the peer's live connection -- never for a candidate that loses
        // the tie-break, which would strand the exemption on a socket that
        // never becomes live.
        fresh_session_node_id: Option<crate::GossipNodeId>,
    ) -> Result<ConnectionHandle<T>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        // Determine peer ID (if known) before creating the stream handle.
        //
        // R2 (TLS identity binding): the identity the caller extracted from the
        // peer's signature-verified TLS certificate (`tofu_node_id`) is
        // cryptographic proof of who is actually on the wire, so it takes
        // precedence over the `addr -> peer` cache. That cache can be STALE after
        // a rekey/restart: if a different peer previously occupied this address,
        // an unpinned (placeholder-SNI) dial would otherwise bind the old
        // identity while the wire is cryptographically the new one, and the
        // per-message identity guard (which requires the frame's sender to equal
        // `embedded_peer_id`) would then black-hole every frame. The cached maps
        // are consulted only as a fallback for non-TLS paths that carry no cert
        // identity. The `addr -> peer` row is refreshed to the bound identity
        // below (see `addr_to_peer_id.upsert_sync`). Binding `embedded_peer_id`
        // is also what makes every subsequent per-message gossip frame on this
        // link cert-identity checked (the protocol guard requires
        // `embedded_peer_id.is_some()`); bootstrap dials previously left it
        // `None` and gossip on the link was never identity-checked.
        let peer_id_opt = tofu_node_id
            .as_ref()
            .map(crate::PeerId::from_public_key)
            .or_else(|| self.addr_to_peer_id.read_sync(&addr, |_, v| v.clone()))
            .or_else(|| {
                // Try reverse lookup: find peer ID that maps to this address.
                let mut found: Option<crate::PeerId> = None;
                self.peer_id_to_addr.iter_sync(|peer_id, peer_addr| {
                    if peer_addr == &addr {
                        found = Some(peer_id.clone());
                        return false;
                    }
                    true
                });
                found
            });

        let correlation_tracker = peer_id_opt
            .as_ref()
            .map(|peer_id| self.get_or_create_correlation_tracker(peer_id))
            .unwrap_or_else(CorrelationTracker::new);

        // Create lock-free connection for receiving
        let (buffer_config, schema_hash, read_context, response_writer) = registry_weak
            .upgrade()
            .map(|registry| {
                let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(addr));
                let read_context = ReadContext {
                    streaming_state_handoff: None,
                    registry_weak: Arc::downgrade(&registry),
                    peer_addr: addr,
                    session_source: local_session_addr,
                    peer_id: peer_id_opt.clone(),
                    max_message_size: registry.config.max_message_size,
                    expected_schema_hash: registry.config.schema_hash,
                    aligned_pool: registry.connection_pool.aligned_bytes_pool(),
                    inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
                    response_correlation: Some(correlation_tracker.clone()),
                    response_writer: Some(response_writer.clone()),
                    tell_handler_sync: registry.actor_tell_handler_sync.load_full(),
                    tell_handler_sync_context: registry.actor_tell_handler_sync_context.load_full(),
                    ask_immediate_handler_sync: registry
                        .actor_ask_immediate_handler_sync
                        .load_full(),
                    ask_handler_sync: registry.actor_ask_handler_sync.load_full(),
                    sync_actor_handler: registry.actor_message_handler_sync.load_full(),
                };
                (
                    BufferConfig::default().with_ask_window(registry.config.ask_window),
                    registry.config.schema_hash,
                    Some(read_context),
                    Some(response_writer),
                )
            })
            .unwrap_or((BufferConfig::default(), None, None, None));
        let (stream_handle, writer_task_handle, reader_task_handle) = LockFreeStreamHandle::new(
            stream,
            addr,
            ChannelId::Global,
            buffer_config,
            schema_hash,
            read_context,
        );
        let stream_handle = Arc::new(stream_handle);
        // Gate this handle's `write_routed_actor_ask` behind its own
        // identifying FullSync below (see `mark_identified`). Armed here,
        // before this handle is shared with anything else, so there is no
        // window in which a caller could observe the default "not gated"
        // state and enqueue a `RouteBind` ahead of the identify.
        stream_handle.begin_identify_gate();
        if let Some(response_writer) = response_writer.as_ref() {
            response_writer.bind_stream_handle(stream_handle.clone());
        }

        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        conn.remote_boot_id = registry_weak.upgrade().and_then(|registry| {
            registry
                .peer_capabilities
                .read_sync(&addr, |_, caps| caps.remote_boot_id)
        });
        // R-11: outbound session_source is this socket's own local
        // ephemeral port (unique per connection), not the dial target
        // `addr` this instance was constructed with (the peer's fixed
        // listening port, shared by every connection ever made to it) --
        // see `ReadContext::session_source` and `LockFreeConnection::session_source`.
        conn.session_source = local_session_addr;
        conn.stream_handle = Some(stream_handle.clone());
        conn.set_state(ConnectionState::Connected);
        conn.update_last_used();

        // Track the writer task handle (H-004).
        conn.task_tracker
            .set_writer(writer_task_handle.abort_handle());
        if let Some(reader_task_handle) = reader_task_handle {
            conn.task_tracker
                .set_reader(reader_task_handle.abort_handle());
        }

        // For outgoing connections, we might know the peer ID from configuration
        if let Some(peer_id) = peer_id_opt.clone() {
            // Use shared correlation tracker for this peer
            conn.correlation = Some(correlation_tracker.clone());
            conn.embedded_peer_id = Some(peer_id.clone());
            debug!(
                "CONNECTION POOL: Using shared correlation tracker for peer {:?} at {}",
                peer_id, addr
            );
        } else {
            // No peer ID yet, create a new correlation tracker
            // This will be replaced when we learn the peer ID from their FullSync message
            conn.correlation = Some(correlation_tracker.clone());
            debug!(
                "CONNECTION POOL: Created new correlation tracker for unknown peer at {}",
                addr
            );
        }

        let connection_arc = Arc::new(conn);

        // Snapshot any pre-existing rival for this peer BEFORE this candidate
        // is indexed by address/peer_id below, via the PURE
        // `peer_current_connection_snapshot` — never `get_connection_by_peer_id`.
        // Two independent reasons this must be a pure read:
        //
        // 1. (fixed earlier) `get_connection_by_peer_id`'s configured-address
        //    and alias fallbacks read straight out of `connections_by_addr` /
        //    `addr_to_peer_id`. Calling it *after* indexing this candidate
        //    risks it returning the brand new connection as its own
        //    "existing rival" — capturing the snapshot before the candidate
        //    is indexed closes that.
        // 2. `get_connection_by_peer_id` is ALSO not pure with respect to the
        //    candidate's own peer session: when the primary slot holds an
        //    unusable connection it self-heal-clears it as a side effect of
        //    being READ. A decision snapshot must never mutate — if a
        //    PREFERRED inbound is published for this peer in the internal
        //    check-then-clear gap of that self-heal, the unconditional clear
        //    erases the fresh session, `existing_before` comes back `None`,
        //    and the decision below proceeds as if there were no rival at
        //    all, recreating the exact collateral teardown this fixes. A
        //    pure snapshot can never trigger that clear in the first place,
        //    mutation or not.
        //
        // Capturing the rival here, while the candidate is still unindexed
        // and via a snapshot that cannot itself mutate anything, guarantees
        // the decision and any eviction below can only ever target the real
        // prior connection instance, never this new one and never a
        // concurrently published replacement destroyed by the snapshot
        // itself.
        let existing_before = peer_id_opt
            .as_ref()
            .and_then(|peer_id| self.peer_current_connection_snapshot(peer_id));

        if let Some(peer_id) = peer_id_opt.as_ref() {
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::OutboundFinalizeExistingSnapshotTaken {
                    peer: peer_id.clone(),
                    addr,
                },
            );
        }

        // Insert into lock-free map before spawning.
        let _ = self
            .connections_by_addr
            .upsert_sync(addr, connection_arc.clone());
        if let Some(peer_id) = peer_id_opt.as_ref() {
            let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
            // Identity-keyed publish gate. This freshly-dialed OUTBOUND must not
            // displace an existing live session the tie-break says to keep. A
            // higher-NodeId node that fell back to dialing (its preferred-inbound
            // wait timed out) must not overwrite a preferred inbound that arrived
            // concurrently: publishing the outbound here, then evicting it as the
            // wrong direction on the next tick, collaterally tore down the good
            // inbound — the single-node-restart reconnect thrash. The decision is
            // identity-derived for direction (`should_keep_connection`) and
            // monotonic by authenticated stream epoch within that direction,
            // never keyed on `addr`. If the existing preferred, newer session
            // wins, we leave the
            // outbound indexed by address only (its FullSync/handle still work)
            // without making it the session; it is retired by its own IO
            // lifecycle, and the next outbound tie-break tick reuses the
            // preferred session.
            let decision = match existing_before.as_ref() {
                None => ConnectionConflictDecision::AcceptIncoming,
                Some(existing) => registry_weak
                    .upgrade()
                    .map(|registry| {
                        let keep_existing = registry.should_keep_connection(
                            peer_id,
                            existing.direction == ConnectionDirection::Outbound,
                        );
                        let keep_incoming = registry.should_keep_connection(peer_id, true);
                        resolve_authenticated_connection_conflict(
                            existing,
                            &connection_arc,
                            keep_existing,
                            keep_incoming,
                            incoming_session_is_newer(&connection_arc, existing),
                        )
                    })
                    .unwrap_or(ConnectionConflictDecision::AcceptIncoming),
            };
            match decision {
                ConnectionConflictDecision::AcceptIncoming => {
                    // Compare-and-publish against the exact `existing_before`
                    // snapshot this decision was computed from — never an
                    // unconditional publish. A PREFERRED inbound published
                    // for this peer in the gap between that snapshot and
                    // this call must never be overwritten by this fallback
                    // outbound; see `publish_outbound_or_reresolve`.
                    //
                    // A `false` return means the compare-and-publish lost to
                    // a concurrently published rival AND the re-resolved,
                    // address-blind tie-break rejected our own candidate
                    // (`RejectIncoming`/`EvictStaleRejectIncoming`). That is
                    // the SAME reject outcome as the eager decision arms
                    // below, so it gets the IDENTICAL cleanup: fully
                    // unpublish the provisionally-indexed candidate (never
                    // bump `connection_counter`, never send FullSync) and
                    // propagate the rejection to the caller. Before this
                    // fix, a `false` here was silently ignored and execution
                    // fell through to the counter bump / FullSync / `Ok`
                    // below for the LOSING candidate — leaving it indexed in
                    // `connections_by_addr` where it could shadow the
                    // preferred rival in address lookups.
                    if !self.publish_outbound_or_reresolve(
                        peer_id,
                        &connection_arc,
                        existing_before.as_ref(),
                        &registry_weak,
                    ) {
                        self.unpublish_rejected_outbound_candidate(
                            addr,
                            &connection_arc,
                            peer_id,
                            existing_before.as_ref(),
                        );
                        return Err(crate::GossipError::ConnectionExists);
                    }
                }
                ConnectionConflictDecision::ReplaceExisting => {
                    // Evict the *specific* rival the decision above was
                    // computed about — instance-scoped, so a rival already
                    // superseded by a concurrent publish (e.g. a fresh
                    // preferred inbound) is left alone. The follow-up publish
                    // must NOT be unconditional, though: the tie-break
                    // decided THIS outbound beats `existing_before`, not that
                    // it beats whatever might have superseded it in the gap
                    // between the eviction attempt above and this publish —
                    // that is exactly the reviewer finding (a fresh session
                    // already published for this peer left the eviction a
                    // harmless no-op, but the old unconditional publish still
                    // clobbered it). Route through the SAME compare-and-
                    // publish + bounded re-resolve `AcceptIncoming` uses,
                    // with `expected` derived from the eviction's own outcome
                    // via `evict_before_replace`, and treat a re-resolved
                    // loss identically to the eager reject arms below: fully
                    // unpublish this candidate and surface the error.
                    let expected = existing_before
                        .as_ref()
                        .and_then(|existing| self.evict_before_replace(peer_id, existing));
                    if !self.publish_outbound_or_reresolve(
                        peer_id,
                        &connection_arc,
                        expected.as_ref(),
                        &registry_weak,
                    ) {
                        self.unpublish_rejected_outbound_candidate(
                            addr,
                            &connection_arc,
                            peer_id,
                            existing_before.as_ref(),
                        );
                        return Err(crate::GossipError::ConnectionExists);
                    }
                }
                ConnectionConflictDecision::RejectIncoming => {
                    debug!(
                        peer = %addr,
                        peer_id = %peer_id,
                        "outbound finalize kept existing preferred session; not displacing it"
                    );
                    // The candidate was provisionally indexed by address
                    // above, before this decision was known. It lost the
                    // tie-break: fully un-publish it so `connections_by_addr`
                    // / `addr_to_peer_id` keep pointing at the preferred
                    // existing session, never silently overwritten by the
                    // rejected candidate. Do not bump `connection_counter`,
                    // do not send FullSync, and do not hand this candidate
                    // back to the caller as a live handle.
                    self.unpublish_rejected_outbound_candidate(
                        addr,
                        &connection_arc,
                        peer_id,
                        existing_before.as_ref(),
                    );
                    return Err(crate::GossipError::ConnectionExists);
                }
                ConnectionConflictDecision::EvictStaleRejectIncoming => {
                    // The rival was dead, but this freshly-dialed outbound is
                    // *also* not the tie-break-preferred direction (e.g. the
                    // higher-NodeId side's fallback dial after a
                    // preferred-inbound-wait timeout). Clean up the stale
                    // entry so it does not linger, but do not publish this
                    // outbound as the session either — a preferred inbound
                    // may still arrive and must not be pre-empted.
                    //
                    // Instance-scoped: only ever tears down the exact rival
                    // instance `existing_before` captured above. If a
                    // concurrent inbound accept has already published a
                    // fresh preferred inbound for this peer between that
                    // capture and this call, `disconnect_connection_instance`
                    // is a no-op — a peer-wide
                    // `disconnect_connection_by_peer_id` here would have torn
                    // that fresh inbound down instead, reproducing the
                    // reconnect thrash from the outbound-finalize side.
                    if let Some(existing) = existing_before.as_ref() {
                        let _ = self.disconnect_connection_instance(peer_id, existing);
                    }
                    debug!(
                        peer = %addr,
                        peer_id = %peer_id,
                        "outbound finalize evicted a stale rival but declined to publish a \
                         non-preferred outbound as the session"
                    );
                    // As with `RejectIncoming`, this candidate lost the
                    // tie-break and must not be left indexed or served —
                    // only the stale RIVAL was evicted above; the candidate
                    // itself still needs its own provisional indexing undone.
                    // `existing_before` here is the stale rival itself (dead
                    // by construction of this decision arm), so
                    // `unpublish_rejected_outbound_candidate`'s liveness check
                    // declines to "restore" it and simply clears the slot —
                    // but a fresh preferred inbound published concurrently at
                    // this exact address, between the `existing_before`
                    // snapshot and this candidate's provisional upsert, is a
                    // distinct (live) connection this call cannot see; that
                    // narrower race is out of scope here and shares the
                    // pre-existing address-reindex repair path.
                    self.unpublish_rejected_outbound_candidate(
                        addr,
                        &connection_arc,
                        peer_id,
                        existing_before.as_ref(),
                    );
                    return Err(crate::GossipError::ConnectionExists);
                }
            }
        }
        // Count this connection exactly once, mirroring `add_connection_by_peer_id`,
        // paired and insert-gated via `count_in_new_instance` — see its
        // comment. Without this the outbound path published a live
        // connection that the teardown paths later decremented, underflowing
        // `connection_counter`.
        self.count_in_new_instance(stream_handle.instance_id());
        debug!(
            "CONNECTION POOL: Added connection via get_connection to {} - pool now has {} connections",
            addr,
            self.connections_by_addr.len()
        );
        // Another task can observe and tear down the connection immediately after publication,
        // so publication must not assume the address entry remains present beyond this point.
        debug!("CONNECTION POOL: Published connection for {}", addr);

        // From here on, this candidate is published and counted. Guard the
        // window until its identify gate is resolved -- see
        // `IdentifyGateGuard`'s own doc comment for why this is needed even
        // beyond the explicit `identify_send_failed` check below.
        let identify_gate_guard =
            IdentifyGateGuard::new(self, addr, connection_arc.clone(), peer_id_opt.clone());

        // R-11: arm the one-shot lower-sequence exemption for OUTBOUND
        // sessions too, not just inbound. Every early return above (a rival
        // won the tie-break, or a publish race) happens before this point,
        // so reaching here is the confirmation that THIS candidate WAS the
        // peer's live connection at publication time -- arming any earlier
        // could strand the exemption on a socket that never becomes live
        // while leaving the surviving connection's subsequent gossip
        // rejected (its source no longer matches this failed candidate).
        //
        // Publication and this arm are still two separate operations,
        // though: another task can supersede `connection_arc` between the
        // publish above and this `.await` completing. `connection_arc` is
        // passed through so `arm_sequence_reset_for_new_session` can
        // revalidate it is still the peer's current connection immediately
        // before mutating the registry, and decline to arm otherwise.
        if let (Some(registry_arc), Some(node_id)) =
            (registry_weak.upgrade(), fresh_session_node_id)
        {
            let arming_peer_id = crate::PeerId::from_public_key(&node_id);
            registry_arc
                .arm_sequence_reset_for_new_session(
                    addr,
                    node_id,
                    local_session_addr,
                    &arming_peer_id,
                    &connection_arc,
                )
                .await;
        }

        // Send initial FullSync message to identify ourselves. This MUST
        // stay after the arm above: the peer cannot possibly respond to a
        // frame we have not sent yet, so sequencing arm-then-identify (never
        // the reverse) is what guarantees the peer's own restart response
        // can never be processed by this connection's reader task before
        // the lower-sequence exemption exists (R-11). `mark_identified`
        // below is what actually unblocks a routed ask that raced the
        // connect and is parked in `wait_until_identified`, having
        // discovered this candidate via `connections_by_addr` right after
        // publication above -- it can only enqueue its `RouteBind` once
        // this send has already landed in the write queue.
        let mut identify_send_failed = false;
        match registry_weak.upgrade() {
            Some(registry_arc) => {
                let initial_msg = {
                    let (local_actors, known_actors) = registry_arc.snapshot_actor_pairs();
                    let gossip_state = registry_arc.gossip_state.lock().await;

                    RegistryMessage::FullSync {
                        local_actors,
                        known_actors,
                        sender_peer_id: registry_arc.peer_id.clone(),
                        sender_bind_addr: Some(registry_arc.advertised_addr().to_string()), // reachable advertised address (NAT-aware), not the raw bind
                        sequence: gossip_state.gossip_sequence,
                        wall_clock_time: crate::current_timestamp(),
                        extensions: None,
                    }
                };

                // Serialize and send the initial message without flattening header + payload.
                match rkyv::to_bytes::<rkyv::rancor::Error>(&initial_msg) {
                    Ok(data) => {
                        // Create a connection handle to send the message
                        let conn_handle: ConnectionHandle<T> = ConnectionHandle::new_stream(
                            addr,
                            connection_arc.direction,
                            stream_handle.clone(),
                            connection_arc
                                .correlation
                                .clone()
                                .unwrap_or_else(CorrelationTracker::new),
                        );
                        match conn_handle
                            .send_gossip_payload(bytes::Bytes::from_owner(data))
                            .await
                        {
                            Ok(()) => {
                                // The enqueue can succeed even though the IO
                                // task backing it had already exited (or exits
                                // an instant later) -- the frame was accepted
                                // into the queue, but nothing will ever flush
                                // it to the peer. Revalidate liveness right
                                // after the enqueue so that race is caught too,
                                // not just an outright enqueue failure.
                                if conn_handle.is_closed() {
                                    warn!(
                                        peer = %addr,
                                        "identify FullSync enqueued but this connection's IO task \
                                         had already exited; treating identify as failed"
                                    );
                                    identify_send_failed = true;
                                } else {
                                    stream_handle.mark_identified();
                                    info!(peer = %addr, "Sent initial FullSync message to identify ourselves");
                                }
                            }
                            Err(e) => {
                                warn!(peer = %addr, error = %e, "Failed to send initial FullSync message");
                                identify_send_failed = true;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(peer = %addr, error = %e, "Failed to serialize initial FullSync message");
                        identify_send_failed = true;
                    }
                }
            }
            None => {
                // The registry is gone (shutting down): this candidate can
                // never be a live session, so treat it exactly like a
                // failed send rather than silently leaving it published,
                // counted, and permanently un-identified.
                warn!(
                    peer = %addr,
                    "registry gone while sending identify FullSync; aborting this candidate"
                );
                identify_send_failed = true;
            }
        }

        if identify_send_failed {
            // Unlike a candidate that loses the outbound tie-break above,
            // this one WAS published (and counted): `connections_by_addr`,
            // `addr_to_peer_id`, the peer's session slot, and
            // `connection_counter` all need unwinding, not just its own IO
            // tasks. It can never actually identify itself, and left alone
            // it would suppress a redial forever with a dead "current"
            // connection nothing else can reap. `identify_gate_guard`,
            // still armed here, does exactly that unwinding on drop.
            return Err(crate::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("failed to send identifying FullSync to {addr}"),
            )));
        }

        // A clean, successful identify: nothing left for the guard to
        // unwind.
        identify_gate_guard.disarm();

        // Reset failure state for this peer since we successfully connected.
        if let Some(registry) = registry_weak.upgrade() {
            let registry_clone = registry.clone();
            let peer_addr = addr;
            tokio::spawn(async move {
                let mut gossip_state = registry_clone.gossip_state.lock().await;

                if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
                    let had_failures = peer_info.failures > 0;
                    peer_info.outbound_dial_success = true;
                    // We just independently proved this exact address is
                    // dialable (this function only ever runs after a
                    // fresh outbound dial completed, never for a reused
                    // inbound connection); a stale fallback attribution
                    // no longer applies.
                    peer_info.mark_dialability_confirmed();
                    if had_failures {
                        info!(peer = %peer_addr,
                                  prev_failures = peer_info.failures,
                                  "✅ Successfully established outgoing connection - resetting failure state");
                        peer_info.failures = 0;
                        peer_info.last_failure_time = None;
                        peer_info.last_failure_instant = None;
                    }
                    peer_info.last_success = crate::current_timestamp();
                }
            });
        }

        info!(peer = %addr, "successfully created new persistent connection");

        debug!(
            "CONNECTION POOL: After get_connection, pool has {} connections",
            self.connections_by_addr.len()
        );
        debug!(
            "CONNECTION POOL: Pool contains connection to {}? {}",
            addr,
            self.connections_by_addr.contains_sync(&addr)
        );

        Ok(ConnectionHandle::new_stream(
            addr,
            connection_arc.direction,
            stream_handle,
            connection_arc
                .correlation
                .clone()
                .unwrap_or_else(CorrelationTracker::new),
        ))
    }

    /// Mark a connection as disconnected
    pub fn mark_disconnected(&self, addr: SocketAddr) {
        if let Some(conn) = self.connections_by_addr.read_sync(&addr, |_, v| v.clone()) {
            conn.set_state(ConnectionState::Disconnected);
            info!(peer = %addr, "marked connection as disconnected");
        }
    }

    /// Check if we have a connection to a peer by address
    pub fn has_connection(&self, addr: &SocketAddr) -> bool {
        self.connections_by_addr
            .read_sync(addr, |_, v| self.is_usable_connection(v))
            .unwrap_or(false)
    }

    /// Check if we have a connection to a peer by peer ID
    pub fn has_connection_by_peer_id(&self, peer_id: &crate::PeerId) -> bool {
        self.peer_sessions
            .read_sync(peer_id, |_, session| {
                session
                    .current_connection()
                    .map(|conn| self.is_usable_connection(&conn))
                    .unwrap_or(false)
            })
            .unwrap_or(false)
            || self.aliased_connection_by_peer_id(peer_id).is_some()
    }

    pub fn cleanup_stale_connections(&self) {
        // Find disconnected peers and use peer-id-based removal to clean up all maps
        let mut stale_peer_ids: Vec<crate::PeerId> = Vec::new();
        self.peer_sessions.iter_sync(|peer_id, session| {
            if session
                .current_connection()
                .map(|conn| !self.is_usable_connection(&conn))
                .unwrap_or(false)
            {
                stale_peer_ids.push(peer_id.clone());
            }
            true
        });

        for peer_id in stale_peer_ids {
            if let Some(_conn) = self.disconnect_connection_by_peer_id(&peer_id) {
                debug!(peer_id = %peer_id, "cleaned up disconnected connection (all aliases)");
            }
        }

        self.prune_idle_peer_sessions();
    }

    /// Bound restart-churn state without racing a reconnect. `remove_if_sync`
    /// holds the map bucket across the final eligibility check and removal;
    /// a concurrent `get_or_create_peer_session` therefore either holds an
    /// extra Arc (making this predicate false) or observes the removal and
    /// creates a fresh session. The correlation Arc check keeps an in-flight
    /// ask's tracker routable even after its connection has gone away.
    fn prune_idle_peer_sessions(&self) {
        const SESSION_IDLE_TTL: Duration = Duration::from_secs(300);
        let mut candidates = Vec::new();
        self.peer_sessions.iter_sync(|peer_id, session| {
            if !session.is_required_peer()
                && session.current_connection().is_none()
                && session.idle_for(SESSION_IDLE_TTL)
            {
                candidates.push(peer_id.clone());
            }
            true
        });

        for peer_id in candidates {
            let removed = self.peer_sessions.remove_if_sync(&peer_id, |session| {
                !session.is_required_peer()
                    && session.current_connection().is_none()
                    && session.idle_for(SESSION_IDLE_TTL)
                    && Arc::strong_count(session) == 1
                    && Arc::strong_count(&session.correlation) == 1
            });
            if removed.is_some() {
                // `peer_id_to_addr` is an independent route authority and
                // may already be serving a freshly-created replacement
                // session. Do not couple its lifecycle to a best-effort
                // cache eviction from a separate map.
                debug!(peer_id = %peer_id, "pruned idle peer session");
            }
        }

        // ACTOR_REM_2 R7: also reclaim route entries whose address has been
        // taken over by a different identity, so the (session-independent)
        // route index stays bounded under identity churn.
        self.reconcile_route_index();
    }

    /// ACTOR_REM_2 R7: bound `peer_id_to_addr`. It is deliberately an
    /// independent route authority that outlives sessions (see
    /// `prune_idle_peer_sessions`), but a peer that restarts with a NEW identity
    /// at the same address leaves its old `peer_id -> addr` entry orphaned for
    /// the process lifetime — an unbounded leak under identity churn (e.g.
    /// Kubernetes pods restarting with fresh keys). Drop any entry whose address
    /// is now owned by a DIFFERENT peer_id in the bounded, address-keyed
    /// `addr_to_peer_id` index. Entries whose address is unclaimed (or still
    /// self-owned) are retained, preserving the route-authority invariant.
    fn reconcile_route_index(&self) {
        let mut entries: Vec<(crate::PeerId, SocketAddr)> = Vec::new();
        self.peer_id_to_addr.iter_sync(|peer_id, addr| {
            entries.push((peer_id.clone(), *addr));
            true
        });
        for (peer_id, addr) in entries {
            let superseded = self
                .addr_to_peer_id
                .read_sync(&addr, |_, owner| *owner != peer_id)
                .unwrap_or(false);
            if superseded {
                let _ = self
                    .peer_id_to_addr
                    .remove_if_sync(&peer_id, |cur| *cur == addr);
            }
        }
    }

    /// Close all connections (for shutdown)
    pub fn close_all_connections(&self) {
        // Use peer-id-based removal to properly clean up all address aliases
        // This avoids double-decrement of connection_counter when a connection
        // has both ephemeral and bind addresses after reindexing
        let peer_ids = self.session_peer_ids();
        let count = peer_ids.len();
        for peer_id in peer_ids {
            self.disconnect_connection_by_peer_id(&peer_id);
        }
        // Also remove any remaining connections that were not indexed by peer_id
        // (e.g., outbound connections established before peer_id mapping exists).
        let mut addrs: Vec<SocketAddr> = Vec::new();
        self.connections_by_addr.iter_sync(|addr, _| {
            addrs.push(*addr);
            true
        });
        let mut addr_count = 0usize;
        for addr in addrs {
            if self.remove_connection(addr).is_some() {
                addr_count += 1;
            }
        }
        info!(
            "closed all {} connections ({} by peer_id, {} by addr-only)",
            count + addr_count,
            count,
            addr_count
        );
    }
}

/// Resolve the peer state address for a sender.
async fn resolve_peer_state_addr(
    registry: &GossipRegistry,
    sender_peer_id: Option<&crate::PeerId>,
    socket_addr: SocketAddr,
) -> SocketAddr {
    if let Some(peer_id) = sender_peer_id {
        if let Some(addr) = {
            let pool = &registry.connection_pool;
            pool.peer_id_to_addr
                .read_sync(peer_id, |_, v| *v)
                .filter(|addr| addr.port() != 0)
        } {
            return addr;
        }

        if let Some(addr) = registry.lookup_advertised_addr(&peer_id.to_node_id()).await {
            return addr;
        }
    }

    if let Some(node_id) = registry
        .peer_capability_addr_to_node
        .read_sync(&socket_addr, |_, v| *v)
    {
        if let Some(addr) = registry.lookup_advertised_addr(&node_id).await {
            return addr;
        }
    }

    socket_addr
}

/// R16i: answer an inbound-only / NAT'd peer's clock probe inline on the
/// connection it arrived on. Such a peer is never dialed outbound, so the owed
/// echo would otherwise never be flushed by a scheduled gossip round. The
/// carrier is an empty-changes `DeltaGossipResponse`; the receiver processes
/// its `extensions` (the echo) exactly as it does for any delta response, and
/// the empty change set is a no-op for actor state.
async fn answer_inbound_clock_probe(
    registry: &Arc<GossipRegistry>,
    peer_id: &crate::PeerId,
    peer_addr: SocketAddr,
    extensions: crate::registry::GossipExtensionsV1,
) {
    let current_sequence = {
        let gossip_state = registry.gossip_state.lock().await;
        gossip_state.gossip_sequence
    };
    let response = RegistryMessage::DeltaGossipResponse {
        delta: crate::registry::RegistryDelta {
            since_sequence: current_sequence,
            current_sequence,
            changes: Vec::new(),
            sender_peer_id: registry.peer_id.clone(),
            wall_clock_time: crate::current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        },
        extensions: Some(extensions),
    };
    let response_data = match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
        Ok(data) => data,
        Err(e) => {
            warn!(error = %e, "R16i: failed to serialize inline clock-echo response");
            return;
        }
    };
    let payload = bytes::Bytes::from_owner(response_data);
    // Locally generated, not gated by any caller: reject here rather than
    // let `framing` panic (>= 2^27 bytes) or hand the peer a frame it will
    // hard-reject as MessageTooLarge, tearing the whole connection down.
    // Goes through the same helper every other inline-send gate uses so the
    // admission check and `write_gossip_frame_prefix`'s own
    // `GOSSIP_HEADER_LEN` overhead can't drift apart.
    if let Err(e) = framing::reject_oversize_for_inline_send(
        framing::GOSSIP_HEADER_LEN,
        payload.len(),
        registry.config.max_message_size,
    ) {
        debug!(peer = %peer_addr, error = %e, "inline clock-echo response too large to frame");
        return;
    }
    let header = match framing::try_write_gossip_frame_prefix(payload.len()) {
        Ok(header) => bytes::Bytes::copy_from_slice(&header),
        Err(e) => {
            debug!(peer = %peer_addr, error = %e, "clock-echo response too large to frame");
            return;
        }
    };
    let pool = &registry.connection_pool;
    let send_result = match pool.send_to_peer_id_parts(peer_id, header.clone(), payload.clone()) {
        Ok(()) => Ok(()),
        Err(_) => pool.send_lock_free_parts(peer_addr, header, payload),
    };
    if let Err(e) = send_result {
        debug!(peer = %peer_addr, error = %e, "R16i: could not answer inbound clock probe inline");
    }
}

/// Bind FullSync address-keyed state to evidence this process observed.
/// A peer-authenticated `sender_bind_addr` is still only a self-report, so a
/// rejected provisional advertisement falls back to the raw address of this
/// authenticated transport. The fallback is verified and session-scoped by
/// construction; an arbitrary advertised address never receives an exclusive
/// route merely because it appeared in a frame.
async fn claim_authenticated_gossip_addr(
    registry: &GossipRegistry,
    advertised_addr: Option<SocketAddr>,
    observed_addr: SocketAddr,
    peer_id: &crate::PeerId,
    session_source: SocketAddr,
) -> Option<(SocketAddr, crate::registry_owner::CommitSeq)> {
    if let Some(advertised_addr) = advertised_addr {
        let claim_kind = if advertised_addr == observed_addr {
            crate::addr_ownership::ClaimKind::Verified
        } else {
            crate::addr_ownership::ClaimKind::Provisional
        };
        let commit = registry
            .add_connection_scoped_peer_claim(
                advertised_addr,
                peer_id.to_node_id(),
                claim_kind,
                session_source,
            )
            .await;
        if let Some(receipt) = commit.1 {
            return Some((advertised_addr, receipt.generation()));
        }
        if advertised_addr == observed_addr {
            return None;
        }

        debug!(
            peer = %peer_id,
            advertised_addr = %advertised_addr,
            observed_addr = %observed_addr,
            "provisional gossip address was not admitted; binding frame to authenticated transport source"
        );
    }

    let (_, receipt) = registry
        .add_connection_scoped_peer_claim(
            observed_addr,
            peer_id.to_node_id(),
            crate::addr_ownership::ClaimKind::Verified,
            session_source,
        )
        .await;
    let receipt = receipt?;
    {
        let mut state = registry.gossip_state.lock().await;
        if let Some(peer) = state.peers.get_mut(&observed_addr) {
            peer.mark_transport_source_keyed_fallback(receipt.created_ownership());
        }
    }
    Some((observed_addr, receipt.generation()))
}

/// Handle an incoming message on a bidirectional connection
pub(crate) fn handle_incoming_message(
    registry: Arc<GossipRegistry>,
    _peer_addr: SocketAddr,
    // R-11: this connection's own session discriminator -- see
    // `ReadContext::session_source`. For inbound connections this equals
    // `_peer_addr`; for outbound connections it is this specific socket's
    // own local ephemeral port, not the (shared, non-unique) dial target.
    // Passed to `merge_full_sync_from` so the restart-sequence exemption is
    // scoped to the exact connection that armed it.
    session_source: SocketAddr,
    // Identity bound to this transport by the authenticated handshake.  Wire
    // payloads are not an authority for address ownership: FullSync claims
    // below are constructed from this value after checking that the claimed
    // sender agrees with it.
    authenticated_peer_id: Option<crate::PeerId>,
    msg: RegistryMessage,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async move {
        match msg {
            RegistryMessage::DeltaGossip { delta, extensions } => {
                debug!(
                    sender = %delta.sender_peer_id,
                    since_sequence = delta.since_sequence,
                    changes = delta.changes.len(),
                    "received delta gossip message on bidirectional connection"
                );

                // Unlike `FullSync` below, this arm didn't verify
                // `delta.sender_peer_id` -- a self-reported wire field --
                // against the connection's actual authenticated identity:
                // an authenticated peer could claim another identity and
                // get that victim's address's failure-bookkeeping reset,
                // manufacturing arbitrary liveness signal for any address
                // of its choosing. Fixed with the same equality check
                // `FullSync` performs, moved to the very top of this arm --
                // before anything at all keyed on the claimed identity.
                let Some(authenticated_sender_peer_id) = authenticated_peer_id.as_ref() else {
                    warn!(
                        tcp_source = %_peer_addr,
                        claimed_sender = %delta.sender_peer_id,
                        "Ignoring DeltaGossip without an authenticated transport identity"
                    );
                    return Ok(());
                };
                if authenticated_sender_peer_id != &delta.sender_peer_id {
                    warn!(
                        tcp_source = %_peer_addr,
                        authenticated_sender = %authenticated_sender_peer_id,
                        claimed_sender = %delta.sender_peer_id,
                        "Ignoring DeltaGossip whose claimed sender does not match the \
                         authenticated transport"
                    );
                    return Ok(());
                }

                let sender_socket_addr =
                    resolve_peer_state_addr(&registry, Some(&delta.sender_peer_id), _peer_addr)
                        .await;
                registry.record_inbound_gossip_extensions(
                    sender_socket_addr,
                    extensions,
                    crate::current_timestamp_nanos(),
                );

                // Captured alongside session validation below so
                // `apply_delta_from` can atomically recheck it under its
                // own lock, immediately before applying any change --
                // collecting `delta` for that call (and releasing the lock
                // acquired just below first) leaves a gap a newer session
                // can arm in.
                let mut captured_epoch: Option<u64> = None;

                // OPTIMIZATION: Do all peer management in one lock acquisition
                {
                    let mut gossip_state = registry.gossip_state.lock().await;

                    // Add the sender as a peer (inlined to avoid separate lock)
                    if delta.sender_peer_id != registry.peer_id {
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            gossip_state.peers.entry(sender_socket_addr)
                        {
                            let current_time = crate::current_timestamp();
                            let current_time_ms = crate::current_timestamp_millis();
                            e.insert(crate::registry::PeerInfo {
                                address: sender_socket_addr,
                                peer_address: None,
                                inbound_observed: true,
                                outbound_dial_success: false,
                                node_id: None,
                                dns_name: None,
                                failures: 0,
                                last_attempt: current_time,
                                last_success: current_time,
                                last_sequence: 0,
                                last_sent_sequence: 0,
                                consecutive_deltas: 0,
                                last_failure_time: None,
                                last_failure_instant: None,
                                last_dns_refresh_attempt: None,
                                last_response_received_ms: current_time_ms,
                                accept_lower_sequence_from: None,
                                current_session_source: None,
                                current_session_connection: None,
                                current_session_epoch: 0,
                                identity_verified: false,
                                transport_source_keyed: false,
                            });
                        }
                    }

                    // Update peer info and check if we need to clear pending failures.
                    // Gated the same way `merge_full_sync_from` gates FullSync:
                    // once a session is armed for this peer, only its own
                    // connection may advance `last_sequence` (or feed changes
                    // into `apply_delta_from` below) -- an old, still-draining
                    // connection's in-flight delta must not be able to restore
                    // a pre-restart high-water mark after the new session's
                    // reset, which would make the new session's own
                    // low-sequence syncs look stale again with the one-shot
                    // exemption already spent. This also covers ALL
                    // failure/health bookkeeping below (including the
                    // "previously failed peer" pending-failure clear, folded
                    // in here rather than checked unconditionally before
                    // this gate): none of it may be attributable to a
                    // connection that isn't the peer's current session.
                    let mut from_current_session = true;
                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        from_current_session = registry.peer_info_is_from_current_session(
                            &delta.sender_peer_id,
                            peer_info,
                            Some(session_source),
                        );
                        captured_epoch = Some(peer_info.current_session_epoch);
                        if !from_current_session {
                            // The post-lock guard below rejects stale sessions.
                        } else {
                            let was_failed =
                                peer_info.failures >= registry.config.max_peer_failures;
                            if was_failed {
                                info!(
                                    peer = %delta.sender_peer_id,
                                    "✅ Received delta from previously failed peer - connection restored!"
                                );
                            }
                            // Always reset failure state when we receive messages from the peer
                            // This proves the peer is alive and communicating
                            let had_failures = peer_info.failures > 0;
                            if had_failures {
                                info!(peer = %delta.sender_peer_id,
                              prev_failures = peer_info.failures,
                              "🔄 Resetting failure state after receiving DeltaGossip");
                                peer_info.failures = 0;
                                peer_info.last_failure_time = None;
                                peer_info.last_failure_instant = None;
                            }
                            peer_info.last_success = crate::current_timestamp();
                            // Inbound payload from peer — proves app-level liveness.
                            // The response-asymmetry detector in
                            // `apply_gossip_results` reads this field to decide
                            // whether outbound writes that returned `Ok(None)`
                            // were actually heard by the peer's application
                            // layer. Mirror the inline-response path in
                            // `GossipRegistry::handle_gossip_response`.
                            peer_info.last_response_received_ms = crate::current_timestamp_millis();

                            peer_info.last_sequence =
                                std::cmp::max(peer_info.last_sequence, delta.current_sequence);
                            peer_info.consecutive_deltas += 1;
                        }
                    }

                    if !from_current_session {
                        debug!(
                            peer = %sender_socket_addr,
                            "ignoring delta gossip from a connection that is not this \
                             peer's current authenticated session"
                        );
                        return Ok(());
                    }

                    gossip_state.delta_exchanges += 1;
                }

                // Apply the delta using the canonical registry logic (vector clocks +
                // deterministic tiebreakers). The previous "inline apply" fast-path had
                // multiple conflict-resolution implementations depending on lock contention,
                // which could cause nodes to diverge.
                //
                // `_peer_addr` is the verified socket address of the
                // connection this delta arrived on — the §1.6 trust anchor
                // for advertised-address repair (outranks configured/
                // discovered route state, which may be stale).
                let sender_peer_id = delta.sender_peer_id.clone();
                registry
                    .apply_delta_from(
                        delta,
                        Some(_peer_addr),
                        captured_epoch.map(|generation| (sender_socket_addr, generation)),
                        // The identity bound to THIS transport by the
                        // authenticated handshake, independent of whatever
                        // `delta.sender_peer_id` claims -- gates the
                        // TTL-liveness refresh on an unchanged
                        // reannouncement to owner-issued deltas only (see
                        // `apply_delta_from`'s doc). Everything else in
                        // this arm keeps using the payload's claimed
                        // `sender_peer_id`, matching prior behavior.
                        authenticated_peer_id.as_ref(),
                    )
                    .await?;

                // R16i: An inbound-only / NAT'd peer we never dial outbound will
                // never receive a scheduled gossip round from us, so a clock echo
                // owed from its probe (recorded above) would wait forever. Answer
                // it inline on the connection it arrived on. `take_clock_echo_*`
                // returns `None` for peers we do dial, so normal peers still flush
                // the echo on their next outbound round (no extra traffic).
                if let Some(extensions) = registry
                    .take_clock_echo_for_undialable_peer(
                        sender_socket_addr,
                        crate::current_timestamp_nanos(),
                    )
                    .await
                {
                    answer_inbound_clock_probe(
                        &registry,
                        &sender_peer_id,
                        sender_socket_addr,
                        extensions,
                    )
                    .await;
                }

                // Note: the actor-state response is sent during regular gossip rounds
                Ok(())
            }
            RegistryMessage::FullSync {
                local_actors,
                known_actors,
                sender_peer_id,
                sender_bind_addr,
                sequence,
                wall_clock_time,
                extensions,
            } => {
                let Some(authenticated_sender_peer_id) = authenticated_peer_id.as_ref() else {
                    warn!(
                        tcp_source = %_peer_addr,
                        claimed_sender = %sender_peer_id,
                        "Ignoring FullSync without an authenticated transport identity"
                    );
                    return Ok(());
                };
                if authenticated_sender_peer_id != &sender_peer_id {
                    warn!(
                        tcp_source = %_peer_addr,
                        authenticated_sender = %authenticated_sender_peer_id,
                        claimed_sender = %sender_peer_id,
                        "Ignoring FullSync whose claimed sender does not match the authenticated transport"
                    );
                    return Ok(());
                }

                // Use the peer's advertised listening address when it is dialable.
                // Remote loopback binds are local-only and must not be rewritten into
                // remote-ip:ephemeral-port peer entries.
                let advertised_sender_addr =
                    resolve_peer_addr_checked(sender_bind_addr.as_deref(), _peer_addr);
                if advertised_sender_addr.is_none() {
                    warn!(
                        tcp_source = %_peer_addr,
                        sender = %sender_peer_id,
                        sender_bind_addr = ?sender_bind_addr,
                        "Ignoring non-dialable FullSync bind hint; binding authenticated payload to transport source"
                    );
                }

                // Claim before ANY address-keyed mutation. A mismatched
                // advertised bind is only a self-report; if it cannot create
                // ownership, bind this frame to the authenticated transport's
                // observed source instead.
                let Some((sender_socket_addr, commit_seq)) = claim_authenticated_gossip_addr(
                    &registry,
                    advertised_sender_addr,
                    _peer_addr,
                    authenticated_sender_peer_id,
                    session_source,
                )
                .await
                else {
                    warn!(
                        tcp_source = %_peer_addr,
                        sender = %sender_peer_id,
                        claimed_addr = ?advertised_sender_addr,
                        "Rejecting FullSync address claim: ownership conflict"
                    );
                    return Ok(());
                };

                // Note: sender_peer_id is now a PeerId (e.g., "node_a"), not an address
                debug!(
                    "Received FullSync from node '{}' at bind_addr {} (tcp_source={})",
                    sender_peer_id, sender_socket_addr, _peer_addr
                );

                // OPTIMIZATION: Do all peer management in one lock acquisition
                {
                    let mut gossip_state = registry.gossip_state.lock().await;

                    // The claim was accepted, but this handler resumes long
                    // after the owner actor replied, and a newer claim can
                    // have taken the address in between. Every mutation keyed
                    // on `sender_socket_addr` below therefore runs in the same
                    // critical section as the check that authorizes it — the
                    // watermark lives inside `GossipState` precisely so the
                    // two cannot be split. Extension/clock state is recorded
                    // here, under the same guard, for the same reason: it is
                    // address-keyed and would otherwise be attributed to the
                    // losing claimant.
                    if !gossip_state.admit_ownership_projection(sender_socket_addr, commit_seq) {
                        drop(gossip_state);
                        debug!(
                            tcp_source = %_peer_addr,
                            sender = %sender_peer_id,
                            claimed_addr = %sender_socket_addr,
                            commit_seq,
                            "address ownership advanced past this FullSync claim; dropping it"
                        );
                        return Ok(());
                    }

                    registry.record_inbound_gossip_extensions(
                        sender_socket_addr,
                        extensions,
                        crate::current_timestamp_nanos(),
                    );

                    // If the resolved bind address differs from the TCP source address,
                    // migrate the PeerInfo from the ephemeral port entry to the bind
                    // address. `migrate_peer_entry` merges rather than overwrites when
                    // a bind-keyed entry already exists, so an already-established
                    // replay high-water mark / armed session there is never regressed.
                    if sender_socket_addr != _peer_addr && _peer_addr != registry.bind_addr {
                        if let Some(node_id) =
                            gossip_state.peers.get(&_peer_addr).and_then(|p| p.node_id)
                        {
                            info!(
                                old_addr = %_peer_addr,
                                new_addr = %sender_socket_addr,
                                node_id = ?node_id,
                                "🔄 Migrating peer info from ephemeral TCP source to bind address from FullSync"
                            );
                        }
                        gossip_state.migrate_peer_entry(_peer_addr, sender_socket_addr);
                    }

                    // Add the sender as a peer if not already present (inlined to avoid separate lock)
                    if sender_socket_addr != registry.bind_addr {
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            gossip_state.peers.entry(sender_socket_addr)
                        {
                            info!(peer = %sender_socket_addr, "Adding new peer from FullSync");
                            let current_time = crate::current_timestamp();
                            let current_time_ms = crate::current_timestamp_millis();
                            e.insert(crate::registry::PeerInfo {
                                address: sender_socket_addr,
                                peer_address: Some(_peer_addr), // Remember the actual connection address
                                inbound_observed: true,
                                outbound_dial_success: false,
                                node_id: None,
                                dns_name: None,
                                failures: 0,
                                last_attempt: current_time,
                                last_success: current_time,
                                last_sequence: 0,
                                last_sent_sequence: 0,
                                consecutive_deltas: 0,
                                last_failure_time: None,
                                last_failure_instant: None,
                                last_dns_refresh_attempt: None,
                                last_response_received_ms: current_time_ms,
                                accept_lower_sequence_from: None,
                                current_session_source: None,
                                current_session_connection: None,
                                current_session_epoch: 0,
                                identity_verified: false,
                                transport_source_keyed: false,
                            });
                        }
                    }

                    // Failure/health bookkeeping (failures, last_failure_time,
                    // last_success, last_response_received_ms,
                    // consecutive_deltas) is deliberately NOT reset here.
                    // Resetting it proves peer liveness, which must only be
                    // attributable to the CURRENT authenticated session --
                    // an old, still-draining connection's in-flight FullSync
                    // must not be able to mask real unresponsiveness or
                    // perturb `should_use_delta_state`'s strategy choice.
                    // Moved to after `merge_full_sync_from` below, gated on
                    // its return value (the same `from_current_session`
                    // verdict the actor/sequence state is already gated on).
                }

                debug!(
                    sender = %sender_peer_id,
                    sequence = sequence,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "📨 INCOMING: Received full sync message on bidirectional connection"
                );

                // Only remaining async operation. Peer bookkeeping keys on
                // the bind-derived address; address REPAIR anchors on the
                // verified TCP source (§1.6).
                let from_current_session = registry
                    .merge_full_sync_from_owned(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_peer_id.clone(),
                        sender_socket_addr,
                        Some(_peer_addr),
                        Some(session_source),
                        sequence,
                        wall_clock_time,
                        commit_seq,
                        Some(authenticated_sender_peer_id),
                    )
                    .await;

                if from_current_session {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    // Same guard, same check: peer/session bookkeeping is
                    // address-keyed and must not be applied on behalf of a
                    // claim the address has since moved past. Only the
                    // bookkeeping is skipped here; the response below is
                    // addressed to the sender's identity, not to the
                    // contested address, and still goes out.
                    let admitted =
                        gossip_state.admit_ownership_projection(sender_socket_addr, commit_seq);
                    if admitted
                        && registry.registry_owner.routes_to(&sender_socket_addr)
                            == Some(sender_peer_id.clone())
                    {
                        let pool = &registry.connection_pool;
                        let _ = pool
                            .peer_id_to_addr
                            .upsert_sync(sender_peer_id.clone(), sender_socket_addr);
                        if sender_socket_addr != _peer_addr {
                            pool.reindex_connection_addr(&sender_peer_id, sender_socket_addr);
                        }
                        debug!(
                            "BIDIRECTIONAL: Registered incoming connection - peer_id={} addr={}",
                            sender_peer_id, sender_socket_addr
                        );
                    }
                    if let Some(peer_info) = gossip_state
                        .peers
                        .get_mut(&sender_socket_addr)
                        .filter(|_| admitted)
                    {
                        let prev_failures = peer_info.failures;
                        if peer_info.failures > 0 {
                            info!(peer = %sender_socket_addr,
                              prev_failures = prev_failures,
                              "🔄 Resetting failure state after receiving FullSync");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                            peer_info.last_failure_instant = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        // Inbound payload from peer — proves app-level liveness.
                        // See `handle_incoming_message::DeltaGossip` for the
                        // full rationale.
                        peer_info.last_response_received_ms = crate::current_timestamp_millis();
                        peer_info.consecutive_deltas = 0;
                    }
                    if admitted {
                        gossip_state.full_sync_exchanges += 1;
                    }
                }

                // Send back our state as a response so the sender can receive our actors
                // This is critical for late-joining nodes (like Node C) to get existing state
                {
                    // Get our current state
                    let (our_local_actors, our_known_actors, our_sequence) = {
                        let (local_actors, known_actors) = registry.snapshot_actor_pairs();
                        let gossip_state = registry.gossip_state.lock().await;
                        (local_actors, known_actors, gossip_state.gossip_sequence)
                    };

                    // Calculate sizes before moving
                    let local_actors_count = our_local_actors.len();
                    let known_actors_count = our_known_actors.len();

                    // Create a FullSyncResponse message
                    let response = RegistryMessage::FullSyncResponse {
                        local_actors: our_local_actors,
                        known_actors: our_known_actors,
                        sender_peer_id: registry.peer_id.clone(), // Use peer ID
                        sender_bind_addr: Some(registry.advertised_addr().to_string()), // reachable advertised address (NAT-aware)
                        sequence: our_sequence,
                        wall_clock_time: crate::current_timestamp(),
                        extensions: registry
                            .gossip_extensions_for_outbound(
                                sender_socket_addr,
                                crate::current_timestamp_nanos(),
                            )
                            .await,
                    };

                    // Send the response back through existing connection
                    // We'll use send_lock_free which doesn't create new connections
                    let response_data = match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
                        Ok(data) => data,
                        Err(e) => {
                            warn!(error = %e, "Failed to serialize FullSync response");
                            return Ok(());
                        }
                    };

                    // Try to send immediately on existing connection
                    {
                        debug!(
                            "FULLSYNC RESPONSE: Node {} is about to acquire connection pool lock",
                            registry.bind_addr
                        );
                        let pool = &registry.connection_pool;
                        debug!(
                            "FULLSYNC RESPONSE: Node {} got pool lock, pool has {} total entries",
                            registry.bind_addr,
                            pool.connection_count()
                        );
                        debug!("FULLSYNC RESPONSE: Pool instance address: {:p}", &*pool);

                        // Log details about each connection.
                        pool.connections_by_addr.iter_sync(|addr, conn| {
                            debug!(
                                "FULLSYNC RESPONSE: Connection to {} - state={:?}",
                                addr,
                                conn.get_state()
                            );
                            true
                        });

                        let payload = bytes::Bytes::from_owner(response_data);
                        // Locally generated, not gated by any caller: reject
                        // here rather than let `framing` panic (>= 2^27
                        // bytes) or hand the peer a frame it will
                        // hard-reject as MessageTooLarge, tearing the whole
                        // connection down. Goes through the same helper every
                        // other inline-send gate uses so the admission check
                        // and `write_gossip_frame_prefix`'s own
                        // `GOSSIP_HEADER_LEN` overhead can't drift apart.
                        if let Err(e) = framing::reject_oversize_for_inline_send(
                            framing::GOSSIP_HEADER_LEN,
                            payload.len(),
                            registry.config.max_message_size,
                        ) {
                            warn!(error = %e, "FullSync response too large to frame");
                            return Ok(());
                        }
                        let header = bytes::Bytes::copy_from_slice(
                            &framing::try_write_gossip_frame_prefix(payload.len())?,
                        );

                        // Debug: Log what connections we have
                        let mut available_addrs: Vec<SocketAddr> = Vec::new();
                        pool.connections_by_addr.iter_sync(|addr, _| {
                            available_addrs.push(*addr);
                            true
                        });
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Available connections by addr: {:?}",
                            available_addrs
                        );
                        debug!("FULLSYNC RESPONSE DEBUG: Available node mappings: {:?}", {
                            let mut mappings: Vec<(crate::PeerId, SocketAddr)> = Vec::new();
                            pool.peer_id_to_addr.iter_sync(|peer_id, addr| {
                                mappings.push((peer_id.clone(), *addr));
                                true
                            });
                            mappings
                        });
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Looking for connection to sender_peer_id: {}",
                            sender_peer_id
                        );
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: sender_socket_addr={}",
                            sender_socket_addr
                        );

                        // Try to send using peer ID
                        let send_result = match pool.send_to_peer_id_parts(
                            &sender_peer_id,
                            header.clone(),
                            payload.clone(),
                        ) {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                warn!("Failed to send via peer ID {}: {}", sender_peer_id, e);
                                // Fall back to socket address, reusing the
                                // already-validated header (same payload).
                                pool.send_lock_free_parts(sender_socket_addr, header, payload)
                            }
                        };

                        if send_result.is_err() {
                            warn!(
                                "Primary send failed for peer {}, no fallback available",
                                sender_peer_id
                            );
                        }

                        match send_result {
                            Ok(()) => {
                                debug!(peer = %sender_socket_addr,
                                  peer_id = %sender_peer_id,
                                  local_actors = local_actors_count,
                                  known_actors = known_actors_count,
                                  bind_addr = %registry.bind_addr,
                                  "📤 RESPONSE: Successfully sent FullSync response with our state");
                            }
                            Err(e) => {
                                // If we can't send immediately, queue it for the next gossip round
                                warn!(peer = %sender_socket_addr,
                                  peer_id = %sender_peer_id,
                                  error = %e,
                                  "Could not send FullSync response immediately - will be sent in next gossip round");

                                // Store in gossip state to be sent during next gossip round.
                                // Gated on `from_current_session`: `sender_socket_addr` is a
                                // bind address, not a connection instance, so a stale/draining
                                // connection's inbound traffic resolves to the SAME PeerInfo
                                // entry the current session owns. Without this gate, a
                                // superseded connection could force the legitimate session's
                                // gossip strategy into full-sync-every-round by repeatedly
                                // failing this send path.
                                if from_current_session {
                                    let mut gossip_state = registry.gossip_state.lock().await;

                                    // Mark that we need to send a full sync to this peer
                                    if let Some(peer_info) =
                                        gossip_state.peers.get_mut(&sender_socket_addr)
                                    {
                                        // Force a full sync on the next gossip round
                                        peer_info.consecutive_deltas =
                                            registry.config.max_delta_history as u64;
                                        info!(peer = %sender_socket_addr,
                                          "Marked peer for full sync in next gossip round");
                                    }
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
            RegistryMessage::FullSyncRequest {
                sender_peer_id,
                sender_bind_addr: _, // Not used for requests, but must be present
                sequence: _,
                wall_clock_time: _,
            } => {
                debug!(
                    sender = %sender_peer_id,
                    "received full sync request on bidirectional connection"
                );

                {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    gossip_state.full_sync_exchanges += 1;
                }

                // Note: Response will be sent during regular gossip rounds
                Ok(())
            }
            // Handle response messages (these can arrive on incoming connections too)
            RegistryMessage::DeltaGossipResponse { delta, extensions } => {
                debug!(
                    sender = %delta.sender_peer_id,
                    changes = delta.changes.len(),
                    "received delta gossip response on bidirectional connection"
                );
                let sender_socket_addr =
                    resolve_peer_state_addr(&registry, Some(&delta.sender_peer_id), _peer_addr)
                        .await;
                registry.record_inbound_gossip_extensions(
                    sender_socket_addr,
                    extensions,
                    crate::current_timestamp_nanos(),
                );

                // Gated the same way the DeltaGossip branch above is: once a
                // session is armed for this peer, only its own connection's
                // deltas may be applied. The captured generation is
                // rechecked atomically inside `apply_delta_from`, under its
                // own lock, immediately before applying any change -- this
                // lock is released before that call, leaving a gap a newer
                // session can arm in.
                let (from_current_session, captured_epoch) = {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        let allowed = registry.peer_info_is_from_current_session(
                            &delta.sender_peer_id,
                            peer_info,
                            Some(session_source),
                        );
                        (allowed, Some(peer_info.current_session_epoch))
                    } else {
                        (true, None)
                    }
                };
                if !from_current_session {
                    debug!(
                        peer = %sender_socket_addr,
                        "ignoring delta gossip response from a connection that is not \
                         this peer's current authenticated session"
                    );
                    return Ok(());
                }

                // Same §1.6 trust anchor as the DeltaGossip branch above:
                // responses also carry actor additions, and repair must use
                // the verified socket address of this connection.
                if let Err(err) = registry
                    .apply_delta_from(
                        delta,
                        Some(_peer_addr),
                        captured_epoch.map(|generation| (sender_socket_addr, generation)),
                        authenticated_peer_id.as_ref(),
                    )
                    .await
                {
                    warn!(error = %err, "failed to apply delta from response");
                } else {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    gossip_state.delta_exchanges += 1;
                }
                Ok(())
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_peer_id,
                sender_bind_addr,
                sequence,
                wall_clock_time,
                extensions,
            } => {
                let Some(authenticated_sender_peer_id) = authenticated_peer_id.as_ref() else {
                    warn!(
                        tcp_source = %_peer_addr,
                        claimed_sender = %sender_peer_id,
                        "Ignoring FullSyncResponse without an authenticated transport identity"
                    );
                    return Ok(());
                };
                if authenticated_sender_peer_id != &sender_peer_id {
                    warn!(
                        tcp_source = %_peer_addr,
                        authenticated_sender = %authenticated_sender_peer_id,
                        claimed_sender = %sender_peer_id,
                        "Ignoring FullSyncResponse whose claimed sender does not match the authenticated transport"
                    );
                    return Ok(());
                }

                let advertised_sender_addr =
                    resolve_peer_addr_checked(sender_bind_addr.as_deref(), _peer_addr);
                if advertised_sender_addr.is_none() {
                    warn!(
                        tcp_source = %_peer_addr,
                        sender = %sender_peer_id,
                        sender_bind_addr = ?sender_bind_addr,
                        "Ignoring non-dialable FullSyncResponse bind hint; binding authenticated payload to transport source"
                    );
                }

                let Some((sender_socket_addr, commit_seq)) = claim_authenticated_gossip_addr(
                    &registry,
                    advertised_sender_addr,
                    _peer_addr,
                    authenticated_sender_peer_id,
                    session_source,
                )
                .await
                else {
                    warn!(
                        tcp_source = %_peer_addr,
                        sender = %sender_peer_id,
                        claimed_addr = ?advertised_sender_addr,
                        "Rejecting FullSyncResponse address claim: ownership conflict"
                    );
                    return Ok(());
                };

                {
                    // Extension/clock state is address-keyed, so it is
                    // admitted and written under the same `gossip_state`
                    // guard, exactly like the FullSync arm: a claim the
                    // address has already moved past must record nothing.
                    let mut gossip_state = registry.gossip_state.lock().await;
                    if !gossip_state.admit_ownership_projection(sender_socket_addr, commit_seq) {
                        drop(gossip_state);
                        debug!(
                            tcp_source = %_peer_addr,
                            sender = %sender_peer_id,
                            claimed_addr = %sender_socket_addr,
                            commit_seq,
                            "address ownership advanced past this FullSyncResponse claim; \
                             dropping it"
                        );
                        return Ok(());
                    }
                    registry.record_inbound_gossip_extensions(
                        sender_socket_addr,
                        extensions,
                        crate::current_timestamp_nanos(),
                    );
                }

                debug!(
                    sender = %sender_peer_id,
                    bind_addr = %sender_socket_addr,
                    tcp_source = %_peer_addr,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "RECEIVED: FullSyncResponse from peer (using bind_addr)"
                );

                let from_current_session = registry
                    .merge_full_sync_from_owned(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_peer_id.clone(),
                        sender_socket_addr,
                        Some(_peer_addr),
                        Some(session_source),
                        sequence,
                        wall_clock_time,
                        commit_seq,
                        Some(authenticated_sender_peer_id),
                    )
                    .await;

                // Reset failure state when receiving response
                let mut gossip_state = registry.gossip_state.lock().await;

                // Same guard, same check: the peer-entry migration and the
                // failure/health bookkeeping below are both keyed on the
                // claimed address and must not be applied on behalf of a
                // claim that has since been displaced.
                if !gossip_state.admit_ownership_projection(sender_socket_addr, commit_seq) {
                    drop(gossip_state);
                    debug!(
                        sender = %sender_peer_id,
                        claimed_addr = %sender_socket_addr,
                        commit_seq,
                        "address ownership advanced past this FullSyncResponse claim; \
                         skipping peer-state update"
                    );
                    return Ok(());
                }

                if registry.registry_owner.routes_to(&sender_socket_addr)
                    == Some(sender_peer_id.clone())
                {
                    let pool = &registry.connection_pool;
                    let _ = pool
                        .peer_id_to_addr
                        .upsert_sync(sender_peer_id.clone(), sender_socket_addr);
                    if sender_socket_addr != _peer_addr {
                        pool.reindex_connection_addr(&sender_peer_id, sender_socket_addr);
                    }
                    debug!(
                        "BIDIRECTIONAL: Updated connection mapping from FullSyncResponse - peer_id={} addr={}",
                        sender_peer_id, sender_socket_addr
                    );
                }

                // If the resolved bind address differs from the TCP source address,
                // migrate the PeerInfo from the ephemeral port entry to the bind
                // address. `migrate_peer_entry` merges rather than overwrites when
                // a bind-keyed entry already exists, so an already-established
                // replay high-water mark / armed session there is never regressed.
                if sender_socket_addr != _peer_addr && _peer_addr != registry.bind_addr {
                    if let Some(node_id) =
                        gossip_state.peers.get(&_peer_addr).and_then(|p| p.node_id)
                    {
                        info!(
                            old_addr = %_peer_addr,
                            new_addr = %sender_socket_addr,
                            node_id = ?node_id,
                            "🔄 Migrating peer info from ephemeral TCP source to bind address from FullSyncResponse"
                        );
                    }
                    gossip_state.migrate_peer_entry(_peer_addr, sender_socket_addr);
                }

                // Failure/health bookkeeping must only be attributable to
                // the CURRENT authenticated session -- see the FullSync arm
                // above for the full rationale. Gated on the same
                // `from_current_session` verdict `merge_full_sync_from`
                // already computed for the actor/sequence state.
                if from_current_session {
                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        if peer_info.failures > 0 {
                            info!(peer = %sender_socket_addr,
                                prev_failures = peer_info.failures,
                                "resetting failure state after receiving FullSyncResponse");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                            peer_info.last_failure_instant = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        // Inbound payload from peer — proves app-level liveness.
                        peer_info.last_response_received_ms = crate::current_timestamp_millis();
                    }
                }

                if from_current_session {
                    gossip_state.full_sync_exchanges += 1;
                }
                Ok(())
            }
            RegistryMessage::PeerListGossip {
                peers,
                timestamp,
                // The wire `sender_addr` is an unauthenticated, sender-chosen
                // string. Never use it for attribution — bind everything to the
                // authenticated connection address (`peer_state_addr`) instead so
                // logs/merge cannot be misattributed to another peer.
                sender_addr: _wire_sender_addr,
            } => {
                let peer_state_addr = resolve_peer_state_addr(&registry, None, _peer_addr).await;
                let authenticated_sender = peer_state_addr.to_string();
                debug!(
                    peer_count = peers.len(),
                    timestamp = timestamp,
                    sender = %authenticated_sender,
                    "received peer list gossip message"
                );

                // Accept peer list only from connected peers
                if !registry.has_active_connection(&peer_state_addr).await {
                    debug!(
                        peer = %peer_state_addr,
                        "ignoring peer list gossip from non-connected peer"
                    );
                    return Ok(());
                }

                if !registry.peer_supports_peer_list(&peer_state_addr).await {
                    debug!(
                        peer = %peer_state_addr,
                        "ignoring peer list gossip from peer without capability"
                    );
                    return Ok(());
                }

                let candidates = registry
                    .on_peer_list_gossip(peers, &authenticated_sender, timestamp)
                    .await;

                if candidates.is_empty() {
                    return Ok(());
                }

                let candidates_for_tracker = candidates.clone();
                let registry_clone = registry.clone();
                let discovery_handle = tokio::spawn(async move {
                    for (addr, _claim_generation) in candidates {
                        // PeerListGossip is only a discovery hint. Keep its
                        // claimed identity in `known_peers` (where
                        // `on_peer_list_gossip` put it), but create no
                        // exclusive owner/route until a TLS dial or accept
                        // verifies the address itself.
                        registry_clone.add_peer(addr).await;

                        match registry_clone.get_connection(addr).await {
                            Ok(_) => {
                                registry_clone.mark_peer_connected(addr).await;
                                debug!(peer = %addr, "connected to discovered peer");
                            }
                            Err(e) => {
                                registry_clone.mark_peer_failed(addr).await;
                                warn!(peer = %addr, error = %e, "failed to connect to discovered peer");
                            }
                        }
                    }
                });

                registry
                    .track_discovery_task(discovery_handle.abort_handle(), candidates_for_tracker)
                    .await;

                Ok(())
            }
        }
    })
}
