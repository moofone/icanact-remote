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
    /// A live rival exists but the tie-break prefers the incoming candidate —
    /// evict the rival, take incoming as the session.
    ReplaceExisting,
    /// A live rival exists and the tie-break does not strictly prefer the
    /// incoming candidate over it — keep the rival, reject incoming.
    RejectIncoming,
    /// The existing entry is stale/dead *and* the incoming candidate is not
    /// identity-preferred either — evict the stale entry, but do not accept
    /// incoming as the session either (neither survives as "the" session).
    EvictStaleRejectIncoming,
}

/// THE single decision authority for connection keep/drop/dedup/replace
/// outcomes for a verified peer identity.
///
/// Its inputs are *purely* identity-derived: `keep_existing` / `keep_incoming`
/// are the results of [`GossipRegistry::should_keep_connection`], a pure
/// function of verified peer NodeId ordering plus connection direction, and
/// `existing_usable` is the rival's liveness. There is deliberately **no
/// `SocketAddr` parameter** — this is enforced structurally (at the type/
/// signature level, not by a runtime check): a keep/drop/dedup outcome can
/// never be a function of where a peer happens to be dialing from, only of
/// its cryptographic identity. A changed socket address is handled as
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
) -> ConnectionConflictDecision {
    if !existing_usable {
        return if keep_incoming {
            ConnectionConflictDecision::AcceptIncoming
        } else {
            ConnectionConflictDecision::EvictStaleRejectIncoming
        };
    }
    if !keep_existing && keep_incoming {
        return ConnectionConflictDecision::ReplaceExisting;
    }
    // Covers both "the rival is kept" and "neither side is strictly
    // preferred" (equal/degenerate) — in both cases the live rival survives
    // rather than being displaced by an equally-unpreferred incoming.
    ConnectionConflictDecision::RejectIncoming
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
        const POOL_SIZE: usize = 256;
        const BUFFER_SIZE: usize = TCP_BUFFER_SIZE / 128; // Smaller pool buffers (8KB default)
        let pool = Self {
            connections_by_peer: SccHashMap::default(),
            addr_to_peer_id: SccHashMap::default(),
            peer_id_to_addr: SccHashMap::default(),
            connections_by_addr: SccHashMap::default(),
            peer_sessions: SccHashMap::default(),
            outbound_dial_gates: SccHashMap::default(),
            max_connections,
            connection_timeout,
            registry: ArcSwapWeak::new(std::sync::Weak::new()),
            message_buffer_pool: Arc::new(MessageBufferPool::new(POOL_SIZE, BUFFER_SIZE)),
            aligned_bytes_pool: Arc::new(crate::AlignedBytesPool::new(
                aligned_pool_size.max(crate::aligned::DEFAULT_ALIGNED_POOL_SIZE),
            )),
            connection_counter: AtomicUsize::new(0),
            _marker: PhantomData,
        };

        // Log the pool's address for debugging
        debug!(
            "CONNECTION POOL: Created new pool at {:p}",
            &pool as *const _
        );
        pool
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
                stream_handle.clone(),
                correlation,
            ));
        }

        None
    }

    fn get_or_create_peer_session(&self, peer_id: &crate::PeerId) -> Arc<PeerSession> {
        self.peer_sessions
            .entry_sync(peer_id.clone())
            .or_insert_with(|| {
                debug!(
                    "CONNECTION POOL: Creating new peer session for peer {}",
                    peer_id
                );
                Arc::new(PeerSession::new())
            })
            .get()
            .clone()
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

    pub(crate) fn set_current_peer_connection(
        &self,
        peer_id: &crate::PeerId,
        connection: Option<Arc<LockFreeConnection>>,
    ) {
        self.get_or_create_peer_session(peer_id)
            .set_current_connection(connection);
    }

    pub(crate) fn publish_current_peer_connection(
        &self,
        peer_id: &crate::PeerId,
        connection: Arc<LockFreeConnection>,
    ) {
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
        self.set_current_peer_connection(peer_id, Some(connection.clone()));
        let _ = self
            .connections_by_peer
            .upsert_sync(peer_id.clone(), connection);
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
        session.compare_and_set_current_connection(expected, connection.clone())?;

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
            .upsert_sync(peer_id.clone(), connection);
        Ok(())
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
    fn resolve_and_act_on_outbound_rival(
        &self,
        peer_id: &crate::PeerId,
        connection_arc: &Arc<LockFreeConnection>,
        rival: &Arc<LockFreeConnection>,
        registry_weak: &std::sync::Weak<GossipRegistry>,
    ) -> bool {
        let decision = registry_weak
            .upgrade()
            .map(|registry| {
                let keep_existing = registry.should_keep_connection(
                    peer_id,
                    rival.direction == ConnectionDirection::Outbound,
                );
                let keep_incoming = registry.should_keep_connection(peer_id, true);
                resolve_connection_conflict(rival.has_live_stream(), keep_existing, keep_incoming)
            })
            .unwrap_or(ConnectionConflictDecision::RejectIncoming);
        match decision {
            ConnectionConflictDecision::AcceptIncoming => {
                // The rival is stale/dead by the time we re-resolved and our
                // outbound is still preferred — one bounded retry against
                // it. A further nested race here is out of scope, same as
                // elsewhere in this function.
                let _ = self.compare_and_publish_peer_connection(
                    peer_id,
                    Some(rival),
                    connection_arc.clone(),
                );
                true
            }
            ConnectionConflictDecision::ReplaceExisting => {
                let _ = self.disconnect_connection_instance(peer_id, rival);
                self.publish_current_peer_connection(peer_id, connection_arc.clone());
                true
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

    pub(crate) fn clear_current_peer_connection(&self, peer_id: &crate::PeerId) {
        self.set_current_peer_connection(peer_id, None);
        let _ = self.connections_by_peer.remove_sync(peer_id);
    }

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

    fn is_usable_connection(&self, conn: &LockFreeConnection) -> bool {
        conn.has_live_stream()
    }

    fn indexed_connection_by_peer_id(
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

    /// Store or update the address for a peer
    /// Only updates if no address is already configured for this peer
    pub fn update_node_address(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        // Check if we already have a configured address for this node
        if let Some(existing_addr) = self.get_configured_peer_addr(peer_id) {
            debug!(
                "CONNECTION POOL: Node {} already has configured address {}, not updating to ephemeral port {}",
                peer_id, existing_addr, addr
            );
            return;
        }

        // Only update if no address is configured
        if self
            .peer_id_to_addr
            .insert_sync(peer_id.clone(), addr)
            .is_ok()
        {
            self.set_discovered_peer_addr(peer_id, addr);
            let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
        }
        debug!(
            "CONNECTION POOL: Set initial address for node {} to {}",
            peer_id, addr
        );
    }

    /// Reindex an existing connection under a new logical address for the peer.
    ///
    /// This is needed when a peer connects FROM an ephemeral TCP port but advertises
    /// a different bind address in gossip. We need to update `connections_by_addr` so
    /// that lookups by the advertised address find the connection.
    pub fn reindex_connection_addr(&self, peer_id: &crate::PeerId, new_addr: SocketAddr) {
        // First, check if this peer still has an active connection
        // This guards against race conditions where disconnect happens between checks
        let Some(connection) = self.get_connection_by_peer_id(peer_id) else {
            // Peer was disconnected, nothing to reindex.
            return;
        };

        // Check if new_addr is already indexed
        if let Some(existing_peer_id) = self.addr_to_peer_id.read_sync(&new_addr, |_, v| v.clone())
        {
            if existing_peer_id == *peer_id {
                // Already indexed under the advertised address for this peer.
                // Callers commonly upsert `addr_to_peer_id[new_addr]` themselves
                // BEFORE calling this function (e.g. registry.rs's same-identity
                // address-change path), so this branch fires on essentially
                // every reindex. If we trusted the alias alone and returned, we
                // could leave `connections_by_addr[new_addr]` missing or
                // pointed at a stale/dead connection while
                // `addr_to_peer_id[new_addr]` looks correct — a lookup/dial by
                // the new address would then miss the live session and spin up
                // a duplicate connection instead of reusing it. Always repair
                // `connections_by_addr[new_addr]` to the current live
                // connection so both maps stay consistent.
                let _ = self
                    .connections_by_addr
                    .upsert_sync(new_addr, connection.clone());
                // We still need to ensure the OLD (ephemeral) address is indexed too!
                // Without this, lookups by ephemeral address fail after gossip rounds.
                let old_addr = connection.addr;
                if old_addr != new_addr && !self.connections_by_addr.contains_sync(&old_addr) {
                    let _ = self
                        .connections_by_addr
                        .upsert_sync(old_addr, connection.clone());
                    let _ = self.addr_to_peer_id.upsert_sync(old_addr, peer_id.clone());
                    debug!(
                        old_addr = %old_addr,
                        new_addr = %new_addr,
                        peer_id = %peer_id,
                        "📍 Added missing ephemeral address mapping"
                    );
                }
                return;
            } else {
                // Stale entry from different peer - remove it before reindexing
                // This can happen if an old connection wasn't fully cleaned up
                warn!(
                    "CONNECTION POOL: Removing stale address mapping {} (was peer {}, now peer {})",
                    new_addr, existing_peer_id, peer_id
                );
                let _ = self.connections_by_addr.remove_sync(&new_addr);
                let _ = self.addr_to_peer_id.remove_sync(&new_addr);
            }
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
        let _ = self.addr_to_peer_id.upsert_sync(new_addr, peer_id.clone());
        // Also update peer_id_to_addr so disconnect uses the correct address
        self.set_discovered_peer_addr(peer_id, new_addr);

        // IMPORTANT: Keep the old (ephemeral) address entry as well!
        // Inbound messages still arrive with the TCP source address (old_addr),
        // so we need both addresses to point to the same connection.
        // The old entry is NOT removed - both addresses are valid for this peer.
        if old_addr != new_addr {
            // Re-insert connection under old addr to ensure both addresses work
            let _ = self.connections_by_addr.upsert_sync(old_addr, connection);
            // Keep addr_to_peer_id for old_addr so lookups work
            let _ = self.addr_to_peer_id.upsert_sync(old_addr, peer_id.clone());
        }

        info!(
            old_addr = %old_addr,
            new_addr = %new_addr,
            peer_id = %peer_id,
            "📍 Reindexed connection from ephemeral port to bind address"
        );
    }

    /// Get a connection by peer ID
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
            self.clear_current_peer_connection_if_matches(peer_id, &conn);
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

        // Also index by address for direct lookups
        let _ = self.connections_by_addr.upsert_sync(addr, connection);

        self.connection_counter.fetch_add(1, Ordering::AcqRel);
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
    }

    /// Send data to a peer by ID.
    pub fn send_to_peer_id(&self, peer_id: &crate::PeerId, data: bytes::Bytes) -> Result<()> {
        debug!(
            "CONNECTION POOL: send_to_peer_id called for peer '{}', pool has {} peer connections",
            peer_id,
            self.connections_by_peer.len()
        );
        if let Some(connection) = self.get_connection_by_peer_id(peer_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                debug!(
                    "CONNECTION POOL: Sending {} bytes to peer '{}'",
                    data.len(),
                    peer_id
                );
                return stream_handle.write_bytes_nonblocking(data);
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

    /// Send bytes to a peer by its ID (zero-copy version)
    pub fn send_bytes_to_peer_id(&self, peer_id: &crate::PeerId, data: bytes::Bytes) -> Result<()> {
        debug!(
            "CONNECTION POOL: send_bytes_to_peer_id called for peer '{}', pool has {} peer connections",
            peer_id,
            self.connections_by_peer.len()
        );
        if let Some(connection) = self.get_connection_by_peer_id(peer_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                debug!(
                    "CONNECTION POOL: Sending {} bytes to peer '{}'",
                    data.len(),
                    peer_id
                );
                return stream_handle.write_bytes_nonblocking(data);
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

    /// Add a new lock-free connection - completely lock-free operation
    pub fn add_lock_free_connection(
        &self,
        addr: SocketAddr,
        tcp_stream: TcpStream,
    ) -> Result<Arc<LockFreeConnection>> {
        let connection_count = self.connection_counter.fetch_add(1, Ordering::AcqRel);

        if connection_count >= self.max_connections {
            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            return Err(crate::GossipError::Network(std::io::Error::other(format!(
                "Max connections ({}) reached",
                self.max_connections
            ))));
        }

        let correlation_tracker = CorrelationTracker::new();

        // Create lock-free streaming handle with exclusive socket ownership.
        let (buffer_config, schema_hash, read_context, response_writer) = self
            .registry
            .load()
            .upgrade()
            .map(|registry| {
                let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(addr));
                let read_context = ReadContext {
                    registry_weak: Arc::downgrade(&registry),
                    peer_addr: addr,
                    peer_id: None,
                    max_message_size: registry.config.max_message_size,
                    expected_schema_hash: registry.config.schema_hash,
                    aligned_pool: registry.connection_pool.aligned_bytes_pool(),
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
            tcp_stream,
            addr,
            ChannelId::Global,
            buffer_config,
            schema_hash,
            read_context,
        );
        let stream_handle = Arc::new(stream_handle);
        if let Some(response_writer) = response_writer.as_ref() {
            response_writer.bind_stream_handle(stream_handle.clone());
        }

        let mut connection = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        connection.stream_handle = Some(stream_handle);
        connection.correlation = Some(correlation_tracker);
        connection.set_state(ConnectionState::Connected);
        connection.update_last_used();

        // Track the writer task handle (H-004).
        connection
            .task_tracker
            .set_writer(writer_task_handle.abort_handle());
        if let Some(reader_task_handle) = reader_task_handle {
            connection
                .task_tracker
                .set_reader(reader_task_handle.abort_handle());
        }

        let connection_arc = Arc::new(connection);

        // Insert into lock-free hash map.
        let _ = self
            .connections_by_addr
            .upsert_sync(addr, connection_arc.clone());
        debug!(
            "CONNECTION POOL: Added lock-free connection to {} - pool now has {} connections",
            addr,
            self.connections_by_addr.len()
        );

        Ok(connection_arc)
    }

    /// Send data through lock-free connection - NO BLOCKING.
    pub fn send_lock_free(&self, addr: SocketAddr, data: bytes::Bytes) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                return stream_handle.write_bytes_nonblocking(data);
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
            let mut addrs: Vec<SocketAddr> = Vec::new();
            self.connections_by_addr.iter_sync(|addr, _| {
                addrs.push(*addr);
                true
            });
            warn!("Available connections: {:?}", addrs);
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found",
        )))
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

    /// Try to send data through any available connection for a node
    /// This handles cases where we might have multiple connections (incoming/outgoing)
    pub fn send_to_node(
        &self,
        node_addr: SocketAddr,
        data: bytes::Bytes,
        _registry: &GossipRegistry,
    ) -> Result<()> {
        // First try direct lookup
        if let Ok(()) = self.send_lock_free(node_addr, data) {
            return Ok(());
        }

        // If that fails, look for any connection that could reach this node
        // This could be enhanced with a node ID -> connections mapping
        debug!(node_addr = %node_addr, "Direct send failed, looking for alternative connections");

        // For now, we'll rely on the caller to handle fallback strategies
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No connection found for node {}", node_addr),
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

            self.decrement_connection_counter();
            self.clear_capabilities_for_addr(&addr);

            // H-004: Abort background tasks (writer, reader) to prevent resource leaks
            connection.abort_tasks();

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
        if let Some(connection) = self.indexed_connection_by_peer_id(peer_id) {
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

            self.decrement_connection_counter();

            // H-004: Abort background tasks (writer, reader) to prevent resource leaks
            connection.abort_tasks();

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
        if removed {
            match existing_before {
                Some(existing) if existing.addr == addr && existing.has_live_stream() => {
                    let _ = self.connections_by_addr.upsert_sync(addr, existing.clone());
                    let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
                }
                _ => {
                    let _ = self.addr_to_peer_id.remove_sync(&addr);
                    self.clear_capabilities_for_addr(&addr);
                }
            }
        }
        // Abort the writer/reader tasks regardless of whether the address
        // removal above found this exact instance still indexed — the
        // candidate is being discarded either way and its tasks must not be
        // left running unaccounted for.
        candidate.abort_tasks();
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
    pub(crate) fn disconnect_connection_instance(
        &self,
        peer_id: &crate::PeerId,
        target: &Arc<LockFreeConnection>,
    ) -> bool {
        // Atomic compare-and-clear on the peer's PRIMARY current-connection
        // slot, by `Arc` identity, via `PeerSession::compare_and_clear_current_connection`
        // (a single lock-free CAS on the underlying `ArcSwapOption`). This
        // IS the entire re-validation: it either finds `target` still
        // installed and clears it right here, atomically, or it finds
        // something else — a concurrent publish (e.g. a fresh preferred
        // inbound) has already superseded `target` — and declines. There is
        // deliberately no separate check-then-act pair here: a read
        // followed by an unconditional clear has a gap in which exactly
        // that concurrent publish can land and be clobbered.
        let cleared = self
            .peer_sessions
            .read_sync(peer_id, |_, session| {
                session.compare_and_clear_current_connection(target)
            })
            .unwrap_or(false);
        if !cleared {
            debug!(
                peer_id = %peer_id,
                "declined instance-scoped disconnect: the connection currently indexed for \
                 this peer is no longer the expected instance (superseded by a concurrent \
                 publish)"
            );
            return false;
        }

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
        let _ = self
            .connections_by_peer
            .remove_if_sync(peer_id, |v| Arc::ptr_eq(v, target));

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
                let _ = self.addr_to_peer_id.remove_sync(&addr);
                self.clear_capabilities_for_addr(&addr);
            }
        }

        self.decrement_connection_counter();

        // H-004: Abort background tasks (writer, reader) to prevent resource leaks.
        target.abort_tasks();
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
        // was clobbered — the exact collateral-teardown/reconnect-thrash race
        // this PR closes, reopened through this one call site. The CAS
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

        self.decrement_connection_counter();
        connection.abort_tasks();
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
        let target = by_addr.or_else(|| {
            self.peer_sessions
                .read_sync(peer_id, |_, session| session.current_connection())
                .flatten()
                .filter(matches_instance)
        })?;

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

    /// Release the `connection_counter` contribution of an instance the
    /// caller has confirmed is superseded/displaced, in the one case
    /// `remove_connection_instance_by_id` itself cannot decrement for:
    /// when that call returns `None` because the instance is no longer the
    /// entry indexed at `addr` at all (e.g. a fresh reconnect already
    /// reindexed the same bind address before the failed instance's
    /// teardown could run).
    ///
    /// `remove_connection_instance_by_id` decrements exactly once, but only
    /// on the branch where it actually finds-and-removes the instance at
    /// `addr` — its early `removed?` return leaves the counter untouched
    /// when the addr slot no longer holds this instance. Every instance
    /// ever passed to that function was, by its own documented contract,
    /// already published/counted once (`add_lock_free_connection`/
    /// `add_connection_by_peer_id`/the outbound-finalize counter bump); if
    /// its retirement is not the one that removed it from the index, no
    /// other path will EVER decrement it again — that contribution is
    /// otherwise leaked permanently. Callers must invoke this exactly once
    /// per superseded instance, and only when `remove_connection_instance_by_id`
    /// returned `None` for that same instance — never when it returned
    /// `Some` (which already decremented), and never for a still-live
    /// current session.
    pub(crate) fn release_displaced_connection_count(&self) {
        self.decrement_connection_counter();
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
    /// points at `current` by `Arc::ptr_eq`, then performs the exact single
    /// compensating `release_displaced_connection_count()` release the
    /// same-address-failover path above already documents: `current` was
    /// counted exactly once when originally published, and once its CAS
    /// against `peer_sessions` has declined, no other path will ever find
    /// and decrement it again.
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
        for addr in alias_addrs {
            let removed = self
                .connections_by_addr
                .remove_if_sync(&addr, |v| Arc::ptr_eq(v, current))
                .is_some();
            if removed {
                let _ = self.addr_to_peer_id.remove_sync(&addr);
                self.clear_capabilities_for_addr(&addr);
            }
        }

        self.release_displaced_connection_count();
        current.abort_tasks();
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

    /// Decrement the connection counter without ever wrapping below zero.
    ///
    /// `AtomicUsize::fetch_sub` underflows to `usize::MAX` if the counter and
    /// the real connection set ever drift apart; saturating here keeps the
    /// admission gate (`add_lock_free_connection`) sane even under accounting
    /// skew.
    fn decrement_connection_counter(&self) {
        let _ =
            self.connection_counter
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    Some(count.saturating_sub(1))
                });
    }

    /// Raw admission-gate counter (`add_lock_free_connection`'s
    /// `connection_count >= max_connections` check), exposed read-only for
    /// tests that must observe it staying balanced across
    /// publish/teardown/failover cycles — see
    /// `superseded_same_addr_failover_does_not_leak_connection_counter`.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn raw_connection_counter(&self) -> usize {
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

    /// Get all connections (including disconnected) - for debugging
    pub fn get_all_connections(&self) -> Vec<SocketAddr> {
        let mut peers: Vec<SocketAddr> = Vec::new();
        self.connections_by_addr.iter_sync(|addr, _| {
            peers.push(*addr);
            true
        });
        peers
    }

    /// Get a buffer from the pool or create a new one
    pub fn get_buffer(&mut self, min_capacity: usize) -> Vec<u8> {
        // Use the message buffer pool for lock-free buffer management
        if let Some(buffer) = self.message_buffer_pool.get_buffer() {
            if buffer.capacity() >= min_capacity {
                return buffer;
            }
            // Buffer too small, return it and create new one
            self.message_buffer_pool.return_buffer(buffer);
        }
        Vec::with_capacity(min_capacity.max(1024)) // Minimum 1KB buffers
    }

    /// Return a buffer to the pool for reuse
    pub fn return_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= TCP_BUFFER_SIZE {
            // Return to the lock-free message buffer pool (up to TCP_BUFFER_SIZE)
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Get a message buffer from the pool for zero-copy processing
    pub fn get_message_buffer(&self) -> Vec<u8> {
        self.message_buffer_pool
            .get_buffer()
            .unwrap_or_else(|| Vec::with_capacity(TCP_BUFFER_SIZE / 256)) // Default small buffer
    }

    /// Return a message buffer to the pool
    pub fn return_message_buffer(&self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= TCP_BUFFER_SIZE {
            // Keep buffers with reasonable size (up to TCP_BUFFER_SIZE)
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
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
            let _ = self.connections_by_addr.remove_sync(&addr);
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

    pub(crate) async fn get_connection_to_required_peer(
        &self,
        peer_id: &crate::PeerId,
    ) -> Result<ConnectionHandle<T>> {
        let addr = self
            .get_required_peer_addr(peer_id)
            .or_else(|| self.get_configured_peer_addr(peer_id))
            .ok_or_else(|| {
                crate::GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No required address configured for peer '{}'", peer_id),
                ))
            })?;

        self.get_connection_to_peer_at(peer_id, addr).await
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
                            let _ = self.connections_by_addr.remove_sync(&addr);
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
                    let _ = self.connections_by_addr.remove_sync(&addr);
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
                    let result = self
                        .connect_via_stream(
                            addr,
                            resolved_node_id,
                            max_connections,
                            connection_timeout,
                            registry_weak.clone(),
                        )
                        .await;
                    gate_completion.finish(result.is_ok());
                    return result;
                }
                OutboundDialLease::Follower(gate) => {
                    gate.wait().await;
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn finalize_new_outbound_connection<S>(
        &self,
        addr: SocketAddr,
        stream: S,
        registry_weak: std::sync::Weak<GossipRegistry>,
        tofu_node_id: Option<crate::GossipNodeId>,
    ) -> Result<ConnectionHandle<T>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        // Determine peer ID (if known) before creating the stream handle.
        //
        // R2 (TLS identity binding): when no GossipNodeId was pinned for this address,
        // the caller passes the identity it extracted from the peer's
        // signature-verified TLS certificate (`tofu_node_id`). We bind the
        // connection's `embedded_peer_id` to that learned identity so every
        // subsequent per-message gossip frame on this link IS cert-identity
        // checked (the protocol guard requires `embedded_peer_id.is_some()`).
        // Without this, bootstrap (placeholder-SNI) dials left `embedded_peer_id`
        // as `None` and gossip on the link was never identity-checked.
        let peer_id_opt = self
            .addr_to_peer_id
            .read_sync(&addr, |_, v| v.clone())
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
            })
            .or_else(|| tofu_node_id.as_ref().map(crate::PeerId::from_public_key));

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
                    registry_weak: Arc::downgrade(&registry),
                    peer_addr: addr,
                    peer_id: peer_id_opt.clone(),
                    max_message_size: registry.config.max_message_size,
                    expected_schema_hash: registry.config.schema_hash,
                    aligned_pool: registry.connection_pool.aligned_bytes_pool(),
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
        if let Some(response_writer) = response_writer.as_ref() {
            response_writer.bind_stream_handle(stream_handle.clone());
        }

        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
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
        // is indexed by address/peer_id below. `get_connection_by_peer_id` is
        // not a pure lookup: its configured-address and alias fallbacks read
        // straight out of `connections_by_addr` / `addr_to_peer_id`. Calling
        // it *after* indexing this candidate risks it returning the brand
        // new connection as its own "existing rival" (the fallback matches
        // the configured address, which is exactly the address we just
        // dialed and already inserted) — that silently bypasses the
        // `existing_usable == false` / `EvictStaleRejectIncoming` path and
        // can let a non-preferred outbound become the current session, and
        // makes `ReplaceExisting` "evict" nothing at all. Capturing the
        // rival here, while the candidate is still unindexed, guarantees the
        // decision and any eviction below can only ever target the real
        // prior connection instance, never this new one.
        let existing_before = peer_id_opt
            .as_ref()
            .and_then(|peer_id| self.get_connection_by_peer_id(peer_id));

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
            // purely identity-derived (`should_keep_connection`), never keyed on
            // `addr`. If the existing preferred session wins, we leave the
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
                        resolve_connection_conflict(
                            existing.has_live_stream(),
                            keep_existing,
                            keep_incoming,
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
                    // preferred inbound) is left alone. We still publish our
                    // outbound afterward: the tie-break already decided we
                    // win regardless of exactly what is indexed at this
                    // instant.
                    if let Some(existing) = existing_before.as_ref() {
                        let _ = self.disconnect_connection_instance(peer_id, existing);
                    }
                    self.publish_current_peer_connection(peer_id, connection_arc.clone());
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
        // Count this connection exactly once, mirroring `add_connection_by_peer_id`.
        // Without this the outbound path published a live connection that the
        // teardown paths later decremented, underflowing `connection_counter`.
        self.connection_counter.fetch_add(1, Ordering::AcqRel);
        debug!(
            "CONNECTION POOL: Added connection via get_connection to {} - pool now has {} connections",
            addr,
            self.connections_by_addr.len()
        );
        // Another task can observe and tear down the connection immediately after publication,
        // so publication must not assume the address entry remains present beyond this point.
        debug!("CONNECTION POOL: Published connection for {}", addr);

        // Send initial FullSync message to identify ourselves
        if let Some(registry_arc) = registry_weak.upgrade() {
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
                        stream_handle.clone(),
                        connection_arc
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                    );
                    if let Err(e) = conn_handle
                        .send_gossip_payload(bytes::Bytes::from_owner(data))
                        .await
                    {
                        warn!(peer = %addr, error = %e, "Failed to send initial FullSync message");
                    } else {
                        info!(peer = %addr, "Sent initial FullSync message to identify ourselves");
                    }
                }
                Err(e) => {
                    warn!(peer = %addr, error = %e, "Failed to serialize initial FullSync message");
                }
            }
        }

        // Reset failure state for this peer since we successfully connected.
        if let Some(registry) = registry_weak.upgrade() {
            let registry_clone = registry.clone();
            let peer_addr = addr;
            tokio::spawn(async move {
                let mut gossip_state = registry_clone.gossip_state.lock().await;

                // Check if we need to reset failures and clear pending
                let need_to_clear_pending = if let Some(peer_info) =
                    gossip_state.peers.get_mut(&peer_addr)
                {
                    let had_failures = peer_info.failures > 0;
                    peer_info.outbound_dial_success = true;
                    if had_failures {
                        info!(peer = %peer_addr,
                                  prev_failures = peer_info.failures,
                                  "✅ Successfully established outgoing connection - resetting failure state");
                        peer_info.failures = 0;
                        peer_info.last_failure_time = None;
                    }
                    peer_info.last_success = crate::current_timestamp();
                    had_failures
                } else {
                    false
                };

                // Clear pending failure record if needed
                if need_to_clear_pending {
                    gossip_state.pending_peer_failures.remove(&peer_addr);
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

    /// Remove a connection from the pool by address
    pub fn remove_connection_mut(&mut self, addr: SocketAddr) {
        if let Some((_, conn)) = self.connections_by_addr.remove_sync(&addr) {
            // H-004: Abort background tasks (writer, reader) to prevent resource leaks
            conn.abort_tasks();

            info!(addr = %addr, "removed connection from pool");
            // Dropping the sender will cause the receiver to return None,
            // signaling the connection handler to shut down
            // No need to drop writer
            self.clear_capabilities_for_addr(&addr);
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

    /// Check health of all connections
    pub async fn check_connection_health(&self) -> Vec<SocketAddr> {
        // Health checking is now done by the persistent connection handlers
        Vec::new()
    }

    /// Clean up stale connections
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
    /// Handle persistent connection reader - only reads messages, no channels
    #[allow(dead_code)]
    pub(crate) async fn handle_persistent_connection_reader(
        mut reader: tokio::net::tcp::OwnedReadHalf,
        _writer: Option<tokio::net::tcp::OwnedWriteHalf>,
        peer_addr: SocketAddr,
        registry_weak: Option<std::sync::Weak<GossipRegistry>>,
    ) {
        let max_message_size = registry_weak
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|registry| registry.config.max_message_size)
            .unwrap_or(10 * 1024 * 1024);
        let aligned_pool = registry_weak
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|registry| registry.connection_pool.aligned_bytes_pool());

        let mut streaming_state = crate::protocol::StreamingState::new();
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let msg_result = tokio::select! {
                result = crate::handle::read_message_from_tls_reader(&mut reader, max_message_size, aligned_pool.as_ref()) => result,
                _ = cleanup_interval.tick() => {
                    streaming_state.cleanup_stale();
                    continue;
                }
            };

            match msg_result {
                Ok(result) => {
                    if let Some(registry) = registry_weak.as_ref().and_then(|w| w.upgrade()) {
                        let authenticated_peer_id =
                            registry.connection_pool.get_peer_id_by_addr(&peer_addr);
                        if let Err(e) = crate::protocol::process_read_result(
                            result,
                            &mut streaming_state,
                            &registry,
                            peer_addr,
                            None,
                            None,
                            authenticated_peer_id.as_ref(),
                        )
                        .await
                        {
                            warn!(peer = %peer_addr, error = %e, "Failed to process message on persistent connection");
                        }
                    } else {
                        warn!(peer = %peer_addr, "Registry dropped, stopping persistent connection reader");
                        break;
                    }
                }
                Err(e) => {
                    warn!(peer = %peer_addr, error = %e, "Persistent connection reader error");
                    break;
                }
            }
        }

        info!(peer = %peer_addr, "CONNECTION_POOL: Triggering peer failure handling");
        if let Some(ref registry_weak) = registry_weak {
            if let Some(registry) = registry_weak.upgrade() {
                // Dead-code path (`#[allow(dead_code)]`, no call sites): no
                // stream-handle instance id is available here, so this
                // conservatively falls back to the "may be the current
                // session" path, same as before this parameter existed.
                if let Err(e) = registry
                    .handle_peer_connection_failure(peer_addr, None)
                    .await
                {
                    warn!(error = %e, peer = %peer_addr, "CONNECTION_POOL: Failed to handle peer connection failure");
                }
            }
        }
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

/// Handle an incoming message on a bidirectional connection
pub(crate) fn handle_incoming_message(
    registry: Arc<GossipRegistry>,
    _peer_addr: SocketAddr,
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

                let sender_socket_addr =
                    resolve_peer_state_addr(&registry, Some(&delta.sender_peer_id), _peer_addr)
                        .await;
                registry.record_inbound_gossip_extensions(
                    sender_socket_addr,
                    extensions,
                    crate::current_timestamp_nanos(),
                );

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
                                last_dns_refresh_attempt: None,
                                last_response_received_ms: current_time_ms,
                            });
                        }
                    }

                    // Check if this is a previously failed peer
                    let was_failed = gossip_state
                        .peers
                        .get(&sender_socket_addr)
                        .map(|info| info.failures >= registry.config.max_peer_failures)
                        .unwrap_or(false);

                    if was_failed {
                        info!(
                            peer = %delta.sender_peer_id,
                            "✅ Received delta from previously failed peer - connection restored!"
                        );

                        // Clear the pending failure record
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }

                    // Update peer info and check if we need to clear pending failures
                    let need_to_clear_pending =
                        if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                            // Always reset failure state when we receive messages from the peer
                            // This proves the peer is alive and communicating
                            let had_failures = peer_info.failures > 0;
                            if had_failures {
                                info!(peer = %delta.sender_peer_id,
                              prev_failures = peer_info.failures,
                              "🔄 Resetting failure state after receiving DeltaGossip");
                                peer_info.failures = 0;
                                peer_info.last_failure_time = None;
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

                            had_failures
                        } else {
                            false
                        };

                    // Clear pending failure record if needed
                    if need_to_clear_pending {
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }
                    gossip_state.delta_exchanges += 1;
                }

                // Apply the delta using the canonical registry logic (vector clocks +
                // deterministic tiebreakers). The previous "inline apply" fast-path had
                // multiple conflict-resolution implementations depending on lock contention,
                // which could cause nodes to diverge.
                //
                // Only ACK immediate-priority actor additions that actually
                // mutated local state. Duplicate deltas (same vector clock or
                // already-tombstoned) return an empty list, so we don't emit
                // redundant `ImmediateAck` frames for senders that broadcast
                // the same change more than once.
                //
                // `_peer_addr` is the verified socket address of the
                // connection this delta arrived on — the §1.6 trust anchor
                // for advertised-address repair (outranks configured/
                // discovered route state, which may be stale).
                let immediate_actors = registry.apply_delta_from(delta, Some(_peer_addr)).await?;

                // NEW: Send ACK back for immediate registrations
                if !immediate_actors.is_empty() {
                    // Send ACKs for immediate priority actor additions
                    // Use lock-free send since we're responding on the same connection
                    for actor_name in immediate_actors {
                        // Send lightweight ACK immediately
                        let ack = crate::registry::RegistryMessage::ImmediateAck {
                            actor_name: actor_name.clone(),
                            success: true,
                        };

                        // Serialize and send
                        if let Ok(serialized) = rkyv::to_bytes::<rkyv::rancor::Error>(&ack) {
                            let pool = &registry.connection_pool;
                            let payload = bytes::Bytes::from_owner(serialized);
                            let header = bytes::Bytes::copy_from_slice(
                                &framing::write_gossip_frame_prefix(payload.len()),
                            );
                            // Use send_lock_free_parts to send directly without copying payload bytes.
                            if let Err(e) =
                                pool.send_lock_free_parts(sender_socket_addr, header, payload)
                            {
                                warn!("Failed to send ImmediateAck: {}", e);
                            } else {
                                info!("Sent ImmediateAck for actor '{}'", actor_name);
                            }
                        }
                    }
                }

                // Note: Response will be sent during regular gossip rounds
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
                // Use the peer's advertised listening address when it is dialable.
                // Remote loopback binds are local-only and must not be rewritten into
                // remote-ip:ephemeral-port peer entries.
                let Some(sender_socket_addr) =
                    resolve_peer_addr_checked(sender_bind_addr.as_deref(), _peer_addr)
                else {
                    warn!(
                        tcp_source = %_peer_addr,
                        sender = %sender_peer_id,
                        sender_bind_addr = ?sender_bind_addr,
                        "Ignoring FullSync from peer with non-dialable advertised bind address"
                    );
                    return Ok(());
                };
                registry.record_inbound_gossip_extensions(
                    sender_socket_addr,
                    extensions,
                    crate::current_timestamp_nanos(),
                );

                // Note: sender_peer_id is now a PeerId (e.g., "node_a"), not an address
                debug!(
                    "Received FullSync from node '{}' at bind_addr {} (tcp_source={})",
                    sender_peer_id, sender_socket_addr, _peer_addr
                );

                // OPTIMIZATION: Do all peer management in one lock acquisition
                {
                    let mut gossip_state = registry.gossip_state.lock().await;

                    // FIX: If the resolved bind address differs from the TCP source address,
                    // migrate the PeerInfo from the ephemeral port entry to the bind address.
                    // This preserves node_id, sequence, and failure state learned during TLS handshake.
                    if sender_socket_addr != _peer_addr && _peer_addr != registry.bind_addr {
                        if let Some(mut old_peer_info) = gossip_state.peers.remove(&_peer_addr) {
                            info!(
                                old_addr = %_peer_addr,
                                new_addr = %sender_socket_addr,
                                node_id = ?old_peer_info.node_id,
                                "🔄 Migrating peer info from ephemeral TCP source to bind address from FullSync"
                            );
                            // Update the address field and preserve the connection address
                            old_peer_info.address = sender_socket_addr;
                            old_peer_info.peer_address = Some(_peer_addr);
                            // Insert with new key (bind address), preserving all state
                            gossip_state.peers.insert(sender_socket_addr, old_peer_info);
                            // Also clean up pending failures for the old address
                            gossip_state.pending_peer_failures.remove(&_peer_addr);
                        }
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
                                last_dns_refresh_attempt: None,
                                last_response_received_ms: current_time_ms,
                            });
                        }
                    }

                    // Update peer info and reset failure state
                    let had_failures = gossip_state
                        .peers
                        .get(&sender_socket_addr)
                        .map(|info| info.failures > 0)
                        .unwrap_or(false);

                    if had_failures {
                        // Clear the pending failure record
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }

                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        let prev_failures = peer_info.failures;
                        // Always reset failure state when we receive a FullSync from the peer
                        // This proves the peer is alive and communicating
                        if peer_info.failures > 0 {
                            info!(peer = %sender_socket_addr,
                              prev_failures = prev_failures,
                              "🔄 Resetting failure state after receiving FullSync");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        // Inbound payload from peer — proves app-level liveness.
                        // See `handle_incoming_message::DeltaGossip` for the
                        // full rationale.
                        peer_info.last_response_received_ms = crate::current_timestamp_millis();
                        peer_info.consecutive_deltas = 0;
                    } else {
                        warn!(peer = %sender_socket_addr, "Peer not found in peer list when trying to reset failure state");
                    }
                    gossip_state.full_sync_exchanges += 1;
                }

                debug!(
                    sender = %sender_peer_id,
                    sequence = sequence,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "📨 INCOMING: Received full sync message on bidirectional connection"
                );

                // IMPORTANT: Register the incoming connection with the peer_id mapping
                // This allows bidirectional communication to work properly
                {
                    let pool = &registry.connection_pool;

                    // NOTE: Do NOT remove addr_to_peer_id for the ephemeral address here.
                    // The reindex_connection_addr function preserves both addresses,
                    // and disconnect_connection_by_peer_id needs both entries to clean up properly.

                    let _ = pool
                        .peer_id_to_addr
                        .upsert_sync(sender_peer_id.clone(), sender_socket_addr);
                    let _ = pool
                        .addr_to_peer_id
                        .upsert_sync(sender_socket_addr, sender_peer_id.clone());

                    // CRITICAL FIX: Reindex the connection from ephemeral TCP port to bind address
                    // Without this, get_connection(bind_addr) fails because the connection is
                    // still indexed under the ephemeral port the peer connected FROM.
                    // This allows messages to be sent back to the peer using their advertised address.
                    // Note: reindex_connection_addr already has early-return if already indexed,
                    // and logs internally when it actually does work.
                    if sender_socket_addr != _peer_addr {
                        pool.reindex_connection_addr(&sender_peer_id, sender_socket_addr);
                    }

                    debug!(
                        "BIDIRECTIONAL: Registered incoming connection - peer_id={} addr={}",
                        sender_peer_id, sender_socket_addr
                    );
                }

                // Only remaining async operation. Peer bookkeeping keys on
                // the bind-derived address; address REPAIR anchors on the
                // verified TCP source (§1.6).
                registry
                    .merge_full_sync_from(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_peer_id.clone(),
                        sender_socket_addr,
                        Some(_peer_addr),
                        sequence,
                        wall_clock_time,
                    )
                    .await;

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
                        let header = bytes::Bytes::copy_from_slice(
                            &framing::write_gossip_frame_prefix(payload.len()),
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
                            header,
                            payload.clone(),
                        ) {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                warn!("Failed to send via peer ID {}: {}", sender_peer_id, e);
                                // Fall back to socket address
                                let fallback_header = bytes::Bytes::copy_from_slice(
                                    &framing::write_gossip_frame_prefix(payload.len()),
                                );
                                pool.send_lock_free_parts(
                                    sender_socket_addr,
                                    fallback_header,
                                    payload,
                                )
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

                                // Store in gossip state to be sent during next gossip round
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

                // Same §1.6 trust anchor as the DeltaGossip branch above:
                // responses also carry actor additions, and repair must use
                // the verified socket address of this connection.
                if let Err(err) = registry.apply_delta_from(delta, Some(_peer_addr)).await {
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
                let Some(sender_socket_addr) =
                    resolve_peer_addr_checked(sender_bind_addr.as_deref(), _peer_addr)
                else {
                    warn!(
                        tcp_source = %_peer_addr,
                        sender = %sender_peer_id,
                        sender_bind_addr = ?sender_bind_addr,
                        "Ignoring FullSyncResponse from peer with non-dialable advertised bind address"
                    );
                    return Ok(());
                };
                registry.record_inbound_gossip_extensions(
                    sender_socket_addr,
                    extensions,
                    crate::current_timestamp_nanos(),
                );

                debug!(
                    sender = %sender_peer_id,
                    bind_addr = %sender_socket_addr,
                    tcp_source = %_peer_addr,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "RECEIVED: FullSyncResponse from peer (using bind_addr)"
                );

                registry
                    .merge_full_sync_from(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_peer_id.clone(),
                        sender_socket_addr,
                        Some(_peer_addr),
                        sequence,
                        wall_clock_time,
                    )
                    .await;

                // FIX: Update peer_id mappings (mirror the FullSync handler logic)
                // This prevents stale ephemeral addresses from being reintroduced via resolve_peer_state_addr
                {
                    let pool = &registry.connection_pool;

                    // NOTE: Do NOT remove addr_to_peer_id for the ephemeral address here.
                    // The reindex_connection_addr function preserves both addresses,
                    // and disconnect_connection_by_peer_id needs both entries to clean up properly.

                    let _ = pool
                        .peer_id_to_addr
                        .upsert_sync(sender_peer_id.clone(), sender_socket_addr);
                    let _ = pool
                        .addr_to_peer_id
                        .upsert_sync(sender_socket_addr, sender_peer_id.clone());

                    // CRITICAL FIX: Reindex the connection from ephemeral TCP port to bind address
                    // Mirror the FullSync handler fix - allows sending to advertised address
                    // Note: reindex_connection_addr already has early-return if already indexed,
                    // and logs internally when it actually does work.
                    if sender_socket_addr != _peer_addr {
                        pool.reindex_connection_addr(&sender_peer_id, sender_socket_addr);
                    }

                    debug!(
                        "BIDIRECTIONAL: Updated connection mapping from FullSyncResponse - peer_id={} addr={}",
                        sender_peer_id, sender_socket_addr
                    );
                }

                // Reset failure state when receiving response
                let mut gossip_state = registry.gossip_state.lock().await;

                // FIX: If the resolved bind address differs from the TCP source address,
                // migrate the PeerInfo from the ephemeral port entry to the bind address.
                // This preserves node_id, sequence, and failure state learned during TLS handshake.
                if sender_socket_addr != _peer_addr && _peer_addr != registry.bind_addr {
                    if let Some(mut old_peer_info) = gossip_state.peers.remove(&_peer_addr) {
                        info!(
                            old_addr = %_peer_addr,
                            new_addr = %sender_socket_addr,
                            node_id = ?old_peer_info.node_id,
                            "🔄 Migrating peer info from ephemeral TCP source to bind address from FullSyncResponse"
                        );
                        // Update the address field and preserve the connection address
                        old_peer_info.address = sender_socket_addr;
                        old_peer_info.peer_address = Some(_peer_addr);
                        // Insert with new key (bind address), preserving all state
                        gossip_state.peers.insert(sender_socket_addr, old_peer_info);
                        // Also clean up pending failures for the old address
                        gossip_state.pending_peer_failures.remove(&_peer_addr);
                    }
                }

                // Reset failure state for responding peer
                let need_to_clear_pending =
                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        let had_failures = peer_info.failures > 0;
                        if had_failures {
                            info!(peer = %sender_socket_addr,
                          prev_failures = peer_info.failures,
                          "🔄 Resetting failure state after receiving FullSyncResponse");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        // Inbound payload from peer — proves app-level liveness.
                        // See `handle_incoming_message::DeltaGossip` for the
                        // full rationale.
                        peer_info.last_response_received_ms = crate::current_timestamp_millis();
                        had_failures
                    } else {
                        false
                    };

                // Clear pending failure record if needed
                if need_to_clear_pending {
                    gossip_state
                        .pending_peer_failures
                        .remove(&sender_socket_addr);
                }

                gossip_state.full_sync_exchanges += 1;
                Ok(())
            }
            RegistryMessage::PeerHealthQuery {
                sender,
                target_peer,
                timestamp: _,
            } => {
                let sender_socket_addr =
                    resolve_peer_state_addr(&registry, Some(&sender), _peer_addr).await;
                debug!(
                    sender = %sender,
                    target = %target_peer,
                    "received peer health query"
                );

                // Check our connection status to the target peer
                let target_addr = match target_peer.parse::<SocketAddr>() {
                    Ok(addr) => addr,
                    Err(_) => {
                        warn!(
                            "Invalid target peer address in health query: {}",
                            target_peer
                        );
                        return Ok(());
                    }
                };

                let is_alive = {
                    let pool = &registry.connection_pool;
                    pool.has_connection(&target_addr)
                };

                let last_contact = if is_alive {
                    crate::current_timestamp()
                } else {
                    // Check when we last had successful contact
                    let gossip_state = registry.gossip_state.lock().await;
                    gossip_state
                        .peers
                        .get(&target_addr)
                        .map(|info| info.last_success)
                        .unwrap_or(0)
                };

                // Send our health report back
                let mut peer_statuses = HashMap::new();

                // Get actual failure count from gossip state
                let failure_count = {
                    let gossip_state = registry.gossip_state.lock().await;
                    gossip_state
                        .peers
                        .get(&target_addr)
                        .map(|info| info.failures as u32)
                        .unwrap_or(0)
                };

                peer_statuses.insert(
                    target_peer,
                    crate::registry::PeerHealthStatus {
                        is_alive,
                        last_contact,
                        failure_count,
                    },
                );

                let report = RegistryMessage::PeerHealthReport {
                    reporter: registry.peer_id.clone(),
                    peer_statuses: peer_statuses.into_iter().collect(),
                    timestamp: crate::current_timestamp(),
                };

                // Send report back to the querying peer
                if let Ok(data) = rkyv::to_bytes::<rkyv::rancor::Error>(&report) {
                    // Use the actual peer address we received from
                    let sender_addr = sender_socket_addr;

                    let pool = &registry.connection_pool;
                    let payload = bytes::Bytes::from_owner(data);
                    let header = bytes::Bytes::copy_from_slice(
                        &framing::write_gossip_frame_prefix(payload.len()),
                    );

                    // Use send_lock_free_parts which doesn't copy payload bytes.
                    if let Err(e) = pool.send_lock_free_parts(sender_addr, header, payload) {
                        warn!(peer = %sender_addr, error = %e, "Failed to send peer health report");
                    }
                }

                Ok(())
            }
            RegistryMessage::PeerHealthReport {
                reporter,
                peer_statuses,
                timestamp: _,
            } => {
                let reporter_addr =
                    resolve_peer_state_addr(&registry, Some(&reporter), _peer_addr).await;
                debug!(
                    reporter = %reporter,
                    peers = peer_statuses.len(),
                    "received peer health report"
                );

                // Store the health reports
                {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    for (peer, status) in peer_statuses {
                        if let Ok(peer_addr) = peer.parse::<SocketAddr>() {
                            // For now, use the reporter's peer address from the connection
                            gossip_state
                                .peer_health_reports
                                .entry(peer_addr)
                                .or_insert_with(HashMap::new)
                                .insert(reporter_addr, status);
                        }
                    }
                }

                // Check if we have enough reports to make a decision
                registry.check_peer_consensus().await;

                Ok(())
            }
            RegistryMessage::ActorMessage { .. } => {
                warn!(
                    peer = %_peer_addr,
                    "Registry ActorMessage is no longer supported in v3; use ActorTell/ActorAsk frames"
                );
                Ok(())
            }

            RegistryMessage::ImmediateAck {
                actor_name,
                success,
            } => {
                debug!(
                    actor_name = %actor_name,
                    success = success,
                    "received immediate ACK for synchronous registration"
                );

                // Look up and complete the pending ACK waiter for this actor.
                if let Some((_, pending)) = registry.pending_acks.remove_sync(&actor_name) {
                    pending.complete(success);
                    info!(
                        actor_name = %actor_name,
                        success = success,
                        "✅ Completed ACK for waiting synchronous registration"
                    );
                } else {
                    debug!(
                        actor_name = %actor_name,
                        "Received ACK but no pending registration found (may have timed out)"
                    );
                }

                Ok(())
            }

            RegistryMessage::PeerListGossip {
                peers,
                timestamp,
                sender_addr,
            } => {
                let peer_state_addr = resolve_peer_state_addr(&registry, None, _peer_addr).await;
                debug!(
                    peer_count = peers.len(),
                    timestamp = timestamp,
                    sender = %sender_addr,
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
                    .on_peer_list_gossip(peers, &sender_addr, timestamp)
                    .await;

                if candidates.is_empty() {
                    return Ok(());
                }

                let registry_clone = registry.clone();
                let discovery_handle = tokio::spawn(async move {
                    for addr in candidates {
                        let node_id = registry_clone.lookup_node_id(&addr).await;
                        registry_clone.add_peer_with_node_id(addr, node_id).await;

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

                // Track the discovery task (H-004): keep at most one dial task alive.
                registry.discovery_task.set(discovery_handle.abort_handle());

                Ok(())
            }
        }
    })
}
