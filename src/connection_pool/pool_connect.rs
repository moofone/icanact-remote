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
            udp_socket: ArcSwapOption::empty(),
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

    /// Install the shared UDP socket used by datagram transport mode.
    pub fn set_udp_socket(&self, socket: Arc<UdpSocket>) {
        self.udp_socket.store(Some(socket));
    }

    fn udp_socket(&self) -> Result<Arc<UdpSocket>> {
        self.udp_socket.load_full().ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "udp socket is not initialized",
            ))
        })
    }

    /// Returns the shared UDP socket without error — `None` if UDP transport is not configured.
    /// Used by the ask responder to reply to UDP asks without a connection-pool lookup.
    pub(crate) fn udp_socket_opt(&self) -> Option<Arc<UdpSocket>> {
        self.udp_socket.load_full()
    }

    fn is_udp_transport_enabled(&self) -> bool {
        self.registry
            .load()
            .upgrade()
            .map(|registry| registry.udp_mode)
            .unwrap_or(false)
    }

    fn current_schema_hash(&self) -> Option<u64> {
        self.registry
            .load()
            .upgrade()
            .and_then(|registry| registry.config.schema_hash)
    }

    fn udp_write_queue_capacity(&self) -> usize {
        self.registry
            .load()
            .upgrade()
            .map(|registry| {
                BufferConfig::default()
                    .with_ask_window(registry.config.ask_window)
                    .write_queue_capacity()
            })
            .unwrap_or_else(|| BufferConfig::default().write_queue_capacity())
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

        if self.is_udp_transport_enabled() {
            let socket = self.udp_socket.load_full()?;
            return Some(ConnectionHandle::new_udp(
                addr,
                socket,
                self.udp_write_queue_capacity(),
                self.current_schema_hash(),
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

    fn set_session_configured_addr(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        self.get_or_create_peer_session(peer_id).set_configured_addr(addr);
        let _ = self.peer_id_to_addr.upsert_sync(peer_id.clone(), addr);
    }

    pub(crate) fn set_configured_peer_addr(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        self.set_session_configured_addr(peer_id, addr);
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
                        ConnectionDirection::Inbound => crate::lifecycle::TransportDirection::Inbound,
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
        if self.is_udp_transport_enabled() {
            conn.is_connected()
        } else {
            conn.has_live_stream()
        }
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

    async fn ensure_udp_connection(&self, addr: SocketAddr) -> Result<()> {
        if let Some(existing) = self.get_lock_free_connection(addr) {
            if existing.is_connected() {
                return Ok(());
            }
            let _ = self.remove_connection(addr);
        }

        let udp_socket = self.udp_socket()?;
        let registry_weak = self.registry.load_full();

        // Mirror TCP path peer identity resolution so UDP shares the same correlation tracking.
        let peer_id_opt = self
            .addr_to_peer_id
            .read_sync(&addr, |_, v| v.clone())
            .or_else(|| {
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

        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        conn.set_state(ConnectionState::Connected);
        conn.update_last_used();
        conn.correlation = Some(correlation_tracker.clone());
        if let Some(peer_id) = peer_id_opt.clone() {
            conn.embedded_peer_id = Some(peer_id.clone());
        }

        let connection_arc = Arc::new(conn);
        let _ = self
            .connections_by_addr
            .upsert_sync(addr, connection_arc.clone());

        if let Some(peer_id) = peer_id_opt.clone() {
            let _ = self
                .connections_by_peer
                .upsert_sync(peer_id.clone(), connection_arc.clone());
            let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
            self.publish_current_peer_connection(&peer_id, connection_arc.clone());
        }

        // Keep startup identity exchange parity with stream transports.
        if let Some(registry_arc) = registry_weak.upgrade() {
            let initial_msg = {
                let (local_actors, known_actors) = registry_arc.snapshot_actor_pairs();
                let gossip_state = registry_arc.gossip_state.lock().await;

                RegistryMessage::FullSync {
                    local_actors,
                    known_actors,
                    sender_peer_id: registry_arc.peer_id.clone(),
                    sender_bind_addr: Some(registry_arc.bind_addr.to_string()),
                    sequence: gossip_state.gossip_sequence,
                    wall_clock_time: crate::current_timestamp(),
                    extensions: None,
                }
            };

            match rkyv::to_bytes::<rkyv::rancor::Error>(&initial_msg) {
                Ok(data) => {
                    let header = framing::write_gossip_frame_prefix(data.len());
                    let mut msg_buffer = Vec::with_capacity(header.len() + data.len());
                    msg_buffer.extend_from_slice(&header); // ALLOW_COPY
                    msg_buffer.extend_from_slice(&data); // ALLOW_COPY

                    let conn_handle: ConnectionHandle<T> = ConnectionHandle::new_udp(
                        addr,
                        udp_socket.clone(),
                        self.udp_write_queue_capacity(),
                        registry_arc.config.schema_hash,
                        correlation_tracker.clone(),
                    );
                    if let Err(e) = conn_handle.send_data(msg_buffer).await {
                        warn!(peer = %addr, error = %e, "Failed to send initial FullSync message");
                    } else {
                        info!(peer = %addr, "Sent initial FullSync message to identify ourselves");
                    }
                }
                Err(e) => {
                    warn!(peer = %addr, error = %e, "Failed to serialize initial FullSync message");
                }
            }

            let registry_clone_for_mark = registry_arc.clone();
            tokio::spawn(async move {
                registry_clone_for_mark.mark_peer_connected(addr).await;
            });
        }

        Ok(())
    }

    /// Ensure the per-peer UDP connection handle exists.
    pub async fn ensure_udp_peer_connection(&self, addr: SocketAddr) -> Result<()> {
        self.ensure_udp_connection(addr).await
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
            self.get_or_create_peer_session(peer_id).set_configured_addr(addr);
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
                // But we still need to ensure the OLD (ephemeral) address is indexed too!
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
        self.set_session_configured_addr(peer_id, new_addr);

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
        debug!("CONNECTION POOL: Got correlation tracker for peer {}", peer_id);
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
        self.set_session_configured_addr(&peer_id, addr);
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

    fn try_send_udp_bytes_to_addr(&self, addr: SocketAddr, data: bytes::Bytes) -> Result<()> {
        let socket = self.udp_socket()?;
        crate::transport::try_send_bytes_to_addr(socket.as_ref(), addr, data)
    }

    fn try_send_udp_parts_to_addr(
        &self,
        addr: SocketAddr,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let socket = self.udp_socket()?;
        crate::transport::try_send_parts_to_addr(socket.as_ref(), addr, header, payload)
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
            } else if self.is_udp_transport_enabled() {
                return self.try_send_udp_bytes_to_addr(connection.addr, data);
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
            } else if self.is_udp_transport_enabled() {
                return self.try_send_udp_parts_to_addr(connection.addr, header, payload);
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
            } else if self.is_udp_transport_enabled() {
                return self.try_send_udp_bytes_to_addr(connection.addr, data);
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
            } else if self.is_udp_transport_enabled() {
                return self.try_send_udp_bytes_to_addr(connection.addr, data);
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
            } else if self.is_udp_transport_enabled() {
                return self.try_send_udp_parts_to_addr(connection.addr, header, payload);
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
            self.connections_by_addr.iter_sync(|alias_addr, alias_conn| {
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

            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            self.clear_capabilities_for_addr(&addr);

            // H-004: Abort background tasks (writer, reader) to prevent resource leaks
            connection.abort_tasks();

            Some(connection)
        } else {
            None
        }
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

            for addr in &addrs_to_remove {
                let _ = self.addr_to_peer_id.remove_sync(addr);
                let _ = self.connections_by_addr.remove_sync(addr);
                self.clear_capabilities_for_addr(addr);
            }

            self.connection_counter.fetch_sub(1, Ordering::AcqRel);

            // H-004: Abort background tasks (writer, reader) to prevent resource leaks
            connection.abort_tasks();

            Some(connection)
        } else {
            None
        }
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

        // Look up the address for this node
        let addr = if let Some(addr) = self.get_configured_peer_addr(peer_id) {
            addr
        } else {
            return Err(crate::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No address configured for peer '{}'", peer_id),
            )));
        };

        debug!(
            "CONNECTION POOL: Creating new connection to peer '{}' at {}",
            peer_id, addr
        );

        // Convert PeerId to NodeId for TLS
        let node_id_for_tls = Some(peer_id.to_node_id());

        // Create the connection and store it by node ID
        // Pass the NodeId so TLS can work even if gossip state doesn't have it yet
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
        node_id: Option<crate::NodeId>,
    ) -> Result<ConnectionHandle<T>> {
        let _current_time = current_timestamp();
        // Debug logging removed for performance - these logs were too verbose
        // debug!("CONNECTION POOL: get_connection called on pool at {:p} for {}", self as *const _, addr);
        // debug!("CONNECTION POOL: This pool instance has {} connections stored", self.connections_by_addr.len());

        // Extract what we need before any await points to avoid Send issues
        let max_connections = self.max_connections;
        let connection_timeout = self.connection_timeout;
        let registry_weak = self.registry.load_full();

        if self.is_udp_transport_enabled() {
            return self.connect_via_udp(addr).await;
        }

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
    ) -> Result<ConnectionHandle<T>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        // Determine peer ID (if known) before creating the stream handle.
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

        // Insert into lock-free map before spawning.
        let _ = self
            .connections_by_addr
            .upsert_sync(addr, connection_arc.clone());
        if let Some(peer_id) = peer_id_opt.as_ref() {
            let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
            self.publish_current_peer_connection(peer_id, connection_arc.clone());
        }
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
                    sender_bind_addr: Some(registry_arc.bind_addr.to_string()), // Use our listening address, not ephemeral port
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
                if let Err(e) = registry.handle_peer_connection_failure(peer_addr).await {
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
                let immediate_actors = registry.apply_delta(delta).await?;

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

                // Only remaining async operation
                registry
                    .merge_full_sync(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_peer_id.clone(),
                        sender_socket_addr,
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
                        sender_bind_addr: Some(registry.bind_addr.to_string()), // Our listening address
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

                if let Err(err) = registry.apply_delta(delta).await {
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
                    .merge_full_sync(
                        local_actors.into_iter().collect(),
                        known_actors.into_iter().collect(),
                        sender_peer_id.clone(),
                        sender_socket_addr,
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
