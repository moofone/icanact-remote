use super::*;

static OUTBOUND_CONNECT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

impl<T> ConnectionPool<T> {
    pub(super) async fn connect_via_stream(
        &self,
        mut addr: SocketAddr,
        resolved_node_id: Option<crate::GossipNodeId>,
        max_connections: usize,
        connection_timeout: Duration,
        registry_weak: std::sync::Weak<GossipRegistry>,
    ) -> Result<ConnectionHandle<T>> {
        let attempt_id = OUTBOUND_CONNECT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed);
        let requested_addr = addr;
        let remote_node_id = resolved_node_id
            .as_ref()
            .map(|node_id| node_id.fmt_short())
            .unwrap_or_else(|| "unknown".to_string());
        info!(
            target: "icanact_remote_lifecycle",
            attempt_id,
            addr = %addr,
            remote_node_id = %remote_node_id,
            timeout_ms = connection_timeout.as_millis(),
            "outbound_connect_start"
        );

        // Make room if necessary - evict the least-recently-used connection
        // that is safe to drop. Configured/required peers are never chosen, so
        // a new (often transient/discovered) dial cannot disconnect a live
        // cluster member to fit under the pool cap.
        if self.connections_by_addr.len() >= max_connections {
            if let Some(oldest_addr) = self.select_lru_eviction_victim() {
                let _ = self.remove_connection(oldest_addr);
                warn!(addr = %oldest_addr, "removed oldest connection to make room");
            }
        }

        // Duplicate connection tie-breaker: decide whether to reuse an existing link
        if let (Some(registry_arc), Some(node_id_value)) =
            (registry_weak.upgrade(), resolved_node_id.as_ref())
        {
            let remote_peer_id = crate::PeerId::from(node_id_value);
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::OutboundStart {
                    peer: Some(remote_peer_id.clone()),
                    addr,
                    attempt_id,
                },
            );
            if let Some(existing_conn) = self.get_connection_by_peer_id(&remote_peer_id) {
                let alive = if let Some(stream_handle) = existing_conn.stream_handle.as_ref() {
                    existing_conn.is_connected() && !stream_handle.exit_flag.load(Ordering::Acquire)
                } else {
                    false
                };
                if !alive {
                    info!(
                        target: "icanact_remote_lifecycle",
                        attempt_id,
                        remote = %remote_peer_id,
                        addr = %existing_conn.addr,
                        "outbound_tiebreak_evict_stale"
                    );
                    let _ = self.remove_connection(existing_conn.addr);
                } else if registry_arc.should_keep_connection(
                    &remote_peer_id,
                    existing_conn.direction == ConnectionDirection::Outbound,
                ) {
                    info!(
                        target: "icanact_remote_lifecycle",
                        attempt_id,
                        remote = %remote_peer_id,
                        addr = %existing_conn.addr,
                        "outbound_tiebreak_reuse_existing"
                    );
                    if let Some(handle) =
                        self.make_connection_handle(existing_conn.addr, &existing_conn)
                    {
                        return Ok(handle);
                    }
                    return Err(GossipError::Network(std::io::Error::other(
                        "Existing connection missing writer handle",
                    )));
                } else {
                    info!(
                        target: "icanact_remote_lifecycle",
                        attempt_id,
                        remote = %remote_peer_id,
                        addr = %existing_conn.addr,
                        existing_direction = ?existing_conn.direction,
                        "outbound_tiebreak_evict_wrong_direction"
                    );
                    crate::lifecycle::record_transport_event(
                        crate::lifecycle::TransportLifecycleEvent::WrongDirectionEvicted {
                            peer: remote_peer_id.clone(),
                            addr: existing_conn.addr,
                            direction: match existing_conn.direction {
                                ConnectionDirection::Inbound => {
                                    crate::lifecycle::TransportDirection::Inbound
                                }
                                ConnectionDirection::Outbound => {
                                    crate::lifecycle::TransportDirection::Outbound
                                }
                            },
                        },
                    );
                    let _ = self.disconnect_connection_by_peer_id(&remote_peer_id);
                    // Arm the storm-prevention cooldown: this is a direct,
                    // local observation of a duplicate-connection conflict
                    // (not a generic socket failure), so it is safe and
                    // narrow to gate this peer's *next* reconnect attempt on
                    // it — see `GossipRegistry::note_tie_break_eviction`.
                    registry_arc.note_tie_break_eviction(&remote_peer_id);
                }
            }

            if !registry_arc.should_keep_connection(&remote_peer_id, true) {
                info!(
                    target: "icanact_remote_lifecycle",
                    attempt_id,
                    remote = %remote_peer_id,
                    addr = %addr,
                    timeout_ms = connection_timeout.as_millis(),
                    "outbound_connect_wait_preferred_inbound"
                );
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::OutboundSuppressedWaitInbound {
                        peer: remote_peer_id.clone(),
                        addr,
                        attempt_id,
                    },
                );
                if let Some(handle) = self
                    .wait_for_preferred_connection(
                        &remote_peer_id,
                        &registry_arc,
                        connection_timeout,
                    )
                    .await
                {
                    info!(
                        target: "icanact_remote_lifecycle",
                        attempt_id,
                        remote = %remote_peer_id,
                        addr = %handle.addr,
                        "outbound_connect_preferred_inbound_ready"
                    );
                    crate::lifecycle::record_transport_event(
                        crate::lifecycle::TransportLifecycleEvent::OutboundSuppressedInboundReady {
                            peer: remote_peer_id,
                            addr: handle.addr,
                            attempt_id,
                        },
                    );
                    return Ok(handle);
                }
                // Storm guard: the preferred-inbound wait just timed out,
                // which is expected during normal asymmetric bootstrap but is
                // also exactly what happens on every tick of a tie-break
                // oscillation (dial, get evicted/rejected almost instantly,
                // wait, time out, dial again — see
                // `GossipRegistry::note_tie_break_eviction`). If a
                // connection to this peer died very recently, do not fall
                // back to a fresh dial this call; let the caller's own retry
                // cadence (bounded by `tie_break_reconnect_cooldown`) try
                // again shortly instead of hammering TCP/TLS on every call.
                if registry_arc.tie_break_cooldown_active(&remote_peer_id) {
                    info!(
                        target: "icanact_remote_lifecycle",
                        attempt_id,
                        remote = %remote_peer_id,
                        addr = %addr,
                        "outbound_connect_preferred_inbound_timeout_cooldown_active_skip_dial"
                    );
                    return Err(GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "tie-break reconnect cooldown active; skipping fallback dial",
                    )));
                }
                info!(
                    target: "icanact_remote_lifecycle",
                    attempt_id,
                    remote = %remote_peer_id,
                    addr = %addr,
                    "outbound_connect_preferred_inbound_timeout_fallback_dial"
                );
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::OutboundSuppressedInboundTimeout {
                        peer: remote_peer_id,
                        addr,
                        attempt_id,
                    },
                );
            }
        }

        let mut dns_refreshed = false;
        loop {
            let attempt_started = Instant::now();
            let attempt: Result<ConnectionHandle<T>> =
                match tokio::time::timeout(connection_timeout, async {
                    debug!("CONNECTION POOL: Attempting to connect to {}", addr);
                    let tcp_started = Instant::now();
                    let stream = match TcpStream::connect(addr).await {
                        Ok(stream) => {
                            info!(
                                target: "icanact_remote_lifecycle",
                                attempt_id,
                                addr = %addr,
                                elapsed_ms = tcp_started.elapsed().as_millis(),
                                "tcp_connect_ok"
                            );
                            stream
                        }
                        Err(e) => {
                            warn!(
                                target: "icanact_remote_lifecycle",
                                attempt_id,
                                addr = %addr,
                                elapsed_ms = tcp_started.elapsed().as_millis(),
                                error = %e,
                                "tcp_connect_failed"
                            );
                            debug!(
                                "CONNECTION POOL: Connection to {} failed: {} (will retry in {}s if this is a gossip peer)",
                                addr, e, 5
                            );
                            return Err(GossipError::Network(e));
                        }
                    };
                    debug!("CONNECTION POOL: Successfully connected to {}", addr);

                    stream.set_nodelay(true).map_err(GossipError::Network)?;

                    let registry_arc = registry_weak.upgrade().ok_or(GossipError::Shutdown)?;
                    crate::net::apply_tcp_keepalive(&stream, &registry_arc.config);

                    let tls_config = registry_arc.tls_config.clone().ok_or_else(|| {
                        GossipError::TlsConfigError(format!(
                            "TLS is required but not configured (addr={})",
                            addr
                        ))
                    })?;

                    let mut discovered_node_id = match resolved_node_id {
                        Some(node_id) => Some(node_id),
                        None => registry_arc.lookup_node_id(&addr).await,
                    };

                    let (server_name, server_name_label) = if let Some(node_id) = discovered_node_id {
                        let dns_name = crate::tls::name::encode(&node_id);
                        let server_name = rustls::pki_types::ServerName::try_from(dns_name)
                            .map_err(|e| GossipError::TlsError(format!("Invalid DNS name: {}", e)))?;
                        (server_name, format!("GossipNodeId {}", node_id.fmt_short()))
                    } else {
                        let placeholder = format!("peer-{}.icanact.invalid", addr.port());
                        let server_name = rustls::pki_types::ServerName::try_from(placeholder.clone())
                            .map_err(|e| {
                                GossipError::TlsError(format!("Invalid fallback DNS name: {}", e))
                            })?;
                        (
                            server_name,
                            format!("placeholder SNI {} (GossipNodeId unknown)", placeholder),
                        )
                    };

                    debug!(
                        addr = %addr,
                        server_name = %server_name_label,
                        "stream connect: performing TLS handshake"
                    );
                    let connector = tls_config.connector();
                    let tls_started = Instant::now();
                    let mut tls_stream = match connector.connect(server_name, stream).await {
                        Ok(tls_stream) => {
                            info!(
                                target: "icanact_remote_lifecycle",
                                attempt_id,
                                addr = %addr,
                                server_name = %server_name_label,
                                elapsed_ms = tls_started.elapsed().as_millis(),
                                "tls_handshake_ok"
                            );
                            tls_stream
                        }
                        Err(e) => {
                            warn!(
                                target: "icanact_remote_lifecycle",
                                attempt_id,
                                addr = %addr,
                                server_name = %server_name_label,
                                elapsed_ms = tls_started.elapsed().as_millis(),
                                error = %e,
                                "tls_handshake_failed"
                            );
                            return Err(GossipError::TlsError(format!(
                                "TLS handshake failed: {}",
                                e
                            )));
                        }
                    };

                    if discovered_node_id.is_none() {
                        if let Some(certs) = tls_stream.get_ref().1.peer_certificates() {
                            if let Some(cert) = certs.first() {
                                if let Ok(node_id) = crate::tls::extract_node_id_from_cert(cert) {
                                    if registry_arc.lookup_node_id(&addr).await.is_none() {
                                        registry_arc.add_peer_with_node_id(addr, Some(node_id)).await;
                                    }
                                    discovered_node_id = Some(node_id);
                                }
                            }
                        }
                    }

                    let negotiated_alpn = tls_stream.get_ref().1.alpn_protocol().map(|proto| proto.to_vec());
                    let hello_started = Instant::now();
                    let peer_caps = match crate::handshake::perform_hello_handshake(
                        &mut tls_stream,
                        negotiated_alpn.as_deref(),
                        registry_arc.config.enable_peer_discovery,
                    )
                    .await
                    {
                        Ok(peer_caps) => {
                            info!(
                                target: "icanact_remote_lifecycle",
                                attempt_id,
                                addr = %addr,
                                elapsed_ms = hello_started.elapsed().as_millis(),
                                "hello_handshake_ok"
                            );
                            peer_caps
                        }
                        Err(e) => {
                            warn!(
                                target: "icanact_remote_lifecycle",
                                attempt_id,
                                addr = %addr,
                                elapsed_ms = hello_started.elapsed().as_millis(),
                                error = %e,
                                "hello_handshake_failed"
                            );
                            return Err(e);
                        }
                    };
                    registry_arc.set_peer_capabilities(addr, peer_caps);

                    let associated_node_id = match discovered_node_id {
                        Some(node_id) => Some(node_id),
                        None => registry_arc.lookup_node_id(&addr).await,
                    };
                    if let Some(node_id) = associated_node_id {
                        registry_arc
                            .associate_peer_capabilities_with_node(addr, node_id)
                            .await;
                        registry_arc.mark_peer_connected(addr).await;
                    }

                    let finalize_started = Instant::now();
                    let result = self
                        .finalize_new_outbound_connection(
                            addr,
                            tls_stream,
                            registry_weak.clone(),
                            // R2: bind the connection identity to the GossipNodeId we
                            // extracted from the peer's verified TLS cert when no
                            // GossipNodeId was pinned (bootstrap/placeholder-SNI dials).
                            discovered_node_id,
                        )
                        .await;
                    match &result {
                        Ok(_) => info!(
                            target: "icanact_remote_lifecycle",
                            attempt_id,
                            requested_addr = %requested_addr,
                            final_addr = %addr,
                            finalize_ms = finalize_started.elapsed().as_millis(),
                            total_ms = attempt_started.elapsed().as_millis(),
                            "outbound_connect_ready"
                        ),
                        Err(e) => warn!(
                            target: "icanact_remote_lifecycle",
                            attempt_id,
                            requested_addr = %requested_addr,
                            final_addr = %addr,
                            finalize_ms = finalize_started.elapsed().as_millis(),
                            total_ms = attempt_started.elapsed().as_millis(),
                            error = %e,
                            "outbound_connect_finalize_failed"
                        ),
                    }
                    result
                })
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        warn!(
                            target: "icanact_remote_lifecycle",
                            attempt_id,
                            requested_addr = %requested_addr,
                            final_addr = %addr,
                            timeout_ms = connection_timeout.as_millis(),
                            total_ms = attempt_started.elapsed().as_millis(),
                            "outbound_connect_timeout"
                        );
                        Err(GossipError::Timeout)
                    }
                };

            match attempt {
                Ok(handle) => return Ok(handle),
                Err(e) => {
                    if !dns_refreshed {
                        if let Some(registry_arc) = registry_weak.upgrade() {
                            if let Some(new_addr) = registry_arc.refresh_peer_dns(addr).await {
                                info!(
                                    target: "icanact_remote_lifecycle",
                                    attempt_id,
                                    old_addr = %addr,
                                    new_addr = %new_addr,
                                    error = %e,
                                    "outbound_connect_dns_refreshed"
                                );
                                addr = new_addr;
                                dns_refreshed = true;
                                continue;
                            }
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn wait_for_preferred_connection(
        &self,
        remote_peer_id: &crate::PeerId,
        registry: &GossipRegistry,
        timeout: Duration,
    ) -> Option<ConnectionHandle<T>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(conn) = self.get_connection_by_peer_id(remote_peer_id) {
                let is_outbound = conn.direction == ConnectionDirection::Outbound;
                if registry.should_keep_connection(remote_peer_id, is_outbound) {
                    if let Some(handle) = self.make_connection_handle(conn.addr, &conn) {
                        return Some(handle);
                    }
                }
            }

            if Instant::now() >= deadline {
                return None;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
