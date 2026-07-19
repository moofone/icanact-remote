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

        // Guard 2 (defense-in-depth self-dial short-circuit): if the
        // target's identity is already known and it is our own, refuse
        // immediately — never enter the eviction-for-room, tie-break,
        // should_keep_connection, or wait-for-preferred-inbound machinery
        // for a self-dial. Guard 1 (identity-keyed self-filtering in
        // GossipRegistry::on_peer_list_gossip / add_peer_with_node_id)
        // should prevent a self-dial from ever being attempted in the first
        // place; this is the last-resort chokepoint for any other path that
        // might still hand us our own peer_id (e.g. a stale configured-peer
        // entry). should_keep_connection already returns `false` for self
        // in both directions unconditionally, so without this guard a
        // self-dial would free-run outbound_connect_wait_preferred_inbound
        // -> outbound_connect_preferred_inbound_timeout_fallback_dial every
        // `connection_timeout`, forever — wait_for_preferred_connection can
        // never see a legitimately "preferred" inbound from ourselves. This
        // check is purely identity-keyed (PeerId equality) so it can never
        // misfire on a real, distinct peer, and it does not touch
        // should_keep_connection's NodeId-ordering tie-break at all.
        if let Some(node_id_value) = resolved_node_id.as_ref() {
            let target_peer_id = crate::PeerId::from(node_id_value);
            if let Some(registry_arc) = registry_weak.upgrade()
                && target_peer_id == registry_arc.peer_id
            {
                warn!(
                    target: "icanact_remote_lifecycle",
                    attempt_id,
                    addr = %addr,
                    peer_id = %target_peer_id,
                    "outbound_connect_refused_self_dial"
                );
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "refusing to dial self peer_id",
                )));
            }
        }

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
            // Computed once and reused both for the existing-connection
            // tie-break immediately below and the preferred-inbound
            // suppression check further down (`should_keep_connection(peer,
            // true)` is a pure function of identity, so both call sites
            // asking the identical question must see the identical answer —
            // and this avoids a redundant call).
            let keep_outbound_dial = registry_arc.should_keep_connection(&remote_peer_id, true);
            if let Some(existing_conn) = self.get_connection_by_peer_id(&remote_peer_id) {
                let alive = if let Some(stream_handle) = existing_conn.stream_handle.as_ref() {
                    existing_conn.is_connected() && !stream_handle.exit_flag.load(Ordering::Acquire)
                } else {
                    false
                };
                if !alive {
                    // Routed through the shared identity-only chokepoint
                    // (`resolve_connection_conflict`). Both outcomes it can
                    // produce for a stale/dead rival (`AcceptIncoming` /
                    // `EvictStaleRejectIncoming`) lead to the identical action
                    // here — evict — because this call site defers the actual
                    // dial-vs-wait-for-preferred-inbound decision to the
                    // `keep_outbound_dial` check below, regardless of which of
                    // the two decisions comes back.
                    let decision = resolve_connection_conflict(false, false, keep_outbound_dial);
                    debug_assert!(
                        matches!(
                            decision,
                            ConnectionConflictDecision::AcceptIncoming
                                | ConnectionConflictDecision::EvictStaleRejectIncoming
                        ),
                        "resolve_connection_conflict must never return a live-rival decision \
                         when existing_usable=false, got {decision:?}"
                    );
                    info!(
                        target: "icanact_remote_lifecycle",
                        attempt_id,
                        remote = %remote_peer_id,
                        addr = %existing_conn.addr,
                        "outbound_tiebreak_evict_stale"
                    );
                    // Instance-scoped, never address-keyed: `existing_conn` was
                    // observed dead/stale above, but a fresh preferred
                    // connection can be reindexed at the exact same bind
                    // address between that aliveness check and this eviction
                    // (e.g. a concurrent inbound accept). A plain
                    // `remove_connection(existing_conn.addr)` would delete
                    // whatever is *currently* at that address — the fresh
                    // session, not the stale instance actually being retired.
                    // `disconnect_connection_instance` re-validates by `Arc`
                    // identity immediately before acting and is a no-op if
                    // `existing_conn` has already been superseded.
                    let _ = self.disconnect_connection_instance(&remote_peer_id, &existing_conn);
                } else {
                    // NOT routed through `resolve_connection_conflict` — see
                    // that function's doc comment ("Explicitly-justified
                    // exceptions") for the precise reason: this is a
                    // one-input question ("is the connection I already hold
                    // still the tie-break-correct one to keep"), asked before
                    // any concrete incoming candidate exists. Synthesizing
                    // `keep_outbound_dial` as a second input here would be
                    // dishonest — it can be `false` (this side is the
                    // higher-NodeId side) while the live existing outbound
                    // must still be evicted as wrong-direction regardless, and
                    // the chokepoint's formula would then wrongly reuse it.
                    let keep_existing = registry_arc.should_keep_connection(
                        &remote_peer_id,
                        existing_conn.direction == ConnectionDirection::Outbound,
                    );
                    if keep_existing {
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
                    }
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
                    // Instance-scoped, never peer-wide: `keep_existing` was
                    // computed against this specific `existing_conn`, but a
                    // fresh preferred inbound can be published/reindexed for
                    // this peer between that decision and this eviction
                    // (e.g. a concurrent inbound accept winning the tie-break
                    // first). A plain `disconnect_connection_by_peer_id`
                    // tears down "whatever is currently indexed" for the
                    // peer — which would be that fresh session, not the
                    // wrong-direction instance actually being retired here.
                    // `disconnect_connection_instance` re-validates by `Arc`
                    // identity immediately before acting and is a no-op if
                    // `existing_conn` has already been superseded.
                    let _ = self.disconnect_connection_instance(&remote_peer_id, &existing_conn);
                    // Arm the storm-prevention cooldown: this is a direct,
                    // local observation of a duplicate-connection conflict
                    // (not a generic socket failure), so it is safe and
                    // narrow to gate this peer's *next* reconnect attempt on
                    // it — see `GossipRegistry::note_tie_break_eviction`.
                    registry_arc.note_tie_break_eviction(&remote_peer_id);
                }
            }

            if !keep_outbound_dial {
                // Bound the preferred-inbound wait by `preferred_inbound_wait`,
                // NOT `connection_timeout`. Under the configured-peer supervisor
                // each reconnect attempt is wrapped in a bounded budget
                // (`min(connection_timeout, 900ms)`); if this wait were the full
                // `connection_timeout` (10s default) the budget would cancel it
                // every tick and the higher-NodeId side would never reach the
                // fallback dial below — it would stall for the whole
                // `connection_timeout` waiting for the peer to dial in. That is
                // the SWIM Dead-verdict reconnect amplifier: a falsely-`Dead`
                // peer whose session a consumer tore down cannot re-establish
                // inside the consumer's disconnect-debounce window, so it is
                // re-torn-down before recovery. A short wait lets a single
                // supervisor tick wait out the window and still fall back to
                // dialing, so reconnect completes in ~1-2 ticks.
                let preferred_inbound_wait = registry_arc.config.preferred_inbound_wait;
                info!(
                    target: "icanact_remote_lifecycle",
                    attempt_id,
                    remote = %remote_peer_id,
                    addr = %addr,
                    timeout_ms = preferred_inbound_wait.as_millis(),
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
                        preferred_inbound_wait,
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

                    // Guard 2b (post-cert self-dial short-circuit, address-only
                    // path): Guard 2 above only fires when `resolved_node_id`
                    // is already populated *before* dialing. For an
                    // address-only outbound (bootstrap/configured-seed
                    // mistake, a DNS refresh that lands on a self address, or
                    // a stale connections_by_addr/discovery entry with no
                    // node_id attached), `resolved_node_id` is `None` on
                    // entry and Guard 2 is skipped entirely — the dial
                    // proceeds through TCP/TLS and `discovered_node_id` is
                    // only known now, from the peer's cert-verified identity
                    // above. Re-check identity here, immediately after the
                    // cert-derived identity becomes available and strictly
                    // before the hello handshake / `finalize_new_outbound_connection`,
                    // so a self-dial can never be indexed, published, or
                    // counted under this registry's own PeerId. Terminal
                    // error — this returns directly out of the connect
                    // future, so no wait-for-preferred-inbound / fallback /
                    // retry machinery is armed for it.
                    if let Some(node_id_value) = discovered_node_id.as_ref() {
                        let discovered_peer_id = crate::PeerId::from(node_id_value);
                        if discovered_peer_id == registry_arc.peer_id {
                            warn!(
                                target: "icanact_remote_lifecycle",
                                attempt_id,
                                addr = %addr,
                                peer_id = %discovered_peer_id,
                                "outbound_connect_refused_self_dial_post_cert"
                            );
                            return Err(GossipError::Network(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "refusing to dial self peer_id",
                            )));
                        }
                    }

                    let negotiated_alpn = tls_stream.get_ref().1.alpn_protocol().map(|proto| proto.to_vec());
                    let hello_started = Instant::now();
                    let peer_caps = match crate::handshake::perform_hello_handshake(
                        &mut tls_stream,
                        negotiated_alpn.as_deref(),
                        registry_arc.config.enable_peer_discovery,
                        registry_arc.config.schema_hash,
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
