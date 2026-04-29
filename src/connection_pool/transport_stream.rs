use super::*;

impl<T> ConnectionPool<T> {
    pub(super) async fn connect_via_stream(
        &self,
        mut addr: SocketAddr,
        resolved_node_id: Option<crate::NodeId>,
        max_connections: usize,
        connection_timeout: Duration,
        registry_weak: std::sync::Weak<GossipRegistry>,
    ) -> Result<ConnectionHandle<T>> {
        // Make room if necessary - evict the least-recently-used connection.
        if self.connections_by_addr.len() >= max_connections {
            let mut oldest: Option<(SocketAddr, usize)> = None;
            self.connections_by_addr.iter_sync(|addr, conn| {
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

            if let Some((oldest_addr, _)) = oldest {
                let _ = self.remove_connection(oldest_addr);
                warn!(addr = %oldest_addr, "removed oldest connection to make room");
            }
        }

        // Duplicate connection tie-breaker: decide whether to reuse an existing link
        if let (Some(registry_arc), Some(node_id_value)) =
            (registry_weak.upgrade(), resolved_node_id.as_ref())
        {
            let remote_peer_id = crate::PeerId::from(node_id_value);
            if let Some(existing_conn) = self.get_connection_by_peer_id(&remote_peer_id) {
                let alive = if let Some(stream_handle) = existing_conn.stream_handle.as_ref() {
                    existing_conn.is_connected() && !stream_handle.exit_flag.load(Ordering::Acquire)
                } else {
                    false
                };
                if !alive {
                    debug!(
                        remote = %remote_peer_id,
                        addr = %existing_conn.addr,
                        "tie-breaker: evicting stale existing connection before dialing"
                    );
                    let _ = self.remove_connection(existing_conn.addr);
                } else if !registry_arc.should_keep_connection(&remote_peer_id, true) {
                    debug!(
                        remote = %remote_peer_id,
                        "tie-breaker: reusing existing connection instead of dialing outbound"
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
                    debug!(
                        remote = %remote_peer_id,
                        "tie-breaker: existing live connection already satisfies outbound policy; reusing it"
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
            }
        }

        let mut dns_refreshed = false;
        loop {
            let attempt: Result<ConnectionHandle<T>> = async {
                debug!("CONNECTION POOL: Attempting to connect to {}", addr);
                let stream = tokio::time::timeout(connection_timeout, TcpStream::connect(addr))
                    .await
                    .map_err(|_| {
                        debug!(
                            "CONNECTION POOL: Connection to {} timed out after {:?}",
                            addr, connection_timeout
                        );
                        GossipError::Timeout
                    })?
                    .map_err(|e| {
                        debug!(
                            "CONNECTION POOL: Connection to {} failed: {} (will retry in {}s if this is a gossip peer)",
                            addr, e, 5
                        );
                        GossipError::Network(e)
                    })?;
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
                    (server_name, format!("NodeId {}", node_id.fmt_short()))
                } else {
                    let placeholder = format!("peer-{}.icanact.invalid", addr.port());
                    let server_name = rustls::pki_types::ServerName::try_from(placeholder.clone())
                        .map_err(|e| {
                            GossipError::TlsError(format!("Invalid fallback DNS name: {}", e))
                        })?;
                    (
                        server_name,
                        format!("placeholder SNI {} (NodeId unknown)", placeholder),
                    )
                };

                debug!(
                    addr = %addr,
                    server_name = %server_name_label,
                    "stream connect: performing TLS handshake"
                );
                let connector = tls_config.connector();
                let mut tls_stream =
                    tokio::time::timeout(connection_timeout, connector.connect(server_name, stream))
                        .await
                        .map_err(|_| GossipError::Timeout)?
                        .map_err(|e| {
                            GossipError::TlsError(format!("TLS handshake failed: {}", e))
                        })?;

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
                let peer_caps = tokio::time::timeout(
                    connection_timeout,
                    crate::handshake::perform_hello_handshake(
                        &mut tls_stream,
                        negotiated_alpn.as_deref(),
                        registry_arc.config.enable_peer_discovery,
                    ),
                )
                .await
                .map_err(|_| GossipError::Timeout)??;
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

                self.finalize_new_outbound_connection(addr, tls_stream, registry_weak.clone())
                    .await
            }
            .await;

            match attempt {
                Ok(handle) => return Ok(handle),
                Err(e) => {
                    if !dns_refreshed {
                        if let Some(registry_arc) = registry_weak.upgrade() {
                            if let Some(new_addr) = registry_arc.refresh_peer_dns(addr).await {
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
}
