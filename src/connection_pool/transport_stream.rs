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
                    let correlation = self.get_or_create_correlation_tracker(&remote_peer_id);
                    if let Some(handle) =
                        self.make_connection_handle(existing_conn.addr, &existing_conn, correlation)
                    {
                        return Ok(handle);
                    }
                    return Err(GossipError::Network(std::io::Error::other(
                        "Existing connection missing writer handle",
                    )));
                } else {
                    debug!(
                        remote = %remote_peer_id,
                        "tie-breaker: replacing existing connection with outbound dial"
                    );
                    if let Some(removed) = self.disconnect_connection_by_peer_id(&remote_peer_id) {
                        if let Some(handle) = removed.stream_handle.as_ref() {
                            handle.shutdown();
                        }
                    }
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

                let _ = resolved_node_id;
                let _ = registry_arc;
                let _ = stream;
                Err(GossipError::InvalidConfig(format!(
                    "stream transport auth is no longer implemented in core (addr={}); use icanact-remote-transports",
                    addr
                )))
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
