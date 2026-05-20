use std::marker::PhantomData;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use crate::aligned::{AlignedBuffer, AlignedBytes};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream, UdpSocket},
    time::{Instant, interval},
};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    GossipConfig, GossipError, RegistrationPriority, RemoteActorLocation, Result,
    registry::{GossipRegistry, GossipResult, GossipTask, RegistryMessage, RegistryStats},
    transport::{RegistryTransportBootstrap, TransportWireKind},
};

const REGISTRY_MESSAGE_ALIGNMENT: usize = {
    let message_align = std::mem::align_of::<rkyv::Archived<RegistryMessage>>();
    let location_align = std::mem::align_of::<rkyv::Archived<RemoteActorLocation>>();
    if message_align > location_align {
        message_align
    } else {
        location_align
    }
};
type RegistryAlignedVec = rkyv::util::AlignedVec<{ REGISTRY_MESSAGE_ALIGNMENT }>;

#[inline]
fn next_gossip_deadline(now: Instant, gossip_interval: Duration, jitter: Duration) -> Instant {
    now + gossip_interval + jitter
}

#[inline]
fn decode_registry_message(
    payload: &[u8],
) -> std::result::Result<RegistryMessage, rkyv::rancor::Error> {
    fn decode_from_aligned_bytes(
        bytes: &[u8],
    ) -> std::result::Result<RegistryMessage, rkyv::rancor::Error> {
        let archived = rkyv::access::<
            <RegistryMessage as rkyv::Archive>::Archived,
            rkyv::rancor::Error,
        >(bytes)?;
        let mut pool = rkyv::de::Pool::new();
        let deserializer = rkyv::rancor::Strategy::wrap(&mut pool);
        rkyv::Deserialize::deserialize(archived, deserializer)
    }

    if is_registry_payload_aligned(payload) {
        decode_from_aligned_bytes(payload)
    } else {
        // Ensure proper alignment for rkyv archived access.
        let mut aligned = RegistryAlignedVec::with_capacity(payload.len());
        aligned.extend_from_slice(payload);
        decode_from_aligned_bytes(aligned.as_ref())
    }
}

#[inline]
fn is_registry_payload_aligned(payload: &[u8]) -> bool {
    let ptr = payload.as_ptr() as usize;
    ptr.is_multiple_of(REGISTRY_MESSAGE_ALIGNMENT)
}

/// Main API for the gossip registry with vector clocks and separated locks
pub struct GossipRegistryHandle<T = crate::BuilderTlsBootstrap> {
    pub registry: Arc<GossipRegistry>,
    _server_handle: Option<tokio::task::JoinHandle<()>>,
    _timer_handle: Option<tokio::task::JoinHandle<()>>,
    _monitor_handle: Option<tokio::task::JoinHandle<()>>,
    _marker: PhantomData<fn() -> T>,
}

/// Cloneable client view for performing peer lookups and sending messages via the underlying
/// registry/connection pool.
///
/// This intentionally does **not** own server/task lifetimes; it is safe to clone and use from
/// service handlers (e.g. feature-gated relay protocols).
#[derive(Clone)]
pub struct GossipClient<T = ()> {
    registry: Arc<GossipRegistry>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Drop for GossipRegistryHandle<T> {
    fn drop(&mut self) {
        // If the handle is dropped without an explicit shutdown, the runtime would otherwise
        // keep the accept loop + timer alive and the process may require multiple SIGINTs
        // to terminate. Aborting here makes example binaries exit cleanly on Ctrl+C.
        if let Some(handle) = self._server_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self._timer_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self._monitor_handle.take() {
            handle.abort();
        }
    }
}

impl<T> GossipRegistryHandle<T> {
    /// Create and start a new gossip registry using a compile-time transport stack.
    pub async fn new_with_transport_stack(
        bind_addr: SocketAddr,
        secret_key: crate::SecretKey,
        config: Option<GossipConfig>,
        transport_stack: T,
    ) -> Result<Self>
    where
        T: RegistryTransportBootstrap,
    {
        let mut config = config.unwrap_or_default();
        if let Some(policy) = transport_stack.connection_recovery_policy() {
            config.connection_recovery = policy;
        }
        transport_stack.prepare_config(&secret_key, &mut config)?;

        let (listener, udp_socket, actual_bind_addr) = match transport_stack.wire_kind() {
            TransportWireKind::TcpStream => {
                // Create the TCP listener first to get the actual bound address.
                //
                // We set `SO_REUSEADDR` so tests and local dev can restart a server on the same
                // port without spurious `AddrInUse` (common on macOS due to TIME_WAIT).
                let listener = bind_with_reuseaddr(bind_addr)?;
                let actual_bind_addr = listener.local_addr()?;
                (Some(listener), None, actual_bind_addr)
            }
            TransportWireKind::UdpDatagram => {
                return Err(GossipError::InvalidConfig(
                    "UDP datagram transport is disabled because plaintext datagrams cannot authenticate peer identity"
                        .to_string(),
                ));
            }
        };

        // Create registry and let the selected transport stack configure it.
        let mut registry = GossipRegistry::<()>::new(actual_bind_addr, config.clone());
        transport_stack.configure_registry(&mut registry, secret_key)?;
        let registry = Arc::new(registry);

        // Set the registry reference in the connection pool
        {
            let pool = &registry.connection_pool;
            pool.set_registry(registry.clone());
        }

        if let Some(socket) = udp_socket.clone() {
            registry.connection_pool.set_udp_socket(socket);
        }

        // Start the server with the selected wire transport
        let server_registry = registry.clone();
        let server_handle = match (listener, udp_socket) {
            (Some(listener), None) => tokio::spawn(async move {
                if let Err(err) = start_gossip_server_with_listener(server_registry, listener).await
                {
                    error!(error = %err, "server error");
                }
            }),
            (None, Some(socket)) => tokio::spawn(async move {
                if let Err(err) = start_gossip_server_with_udp_socket(server_registry, socket).await
                {
                    error!(error = %err, "udp server error");
                }
            }),
            _ => {
                return Err(GossipError::Network(std::io::Error::other(
                    "invalid transport bootstrap wiring",
                )));
            }
        };

        // Start the gossip timer
        let timer_registry = registry.clone();
        let timer_handle = tokio::spawn(async move {
            start_gossip_timer(timer_registry).await;
        });

        // Connection monitoring is now done in the gossip timer
        let monitor_handle = None;

        // Log startup with DNS gossip mode status
        let dns_mode = config.advertise_dns.as_deref().unwrap_or("disabled");
        info!(
            bind_addr = %actual_bind_addr,
            advertise_dns = dns_mode,
            transport = transport_stack.stack_name(),
            "gossip registry started"
        );

        Ok(Self {
            registry,
            _server_handle: Some(server_handle),
            _timer_handle: Some(timer_handle),
            _monitor_handle: monitor_handle,
            _marker: PhantomData,
        })
    }

    /// Register a local actor
    pub async fn register(&self, name: String, address: SocketAddr) -> Result<()> {
        let location = RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        self.registry.register_actor(name, location).await
    }

    /// Register a local actor with metadata
    pub async fn register_with_metadata(
        &self,
        name: String,
        address: SocketAddr,
        metadata: Vec<u8>,
    ) -> Result<()> {
        let location = RemoteActorLocation::new_with_metadata(
            address,
            self.registry.peer_id.clone(),
            metadata,
        );
        self.registry.register_actor(name, location).await
    }

    /// Register a local actor with high priority (faster propagation)
    pub async fn register_urgent(
        &self,
        name: String,
        address: SocketAddr,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let mut location =
            RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        location.priority = priority;
        self.registry
            .register_actor_with_priority(name, location, priority)
            .await
    }

    /// Register a local actor with specified priority
    pub async fn register_with_priority(
        &self,
        name: String,
        address: SocketAddr,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let mut location =
            RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        location.priority = priority;
        self.registry
            .register_actor_with_priority(name, location, priority)
            .await
    }

    /// Unregister a local actor
    pub async fn unregister(&self, name: &str) -> Result<Option<RemoteActorLocation>> {
        self.registry.unregister_actor(name).await
    }

    /// Lookup an actor and return a RemoteActorRef with cached connection.
    ///
    /// This does ALL the work in one call:
    /// - Finds the actor in the registry
    /// - Gets the connection handle (cached in pool)
    /// - Returns RemoteActorRef for zero-lookup message sending
    ///
    /// # Example
    /// ```ignore
    /// // Step 1: Lookup does ALL the work - finds actor AND caches connection
    /// let remote_actor = registry.lookup("chat_service").await?;
    ///
    /// // Step 2: tell/ask use cached connection - ZERO lookups, just pointer deref
    /// remote_actor.tell(message1).await?;
    /// remote_actor.ask(request).await?;
    /// ```
    pub async fn lookup(&self, name: &str) -> Option<crate::RemoteActorRef> {
        // Step 1: Find the actor location
        let location = self.registry.lookup_actor(name).await?;

        // Step 2: Get the connection to the peer hosting this actor
        // Use peer_id to get existing connection, not the actor's address
        let conn = if location.peer_id == self.registry.peer_id {
            // Actor is local - try to get connection to the actor's address
            // If it's not listening yet, return None (connection will be optional)
            let addr: SocketAddr = location.address.parse().ok()?;
            self.registry.get_connection(addr).await.ok()
        } else {
            // Actor is remote - get connection to the peer
            self.get_connection_to_peer(&location.peer_id).await.ok()
        };
        let conn = conn.and_then(|conn| if conn.is_closed() { None } else { Some(conn) });

        // Step 3: Return RemoteActorRef with optional connection AND registry reference (for auto-reconnection)
        Some(crate::RemoteActorRef::with_registry(
            location,
            conn,
            self.registry.clone(),
        ))
    }

    /// Snapshot the currently-known actor directory as owned `(name, location)` pairs.
    ///
    /// Semantics (deterministic):
    /// - Includes both local and remote-known actors.
    /// - If the same name exists in both maps, the local entry wins.
    /// - Returned vector is sorted by name for stable debugging/tests.
    ///
    /// This is a control-plane API intended for building higher-level directory views.
    pub fn snapshot_known_actors(&self) -> Vec<(String, RemoteActorLocation)> {
        self.registry.snapshot_known_actors()
    }

    /// Get a cloneable client handle for lookups without taking ownership of server/task lifetimes.
    pub fn client(&self) -> GossipClient<T> {
        GossipClient {
            registry: Arc::clone(&self.registry),
            _marker: PhantomData,
        }
    }

    /// Disconnect the cached transport session for a peer, preserving configured peer address.
    pub fn disconnect_peer_connection(&self, peer_id: &crate::PeerId) -> bool {
        self.registry
            .connection_pool
            .disconnect_connection_by_peer_id(peer_id)
            .is_some()
    }

    pub fn peer_clock_snapshot(
        &self,
        peer_addr: &std::net::SocketAddr,
    ) -> Option<crate::registry::PeerClockSnapshot> {
        self.registry.peer_clock_snapshot(peer_addr)
    }

    pub fn peer_clock_snapshots(&self) -> Vec<crate::registry::PeerClockSnapshot> {
        self.registry.peer_clock_snapshots()
    }

    /// Get registry statistics including vector clock metrics
    pub async fn stats(&self) -> RegistryStats {
        self.registry.get_stats().await
    }

    /// Add a peer to the gossip network
    pub async fn add_peer(&self, peer_id: &crate::PeerId) -> crate::Peer {
        // Pre-configure the peer as allowed (address will be set when connect() is called)
        {
            let pool = &self.registry.connection_pool;
            // Use a placeholder address - will be updated when connect() is called.
            pool.set_configured_peer_addr(peer_id, "0.0.0.0:0".parse().unwrap());
        }

        crate::Peer {
            peer_id: peer_id.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Get a connection handle for direct communication (reuses existing pool connections)
    pub(crate) async fn get_connection(
        &self,
        addr: SocketAddr,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        self.registry.get_connection(addr).await
    }

    /// Get a connection handle by peer ID (ensures TLS NodeId is known)
    pub(crate) async fn get_connection_to_peer(
        &self,
        peer_id: &crate::PeerId,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        self.registry
            .connection_pool
            .get_connection_to_peer(peer_id)
            .await
    }

    /// Lookup a peer and return a RemoteActorRef for communicating with it.
    ///
    /// This is the primary entry point for sending messages to remote peers.
    /// It automatically manages connection pooling and reconnects if needed.
    pub async fn lookup_peer(&self, peer_id: &crate::PeerId) -> Result<crate::RemoteActorRef> {
        let conn = self.get_connection_to_peer(peer_id).await?;
        let addr = conn.addr;

        // Create a location for this peer
        let location = crate::RemoteActorLocation::new_with_peer(addr, peer_id.clone());

        // Return a RemoteActorRef linked to this registry
        Ok(crate::RemoteActorRef::with_registry(
            location,
            Some(conn),
            self.registry.clone(),
        ))
    }

    /// Lookup a peer by address and return a RemoteActorRef.
    ///
    /// Note: Prefer `lookup_peer` if possible as it ensures TLS identity verification.
    /// This method is primarily useful for testing or when only the address is known.
    pub async fn lookup_address(&self, addr: SocketAddr) -> Result<crate::RemoteActorRef> {
        let conn = self.get_connection(addr).await?;

        // Try to resolve the PeerId
        let peer_id = self
            .registry
            .connection_pool
            .get_peer_id_by_addr(&addr)
            .ok_or_else(|| {
                crate::GossipError::ActorNotFound(format!("No peer ID found for {}", addr))
            })?;

        let location = crate::RemoteActorLocation::new_with_peer(addr, peer_id);

        Ok(crate::RemoteActorRef::with_registry(
            location,
            Some(conn),
            self.registry.clone(),
        ))
    }

    /// Set the DNS name for a peer. When a peer has a DNS name configured,
    /// the gossip system will re-resolve the DNS to get the current IP address
    /// when attempting to reconnect after a connection failure.
    ///
    /// This is essential for Kubernetes deployments where pods may restart
    /// and get new IP addresses, but the Service DNS name remains stable.
    ///
    /// # Arguments
    /// * `peer_addr` - The current socket address of the peer
    /// * `dns_name` - The DNS name to use for re-resolution (e.g., "data-feeder-icanact:9400")
    ///
    /// # Example
    /// ```ignore
    /// // After connecting to a peer, set its DNS name for automatic re-resolution
    /// handle.set_peer_dns_name(resolved_addr, "data-feeder-icanact:9400".to_string()).await;
    /// ```
    pub async fn set_peer_dns_name(&self, peer_addr: std::net::SocketAddr, dns_name: String) {
        self.registry.set_peer_dns_name(peer_addr, dns_name).await;
    }

    /// Manually trigger DNS re-resolution for a peer.
    /// Returns the new address if the IP changed, None if unchanged or failed.
    pub async fn refresh_peer_dns(
        &self,
        peer_addr: std::net::SocketAddr,
    ) -> Option<std::net::SocketAddr> {
        self.registry.refresh_peer_dns(peer_addr).await
    }

    /// Bootstrap peer connections non-blocking (Phase 4)
    ///
    /// Dials seed peers asynchronously - doesn't block startup on gossip propagation.
    /// Failed connections are logged but don't prevent the node from starting.
    /// This is the recommended way to bootstrap a node with seed peers.
    pub async fn bootstrap_non_blocking(&self, seeds: Vec<SocketAddr>) {
        let seed_count = seeds.len();
        let registry_weak = Arc::downgrade(&self.registry);

        for seed in seeds {
            let registry_weak = registry_weak.clone();
            tokio::spawn(async move {
                let Some(registry) = registry_weak.upgrade() else {
                    return;
                };
                if registry.shutdown.load(Ordering::Relaxed) {
                    return;
                }

                let connect_timeout = registry.config.connection_timeout;
                match tokio::time::timeout(connect_timeout, registry.get_connection(seed)).await {
                    Ok(Ok(_conn)) => {
                        debug!(seed = %seed, "bootstrap connection established");
                        // Mark peer as connected
                        registry.mark_peer_connected(seed).await;
                    }
                    Ok(Err(e)) => {
                        warn!(seed = %seed, error = %e, "bootstrap peer unavailable");
                        // Note: Don't penalize at startup - peer might be starting up too
                    }
                    Err(_) => {
                        warn!(seed = %seed, "bootstrap peer dial timed out");
                    }
                }
            });
        }

        debug!(
            seed_count = seed_count,
            "initiated non-blocking bootstrap for seed peers"
        );
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        // Signal shutdown first, then abort background tasks so we don't get stuck
        // waiting on locks held by long-running timer/server work.
        self.registry
            .shutdown
            .store(true, std::sync::atomic::Ordering::Release);

        // Cancel background tasks.
        if let Some(handle) = self._server_handle.as_ref() {
            handle.abort();
        }
        if let Some(handle) = self._timer_handle.as_ref() {
            handle.abort();
        }
        if let Some(handle) = self._monitor_handle.as_ref() {
            handle.abort();
        }

        // Now do full cleanup (closes connections, clears state).
        self.registry.shutdown().await;
    }

    /// Shutdown and wait for owned task handles to observe cancellation.
    /// Prefer this in binaries that want deterministic single-Ctrl+C termination.
    pub async fn shutdown_and_wait(mut self) {
        // Signal shutdown immediately, then abort background tasks first to reduce lock contention
        // during registry cleanup (and to make Ctrl+C return promptly).
        self.registry.shutdown.store(true, Ordering::Release);

        if let Some(handle) = self._server_handle.take() {
            handle.abort();
            // Don't risk hanging shutdown on a task stuck in a non-cancellation-safe await.
            let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
        }
        if let Some(handle) = self._timer_handle.take() {
            handle.abort();
            let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
        }
        if let Some(handle) = self._monitor_handle.take() {
            handle.abort();
            let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
        }

        // Final cleanup after all background tasks are canceled.
        self.registry.shutdown().await;
    }

    /// Drop the bootstrap type parameter marker.
    ///
    /// All `GossipRegistryHandle<T>` instances are functionally identical regardless of `T` —
    /// the type parameter is `PhantomData` only. Use this when you need to store handles
    /// constructed with different bootstrap types in the same field or container.
    pub fn forget_bootstrap(self) -> GossipRegistryHandle {
        // Use ManuallyDrop to suppress T's Drop impl while we move the fields
        // to a new GossipRegistryHandle with a different (erased) type parameter.
        // SAFETY: every field is moved into the returned handle which takes
        // ownership and whose Drop impl will run normally.
        let this = std::mem::ManuallyDrop::new(self);
        unsafe {
            GossipRegistryHandle {
                registry: std::ptr::read(&this.registry),
                _server_handle: std::ptr::read(&this._server_handle),
                _timer_handle: std::ptr::read(&this._timer_handle),
                _monitor_handle: std::ptr::read(&this._monitor_handle),
                _marker: PhantomData,
            }
        }
    }
}

impl<T> GossipClient<T> {
    pub(crate) fn from_registry(registry: Arc<GossipRegistry>) -> Self {
        Self {
            registry,
            _marker: PhantomData,
        }
    }

    pub fn lookup_connected_connection(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<crate::RemoteConnection> {
        let conn = self
            .registry
            .connection_pool
            .get_connected_connection_to_peer(peer_id)?;
        Some(crate::RemoteConnection::from_handle(conn))
    }

    pub fn lookup_connected_peer(&self, peer_id: &crate::PeerId) -> Option<crate::RemoteActorRef> {
        let conn = self
            .registry
            .connection_pool
            .get_connected_connection_to_peer(peer_id)?;
        let addr = conn.addr;
        let location = crate::RemoteActorLocation::new_with_peer(addr, peer_id.clone());
        Some(crate::RemoteActorRef::with_registry(
            location,
            Some(conn),
            Arc::clone(&self.registry),
        ))
    }

    /// Disconnect the cached transport session for a peer, preserving configured peer address.
    pub fn disconnect_peer_connection(&self, peer_id: &crate::PeerId) -> bool {
        self.registry
            .connection_pool
            .disconnect_connection_by_peer_id(peer_id)
            .is_some()
    }

    pub fn peer_clock_snapshot(
        &self,
        peer_addr: &std::net::SocketAddr,
    ) -> Option<crate::registry::PeerClockSnapshot> {
        self.registry.peer_clock_snapshot(peer_addr)
    }

    pub fn peer_clock_snapshots(&self) -> Vec<crate::registry::PeerClockSnapshot> {
        self.registry.peer_clock_snapshots()
    }

    /// Lookup a peer and return a RemoteActorRef for communicating with it.
    ///
    /// This mirrors `GossipRegistryHandle::lookup_peer` but is available on a cloneable handle.
    pub async fn lookup_peer(&self, peer_id: &crate::PeerId) -> Result<crate::RemoteActorRef> {
        let conn = self
            .registry
            .connection_pool
            .get_connection_to_peer(peer_id)
            .await?;
        let addr = conn.addr;

        let location = crate::RemoteActorLocation::new_with_peer(addr, peer_id.clone());
        Ok(crate::RemoteActorRef::with_registry(
            location,
            Some(conn),
            Arc::clone(&self.registry),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KeyPair;
    use crate::transport::RegistryTransportBootstrap;
    use std::net::SocketAddr;
    use std::time::Duration;

    #[derive(Debug, Clone, Copy, Default)]
    struct TestTlsBootstrap;
    struct TestNoopBootstrap;
    struct TestUdpBootstrap;
    struct TestRecoveringBootstrap;

    impl RegistryTransportBootstrap for TestTlsBootstrap {
        fn stack_name(&self) -> &'static str {
            "test+tls"
        }

        fn prepare_config(
            &self,
            secret_key: &crate::SecretKey,
            config: &mut GossipConfig,
        ) -> Result<()> {
            let derived_keypair = secret_key.to_keypair();
            match config.key_pair.as_ref() {
                Some(existing) => {
                    if existing.peer_id() != derived_keypair.peer_id() {
                        return Err(GossipError::InvalidKeyPair(
                            "GossipConfig.key_pair does not match TLS secret key".to_string(),
                        ));
                    }
                }
                None => {
                    config.key_pair = Some(derived_keypair);
                }
            }
            Ok(())
        }

        fn configure_registry(
            &self,
            registry: &mut crate::registry::GossipRegistry,
            secret_key: crate::SecretKey,
        ) -> Result<()> {
            registry.enable_tls(secret_key)
        }
    }

    impl RegistryTransportBootstrap for TestNoopBootstrap {
        fn stack_name(&self) -> &'static str {
            "test+noop"
        }

        fn prepare_config(
            &self,
            secret_key: &crate::SecretKey,
            config: &mut GossipConfig,
        ) -> Result<()> {
            let derived_keypair = secret_key.to_keypair();
            match config.key_pair.as_ref() {
                Some(existing) => {
                    if existing.peer_id() != derived_keypair.peer_id() {
                        return Err(GossipError::InvalidKeyPair(
                            "GossipConfig.key_pair does not match secret key".to_string(),
                        ));
                    }
                }
                None => {
                    config.key_pair = Some(derived_keypair);
                }
            }
            Ok(())
        }

        fn configure_registry(
            &self,
            _registry: &mut crate::registry::GossipRegistry,
            _secret_key: crate::SecretKey,
        ) -> Result<()> {
            Ok(())
        }
    }

    impl RegistryTransportBootstrap for TestUdpBootstrap {
        fn stack_name(&self) -> &'static str {
            "test+udp"
        }

        fn wire_kind(&self) -> TransportWireKind {
            TransportWireKind::UdpDatagram
        }

        fn prepare_config(
            &self,
            secret_key: &crate::SecretKey,
            config: &mut GossipConfig,
        ) -> Result<()> {
            TestNoopBootstrap.prepare_config(secret_key, config)
        }

        fn configure_registry(
            &self,
            registry: &mut crate::registry::GossipRegistry,
            secret_key: crate::SecretKey,
        ) -> Result<()> {
            registry.enable_udp(secret_key)
        }
    }

    impl RegistryTransportBootstrap for TestRecoveringBootstrap {
        fn stack_name(&self) -> &'static str {
            "test+recovering"
        }

        fn connection_recovery_policy(&self) -> Option<crate::ConnectionRecoveryPolicy> {
            Some(crate::ConnectionRecoveryPolicy::aggressive_ask_timeout_recovery())
        }

        fn prepare_config(
            &self,
            secret_key: &crate::SecretKey,
            config: &mut GossipConfig,
        ) -> Result<()> {
            TestNoopBootstrap.prepare_config(secret_key, config)
        }

        fn configure_registry(
            &self,
            registry: &mut crate::registry::GossipRegistry,
            secret_key: crate::SecretKey,
        ) -> Result<()> {
            TestNoopBootstrap.configure_registry(registry, secret_key)
        }
    }

    fn test_cfg() -> GossipConfig {
        GossipConfig {
            gossip_interval: Duration::from_millis(25),
            ask_window: 1024,
            ..Default::default()
        }
    }

    #[test]
    fn gossip_deadline_reschedules_from_now_after_runtime_delay() {
        let old_tick = Instant::now() - Duration::from_secs(30);
        let delayed_now = Instant::now();
        let next = next_gossip_deadline(
            delayed_now,
            Duration::from_millis(250),
            Duration::from_millis(10),
        );

        assert!(
            next > delayed_now,
            "next gossip tick must be in the future after a delayed runtime wake"
        );
        assert!(
            next > old_tick + Duration::from_secs(30),
            "next gossip tick must not replay a stale missed-tick schedule"
        );
    }

    async fn new_registry(
        bind: SocketAddr,
        seed: &str,
    ) -> crate::Result<GossipRegistryHandle<TestNoopBootstrap>> {
        let keypair = KeyPair::new_for_testing(seed);
        let mut config = test_cfg();
        config.key_pair = Some(keypair.clone());
        GossipRegistryHandle::new_with_transport_stack(
            bind,
            keypair.to_secret_key(),
            Some(config),
            TestNoopBootstrap,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_known_actors_includes_local_and_known_and_is_sorted() -> crate::Result<()> {
        let h = new_registry("127.0.0.1:0".parse().unwrap(), "snap").await?;
        let bind = h.registry.bind_addr;

        h.register("b_local".to_string(), bind).await?;

        // Simulate a remote-gossiped entry (known_actors) without wiring up a full mesh.
        let remote_loc = RemoteActorLocation::new_with_peer(
            "127.0.0.1:9999".parse().unwrap(),
            h.registry.peer_id.clone(),
        );
        let _ = h
            .registry
            .actor_state
            .known_actors
            .upsert_sync("a_known".to_string(), remote_loc);

        // Duplicate name in known + local: local must win.
        let remote_loc2 = RemoteActorLocation::new_with_peer(
            "127.0.0.1:7777".parse().unwrap(),
            h.registry.peer_id.clone(),
        );
        let _ = h
            .registry
            .actor_state
            .known_actors
            .upsert_sync("b_local".to_string(), remote_loc2);

        let snap = h.snapshot_known_actors();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].0.as_str(), "a_known");
        assert_eq!(snap[1].0.as_str(), "b_local");

        let b = snap.iter().find(|(n, _)| n == "b_local").unwrap();
        assert_eq!(b.1.address, bind.to_string());

        h.shutdown_and_wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_with_transport_stack_bootstraps_test_tls_runtime() -> crate::Result<()> {
        let keypair = KeyPair::new_for_testing("stack-bootstrap");
        let mut config = test_cfg();
        config.key_pair = Some(keypair.clone());

        let handle = GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            keypair.to_secret_key(),
            Some(config),
            TestTlsBootstrap,
        )
        .await?;
        handle.shutdown_and_wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_datagram_transport_is_rejected_without_datagram_authentication() {
        let keypair = KeyPair::new_for_testing("udp-disabled");
        let mut config = test_cfg();
        config.key_pair = Some(keypair.clone());

        let result = GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            keypair.to_secret_key(),
            Some(config),
            TestUdpBootstrap,
        )
        .await;
        let err = match result {
            Ok(handle) => {
                handle.shutdown_and_wait().await;
                panic!("UDP datagram transport must not start without datagram authentication");
            }
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("UDP datagram transport is disabled"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_with_transport_stack_applies_connection_recovery_policy() -> crate::Result<()> {
        let keypair = KeyPair::new_for_testing("stack-recovery");
        let mut config = test_cfg();
        config.key_pair = Some(keypair.clone());

        let handle = GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            keypair.to_secret_key(),
            Some(config),
            TestRecoveringBootstrap,
        )
        .await?;

        assert_eq!(
            handle.registry.config.connection_recovery,
            crate::ConnectionRecoveryPolicy::aggressive_ask_timeout_recovery()
        );

        handle.shutdown_and_wait().await;
        Ok(())
    }
}

pub(crate) fn bind_with_reuseaddr(bind_addr: SocketAddr) -> Result<TcpListener> {
    use socket2::{Domain, Socket, Type};

    fn is_sandbox_eperm(err: &std::io::Error) -> bool {
        // In some sandbox profiles on macOS, networking syscalls can fail with EPERM but
        // the `ErrorKind` is not consistently `PermissionDenied`. Treat raw OS EPERM as
        // a soft failure and fall back to std's listener.
        err.kind() == std::io::ErrorKind::PermissionDenied || err.raw_os_error() == Some(1)
    }

    fn bind_fallback_std(bind_addr: SocketAddr) -> Result<TcpListener> {
        // macOS sandboxed runs can return transient EPERM for otherwise-valid `bind()` calls.
        // Retrying here is cheap (only on startup) and makes socket-heavy integration tests
        // deterministic.
        // Some sandbox profiles exhibit long EPERM bursts under load, so allow a longer retry
        // window. This only impacts startup when EPERM is actually occurring.
        //
        // IMPORTANT: use backoff, not a tight loop. Hammering bind() every ~10ms can prolong the
        // sandbox burst and makes `cargo test --all` retries converge poorly.
        let max_wait_ms: u64 = std::env::var("ICANACT_EPERM_BIND_MAX_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);
        let backoff_start_ms: u64 = std::env::var("ICANACT_EPERM_BIND_BACKOFF_START_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);
        let backoff_max_ms: u64 = std::env::var("ICANACT_EPERM_BIND_BACKOFF_MAX_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000);

        let deadline = std::time::Instant::now() + Duration::from_millis(max_wait_ms);
        let mut backoff = Duration::from_millis(backoff_start_ms);

        loop {
            match std::net::TcpListener::bind(bind_addr) {
                Ok(std_listener) => {
                    if let Err(e) = std_listener.set_nonblocking(true) {
                        if is_sandbox_eperm(&e) && std::time::Instant::now() < deadline {
                            std::thread::sleep(backoff);
                            backoff = std::cmp::min(
                                backoff.saturating_mul(2),
                                Duration::from_millis(backoff_max_ms),
                            );
                            continue;
                        }
                        return Err(GossipError::Network(e));
                    }

                    match TcpListener::from_std(std_listener) {
                        Ok(listener) => return Ok(listener),
                        Err(e) => {
                            // Treat tokio's conversion failure the same way as bind flakiness
                            // in sandboxed environments.
                            if is_sandbox_eperm(&e) && std::time::Instant::now() < deadline {
                                std::thread::sleep(backoff);
                                backoff = std::cmp::min(
                                    backoff.saturating_mul(2),
                                    Duration::from_millis(backoff_max_ms),
                                );
                                continue;
                            }
                            return Err(GossipError::Network(e));
                        }
                    }
                }
                Err(e) => {
                    if is_sandbox_eperm(&e) && std::time::Instant::now() < deadline {
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(
                            backoff.saturating_mul(2),
                            Duration::from_millis(backoff_max_ms),
                        );
                        continue;
                    }
                    return Err(GossipError::Network(e));
                }
            }
        }
    }

    // For ephemeral ports, std's bind path is already fast and reliable, and avoids
    // sandbox-sensitive socket option syscalls (EPERM flakiness in some environments).
    if bind_addr.port() == 0 {
        return bind_fallback_std(bind_addr);
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    fn set_reuse_port_best_effort(socket: &Socket) -> std::io::Result<()> {
        // Best-effort only. This is not required for correctness, so callers should ignore errors.
        use std::os::unix::io::AsRawFd;

        // Safety: setsockopt with stable pointers; any error is returned and treated as best-effort.
        unsafe {
            let fd = socket.as_raw_fd();
            let optval: libc::c_int = 1;
            let rc = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            );
            if rc == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        }
    }

    let domain = match bind_addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = match Socket::new(domain, Type::STREAM, None) {
        Ok(s) => s,
        Err(e) if is_sandbox_eperm(&e) => {
            // Some sandbox profiles disallow raw socket creation/config; fall back.
            return bind_fallback_std(bind_addr);
        }
        Err(e) => return Err(GossipError::Network(e)),
    };
    // Best-effort: some sandboxed environments return EPERM for these sockopts.
    // They are not required for correctness (especially for ephemeral ports), so
    // we don't fail startup if they're blocked.
    let _ = socket.set_reuse_address(true);
    // Best-effort: allow immediate rebinding during restarts on platforms that support it.
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    {
        let _ = set_reuse_port_best_effort(&socket);
    }

    if let Err(e) = socket.bind(&bind_addr.into()) {
        if is_sandbox_eperm(&e) {
            return bind_fallback_std(bind_addr);
        }
        return Err(GossipError::Network(e));
    }
    if let Err(e) = socket.listen(1024) {
        if is_sandbox_eperm(&e) {
            return bind_fallback_std(bind_addr);
        }
        return Err(GossipError::Network(e));
    }

    if let Err(e) = socket.set_nonblocking(true) {
        if is_sandbox_eperm(&e) {
            return bind_fallback_std(bind_addr);
        }
        return Err(GossipError::Network(e));
    }
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener).map_err(GossipError::Network)
}

#[allow(dead_code)]
pub(crate) fn bind_udp_with_reuseaddr(bind_addr: SocketAddr) -> Result<UdpSocket> {
    use socket2::{Domain, Socket, Type};

    let domain = match bind_addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::DGRAM, None).map_err(GossipError::Network)?;
    let _ = socket.set_reuse_address(true);
    let udp_buf_size = std::env::var("ICANACT_UDP_SOCKET_BUFFER_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let _ = socket.set_recv_buffer_size(udp_buf_size);
    let _ = socket.set_send_buffer_size(udp_buf_size);
    socket
        .bind(&bind_addr.into())
        .map_err(GossipError::Network)?;
    socket.set_nonblocking(true).map_err(GossipError::Network)?;
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket).map_err(GossipError::Network)
}

/// Start the gossip registry server with an existing listener
#[instrument(skip(registry, listener))]
async fn start_gossip_server_with_listener(
    registry: Arc<GossipRegistry>,
    listener: TcpListener,
) -> Result<()> {
    let bind_addr = registry.bind_addr;
    info!(bind_addr = %bind_addr, "gossip server started");

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!(peer_addr = %peer_addr, "📥 ACCEPTED incoming connection");
                // Set TCP_NODELAY for low-latency communication
                let _ = stream.set_nodelay(true);
                crate::net::apply_tcp_keepalive(&stream, &registry.config);

                let registry_clone = registry.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer_addr, registry_clone).await;
                });
            }
            Err(err) => {
                error!(error = %err, "failed to accept connection");
            }
        }
    }
}

/// Parse and dispatch a UDP datagram using the shared message parser/protocol pipeline.
///
/// A single UDP datagram may contain one or more framed messages. This path keeps UDP receive
/// native (no channel bridge) while reusing the same parser/dispatcher logic
/// used by the other transport stacks.
struct UdpPeerContext {
    addr: SocketAddr,
    connection: Arc<crate::connection_pool::LockFreeConnection>,
    authenticated_peer_id: crate::PeerId,
}

async fn process_udp_datagram_native(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    mut datagram: crate::PooledAlignedBuffer,
    datagram_len: usize,
    streaming_states: &mut HashMap<SocketAddr, crate::protocol::StreamingState>,
    peer_context: &mut Option<UdpPeerContext>,
) -> Result<()> {
    if datagram_len < crate::framing::LENGTH_PREFIX_LEN {
        return Ok(());
    }

    let cached_peer_ready = peer_context
        .as_ref()
        .map(|ctx| ctx.addr == peer_addr && ctx.connection.is_connected())
        .unwrap_or(false);
    if !cached_peer_ready {
        let mut response_connection = registry.connection_pool.get_connection_by_addr(&peer_addr);
        if response_connection.is_none() {
            registry
                .connection_pool
                .ensure_udp_peer_connection(peer_addr)
                .await?;
            response_connection = registry.connection_pool.get_connection_by_addr(&peer_addr);
        }
        let response_connection = response_connection.ok_or_else(|| {
            GossipError::InvalidConfig(format!(
                "UDP datagram from {peer_addr} has no established connection"
            ))
        })?;
        let authenticated_peer_id = response_connection
            .embedded_peer_id
            .clone()
            .or_else(|| registry.connection_pool.get_peer_id_by_addr(&peer_addr))
            .ok_or_else(|| {
                GossipError::InvalidConfig(format!(
                    "UDP datagram from {peer_addr} has no established peer association"
                ))
            })?;
        *peer_context = Some(UdpPeerContext {
            addr: peer_addr,
            connection: response_connection,
            authenticated_peer_id,
        });
    }
    let msg_len = u32::from_be_bytes(
        datagram.as_ref()[..crate::framing::LENGTH_PREFIX_LEN]
            .try_into()
            .expect("slice length checked"),
    ) as usize;
    if msg_len > registry.config.max_message_size {
        return Err(GossipError::MessageTooLarge {
            size: msg_len,
            max: registry.config.max_message_size,
        });
    }
    let frame_len = crate::framing::LENGTH_PREFIX_LEN + msg_len;

    // Common case: one framed message per datagram.
    if frame_len == datagram_len {
        if msg_len >= crate::framing::PUBSUB_HEADER_LEN {
            let msg_data = &datagram.as_ref()
                [crate::framing::LENGTH_PREFIX_LEN..crate::framing::LENGTH_PREFIX_LEN + msg_len];
            if msg_data[0] == crate::MessageType::PubSub as u8 {
                let payload_len = msg_len - crate::framing::PUBSUB_HEADER_LEN;
                let payload_offset =
                    crate::framing::LENGTH_PREFIX_LEN + crate::framing::PUBSUB_HEADER_LEN;
                let payload = &datagram.as_ref()[payload_offset..payload_offset + payload_len];
                let authenticated_peer_id = &peer_context
                    .as_ref()
                    .expect("UDP peer context is initialized before parsing datagram")
                    .authenticated_peer_id;
                if let Some(handler) = registry.pubsub_ingress_handler.load().as_ref() {
                    if let Err(e) = handler.handle_borrowed(authenticated_peer_id, payload) {
                        warn!(peer = %peer_addr, error = %e, "Failed to process UDP PubSub frame");
                    }
                }
                return Ok(());
            }
        }

        let peer_context = peer_context
            .as_ref()
            .filter(|ctx| ctx.addr == peer_addr && ctx.connection.is_connected())
            .ok_or_else(|| {
                GossipError::InvalidConfig(format!(
                    "UDP datagram from {peer_addr} has no established peer context"
                ))
            })?;
        let response_connection = Arc::clone(&peer_context.connection);
        let authenticated_peer_id = peer_context.authenticated_peer_id.clone();
        let response_correlation = response_connection.correlation.clone();
        datagram.truncate(frame_len);
        let parsed = parse_message_from_pooled_buffer(datagram, msg_len)?;
        let streaming_state = streaming_states.entry(peer_addr).or_default();
        crate::protocol::process_read_result(
            parsed,
            streaming_state,
            registry,
            peer_addr,
            response_correlation.as_deref(),
            Some(&response_connection),
            Some(&authenticated_peer_id),
        )
        .await?;
        return Ok(());
    }

    let aligned_pool = registry.connection_pool.aligned_bytes_pool();
    let datagram_bytes = Bytes::from_owner(datagram);
    let datagram_slice = &datagram_bytes.as_ref()[..datagram_len];
    let mut offset = 0usize;
    let peer_context = peer_context
        .as_ref()
        .filter(|ctx| ctx.addr == peer_addr && ctx.connection.is_connected())
        .ok_or_else(|| {
            GossipError::InvalidConfig(format!(
                "UDP datagram from {peer_addr} has no established peer context"
            ))
        })?;
    let response_connection = Arc::clone(&peer_context.connection);
    let authenticated_peer_id = peer_context.authenticated_peer_id.clone();
    while offset + crate::framing::LENGTH_PREFIX_LEN <= datagram_len {
        let msg_len = u32::from_be_bytes(
            datagram_slice[offset..offset + crate::framing::LENGTH_PREFIX_LEN]
                .try_into()
                .expect("slice length checked"),
        ) as usize;

        if msg_len > registry.config.max_message_size {
            return Err(GossipError::MessageTooLarge {
                size: msg_len,
                max: registry.config.max_message_size,
            });
        }

        let frame_len = crate::framing::LENGTH_PREFIX_LEN + msg_len;
        if offset + frame_len > datagram_len {
            // Truncated frame tail in one datagram: drop the remainder to preserve framing safety.
            warn!(
                peer = %peer_addr,
                datagram_len = datagram_len,
                frame_offset = offset,
                frame_len = frame_len,
                "dropping truncated udp frame batch tail"
            );
            break;
        }

        let response_correlation = response_connection.correlation.clone();
        let mut frame =
            unsafe { crate::PooledAlignedBuffer::with_len_uninit(frame_len, aligned_pool.clone()) };
        frame
            .as_mut_slice()
            .copy_from_slice(&datagram_slice[offset..offset + frame_len]);
        let parsed = parse_message_from_pooled_buffer(frame, msg_len)?;
        let streaming_state = streaming_states.entry(peer_addr).or_default();
        crate::protocol::process_read_result(
            parsed,
            streaming_state,
            registry,
            peer_addr,
            response_correlation.as_deref(),
            Some(&response_connection),
            Some(&authenticated_peer_id),
        )
        .await?;

        offset += frame_len;
    }

    Ok(())
}

/// Start the gossip registry server with a UDP socket.
#[instrument(skip(registry, socket))]
async fn start_gossip_server_with_udp_socket(
    registry: Arc<GossipRegistry>,
    socket: Arc<UdpSocket>,
) -> Result<()> {
    let bind_addr = registry.bind_addr;
    info!(bind_addr = %bind_addr, "gossip udp server started");

    let max_datagram_size =
        (registry.config.max_message_size + crate::framing::LENGTH_PREFIX_LEN).min(65_507);
    let datagram_capacity = max_datagram_size.max(2048);
    let aligned_pool = registry.connection_pool.aligned_bytes_pool();
    let mut streaming_states = HashMap::<SocketAddr, crate::protocol::StreamingState>::new();
    let mut peer_context: Option<UdpPeerContext> = None;

    loop {
        let mut datagram = unsafe {
            crate::PooledAlignedBuffer::with_len_uninit(datagram_capacity, aligned_pool.clone())
        };
        match socket.recv_from(datagram.as_mut_slice()).await {
            Ok((len, peer_addr)) => {
                if len >= crate::framing::LENGTH_PREFIX_LEN {
                    if let Err(err) = process_udp_datagram_native(
                        &registry,
                        peer_addr,
                        datagram,
                        len,
                        &mut streaming_states,
                        &mut peer_context,
                    )
                    .await
                    {
                        warn!(peer = %peer_addr, error = %err, "failed to process udp datagram");
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "failed to receive udp datagram");
            }
        }
    }
}

/// Start the gossip timer with vector clock support
#[instrument(skip(registry))]
async fn start_gossip_timer(registry: Arc<GossipRegistry>) {
    debug!("start_gossip_timer function called");

    let gossip_interval = registry.config.gossip_interval;
    let cleanup_interval = registry.config.cleanup_interval;
    let vector_clock_gc_interval = registry.config.vector_clock_gc_frequency;
    let peer_gossip_interval = registry.config.peer_gossip_interval;
    let mut udp_failure_timer = if registry.udp_mode {
        let mut t = interval(registry.udp_failure_detector_config.health_probe_interval);
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Some(t)
    } else {
        None
    };

    let max_jitter = std::cmp::min(gossip_interval, Duration::from_millis(1000));
    let jitter_ms = if max_jitter.is_zero() {
        0
    } else {
        let max_ms = max_jitter.as_millis().max(1) as u64;
        rand::random::<u64>() % max_ms
    };
    let jitter = Duration::from_millis(jitter_ms);
    let mut next_gossip_tick = next_gossip_deadline(Instant::now(), gossip_interval, jitter);
    let mut cleanup_timer = interval(cleanup_interval);
    cleanup_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut vector_clock_gc_timer = interval(vector_clock_gc_interval);
    vector_clock_gc_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Peer gossip timer - only if peer discovery is enabled
    let mut peer_gossip_timer = peer_gossip_interval.map(|i| {
        let mut t = interval(i);
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        t
    });

    debug!(
        gossip_interval_ms = gossip_interval.as_millis(),
        cleanup_interval_secs = cleanup_interval.as_secs(),
        vector_clock_gc_interval_secs = vector_clock_gc_interval.as_secs(),
        peer_gossip_interval_secs = peer_gossip_interval.map(|i| i.as_secs()),
        udp_failure_detector = registry.udp_mode,
        "gossip timer started with non-blocking I/O"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next_gossip_tick) => {
                let jitter_ms = if max_jitter.is_zero() {
                    0
                } else {
                    let max_ms = max_jitter.as_millis().max(1) as u64;
                    rand::random::<u64>() % max_ms
                };
                let jitter = Duration::from_millis(jitter_ms);
                next_gossip_tick = next_gossip_deadline(Instant::now(), gossip_interval, jitter);

                // Step 1: Prepare gossip tasks while holding the lock briefly
                let tasks = {
                    if registry.is_shutdown().await {
                        break;
                    }
                    match registry.prepare_gossip_round().await {
                        Ok(tasks) => tasks,
                        Err(err) => {
                            error!(error = %err, "failed to prepare gossip round");
                            continue;
                        }
                    }
                };

                if tasks.is_empty() {
                    continue;
                }

                // Step 2: Execute all gossip tasks WITHOUT holding the registry lock
                // Use zero-copy optimized sending for each individual gossip message
                let results = {
                    let mut futures = Vec::new();

                    for task in tasks {
                        let registry_clone = registry.clone();
                        let peer_addr = task.peer_addr;
                        let sent_sequence = task.current_sequence;
                        let future = tokio::spawn(async move {
                            // Send the message using zero-copy persistent connections
                            let outcome = send_gossip_message_zero_copy(task, registry_clone).await;
                            GossipResult {
                                peer_addr,
                                sent_sequence,
                                outcome: outcome.map(|_| None),
                            }
                        });
                        futures.push(future);
                    }

                    // Wait for all gossip operations to complete concurrently
                    let mut results = Vec::new();
                    for future in futures {
                        match future.await {
                            Ok(result) => results.push(result),
                            Err(err) => {
                                error!(error = %err, "gossip task panicked");
                            }
                        }
                    }
                    results
                };

                // Step 3: Apply results while holding the lock briefly
                {
                    if !registry.is_shutdown().await {
                        registry.apply_gossip_results(results).await;
                    }
                }
            }
            _ = cleanup_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                registry.cleanup_stale_actors().await;
                // Also check for consensus timeouts
                registry.check_peer_consensus().await;
                // Clean up peers that have been dead for too long
                registry.cleanup_dead_peers().await;
                // Clean up stale peers from peer discovery (Phase 4)
                registry.prune_stale_peers().await;
            }
            _ = vector_clock_gc_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                // Run vector clock garbage collection
                registry.run_vector_clock_gc().await;
            }
            // Peer gossip timer - for peer list gossip (Phase 4)
            _ = async {
                if let Some(ref mut timer) = peer_gossip_timer {
                    timer.tick().await
                } else {
                    // If peer gossip is disabled, wait forever (never fires)
                    std::future::pending::<tokio::time::Instant>().await
                }
            } => {
                if registry.is_shutdown().await {
                    break;
                }
                // Only gossip peer list if peer discovery is enabled
                if registry.config.enable_peer_discovery {
                    let tasks = registry.gossip_peer_list().await;
                    if tasks.is_empty() {
                        continue;
                    }

                    let mut futures = Vec::new();
                    for task in tasks {
                        let registry_clone = registry.clone();
                        let future = tokio::spawn(async move {
                            if let Err(err) =
                                send_gossip_message_zero_copy(task, registry_clone).await
                            {
                                warn!(error = %err, "peer list gossip send failed");
                            }
                        });
                        futures.push(future);
                    }

                    for future in futures {
                        if let Err(err) = future.await {
                            error!(error = %err, "peer list gossip task panicked");
                        }
                    }
                }
            }
            // UDP detector timer - only active for udp experimental stack.
            _ = async {
                if let Some(ref mut timer) = udp_failure_timer {
                    timer.tick().await
                } else {
                    std::future::pending::<tokio::time::Instant>().await
                }
            } => {
                if registry.is_shutdown().await {
                    break;
                }
                if let Err(err) = registry.run_udp_failure_detector_once().await {
                    warn!(error = %err, "udp failure detector tick failed");
                }
            }
        }
    }

    debug!("gossip timer stopped");
}

/// Handle incoming TCP connections - immediately set up bidirectional communication
#[instrument(skip(stream, registry), fields(peer = %peer_addr))]
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    let Some(tls_config) = registry.tls_config.clone() else {
        warn!(peer = %peer_addr, "stream connection received without TLS config");
        return;
    };

    let acceptor = tls_config.acceptor();
    let tls_accept_started = Instant::now();
    info!(
        target: "icanact_remote_lifecycle",
        peer = %peer_addr,
        timeout_ms = registry.config.connection_timeout.as_millis(),
        "inbound_tls_accept_start"
    );
    match tokio::time::timeout(registry.config.connection_timeout, acceptor.accept(stream)).await {
        Err(_) => {
            warn!(
                target: "icanact_remote_lifecycle",
                peer = %peer_addr,
                timeout_ms = registry.config.connection_timeout.as_millis(),
                elapsed_ms = tls_accept_started.elapsed().as_millis(),
                "TLS accept timed out"
            );
        }
        Ok(Err(err)) => {
            warn!(
                target: "icanact_remote_lifecycle",
                peer = %peer_addr,
                elapsed_ms = tls_accept_started.elapsed().as_millis(),
                error = %err,
                "TLS accept failed"
            );
        }
        Ok(Ok(mut tls_stream)) => {
            info!(
                target: "icanact_remote_lifecycle",
                peer = %peer_addr,
                elapsed_ms = tls_accept_started.elapsed().as_millis(),
                "inbound_tls_accept_ok"
            );
            let negotiated_alpn = tls_stream
                .get_ref()
                .1
                .alpn_protocol()
                .map(|proto| proto.to_vec());
            let peer_node_id = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
                .and_then(|cert| crate::tls::extract_node_id_from_cert(cert).ok());

            let hello_started = Instant::now();
            let capabilities = match tokio::time::timeout(
                registry.config.connection_timeout,
                crate::handshake::perform_hello_handshake(
                    &mut tls_stream,
                    negotiated_alpn.as_deref(),
                    registry.config.enable_peer_discovery,
                ),
            )
            .await
            {
                Ok(Ok(caps)) => {
                    info!(
                        target: "icanact_remote_lifecycle",
                        peer = %peer_addr,
                        peer_node_id = %peer_node_id
                            .as_ref()
                            .map(|node_id| node_id.fmt_short())
                            .unwrap_or_else(|| "unknown".to_string()),
                        elapsed_ms = hello_started.elapsed().as_millis(),
                        "inbound_hello_handshake_ok"
                    );
                    caps
                }
                Ok(Err(err)) => {
                    warn!(
                        target: "icanact_remote_lifecycle",
                        peer = %peer_addr,
                        elapsed_ms = hello_started.elapsed().as_millis(),
                        error = %err,
                        "Hello handshake failed, closing inbound TLS connection"
                    );
                    return;
                }
                Err(_) => {
                    warn!(
                        target: "icanact_remote_lifecycle",
                        peer = %peer_addr,
                        timeout_ms = registry.config.connection_timeout.as_millis(),
                        elapsed_ms = hello_started.elapsed().as_millis(),
                        "Hello handshake timed out, closing inbound TLS connection"
                    );
                    return;
                }
            };

            registry.set_peer_capabilities(peer_addr, capabilities);
            if let Some(node_id) = registry.lookup_node_id(&peer_addr).await {
                registry
                    .associate_peer_capabilities_with_node(peer_addr, node_id)
                    .await;
            }

            let registry_weak = Arc::downgrade(&registry);
            match handle_incoming_connection_tls(
                tls_stream,
                peer_addr,
                registry,
                Some(registry_weak),
                peer_node_id,
            )
            .await
            {
                ConnectionCloseOutcome::Normal { node_id } => {
                    debug!(peer = %peer_addr, ?node_id, "stream connection closed");
                }
                ConnectionCloseOutcome::DroppedByTieBreaker => {
                    debug!(peer = %peer_addr, "stream connection dropped by tie-breaker");
                }
            }
        }
    }
}

#[allow(dead_code)]
enum ConnectionCloseOutcome {
    Normal { node_id: Option<String> },
    DroppedByTieBreaker,
}

fn inbound_tls_sender_is_authenticated(
    peer_node_id: Option<crate::NodeId>,
    claimed_node_id: crate::NodeId,
) -> bool {
    peer_node_id.is_some_and(|authenticated_node_id| authenticated_node_id == claimed_node_id)
}

/// Handle an incoming TLS connection - processes all messages over encrypted stream
#[allow(dead_code)]
async fn handle_incoming_connection_tls<S>(
    mut stream: S,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
    _registry_weak: Option<std::sync::Weak<GossipRegistry>>,
    peer_node_id: Option<crate::NodeId>,
) -> ConnectionCloseOutcome
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let max_message_size = registry.config.max_message_size;
    let aligned_pool = registry.connection_pool.aligned_bytes_pool();

    // First, read the initial message to identify the sender
    let msg_result =
        read_message_from_tls_reader(&mut stream, max_message_size, Some(&aligned_pool)).await;
    let known_node_id = match peer_node_id {
        Some(node_id) => Some(node_id),
        None => registry.lookup_node_id(&peer_addr).await,
    };

    let (sender_node_id, _initial_correlation_id, sender_bind_addr_opt) = match &msg_result {
        Ok(MessageReadResult::Gossip(msg, correlation_id)) => {
            let (node_id, bind_addr) = match msg {
                RegistryMessage::DeltaGossip { delta, .. } => (delta.sender_peer_id.to_hex(), None),
                RegistryMessage::FullSync {
                    sender_peer_id,
                    sender_bind_addr,
                    ..
                } => (sender_peer_id.to_hex(), sender_bind_addr.clone()),
                RegistryMessage::FullSyncRequest {
                    sender_peer_id,
                    sender_bind_addr,
                    ..
                } => (sender_peer_id.to_hex(), sender_bind_addr.clone()),
                RegistryMessage::FullSyncResponse {
                    sender_peer_id,
                    sender_bind_addr,
                    ..
                } => (sender_peer_id.to_hex(), sender_bind_addr.clone()),
                RegistryMessage::DeltaGossipResponse { delta, .. } => {
                    (delta.sender_peer_id.to_hex(), None)
                }
                RegistryMessage::PeerHealthQuery { sender, .. } => (sender.to_hex(), None),
                RegistryMessage::PeerHealthReport { reporter, .. } => (reporter.to_hex(), None),
                RegistryMessage::ImmediateAck { .. } => {
                    warn!("Received ImmediateAck as first message - cannot identify sender");
                    return ConnectionCloseOutcome::Normal { node_id: None };
                }
                RegistryMessage::ActorMessage { .. } => {
                    warn!(
                        peer = %peer_addr,
                        "Registry ActorMessage is no longer supported in v3; closing connection"
                    );
                    return ConnectionCloseOutcome::Normal { node_id: None };
                }
                RegistryMessage::PeerListGossip { sender_addr, .. } => (sender_addr.clone(), None),
            };
            (node_id, *correlation_id, bind_addr)
        }
        Ok(MessageReadResult::AskRaw { correlation_id, .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), Some(*correlation_id), None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Ask request arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Response { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None, None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Response arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::DirectAsk { correlation_id, .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), Some(*correlation_id), None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "DirectAsk arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::DirectResponse { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None, None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Response arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::PubSub { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None, None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "PubSub frame arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Actor { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None, None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Actor frame arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Streaming { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None, None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Streaming frame arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Raw(_)) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None, None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Raw message arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Err(e) => {
            warn!(error = %e, peer_addr = %peer_addr, "⚠️ Failed to read initial message from TLS stream - early exit");
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    };

    info!(
        target: "icanact_remote_lifecycle",
        peer_addr = %peer_addr,
        node_id = %sender_node_id,
        sender_bind_addr = sender_bind_addr_opt
            .as_ref()
            .map(|addr| addr.as_str())
            .unwrap_or("none"),
        "inbound_identified"
    );

    // Update the gossip state with the NodeId for this peer
    // This is critical for bidirectional TLS connections
    let peer_id = match crate::PeerId::from_hex(&sender_node_id) {
        Ok(peer_id) => peer_id,
        Err(err) => {
            warn!(
                peer_addr = %peer_addr,
                error = %err,
                "Invalid peer id in first message; dropping connection"
            );
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    };
    let sender_node_id_from_message = peer_id.to_node_id();
    if !inbound_tls_sender_is_authenticated(peer_node_id, sender_node_id_from_message) {
        match peer_node_id {
            Some(authenticated_node_id) => {
                warn!(
                    peer_addr = %peer_addr,
                    authenticated_node_id = %authenticated_node_id.fmt_short(),
                    claimed_node_id = %sender_node_id_from_message.fmt_short(),
                    "TLS client certificate NodeId does not match first message sender; dropping connection"
                );
            }
            None => {
                warn!(
                    peer_addr = %peer_addr,
                    claimed_node_id = %sender_node_id_from_message.fmt_short(),
                    "TLS client certificate NodeId missing for inbound connection; dropping connection"
                );
            }
        }
        return ConnectionCloseOutcome::Normal { node_id: None };
    }
    let node_id_opt = Some(sender_node_id_from_message);

    // Prefer the sender's advertised bind address (validated) and fall back
    // to any configured address, then the TCP source address.
    let sender_bind_addr = sender_bind_addr_opt.as_deref();
    let resolved_sender_addr =
        sender_bind_addr.map(|addr| crate::registry::resolve_peer_addr(Some(addr), peer_addr));
    let configured_addr = {
        let pool = &registry.connection_pool;
        pool.peer_id_to_addr
            .read_sync(&peer_id, |_, v| *v)
            .filter(|addr| addr.port() != 0 && !addr.ip().is_unspecified())
    };
    let peer_state_addr = resolved_sender_addr
        .or_else(|| configured_addr)
        .unwrap_or(peer_addr);

    if let Some(node_id) = node_id_opt {
        registry
            .add_peer_with_node_id(peer_state_addr, Some(node_id))
            .await;
        // Associate capabilities captured during the Hello handshake (stored under peer_addr).
        registry
            .associate_peer_capabilities_with_node(peer_addr, node_id)
            .await;
        if peer_state_addr != peer_addr {
            registry
                .associate_peer_capabilities_with_node(peer_state_addr, node_id)
                .await;
        }
        if peer_state_addr != peer_addr {
            let mut gossip_state = registry.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&peer_state_addr) {
                peer_info.peer_address = Some(peer_addr);
            }
        }

        // Notify peer discovery that a connection is established (incoming)
        registry.mark_peer_connected(peer_state_addr).await;
        registry
            .mark_inbound_connection_observed(peer_state_addr, peer_addr)
            .await;

        debug!(
            peer_addr = %peer_addr,
            peer_state_addr = %peer_state_addr,
            "Updated gossip state with NodeId for incoming TLS connection"
        );
    }

    // Register the TLS stream with the connection pool before handling the first message so responses work
    let (response_correlation, response_connection) = {
        let buffer_config = crate::connection_pool::BufferConfig::default()
            .with_ask_window(registry.config.ask_window);
        let correlation_tracker = registry
            .connection_pool
            .get_or_create_correlation_tracker(&peer_id);
        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(peer_addr));
        let read_context = crate::connection_pool::ReadContext {
            registry_weak: Arc::downgrade(&registry),
            peer_addr,
            peer_id: Some(peer_id.clone()),
            max_message_size,
            expected_schema_hash: registry.config.schema_hash,
            aligned_pool: aligned_pool.clone(),
            response_correlation: Some(correlation_tracker.clone()),
            response_writer: Some(response_writer.clone()),
            tell_handler_sync: registry.actor_tell_handler_sync.load_full(),
            ask_immediate_handler_sync: registry.actor_ask_immediate_handler_sync.load_full(),
            ask_handler_sync: registry.actor_ask_handler_sync.load_full(),
            sync_actor_handler: registry.actor_message_handler_sync.load_full(),
        };
        let (stream_handle, writer_task_handle, reader_task_handle) =
            crate::connection_pool::LockFreeStreamHandle::new(
                stream,
                peer_addr,
                crate::connection_pool::ChannelId::Global,
                buffer_config,
                registry.config.schema_hash,
                Some(read_context),
            );
        let stream_handle = Arc::new(stream_handle);
        response_writer.bind_stream_handle(stream_handle.clone());

        let mut connection = crate::connection_pool::LockFreeConnection::new(
            peer_state_addr,
            crate::connection_pool::ConnectionDirection::Inbound,
        );
        connection.stream_handle = Some(stream_handle);
        connection.set_state(crate::connection_pool::ConnectionState::Connected);
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

        // CRITICAL: Get the shared correlation tracker BEFORE wrapping in Arc
        // This ensures the inbound connection uses the same correlation tracker as the outbound connection
        connection.correlation = Some(correlation_tracker);
        debug!(
            peer_id = %peer_id,
            "Set shared correlation tracker on inbound connection before Arc::new"
        );

        // CRITICAL: Set embedded_peer_id so responses can find the shared correlation tracker
        // even after addr_to_peer_id mapping is migrated from ephemeral to bind address
        connection.embedded_peer_id = Some(peer_id.clone());

        let connection_arc = Arc::new(connection);

        let keep_connection = {
            let pool = &registry.connection_pool;

            if let Some(existing_conn) = pool.get_connection_by_peer_id(&peer_id) {
                let existing_usable = existing_conn.has_live_stream();
                let keep_existing = existing_usable
                    && registry.should_keep_connection(
                        &peer_id,
                        existing_conn.direction
                            == crate::connection_pool::ConnectionDirection::Outbound,
                    );

                if !existing_usable {
                    info!(
                        target: "icanact_remote_lifecycle",
                        peer_id = %peer_id,
                        addr = %existing_conn.addr,
                        peer_state_addr = %peer_state_addr,
                        "inbound_tiebreak_evict_stale"
                    );
                    let _ = pool.disconnect_connection_by_peer_id(&peer_id);
                    pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        peer_state_addr,
                        connection_arc.clone(),
                    );
                    true
                } else if !keep_existing {
                    info!(
                        target: "icanact_remote_lifecycle",
                        peer_id = %peer_id,
                        addr = %existing_conn.addr,
                        peer_state_addr = %peer_state_addr,
                        existing_direction = ?existing_conn.direction,
                        "inbound_tiebreak_replace_wrong_direction"
                    );
                    crate::lifecycle::record_transport_event(
                        crate::lifecycle::TransportLifecycleEvent::WrongDirectionEvicted {
                            peer: peer_id.clone(),
                            addr: existing_conn.addr,
                            direction: match existing_conn.direction {
                                crate::connection_pool::ConnectionDirection::Inbound => {
                                    crate::lifecycle::TransportDirection::Inbound
                                }
                                crate::connection_pool::ConnectionDirection::Outbound => {
                                    crate::lifecycle::TransportDirection::Outbound
                                }
                            },
                        },
                    );
                    let _ = pool.disconnect_connection_by_peer_id(&peer_id);
                    pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        peer_state_addr,
                        connection_arc.clone(),
                    );
                    true
                } else {
                    info!(
                        target: "icanact_remote_lifecycle",
                        peer_id = %peer_id,
                        addr = %existing_conn.addr,
                        peer_state_addr = %peer_state_addr,
                        existing_direction = ?existing_conn.direction,
                        "inbound_tiebreak_reject_live_duplicate"
                    );
                    registry.clear_peer_capabilities(&peer_addr);
                    false
                }
            } else {
                pool.add_connection_by_peer_id(
                    peer_id.clone(),
                    peer_state_addr,
                    connection_arc.clone(),
                );
                info!(
                    target: "icanact_remote_lifecycle",
                    peer_id = %peer_id,
                    peer_addr = %peer_addr,
                    peer_state_addr = %peer_state_addr,
                    "inbound_connection_accepted"
                );
                true
            }
        };
        if keep_connection {
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::InboundReady {
                    peer: peer_id.clone(),
                    addr: peer_state_addr,
                },
            );
        }

        if !keep_connection {
            if let Some(handle) = connection_arc.stream_handle.as_ref() {
                handle.shutdown();
            }
            return ConnectionCloseOutcome::DroppedByTieBreaker;
        }

        // CRITICAL FIX: Also index by ephemeral peer_addr if it differs from peer_state_addr.
        // This ensures that handle_response_message (which looks up by peer_addr) can find
        // the connection AND the correlation tracker. Without this, responses fail to be
        // delivered because they are looked up by the ephemeral address but only indexed
        // by the configured bind address.
        if peer_addr != peer_state_addr {
            let pool = &registry.connection_pool;
            pool.index_connection_by_addr(peer_addr, connection_arc.clone());
            // Also add the addr_to_peer_id mapping so handle_response_message can look up
            // the shared correlation tracker via peer_id
            pool.add_addr_to_peer_id(peer_addr, peer_id.clone());
            debug!(
                node_id = %sender_node_id,
                peer_addr = %peer_addr,
                peer_state_addr = %peer_state_addr,
                "Also indexed incoming connection by ephemeral address for response delivery"
            );
        }

        debug!(
            node_id = %sender_node_id,
            peer_addr = %peer_addr,
            "Added incoming TLS connection to pool for bidirectional communication"
        );
        (connection_arc.correlation.clone(), connection_arc)
    };

    let mut streaming_state = crate::protocol::StreamingState::new();

    // Process the initial message with correlation ID if present
    // We can safely unwrap here because the error case was handled by the match block above (returning early)
    if let Err(e) = crate::protocol::process_read_result(
        msg_result.unwrap(),
        &mut streaming_state,
        &registry,
        peer_addr,
        response_correlation.as_ref().map(|c| c.as_ref()),
        Some(&response_connection),
        Some(&peer_id),
    )
    .await
    {
        warn!(error = %e, "Failed to process initial TLS message - connection will be closed");
        return ConnectionCloseOutcome::Normal { node_id: None };
    }

    // Continue processing via the IO task; wait for it to exit.
    if let Some(handle) = response_connection.stream_handle.as_ref() {
        handle.wait_for_exit().await;
    }

    warn!(peer_addr = %peer_addr, sender_node_id = %sender_node_id,
        "📤 Incoming TLS connection handler loop exited - peer may need reconnection");
    ConnectionCloseOutcome::Normal {
        node_id: Some(sender_node_id),
    }
}

/// Result type for message reading that can handle gossip, actor, and streaming messages
#[derive(Debug)]
pub(crate) enum MessageReadResult {
    Gossip(RegistryMessage, Option<u16>),
    AskRaw {
        correlation_id: u16,
        payload: AlignedBytes,
    },
    Response {
        correlation_id: u16,
        payload: AlignedBytes,
    },
    Raw(bytes::Bytes),
    PubSub {
        payload: AlignedBytes,
    },
    Actor {
        msg_type: u8,
        correlation_id: u16,
        actor_id: u64,
        type_hash: u32,
        schema_hash: Option<u64>,
        payload: AlignedBytes,
    },
    Streaming {
        msg_type: u8,
        correlation_id: u16,
        schema_hash: Option<u64>,
        stream_header: crate::StreamHeader,
        chunk_data: bytes::Bytes,
    },
    /// Fast-path direct ask (bypasses actor message handler)
    DirectAsk {
        correlation_id: u16,
        payload: AlignedBytes,
    },
    /// Fast-path direct response
    DirectResponse {
        correlation_id: u16,
        payload: AlignedBytes,
    },
}

pub(crate) async fn handle_raw_ask_request(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: &[u8],
) {
    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    {
        let response = if std::env::var("ICANACT_REMOTE_TYPED_ECHO").is_ok() && payload.len() >= 8 {
            payload.to_vec()
        } else {
            crate::connection_pool::process_mock_request_payload(payload)
        };

        let conn = {
            let pool = &registry.connection_pool;
            pool.get_connection_by_addr(&peer_addr)
        };

        if let Some(conn) = conn {
            if let Some(ref stream_handle) = conn.stream_handle {
                let header = crate::framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    response.len(),
                );

                if let Err(e) = stream_handle
                    .write_header_and_payload_control(
                        bytes::Bytes::copy_from_slice(&header),
                        bytes::Bytes::from(response),
                    )
                    .await
                {
                    warn!(peer = %peer_addr, error = %e, "Failed to send Ask response");
                } else {
                    // Intentionally quiet: this is the hot-path and can spam logs in benchmarks.
                }
            } else {
                warn!(peer = %peer_addr, "No stream handle for Ask response");
            }
        } else {
            warn!(peer = %peer_addr, "No connection found for Ask response");
        }
    }
    #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
    {
        let _ = registry;
        let _ = payload;
        warn!(
            peer = %peer_addr,
            correlation_id = correlation_id,
            "Received raw Ask request - not supported"
        );
    }
}

/// Send a response back to the peer for a streaming ask request.
/// This is called after handle_actor_message returns with a response.
/// Uses send_response_auto_bytes to preserve zero-copy streaming for large responses.
pub(crate) async fn send_streaming_response(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    response: bytes::Bytes,
) {
    let pool = &registry.connection_pool;

    // IMPORTANT: Look up by peer_id first, then fall back to peer_addr
    // For responses, we prefer OUTBOUND connection, but will use INBOUND if that's all we have
    // (both connections use the same TCP wire - responses go back on the same connection)
    let mut conn_opt: Option<Arc<crate::connection_pool::LockFreeConnection>> = None;
    if let Some(peer_id) = pool.get_peer_id_by_addr(&peer_addr) {
        // Get connection by peer_id - this returns the best available connection
        let conn = pool.get_connection_by_peer_id(&peer_id);

        // For responses, we prefer OUTBOUND connection over INBOUND
        // because we typically have an outbound connection for ongoing communication
        if let Some(ref c) = conn {
            if c.direction == crate::connection_pool::ConnectionDirection::Outbound {
                conn_opt = Some(c.clone());
            } else {
                // We only have an inbound connection
                // That's OK! The inbound connection is the same TCP wire, just from the peer's perspective
                // Responses will go back on the same TCP connection
                conn_opt = Some(c.clone());
            }
        }
    } else {
        conn_opt = pool.get_connection_by_addr(&peer_addr);
    };

    if let Some(conn) = conn_opt {
        if let Some(ref stream_handle) = conn.stream_handle {
            // Streaming responses always use the streaming protocol.
            if let Err(e) = stream_handle
                .stream_response_bytes(response, correlation_id)
                .await
            {
                warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id, "Failed to send streaming response");
            } else {
                // Intentionally quiet: this is the hot-path and can spam logs in benchmarks.
            }
        } else {
            // UDP transport has no stream writer path; fall back to inline response.
            send_inline_response(registry, peer_addr, correlation_id, response).await;
        }
    } else {
        warn!(peer = %peer_addr, correlation_id = correlation_id, "No connection found for streaming response");
    }
}

/// Send a response back to the peer for a non-streaming ask request.
/// Always uses the inline write queue (never streaming).
pub(crate) async fn send_inline_response(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    response: bytes::Bytes,
) {
    let pool = &registry.connection_pool;
    if let Some(conn) = pool.get_existing_connection(peer_addr) {
        if let Err(e) = conn.send_response_bytes(correlation_id, response).await {
            warn!(
                peer = %peer_addr,
                error = %e,
                correlation_id = correlation_id,
                "Failed to send inline response"
            );
        }
    } else {
        warn!(
            peer = %peer_addr,
            correlation_id = correlation_id,
            "No connection found for inline response"
        );
    }
}

/// Send a response back to the peer for a non-streaming ask request using aligned bytes.
pub(crate) async fn send_inline_response_aligned(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    response: crate::AlignedBytes,
) {
    send_inline_response(registry, peer_addr, correlation_id, response.into_bytes()).await;
}

/// Send a pooled response back to the peer for an ask request.
/// This keeps rkyv payloads zero-copy by writing the pooled buffer directly.
pub(crate) async fn send_pooled_response(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: crate::typed::PooledPayload,
    prefix: Option<[u8; 16]>,
    payload_len: usize,
) {
    let pool = &registry.connection_pool;
    if let Some(conn) = pool.get_existing_connection(peer_addr) {
        if let Err(e) = conn
            .send_response_pooled(correlation_id, payload, prefix, payload_len)
            .await
        {
            warn!(
                peer = %peer_addr,
                error = %e,
                correlation_id = correlation_id,
                "Failed to send pooled response"
            );
        }
    } else {
        warn!(
            peer = %peer_addr,
            correlation_id = correlation_id,
            "No connection found for pooled response"
        );
    }
}

pub(crate) async fn handle_response_message(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: crate::AlignedBytes,
    response_correlation: Option<&crate::connection_pool::CorrelationTracker>,
) {
    let mut payload = Some(payload);

    if let Some(correlation) = response_correlation {
        if correlation.complete(correlation_id, &mut payload) {
            return;
        }
    }

    let pool = &registry.connection_pool;

    // First, try to deliver via connection's embedded correlation tracker
    if let Some(conn) = pool.get_connection_by_addr(&peer_addr) {
        if let Some(ref correlation) = conn.correlation {
            if correlation.complete(correlation_id, &mut payload) {
                return;
            }
        }
    }

    // FALLBACK: Use shared correlation tracker by peer_id.
    if let Some(peer_id) = pool.get_peer_id_by_addr(&peer_addr) {
        if let Some(correlation) = pool.get_shared_correlation_tracker(&peer_id) {
            let _ = correlation.complete(correlation_id, &mut payload);
        }
    }
}

/// Parse a fully buffered TLS message from a pooled aligned buffer.
pub(crate) fn parse_message_from_pooled_buffer(
    buffer: crate::PooledAlignedBuffer,
    msg_len: usize,
) -> Result<MessageReadResult> {
    let msg_data = &buffer.as_ref()[crate::framing::LENGTH_PREFIX_LEN..];

    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    {
        if std::env::var("ICANACT_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
            crate::test_helpers::record_raw_payload(Bytes::copy_from_slice(msg_data));
        }
    }

    let raw = |buffer: crate::PooledAlignedBuffer| {
        let msg_buf = Bytes::from_owner(buffer);
        let msg_data = msg_buf.slice(crate::framing::LENGTH_PREFIX_LEN..);
        MessageReadResult::Raw(msg_data)
    };

    // Fast path: actor messages dominate the remote tell/ask hot path.
    if msg_len >= crate::framing::ACTOR_HEADER_LEN
        && matches!(
            crate::MessageType::from_byte(msg_data[0]),
            Some(crate::MessageType::ActorTell | crate::MessageType::ActorAsk)
        )
    {
        let msg_type_byte = msg_data[0];
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);
        let actor_id = u64::from_be_bytes(msg_data[12..20].try_into().unwrap());
        let type_hash = u32::from_be_bytes(msg_data[20..24].try_into().unwrap());
        let payload_len =
            u32::from_be_bytes([msg_data[24], msg_data[25], msg_data[26], msg_data[27]]) as usize;

        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
            return Ok(raw(buffer));
        }

        let payload_offset = crate::framing::LENGTH_PREFIX_LEN + crate::framing::ACTOR_HEADER_LEN;
        let payload = AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?;

        return Ok(MessageReadResult::Actor {
            msg_type: msg_type_byte,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload,
        });
    } else if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::Ask as u8
    {
        // This is an Ask message with envelope format:
        // [type:1][correlation_id:2][pad:1][payload:N]

        // Extract correlation ID (bytes 1-2)
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload_len = msg_len - crate::framing::ASK_RESPONSE_HEADER_LEN;
        let payload_offset =
            crate::framing::LENGTH_PREFIX_LEN + crate::framing::ASK_RESPONSE_HEADER_LEN;
        let payload_slice = &buffer.as_ref()[payload_offset..payload_offset + payload_len];

        // Try to deserialize as RegistryMessage first (Ask wrapper for gossip)
        match decode_registry_message(payload_slice) {
            Ok(msg) => {
                return Ok(MessageReadResult::Gossip(msg, Some(correlation_id)));
            }
            Err(err) => {
                let _ = err; // Non-gossip asks are expected to fail RegistryMessage decode.
                return Ok(MessageReadResult::AskRaw {
                    correlation_id,
                    payload: AlignedBytes::from_pooled_buffer_range(
                        buffer,
                        payload_offset,
                        payload_len,
                    )?,
                });
            }
        }
    } else if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::Response as u8
    {
        // Response message format:
        // [type:1][correlation_id:2][pad:1][payload:N]
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload_len = msg_len - crate::framing::ASK_RESPONSE_HEADER_LEN;
        let payload_offset =
            crate::framing::LENGTH_PREFIX_LEN + crate::framing::ASK_RESPONSE_HEADER_LEN;
        return Ok(MessageReadResult::Response {
            correlation_id,
            payload: AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?,
        });
    } else if msg_len >= crate::framing::DIRECT_ASK_HEADER_LEN
        && msg_data[0] == crate::MessageType::DirectAsk as u8
    {
        // DirectAsk message format (fast path):
        // [type:1][correlation_id:2][payload_len:4][payload:N]
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload_len =
            u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;

        if msg_data.len() < crate::framing::DIRECT_ASK_HEADER_LEN + payload_len {
            return Ok(raw(buffer));
        }

        let payload_offset =
            crate::framing::LENGTH_PREFIX_LEN + crate::framing::DIRECT_ASK_HEADER_LEN;
        return Ok(MessageReadResult::DirectAsk {
            correlation_id,
            payload: AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?,
        });
    } else if msg_len >= crate::framing::DIRECT_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::DirectResponse as u8
    {
        // DirectResponse message format (fast path):
        // [type:1][correlation_id:2][payload_len:4][payload:N]
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload_len =
            u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;

        if msg_data.len() < crate::framing::DIRECT_RESPONSE_HEADER_LEN + payload_len {
            return Ok(raw(buffer));
        }

        let payload_offset =
            crate::framing::LENGTH_PREFIX_LEN + crate::framing::DIRECT_RESPONSE_HEADER_LEN;
        return Ok(MessageReadResult::DirectResponse {
            correlation_id,
            payload: AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?,
        });
    } else if msg_len >= crate::framing::PUBSUB_HEADER_LEN
        && msg_data[0] == crate::MessageType::PubSub as u8
    {
        let payload_len = msg_len - crate::framing::PUBSUB_HEADER_LEN;
        let payload_offset = crate::framing::LENGTH_PREFIX_LEN + crate::framing::PUBSUB_HEADER_LEN;
        return Ok(MessageReadResult::PubSub {
            payload: AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?,
        });
    } else {
        // Check if this is a Gossip message with type prefix
        if msg_len >= 1 {
            let first_byte = msg_data[0];
            // Check if it's a known message type
            if let Some(msg_type) = crate::MessageType::from_byte(first_byte) {
                match msg_type {
                    crate::MessageType::Gossip
                        if msg_data.len() >= crate::framing::GOSSIP_HEADER_LEN =>
                    {
                        // This is a gossip message with type prefix, skip the type byte
                        let payload_offset =
                            crate::framing::LENGTH_PREFIX_LEN + crate::framing::GOSSIP_HEADER_LEN;
                        let payload_slice = &buffer.as_ref()[payload_offset..];
                        match decode_registry_message(payload_slice) {
                            Ok(msg) => return Ok(MessageReadResult::Gossip(msg, None)),
                            Err(err) => {
                                // Avoid spamming logs with rkyv error strings (can be very noisy in stress tests).
                                trace!(
                                    payload_len = payload_slice.len(),
                                    "Failed to decode gossip payload"
                                );
                                let _ = err;
                                return Ok(raw(buffer));
                            }
                        }
                    }
                    crate::MessageType::Gossip => {
                        return Ok(raw(buffer));
                    }
                    crate::MessageType::ActorTell | crate::MessageType::ActorAsk => {
                        // This is an actor message with envelope format:
                        // [type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN {
                            // Need at least 28 bytes for header
                            return Ok(raw(buffer));
                        }

                        // Parse the actor message envelope
                        let msg_type_byte = msg_data[0];
                        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                        let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);
                        let actor_id = u64::from_be_bytes(msg_data[12..20].try_into().unwrap());
                        let type_hash = u32::from_be_bytes(msg_data[20..24].try_into().unwrap());
                        let payload_len = u32::from_be_bytes([
                            msg_data[24],
                            msg_data[25],
                            msg_data[26],
                            msg_data[27],
                        ]) as usize;

                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
                            return Ok(raw(buffer));
                        }

                        let payload_offset =
                            crate::framing::LENGTH_PREFIX_LEN + crate::framing::ACTOR_HEADER_LEN;
                        let payload = AlignedBytes::from_pooled_buffer_range(
                            buffer,
                            payload_offset,
                            payload_len,
                        )?;

                        return Ok(MessageReadResult::Actor {
                            msg_type: msg_type_byte,
                            correlation_id,
                            actor_id,
                            type_hash,
                            schema_hash,
                            payload,
                        });
                    }
                    crate::MessageType::StreamStart
                    | crate::MessageType::StreamData
                    | crate::MessageType::StreamEnd
                    | crate::MessageType::StreamResponseStart
                    | crate::MessageType::StreamResponseData
                    | crate::MessageType::StreamResponseEnd => {
                        // Handle streaming messages
                        // Message format: [type:1][correlation_id:2][reserved:9][stream_header:36][chunk_data:N]
                        if msg_data.len()
                            < crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE
                        {
                            return Ok(raw(buffer));
                        }

                        // Extract correlation_id (bytes 1-2 after msg_type)
                        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                        let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);

                        // Parse the stream header (36 bytes starting at offset 12)
                        let header_bytes = &msg_data[crate::framing::STREAM_HEADER_PREFIX_LEN
                            ..crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE];
                        let stream_header = match crate::StreamHeader::from_bytes(header_bytes) {
                            Some(header) => header,
                            None => return Ok(raw(buffer)),
                        };

                        // Extract chunk data (everything after the header)
                        let chunk_start = crate::framing::LENGTH_PREFIX_LEN
                            + crate::framing::STREAM_HEADER_PREFIX_LEN
                            + crate::StreamHeader::SERIALIZED_SIZE;
                        let chunk_len = msg_len.saturating_sub(
                            crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE,
                        );
                        let msg_buf = Bytes::from_owner(buffer);
                        let chunk_data = if chunk_len > 0 {
                            msg_buf.slice(chunk_start..chunk_start + chunk_len)
                        } else {
                            bytes::Bytes::new()
                        };

                        return Ok(MessageReadResult::Streaming {
                            msg_type: first_byte,
                            correlation_id,
                            schema_hash,
                            stream_header,
                            chunk_data,
                        });
                    }
                    crate::MessageType::PubSub => {
                        if msg_data.len() < crate::framing::PUBSUB_HEADER_LEN {
                            return Ok(raw(buffer));
                        }
                        let payload_len = msg_len - crate::framing::PUBSUB_HEADER_LEN;
                        let payload_offset =
                            crate::framing::LENGTH_PREFIX_LEN + crate::framing::PUBSUB_HEADER_LEN;
                        return Ok(MessageReadResult::PubSub {
                            payload: AlignedBytes::from_pooled_buffer_range(
                                buffer,
                                payload_offset,
                                payload_len,
                            )?,
                        });
                    }
                    _ => {
                        // Unknown message type, treat as raw payload.
                        return Ok(raw(buffer));
                    }
                }
            }
        }
    }

    Ok(raw(buffer))
}

/// Read a message from a TLS reader
#[allow(dead_code)]
pub(crate) async fn read_message_from_tls_reader<R>(
    reader: &mut R,
    max_message_size: usize,
    aligned_pool: Option<&Arc<crate::AlignedBytesPool>>,
) -> Result<MessageReadResult>
where
    R: AsyncReadExt + Unpin,
{
    // CRITICAL_PATH: frame decode + header parsing must preserve alignment and bounds.
    // Read the message length (4 bytes)
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;

    if msg_len > max_message_size {
        return Err(crate::GossipError::MessageTooLarge {
            size: msg_len,
            max: max_message_size,
        });
    }

    // Read the message data into an aligned buffer that keeps the length prefix.
    let total_len = msg_len + crate::framing::LENGTH_PREFIX_LEN;

    if let Some(pool) = aligned_pool {
        let mut buffer =
            unsafe { crate::PooledAlignedBuffer::with_len_uninit(total_len, pool.clone()) };
        buffer.as_mut_slice()[..crate::framing::LENGTH_PREFIX_LEN].copy_from_slice(&len_buf);
        reader
            .read_exact(&mut buffer.as_mut_slice()[crate::framing::LENGTH_PREFIX_LEN..])
            .await?;
        return parse_message_from_pooled_buffer(buffer, msg_len);
    }

    let msg_buf = {
        let mut buffer = AlignedBuffer::with_capacity(total_len);
        // SAFETY: We immediately fill the entire buffer via read_exact below.
        unsafe {
            buffer.set_len(total_len);
        }
        buffer[..crate::framing::LENGTH_PREFIX_LEN].copy_from_slice(&len_buf);
        reader
            .read_exact(&mut buffer[crate::framing::LENGTH_PREFIX_LEN..])
            .await?;
        Bytes::from_owner(buffer)
    };
    let msg_data = msg_buf.slice(crate::framing::LENGTH_PREFIX_LEN..);

    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    {
        if std::env::var("ICANACT_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
            crate::test_helpers::record_raw_payload(msg_data.clone());
        }
    }

    // Check if this is an Ask message with envelope
    if msg_len >= crate::framing::ACTOR_HEADER_LEN
        && matches!(
            crate::MessageType::from_byte(msg_data[0]),
            Some(crate::MessageType::ActorTell | crate::MessageType::ActorAsk)
        )
    {
        let msg_type_byte = msg_data[0];
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);
        let actor_id = u64::from_be_bytes(msg_data[12..20].try_into().unwrap());
        let type_hash = u32::from_be_bytes(msg_data[20..24].try_into().unwrap());
        let payload_len =
            u32::from_be_bytes([msg_data[24], msg_data[25], msg_data[26], msg_data[27]]) as usize;

        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
            return Ok(MessageReadResult::Raw(msg_data));
        }

        let payload = msg_data.slice(
            crate::framing::ACTOR_HEADER_LEN..crate::framing::ACTOR_HEADER_LEN + payload_len,
        );
        let payload = AlignedBytes::from_bytes(payload)?;

        return Ok(MessageReadResult::Actor {
            msg_type: msg_type_byte,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload,
        });
    } else if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::Ask as u8
    {
        // This is an Ask message with envelope format:
        // [type:1][correlation_id:2][pad:1][payload:N]

        // Extract correlation ID (bytes 1-2)
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);

        // The actual RegistryMessage starts at byte 8
        // Create a properly aligned buffer for the payload
        let payload = msg_data.slice(crate::framing::ASK_RESPONSE_HEADER_LEN..);

        // Try to deserialize as RegistryMessage first (Ask wrapper for gossip)
        match decode_registry_message(payload.as_ref()) {
            Ok(msg) => Ok(MessageReadResult::Gossip(msg, Some(correlation_id))),
            Err(err) => {
                let _ = err; // Non-gossip asks are expected to fail RegistryMessage decode.
                Ok(MessageReadResult::AskRaw {
                    correlation_id,
                    payload: AlignedBytes::from_bytes(payload)?,
                })
            }
        }
    } else if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::Response as u8
    {
        // Response message format:
        // [type:1][correlation_id:2][pad:1][payload:N]
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload = msg_data.slice(crate::framing::ASK_RESPONSE_HEADER_LEN..);
        Ok(MessageReadResult::Response {
            correlation_id,
            payload: AlignedBytes::from_bytes(payload)?,
        })
    } else if msg_len >= crate::framing::DIRECT_ASK_HEADER_LEN
        && msg_data[0] == crate::MessageType::DirectAsk as u8
    {
        // DirectAsk message format (fast path):
        // [type:1][correlation_id:2][payload_len:4][payload:N]
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload_len =
            u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;

        if msg_data.len() < crate::framing::DIRECT_ASK_HEADER_LEN + payload_len {
            return Ok(MessageReadResult::Raw(msg_data));
        }

        let payload = msg_data.slice(
            crate::framing::DIRECT_ASK_HEADER_LEN
                ..crate::framing::DIRECT_ASK_HEADER_LEN + payload_len,
        );
        Ok(MessageReadResult::DirectAsk {
            correlation_id,
            payload: AlignedBytes::from_bytes(payload)?,
        })
    } else if msg_len >= crate::framing::DIRECT_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::DirectResponse as u8
    {
        // DirectResponse message format (fast path):
        // [type:1][correlation_id:2][payload_len:4][payload:N]
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload_len =
            u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;

        if msg_data.len() < crate::framing::DIRECT_RESPONSE_HEADER_LEN + payload_len {
            return Ok(MessageReadResult::Raw(msg_data));
        }

        let payload = msg_data.slice(
            crate::framing::DIRECT_RESPONSE_HEADER_LEN
                ..crate::framing::DIRECT_RESPONSE_HEADER_LEN + payload_len,
        );
        Ok(MessageReadResult::DirectResponse {
            correlation_id,
            payload: AlignedBytes::from_bytes(payload)?,
        })
    } else if msg_len >= crate::framing::PUBSUB_HEADER_LEN
        && msg_data[0] == crate::MessageType::PubSub as u8
    {
        let payload = msg_data.slice(crate::framing::PUBSUB_HEADER_LEN..);
        Ok(MessageReadResult::PubSub {
            payload: AlignedBytes::from_bytes(payload)?,
        })
    } else {
        // Check if this is a Gossip message with type prefix
        if msg_len >= 1 {
            let first_byte = msg_data[0];
            // Check if it's a known message type
            if let Some(msg_type) = crate::MessageType::from_byte(first_byte) {
                match msg_type {
                    crate::MessageType::Gossip
                        if msg_data.len() >= crate::framing::GOSSIP_HEADER_LEN =>
                    {
                        // This is a gossip message with type prefix, skip the type byte
                        // Create a properly aligned buffer for the payload
                        let payload = msg_data.slice(crate::framing::GOSSIP_HEADER_LEN..);
                        match decode_registry_message(payload.as_ref()) {
                            Ok(msg) => return Ok(MessageReadResult::Gossip(msg, None)),
                            Err(err) => {
                                // Avoid spamming logs with rkyv error strings (can be very noisy in stress tests).
                                trace!(
                                    payload_len = payload.len(),
                                    "Failed to decode gossip payload"
                                );
                                let _ = err;
                                return Ok(MessageReadResult::Raw(msg_data));
                            }
                        }
                    }
                    crate::MessageType::Gossip => {
                        return Ok(MessageReadResult::Raw(msg_data));
                    }
                    crate::MessageType::ActorTell | crate::MessageType::ActorAsk => {
                        // This is an actor message with envelope format:
                        // [type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN {
                            // Need at least 28 bytes for header
                            return Ok(MessageReadResult::Raw(msg_data));
                        }

                        // Parse the actor message envelope
                        // Wire format: [type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
                        let msg_type_byte = msg_data[0];
                        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                        let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);
                        // Skip reserved bytes (3-11), actor_id starts at byte 12
                        let actor_id = u64::from_be_bytes(msg_data[12..20].try_into().unwrap());
                        let type_hash = u32::from_be_bytes(msg_data[20..24].try_into().unwrap());
                        let payload_len =
                            u32::from_be_bytes(msg_data[24..28].try_into().unwrap()) as usize;

                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
                            return Ok(MessageReadResult::Raw(msg_data));
                        }

                        let payload = msg_data.slice(
                            crate::framing::ACTOR_HEADER_LEN
                                ..crate::framing::ACTOR_HEADER_LEN + payload_len,
                        );
                        let payload = AlignedBytes::from_bytes(payload)?;

                        return Ok(MessageReadResult::Actor {
                            msg_type: msg_type_byte,
                            correlation_id,
                            actor_id,
                            type_hash,
                            schema_hash,
                            payload,
                        });
                    }
                    crate::MessageType::StreamStart
                    | crate::MessageType::StreamData
                    | crate::MessageType::StreamEnd
                    | crate::MessageType::StreamResponseStart
                    | crate::MessageType::StreamResponseData
                    | crate::MessageType::StreamResponseEnd => {
                        // Handle streaming messages
                        // Message format: [type:1][correlation_id:2][reserved:9][stream_header:36][chunk_data:N]
                        if msg_data.len()
                            < crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE
                        {
                            return Ok(MessageReadResult::Raw(msg_data));
                        }

                        // Extract correlation_id (bytes 1-2 after msg_type)
                        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                        let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);

                        // Parse the stream header (36 bytes starting at offset 12)
                        let header_bytes = &msg_data[crate::framing::STREAM_HEADER_PREFIX_LEN
                            ..crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE];
                        let stream_header = match crate::StreamHeader::from_bytes(header_bytes) {
                            Some(header) => header,
                            None => return Ok(MessageReadResult::Raw(msg_data)),
                        };

                        // Extract chunk data (everything after the header)
                        let chunk_data = if msg_data.len()
                            > crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE
                        {
                            msg_data.slice(
                                crate::framing::STREAM_HEADER_PREFIX_LEN
                                    + crate::StreamHeader::SERIALIZED_SIZE..,
                            )
                        } else {
                            bytes::Bytes::new()
                        };

                        return Ok(MessageReadResult::Streaming {
                            msg_type: first_byte,
                            correlation_id,
                            schema_hash,
                            stream_header,
                            chunk_data,
                        });
                    }
                    crate::MessageType::PubSub => {
                        if msg_data.len() < crate::framing::PUBSUB_HEADER_LEN {
                            return Ok(MessageReadResult::Raw(msg_data));
                        }
                        let payload = msg_data.slice(crate::framing::PUBSUB_HEADER_LEN..);
                        return Ok(MessageReadResult::PubSub {
                            payload: AlignedBytes::from_bytes(payload)?,
                        });
                    }
                    _ => {
                        // Unknown message type, treat as raw payload.
                        return Ok(MessageReadResult::Raw(msg_data));
                    }
                }
            }
        }

        Ok(MessageReadResult::Raw(msg_data))
    }
}

#[cfg(test)]
mod framing_tests {
    use super::{MessageReadResult, read_message_from_tls_reader};
    use crate::{MessageType, framing, registry::RegistryMessage};
    use tokio::io::AsyncWriteExt;

    async fn read_frame(frame: Vec<u8>) -> MessageReadResult {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            writer.write_all(&frame).await.unwrap();
        });
        read_message_from_tls_reader(&mut reader, 1024 * 1024, None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn ask_raw_parses_with_padded_header() {
        let payload_bytes = b"hello";
        let header = framing::write_ask_response_header(MessageType::Ask, 42, payload_bytes.len());
        let mut frame = Vec::with_capacity(header.len() + payload_bytes.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(payload_bytes);

        match read_frame(frame).await {
            MessageReadResult::AskRaw {
                correlation_id,
                payload: body,
            } => {
                assert_eq!(correlation_id, 42);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn response_parses_with_padded_header() {
        let payload_bytes = b"world";
        let header =
            framing::write_ask_response_header(MessageType::Response, 7, payload_bytes.len());
        let mut frame = Vec::with_capacity(header.len() + payload_bytes.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(payload_bytes);

        match read_frame(frame).await {
            MessageReadResult::Response {
                correlation_id,
                payload: body,
            } => {
                assert_eq!(correlation_id, 7);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn actor_tell_parses_with_reordered_header() {
        let payload_bytes = b"actor_payload";
        let actor_id = 0x0102030405060708u64;
        let type_hash = 0x11223344u32;

        // Wire format: [len:4][type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
        let total_len = framing::ACTOR_HEADER_LEN + payload_bytes.len();
        let mut frame = Vec::with_capacity(framing::LENGTH_PREFIX_LEN + total_len);
        frame.extend_from_slice(&(total_len as u32).to_be_bytes()); // 4 bytes: length prefix
        frame.push(MessageType::ActorTell as u8); // 1 byte: message type
        frame.extend_from_slice(&0u16.to_be_bytes()); // 2 bytes: correlation_id
        frame.extend_from_slice(&[0u8; 9]); // 9 bytes: reserved (for 32-byte alignment)
        frame.extend_from_slice(&actor_id.to_be_bytes()); // 8 bytes: actor_id
        frame.extend_from_slice(&type_hash.to_be_bytes()); // 4 bytes: type_hash
        frame.extend_from_slice(&(payload_bytes.len() as u32).to_be_bytes()); // 4 bytes: payload_len
        frame.extend_from_slice(payload_bytes); // N bytes: payload

        match read_frame(frame).await {
            MessageReadResult::Actor {
                msg_type,
                correlation_id,
                actor_id: parsed_actor_id,
                type_hash: parsed_type_hash,
                schema_hash,
                payload: body,
            } => {
                assert_eq!(msg_type, MessageType::ActorTell as u8);
                assert_eq!(correlation_id, 0);
                assert_eq!(parsed_actor_id, actor_id);
                assert_eq!(parsed_type_hash, type_hash);
                assert_eq!(schema_hash, None);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn gossip_registry_payload_deserializes_from_aligned_buffer() {
        let message = RegistryMessage::ImmediateAck {
            actor_name: "test_actor".to_string(),
            success: true,
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&message).unwrap();
        let header = framing::write_gossip_frame_prefix(payload.len());
        let mut frame = Vec::with_capacity(header.len() + payload.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&payload);

        match read_frame(frame).await {
            MessageReadResult::Gossip(parsed, correlation_id) => {
                assert!(correlation_id.is_none());
                match parsed {
                    RegistryMessage::ImmediateAck {
                        actor_name,
                        success,
                    } => {
                        assert_eq!(actor_name, "test_actor");
                        assert!(success);
                    }
                    other => panic!("unexpected gossip payload: {:?}", other),
                }
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }
}

#[cfg(test)]
mod keepalive_apply_tests {
    use super::*;

    #[tokio::test]
    async fn tcp_keepalive_bootstrap_supports_builder_tls_runtime() {
        let a_keypair = crate::KeyPair::new_for_testing("keepalive-a");

        let handle = GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            a_keypair.to_secret_key(),
            None,
            crate::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        handle.shutdown_and_wait().await;
    }
}

/// Zero-copy gossip message sender - eliminates bottlenecks in serialization and connection handling
async fn send_gossip_message_zero_copy(
    mut task: GossipTask,
    registry: Arc<GossipRegistry>,
) -> Result<()> {
    let mut conn = registry
        .connection_pool
        .get_existing_connection(task.peer_addr);
    if conn.is_none() && !registry.should_attempt_outbound_dial(task.peer_addr).await {
        debug!(
            peer = %task.peer_addr,
            "Skipping outbound dial for inbound-only undialable peer; waiting for remote-side reconnect"
        );
        return Ok(());
    }

    // Check if this is a retry attempt and if DNS refresh is needed
    let (is_retry, has_dns) = {
        let gossip_state = registry.gossip_state.lock().await;
        let peer_info = gossip_state.peers.get(&task.peer_addr);
        (
            peer_info.map(|p| p.failures > 0).unwrap_or(false),
            peer_info.map(|p| p.dns_name.is_some()).unwrap_or(false),
        )
    };

    if is_retry {
        info!(
            peer = %task.peer_addr,
            has_dns = has_dns,
            "🔄 GOSSIP RETRY: Attempting to reconnect to previously failed peer"
        );

        // If the peer has a DNS name, try to re-resolve it before connecting
        // This handles Kubernetes pod restarts where the IP changes
        if has_dns {
            if let Some(new_addr) = registry.refresh_peer_dns(task.peer_addr).await {
                info!(
                    old_addr = %task.peer_addr,
                    new_addr = %new_addr,
                    "🔄 DNS refresh: Using new IP address for peer"
                );
                task.peer_addr = new_addr;
            }
        }
    }

    // Get connection with minimal lock contention
    let conn = if let Some(conn) = conn.take() {
        conn
    } else {
        let pool = &registry.connection_pool;
        debug!(
            "GOSSIP: Pool has {} connections before get_connection",
            pool.connection_count()
        );
        match pool.get_connection(task.peer_addr).await {
            Ok(conn) => {
                if is_retry {
                    info!(
                        peer = %task.peer_addr,
                        "✅ GOSSIP RETRY: Successfully reconnected to peer"
                    );
                }
                conn
            }
            Err(e) => {
                if is_retry {
                    info!(
                        peer = %task.peer_addr,
                        error = %e,
                        "❌ GOSSIP RETRY: Failed to reconnect to peer"
                    );
                }
                return Err(e);
            }
        }
    };

    if matches!(
        task.message,
        crate::registry::RegistryMessage::PeerListGossip { .. }
    ) && !registry.peer_supports_peer_list(&task.peer_addr).await
    {
        debug!(
            peer = %task.peer_addr,
            "Skipping PeerListGossip send - peer lacks negotiated capability"
        );
        return Ok(());
    }

    // CRITICAL: Set precise timing RIGHT BEFORE TCP write to exclude all scheduling delays
    // Update wall_clock_time in delta changes to current time for accurate propagation measurement
    let _current_time_secs = crate::current_timestamp();
    let current_time_nanos = crate::current_timestamp_nanos();

    // Debug: Check if there's a delay in the task creation vs sending
    if let crate::registry::RegistryMessage::DeltaGossip { delta, .. } = &task.message {
        for change in &delta.changes {
            if let crate::registry::RegistryChange::ActorAdded { location, .. } = change {
                let creation_time_nanos = location.wall_clock_time as u128 * 1_000_000_000;
                let delay_nanos = current_time_nanos as u128 - creation_time_nanos;
                let _delay_ms = delay_nanos as f64 / 1_000_000.0;
                // eprintln!("🔍 DELTA_SEND_DELAY: {}ms between delta creation and sending", delay_ms);
            }
        }
    }

    match &mut task.message {
        crate::registry::RegistryMessage::DeltaGossip { delta, extensions } => {
            delta.precise_timing_nanos = current_time_nanos;
            // Update wall_clock_time in all changes to current time for accurate propagation measurement
            for change in &mut delta.changes {
                match change {
                    crate::registry::RegistryChange::ActorAdded { location, .. } => {
                        // Set wall_clock_time to nanoseconds for consistent timing measurements
                        location.wall_clock_time = current_time_nanos / 1_000_000_000;
                    }
                    crate::registry::RegistryChange::ActorRemoved { .. } => {
                        // No wall_clock_time to update
                    }
                }
            }
            *extensions = registry
                .gossip_extensions_for_outbound(task.peer_addr, current_time_nanos)
                .await;
        }
        crate::registry::RegistryMessage::FullSync { extensions, .. } => {
            *extensions = registry
                .gossip_extensions_for_outbound(task.peer_addr, current_time_nanos)
                .await;
        }
        _ => {}
    }

    // Serialize the message AFTER updating timing
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&task.message)?;

    // Create message with Gossip type prefix
    let mut msg_with_type = Vec::with_capacity(crate::framing::GOSSIP_HEADER_LEN + data.len());
    msg_with_type.push(crate::MessageType::Gossip as u8);
    msg_with_type.resize(crate::framing::GOSSIP_HEADER_LEN, 0);
    msg_with_type.extend_from_slice(&data);

    // Use zero-copy tell() which uses try_send() internally for max performance
    // This completely bypasses async overhead when the channel has capacity
    let tcp_start = std::time::Instant::now();
    conn.tell(bytes::Bytes::from(msg_with_type)).await?;
    let _tcp_elapsed = tcp_start.elapsed();
    // eprintln!("🔍 TCP_WRITE_TIME: {:?}", tcp_elapsed);
    Ok(())
}
#[cfg(test)]
mod inbound_tls_identity_tests {
    use super::inbound_tls_sender_is_authenticated;
    use crate::KeyPair;

    #[test]
    fn inbound_tls_sender_identity_requires_matching_certificate_node_id() {
        let authenticated = KeyPair::new_for_testing("inbound-tls-authenticated")
            .peer_id()
            .to_node_id();
        let claimed = KeyPair::new_for_testing("inbound-tls-claimed")
            .peer_id()
            .to_node_id();

        assert!(inbound_tls_sender_is_authenticated(
            Some(authenticated),
            authenticated
        ));
        assert!(!inbound_tls_sender_is_authenticated(None, authenticated));
        assert!(!inbound_tls_sender_is_authenticated(
            Some(authenticated),
            claimed
        ));
    }
}
