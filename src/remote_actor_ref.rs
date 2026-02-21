use crate::RemoteActorLocation;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

/// A remote actor reference with a cached connection for zero-lookup message sending.
///
/// This is returned by `lookup()` and provides `tell()`, `ask()`, and `ask_streaming_bytes()`
/// methods that use the cached connection directly (no hashmap lookups, just pointer deref).
///
/// # Resource Management
///
/// `RemoteActorRef` uses weak references to prevent memory leaks:
/// - `registry: Weak<GossipRegistry>` - doesn't prevent registry cleanup
/// - `connection: Option<Arc<Mutex<ConnectionHandle>>>` - optional strong ref
///
/// When the registry shuts down, `tell()`/`ask()` will return `Err(Shutdown)`.
/// Connections are cleaned up by periodic `cleanup_stale_connections()` calls.
///
/// # Connection Optional for Unstarted Actors
///
/// For actors that are registered but not yet listening (e.g., during testing),
/// `connection` may be `None`. In this case, `tell()`/`ask()` will attempt to
/// establish the connection lazily on first use.
///
/// # DNS Reconnection (Kubernetes Pod Restarts)
///
/// When a peer's IP changes due to DNS refresh (e.g., Kubernetes pod restart):
/// - The old TCP connection dies and is removed from the connection pool
/// - `RemoteActorRef` detects this on the next `tell()`/`ask()` call
/// - It automatically reconnects to the peer using the updated peer_id→addr mapping
/// - Subsequent messages use the fresh connection (zero additional lookups)
///
/// This provides **self-healing** behavior - no manual re-lookup needed!
///
/// # Example
/// ```ignore
/// // Step 1: Lookup does ALL the work - finds actor AND caches connection
/// let remote_actor = registry.lookup("chat_service").await?;
///
/// // Step 2: tell/ask use cached connection - ZERO lookups, just pointer deref
/// remote_actor.tell(message1).await?;
/// remote_actor.tell(message2).await?;
/// remote_actor.ask(request).await?;
///
/// // Even if peer's IP changes (pod restart), RemoteActorRef auto-reconnects!
/// ```
#[derive(Clone)]
pub struct RemoteActorRef {
    /// The actor location information
    pub location: RemoteActorLocation,
    /// Cached connection handle - set during lookup(), used for direct zero-lookup sending
    /// Lock-free access - ConnectionHandle uses lock-free stream operations
    /// None for actors that aren't listening yet (will be established on first use)
    ///
    /// # Testing
    /// This field is only accessible in test builds via cfg(test). Production code should
    /// always use the public `tell()`, `ask()`, and `ask_streaming_bytes()` methods.
    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    pub connection: Option<Arc<crate::connection_pool::ConnectionHandle>>,
    /// Cached connection handle (production - private)
    #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
    connection: Option<Arc<crate::connection_pool::ConnectionHandle>>,
    /// Registry weak reference - doesn't prevent registry shutdown/cleanup
    /// Used for reconnection after DNS changes
    registry: Weak<crate::registry::GossipRegistry>,
}

impl RemoteActorRef {
    /// Create a new RemoteActorRef from location and connection
    /// Note: This creates a RemoteActorRef without a registry reference (cannot auto-reconnect)
    /// Prefer using `with_registry()` which is called by `lookup()`
    pub fn new(
        location: RemoteActorLocation,
        connection: crate::connection_pool::ConnectionHandle,
    ) -> Self {
        Self {
            location,
            connection: Some(Arc::new(connection)),
            registry: Weak::new(), // No registry reference - cannot reconnect
        }
    }

    /// Create a new RemoteActorRef with optional connection and registry reference (for auto-reconnection)
    /// Called by `lookup()` - uses Weak to prevent reference cycles
    pub(crate) fn with_registry(
        location: RemoteActorLocation,
        connection: Option<crate::connection_pool::ConnectionHandle>,
        registry: Arc<crate::registry::GossipRegistry>,
    ) -> Self {
        Self {
            location,
            connection: connection.map(Arc::new),
            registry: Arc::downgrade(&registry), // Weak reference - prevents cycle
        }
    }

    /// Check if registry is still alive (for shutdown detection)
    /// Lock-free check using strong_count
    ///
    /// Note: This may return true even after shutdown() is called if there are
    /// other Arc references (e.g., from background tasks). The reliable way to
    /// detect shutdown is to attempt operations and check for Err(Shutdown).
    pub fn is_registry_alive(&self) -> bool {
        self.registry.strong_count() > 0
    }

    /// Get a reference to the underlying connection handle for advanced use cases.
    ///
    /// This provides access to low-level operations like `ask_direct()` which
    /// bypass the RegistryMessage overhead for maximum performance.
    ///
    /// Returns None if no connection is established yet.
    pub fn connection_ref(&self) -> Option<&Arc<crate::connection_pool::ConnectionHandle>> {
        self.connection.as_ref()
    }

    /// Send a fire-and-forget message to the remote actor.
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    /// ConnectionHandle internally uses lock-free stream operations.
    ///
    /// Returns error if registry has shut down or no connection is available.
    pub async fn tell(&self, message: &[u8]) -> crate::Result<()> {
        // Check if registry has been shut down
        if let Some(registry) = self.registry.upgrade() {
            // LOCK-FREE CHECK: Check atomic shutdown flag
            if registry.shutdown.load(Ordering::Relaxed) {
                return Err(crate::GossipError::Shutdown);
            }
        } else {
            // Registry was dropped
            return Err(crate::GossipError::Shutdown);
        }

        // Get connection reference
        let conn = self.connection.as_ref().ok_or_else(|| {
            crate::GossipError::ActorNotFound(format!(
                "'{}' - not listening yet",
                self.location.address
            ))
        })?;

        // Direct call - ZERO LOCKS
        // ConnectionHandle.tell() uses internal lock-free operations
        conn.tell(message).await
    }

    /// Send a request and wait for a response.
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    /// ZERO-COPY: Returns Bytes instead of Vec<u8> to avoid allocation.
    /// ConnectionHandle internally uses lock-free stream operations.
    ///
    /// Returns error if registry has shut down or no connection is available.
    pub async fn ask(&self, request: &[u8]) -> crate::Result<bytes::Bytes> {
        // Check if registry has been shut down
        if let Some(registry) = self.registry.upgrade() {
            if registry.shutdown.load(Ordering::Relaxed) {
                return Err(crate::GossipError::Shutdown);
            }
        } else {
            return Err(crate::GossipError::Shutdown);
        }

        let conn = self.connection.as_ref().ok_or_else(|| {
            crate::GossipError::ActorNotFound(format!(
                "'{}' - not listening yet",
                self.location.address
            ))
        })?;

        // Direct call - ZERO LOCKS
        conn.ask(request).await
    }

    /// Send a request with timeout and wait for response
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    /// ZERO-COPY: Takes owned Bytes to avoid allocation.
    pub async fn ask_with_timeout(
        &self,
        request: bytes::Bytes,
        timeout: std::time::Duration,
    ) -> crate::Result<bytes::Bytes> {
        // Check if registry has been shut down
        if let Some(registry) = self.registry.upgrade() {
            if registry.shutdown.load(Ordering::Relaxed) {
                return Err(crate::GossipError::Shutdown);
            }
        } else {
            return Err(crate::GossipError::Shutdown);
        }

        let conn = self.connection.as_ref().ok_or_else(|| {
            crate::GossipError::ActorNotFound(format!(
                "'{}' - not listening yet",
                self.location.address
            ))
        })?;

        conn.ask_with_timeout_bytes(request, timeout).await
    }

    /// Send a request and return a deferred handle that can be awaited later.
    ///
    /// This is the correct way to delegate "waiting for the response" to another task.
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    pub async fn ask_deferred(
        &self,
        request: &[u8],
    ) -> crate::Result<crate::connection_pool::PendingAsk> {
        // Check if registry has been shut down
        if let Some(registry) = self.registry.upgrade() {
            if registry.shutdown.load(Ordering::Relaxed) {
                return Err(crate::GossipError::Shutdown);
            }
        } else {
            return Err(crate::GossipError::Shutdown);
        }

        let conn = self.connection.as_ref().ok_or_else(|| {
            crate::GossipError::ActorNotFound(format!(
                "'{}' - not listening yet",
                self.location.address
            ))
        })?;

        conn.ask_deferred(request).await
    }

    /// Send a typed fire-and-forget message
    pub async fn tell_typed<T>(&self, message: &T) -> crate::Result<()>
    where
        T: crate::typed::WireEncode,
    {
        let bytes = crate::typed::encode_typed(message)?;
        self.tell(&bytes).await
    }

    /// Send a typed request and wait for a typed response
    pub async fn ask_typed<T, R>(&self, request: &T) -> crate::Result<R>
    where
        T: crate::typed::WireEncode,
        R: crate::typed::WireType + rkyv::Archive,
        for<'a> R::Archived: rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            > + rkyv::Deserialize<R, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    {
        let req_bytes = crate::typed::encode_typed(request)?;
        let resp_bytes = self.ask(&req_bytes).await?;
        crate::typed::decode_typed(&resp_bytes)
    }

    /// Send a large request using streaming (for payloads > 1MB)
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    /// ConnectionHandle internally uses lock-free stream operations.
    ///
    /// Returns error if registry has shut down or no connection is available.
    pub async fn ask_streaming_bytes(
        &self,
        payload: bytes::Bytes,
        actor_id: u64,
        type_hash: u32,
        timeout: std::time::Duration,
    ) -> crate::Result<bytes::Bytes> {
        // Check if registry has been shut down
        if let Some(registry) = self.registry.upgrade() {
            if registry.shutdown.load(Ordering::Relaxed) {
                return Err(crate::GossipError::Shutdown);
            }
        } else {
            return Err(crate::GossipError::Shutdown);
        }

        let conn = self.connection.as_ref().ok_or_else(|| {
            crate::GossipError::ActorNotFound(format!(
                "'{}' - not listening yet",
                self.location.address
            ))
        })?;

        // Direct call - ZERO LOCKS
        conn.ask_streaming_bytes(payload, type_hash, actor_id, timeout)
            .await
    }

    /// Get the streaming threshold for this connection
    pub fn streaming_threshold(&self) -> usize {
        crate::connection_pool::STREAMING_THRESHOLD
    }
}

// Custom Debug implementation
impl std::fmt::Debug for RemoteActorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteActorRef")
            .field("location", &self.location)
            .field("connection", &"<connection>")
            .field("registry_alive", &self.is_registry_alive())
            .finish()
    }
}
