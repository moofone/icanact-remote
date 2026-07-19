use crate::RemoteActorLocation;
use arc_swap::ArcSwapOption;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

/// High-level connection view used by `RemoteActorRef`.
#[derive(Clone)]
pub struct RemoteConnection {
    pub addr: SocketAddr,
    inner: crate::connection_pool::ConnectionHandle,
}

impl RemoteConnection {
    pub(crate) fn from_handle(handle: crate::connection_pool::ConnectionHandle) -> Self {
        let addr = handle.addr;
        Self {
            addr,
            inner: handle,
        }
    }

    pub fn bytes_written(&self) -> usize {
        self.inner.bytes_written()
    }

    /// Instance id of the specific connection this handle was cached from —
    /// used to retire exactly this instance (never "whatever is currently
    /// indexed for the peer") when an ask on it fails or is cancelled.
    pub(crate) fn instance_id(&self) -> Option<u64> {
        self.inner.instance_id()
    }

    pub fn is_streaming_active(&self) -> bool {
        self.inner.is_streaming_active()
    }

    pub fn sequence_number(&self) -> usize {
        self.inner.sequence_number()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub async fn tell_bytes(&self, message: bytes::Bytes) -> crate::Result<()> {
        self.inner.tell_bytes(message).await
    }

    pub fn try_tell_bytes(&self, message: bytes::Bytes) -> crate::Result<()> {
        self.inner.try_tell_bytes(message)
    }

    pub async fn tell_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> crate::Result<()> {
        self.inner
            .tell_actor_frame(actor_id, type_hash, payload)
            .await
    }

    pub fn try_tell_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> crate::Result<()> {
        self.inner
            .try_tell_actor_frame(actor_id, type_hash, payload)
    }

    pub async fn pubsub_frame(&self, payload: bytes::Bytes) -> crate::Result<()> {
        self.inner.send_pubsub_payload(payload).await
    }

    pub fn try_pubsub_frame(&self, payload: bytes::Bytes) -> crate::Result<()> {
        self.inner.try_send_pubsub_payload(payload)
    }

    pub fn try_pubsub_frame_pooled(
        &self,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> crate::Result<()> {
        self.inner
            .try_send_pubsub_payload_pooled(payload, prefix, payload_len)
    }

    pub async fn ask_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
    ) -> crate::Result<bytes::Bytes> {
        self.inner
            .ask_actor_frame(actor_id, type_hash, payload, timeout)
            .await
    }

    pub async fn ask_actor_frame_aligned(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
    ) -> crate::Result<crate::AlignedBytes> {
        self.inner
            .ask_actor_frame_aligned(actor_id, type_hash, payload, timeout)
            .await
    }

    pub async fn ask_actor_frame_no_timeout(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> crate::Result<bytes::Bytes> {
        self.inner
            .ask_actor_frame_no_timeout(actor_id, type_hash, payload)
            .await
    }

    pub async fn ask_actor_frame_no_timeout_aligned(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> crate::Result<crate::AlignedBytes> {
        self.inner
            .ask_actor_frame_no_timeout_aligned(actor_id, type_hash, payload)
            .await
    }

    pub async fn ask_actor_frame_deferred(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
    ) -> crate::Result<crate::DeferredAsk> {
        let pending = self
            .inner
            .ask_actor_frame_deferred(actor_id, type_hash, payload, timeout)
            .await?;
        Ok(crate::DeferredAsk::from_pending(pending))
    }

    pub async fn ask_with_timeout_bytes(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> crate::Result<bytes::Bytes> {
        self.inner.ask_with_timeout_bytes(request, timeout).await
    }

    pub async fn ask(&self, request: bytes::Bytes) -> crate::Result<bytes::Bytes> {
        self.inner.ask(request).await
    }

    pub async fn ask_direct(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> crate::Result<bytes::Bytes> {
        self.inner.ask_direct(request, timeout).await
    }

    pub async fn ask_direct_no_timeout(
        &self,
        request: bytes::Bytes,
    ) -> crate::Result<bytes::Bytes> {
        self.inner.ask_direct_no_timeout(request).await
    }

    pub async fn ask_streaming_bytes(
        &self,
        payload: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
        timeout: Duration,
    ) -> crate::Result<bytes::Bytes> {
        self.inner
            .ask_streaming_bytes(payload, type_hash, actor_id, timeout)
            .await
    }

    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> crate::Result<()> {
        self.inner
            .stream_large_message(msg, type_hash, actor_id)
            .await
    }

    /// Canonical zero-copy streaming tell API for an already-owned payload.
    pub async fn stream_large_message_bytes(
        &self,
        payload: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
    ) -> crate::Result<()> {
        self.inner
            .stream_large_message_bytes(payload, type_hash, actor_id)
            .await
    }

    pub async fn ask_deferred(&self, request: bytes::Bytes) -> crate::Result<crate::DeferredAsk> {
        let pending = self.inner.ask_deferred(request).await?;
        Ok(crate::DeferredAsk::from_pending(pending))
    }

    pub async fn tell_typed<M>(&self, message: &M) -> crate::Result<()>
    where
        M: crate::typed::WireEncode,
    {
        self.inner.tell_typed(message).await
    }

    pub async fn ask_typed<M, R>(&self, request: &M) -> crate::Result<R>
    where
        M: crate::typed::WireEncode,
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
        self.inner.ask_typed(request).await
    }

    pub async fn ask_typed_archived<M, R>(
        &self,
        request: &M,
    ) -> crate::Result<crate::typed::ArchivedBytes<R>>
    where
        M: crate::typed::WireEncode,
        R: crate::typed::WireType + rkyv::Archive,
        for<'a> R::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        self.inner.ask_typed_archived(request).await
    }

    pub async fn ask_typed_archived_with_timeout<M, R>(
        &self,
        request: &M,
        timeout: std::time::Duration,
    ) -> crate::Result<crate::typed::ArchivedBytes<R>>
    where
        M: crate::typed::WireEncode,
        R: crate::typed::WireType + rkyv::Archive,
        for<'a> R::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        self.inner
            .ask_typed_archived_with_timeout(request, timeout)
            .await
    }
}

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
/// When the registry shuts down, `tell()`/`ask()` will fail on the next use.
/// Cached connections may observe `ConnectionClosed` before weak-registry shutdown is noticed.
/// Connections are cleaned up by periodic `cleanup_stale_connections()` calls.
///
/// # Connection Optional for Unstarted Actors
///
/// For actors that are registered but not yet listening (e.g., during testing),
/// `connection` may be `None`. In this case, `tell()`/`ask()` will attempt to
/// establish the connection lazily on first use.
///
/// # Self-Healing Reconnection
///
/// The cached connection lives in a lock-free swappable slot
/// (`ArcSwapOption<RemoteConnection>`), not a fixed field set once at
/// construction. When a transport-level failure is observed on `tell()`/
/// `ask()` (connection reset, broken pipe, or an already-closed handle) —
/// or an actor-ask times out, subject to `ConnectionRecoveryPolicy` — the
/// ref re-resolves the peer through the registry's connection pool
/// (`peer_id` → address, refreshing DNS as needed), retries the operation
/// once against the fresh connection, and **persists** the healed
/// connection back into the slot so every subsequent call on this ref (and
/// on any of its clones, which share the same slot) uses it directly with
/// zero additional lookups.
///
/// This provides **self-healing** behavior - no manual re-lookup needed!
/// A failed re-resolution (e.g. the peer is genuinely unreachable, or the
/// registry has shut down) still returns a normal error rather than
/// retrying indefinitely.
///
/// # Example
/// ```no_run
/// # use bytes::Bytes;
/// # use icanact_remote::{GossipRegistryHandle, Result};
/// # async fn send_messages(registry: &GossipRegistryHandle) -> Result<()> {
/// // Step 1: Lookup does ALL the work - finds actor AND caches connection
/// let Some(remote_actor) = registry.lookup("chat_service").await else {
///     return Ok(());
/// };
///
/// // Step 2: tell/ask use cached connection - ZERO lookups, just pointer deref
/// remote_actor.tell(Bytes::from_static(b"message1")).await?;
/// remote_actor.tell(Bytes::from_static(b"message2")).await?;
/// let _response = remote_actor.ask(Bytes::from_static(b"request")).await?;
///
/// // Even if peer's IP changes (pod restart), RemoteActorRef auto-reconnects!
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RemoteActorRef<T = ()> {
    /// The actor location information
    pub location: RemoteActorLocation,
    /// Initial cached connection, retained for debug/test compatibility with
    /// the previously public field. Use [`Self::connection_ref`] to observe
    /// the current self-healing connection.
    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    pub connection: Option<RemoteConnection>,
    #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
    connection: Option<RemoteConnection>,
    /// Cached connection handle - set during `lookup()`, used for direct
    /// zero-lookup sending. Lock-free swappable slot: a transport-level
    /// failure or actor-ask timeout can atomically replace the contents
    /// with a freshly re-resolved connection (see the "Self-Healing
    /// Reconnection" section above), and every clone of this ref shares the
    /// same `Arc<ArcSwapOption<..>>` slot so the heal is visible everywhere.
    /// `None` for actors that aren't listening yet (established lazily on
    /// first use).
    connection_slot: Arc<ArcSwapOption<RemoteConnection>>,
    /// Registry weak reference - doesn't prevent registry shutdown/cleanup
    /// Used for reconnection after DNS changes
    registry: Weak<crate::registry::GossipRegistry>,
    _marker: PhantomData<fn() -> T>,
}

struct ActorAskCancellationGuard {
    registry: Arc<crate::registry::GossipRegistry>,
    peer_id: crate::PeerId,
    addr: SocketAddr,
    instance_id: Option<u64>,
    armed: bool,
}

impl ActorAskCancellationGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ActorAskCancellationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Instance-scoped, not peer-wide: this guard was armed for the
        // SPECIFIC connection instance the cancelled ask was made on
        // (`addr`/`instance_id` captured from that connection at guard
        // creation). A peer-wide `disconnect_connection_by_peer_id` here
        // would tear down whatever is *currently* indexed for the peer,
        // which may already be a fresh, healthy reconnection established
        // after this ask was sent — collateral teardown of a session this
        // cancellation has nothing to do with. `instance_id == None` means
        // the connection this ask ran on had no live stream handle to begin
        // with, so there is nothing identifiable to retire.
        // Peer-id-aware: `self.peer_id` is known here (captured at guard
        // creation), so current-session cleanup does not depend on
        // `addr_to_peer_id[self.addr]` still holding this peer's alias — see
        // `ConnectionPool::remove_connection_instance_for_peer`.
        let evicted = self.instance_id.is_some_and(|instance_id| {
            self.registry
                .connection_pool
                .remove_connection_instance_for_peer(&self.peer_id, self.addr, instance_id)
                .is_some()
        });
        tracing::warn!(
            peer_id = %self.peer_id,
            addr = %self.addr,
            evicted,
            "actor ask cancelled; evicted the specific peer transport session instance it ran on"
        );
    }
}

impl<T> RemoteActorRef<T> {
    fn remaining_until(deadline: tokio::time::Instant) -> crate::Result<std::time::Duration> {
        deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(crate::GossipError::Timeout)
    }

    async fn ask_actor_frame_with_deadline(
        conn: &RemoteConnection,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: std::time::Duration,
    ) -> crate::Result<bytes::Bytes> {
        match tokio::time::timeout(
            timeout,
            conn.ask_actor_frame(actor_id, type_hash, payload, timeout),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(crate::GossipError::Timeout),
        }
    }

    #[inline]
    fn current_connection_or_not_listening(&self) -> crate::Result<Arc<RemoteConnection>> {
        self.connection_slot.load_full().ok_or_else(|| {
            crate::GossipError::ActorNotFound(format!(
                "'{}' - not listening yet",
                self.location.address
            ))
        })
    }

    /// Classify whether `err` indicates a dead/broken transport session (as
    /// opposed to e.g. an application-level error, `Timeout`, or
    /// `ActorNotFound`) that warrants re-resolving the connection through
    /// the registry rather than surfacing it as-is.
    fn is_transport_failure(err: &crate::GossipError) -> bool {
        match err {
            crate::GossipError::ConnectionClosed(_) | crate::GossipError::ConnectionDropped => true,
            crate::GossipError::Network(io_err) => matches!(
                io_err.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::UnexpectedEof
            ),
            _ => false,
        }
    }

    /// Atomically install `new` as the cached connection iff the slot is
    /// still exactly `expected` (`None` meaning "still empty", `Some(arc)`
    /// meaning "still holding that exact `Arc`") - a single lock-free CAS on
    /// the underlying `ArcSwapOption`, mirroring
    /// `ConnectionPool::compare_and_set_current_connection`.
    ///
    /// This closes the check-then-act gap: a repair computed against a
    /// snapshot (`expected`) taken before this call must never blindly
    /// clobber whatever another concurrent repair already installed. Either
    /// the slot still holds `expected` and is atomically swapped for `new`,
    /// or it holds something else and is left untouched (the caller gets
    /// that "something else" back to reuse instead of wastefully dialing
    /// twice).
    fn compare_and_set_connection(
        &self,
        expected: Option<&Arc<RemoteConnection>>,
        new: Arc<RemoteConnection>,
    ) -> std::result::Result<(), Option<Arc<RemoteConnection>>> {
        let expected_owned: Option<Arc<RemoteConnection>> = expected.cloned();
        let previous = self.connection_slot.compare_and_swap(&expected_owned, Some(new));
        let matched = match (&expected_owned, &*previous) {
            (None, None) => true,
            (Some(exp), Some(prev)) => Arc::ptr_eq(exp, prev),
            _ => false,
        };
        if matched {
            Ok(())
        } else {
            Err((*previous).clone())
        }
    }

    /// Re-resolve the peer through the registry's connection pool and
    /// persist the freshly dialed (or reused) connection into the shared
    /// slot, so every subsequent call on this ref - and any of its clones -
    /// observes the healed connection with zero additional lookups.
    ///
    /// If another concurrent caller already healed this ref past `failed`,
    /// this returns that connection directly without dialing again.
    /// Returns `Err(GossipError::Shutdown)` if the registry is gone, or
    /// whatever error the pool's dial attempt produced - self-healing never
    /// loops, it retries exactly once per failed call.
    async fn reheal_connection(
        &self,
        failed: &Arc<RemoteConnection>,
    ) -> crate::Result<Arc<RemoteConnection>> {
        let Some(registry) = self.registry.upgrade() else {
            return Err(crate::GossipError::Shutdown);
        };
        if registry.shutdown.load(Ordering::Relaxed) {
            // Don't attempt to re-resolve against a registry that is already
            // tearing down - the peer's own listener may already be gone too,
            // which would otherwise surface as a raw dial error (e.g.
            // `ConnectionRefused`) instead of the expected `Shutdown`.
            return Err(crate::GossipError::Shutdown);
        }

        // Somebody else may have already repaired this ref concurrently -
        // if the live slot no longer points at the instance that just
        // failed, reuse it instead of dialing again.
        if let Some(current) = self.connection_slot.load_full() {
            if !Arc::ptr_eq(&current, failed) {
                return Ok(current);
            }
        }

        let peer_id = self.location.peer_id.clone();
        let handle = registry
            .connection_pool
            .get_connection_to_peer(&peer_id)
            .await?;
        let fresh = Arc::new(RemoteConnection::from_handle(handle));

        Ok(
            match self.compare_and_set_connection(Some(failed), fresh.clone()) {
                Ok(()) => fresh,
                Err(Some(other)) => other,
                Err(None) => {
                    // Slot had already been cleared out from under us; nothing
                    // better than our own fresh dial is available.
                    self.connection_slot.store(Some(fresh.clone()));
                    fresh
                }
            },
        )
    }

    /// Repair the cached transport after an ask failed, without replaying the
    /// request. A write-side transport error is ambiguous: the remote actor
    /// may already have received and processed the request, so retrying it
    /// could duplicate a non-idempotent operation.
    async fn preserve_ambiguous_ask_error(
        &self,
        failed: &Arc<RemoteConnection>,
        err: crate::GossipError,
    ) -> crate::GossipError {
        if let Err(repair_err) = self.reheal_connection(failed).await {
            tracing::debug!(error = ?repair_err, "failed to repair cached connection after ambiguous ask failure");
        }
        err
    }

    /// Create a new RemoteActorRef with optional connection and registry reference (for auto-reconnection)
    /// Called by `lookup()` - uses Weak to prevent reference cycles
    pub(crate) fn with_registry(
        location: RemoteActorLocation,
        connection: Option<crate::connection_pool::ConnectionHandle>,
        registry: Arc<crate::registry::GossipRegistry>,
    ) -> Self {
        let connection = connection.map(RemoteConnection::from_handle);
        Self {
            location,
            connection_slot: Arc::new(ArcSwapOption::from_pointee(connection.clone())),
            connection,
            registry: Arc::downgrade(&registry), // Weak reference - prevents cycle
            _marker: PhantomData,
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
    pub fn connection_ref(&self) -> Option<RemoteConnection> {
        self.connection_slot.load_full().map(|arc| (*arc).clone())
    }

    fn actor_ask_cancellation_guard(
        &self,
        conn: &RemoteConnection,
    ) -> Option<ActorAskCancellationGuard> {
        let registry = self.registry.upgrade()?;
        if !registry.config.connection_recovery.evict_peer_on_ask_cancel {
            return None;
        }
        Some(ActorAskCancellationGuard {
            registry,
            peer_id: self.location.peer_id.clone(),
            addr: conn.addr,
            instance_id: conn.instance_id(),
            armed: true,
        })
    }

    async fn recover_connection_after_actor_ask_timeout(
        &self,
        deadline: tokio::time::Instant,
        failed_conn: &Arc<RemoteConnection>,
    ) -> crate::Result<Option<Arc<RemoteConnection>>> {
        let Some(registry) = self.registry.upgrade() else {
            return Err(crate::GossipError::Shutdown);
        };
        if registry.shutdown.load(Ordering::Relaxed) {
            return Err(crate::GossipError::Shutdown);
        }
        let policy = registry.config.connection_recovery;
        if !policy.evict_peer_on_ask_timeout {
            return Ok(None);
        }

        let peer_id = &self.location.peer_id;
        // Instance-scoped, not peer-wide: retire exactly the connection
        // instance the timed-out ask actually ran on
        // (`failed_conn`'s own addr/instance id), never whatever happens to
        // be indexed for the peer at this moment — a concurrent reconnect
        // landing here must not be collaterally destroyed by a timeout on a
        // now-superseded instance.
        // Peer-id-aware: `peer_id` is known here, so current-session cleanup
        // does not depend on `addr_to_peer_id[failed_conn.addr]` still
        // holding this peer's alias — see
        // `ConnectionPool::remove_connection_instance_for_peer`.
        let evicted = failed_conn.instance_id().is_some_and(|instance_id| {
            registry
                .connection_pool
                .remove_connection_instance_for_peer(peer_id, failed_conn.addr, instance_id)
                .is_some()
        });
        tracing::warn!(
            peer_id = %peer_id,
            addr = %failed_conn.addr,
            evicted,
            retry = policy.retry_actor_ask_once_after_timeout,
            "actor ask timed out; evicted the specific peer transport session instance it ran on"
        );

        if !policy.retry_actor_ask_once_after_timeout {
            return Ok(None);
        }

        let remaining = Self::remaining_until(deadline)?;
        let handle = tokio::time::timeout(
            remaining,
            registry.connection_pool.get_connection_to_peer(peer_id),
        )
        .await
        .map_err(|_| crate::GossipError::Timeout)??;
        let fresh = Arc::new(RemoteConnection::from_handle(handle));

        // Persist the recovered connection back into the shared slot so
        // every subsequent call on this ref (and its clones) uses it
        // directly - this is the "F3" follow-up the previous local-variable
        // only recovery deferred.
        let healed = match self.compare_and_set_connection(Some(failed_conn), fresh.clone()) {
            Ok(()) => fresh,
            Err(Some(other)) => other,
            Err(None) => {
                self.connection_slot.store(Some(fresh.clone()));
                fresh
            }
        };
        Ok(Some(healed))
    }

    /// Send a fire-and-forget message to the remote actor.
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    /// ConnectionHandle internally uses lock-free stream operations.
    ///
    /// Returns error if registry has shut down or no connection is available.
    pub async fn tell(&self, message: bytes::Bytes) -> crate::Result<()> {
        self.tell_bytes(message).await
    }

    /// Send a fire-and-forget message using owned bytes (no payload copy at this layer).
    pub async fn tell_bytes(&self, message: bytes::Bytes) -> crate::Result<()> {
        let conn = self.current_connection_or_not_listening()?;

        // Direct call - ZERO LOCKS
        // ConnectionHandle.tell_bytes() avoids an extra payload clone.
        match conn.tell_bytes(message.clone()).await {
            Err(err) if Self::is_transport_failure(&err) => {
                self.reheal_connection(&conn)
                    .await?
                    .tell_bytes(message)
                    .await
            }
            other => other,
        }
    }

    /// Non-blocking tell using owned bytes.
    ///
    /// Returns `GossipError::WriteQueueFull` when the connection write queue is saturated.
    ///
    /// This is synchronous and cannot perform the async re-resolution self-healing
    /// relies on, so it benefits from a healed connection only if a previous
    /// `async` call on this ref already repaired the slot.
    pub fn try_tell_bytes(&self, message: bytes::Bytes) -> crate::Result<()> {
        let conn = self.current_connection_or_not_listening()?;
        conn.try_tell_bytes(message)
    }

    /// Send an actor-routed fire-and-forget frame (MessageType::ActorTell).
    pub async fn tell_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> crate::Result<()> {
        let conn = self.current_connection_or_not_listening()?;
        match conn
            .tell_actor_frame(actor_id, type_hash, payload.clone())
            .await
        {
            Err(err) if Self::is_transport_failure(&err) => {
                self.reheal_connection(&conn)
                    .await?
                    .tell_actor_frame(actor_id, type_hash, payload)
                    .await
            }
            other => other,
        }
    }

    /// Non-blocking actor-routed tell. Returns `GossipError::WriteQueueFull` on backpressure.
    ///
    /// See `try_tell_bytes` for why this cannot self-heal synchronously.
    pub fn try_tell_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> crate::Result<()> {
        let conn = self.current_connection_or_not_listening()?;
        conn.try_tell_actor_frame(actor_id, type_hash, payload)
    }

    /// Ask an actor-routed frame and wait for a reply (MessageType::ActorAsk).
    pub async fn ask_actor_frame(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: std::time::Duration,
    ) -> crate::Result<bytes::Bytes> {
        let conn = self.current_connection_or_not_listening()?;
        let deadline = tokio::time::Instant::now() + timeout;
        let mut guard = self.actor_ask_cancellation_guard(&conn);
        let remaining = Self::remaining_until(deadline)?;
        let result = Self::ask_actor_frame_with_deadline(
            &conn,
            actor_id,
            type_hash,
            payload.clone(),
            remaining,
        )
        .await;
        if let Some(guard) = guard.as_mut() {
            guard.disarm();
        }
        match result {
            Err(crate::GossipError::Timeout) => {
                // The remote may have received the request before the local
                // timeout fired. Repair the cached connection for the next
                // operation, but never replay this potentially non-idempotent
                // ask.
                let recovery_deadline = tokio::time::Instant::now() + timeout;
                if let Err(repair_err) = self
                    .recover_connection_after_actor_ask_timeout(recovery_deadline, &conn)
                    .await
                {
                    tracing::debug!(error = ?repair_err, "failed to repair cached connection after timed-out ask");
                }
                Err(crate::GossipError::Timeout)
            }
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            result => result,
        }
    }

    /// Ask an actor-routed frame without timeout allocation.
    pub async fn ask_actor_frame_no_timeout(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> crate::Result<bytes::Bytes> {
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_actor_frame_no_timeout(actor_id, type_hash, payload).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Send a request and wait for a response.
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    /// ZERO-COPY: Returns `Bytes` instead of `Vec<u8>` to avoid allocation.
    /// ConnectionHandle internally uses lock-free stream operations.
    ///
    /// Returns error if registry has shut down or no connection is available.
    pub async fn ask(&self, request: bytes::Bytes) -> crate::Result<bytes::Bytes> {
        let conn = self.current_connection_or_not_listening()?;
        // Direct call - ZERO LOCKS
        match conn.ask(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
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
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_with_timeout_bytes(request, timeout).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Send a direct request and wait for a direct response.
    pub async fn ask_direct(
        &self,
        request: bytes::Bytes,
        timeout: std::time::Duration,
    ) -> crate::Result<bytes::Bytes> {
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_direct(request, timeout).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Send a direct request and wait without timeout allocation.
    pub async fn ask_direct_no_timeout(
        &self,
        request: bytes::Bytes,
    ) -> crate::Result<bytes::Bytes> {
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_direct_no_timeout(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Send a request and return a deferred handle that can be awaited later.
    ///
    /// This is the correct way to delegate "waiting for the response" to another task.
    ///
    /// ZERO-LOCK: Uses cached connection directly with no mutex overhead.
    pub async fn ask_deferred(&self, request: bytes::Bytes) -> crate::Result<crate::DeferredAsk> {
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_deferred(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Send a typed fire-and-forget message
    pub async fn tell_typed<M>(&self, message: &M) -> crate::Result<()>
    where
        M: crate::typed::WireEncode,
    {
        // Check if registry has been shut down
        if let Some(registry) = self.registry.upgrade() {
            if registry.shutdown.load(Ordering::Relaxed) {
                return Err(crate::GossipError::Shutdown);
            }
        } else {
            return Err(crate::GossipError::Shutdown);
        }

        let conn = self.current_connection_or_not_listening()?;
        match conn.tell_typed(message).await {
            Err(err) if Self::is_transport_failure(&err) => {
                self.reheal_connection(&conn)
                    .await?
                    .tell_typed(message)
                    .await
            }
            other => other,
        }
    }

    /// Send a typed request and wait for a typed response
    pub async fn ask_typed<M, R>(&self, request: &M) -> crate::Result<R>
    where
        M: crate::typed::WireEncode,
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
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_typed(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Send a typed request and keep the reply as an archived zero-copy view.
    pub async fn ask_typed_archived<M, R>(
        &self,
        request: &M,
    ) -> crate::Result<crate::typed::ArchivedBytes<R>>
    where
        M: crate::typed::WireEncode,
        R: crate::typed::WireType + rkyv::Archive,
        for<'a> R::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_typed_archived(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Send a typed request and keep the reply as an archived zero-copy view.
    pub async fn ask_typed_archived_with_timeout<M, R>(
        &self,
        request: &M,
        timeout: std::time::Duration,
    ) -> crate::Result<crate::typed::ArchivedBytes<R>>
    where
        M: crate::typed::WireEncode,
        R: crate::typed::WireType + rkyv::Archive,
        for<'a> R::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let conn = self.current_connection_or_not_listening()?;
        match conn.ask_typed_archived_with_timeout(request, timeout).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
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
        let conn = self.current_connection_or_not_listening()?;
        // Direct call - ZERO LOCKS
        match conn.ask_streaming_bytes(payload, type_hash, actor_id, timeout).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err).await)
            }
            other => other,
        }
    }

    /// Get the streaming threshold for this connection
    pub fn streaming_threshold(&self) -> usize {
        crate::connection_pool::STREAMING_THRESHOLD
    }
}

// Custom Debug implementation
impl<T> std::fmt::Debug for RemoteActorRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteActorRef")
            .field("location", &self.location)
            .field("connection", &"<connection>")
            .field("registry_alive", &self.is_registry_alive())
            .finish()
    }
}
