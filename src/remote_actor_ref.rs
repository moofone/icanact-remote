use crate::RemoteActorLocation;
use arc_swap::ArcSwapOption;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::watch;

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

    /// Ask an actor with a caller-controlled out-of-band request id. The
    /// identity is carried in the uncompact ActorAsk header and is available
    /// through the receiver's [`crate::AskContext::request_id`].
    pub async fn ask_actor_frame_with_request_id(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
        request_id: u64,
    ) -> crate::Result<bytes::Bytes> {
        self.inner
            .ask_actor_frame_with_request_id(actor_id, type_hash, payload, timeout, request_id)
            .await
    }

    /// Aligned response variant of [`Self::ask_actor_frame_with_request_id`].
    pub async fn ask_actor_frame_aligned_with_request_id(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
        timeout: Duration,
        request_id: u64,
    ) -> crate::Result<crate::AlignedBytes> {
        self.inner
            .ask_actor_frame_aligned_with_request_id(
                actor_id, type_hash, payload, timeout, request_id,
            )
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
/// - `connection: Arc<ArcSwapOption<RemoteConnection>>` - live shared slot
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
/// (`peer_id` → address, refreshing DNS as needed), and **persists** the
/// healed connection back into the slot so every subsequent call on this ref
/// (and on any of its clones, which share the same slot) uses it directly
/// with zero additional lookups. The failed operation is not replayed because
/// a transport error is ambiguous; an actor ask is retried only when the
/// caller explicitly enables the timeout-retry policy.
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
    /// Initial cached connection snapshot retained for source compatibility
    /// with the former public debug/test field. Use [`Self::connection_ref`]
    /// for the live self-healing connection; this snapshot is not updated by
    /// later repairs.
    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    pub connection: Option<RemoteConnection>,
    #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
    connection: Option<RemoteConnection>,
    /// Lock-free live slot shared by every clone of this ref. A transport
    /// failure or actor-ask timeout replaces this slot atomically.
    connection_slot: Arc<ArcSwapOption<RemoteConnection>>,
    /// Registry weak reference - doesn't prevent registry shutdown/cleanup
    /// Used for reconnection after DNS changes
    registry: Weak<crate::registry::GossipRegistry>,
    recovery_in_flight: Arc<AtomicBool>,
    /// Monotonic completion signal for a recovery attempt. A watch channel is
    /// used instead of a bare Notify so a waiter cannot miss a completion
    /// that races with subscribing.
    recovery_completed: watch::Sender<u64>,
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

struct AmbiguousRecoveryGuard {
    in_flight: Arc<AtomicBool>,
    completed: watch::Sender<u64>,
}

impl Drop for AmbiguousRecoveryGuard {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
        self.completed.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
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

    /// Return a usable cached connection, waiting for an already-running
    /// self-heal when the slot still contains a closed transport. The first
    /// request that observes a closed slot owns the repair; later requests do
    /// not fail spuriously with `ConnectionClosed` while that repair is in
    /// flight. Waiting is bounded by the caller's deadline when one exists.
    async fn current_connection_for_operation(
        &self,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::Result<Arc<RemoteConnection>> {
        let mut completed = self.recovery_completed.subscribe();
        loop {
            let current = self.current_connection_or_not_listening()?;
            if !current.is_closed() {
                return Ok(current);
            }

            // Preserve ask/tell safety semantics: the first operation on a
            // known-closed cached transport must fail and initiate recovery
            // through its existing error path. Only a later operation that
            // arrives while that recovery is already in flight waits here;
            // it must never replay the earlier ambiguous request.
            if !self.recovery_in_flight.load(Ordering::Acquire) {
                return Ok(current);
            }

            let wait = completed.changed();
            match deadline {
                Some(operation_deadline) => {
                    let remaining = Self::remaining_until(operation_deadline)?;
                    tokio::time::timeout(remaining, wait)
                        .await
                        .map_err(|_| crate::GossipError::Timeout)?
                        .map_err(|_| crate::GossipError::Shutdown)?;
                }
                None => {
                    wait.await.map_err(|_| crate::GossipError::Shutdown)?;
                }
            }
        }
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
        let previous = self
            .connection_slot
            .compare_and_swap(&expected_owned, Some(new));
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

    /// A `get_connection_to_peer` candidate is not a usable replacement for
    /// `failed` if it is literally the same transport instance - the pool's
    /// own index simply has not retired it yet - or is already closed.
    /// Either way, accepting it would just wrap an identical or dead session
    /// in a fresh `Arc`: a caller's CAS against it "succeeds" without the
    /// underlying connection having actually changed. Checked by instance
    /// identity, never by address: identity is the only signal that stays
    /// correct no matter how the pool's index happens to be ordered at the
    /// moment of the call.
    fn is_unhealed_candidate(candidate: &RemoteConnection, failed: &Arc<RemoteConnection>) -> bool {
        if candidate.is_closed() {
            return true;
        }

        match (candidate.instance_id(), failed.instance_id()) {
            (Some(candidate_id), Some(failed_id)) => candidate_id == failed_id,
            // A handle without a stream instance cannot be a usable replacement. It may have
            // lost its stream between pool lookup and this check, so reject it regardless of
            // whether the failed handle had an instance of its own.
            (None, _) => true,
            (Some(_), None) => false,
        }
    }

    /// Dial (or reuse) a connection to `peer_id`, respecting `deadline` when
    /// the caller has its own budget to enforce (`None` lets the pool's own
    /// connection timeout govern instead, for a caller whose outer timeout
    /// already wraps the whole repair).
    async fn dial_connection_to_peer(
        registry: &Arc<crate::registry::GossipRegistry>,
        peer_id: &crate::PeerId,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::Result<crate::connection_pool::ConnectionHandle> {
        match deadline {
            Some(deadline) => {
                let remaining = Self::remaining_until(deadline)?;
                tokio::time::timeout(
                    remaining,
                    registry.connection_pool.get_connection_to_peer(peer_id),
                )
                .await
                .map_err(|_| crate::GossipError::Timeout)?
            }
            None => {
                registry
                    .connection_pool
                    .get_connection_to_peer(peer_id)
                    .await
            }
        }
    }

    /// Dial a replacement for `failed` and reject it via
    /// [`Self::is_unhealed_candidate`] if the pool's own get-or-create just
    /// handed the identical (or already-dead) instance back. On rejection,
    /// evict that specific instance by identity - never by address, which
    /// could collaterally take out an unrelated concurrent reconnection -
    /// and dial exactly once more; bounded to a single extra attempt, the
    /// same "self-healing never loops" contract the callers of this
    /// document.
    async fn dial_replacement(
        registry: &Arc<crate::registry::GossipRegistry>,
        peer_id: &crate::PeerId,
        failed: &Arc<RemoteConnection>,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::Result<Arc<RemoteConnection>> {
        let candidate = RemoteConnection::from_handle(
            Self::dial_connection_to_peer(registry, peer_id, deadline).await?,
        );
        if !Self::is_unhealed_candidate(&candidate, failed) {
            return Ok(Arc::new(candidate));
        }
        // Evict the candidate that was actually rejected. It may be a different closed
        // instance from `failed`; removing `failed` here leaves that candidate indexed and lets
        // the next pool lookup return the same dead handle again.
        if let Some(instance_id) = candidate.instance_id() {
            registry
                .connection_pool
                .remove_connection_instance_for_peer(peer_id, candidate.addr, instance_id);
        }
        let redial = RemoteConnection::from_handle(
            Self::dial_connection_to_peer(registry, peer_id, deadline).await?,
        );
        if Self::is_unhealed_candidate(&redial, failed) {
            if let Some(instance_id) = redial.instance_id() {
                registry
                    .connection_pool
                    .remove_connection_instance_for_peer(peer_id, redial.addr, instance_id);
            }
            return Err(crate::GossipError::ConnectionClosed(redial.addr));
        }
        Ok(Arc::new(redial))
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
        deadline: Option<tokio::time::Instant>,
    ) -> crate::Result<Arc<RemoteConnection>> {
        let Some(registry) = self.registry.upgrade() else {
            return Err(crate::GossipError::Shutdown);
        };
        if registry.shutdown.load(Ordering::Acquire) {
            // Don't attempt to re-resolve against a registry that is already
            // tearing down - the peer's own listener may already be gone too,
            // which would otherwise surface as a raw dial error (e.g.
            // `ConnectionRefused`) instead of the expected `Shutdown`.
            return Err(crate::GossipError::Shutdown);
        }

        // Somebody else may have already repaired this ref concurrently -
        // if the live slot no longer points at the instance that just
        // failed, reuse it only when that replacement is still healthy.
        // A closed replacement is not a heal: retain it as the CAS expected
        // value below so this repair can replace that exact stale pointer.
        let expected = self.connection_slot.load_full();
        if let Some(current) = expected.as_ref() {
            if !Arc::ptr_eq(current, failed) && !current.is_closed() {
                return Ok(current.clone());
            }
        }

        let peer_id = self.location.peer_id.clone();
        // Retire the exact failed instance before asking the pool for a replacement. The pool's
        // get-or-create path may still have that session indexed briefly after the transport
        // reports its error; resolving first can return the same dead instance and make the CAS
        // below appear to heal while changing nothing underneath.
        if let Some(current) = expected.as_ref() {
            if let Some(instance_id) = current.instance_id() {
                registry
                    .connection_pool
                    .remove_connection_instance_for_peer(&peer_id, current.addr, instance_id);
            }
        } else if let Some(instance_id) = failed.instance_id() {
            registry
                .connection_pool
                .remove_connection_instance_for_peer(&peer_id, failed.addr, instance_id);
        }
        let fresh = Self::dial_replacement(&registry, &peer_id, failed, deadline).await?;

        Ok(
            match self.compare_and_set_connection(expected.as_ref(), fresh.clone()) {
                Ok(()) => fresh,
                Err(Some(other)) if !other.is_closed() => other,
                Err(Some(other)) => {
                    if let Some(instance_id) = other.instance_id() {
                        registry
                            .connection_pool
                            .remove_connection_instance_for_peer(&peer_id, other.addr, instance_id);
                    }
                    match self.compare_and_set_connection(Some(&other), fresh.clone()) {
                        Ok(()) => fresh,
                        Err(Some(current)) if !current.is_closed() => current,
                        Err(Some(current)) => {
                            return Err(crate::GossipError::ConnectionClosed(current.addr));
                        }
                        Err(None) => {
                            self.connection_slot.store(Some(fresh.clone()));
                            fresh
                        }
                    }
                }
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
        deadline: Option<tokio::time::Instant>,
    ) -> crate::GossipError {
        let Some(registry) = self.registry.upgrade() else {
            return err;
        };
        let Some(claim) = self.claim_ambiguous_ask_recovery() else {
            return err;
        };
        // If the caller's budget expires while the dial is still in flight,
        // keep the detached repair alive for one full additional connection
        // window. Otherwise a short ask timeout cancels the first repair just
        // before the peer's preferred-inbound path completes, and every
        // subsequent cached-ref operation observes the same closed slot.
        let recovery_deadline = deadline
            .map(|operation_deadline| operation_deadline + registry.config.connection_timeout)
            .unwrap_or_else(|| tokio::time::Instant::now() + registry.config.connection_timeout);
        let repair = self.reheal_connection(failed, Some(recovery_deadline));

        if let Some(operation_deadline) = deadline {
            match Self::remaining_until(operation_deadline) {
                Ok(remaining) => match tokio::time::timeout(remaining, repair).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(repair_err)) => {
                        tracing::debug!(
                            error = ?repair_err,
                            "failed to repair cached connection after ambiguous ask failure"
                        );
                    }
                    Err(_) => {
                        self.spawn_ambiguous_ask_recovery(
                            Arc::clone(failed),
                            recovery_deadline,
                            claim,
                        );
                    }
                },
                Err(_) => {
                    self.spawn_ambiguous_ask_recovery(Arc::clone(failed), recovery_deadline, claim);
                }
            }
        } else if let Err(repair_err) = repair.await {
            tracing::debug!(
                error = ?repair_err,
                "failed to repair cached connection after ambiguous ask failure"
            );
        }
        err
    }

    fn claim_ambiguous_ask_recovery(&self) -> Option<AmbiguousRecoveryGuard> {
        let claim = self
            .recovery_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| AmbiguousRecoveryGuard {
                in_flight: Arc::clone(&self.recovery_in_flight),
                completed: self.recovery_completed.clone(),
            });
        claim
    }

    fn spawn_ambiguous_ask_recovery(
        &self,
        failed: Arc<RemoteConnection>,
        deadline: tokio::time::Instant,
        claim: AmbiguousRecoveryGuard,
    ) {
        let remote = RemoteActorRef::<()> {
            location: self.location.clone(),
            connection: self.connection.clone(),
            connection_slot: Arc::clone(&self.connection_slot),
            registry: self.registry.clone(),
            recovery_in_flight: Arc::clone(&self.recovery_in_flight),
            recovery_completed: self.recovery_completed.clone(),
            _marker: PhantomData,
        };
        tokio::spawn(async move {
            let _claim = claim;
            if let Err(repair_err) = remote.reheal_connection(&failed, Some(deadline)).await {
                tracing::debug!(
                    error = ?repair_err,
                    "failed to repair cached connection after ambiguous ask failure"
                );
            }
        });
    }

    /// Create a new RemoteActorRef with optional connection and registry reference (for auto-reconnection)
    /// Called by `lookup()` - uses Weak to prevent reference cycles
    pub(crate) fn with_registry(
        location: RemoteActorLocation,
        connection: Option<crate::connection_pool::ConnectionHandle>,
        registry: Arc<crate::registry::GossipRegistry>,
    ) -> Self {
        let connection = connection.map(RemoteConnection::from_handle);
        let (recovery_completed, _) = watch::channel(0);
        Self {
            location,
            connection: connection.clone(),
            connection_slot: Arc::new(ArcSwapOption::from(connection.map(Arc::new))),
            registry: Arc::downgrade(&registry), // Weak reference - prevents cycle
            recovery_in_flight: Arc::new(AtomicBool::new(false)),
            recovery_completed,
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
        if registry.shutdown.load(Ordering::Acquire) {
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

        // Dial and persist a replacement unconditionally once eviction is
        // enabled - whether the CALLER replays the timed-out ask onto it is
        // an entirely separate decision, gated below by
        // `retry_actor_ask_once_after_timeout`. Conflating the two left the
        // safety-oriented combination (evict on timeout, never replay)
        // stuck holding the just-evicted connection forever: the eviction
        // above ran, but nothing ever replaced what this ref itself was
        // still caching, so the next call on it hit the identical dead
        // handle again.
        let fresh = Self::dial_replacement(&registry, peer_id, failed_conn, Some(deadline)).await?;
        let healed = match self.compare_and_set_connection(Some(failed_conn), fresh.clone()) {
            Ok(()) => fresh,
            Err(Some(other)) if !other.is_closed() => other,
            Err(Some(other)) => {
                if let Some(instance_id) = other.instance_id() {
                    registry
                        .connection_pool
                        .remove_connection_instance_for_peer(peer_id, other.addr, instance_id);
                }
                match self.compare_and_set_connection(Some(&other), fresh.clone()) {
                    Ok(()) => fresh,
                    Err(Some(current)) if !current.is_closed() => current,
                    Err(Some(current)) => {
                        return Err(crate::GossipError::ConnectionClosed(current.addr));
                    }
                    Err(None) => {
                        self.connection_slot.store(Some(fresh.clone()));
                        fresh
                    }
                }
            }
            Err(None) => {
                self.connection_slot.store(Some(fresh.clone()));
                fresh
            }
        };

        if !policy.retry_actor_ask_once_after_timeout {
            return Ok(None);
        }
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
        let conn = self.current_connection_for_operation(None).await?;

        // Direct call - ZERO LOCKS
        // ConnectionHandle.tell_bytes() avoids an extra payload clone.
        match conn.tell_bytes(message).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let conn = self.current_connection_for_operation(None).await?;
        match conn.tell_actor_frame(actor_id, type_hash, payload).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let deadline = tokio::time::Instant::now() + timeout;
        let conn = self
            .current_connection_for_operation(Some(deadline))
            .await?;
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
                // timeout fired. By default, repair the cached connection for
                // the next operation but do not replay this potentially
                // non-idempotent ask. The timeout-retry policy is the explicit
                // caller opt-in to one replay after recovery.
                let recovery_deadline = tokio::time::Instant::now() + timeout;
                match self
                    .recover_connection_after_actor_ask_timeout(recovery_deadline, &conn)
                    .await
                {
                    Ok(Some(reconnected)) => {
                        let remaining = Self::remaining_until(recovery_deadline)?;
                        let mut retry_guard = self.actor_ask_cancellation_guard(&reconnected);
                        let retry_result = Self::ask_actor_frame_with_deadline(
                            &reconnected,
                            actor_id,
                            type_hash,
                            payload,
                            remaining,
                        )
                        .await;
                        if let Some(guard) = retry_guard.as_mut() {
                            guard.disarm();
                        }
                        retry_result
                    }
                    Ok(None) => Err(crate::GossipError::Timeout),
                    Err(repair_err) => {
                        tracing::debug!(error = ?repair_err, "failed to repair cached connection after timed-out ask");
                        Err(repair_err)
                    }
                }
            }
            Err(err) if Self::is_transport_failure(&err) => Err(self
                .preserve_ambiguous_ask_error(&conn, err, Some(deadline))
                .await),
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
        let conn = self.current_connection_for_operation(None).await?;
        match conn
            .ask_actor_frame_no_timeout(actor_id, type_hash, payload)
            .await
        {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let conn = self.current_connection_for_operation(None).await?;
        // Direct call - ZERO LOCKS
        match conn.ask(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let deadline = tokio::time::Instant::now() + timeout;
        let conn = self
            .current_connection_for_operation(Some(deadline))
            .await?;
        match conn.ask_with_timeout_bytes(request, timeout).await {
            Err(err) if Self::is_transport_failure(&err) => Err(self
                .preserve_ambiguous_ask_error(&conn, err, Some(deadline))
                .await),
            other => other,
        }
    }

    /// Send a direct request and wait for a direct response.
    pub async fn ask_direct(
        &self,
        request: bytes::Bytes,
        timeout: std::time::Duration,
    ) -> crate::Result<bytes::Bytes> {
        let deadline = tokio::time::Instant::now() + timeout;
        let conn = self
            .current_connection_for_operation(Some(deadline))
            .await?;
        match conn.ask_direct(request, timeout).await {
            Err(err) if Self::is_transport_failure(&err) => Err(self
                .preserve_ambiguous_ask_error(&conn, err, Some(deadline))
                .await),
            other => other,
        }
    }

    /// Send a direct request and wait without timeout allocation.
    pub async fn ask_direct_no_timeout(
        &self,
        request: bytes::Bytes,
    ) -> crate::Result<bytes::Bytes> {
        let conn = self.current_connection_for_operation(None).await?;
        match conn.ask_direct_no_timeout(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let conn = self.current_connection_for_operation(None).await?;
        match conn.ask_deferred(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
            if registry.shutdown.load(Ordering::Acquire) {
                return Err(crate::GossipError::Shutdown);
            }
        } else {
            return Err(crate::GossipError::Shutdown);
        }

        let conn = self.current_connection_for_operation(None).await?;
        match conn.tell_typed(message).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let conn = self.current_connection_for_operation(None).await?;
        match conn.ask_typed(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let conn = self.current_connection_for_operation(None).await?;
        match conn.ask_typed_archived(request).await {
            Err(err) if Self::is_transport_failure(&err) => {
                Err(self.preserve_ambiguous_ask_error(&conn, err, None).await)
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
        let deadline = tokio::time::Instant::now() + timeout;
        let conn = self
            .current_connection_for_operation(Some(deadline))
            .await?;
        match conn.ask_typed_archived_with_timeout(request, timeout).await {
            Err(err) if Self::is_transport_failure(&err) => Err(self
                .preserve_ambiguous_ask_error(&conn, err, Some(deadline))
                .await),
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
        let deadline = tokio::time::Instant::now() + timeout;
        let conn = self
            .current_connection_for_operation(Some(deadline))
            .await?;
        // Direct call - ZERO LOCKS
        match conn
            .ask_streaming_bytes(payload, type_hash, actor_id, timeout)
            .await
        {
            Err(err) if Self::is_transport_failure(&err) => Err(self
                .preserve_ambiguous_ask_error(&conn, err, Some(deadline))
                .await),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ActorMessageFuture, ActorMessageHandler};
    use crate::{GossipConfig, GossipRegistryHandle, KeyPair};
    use tokio::time::{Duration, sleep};

    struct NeverRespondsHandler;

    impl ActorMessageHandler for NeverRespondsHandler {
        fn handle_actor_message(
            &self,
            _actor_id: u64,
            _type_hash: u32,
            _payload: crate::AlignedBytes,
            _correlation_id: Option<u32>,
        ) -> ActorMessageFuture<'_> {
            Box::pin(async move {
                sleep(Duration::from_secs(30)).await;
                Ok(None)
            })
        }
    }

    /// `add_peer`/`connect` dial in one fixed direction; order the pair so
    /// the lower `NodeId` always dials out, matching every other two-node
    /// test in this crate.
    fn ordered_pair(seed_a: &str, seed_b: &str) -> (KeyPair, KeyPair) {
        let first = KeyPair::new_for_testing(seed_a);
        let second = KeyPair::new_for_testing(seed_b);
        if first
            .peer_id()
            .to_node_id()
            .as_bytes()
            .cmp(second.peer_id().to_node_id().as_bytes())
            .is_lt()
        {
            (first, second)
        } else {
            (second, first)
        }
    }

    /// `reheal_connection` must never accept the pool's own `get_connection_to_peer`
    /// handing back the exact instance passed in as `failed`: wrapping that
    /// same instance in a fresh `Arc<RemoteConnection>` would make the
    /// caller's CAS succeed while the slot still points at a connection that
    /// is no better than the one that just failed. Calling the repair path
    /// directly with the ref's own live connection as `failed` reproduces
    /// exactly the shape the pool exhibits when it has not yet retired the
    /// instance a caller is repairing away from: `get_connection_to_peer`
    /// legitimately has nothing else to offer but that same instance.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reheal_connection_never_returns_the_instance_passed_in_as_failed() {
        let addr_a: SocketAddr = "127.0.0.1:28491".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:28492".parse().unwrap();
        let (key_pair_a, key_pair_b) = ordered_pair("reheal_identity_a", "reheal_identity_b");
        let peer_id_a = key_pair_a.peer_id();
        let peer_id_b = key_pair_b.peer_id();
        assert_ne!(
            peer_id_a, peer_id_b,
            "distinct test seeds must not collapse to the same PeerId"
        );

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            peer_supervisor_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            crate::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            crate::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(300)).await;

        let remote_actor = handle_a
            .lookup_peer(&peer_id_b)
            .await
            .expect("lookup should succeed");
        let current = remote_actor
            .connection_slot
            .load_full()
            .expect("connection should be cached");

        let healed = remote_actor
            .reheal_connection(&current, None)
            .await
            .expect("reheal should still resolve a connection");

        assert_ne!(
            healed.instance_id(),
            current.instance_id(),
            "reheal must not accept the same instance that was passed in as failed, even when \
             the pool's own get-or-create still has it indexed"
        );

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    }

    /// A timeout-recovery failure is a distinct caller-visible outcome from the original ask
    /// timing out. In particular, a registry that is already shutting down must not be reported
    /// as a peer timeout: callers use `Shutdown` to stop issuing work and `Timeout` to consider a
    /// bounded retry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn timeout_recovery_propagates_registry_shutdown() {
        let addr_a: SocketAddr = "127.0.0.1:28493".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:28494".parse().unwrap();
        let (key_pair_a, key_pair_b) = ordered_pair("timeout_shutdown_a", "timeout_shutdown_b");
        let peer_id_b = key_pair_b.peer_id();
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            peer_supervisor_interval: Duration::from_secs(300),
            connection_recovery: crate::ConnectionRecoveryPolicy {
                evict_peer_on_ask_timeout: true,
                evict_peer_on_ask_cancel: false,
                retry_actor_ask_once_after_timeout: false,
                consecutive_timeout_threshold: 0,
            },
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            crate::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            crate::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        handle_b
            .registry
            .set_actor_message_handler(Arc::new(NeverRespondsHandler))
            .await;

        handle_a
            .add_peer(&peer_id_b)
            .await
            .connect(&addr_b)
            .await
            .unwrap();
        sleep(Duration::from_millis(300)).await;
        let remote_actor = handle_a
            .lookup_peer(&peer_id_b)
            .await
            .expect("lookup should succeed");

        // Keep the cached connection alive long enough for the first ask to time out, then make
        // the recovery boundary observe shutdown. The old implementation converted this precise
        // `Shutdown` into `Timeout`.
        handle_a
            .registry
            .shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        let result = remote_actor
            .ask_actor_frame(
                0x5E1F_4EA1,
                0xC0DE_CAFE,
                bytes::Bytes::from_static(b"shutdown"),
                Duration::from_millis(50),
            )
            .await;
        assert!(
            matches!(result, Err(crate::GossipError::Shutdown)),
            "timeout recovery must preserve Shutdown, got {result:?}"
        );

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    }

    #[tokio::test]
    async fn recovery_completion_signal_cannot_be_missed_by_a_waiter() {
        let in_flight = Arc::new(AtomicBool::new(true));
        let (completed, _) = watch::channel(0_u64);
        let mut waiter = completed.subscribe();
        let guard = AmbiguousRecoveryGuard {
            in_flight: Arc::clone(&in_flight),
            completed,
        };

        // The waiter subscribes before the owner finishes. Dropping the
        // owner must publish a generation change that remains observable even
        // when the wake-up races the waiter entering `changed()`.
        drop(guard);
        tokio::time::timeout(Duration::from_millis(100), waiter.changed())
            .await
            .expect("recovery completion must wake a waiter")
            .expect("completion sender must remain alive for the waiter");
        assert!(!in_flight.load(Ordering::Acquire));
    }
}
