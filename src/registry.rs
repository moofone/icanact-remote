use std::{
    collections::{HashMap, HashSet},
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task::AbortHandle;

use arc_swap::ArcSwapOption;
use futures::future::{BoxFuture, poll_fn};
use futures::task::AtomicWaker;
use lru::LruCache;
use scc::HashMap as SccHashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll};
use tokio::sync::Mutex;

use rand::seq::SliceRandom;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tracing::{debug, error, info, warn};

use crate::{
    GossipConfig, GossipError, NodeId, RegistrationPriority, RemoteActorLocation, Result,
    connection_pool::ConnectionPool,
    current_timestamp,
    handshake::PeerCapabilities,
    peer_discovery::{PeerDiscovery, PeerDiscoveryConfig},
};

/// Classify a `GossipError` as a hard transport failure that proves the
/// remote socket is gone (BrokenPipe / ConnectionReset / ConnectionAborted
/// / NotConnected / ConnectionRefused). Used by `apply_gossip_results` to
/// fast-path the dead-peer cleanup hook on the same round instead of
/// waiting for `max_peer_failures` separate rounds.
///
/// Soft errors (Timeout, decoding failures, application-level rejects)
/// keep the existing one-failure-at-a-time accumulation so a transient
/// blip cannot immediately evict a peer.
fn is_hard_socket_error(err: &GossipError) -> bool {
    match err {
        GossipError::Network(io_err) => {
            use std::io::ErrorKind::*;
            matches!(
                io_err.kind(),
                BrokenPipe | ConnectionReset | ConnectionAborted | NotConnected | ConnectionRefused
            )
        }
        _ => false,
    }
}

#[inline]
fn stable_concurrent_location_wins(
    candidate: &RemoteActorLocation,
    existing: &RemoteActorLocation,
) -> bool {
    use std::cmp::Ordering;

    // Stable total order for concurrent updates.
    //
    // This must NOT use Rust's `Hash` (DefaultHasher), since hash outputs are not guaranteed
    // stable across Rust versions/targets and can cause gossip divergence.
    //
    // Ordering rationale:
    // - wall_clock_time: best-effort LWW tie-breaker (already part of the on-wire data)
    // - node_id: deterministic and stable across nodes
    // - address/metadata/local_registration_time: final stable tie-breakers to avoid "equal"
    match candidate.wall_clock_time.cmp(&existing.wall_clock_time) {
        Ordering::Greater => return true,
        Ordering::Less => return false,
        Ordering::Equal => {}
    }

    match candidate.node_id.cmp(&existing.node_id) {
        Ordering::Greater => return true,
        Ordering::Less => return false,
        Ordering::Equal => {}
    }

    match candidate.address.cmp(&existing.address) {
        Ordering::Greater => return true,
        Ordering::Less => return false,
        Ordering::Equal => {}
    }

    match candidate.metadata.cmp(&existing.metadata) {
        Ordering::Greater => return true,
        Ordering::Less => return false,
        Ordering::Equal => {}
    }

    candidate.local_registration_time > existing.local_registration_time
}

#[inline]
fn stable_concurrent_removal_wins(
    removing_node_id: &crate::NodeId,
    removal_clock: &crate::VectorClock,
    existing: &RemoteActorLocation,
) -> bool {
    use std::cmp::Ordering;

    // Stable total order for concurrent removal vs existing state.
    // Compare vector-clocks by their sorted representation first, then node IDs.
    // (VectorClock::to_vec is stable-sorted by NodeId.)
    match removal_clock.to_vec().cmp(&existing.vector_clock.to_vec()) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => removing_node_id > &existing.node_id,
    }
}

/// Resolve the effective peer address from sender_bind_addr with validation.
/// Falls back to tcp_source_addr if sender_bind_addr is None, invalid, unspecified (0.0.0.0),
/// or loopback when the TCP source is not loopback.
///
/// # Arguments
/// * `sender_bind_addr` - Optional bind address from the message
/// * `tcp_source_addr` - The TCP source address (fallback)
///
/// # Returns
/// A valid routable SocketAddr
pub fn resolve_peer_addr(
    sender_bind_addr: Option<&str>,
    tcp_source_addr: SocketAddr,
) -> SocketAddr {
    if let Some(bind_addr_str) = sender_bind_addr {
        if let Ok(bind_addr) = bind_addr_str.parse::<SocketAddr>() {
            if bind_addr.port() == 0 {
                debug!(
                    "sender_bind_addr {} has port 0, falling back to TCP source {}",
                    bind_addr, tcp_source_addr
                );
                return tcp_source_addr;
            }

            let ip = bind_addr.ip();

            // Validate: reject unspecified (0.0.0.0, ::)
            if ip.is_unspecified() {
                // Use TCP source IP with bind_addr port
                debug!(
                    "sender_bind_addr {} has unspecified IP, using TCP source IP {} with port {}",
                    bind_addr,
                    tcp_source_addr.ip(),
                    bind_addr.port()
                );
                return SocketAddr::new(tcp_source_addr.ip(), bind_addr.port());
            }

            // Validate: reject loopback (127.0.0.1, ::1) when TCP source is not loopback
            // A remote peer advertising loopback is unreachable from outside
            if ip.is_loopback() && !tcp_source_addr.ip().is_loopback() {
                warn!(
                    "sender_bind_addr {} is loopback but TCP source {} is not, using TCP source IP with bind port {}",
                    bind_addr,
                    tcp_source_addr,
                    bind_addr.port()
                );
                return SocketAddr::new(tcp_source_addr.ip(), bind_addr.port());
            }

            return bind_addr;
        } else {
            warn!(
                "Failed to parse sender_bind_addr '{}', falling back to TCP source {}",
                bind_addr_str, tcp_source_addr
            );
        }
    }
    // Fallback to TCP source address
    tcp_source_addr
}

/// Response payload for actor asks.
pub enum ActorResponse {
    Bytes(bytes::Bytes),
    Aligned(crate::AlignedBytes),
    Pooled {
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    },
}

pub enum AskDisposition {
    Immediate(ActorResponse),
    ImmediateBytes(bytes::Bytes),
    ImmediateAligned(crate::AlignedBytes),
    ImmediatePooled {
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    },
    Deferred,
}

#[derive(Clone)]
pub struct ActorMessageHandlerCell {
    handler: Arc<dyn ActorMessageHandler>,
}

#[derive(Clone)]
pub struct PubSubIngressHandlerCell {
    handler: Arc<dyn crate::pubsub::PubSubIngressHandler>,
}

#[derive(Clone)]
pub struct ActorTellHandlerSyncCell {
    owner: Arc<dyn ActorTellHandlerSync>,
    ptr: usize,
    call: unsafe fn(usize, u64, u32, crate::aligned::AlignedBytes) -> Result<()>,
}

impl ActorTellHandlerSyncCell {
    pub(crate) fn new<H>(handler: Arc<H>) -> Self
    where
        H: ActorTellHandlerSync + 'static,
    {
        unsafe fn call_impl<H>(
            ptr: usize,
            actor_id: u64,
            type_hash: u32,
            payload: crate::aligned::AlignedBytes,
        ) -> Result<()>
        where
            H: ActorTellHandlerSync + 'static,
        {
            let handler = unsafe { &*(ptr as *const H) };
            handler.handle_actor_tell_sync(actor_id, type_hash, payload)
        }

        let ptr = Arc::as_ptr(&handler) as usize;
        let owner: Arc<dyn ActorTellHandlerSync> = handler;
        Self {
            owner,
            ptr,
            call: call_impl::<H>,
        }
    }

    #[inline]
    pub(crate) fn handle(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
    ) -> Result<()> {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, actor_id, type_hash, payload) }
    }
}

#[derive(Clone)]
pub struct ActorAskHandlerSyncCell {
    owner: Arc<dyn ActorAskHandlerSync>,
    ptr: usize,
    call: for<'a> unsafe fn(
        usize,
        u64,
        u32,
        crate::aligned::AlignedBytes,
        crate::AskContext<'a>,
    ) -> Result<AskDisposition>,
}

#[derive(Clone)]
pub struct ActorAskImmediateHandlerSyncCell {
    owner: Arc<dyn ActorAskImmediateHandlerSync>,
    ptr: usize,
    can_handle: unsafe fn(usize, u64, u32) -> bool,
    call: unsafe fn(usize, u64, u32, crate::aligned::AlignedBytes) -> Result<AskDisposition>,
}

impl ActorAskImmediateHandlerSyncCell {
    pub(crate) fn new<H>(handler: Arc<H>) -> Self
    where
        H: ActorAskImmediateHandlerSync + 'static,
    {
        unsafe fn can_handle_impl<H>(ptr: usize, actor_id: u64, type_hash: u32) -> bool
        where
            H: ActorAskImmediateHandlerSync + 'static,
        {
            let handler = unsafe { &*(ptr as *const H) };
            handler.can_handle_actor_ask_sync_immediate(actor_id, type_hash)
        }

        unsafe fn call_impl<H>(
            ptr: usize,
            actor_id: u64,
            type_hash: u32,
            payload: crate::aligned::AlignedBytes,
        ) -> Result<AskDisposition>
        where
            H: ActorAskImmediateHandlerSync + 'static,
        {
            let handler = unsafe { &*(ptr as *const H) };
            handler.handle_actor_ask_sync_immediate(actor_id, type_hash, payload)
        }

        let ptr = Arc::as_ptr(&handler) as usize;
        let owner: Arc<dyn ActorAskImmediateHandlerSync> = handler;
        Self {
            owner,
            ptr,
            can_handle: can_handle_impl::<H>,
            call: call_impl::<H>,
        }
    }

    #[inline]
    pub(crate) fn can_handle(&self, actor_id: u64, type_hash: u32) -> bool {
        let _keepalive = &self.owner;
        unsafe { (self.can_handle)(self.ptr, actor_id, type_hash) }
    }

    #[inline]
    pub(crate) fn handle(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
    ) -> Result<AskDisposition> {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, actor_id, type_hash, payload) }
    }
}

impl ActorAskHandlerSyncCell {
    pub(crate) fn new<H>(handler: Arc<H>) -> Self
    where
        H: ActorAskHandlerSync + 'static,
    {
        unsafe fn call_impl<H>(
            ptr: usize,
            actor_id: u64,
            type_hash: u32,
            payload: crate::aligned::AlignedBytes,
            context: crate::AskContext<'_>,
        ) -> Result<AskDisposition>
        where
            H: ActorAskHandlerSync + 'static,
        {
            let handler = unsafe { &*(ptr as *const H) };
            handler.handle_actor_ask_sync(actor_id, type_hash, payload, context)
        }

        let ptr = Arc::as_ptr(&handler) as usize;
        let owner: Arc<dyn ActorAskHandlerSync> = handler;
        Self {
            owner,
            ptr,
            call: call_impl::<H>,
        }
    }

    #[inline]
    pub(crate) fn handle(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        context: crate::AskContext<'_>,
    ) -> Result<AskDisposition> {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, actor_id, type_hash, payload, context) }
    }
}

#[derive(Clone)]
pub struct ActorMessageHandlerSyncCell {
    owner: Arc<dyn ActorMessageHandlerSync>,
    ptr: usize,
    call: unsafe fn(
        usize,
        u64,
        u32,
        crate::aligned::AlignedBytes,
        Option<u16>,
    ) -> Result<Option<ActorResponse>>,
}

impl ActorMessageHandlerSyncCell {
    pub(crate) fn new<H>(handler: Arc<H>) -> Self
    where
        H: ActorMessageHandlerSync + 'static,
    {
        unsafe fn call_impl<H>(
            ptr: usize,
            actor_id: u64,
            type_hash: u32,
            payload: crate::aligned::AlignedBytes,
            correlation_id: Option<u16>,
        ) -> Result<Option<ActorResponse>>
        where
            H: ActorMessageHandlerSync + 'static,
        {
            let handler = unsafe { &*(ptr as *const H) };
            handler.handle_actor_message_sync(actor_id, type_hash, payload, correlation_id)
        }

        let ptr = Arc::as_ptr(&handler) as usize;
        let owner: Arc<dyn ActorMessageHandlerSync> = handler;
        Self {
            owner,
            ptr,
            call: call_impl::<H>,
        }
    }

    #[inline]
    pub(crate) fn handle(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> Result<Option<ActorResponse>> {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, actor_id, type_hash, payload, correlation_id) }
    }
}

impl PubSubIngressHandlerCell {
    pub(crate) fn new(handler: Arc<dyn crate::pubsub::PubSubIngressHandler>) -> Self {
        Self { handler }
    }

    #[inline]
    pub(crate) fn handle(
        &self,
        authenticated_source_peer_id: &crate::PeerId,
        payload: crate::AlignedBytes,
    ) -> Result<()> {
        self.handler
            .handle_pubsub_frame(authenticated_source_peer_id, payload)
    }
}

#[derive(Clone)]
pub struct PeerDisconnectHandlerCell {
    handler: Arc<dyn PeerDisconnectHandler>,
}

#[derive(Clone)]
pub struct PeerConnectHandlerCell {
    handler: Arc<dyn PeerConnectHandler>,
}

#[derive(Debug, Clone)]
pub struct UdpFailureDetectorConfig {
    pub health_probe_interval: Duration,
    pub suspect_after_missed_probes: u32,
    pub terminate_after_missed_probes: u32,
    pub terminate_timeout: Duration,
}

impl Default for UdpFailureDetectorConfig {
    fn default() -> Self {
        Self {
            health_probe_interval: Duration::from_millis(200),
            suspect_after_missed_probes: 2,
            terminate_after_missed_probes: 4,
            terminate_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpLivenessState {
    Alive,
    Suspect,
    Terminated,
}

#[derive(Debug, Clone)]
struct UdpPeerLiveness {
    state: UdpLivenessState,
    missed_probes: u32,
    last_probe_at_ms: u64,
    suspect_since_ms: Option<u64>,
}

impl UdpPeerLiveness {
    fn alive(now_ms: u64) -> Self {
        Self {
            state: UdpLivenessState::Alive,
            missed_probes: 0,
            last_probe_at_ms: now_ms,
            suspect_since_ms: None,
        }
    }
}

impl ActorResponse {
    pub fn pooled(
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Self {
        Self::Pooled {
            payload,
            prefix,
            payload_len,
        }
    }
}

impl From<bytes::Bytes> for ActorResponse {
    fn from(value: bytes::Bytes) -> Self {
        ActorResponse::Bytes(value)
    }
}

impl From<Vec<u8>> for ActorResponse {
    fn from(value: Vec<u8>) -> Self {
        ActorResponse::Bytes(bytes::Bytes::from(value))
    }
}

impl From<crate::AlignedBytes> for ActorResponse {
    fn from(value: crate::AlignedBytes) -> Self {
        ActorResponse::Aligned(value)
    }
}

/// Future type for actor message handling responses
pub type ActorMessageFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<ActorResponse>>> + Send + 'a>>;

/// Callback trait for handling incoming actor messages
pub trait ActorMessageHandler: Send + Sync {
    /// Handle an incoming actor message
    fn handle_actor_message(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_>;
}

/// Synchronous tell handler for ultra-low-latency fire-and-forget paths.
pub trait ActorTellHandlerSync: Send + Sync {
    fn handle_actor_tell_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
    ) -> Result<()>;
}

/// Synchronous ask handler that may either reply inline or complete later via `AskResponder`.
pub trait ActorAskHandlerSync: Send + Sync {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        context: crate::AskContext<'_>,
    ) -> Result<AskDisposition>;
}

/// Synchronous ask handler for the immediate-response hot path.
pub trait ActorAskImmediateHandlerSync: Send + Sync {
    fn can_handle_actor_ask_sync_immediate(&self, _actor_id: u64, _type_hash: u32) -> bool {
        true
    }

    fn handle_actor_ask_sync_immediate(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
    ) -> Result<AskDisposition>;
}

/// Synchronous actor message handler for ultra-low-latency paths.
pub trait ActorMessageHandlerSync: Send + Sync {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> Result<Option<ActorResponse>>;
}

pub trait PeerDisconnectHandler: Send + Sync {
    fn handle_peer_disconnect(
        &self,
        peer_addr: SocketAddr,
        peer_id: Option<crate::PeerId>,
    ) -> BoxFuture<'_, ()>;
}

pub trait PeerConnectHandler: Send + Sync {
    fn handle_peer_connect(
        &self,
        peer_addr: SocketAddr,
        peer_id: Option<crate::PeerId>,
    ) -> BoxFuture<'_, ()>;
}

/// Registry change types for delta tracking with vector clocks
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub enum RegistryChange {
    /// Actor was added or updated
    ActorAdded {
        name: String,
        location: RemoteActorLocation,
        priority: RegistrationPriority,
    },
    /// Actor was removed
    ActorRemoved {
        name: String,
        vector_clock: crate::VectorClock,
        removing_node_id: crate::NodeId, // Node that performed the removal
        priority: RegistrationPriority,
    },
}

/// Delta representing changes since a specific sequence number
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct RegistryDelta {
    pub since_sequence: u64,
    pub current_sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub sender_peer_id: crate::PeerId, // Peer's unique identifier
    pub wall_clock_time: u64,          // For debugging/monitoring only
    pub precise_timing_nanos: u64,     // High precision timing for latency measurements
}

/// Peer health status from a reporter's perspective
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct PeerHealthStatus {
    /// Is the peer reachable from this reporter
    pub is_alive: bool,
    /// Last successful contact timestamp
    pub last_contact: u64,
    /// Number of failed connection attempts
    pub failure_count: u32,
}

/// Pending peer failure awaiting consensus
#[derive(Debug, Clone)]
pub struct PendingFailure {
    /// When we first detected the failure
    pub first_detected: u64,
    /// Timeout for collecting consensus
    pub consensus_deadline: u64,
    /// Have we queried other peers yet
    pub query_sent: bool,
}

/// Pending ACK state for synchronous registrations.
///
/// This avoids `tokio::sync::oneshot` and does not require holding any locks while awaiting
/// a network callback.
#[derive(Debug)]
pub struct PendingAck {
    // 0 = pending, 1 = success, 2 = failure, 3 = canceled (timeout/shutdown).
    state: AtomicU8,
    waker: AtomicWaker,
}

impl PendingAck {
    const PENDING: u8 = 0;
    const SUCCESS: u8 = 1;
    const FAILURE: u8 = 2;
    const CANCELED: u8 = 3;

    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(Self::PENDING),
            waker: AtomicWaker::new(),
        }
    }

    /// Completes the ACK (idempotent). Late completions after `cancel()` are ignored.
    pub fn complete(&self, success: bool) {
        let target = if success {
            Self::SUCCESS
        } else {
            Self::FAILURE
        };
        let _ =
            self.state
                .compare_exchange(Self::PENDING, target, Ordering::AcqRel, Ordering::Acquire);
        self.waker.wake();
    }

    /// Cancels the ACK (idempotent), typically used when timing out.
    pub fn cancel(&self) {
        let _ = self.state.compare_exchange(
            Self::PENDING,
            Self::CANCELED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.waker.wake();
    }

    /// Wait for completion. Returns `Some(success)` when completed, or `None` if canceled.
    pub async fn wait(&self) -> Option<bool> {
        poll_fn(|cx| self.poll_wait(cx)).await
    }

    fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<Option<bool>> {
        match self.state.load(Ordering::Acquire) {
            Self::SUCCESS => return Poll::Ready(Some(true)),
            Self::FAILURE => return Poll::Ready(Some(false)),
            Self::CANCELED => return Poll::Ready(None),
            _ => {}
        }

        self.waker.register(cx.waker());

        match self.state.load(Ordering::Acquire) {
            Self::SUCCESS => Poll::Ready(Some(true)),
            Self::FAILURE => Poll::Ready(Some(false)),
            Self::CANCELED => Poll::Ready(None),
            _ => Poll::Pending,
        }
    }
}

/// Message types for the gossip protocol
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub enum RegistryMessage {
    /// Delta gossip message containing only changes
    DeltaGossip { delta: RegistryDelta },
    /// Response to delta gossip with our own delta
    DeltaGossipResponse { delta: RegistryDelta },
    /// Request for full sync (fallback when deltas are unavailable)
    FullSyncRequest {
        sender_peer_id: crate::PeerId,    // Peer's unique identifier
        sender_bind_addr: Option<String>, // Sender's listening address (optional for backwards compat)
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Full synchronization message
    FullSync {
        local_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        known_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        sender_peer_id: crate::PeerId,                    // Peer's unique identifier
        sender_bind_addr: Option<String>, // Sender's listening address (optional for backwards compat)
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Response to full sync
    FullSyncResponse {
        local_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        known_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        sender_peer_id: crate::PeerId,                    // Peer's unique identifier
        sender_bind_addr: Option<String>, // Sender's listening address (optional for backwards compat)
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Peer health status report
    PeerHealthReport {
        reporter: crate::PeerId,
        peer_statuses: Vec<(String, PeerHealthStatus)>, // Use Vec for rkyv serialization
        timestamp: u64,
    },
    /// Lightweight ACK for immediate registrations
    ImmediateAck { actor_name: String, success: bool },
    /// Query for peer health consensus
    PeerHealthQuery {
        sender: crate::PeerId,
        target_peer: String,
        timestamp: u64,
    },
    /// Direct actor message (tell or ask)
    ActorMessage {
        actor_id: String,
        type_hash: u32,
        payload: Vec<u8>,
        correlation_id: Option<u16>,
    },
    /// Peer list gossip for automatic peer discovery
    /// Contains list of known peers with their connection info
    PeerListGossip {
        /// List of known peers (address as string for rkyv, peer info)
        peers: Vec<PeerInfoGossip>,
        /// Timestamp when this gossip was generated
        timestamp: u64,
        /// Sender's advertised address (so receiver can add us to their peer list)
        sender_addr: String,
    },
}

/// Statistics about the gossip registry
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct RegistryStats {
    pub local_actors: usize,
    pub known_actors: usize,
    pub active_peers: usize,
    pub failed_peers: usize,
    pub total_gossip_rounds: u64,
    pub current_sequence: u64,
    pub uptime_seconds: u64,
    pub last_gossip_timestamp: u64,
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub delta_history_size: usize,
    pub avg_delta_size: f64,
    // Peer discovery metrics (Phase 5)
    /// Number of peers discovered via gossip (in known_peers cache)
    pub discovered_peers: usize,
    /// Number of failed peer discovery attempts (connection failures to discovered peers)
    pub failed_discovery_attempts: u64,
    /// Average mesh connectivity (connected_peers / known_peers ratio)
    pub avg_mesh_connectivity: f64,
    /// Time taken to form initial mesh (first N peers connected), in milliseconds
    pub mesh_formation_time_ms: Option<u64>,
}

/// Peer information with failure tracking and delta state
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub address: SocketAddr, // Listening address (resolved from DNS or direct IP)
    pub peer_address: Option<SocketAddr>, // Actual connection address (may be NATed)
    /// Whether this peer has been observed on an inbound connection accepted by this node.
    pub inbound_observed: bool,
    /// Whether this node has ever established an outbound dial to this peer.
    pub outbound_dial_success: bool,
    pub node_id: Option<crate::NodeId>, // NodeId for TLS verification (may be learned on connect)
    /// DNS name for this peer (e.g., "data-feeder-icanact:9400").
    /// When set, the address will be re-resolved via DNS on reconnection attempts.
    /// This handles Kubernetes pod restarts where the IP changes but DNS stays the same.
    pub dns_name: Option<String>,
    pub failures: usize,
    pub last_attempt: u64,
    pub last_success: u64,
    pub last_sequence: u64,
    /// Last sequence we successfully sent to this peer
    pub last_sent_sequence: u64,
    /// Number of consecutive delta exchanges with this peer
    pub consecutive_deltas: u64,
    /// When this peer last failed (for tracking permanent failures)
    pub last_failure_time: Option<u64>,
    /// Last time we attempted a DNS refresh for this peer (rate limiting).
    pub last_dns_refresh_attempt: Option<u64>,
    /// Last time we received a gossip response *payload* from this peer
    /// (not merely sent to). Used by the response-asymmetry liveness
    /// detector: if we keep sending and never see a response within
    /// `config.peer_liveness_window`, treat the peer as failed even when
    /// the persistent-connection write succeeds at the kernel level.
    /// `0` means "no response observed yet" — treated as new-peer (no
    /// stale verdict until either we get one or the configured grace
    /// expires from `last_attempt`).
    pub last_response_received: u64,
}

impl PeerInfo {
    /// Create a PeerInfo for the local node (self) for gossip inclusion
    /// Used when including ourselves in peer list gossip
    pub fn local(advertise_addr: SocketAddr) -> Self {
        let now = crate::current_timestamp();
        Self {
            address: advertise_addr,
            peer_address: None,
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: None,
            dns_name: None,
            failures: 0,
            last_attempt: now,
            last_success: now, // Important: prevents pruning
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        }
    }

    /// Create a new PeerInfo with a DNS name for automatic re-resolution on reconnect.
    ///
    /// When a peer is configured with a DNS name, the gossip system will re-resolve
    /// the DNS name to get the current IP address when attempting to reconnect
    /// after a connection failure. This handles Kubernetes pod restarts where
    /// the pod IP changes but the service DNS name stays the same.
    ///
    /// # Arguments
    /// * `address` - The currently resolved IP address
    /// * `dns_name` - The DNS name to re-resolve on reconnect (e.g., "data-feeder-icanact:9400")
    pub fn with_dns(address: SocketAddr, dns_name: String) -> Self {
        let now = crate::current_timestamp();
        Self {
            address,
            peer_address: None,
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: None,
            dns_name: Some(dns_name),
            failures: 0,
            last_attempt: now,
            last_success: 0,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        }
    }

    /// Check if this peer has a DNS name configured for re-resolution
    pub fn has_dns_name(&self) -> bool {
        self.dns_name.is_some()
    }

    /// Get the DNS name if configured
    pub fn get_dns_name(&self) -> Option<&str> {
        self.dns_name.as_deref()
    }

    /// Update the resolved address (called after DNS re-resolution)
    pub fn update_address(&mut self, new_addr: SocketAddr) {
        self.address = new_addr;
    }

    /// Convert to gossip-serializable format
    pub fn to_gossip(&self) -> PeerInfoGossip {
        PeerInfoGossip {
            address: self.address.to_string(),
            peer_address: self.peer_address.map(|a| a.to_string()),
            node_id: self.node_id,
            failures: self.failures,
            last_attempt: self.last_attempt,
            last_success: self.last_success,
            dns_name: self.dns_name.clone(),
        }
    }

    /// Create from gossip-serializable format
    pub fn from_gossip(gossip: &PeerInfoGossip) -> Option<Self> {
        let address: SocketAddr = gossip.address.parse().ok()?;
        let peer_address = gossip.peer_address.as_ref().and_then(|a| a.parse().ok());

        Some(Self {
            address,
            peer_address,
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: gossip.node_id,
            dns_name: gossip.dns_name.clone(), // DNS names are now gossiped for fault tolerance
            failures: gossip.failures,
            last_attempt: gossip.last_attempt,
            last_success: gossip.last_success,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        })
    }
}

/// Peer information for gossip (rkyv-serializable version)
/// Uses String instead of SocketAddr for rkyv serialization
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct PeerInfoGossip {
    /// Listening address (as string for rkyv serialization)
    pub address: String,
    /// Actual connection address if different (NAT)
    pub peer_address: Option<String>,
    /// NodeId for TLS verification
    pub node_id: Option<crate::NodeId>,
    /// Number of consecutive failures
    pub failures: usize,
    /// Last connection attempt timestamp
    pub last_attempt: u64,
    /// Last successful connection timestamp
    pub last_success: u64,
    /// DNS name for this peer (e.g., "data-feeder.default.svc.cluster.local:9000")
    /// Used to re-resolve the address if the underlying IP changes
    pub dns_name: Option<String>,
}

/// Historical delta for efficient incremental updates
#[derive(Debug, Clone)]
pub struct HistoricalDelta {
    pub sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub wall_clock_time: u64,
}

/// Data needed to perform gossip with a single peer
#[derive(Debug)]
pub struct GossipTask {
    pub peer_addr: SocketAddr,
    pub message: RegistryMessage,
    pub current_sequence: u64,
}

/// Result of a gossip operation
#[derive(Debug)]
pub struct GossipResult {
    pub peer_addr: SocketAddr,
    pub sent_sequence: u64,
    pub outcome: Result<Option<RegistryMessage>>,
}

/// Separated actor state for read-heavy operations (now with vector clocks)
#[derive(Default)]
pub struct ActorState {
    pub local_actors: SccHashMap<String, RemoteActorLocation>,
    pub known_actors: SccHashMap<String, RemoteActorLocation>,
}

/// Gossip coordination state for write-heavy operations
#[derive(Debug)]
pub struct GossipState {
    pub gossip_sequence: u64,
    pub pending_changes: Vec<RegistryChange>,
    pub urgent_changes: Vec<RegistryChange>, // High/Critical priority changes
    pub delta_history: Vec<HistoricalDelta>,
    pub peers: HashMap<SocketAddr, PeerInfo>,
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub shutdown: bool,
    /// Track which actors are connected from which peer address
    pub peer_to_actors: HashMap<SocketAddr, HashSet<String>>,
    /// Track peer health reports from different observers
    pub peer_health_reports: HashMap<SocketAddr, HashMap<SocketAddr, PeerHealthStatus>>,
    /// Pending peer failures that need consensus
    pub pending_peer_failures: HashMap<SocketAddr, PendingFailure>,

    // =================== Peer Discovery State ===================
    /// Last time we sent peer list gossip (for rate limiting)
    pub last_peer_gossip_time: u64,
    /// Peer discovery manager (None if peer discovery is disabled)
    pub peer_discovery: Option<PeerDiscovery>,
    /// LRU cache of known peers discovered via gossip
    pub known_peers: LruCache<SocketAddr, PeerInfo>,
    /// Timestamp (in millis since start_time) when mesh formation completed
    pub mesh_formation_time_ms: Option<u64>,
}

/// Core gossip registry implementation with separated locks
#[derive(Clone)]
pub struct GossipRegistry<T = ()> {
    // Immutable config
    pub bind_addr: SocketAddr,
    pub peer_id: crate::PeerId, // Unique peer identifier (public key)
    pub config: GossipConfig,
    pub start_time: u64,
    pub start_instant: Instant,

    // UDP datagram transport mode flag and liveness detector controls.
    pub udp_mode: bool,
    pub udp_failure_detector_config: UdpFailureDetectorConfig,
    udp_liveness: Arc<Mutex<HashMap<SocketAddr, UdpPeerLiveness>>>,

    /// Atomic shutdown flag for lock-free checking in hot paths
    pub shutdown: Arc<AtomicBool>,

    // Separated lockable state
    pub actor_state: Arc<ActorState>,
    pub gossip_state: Arc<Mutex<GossipState>>,
    // Connection pool is internally lock-free (scc-based), no external locking needed
    pub connection_pool: Arc<ConnectionPool<T>>,
    pub tls_config: Option<Arc<crate::tls::TlsConfig>>,
    pub peer_capabilities: Arc<SccHashMap<SocketAddr, crate::handshake::PeerCapabilities>>,
    pub peer_capabilities_by_node:
        Arc<SccHashMap<crate::NodeId, crate::handshake::PeerCapabilities>>,
    pub peer_capability_addr_to_node: Arc<SccHashMap<SocketAddr, crate::NodeId>>,

    // Actor message handler callback
    pub actor_message_handler: Arc<ArcSwapOption<ActorMessageHandlerCell>>,
    pub actor_tell_handler_sync: Arc<ArcSwapOption<ActorTellHandlerSyncCell>>,
    pub actor_ask_immediate_handler_sync: Arc<ArcSwapOption<ActorAskImmediateHandlerSyncCell>>,
    pub actor_ask_handler_sync: Arc<ArcSwapOption<ActorAskHandlerSyncCell>>,
    pub actor_message_handler_sync: Arc<ArcSwapOption<ActorMessageHandlerSyncCell>>,
    pub pubsub_ingress_handler: Arc<ArcSwapOption<PubSubIngressHandlerCell>>,
    pub peer_disconnect_handler: Arc<ArcSwapOption<PeerDisconnectHandlerCell>>,
    pub peer_connect_handler: Arc<ArcSwapOption<PeerConnectHandlerCell>>,

    // Stream assembly state (lock-free map).
    pub stream_assemblies: Arc<SccHashMap<u64, StreamAssembly>>,

    // Pending ACKs for synchronous registrations (bounded, lock-free map).
    pub pending_acks: Arc<SccHashMap<String, Arc<PendingAck>>>,

    /// Tracks the currently-running peer discovery dial task (H-004).
    pub discovery_task: Arc<DiscoveryTaskTracker>,

    /// Injectable DNS resolver used for deterministic tests and uniform reconnect behavior.
    pub dns_resolver: Arc<tokio::sync::RwLock<Arc<dyn crate::dns::DnsResolver>>>,
}

#[derive(Debug, Default)]
pub struct DiscoveryTaskTracker {
    handle: ArcSwapOption<AbortHandle>,
}

impl DiscoveryTaskTracker {
    pub fn set(&self, handle: AbortHandle) {
        if let Some(old) = self.handle.swap(Some(Arc::new(handle))) {
            old.abort();
        }
    }

    pub fn abort(&self) {
        if let Some(handle) = self.handle.swap(None) {
            handle.abort();
        }
    }
}

impl Drop for DiscoveryTaskTracker {
    fn drop(&mut self) {
        self.abort();
    }
}

/// State for assembling streamed messages
#[derive(Debug)]
pub struct StreamAssembly {
    pub header: crate::StreamHeader,
    pub received_indices: std::collections::BTreeSet<u32>,
    pub received_bytes: usize,
    pub buffer: crate::PooledAlignedBuffer,
    pub chunk_stride: Option<usize>,
    /// Timestamp when stream assembly started (for stale cleanup)
    pub started_at: std::time::Instant,
    /// Correlation ID for ask_streaming (to send response back)
    pub correlation_id: Option<u16>,
    /// Peer address to send response to (for ask_streaming)
    pub peer_addr: Option<std::net::SocketAddr>,
}

impl StreamAssembly {
    /// Check if the stream assembly is complete (all chunks received with no gaps)
    pub fn is_complete(&self) -> bool {
        let Some(stride) = self.chunk_stride else {
            return false;
        };
        let expected_chunks = self.header.total_size.div_ceil(stride as u64);

        if self.received_indices.len() as u64 != expected_chunks {
            return false;
        }

        // Verify all indices 0..N-1 are present (no gaps)
        for i in 0..expected_chunks as u32 {
            if !self.received_indices.contains(&i) {
                return false;
            }
        }

        true
    }
}

/// Result of completing a stream assembly
#[derive(Debug)]
pub struct StreamAssemblyResult {
    /// The assembled complete message
    pub data: crate::AlignedBytes,
    /// Correlation ID for ask_streaming responses
    pub correlation_id: Option<u16>,
    /// Peer address to send response to
    pub peer_addr: Option<std::net::SocketAddr>,
    /// Original stream header
    pub header: crate::StreamHeader,
}

impl<T: 'static> GossipRegistry<T> {
    fn as_regular_gossip_change(change: &RegistryChange) -> RegistryChange {
        match change {
            RegistryChange::ActorAdded { name, location, .. } => RegistryChange::ActorAdded {
                name: name.clone(),
                location: location.clone(),
                priority: RegistrationPriority::Normal,
            },
            RegistryChange::ActorRemoved {
                name,
                vector_clock,
                removing_node_id,
                ..
            } => RegistryChange::ActorRemoved {
                name: name.clone(),
                vector_clock: vector_clock.clone(),
                removing_node_id: *removing_node_id,
                priority: RegistrationPriority::Normal,
            },
        }
    }

    /// Create a new gossip registry
    pub fn new(bind_addr: SocketAddr, config: GossipConfig) -> Self {
        // Use public key from config (required for TLS identity)
        let peer_id = config
            .key_pair
            .as_ref()
            .expect("GossipConfig.key_pair is required for TLS-only mode")
            .peer_id();

        info!(
            bind_addr = %bind_addr,
            peer_id = %peer_id,
            "creating new gossip registry"
        );

        let aligned_pool_size =
            crate::aligned::DEFAULT_ALIGNED_POOL_SIZE.max(config.ask_window.saturating_mul(8));
        let connection_pool = ConnectionPool::new_with_aligned_pool_size(
            config.max_pooled_connections,
            config.connection_timeout,
            aligned_pool_size,
        );
        let peer_capabilities = Arc::new(SccHashMap::default());

        Self {
            bind_addr,
            peer_id,
            config: config.clone(),
            start_time: current_timestamp(),
            start_instant: crate::current_instant(),
            udp_mode: false,
            udp_failure_detector_config: UdpFailureDetectorConfig::default(),
            udp_liveness: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            actor_state: Arc::new(ActorState::default()),
            gossip_state: Arc::new(Mutex::new(GossipState {
                gossip_sequence: 0,
                pending_changes: Vec::new(),
                urgent_changes: Vec::new(),
                delta_history: Vec::new(),
                peers: HashMap::new(),
                delta_exchanges: 0,
                full_sync_exchanges: 0,
                shutdown: false,
                peer_to_actors: HashMap::new(),
                peer_health_reports: HashMap::new(),
                pending_peer_failures: HashMap::new(),
                // Peer discovery state
                last_peer_gossip_time: 0,
                peer_discovery: if config.enable_peer_discovery {
                    Some(PeerDiscovery::new(
                        bind_addr,
                        PeerDiscoveryConfig {
                            max_peers: config.max_peers,
                            allow_private_discovery: config.allow_private_discovery,
                            allow_loopback_discovery: config.allow_loopback_discovery,
                            allow_link_local_discovery: config.allow_link_local_discovery,
                            fail_ttl: config.fail_ttl,
                            pending_ttl: config.pending_ttl,
                        },
                    ))
                } else {
                    None
                },
                known_peers: LruCache::new(
                    NonZeroUsize::new(config.known_peers_capacity)
                        .unwrap_or(NonZeroUsize::new(10_000).unwrap()),
                ),
                mesh_formation_time_ms: None,
            })),
            connection_pool: Arc::new(connection_pool),
            tls_config: None,
            peer_capabilities: peer_capabilities.clone(),
            peer_capabilities_by_node: Arc::new(SccHashMap::default()),
            peer_capability_addr_to_node: Arc::new(SccHashMap::default()),
            actor_message_handler: Arc::new(ArcSwapOption::empty()),
            actor_tell_handler_sync: Arc::new(ArcSwapOption::empty()),
            actor_ask_immediate_handler_sync: Arc::new(ArcSwapOption::empty()),
            actor_ask_handler_sync: Arc::new(ArcSwapOption::empty()),
            actor_message_handler_sync: Arc::new(ArcSwapOption::empty()),
            pubsub_ingress_handler: Arc::new(ArcSwapOption::empty()),
            peer_disconnect_handler: Arc::new(ArcSwapOption::empty()),
            peer_connect_handler: Arc::new(ArcSwapOption::empty()),
            stream_assemblies: Arc::new(SccHashMap::default()),
            pending_acks: Arc::new(SccHashMap::default()),
            discovery_task: Arc::new(DiscoveryTaskTracker::default()),
            dns_resolver: Arc::new(tokio::sync::RwLock::new(Arc::new(
                crate::TokioDnsResolver::default(),
            )
                as Arc<dyn crate::dns::DnsResolver>)),
        }
    }

    /// Override the DNS resolver used for peer refreshes (primarily for deterministic tests).
    pub async fn set_dns_resolver(&self, resolver: Arc<dyn crate::dns::DnsResolver>) {
        *self.dns_resolver.write().await = resolver;
    }

    /// Enable TLS for secure connections
    /// This must be called before starting the registry to enable TLS
    pub fn enable_tls(&mut self, secret_key: crate::SecretKey) -> Result<()> {
        self.udp_mode = false;
        self.tls_config = Some(Arc::new(crate::tls::TlsConfig::with_peer_discovery(
            secret_key,
            self.config.enable_peer_discovery,
        )?));
        Ok(())
    }

    /// Enable signed Noise-style authentication for plain TCP connections.
    pub fn enable_noise_auth(&mut self, _secret_key: crate::SecretKey) -> Result<()> {
        self.udp_mode = false;
        tracing::warn!(
            "Noise transport auth is not active in this build; falling back to plain stream transport"
        );
        Ok(())
    }

    /// Enable UDP datagram transport mode with transport-neutral liveness detection.
    ///
    /// Native UDP datagrams are currently rejected because the datagram receive path
    /// has no handshake or message authentication boundary. Keeping this API as an
    /// explicit error prevents deployments from accidentally exposing unauthenticated
    /// gossip and actor dispatch over UDP.
    pub fn enable_udp(&mut self, _secret_key: crate::SecretKey) -> Result<()> {
        Err(GossipError::InvalidConfig(
            "native UDP datagram transport is disabled until inbound datagrams are authenticated"
                .to_string(),
        ))
    }

    fn udp_now_ms(&self) -> u64 {
        self.start_instant.elapsed().as_millis() as u64
    }

    async fn reset_udp_liveness_for_peer(&self, peer_addr: SocketAddr) {
        if !self.udp_mode {
            return;
        }
        let mut liveness = self.udp_liveness.lock().await;
        liveness.insert(peer_addr, UdpPeerLiveness::alive(self.udp_now_ms()));
    }

    /// Run one UDP liveness detector cycle.
    /// Any transition to `Terminated` emits the same disconnect path as stream transports.
    pub async fn run_udp_failure_detector_once(&self) -> Result<usize> {
        if !self.udp_mode {
            return Ok(0);
        }

        let tracked_peers: Vec<SocketAddr> = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state.peers.keys().copied().collect()
        };
        if tracked_peers.is_empty() {
            return Ok(0);
        }

        let now_ms = self.udp_now_ms();
        let mut should_terminate = Vec::new();
        {
            let mut liveness = self.udp_liveness.lock().await;
            for peer_addr in tracked_peers {
                if self.connection_pool.has_connection(&peer_addr) {
                    liveness.insert(peer_addr, UdpPeerLiveness::alive(now_ms));
                    continue;
                }

                let peer_state = liveness
                    .entry(peer_addr)
                    .or_insert_with(|| UdpPeerLiveness::alive(now_ms));

                if now_ms.saturating_sub(peer_state.last_probe_at_ms)
                    < self
                        .udp_failure_detector_config
                        .health_probe_interval
                        .as_millis() as u64
                {
                    continue;
                }

                peer_state.last_probe_at_ms = now_ms;
                peer_state.missed_probes = peer_state.missed_probes.saturating_add(1);

                if peer_state.state == UdpLivenessState::Alive
                    && peer_state.missed_probes
                        >= self.udp_failure_detector_config.suspect_after_missed_probes
                {
                    peer_state.state = UdpLivenessState::Suspect;
                    peer_state.suspect_since_ms = Some(now_ms);
                }

                let suspect_timed_out = peer_state.suspect_since_ms.is_some_and(|since| {
                    now_ms.saturating_sub(since)
                        >= self
                            .udp_failure_detector_config
                            .terminate_timeout
                            .as_millis() as u64
                });

                let should_term = peer_state.missed_probes
                    >= self
                        .udp_failure_detector_config
                        .terminate_after_missed_probes
                    || suspect_timed_out;
                if peer_state.state != UdpLivenessState::Terminated && should_term {
                    peer_state.state = UdpLivenessState::Terminated;
                    should_terminate.push(peer_addr);
                }
            }
        }

        for peer_addr in should_terminate.iter().copied() {
            if let Err(err) = self.handle_peer_connection_failure(peer_addr).await {
                warn!(
                    peer = %peer_addr,
                    error = %err,
                    "udp detector failed to propagate terminated peer"
                );
            }
        }

        Ok(should_terminate.len())
    }

    /// Track negotiated peer capabilities for a peer connection
    pub fn set_peer_capabilities(&self, addr: SocketAddr, caps: PeerCapabilities) {
        let _ = self.peer_capabilities.upsert_sync(addr, caps);
    }

    /// Attach capabilities recorded for an address to a specific NodeId (once known)
    pub async fn associate_peer_capabilities_with_node(&self, addr: SocketAddr, node_id: NodeId) {
        let caps = self.peer_capabilities.read_sync(&addr, |_, v| *v);
        if let Some(caps) = caps {
            let _ = self.peer_capabilities_by_node.upsert_sync(node_id, caps);
        }
        let _ = self.peer_capability_addr_to_node.upsert_sync(addr, node_id);
        self.propagate_node_id_to_known_addresses(addr, node_id)
            .await;
    }

    /// Remove stored capabilities for a peer (e.g., when connection closes)
    pub fn clear_peer_capabilities(&self, addr: &SocketAddr) {
        let _ = self.peer_capabilities.remove_sync(addr);
        if let Some((_, node_id)) = self.peer_capability_addr_to_node.remove_sync(addr) {
            let still_has_addr = AtomicBool::new(false);
            self.peer_capability_addr_to_node.iter_sync(|_, v| {
                if *v == node_id {
                    still_has_addr.store(true, Ordering::Relaxed);
                    return false;
                }
                true
            });

            if !still_has_addr.load(Ordering::Relaxed) {
                let _ = self.peer_capabilities_by_node.remove_sync(&node_id);
            }
        }
    }

    /// Determine whether a peer supports receiving PeerListGossip
    pub async fn peer_supports_peer_list(&self, addr: &SocketAddr) -> bool {
        if let Some(caps) = self.peer_capabilities.read_sync(addr, |_, v| *v) {
            return caps.can_send_peer_list();
        }

        let node_id = self.peer_capability_addr_to_node.read_sync(addr, |_, v| *v);
        if let Some(node_id) = node_id {
            if let Some(caps) = self
                .peer_capabilities_by_node
                .read_sync(&node_id, |_, v| *v)
            {
                return caps.can_send_peer_list();
            }
        }

        if let Some(node_id) = self.lookup_node_id(addr).await {
            let _ = self
                .peer_capability_addr_to_node
                .upsert_sync(*addr, node_id);
            if let Some(caps) = self
                .peer_capabilities_by_node
                .read_sync(&node_id, |_, v| *v)
            {
                return caps.can_send_peer_list();
            }
        }

        let found = AtomicBool::new(false);
        let want_ip = addr.ip();
        self.peer_capabilities.iter_sync(|k, v| {
            if k.ip() == want_ip && v.can_send_peer_list() {
                found.store(true, Ordering::Relaxed);
                return false;
            }
            true
        });
        found.load(Ordering::Relaxed)
    }

    async fn propagate_node_id_to_known_addresses(&self, addr: SocketAddr, node_id: NodeId) {
        // Only track in known_peers if peer discovery is enabled
        if !self.config.enable_peer_discovery {
            return;
        }

        let mut gossip_state = self.gossip_state.lock().await;
        if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
            peer_info.node_id = Some(node_id);
        } else {
            gossip_state.known_peers.put(
                addr,
                PeerInfo {
                    address: addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(node_id),
                    dns_name: None,
                    failures: 0,
                    last_attempt: current_timestamp(),
                    last_success: current_timestamp(),
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }
    }

    /// Register an actor message handler callback
    pub async fn set_actor_message_handler(&self, handler: Arc<dyn ActorMessageHandler>) {
        self.actor_message_handler
            .store(Some(Arc::new(ActorMessageHandlerCell { handler })));
        info!("actor message handler registered");
    }

    /// Register a synchronous tell handler callback (fast path).
    pub async fn set_actor_tell_handler_sync<H>(&self, handler: Arc<H>)
    where
        H: ActorTellHandlerSync + 'static,
    {
        self.actor_tell_handler_sync
            .store(Some(Arc::new(ActorTellHandlerSyncCell::new(handler))));
        info!("actor tell handler sync registered");
    }

    /// Register a synchronous immediate ask handler callback.
    pub async fn set_actor_ask_immediate_handler_sync<H>(&self, handler: Arc<H>)
    where
        H: ActorAskImmediateHandlerSync + 'static,
    {
        self.actor_ask_immediate_handler_sync.store(Some(Arc::new(
            ActorAskImmediateHandlerSyncCell::new(handler),
        )));
        info!("actor ask immediate handler sync registered");
    }

    /// Register a synchronous ask handler callback.
    pub async fn set_actor_ask_handler_sync<H>(&self, handler: Arc<H>)
    where
        H: ActorAskHandlerSync + 'static,
    {
        self.actor_ask_handler_sync
            .store(Some(Arc::new(ActorAskHandlerSyncCell::new(handler))));
        info!("actor ask handler sync registered");
    }

    /// Register a synchronous actor message handler callback (fast path).
    pub async fn set_actor_message_handler_sync<H>(&self, handler: Arc<H>)
    where
        H: ActorMessageHandlerSync + 'static,
    {
        self.actor_message_handler_sync
            .store(Some(Arc::new(ActorMessageHandlerSyncCell::new(handler))));
        info!("actor message handler sync registered");
    }

    /// Register the routed PubSub ingress handler.
    pub async fn set_pubsub_ingress_handler(
        &self,
        handler: Arc<dyn crate::pubsub::PubSubIngressHandler>,
    ) {
        self.pubsub_ingress_handler
            .store(Some(Arc::new(PubSubIngressHandlerCell::new(handler))));
        info!("pubsub ingress handler registered");
    }

    /// Remove the actor message handler callback
    pub async fn clear_actor_message_handler(&self) {
        self.actor_message_handler.store(None);
        info!("actor message handler cleared");
    }

    /// Remove the synchronous tell handler callback.
    pub async fn clear_actor_tell_handler_sync(&self) {
        self.actor_tell_handler_sync.store(None);
        info!("actor tell handler sync cleared");
    }

    /// Remove the synchronous immediate ask handler callback.
    pub async fn clear_actor_ask_immediate_handler_sync(&self) {
        self.actor_ask_immediate_handler_sync.store(None);
        info!("actor ask immediate handler sync cleared");
    }

    /// Remove the synchronous ask handler callback.
    pub async fn clear_actor_ask_handler_sync(&self) {
        self.actor_ask_handler_sync.store(None);
        info!("actor ask handler sync cleared");
    }

    /// Remove the synchronous actor message handler callback
    pub async fn clear_actor_message_handler_sync(&self) {
        self.actor_message_handler_sync.store(None);
        info!("actor message handler sync cleared");
    }

    /// Register a peer disconnect handler callback
    pub async fn set_peer_disconnect_handler(&self, handler: Arc<dyn PeerDisconnectHandler>) {
        self.peer_disconnect_handler
            .store(Some(Arc::new(PeerDisconnectHandlerCell { handler })));
        info!("peer disconnect handler registered");
    }

    /// Register a peer connect handler callback
    pub async fn set_peer_connect_handler(&self, handler: Arc<dyn PeerConnectHandler>) {
        self.peer_connect_handler
            .store(Some(Arc::new(PeerConnectHandlerCell { handler })));
        info!("peer connect handler registered");
    }

    /// Remove the peer disconnect handler callback
    pub async fn clear_peer_disconnect_handler(&self) {
        self.peer_disconnect_handler.store(None);
        info!("peer disconnect handler cleared");
    }

    /// Remove the peer connect handler callback
    pub async fn clear_peer_connect_handler(&self) {
        self.peer_connect_handler.store(None);
        info!("peer connect handler cleared");
    }

    /// Handle an incoming actor message by forwarding to the registered callback
    pub async fn handle_actor_message(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> Result<Option<ActorResponse>> {
        if let Some(cell) = self.actor_message_handler_sync.load_full() {
            return cell.handle(actor_id, type_hash, payload, correlation_id);
        }
        if let Some(cell) = self.actor_message_handler.load_full() {
            debug!(
                actor_id = actor_id,
                type_hash = type_hash,
                payload_len = payload.len(),
                "forwarding actor message to handler"
            );
            cell.handler
                .handle_actor_message(actor_id, type_hash, payload, correlation_id)
                .await
        } else {
            warn!(
                actor_id = actor_id,
                type_hash = type_hash,
                "no actor message handler registered - message dropped"
            );
            Ok(None)
        }
    }

    // Add bootstrap peers for initial connection (DEPRECATED)
    // pub async fn add_bootstrap_peers(&self, bootstrap_peers: Vec<SocketAddr>) {
    //     let mut gossip_state = self.gossip_state.lock().await;
    //     let current_time = current_timestamp();

    //     for peer in bootstrap_peers {
    //         if peer != self.bind_addr {
    //             gossip_state.peers.insert(
    //                 peer,
    //                 PeerInfo {
    //                     address: peer,
    //                     peer_address: None,
    //                     // Start with max failures - peers are offline until proven otherwise
    //                     failures: self.config.max_peer_failures,
    //                     last_attempt: 0,
    //                     last_success: 0,
    //                     last_sequence: 0,
    //                     last_sent_sequence: 0,
    //                     consecutive_deltas: 0,
    //                     // Mark the failure time so retry logic works
    //                     last_failure_time: Some(current_time),
    //                 },
    //             );
    //             info!(peer = %peer, "added bootstrap peer as initially offline");
    //         }
    //     }
    //     info!(
    //         peer_count = gossip_state.peers.len(),
    //         "added bootstrap peers (all initially offline)"
    //     );
    // }

    // /// Add bootstrap peers with their expected node names
    // pub async fn add_bootstrap_peers_with_names(&self, bootstrap_peers: Vec<crate::PeerConfig>) {
    //     let mut gossip_state = self.gossip_state.lock().await;
    //     let pool = &self.connection_pool;
    //     let current_time = current_timestamp();

    //     for peer_config in bootstrap_peers {
    //         if peer_config.addr != self.bind_addr {
    //             // Store the peer with its address
    //             gossip_state.peers.insert(
    //                 peer_config.addr,
    //                 PeerInfo {
    //                     address: peer_config.addr,
    //                     peer_address: None,
    //                     // Start with max failures - peers are offline until proven otherwise
    //                     failures: self.config.max_peer_failures,
    //                     last_attempt: 0,
    //                     last_success: 0,
    //                     last_sequence: 0,
    //                     last_sent_sequence: 0,
    //                     consecutive_deltas: 0,
    //                     // Mark the failure time so retry logic works
    //                     last_failure_time: Some(current_time),
    //                 },
    //             );

    //             // Map the expected node name to this address
    //             pool.update_node_address(&peer_config.node_name, peer_config.addr);
    //             info!("Bootstrap peer: {} -> {}", peer_config.node_name, peer_config.addr);
    //         }
    //     }
    //     info!(
    //         peer_count = gossip_state.peers.len(),
    //         "added bootstrap peers with names (all initially offline)"
    //     );
    // }

    /// Add a new peer (called when receiving connections)
    pub async fn add_peer(&self, peer_addr: SocketAddr) {
        self.add_peer_with_node_id(peer_addr, None).await;
    }

    /// Add a new peer with NodeId for TLS verification
    pub async fn add_peer_with_node_id(
        &self,
        peer_addr: SocketAddr,
        node_id: Option<crate::NodeId>,
    ) {
        debug!(peer = %peer_addr, self_addr = %self.bind_addr, has_node_id = node_id.is_some(), "add_peer_with_node_id called");
        if peer_addr != self.bind_addr {
            {
                let mut gossip_state = self.gossip_state.lock().await;

                // Check if we already have this peer
                if let Some(existing_peer) = gossip_state.peers.get_mut(&peer_addr) {
                    // Update NodeId if provided and not already set
                    if node_id.is_some() && existing_peer.node_id.is_none() {
                        existing_peer.node_id = node_id;
                        debug!(peer = %peer_addr, "updated existing peer with NodeId");
                    } else {
                        debug!(peer = %peer_addr, "peer already tracked");
                    }
                } else {
                    // New peer
                    // Check if we have a dns_name from known_peers (discovered via gossip)
                    // Do this before the entry check to avoid borrow conflicts
                    let dns_name = gossip_state
                        .known_peers
                        .peek(&peer_addr)
                        .and_then(|p| p.dns_name.clone());

                    let current_time = current_timestamp();
                    gossip_state.peers.insert(
                        peer_addr,
                        PeerInfo {
                            address: peer_addr,
                            peer_address: None,
                            inbound_observed: false,
                            outbound_dial_success: false,
                            node_id,
                            dns_name,
                            failures: 0,
                            last_attempt: current_time,
                            last_success: current_time,
                            last_sequence: 0,
                            last_sent_sequence: 0,
                            consecutive_deltas: 0,
                            last_failure_time: None,
                            last_dns_refresh_attempt: None,
                            last_response_received: current_time,
                        },
                    );

                    if let Some(node_id) = node_id {
                        let _ = self
                            .peer_capability_addr_to_node
                            .upsert_sync(peer_addr, node_id);
                        let caps = self.peer_capabilities.read_sync(&peer_addr, |_, v| *v);
                        if let Some(caps) = caps {
                            let _ = self.peer_capabilities_by_node.upsert_sync(node_id, caps);
                        }
                    }
                    debug!(
                        peer = %peer_addr,
                        peers_count = gossip_state.peers.len(),
                        has_node_id = node_id.is_some(),
                        "📌 Added new peer (listening address)"
                    );
                }
            } // Lock dropped

            // Safely update connection pool if we have a NodeId
            // This is critical for TLS connections to work (get_connection_to_peer needs this mapping)
            if let Some(id) = node_id {
                let peer_id = id.to_peer_id();

                let mut conn_to_abort = None;

                // Check if we need to close an existing connection to a different address
                {
                    let pool = &self.connection_pool;
                    if let Some(old_addr) = pool.get_configured_peer_addr(&peer_id) {
                        if old_addr != peer_addr {
                            info!(
                                peer_id = %peer_id,
                                old_addr = %old_addr,
                                new_addr = %peer_addr,
                                "Closing old connection for peer due to address change"
                            );

                            conn_to_abort = pool.disconnect_connection_by_peer_id(&peer_id);
                        }
                    }
                }

                // Abort tasks outside the lock to avoid potential deadlocks
                if let Some(conn) = conn_to_abort {
                    conn.abort_tasks();
                }

                self.configure_peer(peer_id, peer_addr).await;
            }
        } else {
            info!(peer = %peer_addr, "not adding peer - same as self");
        }
    }

    /// Configure a peer by peer ID and its expected connection address
    pub async fn configure_peer(&self, peer_id: crate::PeerId, connect_addr: SocketAddr) {
        let pool = &self.connection_pool;
        info!(peer_id = %peer_id, addr = %connect_addr, "Configured peer");
        pool.set_configured_peer_addr(&peer_id, connect_addr);
        let _ = pool
            .addr_to_peer_id
            .upsert_sync(connect_addr, peer_id.clone());
        pool.reindex_connection_addr(&peer_id, connect_addr);
        if let Some(cell) = self.peer_connect_handler.load_full() {
            cell.handler
                .handle_peer_connect(connect_addr, Some(peer_id))
                .await;
        }
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
    pub async fn set_peer_dns_name(&self, peer_addr: SocketAddr, dns_name: String) {
        let mut gossip_state = self.gossip_state.lock().await;
        if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
            info!(peer = %peer_addr, dns_name = %dns_name, "Set DNS name for peer");
            peer_info.dns_name = Some(dns_name);
        } else {
            warn!(peer = %peer_addr, dns_name = %dns_name, "Peer not found when setting DNS name");
        }
    }

    /// Re-resolve DNS for a peer and update its address if changed.
    /// Returns the new address if resolution succeeded and the IP changed, None otherwise.
    ///
    /// This is called automatically by the gossip retry logic when a peer with
    /// a DNS name fails to connect.
    ///
    /// DNS round-robin handling: If the current address is still in the DNS results,
    /// we keep it to avoid unnecessary churn. Only switch if current IP is not in results.
    pub async fn refresh_peer_dns(&self, peer_addr: SocketAddr) -> Option<SocketAddr> {
        const MIN_REFRESH_INTERVAL_SECS: u64 = 1;

        let (dns_name, should_refresh) = {
            let mut gossip_state = self.gossip_state.lock().await;
            let Some(peer) = gossip_state.peers.get_mut(&peer_addr) else {
                return None;
            };
            let Some(dns_name) = peer.dns_name.clone() else {
                return None;
            };

            let now = crate::current_timestamp();
            let eligible = peer
                .last_dns_refresh_attempt
                .map(|t| now.saturating_sub(t) >= MIN_REFRESH_INTERVAL_SECS)
                .unwrap_or(true);
            if eligible {
                peer.last_dns_refresh_attempt = Some(now);
            }
            (dns_name, eligible)
        };

        if !should_refresh {
            return None;
        }

        // Resolve DNS to get ALL current IPs - collect to Vec to check if current is valid.
        let resolver = { self.dns_resolver.read().await.clone() };
        let resolved_addrs: Vec<SocketAddr> = match resolver.lookup(&dns_name).await {
            Ok(addrs) => addrs,
            Err(e) => {
                warn!(dns_name = %dns_name, error = %e, "Failed to resolve DNS for peer");
                return None;
            }
        };

        if resolved_addrs.is_empty() {
            warn!(dns_name = %dns_name, "DNS resolution returned no addresses");
            return None;
        }

        // DNS round-robin fix: If current address is still in DNS results, keep it
        // This avoids unnecessary churn when DNS returns multiple addresses
        // Compare full SocketAddr (IP + port) to handle port changes
        if resolved_addrs.contains(&peer_addr) {
            debug!(
                addr = %peer_addr,
                dns_name = %dns_name,
                resolved_count = resolved_addrs.len(),
                "DNS re-resolution: current address still valid in DNS results"
            );
            return None;
        }

        // Current IP is NOT in DNS results - switch to the first safe address.
        let Some(new_addr) = resolved_addrs.iter().copied().find(|addr| {
            crate::net_security::is_safe_to_dial(
                addr,
                self.config.allow_private_discovery,
                self.config.allow_loopback_discovery,
                self.config.allow_link_local_discovery,
            )
        }) else {
            warn!(
                dns_name = %dns_name,
                resolved_count = resolved_addrs.len(),
                "DNS re-resolution: all resolved addresses blocked by security filter"
            );
            return None;
        };

        info!(
            old_addr = %peer_addr,
            new_addr = %new_addr,
            dns_name = %dns_name,
            resolved_count = resolved_addrs.len(),
            "🔄 DNS re-resolution: peer IP changed (old IP not in DNS results)"
        );

        // PRE-CHECK: Verify no collisions in connection pool BEFORE any migration
        // This prevents inconsistent state if we migrate gossip_state but pool has collision
        {
            let pool = &self.connection_pool;
            if let Some(existing_peer_id) =
                pool.addr_to_peer_id.read_sync(&new_addr, |_, v| v.clone())
            {
                if let Some(old_peer_id) =
                    pool.addr_to_peer_id.read_sync(&peer_addr, |_, v| v.clone())
                {
                    if existing_peer_id != old_peer_id {
                        warn!(
                            old_addr = %peer_addr,
                            new_addr = %new_addr,
                            dns_name = %dns_name,
                            "DNS refresh: new address already mapped to different peer in pool, aborting"
                        );
                        return None;
                    }
                }
            }
        } // pool lock released

        // PHASE 1: Check for gossip_state collisions and perform migration
        // Release lock before acquiring connection_pool to prevent deadlock
        let migration_result = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Check if the new address is already used by another peer in gossip_state
            if gossip_state.peers.contains_key(&new_addr) {
                warn!(
                    old_addr = %peer_addr,
                    new_addr = %new_addr,
                    dns_name = %dns_name,
                    "DNS re-resolution: new address already in use by another peer, skipping update"
                );
                return None;
            }

            // Try to move the peer entry from old address to new address
            if let Some(mut peer_info) = gossip_state.peers.remove(&peer_addr) {
                peer_info.address = new_addr;
                peer_info.failures = 0; // Reset failures on DNS change
                peer_info.last_failure_time = None;
                gossip_state.peers.insert(new_addr, peer_info.clone());

                // Migrate peer_to_actors mapping if it exists
                if let Some(actors) = gossip_state.peer_to_actors.remove(&peer_addr) {
                    gossip_state.peer_to_actors.insert(new_addr, actors);
                }

                // Migrate peer_health_reports - both as subject and as reporter
                if let Some(reports) = gossip_state.peer_health_reports.remove(&peer_addr) {
                    gossip_state.peer_health_reports.insert(new_addr, reports);
                }
                // Also update any reports about this peer from other reporters
                for (_, reports) in gossip_state.peer_health_reports.iter_mut() {
                    if let Some(status) = reports.remove(&peer_addr) {
                        reports.insert(new_addr, status);
                    }
                }

                // Migrate pending_peer_failures if exists
                if let Some(failure) = gossip_state.pending_peer_failures.remove(&peer_addr) {
                    gossip_state.pending_peer_failures.insert(new_addr, failure);
                }

                // Also update known_peers to avoid stale addresses being re-gossiped
                gossip_state.known_peers.pop(&peer_addr);
                gossip_state.known_peers.put(new_addr, peer_info);

                true // Migration succeeded
            } else {
                // Peer was removed between DNS lookup and update - don't proceed
                debug!(
                    old_addr = %peer_addr,
                    dns_name = %dns_name,
                    "DNS re-resolution: peer was removed, skipping update"
                );
                false
            }
        }; // gossip_state lock released here

        if !migration_result {
            return None;
        }

        // PHASE 2: Update connection pool (separate lock acquisition)
        // Re-check that peer still exists to avoid reintroducing stale entries
        {
            let gossip_state = self.gossip_state.lock().await;
            if !gossip_state.peers.contains_key(&new_addr) {
                debug!(
                    new_addr = %new_addr,
                    dns_name = %dns_name,
                    "DNS refresh: peer was removed mid-refresh, skipping pool/capability update"
                );
                return None;
            }
        } // gossip_state lock released

        {
            let pool = &self.connection_pool;

            // Get peer_id for this address
            if let Some(peer_id) = pool.get_peer_id_by_addr(&peer_addr) {
                // Update peer_id_to_addr mapping
                pool.set_configured_peer_addr(&peer_id, new_addr);
                // Add new address to addr_to_peer_id mapping
                pool.add_addr_to_peer_id(new_addr, peer_id.clone());

                // Migrate connections_by_addr: move connection from old addr to new addr
                // ONLY if the connection is still connected - dead connections should be removed
                if let Some((_, connection)) = pool.connections_by_addr.remove_sync(&peer_addr) {
                    if connection.is_connected() {
                        // Connection is alive - migrate it to new address
                        let _ = pool
                            .connections_by_addr
                            .upsert_sync(new_addr, connection.clone());
                        pool.publish_current_peer_connection(&peer_id, connection);
                        debug!(
                            old_addr = %peer_addr,
                            new_addr = %new_addr,
                            "DNS refresh: migrated live connection from old to new address"
                        );
                    } else {
                        // Connection is dead - remove it from connections_by_peer too
                        // This ensures the next send attempt will trigger reconnection
                        pool.clear_current_peer_connection_if_matches(&peer_id, &connection);
                        info!(
                            old_addr = %peer_addr,
                            new_addr = %new_addr,
                            "DNS refresh: removed dead connection, will establish new connection"
                        );
                    }
                }

                // Clean up old addr_to_peer_id mapping
                let _ = pool.addr_to_peer_id.remove_sync(&peer_addr);
            }
        } // connection_pool lock released here

        // PHASE 3: Migrate capability state (scc-based, lock-free)
        // Re-check peer existence to avoid reintroducing stale entries
        {
            let gossip_state = self.gossip_state.lock().await;
            if !gossip_state.peers.contains_key(&new_addr) {
                debug!(
                    new_addr = %new_addr,
                    dns_name = %dns_name,
                    "DNS refresh: peer was removed mid-refresh, skipping capability migration"
                );
                return None;
            }
        }

        // Migrate peer_capabilities from old address to new address
        if let Some((_, caps)) = self.peer_capabilities.remove_sync(&peer_addr) {
            let _ = self.peer_capabilities.upsert_sync(new_addr, caps);
            debug!(
                old_addr = %peer_addr,
                new_addr = %new_addr,
                "DNS refresh: migrated peer capabilities from old to new address"
            );
        }

        // Migrate peer_capability_addr_to_node
        if let Some((_, node_id)) = self.peer_capability_addr_to_node.remove_sync(&peer_addr) {
            let _ = self
                .peer_capability_addr_to_node
                .upsert_sync(new_addr, node_id);
            debug!(
                old_addr = %peer_addr,
                new_addr = %new_addr,
                "DNS refresh: migrated peer_capability_addr_to_node"
            );
        }

        Some(new_addr)
    }

    /// Connect to a configured peer by peer ID
    pub async fn connect_to_peer(&self, peer_id: &crate::PeerId) -> Result<()> {
        let pool = &self.connection_pool;
        pool.get_connection_to_peer(peer_id).await?;
        info!(peer_id = %peer_id, "Connected to peer");
        Ok(())
    }

    /// Register a local actor (fast path - minimal locking) with vector clock increment
    pub async fn register_actor(&self, name: String, location: RemoteActorLocation) -> Result<()> {
        self.register_actor_with_priority(name, location, RegistrationPriority::Normal)
            .await
    }

    /// Register actor with confirmation from at least one peer
    /// Returns when first peer ACKs or timeout
    pub async fn register_actor_sync(
        &self,
        name: String,
        location: RemoteActorLocation,
        timeout: Duration,
    ) -> Result<()> {
        // Step 1: Check if we have any healthy peers
        let peer_count = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(_, info)| info.failures < self.config.max_peer_failures)
                .count()
        };

        if peer_count == 0 {
            // No peers - just do local registration and return
            self.register_actor_with_priority(name, location, RegistrationPriority::Immediate)
                .await?;

            info!("Sync registration completed immediately (no peers to confirm)");
            return Ok(());
        }

        // Have peers - wait for ACK from at least one.
        let pending = Arc::new(PendingAck::new());
        if self
            .pending_acks
            .insert_sync(name.clone(), pending.clone())
            .is_err()
        {
            return Err(GossipError::Network(io::Error::other(
                "Another synchronous registration is already pending for this actor",
            )));
        }

        // Register with immediate priority (triggers instant gossip to peers).
        if let Err(err) = self
            .register_actor_with_priority(name.clone(), location, RegistrationPriority::Immediate)
            .await
        {
            let _ = self.pending_acks.remove_sync(&name);
            pending.cancel();
            return Err(err);
        }

        // Wait for first ACK or timeout.
        let outcome = tokio::time::timeout(timeout, pending.wait()).await;
        let _ = self.pending_acks.remove_sync(&name);

        match outcome {
            Ok(Some(true)) => {
                info!("Sync registration confirmed by peer for actor '{}'", name);
                Ok(())
            }
            Ok(Some(false)) => {
                warn!(
                    "Sync registration failed according to peer for actor '{}'",
                    name
                );
                Err(GossipError::Network(io::Error::other(
                    "Peer rejected registration",
                )))
            }
            Ok(None) => {
                // Canceled locally (shouldn't usually happen unless shutdown/cleanup raced).
                warn!(
                    "Sync registration canceled while waiting for peer ACK for actor '{}'",
                    name
                );
                Ok(())
            }
            Err(_) => {
                // Timeout - maybe peer is slow or disconnected.
                pending.cancel();
                warn!(
                    "Sync registration timed out waiting for peer ACK for actor '{}', continuing anyway",
                    name
                );
                Ok(()) // Still return Ok - gossip will eventually propagate
            }
        }
    }

    /// Get the current number of actors in the registry (both local and known)
    pub async fn get_actor_count(&self) -> usize {
        self.actor_state.local_actors.len() + self.actor_state.known_actors.len()
    }

    /// Register a local actor with specific priority
    pub async fn register_actor_with_priority(
        &self,
        name: String,
        mut location: RemoteActorLocation,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let register_start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        // Update the location with current wall time and priority
        location.wall_clock_time = current_timestamp();
        location.priority = priority;

        // If peers reflected our own actor back to us via gossip, treat that as stale local state:
        // local registration must be able to (re)assert itself without tripping "already exists".
        let self_node_id = self.peer_id.to_node_id();
        if let Some(loc) = self
            .actor_state
            .known_actors
            .read_sync(name.as_str(), |_, v| v.clone())
        {
            if loc.node_id == self_node_id {
                let _ = self.actor_state.known_actors.remove_sync(name.as_str());
            }
        }

        // Actor map is lock-free (scc). We still enforce "already exists" semantics across
        // local+known with a best-effort rollback on races.
        if self.actor_state.local_actors.contains_sync(name.as_str())
            || self.actor_state.known_actors.contains_sync(name.as_str())
        {
            return Err(GossipError::ActorAlreadyExists(name));
        }

        // Increment vector clock before insertion for atomicity of "this write".
        location.vector_clock.increment(location.node_id);

        if self
            .actor_state
            .local_actors
            .insert_sync(name.clone(), location.clone())
            .is_err()
        {
            return Err(GossipError::ActorAlreadyExists(name));
        }

        // If a remote actor raced in concurrently, roll back and preserve original semantics.
        if self.actor_state.known_actors.contains_sync(name.as_str()) {
            let _ = self.actor_state.local_actors.remove_sync(name.as_str());
            return Err(GossipError::ActorAlreadyExists(name));
        }

        // Update gossip state with pending change - choose queue based on priority
        let should_trigger_immediate = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Check shutdown
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }

            let change = RegistryChange::ActorAdded {
                name: name.clone(),
                location,
                priority,
            };

            if priority.should_trigger_immediate_gossip() {
                gossip_state.urgent_changes.push(change.clone());
                gossip_state
                    .pending_changes
                    .push(Self::as_regular_gossip_change(&change));
                true
            } else {
                gossip_state.pending_changes.push(change);
                false
            }
        };

        if priority.should_trigger_immediate_gossip() {
            let gossip_trigger_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let registration_duration_ms =
                (gossip_trigger_time - register_start_time) as f64 / 1_000_000.0;

            info!(
                actor_name = %name,
                bind_addr = %self.bind_addr,
                priority = ?priority,
                registration_duration_ms = registration_duration_ms,
                "🚀 REGISTERED_ACTOR_IMMEDIATE: Will trigger immediate propagation"
            );
        } else {
            info!(
                actor_name = %name,
                bind_addr = %self.bind_addr,
                priority = ?priority,
                "REGISTERED_ACTOR"
            );
        }

        // Trigger immediate gossip if this was an urgent change
        if should_trigger_immediate {
            if let Err(err) = self.trigger_immediate_gossip().await {
                warn!(error = %err, "failed to trigger immediate gossip");
            }
        }

        Ok(())
    }

    /// Unregister a local actor
    pub async fn unregister_actor(&self, name: &str) -> Result<Option<RemoteActorLocation>> {
        // Remove from actor state
        let removed = self
            .actor_state
            .local_actors
            .remove_sync(name)
            .map(|(_, v)| v);

        // If we learned our own actor via gossip (e.g., peers reflecting state back),
        // clear the known_actors entry too so re-register behaves as expected.
        let self_node_id = self.peer_id.to_node_id();
        if let Some(loc) = self
            .actor_state
            .known_actors
            .read_sync(name, |_, v| v.clone())
        {
            if loc.node_id == self_node_id {
                let _ = self.actor_state.known_actors.remove_sync(name);
            }
        }

        if let Some(ref location) = removed {
            info!(actor_name = %name, "unregistered local actor");

            // Track this change for delta gossip - use the priority from the removed actor
            let should_trigger_immediate = {
                let mut gossip_state = self.gossip_state.lock().await;

                // Check shutdown
                if gossip_state.shutdown {
                    return Err(GossipError::Shutdown);
                }

                // Create a new vector clock for the removal with proper causality
                let removal_clock = location.vector_clock.clone();
                removal_clock.increment(self.peer_id.to_node_id());

                let change = RegistryChange::ActorRemoved {
                    name: name.to_string(),
                    vector_clock: removal_clock,
                    removing_node_id: self.peer_id.to_node_id(),
                    priority: location.priority,
                };

                if location.priority.should_trigger_immediate_gossip() {
                    gossip_state.urgent_changes.push(change.clone());
                    gossip_state
                        .pending_changes
                        .push(Self::as_regular_gossip_change(&change));
                    true
                } else {
                    gossip_state.pending_changes.push(change);
                    false
                }
            };

            // Trigger immediate gossip if this was an urgent change
            if should_trigger_immediate {
                if let Err(err) = self.trigger_immediate_gossip().await {
                    warn!(error = %err, "failed to trigger immediate gossip for actor removal");
                }
            }
        }
        Ok(removed)
    }

    /// Lookup an actor (read-only fast path)
    pub async fn lookup_actor(&self, name: &str) -> Option<RemoteActorLocation> {
        // Check local actors first
        if let Some(location) = self
            .actor_state
            .local_actors
            .read_sync(name, |_, v| v.clone())
        {
            debug!(actor_name = %name, location = "local", "actor found");
            return Some(location);
        }

        // Check known remote actors
        if let Some(location) = self
            .actor_state
            .known_actors
            .read_sync(name, |_, v| v.clone())
        {
            let now = current_timestamp();
            let age_secs = now.saturating_sub(location.wall_clock_time);
            if age_secs < self.config.actor_ttl.as_secs() {
                debug!(
                    actor_name = %name,
                    location = "remote",
                    age_seconds = age_secs,
                    "actor found"
                );
                return Some(location);
            }
        }

        debug!(actor_name = %name, "actor not found");
        None
    }

    /// Get registry statistics
    pub async fn get_stats(&self) -> RegistryStats {
        let local_actors = self.actor_state.local_actors.len();
        let known_actors = self.actor_state.known_actors.len();

        let (
            gossip_sequence,
            active_peers,
            failed_peers,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
            discovered_peers,
            failed_discovery_attempts,
            avg_mesh_connectivity,
            mesh_formation_time_ms,
        ) = {
            let gossip_state = self.gossip_state.lock().await;
            let active_peers = gossip_state
                .peers
                .values()
                .filter(|p| p.failures < self.config.max_peer_failures)
                .count();
            let failed_peers = gossip_state.peers.len() - active_peers;

            // Peer discovery metrics (Phase 5)
            let discovered_peers = gossip_state.known_peers.len();
            let failed_discovery_attempts = gossip_state
                .peer_discovery
                .as_ref()
                .map(|pd| pd.failed_peer_count() as u64)
                .unwrap_or(0);

            // Calculate mesh connectivity: active connections / discovered peers
            let avg_mesh_connectivity = if discovered_peers > 0 {
                active_peers as f64 / discovered_peers as f64
            } else {
                0.0
            };

            (
                gossip_state.gossip_sequence,
                active_peers,
                failed_peers,
                gossip_state.delta_exchanges,
                gossip_state.full_sync_exchanges,
                gossip_state.delta_history.len(),
                discovered_peers,
                failed_discovery_attempts,
                avg_mesh_connectivity,
                gossip_state.mesh_formation_time_ms,
            )
        };

        let current_time = current_timestamp();
        let avg_delta_size = if delta_exchanges > 0 {
            // This is approximate since we don't hold the lock
            delta_history_size as f64
        } else {
            0.0
        };

        RegistryStats {
            local_actors,
            known_actors,
            active_peers,
            failed_peers,
            total_gossip_rounds: gossip_sequence,
            current_sequence: gossip_sequence,
            uptime_seconds: current_time.saturating_sub(self.start_time),
            last_gossip_timestamp: current_time,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
            avg_delta_size,
            discovered_peers,
            failed_discovery_attempts,
            avg_mesh_connectivity,
            mesh_formation_time_ms,
        }
    }

    /// Apply delta changes from a peer
    pub async fn apply_delta(&self, delta: RegistryDelta) -> Result<()> {
        let total_changes = delta.changes.len();
        let sender_peer_id = delta.sender_peer_id.clone();

        // Pre-compute priority flags to avoid redundant checks
        let has_immediate = delta.changes.iter().any(|change| match change {
            RegistryChange::ActorAdded { priority, .. } => {
                priority.should_trigger_immediate_gossip()
            }
            RegistryChange::ActorRemoved { priority, .. } => {
                priority.should_trigger_immediate_gossip()
            }
        });

        if has_immediate {
            error!(
                "🎯 RECEIVING IMMEDIATE CHANGES: {} total changes from {}",
                total_changes, sender_peer_id
            );
        }

        let mut peer_actors_added = std::collections::HashSet::new();
        let mut peer_actors_removed = std::collections::HashSet::new();

        // Pre-capture timing info outside lock for better performance
        let received_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        // Apply changes to lock-free actor maps (scc).
        let applied_count = {
            let mut applied = 0;

            for change in delta.changes {
                match &change {
                    RegistryChange::ActorAdded {
                        name,
                        location,
                        priority: _,
                    } => {
                        // Ignore stale self-announcements coming back from peers. Once we
                        // unregister a local actor, remote nodes may still temporarily think we
                        // host it and echo that state back. Re-accepting it here would resurrect
                        // the actor in our known_actors map and re-advertise it indefinitely.
                        if location.peer_id == self.peer_id {
                            debug!(
                                actor_name = %name,
                                "skipping remote actor update - change references this node as the host"
                            );
                            continue;
                        }

                        // Don't override local actors - early exit
                        if self.actor_state.local_actors.contains_sync(name.as_str()) {
                            debug!(
                                actor_name = %name,
                                "skipping remote actor update - actor is local"
                            );
                            continue;
                        }

                        // Check if we already know about this actor
                        let should_apply = self
                            .actor_state
                            .known_actors
                            .read_sync(name.as_str(), |_, existing_location| {
                                // Use vector clock for causal ordering
                                match location
                                    .vector_clock
                                    .compare(&existing_location.vector_clock)
                                {
                                    crate::ClockOrdering::After => true,
                                    // Important: treat `Equal` as a deterministic tie-break case too.
                                    // Vector clocks can compare equal (notably at initial zero values)
                                    // even when the candidate/existing locations differ. If we keep the
                                    // "first seen" value here, convergence can become order-dependent.
                                    crate::ClockOrdering::Concurrent
                                    | crate::ClockOrdering::Equal => {
                                        stable_concurrent_location_wins(location, existing_location)
                                    }
                                    crate::ClockOrdering::Before => false,
                                }
                            })
                            .unwrap_or_else(|| {
                                debug!(actor_name = %name, "applying new actor");
                                true
                            });

                        if should_apply {
                            // Only clone when actually inserting
                            let actor_name = name.clone();
                            let _ = self
                                .actor_state
                                .known_actors
                                .upsert_sync(actor_name.clone(), location.clone());
                            applied += 1;

                            // Track this actor as belonging to the sender
                            peer_actors_added.insert(actor_name);

                            // Move timing calculations outside critical section for logging
                            if tracing::enabled!(tracing::Level::INFO) {
                                let propagation_time_nanos =
                                    received_timestamp - location.local_registration_time;
                                let propagation_time_ms =
                                    propagation_time_nanos as f64 / 1_000_000.0;

                                // Calculate time from when delta was sent (network + processing)
                                let network_processing_time_nanos =
                                    received_timestamp - delta.precise_timing_nanos as u128;
                                let network_processing_time_ms =
                                    network_processing_time_nanos as f64 / 1_000_000.0;

                                // Calculate pure serialization + processing time (excluding network)
                                let processing_only_time_ms =
                                    propagation_time_ms - network_processing_time_ms;

                                info!(
                                    actor_name = %name,
                                    priority = ?location.priority,
                                    propagation_time_ms = propagation_time_ms,
                                    network_processing_time_ms = network_processing_time_ms,
                                    processing_only_time_ms = processing_only_time_ms,
                                    "RECEIVED_ACTOR"
                                );
                            }
                        }
                    }
                    RegistryChange::ActorRemoved {
                        name,
                        vector_clock,
                        removing_node_id,
                        priority: _,
                    } => {
                        // Don't remove local actors - early exit
                        if self.actor_state.local_actors.contains_sync(name.as_str()) {
                            debug!(
                                actor_name = %name,
                                "skipping actor removal - actor is local"
                            );
                            continue;
                        }

                        // Check vector clock ordering before applying removal
                        let should_remove = self
                            .actor_state
                            .known_actors
                            .read_sync(name.as_str(), |_, existing_location| {
                                // Use vector clock for causal ordering
                                match vector_clock.compare(&existing_location.vector_clock) {
                                    crate::ClockOrdering::After => {
                                        // Removal is causally after current state
                                        debug!(
                                            actor_name = %name,
                                            "removal is causally after current state - applying"
                                        );
                                        true
                                    }
                                    crate::ClockOrdering::Concurrent => {
                                        let should_apply = stable_concurrent_removal_wins(
                                            removing_node_id,
                                            vector_clock,
                                            existing_location,
                                        );
                                        debug!(
                                            actor_name = %name,
                                            removing_node = %removing_node_id.fmt_short(),
                                            existing_node = %existing_location.node_id.fmt_short(),
                                            should_apply = should_apply,
                                            "removal is concurrent with current state - using node_id tiebreaker"
                                        );
                                        should_apply
                                    }
                                    _ => {
                                        // Removal is outdated (Before or Equal) - ignore it
                                        debug!(
                                            actor_name = %name,
                                            "removal is outdated - ignoring"
                                        );
                                        false
                                    }
                                }
                            })
                            .unwrap_or_else(|| {
                                // Actor doesn't exist, nothing to remove
                                debug!(actor_name = %name, "actor not found - ignoring removal");
                                false
                            });

                        if should_remove
                            && self
                                .actor_state
                                .known_actors
                                .remove_sync(name.as_str())
                                .is_some()
                        {
                            applied += 1;
                            peer_actors_removed.insert(name.clone());
                            debug!(actor_name = %name, "applied actor removal");
                        }
                    }
                }
            }

            applied
        };

        let peer_actor_changes = peer_actors_added.len() + peer_actors_removed.len();

        if let Some(sender_addr) = {
            let pool = &self.connection_pool;
            pool.get_configured_peer_addr(&sender_peer_id)
        } {
            let mut gossip_state = self.gossip_state.lock().await;
            let entry = gossip_state
                .peer_to_actors
                .entry(sender_addr)
                .or_insert_with(std::collections::HashSet::new);

            for name in peer_actors_removed {
                entry.remove(&name);
            }
            for name in peer_actors_added {
                entry.insert(name);
            }
        } else {
            debug!(
                sender = %sender_peer_id,
                "no address mapping for sender; skipping peer_to_actors update"
            );
        }

        debug!(
            sender = %sender_peer_id,
            total_changes,
            applied_changes = applied_count,
            peer_actor_changes = peer_actor_changes,
            "completed delta application with vector clock conflict resolution"
        );

        Ok(())
    }

    /// Determine whether to use delta or full sync for a peer
    fn should_use_delta_state(&self, gossip_state: &GossipState, peer_info: &PeerInfo) -> bool {
        // Prefer full sync for brand new peers unless we already have committed state changes that
        // must be delivered (e.g. removals). In that case, we can safely bootstrap them with a delta
        // starting from sequence zero because create_delta_from_state includes the entire registry
        // snapshot when since_sequence == 0.
        if peer_info.last_sequence == 0 {
            if gossip_state.gossip_sequence == 0 {
                return false;
            }
            debug!(
                peer = %peer_info.address,
                committed_sequence = gossip_state.gossip_sequence,
                "peer has no recorded sequence but committed changes exist; using delta bootstrap"
            );
        }

        // For small clusters (≤ threshold total nodes) we normally prefer full sync
        // for robustness, but if we have committed changes the peer hasn't seen yet
        // we must still send deltas so removals/updates propagate promptly.
        let healthy_peers = gossip_state
            .peers
            .values()
            .filter(|p| p.failures < self.config.max_peer_failures)
            .count();
        let total_healthy_nodes = healthy_peers + 1;
        if total_healthy_nodes <= self.config.small_cluster_threshold {
            if gossip_state.gossip_sequence <= peer_info.last_sequence {
                debug!(
                    total_healthy_nodes,
                    peer_last_sequence = peer_info.last_sequence,
                    current_sequence = gossip_state.gossip_sequence,
                    "using full sync for small cluster with no new changes"
                );
                return false;
            }

            debug!(
                total_healthy_nodes,
                peer_last_sequence = peer_info.last_sequence,
                current_sequence = gossip_state.gossip_sequence,
                "small cluster override: pending changes exist, allowing delta"
            );
        }

        // Force full sync periodically
        if peer_info.consecutive_deltas >= self.config.full_sync_interval {
            return false;
        }

        // Check if we have the required delta history.
        // For peers that have never seen a delta (last_sequence == 0), we can still bootstrap them
        // because create_delta_from_state includes the full snapshot when since_sequence == 0.
        let oldest_available = gossip_state
            .delta_history
            .first()
            .map(|d| d.sequence)
            .unwrap_or(gossip_state.gossip_sequence);

        if peer_info.last_sequence > 0 && peer_info.last_sequence < oldest_available {
            debug!(
                peer_last_sequence = peer_info.last_sequence,
                oldest_available,
                "peer is too far behind for available delta history; using full sync"
            );
            return false;
        }

        true
    }

    /// Create a delta containing changes since the specified sequence
    async fn create_delta_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        since_sequence: u64,
    ) -> Result<RegistryDelta> {
        let estimated_size =
            local_actors.len() + known_actors.len() + gossip_state.pending_changes.len();
        let mut changes = Vec::with_capacity(estimated_size);
        let current_time = current_timestamp();

        // If this is a brand new peer (since_sequence = 0), include all actors we know about
        if since_sequence == 0 {
            // Include all local actors as additions
            let mut local_names: Vec<&String> = local_actors.keys().collect();
            local_names.sort();
            for name in local_names {
                let location = &local_actors[name];
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    priority: RegistrationPriority::Normal,
                });
            }

            // Include all known remote actors as additions
            let mut known_names: Vec<&String> = known_actors.keys().collect();
            known_names.sort();
            for name in known_names {
                let location = &known_actors[name];
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    priority: RegistrationPriority::Normal,
                });
            }
        }

        // Include urgent changes first (they have higher priority)
        changes.extend(gossip_state.urgent_changes.clone());

        // Include pending changes from current round
        changes.extend(gossip_state.pending_changes.clone());

        // Include historical changes since the requested sequence
        for delta in &gossip_state.delta_history {
            if delta.sequence > since_sequence {
                changes.extend(delta.changes.clone());
            }
        }

        // Deduplicate changes to send only the most recent change for each actor
        let deduped_changes = Self::deduplicate_changes(changes);

        Ok(RegistryDelta {
            since_sequence,
            current_sequence: gossip_state.gossip_sequence,
            changes: deduped_changes,
            sender_peer_id: self.peer_id.clone(),
            wall_clock_time: current_time,
            precise_timing_nanos: crate::current_timestamp_nanos(), // Set high precision timing
        })
    }

    /// Create a full sync message from state
    async fn create_full_sync_message_from_state(
        &self,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        sequence: u64,
    ) -> RegistryMessage {
        debug!(
            "Creating full sync message: {} local actors, {} known actors",
            local_actors.len(),
            known_actors.len()
        );
        // Stable ordering: protocol-visible iteration must not depend on hash iteration order.
        let mut local_pairs: Vec<(String, RemoteActorLocation)> = local_actors
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        local_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let mut known_pairs: Vec<(String, RemoteActorLocation)> = known_actors
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        known_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        RegistryMessage::FullSync {
            local_actors: local_pairs,
            known_actors: known_pairs,
            sender_peer_id: self.peer_id.clone(), // Use peer ID
            sender_bind_addr: Some(self.bind_addr.to_string()), // Use our listening address, not ephemeral port
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a full sync response message from state
    pub async fn create_full_sync_response_from_state(
        &self,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        sequence: u64,
    ) -> RegistryMessage {
        // Stable ordering: protocol-visible iteration must not depend on hash iteration order.
        let mut local_pairs: Vec<(String, RemoteActorLocation)> = local_actors
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        local_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let mut known_pairs: Vec<(String, RemoteActorLocation)> = known_actors
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        known_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        RegistryMessage::FullSyncResponse {
            local_actors: local_pairs,
            known_actors: known_pairs,
            sender_peer_id: self.peer_id.clone(), // Use peer ID
            sender_bind_addr: Some(self.bind_addr.to_string()), // Use our listening address, not ephemeral port
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a delta response for incoming gossip
    pub async fn create_delta_response_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        since_sequence: u64,
    ) -> Result<RegistryMessage> {
        let delta = self
            .create_delta_from_state(gossip_state, local_actors, known_actors, since_sequence)
            .await?;
        Ok(RegistryMessage::DeltaGossipResponse { delta })
    }

    fn snapshot_actor_maps(
        &self,
    ) -> (
        HashMap<String, RemoteActorLocation>,
        HashMap<String, RemoteActorLocation>,
    ) {
        let mut local = HashMap::with_capacity(self.actor_state.local_actors.len());
        self.actor_state.local_actors.iter_sync(|k, v| {
            local.insert(k.clone(), v.clone());
            true
        });

        let mut known = HashMap::with_capacity(self.actor_state.known_actors.len());
        self.actor_state.known_actors.iter_sync(|k, v| {
            known.insert(k.clone(), v.clone());
            true
        });

        (local, known)
    }

    pub(crate) fn snapshot_actor_pairs(
        &self,
    ) -> (
        Vec<(String, RemoteActorLocation)>,
        Vec<(String, RemoteActorLocation)>,
    ) {
        let mut local = Vec::with_capacity(self.actor_state.local_actors.len());
        self.actor_state.local_actors.iter_sync(|k, v| {
            local.push((k.clone(), v.clone()));
            true
        });

        let mut known = Vec::with_capacity(self.actor_state.known_actors.len());
        self.actor_state.known_actors.iter_sync(|k, v| {
            known.push((k.clone(), v.clone()));
            true
        });

        (local, known)
    }

    #[inline]
    fn peer_has_live_connection(&self, peer: &PeerInfo) -> bool {
        if self.connection_pool.has_connection(&peer.address) {
            return true;
        }
        if let Some(peer_addr) = peer.peer_address
            && self.connection_pool.has_connection(&peer_addr)
        {
            return true;
        }
        if let Some(node_id) = peer.node_id {
            let peer_id = node_id.to_peer_id();
            return self.connection_pool.has_connection_by_peer_id(&peer_id);
        }
        false
    }

    #[inline]
    fn is_practically_dialable_from_here(&self, peer_addr: SocketAddr) -> bool {
        if peer_addr.port() == 0 {
            return false;
        }

        let local_ip = self.bind_addr.ip();
        let peer_ip = peer_addr.ip();
        if peer_ip.is_unspecified() || peer_ip.is_multicast() {
            return false;
        }

        match (local_ip, peer_ip) {
            (IpAddr::V4(local), IpAddr::V4(peer)) => {
                if peer.is_loopback() {
                    return local.is_loopback();
                }
                if peer.is_link_local() {
                    return local.is_link_local();
                }
                if peer.is_private() {
                    return local.is_private();
                }
                true
            }
            (IpAddr::V6(local), IpAddr::V6(peer)) => {
                if peer.is_loopback() {
                    return local.is_loopback();
                }
                if peer.is_unicast_link_local() {
                    return local.is_unicast_link_local();
                }
                if peer.is_unique_local() {
                    return local.is_unique_local();
                }
                true
            }
            _ => false,
        }
    }

    #[inline]
    fn should_suppress_outbound_retry_for_peer(&self, peer: &PeerInfo) -> bool {
        if !self.config.nat_role_reconnect_enabled {
            return false;
        }
        if peer.outbound_dial_success || !peer.inbound_observed {
            return false;
        }
        if self.peer_has_live_connection(peer) {
            return false;
        }
        !self.is_practically_dialable_from_here(peer.address)
    }

    /// Snapshot the currently-known actor directory as owned `(name, location)` pairs.
    ///
    /// Semantics (deterministic):
    /// - Includes both local and remote-known actors.
    /// - If the same name exists in both maps, the local entry wins.
    /// - Returned vector is sorted by name for stable debugging/tests.
    pub fn snapshot_known_actors(&self) -> Vec<(String, RemoteActorLocation)> {
        let (local, known) = self.snapshot_actor_pairs();

        let mut merged: HashMap<String, RemoteActorLocation> =
            HashMap::with_capacity(local.len() + known.len());
        for (name, loc) in known {
            merged.insert(name, loc);
        }
        for (name, loc) in local {
            merged.insert(name, loc);
        }

        let mut out: Vec<(String, RemoteActorLocation)> = merged.into_iter().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Prepare gossip round with consistent lock ordering to prevent deadlocks
    pub async fn prepare_gossip_round(&self) -> Result<Vec<GossipTask>> {
        debug!("Starting gossip round");

        // Step 1: Check shutdown status first
        {
            let gossip_state = self.gossip_state.lock().await;
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }
        }

        // Step 2: Atomically commit pending changes
        let (current_sequence, has_peers) = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Double-check shutdown after acquiring locks
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }

            // Check if we have changes to commit. We need to consider
            // BOTH pending_changes and urgent_changes here: callers
            // such as `handle_peer_death` from `apply_gossip_results`
            // push into `urgent_changes` without a follow-up
            // `trigger_immediate_gossip` (the gossip-results path is
            // already inside a gossip round and re-entering would race
            // failure accounting). Without urgent in this check + drain,
            // those entries linger in `urgent_changes` and get
            // re-broadcast on every periodic tick via
            // `create_delta_from_state`'s clone, instead of being a
            // one-shot urgent fan-out.
            let had_changes = !gossip_state.pending_changes.is_empty()
                || !gossip_state.urgent_changes.is_empty();

            // Increment sequence if we have changes
            if had_changes {
                // Increment sequence number
                gossip_state.gossip_sequence += 1;

                // Commit pending + urgent changes to history together.
                // Urgent entries are folded into the same `HistoricalDelta`
                // so subsequent rounds pull them from `delta_history` (the
                // since_sequence-bounded path in `create_delta_from_state`)
                // rather than re-cloning the urgent queue forever.
                let mut combined = std::mem::take(&mut gossip_state.pending_changes);
                combined.append(&mut gossip_state.urgent_changes);
                let delta = HistoricalDelta {
                    sequence: gossip_state.gossip_sequence,
                    changes: combined,
                    wall_clock_time: current_timestamp(),
                };

                gossip_state.delta_history.push(delta);

                // Trim history if needed
                if gossip_state.delta_history.len() > self.config.max_delta_history {
                    gossip_state.delta_history.remove(0);
                }
            }

            (gossip_state.gossip_sequence, !gossip_state.peers.is_empty())
            // Lock is automatically released here
        };

        if !has_peers {
            return Ok(Vec::new());
        }

        // Get actor state snapshot for message creation.
        let (local_actors, known_actors) = self.snapshot_actor_maps();

        // Step 3: Select peers and create messages
        let tasks = {
            let gossip_state = self.gossip_state.lock().await;

            // Debug log all peers and their states
            debug!(
                "🔍 Gossip round: examining {} total peers",
                gossip_state.peers.len()
            );
            let current_time = current_timestamp();
            for (addr, peer_info) in &gossip_state.peers {
                let time_since_last_attempt = current_time.saturating_sub(peer_info.last_attempt);
                let retry_eligible = peer_info.failures >= self.config.max_peer_failures
                    && time_since_last_attempt > self.config.peer_retry_interval.as_secs();
                let suppress_outbound = self.should_suppress_outbound_retry_for_peer(peer_info);
                debug!(
                    peer = %addr,
                    failures = peer_info.failures,
                    last_attempt = peer_info.last_attempt,
                    time_since_last_attempt = time_since_last_attempt,
                    retry_interval = self.config.peer_retry_interval.as_secs(),
                    retry_eligible = retry_eligible,
                    max_failures = self.config.max_peer_failures,
                    inbound_observed = peer_info.inbound_observed,
                    outbound_dial_success = peer_info.outbound_dial_success,
                    suppress_outbound = suppress_outbound,
                    "📊 Peer state in gossip round"
                );
            }

            let available_peers: Vec<SocketAddr> = gossip_state
                .peers
                .iter()
                .filter_map(|(peer_addr, peer)| {
                    let retry_window_open = peer.failures < self.config.max_peer_failures
                        || (current_time.saturating_sub(peer.last_attempt))
                            > self.config.peer_retry_interval.as_secs();
                    if !retry_window_open {
                        return None;
                    }
                    if self.should_suppress_outbound_retry_for_peer(peer) {
                        debug!(
                            peer = %peer_addr,
                            inbound_observed = peer.inbound_observed,
                            outbound_dial_success = peer.outbound_dial_success,
                            peer_addr_key = %peer.address,
                            "Suppressing outbound retry for inbound-only undialable peer"
                        );
                        return None;
                    }
                    Some(*peer_addr)
                })
                .collect();

            if available_peers.is_empty() {
                info!(
                    total_peers = gossip_state.peers.len(),
                    max_failures = self.config.max_peer_failures,
                    "❌ No available peers for gossip round"
                );
                return Ok(Vec::new());
            }

            debug!(
                available_count = available_peers.len(),
                "✅ Found {} available peers for gossip",
                available_peers.len()
            );

            // Select peers using adaptive fanout
            let adaptive_fanout = std::cmp::min(
                std::cmp::max(3, (available_peers.len() as f64).log2().ceil() as usize),
                self.config.max_gossip_peers,
            );

            let selected_peers: Vec<SocketAddr> = {
                let mut rng = rand::rng();
                let mut peers = available_peers;
                peers.shuffle(&mut rng);
                peers.into_iter().take(adaptive_fanout).collect()
            };

            debug!(
                selected_count = selected_peers.len(),
                selected_peers = ?selected_peers,
                "📮 Selected {} peers for gossip",
                selected_peers.len()
            );

            // Log if we're retrying any failed peers
            for peer in &selected_peers {
                if let Some(peer_info) = gossip_state.peers.get(peer) {
                    if peer_info.failures >= self.config.max_peer_failures {
                        let time_since_failure = peer_info
                            .last_failure_time
                            .map(|t| current_time - t)
                            .unwrap_or(0);
                        let time_since_last_attempt = current_time - peer_info.last_attempt;
                        info!(
                            peer = %peer,
                            failures = peer_info.failures,
                            time_since_failure_secs = time_since_failure,
                            time_since_last_attempt_secs = time_since_last_attempt,
                            retry_interval_secs = self.config.peer_retry_interval.as_secs(),
                            "🔄 GOSSIP RETRY: Including previously failed peer in gossip round"
                        );
                    }
                }
            }

            let mut tasks = Vec::new();
            let mut full_sync_message: Option<RegistryMessage> = None;
            let mut delta_messages: HashMap<u64, RegistryMessage> = HashMap::new();
            for peer_addr in selected_peers {
                let peer_info = gossip_state
                    .peers
                    .get(&peer_addr)
                    .cloned()
                    .unwrap_or(PeerInfo {
                        address: peer_addr,
                        peer_address: None,
                        inbound_observed: false,
                        outbound_dial_success: false,
                        node_id: None,
                        dns_name: None,
                        failures: 0,
                        last_attempt: 0,
                        last_success: 0,
                        last_sequence: 0,
                        last_sent_sequence: 0,
                        consecutive_deltas: 0,
                        last_failure_time: None,
                        last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                    });

                let use_delta = self.should_use_delta_state(&gossip_state, &peer_info);

                let message = if use_delta {
                    if let Some(message) = delta_messages.get(&peer_info.last_sequence) {
                        message.clone()
                    } else {
                        match self
                            .create_delta_from_state(
                                &gossip_state,
                                &local_actors,
                                &known_actors,
                                peer_info.last_sequence,
                            )
                            .await
                        {
                            Ok(delta) => {
                                let message = RegistryMessage::DeltaGossip { delta };
                                delta_messages.insert(peer_info.last_sequence, message.clone());
                                message
                            }
                            Err(err) => {
                                debug!(
                                    peer = %peer_addr,
                                    error = %err,
                                    "failed to create delta, falling back to full sync"
                                );
                                if full_sync_message.is_none() {
                                    full_sync_message = Some(
                                        self.create_full_sync_message_from_state(
                                            &local_actors,
                                            &known_actors,
                                            current_sequence,
                                        )
                                        .await,
                                    );
                                }
                                full_sync_message
                                    .as_ref()
                                    .expect("full sync message initialized")
                                    .clone()
                            }
                        }
                    }
                } else {
                    if full_sync_message.is_none() {
                        full_sync_message = Some(
                            self.create_full_sync_message_from_state(
                                &local_actors,
                                &known_actors,
                                current_sequence,
                            )
                            .await,
                        );
                    }
                    full_sync_message
                        .as_ref()
                        .expect("full sync message initialized")
                        .clone()
                };

                match &message {
                    RegistryMessage::DeltaGossip { .. } => {
                        debug!(
                            peer = %peer_addr,
                            current_sequence,
                            peer_last_sequence = peer_info.last_sequence,
                            "📤 sending delta gossip"
                        );
                    }
                    RegistryMessage::FullSync { .. } => {
                        debug!(
                            peer = %peer_addr,
                            current_sequence,
                            peer_last_sequence = peer_info.last_sequence,
                            "📤 sending full sync"
                        );
                    }
                    _ => {}
                }

                tasks.push(GossipTask {
                    peer_addr,
                    message,
                    current_sequence,
                });
            }

            tasks
        };

        debug!(
            task_count = tasks.len(),
            current_sequence = current_sequence,
            "prepared gossip round with atomic sequence/vector clock increment"
        );

        Ok(tasks)
    }

    /// Apply results from gossip tasks
    pub async fn apply_gossip_results(&self, results: Vec<GossipResult>) {
        let current_time = current_timestamp();
        let liveness_window_secs = self.config.peer_liveness_window.as_secs();

        // Collect peers that crossed the death threshold in this batch; we
        // fire `handle_peer_death` after dropping the `gossip_state` lock
        // to avoid lock-ordering issues (it acquires the same lock to
        // reset `last_sequence`).
        let mut newly_dead: Vec<SocketAddr> = Vec::new();

        for result in results {
            match result.outcome {
                Ok(response_opt) => {
                    let mut crossed_threshold = false;
                    {
                        let mut gossip_state = self.gossip_state.lock().await;
                        if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                            peer_info.last_attempt = current_time;
                            peer_info.last_sent_sequence = result.sent_sequence;

                            // Only update last_success if we're not in a failed state.
                            // Note: with persistent connections, `Ok(_)` doesn't prove
                            // the peer is alive — only that our kernel buffer accepted
                            // the bytes. Response-asymmetry detection below catches
                            // the half-open / paused-peer case.
                            if peer_info.failures < self.config.max_peer_failures {
                                peer_info.last_success = current_time;
                            }

                            // Response-asymmetry liveness check (Part 3b in the
                            // gossip-protocol-native cleanup plan):
                            //
                            // If we haven't received any inbound payload from
                            // this peer (delta response, full sync, etc.) for
                            // `peer_liveness_window` seconds, treat the next
                            // no-response round as a soft failure. This catches
                            // "outbound writes succeed at the kernel level but
                            // the peer isn't reading anymore" — the scenario
                            // that kept `538a99…` alive on `stratum-devnet-a`
                            // for 66 minutes.
                            //
                            // `last_response_received` is initialised to the
                            // peer's creation time, so a brand-new peer doesn't
                            // immediately look stale; it has at least one
                            // `peer_liveness_window` to be observed responding.
                            let silence_secs = current_time
                                .saturating_sub(peer_info.last_response_received);
                            if response_opt.is_none()
                                && silence_secs > liveness_window_secs
                                && peer_info.failures < self.config.max_peer_failures
                            {
                                peer_info.failures += 1;
                                info!(
                                    peer = %result.peer_addr,
                                    silence_secs,
                                    new_failures = peer_info.failures,
                                    "no response within peer_liveness_window; \
                                     incrementing failures"
                                );
                                if peer_info.failures == self.config.max_peer_failures {
                                    peer_info.last_failure_time = Some(current_time);
                                    crossed_threshold = true;
                                    info!(peer = %result.peer_addr,
                                          "peer reached max failures \
                                           (response-asymmetry)");
                                }
                            }
                        }
                    }

                    if crossed_threshold {
                        newly_dead.push(result.peer_addr);
                    }

                    // Record that this peer is active (we successfully sent to it).
                    self.record_peer_activity(result.peer_addr).await;

                    // Process response if we got one. Note: receiving a response
                    // updates `last_response_received` via the call below into
                    // `handle_gossip_response` → `mark_response_received`, which
                    // also resets `failures` to 0 via `record_peer_activity`.
                    if let Some(response) = response_opt {
                        self.mark_response_received(result.peer_addr, current_time)
                            .await;
                        if let Err(err) = self
                            .handle_gossip_response(result.peer_addr, response)
                            .await
                        {
                            warn!(peer = %result.peer_addr, error = %err, "failed to handle gossip response");
                        }
                    }
                }
                Err(err) => {
                    // Failure case
                    let hard_socket_err = is_hard_socket_error(&err);
                    warn!(peer = %result.peer_addr, error = %err,
                          hard_socket_err,
                          "failed to gossip to peer");
                    let mut crossed_threshold = false;
                    {
                        let mut gossip_state = self.gossip_state.lock().await;
                        if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                            // Hard socket termination (BrokenPipe, ConnectionReset,
                            // ConnectionAborted, NotConnected, RefusedConnection) is
                            // unambiguous evidence the peer is gone — jump straight
                            // to threshold instead of waiting for `max_peer_failures`
                            // separate gossip rounds.
                            //
                            // For other errors (Timeout, DecodingError, etc.) keep
                            // the existing one-failure-at-a-time accumulation, so a
                            // transient blip doesn't immediately evict a peer.
                            if peer_info.failures < self.config.max_peer_failures {
                                let increment = if hard_socket_err {
                                    self.config.max_peer_failures - peer_info.failures
                                } else {
                                    1
                                };
                                peer_info.failures += increment;
                                info!(peer = %result.peer_addr,
                                      new_failures = peer_info.failures,
                                      max_failures = self.config.max_peer_failures,
                                      hard_socket_err,
                                      "incremented peer failure count");

                                // Mark failure time if this puts us at max failures
                                if peer_info.failures >= self.config.max_peer_failures {
                                    peer_info.last_failure_time = Some(current_time);
                                    crossed_threshold = true;
                                    info!(peer = %result.peer_addr,
                                          hard_socket_err,
                                          "peer reached max failures");
                                }
                            } else {
                                // Already at max failures, just update attempt time
                                debug!(peer = %result.peer_addr,
                                       failures = peer_info.failures,
                                       "peer already at max failures, not incrementing");
                            }
                            peer_info.last_attempt = current_time;
                        }
                    }
                    if crossed_threshold {
                        newly_dead.push(result.peer_addr);
                    }
                }
            }
        }

        // Fire the dead-peer cleanup hook outside the gossip_state lock.
        // De-duplicate in case the same addr crossed threshold via both
        // soft and hard paths in one batch.
        newly_dead.sort_by_key(|a| (a.ip(), a.port()));
        newly_dead.dedup();
        for addr in newly_dead {
            self.handle_peer_death(addr).await;
        }
    }

    /// Record that we received a response payload from a peer. Updates
    /// `last_response_received` so the response-asymmetry detector in
    /// `apply_gossip_results` knows the peer is alive at the application
    /// layer, not just the kernel-buffer-accepted layer.
    async fn mark_response_received(&self, peer_addr: SocketAddr, now: u64) {
        let mut gossip_state = self.gossip_state.lock().await;
        if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
            peer_info.last_response_received = now;
        }
    }

    /// Cleanup hook fired when a peer's `failures` crosses
    /// `max_peer_failures` for the first time (gossip protocol's verdict
    /// that the peer is dead). Prunes the dead peer's `known_actors`
    /// entries, force-disconnects its pool connection so it must
    /// re-handshake on return, and resets its sequence counter so the
    /// recovered peer's next FullSync is accepted instead of dropped as
    /// a duplicate.
    ///
    /// Re-admission on recovery is automatic via existing paths:
    /// `record_peer_activity` zeroes `failures` on inbound activity, and
    /// the next FullSync repopulates `known_actors` through the existing
    /// `merge_full_sync` / `apply_delta` paths.
    async fn handle_peer_death(&self, dead_peer_addr: SocketAddr) {
        // Two ways to learn this peer's PeerId:
        //   1. The pool's addr→peer_id map (populated after a successful
        //      connection handshake).
        //   2. The gossip state's `node_id` field (set when we added the
        //      peer, even before any handshake completes).
        // Try both because a peer that died before/during initial
        // connect may only be in the gossip-state map, while a peer that
        // dropped mid-flight may have been evicted from the pool already.
        let pool_lookup = self.connection_pool.get_peer_id_by_addr(&dead_peer_addr);
        let dead_peer_id = if pool_lookup.is_some() {
            pool_lookup
        } else {
            let state = self.gossip_state.lock().await;
            state
                .peers
                .get(&dead_peer_addr)
                .and_then(|p| p.node_id.as_ref().map(|n| n.to_peer_id()))
        };
        let Some(dead_peer_id) = dead_peer_id else {
            // No PeerId resolvable for this address — nothing in
            // `known_actors` is keyed against it. Still reset sequence
            // for cleanliness.
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&dead_peer_addr) {
                peer_info.last_sequence = 0;
                peer_info.last_response_received = 0;
            }
            return;
        };

        // 1. Prune known_actors owned by this peer, capturing each
        //    pruned location so we can fan an ActorRemoved gossip
        //    change out to the rest of the cluster (C3 — without
        //    this, indirect peers keep routing to the dead peer's
        //    actors until their own gossip rounds time out).
        let mut to_remove: Vec<(String, RemoteActorLocation)> = Vec::new();
        self.actor_state.known_actors.iter_sync(|name, location| {
            if location.peer_id == dead_peer_id {
                to_remove.push((name.clone(), location.clone()));
            }
            true
        });
        for (name, _) in &to_remove {
            let _ = self.actor_state.known_actors.remove_sync(name.as_str());
        }

        // 2. Force-disconnect the pool entry so the peer must re-handshake
        //    when it returns. Prevents writes into a stale half-open
        //    connection in the window between this prune and the pool's
        //    own `cleanup_stale_connections` pass.
        let _ = self
            .connection_pool
            .disconnect_connection_by_peer_id(&dead_peer_id);

        // 3. Reset per-peer sequence + last_response_received so a
        //    recovering peer's next FullSync is accepted (mirrors the
        //    DNS-change reset path at registry.rs ~1980), and enqueue
        //    an Immediate-priority ActorRemoved for every pruned actor
        //    so the next gossip round propagates the death cluster-wide.
        let mut have_urgent = false;
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&dead_peer_addr) {
                peer_info.last_sequence = 0;
                peer_info.last_response_received = 0;
            }
            let removing_node_id = self.peer_id.to_node_id();
            for (name, location) in &to_remove {
                let removal_clock = location.vector_clock.clone();
                removal_clock.increment(removing_node_id);
                let change = RegistryChange::ActorRemoved {
                    name: name.clone(),
                    vector_clock: removal_clock,
                    removing_node_id,
                    priority: RegistrationPriority::Immediate,
                };
                gossip_state.urgent_changes.push(change.clone());
                gossip_state.pending_changes.push(change);
                have_urgent = true;
            }
        }

        // 4. The urgent_changes queue will be drained by the next
        //    gossip round (periodic or explicit). We deliberately do
        //    NOT call `trigger_immediate_gossip` here because some
        //    callers of `handle_peer_death` (notably
        //    `apply_gossip_results`) are themselves inside the gossip
        //    loop and an immediate re-entry would race with the
        //    failure accounting they just performed. The queued
        //    Immediate-priority changes will fan out on the next
        //    tick, which is still within one `gossip_interval` —
        //    several orders of magnitude faster than the peer
        //    timeout window we used to depend on.
        let _ = have_urgent;

        info!(
            peer_id = %dead_peer_id,
            peer_addr = %dead_peer_addr,
            pruned_actors = to_remove.len(),
            "gossip declared peer dead — pruned known_actors, disconnected pool, \
             enqueued ActorRemoved gossip"
        );
    }

    /// Handle gossip response with vector clock updates
    pub async fn handle_gossip_response(
        &self,
        addr: SocketAddr,
        response: RegistryMessage,
    ) -> Result<()> {
        match response {
            RegistryMessage::DeltaGossipResponse { delta } => {
                info!(
                    peer = %addr,
                    sender = %delta.sender_peer_id,
                    changes = delta.changes.len(),
                    "📥 GOSSIP: Received delta gossip response"
                );

                let delta_sequence = delta.current_sequence;

                self.apply_delta(delta).await?;
                // Don't add peer here - peers are managed through handle_connection

                let now = current_timestamp();
                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    peer_info.last_sequence = delta_sequence;
                    peer_info.consecutive_deltas += 1;
                    // Inbound payload from peer — proves app-level liveness.
                    // Used by the response-asymmetry detector in
                    // `apply_gossip_results`.
                    peer_info.last_response_received = now;
                }
                gossip_state.delta_exchanges += 1;
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_peer_id,
                sender_bind_addr,
                sequence,
                wall_clock_time,
            } => {
                // Use resolve_peer_addr for safe address resolution with validation
                let sender_socket_addr = resolve_peer_addr(sender_bind_addr.as_deref(), addr);

                info!(
                    tcp_source = %addr,
                    bind_addr = %sender_socket_addr,
                    sender = %sender_peer_id,
                    sequence = sequence,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "📥 GOSSIP: Received full sync response (using bind_addr, not TCP source)"
                );

                // Use the peer's BIND address (not ephemeral TCP source port)
                self.merge_full_sync(
                    local_actors.into_iter().collect(),
                    known_actors.into_iter().collect(),
                    sender_peer_id,
                    sender_socket_addr,
                    sequence,
                    wall_clock_time,
                )
                .await;

                let now = current_timestamp();
                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                    peer_info.consecutive_deltas = 0;
                    peer_info.last_sequence = sequence;
                    // Inbound payload from peer — proves app-level liveness.
                    peer_info.last_response_received = now;
                }
                gossip_state.full_sync_exchanges += 1;
            }
            _ => {
                warn!(peer = %addr, "received unexpected message type in response");
            }
        }

        Ok(())
    }

    /// Merge incoming full sync data with vector clock-based conflict resolution
    pub async fn merge_full_sync(
        &self,
        remote_local: HashMap<String, RemoteActorLocation>,
        remote_known: HashMap<String, RemoteActorLocation>,
        sender_peer_id: crate::PeerId,
        sender_addr: SocketAddr,
        sequence: u64,
        _wall_clock_time: u64,
    ) {
        // Don't add peer here - peers are managed through handle_connection

        // Record comprehensive node activity

        // Check if we've already processed this or a newer sequence from this peer
        {
            let gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get(&sender_addr) {
                if sequence < peer_info.last_sequence {
                    debug!(
                        last_sequence = peer_info.last_sequence,
                        received_sequence = sequence,
                        "ignoring old gossip message"
                    );
                    return;
                }
            }
        }

        // Update peer sequence and vector clock
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr) {
                peer_info.last_sequence = std::cmp::max(peer_info.last_sequence, sequence);
            }
        }

        let mut new_actors = 0;
        let mut updated_actors = 0;
        let mut peer_actors = std::collections::HashSet::new();

        // Pre-compute all updates outside the lock to minimize lock hold time
        let mut updates_to_apply = Vec::new();

        // STEP 1: Read current state snapshot.
        let (local_actors, known_actors) = self.snapshot_actor_maps();

        // STEP 2: Compute all updates outside any lock (fast, pure computation)
        // Process remote local actors
        for (name, location) in remote_local {
            peer_actors.insert(name.clone());
            if !local_actors.contains_key(&name) {
                match known_actors.get(&name) {
                    Some(existing_location) => {
                        // Use vector clock for causal ordering
                        match location
                            .vector_clock
                            .compare(&existing_location.vector_clock)
                        {
                            crate::ClockOrdering::After => {
                                // New location is causally after existing
                                updates_to_apply.push((name.clone(), location));
                                updated_actors += 1;
                            }
                            crate::ClockOrdering::Concurrent => {
                                if stable_concurrent_location_wins(&location, existing_location) {
                                    updates_to_apply.push((name.clone(), location));
                                    updated_actors += 1;
                                }
                            }
                            _ => {} // Keep existing for Before or Equal
                        }
                    }
                    None => {
                        updates_to_apply.push((name.clone(), location));
                        new_actors += 1;
                    }
                }
            }
        }

        // Process remote known actors
        for (name, location) in remote_known {
            if local_actors.contains_key(&name) {
                continue; // Don't override local actors
            }

            match known_actors.get(&name) {
                Some(existing_location) => {
                    // Use vector clock for causal ordering
                    match location
                        .vector_clock
                        .compare(&existing_location.vector_clock)
                    {
                        crate::ClockOrdering::After => {
                            // New location is causally after existing
                            updates_to_apply.push((name, location));
                            updated_actors += 1;
                        }
                        crate::ClockOrdering::Concurrent => {
                            if stable_concurrent_location_wins(&location, existing_location) {
                                updates_to_apply.push((name, location));
                                updated_actors += 1;
                            }
                        }
                        _ => {} // Keep existing for Before or Equal
                    }
                }
                None => {
                    updates_to_apply.push((name, location));
                    new_actors += 1;
                }
            }
        }

        // STEP 3: Apply all updates to the lock-free map.
        if !updates_to_apply.is_empty() {
            for (name, location) in &updates_to_apply {
                // Actor locations learned through full sync are routing targets, not
                // necessarily gossip seeds. Configure the TLS peer-id mapping for direct
                // sends without enrolling every learned actor host into periodic gossip.
                if let Ok(addr) = location.address.parse::<SocketAddr>() {
                    self.configure_peer(location.peer_id.clone(), addr).await;
                    debug!(
                        actor = %name,
                        peer_addr = %addr,
                        "Configured direct route for actor's host"
                    );
                }

                let _ = self
                    .actor_state
                    .known_actors
                    .upsert_sync(name.clone(), location.clone());
            }
        }

        // Update peer-to-actors mapping for failure tracking and detect actors the peer
        // no longer advertises so we can prune stale entries deterministically.
        let removed_peer_actors = {
            let mut gossip_state = self.gossip_state.lock().await;
            match gossip_state
                .peer_to_actors
                .insert(sender_addr, peer_actors.clone())
            {
                Some(previous) => previous
                    .difference(&peer_actors)
                    .cloned()
                    .collect::<Vec<_>>(),
                None => Vec::new(),
            }
        };

        if !removed_peer_actors.is_empty() {
            for actor_name in removed_peer_actors {
                if self
                    .actor_state
                    .local_actors
                    .contains_sync(actor_name.as_str())
                {
                    continue;
                }

                // Safety: only remove an omitted actor if it is still attributed to the sender.
                // Under reordering/churn, a peer can omit an actor that has already moved to a
                // different host. Removing it here would be deterministic but incorrect.
                let still_from_sender = self
                    .actor_state
                    .known_actors
                    .read_sync(actor_name.as_str(), |_, existing| {
                        existing.peer_id == sender_peer_id
                    })
                    .unwrap_or(false);
                if !still_from_sender {
                    debug!(
                        actor_name = %actor_name,
                        peer = %sender_addr,
                        "skipping omitted-actor removal; actor is no longer attributed to this peer"
                    );
                    continue;
                }

                if self
                    .actor_state
                    .known_actors
                    .remove_sync(actor_name.as_str())
                    .is_some()
                {
                    info!(
                        actor_name = %actor_name,
                        peer = %sender_addr,
                        "removed stale actor after peer full sync omitted it"
                    );
                }
            }
        }

        debug!(
            new_actors = new_actors,
            updated_actors = updated_actors,
            peer = %sender_addr,
            peer_actor_count = peer_actors.len(),
            "merged gossip data using vector clock conflict resolution"
        );
    }

    /// Clean up stale actor entries (using wall clock for TTL)
    pub async fn cleanup_stale_actors(&self) {
        let now = current_timestamp();
        let ttl_secs = self.config.actor_ttl.as_secs();

        // Clean up stale known actors (using wall clock time for TTL)
        {
            let before_count = self.actor_state.known_actors.len();

            let mut to_remove = Vec::new();
            self.actor_state.known_actors.iter_sync(|k, location| {
                if now.saturating_sub(location.wall_clock_time) >= ttl_secs {
                    to_remove.push(k.clone());
                }
                true
            });

            for name in &to_remove {
                let _ = self.actor_state.known_actors.remove_sync(name.as_str());
            }

            let removed = before_count.saturating_sub(self.actor_state.known_actors.len());
            if removed > 0 {
                info!(removed_count = removed, "cleaned up stale actor entries");
            }
        }

        // Clean up old delta history (using wall clock)
        {
            let mut gossip_state = self.gossip_state.lock().await;
            let history_ttl = self.config.actor_ttl.as_secs() * 2;
            gossip_state
                .delta_history
                .retain(|delta| now - delta.wall_clock_time < history_ttl);
        }

        // Enforce bounds on data structures
        self.enforce_bounds().await;

        // Clean up connection pool
        {
            let connection_pool = &self.connection_pool;
            connection_pool.cleanup_stale_connections();
        }

        // Clean up stale stream assemblies (incomplete streams older than 60 seconds)
        self.cleanup_stale_stream_assemblies().await;
    }

    /// Clean up incomplete stream assemblies that have been stale for too long.
    /// This prevents memory leaks when StreamStart arrives but StreamEnd never comes.
    pub async fn cleanup_stale_stream_assemblies(&self) {
        const STREAM_ASSEMBLY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

        let before_count = self.stream_assemblies.len();

        self.stream_assemblies.retain_sync(|stream_id, assembly| {
            let age = assembly.started_at.elapsed();
            if age > STREAM_ASSEMBLY_TIMEOUT {
                warn!(
                    stream_id = *stream_id,
                    age_secs = age.as_secs(),
                    received_bytes = assembly.received_bytes,
                    expected_bytes = assembly.header.total_size,
                    chunks_received = assembly.received_indices.len(),
                    "Cleaning up stale stream assembly - StreamEnd never arrived"
                );
                return false;
            }
            true
        });

        let removed = before_count.saturating_sub(self.stream_assemblies.len());
        if removed > 0 {
            info!(
                removed_count = removed,
                remaining = self.stream_assemblies.len(),
                "Cleaned up stale stream assemblies"
            );
        }
    }

    /// Clean up actors from peers that have been disconnected for longer than dead_peer_timeout
    /// IMPORTANT: We keep the peer itself to allow reconnection, only clean up their actors
    pub async fn cleanup_dead_peers(&self) {
        let current_time = current_timestamp();
        let dead_peer_timeout_secs = self.config.dead_peer_timeout.as_secs();

        let peers_to_cleanup: Vec<SocketAddr> = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(_, info)| {
                    // Check if peer has been disconnected for too long
                    info.failures >= self.config.max_peer_failures
                        && info.last_failure_time.is_some_and(|failure_time| {
                            (current_time - failure_time) > dead_peer_timeout_secs
                        })
                })
                .map(|(addr, _)| *addr)
                .collect()
        };

        if !peers_to_cleanup.is_empty() {
            // IMPORTANT: Always acquire locks in consistent order to prevent deadlocks
            // Order: actor_state before gossip_state
            let mut gossip_state = self.gossip_state.lock().await;

            for peer_addr in &peers_to_cleanup {
                // IMPORTANT: We do NOT remove the peer itself - it stays in the peer list
                // This allows us to reconnect when the peer comes back online

                // Remove peer's actors from known_actors to free memory
                if let Some(actor_names) = gossip_state.peer_to_actors.get(peer_addr).cloned() {
                    for actor_name in &actor_names {
                        let _ = self
                            .actor_state
                            .known_actors
                            .remove_sync(actor_name.as_str());
                    }
                    gossip_state.peer_to_actors.remove(peer_addr);

                    info!(
                        peer = %peer_addr,
                        actors_removed = actor_names.len(),
                        timeout_minutes = dead_peer_timeout_secs / 60,
                        "cleaned up actors from long-disconnected peer (peer retained for reconnection)"
                    );
                }

                // Clean up health reports but keep the peer entry
                gossip_state.peer_health_reports.remove(peer_addr);
                gossip_state.pending_peer_failures.remove(peer_addr);
            }
        }
    }

    /// Run vector clock garbage collection to prevent unbounded growth
    pub async fn run_vector_clock_gc(&self) {
        let (active_nodes, dead_nodes_with_timeout) = {
            // Collect all active node IDs and dead nodes with timeout info
            let gossip_state = self.gossip_state.lock().await;
            let mut active = HashSet::new();
            let mut dead = HashMap::new();

            // Add our own node ID - always active
            active.insert(self.peer_id.to_node_id());

            // Add all known peer node IDs based on their status
            let current_time = current_timestamp();
            for peer_info in gossip_state.peers.values() {
                if let Some(node_id) = &peer_info.node_id {
                    if peer_info.failures < self.config.max_peer_failures {
                        // Peer is healthy - keep their entries
                        active.insert(*node_id);
                    } else if let Some(failure_time) = peer_info.last_failure_time {
                        // Peer is failed - check how long it's been dead
                        let time_since_failure = current_time - failure_time;
                        let retention_secs = self.config.vector_clock_retention_period.as_secs();

                        if time_since_failure > retention_secs {
                            // Dead for longer than retention period - can be GC'd
                            dead.insert(*node_id, time_since_failure);
                        } else {
                            // Dead but within retention period - keep their entries
                            active.insert(*node_id);
                        }
                    } else {
                        // Failed but no failure time recorded - keep to be safe
                        active.insert(*node_id);
                    }
                }
            }

            (active, dead)
        };

        // Run GC on all actor vector clocks
        let mut gc_count = 0;
        let mut largest_clock_size = 0;
        self.actor_state.local_actors.iter_sync(|_, location| {
            let before_size = location.vector_clock.len();
            location.vector_clock.gc_old_nodes(&active_nodes);
            let after_size = location.vector_clock.len();
            if before_size > after_size {
                gc_count += before_size - after_size;
            }
            largest_clock_size = largest_clock_size.max(after_size);
            true
        });

        self.actor_state.known_actors.iter_sync(|_, location| {
            let before_size = location.vector_clock.len();
            location.vector_clock.gc_old_nodes(&active_nodes);
            let after_size = location.vector_clock.len();
            if before_size > after_size {
                gc_count += before_size - after_size;
            }
            largest_clock_size = largest_clock_size.max(after_size);
            true
        });

        if gc_count > 0 {
            info!(
                entries_removed = gc_count,
                active_nodes = active_nodes.len(),
                dead_nodes_removed = dead_nodes_with_timeout.len(),
                largest_clock_size = largest_clock_size,
                "vector clock garbage collection completed"
            );

            // Log details about removed nodes
            for (node_id, time_dead) in dead_nodes_with_timeout {
                debug!(
                    node_id = ?node_id,
                    dead_for_secs = time_dead,
                    "removed dead node from vector clocks"
                );
            }
        }

        // Warn if clocks are still large after GC
        if largest_clock_size > 1000 {
            warn!(
                largest_size = largest_clock_size,
                active_nodes = active_nodes.len(),
                "Vector clocks still large after GC. Consider shorter retention period or investigating node churn."
            );
        }
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        debug!("shutting down gossip registry");

        // Set shutdown flag
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.shutdown = true;
        }

        // Set atomic shutdown flag
        self.shutdown.store(true, Ordering::Release);

        // Close all connections in the pool
        {
            let connection_pool = &self.connection_pool;
            connection_pool.close_all_connections();
        }

        // Clear actor state
        {
            self.actor_state.local_actors.clear_sync();
            self.actor_state.known_actors.clear_sync();
        }

        // Clear gossip state
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.pending_changes.clear();
            gossip_state.urgent_changes.clear();
            gossip_state.delta_history.clear();
            gossip_state.peers.clear();
        }

        debug!("gossip registry shutdown complete");
    }

    /// Get a connection handle for direct communication (for performance testing)
    pub(crate) async fn get_connection(
        &self,
        addr: SocketAddr,
    ) -> Result<crate::connection_pool::ConnectionHandle<T>> {
        self.connection_pool.get_connection(addr).await
    }

    /// Get a connection handle directly from the pool without mutex lock
    /// Only works for already established connections
    #[allow(dead_code)]
    pub(crate) fn get_connection_direct(
        &self,
        addr: SocketAddr,
    ) -> Option<crate::connection_pool::ConnectionHandle<T>> {
        // Best-effort: avoid await by using try_lock; return None if busy or not connected.
        self.connection_pool.get_existing_connection(addr)
    }

    pub async fn is_shutdown(&self) -> bool {
        let gossip_state = self.gossip_state.lock().await;
        gossip_state.shutdown
    }

    /// Record that a peer (by address) is active
    pub async fn record_peer_activity(&self, peer_addr: SocketAddr) {
        let gossip_state = self.gossip_state.lock().await;

        // Peer activity is now tracked directly by address
        if let Some(_peer_info) = gossip_state.peers.get(&peer_addr) {
            // Activity is recorded through last_success/last_attempt timestamps
        }

        drop(gossip_state);
        self.reset_udp_liveness_for_peer(peer_addr).await;
    }

    /// Handle peer connection failure - start consensus process
    /// This is called for socket disconnections (not timeouts)
    pub async fn handle_peer_connection_failure(&self, failed_peer_addr: SocketAddr) -> Result<()> {
        info!(
            failed_peer = %failed_peer_addr,
            "socket disconnection detected, marking connection as failed (actors remain available)"
        );

        let current_time = current_timestamp();
        let mut peer_id = {
            let pool = &self.connection_pool;
            pool.get_peer_id_by_addr(&failed_peer_addr)
        };
        if peer_id.is_none() {
            let gossip_state = self.gossip_state.lock().await;
            peer_id = gossip_state
                .peers
                .get(&failed_peer_addr)
                .and_then(|peer| peer.node_id)
                .map(crate::PeerId::from);
        }

        // IMMEDIATELY mark the connection as failed and remove from pool
        // Use disconnect_connection_by_peer_id when possible to clean up ALL address aliases
        {
            let pool = &self.connection_pool;
            // Try to find peer_id for proper cleanup of all aliases
            if let Some(peer_id) = peer_id.clone() {
                if let Some(current) = pool.get_connection_by_peer_id(&peer_id) {
                    info!(
                        addr = %failed_peer_addr,
                        peer_id = %peer_id,
                        current_addr = %current.addr,
                        current_direction = ?current.direction,
                        current_stream_instance_id = ?current.stream_handle.as_ref().map(|handle| handle.instance_id()),
                        "handling peer connection failure for indexed connection"
                    );
                }
                if let Some(_conn) = pool.disconnect_connection_by_peer_id(&peer_id) {
                    info!(addr = %failed_peer_addr, peer_id = %peer_id, "removed disconnected connection from pool (all address aliases cleaned up)");
                }
            } else if pool.has_connection(&failed_peer_addr) {
                // Fallback: no peer_id found, remove by address only
                pool.remove_connection(failed_peer_addr);
                info!(addr = %failed_peer_addr, "removed disconnected connection from pool (by address only)");
            }
        }

        if let Some(cell) = self.peer_disconnect_handler.load_full() {
            let handler = cell.handler.clone();
            let peer_id = peer_id.clone();
            tokio::spawn(async move {
                handler
                    .handle_peer_disconnect(failed_peer_addr, peer_id)
                    .await;
            });
        }

        // IMMEDIATELY mark peer as failed in our local state
        let mut crossed_threshold = false;
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&failed_peer_addr) {
                let was_below = peer_info.failures < self.config.max_peer_failures;
                peer_info.failures = self.config.max_peer_failures;
                peer_info.last_failure_time = Some(current_time);
                peer_info.last_attempt = current_time; // Update last_attempt so retry happens after interval
                crossed_threshold = was_below;
                info!(
                    peer = %failed_peer_addr,
                    retry_after_secs = self.config.peer_retry_interval.as_secs(),
                    "marked peer as disconnected in local state, will retry after interval"
                );
            }
        }

        // Gossip protocol's verdict: socket disconnect = peer is dead.
        // Fire the cleanup hook on the same code path as the
        // failures-incremented-to-threshold case in `apply_gossip_results`,
        // so `known_actors` and pool-state evictions are uniform.
        if crossed_threshold {
            self.handle_peer_death(failed_peer_addr).await;

            // C1 + C2: socket-close observation must produce an
            // immediate cluster-wide broadcast, not wait for the
            // periodic gossip tick. `handle_peer_death` (after the
            // C3 fix) enqueues an Immediate-priority `ActorRemoved`
            // change for every pruned actor — kick a gossip round
            // right now so indirect peers learn about the death in
            // sub-millisecond time rather than `gossip_interval`.
            //
            // This path is NOT called from inside `apply_gossip_results`
            // (which has its own re-entry concerns), so triggering
            // immediate gossip here is safe.
            if let Err(err) = self.trigger_immediate_gossip().await {
                warn!(
                    error = %err,
                    failed_peer = %failed_peer_addr,
                    "trigger_immediate_gossip after socket-close failed; \
                     ActorRemoved deltas will go out on next periodic gossip"
                );
            }
        }

        // Now start consensus process for actor invalidation.
        //
        // IMPORTANT: Do not spawn an untracked delayed task here. Under churn this can
        // accumulate background work and keep registry state alive longer than intended.
        // Instead, we do the 100ms delay inline, but only on the first observation of
        // a pending failure for this peer.
        let should_send_query = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Record our own health report.
            let our_report = PeerHealthStatus {
                is_alive: false,
                last_contact: current_time,
                failure_count: 1,
            };

            gossip_state
                .peer_health_reports
                .entry(failed_peer_addr)
                .or_insert_with(HashMap::new)
                .insert(self.bind_addr, our_report);

            // If we don't have a pending failure, create one.
            match gossip_state.pending_peer_failures.entry(failed_peer_addr) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    let pending = PendingFailure {
                        first_detected: current_time,
                        consensus_deadline: current_time + 5, // 5 second timeout
                        query_sent: false,
                    };
                    e.insert(pending);

                    info!(
                        failed_peer = %failed_peer_addr,
                        deadline = current_time + 5,
                        "created pending failure record, waiting for consensus on actor invalidation"
                    );
                    true
                }
                std::collections::hash_map::Entry::Occupied(_) => false,
            }
        };

        if should_send_query {
            // Don't query immediately - give other nodes time to detect their own disconnections.
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            if self.shutdown.load(std::sync::atomic::Ordering::Acquire) {
                return Ok(());
            }

            info!(
                failed_peer = %failed_peer_addr,
                "delayed query: now checking peer consensus after 100ms"
            );

            // Query other peers for their view of the failed peer.
            // This is to determine if we should invalidate actors, not the connection status.
            if let Err(e) = self.query_peer_health_consensus(failed_peer_addr).await {
                warn!(error = %e, "failed to query peer health consensus");
            }
        }

        Ok(())
    }

    /// Handle a peer connection failure by peer ID instead of address
    pub async fn handle_peer_connection_failure_by_peer_id(
        &self,
        failed_peer_id: &crate::PeerId,
    ) -> Result<()> {
        info!(
            failed_peer_id = %failed_peer_id,
            "node disconnection detected by ID, marking connection as failed (actors remain available)"
        );

        // First, find the peer address from the node ID
        let failed_peer_addr = {
            let pool = &self.connection_pool;

            // Try to find the address from our node ID mapping
            let addr_opt = pool.get_configured_peer_addr(failed_peer_id);

            match addr_opt {
                Some(addr) => addr,
                None => {
                    warn!(
                        peer_id = %failed_peer_id,
                        "cannot find address for failed peer ID - may have already been removed"
                    );
                    return Ok(());
                }
            }
        };

        let current_time = current_timestamp();

        // IMMEDIATELY remove the connection from pool
        // Use disconnect_connection_by_peer_id to clean up ALL address aliases
        // (ephemeral port + bind address mappings created during reindex)
        {
            let pool = &self.connection_pool;

            if let Some(_conn) = pool.disconnect_connection_by_peer_id(failed_peer_id) {
                info!(
                    addr = %failed_peer_addr,
                    node_id = %failed_peer_id,
                    "removed disconnected connection from pool (all address aliases cleaned up)"
                );
            } else {
                info!(
                    addr = %failed_peer_addr,
                    node_id = %failed_peer_id,
                    "connection already removed from pool"
                );
            }

            info!(
                node_id = %failed_peer_id,
                addr = %failed_peer_addr,
                connections_remaining = pool.connection_count(),
                "connection cleanup complete"
            );
        }

        if let Some(cell) = self.peer_disconnect_handler.load_full() {
            let handler = cell.handler.clone();
            let peer_id = Some(failed_peer_id.clone());
            tokio::spawn(async move {
                handler
                    .handle_peer_disconnect(failed_peer_addr, peer_id)
                    .await;
            });
        }

        // IMMEDIATELY mark peer as failed in our local state
        let mut crossed_threshold = false;
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&failed_peer_addr) {
                let was_below = peer_info.failures < self.config.max_peer_failures;
                peer_info.failures = self.config.max_peer_failures;
                peer_info.last_failure_time = Some(current_time);
                peer_info.last_attempt = current_time; // Update last_attempt so retry happens after interval
                crossed_threshold = was_below;
                info!(
                    peer = %failed_peer_addr,
                    node_id = %failed_peer_id,
                    retry_after_secs = self.config.peer_retry_interval.as_secs(),
                    "marked peer as disconnected in local state, will retry after interval"
                );
            }
        }

        // Gossip protocol's verdict: socket disconnect = peer is dead.
        if crossed_threshold {
            self.handle_peer_death(failed_peer_addr).await;
        }

        // Now start consensus process for actor invalidation (same as address-based method).
        let should_send_query = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Record our own health report.
            let our_report = PeerHealthStatus {
                is_alive: false,
                last_contact: current_time,
                failure_count: 1,
            };

            gossip_state
                .peer_health_reports
                .entry(failed_peer_addr)
                .or_insert_with(HashMap::new)
                .insert(self.bind_addr, our_report);

            // If we don't have a pending failure, create one.
            match gossip_state.pending_peer_failures.entry(failed_peer_addr) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    let pending = PendingFailure {
                        first_detected: current_time,
                        consensus_deadline: current_time + 5, // 5 second timeout
                        query_sent: false,
                    };
                    e.insert(pending);

                    info!(
                        failed_peer = %failed_peer_addr,
                        node_id = %failed_peer_id,
                        deadline = current_time + 5,
                        "created pending failure record, waiting for consensus on actor invalidation"
                    );
                    true
                }
                std::collections::hash_map::Entry::Occupied(_) => false,
            }
        };

        if should_send_query {
            // Don't query immediately - give other nodes time to detect their own disconnections.
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            if self.shutdown.load(std::sync::atomic::Ordering::Acquire) {
                return Ok(());
            }

            info!(
                failed_peer = %failed_peer_addr,
                "delayed query: now checking peer consensus after 100ms"
            );

            // Query other peers for their view of the failed peer.
            // This is to determine if we should invalidate actors, not the connection status.
            if let Err(e) = self.query_peer_health_consensus(failed_peer_addr).await {
                warn!(error = %e, "failed to query peer health consensus");
            }
        }

        Ok(())
    }

    /// Query other peers for their view of a potentially failed peer
    async fn query_peer_health_consensus(&self, target_peer: SocketAddr) -> Result<()> {
        // Note: We may have a connection to this address but for a different peer (after reconnection)
        // This is OK - we query other peers to get consensus about the ORIGINAL peer

        // TODO: Ideally we'd track which peer_id a connection was established for and check that
        // For now, we rely on the fact that if we're querying, we've already decided the peer might be failed

        warn!(
            target_peer = %target_peer,
            "🔍 Starting consensus query for peer (may have active connection to different peer after reconnection)"
        );

        let query_msg = RegistryMessage::PeerHealthQuery {
            sender: self.peer_id.clone(),
            target_peer: target_peer.to_string(),
            timestamp: current_timestamp(),
        };

        // Get list of healthy peers to query
        let peers_to_query = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(addr, info)| {
                    **addr != target_peer && // Don't query the target
                    info.failures < self.config.max_peer_failures // Only query healthy peers
                })
                .map(|(addr, _)| *addr)
                .collect::<Vec<_>>()
        };

        info!(
            target_peer = %target_peer,
            querying_peers = peers_to_query.len(),
            "querying peers for health consensus"
        );

        // Send queries to all healthy peers
        let payload = bytes::Bytes::from_owner(
            rkyv::to_bytes::<rkyv::rancor::Error>(&query_msg).map_err(crate::GossipError::from)?,
        );
        for peer in peers_to_query {
            // Try to send through existing connection
            let pool = &self.connection_pool;
            if let Ok(conn) = pool.get_connection(peer).await {
                let _ = conn.send_gossip_payload(payload.clone()).await;
            }
        }

        // Mark query as sent
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(pending) = gossip_state.pending_peer_failures.get_mut(&target_peer) {
                pending.query_sent = true;
            }
        }

        Ok(())
    }

    /// Check if we have consensus about any pending peer failures
    pub async fn check_peer_consensus(&self) {
        let current_time = current_timestamp();

        {
            let mut gossip_state = self.gossip_state.lock().await;
            let mut completed_failures = Vec::new();

            for (peer_addr, pending) in &gossip_state.pending_peer_failures {
                // Check if we've reached the deadline or have enough reports
                let reports = gossip_state.peer_health_reports.get(peer_addr);
                let total_peers = gossip_state.peers.len();

                if let Some(reports) = reports {
                    let alive_count = reports.values().filter(|r| r.is_alive).count();
                    let dead_count = reports.values().filter(|r| !r.is_alive).count();
                    let total_reports = reports.len();

                    info!(
                        peer = %peer_addr,
                        alive_votes = alive_count,
                        dead_votes = dead_count,
                        total_reports = total_reports,
                        total_peers = total_peers,
                        "checking peer consensus"
                    );

                    // We NO LONGER remove actors even if consensus says the peer is dead
                    // The actors remain configured and available for when the node reconnects

                    if total_peers <= 1 {
                        // Only us and the failed peer
                        completed_failures.push(*peer_addr);
                        info!(
                            peer = %peer_addr,
                            "2-node cluster: peer is disconnected, keeping actors for potential reconnection"
                        );
                    } else {
                        // Multiple nodes - check consensus
                        let majority = total_peers.div_ceil(2);

                        if dead_count >= majority || current_time >= pending.consensus_deadline {
                            completed_failures.push(*peer_addr);

                            // Check if we have an active connection to this peer
                            let has_active_connection = {
                                let pool = &self.connection_pool;
                                pool.has_connection(peer_addr)
                            };

                            if dead_count > alive_count {
                                // Majority says dead
                                if has_active_connection {
                                    error!(
                                        peer = %peer_addr,
                                        dead_votes = dead_count,
                                        alive_votes = alive_count,
                                        "🚨 CONSENSUS CONFLICT: Majority says peer is dead but we have an ACTIVE SOCKET CONNECTION! This should not happen!"
                                    );
                                } else {
                                    info!(
                                        peer = %peer_addr,
                                        dead_votes = dead_count,
                                        alive_votes = alive_count,
                                        "consensus: majority says peer is dead, but keeping actors for reconnection"
                                    );
                                }
                            } else if alive_count > dead_count {
                                // Majority says alive
                                info!(
                                    peer = %peer_addr,
                                    alive_votes = alive_count,
                                    dead_votes = dead_count,
                                    "consensus: peer is alive elsewhere, keeping actors"
                                );
                            } else {
                                // Tie or timeout
                                if has_active_connection {
                                    error!(
                                        peer = %peer_addr,
                                        "🚨 CONSENSUS CONFLICT: Consensus timeout/tie but we have an ACTIVE SOCKET CONNECTION!"
                                    );
                                } else {
                                    info!(
                                        peer = %peer_addr,
                                        "consensus timeout or tie, keeping actors"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Remove completed failures
            for peer in completed_failures {
                gossip_state.pending_peer_failures.remove(&peer);
                gossip_state.peer_health_reports.remove(&peer);
            }
        }

        // We no longer invalidate actors based on consensus
        // The connection state is already updated, actors remain available
    }

    /// Deduplicate changes, keeping the causally-most-recent change for each actor.
    ///
    /// C7 fix: previously this function kept the *iteration-last* change per name, which
    /// meant out-of-order arrival of `ActorAdded(seq=10)` and `ActorRemoved(seq=20)` for the
    /// same actor could collapse to whichever happened to be inserted last, resurrecting
    /// deleted actors when deltas arrived out of order.
    ///
    /// Resolution rule:
    ///   - If candidate's vector clock happens-after existing: candidate wins.
    ///   - If candidate happens-before existing: existing wins.
    ///   - If concurrent or equal: **remove-wins** — `ActorRemoved` displaces `ActorAdded`,
    ///     never the other way around. This is the standard CRDT tie-break for tombstones
    ///     and ensures a stale `ActorAdded` cannot resurrect an actor that some peer has
    ///     concurrently observed as removed.
    pub fn deduplicate_changes(changes: Vec<RegistryChange>) -> Vec<RegistryChange> {
        let mut actor_changes: HashMap<String, RegistryChange> = HashMap::new();

        for change in changes {
            let actor_name = Self::get_change_actor_name(&change);
            match actor_changes.entry(actor_name) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(change);
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if Self::change_wins(&change, e.get()) {
                        e.insert(change);
                    }
                }
            }
        }

        // Stable ordering: avoid propagating hash iteration order into protocol-visible messages.
        let mut ordered: Vec<(String, RegistryChange)> = actor_changes.into_iter().collect();
        ordered.sort_by(|a, b| a.0.cmp(&b.0));
        ordered.into_iter().map(|(_, v)| v).collect()
    }

    /// Causal-precedence + remove-wins comparator used by `deduplicate_changes`.
    fn change_wins(candidate: &RegistryChange, existing: &RegistryChange) -> bool {
        let cand_clock = Self::change_vector_clock(candidate);
        let exist_clock = Self::change_vector_clock(existing);
        match cand_clock.compare(exist_clock) {
            crate::ClockOrdering::After => true,
            crate::ClockOrdering::Before => false,
            crate::ClockOrdering::Equal | crate::ClockOrdering::Concurrent => {
                // Remove-wins: `ActorRemoved` displaces `ActorAdded`, never the
                // reverse, on causal ties. Two `ActorRemoved` for the same name
                // are kept stably (existing wins). Two `ActorAdded` likewise.
                matches!(candidate, RegistryChange::ActorRemoved { .. })
                    && !matches!(existing, RegistryChange::ActorRemoved { .. })
            }
        }
    }

    fn change_vector_clock(change: &RegistryChange) -> &crate::VectorClock {
        match change {
            RegistryChange::ActorAdded { location, .. } => &location.vector_clock,
            RegistryChange::ActorRemoved { vector_clock, .. } => vector_clock,
        }
    }

    /// Extract the actor name from a registry change
    fn get_change_actor_name(change: &RegistryChange) -> String {
        match change {
            RegistryChange::ActorAdded { name, .. } | RegistryChange::ActorRemoved { name, .. } => {
                name.clone()
            }
        }
    }

    /// Enforce bounds on gossip state data structures to prevent unbounded growth
    async fn enforce_bounds(&self) {
        let mut gossip_state = self.gossip_state.lock().await;

        // Apply vector clock compaction to all changes before bounds enforcement
        let max_clock_size = self.config.max_vector_clock_size;

        // Compact vector clocks in pending changes
        for change in &gossip_state.pending_changes {
            match change {
                RegistryChange::ActorAdded { location, .. } => {
                    if location.vector_clock.len() > max_clock_size {
                        location.vector_clock.compact(max_clock_size);
                    }
                }
                RegistryChange::ActorRemoved { vector_clock, .. } => {
                    if vector_clock.len() > max_clock_size {
                        vector_clock.compact(max_clock_size);
                    }
                }
            }
        }

        // Compact vector clocks in urgent changes
        for change in &gossip_state.urgent_changes {
            match change {
                RegistryChange::ActorAdded { location, .. } => {
                    if location.vector_clock.len() > max_clock_size {
                        location.vector_clock.compact(max_clock_size);
                    }
                }
                RegistryChange::ActorRemoved { vector_clock, .. } => {
                    if vector_clock.len() > max_clock_size {
                        vector_clock.compact(max_clock_size);
                    }
                }
            }
        }

        // Compact vector clocks in delta history
        for delta in &gossip_state.delta_history {
            for change in &delta.changes {
                match change {
                    RegistryChange::ActorAdded { location, .. } => {
                        if location.vector_clock.len() > max_clock_size {
                            location.vector_clock.compact(max_clock_size);
                        }
                    }
                    RegistryChange::ActorRemoved { vector_clock, .. } => {
                        if vector_clock.len() > max_clock_size {
                            vector_clock.compact(max_clock_size);
                        }
                    }
                }
            }
        }

        // Bound pending changes
        let max_pending = 1000;
        if gossip_state.pending_changes.len() > max_pending {
            debug!(
                "Trimming pending changes from {} to {}",
                gossip_state.pending_changes.len(),
                max_pending
            );
            gossip_state.pending_changes.truncate(max_pending);
        }

        // Bound urgent changes (smaller limit since these are high priority)
        let max_urgent = 100;
        if gossip_state.urgent_changes.len() > max_urgent {
            debug!(
                "Trimming urgent changes from {} to {}",
                gossip_state.urgent_changes.len(),
                max_urgent
            );
            gossip_state.urgent_changes.truncate(max_urgent);
        }

        // Bound delta history
        if gossip_state.delta_history.len() > self.config.max_delta_history {
            let excess = gossip_state.delta_history.len() - self.config.max_delta_history;
            debug!("Trimming delta history by {} entries", excess);
            gossip_state.delta_history.drain(0..excess);
        }

        // Bound peers list
        let max_peers = 1000;
        if gossip_state.peers.len() > max_peers {
            debug!(
                "Trimming peers from {} to {}",
                gossip_state.peers.len(),
                max_peers
            );
            let _current_time = current_timestamp();
            let mut peers_by_age: Vec<_> = gossip_state
                .peers
                .iter()
                .map(|(addr, peer)| (*addr, peer.last_success))
                .collect();
            peers_by_age.sort_by_key(|(_, last_success)| *last_success);

            let to_remove = gossip_state.peers.len() - max_peers;
            let addrs_to_remove: Vec<_> = peers_by_age
                .iter()
                .take(to_remove)
                .map(|(addr, _)| *addr)
                .collect();
            for addr in &addrs_to_remove {
                gossip_state.peers.remove(addr);
                // LEAK-002 fix: Also clean up associated maps for evicted peers
                gossip_state.peer_to_actors.remove(addr);
                gossip_state.peer_health_reports.remove(addr);
                gossip_state.pending_peer_failures.remove(addr);
            }
            if !addrs_to_remove.is_empty() {
                debug!(
                    removed_count = addrs_to_remove.len(),
                    "Trimmed stale peers and associated data"
                );
            }
        }
    }

    /// Trigger immediate gossip for urgent changes - optimized for speed
    pub async fn trigger_immediate_gossip(&self) -> Result<()> {
        if !self.config.immediate_propagation_enabled {
            return Ok(());
        }

        // Fast path: get urgent changes and peers in one go
        let (urgent_changes, critical_peers) = {
            let mut gossip_state = self.gossip_state.lock().await;

            if gossip_state.urgent_changes.is_empty() {
                return Ok(());
            }

            // Take all urgent changes for immediate propagation (avoid clone)
            let changes = std::mem::take(&mut gossip_state.urgent_changes);

            // Get healthy peers quickly - just take the first few healthy ones
            let peers: Vec<_> = gossip_state
                .peers
                .iter()
                .filter(|(_, peer)| peer.failures < self.config.max_peer_failures)
                .take(self.config.urgent_gossip_fanout)
                .map(|(addr, _)| *addr)
                .collect();

            (changes, peers)
        };

        if urgent_changes.is_empty() {
            return Ok(());
        }

        let urgent_changes_for_retry = urgent_changes.clone();

        if critical_peers.is_empty() {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state
                .pending_changes
                .extend(urgent_changes.iter().map(Self::as_regular_gossip_change));
            return Ok(());
        }

        for change in &urgent_changes {
            match change {
                RegistryChange::ActorAdded {
                    name,
                    location,
                    priority,
                    ..
                } => {
                    info!(
                        "  ➕ IMMEDIATE: Adding actor {} at {} (priority: {:?})",
                        name, location.address, priority
                    );
                }
                RegistryChange::ActorRemoved { name, priority, .. } => {
                    info!(
                        "  ➖ IMMEDIATE: Removing actor {} (priority: {:?})",
                        name, priority
                    );
                }
            }
        }

        // Store count before moving
        let urgent_changes_count = urgent_changes.len();

        let wall_clock_time = current_timestamp(); // For debugging/monitoring only
        let precise_timing_nanos = crate::current_timestamp_nanos(); // High precision timing

        // Serialize message(s), chunking if we exceed max_message_size
        let serialization_start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut serialized_messages: Vec<bytes::Bytes> = Vec::new();
        let mut current_changes: Vec<RegistryChange> = Vec::new();
        let mut current_serialized: Option<bytes::Bytes> = None;

        for change in urgent_changes {
            current_changes.push(change);

            let message = RegistryMessage::DeltaGossip {
                delta: RegistryDelta {
                    sender_peer_id: self.peer_id.clone(),
                    since_sequence: 0,   // Not used for immediate gossip
                    current_sequence: 0, // Not used for immediate gossip
                    changes: current_changes.clone(),
                    wall_clock_time,
                    precise_timing_nanos,
                },
            };
            let serialized =
                bytes::Bytes::from_owner(rkyv::to_bytes::<rkyv::rancor::Error>(&message)?);

            if serialized.len() > self.config.max_message_size && current_changes.len() > 1 {
                let last = current_changes.pop().unwrap();
                if let Some(previous) = current_serialized.take() {
                    serialized_messages.push(previous);
                }
                current_changes.clear();
                current_changes.push(last);
                let tail_message = RegistryMessage::DeltaGossip {
                    delta: RegistryDelta {
                        sender_peer_id: self.peer_id.clone(),
                        since_sequence: 0,
                        current_sequence: 0,
                        changes: current_changes.clone(),
                        wall_clock_time,
                        precise_timing_nanos,
                    },
                };
                current_serialized = Some(bytes::Bytes::from_owner(rkyv::to_bytes::<
                    rkyv::rancor::Error,
                >(
                    &tail_message
                )?));
            } else if serialized.len() > self.config.max_message_size {
                warn!(
                    size = serialized.len(),
                    max = self.config.max_message_size,
                    "Immediate gossip change exceeds max message size; sending as single chunk"
                );
                serialized_messages.push(serialized);
                current_changes.clear();
                current_serialized = None;
            } else {
                current_serialized = Some(serialized);
            }
        }

        if let Some(serialized) = current_serialized.take() {
            serialized_messages.push(serialized);
        }

        let serialization_end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let serialization_duration_ms =
            (serialization_end - serialization_start) as f64 / 1_000_000.0;

        if serialized_messages.len() > 1 {
            warn!(
                chunks = serialized_messages.len(),
                max = self.config.max_message_size,
                "Immediate gossip split into multiple chunks to honor max message size"
            );
        }

        // Log immediate propagation with timing
        info!(
            "🚀 IMMEDIATE GOSSIP: Broadcasting {} urgent changes to {} peers (serialization: {:.3}ms, chunks: {})",
            urgent_changes_count,
            critical_peers.len(),
            serialization_duration_ms,
            serialized_messages.len()
        );

        // Pre-establish all connections once.
        let peer_connections: Vec<(SocketAddr, crate::connection_pool::ConnectionHandle<T>)> = {
            let pool_guard = &self.connection_pool;
            let mut connections = Vec::new();

            for peer_addr in &critical_peers {
                if let Ok(conn) = pool_guard.get_connection(*peer_addr).await {
                    connections.push((*peer_addr, conn));
                }
            }

            connections
        };

        if peer_connections.is_empty() {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.pending_changes.extend(
                urgent_changes_for_retry
                    .iter()
                    .map(Self::as_regular_gossip_change),
            );
            return Ok(());
        }

        let payloads = Arc::new(serialized_messages);

        // Send to all peers concurrently with pre-established connections.
        let mut join_handles = Vec::new();

        let mut had_failure = false;
        for (peer_addr, conn) in peer_connections {
            let payloads = payloads.clone();
            let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
                // Measure pure network send time
                let send_start = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();

                for payload in payloads.iter() {
                    conn.send_gossip_payload(payload.clone()).await?;
                }

                let send_end = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let send_time_ms = (send_end - send_start) as f64 / 1_000_000.0;

                debug!(peer = %peer_addr, send_time_ms = send_time_ms, "Network send completed");

                Ok(())
            });

            join_handles.push(handle);
        }

        // Wait for all sends to complete
        for handle in join_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!("immediate gossip send failed: {}", e);
                    had_failure = true;
                }
                Err(e) => {
                    warn!("immediate gossip task failed: {}", e);
                    had_failure = true;
                }
            }
        }

        let mut gossip_state = self.gossip_state.lock().await;
        if had_failure {
            warn!(
                "immediate gossip encountered send failures - scheduling retry via regular gossip"
            );
        }
        gossip_state.pending_changes.extend(
            urgent_changes_for_retry
                .iter()
                .map(Self::as_regular_gossip_change),
        );

        Ok(())
    }

    /// Immediately invalidate all actors from a failed peer
    pub async fn invalidate_peer_actors(&self, failed_peer_addr: SocketAddr) -> Result<()> {
        info!(failed_peer = %failed_peer_addr, "invalidating actors from failed peer");

        // Get the list of actors to invalidate from this peer
        let (actors_to_remove, should_trigger_immediate) = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Get actors belonging to the failed peer
            let actors_to_remove = gossip_state
                .peer_to_actors
                .remove(&failed_peer_addr)
                .unwrap_or_default();

            if actors_to_remove.is_empty() {
                debug!(failed_peer = %failed_peer_addr, "no actors to invalidate");
                return Ok(());
            }

            // Create removal changes for each actor with proper vector clocks
            let mut removal_changes = Vec::new();

            // We need to get the current vector clocks for these actors
            for actor_name in &actors_to_remove {
                // Get the existing vector clock for this actor if it exists
                let removal_clock = self
                    .actor_state
                    .known_actors
                    .read_sync(actor_name.as_str(), |_, location| {
                        // Use the actor's current vector clock and increment it.
                        let clock = location.vector_clock.clone();
                        clock.increment(self.peer_id.to_node_id());
                        clock
                    })
                    .unwrap_or_else(|| {
                        // If actor not found (shouldn't happen), create a new clock with our increment.
                        let clock = crate::VectorClock::new();
                        clock.increment(self.peer_id.to_node_id());
                        clock
                    });

                let change = RegistryChange::ActorRemoved {
                    name: actor_name.clone(),
                    vector_clock: removal_clock,
                    removing_node_id: self.peer_id.to_node_id(),
                    priority: RegistrationPriority::Immediate, // Node failures are always immediate
                };
                removal_changes.push(change);
            }

            // Add all removal changes to urgent queue
            gossip_state.urgent_changes.extend(removal_changes);

            (actors_to_remove, !gossip_state.urgent_changes.is_empty())
        };

        // Remove actors from known_actors (they shouldn't be in local_actors if they're from a failed node)
        let removed_count = {
            let mut removed = 0;

            for actor_name in &actors_to_remove {
                if self
                    .actor_state
                    .known_actors
                    .remove_sync(actor_name.as_str())
                    .is_some()
                {
                    removed += 1;
                }
                // Also remove from local_actors if somehow present (defensive)
                if self
                    .actor_state
                    .local_actors
                    .remove_sync(actor_name.as_str())
                    .is_some()
                {
                    removed += 1;
                }
            }
            removed
        };

        info!(
            failed_peer = %failed_peer_addr,
            actors_removed = removed_count,
            actors_invalidated = ?actors_to_remove,
            "PEER_FAILURE_INVALIDATION"
        );

        // Trigger immediate gossip to propagate the failures
        if should_trigger_immediate {
            if let Err(err) = self.trigger_immediate_gossip().await {
                warn!(error = %err, "failed to trigger immediate gossip for node failure");
            }
        }

        Ok(())
    }

    /// Start assembling a streamed message
    ///
    /// # Arguments
    /// * `header` - Stream header with metadata
    /// * `correlation_id` - Optional correlation ID for ask_streaming responses
    /// * `peer_addr` - Optional peer address to send response to
    pub async fn start_stream_assembly(
        &self,
        header: crate::StreamHeader,
        correlation_id: Option<u16>,
        peer_addr: Option<std::net::SocketAddr>,
    ) {
        let total_size = match usize::try_from(header.total_size) {
            Ok(size) => size,
            Err(_) => {
                warn!(
                    stream_id = header.stream_id,
                    total_size = header.total_size,
                    "Stream assembly size overflows usize"
                );
                return;
            }
        };

        if total_size > crate::MAX_STREAM_SIZE {
            warn!(
                stream_id = header.stream_id,
                total_size = total_size,
                max_size = crate::MAX_STREAM_SIZE,
                "Stream assembly size exceeds MAX_STREAM_SIZE"
            );
            return;
        }

        let _ = self.stream_assemblies.upsert_sync(
            header.stream_id,
            StreamAssembly {
                header,
                received_indices: std::collections::BTreeSet::new(),
                received_bytes: 0,
                buffer: self.connection_pool.make_pooled_aligned_buffer(total_size),
                chunk_stride: None,
                started_at: std::time::Instant::now(),
                correlation_id,
                peer_addr,
            },
        );
        debug!(
            stream_id = header.stream_id,
            ?correlation_id,
            ?peer_addr,
            "Started stream assembly"
        );
    }

    /// Add a chunk to stream assembly
    pub async fn add_stream_chunk(&self, header: crate::StreamHeader, chunk_data: Vec<u8>) {
        let stream_id = header.stream_id;
        let res = self.stream_assemblies.update_sync(
            &stream_id,
            |_, assembly| -> std::result::Result<(), &'static str> {
                if header.total_size != assembly.header.total_size {
                    return Err("Stream assembly total_size mismatch");
                }

                if header.chunk_size as usize != chunk_data.len() {
                    return Err("Stream assembly chunk_size mismatch");
                }

                if assembly.chunk_stride.is_none() && header.chunk_size > 0 {
                    assembly.chunk_stride = Some(header.chunk_size as usize);
                }

                let stride = assembly.chunk_stride.unwrap_or(header.chunk_size as usize);
                let offset = header.chunk_index as usize * stride;
                let end = offset + chunk_data.len();
                if end > assembly.header.total_size as usize {
                    return Err("Stream assembly chunk overflow");
                }

                // CRITICAL_PATH: write chunk directly into final buffer.
                assembly.buffer.as_mut_slice()[offset..end].copy_from_slice(&chunk_data);
                assembly.received_bytes += chunk_data.len();
                assembly.received_indices.insert(header.chunk_index);

                Ok(())
            },
        );

        match res {
            Some(Ok(())) => {}
            Some(Err(msg)) => {
                warn!(
                    stream_id = stream_id,
                    msg = msg,
                    "Stream assembly chunk rejected"
                );
            }
            None => {
                warn!(stream_id = stream_id, "Stream assembly not found for chunk");
            }
        }
    }

    /// Complete stream assembly and return the complete message with metadata
    pub async fn complete_stream_assembly(&self, stream_id: u64) -> Option<StreamAssemblyResult> {
        if let Some((_, assembly)) = self.stream_assemblies.remove_sync(&stream_id) {
            // Verify we have all chunks with proper gap detection
            if !assembly.is_complete() {
                warn!(
                    stream_id = stream_id,
                    received = assembly.received_indices.len(),
                    "Incomplete stream assembly"
                );
                return None;
            }

            info!(
                stream_id = stream_id,
                total_size = assembly.header.total_size,
                correlation_id = ?assembly.correlation_id,
                "Completed stream assembly"
            );

            let complete = assembly.buffer.into_aligned_bytes();

            Some(StreamAssemblyResult {
                data: complete,
                correlation_id: assembly.correlation_id,
                peer_addr: assembly.peer_addr,
                header: assembly.header,
            })
        } else {
            warn!(
                stream_id = stream_id,
                "Stream assembly not found for completion"
            );
            None
        }
    }

    // =================== Peer Discovery Methods ===================

    /// Maximum size of peer list in gossip messages (resource exhaustion protection)
    pub const MAX_PEER_LIST_SIZE: usize = 1000;

    /// Create a snapshot of current peers for gossip
    /// Includes self (using advertised_address from config)
    pub async fn peers_snapshot(&self) -> Vec<PeerInfoGossip> {
        let gossip_state = self.gossip_state.lock().await;
        let mut peers: Vec<PeerInfoGossip> = Vec::new();

        // Include self (using advertised address or bind address)
        let self_addr = self.config.advertise_address.unwrap_or(self.bind_addr);
        let mut self_info = PeerInfo::local(self_addr);
        // Include our DNS name in gossip so peers can re-resolve us on reconnect
        self_info.dns_name = self.config.advertise_dns.clone();
        peers.push(self_info.to_gossip());

        // Include active peers
        for (_, peer_info) in gossip_state.peers.iter() {
            // Skip failed peers
            if peer_info.failures >= self.config.max_peer_failures {
                continue;
            }
            // Don't gossip unsafe addresses (prevents private/bogon propagation).
            if !crate::net_security::is_safe_to_dial(
                &peer_info.address,
                self.config.allow_private_discovery,
                self.config.allow_loopback_discovery,
                self.config.allow_link_local_discovery,
            ) {
                continue;
            }
            peers.push(peer_info.to_gossip());
        }

        // Include known peers from LRU cache (up to limit)
        let remaining = Self::MAX_PEER_LIST_SIZE.saturating_sub(peers.len());
        for (_, peer_info) in gossip_state.known_peers.iter().take(remaining) {
            if !crate::net_security::is_safe_to_dial(
                &peer_info.address,
                self.config.allow_private_discovery,
                self.config.allow_loopback_discovery,
                self.config.allow_link_local_discovery,
            ) {
                continue;
            }
            peers.push(peer_info.to_gossip());
        }

        // Truncate to max size
        peers.truncate(Self::MAX_PEER_LIST_SIZE);

        peers
    }

    /// Periodic gossip of peer list to random subset of peers
    /// Returns gossip tasks for the caller to send.
    pub async fn gossip_peer_list(&self) -> Vec<GossipTask> {
        // Check if peer discovery is enabled
        if !self.config.enable_peer_discovery {
            return Vec::new();
        }

        // Check gossip interval
        let now = current_timestamp();
        let current_sequence = {
            let gossip_state = self.gossip_state.lock().await;
            if let Some(interval) = self.config.peer_gossip_interval {
                let interval_secs = interval.as_secs();
                if now
                    < gossip_state
                        .last_peer_gossip_time
                        .saturating_add(interval_secs)
                {
                    return Vec::new(); // Not time yet
                }
            }
            gossip_state.gossip_sequence
        };

        // Get peer snapshot
        let peers = self.peers_snapshot().await;
        if peers.is_empty() {
            return Vec::new();
        }

        // Get active peers to gossip to
        let targets: Vec<SocketAddr> = {
            let gossip_state = self.gossip_state.lock().await;
            let mut active_peers: Vec<SocketAddr> = gossip_state
                .peers
                .iter()
                .filter(|(_, info)| info.failures < self.config.max_peer_failures)
                .map(|(addr, _)| *addr)
                .collect();

            // Shuffle and take max_peer_gossip_targets
            active_peers.shuffle(&mut rand::rng());
            active_peers.truncate(self.config.max_peer_gossip_targets);
            active_peers
        };

        if targets.is_empty() {
            return Vec::new();
        }

        // Update last gossip time
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.last_peer_gossip_time = now;
        }

        // Create message
        let self_addr = self.config.advertise_address.unwrap_or(self.bind_addr);
        let msg = RegistryMessage::PeerListGossip {
            peers,
            timestamp: now,
            sender_addr: self_addr.to_string(),
        };

        // Prepare tasks for caller to send
        let target_count = targets.len();
        let tasks: Vec<GossipTask> = targets
            .into_iter()
            .map(|peer_addr| GossipTask {
                peer_addr,
                message: msg.clone(),
                current_sequence,
            })
            .collect();

        let peer_count = if let RegistryMessage::PeerListGossip { ref peers, .. } = msg {
            peers.len()
        } else {
            0
        };
        info!(
            targets = target_count,
            peer_count = peer_count,
            "peer list gossip round completed"
        );

        tasks
    }

    /// Handle incoming peer list gossip
    /// Returns candidates to connect to
    ///
    /// IMPORTANT: "Don't penalize the messenger" principle (Phase 4):
    /// - Unreachable peers in the list do NOT cause sender to be penalized
    /// - We only penalize the sender for INVALID data (bogon IPs, malformed data)
    /// - Backoff is applied to TARGET peers only, not the gossip source
    pub async fn on_peer_list_gossip(
        &self,
        peers: Vec<PeerInfoGossip>,
        sender_addr: &str,
        timestamp: u64,
    ) -> Vec<SocketAddr> {
        // Resource exhaustion protection - sender is sending suspicious data
        if peers.len() > Self::MAX_PEER_LIST_SIZE {
            warn!(
                size = peers.len(),
                max = Self::MAX_PEER_LIST_SIZE,
                sender = %sender_addr,
                "peer list too large, rejecting - potential attack"
            );
            // Note: We could penalize sender here, but for now just reject
            return vec![];
        }

        // Check if peer discovery is enabled
        if !self.config.enable_peer_discovery {
            debug!("peer discovery disabled, ignoring peer list gossip");
            return vec![];
        }

        let _now = current_timestamp();

        // DON'T PENALIZE THE MESSENGER:
        // Count bogon IPs to detect if sender is sending suspicious data
        // We only penalize for INVALID data, not for unreachable peers
        let mut bogon_count = 0;
        for peer_gossip in &peers {
            if let Ok(addr) = peer_gossip.address.parse::<SocketAddr>() {
                if !crate::net_security::is_safe_to_dial(
                    &addr,
                    self.config.allow_private_discovery,
                    self.config.allow_loopback_discovery,
                    self.config.allow_link_local_discovery,
                ) {
                    bogon_count += 1;
                }
            }
        }

        // If more than 50% of peers are bogons, log warning (could penalize sender)
        if !peers.is_empty() && bogon_count * 2 > peers.len() {
            warn!(
                bogon_count = bogon_count,
                total = peers.len(),
                sender = %sender_addr,
                "peer list contains mostly bogon IPs - sender may be malicious"
            );
            // Note: We choose to log but not block - could be misconfiguration
        }

        // Ingest peers into known_peers and get candidates
        let candidates = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Only update known_peers if peer discovery is enabled
            // OR if we're already connected to the peer (in gossip_state.peers)
            if gossip_state.peer_discovery.is_some() {
                // Update known_peers LRU cache and active peers
                for peer_gossip in &peers {
                    if let Some(peer_info) = PeerInfo::from_gossip(peer_gossip) {
                        // Security filter: do not ingest unsafe/bogon addresses into known_peers.
                        if !crate::net_security::is_safe_to_dial(
                            &peer_info.address,
                            self.config.allow_private_discovery,
                            self.config.allow_loopback_discovery,
                            self.config.allow_link_local_discovery,
                        ) {
                            continue;
                        }

                        // Conservative merge: only update if newer
                        if let Some(existing) = gossip_state.known_peers.get_mut(&peer_info.address)
                        {
                            // Only update if the incoming info is newer
                            if peer_gossip.last_success > existing.last_success {
                                existing.last_success = peer_gossip.last_success;
                                existing.last_attempt = peer_gossip.last_attempt;
                                // Don't overwrite local failure count
                            }
                            // Always update dns_name if gossip provides one and we don't have it
                            // (or update to latest if provided)
                            if peer_info.dns_name.is_some() {
                                existing.dns_name = peer_info.dns_name.clone();
                            }
                        } else {
                            // New peer, add to cache
                            gossip_state
                                .known_peers
                                .put(peer_info.address, peer_info.clone());
                        }

                        // Also update dns_name in active peers (gossip_state.peers)
                        // This ensures existing connected peers get DNS refresh capability
                        if peer_info.dns_name.is_some() {
                            if let Some(active_peer) =
                                gossip_state.peers.get_mut(&peer_info.address)
                            {
                                if active_peer.dns_name.is_none() {
                                    active_peer.dns_name = peer_info.dns_name;
                                    debug!(
                                        addr = %peer_info.address,
                                        dns_name = ?active_peer.dns_name,
                                        "Updated active peer with DNS name from gossip"
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                // Peer discovery disabled: only update DNS names for existing connections
                // Don't add new peers to known_peers
                for peer_gossip in &peers {
                    if let Some(peer_info) = PeerInfo::from_gossip(peer_gossip) {
                        // Only update dns_name in active peers (gossip_state.peers)
                        // This ensures existing connected peers get DNS refresh capability
                        // but we don't add them to known_peers
                        if peer_info.dns_name.is_some() {
                            if let Some(active_peer) =
                                gossip_state.peers.get_mut(&peer_info.address)
                            {
                                if active_peer.dns_name.is_none() {
                                    active_peer.dns_name = peer_info.dns_name;
                                    debug!(
                                        addr = %peer_info.address,
                                        dns_name = ?active_peer.dns_name,
                                        "Updated active peer with DNS name from gossip (peer discovery disabled)"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Get candidates from peer discovery manager
            // Note: PeerDiscovery filters out unsafe addresses via is_safe_to_dial()
            // but does NOT penalize the sender - only skips unsafe targets
            if let Some(ref mut discovery) = gossip_state.peer_discovery {
                discovery.on_peer_list_gossip(&peers)
            } else {
                vec![]
            }
        };

        debug!(
            peer_count = peers.len(),
            candidates = candidates.len(),
            sender = %sender_addr,
            timestamp = timestamp,
            "processed peer list gossip"
        );

        candidates
    }

    /// Prune stale peers from known_peers based on TTLs
    pub async fn prune_stale_peers(&self) {
        let now = current_timestamp();

        let mut gossip_state = self.gossip_state.lock().await;

        // Prune from known_peers based on TTLs
        let fail_ttl_secs = self.config.fail_ttl.as_secs();
        let stale_ttl_secs = self.config.stale_ttl.as_secs();

        // Collect keys to remove
        let to_remove: Vec<SocketAddr> = gossip_state
            .known_peers
            .iter()
            .filter(|(_, info)| {
                // Remove if:
                // 1. Failed and exceeded fail_ttl
                if info.failures > 0 {
                    if let Some(failure_time) = info.last_failure_time {
                        if now > failure_time.saturating_add(fail_ttl_secs) {
                            return true;
                        }
                    }
                }
                // 2. Stale (no success for stale_ttl)
                if info.last_success > 0 && now > info.last_success.saturating_add(stale_ttl_secs) {
                    return true;
                }
                false
            })
            .map(|(addr, _)| *addr)
            .collect();

        // Remove stale peers
        for addr in &to_remove {
            gossip_state.known_peers.pop(addr);
        }

        if !to_remove.is_empty() {
            debug!(
                removed = to_remove.len(),
                remaining = gossip_state.known_peers.len(),
                "pruned stale peers from known_peers"
            );
        }

        // Also prune from peer_discovery if enabled
        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            let stats = discovery.cleanup_expired(now);
            if stats.pending_removed > 0 || stats.failed_removed > 0 {
                debug!(
                    pending_removed = stats.pending_removed,
                    failed_removed = stats.failed_removed,
                    "peer discovery cleanup removed expired entries"
                );
            }
        }
    }

    /// Lookup advertised address for a NodeId
    /// First checks active peers, then falls back to known_peers
    pub async fn lookup_advertised_addr(&self, node_id: &crate::NodeId) -> Option<SocketAddr> {
        let gossip_state = self.gossip_state.lock().await;

        // First check active peers
        for (addr, peer_info) in gossip_state.peers.iter() {
            if peer_info.node_id.as_ref() == Some(node_id) {
                return Some(*addr);
            }
        }

        // Fallback to known_peers
        for (addr, peer_info) in gossip_state.known_peers.iter() {
            if peer_info.node_id.as_ref() == Some(node_id) {
                return Some(*addr);
            }
        }

        None
    }

    /// Lookup NodeId for a given address (active peers first, then known_peers, then
    /// direct routing configuration).
    pub async fn lookup_node_id(&self, addr: &SocketAddr) -> Option<crate::NodeId> {
        let mut gossip_state = self.gossip_state.lock().await;

        if let Some(peer_info) = gossip_state.peers.get(addr)
            && let Some(node_id) = peer_info.node_id
        {
            return Some(node_id);
        }

        if let Some(peer_info) = gossip_state.known_peers.get(addr)
            && let Some(node_id) = peer_info.node_id
        {
            return Some(node_id);
        }

        drop(gossip_state);

        // Full-sync actor locations are configured as direct routes rather than
        // gossip peers. Still derive the expected NodeId from that pinned PeerId
        // so address-based TLS dials cannot fall back to placeholder SNI.
        self.connection_pool
            .get_peer_id_by_addr(addr)
            .map(|peer_id| peer_id.to_node_id())
    }

    /// Connect-on-demand for actor messaging (Phase 4)
    ///
    /// This method allows connecting to a node for actor messaging even if the
    /// max_peers soft cap has been reached. The soft cap only limits automatic
    /// peer discovery, not direct actor communication.
    ///
    /// First checks active connections, then uses known_peers to look up the address.
    pub async fn ensure_connection_for_actor(
        &self,
        node_id: &crate::NodeId,
    ) -> Result<crate::connection_pool::ConnectionHandle<T>> {
        // Check if we have an active connection to the node already
        if let Some(addr) = self.lookup_advertised_addr(node_id).await {
            if self.has_active_connection(&addr).await {
                debug!(node_id = %node_id.fmt_short(), addr = %addr, "using existing connection for actor messaging");
                return self.get_connection(addr).await;
            }

            // Connect-on-demand: This can exceed max_peers soft cap
            // because actor messaging takes priority over peer discovery limits
            debug!(
                node_id = %node_id.fmt_short(),
                addr = %addr,
                "connect-on-demand for actor messaging (may exceed soft cap)"
            );
            return self.get_connection(addr).await;
        }

        Err(GossipError::ActorNotFound(format!(
            "no known address for node {}",
            node_id.fmt_short()
        )))
    }

    /// Check if we have an active connection to a peer
    /// Used for "local connection wins" - we trust our direct connection over gossip reports
    pub async fn has_active_connection(&self, addr: &SocketAddr) -> bool {
        let pool = &self.connection_pool;
        pool.has_connection(addr)
    }

    /// Mark a peer connection as established (clears failure state)
    pub async fn mark_peer_connected(&self, addr: SocketAddr) {
        {
            let mut gossip_state = self.gossip_state.lock().await;

            // Update known_peers
            if let Some(peer_info) = gossip_state.known_peers.get_mut(&addr) {
                peer_info.failures = 0;
                peer_info.last_failure_time = None;
                peer_info.last_success = current_timestamp();
                if let Some(node_id) = peer_info.node_id {
                    let _ = self.peer_capability_addr_to_node.upsert_sync(addr, node_id);
                    let caps = self
                        .peer_capabilities_by_node
                        .read_sync(&node_id, |_, v| *v);
                    if let Some(caps) = caps {
                        let _ = self.peer_capabilities.upsert_sync(addr, caps);
                    }
                }
            }

            // Update peer_discovery
            let should_track_mesh_time = self.config.mesh_formation_target > 0
                && gossip_state.mesh_formation_time_ms.is_none();

            if let Some(ref mut discovery) = gossip_state.peer_discovery {
                discovery.on_peer_connected(addr);

                if should_track_mesh_time {
                    let target = self.config.mesh_formation_target;
                    if discovery.connected_peer_count() >= target {
                        let elapsed_ms = self.start_instant.elapsed().as_millis() as u64;
                        gossip_state.mesh_formation_time_ms = Some(elapsed_ms);
                    }
                }
            }

            debug!(addr = %addr, "marked peer as connected");
        }

        // Notify peer connect handler (outgoing connections may only hit this path).
        if let Some(cell) = self.peer_connect_handler.load_full() {
            let peer_id = self.connection_pool.get_peer_id_by_addr(&addr);
            cell.handler.handle_peer_connect(addr, peer_id).await;
        }

        self.reset_udp_liveness_for_peer(addr).await;
    }

    /// Record that this peer was observed on an inbound connection accepted by this node.
    pub async fn mark_inbound_connection_observed(
        &self,
        peer_addr: SocketAddr,
        source: SocketAddr,
    ) {
        let now = current_timestamp();
        let mut gossip_state = self.gossip_state.lock().await;
        let peer = gossip_state.peers.entry(peer_addr).or_insert(PeerInfo {
            address: peer_addr,
            peer_address: None,
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: None,
            dns_name: None,
            failures: 0,
            last_attempt: now,
            last_success: now,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        });
        peer.inbound_observed = true;
        if source != peer_addr {
            peer.peer_address = Some(source);
        }
        peer.last_success = now;
    }

    /// Returns whether this node should attempt an outbound dial to `peer_addr`.
    pub async fn should_attempt_outbound_dial(&self, peer_addr: SocketAddr) -> bool {
        let gossip_state = self.gossip_state.lock().await;
        let Some(peer) = gossip_state.peers.get(&peer_addr) else {
            return true;
        };
        !self.should_suppress_outbound_retry_for_peer(peer)
    }

    /// Mark a peer connection as failed (applies backoff)
    /// Implements "local connection wins" - if we have an active connection, we trust it
    /// over gossip reports and skip marking as failed.
    pub async fn mark_peer_failed(&self, addr: SocketAddr) {
        // LOCAL CONNECTION WINS: If we have an active connection to this peer,
        // don't mark it as failed based on gossip reports from other nodes.
        // We trust our direct connection over third-party reports.
        {
            let pool = &self.connection_pool;
            if pool.has_connection(&addr) {
                debug!(
                    addr = %addr,
                    "local connection wins - skipping failure mark for connected peer"
                );
                return;
            }
        }

        let mut gossip_state = self.gossip_state.lock().await;

        let now = current_timestamp();

        // Update known_peers
        if let Some(peer_info) = gossip_state.known_peers.get_mut(&addr) {
            peer_info.failures = peer_info.failures.saturating_add(1);
            peer_info.last_failure_time = Some(now);
            peer_info.last_attempt = now;
        }

        // Update peer_discovery
        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            discovery.on_peer_failure(addr);
        }

        debug!(addr = %addr, "marked peer as failed");
    }

    /// Mark a peer as disconnected
    pub async fn mark_peer_disconnected(&self, addr: SocketAddr) {
        let mut gossip_state = self.gossip_state.lock().await;

        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            discovery.on_peer_disconnected(addr);
        }

        debug!(addr = %addr, "marked peer as disconnected");
    }

    /// Duplicate connection tie-breaker
    /// When both nodes try to connect simultaneously, use NodeId comparison:
    /// - Lower NodeId keeps outbound connection
    /// - Higher NodeId keeps inbound connection
    ///
    /// Returns true if this connection should be kept.
    pub fn should_keep_connection(
        &self,
        remote_peer_id: &crate::PeerId,
        is_outbound: bool,
    ) -> bool {
        let local_id = self.peer_id.to_node_id();
        let remote_id = remote_peer_id.to_node_id();

        match local_id.as_bytes().cmp(remote_id.as_bytes()) {
            std::cmp::Ordering::Less => is_outbound,
            std::cmp::Ordering::Greater => !is_outbound,
            std::cmp::Ordering::Equal => {
                // Same node ID shouldn't happen in practice
                warn!(local = %local_id, remote = %remote_id, "duplicate connection from same NodeId");
                true
            }
        }
    }

    /// Check if we already have a connection to a peer by peer ID
    pub async fn has_connection_to_peer(&self, peer_id: &crate::PeerId) -> bool {
        let pool = &self.connection_pool;
        pool.has_connection_by_peer_id(peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyPair, PeerId};
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::sync::Arc;
    use std::time::Duration;

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn test_peer_id(seed: &str) -> PeerId {
        KeyPair::new_for_testing(seed).peer_id()
    }

    fn test_location(addr: SocketAddr) -> RemoteActorLocation {
        RemoteActorLocation::new_with_peer(addr, test_peer_id("test_peer"))
    }

    fn test_config() -> GossipConfig {
        GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("registry_tests")),
            gossip_interval: Duration::from_millis(100),
            cleanup_interval: Duration::from_millis(200),
            peer_retry_interval: Duration::from_millis(50),
            immediate_propagation_enabled: true, // Enable for testing
            ..Default::default()
        }
    }

    #[test]
    fn enable_udp_rejects_unauthenticated_datagram_mode() {
        let keypair = KeyPair::new_for_testing("udp-disabled-registry");
        let mut registry = GossipRegistry::<()>::new(test_addr(0), test_config());

        let err = registry
            .enable_udp(keypair.to_secret_key())
            .expect_err("unauthenticated UDP mode must remain disabled");

        match err {
            GossipError::InvalidConfig(message) => {
                assert!(message.contains("disabled until inbound datagrams are authenticated"));
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn test_registry_change_serialization() {
        let location = test_location(test_addr(8080));
        let change = RegistryChange::ActorAdded {
            name: "test".to_string(),
            location,
            priority: RegistrationPriority::Immediate,
        };

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&change).unwrap();
        let deserialized: RegistryChange =
            rkyv::from_bytes::<RegistryChange, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

        match deserialized {
            RegistryChange::ActorAdded { name, .. } => {
                assert_eq!(name, "test");
            }
            _ => panic!("Wrong change type"),
        }
    }

    #[test]
    fn test_registry_delta_serialization() {
        let delta = RegistryDelta {
            since_sequence: 10,
            current_sequence: 15,
            changes: vec![],
            sender_peer_id: test_peer_id("test_peer"),
            wall_clock_time: 1000,
            precise_timing_nanos: 1_000_000_000_000, // 1000 seconds in nanoseconds
        };

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&delta).unwrap();
        let deserialized: RegistryDelta =
            rkyv::from_bytes::<RegistryDelta, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

        assert_eq!(deserialized.since_sequence, 10);
        assert_eq!(deserialized.current_sequence, 15);
        assert_eq!(deserialized.sender_peer_id, test_peer_id("test_peer"));
    }

    #[test]
    fn test_peer_health_status() {
        let status = PeerHealthStatus {
            is_alive: true,
            last_contact: 1000,
            failure_count: 2,
        };

        assert!(status.is_alive);
        assert_eq!(status.last_contact, 1000);
        assert_eq!(status.failure_count, 2);
    }

    #[test]
    fn test_registry_message_variants() {
        // Test DeltaGossip
        let delta = RegistryDelta {
            since_sequence: 1,
            current_sequence: 2,
            changes: vec![],
            sender_peer_id: test_peer_id("test_peer"),
            wall_clock_time: 1000,
            precise_timing_nanos: 1_000_000_000_000, // 1000 seconds in nanoseconds
        };
        let msg = RegistryMessage::DeltaGossip { delta };
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();
        let deserialized: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES
        match deserialized {
            RegistryMessage::DeltaGossip { .. } => (),
            _ => panic!("Wrong message type"),
        }

        // Test FullSyncRequest
        let msg = RegistryMessage::FullSyncRequest {
            sender_peer_id: test_peer_id("test_peer"),
            sender_bind_addr: Some("127.0.0.1:9000".to_string()),
            sequence: 10,
            wall_clock_time: 1000,
        };
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();
        let deserialized: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES
        match deserialized {
            RegistryMessage::FullSyncRequest { sequence, .. } => {
                assert_eq!(sequence, 10);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_registry_message_wire_fixture_hash_is_stable() {
        let fixture_messages = vec![
            RegistryMessage::FullSyncRequest {
                sender_peer_id: test_peer_id("fixture-peer-a"),
                sender_bind_addr: Some("127.0.0.1:9200".to_string()),
                sequence: 42,
                wall_clock_time: 1_700_000_001,
            },
            RegistryMessage::ImmediateAck {
                actor_name: "fixture_actor".to_string(),
                success: true,
            },
            RegistryMessage::PeerHealthQuery {
                sender: test_peer_id("fixture-peer-b"),
                target_peer: test_peer_id("fixture-peer-c").to_string(),
                timestamp: 1_700_000_123,
            },
        ];

        let mut hasher = Sha256::new();
        for msg in fixture_messages {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(bytes.as_ref());
        }

        let digest = hex::encode(hasher.finalize());
        assert_eq!(
            digest,
            "a5e348bd7402159dd18e6422c7f6a13f99093f857c922bab66b3ebfd2b98a1ad"
        );
    }

    #[test]
    fn test_registry_stats() {
        let stats = RegistryStats {
            local_actors: 5,
            known_actors: 10,
            active_peers: 3,
            failed_peers: 1,
            total_gossip_rounds: 100,
            current_sequence: 100,
            uptime_seconds: 3600,
            last_gossip_timestamp: 1000,
            delta_exchanges: 50,
            full_sync_exchanges: 10,
            delta_history_size: 20,
            avg_delta_size: 5.5,
            // Peer discovery metrics (Phase 5)
            discovered_peers: 15,
            failed_discovery_attempts: 2,
            avg_mesh_connectivity: 0.2,
            mesh_formation_time_ms: Some(500),
        };

        assert_eq!(stats.local_actors, 5);
        assert_eq!(stats.known_actors, 10);
        assert_eq!(stats.active_peers, 3);
        assert_eq!(stats.failed_peers, 1);
        assert_eq!(stats.discovered_peers, 15);
        assert_eq!(stats.failed_discovery_attempts, 2);
    }

    #[test]
    fn test_peer_info() {
        let mut peer = PeerInfo {
            address: test_addr(8080),
            peer_address: Some(test_addr(8081)),
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: None,
            dns_name: None,
            failures: 0,
            last_attempt: 100,
            last_success: 100,
            last_sequence: 5,
            last_sent_sequence: 5,
            consecutive_deltas: 3,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        };

        assert_eq!(peer.address, test_addr(8080));
        assert_eq!(peer.failures, 0);

        peer.failures += 1;
        peer.last_failure_time = Some(200);
        assert_eq!(peer.failures, 1);
        assert_eq!(peer.last_failure_time, Some(200));
    }

    #[test]
    fn test_deduplicate_changes() {
        let location1 = test_location(test_addr(8080));
        let location2 = test_location(test_addr(8081));

        let changes = vec![
            RegistryChange::ActorAdded {
                name: "actor1".to_string(),
                location: location1.clone(),
                priority: RegistrationPriority::Normal,
            },
            RegistryChange::ActorAdded {
                name: "actor1".to_string(),
                location: location2,
                priority: RegistrationPriority::Immediate,
            },
            RegistryChange::ActorRemoved {
                name: "actor2".to_string(),
                vector_clock: crate::VectorClock::new(),
                removing_node_id: crate::SecretKey::generate().public(),
                priority: RegistrationPriority::Normal,
            },
        ];

        let deduped = GossipRegistry::<()>::deduplicate_changes(changes);
        assert_eq!(deduped.len(), 2); // Only one change per actor

        // Verify we kept the last change for actor1
        let actor1_changes: Vec<_> = deduped
            .iter()
            .filter(|c| match c {
                RegistryChange::ActorAdded { name, .. } => name == "actor1",
                _ => false,
            })
            .collect();
        assert_eq!(actor1_changes.len(), 1);
    }

    /// C7 regression: when an `ActorRemoved` arrives before its
    /// corresponding `ActorAdded` (e.g. delta reordering across peers),
    /// the actor must not be resurrected. The causal comparison is
    /// driven by vector clocks; on a causal tie, remove wins.
    #[test]
    fn test_deduplicate_changes_remove_wins_on_reorder() {
        let node = test_peer_id("c7-node").to_node_id();
        let location = test_location(test_addr(9001));
        // Bump the `ActorAdded` vector clock once (representing the
        // initial registration).
        location.vector_clock.increment(node);

        let add = RegistryChange::ActorAdded {
            name: "x".to_string(),
            location: location.clone(),
            priority: RegistrationPriority::Normal,
        };

        // The remove was issued *after* the add — bump the same clock
        // again so it causally happens-after.
        let removal_clock = location.vector_clock.clone();
        removal_clock.increment(node);
        let remove = RegistryChange::ActorRemoved {
            name: "x".to_string(),
            vector_clock: removal_clock,
            removing_node_id: node,
            priority: RegistrationPriority::Immediate,
        };

        // Out-of-order arrival: remove first, then add. The legacy
        // implementation kept whichever was inserted last (the add),
        // resurrecting the deleted actor.
        let reordered = vec![remove.clone(), add.clone()];
        let deduped = GossipRegistry::<()>::deduplicate_changes(reordered);
        assert_eq!(deduped.len(), 1, "should collapse same-name changes");
        assert!(
            matches!(deduped[0], RegistryChange::ActorRemoved { .. }),
            "ActorRemoved must win over earlier ActorAdded regardless of \
             iteration order (got {:?})",
            deduped[0]
        );

        // Sanity: in-order arrival still produces ActorRemoved as the
        // winner.
        let in_order = vec![add, remove];
        let deduped = GossipRegistry::<()>::deduplicate_changes(in_order);
        assert_eq!(deduped.len(), 1);
        assert!(matches!(deduped[0], RegistryChange::ActorRemoved { .. }));
    }

    /// C7 tie case: two concurrent changes (equal vector clocks) for
    /// the same actor — one Add, one Remove — must resolve as Remove
    /// (remove-wins on tie / tombstone preference).
    #[test]
    fn test_deduplicate_changes_concurrent_remove_wins() {
        let node_a = test_peer_id("c7-node-a").to_node_id();
        let node_b = test_peer_id("c7-node-b").to_node_id();
        // Two clocks that are concurrent: each has incremented a
        // different node, so neither happens-before the other.
        let add_loc = test_location(test_addr(9002));
        add_loc.vector_clock.increment(node_a);
        let remove_clock = crate::VectorClock::new();
        remove_clock.increment(node_b);

        let add = RegistryChange::ActorAdded {
            name: "y".to_string(),
            location: add_loc,
            priority: RegistrationPriority::Normal,
        };
        let remove = RegistryChange::ActorRemoved {
            name: "y".to_string(),
            vector_clock: remove_clock,
            removing_node_id: node_b,
            priority: RegistrationPriority::Immediate,
        };

        for order in [vec![add.clone(), remove.clone()], vec![remove, add]] {
            let deduped = GossipRegistry::<()>::deduplicate_changes(order);
            assert_eq!(deduped.len(), 1);
            assert!(
                matches!(deduped[0], RegistryChange::ActorRemoved { .. }),
                "concurrent Add+Remove for same name must collapse to Remove"
            );
        }
    }

    #[test]
    fn test_get_change_actor_name() {
        let location = test_location(test_addr(8080));
        let add_change = RegistryChange::ActorAdded {
            name: "test_actor".to_string(),
            location,
            priority: RegistrationPriority::Normal,
        };
        assert_eq!(
            GossipRegistry::<()>::get_change_actor_name(&add_change),
            "test_actor"
        );

        let remove_change = RegistryChange::ActorRemoved {
            name: "test_actor".to_string(),
            vector_clock: crate::VectorClock::new(),
            removing_node_id: crate::SecretKey::generate().public(),
            priority: RegistrationPriority::Normal,
        };
        assert_eq!(
            GossipRegistry::<()>::get_change_actor_name(&remove_change),
            "test_actor"
        );
    }

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        assert_eq!(registry.bind_addr, test_addr(8080));
        assert!(!registry.is_shutdown().await);
    }

    // #[tokio::test]
    // async fn test_add_bootstrap_peers() {
    //     let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
    //     let peers = vec![test_addr(8081), test_addr(8082), test_addr(8080)]; // Including self

    //     registry.add_bootstrap_peers(peers).await;

    //     let gossip_state = registry.gossip_state.lock().await;
    //     assert_eq!(gossip_state.peers.len(), 2); // Should exclude self
    //     assert!(gossip_state.peers.contains_key(&test_addr(8081)));
    //     assert!(gossip_state.peers.contains_key(&test_addr(8082)));
    //     assert!(!gossip_state.peers.contains_key(&test_addr(8080))); // Self excluded
    // }

    #[tokio::test]
    async fn test_add_peer() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        registry.add_peer(test_addr(8081)).await;
        registry.add_peer(test_addr(8080)).await; // Try to add self
        registry.add_peer(test_addr(8081)).await; // Try to add duplicate

        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.peers.len(), 1);
        assert!(gossip_state.peers.contains_key(&test_addr(8081)));
    }

    #[tokio::test]
    async fn test_register_actor() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        let result = registry
            .register_actor("test_actor".to_string(), location)
            .await;
        assert!(result.is_ok());

        // Verify actor is in local_actors
        assert!(
            registry
                .actor_state
                .local_actors
                .contains_sync("test_actor")
        );

        // Verify pending change was created
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.pending_changes.len(), 1);
    }

    #[tokio::test]
    async fn test_register_actor_duplicate() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        registry
            .register_actor("test_actor".to_string(), location.clone())
            .await
            .unwrap();

        // Try to register again
        let result = registry
            .register_actor("test_actor".to_string(), location)
            .await;
        assert!(matches!(result, Err(GossipError::ActorAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_register_actor_with_priority() {
        let mut config = test_config();
        config.immediate_propagation_enabled = false; // Disable to test queuing
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        let location = test_location(test_addr(9001));
        let result = registry
            .register_actor_with_priority(
                "urgent_actor".to_string(),
                location,
                RegistrationPriority::Immediate,
            )
            .await;
        assert!(result.is_ok());

        // Verify urgent change was created (not cleared since immediate propagation is disabled)
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.urgent_changes.len(), 1);
        // Regular gossip carries the same state change without transport urgency.
        assert_eq!(gossip_state.pending_changes.len(), 1);
        match &gossip_state.pending_changes[0] {
            RegistryChange::ActorAdded { priority, .. } => {
                assert_eq!(*priority, RegistrationPriority::Normal);
            }
            other => panic!("expected ActorAdded, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_immediate_priority_does_not_leak_into_regular_delta_bootstrap() {
        let mut config = test_config();
        config.immediate_propagation_enabled = false;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        registry.add_peer(test_addr(8081)).await;
        registry
            .register_actor_with_priority(
                "urgent_actor".to_string(),
                test_location(test_addr(9001)),
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert_eq!(tasks.len(), 1);
        match &tasks[0].message {
            RegistryMessage::DeltaGossip { delta } => {
                assert!(
                    !delta.changes.iter().any(|change| match change {
                        RegistryChange::ActorAdded { priority, .. }
                        | RegistryChange::ActorRemoved { priority, .. } =>
                            priority.should_trigger_immediate_gossip(),
                    }),
                    "scheduled gossip must not re-emit one-shot immediate priority: {:?}",
                    delta.changes
                );
            }
            other => panic!("expected delta gossip, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_immediate_actor_snapshot_is_rebroadcast_as_regular_gossip() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let mut location = test_location(test_addr(9001));
        location.priority = RegistrationPriority::Immediate;
        registry
            .actor_state
            .known_actors
            .upsert_sync("remote_urgent_actor".to_string(), location);

        let (local_actors, known_actors) = registry.snapshot_actor_maps();
        let gossip_state = registry.gossip_state.lock().await;
        let delta = registry
            .create_delta_from_state(&gossip_state, &local_actors, &known_actors, 0)
            .await
            .unwrap();

        assert_eq!(delta.changes.len(), 1);
        match &delta.changes[0] {
            RegistryChange::ActorAdded { priority, .. } => {
                assert_eq!(*priority, RegistrationPriority::Normal);
            }
            other => panic!("expected ActorAdded, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_unregister_actor() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        registry
            .register_actor("test_actor".to_string(), location)
            .await
            .unwrap();

        let removed = registry.unregister_actor("test_actor").await.unwrap();
        assert!(removed.is_some());

        // Verify actor is removed
        assert!(
            !registry
                .actor_state
                .local_actors
                .contains_sync("test_actor")
        );

        // Verify removal change was created
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.pending_changes.len(), 2); // Add + Remove
    }

    #[tokio::test]
    async fn test_lookup_actor() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Test local actor
        let location = test_location(test_addr(9001));
        registry
            .register_actor("local_actor".to_string(), location.clone())
            .await
            .unwrap();

        let found = registry.lookup_actor("local_actor").await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().socket_addr().unwrap(), test_addr(9001));

        // Test non-existent actor
        let not_found = registry.lookup_actor("missing_actor").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_lookup_actor_ttl() {
        let mut config = test_config();
        config.actor_ttl = Duration::from_millis(50); // Very short TTL for testing
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        // Add a known actor with old timestamp
        let mut location = test_location(test_addr(9001));
        location.wall_clock_time = current_timestamp() - 100; // Old timestamp

        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync("old_actor".to_string(), location);

        // Should not find due to TTL
        let found = registry.lookup_actor("old_actor").await;
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_get_stats() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Add some data
        registry
            .register_actor("actor1".to_string(), test_location(test_addr(9001)))
            .await
            .unwrap();
        registry.add_peer(test_addr(8081)).await;

        let stats = registry.get_stats().await;
        assert_eq!(stats.local_actors, 1);
        assert_eq!(stats.active_peers, 1);
        assert_eq!(stats.failed_peers, 0);
        assert_eq!(stats.uptime_seconds, 0); // Just created
    }

    #[tokio::test]
    async fn test_mesh_formation_metric_records_when_threshold_met() {
        let mut config = test_config();
        config.enable_peer_discovery = true;
        config.mesh_formation_target = 1;

        let registry = GossipRegistry::<()>::new(test_addr(8080), config);
        registry.mark_peer_connected(test_addr(8082)).await;

        let stats = registry.get_stats().await;
        assert!(
            stats.mesh_formation_time_ms.is_some(),
            "mesh formation metric should be recorded when threshold met"
        );
    }

    #[tokio::test]
    async fn test_apply_delta() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "remote_actor".to_string(),
                location,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: test_peer_id("node_b"),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        registry.apply_delta(delta).await.unwrap();

        // Verify actor was added to known_actors
        assert!(
            registry
                .actor_state
                .known_actors
                .contains_sync("remote_actor")
        );
    }

    #[tokio::test]
    async fn test_apply_delta_skip_local() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Register local actor
        let local_location = test_location(test_addr(9001));
        registry
            .register_actor("local_actor".to_string(), local_location)
            .await
            .unwrap();

        // Try to override with remote update
        let remote_location = test_location(test_addr(9002));
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "local_actor".to_string(),
                location: remote_location,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: test_peer_id("node_b"),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        registry.apply_delta(delta).await.unwrap();

        // Verify local actor wasn't overridden
        let actor = registry
            .actor_state
            .local_actors
            .read_sync("local_actor", |_, v| v.clone())
            .unwrap();
        assert_eq!(actor.socket_addr().unwrap(), test_addr(9001)); // Still local address
    }

    #[tokio::test]
    async fn test_should_use_delta_state() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let gossip_state = registry.gossip_state.lock().await;

        // New peer should use full sync
        let new_peer = PeerInfo {
            address: test_addr(8081),
            peer_address: None,
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: None,
            dns_name: None,
            failures: 0,
            last_attempt: 0,
            last_success: 0,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        };
        assert!(!registry.should_use_delta_state(&gossip_state, &new_peer));

        // Peer with history should use delta
        let established_peer = PeerInfo {
            address: test_addr(8081),
            peer_address: None,
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: None,
            dns_name: None,
            failures: 0,
            last_attempt: 100,
            last_success: 100,
            last_sequence: 5,
            last_sent_sequence: 5,
            consecutive_deltas: 10,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        };
        // Add some peers to make it not a small cluster
        drop(gossip_state);
        for i in 0..10 {
            registry.add_peer(test_addr(8090 + i)).await;
        }
        let gossip_state = registry.gossip_state.lock().await;
        assert!(registry.should_use_delta_state(&gossip_state, &established_peer));
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_no_peers() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_with_peers() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Add peers
        registry.add_peer(test_addr(8081)).await;
        registry.add_peer(test_addr(8082)).await;

        // Add some changes
        registry
            .register_actor("actor1".to_string(), test_location(test_addr(9001)))
            .await
            .unwrap();

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert!(!tasks.is_empty());
        assert!(tasks.len() <= 2); // Should gossip to available peers
    }

    #[tokio::test]
    async fn test_shutdown() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Add some data
        registry
            .register_actor("actor1".to_string(), test_location(test_addr(9001)))
            .await
            .unwrap();
        registry.add_peer(test_addr(8081)).await;

        assert!(!registry.is_shutdown().await);

        registry.shutdown().await;

        assert!(registry.is_shutdown().await);

        // Verify data was cleared
        assert!(registry.actor_state.local_actors.is_empty());
        assert!(registry.actor_state.known_actors.is_empty());

        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peers.is_empty());
    }

    #[tokio::test]
    async fn test_handle_peer_connection_failure() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Add a peer
        registry.add_peer(test_addr(8081)).await;

        // Simulate failure
        registry
            .handle_peer_connection_failure(test_addr(8081))
            .await
            .unwrap();

        // Check peer is marked as failed
        let gossip_state = registry.gossip_state.lock().await;
        let peer = gossip_state.peers.get(&test_addr(8081)).unwrap();
        assert_eq!(peer.failures, registry.config.max_peer_failures);
        assert!(peer.last_failure_time.is_some());
    }

    #[tokio::test]
    async fn test_handle_peer_connection_failure_invokes_disconnect_handler_with_peer_id() {
        use crate::registry::PeerDisconnectHandler;
        use futures::future::BoxFuture;
        use std::net::SocketAddr;
        use std::sync::Arc;
        use tokio::sync::oneshot;

        struct TestHandler {
            tx: tokio::sync::Mutex<Option<oneshot::Sender<(SocketAddr, Option<crate::PeerId>)>>>,
        }

        impl PeerDisconnectHandler for TestHandler {
            fn handle_peer_disconnect(
                &self,
                peer_addr: SocketAddr,
                peer_id: Option<crate::PeerId>,
            ) -> BoxFuture<'_, ()> {
                Box::pin(async move {
                    if let Some(tx) = self.tx.lock().await.take() {
                        let _ = tx.send((peer_addr, peer_id));
                    }
                })
            }
        }

        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer_addr = test_addr(8081);
        let peer_id = crate::KeyPair::new_for_testing("disconnect-handler-test").peer_id();

        // Seed peer mapping so handle_peer_connection_failure resolves peer_id by addr.
        let _ = registry
            .connection_pool
            .addr_to_peer_id
            .upsert_sync(peer_addr, peer_id.clone());
        let _ = registry
            .connection_pool
            .peer_id_to_addr
            .upsert_sync(peer_id.clone(), peer_addr);

        let (tx, rx) = oneshot::channel();
        registry
            .set_peer_disconnect_handler(Arc::new(TestHandler {
                tx: tokio::sync::Mutex::new(Some(tx)),
            }))
            .await;

        registry
            .handle_peer_connection_failure(peer_addr)
            .await
            .unwrap();

        let (got_addr, got_peer_id) = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got_addr, peer_addr);
        assert_eq!(got_peer_id, Some(peer_id));
    }

    #[tokio::test]
    async fn test_handle_peer_connection_failure_invokes_disconnect_handler_without_peer_id_when_unknown()
     {
        use crate::registry::PeerDisconnectHandler;
        use futures::future::BoxFuture;
        use std::net::SocketAddr;
        use std::sync::Arc;
        use tokio::sync::oneshot;

        struct TestHandler {
            tx: tokio::sync::Mutex<Option<oneshot::Sender<(SocketAddr, Option<crate::PeerId>)>>>,
        }

        impl PeerDisconnectHandler for TestHandler {
            fn handle_peer_disconnect(
                &self,
                peer_addr: SocketAddr,
                peer_id: Option<crate::PeerId>,
            ) -> BoxFuture<'_, ()> {
                Box::pin(async move {
                    if let Some(tx) = self.tx.lock().await.take() {
                        let _ = tx.send((peer_addr, peer_id));
                    }
                })
            }
        }

        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer_addr = test_addr(8081);

        let (tx, rx) = oneshot::channel();
        registry
            .set_peer_disconnect_handler(Arc::new(TestHandler {
                tx: tokio::sync::Mutex::new(Some(tx)),
            }))
            .await;

        registry
            .handle_peer_connection_failure(peer_addr)
            .await
            .unwrap();

        let (got_addr, got_peer_id) = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got_addr, peer_addr);
        assert_eq!(got_peer_id, None);
    }

    #[tokio::test]
    async fn test_cleanup_stale_actors() {
        let mut config = test_config();
        config.actor_ttl = Duration::from_millis(50);
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        // Add old actor
        let mut old_location = test_location(test_addr(9001));
        old_location.wall_clock_time = current_timestamp() - 100;

        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync("old_actor".to_string(), old_location);

        registry.cleanup_stale_actors().await;

        // Verify old actor was removed
        assert!(!registry.actor_state.known_actors.contains_sync("old_actor"));
    }

    #[tokio::test]
    async fn test_cleanup_dead_peers() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        // Add a failed peer with old failure time
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                test_addr(8081),
                PeerInfo {
                    address: test_addr(8081),
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp() - 100),
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        // Add some actors from the failed peer
        {
            let _ = registry
                .actor_state
                .known_actors
                .upsert_sync("peer_actor".to_string(), test_location(test_addr(9001)));

            let mut gossip_state = registry.gossip_state.lock().await;
            let mut actors = HashSet::new();
            actors.insert("peer_actor".to_string());
            gossip_state.peer_to_actors.insert(test_addr(8081), actors);
        }

        registry.cleanup_dead_peers().await;

        // Verify peer is KEPT but its actors were removed
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peers.contains_key(&test_addr(8081))); // Peer is still there!
        assert!(!gossip_state.peer_to_actors.contains_key(&test_addr(8081))); // But actors mapping is gone

        drop(gossip_state);
        assert!(
            !registry
                .actor_state
                .known_actors
                .contains_sync("peer_actor")
        ); // Actors are cleaned up
    }

    #[tokio::test]
    async fn test_merge_full_sync() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let mut remote_local = HashMap::new();
        remote_local.insert(
            "remote_actor1".to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9001), test_peer_id("remote_actor1")),
        );

        let mut remote_known = HashMap::new();
        remote_known.insert(
            "remote_actor2".to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9002), test_peer_id("remote_actor2")),
        );
        remote_known.insert(
            "forged_local_actor".to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9003), registry.peer_id.clone()),
        );

        registry
            .merge_full_sync(
                remote_local,
                remote_known,
                test_peer_id("merge_full_sync_sender"),
                test_addr(8081),
                1,
                current_timestamp(),
            )
            .await;

        // Verify actors were merged
        assert!(
            registry
                .actor_state
                .known_actors
                .contains_sync("remote_actor1")
        );
        assert!(
            registry
                .actor_state
                .known_actors
                .contains_sync("remote_actor2")
        );

        // Actor hosts learned from full-sync payloads are direct routing targets.
        // They must not become periodic gossip peers, otherwise stale actor
        // advertisements can create retry loops to retired service addresses.
        let gossip_state = registry.gossip_state.lock().await;
        assert!(!gossip_state.peers.contains_key(&test_addr(9001)));
        assert!(!gossip_state.peers.contains_key(&test_addr(9002)));
        assert!(!gossip_state.peers.contains_key(&test_addr(9003)));
        drop(gossip_state);

        // Direct routes still pin the expected NodeId for TLS verification.
        // This prevents address-based actor lookups from using placeholder SNI
        // when a full-sync actor location claims a specific PeerId.
        assert_eq!(
            registry.lookup_node_id(&test_addr(9001)).await,
            Some(test_peer_id("remote_actor1").to_node_id())
        );
        assert_eq!(
            registry.lookup_node_id(&test_addr(9002)).await,
            Some(test_peer_id("remote_actor2").to_node_id())
        );
        assert_eq!(
            registry.lookup_node_id(&test_addr(9003)).await,
            Some(registry.peer_id.to_node_id())
        );

        let remote_actor1 = registry
            .actor_state
            .known_actors
            .read_sync("remote_actor1", |_, v| v.clone())
            .expect("remote_actor1 location");
        let actor1_addr = registry
            .connection_pool
            .peer_id_to_addr
            .read_sync(&remote_actor1.peer_id, |_, v| *v);
        assert_eq!(actor1_addr, Some(test_addr(9001)));
    }

    #[tokio::test]
    async fn test_pending_failure() {
        let pending = PendingFailure {
            first_detected: 1000,
            consensus_deadline: 1005,
            query_sent: false,
        };

        assert_eq!(pending.first_detected, 1000);
        assert_eq!(pending.consensus_deadline, 1005);
        assert!(!pending.query_sent);
    }

    #[tokio::test]
    async fn test_historical_delta() {
        let delta = HistoricalDelta {
            sequence: 10,
            changes: vec![],
            wall_clock_time: 1000,
        };

        assert_eq!(delta.sequence, 10);
        assert!(delta.changes.is_empty());
        assert_eq!(delta.wall_clock_time, 1000);
    }

    #[tokio::test]
    async fn test_gossip_task() {
        let task = GossipTask {
            peer_addr: test_addr(8081),
            message: RegistryMessage::FullSyncRequest {
                sender_peer_id: test_peer_id("test_peer"),
                sender_bind_addr: Some("127.0.0.1:9000".to_string()),
                sequence: 10,
                wall_clock_time: 1000,
            },
            current_sequence: 10,
        };

        assert_eq!(task.peer_addr, test_addr(8081));
        assert_eq!(task.current_sequence, 10);
    }

    #[tokio::test]
    async fn test_gossip_result() {
        let result = GossipResult {
            peer_addr: test_addr(8081),
            sent_sequence: 10,
            outcome: Ok(None),
        };

        assert_eq!(result.peer_addr, test_addr(8081));
        assert_eq!(result.sent_sequence, 10);
        assert!(result.outcome.is_ok());
    }

    #[tokio::test]
    async fn test_trigger_immediate_gossip() {
        let mut config = test_config();
        config.immediate_propagation_enabled = true;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        // NOTE: Don't add a peer here - tests only the no-peers path.
        // (Adding a peer would require TLS setup which is tested in integration tests)

        // Add urgent change
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state
                .urgent_changes
                .push(RegistryChange::ActorAdded {
                    name: "urgent".to_string(),
                    location: test_location(test_addr(9001)),
                    priority: RegistrationPriority::Immediate,
                });
        }

        // Should return Ok when no peers are available
        let result = registry.trigger_immediate_gossip().await;
        assert!(result.is_ok());

        // Urgent changes are cleared (taken) even when there are no peers
        // (current implementation takes changes before checking for peers)
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.urgent_changes.is_empty());
    }

    #[tokio::test]
    async fn test_stream_assembly_copies_chunks_into_buffer() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let header0 = crate::StreamHeader {
            stream_id: 42,
            total_size: 6,
            chunk_size: 3,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };

        registry.start_stream_assembly(header0, None, None).await;
        registry.add_stream_chunk(header0, vec![1, 2, 3]).await;

        let header1 = crate::StreamHeader {
            chunk_index: 1,
            ..header0
        };
        registry.add_stream_chunk(header1, vec![4, 5, 6]).await;

        let result = registry
            .complete_stream_assembly(header0.stream_id)
            .await
            .expect("stream assembly should complete");

        assert_eq!(result.data.as_ref(), &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_enforce_bounds() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Add many pending changes
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            for i in 0..2000 {
                gossip_state
                    .pending_changes
                    .push(RegistryChange::ActorAdded {
                        name: format!("actor{}", i),
                        location: test_location(test_addr(9000 + i as u16)),
                        priority: RegistrationPriority::Normal,
                    });
            }
        }

        registry.enforce_bounds().await;

        // Verify bounds were enforced
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.pending_changes.len() <= 1000);
    }

    #[tokio::test]
    async fn test_check_peer_consensus() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        // Add a pending failure
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            let pending = PendingFailure {
                first_detected: current_timestamp() - 10,
                consensus_deadline: current_timestamp() - 5, // Past deadline
                query_sent: true,
            };
            gossip_state
                .pending_peer_failures
                .insert(test_addr(8081), pending);

            // Add some health reports
            let mut reports = HashMap::new();
            reports.insert(
                test_addr(8080),
                PeerHealthStatus {
                    is_alive: false,
                    last_contact: current_timestamp(),
                    failure_count: 1,
                },
            );
            gossip_state
                .peer_health_reports
                .insert(test_addr(8081), reports);
        }

        registry.check_peer_consensus().await;

        // Verify pending failure was processed
        let gossip_state = registry.gossip_state.lock().await;
        assert!(
            !gossip_state
                .pending_peer_failures
                .contains_key(&test_addr(8081))
        );
    }

    // =================== Phase 1: PeerListGossip Tests ===================

    #[test]
    fn test_peer_list_gossip_serialization() {
        // Test rkyv round-trip for PeerListGossip message
        let peer1 = PeerInfoGossip {
            address: "127.0.0.1:8080".to_string(),
            peer_address: Some("192.168.1.100:8080".to_string()),
            node_id: None,
            failures: 0,
            last_attempt: 1000,
            last_success: 1000,
            dns_name: None,
        };
        let peer2 = PeerInfoGossip {
            address: "127.0.0.1:8081".to_string(),
            peer_address: None,
            node_id: None,
            failures: 2,
            last_attempt: 2000,
            last_success: 1500,
            dns_name: None,
        };

        let msg = RegistryMessage::PeerListGossip {
            peers: vec![peer1, peer2],
            timestamp: 12345,
            sender_addr: "127.0.0.1:9000".to_string(),
        };

        // Serialize
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();

        // Deserialize
        let deserialized: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

        // Verify
        match deserialized {
            RegistryMessage::PeerListGossip {
                peers,
                timestamp,
                sender_addr,
            } => {
                assert_eq!(peers.len(), 2);
                assert_eq!(peers[0].address, "127.0.0.1:8080");
                assert_eq!(
                    peers[0].peer_address,
                    Some("192.168.1.100:8080".to_string())
                );
                assert_eq!(peers[0].failures, 0);
                assert_eq!(peers[1].address, "127.0.0.1:8081");
                assert_eq!(peers[1].peer_address, None);
                assert_eq!(peers[1].failures, 2);
                assert_eq!(timestamp, 12345);
                assert_eq!(sender_addr, "127.0.0.1:9000");
            }
            _ => panic!("Expected PeerListGossip message"),
        }
    }

    #[tokio::test]
    async fn test_peer_list_gossip_tasks_created() {
        let config = GossipConfig {
            enable_peer_discovery: true,
            peer_gossip_interval: None,
            max_peer_gossip_targets: 1,
            allow_loopback_discovery: true,
            ..test_config()
        };

        let registry = GossipRegistry::<()>::new("127.0.0.1:9000".parse().unwrap(), config);
        registry.add_peer(test_addr(9001)).await;
        registry.add_peer(test_addr(9002)).await;

        let tasks = registry.gossip_peer_list().await;
        assert_eq!(tasks.len(), 1);

        match &tasks[0].message {
            RegistryMessage::PeerListGossip { sender_addr, .. } => {
                assert_eq!(sender_addr, "127.0.0.1:9000");
            }
            _ => panic!("Expected PeerListGossip message"),
        }
    }

    #[tokio::test]
    async fn test_on_peer_list_gossip_ingests_known_peers_and_candidates() {
        let config = GossipConfig {
            enable_peer_discovery: true,
            allow_loopback_discovery: true,
            max_peers: 1,
            ..test_config()
        };

        let registry = GossipRegistry::<()>::new("127.0.0.1:9000".parse().unwrap(), config);

        let node_id = crate::SecretKey::generate().public();
        let peers = vec![
            PeerInfoGossip {
                address: "127.0.0.1:9001".to_string(),
                peer_address: None,
                node_id: Some(node_id),
                failures: 0,
                last_attempt: 1,
                last_success: 1,
                dns_name: None,
            },
            PeerInfoGossip {
                address: "127.0.0.1:9002".to_string(),
                peer_address: None,
                node_id: None,
                failures: 0,
                last_attempt: 1,
                last_success: 1,
                dns_name: None,
            },
        ];

        let candidates = registry
            .on_peer_list_gossip(peers, "127.0.0.1:9003", 1)
            .await;

        assert!(candidates.len() <= 1);

        let mut gossip_state = registry.gossip_state.lock().await;
        let addr_9001: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        assert!(gossip_state.known_peers.get(&addr_9001).is_some());
    }

    #[tokio::test]
    async fn test_on_peer_list_gossip_does_not_ingest_private_addrs_when_disallowed() {
        let config = GossipConfig {
            enable_peer_discovery: true,
            allow_loopback_discovery: true,
            allow_private_discovery: false,
            max_peers: 1,
            ..test_config()
        };

        let registry = GossipRegistry::<()>::new("127.0.0.1:9000".parse().unwrap(), config);

        let peers = vec![PeerInfoGossip {
            address: "10.0.0.1:9001".to_string(),
            peer_address: None,
            node_id: None,
            failures: 0,
            last_attempt: 1,
            last_success: 1,
            dns_name: None,
        }];

        let _candidates = registry
            .on_peer_list_gossip(peers, "127.0.0.1:9003", 1)
            .await;

        let mut gossip_state = registry.gossip_state.lock().await;
        let private_addr: SocketAddr = "10.0.0.1:9001".parse().unwrap();
        assert!(
            gossip_state.known_peers.get(&private_addr).is_none(),
            "private address should not be ingested into known_peers when allow_private_discovery=false"
        );
    }

    #[tokio::test]
    async fn test_refresh_peer_dns_rejects_unsafe_resolution_results() {
        // Use a non-loopback current address so it won't match localhost results.
        let peer_addr: SocketAddr = "1.2.3.4:5000".parse().unwrap();

        let config = GossipConfig {
            allow_loopback_discovery: false, // security filter should reject localhost
            ..test_config()
        };

        let registry = GossipRegistry::<()>::new("127.0.0.1:9000".parse().unwrap(), config);

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: Some("localhost:5000".to_string()),
                    failures: 0,
                    last_attempt: 1,
                    last_success: 1,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        let updated = registry.refresh_peer_dns(peer_addr).await;
        assert!(
            updated.is_none(),
            "expected DNS refresh to be rejected due to loopback resolution"
        );

        let gossip_state = registry.gossip_state.lock().await;
        assert!(
            gossip_state.peers.contains_key(&peer_addr),
            "peer entry should not be migrated when DNS resolves only to unsafe addresses"
        );
        assert!(
            !gossip_state
                .peers
                .contains_key(&"127.0.0.1:5000".parse().unwrap()),
            "unsafe resolved address should not be added"
        );
    }

    #[tokio::test]
    async fn test_peers_snapshot_does_not_gossip_unsafe_known_peers() {
        let config = GossipConfig {
            allow_loopback_discovery: true, // keep test simple; self is loopback
            allow_private_discovery: false, // private peers should be filtered out
            enable_peer_discovery: true,
            ..test_config()
        };

        let registry = GossipRegistry::<()>::new("127.0.0.1:9000".parse().unwrap(), config);

        // Simulate attacker-controlled state injection: unsafe private address present in known_peers.
        // Pre-fix, peers_snapshot() would include this and re-gossip it.
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            let private_addr: SocketAddr = "10.0.0.1:9001".parse().unwrap();
            gossip_state.known_peers.put(
                private_addr,
                PeerInfo {
                    address: private_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 0,
                    last_attempt: 1,
                    last_success: 1,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        let snapshot = registry.peers_snapshot().await;
        assert!(
            !snapshot.iter().any(|p| p.address == "10.0.0.1:9001"),
            "unsafe private known_peers entries must not be re-gossiped"
        );
    }

    #[test]
    fn test_peer_info_local_factory() {
        let addr = test_addr(8080);
        let peer_info = PeerInfo::local(addr);

        assert_eq!(peer_info.address, addr);
        assert!(peer_info.peer_address.is_none());
        assert!(peer_info.node_id.is_none());
        assert_eq!(peer_info.failures, 0);
        assert!(peer_info.last_success > 0); // Should have current timestamp
        assert!(peer_info.last_attempt > 0);
        assert_eq!(peer_info.last_sequence, 0);
        assert_eq!(peer_info.last_sent_sequence, 0);
        assert_eq!(peer_info.consecutive_deltas, 0);
        assert!(peer_info.last_failure_time.is_none());
    }

    #[test]
    fn test_peer_info_gossip_conversion() {
        let addr = test_addr(8080);
        let peer_info = PeerInfo {
            address: addr,
            peer_address: Some(test_addr(8081)),
            inbound_observed: false,
            outbound_dial_success: false,
            node_id: None,
            dns_name: None,
            failures: 3,
            last_attempt: 1000,
            last_success: 900,
            last_sequence: 10,
            last_sent_sequence: 8,
            consecutive_deltas: 5,
            last_failure_time: Some(950),
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        };

        // Convert to gossip format
        let gossip = peer_info.to_gossip();
        assert_eq!(gossip.address, "127.0.0.1:8080");
        assert_eq!(gossip.peer_address, Some("127.0.0.1:8081".to_string()));
        assert_eq!(gossip.failures, 3);
        assert_eq!(gossip.last_attempt, 1000);
        assert_eq!(gossip.last_success, 900);

        // Convert back from gossip format
        let restored = PeerInfo::from_gossip(&gossip).unwrap();
        assert_eq!(restored.address, addr);
        assert_eq!(restored.peer_address, Some(test_addr(8081)));
        assert_eq!(restored.failures, 3);
        assert_eq!(restored.last_attempt, 1000);
        assert_eq!(restored.last_success, 900);
        // These fields are not transmitted over gossip, so they should be reset
        assert_eq!(restored.last_sequence, 0);
        assert_eq!(restored.last_sent_sequence, 0);
        assert_eq!(restored.consecutive_deltas, 0);
        assert!(restored.last_failure_time.is_none());
    }

    #[test]
    fn test_peer_info_gossip_serialization() {
        let gossip = PeerInfoGossip {
            address: "10.0.0.1:9000".to_string(),
            peer_address: Some("192.168.1.50:9000".to_string()),
            node_id: None,
            failures: 5,
            last_attempt: 5000,
            last_success: 4000,
            dns_name: Some("my-service.default.svc.cluster.local:9000".to_string()),
        };

        // Serialize
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&gossip).unwrap();

        // Deserialize
        let deserialized: PeerInfoGossip =
            rkyv::from_bytes::<PeerInfoGossip, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

        assert_eq!(deserialized.address, "10.0.0.1:9000");
        assert_eq!(
            deserialized.peer_address,
            Some("192.168.1.50:9000".to_string())
        );
        assert_eq!(deserialized.failures, 5);
        assert_eq!(deserialized.last_attempt, 5000);
        assert_eq!(deserialized.last_success, 4000);
        assert_eq!(
            deserialized.dns_name,
            Some("my-service.default.svc.cluster.local:9000".to_string())
        );
    }

    // Tests for resolve_peer_addr function
    #[test]
    fn test_resolve_peer_addr_with_valid_bind_addr() {
        let tcp_source: SocketAddr = "192.168.1.100:54321".parse().unwrap();
        let result = super::resolve_peer_addr(Some("10.0.0.1:9000"), tcp_source);
        assert_eq!(result, "10.0.0.1:9000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_resolve_peer_addr_with_none() {
        let tcp_source: SocketAddr = "192.168.1.100:54321".parse().unwrap();
        let result = super::resolve_peer_addr(None, tcp_source);
        assert_eq!(result, tcp_source);
    }

    #[test]
    fn test_resolve_peer_addr_with_invalid_string() {
        let tcp_source: SocketAddr = "192.168.1.100:54321".parse().unwrap();
        let result = super::resolve_peer_addr(Some("not-an-address"), tcp_source);
        assert_eq!(result, tcp_source);
    }

    #[test]
    fn test_resolve_peer_addr_with_unspecified_ip() {
        // 0.0.0.0 should use TCP source IP with bind_addr port
        let tcp_source: SocketAddr = "192.168.1.100:54321".parse().unwrap();
        let result = super::resolve_peer_addr(Some("0.0.0.0:9000"), tcp_source);
        // Should use TCP source IP (192.168.1.100) with bind_addr port (9000)
        assert_eq!(result, "192.168.1.100:9000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_resolve_peer_addr_with_ipv6_unspecified() {
        let tcp_source: SocketAddr = "[::1]:54321".parse().unwrap();
        let result = super::resolve_peer_addr(Some("[::]:9000"), tcp_source);
        // Should use TCP source IP (::1) with bind_addr port (9000)
        assert_eq!(result, "[::1]:9000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_resolve_peer_addr_loopback_from_remote() {
        // If peer claims loopback (127.0.0.1) but TCP source is remote, reject it
        let tcp_source: SocketAddr = "192.168.1.100:54321".parse().unwrap();
        let result = super::resolve_peer_addr(Some("127.0.0.1:9000"), tcp_source);
        // Should use TCP source IP (192.168.1.100) with bind_addr port (9000)
        assert_eq!(result, "192.168.1.100:9000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_resolve_peer_addr_loopback_from_loopback() {
        // If both bind_addr and TCP source are loopback, accept it (local testing)
        let tcp_source: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let result = super::resolve_peer_addr(Some("127.0.0.1:9000"), tcp_source);
        // Should accept loopback since TCP source is also loopback
        assert_eq!(result, "127.0.0.1:9000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_resolve_peer_addr_ipv6_loopback_from_remote() {
        // IPv6 loopback from remote should also be rejected
        let tcp_source: SocketAddr = "[2001:db8::1]:54321".parse().unwrap();
        let result = super::resolve_peer_addr(Some("[::1]:9000"), tcp_source);
        // Should use TCP source IP with bind_addr port
        assert_eq!(result, "[2001:db8::1]:9000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_resolve_peer_addr_backwards_compat_none() {
        // Test backwards compatibility - older nodes won't send sender_bind_addr
        let tcp_source: SocketAddr = "10.0.0.50:12345".parse().unwrap();
        let result = super::resolve_peer_addr(None, tcp_source);
        // Should fall back to TCP source
        assert_eq!(result, tcp_source);
    }

    fn read_known_actor(registry: &GossipRegistry, name: &str) -> Option<RemoteActorLocation> {
        registry
            .actor_state
            .known_actors
            .read_sync(name, |_, v| v.clone())
    }

    #[tokio::test]
    async fn test_apply_delta_concurrent_add_add_is_order_independent() {
        let config = test_config();
        let reg1 = GossipRegistry::<()>::new(test_addr(7001), config.clone());
        let reg2 = GossipRegistry::<()>::new(test_addr(7002), config.clone());

        let actor = "actor.concurrent";
        let peer_a = test_peer_id("peer_a");
        let peer_b = test_peer_id("peer_b");

        let mut loc_a = RemoteActorLocation::new_with_peer(test_addr(9001), peer_a.clone());
        let mut loc_b = RemoteActorLocation::new_with_peer(test_addr(9002), peer_b.clone());

        // Force a pure deterministic tie-break (vector clocks are concurrent by construction).
        loc_a.wall_clock_time = 123;
        loc_b.wall_clock_time = 123;
        loc_a.metadata = vec![1, 2, 3];
        loc_b.metadata = vec![1, 2, 3];
        loc_a.local_registration_time = 0;
        loc_b.local_registration_time = 0;

        let expected = if loc_a.node_id > loc_b.node_id {
            loc_a.clone()
        } else {
            loc_b.clone()
        };

        let delta_a = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor.to_string(),
                location: loc_a.clone(),
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: peer_a.clone(),
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };

        let delta_b = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor.to_string(),
                location: loc_b.clone(),
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: peer_b.clone(),
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };

        // Apply A then B.
        reg1.apply_delta(delta_a.clone()).await.unwrap();
        reg1.apply_delta(delta_b.clone()).await.unwrap();
        let got1 = read_known_actor(&reg1, actor).expect("actor should exist");

        // Apply B then A.
        reg2.apply_delta(delta_b).await.unwrap();
        reg2.apply_delta(delta_a).await.unwrap();
        let got2 = read_known_actor(&reg2, actor).expect("actor should exist");

        assert_eq!(got1, got2);
        assert_eq!(got1, expected);
    }

    #[tokio::test]
    async fn test_apply_delta_stale_self_announcement_is_ignored() {
        let reg = GossipRegistry::<()>::new(test_addr(7003), test_config());
        let actor = "actor.self";

        let mut loc = RemoteActorLocation::new_with_peer(test_addr(9001), reg.peer_id.clone());
        loc.wall_clock_time = 123;
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor.to_string(),
                location: loc,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: test_peer_id("some_sender"),
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };

        reg.apply_delta(delta).await.unwrap();
        assert!(read_known_actor(&reg, actor).is_none());
    }

    #[tokio::test]
    async fn test_apply_delta_duplicate_delivery_is_idempotent() {
        let reg = GossipRegistry::<()>::new(test_addr(7004), test_config());
        let actor = "actor.dupe";
        let peer = test_peer_id("peer_dupe");

        let mut loc = RemoteActorLocation::new_with_peer(test_addr(9001), peer.clone());
        loc.wall_clock_time = 123;

        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor.to_string(),
                location: loc,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: peer,
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };

        reg.apply_delta(delta.clone()).await.unwrap();
        let after_first = read_known_actor(&reg, actor).expect("actor should exist");
        reg.apply_delta(delta).await.unwrap();
        let after_second = read_known_actor(&reg, actor).expect("actor should exist");
        assert_eq!(after_first, after_second);
    }

    #[tokio::test]
    async fn test_merge_full_sync_ignores_stale_sequence() {
        let reg = GossipRegistry::<()>::new(test_addr(7005), test_config());
        let sender_addr = test_addr(7006);
        let sender_peer_id = test_peer_id("sender-stale");

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                sender_addr,
                PeerInfo {
                    address: sender_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 0,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 10,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        let actor = "actor.stale.fullsync";
        let remote_loc =
            RemoteActorLocation::new_with_peer(test_addr(9101), test_peer_id("remote"));
        let mut remote_local = HashMap::new();
        remote_local.insert(actor.to_string(), remote_loc);

        reg.merge_full_sync(
            remote_local,
            HashMap::new(),
            sender_peer_id,
            sender_addr,
            5,
            0,
        )
        .await;

        assert!(read_known_actor(&reg, actor).is_none());
    }

    #[tokio::test]
    async fn test_merge_full_sync_omission_does_not_remove_actor_moved_to_different_peer() {
        let reg = GossipRegistry::<()>::new(test_addr(7007), test_config());
        let actor = "actor.move";

        let sender1_addr = test_addr(7008);
        let sender2_addr = test_addr(7009);
        let sender1_peer_id = test_peer_id("sender1");
        let sender2_peer_id = test_peer_id("sender2");

        // First, sender1 advertises the actor (establishes peer_to_actors attribution).
        let loc1 = RemoteActorLocation::new_with_peer(test_addr(9201), sender1_peer_id.clone());
        let mut remote_local_1 = HashMap::new();
        remote_local_1.insert(actor.to_string(), loc1.clone());
        reg.merge_full_sync(
            remote_local_1,
            HashMap::new(),
            sender1_peer_id.clone(),
            sender1_addr,
            1,
            0,
        )
        .await;

        // Then sender2 moves the actor to itself (causally after).
        let mut loc2 = RemoteActorLocation::new_with_peer(test_addr(9202), sender2_peer_id.clone());
        loc2.vector_clock.merge(&loc1.vector_clock);
        loc2.vector_clock.increment(loc2.node_id);
        loc2.wall_clock_time = loc1.wall_clock_time;
        loc2.metadata = loc1.metadata.clone();
        loc2.local_registration_time = loc1.local_registration_time;

        let delta2 = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor.to_string(),
                location: loc2.clone(),
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: sender2_peer_id.clone(),
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };
        reg.apply_delta(delta2).await.unwrap();

        let got_after_move = read_known_actor(&reg, actor).expect("actor should exist");
        assert_eq!(got_after_move.peer_id, sender2_peer_id);

        // Now sender1 sends a full sync omitting the actor. This must NOT remove it, since it
        // no longer belongs to sender1.
        reg.merge_full_sync(
            HashMap::new(),
            HashMap::new(),
            sender1_peer_id,
            sender1_addr,
            2,
            0,
        )
        .await;

        let got_after_omit = read_known_actor(&reg, actor).expect("actor should still exist");
        assert_eq!(got_after_omit.peer_id, sender2_peer_id);

        // Sanity: sender2 omission would be allowed to remove.
        reg.merge_full_sync(
            HashMap::new(),
            HashMap::new(),
            sender2_peer_id,
            sender2_addr,
            2,
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_uses_full_sync_when_peer_too_far_behind() {
        let mut cfg = test_config();
        cfg.small_cluster_threshold = 0;
        let reg = GossipRegistry::<()>::new(test_addr(7010), cfg);
        let peer_addr = test_addr(7011);

        {
            let mut state = reg.gossip_state.lock().await;
            state.gossip_sequence = 10;
            state.delta_history = vec![
                HistoricalDelta {
                    sequence: 8,
                    changes: Vec::new(),
                    wall_clock_time: 0,
                },
                HistoricalDelta {
                    sequence: 9,
                    changes: Vec::new(),
                    wall_clock_time: 0,
                },
            ];
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 0,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 1, // too far behind oldest_available=8
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        let tasks = reg.prepare_gossip_round().await.unwrap();
        assert_eq!(tasks.len(), 1);
        match &tasks[0].message {
            RegistryMessage::FullSync { .. } => {}
            other => panic!("expected full sync, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_suppresses_inbound_only_undialable_retry() {
        let mut cfg = test_config();
        cfg.nat_role_reconnect_enabled = true;
        cfg.max_peer_failures = 1;
        cfg.small_cluster_threshold = 0;
        let reg = GossipRegistry::<()>::new(test_addr(7014), cfg);
        let peer_addr: SocketAddr = "10.0.0.9:9100".parse().unwrap();

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: true,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 1,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(0),
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        let tasks = reg.prepare_gossip_round().await.unwrap();
        assert!(
            tasks.is_empty(),
            "inbound-only undialable peer should be suppressed from outbound retries"
        );
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_suppression_prevents_retry_thrash_over_many_rounds() {
        let mut cfg = test_config();
        cfg.nat_role_reconnect_enabled = true;
        cfg.max_peer_failures = 1;
        cfg.peer_retry_interval = Duration::from_secs(0);
        cfg.small_cluster_threshold = 0;
        let reg = GossipRegistry::<()>::new(test_addr(7016), cfg);
        let peer_addr: SocketAddr = "10.0.0.11:9102".parse().unwrap();

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: true,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 1,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(0),
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        for _ in 0..64 {
            let tasks = reg.prepare_gossip_round().await.unwrap();
            assert!(
                tasks.is_empty(),
                "suppressed inbound-only undialable peer must never be scheduled for retry"
            );
        }

        let state = reg.gossip_state.lock().await;
        let peer = state
            .peers
            .get(&peer_addr)
            .expect("peer should still exist");
        assert_eq!(
            peer.last_attempt, 0,
            "retry suppression should avoid scheduling dial attempts across rounds"
        );
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_backoff_timing_window() {
        let mut cfg = test_config();
        cfg.max_peer_failures = 1;
        cfg.peer_retry_interval = Duration::from_secs(2);
        cfg.small_cluster_threshold = 0;
        let retry_secs = cfg.peer_retry_interval.as_secs();
        let reg = GossipRegistry::<()>::new(test_addr(7020), cfg);
        let peer_addr = test_addr(7021);
        let now = current_timestamp();

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 1,
                    last_attempt: now,
                    last_success: now,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(now),
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        let blocked = reg.prepare_gossip_round().await.unwrap();
        assert!(
            blocked.is_empty(),
            "peer should remain in backoff window before retry interval elapses"
        );

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.get_mut(&peer_addr).unwrap().last_attempt =
                now.saturating_sub(retry_secs + 1);
        }
        let reopened = reg.prepare_gossip_round().await.unwrap();
        assert_eq!(
            reopened.len(),
            1,
            "peer should become retry-eligible after retry interval"
        );
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_mixed_nat_roles_only_suppresses_eligible_peers() {
        let mut cfg = test_config();
        cfg.nat_role_reconnect_enabled = true;
        cfg.max_peer_failures = 1;
        cfg.peer_retry_interval = Duration::from_secs(0);
        cfg.max_gossip_peers = 64;
        cfg.small_cluster_threshold = 0;
        let reg = GossipRegistry::<()>::new(test_addr(7030), cfg);

        let mut suppressed = HashSet::new();
        let mut eligible = HashSet::new();
        {
            let mut state = reg.gossip_state.lock().await;
            for idx in 0..20u16 {
                let addr: SocketAddr = if idx < 10 {
                    format!("10.10.0.{}:{}", 10 + idx, 9100 + idx)
                        .parse()
                        .unwrap()
                } else {
                    test_addr(7300 + idx)
                };
                let peer = PeerInfo {
                    address: addr,
                    peer_address: None,
                    inbound_observed: idx < 10,
                    outbound_dial_success: idx % 3 == 0 && idx < 10,
                    node_id: None,
                    dns_name: None,
                    failures: 1,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(0),
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                };
                if peer.inbound_observed && !peer.outbound_dial_success && idx < 10 {
                    suppressed.insert(addr);
                } else {
                    eligible.insert(addr);
                }
                state.peers.insert(addr, peer);
            }
        }

        let mut scheduled = HashSet::new();
        for _ in 0..64 {
            let tasks = reg.prepare_gossip_round().await.unwrap();
            for task in tasks {
                assert!(
                    !suppressed.contains(&task.peer_addr),
                    "suppressed inbound-only undialable peer should never be selected"
                );
                scheduled.insert(task.peer_addr);
            }
        }

        assert!(
            scheduled.iter().any(|addr| eligible.contains(addr)),
            "expected at least one eligible peer to be scheduled across rounds"
        );
    }

    fn normalized_registry_message_for_wire_compare(mut msg: RegistryMessage) -> RegistryMessage {
        match &mut msg {
            RegistryMessage::DeltaGossip { delta }
            | RegistryMessage::DeltaGossipResponse { delta } => {
                delta.wall_clock_time = 0;
                delta.precise_timing_nanos = 0;
            }
            RegistryMessage::FullSyncRequest {
                sender_bind_addr,
                wall_clock_time,
                ..
            } => {
                *sender_bind_addr = None;
                *wall_clock_time = 0;
            }
            RegistryMessage::FullSync {
                sender_bind_addr,
                wall_clock_time,
                ..
            }
            | RegistryMessage::FullSyncResponse {
                sender_bind_addr,
                wall_clock_time,
                ..
            } => {
                *sender_bind_addr = None;
                *wall_clock_time = 0;
            }
            RegistryMessage::PeerHealthReport { timestamp, .. }
            | RegistryMessage::PeerHealthQuery { timestamp, .. }
            | RegistryMessage::PeerListGossip { timestamp, .. } => {
                *timestamp = 0;
            }
            RegistryMessage::ImmediateAck { .. } | RegistryMessage::ActorMessage { .. } => {}
        }
        msg
    }

    #[tokio::test]
    async fn test_nat_role_policy_is_internal_only_no_wire_message_drift() {
        let mut cfg_off = test_config();
        cfg_off.nat_role_reconnect_enabled = false;
        cfg_off.small_cluster_threshold = 0;

        let mut cfg_on = cfg_off.clone();
        cfg_on.nat_role_reconnect_enabled = true;

        let reg_off = GossipRegistry::<()>::new(test_addr(7040), cfg_off);
        let reg_on = GossipRegistry::<()>::new(test_addr(7041), cfg_on);
        let peer_addr = test_addr(7042);
        let template_peer = PeerInfo {
            address: peer_addr,
            peer_address: None,
            inbound_observed: false,
            outbound_dial_success: true,
            node_id: None,
            dns_name: None,
            failures: 0,
            last_attempt: 0,
            last_success: 0,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
        };
        {
            let mut off_state = reg_off.gossip_state.lock().await;
            off_state.peers.insert(peer_addr, template_peer.clone());
        }
        {
            let mut on_state = reg_on.gossip_state.lock().await;
            on_state.peers.insert(peer_addr, template_peer);
        }

        let mut off_tasks = reg_off.prepare_gossip_round().await.unwrap();
        let mut on_tasks = reg_on.prepare_gossip_round().await.unwrap();
        assert_eq!(off_tasks.len(), 1);
        assert_eq!(on_tasks.len(), 1);

        let off_msg = normalized_registry_message_for_wire_compare(off_tasks.remove(0).message);
        let on_msg = normalized_registry_message_for_wire_compare(on_tasks.remove(0).message);

        let off_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&off_msg).unwrap();
        let on_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&on_msg).unwrap();
        assert_eq!(
            off_bytes.as_ref(),
            on_bytes.as_ref(),
            "nat reconnect role policy must not alter wire message encoding"
        );
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_keeps_retry_for_outbound_dialed_peer() {
        let mut cfg = test_config();
        cfg.nat_role_reconnect_enabled = true;
        cfg.max_peer_failures = 1;
        cfg.small_cluster_threshold = 0;
        let reg = GossipRegistry::<()>::new(test_addr(7015), cfg);
        let peer_addr: SocketAddr = "10.0.0.10:9101".parse().unwrap();

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: true,
                    outbound_dial_success: true,
                    node_id: None,
                    dns_name: None,
                    failures: 1,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(0),
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        let tasks = reg.prepare_gossip_round().await.unwrap();
        assert_eq!(
            tasks.len(),
            1,
            "outbound-dialed peer should remain retry-eligible even if currently undialable"
        );
    }

    #[test]
    fn test_is_practically_dialable_from_here_matrix() {
        let reg_loopback = GossipRegistry::<()>::new(test_addr(8099), test_config());

        assert!(reg_loopback.is_practically_dialable_from_here(test_addr(9000)));
        assert!(!reg_loopback.is_practically_dialable_from_here("127.0.0.1:0".parse().unwrap()));
        assert!(
            !reg_loopback.is_practically_dialable_from_here(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                9000
            ))
        );
        assert!(!reg_loopback.is_practically_dialable_from_here("224.0.0.1:9000".parse().unwrap()));
        assert!(!reg_loopback.is_practically_dialable_from_here("10.1.2.3:9000".parse().unwrap()));
        assert!(!reg_loopback.is_practically_dialable_from_here("[::1]:9000".parse().unwrap()));

        let reg_private_v4 =
            GossipRegistry::<()>::new("10.10.0.1:9000".parse().unwrap(), test_config());
        assert!(
            reg_private_v4.is_practically_dialable_from_here("10.10.0.2:9001".parse().unwrap())
        );
        assert!(
            !reg_private_v4.is_practically_dialable_from_here("127.0.0.1:9001".parse().unwrap())
        );

        let reg_loopback_v6 = GossipRegistry::<()>::new(
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9002),
            test_config(),
        );
        assert!(reg_loopback_v6.is_practically_dialable_from_here("[::1]:9003".parse().unwrap()));
        assert!(
            !reg_loopback_v6.is_practically_dialable_from_here("[fd00::1]:9003".parse().unwrap())
        );

        let reg_ula_v6 =
            GossipRegistry::<()>::new("[fd00::100]:9004".parse().unwrap(), test_config());
        assert!(reg_ula_v6.is_practically_dialable_from_here("[fd00::101]:9005".parse().unwrap()));
        assert!(!reg_ula_v6.is_practically_dialable_from_here("[fe80::1]:9005".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_should_attempt_outbound_dial_role_matrix() {
        let mut cfg = test_config();
        cfg.nat_role_reconnect_enabled = true;
        let reg = GossipRegistry::<()>::new(test_addr(8110), cfg);
        let peer_addr: SocketAddr = "10.1.1.9:9100".parse().unwrap();

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: true,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 0,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        assert!(
            !reg.should_attempt_outbound_dial(peer_addr).await,
            "inbound-only undialable peer should be suppressed"
        );

        {
            let mut state = reg.gossip_state.lock().await;
            state
                .peers
                .get_mut(&peer_addr)
                .unwrap()
                .outbound_dial_success = true;
        }
        assert!(
            reg.should_attempt_outbound_dial(peer_addr).await,
            "outbound-dialed peer should stay retry-eligible"
        );

        {
            let mut state = reg.gossip_state.lock().await;
            let peer = state.peers.get_mut(&peer_addr).unwrap();
            peer.outbound_dial_success = false;
            peer.inbound_observed = false;
        }
        assert!(
            reg.should_attempt_outbound_dial(peer_addr).await,
            "peer without inbound-only role should remain retry-eligible"
        );

        assert!(
            reg.should_attempt_outbound_dial(test_addr(8199)).await,
            "unknown peer should default to retry-eligible"
        );
    }

    #[tokio::test]
    async fn test_should_attempt_outbound_dial_allows_live_connection_even_if_undialable() {
        let mut cfg = test_config();
        cfg.nat_role_reconnect_enabled = true;
        let reg = GossipRegistry::<()>::new(test_addr(8111), cfg);
        let peer_addr: SocketAddr = "10.1.1.10:9101".parse().unwrap();

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: true,
                    outbound_dial_success: false,
                    node_id: None,
                    dns_name: None,
                    failures: 0,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }
        assert!(!reg.should_attempt_outbound_dial(peer_addr).await);

        let (io, _peer_io) = tokio::io::duplex(1024);
        let (stream_handle, _writer_task, _reader_task) =
            crate::connection_pool::LockFreeStreamHandle::new(
                io,
                peer_addr,
                crate::connection_pool::ChannelId::Global,
                crate::connection_pool::BufferConfig::default(),
                None,
                None,
            );
        let mut conn = crate::connection_pool::LockFreeConnection::new(
            peer_addr,
            crate::connection_pool::ConnectionDirection::Outbound,
        );
        conn.stream_handle = Some(Arc::new(stream_handle));
        conn.set_state(crate::connection_pool::ConnectionState::Connected);
        reg.connection_pool
            .index_connection_by_addr(peer_addr, Arc::new(conn));

        assert!(
            reg.should_attempt_outbound_dial(peer_addr).await,
            "live connection should bypass retry suppression"
        );
    }

    #[tokio::test]
    async fn test_mark_inbound_connection_observed_preserves_outbound_success() {
        let mut cfg = test_config();
        cfg.nat_role_reconnect_enabled = true;
        let reg = GossipRegistry::<()>::new(test_addr(8112), cfg);
        let peer_addr: SocketAddr = "10.1.1.11:9102".parse().unwrap();
        let source_addr: SocketAddr = "203.0.113.10:58000".parse().unwrap();

        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: true,
                    node_id: None,
                    dns_name: None,
                    failures: 0,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                    last_dns_refresh_attempt: None,
            last_response_received: crate::current_timestamp(),
                },
            );
        }

        reg.mark_inbound_connection_observed(peer_addr, source_addr)
            .await;

        let state = reg.gossip_state.lock().await;
        let peer = state.peers.get(&peer_addr).expect("peer should exist");
        assert!(peer.inbound_observed);
        assert!(peer.outbound_dial_success);
        assert_eq!(peer.peer_address, Some(source_addr));
    }

    #[tokio::test]
    async fn test_full_sync_response_encoding_is_stable_across_insertion_order() {
        let reg = GossipRegistry::<()>::new(test_addr(7012), test_config());

        let l1 = RemoteActorLocation::new_with_peer(test_addr(9301), test_peer_id("p1"));
        let l2 = RemoteActorLocation::new_with_peer(test_addr(9302), test_peer_id("p2"));

        let mut m1 = HashMap::new();
        m1.insert("b".to_string(), l2.clone());
        m1.insert("a".to_string(), l1.clone());

        let mut m2 = HashMap::new();
        m2.insert("a".to_string(), l1);
        m2.insert("b".to_string(), l2);

        let msg1 = reg
            .create_full_sync_response_from_state(&m1, &HashMap::new(), 1)
            .await;
        let msg2 = reg
            .create_full_sync_response_from_state(&m2, &HashMap::new(), 1)
            .await;

        let b1 = rkyv::to_bytes::<rkyv::rancor::Error>(&msg1).unwrap();
        let b2 = rkyv::to_bytes::<rkyv::rancor::Error>(&msg2).unwrap();
        assert_eq!(b1.as_ref(), b2.as_ref());
    }

    #[tokio::test]
    async fn test_delta_bootstrap_encoding_is_stable_across_insertion_order() {
        let reg = GossipRegistry::<()>::new(test_addr(7013), test_config());

        let l1 = RemoteActorLocation::new_with_peer(test_addr(9401), test_peer_id("p1"));
        let l2 = RemoteActorLocation::new_with_peer(test_addr(9402), test_peer_id("p2"));

        let mut local1 = HashMap::new();
        local1.insert("b".to_string(), l2.clone());
        local1.insert("a".to_string(), l1.clone());

        let mut local2 = HashMap::new();
        local2.insert("a".to_string(), l1);
        local2.insert("b".to_string(), l2);

        let state = reg.gossip_state.lock().await;
        let msg1 = reg
            .create_delta_response_from_state(&state, &local1, &HashMap::new(), 0)
            .await
            .unwrap();
        let msg2 = reg
            .create_delta_response_from_state(&state, &local2, &HashMap::new(), 0)
            .await
            .unwrap();

        // The delta response includes high-resolution timing fields, so raw bytes are expected
        // to differ between subsequent calls. What must be stable is the ordering/content of
        // the snapshot changes produced for bootstrap (`since_sequence == 0`).
        let RegistryMessage::DeltaGossipResponse { delta: d1 } = msg1 else {
            panic!("expected delta gossip response");
        };
        let RegistryMessage::DeltaGossipResponse { delta: d2 } = msg2 else {
            panic!("expected delta gossip response");
        };

        let names1: Vec<String> = d1
            .changes
            .iter()
            .map(|c| match c {
                RegistryChange::ActorAdded { name, .. } => name.clone(),
                RegistryChange::ActorRemoved { name, .. } => name.clone(),
            })
            .collect();
        let names2: Vec<String> = d2
            .changes
            .iter()
            .map(|c| match c {
                RegistryChange::ActorAdded { name, .. } => name.clone(),
                RegistryChange::ActorRemoved { name, .. } => name.clone(),
            })
            .collect();

        assert_eq!(names1, names2);
        assert_eq!(names1, vec!["a".to_string(), "b".to_string()]);
    }
}
