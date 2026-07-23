use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task::AbortHandle;

use arc_swap::ArcSwapOption;
use futures::future::BoxFuture;
use lru::LruCache;
use scc::HashMap as SccHashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify};

use rand::seq::SliceRandom;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tracing::{debug, info, trace, warn};

use crate::{
    GossipConfig, GossipError, GossipNodeId, PeerHealthMode, PeerId, RegistrationPriority,
    RemoteActorLocation, Result,
    connection_pool::ConnectionPool,
    current_timestamp,
    handshake::PeerCapabilities,
    peer_discovery::{PeerDiscovery, PeerDiscoveryConfig},
};

pub const CLOCK_CALIBRATION_INTERVAL_NS: u64 = 60_000_000_000;
pub const CLOCK_CALIBRATION_PROBE_TIMEOUT_NS: u64 = 1_000_000_000;
pub const CLOCK_CALIBRATION_STALE_AFTER_NS: u64 = 180_000_000_000;

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
    removing_node_id: &crate::GossipNodeId,
    removal_clock: &crate::VectorClock,
    existing: &RemoteActorLocation,
) -> bool {
    use std::cmp::Ordering;

    // Stable total order for concurrent removal vs existing state.
    // Compare vector-clocks by their sorted representation first, then node IDs.
    // (VectorClock::to_vec is stable-sorted by GossipNodeId.)
    match removal_clock.to_vec().cmp(&existing.vector_clock.to_vec()) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => removing_node_id > &existing.node_id,
    }
}

#[inline]
fn owner_recovery_wins_tombstone(
    location: &RemoteActorLocation,
    sender_peer_id: &PeerId,
    tombstone: &crate::VectorClock,
) -> bool {
    // A peer-death tombstone is created by an observer. A direct authenticated
    // announcement from the actor owner is the recovery signal after a
    // transient disconnect, even when the actor itself did not re-register.
    // Also allow owner-clock advancement, which older equality-only recovery
    // checks rejected when the tombstone's observer component made the clocks
    // concurrent.
    location.peer_id == *sender_peer_id
        && location.vector_clock.get(&location.node_id) >= tombstone.get(&location.node_id)
}

#[inline]
fn actor_location_belongs_to_peer(
    location: &RemoteActorLocation,
    peer_addr: SocketAddr,
    peer_info: Option<&PeerInfo>,
) -> bool {
    if let Some(node_id) = peer_info.and_then(|info| info.node_id.as_ref()) {
        return location.node_id == *node_id || location.peer_id.to_node_id() == *node_id;
    }

    location
        .address
        .parse::<SocketAddr>()
        .is_ok_and(|addr| addr == peer_addr)
}

/// Process-wide, never-reset counter allocating this peer registry's
/// session epochs. `arm_sequence_reset_for_new_session` and
/// `peer_info_is_from_current_session`'s self-healing expiry each draw a
/// FRESH value from here (never a locally-derived/reset one) every time a
/// peer's current session changes.
///
/// A per-peer counter starting at 0 (or any locally-reset scheme) is
/// reusable: a brand-new `PeerInfo` also starts at the same initial value,
/// so if a peer entry is removed and recreated at the same address (or a
/// completely different peer identity reuses that address), its first arm
/// produces the SAME epoch value a still-in-flight, already-superseded
/// apply captured before the removal -- the equality check in
/// `session_epoch_still_current` would then pass for a write that is
/// actually stale (an ABA hole). Because this counter is global and
/// monotonic for the lifetime of the process, entry recreation always
/// yields an epoch no earlier session ever held, so a stale captured
/// epoch can never accidentally match again.
static SESSION_EPOCH_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Allocate a fresh, process-wide-unique session epoch. `0` is never
/// returned, reserved as `PeerInfo::current_session_epoch`'s "no session
/// has ever been armed for this peer" sentinel.
fn next_session_epoch() -> u64 {
    SESSION_EPOCH_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Atomically re-validates, under an already-held `gossip_state` lock,
/// that `expected_epoch` (captured earlier, at session-validation time,
/// from `PeerInfo::current_session_epoch`) still matches this peer's
/// current session epoch.
///
/// A session-gated decision (this FullSync/delta is from the peer's
/// current session) and the state mutation it authorizes are frequently
/// not performed under one continuous lock hold -- candidate updates are
/// collected, addresses resolved, or the delta handed off to
/// `apply_delta_from` across released-and-reacquired locks or `.await`
/// points. A mismatch here means a newer session has been armed, or the
/// previously-validated one has self-expired, since that original
/// validation: the caller's pending write must be dropped rather than
/// applied, or it could silently overwrite a newer (possibly
/// lower-sequence, restart) session's state with a stale, pre-restart
/// snapshot. A peer no longer tracked at all is treated the same way
/// (not current) rather than as vacuously valid.
pub(crate) fn session_epoch_still_current(
    gossip_state: &GossipState,
    peer_addr: SocketAddr,
    expected_epoch: u64,
) -> bool {
    gossip_state
        .peers
        .get(&peer_addr)
        .is_some_and(|peer_info| peer_info.current_session_epoch == expected_epoch)
}

/// Resolve the effective peer address from sender_bind_addr with validation.
/// Falls back to tcp_source_addr if sender_bind_addr is None or invalid.
/// Uses the TCP source IP plus advertised port for unspecified binds (`0.0.0.0:PORT`).
/// Returns None for advertised addresses that are known to be non-dialable from here.
///
/// # Arguments
/// * `sender_bind_addr` - Optional bind address from the message
/// * `tcp_source_addr` - The TCP source address (fallback)
///
/// # Returns
/// A valid routable SocketAddr, or None when the sender advertised a non-dialable bind address.
pub fn resolve_peer_addr_checked(
    sender_bind_addr: Option<&str>,
    tcp_source_addr: SocketAddr,
) -> Option<SocketAddr> {
    if let Some(bind_addr_str) = sender_bind_addr {
        if let Ok(bind_addr) = bind_addr_str.parse::<SocketAddr>() {
            if bind_addr.port() == 0 {
                warn!(
                    "sender_bind_addr {} has port 0, ignoring non-dialable advertised bind from TCP source {}",
                    bind_addr, tcp_source_addr
                );
                return None;
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
                return Some(SocketAddr::new(tcp_source_addr.ip(), bind_addr.port()));
            }

            // Validate: reject loopback (127.0.0.1, ::1) when TCP source is not loopback
            // A remote peer advertising loopback is unreachable from outside. Do not synthesize
            // remote-ip:loopback-port or remote-ip:ephemeral-port; both poison peer discovery.
            if ip.is_loopback() && !tcp_source_addr.ip().is_loopback() {
                warn!(
                    "sender_bind_addr {} is loopback but TCP source {} is not; ignoring non-dialable advertised bind",
                    bind_addr, tcp_source_addr
                );
                return None;
            }

            return Some(bind_addr);
        } else {
            warn!(
                "Failed to parse sender_bind_addr '{}', falling back to TCP source {}",
                bind_addr_str, tcp_source_addr
            );
        }
    }
    // Fallback to TCP source address
    Some(tcp_source_addr)
}

/// Backwards-compatible peer address resolver for callers that can tolerate
/// falling back to the TCP source. Gossip-directory paths that mutate peer
/// state should use `resolve_peer_addr_checked` so non-dialable advertised
/// binds do not poison the peer table.
pub fn resolve_peer_addr(
    sender_bind_addr: Option<&str>,
    tcp_source_addr: SocketAddr,
) -> SocketAddr {
    resolve_peer_addr_checked(sender_bind_addr, tcp_source_addr).unwrap_or(tcp_source_addr)
}

/// True when `advertised_ip` cannot be a dial target for THIS node given the
/// verified `sender_ip` it was learned over: unspecified and multicast IPs
/// are never unicast dial targets; loopback and link-local are only
/// meaningful when the sender itself is on that same scope.
fn advertised_ip_unusable(advertised_ip: std::net::IpAddr, sender_ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    if advertised_ip.is_unspecified() || advertised_ip.is_multicast() {
        return true;
    }
    if advertised_ip.is_loopback() && !sender_ip.is_loopback() {
        return true;
    }
    let link_local = |ip: IpAddr| match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => v6.is_unicast_link_local(),
    };
    link_local(advertised_ip) && !link_local(sender_ip)
}

/// Resolve a gossiped remote actor location address before it is stored in
/// `known_actors`.
///
/// PEER_ID_REFACTOR §1.5: this NEVER drops — the non-`Option` return type
/// encodes the invariant. Remote actors are routed by `location.peer_id`
/// over the identity-keyed connection pool (`GossipRegistryHandle::lookup`
/// → `get_connection_to_peer`), never by this address; discarding an
/// identity-routable actor over an unusable advertised address is what
/// starved receivers of routed-pubsub interest actors and fed the
/// reconnect/re-gossip churn loop. The resolved value is a best-effort dial
/// hint and re-gossip hygiene only.
///
/// PEER_ID_REFACTOR §1.6: the advertised IP is substituted with the
/// sender's verified source IP only when the sender IS the actor's owner
/// (`owner_is_sender`) — authenticated endpoint learning from the peer
/// itself. Gossip is transitive: a relay's source IP says nothing about the
/// owner's reachability, so relayed locations are stored verbatim rather
/// than falsified. The advertised port is always preserved, including 0:
/// the sender's source port is an ephemeral connect port, not its listen
/// port, so there is nothing valid to substitute.
fn resolve_remote_actor_addr(
    actor_name: &str,
    actor_addr: SocketAddr,
    sender_addr: SocketAddr,
    owner_is_sender: bool,
) -> SocketAddr {
    if !owner_is_sender || !advertised_ip_unusable(actor_addr.ip(), sender_addr.ip()) {
        return actor_addr;
    }
    let resolved = SocketAddr::new(sender_addr.ip(), actor_addr.port());
    debug!(
        actor_name = %actor_name,
        actor_addr = %actor_addr,
        sender_addr = %sender_addr,
        resolved = %resolved,
        "owner-advertised address is unusable from this node; substituting the verified source IP"
    );
    resolved
}

/// True when `addr` may be recorded as a learned dial route
/// (`set_discovered_peer_addr` / `addr_to_peer_id`). Storage in
/// `known_actors` is unconditional (§1.5); dial-hint learning is not —
/// port 0 and IPs unusable from this node must not poison the dial tables.
/// (Ownership is enforced at the call sites: dial hints are only learned
/// from the OWNER's own gossip, §1.6 — a relay's claim about a third
/// party's reachability is unauthenticated.)
fn learnable_dial_route(addr: SocketAddr, sender_addr: SocketAddr) -> bool {
    addr.port() != 0 && !advertised_ip_unusable(addr.ip(), sender_addr.ip())
}

/// Parse a gossiped `location.address` wire string, bounding hostile input:
/// anything that is not a socket address canonicalizes to the unspecified
/// placeholder (`0.0.0.0:0`), which then flows through
/// `resolve_remote_actor_addr` like any other unusable advertised address.
/// The actor stays identity-routable (§1.5) while the stored/re-gossiped
/// field is always a typed `SocketAddr`, never attacker-chosen bytes.
fn canonical_wire_addr(actor_name: &str, address: &str) -> SocketAddr {
    address.parse().unwrap_or_else(|_| {
        warn!(
            actor_name = %actor_name,
            "actor location address does not parse; canonicalizing to the unspecified placeholder"
        );
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
    })
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
    owner: Arc<dyn crate::pubsub::PubSubIngressHandler>,
    ptr: usize,
    call: unsafe fn(usize, &crate::PeerId, crate::AlignedBytes) -> Result<()>,
}

#[derive(Clone)]
pub struct ActorTellHandlerSyncCell {
    owner: Arc<dyn ActorTellHandlerSync>,
    ptr: usize,
    call: unsafe fn(usize, u64, u32, crate::aligned::AlignedBytes) -> Result<()>,
}

#[derive(Clone)]
pub struct ActorTellHandlerSyncContextCell {
    owner: Arc<dyn ActorTellHandlerSyncContext>,
    ptr: usize,
    call: for<'a> unsafe fn(
        usize,
        u64,
        u32,
        crate::aligned::AlignedBytes,
        crate::TellContext<'a>,
    ) -> Result<()>,
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

impl ActorTellHandlerSyncContextCell {
    pub(crate) fn new<H>(handler: Arc<H>) -> Self
    where
        H: ActorTellHandlerSyncContext + 'static,
    {
        unsafe fn call_impl<H>(
            ptr: usize,
            actor_id: u64,
            type_hash: u32,
            payload: crate::aligned::AlignedBytes,
            context: crate::TellContext<'_>,
        ) -> Result<()>
        where
            H: ActorTellHandlerSyncContext + 'static,
        {
            let handler = unsafe { &*(ptr as *const H) };
            handler.handle_actor_tell_sync_context(actor_id, type_hash, payload, context)
        }

        let ptr = Arc::as_ptr(&handler) as usize;
        let owner: Arc<dyn ActorTellHandlerSyncContext> = handler;
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
        context: crate::TellContext<'_>,
    ) -> Result<()> {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, actor_id, type_hash, payload, context) }
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
        Option<u32>,
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
            correlation_id: Option<u32>,
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
        correlation_id: Option<u32>,
    ) -> Result<Option<ActorResponse>> {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, actor_id, type_hash, payload, correlation_id) }
    }
}

impl PubSubIngressHandlerCell {
    pub(crate) fn new<H>(handler: Arc<H>) -> Self
    where
        H: crate::pubsub::PubSubIngressHandler + 'static,
    {
        unsafe fn call_impl<H>(
            ptr: usize,
            authenticated_source_peer_id: &crate::PeerId,
            payload: crate::AlignedBytes,
        ) -> Result<()>
        where
            H: crate::pubsub::PubSubIngressHandler + 'static,
        {
            let handler = unsafe { &*(ptr as *const H) };
            handler.handle_pubsub_frame(authenticated_source_peer_id, payload)
        }

        let ptr = Arc::as_ptr(&handler) as usize;
        let owner: Arc<dyn crate::pubsub::PubSubIngressHandler> = handler;
        Self {
            owner,
            ptr,
            call: call_impl::<H>,
        }
    }

    #[inline]
    pub(crate) fn handle(
        &self,
        authenticated_source_peer_id: &crate::PeerId,
        payload: crate::AlignedBytes,
    ) -> Result<()> {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, authenticated_source_peer_id, payload) }
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

#[derive(Clone)]
pub struct PeerLivenessHandlerCell {
    handler: Arc<dyn PeerLivenessHandler>,
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
        correlation_id: Option<u32>,
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

/// Synchronous tell handler that receives authenticated transport context.
pub trait ActorTellHandlerSyncContext: Send + Sync {
    fn handle_actor_tell_sync_context(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        context: crate::TellContext<'_>,
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
        correlation_id: Option<u32>,
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

/// Optional callback invoked by the p2p configured-peer supervisor when a
/// required peer's *direct* connection transitions between reachable and
/// unreachable. icanact already emits a default structured log/metric while a
/// peer is unreachable; this hook is for services that want a custom signal
/// (e.g. their own event code). Fired only on edges (reachable<->unreachable).
pub trait PeerLivenessHandler: Send + Sync {
    fn handle_peer_liveness(
        &self,
        peer_id: crate::PeerId,
        addr: SocketAddr,
        reachable: bool,
        reason: String,
    ) -> BoxFuture<'_, ()>;
}

/// Registry change types for delta tracking with vector clocks
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
#[repr(u8)]
pub enum RegistryChange {
    /// Actor was added or updated
    ActorAdded {
        name: String,
        location: RemoteActorLocation,
        priority: RegistrationPriority,
    } = 0,
    /// Actor was removed
    ActorRemoved {
        name: String,
        vector_clock: crate::VectorClock,
        removing_node_id: crate::GossipNodeId, // Node that performed the removal
        priority: RegistrationPriority,
    } = 1,
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockProbeV1 {
    pub sample_id: u64,
    pub sender_wall_ns: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockEchoV1 {
    pub sample_id: u64,
    pub origin_sender_wall_ns: u64,
    pub responder_recv_wall_ns: u64,
    pub responder_send_wall_ns: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GossipExtensionsV1 {
    pub clock_probe: Option<ClockProbeV1>,
    pub clock_echo: Option<ClockEchoV1>,
}

impl GossipExtensionsV1 {
    pub fn is_empty(&self) -> bool {
        self.clock_probe.is_none() && self.clock_echo.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PeerClockSnapshot {
    pub peer_addr: SocketAddr,
    pub sample_id: u64,
    pub offset_ns: i64,
    pub rtt_ns: u64,
    pub error_bound_ns: u64,
    pub sampled_at_wall_ns: u64,
    pub sample_count: u64,
}

impl PeerClockSnapshot {
    pub fn sample_age_ns(&self, now_wall_ns: u64) -> u64 {
        now_wall_ns.saturating_sub(self.sampled_at_wall_ns)
    }

    pub fn is_stale_at(&self, now_wall_ns: u64) -> bool {
        self.sample_age_ns(now_wall_ns) > CLOCK_CALIBRATION_STALE_AFTER_NS
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingClockProbe {
    peer_addr: SocketAddr,
    sender_wall_ns: u64,
}

#[derive(Debug, Clone, Copy)]
struct PendingClockEcho {
    sample_id: u64,
    origin_sender_wall_ns: u64,
    responder_recv_wall_ns: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerClockProbeState {
    last_probe_sent_wall_ns: u64,
}

fn compute_clock_sample(
    origin_send_wall_ns: u64,
    responder_recv_wall_ns: u64,
    responder_send_wall_ns: u64,
    origin_recv_wall_ns: u64,
) -> Option<(i64, u64, u64)> {
    if origin_recv_wall_ns < origin_send_wall_ns || responder_send_wall_ns < responder_recv_wall_ns
    {
        return None;
    }

    let outbound = responder_recv_wall_ns as i128 - origin_send_wall_ns as i128;
    let inbound = responder_send_wall_ns as i128 - origin_recv_wall_ns as i128;
    let offset = (outbound + inbound) / 2;
    if offset < i64::MIN as i128 || offset > i64::MAX as i128 {
        return None;
    }

    let origin_elapsed = origin_recv_wall_ns.saturating_sub(origin_send_wall_ns);
    let responder_elapsed = responder_send_wall_ns.saturating_sub(responder_recv_wall_ns);
    let rtt = origin_elapsed.saturating_sub(responder_elapsed);
    Some((offset as i64, rtt, rtt / 2))
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

/// Message types for the gossip protocol
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
#[repr(u8)]
pub enum RegistryMessage {
    /// Delta gossip message containing only changes
    DeltaGossip {
        delta: RegistryDelta,
        extensions: Option<GossipExtensionsV1>,
    } = 0,
    /// Response to delta gossip with our own delta
    DeltaGossipResponse {
        delta: RegistryDelta,
        extensions: Option<GossipExtensionsV1>,
    } = 1,
    /// Request for full sync (fallback when deltas are unavailable)
    FullSyncRequest {
        sender_peer_id: crate::PeerId,    // Peer's unique identifier
        sender_bind_addr: Option<String>, // Sender's listening address (optional for backwards compat)
        sequence: u64,
        wall_clock_time: u64,
    } = 2,
    /// Full synchronization message
    FullSync {
        local_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        known_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        sender_peer_id: crate::PeerId,                    // Peer's unique identifier
        sender_bind_addr: Option<String>, // Sender's listening address (optional for backwards compat)
        sequence: u64,
        wall_clock_time: u64,
        extensions: Option<GossipExtensionsV1>,
    } = 3,
    /// Response to full sync
    FullSyncResponse {
        local_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        known_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        sender_peer_id: crate::PeerId,                    // Peer's unique identifier
        sender_bind_addr: Option<String>, // Sender's listening address (optional for backwards compat)
        sequence: u64,
        wall_clock_time: u64,
        extensions: Option<GossipExtensionsV1>,
    } = 4,
    /// Peer health status report
    PeerHealthReport {
        reporter: crate::PeerId,
        peer_statuses: Vec<(String, PeerHealthStatus)>, // Use Vec for rkyv serialization
        timestamp: u64,
    } = 5,
    /// Query for peer health consensus
    PeerHealthQuery {
        sender: crate::PeerId,
        target_peer: String,
        timestamp: u64,
    } = 7,
    /// Peer list gossip for automatic peer discovery
    /// Contains list of known peers with their connection info
    PeerListGossip {
        /// List of known peers (address as string for rkyv, peer info)
        peers: Vec<PeerInfoGossip>,
        /// Timestamp when this gossip was generated
        timestamp: u64,
        /// Sender's advertised address (so receiver can add us to their peer list)
        sender_addr: String,
    } = 9,
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
    // PEER_ID_REFACTOR observability (§5): storm-signature counters.
    /// Owner-sent unusable advertised IPs repaired from the verified source IP.
    pub addr_substitutions: u64,
    /// Relayed locations whose unusable advertised address was kept verbatim.
    pub relayed_addr_kept: u64,
    /// Duplicate-connection tie-break evictions/rejections observed.
    pub tie_break_evictions: u64,
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
    // GossipNodeId for TLS verification (may be learned on connect).
    pub node_id: Option<crate::GossipNodeId>,
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
    pub last_response_received_ms: u64,
    /// R-11: one-shot "accept a lower-sequence FullSync than `last_sequence`",
    /// scoped to the exact connection that armed it.
    ///
    /// Holds the VERIFIED TCP source address (ephemeral port included) of the
    /// newly authenticated connection. Only a FullSync whose verified source
    /// matches consumes it. That scoping is what makes this safe:
    /// - an old connection still draining during a reconnect has a different
    ///   ephemeral source port, so its in-flight lower-sequence FullSync
    ///   cannot steal the exemption and leave the real restart sync to be
    ///   rejected;
    /// - a replayed or relayed frame arriving by any other path cannot consume
    ///   it either.
    ///
    /// Note this is armed on EVERY inbound authenticated session, not only on
    /// restarts — a new TLS session is also established on routine reconnects.
    /// That is harmless: a peer that merely reconnected reports its current
    /// sequence, which is >= our high-water mark, so the exemption is simply
    /// never exercised. It is consumed only when the peer genuinely comes back
    /// with a lower sequence, i.e. a restart.
    ///
    /// `last_sequence` only ever advances (`max()`), and the `handle_peer_death`
    /// reset the comments still reference no longer exists. A peer that crashes
    /// and restarts at the same address resumes from sequence ~0, so every node
    /// that saw its pre-restart sequence drops all of its FullSyncs forever.
    /// The omission-prune never runs, and actors the peer no longer hosts sit in
    /// `known_actors` until the 24h TTL — asks error, tells are silently
    /// dropped — because the peer is healthy so the dead-peer reap never fires.
    ///
    /// Armed only when a NEW TLS-authenticated session is established for the
    /// peer's identity (see `arm_sequence_reset_for_new_session`), which is
    /// actual restart evidence and cannot be forged mid-session or by a
    /// third party's gossip. Cleared on the first FullSync it admits, so the
    /// stale gate is restored immediately and still blocks in-session replays.
    pub accept_lower_sequence_from: Option<SocketAddr>,
    /// The verified TCP source (ephemeral port included) of the connection
    /// currently authenticated as this peer's live session.
    ///
    /// Set alongside `accept_lower_sequence_from` by
    /// `arm_sequence_reset_for_new_session`, but unlike that one-shot flag
    /// this is never cleared by ordinary traffic -- only overwritten by the
    /// next new session. It is the peer's current epoch, independent of
    /// whether the one-shot reset exemption has already been consumed.
    ///
    /// Every `merge_full_sync_from` update (not only the lower-sequence
    /// exemption path) is gated on this: a message whose verified source
    /// does not match is from a connection we know is not the peer's
    /// current session -- e.g. an old connection still draining through a
    /// reconnect -- and must not perturb `last_sequence` at all, even via
    /// the ordinary non-stale path. Without this, a draining old
    /// connection's in-flight, numerically-high (pre-restart) sequence can
    /// bump `last_sequence` back up after the new session's reset, making
    /// every later FullSync from the restarted peer look stale again with
    /// no exemption left to rescue it (the one-shot was already consumed).
    ///
    /// `None` means no session has ever been armed for this peer (e.g. a
    /// freshly added peer, or a local/test caller that never goes through
    /// the TLS-authenticated arming path); in that case updates are
    /// accepted from any source, matching pre-existing behavior.
    pub current_session_source: Option<SocketAddr>,
    /// The connection instance that armed `current_session_source`, held
    /// weakly so this never keeps a dead connection's resources alive.
    ///
    /// `current_session_source` alone cannot tell a live successor from a
    /// dead predecessor: if the arming connection closes and is succeeded
    /// by a connection that never arms (a cert-type migration, a non-mTLS
    /// client, or simply a `node_id` mismatch), nothing ever clears
    /// `current_session_source`, and the gate would otherwise reject the
    /// live successor's traffic forever. `peer_info_is_from_current_session`
    /// checks whether this instance is still `connection_pool`'s current
    /// published connection for the peer on every use; if not, the armed
    /// session has expired and both this and `current_session_source` /
    /// `accept_lower_sequence_from` are cleared, falling back to the
    /// unarmed (accept-from-any-source) behavior.
    pub current_session_connection: Option<std::sync::Weak<crate::connection_pool::LockFreeConnection>>,
    /// Monotonic counter bumped every time `current_session_source` /
    /// `current_session_connection` change -- armed by
    /// `arm_sequence_reset_for_new_session`, or cleared by
    /// `peer_info_is_from_current_session`'s self-healing expiry.
    ///
    /// A session-gated decision (accept this FullSync/delta, it is from
    /// the current session) and the actual state mutation it authorizes
    /// (known_actors/peer_to_actors upserts, omission pruning, the delta
    /// apply) are not always performed under the same lock acquisition --
    /// collecting candidate updates, resolving addresses, or handing off
    /// to `apply_delta_from` can all involve released-and-reacquired locks
    /// or `.await` points. Without this counter, a connection whose
    /// traffic was validated as "current" at the START of that pipeline
    /// could have its write actually land AFTER a newer session has
    /// armed and completed its own (possibly lower-sequence, restart)
    /// write, silently overwriting the newer session's state with the
    /// old one's pre-restart snapshot.
    ///
    /// Callers capture this value at validation time and re-compare it
    /// immediately before the actual mutation, atomically under whatever
    /// lock guards that mutation (see `merge_full_sync_from`'s STEP 2 and
    /// `apply_delta_from`'s `session_guard` parameter); a mismatch means a
    /// newer session has since been armed or the old one has expired, and
    /// the pending write must be dropped rather than applied.
    pub current_session_epoch: u64,
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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
    /// GossipNodeId for TLS verification
    pub node_id: Option<crate::GossipNodeId>,
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
    /// Monotonic push time, used only for local retention (`cleanup_stale_actors`).
    /// Never leaves this process, so it uses `Instant` rather than wall-clock
    /// time and is immune to NTP steps / VM pauses.
    pub recorded_at: Instant,
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

#[derive(Debug, Clone)]
pub struct RemovedActorTombstone {
    pub vector_clock: crate::VectorClock,
    pub removed_at: u64,
}

impl RemovedActorTombstone {
    fn new(vector_clock: crate::VectorClock) -> Self {
        Self {
            vector_clock,
            removed_at: current_timestamp(),
        }
    }
}

/// Separated actor state for read-heavy operations (now with vector clocks)
#[derive(Default)]
pub struct ActorState {
    pub local_actors: SccHashMap<String, RemoteActorLocation>,
    pub known_actors: SccHashMap<String, RemoteActorLocation>,
    pub removed_actors: SccHashMap<String, RemovedActorTombstone>,
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
    /// Remote-actor admission accounting keyed by authenticated identity.
    pub actor_admissions_by_peer: HashMap<crate::PeerId, HashSet<String>>,
    /// Reverse index used to release per-peer admission capacity on removal.
    pub actor_admission_peer_by_name: HashMap<String, crate::PeerId>,
    /// Legacy peer-health consensus reports from different observers.
    pub peer_health_reports: HashMap<SocketAddr, HashMap<SocketAddr, PeerHealthStatus>>,
    /// Legacy pending peer failures that need consensus.
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

impl GossipState {
    fn actor_admission_count(&self, peer_id: &crate::PeerId) -> usize {
        self.actor_admissions_by_peer
            .get(peer_id)
            .map_or(0, HashSet::len)
    }

    fn record_actor_admission(&mut self, peer_id: &crate::PeerId, name: &str) {
        if let Some(previous_peer) = self
            .actor_admission_peer_by_name
            .insert(name.to_string(), peer_id.clone())
            && previous_peer != *peer_id
        {
            let remove_previous = self
                .actor_admissions_by_peer
                .get_mut(&previous_peer)
                .is_some_and(|names| {
                    names.remove(name);
                    names.is_empty()
                });
            if remove_previous {
                self.actor_admissions_by_peer.remove(&previous_peer);
            }
        }
        self.actor_admissions_by_peer
            .entry(peer_id.clone())
            .or_default()
            .insert(name.to_string());
    }

    fn release_actor_admission(&mut self, name: &str) {
        let Some(peer_id) = self.actor_admission_peer_by_name.remove(name) else {
            return;
        };
        let remove_peer = self
            .actor_admissions_by_peer
            .get_mut(&peer_id)
            .is_some_and(|names| {
                names.remove(name);
                names.is_empty()
            });
        if remove_peer {
            self.actor_admissions_by_peer.remove(&peer_id);
        }
    }
}

/// Core gossip registry implementation with separated locks
#[derive(Clone, Copy)]
struct PeerLivenessStatus {
    reachable: bool,
    updated_at: Instant,
}

#[derive(Clone)]
pub struct GossipRegistry<T = ()> {
    // Immutable config
    pub bind_addr: SocketAddr,
    pub peer_id: crate::PeerId, // Unique peer identifier (public key)
    pub config: GossipConfig,
    pub start_time: u64,
    pub start_instant: Instant,

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
        Arc<SccHashMap<crate::GossipNodeId, crate::handshake::PeerCapabilities>>,
    pub peer_capability_addr_to_node: Arc<SccHashMap<SocketAddr, crate::GossipNodeId>>,
    clock_probe_state: Arc<SccHashMap<SocketAddr, PeerClockProbeState>>,
    /// Per-peer deadline (monotonic `Instant`) before which a fresh outbound
    /// TCP/TLS dial to that peer is suppressed. Only armed once *repeated*
    /// rapid tie-break evictions are observed for the same peer — see
    /// `note_tie_break_eviction` for why a single eviction must not gate
    /// anything (ordinary simultaneous-open bootstrap evicts exactly once
    /// and is not oscillation).
    tie_break_cooldown_until: Arc<SccHashMap<crate::PeerId, Instant>>,
    /// Timestamp of the most recent duplicate-connection tie-break eviction
    /// per peer, used only to detect *back-to-back* evictions (the
    /// oscillation signature) — see `note_tie_break_eviction`.
    tie_break_last_eviction_at: Arc<SccHashMap<crate::PeerId, Instant>>,
    pending_clock_probes: Arc<SccHashMap<u64, PendingClockProbe>>,
    pending_clock_echoes: Arc<SccHashMap<SocketAddr, PendingClockEcho>>,
    peer_clock_snapshots: Arc<SccHashMap<SocketAddr, PeerClockSnapshot>>,
    /// PEER_ID_REFACTOR runtime observability: owner-sent unusable
    /// advertised IPs repaired from the verified source IP
    /// (`resolve_remote_actor_addr`).
    pub(crate) addr_substitutions: Arc<AtomicU64>,
    /// Relayed (sender != owner) locations whose unusable advertised
    /// address was kept verbatim rather than falsified (§1.6).
    pub(crate) relayed_unusable_addr_kept: Arc<AtomicU64>,
    /// Duplicate-connection tie-break evictions/rejections observed
    /// (`note_tie_break_eviction`) — the storm signature counter.
    pub(crate) tie_break_evictions: Arc<AtomicU64>,

    // Actor message handler callback
    pub actor_message_handler: Arc<ArcSwapOption<ActorMessageHandlerCell>>,
    pub actor_tell_handler_sync: Arc<ArcSwapOption<ActorTellHandlerSyncCell>>,
    pub actor_tell_handler_sync_context: Arc<ArcSwapOption<ActorTellHandlerSyncContextCell>>,
    pub actor_ask_immediate_handler_sync: Arc<ArcSwapOption<ActorAskImmediateHandlerSyncCell>>,
    pub actor_ask_handler_sync: Arc<ArcSwapOption<ActorAskHandlerSyncCell>>,
    pub actor_message_handler_sync: Arc<ArcSwapOption<ActorMessageHandlerSyncCell>>,
    pub pubsub_ingress_handler: Arc<ArcSwapOption<PubSubIngressHandlerCell>>,
    pub peer_disconnect_handler: Arc<ArcSwapOption<PeerDisconnectHandlerCell>>,
    pub peer_connect_handler: Arc<ArcSwapOption<PeerConnectHandlerCell>>,
    /// Optional edge-triggered callback for the p2p configured-peer supervisor.
    pub peer_liveness_handler: Arc<ArcSwapOption<PeerLivenessHandlerCell>>,
    /// Last-known reachability per configured peer — used by the supervisor for
    /// edge detection (handler + one-shot recovery log). Dynamic peer IDs age
    /// out after their liveness window so restart churn cannot retain them.
    peer_liveness_status: Arc<SccHashMap<crate::PeerId, PeerLivenessStatus>>,

    /// Tracks the currently-running peer discovery dial task (H-004).
    pub discovery_task: Arc<DiscoveryTaskTracker>,
    peer_gossip_notify: Arc<Notify>,

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

impl<T: 'static> GossipRegistry<T> {
    /// The address this node should advertise to peers for anything it
    /// hosts (gossip `sender_addr`, peer-list snapshots, routed-pubsub
    /// interest actors, ...): `GossipConfig::advertise_address` when set,
    /// falling back to `bind_addr`.
    ///
    /// A node bound to a wildcard address (`0.0.0.0:<port>`) MUST NOT
    /// advertise that raw bind address to peers — it is not dialable. Any
    /// site in this crate that gossips "here is where to reach an actor/
    /// peer hosted by me" must resolve through this helper rather than
    /// reading `self.bind_addr` directly.
    pub(crate) fn advertised_addr(&self) -> SocketAddr {
        self.config.advertise_address.unwrap_or(self.bind_addr)
    }

    fn peer_health_consensus_enabled(&self) -> bool {
        matches!(
            self.config.peer_health_mode,
            PeerHealthMode::LegacyConsensus
        )
    }

    fn duration_millis_u64(duration: Duration) -> u64 {
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
    }

    fn effective_peer_liveness_window_ms(&self, peer_addr: SocketAddr) -> u64 {
        let configured = Self::duration_millis_u64(self.config.peer_liveness_window);
        let peer_id = self
            .connection_pool
            .addr_to_peer_id
            .read_sync(&peer_addr, |_, peer_id| peer_id.clone());
        // Required (configured) peers are floored against the cadence that
        // actually refreshes `last_response_received_ms` — the regular
        // gossip round (`gossip_interval`), not peer-list discovery gossip
        // (`peer_gossip_interval`). See `crate::config::required_peer_liveness_floor_ms`.
        let required_peer_floor = peer_id
            .as_ref()
            .filter(|peer_id| self.connection_pool.is_required_peer(peer_id))
            .map(|_| {
                crate::config::required_peer_liveness_floor_ms(
                    self.config.gossip_interval,
                    self.config.peer_gossip_interval,
                )
            })
            .unwrap_or(0);
        configured.max(required_peer_floor)
    }

    pub(crate) fn trigger_immediate_peer_gossip(&self) {
        if self.config.enable_peer_discovery {
            self.peer_gossip_notify.notify_one();
        }
    }

    pub(crate) async fn wait_immediate_peer_gossip(&self) {
        self.peer_gossip_notify.notified().await;
    }

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
    ///
    /// # Panics
    ///
    /// Panics when `config` is invalid. Use [`Self::try_new`] when invalid
    /// consumer-supplied configuration must be reported without panicking.
    pub fn new(bind_addr: SocketAddr, config: GossipConfig) -> Self {
        Self::try_new(bind_addr, config).expect("invalid GossipConfig")
    }

    /// Create a new gossip registry after validating all startup invariants.
    ///
    /// # Errors
    ///
    /// Returns [`GossipError::InvalidConfig`] when a required startup
    /// invariant is missing, including the TLS identity key.
    pub fn try_new(bind_addr: SocketAddr, mut config: GossipConfig) -> Result<Self> {
        // R5: enforce runtime config invariants (e.g. liveness window >=
        // gossip interval * 2) at the point config enters the registry, clamping
        // unsafe consumer-supplied values with a warning. One-time at startup.
        config.validate_and_normalize()?;

        // Use public key from config (required for TLS identity)
        let peer_id = config
            .key_pair
            .as_ref()
            .ok_or_else(|| {
                GossipError::InvalidConfig(
                    "GossipConfig.key_pair is required for TLS-only mode".to_owned(),
                )
            })?
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

        Ok(Self {
            bind_addr,
            peer_id,
            config: config.clone(),
            start_time: current_timestamp(),
            start_instant: std::time::Instant::now(),
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
                actor_admissions_by_peer: HashMap::new(),
                actor_admission_peer_by_name: HashMap::new(),
                peer_health_reports: HashMap::new(),
                pending_peer_failures: HashMap::new(),
                // Peer discovery state
                last_peer_gossip_time: 0,
                peer_discovery: if config.enable_peer_discovery {
                    Some(
                        PeerDiscovery::new(
                            // `local_addr` stays `bind_addr`: relayed/stale
                            // gossip can describe this node by its raw
                            // bind address with no `node_id` attached (see
                            // `PeerInfo::local`), so `bind_addr` must always
                            // be filtered here regardless of whether
                            // `advertise_address` is configured.
                            bind_addr,
                            PeerDiscoveryConfig {
                                max_peers: config.max_peers,
                                allow_private_discovery: config.allow_private_discovery,
                                allow_loopback_discovery: config.allow_loopback_discovery,
                                allow_link_local_discovery: config.allow_link_local_discovery,
                                fail_ttl: config.fail_ttl,
                                pending_ttl: config.pending_ttl,
                            },
                        )
                        // A peer relaying gossip about this node may
                        // instead describe it by its *advertised* address,
                        // which can differ from `bind_addr` under
                        // NAT/K8s/mesh overlays. Filtering both — bind_addr
                        // via `local_addr` above and the advertised address
                        // here — closes that gap without ever filtering a
                        // distinct real peer.
                        .with_additional_self_addr(config.advertise_address),
                    )
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
            clock_probe_state: Arc::new(SccHashMap::default()),
            tie_break_cooldown_until: Arc::new(SccHashMap::default()),
            tie_break_last_eviction_at: Arc::new(SccHashMap::default()),
            pending_clock_probes: Arc::new(SccHashMap::default()),
            pending_clock_echoes: Arc::new(SccHashMap::default()),
            peer_clock_snapshots: Arc::new(SccHashMap::default()),
            addr_substitutions: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            relayed_unusable_addr_kept: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            tie_break_evictions: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            actor_message_handler: Arc::new(ArcSwapOption::empty()),
            actor_tell_handler_sync: Arc::new(ArcSwapOption::empty()),
            actor_tell_handler_sync_context: Arc::new(ArcSwapOption::empty()),
            actor_ask_immediate_handler_sync: Arc::new(ArcSwapOption::empty()),
            actor_ask_handler_sync: Arc::new(ArcSwapOption::empty()),
            actor_message_handler_sync: Arc::new(ArcSwapOption::empty()),
            pubsub_ingress_handler: Arc::new(ArcSwapOption::empty()),
            peer_disconnect_handler: Arc::new(ArcSwapOption::empty()),
            peer_connect_handler: Arc::new(ArcSwapOption::empty()),
            peer_liveness_handler: Arc::new(ArcSwapOption::empty()),
            peer_liveness_status: Arc::new(SccHashMap::default()),
            discovery_task: Arc::new(DiscoveryTaskTracker::default()),
            peer_gossip_notify: Arc::new(Notify::new()),
            dns_resolver: Arc::new(tokio::sync::RwLock::new(Arc::new(
                crate::TokioDnsResolver::default(),
            )
                as Arc<dyn crate::dns::DnsResolver>)),
        })
    }

    /// Override the DNS resolver used for peer refreshes (primarily for deterministic tests).
    pub async fn set_dns_resolver(&self, resolver: Arc<dyn crate::dns::DnsResolver>) {
        *self.dns_resolver.write().await = resolver;
    }

    /// Enable TLS for secure connections
    /// This must be called before starting the registry to enable TLS
    pub fn enable_tls(&mut self, secret_key: crate::SecretKey) -> Result<()> {
        self.tls_config = Some(Arc::new(crate::tls::TlsConfig::with_peer_discovery(
            secret_key,
            self.config.enable_peer_discovery,
        )?));
        Ok(())
    }

    /// Track negotiated peer capabilities for a peer connection
    pub fn set_peer_capabilities(&self, addr: SocketAddr, caps: PeerCapabilities) {
        let _ = self.peer_capabilities.upsert_sync(addr, caps);
    }

    /// Attach capabilities recorded for an address to a specific GossipNodeId (once known)
    pub async fn associate_peer_capabilities_with_node(
        &self,
        addr: SocketAddr,
        node_id: GossipNodeId,
    ) {
        let caps = self.peer_capabilities.read_sync(&addr, |_, v| *v);
        if let Some(caps) = caps {
            let _ = self.peer_capabilities_by_node.upsert_sync(node_id, caps);
        }
        let _ = self.peer_capability_addr_to_node.upsert_sync(addr, node_id);
        self.propagate_node_id_to_known_addresses(addr, node_id)
            .await;
    }

    /// Remove stored capabilities for a peer (e.g., when connection closes)
    /// ACTOR_REM_2 R13(c): reap the per-peer clock-calibration side tables when
    /// a peer is removed. These addr-keyed tables (probe state, pending echoes,
    /// and the calibration snapshot) were the only per-peer tables NOT threaded
    /// through the dead-peer / eviction / DNS-migration cleanups, so they leaked
    /// one orphan per departed peer for the process lifetime. `pending_clock_
    /// probes` is sample-id-keyed and already age-reaped, so it is not included.
    pub(crate) fn remove_clock_state_for_addr(&self, addr: &SocketAddr) {
        let _ = self.clock_probe_state.remove_sync(addr);
        let _ = self.pending_clock_echoes.remove_sync(addr);
        let _ = self.peer_clock_snapshots.remove_sync(addr);
    }

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

    pub async fn peer_supports_clock_calibration(&self, addr: &SocketAddr) -> bool {
        if let Some(caps) = self.peer_capabilities.read_sync(addr, |_, v| *v) {
            return caps.can_calibrate_clock();
        }

        let node_id = self.peer_capability_addr_to_node.read_sync(addr, |_, v| *v);
        if let Some(node_id) = node_id
            && let Some(caps) = self
                .peer_capabilities_by_node
                .read_sync(&node_id, |_, v| *v)
        {
            return caps.can_calibrate_clock();
        }

        if let Some(node_id) = self.lookup_node_id(addr).await {
            let _ = self
                .peer_capability_addr_to_node
                .upsert_sync(*addr, node_id);
            if let Some(caps) = self
                .peer_capabilities_by_node
                .read_sync(&node_id, |_, v| *v)
            {
                return caps.can_calibrate_clock();
            }
        }

        let found = AtomicBool::new(false);
        let want_ip = addr.ip();
        self.peer_capabilities.iter_sync(|k, v| {
            if k.ip() == want_ip && v.can_calibrate_clock() {
                found.store(true, Ordering::Relaxed);
                return false;
            }
            true
        });
        found.load(Ordering::Relaxed)
    }

    pub fn peer_clock_snapshot(&self, addr: &SocketAddr) -> Option<PeerClockSnapshot> {
        self.peer_clock_snapshots.read_sync(addr, |_, v| *v)
    }

    pub fn peer_clock_snapshots(&self) -> Vec<PeerClockSnapshot> {
        let mut out = Vec::new();
        self.peer_clock_snapshots.iter_sync(|_, v| {
            out.push(*v);
            true
        });
        out.sort_by_key(|s| s.peer_addr);
        out
    }

    pub async fn gossip_extensions_for_outbound(
        &self,
        peer_addr: SocketAddr,
        send_wall_ns: u64,
    ) -> Option<GossipExtensionsV1> {
        if !self.peer_supports_clock_calibration(&peer_addr).await {
            return None;
        }

        let mut extensions = GossipExtensionsV1::default();

        if let Some((_, pending)) = self.pending_clock_echoes.remove_sync(&peer_addr) {
            extensions.clock_echo = Some(ClockEchoV1 {
                sample_id: pending.sample_id,
                origin_sender_wall_ns: pending.origin_sender_wall_ns,
                responder_recv_wall_ns: pending.responder_recv_wall_ns,
                responder_send_wall_ns: send_wall_ns,
            });
        }

        let mut expired_probe_ids = Vec::new();
        let has_live_pending_probe = AtomicBool::new(false);
        self.pending_clock_probes.iter_sync(|sample_id, pending| {
            if pending.peer_addr == peer_addr {
                if send_wall_ns.saturating_sub(pending.sender_wall_ns)
                    >= CLOCK_CALIBRATION_PROBE_TIMEOUT_NS
                {
                    expired_probe_ids.push(*sample_id);
                } else {
                    has_live_pending_probe.store(true, Ordering::Relaxed);
                }
            }
            true
        });
        let had_expired_probe = !expired_probe_ids.is_empty();
        for sample_id in expired_probe_ids {
            let _ = self.pending_clock_probes.remove_sync(&sample_id);
        }

        let has_snapshot = self.peer_clock_snapshots.contains_sync(&peer_addr);
        let interval_elapsed = self
            .clock_probe_state
            .read_sync(&peer_addr, |_, state| {
                send_wall_ns.saturating_sub(state.last_probe_sent_wall_ns)
                    >= CLOCK_CALIBRATION_INTERVAL_NS
            })
            .unwrap_or(true);

        let should_probe = !has_live_pending_probe.load(Ordering::Relaxed)
            && (interval_elapsed || (had_expired_probe && !has_snapshot));

        if should_probe {
            // Unpredictable sample ids: a sequential counter lets an
            // authenticated peer guess in-flight ids and forge echoes. Draw a
            // random id, regenerating on the vanishing chance of a collision.
            let mut sample_id = rand::random::<u64>();
            while self.pending_clock_probes.contains_sync(&sample_id) {
                sample_id = rand::random::<u64>();
            }
            let _ = self.clock_probe_state.upsert_sync(
                peer_addr,
                PeerClockProbeState {
                    last_probe_sent_wall_ns: send_wall_ns,
                },
            );
            let _ = self.pending_clock_probes.upsert_sync(
                sample_id,
                PendingClockProbe {
                    peer_addr,
                    sender_wall_ns: send_wall_ns,
                },
            );
            extensions.clock_probe = Some(ClockProbeV1 {
                sample_id,
                sender_wall_ns: send_wall_ns,
            });
        }

        (!extensions.is_empty()).then_some(extensions)
    }

    /// ACTOR_REM_2 R16i: If we owe `peer_addr` a clock echo (it probed us) *and*
    /// we will never send it a scheduled outbound gossip round — because outbound
    /// retry to it is suppressed (inbound-only / NAT'd, not dialable from here) —
    /// consume and return the owed echo so the caller can answer inline on the
    /// connection the probe arrived on. Returns `None` (leaving the echo queued
    /// for the normal outbound flush) for peers we do dial, and never initiates a
    /// probe, so it has no probe-scheduling side effects. Without this, a
    /// permanently inbound-only peer's owed echo waits forever for an outbound
    /// round that `should_suppress_outbound_retry_for_peer` prevents.
    pub async fn take_clock_echo_for_undialable_peer(
        &self,
        peer_addr: SocketAddr,
        send_wall_ns: u64,
    ) -> Option<GossipExtensionsV1> {
        if !self.pending_clock_echoes.contains_sync(&peer_addr) {
            return None;
        }
        let peer_info = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state.peers.get(&peer_addr).cloned()
        };
        let suppressed = peer_info
            .map(|peer| self.should_suppress_outbound_retry_for_peer(&peer))
            .unwrap_or(false);
        if !suppressed {
            return None;
        }
        let (_, pending) = self.pending_clock_echoes.remove_sync(&peer_addr)?;
        let mut extensions = GossipExtensionsV1::default();
        extensions.clock_echo = Some(ClockEchoV1 {
            sample_id: pending.sample_id,
            origin_sender_wall_ns: pending.origin_sender_wall_ns,
            responder_recv_wall_ns: pending.responder_recv_wall_ns,
            responder_send_wall_ns: send_wall_ns,
        });
        Some(extensions)
    }

    pub fn record_inbound_gossip_extensions(
        &self,
        peer_addr: SocketAddr,
        extensions: Option<GossipExtensionsV1>,
        recv_wall_ns: u64,
    ) {
        let Some(extensions) = extensions else {
            return;
        };

        if let Some(probe) = extensions.clock_probe {
            let _ = self.pending_clock_echoes.upsert_sync(
                peer_addr,
                PendingClockEcho {
                    sample_id: probe.sample_id,
                    origin_sender_wall_ns: probe.sender_wall_ns,
                    responder_recv_wall_ns: recv_wall_ns,
                },
            );
        }

        if let Some(echo) = extensions.clock_echo {
            self.record_clock_echo(peer_addr, echo, recv_wall_ns);
        }
    }

    fn record_clock_echo(
        &self,
        peer_addr: SocketAddr,
        echo: ClockEchoV1,
        origin_recv_wall_ns: u64,
    ) {
        // Validate-then-remove atomically: only consume the pending probe when
        // the echo's origin address and wall clock match the probe we sent.
        // Otherwise an authenticated peer replaying/guessing a sample_id could
        // void another peer's in-flight calibration probe. `remove_if_sync`
        // holds the bucket lock across the predicate, so a mismatch leaves the
        // probe intact.
        let Some((_, pending)) =
            self.pending_clock_probes
                .remove_if_sync(&echo.sample_id, |pending| {
                    pending.peer_addr == peer_addr
                        && pending.sender_wall_ns == echo.origin_sender_wall_ns
                })
        else {
            return;
        };

        let Some((offset_ns, rtt_ns, error_bound_ns)) = compute_clock_sample(
            pending.sender_wall_ns,
            echo.responder_recv_wall_ns,
            echo.responder_send_wall_ns,
            origin_recv_wall_ns,
        ) else {
            return;
        };

        let sample_count = self
            .peer_clock_snapshots
            .read_sync(&peer_addr, |_, snapshot| {
                snapshot.sample_count.saturating_add(1)
            })
            .unwrap_or(1);

        let _ = self.peer_clock_snapshots.upsert_sync(
            peer_addr,
            PeerClockSnapshot {
                peer_addr,
                sample_id: echo.sample_id,
                offset_ns,
                rtt_ns,
                error_bound_ns,
                sampled_at_wall_ns: origin_recv_wall_ns,
                sample_count,
            },
        );
    }

    async fn propagate_node_id_to_known_addresses(&self, addr: SocketAddr, node_id: GossipNodeId) {
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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

    /// Register a synchronous tell handler callback with authenticated context.
    pub async fn set_actor_tell_handler_sync_context<H>(&self, handler: Arc<H>)
    where
        H: ActorTellHandlerSyncContext + 'static,
    {
        self.actor_tell_handler_sync_context.store(Some(Arc::new(
            ActorTellHandlerSyncContextCell::new(handler),
        )));
        info!("actor tell handler sync context registered");
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
    pub async fn set_pubsub_ingress_handler<H>(&self, handler: Arc<H>)
    where
        H: crate::pubsub::PubSubIngressHandler + 'static,
    {
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

    /// Remove the synchronous tell handler callback with authenticated context.
    pub async fn clear_actor_tell_handler_sync_context(&self) {
        self.actor_tell_handler_sync_context.store(None);
        info!("actor tell handler sync context cleared");
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

    /// Register a peer liveness handler callback (p2p configured-peer supervisor)
    pub async fn set_peer_liveness_handler(&self, handler: Arc<dyn PeerLivenessHandler>) {
        self.peer_liveness_handler
            .store(Some(Arc::new(PeerLivenessHandlerCell { handler })));
        info!("peer liveness handler registered");
    }

    /// Remove the peer liveness handler callback
    pub async fn clear_peer_liveness_handler(&self) {
        self.peer_liveness_handler.store(None);
        info!("peer liveness handler cleared");
    }

    /// Drop every application callback retained by this registry.
    ///
    /// Handler implementations commonly own routers, actor refs, or clients
    /// that point back to this registry. Keeping them installed after terminal
    /// shutdown therefore forms a strong ownership cycle and retains the full
    /// connection pool. Shutdown is final, so no callback can be invoked again.
    fn clear_runtime_handlers(&self) {
        self.actor_message_handler.store(None);
        self.actor_tell_handler_sync.store(None);
        self.actor_tell_handler_sync_context.store(None);
        self.actor_ask_immediate_handler_sync.store(None);
        self.actor_ask_handler_sync.store(None);
        self.actor_message_handler_sync.store(None);
        self.pubsub_ingress_handler.store(None);
        self.peer_disconnect_handler.store(None);
        self.peer_connect_handler.store(None);
        self.peer_liveness_handler.store(None);
    }

    /// Handle an incoming actor message by forwarding to the registered callback
    pub async fn handle_actor_message(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::aligned::AlignedBytes,
        correlation_id: Option<u32>,
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

    /// R-11: arm the one-shot lower-sequence acceptance for a peer whose
    /// identity has just completed a NEW TLS-authenticated session.
    ///
    /// This is armed on EVERY inbound authenticated session, not only on
    /// restarts — a new TLS session is also established on routine reconnects
    /// (keepalive failure, network blip, LB/proxy recycle, same-pod
    /// reconnect). That is harmless rather than restart "evidence": a peer
    /// that merely reconnected reports its current sequence, which is >= our
    /// high-water mark, so the exemption is never exercised. It is consumed
    /// only when the peer comes back with a LOWER sequence, i.e. a restart.
    ///
    /// Two things keep it safe:
    /// - it is scoped to the connection that armed it (see `session_source`),
    ///   so it cannot be consumed by a replay arriving on any other path; and
    /// - `node_id` is the TLS client certificate identity, not a wire-claimed
    ///   field, and must match the recorded `node_id`, so a peer cannot arm
    ///   the reset against a *victim* identity (cf. B-5).
    ///
    /// `session_source` is the VERIFIED TCP source address of the newly
    /// authenticated connection, including its ephemeral port. Only a FullSync
    /// arriving from that exact source consumes the exemption, so an old
    /// connection still draining through a reconnect cannot steal it.
    ///
    /// `connection_instance` ties the arm to the exact connection object the
    /// caller believes just went live, not merely to `node_id`. Publication
    /// (compare-and-publish into the connection pool) and this arm are two
    /// separate operations, so a caller can still be running this call after
    /// a NEWER connection for the same peer has already been published --
    /// e.g. a stale outbound finalizer, or a stale inbound accept handler
    /// whose own tie-break resolution is followed by an `.await` a
    /// concurrent, faster accept/finalize can race past.
    ///
    /// The revalidation (a pure, non-mutating snapshot read -- never the
    /// self-healing `get_connection_by_peer_id`, whose side effects must
    /// not fire from a decision path) happens WHILE HOLDING the
    /// `gossip_state` lock, immediately before writing `peer_info`: a
    /// version checked-then-released-then-reacquired would leave the same
    /// race window this is meant to close (a descheduled stale task could
    /// pass the check, let a newer session arm, then resume and clobber it
    /// anyway). `peer_current_connection_snapshot` is a synchronous,
    /// lock-free read with no `.await` of its own, so calling it inside an
    /// already-held async mutex cannot deadlock or block other lockers for
    /// longer than an ordinary map read.
    ///
    /// If a DIFFERENT connection is now the peer's current published one,
    /// `connection_instance` has been superseded and the arm is skipped
    /// entirely rather than clobbering the newer session's discriminator
    /// with this stale caller's obsolete source. A peer with no currently
    /// published connection at all (e.g. a caller that manages sessions
    /// without registering into the connection pool, as local/test callers
    /// do) is not evidence of supersession, so the arm proceeds.
    pub async fn arm_sequence_reset_for_new_session(
        &self,
        peer_addr: SocketAddr,
        node_id: crate::GossipNodeId,
        session_source: SocketAddr,
        peer_id: &crate::PeerId,
        connection_instance: &std::sync::Arc<crate::connection_pool::LockFreeConnection>,
    ) {
        let mut gossip_state = self.gossip_state.lock().await;
        if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr)
            && peer_info.node_id == Some(node_id)
        {
            let superseded_by_a_different_connection = self
                .connection_pool
                .peer_current_connection_snapshot(peer_id)
                .is_some_and(|current| !std::sync::Arc::ptr_eq(&current, connection_instance));
            if superseded_by_a_different_connection {
                debug!(
                    peer = %peer_addr,
                    session_source = %session_source,
                    "R-11: declining to arm sequence-reset; this connection instance \
                     has already been superseded by a newer published session"
                );
                return;
            }

            // Nothing to do for a peer that never got past sequence 0.
            if peer_info.last_sequence > 0 {
                debug!(
                    peer = %peer_addr,
                    session_source = %session_source,
                    last_sequence = peer_info.last_sequence,
                    "R-11: new authenticated session; will accept one \
                     lower-sequence FullSync from this connection"
                );
            }
            peer_info.accept_lower_sequence_from = Some(session_source);
            // Persists across the one-shot's consumption -- see the field
            // doc comment. This is what lets `merge_full_sync_from` keep
            // rejecting an old, still-draining connection's traffic for the
            // rest of this peer's session, not merely for the first sync.
            peer_info.current_session_source = Some(session_source);
            // The connection instance backing `current_session_source`.
            // Checked by `peer_info_is_from_current_session` so a
            // subsequently disconnected/superseded session's discriminator
            // self-expires instead of permanently rejecting a live,
            // non-arming successor's traffic.
            peer_info.current_session_connection = Some(std::sync::Arc::downgrade(connection_instance));
            // A new session epoch begins here: any in-flight apply that
            // captured an epoch before this point must be dropped rather
            // than allowed to write, even if it validated as "current
            // session" moments before this arm. Drawn from the process-wide
            // counter, NEVER derived from the current value (see
            // `next_session_epoch`'s doc comment for why a locally-reset
            // scheme is unsafe here): a `PeerInfo` recreated at the same
            // address (or reused by a different peer identity) must not be
            // able to reproduce an epoch a still-in-flight, already-stale
            // apply captured against the entry it replaced.
            peer_info.current_session_epoch = next_session_epoch();
        }
    }

    pub async fn add_peer_with_node_id(
        &self,
        peer_addr: SocketAddr,
        node_id: Option<crate::GossipNodeId>,
    ) {
        debug!(peer = %peer_addr, self_addr = %self.bind_addr, has_node_id = node_id.is_some(), "add_peer_with_node_id called");
        if peer_addr.ip().is_unspecified() || peer_addr.port() == 0 {
            debug!(
                peer = %peer_addr,
                "refusing to add peer with unspecified address or zero port"
            );
            return;
        }
        // Identity self-filter (authoritative — address alone is not
        // sufficient when advertise_address != bind_addr; see
        // `on_peer_list_gossip`'s matching filter for the full rationale).
        if node_id == Some(self.peer_id.to_node_id()) {
            debug!(
                peer = %peer_addr,
                "refusing to add self as peer (node_id identifies this node)"
            );
            return;
        }
        if peer_addr != self.bind_addr {
            {
                let mut gossip_state = self.gossip_state.lock().await;

                // Check if we already have this peer
                if let Some(existing_peer) = gossip_state.peers.get_mut(&peer_addr) {
                    // Update GossipNodeId if provided and not already set
                    if node_id.is_some() && existing_peer.node_id.is_none() {
                        existing_peer.node_id = node_id;
                        debug!(peer = %peer_addr, "updated existing peer with GossipNodeId");
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
                    let current_time_ms = crate::current_timestamp_millis();
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
                            last_response_received_ms: current_time_ms,
                            accept_lower_sequence_from: None,
                            current_session_source: None,
                            current_session_connection: None,
                            current_session_epoch: 0,
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

            // Safely update connection pool if we have a GossipNodeId
            // This is critical for TLS connections to work (get_connection_to_peer needs this mapping)
            if let Some(id) = node_id {
                let peer_id = id.to_peer_id();

                // A peer re-announced at a new address (e.g. a restart on a
                // fresh ephemeral port) keeps the SAME verified identity, so its
                // live session must not be torn down: identity, not socket
                // address, owns the connection (PEER_ID_REFACTOR §1). Tearing
                // the session down here on `old_addr != new_addr` was the
                // address-keyed leak behind the single-node-restart reconnect
                // thrash — a freshly-accepted preferred inbound got
                // `disconnect_by_peer_id`'d the moment the peer's advertised
                // address changed. We only reindex the address mapping; the old
                // connection, if any, is retired by its own IO lifecycle when it
                // actually dies, never by an address change alone.
                if let Some(old_addr) = self.connection_pool.get_configured_peer_addr(&peer_id)
                    && old_addr != peer_addr
                {
                    debug!(
                        peer_id = %peer_id,
                        old_addr = %old_addr,
                        new_addr = %peer_addr,
                        "peer advertised a new address; reindexing without disconnecting the live session"
                    );
                }

                let pool = &self.connection_pool;
                pool.set_discovered_peer_addr(&peer_id, peer_addr);
                let _ = pool.addr_to_peer_id.upsert_sync(peer_addr, peer_id.clone());
                pool.reindex_connection_addr(&peer_id, peer_addr);
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

    /// p2p configured-peer supervisor tick. For every configured (required)
    /// peer, keep a *direct point-to-point* connection alive: dial only when it
    /// is down (a no-op when already connected, via pooled reuse), and surface a
    /// liveness signal from the connect result. Point-to-point only — no gossip,
    /// no broadcast (≤ N connect-attempts per tick for N configured peers, ~0 in
    /// steady state). Driven by the background timer at `peer_supervisor_interval`;
    /// gossip independently and complementarily observes these connections.
    pub async fn supervise_configured_peers(&self) {
        let peers = self.connection_pool.list_configured_peers();
        for (peer_id, addr) in peers {
            // Already connected -> reachable. Leave the connection alone: do not
            // re-dial (no storm) and do not reset liveness state (so gossip's own
            // dead-peer detection still fires). Data flows over this connection
            // at full speed.
            if self
                .connection_pool
                .get_connected_connection_to_peer(&peer_id)
                .is_some()
            {
                self.note_peer_liveness(&peer_id, addr, true, "connected")
                    .await;
                continue;
            }
            // A repeated duplicate-connection tie-break eviction for this
            // peer happened within the last `tie_break_reconnect_cooldown`
            // window (see `note_tie_break_eviction`). This supervisor loop deliberately
            // bypasses `peer_retry_interval` so a genuinely-down required
            // peer reconnects promptly; without this check that same
            // unthrottled cadence turns a tie-break-induced flap (dial,
            // evicted almost instantly, dial again) into a sustained
            // TCP-connect + TLS-accept storm at the supervisor's tick rate.
            // Skip this tick only; the next tick retries once the cooldown
            // expires, so a real reconnect is delayed by at most one
            // cooldown window, never dropped.
            if self.tie_break_cooldown_active(&peer_id) {
                debug!(
                    peer_id = %peer_id,
                    addr = %addr,
                    "supervisor: reconnect cooldown active, skipping this tick"
                );
                continue;
            }
            // No connection -> actively *establish* one now. This is what makes a
            // freshly-started peer connect immediately (rather than waiting for a
            // lazy gossip round) and what reconnects after a connection is lost.
            // `connect_to_peer` establishes a TCP connection, reuses a healthy
            // pooled connection, and refreshes the gossip liveness state from the
            // result. Bounded so the 1Hz cadence holds even when a peer is down.
            let budget = self.config.connection_timeout.min(Duration::from_millis(
                crate::config::SUPERVISOR_PER_ATTEMPT_BUDGET_MS,
            ));
            match tokio::time::timeout(budget, self.connect_to_peer(&peer_id)).await {
                Ok(Ok(())) => {
                    self.note_peer_liveness(&peer_id, addr, true, "established")
                        .await
                }
                Ok(Err(e)) => {
                    self.note_peer_liveness(
                        &peer_id,
                        addr,
                        false,
                        &format!("connect failed: {e:?}"),
                    )
                    .await
                }
                Err(_) => {
                    self.note_peer_liveness(&peer_id, addr, false, "connect timed out")
                        .await
                }
            }
        }
    }

    /// Emit the supervisor liveness signal. While a required peer is unreachable
    /// this logs a CRITICAL line every tick (continuous alert, greppable as
    /// `not connected; retrying`); the recovery edge logs once. The optional
    /// `PeerLivenessHandler` fires only on reachable<->unreachable edges.
    async fn note_peer_liveness(
        &self,
        peer_id: &crate::PeerId,
        addr: SocketAddr,
        reachable: bool,
        reason: &str,
    ) {
        let prev = self
            .peer_liveness_status
            .read_sync(peer_id, |_, v| v.reachable);
        let flipped = prev != Some(reachable);

        if !reachable {
            tracing::error!(
                event_code = "icanact_peer_unreachable",
                severity = "CRITICAL",
                critical = true,
                peer_id = %peer_id,
                addr = %addr,
                reason = %reason,
                "configured peer is not connected; retrying"
            );
        } else if flipped && prev == Some(false) {
            info!(
                event_code = "icanact_peer_reachable",
                peer_id = %peer_id,
                addr = %addr,
                "configured peer reconnected"
            );
        }

        let _ = self.peer_liveness_status.upsert_sync(
            peer_id.clone(),
            PeerLivenessStatus {
                reachable,
                updated_at: Instant::now(),
            },
        );

        if flipped {
            if let Some(cell) = self.peer_liveness_handler.load_full() {
                cell.handler
                    .handle_peer_liveness(peer_id.clone(), addr, reachable, reason.to_string())
                    .await;
            }
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
            if let Some(existing_peer_id) = pool
                .addr_to_peer_id
                .read_sync(&new_addr, |_, peer_id| peer_id.clone())
            {
                if let Some(old_peer_id) = pool
                    .addr_to_peer_id
                    .read_sync(&peer_addr, |_, peer_id| peer_id.clone())
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
                // Update peer_id_to_addr mapping without making DNS-refreshed
                // discovered routes required supervisor peers.
                pool.set_discovered_peer_addr(&peer_id, new_addr);
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

        // ACTOR_REM_2 R13(c): drop the old address's clock-calibration state on a
        // DNS migration. Calibration (RTT / clock offset) is specific to the
        // endpoint, so the new address must be re-probed rather than inheriting
        // stale samples; not removing it also leaks the old-addr entries.
        self.remove_clock_state_for_addr(&peer_addr);

        Some(new_addr)
    }

    /// Connect to a configured peer by peer ID
    pub async fn connect_to_peer(&self, peer_id: &crate::PeerId) -> Result<()> {
        let pool = &self.connection_pool;
        let configured_addr = pool
            .get_required_peer_addr(peer_id)
            .or_else(|| pool.get_configured_peer_addr(peer_id));
        match pool.get_connection_to_required_peer(peer_id).await {
            Ok(conn) => {
                let mut recovered_addrs = Vec::with_capacity(2);
                recovered_addrs.push(conn.addr);
                if let Some(addr) = configured_addr
                    && addr != conn.addr
                {
                    recovered_addrs.push(addr);
                }
                let mut gossip_state = self.gossip_state.lock().await;
                let now = current_timestamp();
                let now_ms = crate::current_timestamp_millis();
                for (peer_addr, peer_info) in gossip_state.peers.iter_mut() {
                    if recovered_addrs.contains(peer_addr) {
                        peer_info.failures = 0;
                        peer_info.outbound_dial_success = true;
                        peer_info.last_success = now;
                        peer_info.last_response_received_ms = now_ms;
                        peer_info.last_failure_time = None;
                    }
                }
                for (peer_addr, peer_info) in gossip_state.known_peers.iter_mut() {
                    if recovered_addrs.contains(peer_addr) {
                        peer_info.failures = 0;
                        peer_info.outbound_dial_success = true;
                        peer_info.last_success = now;
                        peer_info.last_response_received_ms = now_ms;
                        peer_info.last_failure_time = None;
                    }
                }
                info!(peer_id = %peer_id, "Connected to peer");
                Ok(())
            }
            Err(err) => {
                if let Some(addr) = configured_addr {
                    let mut gossip_state = self.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                        peer_info.failures = self.config.max_peer_failures;
                        peer_info.last_failure_time = Some(current_timestamp());
                    }
                }
                Err(err)
            }
        }
    }

    /// Register a local actor (fast path - minimal locking) with vector clock increment
    pub async fn register_actor(&self, name: String, location: RemoteActorLocation) -> Result<()> {
        self.register_actor_with_priority(name, location, RegistrationPriority::Normal)
            .await
    }

    /// Register a local actor after dropping any learned remote owner for the same name.
    ///
    /// This is for operator-configured services that are known to be singleton owners
    /// after binding their advertised socket. It does not ignore local duplicates.
    pub async fn register_actor_replacing_known(
        &self,
        name: String,
        location: RemoteActorLocation,
    ) -> Result<()> {
        if self
            .actor_state
            .known_actors
            .remove_sync(name.as_str())
            .is_some()
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.release_actor_admission(&name);
        }
        self.register_actor_with_priority(name, location, RegistrationPriority::Normal)
            .await
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
        // Cheap atomic short-circuit before any allocation or
        // contention on `gossip_state`. Mirrors `prepare_gossip_round`'s
        // pre-check so a registration storm during shutdown doesn't
        // pile up on the big lock.
        if self.shutdown.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }

        let register_start_time = crate::current_timestamp_nanos();

        // Update the location with current wall time and priority
        location.wall_clock_time = current_timestamp();
        location.priority = priority;

        // If peers reflected our own actor back to us via gossip, treat that as stale local state:
        // local registration must be able to (re)assert itself without tripping "already exists".
        let self_node_id = self.peer_id.to_node_id();
        if let Some(loc) = self
            .actor_state
            .known_actors
            .read_sync(name.as_str(), |_, location| location.clone())
        {
            if loc.node_id == self_node_id || loc.address == location.address {
                if self
                    .actor_state
                    .known_actors
                    .remove_sync(name.as_str())
                    .is_some()
                {
                    let mut gossip_state = self.gossip_state.lock().await;
                    gossip_state.release_actor_admission(&name);
                }
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
        let previous_tombstone = self
            .actor_state
            .removed_actors
            .read_sync(name.as_str(), |_, tombstone| tombstone.clone());
        if let Some(tombstone) = previous_tombstone.as_ref() {
            location.vector_clock.merge(&tombstone.vector_clock);
        }
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
        let _ = self.actor_state.removed_actors.remove_sync(name.as_str());

        // Update gossip state with pending change - choose queue based on priority
        let should_trigger_immediate = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Re-check shutdown under the lock — the atomic is the
            // canonical source of truth (see `shutdown()`); the
            // legacy mutex bool is a redundant cache that lags the
            // atomic, so trust the atomic.
            if self.shutdown.load(Ordering::Acquire) {
                let _ = self.actor_state.local_actors.remove_sync(name.as_str());
                if let Some(tombstone) = previous_tombstone.clone() {
                    let _ = self
                        .actor_state
                        .removed_actors
                        .upsert_sync(name.clone(), tombstone);
                }
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
            let gossip_trigger_time = crate::current_timestamp_nanos();
            let registration_duration_ms =
                gossip_trigger_time.saturating_sub(register_start_time) as f64 / 1_000_000.0;

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
        // Cheap atomic short-circuit before touching any state.
        if self.shutdown.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }

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
            .read_sync(name, |_, location| location.clone())
        {
            if loc.node_id == self_node_id {
                if self.actor_state.known_actors.remove_sync(name).is_some() {
                    let mut gossip_state = self.gossip_state.lock().await;
                    gossip_state.release_actor_admission(name);
                }
            }
        }

        if let Some(ref location) = removed {
            info!(actor_name = %name, "unregistered local actor");

            // Track this change for delta gossip - use the priority from the removed actor
            let should_trigger_immediate = {
                let mut gossip_state = self.gossip_state.lock().await;

                // Re-check shutdown under the lock — see register_actor_with_priority.
                if self.shutdown.load(Ordering::Acquire) {
                    let _ = self
                        .actor_state
                        .local_actors
                        .upsert_sync(name.to_string(), location.clone());
                    return Err(GossipError::Shutdown);
                }

                // Create a new vector clock for the removal with proper causality
                let removal_clock = location.vector_clock.clone();
                removal_clock.increment(self.peer_id.to_node_id());
                let _ = self.actor_state.removed_actors.upsert_sync(
                    name.to_string(),
                    RemovedActorTombstone::new(removal_clock.clone()),
                );

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
            .read_sync(name, |_, location| location.clone())
        {
            debug!(actor_name = %name, location = "local", "actor found");
            return Some(location);
        }

        // Check known remote actors
        if let Some(location) = self
            .actor_state
            .known_actors
            .read_sync(name, |_, location| location.clone())
        {
            let now = current_timestamp();
            let age_secs = now.saturating_sub(location.wall_clock_time);
            if age_secs < self.config.actor_ttl.as_secs()
                // R-1: a connected owner's actor is reachable regardless of
                // wall-clock age; TTL only gates actors of unreachable peers.
                || self.owner_peer_is_connected(&location)
            {
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
                .filter(|p| {
                    p.failures < self.config.max_peer_failures
                        && (p.outbound_dial_success || p.inbound_observed)
                })
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
            addr_substitutions: self.addr_substitutions.load(Ordering::Relaxed),
            relayed_addr_kept: self.relayed_unusable_addr_kept.load(Ordering::Relaxed),
            tie_break_evictions: self.tie_break_evictions.load(Ordering::Relaxed),
        }
    }

    /// Apply delta changes from a peer.
    ///
    /// Returns the names of immediate-priority `ActorAdded` changes that were
    /// actually applied (i.e. passed vector-clock conflict resolution).
    /// Duplicate deltas whose contents were all suppressed return an empty
    /// list, making delta application observably idempotent.
    pub async fn apply_delta(&self, delta: RegistryDelta) -> Result<Vec<String>> {
        self.apply_delta_from(delta, None, None).await
    }

    /// Apply a delta, resolving advertised addresses against the
    /// AUTHENTICATED socket address of the connection that delivered it
    /// (PEER_ID_REFACTOR §1.6): the verified source of THIS message
    /// outranks any locally derived route (configured/discovered), which
    /// may be stale. Wire receive paths must pass `verified_sender_addr`;
    /// `None` (local/test callers) falls back to the connection-pool route.
    ///
    /// `session_guard`, when the caller already validated this delta
    /// against `peer_info_is_from_current_session` under its own lock
    /// acquisition, is `Some((peer_addr, captured_epoch))` -- the same
    /// key and `PeerInfo::current_session_epoch` value observed at
    /// that validation. It is re-checked atomically here, under the same
    /// `gossip_state` lock this function already takes for its actual
    /// mutations, immediately before applying any change: a mismatch means
    /// a newer session was armed (or the validated one self-expired)
    /// between the caller's validation and this call actually running, and
    /// the whole delta is dropped rather than partially or fully applied.
    /// `None` skips the recheck (no session context to validate against,
    /// e.g. local/test callers), matching the un-gated behavior deltas had
    /// before session scoping existed.
    pub async fn apply_delta_from(
        &self,
        delta: RegistryDelta,
        verified_sender_addr: Option<SocketAddr>,
        session_guard: Option<(SocketAddr, u64)>,
    ) -> Result<Vec<String>> {
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
            trace!(
                "receiving immediate changes: {} total changes from {}",
                total_changes, sender_peer_id
            );
        }

        // Pre-capture timing info outside lock for better performance
        let received_timestamp = crate::current_timestamp_nanos();

        // Resolve the sender's addresses before taking the lock so the
        // lock section is short. TWO distinct roles (never conflate them):
        // - `repair_addr` (§1.6 trust anchor): the verified socket source
        //   of THIS message when the wire path supplied it, else the
        //   configured route. Used ONLY for advertised-address repair.
        // - `bookkeeping_addr`: the peer-state key (configured/discovered
        //   route). `peer_to_actors` must key on it — an ephemeral inbound
        //   TCP source is not in `gossip_state.peers`, and actors filed
        //   under it would be invisible to cleanup_dead_peers/pruning.
        let bookkeeping_addr = {
            let pool = &self.connection_pool;
            pool.get_configured_peer_addr(&sender_peer_id)
        };
        let repair_addr = verified_sender_addr.or(bookkeeping_addr);

        // Critical section: apply all known_actors / removed_actors
        // mutations AND the peer_to_actors update under a single
        // gossip_state acquisition. This serialises us against
        // `cleanup_dead_peers`, which takes the same lock — without
        // this, cleanup could observe a half-applied delta and rip
        // `known_actors` entries that the second half of this delta is
        // about to re-track in `peer_to_actors`.
        let mut applied_count = 0usize;
        let mut peer_actor_names_changed = std::collections::HashSet::new();
        let mut applied_immediate: Vec<String> = Vec::new();
        if let Some((session_peer_addr, _)) = session_guard {
            crate::lifecycle::record_transport_event(
                crate::lifecycle::TransportLifecycleEvent::DeltaApplyPendingMutation {
                    peer: sender_peer_id.clone(),
                    addr: session_peer_addr,
                },
            );
        }
        let log_adds: Vec<(String, RemoteActorLocation)> = {
            let mut gossip_state = self.gossip_state.lock().await;

            if let Some((session_peer_addr, expected_epoch)) = session_guard
                && !session_epoch_still_current(&gossip_state, session_peer_addr, expected_epoch)
            {
                debug!(
                    peer = %session_peer_addr,
                    "dropping delta apply; a newer session was armed after \
                     this message's session validation"
                );
                return Ok(Vec::new());
            }

            let mut log_adds = Vec::new();
            let mut sender_actors = bookkeeping_addr
                .and_then(|addr| gossip_state.peer_to_actors.get(&addr).cloned())
                .unwrap_or_default();
            let mut rejected_by_peer_cap = 0usize;
            let mut rejected_by_global_cap = 0usize;
            for change in delta.changes {
                match change {
                    RegistryChange::ActorAdded {
                        name,
                        mut location,
                        priority,
                    } => {
                        // This is the wire path used by
                        // `RegistrationPriority::Immediate` (e.g. routed-pubsub
                        // interest registration, `pubsub.rs::note_interest`).
                        // Same never-drop contract as `merge_full_sync`
                        // (PEER_ID_REFACTOR §1.5): the wire address is
                        // canonicalized (malformed input is bounded, never
                        // persisted verbatim), then — when we have a trusted
                        // sender address — the owner's unusable advertised IP
                        // is resolved from it. Sanitization happens BEFORE
                        // `current_actor_upsert_plan` so conflict resolution
                        // (`stable_concurrent_location_wins` compares
                        // `location.address`) never operates on
                        // attacker-chosen bytes.
                        let owner_is_sender = location.peer_id == sender_peer_id;
                        let wire_addr = canonical_wire_addr(name.as_str(), &location.address);
                        let resolved = match repair_addr {
                            Some(repair_addr) => {
                                let resolved = resolve_remote_actor_addr(
                                    name.as_str(),
                                    wire_addr,
                                    repair_addr,
                                    owner_is_sender,
                                );
                                self.note_actor_addr_resolution(
                                    wire_addr,
                                    resolved,
                                    repair_addr,
                                    owner_is_sender,
                                );
                                resolved
                            }
                            None => wire_addr,
                        };
                        location.address = resolved.to_string();
                        let Some((clear_tombstone, is_update)) = self.current_actor_upsert_plan(
                            name.as_str(),
                            &location,
                            &sender_peer_id,
                        ) else {
                            continue;
                        };
                        let transfers_admission =
                            gossip_state.actor_admission_peer_by_name.get(&name)
                                != Some(&sender_peer_id);
                        if transfers_admission
                            && gossip_state.actor_admission_count(&sender_peer_id)
                                >= self.config.max_known_actors_per_peer
                        {
                            rejected_by_peer_cap += 1;
                            continue;
                        }
                        if !is_update
                            // `known_actors` is the remote-only map; locally
                            // registered actors live in `local_actors` and do
                            // not consume this admission budget.
                            && self.actor_state.known_actors.len()
                                >= self.config.max_known_actors
                        {
                            rejected_by_global_cap += 1;
                            continue;
                        }
                        if clear_tombstone {
                            let _ = self.actor_state.removed_actors.remove_sync(name.as_str());
                        }
                        let _ = self
                            .actor_state
                            .known_actors
                            .upsert_sync(name.clone(), location.clone());
                        gossip_state.record_actor_admission(&sender_peer_id, &name);
                        sender_actors.insert(name.clone());
                        peer_actor_names_changed.insert(name.clone());
                        applied_count += 1;
                        if priority.should_trigger_immediate_gossip() {
                            applied_immediate.push(name.clone());
                        }
                        if tracing::enabled!(tracing::Level::INFO) {
                            log_adds.push((name, location));
                        }
                    }
                    RegistryChange::ActorRemoved {
                        name,
                        vector_clock,
                        removing_node_id,
                        priority,
                    } => {
                        let Some((removal_clock, tombstone_only)) = self
                            .current_actor_removal_plan(
                                name.as_str(),
                                &vector_clock,
                                &removing_node_id,
                            )
                        else {
                            continue;
                        };

                        let forwarded = RegistryChange::ActorRemoved {
                            name: name.clone(),
                            vector_clock: removal_clock.clone(),
                            removing_node_id,
                            priority,
                        };
                        if tombstone_only {
                            let _ = self.actor_state.removed_actors.upsert_sync(
                                name.clone(),
                                RemovedActorTombstone::new(removal_clock),
                            );
                            gossip_state
                                .pending_changes
                                .push(Self::as_regular_gossip_change(&forwarded));
                            continue;
                        }
                        if self
                            .actor_state
                            .known_actors
                            .remove_sync(name.as_str())
                            .is_some()
                        {
                            gossip_state.release_actor_admission(&name);
                            sender_actors.remove(&name);
                            peer_actor_names_changed.insert(name.clone());
                            applied_count += 1;
                            let _ = self
                                .actor_state
                                .removed_actors
                                .upsert_sync(name, RemovedActorTombstone::new(removal_clock));
                            gossip_state
                                .pending_changes
                                .push(Self::as_regular_gossip_change(&forwarded));
                        }
                    }
                }
            }

            if rejected_by_peer_cap > 0 || rejected_by_global_cap > 0 {
                warn!(
                    sender = %sender_peer_id,
                    rejected_by_peer_cap,
                    rejected_by_global_cap,
                    max_known_actors = self.config.max_known_actors,
                    max_known_actors_per_peer = self.config.max_known_actors_per_peer,
                    "rejected remote actor registrations at configured admission caps"
                );
            }

            if let Some(bookkeeping_addr) = bookkeeping_addr {
                gossip_state
                    .peer_to_actors
                    .insert(bookkeeping_addr, sender_actors);
            } else {
                debug!(
                    sender = %sender_peer_id,
                    "no address mapping for sender; skipping peer_to_actors update"
                );
            }
            log_adds
        };

        // Emit per-actor timing logs outside the critical section.
        //
        // All three timestamps are sourced from `SystemTime::now()` on
        // different machines, so clock skew between sender and receiver can
        // make `received_timestamp` less than either reference, which would
        // wrap a `u128` subtraction to ~2^128 and produce nonsense values
        // (e.g. ~3.4e32 ms). Compute as `i128` to detect skew, clamp negative
        // values to zero, and annotate the log so dashboards can filter.
        for (name, location) in log_adds {
            let propagation_delta_ns =
                received_timestamp as i128 - location.local_registration_time as i128;
            let network_delta_ns = received_timestamp as i128 - delta.precise_timing_nanos as i128;
            let clock_skew = propagation_delta_ns < 0 || network_delta_ns < 0;
            let propagation_time_ms = propagation_delta_ns.max(0) as f64 / 1_000_000.0;
            let network_processing_time_ms = network_delta_ns.max(0) as f64 / 1_000_000.0;
            let processing_only_time_ms =
                (propagation_time_ms - network_processing_time_ms).max(0.0);
            info!(
                actor_name = %name,
                priority = ?location.priority,
                propagation_time_ms = propagation_time_ms,
                network_processing_time_ms = network_processing_time_ms,
                processing_only_time_ms = processing_only_time_ms,
                clock_skew = clock_skew,
                "RECEIVED_ACTOR"
            );
        }

        let peer_actor_changes = peer_actor_names_changed.len();

        debug!(
            sender = %sender_peer_id,
            total_changes,
            applied_changes = applied_count,
            peer_actor_changes = peer_actor_changes,
            "completed delta application with vector clock conflict resolution"
        );

        Ok(applied_immediate)
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

        // Include urgent changes first (they have higher priority).
        //
        // Regularize the priority before embedding into a periodic delta.
        // `prepare_gossip_round` drains `urgent_changes` into `delta_history`
        // with priority demoted via `as_regular_gossip_change`, but releases
        // the gossip lock between that drain and the call into
        // `create_delta_from_state`. Concurrent producers
        // (`register_actor_with_priority`, `handle_peer_death`) can push raw
        // `Immediate` entries into `urgent_changes` in that window. Without
        // regularizing here, those leak into the periodic delta path and
        // arrive at peers tagged Immediate every gossip tick. The dedicated
        // one-shot fan-out path is `trigger_immediate_gossip`, not this one.
        changes.extend(
            gossip_state
                .urgent_changes
                .iter()
                .map(Self::as_regular_gossip_change),
        );

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
            sender_bind_addr: Some(self.advertised_addr().to_string()), // reachable advertised address (NAT-aware), not the raw bind
            sequence,
            wall_clock_time: current_timestamp(),
            extensions: None,
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
            sender_bind_addr: Some(self.advertised_addr().to_string()), // reachable advertised address (NAT-aware), not the raw bind
            sequence,
            wall_clock_time: current_timestamp(),
            extensions: None,
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
        Ok(RegistryMessage::DeltaGossipResponse {
            delta,
            extensions: None,
        })
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

    fn current_actor_upsert_plan(
        &self,
        name: &str,
        location: &RemoteActorLocation,
        sender_peer_id: &PeerId,
    ) -> Option<(bool, bool)> {
        if location.peer_id == self.peer_id {
            debug!(
                actor_name = %name,
                "skipping remote actor update - change references this node as the host"
            );
            return None;
        }

        if self.actor_state.local_actors.contains_sync(name) {
            debug!(
                actor_name = %name,
                "skipping remote actor update - actor is local"
            );
            return None;
        }

        let mut clear_tombstone = false;
        if let Some(tombstone) = self
            .actor_state
            .removed_actors
            .read_sync(name, |_, tombstone| tombstone.vector_clock.clone())
        {
            match location.vector_clock.compare(&tombstone) {
                crate::ClockOrdering::After => {
                    clear_tombstone = true;
                }
                crate::ClockOrdering::Before | crate::ClockOrdering::Concurrent
                    if owner_recovery_wins_tombstone(location, sender_peer_id, &tombstone) =>
                {
                    clear_tombstone = true;
                }
                crate::ClockOrdering::Before
                | crate::ClockOrdering::Equal
                | crate::ClockOrdering::Concurrent => {
                    debug!(
                        actor_name = %name,
                        "skipping remote actor update - actor tombstone is newer or concurrent"
                    );
                    return None;
                }
            }
        }

        let is_update = self
            .actor_state
            .known_actors
            .read_sync(name, |_, existing_location| {
                match location
                    .vector_clock
                    .compare(&existing_location.vector_clock)
                {
                    crate::ClockOrdering::After => Some(true),
                    crate::ClockOrdering::Concurrent | crate::ClockOrdering::Equal => {
                        stable_concurrent_location_wins(location, existing_location).then_some(true)
                    }
                    crate::ClockOrdering::Before => None,
                }
            });

        match is_update {
            Some(Some(true)) => Some((clear_tombstone, true)),
            Some(Some(false)) | Some(None) => None,
            None => {
                debug!(actor_name = %name, "applying new actor");
                Some((clear_tombstone, false))
            }
        }
    }

    fn current_actor_removal_plan(
        &self,
        name: &str,
        vector_clock: &crate::VectorClock,
        removing_node_id: &crate::GossipNodeId,
    ) -> Option<(crate::VectorClock, bool)> {
        if self.actor_state.local_actors.contains_sync(name) {
            debug!(
                actor_name = %name,
                "skipping actor removal - actor is local"
            );
            return None;
        }

        let should_remove = self.actor_state.known_actors.read_sync(
            name,
            |_, existing_location| match vector_clock.compare(&existing_location.vector_clock) {
                crate::ClockOrdering::After => {
                    debug!(
                        actor_name = %name,
                        "removal is causally after current state - applying"
                    );
                    Some(false)
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
                    should_apply.then_some(false)
                }
                _ => {
                    debug!(
                        actor_name = %name,
                        "removal is outdated - ignoring"
                    );
                    None
                }
            },
        );

        match should_remove {
            Some(Some(tombstone_only)) => Some((vector_clock.clone(), tombstone_only)),
            Some(None) => None,
            None => {
                let tombstone_clock = vector_clock.clone();
                if let Some(existing_tombstone) = self
                    .actor_state
                    .removed_actors
                    .read_sync(name, |_, tombstone| tombstone.vector_clock.clone())
                {
                    match vector_clock.compare(&existing_tombstone) {
                        crate::ClockOrdering::Before | crate::ClockOrdering::Equal => {
                            return None;
                        }
                        crate::ClockOrdering::After => {}
                        crate::ClockOrdering::Concurrent => {
                            tombstone_clock.merge(&existing_tombstone);
                        }
                    }
                }

                debug!(
                    actor_name = %name,
                    "actor not found - will record removal tombstone"
                );
                Some((tombstone_clock, true))
            }
        }
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

        // Step 1: Check shutdown status first. Read the atomic instead
        // of taking the gossip_state lock — the atomic is the canonical
        // source and avoids unnecessary contention when shutdown is set.
        if self.shutdown.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }

        // Get actor state snapshot for message creation (scc maps, no
        // lock contention with gossip_state).
        let (local_actors, known_actors) = self.snapshot_actor_maps();

        // Steps 2 + 3 under a single lock acquisition: the
        // commit-pending and select-peers phases used to release and
        // re-acquire the gossip_state lock between them, leaving a race
        // window during which `register_actor`,
        // `mark_inbound_connection_observed`, `cleanup_dead_peers`, or
        // `merge_full_sync` could mutate the state that step 3 was
        // about to read. That produced gossip tasks whose
        // `since_sequence` / `current_sequence` no longer matched the
        // peer's actual `last_sequence`, which receivers then dropped
        // as "old gossip". Holding the lock across
        // `create_delta_from_state` is safe because that helper takes
        // `&GossipState` and does not re-acquire the lock.
        let current_sequence: u64;
        let tasks = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Double-check shutdown after acquiring the lock — see step
            // 1 for why the atomic is canonical.
            if self.shutdown.load(Ordering::Acquire) {
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
            let had_changes =
                !gossip_state.pending_changes.is_empty() || !gossip_state.urgent_changes.is_empty();

            // Increment sequence if we have changes
            if had_changes {
                gossip_state.gossip_sequence += 1;

                // Commit pending + urgent changes to history together.
                // Urgent entries are folded into the same `HistoricalDelta`
                // so subsequent rounds pull them from `delta_history`
                // (the since_sequence-bounded path in
                // `create_delta_from_state`) rather than re-cloning the
                // urgent queue forever.
                let mut combined = std::mem::take(&mut gossip_state.pending_changes);
                combined.extend(
                    std::mem::take(&mut gossip_state.urgent_changes)
                        .iter()
                        .map(Self::as_regular_gossip_change),
                );
                let delta = HistoricalDelta {
                    sequence: gossip_state.gossip_sequence,
                    changes: combined,
                    recorded_at: Instant::now(),
                };
                gossip_state.delta_history.push(delta);
                if gossip_state.delta_history.len() > self.config.max_delta_history {
                    gossip_state.delta_history.remove(0);
                }
            }

            if gossip_state.peers.is_empty() {
                return Ok(Vec::new());
            }
            current_sequence = gossip_state.gossip_sequence;

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

            // Filter to retry-eligible, non-suppressed peers and deduplicate
            // by stable identity (GossipNodeId) so a physical peer that is tracked
            // under multiple SocketAddr keys — ephemeral TCP-source still
            // present alongside its migrated bind address, dual-stack
            // IPv4/IPv6 aliases, DNS-resolved hostnames — receives one
            // delivery per round. Peers whose GossipNodeId is not yet known
            // continue to be keyed by SocketAddr.
            #[derive(Hash, Eq, PartialEq)]
            enum DispatchKey {
                Node(crate::GossipNodeId),
                Addr(SocketAddr),
            }
            let mut seen: std::collections::HashSet<DispatchKey> = std::collections::HashSet::new();
            let mut available_peers: Vec<SocketAddr> = Vec::new();
            for (peer_addr, peer) in gossip_state.peers.iter() {
                let retry_window_open = peer.failures < self.config.max_peer_failures
                    || (current_time.saturating_sub(peer.last_attempt))
                        > self.config.peer_retry_interval.as_secs();
                if !retry_window_open {
                    continue;
                }
                if self.should_suppress_outbound_retry_for_peer(peer) {
                    debug!(
                        peer = %peer_addr,
                        inbound_observed = peer.inbound_observed,
                        outbound_dial_success = peer.outbound_dial_success,
                        peer_addr_key = %peer.address,
                        "Suppressing outbound retry for inbound-only undialable peer"
                    );
                    continue;
                }
                let key = peer
                    .node_id
                    .map(DispatchKey::Node)
                    .unwrap_or(DispatchKey::Addr(*peer_addr));
                if seen.insert(key) {
                    available_peers.push(*peer_addr);
                }
            }

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
                            .map(|t| current_time.saturating_sub(t))
                            .unwrap_or(0);
                        let time_since_last_attempt =
                            current_time.saturating_sub(peer_info.last_attempt);
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
                        last_response_received_ms: crate::current_timestamp_millis(),
                        accept_lower_sequence_from: None,
                        current_session_source: None,
                        current_session_connection: None,
                        current_session_epoch: 0,
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
                                let message = RegistryMessage::DeltaGossip {
                                    delta,
                                    extensions: None,
                                };
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
        let current_time_ms = crate::current_timestamp_millis();

        // Collect peers that crossed the death threshold in this batch; we
        // fire `handle_peer_death` after dropping the `gossip_state` lock
        // to avoid lock-ordering issues (it acquires the same lock to
        // reset `last_sequence`).
        let mut newly_dead: Vec<SocketAddr> = Vec::new();

        for result in results {
            match result.outcome {
                Ok(response_opt) => {
                    let liveness_window_ms =
                        self.effective_peer_liveness_window_ms(result.peer_addr);
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
                            // the effective liveness window, treat the next
                            // no-response round as a soft failure. Configured
                            // peers floor this window to two regular-gossip
                            // (`gossip_interval`) intervals — the cadence
                            // that actually refreshes
                            // `last_response_received_ms` — so one delayed
                            // inbound gossip response cannot false-fail a
                            // required direct route. This still catches
                            // "outbound writes succeed at the kernel level but
                            // the peer isn't reading anymore" — the scenario
                            // that kept `538a99…` alive on `stratum-devnet-a`
                            // for 66 minutes.
                            //
                            // `last_response_received_ms` is initialised to the
                            // peer's creation time, so a brand-new peer doesn't
                            // immediately look stale; it has at least one
                            // `peer_liveness_window` to be observed responding.
                            let silence_ms =
                                current_time_ms.saturating_sub(peer_info.last_response_received_ms);
                            if response_opt.is_none()
                                && silence_ms > liveness_window_ms
                                && peer_info.failures < self.config.max_peer_failures
                            {
                                peer_info.failures += 1;
                                info!(
                                    peer = %result.peer_addr,
                                    silence_ms,
                                    liveness_window_ms,
                                    new_failures = peer_info.failures,
                                    "no response within effective peer liveness window; \
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

                    // Process response if we got one. Only inbound
                    // payload counts as proof of liveness — a successful
                    // send only means the kernel buffer accepted the
                    // bytes, which is exactly what the
                    // response-asymmetry detector above is designed to
                    // catch. Reset failures here, not on every
                    // successful send.
                    if let Some(response) = response_opt {
                        self.mark_response_received(result.peer_addr, current_time_ms)
                            .await;
                        self.record_peer_activity(result.peer_addr).await;
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

        // Crossing the failure threshold is a transport-local verdict,
        // not an actor-liveness verdict. Keep remote actors available
        // for reconnect/failover and let `cleanup_dead_peers` reclaim
        // them only after the dead-peer timeout. This keeps the gossip
        // table stable during short disconnects and avoids publishing
        // ActorRemoved tombstones before the consensus path has even run.
        newly_dead.sort_by_key(|a| (a.ip(), a.port()));
        newly_dead.dedup();
        for addr in newly_dead {
            // The peer crossed the failure threshold via response-asymmetry: we
            // kept sending but received nothing back for `peer_liveness_window`.
            // Tear down the now-stale connection so the very next send/connect
            // re-establishes a fresh one (self-correcting), and so
            // `get_connected_connection_to_peer` stops reporting a dead peer as
            // connected. Only the transport connection is removed — actor state is
            // RETAINED so a reconnecting peer keeps its actors (a returning peer's
            // re-negotiation handshake replaces the entry even sooner via
            // `publish_current_peer_connection`).
            //
            // Captured BEFORE any teardown mutation below, same as the other
            // two connection-teardown paths
            // (`handle_peer_connection_failure`/`_by_peer_id`): re-checked
            // immediately before the discovery clear, so a replacement
            // connection that calls `mark_peer_connected` for this address
            // in the gap is detected and the clear declines rather than
            // wiping out a state it no longer owns.
            let pre_failure_discovery_generation =
                self.capture_pre_failure_discovery_generation(addr).await;

            let pool = &self.connection_pool;
            let peer_id = pool.addr_to_peer_id.read_sync(&addr, |_, v| v.clone());
            let mut torn_down_via_peer_id = false;
            if let Some(peer_id) = peer_id
                && pool.disconnect_connection_by_peer_id(&peer_id).is_some()
            {
                info!(
                    peer = %addr,
                    %peer_id,
                    "peer reached failure threshold; tore down stale connection \
                     (actors retained for reconnection)"
                );
                torn_down_via_peer_id = true;
            }
            if !torn_down_via_peer_id {
                if pool.has_connection(&addr) {
                    pool.remove_connection(addr);
                    info!(
                        peer = %addr,
                        "peer reached failure threshold; removed stale connection by address \
                         (actors retained for reconnection)"
                    );
                } else {
                    info!(
                        peer = %addr,
                        "peer reached failure threshold; no live connection to tear down \
                         (actors retained for reconnection)"
                    );
                }
            }

            // Same guarded discovery clear as the other two teardown paths
            // -- see `handle_peer_connection_failure`'s comment for the
            // full rationale.
            self.clear_discovery_state_if_generation_unchanged(addr, pre_failure_discovery_generation)
                .await;
            self.trigger_immediate_peer_gossip();
        }
    }

    /// Record that we received a response payload from a peer. Updates
    /// `last_response_received_ms` so the response-asymmetry detector in
    /// `apply_gossip_results` knows the peer is alive at the application
    /// layer, not just the kernel-buffer-accepted layer.
    async fn mark_response_received(&self, peer_addr: SocketAddr, now: u64) {
        let mut gossip_state = self.gossip_state.lock().await;
        if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
            // `now` is captured at the start of the gossip batch; a later
            // inbound response may have already advanced these fields.
            // Take the max so concurrent writes never roll the value
            // backwards.
            peer_info.last_response_received_ms = peer_info.last_response_received_ms.max(now);
            peer_info.last_success = peer_info.last_success.max(current_timestamp());
            // A valid application-level response is strong liveness evidence.
            // Hard socket failures do not produce framed responses; if peer can
            // answer, it must recover immediately.
            if peer_info.failures > 0 {
                peer_info.failures = 0;
                peer_info.last_failure_time = None;
            }
        }
    }

    /// Handle gossip response with vector clock updates
    pub async fn handle_gossip_response(
        &self,
        addr: SocketAddr,
        response: RegistryMessage,
    ) -> Result<()> {
        match response {
            RegistryMessage::DeltaGossipResponse { delta, extensions } => {
                self.record_inbound_gossip_extensions(
                    addr,
                    extensions,
                    crate::current_timestamp_nanos(),
                );
                info!(
                    peer = %addr,
                    sender = %delta.sender_peer_id,
                    changes = delta.changes.len(),
                    "📥 GOSSIP: Received delta gossip response"
                );

                let delta_sequence = delta.current_sequence;

                // `addr` is the verified socket address this response
                // arrived from — the §1.6 trust anchor for address repair.
                // No session_guard: this call path is unreachable for real
                // wire traffic (see the doc comment on the FullSyncResponse
                // arm below for the full trace).
                self.apply_delta_from(delta, Some(addr), None).await?;
                // Don't add peer here - peers are managed through handle_connection

                let now = crate::current_timestamp_millis();
                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    // Out-of-order responses must not roll back the
                    // per-peer high-water mark; mirror the
                    // `merge_full_sync` behavior.
                    peer_info.last_sequence = peer_info.last_sequence.max(delta_sequence);
                    peer_info.consecutive_deltas += 1;
                    // Inbound payload from peer — proves app-level liveness.
                    // Used by the response-asymmetry detector in
                    // `apply_gossip_results`.
                    peer_info.last_response_received_ms =
                        peer_info.last_response_received_ms.max(now);
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
                extensions,
            } => {
                self.record_inbound_gossip_extensions(
                    addr,
                    extensions,
                    crate::current_timestamp_nanos(),
                );
                // Use resolve_peer_addr for safe address resolution with validation
                let Some(sender_socket_addr) =
                    resolve_peer_addr_checked(sender_bind_addr.as_deref(), addr)
                else {
                    warn!(
                        tcp_source = %addr,
                        sender = %sender_peer_id,
                        sender_bind_addr = ?sender_bind_addr,
                        "Ignoring full sync response from peer with non-dialable advertised bind address"
                    );
                    return Ok(());
                };

                info!(
                    tcp_source = %addr,
                    bind_addr = %sender_socket_addr,
                    sender = %sender_peer_id,
                    sequence = sequence,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "📥 GOSSIP: Received full sync response (using bind_addr, not TCP source)"
                );

                // Peer bookkeeping keys on the peer's BIND address; address
                // REPAIR anchors on the verified TCP source (§1.6) — the
                // peer-controlled sender_bind_addr is never a trust anchor.
                //
                // `session_source: None` (falls back to `verified_sender_addr`,
                // the peer's dial-target/bind address) is only correct
                // because this call is unreachable for real wire traffic:
                // `handle_gossip_response` is invoked from
                // `apply_gossip_results`, whose `GossipResult::outcome` is
                // always built from `send_gossip_message_zero_copy`'s
                // `Result<()>` (see `gossip_send_outcome_to_result` in
                // handle.rs) -- a fire-and-forget send that never carries a
                // reply. Every FullSyncResponse actually received over the
                // wire arrives asynchronously on its own connection's read
                // task and is processed by `handle_incoming_message`
                // instead, which threads that connection's real
                // `session_source` (the dialling socket's own local
                // ephemeral port for outbound connections). If this call
                // path is ever wired to a genuine response, `None` here
                // would silently drop legitimate current-session outbound
                // FullSyncResponses under the `from_current_session` gate --
                // thread the real per-connection session source instead.
                self.merge_full_sync_from(
                    local_actors.into_iter().collect(),
                    known_actors.into_iter().collect(),
                    sender_peer_id,
                    sender_socket_addr,
                    Some(addr),
                    None,
                    sequence,
                    wall_clock_time,
                )
                .await;

                let now = crate::current_timestamp_millis();
                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                    peer_info.consecutive_deltas = 0;
                    // `last_sequence` is NOT touched here: `merge_full_sync_from`
                    // above already owns it atomically, including the
                    // session-scoped restart-reset semantics. A second,
                    // unscoped `max()` here would let this arrive-any-order,
                    // any-connection update silently restore a stale
                    // high-water mark that `merge_full_sync_from` correctly
                    // rejected or reset moments earlier.
                    //
                    // Inbound payload from peer — proves app-level liveness.
                    peer_info.last_response_received_ms =
                        peer_info.last_response_received_ms.max(now);
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
        wall_clock_time: u64,
    ) {
        let _ = self
            .merge_full_sync_from(
                remote_local,
                remote_known,
                sender_peer_id,
                sender_addr,
                None,
                None,
                sequence,
                wall_clock_time,
            )
            .await;
    }

    /// Whether a gossip message (FullSync or Delta) claiming to arrive on
    /// `session_source` should be treated as coming from `peer_info`'s
    /// current authenticated session.
    ///
    /// Three explicit cases, in order:
    ///
    /// 1. `connection_pool` still shows the ARMED connection as current
    ///    (nothing supersedes it, including when nothing has ever been
    ///    recorded at all -- e.g. a registry-level caller that manages
    ///    sessions without registering into the pool, as most tests do, or
    ///    a narrow window before the pool has indexed anything): only a
    ///    message whose `session_source` matches the armed source is
    ///    current.
    /// 2. `connection_pool` shows a DIFFERENT connection as current, but
    ///    this message's `session_source` is the OLD armed source itself:
    ///    REJECT outright. This is the superseded connection's own
    ///    traffic -- it must not be treated as current, and critically
    ///    must NOT trigger the self-heal clear below, which would let it
    ///    back in via the resulting "no session armed" fallback and spend
    ///    the exemption (via the ordinary path's unconditional
    ///    `accept_lower_sequence_from = None`) before the actual live
    ///    successor ever gets a chance to use it.
    /// 3. `connection_pool` shows a DIFFERENT connection as current, and
    ///    this message's `session_source` is neither the armed source nor
    ///    (by elimination) unaccounted for: genuine evidence of a live
    ///    successor. Self-heal -- clear the expired session right here so
    ///    the fallback in case 1 accepts this (and all subsequent, until a
    ///    new arm) traffic, instead of permanently rejecting a live,
    ///    non-arming successor forever.
    ///
    /// Must be called with `peer_info` borrowed from an already-locked
    /// `gossip_state`, and the caller must perform any subsequent mutation
    /// before releasing that lock -- this function does not itself hold or
    /// re-acquire `gossip_state`, so it is safe (and required, to stay
    /// atomic with the caller's own write) to call from within an existing
    /// critical section.
    pub(crate) fn peer_info_is_from_current_session(
        &self,
        peer_id: &crate::PeerId,
        peer_info: &mut PeerInfo,
        session_source: Option<SocketAddr>,
    ) -> bool {
        let Some(armed_source) = peer_info.current_session_source else {
            // No session has ever been armed for this peer: accept from
            // any source, preserving prior behavior.
            return true;
        };

        // Independent of whether THIS message came from the armed source:
        // is `connection_pool` currently showing a DIFFERENT connection as
        // the peer's current one than the one that armed the session?
        let current_connection = self.connection_pool.peer_current_connection_snapshot(peer_id);
        let pool_shows_different_connection = current_connection.as_ref().is_some_and(|current| {
            !peer_info
                .current_session_connection
                .as_ref()
                .and_then(|weak| weak.upgrade())
                .is_some_and(|armed| std::sync::Arc::ptr_eq(&armed, current))
        });

        let from_armed_source = session_source == Some(armed_source);

        if !pool_shows_different_connection {
            // Case 1: nothing supersedes the armed connection yet.
            return from_armed_source;
        }

        if from_armed_source {
            // Case 2: the OLD, now-superseded connection's own traffic.
            // Reject without touching any session state.
            return false;
        }

        // Case 3 candidate: the pool shows a different connection is
        // current, and this message isn't from the armed source. That
        // alone is not proof it arrived on the PUBLISHED successor,
        // though -- during rapid reconnects a THIRD connection (a
        // stale/tie-break-losing candidate that never itself became
        // current) also has a `session_source` different from the armed
        // one. Self-heal must only fire for traffic that actually arrived
        // on the connection instance `connection_pool` is publishing as
        // current, confirmed here via `LockFreeConnection::session_source`
        // -- the same non-spoofable per-connection identity `Arc::ptr_eq`
        // checks above, applied to the receiving side instead of the
        // armed side.
        let from_current_published_successor = current_connection
            .as_ref()
            .is_some_and(|current| Some(current.session_source) == session_source);

        if !from_current_published_successor {
            // A stale or tie-break-losing third connection. Reject without
            // self-healing or touching any session state -- only the
            // actually-current published successor may do that.
            return false;
        }

        // Case 3: the live successor's own traffic, confirmed. Self-heal.
        peer_info.current_session_source = None;
        peer_info.accept_lower_sequence_from = None;
        peer_info.current_session_connection = None;
        // The session epoch also ends here, for the same reason as in
        // `arm_sequence_reset_for_new_session`: a pending apply that
        // captured the epoch while this (now-expired) session still
        // validated must not be allowed to write. Drawn fresh from the
        // process-wide counter -- see `next_session_epoch`.
        peer_info.current_session_epoch = next_session_epoch();
        true
    }

    /// Merge a full sync, resolving advertised actor addresses against the
    /// AUTHENTICATED socket address of the connection that delivered it
    /// (PEER_ID_REFACTOR §1.6). `sender_addr` is the bind-derived peer
    /// bookkeeping key (may come from the peer-controlled
    /// `sender_bind_addr` wire field); `verified_sender_addr` is the
    /// actual TCP source and, when present, is the ONLY trust anchor used
    /// for address repair — a peer's self-declared bind must never be
    /// what we repair other addresses from. Wire receive paths must pass
    /// it; `None` (local/test callers) falls back to `sender_addr`.
    ///
    /// `session_source` (R-11) is the connection's own session
    /// discriminator (see `ReadContext::session_source`) and is what the
    /// restart-sequence exemption gates on. For inbound connections it is
    /// identical to `verified_sender_addr` (the remote's ephemeral port is
    /// already unique per connection), so callers that don't distinguish
    /// the two may pass `None` and it falls back to `verified_sender_addr`.
    /// For OUTBOUND connections it must be the dialling socket's own local
    /// ephemeral port, NOT `verified_sender_addr` -- the latter is the
    /// peer's fixed listening port there, identical for every connection we
    /// ever make to it, so it cannot tell a redial's new connection apart
    /// from an old one still draining.
    #[allow(clippy::too_many_arguments)]
    /// Returns whether this message validated as being from the peer's
    /// current authenticated session (`peer_info_is_from_current_session`'s
    /// STEP 1 verdict) -- regardless of whether its specific sequence/actor
    /// content went on to be applied, replay-rejected, or dropped by the
    /// narrower STEP 2 epoch recheck. Callers use this to gate any of
    /// THEIR OWN peer-state bookkeeping (failure/health resets,
    /// `consecutive_deltas`, etc.) that must likewise only be touched by
    /// the current session -- see `handle_incoming_message`'s FullSync /
    /// FullSyncResponse arms.
    pub async fn merge_full_sync_from(
        &self,
        remote_local: HashMap<String, RemoteActorLocation>,
        remote_known: HashMap<String, RemoteActorLocation>,
        sender_peer_id: crate::PeerId,
        sender_addr: SocketAddr,
        verified_sender_addr: Option<SocketAddr>,
        session_source: Option<SocketAddr>,
        sequence: u64,
        _wall_clock_time: u64,
    ) -> bool {
        let repair_addr = verified_sender_addr.unwrap_or(sender_addr);
        let session_source = session_source.or(verified_sender_addr);
        // Don't add peer here - peers are managed through handle_connection

        // Record comprehensive node activity

        // Check if we've already processed this or a newer sequence from this
        // peer, and take the sequence update in the SAME critical section.
        //
        // R-11: a restarted peer resumes from sequence ~0, so this gate would
        // otherwise drop all of its FullSyncs forever (`last_sequence` only
        // advances and is never reset — the `handle_peer_death` reset the
        // comments elsewhere reference no longer exists). The omission-prune
        // then never runs and actors the peer no longer hosts linger until the
        // 24h TTL. `accept_lower_sequence` is a one-shot exemption armed only
        // by a new TLS-authenticated session; it is consumed here so the gate
        // is restored immediately.
        //
        // Gate and update are one critical section because the exemption is
        // one-shot: releasing the lock between them would let two concurrent
        // restarted-peer FullSyncs both observe the armed flag.
        // `captured_epoch` is `PeerInfo::current_session_epoch` as
        // observed at the moment this message was validated as being from
        // the peer's current session, below. STEP 2 re-checks it
        // atomically, under its own lock, immediately before mutating
        // `known_actors`/`peer_to_actors` -- collecting and resolving
        // `updates_to_apply` between here and there involves no lock at
        // all, so a newer session can arm (or the validated one can
        // self-expire) in that gap, and this is what lets STEP 2 detect
        // and drop the now-stale pending write instead of applying it.
        let captured_epoch: Option<u64> = {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr) {
                // Once a session has been armed for this peer, only the
                // connection that armed it is treated as authoritative for
                // *any* sequence update, not merely for the lower-sequence
                // exemption below. An old connection still draining through
                // a reconnect can keep delivering in-flight, numerically
                // HIGH (pre-restart) sequences after the new session's
                // reset; letting those bump `last_sequence` back up via the
                // ordinary non-stale path would make every later FullSync
                // from the restarted peer look stale again with no
                // exemption left to rescue it (the one-shot is already
                // spent). `None` means no session has ever been armed for
                // this peer (fresh peer, or a local/test caller bypassing
                // the TLS-authenticated arming path) and is accepted from
                // any source, preserving prior behavior. See
                // `peer_info_is_from_current_session` for the self-healing
                // expiry check that keeps this from permanently rejecting a
                // live successor once the armed connection is gone.
                let from_current_session =
                    self.peer_info_is_from_current_session(&sender_peer_id, peer_info, session_source);

                if !from_current_session {
                    debug!(
                        peer = %sender_addr,
                        last_sequence = peer_info.last_sequence,
                        received_sequence = sequence,
                        "ignoring gossip from a connection that is not this \
                         peer's current authenticated session"
                    );
                    return false;
                }

                if sequence < peer_info.last_sequence {
                    // The one-shot exemption is scoped to the connection
                    // that armed it: only a FullSync from that exact
                    // verified TCP source (ephemeral port included) may
                    // consume it. A replayed or relayed frame arriving by
                    // any other path cannot consume it. Unverifiable
                    // sources (`None`) fail closed.
                    let armed_for_this_connection = peer_info
                        .accept_lower_sequence_from
                        .zip(session_source)
                        .is_some_and(|(armed, verified)| armed == verified);

                    if !armed_for_this_connection {
                        debug!(
                            last_sequence = peer_info.last_sequence,
                            received_sequence = sequence,
                            "ignoring old gossip message"
                        );
                        // The connection IS the peer's current session (the
                        // check above already confirmed that); this
                        // specific sequence just looks like an in-session
                        // replay. `true` because the caller's own
                        // peer-health bookkeeping is about session
                        // authority, not this particular message's content.
                        return true;
                    }
                    info!(
                        peer = %sender_addr,
                        last_sequence = peer_info.last_sequence,
                        received_sequence = sequence,
                        "R-11: accepting lower-sequence FullSync after peer restart"
                    );
                    // Consume the one-shot and adopt the restarted peer's
                    // sequence line wholesale — `max()` would pin us to the
                    // pre-restart high-water mark and re-close the gate against
                    // every subsequent sync from the restarted peer.
                    // `current_session_source` is untouched: it persists for
                    // the rest of this session so later syncs keep rejecting
                    // any other connection's traffic (see above).
                    peer_info.accept_lower_sequence_from = None;
                    peer_info.last_sequence = sequence;
                } else {
                    // Reached only for messages already confirmed to be
                    // from the current session (or before any session was
                    // ever armed), so clearing the one-shot here cannot be
                    // triggered by an unrelated connection.
                    peer_info.accept_lower_sequence_from = None;
                    peer_info.last_sequence = std::cmp::max(peer_info.last_sequence, sequence);
                }
                Some(peer_info.current_session_epoch)
            } else {
                None
            }
        };

        let mut new_actors = 0;
        let mut updated_actors = 0;
        let mut peer_actors = std::collections::HashSet::new();

        // Collect wire candidates outside the lock; validate current state while applying.
        let mut updates_to_apply = Vec::new();

        // STEP 1: Collect candidate updates outside the gossip_state
        // lock. The current actor/tombstone checks happen in the
        // critical section below so stale pre-lock snapshots cannot
        // overwrite newer gossip.
        // Process remote local actors
        for (name, location) in remote_local {
            if location.peer_id == self.peer_id {
                debug!(
                    actor_name = %name,
                    "skipping full-sync actor update - change references this node as the host"
                );
                continue;
            }
            // PEER_ID_REFACTOR §1.5: never dropped over the address. The
            // stored location carries the resolved address, not the raw
            // wire value, so this node's own future full-sync fan-out
            // re-advertises the repaired route instead of the unusable one.
            let owner_is_sender = location.peer_id == sender_peer_id;
            let wire_addr = canonical_wire_addr(&name, &location.address);
            let resolved =
                resolve_remote_actor_addr(&name, wire_addr, repair_addr, owner_is_sender);
            self.note_actor_addr_resolution(wire_addr, resolved, repair_addr, owner_is_sender);
            let mut location = location;
            location.address = resolved.to_string();
            // Dial hints may only be learned from the OWNER's own gossip
            // (§1.6): a relay's claim about a third party's reachability is
            // unauthenticated and must never (over)write that peer's route.
            updates_to_apply.push((name, location, owner_is_sender.then_some(resolved)));
        }

        // Process remote known actors
        for (name, location) in remote_known {
            if location.peer_id == self.peer_id {
                debug!(
                    actor_name = %name,
                    "skipping full-sync known actor update - change references this node as the host"
                );
                continue;
            }
            let owner_is_sender = location.peer_id == sender_peer_id;
            let wire_addr = canonical_wire_addr(&name, &location.address);
            let resolved =
                resolve_remote_actor_addr(&name, wire_addr, repair_addr, owner_is_sender);
            self.note_actor_addr_resolution(wire_addr, resolved, repair_addr, owner_is_sender);
            let mut location = location;
            location.address = resolved.to_string();
            // Dial hints may only be learned from the OWNER's own gossip
            // (§1.6): a relay's claim about a third party's reachability is
            // unauthenticated and must never (over)write that peer's route.
            updates_to_apply.push((name, location, owner_is_sender.then_some(resolved)));
        }

        // HashMap iteration order is intentionally unstable. Sort before
        // admission so every node retains the same names when a sender
        // advertises more actors than its configured allowance.
        updates_to_apply.sort_by(|left, right| left.0.cmp(&right.0));

        // STEP 2: Apply known_actors upserts, peer_to_actors update,
        // and stale-actor removal under a SINGLE gossip_state lock so
        // the "every name in peer_to_actors[sender] is in known_actors"
        // invariant survives a concurrent `cleanup_dead_peers` /
        // `apply_delta` / `handle_peer_death` pass. This mirrors the
        // plan-then-execute fix on `apply_delta`. See test
        // `test_apply_delta_and_cleanup_dead_peers_preserve_invariant`.
        let mut routes_to_configure: Vec<(String, crate::PeerId, SocketAddr)> = Vec::new();
        crate::lifecycle::record_transport_event(
            crate::lifecycle::TransportLifecycleEvent::FullSyncApplyPendingMutation {
                peer: sender_peer_id.clone(),
                addr: sender_addr,
            },
        );
        {
            let mut gossip_state = self.gossip_state.lock().await;

            // Atomic generation recheck, immediately before any write in
            // this block: if a newer session has been armed (or the
            // validated one has self-expired) since STEP 1 captured
            // `captured_epoch`, this pending apply is stale and must
            // be dropped rather than overwrite the newer session's state
            // with this connection's (possibly pre-restart) snapshot.
            if let Some(generation) = captured_epoch
                && !session_epoch_still_current(&gossip_state, sender_addr, generation)
            {
                debug!(
                    peer = %sender_addr,
                    "dropping full-sync actor apply; a newer session was armed \
                     after this message's sequence validation"
                );
                // Was current at STEP 1 (that is what got this far); the
                // narrower epoch race is about this specific pending
                // write, not session authority, so still `true`.
                return true;
            }

            let mut rejected_by_peer_cap = 0usize;
            let mut rejected_by_global_cap = 0usize;

            for (name, location, addr) in &updates_to_apply {
                let known_exists = self.actor_state.known_actors.contains_sync(name.as_str());
                let transfers_admission =
                    gossip_state.actor_admission_peer_by_name.get(name.as_str())
                        != Some(&sender_peer_id);
                if transfers_admission
                    && gossip_state.actor_admission_count(&sender_peer_id)
                        >= self.config.max_known_actors_per_peer
                {
                    rejected_by_peer_cap += 1;
                    continue;
                }
                if !known_exists
                    // `known_actors` is remote-only; local registrations are
                    // stored separately and never consume this budget.
                    && self.actor_state.known_actors.len() >= self.config.max_known_actors
                {
                    rejected_by_global_cap += 1;
                    continue;
                }
                let upsert_plan =
                    self.current_actor_upsert_plan(name.as_str(), location, &sender_peer_id);
                if upsert_plan.is_none() {
                    // An exact duplicate is still an admitted advertisement
                    // when the actor already exists. Other rejected candidates
                    // must not create phantom peer_to_actors entries.
                    if known_exists {
                        peer_actors.insert(name.clone());
                    }
                    continue;
                }
                peer_actors.insert(name.clone());
                let (clear_tombstone, is_update) = upsert_plan.expect("upsert plan checked above");
                if clear_tombstone {
                    let _ = self.actor_state.removed_actors.remove_sync(name.as_str());
                }
                let _ = self
                    .actor_state
                    .known_actors
                    .upsert_sync(name.clone(), location.clone());
                gossip_state.record_actor_admission(&sender_peer_id, name);
                if is_update {
                    updated_actors += 1;
                } else {
                    new_actors += 1;
                }
                // Storage above is unconditional (§1.5); dial-hint learning
                // is gated so port-0/unusable addresses (e.g. a relayed
                // wildcard kept verbatim) never poison the dial tables.
                if let Some(addr) = addr
                    && learnable_dial_route(*addr, repair_addr)
                {
                    routes_to_configure.push((name.clone(), location.peer_id.clone(), *addr));
                }
            }

            if rejected_by_peer_cap > 0 || rejected_by_global_cap > 0 {
                warn!(
                    sender = %sender_peer_id,
                    rejected_by_peer_cap,
                    rejected_by_global_cap,
                    max_known_actors = self.config.max_known_actors,
                    max_known_actors_per_peer = self.config.max_known_actors_per_peer,
                    "rejected full-sync actor registrations at configured admission caps"
                );
            }

            let removed_now: Vec<String> = match gossip_state
                .peer_to_actors
                .insert(sender_addr, peer_actors.clone())
            {
                Some(previous) => previous
                    .difference(&peer_actors)
                    .cloned()
                    .collect::<Vec<_>>(),
                None => Vec::new(),
            };

            // Prune stale known_actors entries the peer no longer
            // advertises — but only if the actor is still attributed
            // to the sender (otherwise a different owner has taken it
            // and we'd be racing them). All under the same lock so a
            // concurrent apply_delta cannot squeeze between the
            // attribution check and the remove.
            for actor_name in &removed_now {
                if self
                    .actor_state
                    .local_actors
                    .contains_sync(actor_name.as_str())
                {
                    continue;
                }
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
                    gossip_state.release_actor_admission(actor_name);
                    info!(
                        actor_name = %actor_name,
                        peer = %sender_addr,
                        "removed stale actor after peer full sync omitted it"
                    );
                }
            }
            // _gossip_state guard drops here.
            let _ = gossip_state;
        }

        // STEP 3: Record learned direct routes outside the lock (these
        // invoke user handlers and may not be held under gossip_state).
        for (name, peer_id, addr) in routes_to_configure {
            self.connection_pool
                .set_discovered_peer_addr(&peer_id, addr);
            let _ = self
                .connection_pool
                .addr_to_peer_id
                .upsert_sync(addr, peer_id.clone());
            debug!(
                actor = %name,
                peer_addr = %addr,
                "Recorded learned direct route for actor's host"
            );
        }

        debug!(
            new_actors = new_actors,
            updated_actors = updated_actors,
            peer = %sender_addr,
            peer_actor_count = peer_actors.len(),
            "merged gossip data using vector clock conflict resolution"
        );
        true
    }

    /// Clean up stale actor entries (using wall clock for TTL)
    /// R-1: whether the owning peer of `location` currently has a live
    /// (Connected) connection. Used to exempt connected owners from actor-TTL
    /// reaping and the lookup age gate, so TTL only governs actors of
    /// unreachable peers (its actual purpose). Best-effort: parses the owner's
    /// advertised address and asks the pool for a live connection.
    fn owner_peer_is_connected(&self, location: &RemoteActorLocation) -> bool {
        location
            .address
            .parse::<SocketAddr>()
            .ok()
            .and_then(|addr| self.connection_pool.get_existing_connection(addr))
            .is_some()
    }

    pub async fn cleanup_stale_actors(&self) {
        let now = current_timestamp();
        let ttl_secs = self.config.actor_ttl.as_secs();

        // Clean up stale known actors (using wall clock time for TTL)
        {
            let before_count = self.actor_state.known_actors.len();

            let mut to_remove = Vec::new();
            self.actor_state.known_actors.iter_sync(|k, location| {
                if now.saturating_sub(location.wall_clock_time) >= ttl_secs
                    // R-1: do not TTL-reap an actor whose owning peer is
                    // currently connected -- TTL then only governs actors of
                    // unreachable peers.
                    && !self.owner_peer_is_connected(location)
                {
                    to_remove.push(k.clone());
                }
                true
            });

            let mut gossip_state = self.gossip_state.lock().await;
            for name in &to_remove {
                if self
                    .actor_state
                    .known_actors
                    .remove_sync(name.as_str())
                    .is_some()
                {
                    gossip_state.release_actor_admission(name);
                }
            }

            let removed = before_count.saturating_sub(self.actor_state.known_actors.len());
            if removed > 0 {
                info!(removed_count = removed, "cleaned up stale actor entries");
            }
        }

        // Bound peer-death/unregister tombstones. They only need to outlive
        // ordinary gossip/vector-clock retention; after that, old actors may be
        // re-created normally without carrying unbounded historical removals.
        {
            let before_count = self.actor_state.removed_actors.len();
            let tombstone_ttl_secs = self.config.vector_clock_retention_period.as_secs();
            let mut to_remove = Vec::new();

            self.actor_state
                .removed_actors
                .iter_sync(|name, tombstone| {
                    if now.saturating_sub(tombstone.removed_at) >= tombstone_ttl_secs {
                        to_remove.push(name.clone());
                    }
                    true
                });

            for name in &to_remove {
                let _ = self.actor_state.removed_actors.remove_sync(name.as_str());
            }

            let removed = before_count.saturating_sub(self.actor_state.removed_actors.len());
            if removed > 0 {
                info!(
                    removed_count = removed,
                    "cleaned up expired actor tombstones"
                );
            }
        }

        // Clean up old delta history. Retention is a pure elapsed-time check
        // against a value that never leaves this process, so it uses the
        // monotonic clock rather than wall-clock arithmetic: a backward or
        // forward wall-clock step can neither panic this nor purge (or
        // preserve) the entire history in one step.
        {
            let mut gossip_state = self.gossip_state.lock().await;
            let history_ttl = self.config.actor_ttl.saturating_mul(2);
            let now_instant = Instant::now();
            gossip_state.delta_history.retain(|delta| {
                now_instant.saturating_duration_since(delta.recorded_at) < history_ttl
            });
        }

        // Enforce bounds on data structures
        self.enforce_bounds().await;

        self.prune_peer_identity_side_tables();

        // Clean up connection pool
        {
            let connection_pool = &self.connection_pool;
            connection_pool.cleanup_stale_connections();
        }
    }

    /// Side tables keyed by ephemeral `PeerId`s cannot rely on address-based
    /// gossip cleanup. Retain only the short windows needed for tie-break
    /// damping and supervisor edge detection; configured peers remain pinned.
    fn prune_peer_identity_side_tables(&self) {
        let now = Instant::now();
        let cooldown = self.config.tie_break_reconnect_cooldown;
        self.tie_break_cooldown_until
            .retain_sync(|_, deadline| *deadline > now);
        self.tie_break_last_eviction_at
            .retain_sync(|_, last| now.saturating_duration_since(*last) <= cooldown);

        let liveness_ttl = self.config.peer_liveness_window.saturating_mul(3);
        self.peer_liveness_status.retain_sync(|peer_id, status| {
            self.connection_pool.is_required_peer(peer_id)
                || now.saturating_duration_since(status.updated_at) <= liveness_ttl
        });
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
                            current_time.saturating_sub(failure_time) > dead_peer_timeout_secs
                        })
                })
                .map(|(addr, _)| *addr)
                .collect()
        };

        let mut should_trigger_immediate = false;

        if !peers_to_cleanup.is_empty() {
            // IMPORTANT: Always acquire locks in consistent order to prevent deadlocks
            // Order: actor_state before gossip_state
            let mut gossip_state = self.gossip_state.lock().await;

            for peer_addr in &peers_to_cleanup {
                // IMPORTANT: We do NOT remove the peer itself - it stays in the peer list
                // This allows us to reconnect when the peer comes back online

                // Remove peer's actors from known_actors to free memory. Re-check
                // current ownership first because peer_to_actors is a side table and
                // may still contain a stale entry after the actor moved to another peer.
                if let Some(actor_names) = gossip_state.peer_to_actors.get(peer_addr).cloned() {
                    let peer_info = gossip_state.peers.get(peer_addr).cloned();
                    let mut actors_removed = 0usize;
                    for actor_name in &actor_names {
                        // Capture the current location (not just a bool) so a
                        // confirmed removal can be tombstoned and gossiped with
                        // the same causal-removal shape every other removal
                        // path uses (unregister_actor / apply_delta_from).
                        let removal_location = self
                            .actor_state
                            .known_actors
                            .read_sync(actor_name.as_str(), |_, location| {
                                actor_location_belongs_to_peer(
                                    location,
                                    *peer_addr,
                                    peer_info.as_ref(),
                                )
                                .then(|| location.clone())
                            })
                            .flatten();

                        if let Some(location) = removal_location {
                            if self
                                .actor_state
                                .known_actors
                                .remove_sync(actor_name.as_str())
                                .is_some()
                            {
                                gossip_state.release_actor_admission(actor_name);
                                actors_removed += 1;

                                // Record a peer-death tombstone causally after the
                                // reaped location, and propagate the removal via
                                // gossip — otherwise a peer holding a stale cached
                                // copy of this actor can re-admit it (see R2:
                                // `current_actor_upsert_plan` only rejects an
                                // incoming ActorAdded when a dominating tombstone
                                // exists or a causally-newer entry is present, and
                                // without this the fast dead-peer reap silently
                                // degrades to the 24h actor_ttl backstop).
                                let removal_clock = location.vector_clock.clone();
                                removal_clock.increment(self.peer_id.to_node_id());
                                let _ = self.actor_state.removed_actors.upsert_sync(
                                    actor_name.clone(),
                                    RemovedActorTombstone::new(removal_clock.clone()),
                                );

                                let change = RegistryChange::ActorRemoved {
                                    name: actor_name.clone(),
                                    vector_clock: removal_clock,
                                    removing_node_id: self.peer_id.to_node_id(),
                                    priority: location.priority,
                                };
                                if location.priority.should_trigger_immediate_gossip() {
                                    gossip_state.urgent_changes.push(change.clone());
                                    gossip_state
                                        .pending_changes
                                        .push(Self::as_regular_gossip_change(&change));
                                    should_trigger_immediate = true;
                                } else {
                                    gossip_state.pending_changes.push(change);
                                }
                            }
                        }
                    }
                    gossip_state.peer_to_actors.remove(peer_addr);

                    info!(
                        peer = %peer_addr,
                        actors_removed,
                        stale_side_table_entries = actor_names.len().saturating_sub(actors_removed),
                        timeout_minutes = dead_peer_timeout_secs / 60,
                        "cleaned up actors from long-disconnected peer (peer retained for reconnection)"
                    );
                }

                // Clean up health reports but keep the peer entry.
                // Strip this peer both as subject (outer key) and as
                // reporter (inner key in every other peer's report
                // map) — otherwise inner entries leak across peer
                // churn.
                gossip_state.peer_health_reports.remove(peer_addr);
                for inner in gossip_state.peer_health_reports.values_mut() {
                    inner.remove(peer_addr);
                }
                gossip_state.pending_peer_failures.remove(peer_addr);
            }
            drop(gossip_state);

            // Drop the gossip_state lock before touching out-of-band
            // tables that have their own locks.
            for peer_addr in &peers_to_cleanup {
                self.clear_peer_capabilities(peer_addr);
                self.remove_clock_state_for_addr(peer_addr);
            }

            // Trigger immediate gossip (outside the gossip_state lock —
            // trigger_immediate_gossip re-acquires it) if any reaped actor
            // had a priority requiring urgent propagation.
            if should_trigger_immediate {
                if let Err(err) = self.trigger_immediate_gossip().await {
                    warn!(error = %err, "failed to trigger immediate gossip for dead-peer actor removal");
                }
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
                        let time_since_failure = current_time.saturating_sub(failure_time);
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

        // The atomic is the canonical source — set it first so any new
        // caller (including spawned tasks not holding the gossip_state
        // lock) sees shutdown immediately. The mutex bool is kept in
        // sync for code that already holds the lock and would prefer
        // not to re-read the atomic separately. If a future shutdown is
        // interrupted between these two writes, observers still
        // converge on "shutting down" via the atomic.
        self.shutdown.store(true, Ordering::Release);
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.shutdown = true;
        }

        // Break callback -> client/router -> registry ownership cycles before
        // connection teardown can emit any terminal disconnect notifications.
        self.clear_runtime_handlers();

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
        // Read the atomic instead of acquiring the gossip_state lock —
        // this is the canonical source of truth and lets the
        // timer/server loops short-circuit without contending for the
        // big lock.
        self.shutdown.load(Ordering::Acquire)
    }

    /// Record that a peer (by address) is active. Refreshes liveness
    /// timestamps; a peer that has not yet been declared dead gets its
    /// `failures` counter reset to zero so a transient blip doesn't
    /// accumulate. Once `failures` has reached `max_peer_failures` the
    /// peer is considered dead — resurrection requires an actual new
    /// connection (`mark_peer_connected`), not just one gossip
    /// roundtrip, because a dying peer can still emit a few last
    /// responses before the socket fully tears down and we don't want
    /// those late acks to undo a hard-socket-error verdict.
    ///
    /// Previously this body was a no-op despite callers (and the
    /// surrounding comments) assuming it reset failures. That left
    /// recovered peers stuck at `failures = max` — the "sticky
    /// failures" symptom.
    pub async fn record_peer_activity(&self, peer_addr: SocketAddr) {
        let now = current_timestamp();
        let max_failures = self.config.max_peer_failures;
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
                peer_info.last_success = peer_info.last_success.max(now);
                if peer_info.failures < max_failures {
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                }
            }
            if let Some(peer_info) = gossip_state.known_peers.get_mut(&peer_addr) {
                if peer_info.failures < max_failures {
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                }
            }
        }
    }

    /// Handle peer connection failure - start consensus process
    /// This is called for socket disconnections (not timeouts)
    ///
    /// `failed_instance_id` is the `LockFreeStreamHandle::instance_id()` of
    /// the SPECIFIC connection instance whose IO task exited, when the
    /// caller can supply it (the persistent-connection writer's exit guard
    /// always can — it holds the instance id of the handle it was created
    /// for). It must never be re-derived by re-resolving `observed_peer_addr`
    /// through the address index: once a fresh connection has been reindexed
    /// under the same bind address as an older, now-superseded link, that
    /// relookup silently returns the NEW instance rather than the one that
    /// actually failed. `None` means the caller cannot identify the specific
    /// failed instance and this falls back to the conservative "may be the
    /// current session" path.
    pub async fn handle_peer_connection_failure(
        &self,
        observed_peer_addr: SocketAddr,
        failed_instance_id: Option<u64>,
    ) -> Result<()> {
        let (failed_peer_addr, peer_id) = self
            .resolve_failed_peer_state_addr(observed_peer_addr)
            .await;
        info!(
            failed_peer = %failed_peer_addr,
            observed_peer = %observed_peer_addr,
            "socket disconnection detected, marking connection as failed (actors remain available)"
        );

        // Captured BEFORE any teardown mutation below, as close as possible
        // to the moment the failure was reported. Re-checked, under the
        // gossip_state lock, immediately before the discovery clear near the
        // end of this function: a replacement connection that calls
        // `mark_peer_connected` for this address anywhere in between bumps
        // this value, and the mismatch tells the clear it must not touch a
        // state the replacement now legitimately owns.
        let pre_failure_discovery_generation = self
            .capture_pre_failure_discovery_generation(failed_peer_addr)
            .await;

        // Address-vs-identity guard. The failure is reported for one specific
        // socket (`observed_peer_addr`). If the peer's current published session
        // is a DIFFERENT connection instance than the one that failed, the
        // failed connection is superseded — e.g. an old link dying shortly
        // after the peer already reconnected from a new address. Tearing down
        // the whole peer session here (`disconnect_connection_by_peer_id` is
        // peer-wide) would collaterally drop the healthy current connection,
        // which is exactly the single-node-restart reconnect thrash: a
        // freshly-accepted preferred inbound `disconnect_by_peer_id`'d moments
        // after acceptance. Retire only the stale link and leave the live
        // session — identified purely by connection-instance identity — intact.
        // Whether the block below already retired the specific failed
        // instance by CAS'd identity (`disconnect_connection_instance`). When
        // true, the peer-wide cleanup block further down must NOT also run
        // `disconnect_connection_by_peer_id` for this peer: that would
        // re-open the exact check/act gap this handler exists to close — a
        // fresh session published for the same peer between the instance-id
        // match above and a peer-wide sweep would be collaterally destroyed
        // instead of only the failed instance.
        let mut instance_teardown_done = false;

        if let Some(peer_id) = peer_id.as_ref() {
            let pool = &self.connection_pool;
            if let Some(current) = pool.get_connection_by_peer_id(peer_id) {
                // Compare INSTANCE IDENTITY directly against the current
                // session's own stream handle. This deliberately does not
                // re-resolve `observed_peer_addr` through
                // `get_lock_free_connection`: once a fresh connection has
                // been reindexed under the same bind address the failed
                // link used, that address now resolves to the NEW instance,
                // and comparing it to `current` (also the new instance)
                // would trivially and incorrectly conclude "the current
                // session is failing". Only a caller-supplied instance id
                // that provably differs from the current session's own
                // proves supersession; when the caller cannot identify the
                // failed instance at all, never treat that absence as
                // evidence of supersession — fall through to the normal
                // failure path, which is always safe for the failing
                // session itself.
                let current_instance_id = current.stream_handle.as_ref().map(|h| h.instance_id());
                match failed_instance_id {
                    Some(failed_id) if Some(failed_id) != current_instance_id => {
                        let retired =
                            pool.remove_connection_instance_by_id(observed_peer_addr, failed_id);
                        // `remove_connection_instance_by_id` only decrements
                        // `connection_counter` when it actually finds and
                        // removes the failed instance at `observed_peer_addr`.
                        // In the same-bind-address restart case, a fresh
                        // inbound has already overwritten that address slot
                        // by the time this runs, so the lookup finds nothing
                        // and returns `None`. `release_displaced_connection_count`
                        // routes through the shared per-instance ownership
                        // table: if `failed_id`'s count is still outstanding
                        // (the common case — displaced from the index but
                        // never actually retired by anything else), this is
                        // the release. If some OTHER teardown path already
                        // retired this exact instance concurrently (e.g. the
                        // IO task's own `ExitGuard` superseded-exit fallback
                        // in `stream_writer.rs` raced this call), the table
                        // already shows it released and this is a safe
                        // no-op — never a second decrement.
                        if retired.is_none() {
                            pool.release_displaced_connection_count(failed_id);
                        }
                        info!(
                            peer_id = %peer_id,
                            observed_peer = %observed_peer_addr,
                            current_addr = %current.addr,
                            retired_instance = retired.is_some(),
                            "socket failure for a superseded connection; retiring only the stale \
                             link and preserving the live identity-verified session"
                        );
                        return Ok(());
                    }
                    Some(failed_id) => {
                        // The failed instance IS the current session. Retire
                        // it here by CAS'd instance identity
                        // (`disconnect_connection_instance`) rather than
                        // falling through to the peer-wide
                        // `disconnect_connection_by_peer_id` below: that call
                        // clears the peer's current-connection slot and
                        // sweeps every address alias mapped to `peer_id`
                        // unconditionally, so a fresh session published for
                        // this peer between this comparison and that sweep
                        // would be torn down along with the failed instance
                        // instead of surviving it. `disconnect_connection_instance`
                        // performs a single atomic compare-and-clear against
                        // `current` and declines (a safe no-op) if a
                        // concurrent publish has already superseded it.
                        crate::lifecycle::record_transport_event(
                            crate::lifecycle::TransportLifecycleEvent::SocketFailureMatchedInstanceTeardownAttempt {
                                peer: peer_id.clone(),
                                addr: current.addr,
                            },
                        );
                        let retired = pool.disconnect_connection_instance(peer_id, &current);
                        if !retired {
                            // The CAS lost: a FRESH session was published for
                            // this peer before it ran, so `current` (the
                            // FAILED instance) was NOT removed from
                            // `peer_sessions`/`connections_by_peer` — nor
                            // should it be, those now correctly point at the
                            // fresh winner. But `current` itself is still
                            // outstanding: its address aliases and
                            // `connection_counter` contribution must still
                            // be released, by its own identity, without
                            // touching the fresh winner. See
                            // `retire_lost_cas_matched_instance`.
                            pool.retire_lost_cas_matched_instance(&current, failed_id);
                            info!(
                                peer_id = %peer_id,
                                observed_peer = %observed_peer_addr,
                                current_addr = %current.addr,
                                "socket failure raced a fresh publish for this peer; the fresh \
                                 session survives as current, and only the superseded failed \
                                 instance's own identity/counter are retired — no peer-wide \
                                 failure accounting, notification, or consensus fires for what \
                                 is, from the peer's perspective, a perfectly live session"
                            );
                            // This is the SAME shape as the already-superseded
                            // branch above (`Some(failed_id) if Some(failed_id)
                            // != current_instance_id`): the failed instance
                            // never was, or no longer is, the peer's actual
                            // live session, so this must return here, exactly
                            // like that branch does, rather than falling
                            // through to `instance_teardown_done`'s tail
                            // below. That tail only skips the redundant
                            // peer-wide POOL SWEEP
                            // (`disconnect_connection_by_peer_id`); it does
                            // NOT skip the peer-wide FAILURE ACCOUNTING further
                            // down (marking `failures = max_peer_failures`,
                            // invoking the peer-disconnect handler, and
                            // driving actor-invalidation consensus) — falling
                            // through into that unconditionally would make the
                            // currently-connected, healthy peer look dead.
                            // Only a genuine current-session teardown (the
                            // `retired == true` case below) may reach that
                            // accounting; a CAS loss must be structurally
                            // unable to reach it, not merely discouraged by a
                            // flag some later block remembers to check.
                            return Ok(());
                        }
                        info!(
                            peer_id = %peer_id,
                            observed_peer = %observed_peer_addr,
                            current_addr = %current.addr,
                            retired,
                            "socket failure matched the current session's connection instance; \
                             retiring by CAS'd instance identity, not peer-wide, so a \
                             concurrently published fresh session survives"
                        );
                        // The CAS retired `current` directly: this genuinely
                        // was the peer's live session and it has genuinely
                        // just been torn down, so — unlike the CAS-loss case
                        // above — the peer-wide failure accounting further
                        // down IS applicable and must still run. Only the
                        // redundant peer-wide POOL SWEEP
                        // (`disconnect_connection_by_peer_id`) is skipped,
                        // since `disconnect_connection_instance` already
                        // performed the equivalent teardown by CAS'd identity.
                        instance_teardown_done = true;
                    }
                    None => {
                        // Caller cannot identify the failed instance at all;
                        // fall through to the legacy peer-wide path below,
                        // unchanged.
                    }
                }
            }
        }

        let current_time = current_timestamp();

        // IMMEDIATELY mark the connection as failed and remove from pool.
        // Use disconnect_connection_by_peer_id when possible to clean up ALL
        // address aliases — UNLESS the block above already retired the
        // specific failed instance by CAS'd identity, in which case running
        // this peer-wide sweep too would reopen the exact collateral-teardown
        // gap that instance-scoped retirement exists to close.
        if instance_teardown_done {
            info!(
                addr = %failed_peer_addr,
                peer_id = ?peer_id,
                "failed connection instance already retired by CAS'd identity; skipping the \
                 peer-wide pool sweep"
            );
        } else {
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
            } else if observed_peer_addr != failed_peer_addr
                && pool.has_connection(&observed_peer_addr)
            {
                pool.remove_connection(observed_peer_addr);
                info!(
                    observed_addr = %observed_peer_addr,
                    canonical_addr = %failed_peer_addr,
                    "removed disconnected connection from pool by observed address"
                );
            }
        }

        // Both branches above are reached only for a CONFIRMED teardown of
        // `failed_peer_addr`'s own session -- the superseded/CAS-loss cases
        // earlier in this function already returned without disturbing a
        // still-live connection. Clear peer discovery's `Connected` state
        // here so the slot is reclaimed: without this, a peer that ever
        // connected stays `Connected` in `peer_discovery` forever (nothing
        // else on the ordinary socket-failure/teardown path ever notifies
        // discovery of a real disconnect), `connected_count_unified` only
        // grows, and once it reaches `max_peers` discovery permanently
        // stops admitting new gossip candidates even with zero live
        // connections.
        //
        // This clear is gated on `pre_failure_discovery_generation`, captured
        // at entry, still matching this address's CURRENT discovery connect
        // generation: the pool-teardown work above ran between that capture
        // and this check with no `gossip_state` lock held, so a replacement
        // connection can have called `mark_peer_connected` for this same
        // address in the gap (a restart-into-live-peer racing this exact
        // failure report) -- with or without any session-epoch change of its
        // own. If it has, the replacement is now the address's legitimate
        // `Connected` owner -- clearing here would make
        // `connected_count_unified` undercount a still-live peer and could
        // let discovery admit connections beyond `max_peers`.
        self.clear_discovery_state_if_generation_unchanged(
            failed_peer_addr,
            pre_failure_discovery_generation,
        )
        .await;

        if let Some(cell) = self.peer_disconnect_handler.load_full() {
            // Skip launching the notifier if we're already shutting
            // down — the spawn would otherwise hold an Arc reference
            // and keep the handler alive past shutdown_and_wait.
            if !self.shutdown.load(Ordering::Acquire) {
                let handler = cell.handler.clone();
                let peer_id = peer_id.clone();
                let shutdown = self.shutdown.clone();
                tokio::spawn(async move {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    handler
                        .handle_peer_disconnect(failed_peer_addr, peer_id)
                        .await;
                });
            }
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

        // NOTE: the tie-break reconnect cooldown (`note_tie_break_eviction`)
        // is deliberately *not* armed here. This handler fires for every
        // observed socket failure, including perfectly ordinary ones (a
        // long-lived connection that finally died, a first-time dial that
        // failed for unrelated reasons) where a required peer must reconnect
        // as fast as possible — arming a cooldown unconditionally here once
        // regressed reconnect-latency-sensitive tests (a fresh peer
        // relationship needing its first connection within an ask's retry
        // budget). The cooldown is armed only at the specific
        // duplicate-connection tie-break call sites
        // (`outbound_tiebreak_evict_wrong_direction` in transport_stream.rs,
        // `inbound_tiebreak_replace_wrong_direction` /
        // `inbound_tiebreak_reject_live_duplicate` /
        // `inbound_tiebreak_reject_non_preferred_inbound` in handle.rs) —
        // i.e. only when there is direct, local evidence of a duplicate/
        // wrong-direction connection conflict, not on every failure.

        if crossed_threshold {
            info!(
                failed_peer = %failed_peer_addr,
                "socket-close crossed failure threshold; retaining actors until consensus/timeout"
            );
            self.trigger_immediate_peer_gossip();
        }

        if !self.peer_health_consensus_enabled() {
            info!(
                failed_peer = %failed_peer_addr,
                "peer-health consensus disabled; retaining transport failure state for retry and TTL cleanup"
            );
            return Ok(());
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

    async fn resolve_failed_peer_state_addr(
        &self,
        observed_peer_addr: SocketAddr,
    ) -> (SocketAddr, Option<crate::PeerId>) {
        let pool = &self.connection_pool;
        let mut peer_id = pool.get_peer_id_by_addr(&observed_peer_addr);

        let state_match = {
            let gossip_state = self.gossip_state.lock().await;

            if let Some(info) = gossip_state.peers.get(&observed_peer_addr) {
                Some((
                    observed_peer_addr,
                    info.node_id.as_ref().map(|node_id| node_id.to_peer_id()),
                ))
            } else {
                gossip_state.peers.iter().find_map(|(addr, info)| {
                    if info.peer_address == Some(observed_peer_addr) {
                        Some((
                            *addr,
                            info.node_id.as_ref().map(|node_id| node_id.to_peer_id()),
                        ))
                    } else {
                        None
                    }
                })
            }
        };

        if peer_id.is_none() {
            peer_id = state_match
                .as_ref()
                .and_then(|(_, peer_id)| peer_id.clone());
        }

        if let Some(peer_id) = peer_id.as_ref() {
            if let Some(configured_addr) = pool.get_configured_peer_addr(peer_id) {
                return (configured_addr, Some(peer_id.clone()));
            }
            if let Some(advertised_addr) = self.lookup_advertised_addr(&peer_id.to_node_id()).await
            {
                return (advertised_addr, Some(peer_id.clone()));
            }
        }

        if let Some((addr, state_peer_id)) = state_match {
            return (addr, peer_id.or(state_peer_id));
        }

        (observed_peer_addr, peer_id)
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

        // Captured BEFORE any teardown mutation below, mirroring
        // `handle_peer_connection_failure`'s address-keyed path: re-checked
        // (via `clear_discovery_state_if_generation_unchanged`) immediately
        // before the discovery clear further down, so a replacement
        // connection that calls `mark_peer_connected` for this address
        // anywhere in between is detected and the clear declines rather
        // than wiping out a state it no longer owns.
        let pre_failure_discovery_generation = self
            .capture_pre_failure_discovery_generation(failed_peer_addr)
            .await;

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

        // Same guarded discovery clear as `handle_peer_connection_failure`'s
        // address-keyed path -- see the comment there for the full
        // rationale. Both paths tear down a peer's pool connection and mark
        // it failed; both must reclaim the peer-discovery slot the same way.
        self.clear_discovery_state_if_generation_unchanged(
            failed_peer_addr,
            pre_failure_discovery_generation,
        )
        .await;

        if let Some(cell) = self.peer_disconnect_handler.load_full() {
            // Skip launching the notifier if we're already shutting
            // down — see the matching branch in
            // `handle_peer_connection_failure`.
            if !self.shutdown.load(Ordering::Acquire) {
                let handler = cell.handler.clone();
                let peer_id = Some(failed_peer_id.clone());
                let shutdown = self.shutdown.clone();
                tokio::spawn(async move {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    handler
                        .handle_peer_disconnect(failed_peer_addr, peer_id)
                        .await;
                });
            }
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

        // See the matching NOTE in `handle_peer_connection_failure`: the
        // tie-break reconnect cooldown is intentionally not armed on generic
        // socket-failure detection — only at the specific duplicate-
        // connection tie-break call sites.

        if crossed_threshold {
            info!(
                failed_peer = %failed_peer_addr,
                node_id = %failed_peer_id,
                "socket-close crossed failure threshold; retaining actors until consensus/timeout"
            );
        }

        if !self.peer_health_consensus_enabled() {
            info!(
                failed_peer = %failed_peer_addr,
                node_id = %failed_peer_id,
                "peer-health consensus disabled; retaining transport failure state for retry and TTL cleanup"
            );
            return Ok(());
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
        if !self.peer_health_consensus_enabled() {
            debug!(
                target_peer = %target_peer,
                "peer-health consensus disabled; skipping peer-health query"
            );
            return Ok(());
        }

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
        if !self.peer_health_consensus_enabled() {
            return;
        }

        let current_time = current_timestamp();

        {
            let mut gossip_state = self.gossip_state.lock().await;
            let mut completed_failures = Vec::new();
            let mut active_connection_recoveries = Vec::new();

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

                            let has_active_connection = gossip_state
                                .peers
                                .get(peer_addr)
                                .is_some_and(|peer| self.peer_has_live_connection(peer));

                            if has_active_connection {
                                active_connection_recoveries.push((
                                    *peer_addr,
                                    dead_count,
                                    alive_count,
                                ));
                                info!(
                                    peer = %peer_addr,
                                    dead_votes = dead_count,
                                    alive_votes = alive_count,
                                    "consensus: local active connection wins, clearing stale pending failure"
                                );
                                continue;
                            }

                            if dead_count > alive_count {
                                // Majority says dead
                                info!(
                                    peer = %peer_addr,
                                    dead_votes = dead_count,
                                    alive_votes = alive_count,
                                    "consensus: majority says peer is dead, but keeping actors for reconnection"
                                );
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
                                info!(
                                    peer = %peer_addr,
                                    "consensus timeout or tie, keeping actors"
                                );
                            }
                        }
                    }
                }
            }

            let now_ms = crate::current_timestamp_millis();
            for (peer_addr, _, _) in &active_connection_recoveries {
                if let Some(peer_info) = gossip_state.peers.get_mut(peer_addr) {
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                    peer_info.last_success = peer_info.last_success.max(current_time);
                    peer_info.last_response_received_ms =
                        peer_info.last_response_received_ms.max(now_ms);
                }
                if let Some(peer_info) = gossip_state.known_peers.get_mut(peer_addr) {
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                    peer_info.last_success = peer_info.last_success.max(current_time);
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
    /// R-12: is this peer entry exempt from `enforce_bounds` LRU eviction?
    ///
    /// Exempt when the peer is genuinely in use:
    /// - it has a live pooled connection, under any of its known addresses; or
    /// - it is an operator-configured / required peer.
    ///
    /// A peer can be keyed in `peers` under an address that differs from the
    /// one the connection is pooled under (inbound-only peers key on the
    /// ephemeral TCP source, NAT'd peers on `peer_address`, DNS-refreshed peers
    /// on a re-resolved `address`), and the pool may index the live connection
    /// by peer ID under none of those addresses at all. Liveness therefore
    /// defers to `peer_has_live_connection`, which already handles the
    /// address-alias and peer-ID-index cases; checking addresses alone would
    /// treat a peer with a live peer-ID-indexed connection as evictable.
    ///
    /// The map key is checked separately for liveness, since
    /// `peer_has_live_connection` only sees the `PeerInfo` fields.
    ///
    /// SECURITY: the configured-peer check deliberately does NOT consult
    /// `peer.address` / `peer.peer_address`. Those are peer-influenced (see
    /// B-5), so trusting them would let an untrusted inbound peer advertise a
    /// configured address to make itself eviction-exempt — and repeat that
    /// across many entries to bypass `max_peers` entirely and drive unbounded
    /// memory growth. Configuration is matched on trusted identity only: the
    /// TLS-authenticated `node_id`, or the `peers` map key, which is our own
    /// bookkeeping key rather than a field the peer fills in.
    ///
    /// Liveness is safe to check via aliases because it consults the actual
    /// connection pool — a peer cannot fabricate a live pooled connection.
    fn peer_is_eviction_exempt(
        &self,
        addr: &SocketAddr,
        peer: &PeerInfo,
        configured_addrs: &std::collections::HashSet<SocketAddr>,
        configured_peer_ids: &std::collections::HashSet<crate::PeerId>,
    ) -> bool {
        if self.peer_has_live_connection(peer) {
            return true;
        }

        // Trusted-identity configuration match.
        if let Some(node_id) = peer.node_id
            && configured_peer_ids.contains(&node_id.to_peer_id())
        {
            return true;
        }
        if configured_addrs.contains(addr) {
            return true;
        }

        // Alias sweep for LIVENESS only (pool-backed, not peer-claimed).
        let candidates = [Some(*addr), peer.peer_address, Some(peer.address)];
        candidates.iter().flatten().any(|candidate| {
            self.connection_pool
                .get_existing_connection(*candidate)
                .is_some()
        })
    }

    /// R-12: choose up to `to_remove` peers to evict, oldest-contact-first,
    /// considering only peers the caller reports as evictable.
    ///
    /// Split out from `enforce_bounds` so the policy is testable without
    /// standing up real pooled connections. Returns fewer than `to_remove`
    /// addresses when the excess is all exempt -- the caller logs that case
    /// rather than forcing an eviction.
    fn select_peers_to_evict(
        peers: &HashMap<SocketAddr, PeerInfo>,
        to_remove: usize,
        is_exempt: impl Fn(&SocketAddr, &PeerInfo) -> bool,
    ) -> Vec<SocketAddr> {
        let mut evictable: Vec<(SocketAddr, u64)> = peers
            .iter()
            .filter(|(addr, peer)| !is_exempt(addr, peer))
            .map(|(addr, peer)| (*addr, peer.last_success))
            .collect();
        // Oldest successful contact first; address breaks ties so eviction is
        // deterministic rather than dependent on HashMap iteration order.
        evictable.sort_by_key(|(addr, last_success)| (*last_success, *addr));
        evictable
            .into_iter()
            .take(to_remove)
            .map(|(addr, _)| addr)
            .collect()
    }

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

        // Bound pending changes.
        //
        // R-12(b): drop the OLDEST entries, not the newest. `truncate` kept the
        // head and discarded the tail, which is exactly backwards: the tail is
        // the most recent changes. `ActorRemoved` is not carried by FullSync
        // (only live entries are), so a burst that overflowed this bound lost
        // removal propagation outright and the stale actor survived until TTL.
        let max_pending = 1000;
        if gossip_state.pending_changes.len() > max_pending {
            let overflow = gossip_state.pending_changes.len() - max_pending;
            warn!(
                dropped = overflow,
                retained = max_pending,
                "pending changes overflowed the bound; dropping the oldest entries"
            );
            gossip_state.pending_changes.drain(..overflow);
        }

        // Bound urgent changes (smaller limit since these are high priority).
        // Same oldest-first policy as above, for the same reason.
        let max_urgent = 100;
        if gossip_state.urgent_changes.len() > max_urgent {
            let overflow = gossip_state.urgent_changes.len() - max_urgent;
            warn!(
                dropped = overflow,
                retained = max_urgent,
                "urgent changes overflowed the bound; dropping the oldest entries"
            );
            gossip_state.urgent_changes.drain(..overflow);
        }

        // Bound delta history
        if gossip_state.delta_history.len() > self.config.max_delta_history {
            let excess = gossip_state.delta_history.len() - self.config.max_delta_history;
            debug!("Trimming delta history by {} entries", excess);
            gossip_state.delta_history.drain(0..excess);
        }

        // Bound peers list.
        //
        // R-12(a): honour `config.max_peers` (this was hardcoded to 1000, so
        // the operator's cap was silently ignored), and never evict a peer that
        // is still in use. Eviction is destructive well beyond the `peers` map
        // -- it drops `peer_to_actors` (breaking every future dead-peer reap
        // for that peer), fires `on_peer_disconnected`, and destroys `node_id`
        // and `last_sequence`. Doing that to a live peer is a fault, not a
        // trim.
        let max_peers = self.config.max_peers.max(1);
        let mut evicted_addrs: Vec<SocketAddr> = Vec::new();
        if gossip_state.peers.len() > max_peers {
            let configured = self.connection_pool.list_configured_peers();
            let configured_peer_ids: std::collections::HashSet<crate::PeerId> = configured
                .iter()
                .map(|(peer_id, _)| peer_id.clone())
                .collect();
            let configured_addrs: std::collections::HashSet<SocketAddr> =
                configured.into_iter().map(|(_, addr)| addr).collect();

            let to_remove = gossip_state.peers.len() - max_peers;
            evicted_addrs =
                Self::select_peers_to_evict(&gossip_state.peers, to_remove, |addr, peer| {
                    self.peer_is_eviction_exempt(
                        addr,
                        peer,
                        &configured_addrs,
                        &configured_peer_ids,
                    )
                });

            if evicted_addrs.len() < to_remove {
                warn!(
                    over_cap = to_remove,
                    evictable = evicted_addrs.len(),
                    max_peers,
                    "peer table is over its bound but the excess is all in-use \
                     (live/configured/alias-linked) peers; leaving them in place"
                );
            }
            for addr in &evicted_addrs {
                gossip_state.peers.remove(addr);
                gossip_state.peer_to_actors.remove(addr);
                gossip_state.pending_peer_failures.remove(addr);

                // peer_health_reports has two leak shapes: the outer
                // entry (this peer as subject) and inner entries keyed
                // by this peer as reporter. Clear both.
                gossip_state.peer_health_reports.remove(addr);
                for inner in gossip_state.peer_health_reports.values_mut() {
                    inner.remove(addr);
                }

                if let Some(ref mut discovery) = gossip_state.peer_discovery {
                    discovery.on_peer_disconnected(*addr);
                }
            }
            if !evicted_addrs.is_empty() {
                debug!(
                    removed_count = evicted_addrs.len(),
                    "Trimmed stale peers and associated data"
                );
            }
        }
        drop(gossip_state);

        // Per-peer side tables that live outside `gossip_state` must be
        // cleaned with their own locks. Doing this here keeps eviction
        // self-contained instead of relying on the deferred
        // `cleanup_dead_peers` pass.
        if !evicted_addrs.is_empty() {
            for addr in &evicted_addrs {
                self.clear_peer_capabilities(addr);
                self.remove_clock_state_for_addr(addr);
            }
        }
    }

    /// Choose peer addresses to fan an immediate-priority broadcast out to.
    ///
    /// Healthy entries (`failures < max_peer_failures`) are deduplicated by
    /// stable identity before the `urgent_gossip_fanout` cap is applied, so
    /// a physical peer that appears in `peers` under multiple `SocketAddr`
    /// keys (e.g. ephemeral TCP-source plus migrated bind address, dual-stack
    /// aliases) counts once. Peers whose `node_id` is not yet known are
    /// keyed by address — a pre-handshake peer can't be confused with any
    /// other peer record.
    fn select_immediate_gossip_peers(
        peers: &HashMap<SocketAddr, PeerInfo>,
        max_peer_failures: usize,
        fanout: usize,
    ) -> Vec<SocketAddr> {
        #[derive(Hash, Eq, PartialEq)]
        enum DispatchKey {
            Node(crate::GossipNodeId),
            Addr(SocketAddr),
        }

        let mut seen: std::collections::HashSet<DispatchKey> = std::collections::HashSet::new();
        let mut selected: Vec<SocketAddr> = Vec::new();
        for (addr, peer) in peers.iter() {
            if peer.failures >= max_peer_failures {
                continue;
            }
            let key = peer
                .node_id
                .map(DispatchKey::Node)
                .unwrap_or(DispatchKey::Addr(*addr));
            if seen.insert(key) {
                selected.push(*addr);
                if selected.len() >= fanout {
                    break;
                }
            }
        }
        selected
    }

    #[cfg(test)]
    pub(crate) async fn select_immediate_gossip_peers_for_test(&self) -> Vec<SocketAddr> {
        let state = self.gossip_state.lock().await;
        Self::select_immediate_gossip_peers(
            &state.peers,
            self.config.max_peer_failures,
            self.config.urgent_gossip_fanout,
        )
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

            // Select target peers, deduplicating by stable identity so a
            // single physical peer that appears under multiple SocketAddr
            // aliases (ephemeral TCP source still present alongside its
            // migrated bind address, dual-stack IPv4/IPv6, DNS-resolved
            // hostnames mapped to several addresses, etc.) receives one
            // delivery rather than `aliases × peers` deliveries.
            let peers = Self::select_immediate_gossip_peers(
                &gossip_state.peers,
                self.config.max_peer_failures,
                self.config.urgent_gossip_fanout,
            );

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
        let serialization_start = crate::current_timestamp_nanos();

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
                extensions: None,
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
                    extensions: None,
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

        let serialization_end = crate::current_timestamp_nanos();
        let serialization_duration_ms =
            serialization_end.saturating_sub(serialization_start) as f64 / 1_000_000.0;

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
                let send_start = crate::current_timestamp_nanos();

                for payload in payloads.iter() {
                    conn.send_gossip_payload(payload.clone()).await?;
                }

                let send_end = crate::current_timestamp_nanos();
                let send_time_ms = send_end.saturating_sub(send_start) as f64 / 1_000_000.0;

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

        if had_failure {
            warn!(
                "immediate gossip encountered send failures - scheduling retry via regular gossip"
            );
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.pending_changes.extend(
                urgent_changes_for_retry
                    .iter()
                    .map(Self::as_regular_gossip_change),
            );
        }

        Ok(())
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
        let self_addr = self.advertised_addr();
        let mut self_info = PeerInfo::local(self_addr);
        // Include our DNS name in gossip so peers can re-resolve us on reconnect
        self_info.dns_name = self.config.advertise_dns.clone();
        peers.push(self_info.to_gossip());

        // Collect active peers (sorted for deterministic wire output)
        let mut active: Vec<&PeerInfo> = gossip_state
            .peers
            .values()
            .filter(|p| p.failures < self.config.max_peer_failures)
            .filter(|p| {
                crate::net_security::is_safe_to_dial(
                    &p.address,
                    self.config.allow_private_discovery,
                    self.config.allow_loopback_discovery,
                    self.config.allow_link_local_discovery,
                )
            })
            .collect();
        active.sort_by_key(|p| p.address);
        for p in active {
            peers.push(p.to_gossip());
        }

        // Include known peers from LRU cache (sorted, up to limit)
        let remaining = Self::MAX_PEER_LIST_SIZE.saturating_sub(peers.len());
        if remaining > 0 {
            let mut known: Vec<&PeerInfo> = gossip_state
                .known_peers
                .iter()
                .map(|(_, info)| info)
                .filter(|p| {
                    crate::net_security::is_safe_to_dial(
                        &p.address,
                        self.config.allow_private_discovery,
                        self.config.allow_loopback_discovery,
                        self.config.allow_link_local_discovery,
                    )
                })
                .collect();
            known.sort_by_key(|p| p.address);
            for p in known.into_iter().take(remaining) {
                peers.push(p.to_gossip());
            }
        }

        // Truncate to max size (sort already determines which entries survive)
        peers.truncate(Self::MAX_PEER_LIST_SIZE);

        peers
    }

    /// Periodic gossip of peer list to random subset of peers
    /// Returns gossip tasks for the caller to send.
    pub async fn gossip_peer_list(&self) -> Vec<GossipTask> {
        self.gossip_peer_list_inner(false).await
    }

    /// Immediate peer-list gossip after direct peer availability changes.
    pub async fn gossip_peer_list_immediate(&self) -> Vec<GossipTask> {
        self.gossip_peer_list_inner(true).await
    }

    async fn gossip_peer_list_inner(&self, force: bool) -> Vec<GossipTask> {
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
                if !force
                    && now
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
        let self_addr = self.advertised_addr();
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

        // Identity self-filter: a peer may relay gossip describing THIS
        // node under its *advertised* address (which can differ from
        // `bind_addr` under NAT/K8s/mesh — see `advertised_addr()`). The
        // address-keyed self-filter in `PeerDiscovery` only ever knew about
        // `bind_addr`, so a relayed entry naming our own advertised address
        // slipped through as a dial candidate and fed a self-dial livelock
        // (`should_keep_connection` is unconditionally `false` for self in
        // both directions, so `wait_for_preferred_connection` never
        // converges). Identity is the authoritative signal here — filter by
        // `node_id` first, independent of whatever address is attached.
        let self_node_id = self.peer_id.to_node_id();
        let peers: Vec<PeerInfoGossip> = peers
            .into_iter()
            .filter(|peer_gossip| {
                if peer_gossip.node_id == Some(self_node_id) {
                    debug!(
                        addr = %peer_gossip.address,
                        sender = %sender_addr,
                        "dropping relayed gossip describing this node's own identity (self node_id)"
                    );
                    return false;
                }
                true
            })
            .collect();

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

    /// Lookup advertised address for a GossipNodeId
    /// First checks active peers, then falls back to known_peers
    pub async fn lookup_advertised_addr(
        &self,
        node_id: &crate::GossipNodeId,
    ) -> Option<SocketAddr> {
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

    /// Lookup GossipNodeId for a given address (active peers first, then known_peers, then
    /// direct routing configuration).
    pub async fn lookup_node_id(&self, addr: &SocketAddr) -> Option<crate::GossipNodeId> {
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
        // gossip peers. Still derive the expected GossipNodeId from that pinned PeerId
        // so address-based TLS dials cannot fall back to placeholder SNI. Fall
        // back to the configured peer map so even the *first* dial to a
        // configured-but-not-yet-connected peer pins its GossipNodeId.
        self.connection_pool
            .get_peer_id_by_addr(addr)
            .or_else(|| self.connection_pool.configured_peer_id_for_addr(addr))
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
        node_id: &crate::GossipNodeId,
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
        let now = current_timestamp();
        {
            let mut gossip_state = self.gossip_state.lock().await;

            // A fresh connection was established and verified by the
            // caller (post-handshake / first framed message received).
            // Clear the death verdict unconditionally: softer evidence
            // paths (`record_peer_activity`, `mark_response_received`)
            // keep their `< max_peer_failures` gate so a stray response
            // cannot resurrect a peer, but a *real* reconnect must
            // recover one — otherwise a single death verdict welds the
            // peer out of the active set until the process restarts.
            if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                peer_info.failures = 0;
                peer_info.last_failure_time = None;
                peer_info.last_success = peer_info.last_success.max(now);
                peer_info.last_response_received_ms = peer_info
                    .last_response_received_ms
                    .max(crate::current_timestamp_millis());
            }

            // Update known_peers
            if let Some(peer_info) = gossip_state.known_peers.get_mut(&addr) {
                peer_info.failures = 0;
                peer_info.last_failure_time = None;
                peer_info.last_success = now;
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

            self.record_peer_discovery_connected(&mut gossip_state, addr);

            debug!(addr = %addr, "marked peer as connected");
        }

        // Notify peer connect handler (outgoing connections may only hit this path).
        if let Some(cell) = self.peer_connect_handler.load_full() {
            let peer_id = self.connection_pool.get_peer_id_by_addr(&addr);
            cell.handler.handle_peer_connect(addr, peer_id).await;
        }

        self.trigger_immediate_peer_gossip();
    }

    /// Record that this peer was observed on an inbound connection accepted by this node.
    pub async fn mark_inbound_connection_observed(
        &self,
        peer_addr: SocketAddr,
        source: SocketAddr,
    ) {
        let now = current_timestamp();
        let now_ms = crate::current_timestamp_millis();
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
            last_response_received_ms: now_ms,
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
        });
        peer.inbound_observed = true;
        if source != peer_addr {
            peer.peer_address = Some(source);
        }
        peer.last_success = peer.last_success.max(now);
        // Inbound payload is real liveness evidence — the framing layer
        // has decoded and dispatched at least one valid message on this
        // socket. Refresh the response-asymmetry timestamp and clear
        // the death verdict unconditionally (mirrors mark_peer_connected;
        // softer signals like record_peer_activity stay gated).
        peer.last_response_received_ms = peer.last_response_received_ms.max(now_ms);
        peer.failures = 0;
        peer.last_failure_time = None;
        self.record_peer_discovery_connected(&mut gossip_state, peer_addr);
        drop(gossip_state);
        self.trigger_immediate_peer_gossip();
    }

    /// The connection-instance token (see #156's `LockFreeStreamHandle::instance_id`)
    /// of whatever connection is CURRENTLY published in `connection_pool` for
    /// `addr`, if any. This is the identity `PeerDiscovery::on_peer_connected`
    /// uses to tell a genuine replacement connection at the same address
    /// (a different token) from a redundant re-mark of the same one (an
    /// identical token) -- reusing the exact per-connection instance
    /// identity #156 already established for the analogous
    /// `arm_sequence_reset_for_new_session`/`disconnect_connection_instance`
    /// mechanisms, rather than inventing a second one.
    fn current_connection_instance_token(&self, addr: SocketAddr) -> Option<u64> {
        self.connection_pool
            .get_lock_free_connection(addr)
            .and_then(|conn| conn.stream_handle.as_ref().map(|handle| handle.instance_id()))
    }

    fn record_peer_discovery_connected(&self, gossip_state: &mut GossipState, addr: SocketAddr) {
        let should_track_mesh_time =
            self.config.mesh_formation_target > 0 && gossip_state.mesh_formation_time_ms.is_none();
        let instance_token = self.current_connection_instance_token(addr);

        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            discovery.on_peer_connected(addr, instance_token);

            if should_track_mesh_time
                && discovery.connected_peer_count() >= self.config.mesh_formation_target
            {
                gossip_state.mesh_formation_time_ms =
                    Some(self.start_instant.elapsed().as_millis() as u64);
            }
        }
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

    /// Snapshot `addr`'s current peer-discovery connect generation (see
    /// `PeerDiscovery::connect_generation`), as early as possible, before a
    /// caller starts any teardown work for a reported connection failure.
    /// `None` means discovery does not currently show `addr` as `Connected`
    /// at all.
    ///
    /// This is deliberately NOT `PeerInfo::current_session_epoch`: that
    /// session-epoch mechanism only advances when a TLS session is actually
    /// (re)armed, but `mark_peer_connected` -- which is what actually flips
    /// discovery's `Connected` state -- can run WITHOUT any session-epoch
    /// change at all (e.g. a discovery-driven connect-on-demand success
    /// whose identity isn't yet known, so no session was ever armed), and
    /// can fire even when `gossip_state.peers` has no entry for `addr`
    /// whatsoever. A session-epoch-only guard would miss exactly that
    /// transition and let a stale failure report clear a replacement's
    /// legitimate `Connected` state.
    async fn capture_pre_failure_discovery_generation(&self, addr: SocketAddr) -> Option<u64> {
        let gossip_state = self.gossip_state.lock().await;
        gossip_state
            .peer_discovery
            .as_ref()
            .and_then(|discovery| discovery.connect_generation(&addr))
    }

    /// Whether `addr`'s discovery connect-generation snapshot is still
    /// exactly `expected_generation` (captured earlier by
    /// `capture_pre_failure_discovery_generation`), for the specific purpose
    /// of deciding whether it is safe to clear peer-discovery state.
    ///
    /// Only an EXACT `Some(g)` == `Some(g)` match is "unchanged" and
    /// safe-to-clear. `expected_generation` being `None` is handled by the
    /// caller as a no-op before this is ever called -- it is never treated
    /// as "unchanged" here, because a `Connected` address always has
    /// `Some(generation)` (see `PeerDiscovery::on_peer_connected`); `None`
    /// can only mean the address is Pending, Failed, or entirely untracked,
    /// none of which this clear (which only ever calls
    /// `on_peer_disconnected`, an unconditional removal regardless of
    /// variant) may touch. Any change -- a different `Some(g')`, or the
    /// address no longer showing `Some` at all -- means some
    /// `on_peer_connected`/`on_peer_disconnected` transition happened in
    /// between and must decline.
    fn discovery_connect_generation_unchanged(
        gossip_state: &GossipState,
        addr: SocketAddr,
        expected_generation: u64,
    ) -> bool {
        gossip_state
            .peer_discovery
            .as_ref()
            .and_then(|discovery| discovery.connect_generation(&addr))
            == Some(expected_generation)
    }

    /// Clears `addr`'s peer-discovery `Connected` state only if no
    /// `on_peer_connected` transition has happened for this address since
    /// `expected_generation` was captured (see
    /// `capture_pre_failure_discovery_generation` and
    /// `discovery_connect_generation_unchanged`) -- i.e. only for a
    /// teardown that is still genuinely current.
    ///
    /// `expected_generation` of `None` (discovery did not show `addr` as
    /// `Connected` at the moment the failure was detected -- it was
    /// Pending, Failed, or entirely untracked) is a NO-OP: there is no
    /// `Connected` state to reclaim, and `on_peer_disconnected` removes
    /// whatever unified state DOES exist unconditionally, regardless of
    /// variant -- calling it here would discard a `Failed` peer's backoff
    /// state or a `Pending` reservation, letting an immediate retry bypass
    /// backoff or corrupting capacity accounting, for a peer this specific
    /// report was never about.
    ///
    /// A replacement connection can call `mark_peer_connected` for the same
    /// address in the gap between a failure being detected and this call
    /// running (teardown work in between holds no `gossip_state` lock). If
    /// it has, that replacement already owns `Connected` -- clearing it here
    /// would make `connected_count_unified` undercount a still-live peer and
    /// could let discovery admit connections beyond `max_peers`. Declining
    /// is always safe: a genuinely dead peer whose clear was skipped here
    /// still gets cleaned up the next time its (now current) connection
    /// itself fails.
    async fn clear_discovery_state_if_generation_unchanged(
        &self,
        addr: SocketAddr,
        expected_generation: Option<u64>,
    ) {
        let Some(expected_generation) = expected_generation else {
            debug!(
                addr = %addr,
                "no discovery Connected generation was captured for this address at \
                 failure-detection time (Pending/Failed/untracked); nothing to clear"
            );
            return;
        };

        let mut gossip_state = self.gossip_state.lock().await;
        if Self::discovery_connect_generation_unchanged(&gossip_state, addr, expected_generation) {
            if let Some(ref mut discovery) = gossip_state.peer_discovery {
                discovery.on_peer_disconnected(addr);
            }
        } else {
            debug!(
                addr = %addr,
                "declining to clear peer discovery state; a newer connect \
                 transition has already been recorded for this address since \
                 the failure was detected"
            );
        }
    }

    /// Duplicate connection tie-breaker
    /// When both nodes try to connect simultaneously, use GossipNodeId comparison:
    /// - Lower GossipNodeId keeps outbound connection
    /// - Higher GossipNodeId keeps inbound connection
    ///
    /// Returns true if this connection should be kept.
    pub fn should_keep_connection(
        &self,
        remote_peer_id: &crate::PeerId,
        is_outbound: bool,
    ) -> bool {
        if remote_peer_id == &self.peer_id {
            warn!(
                peer_id = %remote_peer_id,
                "rejecting duplicate connection for local registry identity"
            );
            return false;
        }

        let local_id = self.peer_id.to_node_id();
        let remote_id = remote_peer_id.to_node_id();

        match local_id.as_bytes().cmp(remote_id.as_bytes()) {
            std::cmp::Ordering::Less => is_outbound,
            std::cmp::Ordering::Greater => !is_outbound,
            std::cmp::Ordering::Equal => {
                // Same node ID shouldn't happen in practice
                warn!(
                    local = %local_id,
                    remote = %remote_id,
                    "duplicate connection from same GossipNodeId"
                );
                false
            }
        }
    }

    /// Record that a duplicate-connection tie-break just evicted (or
    /// rejected) a connection for `remote_peer_id`.
    ///
    /// `should_keep_connection` is a pure, stateless function of NodeId
    /// ordering — it has no memory of the eviction it just caused. A single
    /// eviction is completely normal (e.g. ordinary simultaneous-open at
    /// bootstrap, where both sides dial each other and one connection is
    /// evicted exactly once) and must not delay anything — the resulting
    /// connection is typically fine and the peer needs to be reachable as
    /// fast as possible. The pathology is specifically *repeated, rapid*
    /// eviction of the same peer: the losing side of a tie-break (or the
    /// higher-ID side's preferred-inbound fallback dialer, once its wait
    /// times out) re-litigating the same decision on every gossip/supervisor
    /// tick with no backoff, because eviction is a protocol decision rather
    /// than an observed socket failure. Under restart/reconnect churn this
    /// produces a self-sustaining TCP-connect + TLS-accept storm.
    ///
    /// So the cooldown is armed only when this is the *second* eviction for
    /// this peer within `tie_break_reconnect_cooldown` of the previous one —
    /// i.e. only once genuine back-to-back oscillation is directly observed,
    /// never on an isolated first eviction. This bounds the storm's redial
    /// rate without touching which side wins and without taxing ordinary
    /// one-off tie-break convergence.
    /// Record a `resolve_remote_actor_addr` outcome for runtime
    /// observability (PEER_ID_REFACTOR §5): substitutions and
    /// relayed-kept-verbatim events make the storm signature visible in
    /// production telemetry, not only in tests.
    pub(crate) fn note_actor_addr_resolution(
        &self,
        original: SocketAddr,
        resolved: SocketAddr,
        sender_addr: SocketAddr,
        owner_is_sender: bool,
    ) {
        if resolved != original {
            self.addr_substitutions.fetch_add(1, Ordering::Relaxed);
        } else if !owner_is_sender && advertised_ip_unusable(original.ip(), sender_addr.ip()) {
            self.relayed_unusable_addr_kept
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn note_tie_break_eviction(&self, remote_peer_id: &crate::PeerId) {
        self.tie_break_evictions.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let is_rapid_repeat = self
            .tie_break_last_eviction_at
            .read_sync(remote_peer_id, |_, prev| {
                now.saturating_duration_since(*prev) < self.config.tie_break_reconnect_cooldown
            })
            .unwrap_or(false);
        let _ = self
            .tie_break_last_eviction_at
            .upsert_sync(remote_peer_id.clone(), now);
        if is_rapid_repeat {
            let deadline = now + self.config.tie_break_reconnect_cooldown;
            let _ = self
                .tie_break_cooldown_until
                .upsert_sync(remote_peer_id.clone(), deadline);
        }
    }

    /// Returns `true` while a tie-break-triggered reconnect cooldown is
    /// still active for `remote_peer_id` (see `note_tie_break_eviction`).
    pub(crate) fn tie_break_cooldown_active(&self, remote_peer_id: &crate::PeerId) -> bool {
        self.tie_break_cooldown_until
            .read_sync(remote_peer_id, |_, deadline| Instant::now() < *deadline)
            .unwrap_or(false)
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

    fn test_config_with_seed(seed: &str) -> GossipConfig {
        GossipConfig {
            key_pair: Some(KeyPair::new_for_testing(seed)),
            gossip_interval: Duration::from_millis(100),
            cleanup_interval: Duration::from_millis(200),
            peer_retry_interval: Duration::from_millis(50),
            immediate_propagation_enabled: true,
            ..Default::default()
        }
    }

    fn test_config() -> GossipConfig {
        test_config_with_seed("registry_tests")
    }

    fn clock_caps() -> crate::handshake::PeerCapabilities {
        crate::handshake::PeerCapabilities::from_hello_exchange(
            &crate::handshake::Hello::with_features(vec![
                crate::handshake::Feature::ClockCalibration,
            ]),
            &crate::handshake::Hello::with_features(vec![
                crate::handshake::Feature::ClockCalibration,
            ]),
        )
    }

    #[test]
    fn try_new_rejects_missing_identity_key_without_panicking() {
        let result = GossipRegistry::<()>::try_new(test_addr(10_099), GossipConfig::default());
        assert!(matches!(
            result,
            Err(crate::GossipError::InvalidConfig(message))
                if message.contains("key_pair")
        ));
    }

    #[test]
    fn tie_break_cooldown_arms_only_after_rapid_repeat_eviction() {
        let mut config = test_config_with_seed("tie-break-cooldown-repeat");
        config.tie_break_reconnect_cooldown = Duration::from_millis(250);
        let registry = GossipRegistry::<()>::new(test_addr(10_100), config);
        let peer_id = test_peer_id("tie-break-cooldown-peer");

        registry.note_tie_break_eviction(&peer_id);
        assert!(
            !registry.tie_break_cooldown_active(&peer_id),
            "a single ordinary simultaneous-open tie-break must not gate reconnect"
        );

        registry.note_tie_break_eviction(&peer_id);
        assert!(
            registry.tie_break_cooldown_active(&peer_id),
            "a rapid repeated tie-break eviction must arm the reconnect storm guard"
        );
    }

    #[test]
    fn tie_break_cooldown_expires_and_requires_another_rapid_pair() {
        let mut config = test_config_with_seed("tie-break-cooldown-expiry");
        config.tie_break_reconnect_cooldown = Duration::from_millis(30);
        let registry = GossipRegistry::<()>::new(test_addr(10_101), config);
        let peer_id = test_peer_id("tie-break-cooldown-expiry-peer");

        registry.note_tie_break_eviction(&peer_id);
        registry.note_tie_break_eviction(&peer_id);
        assert!(registry.tie_break_cooldown_active(&peer_id));

        std::thread::sleep(Duration::from_millis(45));
        assert!(
            !registry.tie_break_cooldown_active(&peer_id),
            "cooldown must be a bounded delay, not a sticky liveness state"
        );

        registry.note_tie_break_eviction(&peer_id);
        assert!(
            !registry.tie_break_cooldown_active(&peer_id),
            "after expiry, one later eviction is again treated as ordinary bootstrap churn"
        );
    }

    #[test]
    fn clock_sample_math_uses_ntp_four_timestamp_formula() {
        let t1 = 1_000;
        let t2 = 2_550;
        let t3 = 2_570;
        let t4 = 1_120;

        let (offset_ns, rtt_ns, error_bound_ns) =
            compute_clock_sample(t1, t2, t3, t4).expect("valid sample");

        assert_eq!(offset_ns, 1_500);
        assert_eq!(rtt_ns, 100);
        assert_eq!(error_bound_ns, 50);
    }

    #[tokio::test]
    async fn full_sync_learned_actor_route_is_not_a_required_configured_peer() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7400),
            test_config_with_seed("full-sync-learned-route-local"),
        );
        let remote_peer = KeyPair::new_for_testing("full-sync-learned-route-remote").peer_id();
        let actor_addr = test_addr(9400);
        let actor_name = "full-sync/learned-route/service";
        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, remote_peer.clone()),
        );

        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            remote_peer.clone(),
            test_addr(8400),
            1,
            current_timestamp(),
        )
        .await;

        assert!(
            reg.lookup_actor(actor_name).await.is_some(),
            "valid learned actor location should be retained"
        );
        assert_eq!(
            reg.connection_pool
                .peer_id_to_addr
                .read_sync(&remote_peer, |_, addr| *addr),
            Some(actor_addr),
            "valid learned route should remain available for peer-id lookups"
        );
        assert!(
            reg.connection_pool.list_configured_peers().is_empty(),
            "full-sync actor locations are learned routes, not supervised required peers"
        );
    }

    #[tokio::test]
    async fn full_sync_learned_actor_route_does_not_replace_required_peer_dial_addr() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7402),
            test_config_with_seed("full-sync-required-route-local"),
        );
        let remote_peer = KeyPair::new_for_testing("full-sync-required-route-remote").peer_id();
        let required_addr = test_addr(8402);
        let actor_addr = test_addr(9402);
        let actor_name = "full-sync/required-route/service";

        reg.configure_peer(remote_peer.clone(), required_addr).await;

        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, remote_peer.clone()),
        );

        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            remote_peer.clone(),
            required_addr,
            1,
            current_timestamp(),
        )
        .await;

        assert!(
            reg.lookup_actor(actor_name).await.is_some(),
            "valid learned actor location should be retained"
        );
        assert_eq!(
            reg.connection_pool.list_configured_peers(),
            vec![(remote_peer.clone(), required_addr)],
            "required-peer supervisor must keep the configured dial address"
        );
        assert_eq!(
            reg.connection_pool
                .peer_id_to_addr
                .read_sync(&remote_peer, |_, addr| *addr),
            Some(actor_addr),
            "learned route remains available separately from required supervisor address"
        );
    }

    /// An unspecified advertised actor address (`0.0.0.0:<port>`) is the
    /// legitimate wire shape a wildcard-bound peer produces — see
    /// `pubsub.rs::note_interest` and `tests/wildcard_advertise_interest_storm.rs`
    /// — so `merge_full_sync` must repair it using the verified sender
    /// address rather than discard the route entirely (which starves the
    /// receiver of any path to that actor and feeds a reconnect/re-gossip
    /// churn loop). This mirrors `resolve_peer_addr_checked`'s existing
    /// unspecified-bind repair for peer addresses.
    #[tokio::test]
    async fn full_sync_rewrites_unspecified_actor_route_using_sender_addr() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7401),
            test_config_with_seed("full-sync-unspecified-route-local"),
        );
        let remote_peer = KeyPair::new_for_testing("full-sync-unspecified-route-remote").peer_id();
        let actor_addr: SocketAddr = "0.0.0.0:9400".parse().unwrap();
        let actor_name = "full-sync/unspecified-route/service";
        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, remote_peer.clone()),
        );

        let sender_addr: SocketAddr = "10.77.0.33:9400".parse().unwrap();
        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            remote_peer.clone(),
            sender_addr,
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("unspecified actor route must be rewritten and stored, not dropped");
        assert_eq!(
            stored.address,
            SocketAddr::new(sender_addr.ip(), actor_addr.port()).to_string(),
            "rewritten actor location must use the sender's verified IP with the advertised port"
        );
    }

    /// PEER_ID_REFACTOR T1: exhaustive address-class matrix for
    /// `resolve_remote_actor_addr` — every `advertised-class × source-class ×
    /// port × ownership` cell asserted individually. The truth table below is
    /// the human-written contract per advertised value; the sweep applies the
    /// two universal invariants on top: relays never substitute (§1.6) and
    /// the advertised port is always preserved.
    #[test]
    fn wp1_resolve_remote_actor_addr_exhaustive_matrix() {
        use std::net::IpAddr;

        /// When (and only when) the OWNER sent the location, is the
        /// advertised IP kept or substituted with the source IP?
        #[derive(Clone, Copy, Debug)]
        enum OwnerExpectation {
            AlwaysKeep,
            AlwaysSubstitute,
            KeepIfSourceLoopback,
            KeepIfSourceLinkLocal,
        }
        use OwnerExpectation::*;

        let advertised: &[(&str, OwnerExpectation)] = &[
            // unspecified: never a unicast dial target
            ("0.0.0.0", AlwaysSubstitute),
            ("::", AlwaysSubstitute),
            // multicast: never a unicast dial target
            ("224.0.0.1", AlwaysSubstitute),
            ("ff02::1", AlwaysSubstitute),
            // loopback: only meaningful when the sender is loopback too
            ("127.0.0.1", KeepIfSourceLoopback),
            ("::1", KeepIfSourceLoopback),
            // link-local: only meaningful from the same link
            ("169.254.1.1", KeepIfSourceLinkLocal),
            ("fe80::1", KeepIfSourceLinkLocal),
            // private / unique-local / global unicast: always usable as-is
            ("10.1.2.3", AlwaysKeep),
            ("192.168.1.5", AlwaysKeep),
            ("172.16.9.9", AlwaysKeep),
            ("fd00::1", AlwaysKeep),
            ("1.2.3.4", AlwaysKeep),
            ("2606:4700::1", AlwaysKeep),
        ];
        let sources: &[&str] = &[
            "127.0.0.1",    // loopback v4
            "::1",          // loopback v6
            "169.254.9.9",  // link-local v4
            "fe80::9",      // link-local v6
            "10.77.0.33",   // private v4
            "fd00::99",     // unique-local v6
            "1.1.1.1",      // global v4
            "2606:4700::2", // global v6
        ];
        let is_loopback = |ip: IpAddr| ip.is_loopback();
        let is_link_local = |ip: IpAddr| match ip {
            IpAddr::V4(v4) => v4.is_link_local(),
            IpAddr::V6(v6) => v6.is_unicast_link_local(),
        };

        let mut cells = 0usize;
        for (adv_ip_str, expectation) in advertised {
            for src_ip_str in sources {
                for port in [9400u16, 0] {
                    for owner_is_sender in [true, false] {
                        let adv_ip: IpAddr = adv_ip_str.parse().unwrap();
                        let src_ip: IpAddr = src_ip_str.parse().unwrap();
                        let actor_addr = SocketAddr::new(adv_ip, port);
                        let sender_addr = SocketAddr::new(src_ip, 55555);

                        let keep = if !owner_is_sender {
                            // §1.6: a relay's source IP says nothing about
                            // the owner — never substituted, no exceptions.
                            true
                        } else {
                            match expectation {
                                AlwaysKeep => true,
                                AlwaysSubstitute => false,
                                KeepIfSourceLoopback => is_loopback(src_ip),
                                KeepIfSourceLinkLocal => is_link_local(src_ip),
                            }
                        };
                        let expected = if keep {
                            actor_addr
                        } else {
                            // Port always preserved: the sender's source
                            // port is ephemeral, never its listen port.
                            SocketAddr::new(src_ip, port)
                        };

                        let resolved = resolve_remote_actor_addr(
                            "wp1/matrix/service",
                            actor_addr,
                            sender_addr,
                            owner_is_sender,
                        );
                        assert_eq!(
                            resolved, expected,
                            "cell advertised={adv_ip_str} source={src_ip_str} \
                             port={port} owner_is_sender={owner_is_sender}"
                        );
                        cells += 1;
                    }
                }
            }
        }
        assert_eq!(cells, 14 * 8 * 2 * 2, "matrix must cover every cell");
    }

    /// PEER_ID_REFACTOR WP1 (§1.5): an actor location is NEVER dropped over
    /// its address. Port 0 is undialable, but the actor is still routable via
    /// its owning peer's connection (`lookup()` routes remote actors by
    /// `peer_id`, not `.address`), so it must be stored. The port is
    /// preserved: the sender's source port is an ephemeral connect port, not
    /// its listen port, so there is nothing valid to substitute.
    #[tokio::test]
    async fn wp1_full_sync_stores_port_zero_actor_route_keyed_by_identity() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7411),
            test_config_with_seed("wp1-port-zero-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-port-zero-owner").peer_id();
        let actor_addr: SocketAddr = "10.77.0.40:0".parse().unwrap();
        let actor_name = "wp1/port-zero/service";
        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, owner.clone()),
        );

        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            owner.clone(),
            "10.77.0.33:9400".parse().unwrap(),
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("port-0 actor location must be stored (identity-routable), never dropped");
        assert_eq!(stored.peer_id, owner);
        assert_eq!(
            stored.address,
            actor_addr.to_string(),
            "advertised port must be preserved; source port is ephemeral and never substituted"
        );
    }

    /// PEER_ID_REFACTOR WP1: an owner-sent remote-loopback address is
    /// unusable from this node, but the owner is the verified sender, so the
    /// address is resolved from its TLS-verified source IP — never dropped.
    #[tokio::test]
    async fn wp1_full_sync_resolves_owner_remote_loopback_route_using_sender_ip() {
        let reg =
            GossipRegistry::<()>::new(test_addr(7412), test_config_with_seed("wp1-loopback-local"));
        let owner = KeyPair::new_for_testing("wp1-loopback-owner").peer_id();
        let actor_addr: SocketAddr = "127.0.0.1:9400".parse().unwrap();
        let sender_addr: SocketAddr = "10.77.0.33:9400".parse().unwrap();
        let actor_name = "wp1/loopback/service";
        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, owner.clone()),
        );

        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            owner.clone(),
            sender_addr,
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("owner-sent loopback route must be resolved and stored, never dropped");
        assert_eq!(
            stored.address,
            SocketAddr::new(sender_addr.ip(), actor_addr.port()).to_string(),
            "unusable owner-sent IP must be substituted with the verified source IP"
        );
    }

    /// PEER_ID_REFACTOR WP1: link-local advertised by the owner from a
    /// non-link-local source is unusable here and must be resolved from the
    /// verified source IP (today it is accepted verbatim — a latent bug).
    #[tokio::test]
    async fn wp1_full_sync_resolves_owner_link_local_route_using_sender_ip() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7413),
            test_config_with_seed("wp1-link-local-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-link-local-owner").peer_id();
        let actor_addr: SocketAddr = "169.254.1.1:9400".parse().unwrap();
        let sender_addr: SocketAddr = "10.77.0.33:9400".parse().unwrap();
        let actor_name = "wp1/link-local/service";
        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, owner.clone()),
        );

        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            owner.clone(),
            sender_addr,
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("owner-sent link-local route must be resolved and stored");
        assert_eq!(
            stored.address,
            SocketAddr::new(sender_addr.ip(), actor_addr.port()).to_string(),
            "link-local advertised from a non-link-local source must be substituted"
        );
    }

    /// PEER_ID_REFACTOR WP1: a multicast advertised IP can never be a unicast
    /// dial target; owner-sent, it resolves from the verified source IP.
    #[tokio::test]
    async fn wp1_full_sync_resolves_owner_multicast_route_using_sender_ip() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7414),
            test_config_with_seed("wp1-multicast-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-multicast-owner").peer_id();
        let actor_addr: SocketAddr = "224.0.0.1:9400".parse().unwrap();
        let sender_addr: SocketAddr = "10.77.0.33:9400".parse().unwrap();
        let actor_name = "wp1/multicast/service";
        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, owner.clone()),
        );

        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            owner.clone(),
            sender_addr,
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("owner-sent multicast route must be resolved and stored");
        assert_eq!(
            stored.address,
            SocketAddr::new(sender_addr.ip(), actor_addr.port()).to_string(),
        );
    }

    /// PEER_ID_REFACTOR WP1 (§1.6): gossip is transitive — when a RELAY
    /// (sender != owner) forwards a location with an unusable address, the
    /// relay's source IP says nothing about the OWNER's reachability.
    /// Substituting it would falsify the address. It must be stored as-is:
    /// unrouted decoration, while identity routing still works.
    #[tokio::test]
    async fn wp1_full_sync_keeps_relayed_wildcard_route_unfalsified() {
        let reg =
            GossipRegistry::<()>::new(test_addr(7415), test_config_with_seed("wp1-relay-local"));
        let owner = KeyPair::new_for_testing("wp1-relay-owner").peer_id();
        let relay = KeyPair::new_for_testing("wp1-relay-sender").peer_id();
        let actor_addr: SocketAddr = "0.0.0.0:9400".parse().unwrap();
        let relay_addr: SocketAddr = "10.77.0.44:9400".parse().unwrap();
        let actor_name = "wp1/relayed-wildcard/service";
        let mut known_actors = HashMap::new();
        known_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, owner.clone()),
        );

        reg.merge_full_sync(
            HashMap::new(),
            known_actors,
            relay.clone(),
            relay_addr,
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("relayed location must be stored (identity-routable), never dropped");
        assert_eq!(stored.peer_id, owner);
        assert_eq!(
            stored.address,
            actor_addr.to_string(),
            "a relay's source IP must never be substituted for a third party's address"
        );
    }

    /// PEER_ID_REFACTOR WP1: the immediate-delta wire path
    /// (`RegistrationPriority::Immediate`, routed-pubsub interest) obeys the
    /// same never-drop contract as full sync — port 0 stays stored and
    /// identity-routable even when the sender has a configured address to
    /// validate against.
    #[tokio::test]
    async fn wp1_delta_stores_port_zero_actor_route_keyed_by_identity() {
        let registry = GossipRegistry::<()>::new(
            test_addr(7416),
            test_config_with_seed("wp1-delta-port-zero-local"),
        );
        let sender = test_peer_id("wp1-delta-port-zero-sender");
        registry
            .connection_pool
            .set_configured_peer_addr(&sender, "10.77.0.55:9500".parse().unwrap());

        let actor_addr: SocketAddr = "10.77.0.60:0".parse().unwrap();
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "wp1/delta-port-zero/service".to_string(),
                location: RemoteActorLocation::new_with_peer(actor_addr, sender.clone()),
                priority: RegistrationPriority::Immediate,
            }],
            sender_peer_id: sender.clone(),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        registry.apply_delta(delta).await.unwrap();

        let stored = registry
            .lookup_actor("wp1/delta-port-zero/service")
            .await
            .expect("immediate-delta port-0 location must be stored, never dropped");
        assert_eq!(stored.peer_id, sender);
        assert_eq!(stored.address, actor_addr.to_string());
    }

    /// PEER_ID_REFACTOR §1.6 (codex P1): dial-hint learning is
    /// authenticated-source ONLY. A relay forwarding a third party's
    /// location — even with a perfectly usable address — must not be able
    /// to (over)write that peer's dial route: its claim about someone
    /// else's reachability is unauthenticated. The location itself is
    /// still stored (§1.5 never-drop, identity-routable).
    #[tokio::test]
    async fn wp1_full_sync_relay_does_not_learn_dial_route_for_third_party() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7418),
            test_config_with_seed("wp1-relay-route-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-relay-route-owner").peer_id();
        let relay = KeyPair::new_for_testing("wp1-relay-route-sender").peer_id();
        let actor_addr: SocketAddr = "203.0.113.7:9400".parse().unwrap();
        let actor_name = "wp1/relay-route-poison/service";
        let mut known_actors = HashMap::new();
        known_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(actor_addr, owner.clone()),
        );

        reg.merge_full_sync(
            HashMap::new(),
            known_actors,
            relay.clone(),
            "10.77.0.44:9400".parse().unwrap(),
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("relayed location must still be stored (identity-routable)");
        assert_eq!(stored.address, actor_addr.to_string());
        assert_eq!(
            reg.connection_pool.get_configured_peer_addr(&owner),
            None,
            "a relay must never plant or overwrite a third party's dial route (§1.6)"
        );
    }

    /// PEER_ID_REFACTOR (codex P2): a `location.address` that does not
    /// parse as a socket address is hostile/garbage wire data. It must not
    /// be persisted or re-gossiped verbatim — it is canonicalized to the
    /// unspecified address (port 0) and then resolved like any other
    /// unusable advertised address, keeping the actor identity-routable
    /// while bounding the stored field to a typed `SocketAddr`.
    #[tokio::test]
    async fn wp1_full_sync_canonicalizes_malformed_owner_address() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7419),
            test_config_with_seed("wp1-malformed-owner-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-malformed-owner").peer_id();
        let sender_addr: SocketAddr = "10.77.0.33:9400".parse().unwrap();
        let actor_name = "wp1/malformed-owner/service";
        let mut location =
            RemoteActorLocation::new_with_peer("10.0.0.1:9400".parse().unwrap(), owner.clone());
        location.address = "definitely !! not an address \u{1F480}".to_string();
        let mut local_actors = HashMap::new();
        local_actors.insert(actor_name.to_string(), location);

        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            owner.clone(),
            sender_addr,
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("malformed-address location must still be stored (identity-routable)");
        assert_eq!(
            stored.address,
            SocketAddr::new(sender_addr.ip(), 0).to_string(),
            "malformed owner-sent address canonicalizes to unspecified:0, then resolves \
             from the verified source IP — never persisted verbatim"
        );
    }

    /// Codex P2, relay flavor: malformed relayed addresses canonicalize to
    /// the unspecified placeholder and stay there (a relay's source IP is
    /// never substituted for a third party, §1.6).
    #[tokio::test]
    async fn wp1_full_sync_canonicalizes_malformed_relayed_address() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7420),
            test_config_with_seed("wp1-malformed-relay-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-malformed-relay-owner").peer_id();
        let relay = KeyPair::new_for_testing("wp1-malformed-relay-sender").peer_id();
        let actor_name = "wp1/malformed-relay/service";
        let mut location =
            RemoteActorLocation::new_with_peer("10.0.0.1:9400".parse().unwrap(), owner.clone());
        location.address = "<script>alert(1)</script>".to_string();
        let mut known_actors = HashMap::new();
        known_actors.insert(actor_name.to_string(), location);

        reg.merge_full_sync(
            HashMap::new(),
            known_actors,
            relay.clone(),
            "10.77.0.44:9400".parse().unwrap(),
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("malformed relayed location must still be stored (identity-routable)");
        assert_eq!(
            stored.address, "0.0.0.0:0",
            "malformed relayed address is bounded to the canonical placeholder, never stored raw"
        );
        assert_eq!(
            reg.connection_pool.get_configured_peer_addr(&owner),
            None,
            "and it must never configure a dial route"
        );
    }

    /// Codex P2, immediate-delta flavor: the wire path used by routed-pubsub
    /// interest obeys the same canonicalization — malformed addresses are
    /// never persisted verbatim, with or without a configured sender.
    #[tokio::test]
    async fn wp1_delta_canonicalizes_malformed_address() {
        let registry = GossipRegistry::<()>::new(
            test_addr(7421),
            test_config_with_seed("wp1-delta-malformed-local"),
        );
        let sender = test_peer_id("wp1-delta-malformed-sender");
        registry
            .connection_pool
            .set_configured_peer_addr(&sender, "10.77.0.55:9500".parse().unwrap());

        let mut location =
            RemoteActorLocation::new_with_peer("10.0.0.1:9400".parse().unwrap(), sender.clone());
        location.address = "junk:not:a:socket".to_string();
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "wp1/delta-malformed/service".to_string(),
                location,
                priority: RegistrationPriority::Immediate,
            }],
            sender_peer_id: sender.clone(),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        registry.apply_delta(delta).await.unwrap();

        let stored = registry
            .lookup_actor("wp1/delta-malformed/service")
            .await
            .expect("malformed-address delta location must still be stored");
        assert_eq!(
            stored.address, "10.77.0.55:0",
            "canonicalized to unspecified:0 then resolved from the configured sender address"
        );
    }

    /// PEER_ID_REFACTOR §1.7 (codex round-7 P2): dial precedence is
    /// configured → learned → advertised. A REQUIRED peer's
    /// operator-configured dial address must never be displaced by a
    /// learned actor-address hint — the hint may be stale or NAT-only
    /// while the configured address is the routable target. The hint is
    /// still recorded in the fallback index for non-configured lookups.
    #[tokio::test]
    async fn wp1_learned_hint_never_overrides_required_peer_dial_addr() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7423),
            test_config_with_seed("wp1-required-precedence-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-required-precedence-owner").peer_id();
        let required_addr: SocketAddr = "10.77.0.50:9400".parse().unwrap();
        reg.configure_peer(owner.clone(), required_addr).await;

        let learned_addr: SocketAddr = "10.77.0.60:9500".parse().unwrap();
        let mut local_actors = HashMap::new();
        local_actors.insert(
            "wp1/required-precedence/service".to_string(),
            RemoteActorLocation::new_with_peer(learned_addr, owner.clone()),
        );
        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            owner.clone(),
            required_addr,
            1,
            current_timestamp(),
        )
        .await;

        assert_eq!(
            reg.connection_pool.get_configured_peer_addr(&owner),
            Some(required_addr),
            "operator-configured dial address outranks learned hints for required peers (§1.7)"
        );
        assert_eq!(
            reg.connection_pool
                .peer_id_to_addr
                .read_sync(&owner, |_, addr| *addr),
            Some(learned_addr),
            "the learned hint is still recorded in the fallback index"
        );
    }

    /// PEER_ID_REFACTOR §1.6 (codex round-3 P1): full-sync address repair
    /// anchors on the VERIFIED TCP source, never on the bind-derived
    /// `sender_addr` bookkeeping key (which may come from the
    /// peer-controlled `sender_bind_addr` wire field). When both are
    /// supplied, the verified address wins for repair.
    #[tokio::test]
    async fn wp1_full_sync_repair_prefers_verified_source_over_bind_addr() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7422),
            test_config_with_seed("wp1-verified-vs-bind-local"),
        );
        let owner = KeyPair::new_for_testing("wp1-verified-vs-bind-owner").peer_id();
        let bind_addr: SocketAddr = "10.255.255.1:9400".parse().unwrap(); // peer-declared
        let verified_addr: SocketAddr = "10.77.0.33:52511".parse().unwrap(); // TCP source
        let actor_name = "wp1/verified-vs-bind/service";
        let mut local_actors = HashMap::new();
        local_actors.insert(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer("0.0.0.0:9400".parse().unwrap(), owner.clone()),
        );

        reg.merge_full_sync_from(
            local_actors,
            HashMap::new(),
            owner.clone(),
            bind_addr,
            Some(verified_addr),
            None,
            1,
            current_timestamp(),
        )
        .await;

        let stored = reg
            .lookup_actor(actor_name)
            .await
            .expect("wildcard location must be stored and repaired");
        assert_eq!(
            stored.address,
            SocketAddr::new(verified_addr.ip(), 9400).to_string(),
            "repair must use the verified TCP source IP, not the self-declared bind"
        );
    }

    /// R-11 helper: a throwaway connection instance for
    /// `arm_sequence_reset_for_new_session`'s instance-supersession check.
    /// These registry-level tests never publish anything into
    /// `connection_pool`, so `peer_current_connection_snapshot` always
    /// returns `None` for them regardless of which instance is passed here
    /// -- there is no "different" published connection to be superseded
    /// by, so the arm always proceeds. Only the connection-pool-level tests
    /// (`connection_pool::tests`, `handle::tests`) exercise the actual
    /// supersession check against a real published connection.
    fn qa_r11_dummy_connection_instance(
        addr: SocketAddr,
    ) -> std::sync::Arc<crate::connection_pool::LockFreeConnection> {
        std::sync::Arc::new(crate::connection_pool::LockFreeConnection::new(
            addr,
            crate::connection_pool::ConnectionDirection::Inbound,
        ))
    }

    /// Publishes a genuinely-connected `LockFreeConnection` -- WITH a real
    /// stream handle, so it carries its own distinct instance id (see
    /// `LockFreeStreamHandle::instance_id`) -- into `registry`'s connection
    /// pool for `peer_id` at `addr`, indexed by both peer id and address
    /// (`ConnectionPool::add_connection_by_peer_id`), replacing whatever was
    /// previously published there. Unlike `qa_r11_dummy_connection_instance`
    /// (no stream handle, so its `instance_id()` is always `None`), this is
    /// what a test needs to exercise `PeerDiscovery`'s per-instance
    /// replacement-vs-redundant distinction: two calls at the SAME `addr`
    /// yield two connections with two DIFFERENT instance ids, exactly like
    /// two real, distinct TCP connections would.
    async fn publish_connected_instance(
        registry: &GossipRegistry<()>,
        peer_id: &crate::PeerId,
        addr: SocketAddr,
    ) -> std::sync::Arc<crate::connection_pool::LockFreeConnection> {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };
        let (io, _peer_io) = tokio::io::duplex(1024);
        let (stream_handle, _writer, _reader) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Inbound);
        conn.stream_handle = Some(std::sync::Arc::new(stream_handle));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = std::sync::Arc::new(conn);
        assert!(
            registry
                .connection_pool
                .add_connection_by_peer_id(peer_id.clone(), addr, conn.clone()),
            "test setup: publishing the connection instance must succeed"
        );
        conn
    }

    /// R-11 helper: a FullSync from `owner` advertising exactly `actors`,
    /// arriving on the connection whose verified TCP source is `source`.
    async fn qa_r11_full_sync_from(
        reg: &GossipRegistry<()>,
        owner: &crate::PeerId,
        peer_addr: SocketAddr,
        source: SocketAddr,
        sequence: u64,
        actors: &[&str],
    ) {
        let mut local_actors = HashMap::new();
        for name in actors {
            local_actors.insert(
                (*name).to_string(),
                RemoteActorLocation::new_with_peer(peer_addr, owner.clone()),
            );
        }
        reg.merge_full_sync_from(
            local_actors,
            HashMap::new(),
            owner.clone(),
            peer_addr,
            Some(source),
            None,
            sequence,
            current_timestamp(),
        )
        .await;
    }

    /// R-11 (review P1, both bots): the exemption must be scoped to the
    /// connection that armed it.
    ///
    /// A new TLS session is established on every routine reconnect, not only
    /// on restarts, so arming is common. If the exemption were merely
    /// address-level, an OLD connection still draining through the reconnect
    /// could deliver its in-flight lower-sequence FullSync first and consume
    /// it -- and the genuine restart sync arriving on the new connection would
    /// then be rejected by the restored gate, recreating the very outage this
    /// change fixes.
    ///
    /// Scoping to the verified TCP source (ephemeral port included)
    /// distinguishes the two connections.
    #[tokio::test]
    async fn qa_r11_draining_connection_cannot_consume_the_new_sessions_exemption() {
        let reg = GossipRegistry::<()>::new(test_addr(7804), test_config_with_seed("qa-r11-drain"));
        let owner = KeyPair::new_for_testing("qa-r11-drain-owner").peer_id();
        let node_id = owner.to_node_id();
        let peer_addr = test_addr(9404);
        // Two distinct connections from the same peer identity: the old one
        // still draining, and the freshly authenticated one.
        let old_connection = test_addr(55001);
        let new_connection = test_addr(55002);

        reg.add_peer_with_node_id(peer_addr, Some(node_id)).await;
        qa_r11_full_sync_from(
            &reg,
            &owner,
            peer_addr,
            old_connection,
            40,
            &["qa_r11d/X", "qa_r11d/Y"],
        )
        .await;

        // The restarted peer's new connection arms the exemption.
        reg.arm_sequence_reset_for_new_session(peer_addr, node_id, new_connection, &owner, &qa_r11_dummy_connection_instance(new_connection))
            .await;

        // The OLD connection's in-flight lower-sequence FullSync arrives first.
        // It must NOT consume the exemption.
        qa_r11_full_sync_from(&reg, &owner, peer_addr, old_connection, 7, &["qa_r11d/X"]).await;
        assert!(
            reg.lookup_actor("qa_r11d/Y").await.is_some(),
            "R-11: a draining old connection must not be admitted by the new \
             session's exemption"
        );

        // The genuine restart sync on the NEW connection is still admitted.
        qa_r11_full_sync_from(&reg, &owner, peer_addr, new_connection, 1, &["qa_r11d/X"]).await;
        assert!(
            reg.lookup_actor("qa_r11d/Y").await.is_none(),
            "R-11: the exemption must still be available to the connection that \
             armed it, so the restart sync prunes the stale actor"
        );
    }

    /// R-11 helper: a FullSync arriving with the peer's own address as source.
    async fn qa_r11_full_sync(
        reg: &GossipRegistry<()>,
        owner: &crate::PeerId,
        peer_addr: SocketAddr,
        sequence: u64,
        actors: &[&str],
    ) {
        let mut local_actors = HashMap::new();
        for name in actors {
            local_actors.insert(
                (*name).to_string(),
                RemoteActorLocation::new_with_peer(peer_addr, owner.clone()),
            );
        }
        reg.merge_full_sync_from(
            local_actors,
            HashMap::new(),
            owner.clone(),
            peer_addr,
            Some(peer_addr),
            None,
            sequence,
            current_timestamp(),
        )
        .await;
    }

    /// R-11: a peer that crashes and restarts resumes from sequence ~0. The
    /// stale gate (`sequence < last_sequence`) dropped every one of its
    /// FullSyncs forever, because `last_sequence` only ever advances and the
    /// `handle_peer_death` reset the comments reference no longer exists.
    /// The omission-prune therefore never ran, so an actor the peer no longer
    /// hosts stayed in `known_actors` until the 24h TTL — and because the peer
    /// is healthy, the dead-peer reap never fired either.
    #[tokio::test]
    async fn qa_r11_restart_omission_prune_within_one_round() {
        let reg =
            GossipRegistry::<()>::new(test_addr(7801), test_config_with_seed("qa-r11-restart"));
        let owner_kp = KeyPair::new_for_testing("qa-r11-owner");
        let owner = owner_kp.peer_id();
        let node_id = owner.to_node_id();
        let peer_addr = test_addr(9401);

        reg.add_peer_with_node_id(peer_addr, Some(node_id)).await;

        // Pre-restart: B is at sequence 40 hosting X and Y.
        qa_r11_full_sync(&reg, &owner, peer_addr, 40, &["qa_r11/X", "qa_r11/Y"]).await;
        assert!(reg.lookup_actor("qa_r11/X").await.is_some());
        assert!(reg.lookup_actor("qa_r11/Y").await.is_some());

        // B restarts: new authenticated session, sequence back to 1, and it no
        // longer hosts Y.
        reg.arm_sequence_reset_for_new_session(peer_addr, node_id, peer_addr, &owner, &qa_r11_dummy_connection_instance(peer_addr))
            .await;
        qa_r11_full_sync(&reg, &owner, peer_addr, 1, &["qa_r11/X"]).await;

        assert!(
            reg.lookup_actor("qa_r11/X").await.is_some(),
            "R-11: the restarted peer's surviving actor must remain"
        );
        assert!(
            reg.lookup_actor("qa_r11/Y").await.is_none(),
            "R-11: the omission-prune must drop an actor the restarted peer no \
             longer advertises, within one sync round"
        );
    }

    /// R-11: the one-shot must be exactly one-shot. After the restart sync is
    /// admitted the gate is restored, so an in-session replay of an older
    /// sequence is still dropped — that is what the gate exists for.
    #[tokio::test]
    async fn qa_r11_stale_gate_still_blocks_mid_session_replays() {
        let reg = GossipRegistry::<()>::new(test_addr(7802), test_config_with_seed("qa-r11-replay"));
        let owner_kp = KeyPair::new_for_testing("qa-r11-replay-owner");
        let owner = owner_kp.peer_id();
        let node_id = owner.to_node_id();
        let peer_addr = test_addr(9402);

        reg.add_peer_with_node_id(peer_addr, Some(node_id)).await;
        qa_r11_full_sync(&reg, &owner, peer_addr, 40, &["qa_r11r/X", "qa_r11r/Y"]).await;

        // No new session — a replayed old FullSync omitting Y must NOT prune Y.
        qa_r11_full_sync(&reg, &owner, peer_addr, 5, &["qa_r11r/X"]).await;
        assert!(
            reg.lookup_actor("qa_r11r/Y").await.is_some(),
            "R-11: the stale gate must still drop in-session lower-sequence replays"
        );

        // One new session admits exactly one lower-sequence sync...
        reg.arm_sequence_reset_for_new_session(peer_addr, node_id, peer_addr, &owner, &qa_r11_dummy_connection_instance(peer_addr))
            .await;
        qa_r11_full_sync(&reg, &owner, peer_addr, 3, &["qa_r11r/X"]).await;
        assert!(
            reg.lookup_actor("qa_r11r/Y").await.is_none(),
            "R-11: the armed one-shot must admit the restart sync"
        );

        // ...and is consumed: a second, still-lower replay is dropped again.
        qa_r11_full_sync(&reg, &owner, peer_addr, 2, &["qa_r11r/X", "qa_r11r/Z"]).await;
        assert!(
            reg.lookup_actor("qa_r11r/Z").await.is_none(),
            "R-11: the one-shot must be consumed by the sync it admits"
        );
    }

    /// R-11 (security boundary, cf. B-5): the reset is keyed to the
    /// TLS-authenticated identity. Arming for an address whose recorded
    /// `node_id` is a DIFFERENT identity must be a no-op, so a peer cannot
    /// weaponise the reset against a victim's bookkeeping.
    #[tokio::test]
    async fn qa_r11_arming_requires_matching_authenticated_identity() {
        let reg = GossipRegistry::<()>::new(test_addr(7803), test_config_with_seed("qa-r11-ident"));
        let owner = KeyPair::new_for_testing("qa-r11-ident-owner").peer_id();
        let attacker_node_id = KeyPair::new_for_testing("qa-r11-ident-attacker")
            .peer_id()
            .to_node_id();
        let peer_addr = test_addr(9403);

        reg.add_peer_with_node_id(peer_addr, Some(owner.to_node_id()))
            .await;
        qa_r11_full_sync(&reg, &owner, peer_addr, 40, &["qa_r11i/X", "qa_r11i/Y"]).await;

        // A different identity must not be able to arm the victim's reset.
        reg.arm_sequence_reset_for_new_session(peer_addr, attacker_node_id, peer_addr, &owner, &qa_r11_dummy_connection_instance(peer_addr))
            .await;
        qa_r11_full_sync(&reg, &owner, peer_addr, 1, &["qa_r11i/X"]).await;

        assert!(
            reg.lookup_actor("qa_r11i/Y").await.is_some(),
            "R-11: arming under a non-matching identity must not open the stale gate"
        );
    }

    /// R-11 helper: a FullSync arriving with an explicit, independent session
    /// discriminator (`session_source`) distinct from the connection's
    /// verified TCP source (`verified_addr`). Mirrors what an OUTBOUND
    /// connection's receive path reports: `verified_addr` is the peer's
    /// fixed dial-target address (identical for every outbound connection we
    /// ever make to it, since the repair anchor is the connection's remote
    /// end), while `session_source` is the dialling socket's OWN local
    /// ephemeral port (unique per connection).
    async fn qa_r11_full_sync_with_session_source(
        reg: &GossipRegistry<()>,
        owner: &crate::PeerId,
        peer_addr: SocketAddr,
        verified_addr: SocketAddr,
        session_source: SocketAddr,
        sequence: u64,
        actors: &[&str],
    ) {
        let mut local_actors = HashMap::new();
        for name in actors {
            local_actors.insert(
                (*name).to_string(),
                RemoteActorLocation::new_with_peer(peer_addr, owner.clone()),
            );
        }
        reg.merge_full_sync_from(
            local_actors,
            HashMap::new(),
            owner.clone(),
            peer_addr,
            Some(verified_addr),
            Some(session_source),
            sequence,
            current_timestamp(),
        )
        .await;
    }

    /// R-11 (P1 review finding): once the restart-reset accepts the new
    /// session's baseline sequence, it must persist as the new high-water
    /// mark for the REST of the session, not merely for the sync that
    /// triggered it. Before this fix, an unconditional `max()` (both
    /// `handle_gossip_response`'s own redundant update, and the stale
    /// gate's `else` branch before the current-session gate existed) could
    /// let an old, still-draining connection's in-flight, numerically
    /// higher (pre-restart) sequence silently push `last_sequence` back up
    /// after the reset -- making the SECOND and THIRD genuine post-restart
    /// FullSyncs look stale again, with no exemption left to rescue them
    /// (the one-shot was already spent on the first).
    #[tokio::test]
    async fn qa_r11_restarted_peers_later_syncs_survive_stale_traffic_from_old_connection() {
        let reg =
            GossipRegistry::<()>::new(test_addr(7806), test_config_with_seed("qa-r11-persist"));
        let owner = KeyPair::new_for_testing("qa-r11-persist-owner").peer_id();
        let node_id = owner.to_node_id();
        let peer_addr = test_addr(9406);
        let old_connection = test_addr(57001);
        let new_connection = test_addr(57002);

        reg.add_peer_with_node_id(peer_addr, Some(node_id)).await;
        // Pre-restart: peer is at sequence 40.
        qa_r11_full_sync_from(
            &reg,
            &owner,
            peer_addr,
            old_connection,
            40,
            &["qa_r11p/X", "qa_r11p/Y"],
        )
        .await;

        // Peer restarts: new session armed, first sync (seq=1) accepted.
        reg.arm_sequence_reset_for_new_session(peer_addr, node_id, new_connection, &owner, &qa_r11_dummy_connection_instance(new_connection))
            .await;
        qa_r11_full_sync_from(&reg, &owner, peer_addr, new_connection, 1, &["qa_r11p/X"]).await;
        assert!(
            reg.lookup_actor("qa_r11p/Y").await.is_none(),
            "sanity: the first restart sync must prune Y"
        );

        // The OLD connection is still draining and delivers ANOTHER
        // in-flight, pre-restart (numerically high) sequence AFTER the
        // reset. It must be ignored outright, not merged into
        // `last_sequence`.
        qa_r11_full_sync_from(
            &reg,
            &owner,
            peer_addr,
            old_connection,
            41,
            &["qa_r11p/X", "qa_r11p/Y"],
        )
        .await;

        // The restarted peer's SECOND genuine sync (seq=2) must still be
        // accepted -- proven by a brand-new actor actually being added,
        // which can only happen if the sync was processed rather than
        // dropped by the stale gate.
        qa_r11_full_sync_from(
            &reg,
            &owner,
            peer_addr,
            new_connection,
            2,
            &["qa_r11p/X", "qa_r11p/Q"],
        )
        .await;
        assert!(
            reg.lookup_actor("qa_r11p/Q").await.is_some(),
            "R-11: the restarted peer's SECOND post-restart sync must be \
             accepted, not rejected as stale because an old connection's \
             traffic silently restored the pre-restart high-water mark"
        );
        assert!(
            reg.lookup_actor("qa_r11p/Y").await.is_none(),
            "R-11: the old connection's attempt to resurrect Y must still \
             have been ignored"
        );

        // ...and the THIRD.
        qa_r11_full_sync_from(
            &reg,
            &owner,
            peer_addr,
            new_connection,
            3,
            &["qa_r11p/X", "qa_r11p/Q", "qa_r11p/R"],
        )
        .await;
        assert!(
            reg.lookup_actor("qa_r11p/R").await.is_some(),
            "R-11: the restarted peer's THIRD post-restart sync must also \
             be accepted"
        );
    }

    /// R-11 (P1 review finding): only the connection that armed the
    /// exemption may clear it. An old, still-draining connection's in-flight
    /// NON-lower (ordinary) sequence must be dropped outright once a newer
    /// session is known for this peer, and must NOT disarm a still-unused
    /// exemption meant for that newer session.
    #[tokio::test]
    async fn qa_r11_only_the_arming_connection_can_clear_the_exemption() {
        let reg = GossipRegistry::<()>::new(test_addr(7807), test_config_with_seed("qa-r11-clear"));
        let owner = KeyPair::new_for_testing("qa-r11-clear-owner").peer_id();
        let node_id = owner.to_node_id();
        let peer_addr = test_addr(9407);
        let old_connection = test_addr(58001);
        let new_connection = test_addr(58002);

        reg.add_peer_with_node_id(peer_addr, Some(node_id)).await;
        qa_r11_full_sync_from(
            &reg,
            &owner,
            peer_addr,
            old_connection,
            40,
            &["qa_r11c/X", "qa_r11c/Y"],
        )
        .await;

        // New session armed for a genuine restart, but the restart sync
        // itself hasn't arrived yet.
        reg.arm_sequence_reset_for_new_session(peer_addr, node_id, new_connection, &owner, &qa_r11_dummy_connection_instance(new_connection))
            .await;

        // The OLD connection delivers an in-flight, NON-lower sequence (its
        // own continuation, not a restart) before the new connection's
        // low-sequence sync arrives. It must be dropped outright (wrong
        // session) and must NOT clear the still-armed exemption.
        qa_r11_full_sync_from(
            &reg,
            &owner,
            peer_addr,
            old_connection,
            41,
            &["qa_r11c/X", "qa_r11c/Y"],
        )
        .await;

        // The genuine restart sync on the NEW connection must still be
        // admitted -- it would be rejected by the stale gate if the old
        // connection's traffic above had cleared the exemption.
        qa_r11_full_sync_from(&reg, &owner, peer_addr, new_connection, 1, &["qa_r11c/X"]).await;
        assert!(
            reg.lookup_actor("qa_r11c/Y").await.is_none(),
            "R-11: an old connection's non-lower-sequence traffic must not \
             clear the exemption meant for the new session before the \
             restart sync arrives"
        );
    }

    /// R-11 (P1 review finding, outbound): the outbound receive path's
    /// `verified_sender_addr` is the peer's fixed dial-target address,
    /// IDENTICAL for every connection we ever make to it -- unlike inbound,
    /// where the remote's ephemeral port is naturally unique per
    /// connection. Gating the exemption on `verified_sender_addr` alone
    /// would let an old, still-draining OUTBOUND connection consume or
    /// pollute a new outbound session's exemption, since both report the
    /// SAME address. The session-source discriminator (the dialling
    /// socket's own local ephemeral port) is what must distinguish them
    /// instead -- this is the outbound analogue of
    /// `qa_r11_draining_connection_cannot_consume_the_new_sessions_exemption`.
    #[tokio::test]
    async fn qa_r11_draining_outbound_connection_cannot_consume_the_new_sessions_exemption() {
        let reg = GossipRegistry::<()>::new(
            test_addr(7808),
            test_config_with_seed("qa-r11-outbound-drain"),
        );
        let owner = KeyPair::new_for_testing("qa-r11-outbound-drain-owner").peer_id();
        let node_id = owner.to_node_id();
        // The peer's fixed listening port: the dial target for every
        // outbound connection we ever make to it, and what the outbound
        // receive path reports as `verified_sender_addr` regardless of
        // which physical connection delivered the message.
        let peer_addr = test_addr(9408);
        // Only the LOCAL ephemeral session source differs between the old
        // (still draining) and new outbound connections.
        let old_local_session = test_addr(56001);
        let new_local_session = test_addr(56002);

        reg.add_peer_with_node_id(peer_addr, Some(node_id)).await;
        qa_r11_full_sync_with_session_source(
            &reg,
            &owner,
            peer_addr,
            peer_addr,
            old_local_session,
            40,
            &["qa_r11o/X", "qa_r11o/Y"],
        )
        .await;

        // This node redials the restarted peer: a new outbound session is
        // established and armed.
        reg.arm_sequence_reset_for_new_session(peer_addr, node_id, new_local_session, &owner, &qa_r11_dummy_connection_instance(new_local_session))
            .await;

        // The OLD outbound connection is still draining and delivers an
        // in-flight, pre-restart (numerically HIGH) sequence, reporting the
        // SAME verified_sender_addr the new connection would (the exact
        // outbound asymmetry this fix closes).
        qa_r11_full_sync_with_session_source(
            &reg,
            &owner,
            peer_addr,
            peer_addr,
            old_local_session,
            41,
            &["qa_r11o/X", "qa_r11o/Y"],
        )
        .await;
        assert!(
            reg.lookup_actor("qa_r11o/Y").await.is_some(),
            "R-11 (outbound): a draining old outbound connection must not \
             be able to consume or extend the new session's exemption \
             merely because it shares the peer's fixed dial-target address"
        );

        // The genuine restart sync on the NEW outbound connection is still
        // admitted.
        qa_r11_full_sync_with_session_source(
            &reg,
            &owner,
            peer_addr,
            peer_addr,
            new_local_session,
            1,
            &["qa_r11o/X"],
        )
        .await;
        assert!(
            reg.lookup_actor("qa_r11o/Y").await.is_none(),
            "R-11 (outbound): the exemption must still be available to the \
             connection that armed it, so the restart sync prunes the \
             stale actor"
        );

        // A later genuine sync from the NEW connection keeps advancing
        // normally -- the old connection's dead-epoch traffic must not have
        // poisoned `last_sequence` against it.
        qa_r11_full_sync_with_session_source(
            &reg,
            &owner,
            peer_addr,
            peer_addr,
            new_local_session,
            2,
            &["qa_r11o/X", "qa_r11o/Q"],
        )
        .await;
        assert!(
            reg.lookup_actor("qa_r11o/Q").await.is_some(),
            "R-11 (outbound): the new connection's second sync must still \
             be accepted"
        );
    }

    /// PEER_ID_REFACTOR §5 runtime observability: substitutions,
    /// relayed-kept events, and tie-break evictions are counted and exposed
    /// via `get_stats` so the storm signature is visible in prod telemetry.
    #[tokio::test]
    async fn wp4_storm_signature_counters_are_exposed_in_stats() {
        let reg =
            GossipRegistry::<()>::new(test_addr(7417), test_config_with_seed("wp4-counters-local"));
        let owner = KeyPair::new_for_testing("wp4-counters-owner").peer_id();
        let relay = KeyPair::new_for_testing("wp4-counters-relay").peer_id();
        let sender_addr: SocketAddr = "10.77.0.33:9400".parse().unwrap();

        // Owner-sent wildcard → substitution counted.
        let mut local_actors = HashMap::new();
        local_actors.insert(
            "wp4/owner-wildcard/service".to_string(),
            RemoteActorLocation::new_with_peer("0.0.0.0:9400".parse().unwrap(), owner.clone()),
        );
        reg.merge_full_sync(
            local_actors,
            HashMap::new(),
            owner.clone(),
            sender_addr,
            1,
            current_timestamp(),
        )
        .await;

        // Relayed wildcard → kept verbatim, counted separately.
        let mut known_actors = HashMap::new();
        known_actors.insert(
            "wp4/relayed-wildcard/service".to_string(),
            RemoteActorLocation::new_with_peer("0.0.0.0:9401".parse().unwrap(), owner.clone()),
        );
        reg.merge_full_sync(
            HashMap::new(),
            known_actors,
            relay.clone(),
            "10.77.0.44:9400".parse().unwrap(),
            1,
            current_timestamp(),
        )
        .await;

        reg.note_tie_break_eviction(&owner);

        let stats = reg.get_stats().await;
        assert_eq!(stats.addr_substitutions, 1, "owner-sent wildcard repaired");
        assert_eq!(stats.relayed_addr_kept, 1, "relayed wildcard kept verbatim");
        assert_eq!(stats.tie_break_evictions, 1, "tie-break eviction counted");
    }

    #[test]
    fn clock_sample_rejects_reversed_local_or_remote_time() {
        assert!(compute_clock_sample(2, 10, 11, 1).is_none());
        assert!(compute_clock_sample(1, 20, 19, 30).is_none());
    }

    #[tokio::test]
    async fn clock_probe_is_gated_per_peer_without_extra_gossip_tasks() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer = test_addr(8081);
        registry.set_peer_capabilities(peer, clock_caps());

        let first = registry
            .gossip_extensions_for_outbound(peer, 1_000_000_000)
            .await
            .expect("first sample should attach");
        let first_probe = first.clock_probe.expect("probe");

        registry.record_inbound_gossip_extensions(
            peer,
            Some(GossipExtensionsV1 {
                clock_probe: None,
                clock_echo: Some(ClockEchoV1 {
                    sample_id: first_probe.sample_id,
                    origin_sender_wall_ns: first_probe.sender_wall_ns,
                    responder_recv_wall_ns: first_probe.sender_wall_ns + 10,
                    responder_send_wall_ns: first_probe.sender_wall_ns + 20,
                }),
            }),
            first_probe.sender_wall_ns + 30,
        );

        let second = registry
            .gossip_extensions_for_outbound(peer, 1_000_000_000 + 59_000_000_000)
            .await;
        assert!(
            second.is_none(),
            "probe should not create work before the 60s gate"
        );

        let third = registry
            .gossip_extensions_for_outbound(peer, 1_000_000_000 + 60_000_000_000)
            .await
            .expect("sample at the 60s gate should attach");
        assert!(third.clock_probe.is_some());
    }

    #[tokio::test]
    async fn clock_probe_retries_lost_initial_sample_after_timeout() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer = test_addr(8081);
        registry.set_peer_capabilities(peer, clock_caps());

        let first = registry
            .gossip_extensions_for_outbound(peer, 1_000_000_000)
            .await
            .expect("first sample should attach");
        assert!(first.clock_probe.is_some());

        let duplicate = registry
            .gossip_extensions_for_outbound(
                peer,
                1_000_000_000 + CLOCK_CALIBRATION_PROBE_TIMEOUT_NS - 1,
            )
            .await;
        assert!(
            duplicate.is_none(),
            "live pending probe should suppress duplicate work"
        );

        let retry = registry
            .gossip_extensions_for_outbound(
                peer,
                1_000_000_000 + CLOCK_CALIBRATION_PROBE_TIMEOUT_NS,
            )
            .await
            .expect("lost pending probe should be retried");
        assert!(retry.clock_probe.is_some());
    }

    #[tokio::test]
    async fn clock_probe_is_not_attached_without_negotiated_feature() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer = test_addr(8081);

        let extensions = registry.gossip_extensions_for_outbound(peer, 1_000).await;

        assert!(extensions.is_none());
        assert!(registry.pending_clock_probes.is_empty());
        assert!(registry.peer_clock_snapshot(&peer).is_none());
    }

    #[tokio::test]
    async fn clock_echo_roundtrip_records_peer_snapshot() {
        let origin = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let responder = GossipRegistry::<()>::new(test_addr(8081), test_config());
        let peer = test_addr(8081);
        origin.set_peer_capabilities(peer, clock_caps());
        responder.set_peer_capabilities(peer, clock_caps());

        let probe_ext = origin
            .gossip_extensions_for_outbound(peer, 1_000)
            .await
            .expect("origin attaches probe");

        responder.record_inbound_gossip_extensions(peer, Some(probe_ext), 2_550);
        let echo_ext = responder
            .gossip_extensions_for_outbound(peer, 2_570)
            .await
            .expect("responder attaches echo");
        assert!(echo_ext.clock_echo.is_some());

        origin.record_inbound_gossip_extensions(peer, Some(echo_ext), 1_120);
        let snapshot = origin
            .peer_clock_snapshot(&peer)
            .expect("origin records calibration snapshot");

        assert_eq!(snapshot.offset_ns, 1_500);
        assert_eq!(snapshot.rtt_ns, 100);
        assert_eq!(snapshot.error_bound_ns, 50);
        assert_eq!(snapshot.sample_count, 1);
        assert!(!snapshot.is_stale_at(snapshot.sampled_at_wall_ns + 10));
        assert!(
            snapshot
                .is_stale_at(snapshot.sampled_at_wall_ns + CLOCK_CALIBRATION_STALE_AFTER_NS + 1)
        );
    }

    #[tokio::test]
    async fn forged_clock_echo_cannot_void_another_peers_pending_probe() {
        let origin = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let responder = GossipRegistry::<()>::new(test_addr(8081), test_config());
        let peer_a = test_addr(8081);
        let peer_b = test_addr(8082);
        origin.set_peer_capabilities(peer_a, clock_caps());
        responder.set_peer_capabilities(peer_a, clock_caps());

        // Origin probes peer A: pending_clock_probes now holds A's probe.
        let probe_ext = origin
            .gossip_extensions_for_outbound(peer_a, 1_000)
            .await
            .expect("origin attaches probe for A");
        let sample_id = probe_ext.clock_probe.expect("probe").sample_id;
        assert!(origin.pending_clock_probes.contains_sync(&sample_id));

        // A genuine responder builds the echo peer A would return.
        responder.record_inbound_gossip_extensions(peer_a, Some(probe_ext), 2_550);
        let echo_ext = responder
            .gossip_extensions_for_outbound(peer_a, 2_570)
            .await
            .expect("responder attaches echo");

        // An authenticated peer B replays/guesses A's sample_id and delivers the
        // echo from its own address. This forgery must NOT destroy A's probe.
        origin.record_inbound_gossip_extensions(peer_b, Some(echo_ext), 1_120);
        assert!(
            origin.pending_clock_probes.contains_sync(&sample_id),
            "forged echo from peer B must not void peer A's in-flight probe"
        );
        assert!(origin.peer_clock_snapshot(&peer_b).is_none());

        // The genuine echo from A still lands.
        origin.record_inbound_gossip_extensions(peer_a, Some(echo_ext), 1_120);
        assert!(
            origin.peer_clock_snapshot(&peer_a).is_some(),
            "genuine echo from A must record the calibration snapshot"
        );
    }

    #[tokio::test]
    async fn clock_calibration_does_not_touch_vector_clock_or_delta_state() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer = test_addr(8081);
        registry.set_peer_capabilities(peer, clock_caps());

        let location = test_location(test_addr(9000));
        location.vector_clock.increment(location.node_id);
        let before = location.vector_clock.clone();
        let sequence_before = {
            let state = registry.gossip_state.lock().await;
            (state.gossip_sequence, state.delta_history.len())
        };

        let probe = registry
            .gossip_extensions_for_outbound(peer, 10_000)
            .await
            .expect("probe");
        registry.record_inbound_gossip_extensions(peer, Some(probe), 10_100);

        assert_eq!(
            location.vector_clock.compare(&before),
            crate::ClockOrdering::Equal
        );
        let sequence_after = {
            let state = registry.gossip_state.lock().await;
            (state.gossip_sequence, state.delta_history.len())
        };
        assert_eq!(sequence_after, sequence_before);
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
        let msg = RegistryMessage::DeltaGossip {
            delta,
            extensions: None,
        };
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
            "410133a9bd50aee88fc0da1b30ece8a53313492dfcf3a8c4ff5f3048121c1d85"
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
            addr_substitutions: 0,
            relayed_addr_kept: 0,
            tie_break_evictions: 0,
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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

    /// Audit finding A1: TLS server-cert GossipNodeId pinning is only enforced when
    /// the dial supplies a GossipNodeId-encoded SNI. A configured cluster peer that
    /// has not yet connected had no addr->GossipNodeId mapping (it lived only in the
    /// *configured* peer map), so `lookup_node_id` returned `None` and the
    /// first dial fell back to an unauthenticated placeholder SNI. The expected
    /// GossipNodeId must be resolvable from the configured peer map alone.
    #[tokio::test]
    async fn lookup_node_id_resolves_configured_peer_before_first_connection() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer = test_peer_id("a1_configured_peer");
        let peer_addr = test_addr(9301);

        // Configure the peer's dial address without ever connecting to it.
        registry
            .connection_pool
            .set_configured_peer_addr(&peer, peer_addr);

        let resolved = registry.lookup_node_id(&peer_addr).await;
        assert_eq!(
            resolved,
            Some(peer.to_node_id()),
            "a configured peer's GossipNodeId must be resolvable by address so the \
             dial pins it in the SNI instead of using a placeholder"
        );
    }

    /// Liveness-window clamp bug: raising `gossip_interval` (the cadence that
    /// actually refreshes `last_response_received_ms` via delta/full-sync
    /// responses) far above `peer_gossip_interval` must still raise the
    /// required-peer floor. Before the fix, the floor was computed from
    /// `peer_gossip_interval*2` only, so a required peer with a slow
    /// `gossip_interval` would be false-failed by the response-asymmetry
    /// detector well before any inbound payload was actually overdue.
    #[tokio::test]
    async fn effective_liveness_window_floors_required_peer_to_gossip_interval_not_peer_gossip_interval()
     {
        let mut config = test_config_with_seed("effective-liveness-window-required-peer");
        config.gossip_interval = Duration::from_secs(30);
        config.peer_gossip_interval = Some(Duration::from_secs(5));
        config.peer_liveness_window = Duration::from_secs(10);
        let registry = GossipRegistry::<()>::new(test_addr(9500), config);
        let peer = test_peer_id("effective-liveness-window-required-peer-remote");
        let peer_addr = test_addr(9501);

        registry
            .connection_pool
            .set_configured_peer_addr(&peer, peer_addr);
        registry
            .connection_pool
            .add_addr_to_peer_id(peer_addr, peer.clone());

        let effective = registry.effective_peer_liveness_window_ms(peer_addr);

        assert!(
            effective >= 60_000,
            "required-peer floor must reflect the gossip_interval (30s) cadence that \
             actually refreshes last_response_received_ms, not peer_gossip_interval (5s); \
             got {effective}ms"
        );
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

    /// Build a `PeerInfo` rooted at `addr` and pre-bound to `node_id`.
    /// Used by the duplicate-broadcast regression tests to manufacture the
    /// "two SocketAddr aliases for the same physical peer" state that the
    /// devnet stratum trace surfaced for sender `f4061522…`.
    fn peer_info_with_node_id(addr: SocketAddr, node_id: crate::GossipNodeId) -> PeerInfo {
        let now = crate::current_timestamp();
        let now_ms = crate::current_timestamp_millis();
        PeerInfo {
            address: addr,
            peer_address: None,
            inbound_observed: true,
            outbound_dial_success: true,
            node_id: Some(node_id),
            dns_name: None,
            failures: 0,
            last_attempt: now,
            last_success: now,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
            last_dns_refresh_attempt: None,
            last_response_received_ms: now_ms,
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
        }
    }

    /// Regression for the devnet f4061522 trace: when the same physical peer
    /// appears in `gossip_state.peers` under two distinct `SocketAddr` keys
    /// (e.g. ephemeral TCP-source address still present alongside the
    /// migrated bind address, or DNS-resolved IPv4/IPv6 aliases), the
    /// periodic gossip round must emit **one** task per stable peer identity
    /// rather than one task per address alias. Without dedup the same delta
    /// is delivered N times to the same socket, which is what produced the
    /// 3× "RECEIVING IMMEDIATE CHANGES" burst from f4061522 in 130μs.
    #[tokio::test]
    async fn prepare_gossip_round_deduplicates_peer_aliases_by_node_id() {
        let mut config = test_config();
        config.small_cluster_threshold = 0;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        let shared_peer_id = test_peer_id("shared_remote");
        let shared_node_id = shared_peer_id.to_node_id();

        // Two SocketAddr aliases for the same physical peer.
        let alias_a = test_addr(9101);
        let alias_b = test_addr(9102);
        {
            let mut state = registry.gossip_state.lock().await;
            state
                .peers
                .insert(alias_a, peer_info_with_node_id(alias_a, shared_node_id));
            state
                .peers
                .insert(alias_b, peer_info_with_node_id(alias_b, shared_node_id));
        }

        registry
            .register_actor(
                "shared_target_actor".to_string(),
                test_location(test_addr(7777)),
            )
            .await
            .unwrap();

        let tasks = registry.prepare_gossip_round().await.unwrap();

        let aliases_targeted = tasks
            .iter()
            .filter(|t| t.peer_addr == alias_a || t.peer_addr == alias_b)
            .count();
        assert_eq!(
            aliases_targeted,
            1,
            "expected exactly one gossip task for the shared GossipNodeId across \
             aliases {alias_a} and {alias_b}, got {aliases_targeted} (tasks: {:?})",
            tasks.iter().map(|t| t.peer_addr).collect::<Vec<_>>()
        );
    }

    /// A peer can be genuinely retry-eligible (its `last_attempt` really is
    /// more than `peer_retry_interval` in the past) while its independently
    /// recorded `last_failure_time` sits ahead of the current wall-clock read
    /// — e.g. a backward NTP step landed between the two writes. The
    /// gossip-retry log path must not panic computing `time_since_failure`
    /// for such a peer.
    #[tokio::test]
    async fn prepare_gossip_round_survives_backward_last_failure_time_step() {
        let mut config = test_config();
        config.small_cluster_threshold = 0;
        config.max_peer_failures = 3;
        config.peer_retry_interval = Duration::from_millis(1);
        let registry = GossipRegistry::<()>::new(test_addr(8094), config);
        let peer_addr = test_addr(8095);
        let peer_id = test_peer_id("backward-clock-retry-log");

        {
            let mut state = registry.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: true,
                    outbound_dial_success: true,
                    node_id: Some(peer_id.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    // Genuinely in the past, so the (safe, saturating) retry
                    // window gate lets this peer through.
                    last_attempt: current_timestamp().saturating_sub(60),
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    // Recorded "in the future" relative to the wall clock read
                    // inside `prepare_gossip_round` — simulates a backward step.
                    last_failure_time: Some(current_timestamp() + 10_000),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
        }

        // Must not panic (RED: raw subtraction underflows in debug builds).
        let _ = registry.prepare_gossip_round().await;
    }

    /// Mirror of the above for the urgent fan-out path
    /// (`trigger_immediate_gossip`): a single immediate-priority registration
    /// must produce at most one DeltaGossip per physical peer, regardless of
    /// how many SocketAddr aliases share its GossipNodeId.
    #[tokio::test]
    async fn trigger_immediate_gossip_deduplicates_peer_aliases_by_node_id() {
        let mut config = test_config();
        config.urgent_gossip_fanout = 8;
        config.immediate_propagation_enabled = true;
        let registry = std::sync::Arc::new(GossipRegistry::<()>::new(test_addr(8080), config));

        let shared_peer_id = test_peer_id("shared_remote_urgent");
        let shared_node_id = shared_peer_id.to_node_id();
        let alias_a = test_addr(9201);
        let alias_b = test_addr(9202);
        let alias_c = test_addr(9203);

        {
            let mut state = registry.gossip_state.lock().await;
            for addr in [alias_a, alias_b, alias_c] {
                state
                    .peers
                    .insert(addr, peer_info_with_node_id(addr, shared_node_id));
            }
        }

        // Push an urgent change directly so we don't depend on the
        // outbound-connection path (`trigger_immediate_gossip` early-exits
        // when no live connections exist, but it still selects peers first
        // — and that selection is the point we're asserting on).
        {
            let mut state = registry.gossip_state.lock().await;
            let change = RegistryChange::ActorAdded {
                name: "urgent_target".to_string(),
                location: {
                    let mut loc = test_location(test_addr(7778));
                    loc.priority = RegistrationPriority::Immediate;
                    loc
                },
                priority: RegistrationPriority::Immediate,
            };
            state.urgent_changes.push(change);
        }

        let selected = registry.select_immediate_gossip_peers_for_test().await;
        let aliases_selected = selected
            .iter()
            .filter(|addr| **addr == alias_a || **addr == alias_b || **addr == alias_c)
            .count();
        assert_eq!(
            aliases_selected, 1,
            "expected exactly one immediate-gossip target for shared GossipNodeId across \
             aliases {alias_a},{alias_b},{alias_c}, got {aliases_selected} (selected: {:?})",
            selected
        );
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
    async fn register_actor_reclaims_stale_known_actor_at_same_address() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let actor_name = "test_actor_stale_same_addr";
        let service_addr = test_addr(9001);
        let stale_peer = test_peer_id("stale-known-owner");

        registry.actor_state.known_actors.upsert_sync(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(service_addr, stale_peer),
        );

        let result = registry
            .register_actor(actor_name.to_string(), test_location(service_addr))
            .await;

        assert!(result.is_ok());
        assert!(registry.actor_state.local_actors.contains_sync(actor_name));
        assert!(!registry.actor_state.known_actors.contains_sync(actor_name));
    }

    #[tokio::test]
    async fn register_actor_rejects_known_actor_at_different_address() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let actor_name = "test_actor_known_elsewhere";
        let stale_peer = test_peer_id("known-owner-elsewhere");

        registry.actor_state.known_actors.upsert_sync(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9002), stale_peer),
        );

        let result = registry
            .register_actor(actor_name.to_string(), test_location(test_addr(9001)))
            .await;

        assert!(matches!(result, Err(GossipError::ActorAlreadyExists(_))));
        assert!(!registry.actor_state.local_actors.contains_sync(actor_name));
        assert!(registry.actor_state.known_actors.contains_sync(actor_name));
    }

    #[tokio::test]
    async fn register_actor_replacing_known_drops_learned_owner() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let actor_name = "test_actor_replace_known";
        let remote_peer = test_peer_id("replace-known-owner");

        registry.actor_state.known_actors.upsert_sync(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9002), remote_peer),
        );

        let result = registry
            .register_actor_replacing_known(actor_name.to_string(), test_location(test_addr(9001)))
            .await;

        assert!(result.is_ok());
        assert!(registry.actor_state.local_actors.contains_sync(actor_name));
        assert!(!registry.actor_state.known_actors.contains_sync(actor_name));
    }

    #[tokio::test]
    async fn register_actor_replacing_known_keeps_local_duplicate_rejection() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let actor_name = "test_actor_replace_known_local_duplicate";

        registry
            .register_actor(actor_name.to_string(), test_location(test_addr(9001)))
            .await
            .unwrap();

        let result = registry
            .register_actor_replacing_known(actor_name.to_string(), test_location(test_addr(9001)))
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
            RegistryMessage::DeltaGossip { delta, .. } => {
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
    async fn test_lingering_urgent_change_commits_to_regular_delta_history() {
        let mut config = test_config();
        config.small_cluster_threshold = 0;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);
        registry.add_peer(test_addr(8081)).await;
        let mut location = test_location(test_addr(9001));
        location.priority = RegistrationPriority::Immediate;
        location.vector_clock.increment(location.node_id);

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.gossip_sequence = 1;
            gossip_state
                .urgent_changes
                .push(RegistryChange::ActorAdded {
                    name: "urgent_only_actor".to_string(),
                    location,
                    priority: RegistrationPriority::Immediate,
                });
        }

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert_eq!(tasks.len(), 1);
        match &tasks[0].message {
            RegistryMessage::DeltaGossip { delta, .. } => {
                assert_eq!(delta.changes.len(), 1);
                match &delta.changes[0] {
                    RegistryChange::ActorAdded { priority, .. } => {
                        assert_eq!(*priority, RegistrationPriority::Normal);
                    }
                    other => panic!("expected ActorAdded, got {other:?}"),
                }
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
        registry
            .mark_inbound_connection_observed(test_addr(8081), test_addr(8081))
            .await;

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
    async fn delta_actor_admission_is_capped_globally_and_per_sender() {
        let mut config = test_config();
        config.max_known_actors = 4;
        config.max_known_actors_per_peer = 2;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        for (sender_index, port) in [(0, 8100), (1, 8101), (2, 8102)] {
            let sender = test_peer_id(&format!("actor-cap-sender-{sender_index}"));
            let sender_addr = test_addr(port);
            registry
                .connection_pool
                .set_configured_peer_addr(&sender, sender_addr);
            let changes = (0..5)
                .map(|actor_index| RegistryChange::ActorAdded {
                    name: format!("sender-{sender_index}-actor-{actor_index}"),
                    location: RemoteActorLocation::new_with_peer(sender_addr, sender.clone()),
                    priority: RegistrationPriority::Normal,
                })
                .collect();
            registry
                .apply_delta(RegistryDelta {
                    since_sequence: 0,
                    current_sequence: 1,
                    changes,
                    sender_peer_id: sender,
                    wall_clock_time: current_timestamp(),
                    precise_timing_nanos: crate::current_timestamp_nanos(),
                })
                .await
                .unwrap();
        }

        assert_eq!(registry.actor_state.known_actors.len(), 4);
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.peer_to_actors[&test_addr(8100)].len(), 2);
        assert_eq!(gossip_state.peer_to_actors[&test_addr(8101)].len(), 2);
        assert!(gossip_state.peer_to_actors[&test_addr(8102)].is_empty());
    }

    #[tokio::test]
    async fn delta_peer_cap_persists_without_address_bookkeeping() {
        let mut config = test_config();
        config.max_known_actors = 10;
        config.max_known_actors_per_peer = 2;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);
        let sender = test_peer_id("unmapped-actor-cap-sender");

        for batch in 0..3 {
            let changes = (0..2)
                .map(|actor_index| RegistryChange::ActorAdded {
                    name: format!("batch-{batch}-actor-{actor_index}"),
                    location: RemoteActorLocation::new_with_peer(test_addr(8300), sender.clone()),
                    priority: RegistrationPriority::Normal,
                })
                .collect();
            registry
                .apply_delta(RegistryDelta {
                    since_sequence: batch,
                    current_sequence: batch + 1,
                    changes,
                    sender_peer_id: sender.clone(),
                    wall_clock_time: current_timestamp(),
                    precise_timing_nanos: crate::current_timestamp_nanos(),
                })
                .await
                .unwrap();
        }

        assert_eq!(registry.actor_state.known_actors.len(), 2);
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.actor_admission_count(&sender), 2);
    }

    #[tokio::test]
    async fn local_actors_do_not_consume_remote_admission_capacity() {
        let mut config = test_config();
        config.max_known_actors = 1;
        config.max_known_actors_per_peer = 1;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);
        for index in 0..3 {
            registry
                .register_actor(
                    format!("local-{index}"),
                    test_location(test_addr(8500 + index)),
                )
                .await
                .unwrap();
        }

        let sender = test_peer_id("remote-cap-after-locals");
        registry
            .apply_delta(RegistryDelta {
                since_sequence: 0,
                current_sequence: 1,
                changes: vec![RegistryChange::ActorAdded {
                    name: "remote-after-locals".to_string(),
                    location: RemoteActorLocation::new_with_peer(test_addr(8600), sender.clone()),
                    priority: RegistrationPriority::Normal,
                }],
                sender_peer_id: sender,
                wall_clock_time: current_timestamp(),
                precise_timing_nanos: crate::current_timestamp_nanos(),
            })
            .await
            .unwrap();

        assert_eq!(registry.actor_state.local_actors.len(), 3);
        assert_eq!(registry.actor_state.known_actors.len(), 1);
        assert!(
            registry
                .actor_state
                .known_actors
                .contains_sync("remote-after-locals")
        );
    }

    #[tokio::test]
    async fn actor_update_transfers_per_peer_admission_charge() {
        let mut config = test_config();
        config.max_known_actors = 2;
        config.max_known_actors_per_peer = 1;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);
        let sender_a = test_peer_id("admission-owner-a");
        let sender_b = test_peer_id("admission-owner-b");
        let initial = RemoteActorLocation::new_with_peer(test_addr(8700), sender_a.clone());
        initial.vector_clock.increment(sender_a.to_node_id());
        registry
            .apply_delta(RegistryDelta {
                since_sequence: 0,
                current_sequence: 1,
                changes: vec![RegistryChange::ActorAdded {
                    name: "moved-actor".to_string(),
                    location: initial.clone(),
                    priority: RegistrationPriority::Normal,
                }],
                sender_peer_id: sender_a.clone(),
                wall_clock_time: current_timestamp(),
                precise_timing_nanos: crate::current_timestamp_nanos(),
            })
            .await
            .unwrap();

        let mut moved = RemoteActorLocation::new_with_peer(test_addr(8701), sender_b.clone());
        moved.vector_clock = initial.vector_clock.clone();
        moved.vector_clock.increment(sender_b.to_node_id());
        registry
            .apply_delta(RegistryDelta {
                since_sequence: 1,
                current_sequence: 2,
                changes: vec![RegistryChange::ActorAdded {
                    name: "moved-actor".to_string(),
                    location: moved,
                    priority: RegistrationPriority::Normal,
                }],
                sender_peer_id: sender_b.clone(),
                wall_clock_time: current_timestamp(),
                precise_timing_nanos: crate::current_timestamp_nanos(),
            })
            .await
            .unwrap();

        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.actor_admission_count(&sender_a), 0);
        assert_eq!(gossip_state.actor_admission_count(&sender_b), 1);
        assert_eq!(
            gossip_state.actor_admission_peer_by_name["moved-actor"],
            sender_b
        );
    }

    #[tokio::test]
    async fn full_sync_actor_admission_is_capped_globally_and_per_sender() {
        let mut config = test_config();
        config.max_known_actors = 4;
        config.max_known_actors_per_peer = 2;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        for (sender_index, port) in [(0, 8200), (1, 8201), (2, 8202)] {
            let sender = test_peer_id(&format!("full-sync-cap-sender-{sender_index}"));
            let sender_addr = test_addr(port);
            let actors = (0..5)
                .map(|actor_index| {
                    (
                        format!("full-sync-{sender_index}-actor-{actor_index}"),
                        RemoteActorLocation::new_with_peer(sender_addr, sender.clone()),
                    )
                })
                .collect();
            registry
                .merge_full_sync(
                    actors,
                    HashMap::new(),
                    sender,
                    sender_addr,
                    1,
                    current_timestamp(),
                )
                .await;
        }

        assert_eq!(registry.actor_state.known_actors.len(), 4);
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.peer_to_actors[&test_addr(8200)].len(), 2);
        assert_eq!(gossip_state.peer_to_actors[&test_addr(8201)].len(), 2);
        assert!(gossip_state.peer_to_actors[&test_addr(8202)].is_empty());
    }

    #[tokio::test]
    async fn rejected_full_sync_actor_does_not_create_phantom_peer_attribution() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        registry
            .register_actor("local-wins".to_string(), test_location(test_addr(8400)))
            .await
            .unwrap();
        let sender = test_peer_id("rejected-full-sync-sender");
        let sender_addr = test_addr(8401);
        let remote = HashMap::from([(
            "local-wins".to_string(),
            RemoteActorLocation::new_with_peer(sender_addr, sender.clone()),
        )]);

        registry
            .merge_full_sync(
                remote,
                HashMap::new(),
                sender,
                sender_addr,
                1,
                current_timestamp(),
            )
            .await;

        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peer_to_actors[&sender_addr].is_empty());
        assert!(
            !registry
                .actor_state
                .known_actors
                .contains_sync("local-wins")
        );
    }

    /// Duplicate immediate deltas must be observably idempotent.
    /// Regression for the devnet stratum trace where a single batch
    /// from sender `f4061522…` was delivered three times in ~130μs; the
    /// first delivery returned the immediate-priority names, the next two
    /// returned an empty list so the connection handler skipped the redundant
    /// acks.
    #[tokio::test]
    async fn apply_delta_returns_immediate_names_only_when_mutating_state() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        let make_delta = || RegistryDelta {
            since_sequence: 0,
            current_sequence: 0,
            changes: vec![RegistryChange::ActorAdded {
                name: "urgent_actor".to_string(),
                location: location.clone(),
                priority: RegistrationPriority::Immediate,
            }],
            sender_peer_id: test_peer_id("node_b"),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        let first = registry.apply_delta(make_delta()).await.unwrap();
        assert_eq!(first, vec!["urgent_actor".to_string()]);

        let second = registry.apply_delta(make_delta()).await.unwrap();
        assert!(
            second.is_empty(),
            "duplicate immediate delta must not re-emit ack names, got {second:?}"
        );

        let third = registry.apply_delta(make_delta()).await.unwrap();
        assert!(third.is_empty());
    }

    /// Normal-priority adds never appear in the immediate-ack return value,
    /// even on the first delivery.
    #[tokio::test]
    async fn apply_delta_excludes_non_immediate_priority_from_ack_list() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "normal_actor".to_string(),
                location: test_location(test_addr(9001)),
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: test_peer_id("node_b"),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        let acks = registry.apply_delta(delta).await.unwrap();
        assert!(
            acks.is_empty(),
            "normal priority must not be acked: {acks:?}"
        );
        assert!(
            registry
                .actor_state
                .known_actors
                .contains_sync("normal_actor")
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
            .read_sync("local_actor", |_, location| location.clone())
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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
            .handle_peer_connection_failure(test_addr(8081), None)
            .await
            .unwrap();

        // Check peer is marked as failed
        let gossip_state = registry.gossip_state.lock().await;
        let peer = gossip_state.peers.get(&test_addr(8081)).unwrap();
        assert_eq!(peer.failures, registry.config.max_peer_failures);
        assert!(peer.last_failure_time.is_some());
    }

    /// Bug 2 (P1): `mark_peer_disconnected`/`discovery.on_peer_disconnected`
    /// previously had zero production callers on the ordinary socket-failure/
    /// teardown path -- only `cleanup_peer_state`'s max_peers cap eviction
    /// ever cleared a peer's discovery `Connected` state. So every peer that
    /// ever connected stayed `Connected` forever, `connected_count_unified`
    /// only grew, and once it hit `max_peers` discovery permanently stopped
    /// admitting new gossip candidates even with zero live connections.
    /// `handle_peer_connection_failure` is the real teardown path (invoked
    /// from the IO task's exit handling in `stream_writer.rs` on every
    /// socket failure); it must clear the discovery slot for a genuine
    /// disconnect.
    #[tokio::test]
    async fn handle_peer_connection_failure_clears_discovery_connected_state() {
        let mut config = test_config_with_seed("discovery-disconnect-clear");
        config.enable_peer_discovery = true;
        config.max_peers = 1;
        let registry = GossipRegistry::<()>::new(test_addr(7902), config);
        let peer_addr = test_addr(9912);

        registry.add_peer(peer_addr).await;
        registry.mark_peer_connected(peer_addr).await;

        {
            let state = registry.gossip_state.lock().await;
            let discovery = state.peer_discovery.as_ref().unwrap();
            assert_eq!(discovery.connected_count_unified(), 1);
            assert_eq!(
                discovery.remaining_slots(),
                0,
                "at max_peers=1 the single connected slot must be exhausted"
            );
        }

        registry
            .handle_peer_connection_failure(peer_addr, None)
            .await
            .unwrap();

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.connected_count_unified(),
            0,
            "a real disconnect/teardown must clear the peer's discovery Connected \
             state, or the slot is exhausted forever"
        );
        assert_eq!(
            discovery.remaining_slots(),
            1,
            "the reclaimed slot must be admittable by a later gossip discovery \
             candidate for this same peer"
        );
    }

    /// Race-class guard (same capture-then-recheck discipline as #156's
    /// session-epoch mechanism, applied to discovery's own connect
    /// generation -- see `PeerDiscovery::connect_generation`):
    /// `handle_peer_connection_failure`'s discovery clear runs asynchronously,
    /// after the pool-teardown work that confirmed the failed connection was
    /// genuinely current -- with no `gossip_state` lock held across that gap.
    /// A replacement connection for the SAME address can call
    /// `mark_peer_connected` (re-marking discovery `Connected`) in that exact
    /// gap, with NO intervening disconnect notification at all -- this
    /// drives the REAL production interleaving: discovery still shows the
    /// address `Connected` (from the old connection) the entire time, and
    /// only the connection-instance token distinguishes the replacement
    /// from a redundant re-mark of the connection that's about to fail.
    /// The clear must decline, or it would wipe out the replacement's
    /// legitimate `Connected` state and undercount a still-live peer.
    ///
    /// This drives the two real, production-used steps
    /// (`capture_pre_failure_discovery_generation`, then
    /// `clear_discovery_state_if_generation_unchanged`) in the exact
    /// problem order -- capture, THEN a replacement connects -- which is the
    /// only way to deterministically reproduce a gap that `.await` scheduling
    /// alone cannot reliably force in a unit test.
    #[tokio::test]
    async fn discovery_clear_declines_when_a_replacement_session_armed_after_capture() {
        let mut config = test_config_with_seed("discovery-disconnect-race");
        config.enable_peer_discovery = true;
        config.max_peers = 1;
        let registry = GossipRegistry::<()>::new(test_addr(7903), config);
        let peer_addr = test_addr(9913);
        let owner = test_peer_id("discovery-disconnect-race-peer");
        let owner_node = owner.to_node_id();

        // The original (about-to-fail) connection instance: armed session,
        // discovery Connected.
        registry.add_peer_with_node_id(peer_addr, Some(owner_node)).await;
        let old_conn = publish_connected_instance(&registry, &owner, peer_addr).await;
        let old_session = test_addr(55201);
        registry
            .arm_sequence_reset_for_new_session(peer_addr, owner_node, old_session, &owner, &old_conn)
            .await;
        registry.mark_peer_connected(peer_addr).await;

        // Exactly what `handle_peer_connection_failure` captures at entry,
        // before any teardown mutation.
        let pre_failure_epoch = registry
            .capture_pre_failure_discovery_generation(peer_addr)
            .await;

        // A REPLACEMENT connection instance connects at the SAME address --
        // in the gap between that capture and the eventual discovery clear
        // -- with NO intervening disconnect notification of any kind.
        // Discovery shows the address `Connected` continuously; only the
        // instance token differs.
        let new_conn = publish_connected_instance(&registry, &owner, peer_addr).await;
        let new_session = test_addr(55202);
        registry
            .arm_sequence_reset_for_new_session(peer_addr, owner_node, new_session, &owner, &new_conn)
            .await;
        registry.mark_peer_connected(peer_addr).await;

        {
            let state = registry.gossip_state.lock().await;
            let discovery = state.peer_discovery.as_ref().unwrap();
            assert_eq!(discovery.connected_count_unified(), 1);
        }

        // The stale failure report's discovery clear must decline: a newer
        // connection instance is now current for this address.
        registry
            .clear_discovery_state_if_generation_unchanged(peer_addr, pre_failure_epoch)
            .await;

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.connected_count_unified(),
            1,
            "a replacement connection instance must keep owning discovery's Connected state \
             even with no intervening disconnect notification; a stale failure report must \
             not clear it"
        );
        assert_eq!(
            discovery.get_peer_state(&peer_addr),
            Some(&crate::peer_discovery::PeerState::Connected),
            "the replacement's Connected state must survive the stale clear attempt"
        );
    }

    /// The discovery clear's guard must NOT be a plain
    /// `PeerInfo::current_session_epoch` comparison: `mark_peer_connected`
    /// (what actually flips discovery's `Connected` state) can run WITHOUT
    /// any session-epoch change at all -- e.g. a second successful connect
    /// for the same address whose identity never re-armed a TLS session, or
    /// simply a redundant re-confirmation. If the guard only compared
    /// session epoch, this replacement `Connected` transition would be
    /// invisible to it and a stale failure report would incorrectly wipe it
    /// out. The discovery-specific connect generation
    /// (`PeerDiscovery::connect_generation`) catches it because it bumps on
    /// EVERY `on_peer_connected` call, independent of session epoch.
    #[tokio::test]
    async fn discovery_clear_declines_on_replacement_connect_even_with_unchanged_session_epoch() {
        let mut config = test_config_with_seed("discovery-disconnect-race-no-epoch-change");
        config.enable_peer_discovery = true;
        config.max_peers = 1;
        let registry = GossipRegistry::<()>::new(test_addr(7907), config);
        let peer_addr = test_addr(9917);
        let peer_id = test_peer_id("discovery-disconnect-race-no-epoch-change-peer");

        registry.add_peer(peer_addr).await;
        let _old_conn = publish_connected_instance(&registry, &peer_id, peer_addr).await;
        registry.mark_peer_connected(peer_addr).await;

        let epoch_before = {
            let state = registry.gossip_state.lock().await;
            state.peers.get(&peer_addr).unwrap().current_session_epoch
        };

        // Exactly what `handle_peer_connection_failure` captures at entry,
        // before any teardown mutation.
        let pre_failure_generation = registry
            .capture_pre_failure_discovery_generation(peer_addr)
            .await;

        // A REPLACEMENT connection instance connects for the SAME address --
        // no TLS session is ever armed for it (no
        // `arm_sequence_reset_for_new_session` call at all), so the session
        // epoch does not change -- and with NO intervening disconnect
        // notification either. Only the connection-instance token differs.
        let _new_conn = publish_connected_instance(&registry, &peer_id, peer_addr).await;
        registry.mark_peer_connected(peer_addr).await;

        let epoch_after = {
            let state = registry.gossip_state.lock().await;
            state.peers.get(&peer_addr).unwrap().current_session_epoch
        };
        assert_eq!(
            epoch_before, epoch_after,
            "sanity: the replacement connect must NOT have changed the session epoch"
        );

        // The stale failure report's discovery clear must still decline:
        // the connect-generation guard catches the replacement even though
        // the session epoch alone would have looked unchanged too.
        registry
            .clear_discovery_state_if_generation_unchanged(peer_addr, pre_failure_generation)
            .await;

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.connected_count_unified(),
            1,
            "a replacement connect must keep owning discovery's Connected state even when \
             it never changed the session epoch; a stale failure report must not clear it"
        );
    }

    /// `on_peer_connected` carries no connection-instance identity -- just an
    /// address -- so a REDUNDANT re-confirmation of an address that is
    /// already `Connected` must NOT advance the connect generation. If it
    /// did, a teardown reported for that exact still-live socket, landing in
    /// the gap between a failure's generation capture and the discovery
    /// clear, would see a bumped generation it never actually raced against
    /// and wrongly decline -- permanently stranding the slot as `Connected`
    /// with no live connection behind it at all. A genuine disconnect after
    /// a redundant re-confirmation must still clear normally.
    #[tokio::test]
    async fn redundant_connect_does_not_bump_generation_and_genuine_disconnect_still_clears() {
        let mut config = test_config_with_seed("discovery-redundant-connect");
        config.enable_peer_discovery = true;
        config.max_peers = 1;
        let registry = GossipRegistry::<()>::new(test_addr(7908), config);
        let peer_addr = test_addr(9918);

        registry.add_peer(peer_addr).await;
        registry.mark_peer_connected(peer_addr).await;

        let generation_after_first_connect = registry
            .capture_pre_failure_discovery_generation(peer_addr)
            .await;
        assert!(generation_after_first_connect.is_some());

        // A REDUNDANT re-confirmation of the SAME still-Connected address --
        // e.g. a second status notification for the identical live socket,
        // not a new connection.
        registry.mark_peer_connected(peer_addr).await;

        let generation_after_redundant_connect = registry
            .capture_pre_failure_discovery_generation(peer_addr)
            .await;
        assert_eq!(
            generation_after_first_connect, generation_after_redundant_connect,
            "a redundant Connected -> Connected re-mark must not advance the connect generation"
        );

        // The genuine teardown of that exact socket, using the generation
        // captured back at the FIRST connect (exactly what a real failure
        // handler would have captured before this redundant re-mark ever
        // happened).
        registry
            .clear_discovery_state_if_generation_unchanged(peer_addr, generation_after_first_connect)
            .await;

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.connected_count_unified(),
            0,
            "a genuine disconnect must still clear the slot even though a redundant connect \
             notification was recorded for the same address in between"
        );
        assert_eq!(discovery.remaining_slots(), 1);
    }

    /// The discovery clear must be a NO-OP -- not a clear -- when the
    /// captured generation was `None` (the address was `Pending`/`Failed`/
    /// untracked, never `Connected`, at failure-detection time).
    /// `on_peer_disconnected` removes whatever unified state DOES exist
    /// unconditionally, regardless of variant; calling it here for a
    /// `Failed` peer would discard its backoff state, letting an immediate
    /// retry bypass the backoff the peer legitimately earned, for a report
    /// that was never about a `Connected` state at all.
    #[tokio::test]
    async fn discovery_clear_does_not_disturb_a_failed_peers_backoff_state() {
        let mut config = test_config_with_seed("discovery-none-noop-failed");
        config.enable_peer_discovery = true;
        let registry = GossipRegistry::<()>::new(test_addr(7909), config);
        let peer_addr = test_addr(9919);

        registry.mark_peer_failed(peer_addr).await;

        let failed_state_before = {
            let state = registry.gossip_state.lock().await;
            let discovery = state.peer_discovery.as_ref().unwrap();
            let captured = discovery.connect_generation(&peer_addr);
            assert_eq!(
                captured, None,
                "sanity: a Failed peer must have no discovery connect generation"
            );
            discovery.get_peer_state(&peer_addr).cloned()
        };
        assert!(
            matches!(failed_state_before, Some(crate::peer_discovery::PeerState::Failed { .. })),
            "sanity: the peer must be in the Failed state before the clear"
        );

        // `None` captured -- exactly what a failure report for a peer that
        // was never `Connected` would produce.
        registry
            .clear_discovery_state_if_generation_unchanged(peer_addr, None)
            .await;

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.get_peer_state(&peer_addr),
            failed_state_before.as_ref(),
            "a None-captured clear must not disturb a Failed peer's backoff state at all"
        );
    }

    /// The end-to-end version of the previous test: a peer that WAS
    /// genuinely `Connected` (generation captured while `Connected`, exactly
    /// like a real failure handler would) later fails and accumulates
    /// backoff via `on_peer_failure`, all before the stale, Connected-era
    /// generation is ever used in a clear attempt. `connect_generation` must
    /// read back `None` once the peer is no longer `Connected` (the
    /// accessor's own `peer_states` check, not merely a tidied-up map), and
    /// the clear must leave the peer's `Failed` backoff state completely
    /// untouched.
    #[tokio::test]
    async fn discovery_clear_does_not_wipe_backoff_after_a_connected_peer_later_fails() {
        let mut config = test_config_with_seed("discovery-connected-then-failed");
        config.enable_peer_discovery = true;
        let registry = GossipRegistry::<()>::new(test_addr(7910), config);
        let peer_addr = test_addr(9920);

        registry.add_peer(peer_addr).await;
        registry.mark_peer_connected(peer_addr).await;

        // Exactly what a real failure handler captures at entry, while the
        // peer is still genuinely Connected.
        let pre_failure_generation = registry
            .capture_pre_failure_discovery_generation(peer_addr)
            .await;
        assert!(pre_failure_generation.is_some());

        // The connection fails and discovery independently transitions the
        // peer to Failed, accumulating backoff -- before the stale
        // generation captured above is ever used.
        registry.mark_peer_failed(peer_addr).await;

        let failed_state_before = {
            let state = registry.gossip_state.lock().await;
            let discovery = state.peer_discovery.as_ref().unwrap();
            assert_eq!(
                discovery.connect_generation(&peer_addr),
                None,
                "connect_generation must read back None once the peer is no longer Connected, \
                 regardless of what the underlying map still holds"
            );
            discovery.get_peer_state(&peer_addr).cloned()
        };
        assert!(
            matches!(failed_state_before, Some(crate::peer_discovery::PeerState::Failed { .. })),
            "sanity: the peer must be in the Failed state before the clear"
        );

        // The stale, Connected-era generation must not resurrect a clear
        // here: the address is no longer Connected at all.
        registry
            .clear_discovery_state_if_generation_unchanged(peer_addr, pre_failure_generation)
            .await;

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.get_peer_state(&peer_addr),
            failed_state_before.as_ref(),
            "a stale Connected-era generation must not disturb the peer's Failed backoff \
             state once it has genuinely transitioned away from Connected"
        );
    }

    /// Edge case: `mark_peer_connected` notifies discovery unconditionally,
    /// even when `gossip_state.peers` has no entry for the address at all
    /// (and an entry can also disappear concurrently, e.g. peer-table
    /// eviction). Discovery itself still shows the address `Connected` (with
    /// its own `Some(generation)`) in this case, so the clear must still
    /// reclaim the slot on a genuine disconnect, entirely independent of
    /// whether `gossip_state.peers` ever had an entry at all.
    #[tokio::test]
    async fn handle_peer_connection_failure_clears_discovery_state_with_no_peers_entry() {
        let mut config = test_config_with_seed("discovery-disconnect-no-entry");
        config.enable_peer_discovery = true;
        config.max_peers = 1;
        let registry = GossipRegistry::<()>::new(test_addr(7904), config);
        let peer_addr = test_addr(9914);

        // Discovery Connected with NO corresponding `gossip_state.peers`
        // entry for this address.
        registry.mark_peer_connected(peer_addr).await;

        {
            let state = registry.gossip_state.lock().await;
            assert!(
                !state.peers.contains_key(&peer_addr),
                "sanity: no gossip_state.peers entry was ever created for this address"
            );
            let discovery = state.peer_discovery.as_ref().unwrap();
            assert_eq!(discovery.connected_count_unified(), 1);
        }

        registry
            .handle_peer_connection_failure(peer_addr, None)
            .await
            .unwrap();

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.connected_count_unified(),
            0,
            "a genuine disconnect (no replacement) for a peer with no gossip_state.peers \
             entry must still clear discovery's Connected state, or the slot is exhausted \
             forever"
        );
        assert_eq!(discovery.remaining_slots(), 1);
    }

    /// The peer-ID-keyed failure path (`handle_peer_connection_failure_by_peer_id`)
    /// tears down a peer's pool connection and marks it failed exactly like
    /// the address-keyed path, so it must reclaim the discovery slot the
    /// same way -- a genuine disconnect with no replacement clears it.
    #[tokio::test]
    async fn handle_peer_connection_failure_by_peer_id_clears_discovery_connected_state() {
        let mut config = test_config_with_seed("discovery-disconnect-clear-by-id");
        config.enable_peer_discovery = true;
        config.max_peers = 1;
        let registry = GossipRegistry::<()>::new(test_addr(7905), config);
        let peer_addr = test_addr(9915);
        let peer_id = test_peer_id("discovery-disconnect-clear-by-id-peer");

        registry
            .connection_pool
            .set_configured_peer_addr(&peer_id, peer_addr);
        registry.add_peer(peer_addr).await;
        registry.mark_peer_connected(peer_addr).await;

        {
            let state = registry.gossip_state.lock().await;
            let discovery = state.peer_discovery.as_ref().unwrap();
            assert_eq!(discovery.connected_count_unified(), 1);
            assert_eq!(discovery.remaining_slots(), 0);
        }

        registry
            .handle_peer_connection_failure_by_peer_id(&peer_id)
            .await
            .unwrap();

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.connected_count_unified(),
            0,
            "a genuine disconnect via the peer-ID path must clear discovery's Connected \
             state, or the slot is exhausted forever"
        );
        assert_eq!(discovery.remaining_slots(), 1);
    }

    /// Same race-class guard as
    /// `discovery_clear_declines_when_a_replacement_session_armed_after_capture`,
    /// but driving the peer-ID-keyed failure path: a replacement connection
    /// that armed a fresh session for this identity between the capture and
    /// the clear must keep owning discovery's `Connected` state.
    #[tokio::test]
    async fn discovery_clear_declines_when_a_replacement_session_armed_after_capture_via_peer_id_path()
     {
        let mut config = test_config_with_seed("discovery-disconnect-race-by-id");
        config.enable_peer_discovery = true;
        config.max_peers = 1;
        let registry = GossipRegistry::<()>::new(test_addr(7906), config);
        let peer_addr = test_addr(9916);
        let owner = test_peer_id("discovery-disconnect-race-by-id-peer");
        let owner_node = owner.to_node_id();

        registry
            .connection_pool
            .set_configured_peer_addr(&owner, peer_addr);
        registry.add_peer_with_node_id(peer_addr, Some(owner_node)).await;
        let old_conn = publish_connected_instance(&registry, &owner, peer_addr).await;
        let old_session = test_addr(55301);
        registry
            .arm_sequence_reset_for_new_session(peer_addr, owner_node, old_session, &owner, &old_conn)
            .await;
        registry.mark_peer_connected(peer_addr).await;

        // Exactly what `handle_peer_connection_failure_by_peer_id` captures
        // at entry, before any teardown mutation.
        let pre_failure_epoch = registry
            .capture_pre_failure_discovery_generation(peer_addr)
            .await;

        // A REPLACEMENT connection instance connects for the SAME address,
        // in the gap between that capture and the eventual discovery clear
        // -- with NO intervening disconnect notification. Discovery shows
        // the address `Connected` continuously; only the instance token
        // differs.
        let new_conn = publish_connected_instance(&registry, &owner, peer_addr).await;
        let new_session = test_addr(55302);
        registry
            .arm_sequence_reset_for_new_session(peer_addr, owner_node, new_session, &owner, &new_conn)
            .await;
        registry.mark_peer_connected(peer_addr).await;

        {
            let state = registry.gossip_state.lock().await;
            let discovery = state.peer_discovery.as_ref().unwrap();
            assert_eq!(discovery.connected_count_unified(), 1);
        }

        // The stale failure report's discovery clear must decline: a newer
        // session is now current for this identity.
        registry
            .clear_discovery_state_if_generation_unchanged(peer_addr, pre_failure_epoch)
            .await;

        let state = registry.gossip_state.lock().await;
        let discovery = state.peer_discovery.as_ref().unwrap();
        assert_eq!(
            discovery.connected_count_unified(),
            1,
            "a replacement connection that already armed a fresh session must keep \
             owning discovery's Connected state; a stale failure report via the \
             peer-ID path must not clear it"
        );
        assert_eq!(
            discovery.get_peer_state(&peer_addr),
            Some(&crate::peer_discovery::PeerState::Connected)
        );
    }

    /// RED (thrash repro, collateral teardown): a socket failure reported for
    /// a *superseded* connection instance must never tear down the peer's
    /// current live session. This is the address-vs-identity defect: the
    /// failure handler resolved identity then blanket-`disconnect_connection_by_peer_id`'d,
    /// removing whatever was current — which, after a restart-into-live-peer,
    /// is the freshly-accepted preferred inbound, not the connection that died.
    #[tokio::test]
    async fn socket_failure_of_superseded_connection_preserves_current_session() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9100), test_config());
        let peer_id = test_peer_id("collateral_peer");
        let cur_addr = test_addr(9101);
        let stale_addr = test_addr(9102);
        let pool = &registry.connection_pool;
        pool.set_configured_peer_addr(&peer_id, cur_addr);

        // Current, preferred, live session.
        let (io2, _p2) = tokio::io::duplex(1024);
        let (sh2, _w2, _r2) = LockFreeStreamHandle::new(
            io2,
            cur_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn2 = LockFreeConnection::new(cur_addr, ConnectionDirection::Inbound);
        conn2.stream_handle = Some(Arc::new(sh2));
        conn2.embedded_peer_id = Some(peer_id.clone());
        conn2.set_state(ConnectionState::Connected);
        let conn2 = Arc::new(conn2);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), cur_addr, conn2.clone()));

        // A superseded connection instance for the SAME identity at a different
        // address, still aliased in the addr indices (as after a reconnect).
        let (io1, _p1) = tokio::io::duplex(1024);
        let (sh1, _w1, _r1) = LockFreeStreamHandle::new(
            io1,
            stale_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn1 = LockFreeConnection::new(stale_addr, ConnectionDirection::Outbound);
        conn1.stream_handle = Some(Arc::new(sh1));
        conn1.embedded_peer_id = Some(peer_id.clone());
        conn1.set_state(ConnectionState::Connected);
        let conn1 = Arc::new(conn1);
        pool.index_connection_by_addr(stale_addr, conn1.clone());
        pool.add_addr_to_peer_id(stale_addr, peer_id.clone());

        let before = pool
            .get_connection_by_peer_id(&peer_id)
            .expect("current session must resolve to conn2");
        assert!(Arc::ptr_eq(&before, &conn2));

        let conn1_instance_id = conn1
            .stream_handle
            .as_ref()
            .map(|h| h.instance_id())
            .expect("conn1 must have a stream handle");

        // The superseded connection's socket fails; the caller (the exit
        // guard) identifies it by its OWN instance id, never by re-resolving
        // `stale_addr`.
        registry
            .handle_peer_connection_failure(stale_addr, Some(conn1_instance_id))
            .await
            .unwrap();

        let after = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            after.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn2)),
            "socket failure of a superseded connection collaterally tore down the live \
             current session (thrash: disconnect_connection_by_peer_id is peer-wide, not \
             instance-scoped)"
        );
        assert!(
            pool.get_lock_free_connection(stale_addr).is_none(),
            "the superseded instance itself must still be retired from connections_by_addr"
        );
    }

    /// RED (thrash repro, SAME-bind-address collision): the precise defect
    /// underlying `socket_failure_of_superseded_connection_preserves_current_session`
    /// is not merely "different addresses" but a fresh connection reindexed
    /// under the EXACT SAME bind address an older, now-superseded link used.
    /// An old OUTBOUND connection to a peer's bind address `B` fails AFTER a
    /// fresh INBOUND for the same identity has already been reindexed under
    /// that same `B`. Resolving the failed socket "by address" — looking `B`
    /// up in `connections_by_addr` — returns the NEW current connection, not
    /// the one whose IO task actually exited, so an address-only check
    /// concludes `failed_is_current == true` and tears down the healthy
    /// replacement. Only comparing the failure callback's own captured
    /// instance identity against the current session's instance identity
    /// tells the two apart.
    #[tokio::test]
    async fn socket_failure_of_old_outbound_at_reused_bind_addr_preserves_fresh_inbound() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9300), test_config());
        let peer_id = test_peer_id("reused_bind_addr_peer");
        let bind_addr = test_addr(9301);
        let pool = &registry.connection_pool;
        pool.set_configured_peer_addr(&peer_id, bind_addr);

        // Old, now-superseded OUTBOUND connection instance, originally
        // indexed at the peer's bind address `bind_addr`.
        let (io1, _p1) = tokio::io::duplex(1024);
        let (sh1, _w1, _r1) = LockFreeStreamHandle::new(
            io1,
            bind_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn1 = LockFreeConnection::new(bind_addr, ConnectionDirection::Outbound);
        conn1.stream_handle = Some(Arc::new(sh1));
        conn1.embedded_peer_id = Some(peer_id.clone());
        conn1.set_state(ConnectionState::Connected);
        let conn1 = Arc::new(conn1);
        let conn1_instance_id = conn1
            .stream_handle
            .as_ref()
            .map(|h| h.instance_id())
            .expect("conn1 must have a stream handle");
        pool.index_connection_by_addr(bind_addr, conn1.clone());
        pool.add_addr_to_peer_id(bind_addr, peer_id.clone());

        // A fresh INBOUND session for the SAME identity arrives and is
        // reindexed under the SAME `bind_addr`, becoming the peer's current
        // connection — `connections_by_addr[bind_addr]` now points at conn2,
        // not conn1, even though conn1's IO task is still what is about to
        // fail.
        let (io2, _p2) = tokio::io::duplex(1024);
        let (sh2, _w2, _r2) = LockFreeStreamHandle::new(
            io2,
            bind_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn2 = LockFreeConnection::new(bind_addr, ConnectionDirection::Inbound);
        conn2.stream_handle = Some(Arc::new(sh2));
        conn2.embedded_peer_id = Some(peer_id.clone());
        conn2.set_state(ConnectionState::Connected);
        let conn2 = Arc::new(conn2);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), bind_addr, conn2.clone()));

        let before = pool
            .get_connection_by_peer_id(&peer_id)
            .expect("current session must resolve to conn2");
        assert!(Arc::ptr_eq(&before, &conn2));
        assert!(
            Arc::ptr_eq(
                &pool
                    .get_lock_free_connection(bind_addr)
                    .expect("bind_addr must resolve to the reindexed connection"),
                &conn2
            ),
            "test precondition: bind_addr must now resolve to the fresh inbound, not conn1"
        );

        // conn1's IO task exits and reports failure for `bind_addr` — the
        // address it was originally dialed to — identifying itself by its
        // OWN captured instance id, never by re-resolving `bind_addr`.
        registry
            .handle_peer_connection_failure(bind_addr, Some(conn1_instance_id))
            .await
            .unwrap();

        let after = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            after.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn2)),
            "a stale outbound's socket failure at a bind address since reused by a fresh \
             inbound must never tear down the healthy current session (address-only \
             resolution collateral teardown)"
        );
        assert!(
            Arc::ptr_eq(
                &pool
                    .get_lock_free_connection(bind_addr)
                    .expect("bind_addr must still resolve to conn2 after the stale failure"),
                &conn2
            ),
            "connections_by_addr[bind_addr] must still point at the fresh inbound, never \
             be clobbered or removed by the stale outbound's failure handling"
        );
    }

    /// RED (P2 finding): in the same-bind-address restart case exercised by
    /// `socket_failure_of_old_outbound_at_reused_bind_addr_preserves_fresh_inbound`,
    /// the superseded-instance branch retires the failed instance ONLY via
    /// `remove_connection_instance_by_id(observed_peer_addr, failed_id)`. That
    /// call decrements `connection_counter` exactly when it finds-and-removes
    /// the instance at `observed_peer_addr` — but by the time the OLD
    /// instance's socket failure is reported, a fresh reconnect has already
    /// overwritten that address slot, so the lookup finds nothing, returns
    /// `None`, and decrements nothing. The old instance WAS counted when it
    /// was originally published (`add_connection_by_peer_id` bumps the
    /// counter), so without a compensating decrement its contribution leaks
    /// permanently: every sequential same-address failover (reconnect, then
    /// the stale link's socket finally reports failure) leaks one more count
    /// even though exactly one session is ever live. Left unfixed this
    /// eventually starves the pool's admission gate
    /// (`add_lock_free_connection`'s `connection_count >= max_connections`
    /// check) despite the real connection set never growing.
    #[tokio::test]
    async fn superseded_same_addr_failover_does_not_leak_connection_counter() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9400), test_config());
        let peer_id = test_peer_id("counter_leak_peer");
        let addr = test_addr(9401);
        let pool = &registry.connection_pool;

        async fn spawn_live_connection(
            addr: SocketAddr,
            direction: ConnectionDirection,
            peer_id: &crate::PeerId,
        ) -> (Arc<LockFreeConnection>, u64) {
            let (io, _keep) = tokio::io::duplex(1024);
            let (sh, _w, _r) = LockFreeStreamHandle::new(
                io,
                addr,
                ChannelId::Global,
                BufferConfig::default(),
                None,
                None,
            );
            let mut conn = LockFreeConnection::new(addr, direction);
            let instance_id = sh.instance_id();
            conn.stream_handle = Some(Arc::new(sh));
            conn.embedded_peer_id = Some(peer_id.clone());
            conn.set_state(ConnectionState::Connected);
            (Arc::new(conn), instance_id)
        }

        // Establish the initial live session, counted via
        // `add_connection_by_peer_id` exactly like a real accepted/finalized
        // connection.
        let (initial, _initial_instance) =
            spawn_live_connection(addr, ConnectionDirection::Outbound, &peer_id).await;
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, initial.clone()));

        let baseline = pool.raw_connection_counter_signed();
        assert_eq!(
            baseline, 1,
            "test precondition: exactly one counted, live session"
        );

        let mut old_instance_id = pool
            .get_connection_by_peer_id(&peer_id)
            .and_then(|c| c.stream_handle.as_ref().map(|h| h.instance_id()))
            .expect("initial session must have a live stream handle");

        const FAILOVERS: usize = 4;
        for i in 0..FAILOVERS {
            // A fresh reconnect at the SAME bind address, reindexed and
            // published as current exactly like a real accept/finalize —
            // this overwrites `connections_by_addr[addr]`, displacing the
            // previous instance from the index entirely.
            let direction = if i % 2 == 0 {
                ConnectionDirection::Inbound
            } else {
                ConnectionDirection::Outbound
            };
            let (fresh, fresh_instance_id) = spawn_live_connection(addr, direction, &peer_id).await;
            assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, fresh.clone()));

            let current = pool.get_connection_by_peer_id(&peer_id);
            assert!(
                current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
                "fresh reconnect #{i} must become the peer's current session"
            );

            // The OLD instance's socket now reports failure, identified by
            // its own captured instance id — it is no longer reachable via
            // `addr` at all, having just been displaced above.
            registry
                .handle_peer_connection_failure(addr, Some(old_instance_id))
                .await
                .unwrap();

            let after = pool.get_connection_by_peer_id(&peer_id);
            assert!(
                after.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
                "the superseded instance's failure must never disturb the live current \
                 session, failover #{i}"
            );

            old_instance_id = fresh_instance_id;
        }

        let final_count = pool.raw_connection_counter_signed();
        assert_eq!(
            final_count,
            baseline,
            "connection_counter must return to baseline ({baseline}) after {FAILOVERS} \
             sequential same-address failovers with exactly one live session throughout — \
             got {final_count} (leaked {} if unfixed)",
            final_count.saturating_sub(baseline)
        );
    }

    /// RED (P1 primary finding): a socket failure whose `failed_instance_id`
    /// matches the CURRENT session's own instance id must retire that
    /// instance by CAS'd identity (`disconnect_connection_instance`), never
    /// fall through to the peer-wide `disconnect_connection_by_peer_id`. A
    /// fresh session for the same peer published in the gap between the
    /// instance-id match and a peer-wide teardown must survive — only the
    /// failed instance may be retired.
    ///
    /// Pinned deterministically via `set_transport_lifecycle_recorder`:
    /// both `disconnect_connection_by_peer_id` (buggy, peer-wide) and
    /// `disconnect_connection_instance` (fixed, CAS-scoped) fire a
    /// `SessionRemoved { reason: DisconnectByPeerId }` event for this peer.
    /// The peer-wide path fires it BEFORE its unconditional
    /// `clear_current_peer_connection` store and peer-id-keyed address-alias
    /// sweep — publishing a fresh session from inside this hook lands
    /// exactly in that check-then-act gap and gets clobbered. The CAS path
    /// fires the same event only AFTER its atomic compare-and-clear has
    /// already completed, so the identical publish lands safely afterward.
    #[tokio::test]
    async fn socket_failure_matched_instance_teardown_is_instance_scoped_not_peer_wide() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9400), test_config());
        let peer_id = test_peer_id("matched_instance_peer");
        let old_addr = test_addr(9401);
        let fresh_addr = test_addr(9402);
        let pool = registry.connection_pool.clone();
        pool.set_configured_peer_addr(&peer_id, old_addr);

        // Current, live session whose IO task is about to fail.
        let (io_old, _p_old) = tokio::io::duplex(1024);
        let (sh_old, _w_old, _r_old) = LockFreeStreamHandle::new(
            io_old,
            old_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_old = LockFreeConnection::new(old_addr, ConnectionDirection::Outbound);
        conn_old.stream_handle = Some(Arc::new(sh_old));
        conn_old.embedded_peer_id = Some(peer_id.clone());
        conn_old.set_state(ConnectionState::Connected);
        let conn_old = Arc::new(conn_old);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), old_addr, conn_old.clone()));

        let old_instance_id = conn_old
            .stream_handle
            .as_ref()
            .map(|h| h.instance_id())
            .expect("conn_old must have a stream handle");

        // The FRESH replacement session a concurrent inbound publishes for
        // the same peer identity while `conn_old`'s teardown is in flight —
        // models the publish landing in the match-then-disconnect gap.
        let (io_fresh, _p_fresh) = tokio::io::duplex(1024);
        let (sh_fresh, _w_fresh, _r_fresh) = LockFreeStreamHandle::new(
            io_fresh,
            fresh_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_fresh = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
        conn_fresh.stream_handle = Some(Arc::new(sh_fresh));
        conn_fresh.embedded_peer_id = Some(peer_id.clone());
        conn_fresh.set_state(ConnectionState::Connected);
        let conn_fresh = Arc::new(conn_fresh);

        let _guard = {
            let pool = pool.clone();
            let peer_id = peer_id.clone();
            let conn_fresh = conn_fresh.clone();
            crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
                if let crate::TransportLifecycleEvent::SessionRemoved {
                    peer,
                    reason: crate::SessionRemovalReason::DisconnectByPeerId,
                    ..
                } = &event
                    && *peer == peer_id
                {
                    // Deregister first: the nested `publish_current_peer_connection`
                    // call below fires its own (non-matching) `SessionPublished`
                    // event through this same global hook, and this avoids any
                    // reentrant/recursive invocation of this closure.
                    crate::set_transport_lifecycle_recorder(None);
                    pool.publish_current_peer_connection(&peer_id, conn_fresh.clone());
                }
            }))
        };

        // conn_old's IO task exits and reports failure, identifying itself
        // by its OWN captured instance id — which matches the current
        // session at the moment the handler makes its decision.
        registry
            .handle_peer_connection_failure(old_addr, Some(old_instance_id))
            .await
            .unwrap();

        let after = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            after.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn_fresh)),
            "a fresh session published from inside the matched-instance teardown's \
             check-then-act gap must survive — retiring the failed instance must never \
             fall through to a peer-wide disconnect that clobbers it (got {after:?})"
        );
        assert!(
            pool.connections_by_peer
                .read_sync(&peer_id, |_, v| Arc::ptr_eq(v, &conn_fresh))
                .unwrap_or(false),
            "`connections_by_peer` must still point at the fresh instance after the matched \
             failed instance is retired"
        );
    }

    /// RED (review finding, matched-instance CAS-LOSS fallback): when
    /// `failed_instance_id` matches the CURRENT session's own instance id,
    /// but a FRESH session for the same peer is published in the gap
    /// between that match and `disconnect_connection_instance`'s own CAS —
    /// so the CAS itself observably LOSES (`retired == false`) — the fresh
    /// session must survive as current AND the old, now-orphaned failed
    /// instance must be fully retired by its own identity: no lingering
    /// `connections_by_addr` alias, and its `connection_counter`
    /// contribution released so it does not leak. At HEAD, `retired == false`
    /// was treated identically to a successful retirement
    /// (`instance_teardown_done = true` unconditionally) with no fallback
    /// cleanup at all, leaving the old instance's address alias indexed
    /// forever and its counter contribution permanently leaked.
    ///
    /// Pinned deterministically via `set_transport_lifecycle_recorder` on
    /// `SocketFailureMatchedInstanceTeardownAttempt`, which fires
    /// immediately before the matched-instance branch's
    /// `disconnect_connection_instance` CAS attempt — publishing the fresh
    /// session from inside that hook lands it in the exact gap between the
    /// instance-id match and the CAS, guaranteeing the CAS loses on every
    /// run.
    ///
    /// RED at HEAD: a `connections_by_addr` entry for `old_addr` still
    /// points at `conn_old` (zombie alias) and/or `connection_counter`
    /// stays inflated above the correct single-live-session baseline. GREEN
    /// after the fix: no alias for `conn_old` remains, and the counter
    /// returns to exactly one live session.
    #[tokio::test]
    async fn socket_failure_matched_instance_cas_loss_retires_failed_instance_without_leak() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9400), test_config());
        let peer_id = test_peer_id("matched_instance_cas_loss_peer");
        let old_addr = test_addr(9401);
        let fresh_addr = test_addr(9402);
        let pool = registry.connection_pool.clone();
        pool.set_configured_peer_addr(&peer_id, old_addr);

        // Current, live session whose IO task is about to fail.
        let (io_old, _p_old) = tokio::io::duplex(1024);
        let (sh_old, _w_old, _r_old) = LockFreeStreamHandle::new(
            io_old,
            old_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_old = LockFreeConnection::new(old_addr, ConnectionDirection::Outbound);
        conn_old.stream_handle = Some(Arc::new(sh_old));
        conn_old.embedded_peer_id = Some(peer_id.clone());
        conn_old.set_state(ConnectionState::Connected);
        let conn_old = Arc::new(conn_old);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), old_addr, conn_old.clone()));

        let old_instance_id = conn_old
            .stream_handle
            .as_ref()
            .map(|h| h.instance_id())
            .expect("conn_old must have a stream handle");

        let baseline = pool.raw_connection_counter();
        assert_eq!(
            baseline, 1,
            "test precondition: exactly one counted, live session before the race"
        );

        // The FRESH replacement session a concurrent inbound publishes for
        // the same peer identity WHILE `conn_old`'s teardown CAS is about to
        // run — this is the exact race that makes the CAS lose.
        let (io_fresh, _p_fresh) = tokio::io::duplex(1024);
        let (sh_fresh, _w_fresh, _r_fresh) = LockFreeStreamHandle::new(
            io_fresh,
            fresh_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_fresh = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
        conn_fresh.stream_handle = Some(Arc::new(sh_fresh));
        conn_fresh.embedded_peer_id = Some(peer_id.clone());
        conn_fresh.set_state(ConnectionState::Connected);
        let conn_fresh = Arc::new(conn_fresh);

        let _guard = {
            let pool = pool.clone();
            let peer_id = peer_id.clone();
            let conn_fresh = conn_fresh.clone();
            crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
                if let crate::TransportLifecycleEvent::SocketFailureMatchedInstanceTeardownAttempt {
                    peer,
                    ..
                } = &event
                    && *peer == peer_id
                {
                    // Deregister first: `add_connection_by_peer_id` below
                    // fires its own (non-matching) `SessionPublished` event
                    // through this same global hook, and this avoids any
                    // reentrant/recursive invocation of this closure.
                    crate::set_transport_lifecycle_recorder(None);
                    // Models a real concurrently-published fresh session:
                    // publishes AND counts it, exactly like a real
                    // accept/finalize would.
                    assert!(pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        fresh_addr,
                        conn_fresh.clone()
                    ));
                }
            }))
        };

        // conn_old's IO task exits and reports failure, identifying itself by
        // its OWN captured instance id — which matched the current session
        // at the moment the handler made its decision, but the CAS below
        // will observe the fresh session installed instead and lose.
        registry
            .handle_peer_connection_failure(old_addr, Some(old_instance_id))
            .await
            .unwrap();

        let after = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            after.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn_fresh)),
            "the fresh session published into the CAS-loss race must survive as current \
             (got {after:?})"
        );

        let mut lingering_old_alias = false;
        pool.connections_by_addr.iter_sync(|_, v| {
            if Arc::ptr_eq(v, &conn_old) {
                lingering_old_alias = true;
            }
            true
        });
        assert!(
            !lingering_old_alias,
            "the old failed instance must leave NO lingering `connections_by_addr` alias once \
             its CAS against `peer_sessions` has lost"
        );

        assert!(
            !conn_old.has_live_stream(),
            "the old failed instance's background tasks must be aborted even when its CAS \
             against `peer_sessions` loses"
        );

        let final_count = pool.raw_connection_counter();
        assert_eq!(
            final_count,
            baseline,
            "connection_counter must return to exactly one live session after the CAS-lost \
             failed instance is retired by identity — got {final_count}, baseline {baseline} \
             (leaked {} if unfixed)",
            final_count.saturating_sub(baseline)
        );
    }

    /// RED (P1 finding, `stream_writer.rs` `ExitGuard::drop`, the ONLY
    /// production caller that passes `failed_instance_id` to
    /// `handle_peer_connection_failure`): unlike every other test in this
    /// module, which calls `handle_peer_connection_failure` directly, this
    /// drives the REAL production seam — a genuine
    /// `LockFreeStreamHandle`/`io_task` wired to this registry via a real
    /// `ReadContext`, whose IO task is made to exit by closing its peer
    /// socket (an actual `UnexpectedEof`), exercising `ExitGuard::drop`
    /// itself rather than the handler it eventually (or, at HEAD, does not)
    /// call.
    ///
    /// Setup: `old` is a stale/superseded instance for `peer_id`, still
    /// indexed at `old_addr` in `connections_by_addr` and still contributing
    /// to `connection_counter`. `fresh` has since become the peer's current,
    /// published session at a different address — the ordinary "reconnected
    /// while the old link was still dying" shape. `old`'s own IO task then
    /// exits (peer socket closed).
    ///
    /// At HEAD: the exiting IO task resolves the peer's current session,
    /// sees it is a DIFFERENT instance (`fresh`, not `old`), and sets
    /// `should_cancel_pending = false` — which at HEAD also skips the ONLY
    /// call in this code path that could retire `old`'s own bookkeeping.
    /// `old` is never handed to any cleanup: its `connections_by_addr[old_addr]`
    /// alias and its `connection_counter` contribution both leak forever
    /// (RED), while `fresh` and any pending requests on it are untouched
    /// either way. GREEN after the fix: `old`'s alias and counter
    /// contribution are retired, `fresh` remains exactly as published, and
    /// no peer-wide failure accounting fires (verified via the peer's
    /// `gossip_state` failure counter staying at zero and `fresh` never
    /// being disconnected).
    #[tokio::test]
    async fn superseded_io_exit_retires_own_instance_without_peer_wide_accounting() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle, MASTER_BUFFER_SIZE, ReadContext,
        };

        let registry = Arc::new(GossipRegistry::<()>::new(test_addr(9600), test_config()));
        let peer_id = test_peer_id("superseded_io_exit_peer");
        let old_addr = test_addr(9601);
        let fresh_addr = test_addr(9602);
        let pool = &registry.connection_pool;
        pool.set_configured_peer_addr(&peer_id, old_addr);

        // The registry must be tracking this peer for the gossip-state
        // failure-accounting assertion below to be meaningful (only tracked
        // peers can have `failures` bumped at all).
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                old_addr,
                crate::registry::PeerInfo {
                    peer_address: Some(old_addr),
                    ..crate::registry::PeerInfo::local(old_addr)
                },
            );
        }

        // `old`: a real, registry-wired stream instance — the production
        // seam this finding is about. Its IO task's `ExitGuard` captures
        // `registry_weak`/`peer_addr`/`peer_id` from this `ReadContext`,
        // exactly as a real accepted/dialed connection would.
        let (old_io, old_peer_io) = tokio::io::duplex(1024);
        let old_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry),
            peer_addr: old_addr,
            session_source: old_addr,
            peer_id: Some(peer_id.clone()),
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (old_handle, old_writer_task, _old_reader_task) = LockFreeStreamHandle::new(
            old_io,
            old_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            Some(old_read_ctx),
        );
        let mut conn_old = LockFreeConnection::new(old_addr, ConnectionDirection::Outbound);
        conn_old.stream_handle = Some(Arc::new(old_handle));
        conn_old.embedded_peer_id = Some(peer_id.clone());
        conn_old.set_state(ConnectionState::Connected);
        let conn_old = Arc::new(conn_old);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), old_addr, conn_old.clone()));

        // `fresh`: the peer's current, winning session — a different
        // instance at a different address. Publishing it does not touch
        // `old`'s own `connections_by_addr[old_addr]` alias or its
        // counted contribution; only `old`'s own retirement can do that.
        let (fresh_io, _fresh_peer_io) = tokio::io::duplex(1024);
        let (fresh_handle, _fresh_writer_task, _fresh_reader_task) = LockFreeStreamHandle::new(
            fresh_io,
            fresh_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_fresh = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
        conn_fresh.stream_handle = Some(Arc::new(fresh_handle));
        conn_fresh.embedded_peer_id = Some(peer_id.clone());
        conn_fresh.set_state(ConnectionState::Connected);
        let conn_fresh = Arc::new(conn_fresh);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), fresh_addr, conn_fresh.clone()));

        assert!(
            pool.get_connection_by_peer_id(&peer_id)
                .is_some_and(|c| Arc::ptr_eq(&c, &conn_fresh)),
            "test precondition: `fresh` must be the peer's current session"
        );
        assert_eq!(
            pool.raw_connection_counter(),
            2,
            "test precondition: two counted, live instances before `old` exits"
        );

        // `old`'s IO task exits for real: close its peer socket, producing a
        // genuine `UnexpectedEof` inside `io_task`, which returns and drops
        // its `ExitGuard` — the actual production trigger, not a direct
        // handler call.
        drop(old_peer_io);
        old_writer_task
            .await
            .expect("old instance's IO task must not panic");

        // The current, winning session must be completely untouched.
        assert!(
            pool.get_connection_by_peer_id(&peer_id)
                .is_some_and(|c| Arc::ptr_eq(&c, &conn_fresh)),
            "the superseded exit must never touch the peer's current session"
        );
        assert!(
            pool.connections_by_peer
                .read_sync(&peer_id, |_, v| Arc::ptr_eq(v, &conn_fresh))
                .unwrap_or(false),
            "`connections_by_peer` must still point at `fresh`"
        );
        assert!(
            conn_fresh.has_live_stream(),
            "`fresh`'s background tasks must never be aborted by a superseded sibling's exit"
        );

        // The superseded `old` instance must be fully retired: no lingering
        // `connections_by_addr` alias...
        let old_alias_survives = pool
            .connections_by_addr
            .read_sync(&old_addr, |_, v| Arc::ptr_eq(v, &conn_old))
            .unwrap_or(false);
        assert!(
            !old_alias_survives,
            "RED at HEAD: the superseded `old` instance's own `connections_by_addr[old_addr]` \
             alias must be retired by its own exiting IO task — the ONLY production caller for \
             this cleanup — not left as a zombie forever"
        );

        // ...and its `connection_counter` contribution released, back down
        // to exactly the one live session (`fresh`).
        let final_count = pool.raw_connection_counter();
        assert_eq!(
            final_count, 1,
            "RED at HEAD: `old`'s `connection_counter` contribution must be released when its \
             own IO task retires it — got {final_count}, expected 1 (leaked otherwise)"
        );

        // No peer-wide failure accounting/consensus/gossip-failure signalling
        // may fire for a superseded exit: the peer's tracked failure count
        // must stay at zero.
        {
            let gossip_state = registry.gossip_state.lock().await;
            let failures = gossip_state
                .peers
                .get(&old_addr)
                .map(|info| info.failures)
                .unwrap_or(0);
            assert_eq!(
                failures, 0,
                "a superseded instance's own IO exit must never mark the peer as failed \
                 (peer-wide accounting must only fire for the CURRENT session's own failure)"
            );
        }
    }

    /// RED (P1 finding, review-A): when `failed_instance_id` matches the
    /// CURRENT session's own instance id, but a FRESH session for the same
    /// peer is published in the gap between that match and
    /// `disconnect_connection_instance`'s own CAS — so the CAS itself
    /// observably LOSES (`retired == false`), exactly the race
    /// `socket_failure_matched_instance_cas_loss_retires_failed_instance_without_leak`
    /// already covers for alias/counter cleanup — this must ALSO skip every
    /// piece of PEER-WIDE failure accounting below: marking
    /// `gossip_state.peers[addr].failures = max_peer_failures`, invoking the
    /// peer-disconnect handler, and driving actor-invalidation consensus.
    /// The fresh session is a perfectly live, currently-connected session;
    /// none of that accounting may fire for it.
    ///
    /// At HEAD: the CAS-loss branch calls `retire_lost_cas_matched_instance`
    /// and then unconditionally sets `instance_teardown_done = true`, which
    /// only skips the redundant peer-wide POOL SWEEP
    /// (`disconnect_connection_by_peer_id`) a few lines down — it does
    /// nothing to prevent execution from falling all the way through to the
    /// unconditional peer-wide failure-accounting tail further below
    /// (`peer_info.failures = self.config.max_peer_failures`, the
    /// `peer_disconnect_handler` notification, and the consensus/gossip
    /// trigger), which runs regardless of `instance_teardown_done`. RED at
    /// HEAD: `gossip_state.peers[old_addr].failures` ends up bumped to
    /// `max_peer_failures` even though `fresh` is alive and well. GREEN
    /// after the fix: the CAS-loss branch returns immediately after
    /// retiring the superseded instance, exactly like the
    /// already-superseded branch above it, and the failure counter stays at
    /// zero.
    ///
    /// Pinned deterministically via `set_transport_lifecycle_recorder` on
    /// `SocketFailureMatchedInstanceTeardownAttempt`, identically to
    /// `socket_failure_matched_instance_cas_loss_retires_failed_instance_without_leak`.
    #[tokio::test]
    async fn socket_failure_matched_instance_cas_loss_skips_peer_wide_accounting() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9400), test_config());
        let peer_id = test_peer_id("matched_instance_cas_loss_accounting_peer");
        let old_addr = test_addr(9401);
        let fresh_addr = test_addr(9402);
        let pool = registry.connection_pool.clone();
        pool.set_configured_peer_addr(&peer_id, old_addr);

        // The registry must be tracking this peer for the failure-accounting
        // assertion below to be meaningful (only tracked peers can have
        // `failures` bumped at all).
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                old_addr,
                crate::registry::PeerInfo {
                    peer_address: Some(old_addr),
                    ..crate::registry::PeerInfo::local(old_addr)
                },
            );
        }

        // Current, live session whose IO task is about to fail.
        let (io_old, _p_old) = tokio::io::duplex(1024);
        let (sh_old, _w_old, _r_old) = LockFreeStreamHandle::new(
            io_old,
            old_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_old = LockFreeConnection::new(old_addr, ConnectionDirection::Outbound);
        conn_old.stream_handle = Some(Arc::new(sh_old));
        conn_old.embedded_peer_id = Some(peer_id.clone());
        conn_old.set_state(ConnectionState::Connected);
        let conn_old = Arc::new(conn_old);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), old_addr, conn_old.clone()));

        let old_instance_id = conn_old
            .stream_handle
            .as_ref()
            .map(|h| h.instance_id())
            .expect("conn_old must have a stream handle");

        // The FRESH replacement session a concurrent inbound publishes for
        // the same peer identity WHILE `conn_old`'s teardown CAS is about to
        // run — this is the exact race that makes the CAS lose.
        let (io_fresh, _p_fresh) = tokio::io::duplex(1024);
        let (sh_fresh, _w_fresh, _r_fresh) = LockFreeStreamHandle::new(
            io_fresh,
            fresh_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_fresh = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
        conn_fresh.stream_handle = Some(Arc::new(sh_fresh));
        conn_fresh.embedded_peer_id = Some(peer_id.clone());
        conn_fresh.set_state(ConnectionState::Connected);
        let conn_fresh = Arc::new(conn_fresh);

        let _guard = {
            let pool = pool.clone();
            let peer_id = peer_id.clone();
            let conn_fresh = conn_fresh.clone();
            crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
                if let crate::TransportLifecycleEvent::SocketFailureMatchedInstanceTeardownAttempt {
                    peer,
                    ..
                } = &event
                    && *peer == peer_id
                {
                    // Deregister first: `add_connection_by_peer_id` below
                    // fires its own (non-matching) `SessionPublished` event
                    // through this same global hook, and this avoids any
                    // reentrant/recursive invocation of this closure.
                    crate::set_transport_lifecycle_recorder(None);
                    // Models a real concurrently-published fresh session:
                    // publishes AND counts it, exactly like a real
                    // accept/finalize would.
                    assert!(pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        fresh_addr,
                        conn_fresh.clone()
                    ));
                }
            }))
        };

        // conn_old's IO task exits and reports failure, identifying itself by
        // its OWN captured instance id — which matched the current session
        // at the moment the handler made its decision, but the CAS below
        // will observe the fresh session installed instead and lose.
        registry
            .handle_peer_connection_failure(old_addr, Some(old_instance_id))
            .await
            .unwrap();

        let after = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            after.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn_fresh)),
            "the fresh session published into the CAS-loss race must survive as current \
             (got {after:?})"
        );

        {
            let gossip_state = registry.gossip_state.lock().await;
            let failures = gossip_state
                .peers
                .get(&old_addr)
                .map(|info| info.failures)
                .unwrap_or(0);
            assert_eq!(
                failures, 0,
                "RED at HEAD: a socket failure whose instance-scoped CAS lost to a \
                 concurrently published fresh session must never mark the peer as failed — \
                 `fresh` is a live, currently-connected session, not a dead peer"
            );
        }
    }

    /// RED (P1 finding, review-B / glm): the far more common real-world
    /// shape of the stream-writer double-decrement is NOT a rejected,
    /// never-counted candidate — it is an ordinary `ReplaceExisting`
    /// tie-break cycle: (a) `old` is the peer's current session, counted
    /// once; (b) an inbound accept wins the tie-break and evicts `old` by
    /// CAS'd identity via `disconnect_connection_instance` — which retires
    /// `old`'s aliases AND releases its `connection_counter` contribution,
    /// then aborts `old`'s background IO task; (c) the winning `fresh`
    /// session is published and counted; (d) `old`'s now-ABORTED IO task
    /// exits (that abort is exactly what makes it exit) and its
    /// `ExitGuard::drop` runs — sees the peer's current session (`fresh`) is
    /// a different instance, classifies itself as "superseded", and (at
    /// HEAD) unconditionally releases a compensating decrement because
    /// `remove_connection_instance_by_id` no longer finds `old` indexed
    /// (step (b) already swept its aliases). That is a SECOND release of a
    /// contribution `disconnect_connection_instance` already released in
    /// step (b) — a genuine double-decrement, not a never-counted
    /// underflow. Because `abort_tasks()` is exactly what triggers this
    /// exit, this fires on EVERY `ReplaceExisting` cycle: left unfixed,
    /// `connection_counter` drifts one-under-real on every single tie-break
    /// eviction, and enough cycles permanently defeat the admission gate
    /// (`add_lock_free_connection`'s `connection_count >= max_connections`
    /// check now under-reports, admitting unbounded connections).
    ///
    /// This deliberately does NOT go through a rejected/never-counted
    /// candidate (that shape is covered by
    /// `socket_failure_matched_instance_cas_loss_retires_failed_instance_without_leak`
    /// and the ownership-table design itself) — it drives the
    /// counted-then-displaced shape: `disconnect_connection_instance` is
    /// called directly (mirroring the exact eviction step the
    /// `ReplaceExisting` tie-break arm performs), THEN the replacement is
    /// published, THEN `old`'s own real, registry-wired IO task is made to
    /// exit for real (a genuine socket close, exercising `ExitGuard::drop`
    /// itself, not a direct handler call) so the superseded-exit fallback
    /// actually fires against an already-released instance.
    #[tokio::test]
    async fn replace_existing_then_io_exit_does_not_double_decrement_connection_counter() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle, MASTER_BUFFER_SIZE, ReadContext,
        };

        let registry = Arc::new(GossipRegistry::<()>::new(test_addr(9700), test_config()));
        let peer_id = test_peer_id("replace_existing_double_decrement_peer");
        let old_addr = test_addr(9701);
        let fresh_addr = test_addr(9702);
        let pool = &registry.connection_pool;
        pool.set_configured_peer_addr(&peer_id, old_addr);

        // `old`: a real, registry-wired stream instance, exactly like the
        // production `ReplaceExisting` loser being evicted.
        let (old_io, old_peer_io) = tokio::io::duplex(1024);
        let old_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry),
            peer_addr: old_addr,
            session_source: old_addr,
            peer_id: Some(peer_id.clone()),
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (old_handle, old_writer_task, _old_reader_task) = LockFreeStreamHandle::new(
            old_io,
            old_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            Some(old_read_ctx),
        );
        let mut conn_old = LockFreeConnection::new(old_addr, ConnectionDirection::Outbound);
        conn_old.stream_handle = Some(Arc::new(old_handle));
        conn_old.embedded_peer_id = Some(peer_id.clone());
        conn_old.set_state(ConnectionState::Connected);
        let conn_old = Arc::new(conn_old);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), old_addr, conn_old.clone()));

        let baseline = pool.raw_connection_counter_signed();
        assert_eq!(
            baseline, 1,
            "test precondition: exactly one counted, live session before the tie-break"
        );

        // (b) The `ReplaceExisting` tie-break's own eviction step: retire
        // `old` by CAS'd instance identity. This is the exact call
        // `finalize_new_outbound_connection`'s `ReplaceExisting` arm makes.
        assert!(
            pool.disconnect_connection_instance(&peer_id, &conn_old),
            "test precondition: `old` must still be current at the moment of eviction"
        );
        // Asserted via the signed accessor: this precondition's expected
        // value is exactly 0, the one steady-state count the clamped
        // `raw_connection_counter()` view cannot distinguish from an
        // unfixed underflow.
        assert_eq!(
            pool.raw_connection_counter_signed(),
            0,
            "test precondition: evicting `old` must release its counted contribution \
             immediately"
        );

        // (c) The tie-break's winner is published — a fresh, unrelated
        // instance, counted exactly once.
        let (fresh_io, _fresh_peer_io) = tokio::io::duplex(1024);
        let (fresh_handle, _fresh_writer_task, _fresh_reader_task) = LockFreeStreamHandle::new(
            fresh_io,
            fresh_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn_fresh = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
        conn_fresh.stream_handle = Some(Arc::new(fresh_handle));
        conn_fresh.embedded_peer_id = Some(peer_id.clone());
        conn_fresh.set_state(ConnectionState::Connected);
        let conn_fresh = Arc::new(conn_fresh);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), fresh_addr, conn_fresh.clone()));

        let before_exit = pool.raw_connection_counter_signed();
        assert_eq!(
            before_exit, 1,
            "test precondition: exactly one counted, live session (`fresh`) after the \
             tie-break completes"
        );

        // (d) `old`'s own IO task — already aborted by step (b)'s
        // `disconnect_connection_instance` — exits for real. Closing its
        // peer socket lets a genuine `UnexpectedEof` race the abort, but
        // either way `ExitGuard::drop` runs and takes the superseded-exit
        // fallback, since the peer's current session (`fresh`) is a
        // different instance than `old`.
        drop(old_peer_io);
        old_writer_task
            .await
            .expect("old instance's IO task must not panic");

        let final_count = pool.raw_connection_counter_signed();
        assert_eq!(
            final_count,
            before_exit,
            "RED at HEAD: `old`'s already-evicted-and-released `connection_counter` \
             contribution must not be released a SECOND time by its own IO task's \
             superseded-exit fallback — got {final_count}, expected {before_exit} to stay \
             matched to the one truly live session (leaked {} decrements if unfixed)",
            before_exit.saturating_sub(final_count)
        );

        // `fresh` must remain completely unaffected throughout.
        assert!(
            pool.get_connection_by_peer_id(&peer_id)
                .is_some_and(|c| Arc::ptr_eq(&c, &conn_fresh)),
            "the double-decrement race must never disturb `fresh`, the live current session"
        );
    }

    /// RED (P1 finding, atomicity of the `connection_counter`/`counted_instances`
    /// pairing): `finish_indexing_accepted_connection` bumps
    /// `connection_counter` FIRST and only inserts the instance's ownership
    /// marker into `counted_instances` afterward. A concurrent teardown
    /// (`disconnect_connection_instance`) for this exact, already-published
    /// candidate that lands in the window between those two operations finds
    /// no marker yet, so `release_counted_connection` releases nothing — then
    /// the marker is inserted moments later regardless, over a connection
    /// that has already been fully evicted (its address aliases removed, its
    /// tasks aborted). Nothing is ever left to release that marker again:
    /// the revalidation cleanup below only decrements through
    /// `remove_connection_instance_by_id`, which finds nothing at either
    /// address (the concurrent teardown already removed them), so the
    /// `connection_counter` contribution just bumped leaks permanently.
    /// Repeated reconnect churn leaks the counter until the admission gate
    /// (`add_lock_free_connection`'s `connection_count >= max_connections`
    /// check) is falsely reached despite no real growth in live connections.
    ///
    /// Pinned deterministically via
    /// `TransportLifecycleEvent::ConnectionCountMarkerAttempt`, which fires
    /// exactly at the counter/marker pairing point, guaranteeing the
    /// concurrent teardown lands in the exact window the finding describes
    /// on every run.
    #[tokio::test]
    async fn count_marker_teardown_race_does_not_leak_connection_counter() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9750), test_config());
        let peer_id = test_peer_id("count_marker_race_peer");
        let addr = test_addr(9751);
        let pool = registry.connection_pool.clone();

        // Asserted via the signed accessor: a baseline of exactly 0 is the
        // one steady-state value the clamped `raw_connection_counter()` view
        // cannot distinguish from an unfixed underflow (both read as 0), so
        // this precondition — and the final comparison below — must pin the
        // signed value, not the clamped one.
        let baseline = pool.raw_connection_counter_signed();
        assert_eq!(
            baseline, 0,
            "test precondition: a fresh pool has no live sessions"
        );

        let (io, _keep) = tokio::io::duplex(1024);
        let (sh, _w, _r) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Inbound);
        conn.stream_handle = Some(Arc::new(sh));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = Arc::new(conn);

        // `finish_indexing_accepted_connection`'s own contract: it is only
        // ever called AFTER a successful compare-and-publish has already
        // installed `conn` as the peer's current session.
        pool.publish_current_peer_connection(&peer_id, conn.clone());

        // The mid-window teardown: fires exactly between the
        // `connection_counter` increment and the `counted_instances` marker
        // insert, mirroring `disconnect_connection_instance` racing this
        // exact, just-published instance (e.g. the IO-exit path, or another
        // tie-break's eviction) in that gap.
        let _guard = {
            let pool = pool.clone();
            let peer_id = peer_id.clone();
            let conn = conn.clone();
            crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
                if let crate::TransportLifecycleEvent::ConnectionCountMarkerAttempt { .. } = &event
                {
                    // Deregister first: `disconnect_connection_instance`
                    // below fires its own `SessionRemoved` event through
                    // this same global hook, and this avoids any
                    // reentrant/recursive invocation of this closure.
                    crate::set_transport_lifecycle_recorder(None);
                    assert!(
                        pool.disconnect_connection_instance(&peer_id, &conn),
                        "test precondition: `conn` must still be current at the moment of \
                         the mid-window teardown"
                    );
                }
            }))
        };

        let indexed = pool.finish_indexing_accepted_connection(&peer_id, addr, None, &conn);
        assert!(
            !indexed,
            "the mid-window teardown must be observed by the revalidation and this \
             candidate treated as rejected, exactly like a re-resolved tie-break loss"
        );

        let final_count = pool.raw_connection_counter_signed();
        assert_eq!(
            final_count,
            baseline,
            "connection_counter must return to baseline ({baseline}) after a mid-window \
             teardown raced the counter/marker pairing — got {final_count} (leaked {} if \
             unfixed; a negative steady-state value would otherwise clamp to 0 and hide the \
             regression)",
            final_count.saturating_sub(baseline)
        );
    }

    /// RED (re-review residual of the #86 fix above): `count_in_new_instance`
    /// fires `ConnectionCountMarkerAttempt` and inserts the `counted_instances`
    /// marker BEFORE it bumps `connection_counter` — so a concurrent teardown
    /// of the SAME instance can land strictly *after* the insert but *before*
    /// the increment: `remove_sync` finds the marker (present), decrements,
    /// and only THEN does the original caller's `fetch_add` run. At a
    /// baseline of 0 this used to leak permanently: the decrement used a
    /// `saturating_sub`, which clamps the transient `0 -> -1` down to `0`
    /// instead of letting it go net-negative, so the following `+1` landed on
    /// `0` and produced `1` — with the marker already gone, nothing can ever
    /// release that phantom unit again. Reconnect/failover churn compounds
    /// this until the admission gate (`add_lock_free_connection`'s
    /// `connection_count >= max_connections` check) falsely trips with no
    /// real growth in live connections.
    ///
    /// Pinned deterministically via
    /// `TransportLifecycleEvent::ConnectionCountIncrementAttempt`, fired
    /// immediately after the marker insert succeeds and immediately before
    /// the paired `fetch_add` — the exact insert-then-teardown-then-increment
    /// window this finding depends on, distinct from
    /// `ConnectionCountMarkerAttempt` (fired before the insert, and already
    /// covered by `count_marker_teardown_race_does_not_leak_connection_counter`
    /// above).
    #[tokio::test]
    async fn count_marker_teardown_between_insert_and_increment_does_not_leak() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9760), test_config());
        let peer_id = test_peer_id("count_marker_insert_increment_race_peer");
        let addr = test_addr(9761);
        let pool = registry.connection_pool.clone();

        // Asserted via the signed accessor: a baseline of exactly 0 is the
        // one steady-state value the clamped `raw_connection_counter()` view
        // cannot distinguish from an unfixed underflow (both read as 0), so
        // this precondition — and the final comparison below — must pin the
        // signed value, not the clamped one.
        let baseline = pool.raw_connection_counter_signed();
        assert_eq!(
            baseline, 0,
            "test precondition: a fresh pool has no live sessions"
        );

        let (io, _keep) = tokio::io::duplex(1024);
        let (sh, _w, _r) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Inbound);
        conn.stream_handle = Some(Arc::new(sh));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = Arc::new(conn);
        let instance_id = conn
            .stream_handle
            .as_ref()
            .expect("stream handle set above")
            .instance_id();

        // The mid-window teardown: fires exactly between the
        // `counted_instances` marker insert and the `connection_counter`
        // increment (inside `count_in_new_instance`, reached here via
        // `add_connection_by_peer_id`), mirroring a concurrent
        // `release_counted_instance` racing this exact instance in that gap.
        let _guard = {
            let pool = pool.clone();
            crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
                if let crate::TransportLifecycleEvent::ConnectionCountIncrementAttempt {
                    instance_id: fired_id,
                } = &event
                    && *fired_id == instance_id
                {
                    // Deregister first: this closure must fire exactly once
                    // for this exact instance.
                    crate::set_transport_lifecycle_recorder(None);
                    pool.release_counted_instance(instance_id);
                }
            }))
        };

        assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, conn.clone()));

        let final_count = pool.raw_connection_counter_signed();
        assert_eq!(
            final_count,
            baseline,
            "connection_counter must return to baseline ({baseline}) after a mid-window \
             teardown raced the marker-insert/counter-increment pairing — got {final_count} \
             (permanently leaked {} if unfixed by a saturating clamp; a negative steady-state \
             value would otherwise clamp to 0 and hide the regression)",
            final_count.saturating_sub(baseline)
        );
    }

    /// RED (absence-of-alias misread as superseded): a socket failure for an
    /// INBOUND current session's *ephemeral* peer address — which was never
    /// (or is no longer) indexed in `connections_by_addr` — must NOT be
    /// misclassified as "superseded, ignore". The current session's `addr`
    /// is the advertised/bind address, not the ephemeral socket address the
    /// IO-failure callback reports, so `get_lock_free_connection` legitimately
    /// returns `None` for the observed address on a genuinely dead current
    /// session. Absence of an alias must fall through to the normal
    /// current-connection failure path (disconnect + failure accounting),
    /// never be read as proof of a different, superseded instance.
    #[tokio::test]
    async fn socket_failure_of_unaliased_ephemeral_addr_fails_current_session() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9200), test_config());
        let peer_id = test_peer_id("ephemeral_absent_peer");
        let bind_addr = test_addr(9201);
        let ephemeral_addr = test_addr(9202);
        let pool = &registry.connection_pool;
        pool.set_configured_peer_addr(&peer_id, bind_addr);

        // Current, live, INBOUND session published under its bind address.
        let (io, _peer_io) = tokio::io::duplex(1024);
        let (stream_handle, _writer, _reader) = LockFreeStreamHandle::new(
            io,
            bind_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(bind_addr, ConnectionDirection::Inbound);
        conn.stream_handle = Some(Arc::new(stream_handle));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = Arc::new(conn);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), bind_addr, conn.clone()));

        // The peer is reachable by identity via `ephemeral_addr` (the socket
        // the IO task actually failed on), but the ephemeral alias was never
        // inserted into `connections_by_addr` — e.g. the IO task exited
        // before the post-accept alias insertion, or it was already cleaned
        // up. `get_lock_free_connection(ephemeral_addr)` must return `None`.
        pool.add_addr_to_peer_id(ephemeral_addr, peer_id.clone());
        assert!(pool.get_lock_free_connection(ephemeral_addr).is_none());

        let before = pool
            .get_connection_by_peer_id(&peer_id)
            .expect("current session must resolve to conn");
        assert!(Arc::ptr_eq(&before, &conn));

        // The current session's own (ephemeral) socket fails.
        registry
            .handle_peer_connection_failure(ephemeral_addr, None)
            .await
            .unwrap();

        let after = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            after.is_none(),
            "socket failure for the current session's unaliased ephemeral address was \
             misclassified as a superseded connection and silently ignored — the dead \
             current session stayed published"
        );
    }

    /// RED (address-vs-identity): a peer re-announced at a NEW address (same
    /// verified identity — e.g. a restart on a fresh ephemeral port) must
    /// reindex the address mapping WITHOUT tearing down the live
    /// identity-verified session. `add_peer_with_node_id`'s "Closing old
    /// connection for peer due to address change" path
    /// (registry.rs `disconnect_connection_by_peer_id` on `old_addr != new_addr`)
    /// is address-keyed and drops the good session.
    #[tokio::test]
    async fn address_change_reindexes_without_tearing_down_live_session() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9110), test_config());
        let node_id = test_peer_id("addr_change_peer").to_node_id();
        let peer_id = node_id.to_peer_id();
        let old_addr = test_addr(9111);
        let new_addr = test_addr(9112);
        let pool = &registry.connection_pool;
        pool.set_configured_peer_addr(&peer_id, old_addr);

        let (io, _p) = tokio::io::duplex(1024);
        let (sh, _w, _r) = LockFreeStreamHandle::new(
            io,
            old_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(old_addr, ConnectionDirection::Inbound);
        conn.stream_handle = Some(Arc::new(sh));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = Arc::new(conn);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), old_addr, conn.clone()));

        // Peer re-announced at a new address with the same identity.
        registry
            .add_peer_with_node_id(new_addr, Some(node_id))
            .await;

        let after = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            after.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn)),
            "address change tore down the live identity-verified session; identity is \
             unchanged so the address must only be reindexed, never disconnected"
        );
    }

    /// RED (address-vs-identity, lookup-by-new-address): the same-identity
    /// address-change path in `add_peer_with_node_id` upserts
    /// `addr_to_peer_id[new_addr]` BEFORE calling `reindex_connection_addr`.
    /// `reindex_connection_addr`'s "already indexed under this peer" branch
    /// used to trust that alias and return without ever writing
    /// `connections_by_addr[new_addr]`. Result: `addr_to_peer_id` says the new
    /// address belongs to this peer, but a direct lookup/dial by that address
    /// finds no connection at all and would spin up a duplicate instead of
    /// reusing the live, identity-verified session.
    #[tokio::test]
    async fn address_change_reindex_makes_new_address_resolve_to_live_connection() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let registry = GossipRegistry::<()>::new(test_addr(9120), test_config());
        let node_id = test_peer_id("addr_change_lookup_peer").to_node_id();
        let peer_id = node_id.to_peer_id();
        let old_addr = test_addr(9121);
        let new_addr = test_addr(9122);
        let pool = &registry.connection_pool;
        pool.set_configured_peer_addr(&peer_id, old_addr);

        let (io, _p) = tokio::io::duplex(1024);
        let (sh, _w, _r) = LockFreeStreamHandle::new(
            io,
            old_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(old_addr, ConnectionDirection::Inbound);
        conn.stream_handle = Some(Arc::new(sh));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = Arc::new(conn);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), old_addr, conn.clone()));

        // Peer re-announced at a new address with the same identity.
        registry
            .add_peer_with_node_id(new_addr, Some(node_id))
            .await;

        assert_eq!(
            pool.addr_to_peer_id.read_sync(&new_addr, |_, v| v.clone()),
            Some(peer_id.clone()),
            "addr_to_peer_id must map the reannounced address to the same identity"
        );
        let by_addr = pool.get_lock_free_connection(new_addr);
        assert!(
            by_addr.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn)),
            "connections_by_addr[new_addr] was missing after the same-identity address \
             change; a lookup/dial by the reannounced address misses the live session and \
             would create a duplicate connection instead of reusing it"
        );
    }

    #[tokio::test]
    async fn transport_only_peer_failure_does_not_start_health_consensus() {
        let mut config = test_config();
        config.peer_health_mode = PeerHealthMode::TransportOnly;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        registry.add_peer(test_addr(8081)).await;

        registry
            .handle_peer_connection_failure(test_addr(8081), None)
            .await
            .unwrap();

        let gossip_state = registry.gossip_state.lock().await;
        let peer = gossip_state.peers.get(&test_addr(8081)).unwrap();
        assert_eq!(peer.failures, registry.config.max_peer_failures);
        assert!(peer.last_failure_time.is_some());
        assert!(gossip_state.pending_peer_failures.is_empty());
        assert!(gossip_state.peer_health_reports.is_empty());
    }

    #[tokio::test]
    async fn peer_connection_failure_retains_remote_actors_for_reconnection() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());
        let peer_addr = test_addr(8081);
        let peer_id = test_peer_id("reconnectable_peer");
        let actor_name = "remote_reconnectable_actor";

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            let mut peer_info = PeerInfo::local(peer_addr);
            peer_info.node_id = Some(peer_id.to_node_id());
            gossip_state.peers.insert(peer_addr, peer_info);

            let mut actors = HashSet::new();
            actors.insert(actor_name.to_string());
            gossip_state.peer_to_actors.insert(peer_addr, actors);
        }

        registry.actor_state.known_actors.upsert_sync(
            actor_name.to_string(),
            RemoteActorLocation::new_with_peer(peer_addr, peer_id.clone()),
        );

        registry
            .handle_peer_connection_failure(peer_addr, None)
            .await
            .unwrap();

        assert!(
            registry.actor_state.known_actors.contains_sync(actor_name),
            "transport failure must not prune remote actors before timeout"
        );
        assert!(
            !registry
                .actor_state
                .removed_actors
                .contains_sync(actor_name),
            "transport failure must not create ActorRemoved tombstones"
        );

        let gossip_state = registry.gossip_state.lock().await;
        assert!(
            gossip_state.peer_to_actors.contains_key(&peer_addr),
            "actor attribution must remain so reconnect/full-sync can repair cleanly"
        );
        assert!(
            gossip_state.pending_peer_failures.contains_key(&peer_addr),
            "failure should still enter the consensus/timeout path"
        );
        assert!(
            gossip_state.urgent_changes.is_empty(),
            "transport failure must not broadcast immediate ActorRemoved"
        );
        assert!(
            gossip_state.pending_changes.is_empty(),
            "transport failure must not enqueue regularized ActorRemoved"
        );
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
            .handle_peer_connection_failure(peer_addr, None)
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
            .handle_peer_connection_failure(peer_addr, None)
            .await
            .unwrap();

        let (got_addr, got_peer_id) = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got_addr, peer_addr);
        assert_eq!(got_peer_id, None);
    }

    /// Regression for the post-shutdown disconnect-handler gate.
    /// `handle_peer_connection_failure` spawns the
    /// `peer_disconnect_handler` notification on a detached
    /// `tokio::spawn`. Without a shutdown gate, the spawn keeps the
    /// registry alive past `shutdown_and_wait` and the handler can
    /// fire after the user has explicitly torn the registry down. The
    /// fix gates the spawn on `self.shutdown` at two points: before
    /// spawning (skip entirely if already shutting down) and inside
    /// the spawned task (bail before invoking the handler).
    #[tokio::test]
    async fn test_post_shutdown_peer_disconnect_handler_is_skipped() {
        use crate::registry::PeerDisconnectHandler;
        use futures::future::BoxFuture;
        use std::net::SocketAddr;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingHandler {
            invocations: Arc<AtomicUsize>,
        }
        impl PeerDisconnectHandler for CountingHandler {
            fn handle_peer_disconnect(
                &self,
                _peer_addr: SocketAddr,
                _peer_id: Option<crate::PeerId>,
            ) -> BoxFuture<'_, ()> {
                let invocations = Arc::clone(&self.invocations);
                Box::pin(async move {
                    invocations.fetch_add(1, Ordering::SeqCst);
                })
            }
        }

        let registry = GossipRegistry::<()>::new(test_addr(8090), test_config());
        let peer_addr = test_addr(8091);

        let invocations = Arc::new(AtomicUsize::new(0));
        registry
            .set_peer_disconnect_handler(Arc::new(CountingHandler {
                invocations: Arc::clone(&invocations),
            }))
            .await;

        // Pre-shutdown sanity check: the handler fires normally so the
        // assertion below is testing the gate, not handler wiring.
        registry
            .handle_peer_connection_failure(peer_addr, None)
            .await
            .unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            invocations.load(Ordering::SeqCst),
            1,
            "handler should fire on pre-shutdown connection failure"
        );

        // Now shutdown and confirm a subsequent failure does NOT
        // invoke the handler. Without the gate, the spawned notifier
        // would keep running and bump the counter to 2.
        registry.shutdown().await;
        registry
            .handle_peer_connection_failure(peer_addr, None)
            .await
            .unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            invocations.load(Ordering::SeqCst),
            1,
            "post-shutdown failure must not invoke peer_disconnect_handler"
        );
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
    async fn cleanup_stale_actors_expires_old_tombstones() {
        let mut config = test_config();
        config.vector_clock_retention_period = Duration::from_secs(10);
        let registry = GossipRegistry::<()>::new(test_addr(8081), config);

        let old_clock = crate::VectorClock::new();
        old_clock.increment(registry.peer_id.to_node_id());
        let fresh_clock = old_clock.clone();
        fresh_clock.increment(registry.peer_id.to_node_id());

        let _ = registry.actor_state.removed_actors.upsert_sync(
            "old_tombstone".to_string(),
            RemovedActorTombstone {
                vector_clock: old_clock,
                removed_at: current_timestamp().saturating_sub(11),
            },
        );
        let _ = registry.actor_state.removed_actors.upsert_sync(
            "fresh_tombstone".to_string(),
            RemovedActorTombstone::new(fresh_clock),
        );

        registry.cleanup_stale_actors().await;

        assert!(
            !registry
                .actor_state
                .removed_actors
                .contains_sync("old_tombstone")
        );
        assert!(
            registry
                .actor_state
                .removed_actors
                .contains_sync("fresh_tombstone")
        );
    }

    /// The delta-history retention check must not panic or misbehave when one
    /// entry's `recorded_at` sits ahead of the current read (e.g. two threads
    /// raced pushing entries around a scheduler pause). Using the monotonic
    /// clock with a saturating duration means this can never underflow or
    /// wrap the way a raw wall-clock subtraction would, and a backward step
    /// can never purge the *entire* history the way release-mode wraparound
    /// on wall-clock arithmetic would.
    #[tokio::test]
    async fn delta_history_retention_survives_wall_clock_backward_step() {
        let mut config = test_config();
        config.actor_ttl = Duration::from_secs(60); // history_ttl = 120s
        let registry = GossipRegistry::<()>::new(test_addr(8082), config);

        {
            let mut state = registry.gossip_state.lock().await;
            state.delta_history = vec![
                // Genuinely expired (> 120s old): should be purged.
                HistoricalDelta {
                    sequence: 1,
                    changes: Vec::new(),
                    recorded_at: Instant::now() - Duration::from_secs(200),
                },
                // Fresh: should be retained.
                HistoricalDelta {
                    sequence: 2,
                    changes: Vec::new(),
                    recorded_at: Instant::now(),
                },
                // Ahead of "now" by the time `cleanup_stale_actors` reads the
                // clock. Must not panic and must not be (mis)treated as
                // expired.
                HistoricalDelta {
                    sequence: 3,
                    changes: Vec::new(),
                    recorded_at: Instant::now() + Duration::from_secs(10_000),
                },
            ];
        }

        // Must not panic.
        registry.cleanup_stale_actors().await;

        let state = registry.gossip_state.lock().await;
        let sequences: Vec<u64> = state.delta_history.iter().map(|d| d.sequence).collect();
        assert_eq!(
            sequences,
            vec![2, 3],
            "expected only the genuinely expired delta (seq 1) purged, \
             got {sequences:?}"
        );
    }

    #[tokio::test]
    async fn test_cleanup_dead_peers() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);
        let peer_addr = test_addr(8081);
        let peer_id = test_peer_id("cleanup-dead-peer");

        // Add a failed peer with old failure time
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(peer_id.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp() - 100),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
        }

        // Add some actors from the failed peer
        {
            let _ = registry.actor_state.known_actors.upsert_sync(
                "peer_actor".to_string(),
                RemoteActorLocation::new_with_peer(peer_addr, peer_id),
            );

            let mut gossip_state = registry.gossip_state.lock().await;
            let mut actors = HashSet::new();
            actors.insert("peer_actor".to_string());
            gossip_state.peer_to_actors.insert(peer_addr, actors);
        }

        registry.cleanup_dead_peers().await;

        // Verify peer is KEPT but its actors were removed
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peers.contains_key(&peer_addr)); // Peer is still there!
        assert!(!gossip_state.peer_to_actors.contains_key(&peer_addr)); // But actors mapping is gone

        drop(gossip_state);
        assert!(
            !registry
                .actor_state
                .known_actors
                .contains_sync("peer_actor")
        ); // Actors are cleaned up
    }

    /// A backward wall-clock step (NTP correction, VM pause/resume) can leave
    /// `last_failure_time` recorded *ahead* of a subsequent `current_timestamp()`
    /// read. The raw `current_time - failure_time` subtraction must not panic,
    /// and must not treat the peer as dead-for-longer-than-timeout (which would
    /// mass-reap every recently-failed peer's actors on the next tick).
    #[tokio::test]
    async fn cleanup_dead_peers_survives_backward_clock_step() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::<()>::new(test_addr(8090), config);
        let peer_addr = test_addr(8091);
        let peer_id = test_peer_id("backward-clock-dead-peer");

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(peer_id.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    // Recorded "in the future" relative to the wall clock read
                    // inside `cleanup_dead_peers` — simulates the clock having
                    // stepped backward since this failure was recorded.
                    last_failure_time: Some(current_timestamp() + 10_000),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
        }

        let _ = registry.actor_state.known_actors.upsert_sync(
            "peer_actor".to_string(),
            RemoteActorLocation::new_with_peer(peer_addr, peer_id),
        );
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            let mut actors = HashSet::new();
            actors.insert("peer_actor".to_string());
            gossip_state.peer_to_actors.insert(peer_addr, actors);
        }

        // Must not panic (RED: raw subtraction underflows in debug builds).
        registry.cleanup_dead_peers().await;

        // Must not treat the peer as dead-for-longer-than-timeout: the
        // clamped elapsed time is ~0, which is not > dead_peer_timeout, so
        // the peer's actors must survive.
        assert!(
            registry
                .actor_state
                .known_actors
                .contains_sync("peer_actor"),
            "backward clock step must not spuriously reap actors as dead-peer timeout"
        );
    }

    /// Same backward-clock hazard as `cleanup_dead_peers_survives_backward_clock_step`,
    /// but for the vector-clock GC path: a future `last_failure_time` must not
    /// panic the raw `current_time - failure_time` subtraction, and must not be
    /// treated as "dead longer than retention" (which would prematurely GC the
    /// node's vector-clock entries).
    #[tokio::test]
    async fn run_vector_clock_gc_survives_backward_clock_step() {
        let mut config = test_config();
        config.vector_clock_retention_period = Duration::from_secs(3600);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::<()>::new(test_addr(8092), config);
        let peer_addr = test_addr(8093);
        let peer_id = test_peer_id("backward-clock-vector-gc");

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(peer_id.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    // Recorded "in the future" relative to the wall clock read
                    // inside `run_vector_clock_gc` — simulates a backward step.
                    last_failure_time: Some(current_timestamp() + 10_000),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
        }

        // Must not panic (RED: raw subtraction underflows in debug builds).
        registry.run_vector_clock_gc().await;
    }

    /// R2 regression: `cleanup_dead_peers` is the only removal path that did
    /// NOT write a `RemovedActorTombstone` or enqueue a `RegistryChange::
    /// ActorRemoved`, unlike `unregister_actor` and the removal branch of
    /// `apply_delta_from`. Without a dominating tombstone, a stale cached
    /// copy from a third peer can re-admit the actor via
    /// `current_actor_upsert_plan`, undoing the fast dead-peer reap and
    /// silently degrading it to the 24h `actor_ttl` backstop.
    #[tokio::test]
    async fn cleanup_dead_peers_writes_tombstone_and_enqueues_removal_change() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::<()>::new(test_addr(7170), config);
        let peer_addr = test_addr(7171);
        let peer_id = test_peer_id("r2-dead-peer-tombstone");
        let actor_name = "svc-x";

        let location = RemoteActorLocation::new_with_peer(peer_addr, peer_id.clone());
        let pre_removal_clock = location.vector_clock.clone();

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(peer_id.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp().saturating_sub(10)),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
            let mut actors = HashSet::new();
            actors.insert(actor_name.to_string());
            gossip_state.peer_to_actors.insert(peer_addr, actors);
        }
        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), location);

        registry.cleanup_dead_peers().await;

        assert!(
            !registry.actor_state.known_actors.contains_sync(actor_name),
            "sanity: actor must actually be reaped"
        );

        let tombstone_clock = registry
            .actor_state
            .removed_actors
            .read_sync(actor_name, |_, tombstone| tombstone.vector_clock.clone());
        let tombstone_clock = tombstone_clock.expect(
            "cleanup_dead_peers must record a RemovedActorTombstone, like every other \
             removal path (unregister_actor / apply_delta_from), or stale peer copies \
             can resurrect the actor",
        );
        assert!(
            matches!(
                pre_removal_clock.compare(&tombstone_clock),
                crate::ClockOrdering::Before
            ),
            "tombstone must be causally after the reaped actor's vector clock so it \
             dominates any stale copy still holding the original clock"
        );

        let gossip_state = registry.gossip_state.lock().await;
        let enqueued = gossip_state
            .pending_changes
            .iter()
            .chain(gossip_state.urgent_changes.iter())
            .any(|change| matches!(change, RegistryChange::ActorRemoved { name, .. } if name == actor_name));
        assert!(
            enqueued,
            "cleanup_dead_peers must enqueue a RegistryChange::ActorRemoved so the \
             removal propagates via gossip to peers holding a stale cached copy"
        );
    }

    /// Companion regression test that demonstrates the underlying
    /// race-window pattern is exploitable. Mirrors the pre-fix
    /// apply_delta ordering: a lockless `known_actors.upsert_sync`
    /// write, then a cleanup pass walks `peer_to_actors[sender]` and
    /// tears the `known_actors` entry out, then a separate locked
    /// `peer_to_actors[sender]` insert finishes the apply. This MUST
    /// produce an inconsistency — the assertion confirms our threat
    /// model, and locks in the requirement that future refactors of
    /// `apply_delta` keep both writes inside a single gossip_state
    /// critical section.
    #[tokio::test]
    async fn test_two_phase_apply_pattern_breaks_invariant() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::<()>::new(test_addr(7110), config);

        let sender_peer_id = test_peer_id("two-phase-pattern");
        let sender_addr = test_addr(7111);
        let actor_name = "actor.twophase";

        {
            let mut state = registry.gossip_state.lock().await;
            state.peers.insert(
                sender_addr,
                PeerInfo {
                    address: sender_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(sender_peer_id.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp().saturating_sub(10)),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: 0,
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
            let mut seeded = HashSet::new();
            seeded.insert(actor_name.to_string());
            state.peer_to_actors.insert(sender_addr, seeded);
        }
        let loc = RemoteActorLocation::new_with_peer(sender_addr, sender_peer_id.clone());
        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), loc.clone());

        // (P1) lockless known_actors upsert (the pre-fix apply_delta
        // did this without holding gossip_state).
        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), loc);

        // Mid-flight cleanup pass — tears the entry from known_actors.
        registry.cleanup_dead_peers().await;

        // (P2) locked peer_to_actors insert.
        {
            let mut state = registry.gossip_state.lock().await;
            state
                .peer_to_actors
                .entry(sender_addr)
                .or_insert_with(HashSet::new)
                .insert(actor_name.to_string());
        }

        // This pattern leaves the maps inconsistent — confirming the
        // bug would manifest if apply_delta ever returns to its
        // two-phase shape.
        let state = registry.gossip_state.lock().await;
        let listed = state
            .peer_to_actors
            .get(&sender_addr)
            .map(|s| s.contains(actor_name))
            .unwrap_or(false);
        let in_known = registry.actor_state.known_actors.contains_sync(actor_name);
        assert!(
            listed,
            "two-phase pattern should leave peer_to_actors referencing the actor"
        );
        assert!(
            !in_known,
            "two-phase pattern should leave known_actors missing the actor — \
             this asserts that the race window is real; the fix in apply_delta \
             closes it by performing both writes under the same lock"
        );
    }

    /// Regression for the cleanup-vs-apply_delta race. `apply_delta`
    /// historically wrote `known_actors` (lockless scc) and
    /// `peer_to_actors` (gossip_state-protected) in two separate
    /// phases. A `cleanup_dead_peers` pass running between them walked
    /// `peer_to_actors[sender]` from the previous round and removed
    /// the `known_actors` entry that the in-flight `apply_delta` was
    /// about to track in `peer_to_actors`, leaving the two maps
    /// inconsistent.
    ///
    /// The fix performs both writes under the same gossip_state lock
    /// so `cleanup_dead_peers` is serialised against the whole
    /// apply_delta critical section. To deterministically expose the
    /// race we hold the gossip_state lock manually while spawning
    /// apply_delta. With the pre-fix code, apply_delta's lockless
    /// phase-1 writes complete during the wait; we then mutate the
    /// protected state to simulate cleanup running while still holding
    /// the lock, release, and let apply_delta finish. With the fix,
    /// apply_delta cannot make any writes until we release the lock,
    /// so the simulated cleanup runs entirely before apply_delta and
    /// the invariant holds.
    #[tokio::test]
    async fn test_apply_delta_and_cleanup_dead_peers_preserve_invariant() {
        let mut config = test_config();
        // Short dead-peer timeout so cleanup is willing to evict.
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::<()>::new(test_addr(7100), config);

        let sender_peer_id = test_peer_id("race-sender");
        let sender_addr = test_addr(7101);
        let actor_name = "actor.race";

        // Make `sender_addr` look dead AND configure the pool's
        // addr→peer_id mapping so apply_delta can resolve
        // `sender_peer_id → sender_addr` and update `peer_to_actors`.
        let _ = registry
            .connection_pool
            .addr_to_peer_id
            .upsert_sync(sender_addr, sender_peer_id.clone());
        let _ = registry
            .connection_pool
            .peer_id_to_addr
            .upsert_sync(sender_peer_id.clone(), sender_addr);
        {
            let mut state = registry.gossip_state.lock().await;
            state.peers.insert(
                sender_addr,
                PeerInfo {
                    address: sender_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(sender_peer_id.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    // Old enough to satisfy `cleanup_dead_peers`'s
                    // timeout check (50ms in our config).
                    last_failure_time: Some(current_timestamp().saturating_sub(10)),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: 0,
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
            // Seed peer_to_actors as if a previous gossip cycle from
            // this peer had registered the actor.
            let mut seeded = HashSet::new();
            seeded.insert(actor_name.to_string());
            state.peer_to_actors.insert(sender_addr, seeded);
        }
        // And seed known_actors with the same prior state.
        let prior_location =
            RemoteActorLocation::new_with_peer(sender_addr, sender_peer_id.clone());
        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), prior_location.clone());

        let registry = Arc::new(registry);

        // Build the new delta the peer is about to deliver.
        let mut new_location =
            RemoteActorLocation::new_with_peer(sender_addr, sender_peer_id.clone());
        new_location.vector_clock = prior_location.vector_clock.clone();
        new_location
            .vector_clock
            .increment(sender_peer_id.to_node_id());
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor_name.to_string(),
                location: new_location,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: sender_peer_id.clone(),
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };

        // Hold gossip_state so cleanup-style mutations can be staged
        // before apply_delta gets a chance to write peer_to_actors.
        let mut held = registry.gossip_state.lock().await;

        // Kick off apply_delta. With the bug it will run its lockless
        // `known_actors.upsert_sync(...)` phase to completion and then
        // park on the gossip_state lock. With the fix it cannot touch
        // `known_actors` at all until we release.
        let r_apply = Arc::clone(&registry);
        let apply_handle = tokio::spawn(async move {
            let _ = r_apply.apply_delta(delta).await;
        });

        // Give apply_delta enough opportunity to execute its phase-1
        // writes (under the bug) and reach the lock().await yield
        // point. Re-locking yields control to the scheduler.
        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        // Simulate the body of `cleanup_dead_peers` while still
        // holding the lock: walk peer_to_actors[sender_addr] and
        // remove every referenced entry from `known_actors`. This is
        // what the real cleanup pass would do and is exactly what
        // tears the in-flight delta apart in the pre-fix code.
        if let Some(actors) = held.peer_to_actors.get(&sender_addr).cloned() {
            for name in &actors {
                let _ = registry.actor_state.known_actors.remove_sync(name.as_str());
            }
            held.peer_to_actors.remove(&sender_addr);
        }
        drop(held);

        // Let apply_delta finish (it must complete; otherwise the
        // test would hang and that itself is a regression).
        tokio::time::timeout(std::time::Duration::from_secs(2), apply_handle)
            .await
            .expect("apply_delta did not finish in time")
            .unwrap();

        // Invariant: every name in `peer_to_actors[a]` must exist in
        // `known_actors`. Pre-fix, the spawned apply_delta has now
        // inserted the actor back into peer_to_actors via its
        // phase-2 write, but we already removed it from known_actors.
        let state = registry.gossip_state.lock().await;
        for (peer, actors) in &state.peer_to_actors {
            for name in actors {
                assert!(
                    registry
                        .actor_state
                        .known_actors
                        .contains_sync(name.as_str()),
                    "peer_to_actors[{}] references actor {:?} but known_actors has no entry",
                    peer,
                    name
                );
            }
        }
    }

    #[tokio::test]
    async fn apply_delta_revalidates_current_actor_before_upsert() {
        let registry = Arc::new(GossipRegistry::<()>::new(test_addr(7120), test_config()));
        let actor_name = "actor.delta.revalidate";
        let stale_peer = test_peer_id("stale-delta-owner");
        let fresh_peer = test_peer_id("fresh-delta-owner");

        let stale = RemoteActorLocation::new_with_peer(test_addr(7121), stale_peer.clone());
        stale.vector_clock.increment(stale_peer.to_node_id());
        let mut fresh = RemoteActorLocation::new_with_peer(test_addr(7122), fresh_peer.clone());
        fresh.vector_clock = stale.vector_clock.clone();
        fresh.vector_clock.increment(fresh_peer.to_node_id());

        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor_name.to_string(),
                location: stale,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: stale_peer,
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };

        let held = registry.gossip_state.lock().await;
        let r_apply = Arc::clone(&registry);
        let apply_handle = tokio::spawn(async move {
            r_apply.apply_delta(delta).await.unwrap();
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), fresh.clone());
        drop(held);

        tokio::time::timeout(Duration::from_secs(2), apply_handle)
            .await
            .expect("apply_delta did not finish in time")
            .unwrap();

        let got = read_known_actor(&registry, actor_name).expect("actor should remain present");
        assert_eq!(
            got.peer_id, fresh.peer_id,
            "stale delta must not overwrite actor ownership that changed while it waited for gossip_state"
        );
    }

    #[tokio::test]
    async fn cleanup_dead_peers_reaps_clock_calibration_side_tables() {
        // ACTOR_REM_2 R13(c): the per-peer clock-calibration tables must be
        // reaped when a peer is cleaned up, or they leak one orphan per departed
        // peer for the process lifetime.
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = Arc::new(GossipRegistry::<()>::new(test_addr(7160), config));
        let dead_peer = test_peer_id("r13c-dead");
        let dead_addr = test_addr(7161);

        {
            let mut state = registry.gossip_state.lock().await;
            state.peers.insert(
                dead_addr,
                PeerInfo {
                    address: dead_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(dead_peer.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 1,
                    last_sent_sequence: 1,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp().saturating_sub(10)),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
        }

        // Seed all three addr-keyed clock tables for the dead peer.
        let _ = registry.clock_probe_state.upsert_sync(
            dead_addr,
            PeerClockProbeState {
                last_probe_sent_wall_ns: 1,
            },
        );
        let _ = registry.pending_clock_echoes.upsert_sync(
            dead_addr,
            PendingClockEcho {
                sample_id: 1,
                origin_sender_wall_ns: 1,
                responder_recv_wall_ns: 2,
            },
        );
        let _ = registry.peer_clock_snapshots.upsert_sync(
            dead_addr,
            PeerClockSnapshot {
                peer_addr: dead_addr,
                sample_id: 1,
                offset_ns: 0,
                rtt_ns: 1,
                error_bound_ns: 0,
                sampled_at_wall_ns: 1,
                sample_count: 1,
            },
        );
        assert!(registry.clock_probe_state.contains_sync(&dead_addr));

        registry.cleanup_dead_peers().await;

        assert!(
            !registry.clock_probe_state.contains_sync(&dead_addr),
            "R13(c): clock_probe_state leaked after peer cleanup"
        );
        assert!(
            !registry.pending_clock_echoes.contains_sync(&dead_addr),
            "R13(c): pending_clock_echoes leaked after peer cleanup"
        );
        assert!(
            !registry.peer_clock_snapshots.contains_sync(&dead_addr),
            "R13(c): peer_clock_snapshots leaked after peer cleanup"
        );
    }

    #[tokio::test]
    async fn take_clock_echo_flushes_owed_echo_for_undialable_inbound_only_peer() {
        // ACTOR_REM_2 R16i: a permanently inbound-only / NAT'd peer is never
        // dialed outbound (`should_suppress_outbound_retry_for_peer`), so an echo
        // owed from its probe never flushes via `gossip_extensions_for_outbound`.
        // `take_clock_echo_for_undialable_peer` must hand that echo back so the
        // caller can answer inline; for a dialable peer it must leave the echo
        // queued for the normal outbound flush.
        let mut config = test_config();
        config.nat_role_reconnect_enabled = true;
        let registry = Arc::new(GossipRegistry::<()>::new(test_addr(7180), config));

        // Private-IP peer while we bind loopback => not practically dialable.
        let nat_addr: SocketAddr = "10.44.0.9:9000".parse().unwrap();
        // Loopback peer => dialable from our loopback bind.
        let dialable_addr = test_addr(7181);

        {
            let mut state = registry.gossip_state.lock().await;
            for addr in [nat_addr, dialable_addr] {
                state.peers.insert(
                    addr,
                    PeerInfo {
                        address: addr,
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
                        last_response_received_ms: crate::current_timestamp_millis(),
                        accept_lower_sequence_from: None,
                        current_session_source: None,
                        current_session_connection: None,
                        current_session_epoch: 0,
                    },
                );
            }
        }

        // Both peers owe an echo (they each just probed us).
        for addr in [nat_addr, dialable_addr] {
            let _ = registry.pending_clock_echoes.upsert_sync(
                addr,
                PendingClockEcho {
                    sample_id: 42,
                    origin_sender_wall_ns: 1_000,
                    responder_recv_wall_ns: 1_200,
                },
            );
        }

        // Undialable inbound-only peer: echo is flushed inline.
        let flushed = registry
            .take_clock_echo_for_undialable_peer(nat_addr, 5_000)
            .await
            .expect("owed echo for an undialable inbound-only peer must be handed back");
        let echo = flushed.clock_echo.expect("must carry the clock echo");
        assert_eq!(echo.sample_id, 42);
        assert_eq!(echo.origin_sender_wall_ns, 1_000);
        assert_eq!(echo.responder_recv_wall_ns, 1_200);
        assert_eq!(echo.responder_send_wall_ns, 5_000);
        assert!(
            flushed.clock_probe.is_none(),
            "inline answer must not initiate a probe"
        );
        assert!(
            !registry.pending_clock_echoes.contains_sync(&nat_addr),
            "flushed echo must be consumed"
        );

        // Dialable peer: leave the echo queued for the normal outbound round.
        assert!(
            registry
                .take_clock_echo_for_undialable_peer(dialable_addr, 5_000)
                .await
                .is_none(),
            "a dialable peer's echo must remain queued for the outbound flush"
        );
        assert!(
            registry.pending_clock_echoes.contains_sync(&dialable_addr),
            "dialable peer's echo must not be consumed"
        );
    }

    #[tokio::test]
    async fn cleanup_dead_peers_does_not_prune_actor_that_moved_to_another_peer() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = Arc::new(GossipRegistry::<()>::new(test_addr(7130), config));
        let actor_name = "actor.peerdeath.moved";
        let dead_peer = test_peer_id("dead-peer");
        let live_peer = test_peer_id("live-peer");
        let dead_addr = test_addr(7131);

        let dead_location = RemoteActorLocation::new_with_peer(dead_addr, dead_peer.clone());
        let live_location = RemoteActorLocation::new_with_peer(test_addr(7132), live_peer.clone());
        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), dead_location);
        {
            let mut state = registry.gossip_state.lock().await;
            state.peers.insert(
                dead_addr,
                PeerInfo {
                    address: dead_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(dead_peer.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 1,
                    last_sent_sequence: 1,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp().saturating_sub(10)),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
            let mut actors = HashSet::new();
            actors.insert(actor_name.to_string());
            state.peer_to_actors.insert(dead_addr, actors);
        }

        let _ = registry
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), live_location.clone());

        registry.cleanup_dead_peers().await;

        let got = read_known_actor(&registry, actor_name).expect("moved actor must survive");
        assert_eq!(
            got.peer_id, live_location.peer_id,
            "timeout cleanup must re-check current ownership before pruning stale side-table entries"
        );
        assert!(
            !registry
                .actor_state
                .removed_actors
                .contains_sync(actor_name),
            "timeout cleanup must not tombstone an actor that already moved"
        );
        let state = registry.gossip_state.lock().await;
        assert!(
            !state.peer_to_actors.contains_key(&dead_addr),
            "timeout cleanup should still clear the stale peer_to_actors side table"
        );
    }

    /// R2 end-to-end regression: node B reaps `svc-x` (owned by dead peer A)
    /// via `cleanup_dead_peers`. Peer C then delivers a delta still
    /// containing the ORIGINAL, un-restamped `svc-x` -> A entry (as if C's
    /// cache had not yet observed the removal). Without a dominating
    /// tombstone from the reap, B re-admits the actor and `lookup_actor`
    /// starts routing to the confirmed-dead node A again.
    #[tokio::test]
    async fn cleanup_dead_peers_tombstone_survives_stale_replay_from_third_peer() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let node_b = Arc::new(GossipRegistry::<()>::new(test_addr(7180), config));
        let actor_name = "svc-x";
        let peer_a = test_peer_id("r2-e2e-peer-a-dead");
        let peer_c = test_peer_id("r2-e2e-peer-c-replay");
        let addr_a = test_addr(7181);

        // B currently knows about svc-x hosted on A.
        let location_from_a = RemoteActorLocation::new_with_peer(addr_a, peer_a.clone());
        let _ = node_b
            .actor_state
            .known_actors
            .upsert_sync(actor_name.to_string(), location_from_a.clone());

        // A is registered as a peer of B and is failed/dead long enough to
        // cross dead_peer_timeout.
        {
            let mut gossip_state = node_b.gossip_state.lock().await;
            gossip_state.peers.insert(
                addr_a,
                PeerInfo {
                    address: addr_a,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: false,
                    node_id: Some(peer_a.to_node_id()),
                    dns_name: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp().saturating_sub(10)),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
                },
            );
            let mut actors = HashSet::new();
            actors.insert(actor_name.to_string());
            gossip_state.peer_to_actors.insert(addr_a, actors);
        }

        // B reaps svc-x from dead peer A.
        node_b.cleanup_dead_peers().await;
        assert!(
            node_b.lookup_actor(actor_name).await.is_none(),
            "sanity: svc-x must be gone from B immediately after the reap"
        );

        // Peer C now replays a delta with the stale, original (un-restamped)
        // svc-x -> A entry, as if its own cache had not caught up yet.
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor_name.to_string(),
                location: location_from_a,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: peer_c,
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        };
        let _ = node_b.apply_delta(delta).await;

        assert!(
            node_b.lookup_actor(actor_name).await.is_none(),
            "B must not re-admit svc-x from a stale third-party replay after \
             cleanup_dead_peers reaped it — the reap must have left a dominating \
             tombstone behind, or the fast dead-peer reclaim silently degrades to \
             the 24h actor_ttl backstop"
        );
    }

    #[tokio::test]
    async fn register_actor_rolls_back_if_shutdown_wins_second_check() {
        let registry = Arc::new(GossipRegistry::<()>::new(test_addr(7140), test_config()));
        let actor_name = "actor.shutdown.rollback";
        let held = registry.gossip_state.lock().await;
        let r_register = Arc::clone(&registry);
        let register_handle = tokio::spawn(async move {
            r_register
                .register_actor_with_priority(
                    actor_name.to_string(),
                    test_location(test_addr(7141)),
                    RegistrationPriority::Normal,
                )
                .await
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        registry.shutdown.store(true, Ordering::Release);
        drop(held);

        let err = tokio::time::timeout(Duration::from_secs(2), register_handle)
            .await
            .expect("register did not finish in time")
            .unwrap()
            .expect_err("registration should observe shutdown");
        assert!(matches!(err, GossipError::Shutdown));
        assert!(
            !registry.actor_state.local_actors.contains_sync(actor_name),
            "failed registration must not leave a local actor behind"
        );
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

        // PEER_ID_REFACTOR §1.6: every location in this payload is owned by
        // a peer OTHER than the sender, so it is a relayed, unauthenticated
        // claim about a third party's reachability. Relayed locations are
        // stored (asserted above, §1.5) but must never pin an addr→GossipNodeId
        // route or a dial hint — that is the dial-route poisoning vector.
        assert_eq!(registry.lookup_node_id(&test_addr(9001)).await, None);
        assert_eq!(registry.lookup_node_id(&test_addr(9002)).await, None);
        assert_eq!(registry.lookup_node_id(&test_addr(9003)).await, None);

        let remote_actor1 = registry
            .actor_state
            .known_actors
            .read_sync("remote_actor1", |_, location| location.clone())
            .expect("remote_actor1 location");
        let actor1_addr = registry
            .connection_pool
            .peer_id_to_addr
            .read_sync(&remote_actor1.peer_id, |_, v| *v);
        assert_eq!(
            actor1_addr, None,
            "relayed third-party locations must not plant dial routes (§1.6)"
        );

        // Owner-sent locations DO pin the route: the sender advertising its
        // own actor is the authenticated source for its own reachability.
        let owner = test_peer_id("merge_full_sync_owner");
        let mut own_local = HashMap::new();
        own_local.insert(
            "owner_actor".to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9004), owner.clone()),
        );
        registry
            .merge_full_sync(
                own_local,
                HashMap::new(),
                owner.clone(),
                test_addr(8082),
                1,
                current_timestamp(),
            )
            .await;
        assert_eq!(
            registry.lookup_node_id(&test_addr(9004)).await,
            Some(owner.to_node_id()),
            "owner-sent locations pin the expected NodeId for TLS verification"
        );
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
        let recorded_at = Instant::now();
        let delta = HistoricalDelta {
            sequence: 10,
            changes: vec![],
            recorded_at,
        };

        assert_eq!(delta.sequence, 10);
        assert!(delta.changes.is_empty());
        assert_eq!(delta.recorded_at, recorded_at);
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

    /// R-12(a): `enforce_bounds` hardcoded `let max_peers = 1000;`, ignoring
    /// `config.max_peers` entirely. An operator capping the peer table at 10
    /// got no bound at all until the table passed 1000.
    #[tokio::test]
    async fn qa_r12_enforce_bounds_honours_configured_max_peers() {
        let config = GossipConfig {
            max_peers: 10,
            ..test_config()
        };
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            for i in 0..25u16 {
                let addr = test_addr(9000 + i);
                let mut peer = PeerInfo::local(addr);
                peer.last_success = 1_000 + u64::from(i);
                gossip_state.peers.insert(addr, peer);
            }
        }

        registry.enforce_bounds().await;

        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(
            gossip_state.peers.len(),
            10,
            "R-12: enforce_bounds must honour config.max_peers, not a hardcoded 1000"
        );
    }

    /// R-12(a): eviction sorted by `last_success` alone with no exemption, so
    /// the peers most likely to be evicted -- inbound-only peers, whose
    /// `last_success` never advances on the key they are stored under -- were
    /// exactly the live ones. Eviction drops `peer_to_actors`, fires
    /// `on_peer_disconnected`, and destroys `node_id`/`last_sequence`.
    ///
    /// Tests the policy function directly: standing up 15 real pooled
    /// connections is not needed to pin the selection rule.
    #[test]
    fn qa_r12_live_peers_never_evicted() {
        let mut peers = HashMap::new();
        for i in 0..15u16 {
            let addr = test_addr(9000 + i);
            let mut peer = PeerInfo::local(addr);
            // The five OLDEST-contact peers are the live ones -- precisely the
            // set the old oldest-first policy would have evicted.
            peer.last_success = 1_000 + u64::from(i);
            peers.insert(addr, peer);
        }
        let live: std::collections::HashSet<SocketAddr> =
            (0..5u16).map(|i| test_addr(9000 + i)).collect();

        let evicted =
            GossipRegistry::<()>::select_peers_to_evict(&peers, 5, |addr, _| live.contains(addr));

        assert_eq!(evicted.len(), 5, "should still evict the requested count");
        for addr in &evicted {
            assert!(
                !live.contains(addr),
                "R-12: live peer {addr} must never be evicted"
            );
        }
        // The oldest *evictable* peers go first.
        let mut sorted = evicted.clone();
        sorted.sort();
        let expected: Vec<SocketAddr> = (5..10u16).map(|i| test_addr(9000 + i)).collect();
        assert_eq!(sorted, expected, "R-12: evict oldest evictable peers first");
    }

    /// R-12(a): when the entire excess is exempt, evict nothing rather than
    /// forcing a live peer out to satisfy the bound.
    #[test]
    fn qa_r12_all_exempt_evicts_nothing() {
        let mut peers = HashMap::new();
        for i in 0..12u16 {
            let addr = test_addr(9000 + i);
            peers.insert(addr, PeerInfo::local(addr));
        }

        let evicted = GossipRegistry::<()>::select_peers_to_evict(&peers, 2, |_, _| true);

        assert!(
            evicted.is_empty(),
            "R-12: an all-exempt peer table must not be force-evicted"
        );
    }

    /// R-12 (review P1, codex): an untrusted peer must not be able to buy
    /// eviction exemption by ADVERTISING a configured address.
    ///
    /// `peer.address` / `peer.peer_address` are peer-influenced (B-5). If the
    /// configured-peer check trusted them, any inbound peer could claim a
    /// configured address to become exempt, and repeat that across many
    /// entries to bypass `max_peers` entirely -> unbounded memory growth.
    /// Configuration is therefore matched on trusted identity only.
    #[tokio::test]
    async fn qa_r12_advertised_alias_does_not_grant_configured_exemption() {
        let config = GossipConfig {
            max_peers: 2,
            ..test_config()
        };
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);
        let configured_addr = test_addr(9999);

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            for i in 0..6u16 {
                let key = test_addr(9500 + i);
                let mut peer = PeerInfo::local(key);
                // Every peer LIES, claiming the configured address as its own
                // advertised aliases.
                peer.address = configured_addr;
                peer.peer_address = Some(configured_addr);
                peer.last_success = 1_000 + u64::from(i);
                gossip_state.peers.insert(key, peer);
            }
        }

        registry.enforce_bounds().await;

        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(
            gossip_state.peers.len(),
            2,
            "R-12: peers must not become eviction-exempt by advertising a \
             configured address; the bound must still be enforced"
        );
    }

    /// R-12(b): the bound used `truncate`, which keeps the head and drops the
    /// TAIL -- the most recent changes. `ActorRemoved` is not carried by
    /// FullSync, so a burst that overflowed the bound lost removal propagation
    /// entirely and the stale actor survived until the 24h TTL.
    #[tokio::test]
    async fn qa_r12_removed_burst_still_propagates() {
        let registry = GossipRegistry::<()>::new(test_addr(8080), test_config());

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            // 1000 older additions...
            for i in 0..1000 {
                gossip_state
                    .pending_changes
                    .push(RegistryChange::ActorAdded {
                        name: format!("actor{i}"),
                        location: test_location(test_addr(9000)),
                        priority: RegistrationPriority::Normal,
                    });
            }
            // ...then the 100 most recent changes, all removals.
            for i in 0..100 {
                gossip_state
                    .pending_changes
                    .push(RegistryChange::ActorRemoved {
                        name: format!("removed{i}"),
                        vector_clock: crate::VectorClock::new(),
                        removing_node_id: crate::KeyPair::new_for_testing("qa_r12_remover")
                            .peer_id()
                            .to_node_id(),
                        priority: RegistrationPriority::Normal,
                    });
            }
        }

        registry.enforce_bounds().await;

        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.pending_changes.len() <= 1000);

        let surviving_removals = gossip_state
            .pending_changes
            .iter()
            .filter(|c| matches!(c, RegistryChange::ActorRemoved { .. }))
            .count();
        assert_eq!(
            surviving_removals, 100,
            "R-12: the newest ActorRemoved entries must survive the bound; \
             dropping them loses removal propagation permanently"
        );
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

    #[tokio::test]
    async fn transport_only_check_peer_consensus_is_noop() {
        let mut config = test_config();
        config.peer_health_mode = PeerHealthMode::TransportOnly;
        let registry = GossipRegistry::<()>::new(test_addr(8080), config);

        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.pending_peer_failures.insert(
                test_addr(8081),
                PendingFailure {
                    first_detected: current_timestamp() - 10,
                    consensus_deadline: current_timestamp() - 5,
                    query_sent: true,
                },
            );
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

        let gossip_state = registry.gossip_state.lock().await;
        assert!(
            gossip_state
                .pending_peer_failures
                .contains_key(&test_addr(8081))
        );
        assert!(
            gossip_state
                .peer_health_reports
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

    // RED (investigate-self-connect-loop): relayed peer-list gossip that
    // describes THIS node's own advertised address (as another peer would
    // see and re-gossip it) is neither filtered by
    // `PeerDiscovery::on_peer_list_gossip` (which self-filters against
    // `local_addr`, constructed from `bind_addr` at registry construction —
    // see `GossipRegistry::new`, `PeerDiscovery::new(bind_addr, ..)`) nor by
    // `on_peer_list_gossip` itself, when `advertise_address` differs from
    // `bind_addr` (the common NAT/K8s/devnet-mesh case). The gossiped entry
    // even carries this node's own `GossipNodeId` and is still not
    // recognized as self. The candidate address is handed back to the
    // caller (`connection_pool/pool_connect.rs` `PeerListGossip` handler),
    // which dials it — driving `ConnectionPool::connect_via_stream` into a
    // self-dial: `should_keep_connection` (registry.rs) is hard-coded
    // `false` for `remote_peer_id == self.peer_id` regardless of direction,
    // so `wait_for_preferred_connection` (transport_stream.rs) can never
    // converge and the outbound path free-runs
    // `outbound_connect_wait_preferred_inbound` ->
    // `outbound_connect_preferred_inbound_timeout_fallback_dial` forever.
    // This test proves the gap at the discovery/self-filter boundary, one
    // level before the connection-pool livelock.
    #[tokio::test]
    async fn on_peer_list_gossip_does_not_filter_self_when_advertise_address_differs_from_bind_addr()
     {
        let config = GossipConfig {
            enable_peer_discovery: true,
            allow_loopback_discovery: true,
            advertise_address: Some(test_addr(19_100)),
            ..test_config_with_seed("self-connect-loop")
        };

        // bind_addr (19_099) != advertise_address (19_100): the same
        // bind-vs-advertise split that occurs in production whenever a node
        // binds to one address/port and advertises a different externally
        // reachable one (NAT, container port mapping, mesh overlay IP).
        let registry = GossipRegistry::<()>::new(test_addr(19_099), config);
        let self_advertised_addr = registry.advertised_addr();
        assert_ne!(
            self_advertised_addr,
            test_addr(19_099),
            "test setup requires advertise_address to differ from bind_addr"
        );

        let self_node_id = registry.peer_id.to_node_id();
        let peers = vec![PeerInfoGossip {
            address: self_advertised_addr.to_string(),
            peer_address: None,
            node_id: Some(self_node_id),
            failures: 0,
            last_attempt: 1,
            last_success: 1,
            dns_name: None,
        }];

        let candidates = registry
            .on_peer_list_gossip(peers, "127.0.0.1:9999", 1)
            .await;

        assert!(
            !candidates.contains(&self_advertised_addr),
            "self-connect guard gap: relayed gossip describing this node's own \
             advertised address (node_id == self.peer_id) was returned as a \
             dial candidate instead of being filtered as self. This is what \
             feeds ConnectionPool::get_connection / connect_via_stream a \
             self-dial, which then free-runs \
             outbound_connect_wait_preferred_inbound -> \
             outbound_connect_preferred_inbound_timeout_fallback_dial forever \
             because should_keep_connection(self, _) is unconditionally false \
             so wait_for_preferred_connection can never converge."
        );
    }

    // RED (self-connect regression, Guard 1 follow-up): the fix above (using
    // `advertise_address` as `PeerDiscovery`'s `local_addr`) closed the gap
    // for a relayed self-entry that carries this node's `node_id`, but it
    // reopened the ORIGINAL gap for a relayed self-entry that describes this
    // node's `bind_addr` and carries NO `node_id` at all. This is exactly
    // the shape of `PeerInfo::local`'s own self-entry (see `PeerInfo::local`,
    // which always sets `node_id: None`) once it has been relayed/gossiped
    // by another node and echoed back. Such an entry:
    //   - passes the identity self-filter in `on_peer_list_gossip`
    //     (line ~7385) because `peer_gossip.node_id != Some(self_node_id)`
    //     (it's `None`);
    //   - must therefore be caught by `PeerDiscovery`'s address-keyed
    //     self-filter instead, which has to know about BOTH `bind_addr` and
    //     `advertise_address`, not just whichever one `local_addr` happens
    //     to hold.
    // This proves `bind_addr` is filtered even when `advertise_address` is
    // configured and differs from it.
    #[tokio::test]
    async fn on_peer_list_gossip_filters_own_bind_addr_even_when_advertise_address_configured() {
        let config = GossipConfig {
            enable_peer_discovery: true,
            allow_loopback_discovery: true,
            advertise_address: Some(test_addr(19_102)),
            ..test_config_with_seed("self-connect-loop-bind-addr")
        };

        let bind_addr = test_addr(19_101);
        let registry = GossipRegistry::<()>::new(bind_addr, config);
        let self_advertised_addr = registry.advertised_addr();
        assert_ne!(
            self_advertised_addr, bind_addr,
            "test setup requires advertise_address to differ from bind_addr"
        );

        // No node_id attached — matches `PeerInfo::local`'s own self-entry
        // shape once relayed by another node.
        let peers = vec![PeerInfoGossip {
            address: bind_addr.to_string(),
            peer_address: None,
            node_id: None,
            failures: 0,
            last_attempt: 1,
            last_success: 1,
            dns_name: None,
        }];

        let candidates = registry
            .on_peer_list_gossip(peers, "127.0.0.1:9998", 1)
            .await;

        assert!(
            !candidates.contains(&bind_addr),
            "self-connect guard regression: relayed gossip describing this \
             node's own bind_addr with no node_id attached was returned as a \
             dial candidate instead of being filtered as self, even though \
             advertise_address is configured. PeerDiscovery's address-keyed \
             self-filter must reject both bind_addr and advertise_address."
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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
        assert_eq!(
            result, tcp_source,
            "remote loopback bind must not be synthesized into tcp-source-ip:bind-port"
        );
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
        assert_eq!(
            result, tcp_source,
            "remote IPv6 loopback bind must not be synthesized into tcp-source-ip:bind-port"
        );
    }

    #[test]
    fn test_resolve_peer_addr_backwards_compat_none() {
        // Test backwards compatibility - older nodes won't send sender_bind_addr
        let tcp_source: SocketAddr = "10.0.0.50:12345".parse().unwrap();
        let result = super::resolve_peer_addr(None, tcp_source);
        // Should fall back to TCP source
        assert_eq!(result, tcp_source);
    }

    #[tokio::test]
    async fn handle_gossip_response_rejects_remote_loopback_full_sync_response() {
        let registry = GossipRegistry::<()>::new("10.77.0.40:9501".parse().unwrap(), test_config());
        let peer_id = test_peer_id("remote-loopback-handle-response");
        let tcp_source: SocketAddr = "10.77.0.40:49152".parse().unwrap();
        let synthesized_self_host_addr: SocketAddr = "10.77.0.40:7777".parse().unwrap();
        let actor_name = "poisoned/handle-gossip-response";
        let poisoned_actor =
            RemoteActorLocation::new_with_peer(synthesized_self_host_addr, peer_id.clone());

        let msg = RegistryMessage::FullSyncResponse {
            local_actors: vec![(actor_name.to_string(), poisoned_actor)],
            known_actors: Vec::new(),
            sender_peer_id: peer_id,
            sender_bind_addr: Some("127.0.0.1:7777".to_string()),
            sequence: 1,
            wall_clock_time: current_timestamp(),
            extensions: None,
        };

        registry
            .handle_gossip_response(tcp_source, msg)
            .await
            .expect("non-dialable response should be ignored without crashing");

        let state = registry.gossip_state.lock().await;
        assert!(
            !state.peers.contains_key(&synthesized_self_host_addr),
            "remote loopback response bind must not synthesize a same-host peer entry"
        );
        drop(state);

        assert!(
            registry.lookup_actor(actor_name).await.is_none(),
            "actors from a non-dialable FullSyncResponse must not be merged"
        );
    }

    fn read_known_actor(registry: &GossipRegistry, name: &str) -> Option<RemoteActorLocation> {
        registry
            .actor_state
            .known_actors
            .read_sync(name, |_, location| location.clone())
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
    async fn test_merge_full_sync_equal_clock_add_add_is_order_independent() {
        let config = test_config();
        let reg1 = GossipRegistry::<()>::new(test_addr(7021), config.clone());
        let reg2 = GossipRegistry::<()>::new(test_addr(7022), config.clone());

        let actor = "actor.fullsync.equal-clock";
        let peer_a = test_peer_id("fullsync_peer_a");
        let peer_b = test_peer_id("fullsync_peer_b");

        let mut loc_a = RemoteActorLocation::new_with_peer(test_addr(9021), peer_a.clone());
        let mut loc_b = RemoteActorLocation::new_with_peer(test_addr(9022), peer_b.clone());

        // Force equal vector-clock comparison and stable non-clock tie-breaks.
        loc_a.wall_clock_time = 123;
        loc_b.wall_clock_time = 123;
        loc_a.metadata = vec![4, 5, 6];
        loc_b.metadata = vec![4, 5, 6];
        loc_a.local_registration_time = 0;
        loc_b.local_registration_time = 0;

        let expected = if stable_concurrent_location_wins(&loc_a, &loc_b) {
            loc_a.clone()
        } else {
            loc_b.clone()
        };

        let mut sync_a = HashMap::new();
        sync_a.insert(actor.to_string(), loc_a.clone());
        let mut sync_b = HashMap::new();
        sync_b.insert(actor.to_string(), loc_b.clone());

        reg1.merge_full_sync(
            sync_a.clone(),
            HashMap::new(),
            peer_a.clone(),
            test_addr(7023),
            1,
            0,
        )
        .await;
        reg1.merge_full_sync(
            sync_b.clone(),
            HashMap::new(),
            peer_b.clone(),
            test_addr(7024),
            1,
            0,
        )
        .await;
        let got1 = read_known_actor(&reg1, actor).expect("actor should exist");

        reg2.merge_full_sync(sync_b, HashMap::new(), peer_b, test_addr(7025), 1, 0)
            .await;
        reg2.merge_full_sync(sync_a, HashMap::new(), peer_a, test_addr(7026), 1, 0)
            .await;
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
    async fn successful_gossip_response_clears_accumulated_soft_failures() {
        let reg = GossipRegistry::<()>::new(test_addr(7050), test_config());
        let peer_addr = test_addr(7051);
        let peer_id = test_peer_id("soft-failure-peer");
        {
            let mut state = reg.gossip_state.lock().await;
            state.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    inbound_observed: false,
                    outbound_dial_success: true,
                    node_id: Some(peer_id.to_node_id()),
                    dns_name: None,
                    failures: 2,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(1),
                    last_dns_refresh_attempt: None,
                    last_response_received_ms: crate::current_timestamp_millis()
                        .saturating_sub(10_000),
                        accept_lower_sequence_from: None,
                        current_session_source: None,
                        current_session_connection: None,
                        current_session_epoch: 0,
                },
            );
        }

        let response = RegistryMessage::DeltaGossipResponse {
            delta: RegistryDelta {
                since_sequence: 0,
                current_sequence: 1,
                changes: Vec::new(),
                sender_peer_id: peer_id,
                wall_clock_time: 0,
                precise_timing_nanos: 0,
            },
            extensions: None,
        };
        reg.apply_gossip_results(vec![GossipResult {
            peer_addr,
            sent_sequence: 1,
            outcome: Ok(Some(response)),
        }])
        .await;

        let state = reg.gossip_state.lock().await;
        let peer = state.peers.get(&peer_addr).expect("peer remains tracked");
        assert_eq!(
            peer.failures, 0,
            "an application-level response must clear soft no-response failures"
        );
        assert!(peer.last_failure_time.is_none());
    }

    #[tokio::test]
    async fn apply_delta_records_tombstone_when_remove_arrives_before_add() {
        let reg = GossipRegistry::<()>::new(test_addr(7052), test_config());
        let actor = "actor.remove.before.add";
        let peer = test_peer_id("remove-before-add-peer");
        let node = peer.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9252), peer.clone());
        loc.vector_clock.increment(node);
        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(node);

        reg.apply_delta(RegistryDelta {
            since_sequence: 0,
            current_sequence: 2,
            changes: vec![RegistryChange::ActorRemoved {
                name: actor.to_string(),
                vector_clock: removal_clock,
                removing_node_id: node,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: peer.clone(),
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        })
        .await
        .unwrap();

        reg.apply_delta(RegistryDelta {
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
        })
        .await
        .unwrap();

        assert!(
            read_known_actor(&reg, actor).is_none(),
            "stale add must not resurrect an actor whose remove arrived first"
        );
    }

    #[tokio::test]
    async fn merge_full_sync_respects_actor_tombstones() {
        let reg = GossipRegistry::<()>::new(test_addr(7053), test_config());
        let actor = "actor.fullsync.tombstone";
        let peer = test_peer_id("fullsync-tombstone-peer");
        let node = peer.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9253), peer.clone());
        loc.vector_clock.increment(node);
        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        let mut remote_local = HashMap::new();
        remote_local.insert(actor.to_string(), loc);
        reg.merge_full_sync(remote_local, HashMap::new(), peer, test_addr(7054), 1, 0)
            .await;

        assert!(
            read_known_actor(&reg, actor).is_none(),
            "stale full sync must not resurrect a tombstoned actor"
        );
    }

    #[tokio::test]
    async fn merge_full_sync_rejects_third_party_replay_after_peer_death_tombstone() {
        let reg = GossipRegistry::<()>::new(test_addr(7055), test_config());
        let actor = "actor.fullsync.third-party-replay";
        let peer = test_peer_id("fullsync-third-party-replay-peer");
        let reflector = test_peer_id("fullsync-third-party-reflector");
        let owner_node = peer.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9255), peer.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        let mut remote_known = HashMap::new();
        remote_known.insert(actor.to_string(), loc);
        reg.merge_full_sync(
            HashMap::new(),
            remote_known,
            reflector,
            test_addr(7056),
            1,
            0,
        )
        .await;

        assert!(
            read_known_actor(&reg, actor).is_none(),
            "third-party full sync must not clear a peer-death tombstone for \
             an actor owned by another peer"
        );
    }

    #[tokio::test]
    async fn merge_full_sync_allows_direct_owner_recovery_after_transient_disconnect() {
        let reg = GossipRegistry::<()>::new(test_addr(7057), test_config());
        let actor = "actor.fullsync.stale-direct-owner";
        let peer = test_peer_id("fullsync-stale-direct-owner-peer");
        let owner_node = peer.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9257), peer.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        let mut remote_local = HashMap::new();
        remote_local.insert(actor.to_string(), loc);
        reg.merge_full_sync(remote_local, HashMap::new(), peer, test_addr(7058), 1, 0)
            .await;

        assert!(
            read_known_actor(&reg, actor).is_some(),
            "direct owner full sync must recover from a peer-death tombstone after \
             an authenticated reconnect"
        );
    }

    #[tokio::test]
    async fn merge_full_sync_allows_direct_owner_recovery_after_owner_clock_advances() {
        let reg = GossipRegistry::<()>::new(test_addr(7059), test_config());
        let actor = "actor.fullsync.recovered-owner";
        let peer = test_peer_id("fullsync-recovered-owner-peer");
        let owner_node = peer.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9259), peer.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        loc.vector_clock.increment(owner_node);
        let mut remote_local = HashMap::new();
        remote_local.insert(actor.to_string(), loc);
        reg.merge_full_sync(remote_local, HashMap::new(), peer, test_addr(7060), 1, 0)
            .await;

        assert!(
            read_known_actor(&reg, actor).is_some(),
            "direct owner full sync may recover after the owner publishes a newer actor version"
        );
    }

    #[tokio::test]
    async fn apply_delta_rejects_third_party_replay_after_peer_death_tombstone() {
        let reg = GossipRegistry::<()>::new(test_addr(7061), test_config());
        let actor = "actor.delta.third-party-replay";
        let peer = test_peer_id("delta-third-party-replay-peer");
        let reflector = test_peer_id("delta-third-party-reflector");
        let owner_node = peer.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9259), peer.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        reg.apply_delta(RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor.to_string(),
                location: loc,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: reflector,
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        })
        .await
        .unwrap();

        assert!(
            read_known_actor(&reg, actor).is_none(),
            "third-party delta must not clear a peer-death tombstone for \
            an actor owned by another peer"
        );
    }

    #[tokio::test]
    async fn protocol_rejects_forged_delta_sender_peer_id() {
        let reg = Arc::new(GossipRegistry::<()>::new(test_addr(7064), test_config()));
        let actor = "actor.delta.forged-sender";
        let owner = test_peer_id("delta-forged-owner");
        let attacker = test_peer_id("delta-forged-attacker");
        let owner_node = owner.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9264), owner.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        let mut streaming_state = crate::protocol::StreamingState::new();
        crate::protocol::process_read_result(
            crate::handle::MessageReadResult::Gossip(
                RegistryMessage::DeltaGossip {
                    delta: RegistryDelta {
                        since_sequence: 0,
                        current_sequence: 1,
                        changes: vec![RegistryChange::ActorAdded {
                            name: actor.to_string(),
                            location: loc,
                            priority: RegistrationPriority::Normal,
                        }],
                        sender_peer_id: owner,
                        wall_clock_time: 0,
                        precise_timing_nanos: 0,
                    },
                    extensions: None,
                },
                None,
            ),
            &mut streaming_state,
            &reg,
            test_addr(7065),
            test_addr(7065),
            None,
            None,
            Some(&attacker),
        )
        .await
        .unwrap();

        assert!(
            read_known_actor(&reg, actor).is_none(),
            "authenticated attacker must not clear another peer's tombstone by \
             forging delta.sender_peer_id"
        );
        assert!(reg.actor_state.removed_actors.contains_sync(actor));
    }

    #[tokio::test]
    async fn protocol_rejects_delta_without_authenticated_identity() {
        // Fail-closed: a gossip delta carrying a claimed sender must be dropped
        // when the connection has no authenticated identity to verify it
        // against, not accepted with the forgeable `sender_peer_id` trusted.
        let reg = Arc::new(GossipRegistry::<()>::new(test_addr(7066), test_config()));
        let actor = "actor.delta.unauthenticated";
        let owner = test_peer_id("delta-unauth-owner");
        let owner_node = owner.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9266), owner.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        let mut streaming_state = crate::protocol::StreamingState::new();
        crate::protocol::process_read_result(
            crate::handle::MessageReadResult::Gossip(
                RegistryMessage::DeltaGossip {
                    delta: RegistryDelta {
                        since_sequence: 0,
                        current_sequence: 1,
                        changes: vec![RegistryChange::ActorAdded {
                            name: actor.to_string(),
                            location: loc,
                            priority: RegistrationPriority::Normal,
                        }],
                        sender_peer_id: owner,
                        wall_clock_time: 0,
                        precise_timing_nanos: 0,
                    },
                    extensions: None,
                },
                None,
            ),
            &mut streaming_state,
            &reg,
            test_addr(7067),
            test_addr(7067),
            None,
            None,
            None, // no authenticated identity: must fail closed
        )
        .await
        .unwrap();

        assert!(
            read_known_actor(&reg, actor).is_none(),
            "delta carrying a claimed sender must be dropped when unauthenticated"
        );
        assert!(reg.actor_state.removed_actors.contains_sync(actor));
    }

    #[tokio::test]
    async fn apply_delta_allows_direct_owner_recovery_after_transient_disconnect() {
        let reg = GossipRegistry::<()>::new(test_addr(7062), test_config());
        let actor = "actor.delta.stale-direct-owner";
        let peer = test_peer_id("delta-stale-direct-owner-peer");
        let owner_node = peer.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9262), peer.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        reg.apply_delta(RegistryDelta {
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
        })
        .await
        .unwrap();

        assert!(
            read_known_actor(&reg, actor).is_some(),
            "direct owner delta must recover from a peer-death tombstone after \
             authenticated owner traffic resumes"
        );
    }

    #[tokio::test]
    async fn apply_delta_allows_direct_owner_recovery_after_owner_clock_advances() {
        let reg = GossipRegistry::<()>::new(test_addr(7063), test_config());
        let actor = "actor.delta.recovered-owner";
        let peer = test_peer_id("delta-recovered-owner-peer");
        let owner_node = peer.to_node_id();
        let observer_node = reg.peer_id.to_node_id();
        let loc = RemoteActorLocation::new_with_peer(test_addr(9263), peer.clone());
        loc.vector_clock.increment(owner_node);

        let removal_clock = loc.vector_clock.clone();
        removal_clock.increment(observer_node);
        let _ = reg
            .actor_state
            .removed_actors
            .upsert_sync(actor.to_string(), RemovedActorTombstone::new(removal_clock));

        loc.vector_clock.increment(owner_node);
        reg.apply_delta(RegistryDelta {
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
        })
        .await
        .unwrap();

        assert!(
            read_known_actor(&reg, actor).is_some(),
            "direct owner delta may recover after the owner publishes a newer actor version"
        );
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    recorded_at: Instant::now(),
                },
                HistoricalDelta {
                    sequence: 9,
                    changes: Vec::new(),
                    recorded_at: Instant::now(),
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
            RegistryMessage::DeltaGossip { delta, .. }
            | RegistryMessage::DeltaGossipResponse { delta, .. } => {
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
            last_response_received_ms: crate::current_timestamp_millis(),
            accept_lower_sequence_from: None,
            current_session_source: None,
            current_session_connection: None,
            current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
                    last_response_received_ms: crate::current_timestamp_millis(),
                    accept_lower_sequence_from: None,
                    current_session_source: None,
                    current_session_connection: None,
                    current_session_epoch: 0,
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
        let RegistryMessage::DeltaGossipResponse { delta: d1, .. } = msg1 else {
            panic!("expected delta gossip response");
        };
        let RegistryMessage::DeltaGossipResponse { delta: d2, .. } = msg2 else {
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

    #[test]
    fn peer_identity_side_table_pruning_expires_dynamic_peers_but_keeps_required_peers() {
        let registry = GossipRegistry::<()>::new(test_addr(7014), test_config());
        let departed = test_peer_id("departed-peer-id");
        let required = test_peer_id("required-peer-id");
        let now = Instant::now();
        let stale_liveness = PeerLivenessStatus {
            reachable: false,
            updated_at: now - registry.config.peer_liveness_window.saturating_mul(4),
        };

        let _ = registry
            .tie_break_cooldown_until
            .upsert_sync(departed.clone(), now - Duration::from_secs(1));
        let _ = registry.tie_break_last_eviction_at.upsert_sync(
            departed.clone(),
            now - registry
                .config
                .tie_break_reconnect_cooldown
                .saturating_mul(2),
        );
        let _ = registry
            .peer_liveness_status
            .upsert_sync(departed.clone(), stale_liveness);
        let _ = registry
            .peer_liveness_status
            .upsert_sync(required.clone(), stale_liveness);
        registry
            .connection_pool
            .set_configured_peer_addr(&required, test_addr(7015));

        registry.prune_peer_identity_side_tables();

        assert!(
            registry
                .tie_break_cooldown_until
                .read_sync(&departed, |_, _| ())
                .is_none()
        );
        assert!(
            registry
                .tie_break_last_eviction_at
                .read_sync(&departed, |_, _| ())
                .is_none()
        );
        assert!(
            registry
                .peer_liveness_status
                .read_sync(&departed, |_, _| ())
                .is_none()
        );
        assert!(
            registry
                .peer_liveness_status
                .read_sync(&required, |_, _| ())
                .is_some(),
            "configured-peer liveness edge state must survive periodic pruning"
        );
    }
}
