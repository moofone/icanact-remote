use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tracing::warn;

use crate::{GossipError, PeerId, RegistrationPriority, RemoteActorLocation, Result};

const CONTROL_PLANE_INTERVAL: Duration = Duration::from_millis(25);
const DEFAULT_TTL: u8 = 8;
const DEFAULT_SEEN_CAPACITY: usize = 16_384;
const INTEREST_PREFIX: &str = "icanact/pubsub/interest/v1";
const FAST_FRAME_MAGIC: &[u8; 4] = b"PSF1";
const FAST_FRAME_HEADER_LEN: usize = 120;
const FAST_FRAME_DEST_PEER_LEN: usize = 32;
const FAST_FRAME_POOL_BUFFERS: usize = 4096;
const FAST_FRAME_POOL_BUFFER_CAPACITY: usize = 4096;

type TopicKey = u64;
type TypeHash = u64;
type SubscriberKey = (TopicKey, TypeHash);
#[derive(Clone)]
struct SubscriberEntry {
    id: u64,
    owner: Arc<dyn Send + Sync + 'static>,
    ptr: usize,
    call: unsafe fn(usize, Bytes),
}
#[derive(Clone)]
struct BorrowedSubscriberEntry {
    id: u64,
    owner: Arc<dyn Send + Sync + 'static>,
    ptr: usize,
    call: unsafe fn(usize, &[u8], PubSubFrameMetadata),
}
#[derive(Clone)]
struct TypeSubscriberEntry {
    owner: Arc<dyn Send + Sync + 'static>,
    ptr: usize,
    call: unsafe fn(usize, u64, Bytes),
}
type SubscriberMap = HashMap<SubscriberKey, Arc<[SubscriberEntry]>>;
type BorrowedSubscriberMap = HashMap<SubscriberKey, Arc<[BorrowedSubscriberEntry]>>;
type TypeSubscriberMap = HashMap<TypeHash, Arc<[TypeSubscriberEntry]>>;
type RouteGroups = HashMap<PeerId, Arc<[PeerId]>>;
type TopicRouteGroups = HashMap<TopicKey, Arc<RouteGroups>>;

#[derive(Clone)]
struct HotBorrowedSubscriber {
    key: SubscriberKey,
    entry: BorrowedSubscriberEntry,
}

impl SubscriberEntry {
    fn new<F>(id: u64, deliver: F) -> Self
    where
        F: Fn(Bytes) + Send + Sync + 'static,
    {
        unsafe fn call_impl<F>(ptr: usize, payload: Bytes)
        where
            F: Fn(Bytes) + Send + Sync + 'static,
        {
            let deliver = unsafe { &*(ptr as *const F) };
            deliver(payload);
        }

        let owner = Arc::new(deliver);
        let ptr = Arc::as_ptr(&owner) as usize;
        let owner: Arc<dyn Send + Sync + 'static> = owner;
        Self {
            id,
            owner,
            ptr,
            call: call_impl::<F>,
        }
    }

    #[inline]
    fn deliver(&self, payload: Bytes) {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, payload) }
    }
}

impl BorrowedSubscriberEntry {
    fn new<F>(id: u64, deliver: F) -> Self
    where
        F: Fn(&[u8]) + Send + Sync + 'static,
    {
        unsafe fn call_impl<F>(ptr: usize, payload: &[u8], _metadata: PubSubFrameMetadata)
        where
            F: Fn(&[u8]) + Send + Sync + 'static,
        {
            let deliver = unsafe { &*(ptr as *const F) };
            deliver(payload);
        }

        let owner = Arc::new(deliver);
        let ptr = Arc::as_ptr(&owner) as usize;
        let owner: Arc<dyn Send + Sync + 'static> = owner;
        Self {
            id,
            owner,
            ptr,
            call: call_impl::<F>,
        }
    }

    fn new_with_metadata<F>(id: u64, deliver: F) -> Self
    where
        F: Fn(&[u8], PubSubFrameMetadata) + Send + Sync + 'static,
    {
        unsafe fn call_impl<F>(ptr: usize, payload: &[u8], metadata: PubSubFrameMetadata)
        where
            F: Fn(&[u8], PubSubFrameMetadata) + Send + Sync + 'static,
        {
            let deliver = unsafe { &*(ptr as *const F) };
            deliver(payload, metadata);
        }

        let owner = Arc::new(deliver);
        let ptr = Arc::as_ptr(&owner) as usize;
        let owner: Arc<dyn Send + Sync + 'static> = owner;
        Self {
            id,
            owner,
            ptr,
            call: call_impl::<F>,
        }
    }

    #[inline]
    fn deliver(&self, payload: &[u8], metadata: PubSubFrameMetadata) {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, payload, metadata) }
    }
}

impl TypeSubscriberEntry {
    fn new<F>(deliver: F) -> Self
    where
        F: Fn(u64, Bytes) + Send + Sync + 'static,
    {
        unsafe fn call_impl<F>(ptr: usize, topic_key: u64, payload: Bytes)
        where
            F: Fn(u64, Bytes) + Send + Sync + 'static,
        {
            let deliver = unsafe { &*(ptr as *const F) };
            deliver(topic_key, payload);
        }

        let owner = Arc::new(deliver);
        let ptr = Arc::as_ptr(&owner) as usize;
        let owner: Arc<dyn Send + Sync + 'static> = owner;
        Self {
            owner,
            ptr,
            call: call_impl::<F>,
        }
    }

    #[inline]
    fn deliver(&self, topic_key: u64, payload: Bytes) {
        let _keepalive = &self.owner;
        unsafe { (self.call)(self.ptr, topic_key, payload) }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubSubDeliveryMode {
    #[default]
    AtMostOnce,
    AtLeastOnceHopAck,
}

#[derive(Clone, Copy, Debug)]
pub struct PubSubDeliveryPolicy {
    pub hops_limit: u8,
    pub mode: PubSubDeliveryMode,
}

impl Default for PubSubDeliveryPolicy {
    fn default() -> Self {
        Self {
            hops_limit: DEFAULT_TTL,
            mode: PubSubDeliveryMode::AtMostOnce,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PubSubScope {
    LocalOnly,
    AutoExternal,
    SelectedPeers(Vec<PeerId>),
    ClusterWide,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PubSubPublishStats {
    pub local_delivered: u32,
    pub remote_attempted: u32,
    pub remote_enqueued: u32,
    pub remote_full: u32,
    pub remote_route_miss: u32,
    pub remote_transport_errors: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PubSubFrameMetadata {
    /// Sender-side Unix nanoseconds stamped immediately before the frame is
    /// enqueued to the routed PubSub transport. 0 = not provided.
    pub publisher_enqueued_ns: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PubSubIngressStats {
    pub accepted: u64,
    pub delivered_local: u64,
    pub forwarded_frames: u64,
    pub duplicate_drops: u64,
    pub ttl_drops: u64,
    pub reflection_drops: u64,
    pub route_miss_drops: u64,
    pub decode_drops: u64,
    pub queue_full_drops: u64,
}

#[derive(Default)]
struct PubSubIngressCounters {
    accepted: AtomicU64,
    delivered_local: AtomicU64,
    forwarded_frames: AtomicU64,
    duplicate_drops: AtomicU64,
    ttl_drops: AtomicU64,
    reflection_drops: AtomicU64,
    route_miss_drops: AtomicU64,
    decode_drops: AtomicU64,
    queue_full_drops: AtomicU64,
}

impl PubSubIngressCounters {
    fn snapshot(&self) -> PubSubIngressStats {
        PubSubIngressStats {
            accepted: self.accepted.load(Ordering::Relaxed),
            delivered_local: self.delivered_local.load(Ordering::Relaxed),
            forwarded_frames: self.forwarded_frames.load(Ordering::Relaxed),
            duplicate_drops: self.duplicate_drops.load(Ordering::Relaxed),
            ttl_drops: self.ttl_drops.load(Ordering::Relaxed),
            reflection_drops: self.reflection_drops.load(Ordering::Relaxed),
            route_miss_drops: self.route_miss_drops.load(Ordering::Relaxed),
            decode_drops: self.decode_drops.load(Ordering::Relaxed),
            queue_full_drops: self.queue_full_drops.load(Ordering::Relaxed),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug)]
pub struct PubSubFrameV1 {
    pub topic_key: u64,
    pub type_hash: u64,
    pub msg_id: u128,
    pub origin_peer_id: PeerId,
    pub source_peer_id: PeerId,
    pub hops_remaining: u8,
    pub mode: PubSubDeliveryMode,
    pub destination_peers: Vec<PeerId>,
    pub payload: Vec<u8>,
}

const PUBSUB_FRAME_V1_WIRE_NAME: &str = "icanact-remote.pubsub.Frame/v1";
crate::wire_type!(PubSubFrameV1, PUBSUB_FRAME_V1_WIRE_NAME);

#[derive(Archive, RkyvSerialize)]
struct PubSubFrameV1Encode<'a> {
    topic_key: u64,
    type_hash: u64,
    msg_id: u128,
    origin_peer_id: PeerId,
    source_peer_id: PeerId,
    hops_remaining: u8,
    mode: PubSubDeliveryMode,
    #[rkyv(with = rkyv::with::AsVec)]
    destination_peers: &'a [PeerId],
    #[rkyv(with = rkyv::with::AsVec)]
    payload: &'a [u8],
}

impl<'a> crate::WireType for PubSubFrameV1Encode<'a> {
    const TYPE_HASH: u64 = crate::typed::fnv1a_hash(PUBSUB_FRAME_V1_WIRE_NAME);
    const TYPE_NAME: &'static str = PUBSUB_FRAME_V1_WIRE_NAME;
}

pub trait PubSubIngressHandler: Send + Sync {
    fn handle_pubsub_frame(
        &self,
        authenticated_source_peer_id: &PeerId,
        payload: crate::AlignedBytes,
    ) -> Result<()>;
}

pub trait PubSubRouteProvider: Send + Sync {
    fn group_destinations(
        &self,
        topic_key: u64,
        destinations: &[PeerId],
    ) -> HashMap<PeerId, Arc<[PeerId]>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PubSubSubscription {
    topic_key: u64,
    type_hash: u64,
    id: u64,
}

#[derive(Clone, Debug)]
pub struct PubSubBorrowedSubscription {
    topic_key: u64,
    type_hash: u64,
    id: u64,
}

#[derive(Default)]
struct InterestState {
    local_counts: HashMap<TopicKey, usize>,
    generations: HashMap<TopicKey, u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutedPubSubMode {
    Routed,
    DirectOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutedPubSubOptions {
    pub mode: RoutedPubSubMode,
}

impl RoutedPubSubOptions {
    pub const fn routed() -> Self {
        Self {
            mode: RoutedPubSubMode::Routed,
        }
    }

    pub const fn direct_only() -> Self {
        Self {
            mode: RoutedPubSubMode::DirectOnly,
        }
    }

    #[inline]
    fn control_plane_enabled(self) -> bool {
        matches!(self.mode, RoutedPubSubMode::Routed)
    }

    #[inline]
    fn forwarding_enabled(self) -> bool {
        matches!(self.mode, RoutedPubSubMode::Routed)
    }

    #[inline]
    fn direct_duplicate_tracking(self) -> bool {
        matches!(self.mode, RoutedPubSubMode::DirectOnly)
    }

    #[inline]
    fn hot_borrowed_subscriber_enabled(self) -> bool {
        matches!(self.mode, RoutedPubSubMode::DirectOnly)
    }
}

impl Default for RoutedPubSubOptions {
    fn default() -> Self {
        Self::routed()
    }
}

#[derive(Default)]
struct SeenState {
    entries: HashSet<(PeerId, u128)>,
    order: VecDeque<(PeerId, u128)>,
    direct_last_by_origin: HashMap<PeerId, u128>,
}

impl SeenState {
    fn accept_routed(&mut self, origin: &PeerId, msg_id: u128) -> bool {
        let key = (origin.clone(), msg_id);
        if !self.entries.insert(key.clone()) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > DEFAULT_SEEN_CAPACITY
            && let Some(evicted) = self.order.pop_front()
        {
            self.entries.remove(&evicted);
        }
        true
    }

    fn accept_direct(&mut self, origin: &PeerId, msg_id: u128) -> bool {
        match self.direct_last_by_origin.get_mut(origin) {
            Some(last) if msg_id <= *last => false,
            Some(last) => {
                *last = msg_id;
                true
            }
            None => {
                self.direct_last_by_origin.insert(origin.clone(), msg_id);
                true
            }
        }
    }
}

pub struct RoutedPubSub {
    registry: Arc<crate::registry::GossipRegistry>,
    client: crate::GossipClient,
    local_peer_id: PeerId,
    options: RoutedPubSubOptions,
    subscribers: ArcSwap<SubscriberMap>,
    borrowed_subscribers: ArcSwap<BorrowedSubscriberMap>,
    hot_borrowed_subscriber: ArcSwapOption<HotBorrowedSubscriber>,
    type_subscribers: ArcSwap<TypeSubscriberMap>,
    interest_state: Arc<Mutex<InterestState>>,
    route_groups: ArcSwap<TopicRouteGroups>,
    conns: ArcSwap<HashMap<PeerId, crate::RemoteConnection>>,
    seen: Mutex<SeenState>,
    counters: PubSubIngressCounters,
    next_sub_id: AtomicU64,
    next_msg_id: AtomicU64,
    route_provider: ArcSwap<Option<Arc<dyn PubSubRouteProvider>>>,
}

/// Pre-resolved PubSub sender for a single peer.
///
/// This bypasses per-publish route grouping and peer lookup. The caller owns
/// when to refresh/rebuild the sender after reconnect.
pub struct PubSubPeerSender {
    local_peer_id: PeerId,
    destination_peer_id: PeerId,
    conn: crate::RemoteConnection,
    next_msg_id: AtomicU64,
}

impl PubSubPeerSender {
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    pub fn try_publish_bytes_with_metadata(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: &[u8],
        policy: PubSubDeliveryPolicy,
        metadata: PubSubFrameMetadata,
    ) -> Result<()> {
        if policy.hops_limit == 0 {
            return Ok(());
        }
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed) as u128;
        if let Some(datagram) = encode_fast_pubsub_datagram_pooled(
            topic_key,
            type_hash,
            msg_id,
            &self.local_peer_id,
            &self.local_peer_id,
            policy.hops_limit,
            policy.mode,
            metadata,
            std::slice::from_ref(&self.destination_peer_id),
            payload,
        ) {
            match self.conn.try_pooled_datagram(datagram) {
                Ok(()) => return Ok(()),
                Err(GossipError::Network(err))
                    if err.kind() == std::io::ErrorKind::NotConnected => {}
                Err(err) => return Err(err),
            }
        }
        let Some((frame, prefix, payload_len)) = encode_fast_frame_pooled(
            topic_key,
            type_hash,
            msg_id,
            &self.local_peer_id,
            &self.local_peer_id,
            policy.hops_limit,
            policy.mode,
            metadata,
            std::slice::from_ref(&self.destination_peer_id),
            payload,
        ) else {
            return Err(GossipError::WriteQueueFull);
        };
        self.conn
            .try_pubsub_frame_pooled(frame, prefix, payload_len)
    }
}

impl RoutedPubSub {
    pub async fn install(registry: Arc<crate::registry::GossipRegistry>) -> Arc<Self> {
        Self::install_with_options(registry, RoutedPubSubOptions::default()).await
    }

    pub async fn install_with_options(
        registry: Arc<crate::registry::GossipRegistry>,
        options: RoutedPubSubOptions,
    ) -> Arc<Self> {
        crate::typed::prewarm_pooled_byte_buffers(
            FAST_FRAME_POOL_BUFFERS,
            FAST_FRAME_POOL_BUFFER_CAPACITY,
        );
        let this = Arc::new(Self {
            local_peer_id: registry.peer_id.clone(),
            client: crate::GossipClient::from_registry(Arc::clone(&registry)),
            registry,
            options,
            subscribers: ArcSwap::from_pointee(HashMap::new()),
            borrowed_subscribers: ArcSwap::from_pointee(HashMap::new()),
            hot_borrowed_subscriber: ArcSwapOption::empty(),
            type_subscribers: ArcSwap::from_pointee(HashMap::new()),
            interest_state: Arc::new(Mutex::new(InterestState::default())),
            route_groups: ArcSwap::from_pointee(HashMap::new()),
            conns: ArcSwap::from_pointee(HashMap::new()),
            seen: Mutex::new(SeenState::default()),
            counters: PubSubIngressCounters::default(),
            next_sub_id: AtomicU64::new(1),
            next_msg_id: AtomicU64::new(1),
            route_provider: ArcSwap::from_pointee(None),
        });
        this.registry
            .set_pubsub_ingress_handler(Arc::clone(&this))
            .await;
        if options.control_plane_enabled() {
            Self::spawn_control_plane(&this);
        }
        this
    }

    pub fn set_route_provider(&self, provider: Arc<dyn PubSubRouteProvider>) {
        self.route_provider.store(Arc::new(Some(provider)));
    }

    pub fn peer_sender(&self, peer_id: &PeerId) -> Option<PubSubPeerSender> {
        let conn = self.client.lookup_connected_connection(peer_id)?;
        Some(PubSubPeerSender {
            local_peer_id: self.local_peer_id.clone(),
            destination_peer_id: peer_id.clone(),
            conn,
            next_msg_id: AtomicU64::new(1),
        })
    }

    pub fn stats(&self) -> PubSubIngressStats {
        self.counters.snapshot()
    }

    pub fn subscribe_bytes(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: impl Fn(Bytes) + Send + Sync + 'static,
    ) -> PubSubSubscription {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let mut next = (*self.subscribers.load_full()).clone();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        topic_subs.push(SubscriberEntry::new(id, deliver));
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.subscribers.store(Arc::new(next));
        if self.options.control_plane_enabled() {
            self.note_interest(topic_key, true);
        }
        PubSubSubscription {
            topic_key,
            type_hash,
            id,
        }
    }

    pub fn subscribe_borrowed_bytes(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: impl Fn(&[u8]) + Send + Sync + 'static,
    ) -> PubSubBorrowedSubscription {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let mut next = (*self.borrowed_subscribers.load_full()).clone();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let entry = BorrowedSubscriberEntry::new(id, deliver);
        topic_subs.push(entry);
        self.refresh_hot_borrowed_subscriber(key, &topic_subs);
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.borrowed_subscribers.store(Arc::new(next));
        if self.options.control_plane_enabled() {
            self.note_interest(topic_key, true);
        }
        PubSubBorrowedSubscription {
            topic_key,
            type_hash,
            id,
        }
    }

    pub fn subscribe_borrowed_bytes_with_metadata(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: impl Fn(&[u8], PubSubFrameMetadata) + Send + Sync + 'static,
    ) -> PubSubBorrowedSubscription {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let mut next = (*self.borrowed_subscribers.load_full()).clone();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let entry = BorrowedSubscriberEntry::new_with_metadata(id, deliver);
        topic_subs.push(entry);
        self.refresh_hot_borrowed_subscriber(key, &topic_subs);
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.borrowed_subscribers.store(Arc::new(next));
        if self.options.control_plane_enabled() {
            self.note_interest(topic_key, true);
        }
        PubSubBorrowedSubscription {
            topic_key,
            type_hash,
            id,
        }
    }

    pub fn subscribe_type_bytes(
        &self,
        type_hash: u64,
        deliver: impl Fn(u64, Bytes) + Send + Sync + 'static,
    ) -> u64 {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let mut next = (*self.type_subscribers.load_full()).clone();
        let mut subs = next
            .get(&type_hash)
            .map(|subs| subs.to_vec())
            .unwrap_or_default();
        subs.push(TypeSubscriberEntry::new(deliver));
        next.insert(type_hash, Arc::from(subs.into_boxed_slice()));
        self.type_subscribers.store(Arc::new(next));
        id
    }

    pub fn unsubscribe(&self, sub: PubSubSubscription) -> bool {
        let key = (sub.topic_key, sub.type_hash);
        let current = self.subscribers.load_full();
        let Some(existing) = current.get(&key) else {
            return false;
        };
        if existing.is_empty() {
            return false;
        }
        let mut next = current.as_ref().clone();
        let mut topic_subs = existing.to_vec();
        let Some(pos) = topic_subs.iter().position(|entry| entry.id == sub.id) else {
            return false;
        };
        topic_subs.remove(pos);
        if topic_subs.is_empty() {
            next.remove(&key);
        } else {
            next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        }
        self.subscribers.store(Arc::new(next));
        if self.options.control_plane_enabled() {
            self.note_interest(sub.topic_key, false);
        }
        true
    }

    pub fn unsubscribe_borrowed(&self, sub: PubSubBorrowedSubscription) -> bool {
        let key = (sub.topic_key, sub.type_hash);
        let current = self.borrowed_subscribers.load_full();
        let Some(existing) = current.get(&key) else {
            return false;
        };
        if existing.is_empty() {
            return false;
        }
        let mut next = current.as_ref().clone();
        let mut topic_subs = existing.to_vec();
        let Some(pos) = topic_subs.iter().position(|entry| entry.id == sub.id) else {
            return false;
        };
        topic_subs.remove(pos);
        if topic_subs.is_empty() {
            next.remove(&key);
        } else {
            self.refresh_hot_borrowed_subscriber(key, &topic_subs);
            next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        }
        if !next.contains_key(&key) {
            self.refresh_hot_borrowed_subscriber(key, &[]);
        }
        self.borrowed_subscribers.store(Arc::new(next));
        if self.options.control_plane_enabled() {
            self.note_interest(sub.topic_key, false);
        }
        true
    }

    pub fn publish_typed<M>(
        &self,
        topic_key: u64,
        msg: &M,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
    ) -> Result<PubSubPublishStats>
    where
        M: crate::WireEncode + crate::WireType,
    {
        let payload = crate::encode_typed(msg)?;
        self.publish_bytes(topic_key, M::TYPE_HASH, payload, scope, policy)
    }

    pub fn publish_bytes(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: Bytes,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
    ) -> Result<PubSubPublishStats> {
        let mut stats = PubSubPublishStats::default();
        stats.local_delivered = self.deliver_local(topic_key, type_hash, Bytes::clone(&payload));
        self.publish_remote_bytes_inner(topic_key, type_hash, payload, scope, policy, &mut stats)?;
        Ok(stats)
    }

    pub fn publish_remote_bytes(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: Bytes,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
    ) -> Result<PubSubPublishStats> {
        let mut stats = PubSubPublishStats::default();
        self.publish_remote_bytes_inner(topic_key, type_hash, payload, scope, policy, &mut stats)?;
        Ok(stats)
    }

    pub fn publish_remote_bytes_with_metadata(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: Bytes,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
        metadata: PubSubFrameMetadata,
    ) -> Result<PubSubPublishStats> {
        let mut stats = PubSubPublishStats::default();
        self.publish_remote_bytes_inner_with_metadata(
            topic_key, type_hash, payload, scope, policy, metadata, &mut stats,
        )?;
        Ok(stats)
    }

    fn publish_remote_bytes_inner(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: Bytes,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
        stats: &mut PubSubPublishStats,
    ) -> Result<()> {
        self.publish_remote_bytes_inner_with_metadata(
            topic_key,
            type_hash,
            payload,
            scope,
            policy,
            PubSubFrameMetadata::default(),
            stats,
        )
    }

    fn publish_remote_bytes_inner_with_metadata(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: Bytes,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
        metadata: PubSubFrameMetadata,
        stats: &mut PubSubPublishStats,
    ) -> Result<()> {
        if matches!(scope, PubSubScope::LocalOnly) || policy.hops_limit == 0 {
            return Ok(());
        }

        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed) as u128;
        match scope {
            PubSubScope::LocalOnly => Ok(()),
            PubSubScope::AutoExternal | PubSubScope::ClusterWide => {
                let groups = self.route_groups.load();
                let Some(groups) = groups.get(&topic_key) else {
                    return Ok(());
                };
                for (next_hop, destinations) in groups.iter() {
                    self.publish_frame_to_next_hop(
                        next_hop,
                        destinations.as_ref(),
                        topic_key,
                        type_hash,
                        msg_id,
                        payload.as_ref(),
                        policy,
                        metadata,
                        stats,
                    );
                }
                Ok(())
            }
            PubSubScope::SelectedPeers(peers) => {
                let groups = if let Some(provider) = self.route_provider.load().as_ref() {
                    provider.group_destinations(topic_key, &peers)
                } else {
                    peers
                        .into_iter()
                        .filter(|peer| peer != &self.local_peer_id)
                        .map(|peer| (peer.clone(), Arc::from(vec![peer].into_boxed_slice())))
                        .collect()
                };
                for (next_hop, destinations) in &groups {
                    self.publish_frame_to_next_hop(
                        next_hop,
                        destinations.as_ref(),
                        topic_key,
                        type_hash,
                        msg_id,
                        payload.as_ref(),
                        policy,
                        metadata,
                        stats,
                    );
                }
                Ok(())
            }
        }
    }

    fn publish_frame_to_next_hop(
        &self,
        next_hop: &PeerId,
        destinations: &[PeerId],
        topic_key: u64,
        type_hash: u64,
        msg_id: u128,
        payload: &[u8],
        policy: PubSubDeliveryPolicy,
        metadata: PubSubFrameMetadata,
        stats: &mut PubSubPublishStats,
    ) {
        let Some((frame, prefix, payload_len)) = encode_fast_frame_pooled(
            topic_key,
            type_hash,
            msg_id,
            &self.local_peer_id,
            &self.local_peer_id,
            policy.hops_limit,
            policy.mode,
            metadata,
            destinations,
            payload,
        ) else {
            stats.remote_full = stats.remote_full.saturating_add(1);
            self.counters
                .queue_full_drops
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        stats.remote_attempted = stats.remote_attempted.saturating_add(1);
        match self.try_send_next_hop_pooled(next_hop, frame, prefix, payload_len) {
            Ok(()) => stats.remote_enqueued = stats.remote_enqueued.saturating_add(1),
            Err(GossipError::WriteQueueFull) => {
                stats.remote_full = stats.remote_full.saturating_add(1);
                self.counters
                    .queue_full_drops
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                tracing::trace!(next_hop = %next_hop, error = %err, "routed pubsub send failed");
                stats.remote_transport_errors = stats.remote_transport_errors.saturating_add(1)
            }
        }
    }

    pub async fn refresh_control_plane(&self) {
        let mut interests: HashMap<TopicKey, HashSet<PeerId>> = HashMap::new();
        for (name, _) in self.registry.snapshot_known_actors() {
            if let Some((topic, peer)) = parse_interest_name(&name) {
                interests.entry(topic).or_default().insert(peer);
            }
        }

        let provider = self.route_provider.load_full();
        let mut next_routes = HashMap::new();
        let mut next_conns = (*self.conns.load_full()).clone();
        for (topic, peers) in interests {
            let peers: Vec<PeerId> = peers
                .into_iter()
                .filter(|peer| peer != &self.local_peer_id)
                .collect();
            if peers.is_empty() {
                continue;
            }
            let grouped = if let Some(provider) = provider.as_ref() {
                provider.group_destinations(topic, &peers)
            } else {
                peers
                    .into_iter()
                    .map(|peer| (peer.clone(), Arc::from(vec![peer].into_boxed_slice())))
                    .collect()
            };

            // Subscriptions are gated on the pool already holding a live
            // connection to each next-hop. We deliberately do NOT call
            // `client.lookup_peer` here: it goes through
            // `pool.get_connection_to_peer` → `get_connection_by_peer_id`,
            // which warn-logs the "No connection found for peer" pair on
            // every miss (`connection_pool::pool_connect.rs:669-682`).
            // With this refresh ticking at `CONTROL_PLANE_INTERVAL`
            // (25 ms), an unreachable peer whose interest entry is being
            // re-gossiped to us produces ~80 warn lines/sec — observed on
            // `stratum-devnet-a` 2026-05-11.
            //
            // Connection lifecycle belongs to the gossip/peer-discovery
            // layer (`peer_discovery.rs`), not to pubsub. We just
            // observe pool state via the non-warning
            // `lookup_connected_peer` and route only to next-hops the
            // pool currently has a usable connection for. When a peer
            // (re)connects, the next refresh tick picks it up. When a
            // peer drops, the next refresh tick removes it — the user's
            // "subscription terminates on disconnect; re-subscribes on
            // reconnect" invariant.
            let mut routable: HashMap<PeerId, Arc<[PeerId]>> = HashMap::new();
            for (next_hop, destinations) in grouped {
                let cached_live = next_conns
                    .get(&next_hop)
                    .map(|conn| !conn.is_closed())
                    .unwrap_or(false);
                if cached_live {
                    routable.insert(next_hop, destinations);
                    continue;
                }
                // Silent pre-check: only call into pool lookup paths
                // (which warn on miss inside
                // `get_connection_by_peer_id`) when the pool already
                // holds a usable connection. `has_connection_by_peer_id`
                // is the only non-warning peer-presence test on the pool.
                if !self
                    .registry
                    .connection_pool
                    .has_connection_by_peer_id(&next_hop)
                {
                    next_conns.remove(&next_hop);
                    continue;
                }
                if let Some(peer_ref) = self.client.lookup_connected_peer(&next_hop)
                    && let Some(conn) = peer_ref.connection_ref()
                {
                    next_conns.insert(next_hop.clone(), conn);
                    routable.insert(next_hop, destinations);
                } else {
                    next_conns.remove(&next_hop);
                }
            }
            if !routable.is_empty() {
                next_routes.insert(topic, Arc::new(routable));
            }
        }
        self.route_groups.store(Arc::new(next_routes));
        self.conns.store(Arc::new(next_conns));
    }

    fn deliver_local(&self, topic_key: u64, type_hash: u64, payload: Bytes) -> u32 {
        let mut delivered = 0u32;
        let borrowed_subscribers = self.borrowed_subscribers.load();
        if let Some(callbacks) = borrowed_subscribers.get(&(topic_key, type_hash)).cloned() {
            for entry in callbacks.iter() {
                entry.deliver(payload.as_ref(), PubSubFrameMetadata::default());
                delivered = delivered.saturating_add(1);
            }
        }
        drop(borrowed_subscribers);

        let subscribers = self.subscribers.load();
        if let Some(callbacks) = subscribers.get(&(topic_key, type_hash)).cloned() {
            for entry in callbacks.iter() {
                entry.deliver(Bytes::clone(&payload));
                delivered = delivered.saturating_add(1);
            }
        }
        drop(subscribers);

        let type_subscribers = self.type_subscribers.load();
        if let Some(callbacks) = type_subscribers.get(&type_hash).cloned() {
            for callback in callbacks.iter() {
                callback.deliver(topic_key, Bytes::clone(&payload));
                delivered = delivered.saturating_add(1);
            }
        }
        delivered
    }

    fn deliver_local_borrowed(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: &[u8],
        metadata: PubSubFrameMetadata,
    ) -> u32 {
        if self.options.hot_borrowed_subscriber_enabled()
            && let Some(hot) = self.hot_borrowed_subscriber.load_full()
            && hot.key == (topic_key, type_hash)
        {
            hot.entry.deliver(payload, metadata);
            return 1;
        }

        let mut delivered = 0u32;
        let borrowed_subscribers = self.borrowed_subscribers.load();
        if let Some(callbacks) = borrowed_subscribers.get(&(topic_key, type_hash)).cloned() {
            for entry in callbacks.iter() {
                entry.deliver(payload, metadata);
                delivered = delivered.saturating_add(1);
            }
        }
        drop(borrowed_subscribers);

        let subscribers = self.subscribers.load();
        if let Some(callbacks) = subscribers.get(&(topic_key, type_hash)).cloned() {
            let owned = Bytes::copy_from_slice(payload);
            for entry in callbacks.iter() {
                entry.deliver(Bytes::clone(&owned));
                delivered = delivered.saturating_add(1);
            }
        }
        drop(subscribers);

        let type_subscribers = self.type_subscribers.load();
        if let Some(callbacks) = type_subscribers.get(&type_hash).cloned() {
            let owned = Bytes::copy_from_slice(payload);
            for callback in callbacks.iter() {
                callback.deliver(topic_key, Bytes::clone(&owned));
                delivered = delivered.saturating_add(1);
            }
        }
        delivered
    }

    fn refresh_hot_borrowed_subscriber(
        &self,
        key: SubscriberKey,
        entries: &[BorrowedSubscriberEntry],
    ) {
        if !self.options.hot_borrowed_subscriber_enabled() {
            return;
        }
        if entries.len() == 1 {
            self.hot_borrowed_subscriber
                .store(Some(Arc::new(HotBorrowedSubscriber {
                    key,
                    entry: entries[0].clone(),
                })));
        } else {
            self.hot_borrowed_subscriber.store(None);
        }
    }

    fn try_send_next_hop(&self, next_hop: &PeerId, frame: Bytes) -> Result<()> {
        let conns = self.conns.load();
        let conn = if let Some(conn) = conns.get(next_hop) {
            conn.clone()
        } else if let Some(peer_ref) = self.client.lookup_connected_peer(next_hop)
            && let Some(conn) = peer_ref.connection_ref()
        {
            let mut next = (**conns).clone();
            next.insert(next_hop.clone(), conn.clone());
            self.conns.store(Arc::new(next));
            conn
        } else {
            self.counters
                .route_miss_drops
                .fetch_add(1, Ordering::Relaxed);
            return Err(GossipError::ActorNotFound("missing pubsub next-hop".into()));
        };
        conn.try_pubsub_frame(frame)
    }

    fn try_send_next_hop_pooled(
        &self,
        next_hop: &PeerId,
        frame: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        let conns = self.conns.load();
        let conn = if let Some(conn) = conns.get(next_hop) {
            conn.clone()
        } else if let Some(peer_ref) = self.client.lookup_connected_peer(next_hop)
            && let Some(conn) = peer_ref.connection_ref()
        {
            let mut next = (**conns).clone();
            next.insert(next_hop.clone(), conn.clone());
            self.conns.store(Arc::new(next));
            conn
        } else {
            self.counters
                .route_miss_drops
                .fetch_add(1, Ordering::Relaxed);
            return Err(GossipError::ActorNotFound("missing pubsub next-hop".into()));
        };
        conn.try_pubsub_frame_pooled(frame, prefix, payload_len)
    }

    fn note_interest(&self, topic_key: u64, present: bool) {
        let (prev, _next, generation) = {
            let mut state = match self.interest_state.lock() {
                Ok(g) => g,
                Err(e) => e.into_inner(),
            };
            let prev = state.local_counts.get(&topic_key).copied().unwrap_or(0);
            let next = if present {
                prev.saturating_add(1)
            } else {
                prev.saturating_sub(1)
            };
            if next == 0 {
                state.local_counts.remove(&topic_key);
            } else {
                state.local_counts.insert(topic_key, next);
            }

            let generation = if (present && prev == 0) || (!present && prev == 1) {
                let next_generation = state
                    .generations
                    .get(&topic_key)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(1);
                state.generations.insert(topic_key, next_generation);
                next_generation
            } else {
                state.generations.get(&topic_key).copied().unwrap_or(0)
            };
            (prev, next, generation)
        };

        if (present && prev == 0) || (!present && prev == 1) {
            let registry = Arc::clone(&self.registry);
            let peer = self.local_peer_id.clone();
            let interest_state = Arc::clone(&self.interest_state);
            tokio::spawn(async move {
                let (current_present, current_generation) = {
                    let state = match interest_state.lock() {
                        Ok(g) => g,
                        Err(e) => e.into_inner(),
                    };
                    (
                        state.local_counts.get(&topic_key).copied().unwrap_or(0) > 0,
                        state.generations.get(&topic_key).copied().unwrap_or(0),
                    )
                };
                if current_present != present || current_generation != generation {
                    return;
                }

                let name = interest_name(topic_key, &peer);
                let result = if present {
                    let mut location = RemoteActorLocation::new_with_peer(registry.bind_addr, peer);
                    location.priority = RegistrationPriority::Immediate;
                    registry
                        .register_actor_with_priority(
                            name,
                            location,
                            RegistrationPriority::Immediate,
                        )
                        .await
                        .map(|_| ())
                } else {
                    registry.unregister_actor(&name).await.map(|_| ())
                };
                if let Err(err) = result {
                    warn!(
                        topic_key,
                        present,
                        generation,
                        error = %err,
                        "failed to update pubsub interest actor"
                    );
                }
            });
        }
    }

    fn spawn_control_plane(this: &Arc<Self>) {
        let weak = Arc::downgrade(this);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(CONTROL_PLANE_INTERVAL);
            loop {
                tick.tick().await;
                let Some(this) = weak.upgrade() else {
                    return;
                };
                this.refresh_control_plane().await;
            }
        });
    }

    fn accept_seen(&self, origin: &PeerId, msg_id: u128) -> bool {
        let mut seen = match self.seen.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        if self.options.direct_duplicate_tracking() {
            seen.accept_direct(origin, msg_id)
        } else {
            seen.accept_routed(origin, msg_id)
        }
    }
}

impl PubSubIngressHandler for RoutedPubSub {
    fn handle_pubsub_frame(
        &self,
        authenticated_source_peer_id: &PeerId,
        payload: crate::AlignedBytes,
    ) -> Result<()> {
        if payload.as_ref().starts_with(FAST_FRAME_MAGIC) {
            return self.handle_fast_pubsub_frame(authenticated_source_peer_id, payload.as_ref());
        }

        let frame = match crate::decode_typed::<PubSubFrameV1>(payload.as_ref()) {
            Ok(frame) => frame,
            Err(err) => {
                self.counters.decode_drops.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
        };
        if frame.hops_remaining == 0 {
            self.counters.ttl_drops.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if frame.source_peer_id != *authenticated_source_peer_id {
            self.counters
                .reflection_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if frame.origin_peer_id == self.local_peer_id || frame.source_peer_id == self.local_peer_id
        {
            self.counters
                .reflection_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if !self.accept_seen(&frame.origin_peer_id, frame.msg_id) {
            self.counters
                .duplicate_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);

        if frame
            .destination_peers
            .iter()
            .any(|peer| peer == &self.local_peer_id)
        {
            let delivered = self.deliver_local(
                frame.topic_key,
                frame.type_hash,
                Bytes::from(frame.payload.clone()),
            );
            self.counters
                .delivered_local
                .fetch_add(delivered as u64, Ordering::Relaxed);
        }

        if frame.hops_remaining <= 1 {
            return Ok(());
        }
        if !self.options.forwarding_enabled() {
            return Ok(());
        }
        let remaining: Vec<PeerId> = frame
            .destination_peers
            .iter()
            .filter(|peer| *peer != &self.local_peer_id)
            .cloned()
            .collect();
        if remaining.is_empty() {
            return Ok(());
        }
        let grouped = if let Some(provider) = self.route_provider.load().as_ref() {
            provider.group_destinations(frame.topic_key, &remaining)
        } else {
            remaining
                .into_iter()
                .map(|peer| (peer.clone(), Arc::from(vec![peer].into_boxed_slice())))
                .collect()
        };
        for (next_hop, peers) in grouped {
            let encoded = encode_frame(
                frame.topic_key,
                frame.type_hash,
                frame.msg_id,
                frame.origin_peer_id.clone(),
                self.local_peer_id.clone(),
                frame.hops_remaining.saturating_sub(1),
                frame.mode,
                peers.as_ref(),
                frame.payload.as_ref(),
            )?;
            match self.try_send_next_hop(&next_hop, encoded) {
                Ok(()) => {
                    self.counters
                        .forwarded_frames
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(GossipError::WriteQueueFull) => {
                    self.counters
                        .queue_full_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    warn!(error = %err, "failed to forward routed pubsub frame");
                }
            }
        }
        Ok(())
    }
}

impl RoutedPubSub {
    fn handle_fast_pubsub_frame(
        &self,
        authenticated_source_peer_id: &PeerId,
        frame: &[u8],
    ) -> Result<()> {
        let Some(decoded) = FastFrameView::parse(frame) else {
            self.counters.decode_drops.fetch_add(1, Ordering::Relaxed);
            return Err(GossipError::InvalidConfig(
                "malformed fast pubsub frame".into(),
            ));
        };
        if decoded.hops_remaining == 0 {
            self.counters.ttl_drops.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if decoded.source_peer_id != *authenticated_source_peer_id {
            self.counters
                .reflection_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if decoded.origin_peer_id == self.local_peer_id
            || decoded.source_peer_id == self.local_peer_id
        {
            self.counters
                .reflection_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if !self.accept_seen(&decoded.origin_peer_id, decoded.msg_id) {
            self.counters
                .duplicate_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);

        let mut should_deliver_local = false;
        let mut has_remaining = false;
        for index in 0..decoded.destination_count {
            let Some(peer) = decoded.destination_peer_at(index) else {
                self.counters.decode_drops.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            };
            if peer == self.local_peer_id {
                should_deliver_local = true;
            } else {
                has_remaining = true;
            }
        }

        if should_deliver_local {
            let delivered = self.deliver_local_borrowed(
                decoded.topic_key,
                decoded.type_hash,
                decoded.payload,
                decoded.metadata,
            );
            self.counters
                .delivered_local
                .fetch_add(delivered as u64, Ordering::Relaxed);
        }

        if decoded.hops_remaining <= 1 || !has_remaining {
            return Ok(());
        }
        if !self.options.forwarding_enabled() {
            return Ok(());
        }

        let remaining: Vec<PeerId> = decoded
            .destination_peer_iter()
            .filter(|peer| *peer != self.local_peer_id)
            .collect();
        let grouped = if let Some(provider) = self.route_provider.load().as_ref() {
            provider.group_destinations(decoded.topic_key, &remaining)
        } else {
            remaining
                .into_iter()
                .map(|peer| (peer.clone(), Arc::from(vec![peer].into_boxed_slice())))
                .collect()
        };
        for (next_hop, peers) in grouped {
            let Some((encoded, prefix, payload_len)) = encode_fast_frame_pooled(
                decoded.topic_key,
                decoded.type_hash,
                decoded.msg_id,
                &decoded.origin_peer_id,
                &self.local_peer_id,
                decoded.hops_remaining.saturating_sub(1),
                decoded.mode,
                decoded.metadata,
                peers.as_ref(),
                decoded.payload,
            ) else {
                self.counters
                    .queue_full_drops
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };
            match self.try_send_next_hop_pooled(&next_hop, encoded, prefix, payload_len) {
                Ok(()) => {
                    self.counters
                        .forwarded_frames
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(GossipError::WriteQueueFull) => {
                    self.counters
                        .queue_full_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    warn!(error = %err, "failed to forward fast routed pubsub frame");
                }
            }
        }
        Ok(())
    }
}

fn encode_frame(
    topic_key: u64,
    type_hash: u64,
    msg_id: u128,
    origin_peer_id: PeerId,
    source_peer_id: PeerId,
    hops_remaining: u8,
    mode: PubSubDeliveryMode,
    destination_peers: &[PeerId],
    payload: &[u8],
) -> Result<Bytes> {
    let frame = PubSubFrameV1Encode {
        topic_key,
        type_hash,
        msg_id,
        origin_peer_id,
        source_peer_id,
        hops_remaining,
        mode,
        destination_peers,
        payload,
    };
    crate::encode_typed(&frame)
}

struct FastFrameView<'a> {
    topic_key: u64,
    type_hash: u64,
    msg_id: u128,
    origin_peer_id: PeerId,
    source_peer_id: PeerId,
    hops_remaining: u8,
    mode: PubSubDeliveryMode,
    metadata: PubSubFrameMetadata,
    destination_peers: &'a [u8],
    destination_count: usize,
    payload: &'a [u8],
}

impl<'a> FastFrameView<'a> {
    fn parse(frame: &'a [u8]) -> Option<Self> {
        if frame.len() < FAST_FRAME_HEADER_LEN || &frame[..4] != FAST_FRAME_MAGIC {
            return None;
        }
        if frame[4] != 1 {
            return None;
        }
        let mode = decode_delivery_mode(frame[5])?;
        let hops_remaining = frame[6];
        let topic_key = u64::from_be_bytes(frame[8..16].try_into().ok()?);
        let type_hash = u64::from_be_bytes(frame[16..24].try_into().ok()?);
        let msg_id = u128::from_be_bytes(frame[24..40].try_into().ok()?);
        let origin_peer_id = PeerId::from_bytes(&frame[40..72]).ok()?;
        let source_peer_id = PeerId::from_bytes(&frame[72..104]).ok()?;
        let destination_count = u16::from_be_bytes(frame[104..106].try_into().ok()?) as usize;
        let metadata = PubSubFrameMetadata {
            publisher_enqueued_ns: u64::from_be_bytes(frame[112..120].try_into().ok()?),
        };
        let destinations_len = destination_count.checked_mul(FAST_FRAME_DEST_PEER_LEN)?;
        let payload_offset = FAST_FRAME_HEADER_LEN.checked_add(destinations_len)?;
        if payload_offset > frame.len() {
            return None;
        }
        let destination_peers = &frame[FAST_FRAME_HEADER_LEN..payload_offset];
        Some(Self {
            topic_key,
            type_hash,
            msg_id,
            origin_peer_id,
            source_peer_id,
            hops_remaining,
            mode,
            metadata,
            destination_peers,
            destination_count,
            payload: &frame[payload_offset..],
        })
    }

    fn destination_peer_at(&self, index: usize) -> Option<PeerId> {
        if index >= self.destination_count {
            return None;
        }
        let start = index.checked_mul(FAST_FRAME_DEST_PEER_LEN)?;
        let end = start.checked_add(FAST_FRAME_DEST_PEER_LEN)?;
        PeerId::from_bytes(&self.destination_peers[start..end]).ok()
    }

    fn destination_peer_iter(&self) -> impl Iterator<Item = PeerId> + '_ {
        (0..self.destination_count).filter_map(|index| self.destination_peer_at(index))
    }
}

fn encode_delivery_mode(mode: PubSubDeliveryMode) -> u8 {
    match mode {
        PubSubDeliveryMode::AtMostOnce => 0,
        PubSubDeliveryMode::AtLeastOnceHopAck => 1,
    }
}

fn decode_delivery_mode(mode: u8) -> Option<PubSubDeliveryMode> {
    match mode {
        0 => Some(PubSubDeliveryMode::AtMostOnce),
        1 => Some(PubSubDeliveryMode::AtLeastOnceHopAck),
        _ => None,
    }
}

fn encode_fast_frame_pooled(
    topic_key: u64,
    type_hash: u64,
    msg_id: u128,
    origin_peer_id: &PeerId,
    source_peer_id: &PeerId,
    hops_remaining: u8,
    mode: PubSubDeliveryMode,
    metadata: PubSubFrameMetadata,
    destination_peers: &[PeerId],
    payload: &[u8],
) -> Option<(crate::typed::PooledPayload, Option<[u8; 16]>, usize)> {
    let payload_len = fast_frame_len(destination_peers, payload);
    let pooled = crate::typed::PooledPayload::try_from_pooled_bytes(payload_len, |out| {
        write_fast_frame(
            out,
            topic_key,
            type_hash,
            msg_id,
            origin_peer_id,
            source_peer_id,
            hops_remaining,
            mode,
            metadata,
            destination_peers,
            payload,
        );
    })?;
    Some((pooled, None, payload_len))
}

fn encode_fast_pubsub_datagram_pooled(
    topic_key: u64,
    type_hash: u64,
    msg_id: u128,
    origin_peer_id: &PeerId,
    source_peer_id: &PeerId,
    hops_remaining: u8,
    mode: PubSubDeliveryMode,
    metadata: PubSubFrameMetadata,
    destination_peers: &[PeerId],
    payload: &[u8],
) -> Option<crate::typed::PooledPayload> {
    let payload_len = fast_frame_len(destination_peers, payload);
    let header = crate::framing::write_pubsub_frame_prefix(payload_len);
    let datagram_len = header.len().saturating_add(payload_len);
    crate::typed::PooledPayload::try_from_pooled_bytes(datagram_len, |out| {
        out.extend_from_slice(&header);
        write_fast_frame(
            out,
            topic_key,
            type_hash,
            msg_id,
            origin_peer_id,
            source_peer_id,
            hops_remaining,
            mode,
            metadata,
            destination_peers,
            payload,
        );
    })
}

fn fast_frame_len(destination_peers: &[PeerId], payload: &[u8]) -> usize {
    FAST_FRAME_HEADER_LEN
        .saturating_add(
            destination_peers
                .len()
                .saturating_mul(FAST_FRAME_DEST_PEER_LEN),
        )
        .saturating_add(payload.len())
}

#[allow(clippy::too_many_arguments)]
fn write_fast_frame(
    out: &mut Vec<u8>,
    topic_key: u64,
    type_hash: u64,
    msg_id: u128,
    origin_peer_id: &PeerId,
    source_peer_id: &PeerId,
    hops_remaining: u8,
    mode: PubSubDeliveryMode,
    metadata: PubSubFrameMetadata,
    destination_peers: &[PeerId],
    payload: &[u8],
) {
    out.extend_from_slice(FAST_FRAME_MAGIC);
    out.push(1);
    out.push(encode_delivery_mode(mode));
    out.push(hops_remaining);
    out.push(0);
    out.extend_from_slice(&topic_key.to_be_bytes());
    out.extend_from_slice(&type_hash.to_be_bytes());
    out.extend_from_slice(&msg_id.to_be_bytes());
    out.extend_from_slice(origin_peer_id.as_bytes());
    out.extend_from_slice(source_peer_id.as_bytes());
    out.extend_from_slice(&(destination_peers.len() as u16).to_be_bytes());
    out.extend_from_slice(&[0u8; 6]);
    out.extend_from_slice(&metadata.publisher_enqueued_ns.to_be_bytes());
    for peer in destination_peers {
        out.extend_from_slice(peer.as_bytes());
    }
    out.extend_from_slice(payload);
}

pub fn topic_key(topic: &str) -> u64 {
    crate::typed::fnv1a_hash(topic)
}

fn interest_name(topic_key: u64, peer: &PeerId) -> String {
    format!("{INTEREST_PREFIX}/{topic_key:016x}/{}", peer.to_hex())
}

fn parse_interest_name(name: &str) -> Option<(u64, PeerId)> {
    let rest = name.strip_prefix(INTEREST_PREFIX)?.strip_prefix('/')?;
    let (topic, peer) = rest.split_once('/')?;
    let topic = u64::from_str_radix(topic, 16).ok()?;
    let peer = PeerId::from_hex(peer).ok()?;
    Some((topic, peer))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pubsub(registry_peer_seed: &str) -> RoutedPubSub {
        test_pubsub_with_options(registry_peer_seed, RoutedPubSubOptions::default())
    }

    fn test_pubsub_with_options(
        registry_peer_seed: &str,
        options: RoutedPubSubOptions,
    ) -> RoutedPubSub {
        let mut config = crate::GossipConfig::default();
        config.key_pair = Some(crate::KeyPair::new_for_testing(registry_peer_seed));
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            config,
        ));
        RoutedPubSub {
            local_peer_id: registry.peer_id.clone(),
            client: crate::GossipClient::from_registry(Arc::clone(&registry)),
            registry,
            options,
            subscribers: ArcSwap::from_pointee(HashMap::new()),
            borrowed_subscribers: ArcSwap::from_pointee(HashMap::new()),
            hot_borrowed_subscriber: ArcSwapOption::empty(),
            type_subscribers: ArcSwap::from_pointee(HashMap::new()),
            interest_state: Arc::new(Mutex::new(InterestState::default())),
            route_groups: ArcSwap::from_pointee(HashMap::new()),
            conns: ArcSwap::from_pointee(HashMap::new()),
            seen: Mutex::new(SeenState::default()),
            counters: PubSubIngressCounters::default(),
            next_sub_id: AtomicU64::new(1),
            next_msg_id: AtomicU64::new(1),
            route_provider: ArcSwap::from_pointee(None),
        }
    }

    fn aligned_frame(bytes: Bytes) -> crate::AlignedBytes {
        crate::AlignedBytes::from_pooled_slice(
            bytes.as_ref(),
            Arc::new(crate::AlignedBytesPool::default()),
        )
    }

    fn add_test_subscriber<F>(pubsub: &RoutedPubSub, topic: u64, type_hash: u64, deliver: F)
    where
        F: Fn(Bytes) + Send + Sync + 'static,
    {
        let mut next = (*pubsub.subscribers.load_full()).clone();
        next.insert(
            (topic, type_hash),
            Arc::from(vec![SubscriberEntry::new(1, deliver)].into_boxed_slice()),
        );
        pubsub.subscribers.store(Arc::new(next));
    }

    #[test]
    fn pubsub_rejects_source_peer_id_that_does_not_match_authenticated_peer() {
        let pubsub = test_pubsub("pubsub-local-auth-mismatch");
        let victim = crate::KeyPair::new_for_testing("pubsub-victim-auth-mismatch").peer_id();
        let attacker = crate::KeyPair::new_for_testing("pubsub-attacker-auth-mismatch").peer_id();
        let topic = topic_key("auth-mismatch");
        let type_hash = 99;
        let delivered = Arc::new(AtomicU64::new(0));
        let delivered_for_sub = Arc::clone(&delivered);
        add_test_subscriber(&pubsub, topic, type_hash, move |_| {
            delivered_for_sub.fetch_add(1, Ordering::Relaxed);
        });

        let spoofed = encode_frame(
            topic,
            type_hash,
            42,
            victim.clone(),
            attacker,
            2,
            PubSubDeliveryMode::AtMostOnce,
            std::slice::from_ref(&pubsub.local_peer_id),
            b"spoofed",
        )
        .unwrap();

        pubsub
            .handle_pubsub_frame(&victim, aligned_frame(spoofed))
            .unwrap();

        let stats = pubsub.stats();
        assert_eq!(stats.accepted, 0);
        assert_eq!(stats.delivered_local, 0);
        assert_eq!(stats.reflection_drops, 1);
        assert_eq!(delivered.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn rejected_spoofed_pubsub_frame_does_not_poison_seen_entries() {
        let pubsub = test_pubsub("pubsub-local-no-poison");
        let victim = crate::KeyPair::new_for_testing("pubsub-victim-no-poison").peer_id();
        let attacker = crate::KeyPair::new_for_testing("pubsub-attacker-no-poison").peer_id();
        let topic = topic_key("no-poison");
        let type_hash = 101;
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let delivered_for_sub = Arc::clone(&delivered);
        add_test_subscriber(&pubsub, topic, type_hash, move |payload| {
            delivered_for_sub.lock().unwrap().push(payload);
        });

        let spoofed = encode_frame(
            topic,
            type_hash,
            7,
            victim.clone(),
            attacker,
            2,
            PubSubDeliveryMode::AtMostOnce,
            std::slice::from_ref(&pubsub.local_peer_id),
            b"spoofed",
        )
        .unwrap();
        pubsub
            .handle_pubsub_frame(&victim, aligned_frame(spoofed))
            .unwrap();

        let legitimate = encode_frame(
            topic,
            type_hash,
            7,
            victim.clone(),
            victim.clone(),
            2,
            PubSubDeliveryMode::AtMostOnce,
            std::slice::from_ref(&pubsub.local_peer_id),
            b"legitimate",
        )
        .unwrap();
        pubsub
            .handle_pubsub_frame(&victim, aligned_frame(legitimate))
            .unwrap();

        let stats = pubsub.stats();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.delivered_local, 1);
        assert_eq!(stats.duplicate_drops, 0);
        assert_eq!(stats.reflection_drops, 1);
        let deliveries = delivered.lock().unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].as_ref(), b"legitimate");
    }

    #[test]
    fn direct_only_pubsub_delivers_without_forwarding() {
        let pubsub = test_pubsub_with_options(
            "pubsub-direct-only-no-forward",
            RoutedPubSubOptions::direct_only(),
        );
        let origin = crate::KeyPair::new_for_testing("pubsub-direct-origin").peer_id();
        let other = crate::KeyPair::new_for_testing("pubsub-direct-other").peer_id();
        let topic = topic_key("direct-only-no-forward");
        let type_hash = 301;
        let delivered = Arc::new(AtomicU64::new(0));
        let delivered_for_sub = Arc::clone(&delivered);
        pubsub.subscribe_borrowed_bytes_with_metadata(topic, type_hash, move |payload, _| {
            assert_eq!(payload, b"payload");
            delivered_for_sub.fetch_add(1, Ordering::Relaxed);
        });

        let frame = encode_frame(
            topic,
            type_hash,
            1,
            origin.clone(),
            origin.clone(),
            2,
            PubSubDeliveryMode::AtMostOnce,
            &[pubsub.local_peer_id.clone(), other],
            b"payload",
        )
        .unwrap();

        pubsub
            .handle_pubsub_frame(&origin, aligned_frame(frame))
            .unwrap();

        assert_eq!(delivered.load(Ordering::Relaxed), 1);
        let stats = pubsub.stats();
        assert_eq!(stats.delivered_local, 1);
        assert_eq!(stats.forwarded_frames, 0);
        assert_eq!(stats.route_miss_drops, 0);
    }

    #[test]
    fn direct_only_duplicate_tracking_rejects_non_increasing_msg_ids() {
        let pubsub = test_pubsub_with_options(
            "pubsub-direct-only-duplicates",
            RoutedPubSubOptions::direct_only(),
        );
        let origin = crate::KeyPair::new_for_testing("pubsub-direct-dupe-origin").peer_id();
        let topic = topic_key("direct-only-duplicates");
        let type_hash = 302;
        let delivered = Arc::new(AtomicU64::new(0));
        let delivered_for_sub = Arc::clone(&delivered);
        pubsub.subscribe_borrowed_bytes(topic, type_hash, move |_| {
            delivered_for_sub.fetch_add(1, Ordering::Relaxed);
        });

        for msg_id in [7u128, 7, 6, 8] {
            let frame = encode_frame(
                topic,
                type_hash,
                msg_id,
                origin.clone(),
                origin.clone(),
                1,
                PubSubDeliveryMode::AtMostOnce,
                std::slice::from_ref(&pubsub.local_peer_id),
                b"payload",
            )
            .unwrap();
            pubsub
                .handle_pubsub_frame(&origin, aligned_frame(frame))
                .unwrap();
        }

        assert_eq!(delivered.load(Ordering::Relaxed), 2);
        let stats = pubsub.stats();
        assert_eq!(stats.accepted, 2);
        assert_eq!(stats.duplicate_drops, 2);
    }

    #[tokio::test]
    async fn unsubscribe_removes_the_requested_subscription_not_last_in_vector() {
        let pubsub = test_pubsub("pubsub-unsubscribe-by-id");
        let topic = topic_key("unsubscribe-by-id");
        let type_hash = 202;
        let delivered = Arc::new(Mutex::new(Vec::new()));

        let delivered_first = Arc::clone(&delivered);
        let first = pubsub.subscribe_bytes(topic, type_hash, move |_| {
            delivered_first.lock().unwrap().push("first");
        });
        let delivered_second = Arc::clone(&delivered);
        let second = pubsub.subscribe_bytes(topic, type_hash, move |_| {
            delivered_second.lock().unwrap().push("second");
        });

        assert!(pubsub.unsubscribe(first));
        pubsub
            .publish_bytes(
                topic,
                type_hash,
                Bytes::from_static(b"payload"),
                PubSubScope::LocalOnly,
                PubSubDeliveryPolicy::default(),
            )
            .unwrap();

        assert_eq!(
            delivered.lock().unwrap().as_slice(),
            &["second"],
            "non-LIFO unsubscribe must remove the subscription identified by id"
        );
        assert!(pubsub.unsubscribe(second));
        assert!(
            !pubsub
                .interest_state
                .lock()
                .unwrap()
                .local_counts
                .contains_key(&topic),
            "interest count must be removed after the final unsubscribe"
        );
    }

    #[test]
    fn interest_name_round_trips() {
        let peer = crate::KeyPair::new_for_testing("pubsub-interest").peer_id();
        let topic = topic_key("orders");
        let name = interest_name(topic, &peer);
        assert_eq!(parse_interest_name(&name), Some((topic, peer)));
    }

    #[test]
    fn pubsub_frame_v1_borrowed_encoder_is_wire_compatible() {
        let peer = crate::KeyPair::new_for_testing("pubsub-frame").peer_id();
        let payload = Bytes::from_static(b"hello");
        let encoded = encode_frame(
            7,
            9,
            11,
            peer.clone(),
            peer.clone(),
            3,
            PubSubDeliveryMode::AtMostOnce,
            std::slice::from_ref(&peer),
            payload.as_ref(),
        )
        .unwrap();
        let decoded = crate::decode_typed::<PubSubFrameV1>(encoded.as_ref()).unwrap();
        assert_eq!(decoded.topic_key, 7);
        assert_eq!(decoded.type_hash, 9);
        assert_eq!(decoded.msg_id, 11);
        assert_eq!(decoded.destination_peers, vec![peer]);
        assert_eq!(decoded.payload, b"hello");
    }
}
