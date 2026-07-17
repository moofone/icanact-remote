use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use lru::LruCache;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::mpsc;
use tracing::warn;

use crate::{GossipError, PeerId, RegistrationPriority, RemoteActorLocation, Result};

const CONTROL_PLANE_INTERVAL: Duration = Duration::from_millis(25);
const DEFAULT_TTL: u8 = 8;
const SEEN_MESSAGE_CAPACITY: usize = 16_384;
const INTEREST_PREFIX: &str = "icanact/pubsub/interest/v1";
const FAST_FRAME_MAGIC: &[u8; 4] = b"PSF1";
const FAST_FRAME_HEADER_LEN: usize = 120;
const FAST_FRAME_DEST_PEER_LEN: usize = 32;
const FAST_FRAME_POOL_BUFFERS: usize = 4096;
const FAST_FRAME_POOL_BUFFER_CAPACITY: usize = 4096;
/// A subscriber must not be able to stall transport ingress.  A full queue
/// drops that subscriber's newest delivery rather than blocking the reader.
const SUBSCRIBER_QUEUE_CAPACITY: usize = 64;
#[cfg(test)]
const UDP_MAX_DATAGRAM_SIZE: usize = 65_507;

type TopicKey = u64;
type TypeHash = u64;
type SubscriberKey = (TopicKey, TypeHash);
#[derive(Clone)]
struct SubscriberEntry {
    id: u64,
    worker: Arc<SubscriberWorker<Bytes>>,
}
#[derive(Clone)]
struct BorrowedSubscriberEntry {
    id: u64,
    worker: Arc<SubscriberWorker<(Bytes, PubSubFrameMetadata)>>,
}
#[derive(Clone)]
struct TypeSubscriberEntry {
    id: u64,
    worker: Arc<SubscriberWorker<(u64, Bytes)>>,
}
type SubscriberMap = HashMap<SubscriberKey, Arc<[SubscriberEntry]>>;
type BorrowedSubscriberMap = HashMap<SubscriberKey, Arc<[BorrowedSubscriberEntry]>>;
type TypeSubscriberMap = HashMap<TypeHash, Arc<[TypeSubscriberEntry]>>;
type RouteGroups = HashMap<PeerId, Arc<[PeerId]>>;
type TopicRouteGroups = HashMap<TopicKey, Arc<RouteGroups>>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SeenMessageKey {
    origin_peer_id: [u8; 32],
    msg_id: u128,
}

#[derive(Clone)]
struct HotBorrowedSubscriber {
    key: SubscriberKey,
    entry: BorrowedSubscriberEntry,
}

#[derive(Clone)]
struct HotRouteGroups {
    topic_key: TopicKey,
    entries: Arc<[HotRouteEntry]>,
}

#[derive(Clone)]
struct HotRouteEntry {
    next_hop: PeerId,
    destinations: Arc<[PeerId]>,
    conn: crate::RemoteConnection,
}

struct SubscriberWorker<T> {
    sender: mpsc::Sender<T>,
    abort: Option<tokio::task::AbortHandle>,
}

impl<T> Drop for SubscriberWorker<T> {
    fn drop(&mut self) {
        if let Some(abort) = &self.abort {
            abort.abort();
        }
    }
}

fn spawn_pubsub_background<F>(runtime: Option<&tokio::runtime::Handle>, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Some(runtime) = runtime {
        runtime.spawn(future);
    } else if std::thread::Builder::new()
        .name("icanact-pubsub-background".into())
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                warn!("failed to create fallback pubsub background runtime");
                return;
            };
            runtime.block_on(future);
        })
        .is_err()
    {
        warn!("failed to spawn fallback pubsub background worker");
    }
}

fn spawn_subscriber_worker<T, F>(
    runtime: Option<&tokio::runtime::Handle>,
    deliver: F,
) -> Arc<SubscriberWorker<T>>
where
    T: Send + 'static,
    F: Fn(T) + Send + Sync + 'static,
{
    async fn drain<T, F>(mut receiver: mpsc::Receiver<T>, deliver: Arc<F>)
    where
        T: Send + 'static,
        F: Fn(T) + Send + Sync + 'static,
    {
        while let Some(message) = receiver.recv().await {
            let deliver = Arc::clone(&deliver);
            let _ = tokio::task::spawn_blocking(move || deliver(message)).await;
        }
    }

    let (sender, receiver) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
    let deliver = Arc::new(deliver);
    let abort = if let Some(runtime) = runtime {
        Some(runtime.spawn(drain(receiver, deliver)).abort_handle())
    } else {
        if std::thread::Builder::new()
            .name("icanact-pubsub-delivery".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    warn!("failed to create fallback pubsub delivery runtime");
                    return;
                };
                runtime.block_on(drain(receiver, deliver));
            })
            .is_err()
        {
            warn!("failed to spawn fallback pubsub delivery worker");
        }
        None
    };
    Arc::new(SubscriberWorker { sender, abort })
}

impl SubscriberEntry {
    fn new<F>(id: u64, runtime: Option<&tokio::runtime::Handle>, deliver: F) -> Self
    where
        F: Fn(Bytes) + Send + Sync + 'static,
    {
        let worker = spawn_subscriber_worker(runtime, deliver);
        Self { id, worker }
    }

    #[inline]
    fn enqueue(&self, payload: Bytes) -> bool {
        self.worker.sender.try_send(payload).is_ok()
    }
}

impl BorrowedSubscriberEntry {
    fn new<F>(id: u64, runtime: Option<&tokio::runtime::Handle>, deliver: F) -> Self
    where
        F: Fn(&[u8]) + Send + Sync + 'static,
    {
        let worker = spawn_subscriber_worker(
            runtime,
            move |(payload, _metadata): (Bytes, PubSubFrameMetadata)| deliver(payload.as_ref()),
        );
        Self { id, worker }
    }

    fn new_with_metadata<F>(id: u64, runtime: Option<&tokio::runtime::Handle>, deliver: F) -> Self
    where
        F: Fn(&[u8], PubSubFrameMetadata) + Send + Sync + 'static,
    {
        let worker = spawn_subscriber_worker(
            runtime,
            move |(payload, metadata): (Bytes, PubSubFrameMetadata)| {
                deliver(payload.as_ref(), metadata)
            },
        );
        Self { id, worker }
    }

    #[inline]
    fn enqueue(&self, payload: Bytes, metadata: PubSubFrameMetadata) -> bool {
        self.worker.sender.try_send((payload, metadata)).is_ok()
    }
}

impl TypeSubscriberEntry {
    fn new<F>(id: u64, runtime: Option<&tokio::runtime::Handle>, deliver: F) -> Self
    where
        F: Fn(u64, Bytes) + Send + Sync + 'static,
    {
        let worker = spawn_subscriber_worker(runtime, move |(topic_key, payload)| {
            deliver(topic_key, payload)
        });
        Self { id, worker }
    }

    #[inline]
    fn enqueue(&self, topic_key: u64, payload: Bytes) -> bool {
        self.worker.sender.try_send((topic_key, payload)).is_ok()
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum PubSubDeliveryMode {
    /// Best-effort delivery: no acknowledgement, no retry. Frames may be
    /// dropped under backpressure / TTL exhaustion. The default.
    #[default]
    AtMostOnce = 0,
    /// RESERVED — NOT YET IMPLEMENTED (ACTOR_REM_2 R12). The discriminant is
    /// stable on the wire so the variant round-trips, but local publish APIs
    /// reject this mode because there is no pending-ack table, ack frame, or
    /// retry timer. This prevents callers from receiving a false reliability
    /// guarantee.
    AtLeastOnceHopAck = 1,
}

#[cfg(test)]
mod delivery_mode_tests {
    use super::*;

    #[test]
    fn delivery_mode_discriminants_are_stable() {
        assert_eq!(PubSubDeliveryMode::AtMostOnce as u8, 0);
        assert_eq!(PubSubDeliveryMode::AtLeastOnceHopAck as u8, 1);
    }
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
    pub subscriber_queue_drops: u64,
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
    subscriber_queue_drops: AtomicU64,
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
            subscriber_queue_drops: self.subscriber_queue_drops.load(Ordering::Relaxed),
        }
    }
}

pub trait PubSubIngressHandler: Send + Sync {
    fn handle_pubsub_frame(
        &self,
        authenticated_source_peer_id: &PeerId,
        payload: crate::AlignedBytes,
    ) -> Result<()>;

    fn handle_pubsub_frame_borrowed(
        &self,
        authenticated_source_peer_id: &PeerId,
        payload: &[u8],
    ) -> Result<()> {
        self.handle_pubsub_frame(
            authenticated_source_peer_id,
            crate::AlignedBytes::from_pooled_slice(
                payload,
                Arc::new(crate::AlignedBytesPool::default()),
            ),
        )
    }
}

pub trait PubSubRouteProvider: Send + Sync {
    fn group_destinations(
        &self,
        topic_key: u64,
        destinations: &[PeerId],
    ) -> HashMap<PeerId, Arc<[PeerId]>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubscriptionKind {
    Owned { topic_key: u64, type_hash: u64 },
    Borrowed { topic_key: u64, type_hash: u64 },
    Type { type_hash: u64 },
}

/// RAII handle for a routed-pubsub subscription (ACTOR_REM_2 R13a).
///
/// Dropping the handle unsubscribes: the subscriber entry is removed from the
/// map and its worker task is torn down. Holding the handle does NOT keep the
/// [`RoutedPubSub`] engine alive — it holds only a `Weak` back-reference, so
/// engine shutdown is never blocked by a forgotten handle.
///
/// Explicit [`RoutedPubSub::unsubscribe`] consumes the handle and is
/// idempotent with `Drop` (release happens exactly once, guarded by an
/// atomic flag).
#[derive(Debug)]
pub struct PubSubSubscription {
    kind: SubscriptionKind,
    id: u64,
    pubsub: Weak<RoutedPubSub>,
    released: AtomicBool,
}

impl PubSubSubscription {
    /// Removes the subscription exactly once. Returns whether an entry was
    /// actually removed (false on second release or when the engine is gone).
    fn release(&self) -> bool {
        if self.released.swap(true, Ordering::AcqRel) {
            return false;
        }
        let Some(pubsub) = self.pubsub.upgrade() else {
            return false;
        };
        pubsub.remove_subscription(self.kind, self.id)
    }
}

impl Drop for PubSubSubscription {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Default)]
struct InterestState {
    local_counts: HashMap<TopicKey, usize>,
    generations: HashMap<TopicKey, u64>,
}

pub struct RoutedPubSub {
    /// Back-reference handed to RAII subscription handles so their `Drop`
    /// can unsubscribe without keeping the engine alive. Always set: every
    /// constructor goes through `Arc::new_cyclic`.
    weak_self: Weak<RoutedPubSub>,
    registry: Arc<crate::registry::GossipRegistry>,
    client: crate::GossipClient,
    local_peer_id: PeerId,
    subscribers: ArcSwap<SubscriberMap>,
    borrowed_subscribers: ArcSwap<BorrowedSubscriberMap>,
    hot_borrowed_subscriber: ArcSwapOption<HotBorrowedSubscriber>,
    type_subscribers: ArcSwap<TypeSubscriberMap>,
    interest_state: Arc<Mutex<InterestState>>,
    route_groups: ArcSwap<TopicRouteGroups>,
    hot_route_groups: ArcSwapOption<HotRouteGroups>,
    conns: ArcSwap<HashMap<PeerId, crate::RemoteConnection>>,
    seen_messages: Mutex<LruCache<SeenMessageKey, ()>>,
    counters: PubSubIngressCounters,
    next_sub_id: AtomicU64,
    msg_id_epoch: u64,
    next_msg_id: AtomicU64,
    route_provider: ArcSwap<Option<Arc<dyn PubSubRouteProvider>>>,
}

/// Fires the test-only subscriber-map RMW hook (see
/// `test_helpers::fire_pubsub_subscriber_rmw_hook`). Compiles to nothing in
/// production builds; with test helpers enabled but no hook installed the
/// cost is a single relaxed atomic load.
#[inline]
fn subscriber_rmw_window_hook() {
    #[cfg(any(test, feature = "test-helpers"))]
    crate::test_helpers::fire_pubsub_subscriber_rmw_hook();
}

impl RoutedPubSub {
    pub async fn install(registry: Arc<crate::registry::GossipRegistry>) -> Arc<Self> {
        crate::typed::prewarm_pooled_byte_buffers(
            FAST_FRAME_POOL_BUFFERS,
            FAST_FRAME_POOL_BUFFER_CAPACITY,
        );
        let msg_id_epoch = new_msg_id_epoch();
        let this = Arc::new_cyclic(|weak_self| Self {
            weak_self: Weak::clone(weak_self),
            local_peer_id: registry.peer_id.clone(),
            client: crate::GossipClient::from_registry(Arc::clone(&registry)),
            registry,
            subscribers: ArcSwap::from_pointee(HashMap::new()),
            borrowed_subscribers: ArcSwap::from_pointee(HashMap::new()),
            hot_borrowed_subscriber: ArcSwapOption::empty(),
            type_subscribers: ArcSwap::from_pointee(HashMap::new()),
            interest_state: Arc::new(Mutex::new(InterestState::default())),
            route_groups: ArcSwap::from_pointee(HashMap::new()),
            hot_route_groups: ArcSwapOption::empty(),
            conns: ArcSwap::from_pointee(HashMap::new()),
            seen_messages: new_seen_messages(),
            counters: PubSubIngressCounters::default(),
            next_sub_id: AtomicU64::new(1),
            msg_id_epoch,
            next_msg_id: AtomicU64::new(1),
            route_provider: ArcSwap::from_pointee(None),
        });
        this.registry
            .set_pubsub_ingress_handler(Arc::clone(&this))
            .await;
        Self::spawn_control_plane(&this);
        this
    }

    pub fn set_route_provider(&self, provider: Arc<dyn PubSubRouteProvider>) {
        self.route_provider.store(Arc::new(Some(provider)));
    }

    fn next_msg_id(&self) -> u128 {
        let counter = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        (u128::from(self.msg_id_epoch) << 64) | u128::from(counter)
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
        subscriber_rmw_window_hook();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        topic_subs.push(SubscriberEntry::new(id, runtime.as_ref(), deliver));
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.subscribers.store(Arc::new(next));
        self.note_interest(topic_key, true);
        self.make_handle(
            SubscriptionKind::Owned {
                topic_key,
                type_hash,
            },
            id,
        )
    }

    pub fn subscribe_borrowed_bytes(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: impl Fn(&[u8]) + Send + Sync + 'static,
    ) -> PubSubSubscription {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let mut next = (*self.borrowed_subscribers.load_full()).clone();
        subscriber_rmw_window_hook();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        let entry = BorrowedSubscriberEntry::new(id, runtime.as_ref(), deliver);
        topic_subs.push(entry);
        self.refresh_hot_borrowed_subscriber(key, &topic_subs);
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.borrowed_subscribers.store(Arc::new(next));
        self.note_interest(topic_key, true);
        self.make_handle(
            SubscriptionKind::Borrowed {
                topic_key,
                type_hash,
            },
            id,
        )
    }

    pub fn subscribe_borrowed_bytes_with_metadata(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: impl Fn(&[u8], PubSubFrameMetadata) + Send + Sync + 'static,
    ) -> PubSubSubscription {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let mut next = (*self.borrowed_subscribers.load_full()).clone();
        subscriber_rmw_window_hook();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        let entry = BorrowedSubscriberEntry::new_with_metadata(id, runtime.as_ref(), deliver);
        topic_subs.push(entry);
        self.refresh_hot_borrowed_subscriber(key, &topic_subs);
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.borrowed_subscribers.store(Arc::new(next));
        self.note_interest(topic_key, true);
        self.make_handle(
            SubscriptionKind::Borrowed {
                topic_key,
                type_hash,
            },
            id,
        )
    }

    pub fn subscribe_type_bytes(
        &self,
        type_hash: u64,
        deliver: impl Fn(u64, Bytes) + Send + Sync + 'static,
    ) -> PubSubSubscription {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let mut next = (*self.type_subscribers.load_full()).clone();
        subscriber_rmw_window_hook();
        let mut subs = next
            .get(&type_hash)
            .map(|subs| subs.to_vec())
            .unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        subs.push(TypeSubscriberEntry::new(id, runtime.as_ref(), deliver));
        next.insert(type_hash, Arc::from(subs.into_boxed_slice()));
        self.type_subscribers.store(Arc::new(next));
        self.make_handle(SubscriptionKind::Type { type_hash }, id)
    }

    fn make_handle(&self, kind: SubscriptionKind, id: u64) -> PubSubSubscription {
        PubSubSubscription {
            kind,
            id,
            pubsub: Weak::clone(&self.weak_self),
            released: AtomicBool::new(false),
        }
    }

    /// Explicitly releases a subscription. Consumes the handle; equivalent to
    /// dropping it, and idempotent with `Drop` (the handle's release flag
    /// guarantees the removal runs at most once). Returns whether a live
    /// subscription entry was removed.
    pub fn unsubscribe(&self, sub: PubSubSubscription) -> bool {
        sub.release()
    }

    fn remove_subscription(&self, kind: SubscriptionKind, id: u64) -> bool {
        match kind {
            SubscriptionKind::Owned {
                topic_key,
                type_hash,
            } => self.remove_owned_subscriber(topic_key, type_hash, id),
            SubscriptionKind::Borrowed {
                topic_key,
                type_hash,
            } => self.remove_borrowed_subscriber(topic_key, type_hash, id),
            SubscriptionKind::Type { type_hash } => self.remove_type_subscriber(type_hash, id),
        }
    }

    fn remove_owned_subscriber(&self, topic_key: u64, type_hash: u64, id: u64) -> bool {
        let key = (topic_key, type_hash);
        let current = self.subscribers.load_full();
        subscriber_rmw_window_hook();
        let Some(existing) = current.get(&key) else {
            return false;
        };
        if existing.is_empty() {
            return false;
        }
        let mut next = current.as_ref().clone();
        let mut topic_subs = existing.to_vec();
        let Some(pos) = topic_subs.iter().position(|entry| entry.id == id) else {
            return false;
        };
        topic_subs.remove(pos);
        if topic_subs.is_empty() {
            next.remove(&key);
        } else {
            next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        }
        self.subscribers.store(Arc::new(next));
        self.note_interest(topic_key, false);
        true
    }

    fn remove_borrowed_subscriber(&self, topic_key: u64, type_hash: u64, id: u64) -> bool {
        let key = (topic_key, type_hash);
        let current = self.borrowed_subscribers.load_full();
        subscriber_rmw_window_hook();
        let Some(existing) = current.get(&key) else {
            return false;
        };
        if existing.is_empty() {
            return false;
        }
        let mut next = current.as_ref().clone();
        let mut topic_subs = existing.to_vec();
        let Some(pos) = topic_subs.iter().position(|entry| entry.id == id) else {
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
        self.note_interest(topic_key, false);
        true
    }

    fn remove_type_subscriber(&self, type_hash: u64, id: u64) -> bool {
        let current = self.type_subscribers.load_full();
        subscriber_rmw_window_hook();
        let Some(existing) = current.get(&type_hash) else {
            return false;
        };
        let Some(pos) = existing.iter().position(|entry| entry.id == id) else {
            return false;
        };
        let mut next = current.as_ref().clone();
        let mut subs = existing.to_vec();
        subs.remove(pos);
        if subs.is_empty() {
            next.remove(&type_hash);
        } else {
            next.insert(type_hash, Arc::from(subs.into_boxed_slice()));
        }
        self.type_subscribers.store(Arc::new(next));
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
        Self::validate_delivery_mode(policy.mode)?;
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
        Self::validate_delivery_mode(policy.mode)?;
        if matches!(scope, PubSubScope::LocalOnly) || policy.hops_limit == 0 {
            return Ok(());
        }

        let msg_id = self.next_msg_id();
        match scope {
            PubSubScope::LocalOnly => Ok(()),
            PubSubScope::AutoExternal | PubSubScope::ClusterWide => {
                let hot_routes = self.hot_route_groups.load();
                if let Some(hot) = hot_routes.as_ref()
                    && hot.topic_key == topic_key
                {
                    for entry in hot.entries.iter() {
                        self.publish_frame_to_conn(
                            &entry.next_hop,
                            &entry.conn,
                            entry.destinations.as_ref(),
                            topic_key,
                            type_hash,
                            msg_id,
                            payload.as_ref(),
                            policy,
                            metadata,
                            stats,
                        );
                    }
                    return Ok(());
                }
                drop(hot_routes);

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

    fn validate_delivery_mode(mode: PubSubDeliveryMode) -> Result<()> {
        if matches!(mode, PubSubDeliveryMode::AtLeastOnceHopAck) {
            return Err(GossipError::InvalidConfig(
                "PubSubDeliveryMode::AtLeastOnceHopAck is not implemented".into(),
            ));
        }
        Ok(())
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
        let conn = match self.lookup_next_hop_conn(next_hop) {
            Ok(conn) => conn,
            Err(err) => {
                tracing::trace!(next_hop = %next_hop, error = %err, "routed pubsub route lookup failed");
                stats.remote_transport_errors = stats.remote_transport_errors.saturating_add(1);
                return;
            }
        };
        self.publish_frame_to_conn(
            next_hop,
            &conn,
            destinations,
            topic_key,
            type_hash,
            msg_id,
            payload,
            policy,
            metadata,
            stats,
        );
    }

    fn publish_frame_to_conn(
        &self,
        next_hop: &PeerId,
        conn: &crate::RemoteConnection,
        destinations: &[PeerId],
        topic_key: u64,
        type_hash: u64,
        msg_id: u128,
        payload: &[u8],
        policy: PubSubDeliveryPolicy,
        metadata: PubSubFrameMetadata,
        stats: &mut PubSubPublishStats,
    ) {
        stats.remote_attempted = stats.remote_attempted.saturating_add(1);

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
        match conn.try_pubsub_frame_pooled(frame, prefix, payload_len) {
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
        // ACTOR_REM_2 R13(b): the conns cache is cloned forward each tick and
        // only next-hops in the current interest set are visited above, so a
        // peer that stops being any topic's next-hop would be carried forever.
        // Retain only next-hops referenced by a current route (they are
        // re-added by the loop above when interest and a live connection
        // return), keeping the cache bounded by live routes, not history.
        let live_next_hops: std::collections::HashSet<PeerId> = next_routes
            .values()
            .flat_map(|groups| groups.keys().cloned())
            .collect();
        next_conns.retain(|next_hop, _| live_next_hops.contains(next_hop));
        self.refresh_hot_route_groups(&next_routes, &next_conns);
        self.route_groups.store(Arc::new(next_routes));
        self.conns.store(Arc::new(next_conns));
    }

    fn refresh_hot_route_groups(
        &self,
        routes: &HashMap<TopicKey, Arc<RouteGroups>>,
        conns: &HashMap<PeerId, crate::RemoteConnection>,
    ) {
        if routes.len() == 1
            && let Some((&topic_key, groups)) = routes.iter().next()
        {
            let mut entries = Vec::with_capacity(groups.len());
            for (next_hop, destinations) in groups.iter() {
                let Some(conn) = conns.get(next_hop) else {
                    self.hot_route_groups.store(None);
                    return;
                };
                entries.push(HotRouteEntry {
                    next_hop: next_hop.clone(),
                    destinations: Arc::clone(destinations),
                    conn: conn.clone(),
                });
            }
            self.hot_route_groups.store(Some(Arc::new(HotRouteGroups {
                topic_key,
                entries: entries.into_boxed_slice().into(),
            })));
        } else {
            self.hot_route_groups.store(None);
        }
    }

    fn deliver_local(&self, topic_key: u64, type_hash: u64, payload: Bytes) -> u32 {
        let mut delivered = 0u32;
        let borrowed_subscribers = self.borrowed_subscribers.load();
        if let Some(callbacks) = borrowed_subscribers.get(&(topic_key, type_hash)).cloned() {
            for entry in callbacks.iter() {
                if entry.enqueue(Bytes::clone(&payload), PubSubFrameMetadata::default()) {
                    delivered = delivered.saturating_add(1);
                } else {
                    self.counters
                        .subscriber_queue_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        drop(borrowed_subscribers);

        let subscribers = self.subscribers.load();
        if let Some(callbacks) = subscribers.get(&(topic_key, type_hash)).cloned() {
            for entry in callbacks.iter() {
                if entry.enqueue(Bytes::clone(&payload)) {
                    delivered = delivered.saturating_add(1);
                } else {
                    self.counters
                        .subscriber_queue_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        drop(subscribers);

        let type_subscribers = self.type_subscribers.load();
        if let Some(callbacks) = type_subscribers.get(&type_hash).cloned() {
            for callback in callbacks.iter() {
                if callback.enqueue(topic_key, Bytes::clone(&payload)) {
                    delivered = delivered.saturating_add(1);
                } else {
                    self.counters
                        .subscriber_queue_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
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
        let hot_subscriber = self.hot_borrowed_subscriber.load();
        if let Some(hot) = hot_subscriber.as_ref()
            && hot.key == (topic_key, type_hash)
        {
            let subscribers = self.subscribers.load();
            let has_owned_subscribers = subscribers
                .get(&(topic_key, type_hash))
                .is_some_and(|callbacks| !callbacks.is_empty());
            drop(subscribers);

            let type_subscribers = self.type_subscribers.load();
            let has_type_subscribers = type_subscribers
                .get(&type_hash)
                .is_some_and(|callbacks| !callbacks.is_empty());
            drop(type_subscribers);

            if !has_owned_subscribers && !has_type_subscribers {
                if hot.entry.enqueue(Bytes::copy_from_slice(payload), metadata) {
                    return 1;
                }
                self.counters
                    .subscriber_queue_drops
                    .fetch_add(1, Ordering::Relaxed);
                return 0;
            }
        }
        drop(hot_subscriber);

        let mut delivered = 0u32;
        let borrowed_subscribers = self.borrowed_subscribers.load();
        if let Some(callbacks) = borrowed_subscribers.get(&(topic_key, type_hash)).cloned() {
            let owned = Bytes::copy_from_slice(payload);
            for entry in callbacks.iter() {
                if entry.enqueue(Bytes::clone(&owned), metadata) {
                    delivered = delivered.saturating_add(1);
                } else {
                    self.counters
                        .subscriber_queue_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        drop(borrowed_subscribers);

        let subscribers = self.subscribers.load();
        if let Some(callbacks) = subscribers.get(&(topic_key, type_hash)).cloned() {
            let owned = Bytes::copy_from_slice(payload);
            for entry in callbacks.iter() {
                if entry.enqueue(Bytes::clone(&owned)) {
                    delivered = delivered.saturating_add(1);
                } else {
                    self.counters
                        .subscriber_queue_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        drop(subscribers);

        let type_subscribers = self.type_subscribers.load();
        if let Some(callbacks) = type_subscribers.get(&type_hash).cloned() {
            let owned = Bytes::copy_from_slice(payload);
            for callback in callbacks.iter() {
                if callback.enqueue(topic_key, Bytes::clone(&owned)) {
                    delivered = delivered.saturating_add(1);
                } else {
                    self.counters
                        .subscriber_queue_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        delivered
    }

    fn refresh_hot_borrowed_subscriber(
        &self,
        key: SubscriberKey,
        entries: &[BorrowedSubscriberEntry],
    ) {
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

    fn lookup_next_hop_conn(&self, next_hop: &PeerId) -> Result<crate::RemoteConnection> {
        let conns = self.conns.load();
        if let Some(conn) = conns.get(next_hop) {
            return Ok(conn.clone());
        }
        if let Some(peer_ref) = self.client.lookup_connected_peer(next_hop)
            && let Some(conn) = peer_ref.connection_ref()
        {
            let mut next = (**conns).clone();
            next.insert(next_hop.clone(), conn.clone());
            self.conns.store(Arc::new(next));
            return Ok(conn);
        }
        self.counters
            .route_miss_drops
            .fetch_add(1, Ordering::Relaxed);
        Err(GossipError::ActorNotFound("missing pubsub next-hop".into()))
    }

    fn try_send_next_hop_pooled(
        &self,
        next_hop: &PeerId,
        frame: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        self.lookup_next_hop_conn(next_hop)?
            .try_pubsub_frame_pooled(frame, prefix, payload_len)
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
            let runtime = tokio::runtime::Handle::try_current().ok();
            spawn_pubsub_background(runtime.as_ref(), async move {
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
                    // Advertise through `advertised_addr()`, never the raw
                    // `bind_addr` — a node bound to a wildcard address
                    // (`0.0.0.0:<port>`, a normal deployment pattern) would
                    // otherwise gossip an undialable interest-actor
                    // location, starving peers of a route to it (the
                    // wildcard-advertise reconnect-storm class of bug; see
                    // `tests/wildcard_advertise_interest_storm.rs`). When
                    // no `GossipConfig::advertise_address` override is
                    // configured this can still resolve to an unspecified
                    // IP; that is deliberately not rejected here.
                    // Registration proceeds and the receiving side's
                    // `validate_remote_actor_addr` (`registry.rs`) rewrites
                    // an unspecified advertised IP using the gossiping
                    // peer's own verified address — the same trust anchor
                    // `resolve_peer_addr_checked` already uses for peer
                    // bind-address resolution — so wildcard-bound nodes
                    // work correctly with zero required configuration,
                    // exactly like peer addresses already do.
                    let advertised = registry.advertised_addr();
                    let mut location = RemoteActorLocation::new_with_peer(advertised, peer);
                    location.priority = RegistrationPriority::Immediate;
                    registry
                        .register_actor_with_priority(
                            name,
                            location,
                            RegistrationPriority::Immediate,
                        )
                        .await
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

    fn accept_seen_bytes(&self, origin: &[u8; 32], msg_id: u128) -> bool {
        let key = SeenMessageKey {
            origin_peer_id: *origin,
            msg_id,
        };
        let mut seen = self.seen_messages.lock().unwrap_or_else(|error| {
            warn!(%error, "pubsub seen-message cache lock poisoned, recovering");
            error.into_inner()
        });
        if seen.get(&key).is_some() {
            return false;
        }
        seen.put(key, ());
        true
    }
}

impl PubSubIngressHandler for RoutedPubSub {
    fn handle_pubsub_frame_borrowed(
        &self,
        authenticated_source_peer_id: &PeerId,
        payload: &[u8],
    ) -> Result<()> {
        if payload.starts_with(FAST_FRAME_MAGIC) {
            return self.handle_fast_pubsub_frame(authenticated_source_peer_id, payload);
        }

        // Only PSF1 fast frames exist on the wire; the legacy rkyv
        // `PubSubFrameV1` decode shim was removed (zero-back-compat policy).
        // Anything else is dropped with a decode stat.
        self.counters.decode_drops.fetch_add(1, Ordering::Relaxed);
        warn!(
            source = %authenticated_source_peer_id,
            len = payload.len(),
            "dropping unrecognized pubsub frame (not PSF1; legacy rkyv V1 frames unsupported)"
        );
        Ok(())
    }

    fn handle_pubsub_frame(
        &self,
        authenticated_source_peer_id: &PeerId,
        payload: crate::AlignedBytes,
    ) -> Result<()> {
        self.handle_pubsub_frame_borrowed(authenticated_source_peer_id, payload.as_ref())
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
        if decoded.source_peer_id != *authenticated_source_peer_id.as_bytes() {
            self.counters
                .reflection_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let local_peer_id = self.local_peer_id.as_bytes();
        if decoded.origin_peer_id == *local_peer_id || decoded.source_peer_id == *local_peer_id {
            self.counters
                .reflection_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if !self.accept_seen_bytes(&decoded.origin_peer_id, decoded.msg_id) {
            self.counters
                .duplicate_drops
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);

        let mut should_deliver_local = false;
        let mut has_remaining = false;
        for index in 0..decoded.destination_count {
            let Some(peer) = decoded.destination_peer_bytes_at(index) else {
                self.counters.decode_drops.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            };
            if peer == local_peer_id {
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

        let origin_peer_id = match PeerId::from_bytes(&decoded.origin_peer_id) {
            Ok(peer_id) => peer_id,
            Err(err) => {
                self.counters.decode_drops.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
        };
        let remaining: Vec<PeerId> = decoded
            .destination_peer_iter()
            .filter(|peer| peer.as_bytes() != local_peer_id)
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
                &origin_peer_id,
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

struct FastFrameView<'a> {
    topic_key: u64,
    type_hash: u64,
    msg_id: u128,
    origin_peer_id: [u8; 32],
    source_peer_id: [u8; 32],
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
        let origin_peer_id = frame[40..72].try_into().ok()?;
        let source_peer_id = frame[72..104].try_into().ok()?;
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

    fn destination_peer_bytes_at(&self, index: usize) -> Option<&'a [u8; 32]> {
        if index >= self.destination_count {
            return None;
        }
        let start = index.checked_mul(FAST_FRAME_DEST_PEER_LEN)?;
        let end = start.checked_add(FAST_FRAME_DEST_PEER_LEN)?;
        self.destination_peers[start..end].try_into().ok()
    }

    fn destination_peer_at(&self, index: usize) -> Option<PeerId> {
        PeerId::from_bytes(self.destination_peer_bytes_at(index)?).ok()
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

fn new_seen_messages() -> Mutex<LruCache<SeenMessageKey, ()>> {
    Mutex::new(LruCache::new(
        NonZeroUsize::new(SEEN_MESSAGE_CAPACITY).expect("seen-message capacity must be non-zero"),
    ))
}

fn new_msg_id_epoch() -> u64 {
    new_msg_id_epoch_with(rand::random::<u64>)
}

fn new_msg_id_epoch_with(entropy: impl FnOnce() -> u64) -> u64 {
    entropy().max(1)
}

#[cfg(test)]
fn legacy_seen_fingerprint_bytes(origin: &[u8; 32], msg_id: u128) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    #[inline]
    fn mix(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let hash = mix(mix(FNV_OFFSET, origin), &msg_id.to_be_bytes());
    if hash == 0 { 1 } else { hash }
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

#[cfg(test)]
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
    if datagram_len > UDP_MAX_DATAGRAM_SIZE {
        return None;
    }
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
    // ACTOR_REM_2 R16h: the destination count is a u16. Writing more than
    // u16::MAX peers would truncate the header count while the body still
    // carried every peer, so the receiver would parse the tail of the
    // destination array as payload (silent corruption). Cap the written peers
    // to what the header can represent so header and body always agree.
    let dest_count = destination_peers.len().min(u16::MAX as usize);
    if dest_count != destination_peers.len() {
        warn!(
            total = destination_peers.len(),
            written = dest_count,
            "fast pubsub frame destination list exceeds u16::MAX; extra destinations dropped"
        );
    }
    out.extend_from_slice(&(dest_count as u16).to_be_bytes());
    out.extend_from_slice(&[0u8; 6]);
    out.extend_from_slice(&metadata.publisher_enqueued_ns.to_be_bytes());
    for peer in &destination_peers[..dest_count] {
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
    use bytes::Buf;

    fn test_pubsub(registry_peer_seed: &str) -> Arc<RoutedPubSub> {
        crate::typed::prewarm_pooled_byte_buffers(
            FAST_FRAME_POOL_BUFFERS,
            FAST_FRAME_POOL_BUFFER_CAPACITY,
        );
        let mut config = crate::GossipConfig::default();
        config.key_pair = Some(crate::KeyPair::new_for_testing(registry_peer_seed));
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            config,
        ));
        let msg_id_epoch = new_msg_id_epoch();
        Arc::new_cyclic(|weak_self| RoutedPubSub {
            weak_self: Weak::clone(weak_self),
            local_peer_id: registry.peer_id.clone(),
            client: crate::GossipClient::from_registry(Arc::clone(&registry)),
            registry,
            subscribers: ArcSwap::from_pointee(HashMap::new()),
            borrowed_subscribers: ArcSwap::from_pointee(HashMap::new()),
            hot_borrowed_subscriber: ArcSwapOption::empty(),
            type_subscribers: ArcSwap::from_pointee(HashMap::new()),
            interest_state: Arc::new(Mutex::new(InterestState::default())),
            route_groups: ArcSwap::from_pointee(HashMap::new()),
            hot_route_groups: ArcSwapOption::empty(),
            conns: ArcSwap::from_pointee(HashMap::new()),
            seen_messages: new_seen_messages(),
            counters: PubSubIngressCounters::default(),
            next_sub_id: AtomicU64::new(1),
            msg_id_epoch,
            next_msg_id: AtomicU64::new(1),
            route_provider: ArcSwap::from_pointee(None),
        })
    }

    fn add_test_subscriber<F>(pubsub: &RoutedPubSub, topic: u64, type_hash: u64, deliver: F)
    where
        F: Fn(Bytes) + Send + Sync + 'static,
    {
        let mut next = (*pubsub.subscribers.load_full()).clone();
        next.insert(
            (topic, type_hash),
            Arc::from(
                vec![SubscriberEntry::new(
                    1,
                    tokio::runtime::Handle::try_current().ok().as_ref(),
                    deliver,
                )]
                .into_boxed_slice(),
            ),
        );
        pubsub.subscribers.store(Arc::new(next));
    }

    #[tokio::test]
    async fn pubsub_msg_ids_do_not_reuse_after_same_peer_restarts() {
        let first = test_pubsub("pubsub-restart-msg-id");
        let second = test_pubsub("pubsub-restart-msg-id");

        assert_eq!(first.local_peer_id, second.local_peer_id);

        let first_msg_id = first.next_msg_id();
        let second_msg_id = second.next_msg_id();

        assert_ne!(
            first_msg_id, second_msg_id,
            "same peer restart must not reuse pubsub message ids while receivers still retain duplicate fingerprints"
        );
        assert_ne!(first_msg_id >> 64, 0);
        assert_ne!(second_msg_id >> 64, 0);
    }

    #[test]
    fn msg_id_epoch_uses_injected_random_entropy() {
        let entropy = 0x9e37_79b9_7f4a_7c15;
        assert_eq!(new_msg_id_epoch_with(|| entropy), entropy);
    }

    #[test]
    fn msg_id_epoch_reserves_zero() {
        assert_eq!(new_msg_id_epoch_with(|| 0), 1);
    }

    #[test]
    fn slot_collision_does_not_make_an_earlier_message_replay_acceptable() {
        let pubsub = test_pubsub("pubsub-exact-dedup");
        let origin = *crate::KeyPair::new_for_testing("pubsub-dedup-origin")
            .peer_id()
            .as_bytes();
        let mut by_slot = HashMap::new();
        let (first, second) = (1_u128..=u128::from(SEEN_MESSAGE_CAPACITY as u64 + 1))
            .find_map(|msg_id| {
                let fingerprint = legacy_seen_fingerprint_bytes(&origin, msg_id);
                let slot = (fingerprint as usize) & (SEEN_MESSAGE_CAPACITY - 1);
                match by_slot.insert(slot, (msg_id, fingerprint)) {
                    Some((previous_id, previous_fingerprint))
                        if previous_fingerprint != fingerprint =>
                    {
                        Some((previous_id, msg_id))
                    }
                    _ => None,
                }
            })
            .expect("pigeonhole principle guarantees a slot collision");

        assert!(pubsub.accept_seen_bytes(&origin, first));
        assert!(pubsub.accept_seen_bytes(&origin, second));
        assert!(
            !pubsub.accept_seen_bytes(&origin, first),
            "a colliding message must not evict a recent exact identity"
        );
    }

    #[test]
    fn exact_dedup_cache_stays_bounded_and_evicts_least_recent_identity() {
        let pubsub = test_pubsub("pubsub-bounded-dedup");
        let origin = *crate::KeyPair::new_for_testing("pubsub-bounded-dedup-origin")
            .peer_id()
            .as_bytes();

        for msg_id in 1..=SEEN_MESSAGE_CAPACITY as u128 + 1 {
            assert!(pubsub.accept_seen_bytes(&origin, msg_id));
        }
        assert_eq!(
            pubsub.seen_messages.lock().unwrap().len(),
            SEEN_MESSAGE_CAPACITY
        );
        assert!(
            pubsub.accept_seen_bytes(&origin, 1),
            "the oldest identity should leave the finite dedup window"
        );
        assert!(
            !pubsub.accept_seen_bytes(&origin, SEEN_MESSAGE_CAPACITY as u128 + 1),
            "a recent identity must remain deduplicated after LRU eviction"
        );
    }

    #[tokio::test]
    async fn oversized_pubsub_payload_skips_datagram_but_encodes_stream_frame() {
        let pubsub = test_pubsub("pubsub-oversize-stream-fallback");
        let topic = topic_key("oversize-stream-fallback");
        let type_hash = 77;
        let payload = vec![0u8; UDP_MAX_DATAGRAM_SIZE];
        let destination = crate::KeyPair::new_for_testing("pubsub-oversize-destination").peer_id();

        assert!(
            encode_fast_pubsub_datagram_pooled(
                topic,
                type_hash,
                42,
                &pubsub.local_peer_id,
                &pubsub.local_peer_id,
                2,
                PubSubDeliveryMode::AtMostOnce,
                PubSubFrameMetadata::default(),
                std::slice::from_ref(&destination),
                &payload,
            )
            .is_none()
        );
        let (frame, _prefix, payload_len) = encode_fast_frame_pooled(
            topic,
            type_hash,
            42,
            &pubsub.local_peer_id,
            &pubsub.local_peer_id,
            2,
            PubSubDeliveryMode::AtMostOnce,
            PubSubFrameMetadata::default(),
            std::slice::from_ref(&destination),
            &payload,
        )
        .expect("streamable pubsub frame");
        assert_eq!(
            payload_len,
            fast_frame_len(std::slice::from_ref(&destination), &payload)
        );
        assert_eq!(frame.remaining(), payload_len);
    }

    #[tokio::test]
    async fn pubsub_rejects_source_peer_id_that_does_not_match_authenticated_peer() {
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

        let (spoofed, _, _) = encode_fast_frame_pooled(
            topic,
            type_hash,
            42,
            &victim,
            &attacker,
            2,
            PubSubDeliveryMode::AtMostOnce,
            PubSubFrameMetadata::default(),
            std::slice::from_ref(&pubsub.local_peer_id),
            b"spoofed",
        )
        .unwrap();

        pubsub
            .handle_pubsub_frame_borrowed(&victim, spoofed.chunk())
            .unwrap();

        let stats = pubsub.stats();
        assert_eq!(stats.accepted, 0);
        assert_eq!(stats.delivered_local, 0);
        assert_eq!(stats.reflection_drops, 1);
        assert_eq!(delivered.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn rejected_spoofed_pubsub_frame_does_not_poison_seen_entries() {
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

        let (spoofed, _, _) = encode_fast_frame_pooled(
            topic,
            type_hash,
            7,
            &victim,
            &attacker,
            2,
            PubSubDeliveryMode::AtMostOnce,
            PubSubFrameMetadata::default(),
            std::slice::from_ref(&pubsub.local_peer_id),
            b"spoofed",
        )
        .unwrap();
        pubsub
            .handle_pubsub_frame_borrowed(&victim, spoofed.chunk())
            .unwrap();

        let (legitimate, _, _) = encode_fast_frame_pooled(
            topic,
            type_hash,
            7,
            &victim,
            &victim,
            2,
            PubSubDeliveryMode::AtMostOnce,
            PubSubFrameMetadata::default(),
            std::slice::from_ref(&pubsub.local_peer_id),
            b"legitimate",
        )
        .unwrap();
        pubsub
            .handle_pubsub_frame_borrowed(&victim, legitimate.chunk())
            .unwrap();

        let stats = pubsub.stats();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.delivered_local, 1);
        assert_eq!(stats.duplicate_drops, 0);
        assert_eq!(stats.reflection_drops, 1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while delivered.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued subscriber should receive the legitimate frame");
        let deliveries = delivered.lock().unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].as_ref(), b"legitimate");
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

        tokio::time::timeout(Duration::from_secs(1), async {
            while delivered.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued subscriber should receive the local publication");
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

    #[tokio::test]
    async fn unsupported_hop_ack_mode_is_rejected_before_local_delivery() {
        let pubsub = test_pubsub("pubsub-hop-ack-rejected");
        let topic = topic_key("hop-ack-rejected");
        let type_hash = 206;
        let delivered = Arc::new(AtomicU64::new(0));
        let delivered_for_subscriber = Arc::clone(&delivered);
        let _subscription = pubsub.subscribe_bytes(topic, type_hash, move |_| {
            delivered_for_subscriber.fetch_add(1, Ordering::Relaxed);
        });
        let policy = PubSubDeliveryPolicy {
            mode: PubSubDeliveryMode::AtLeastOnceHopAck,
            ..PubSubDeliveryPolicy::default()
        };

        let error = pubsub
            .publish_bytes(
                topic,
                type_hash,
                Bytes::from_static(b"must-not-deliver"),
                PubSubScope::LocalOnly,
                policy,
            )
            .expect_err("an unimplemented reliability mode must fail closed");

        assert!(matches!(
            error,
            GossipError::InvalidConfig(message)
                if message == "PubSubDeliveryMode::AtLeastOnceHopAck is not implemented"
        ));
        tokio::task::yield_now().await;
        assert_eq!(delivered.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn unsubscribe_aborts_the_subscriber_worker() {
        let pubsub = test_pubsub("pubsub-unsubscribe-worker");
        let captured = Arc::new(());
        let weak = Arc::downgrade(&captured);
        let captured_by_callback = Arc::clone(&captured);
        let subscription = pubsub.subscribe_bytes(77, 205, move |_| {
            let _keepalive = &captured_by_callback;
        });
        drop(captured);

        assert!(pubsub.unsubscribe(subscription));
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unsubscribing must release the callback held by its worker");
    }

    #[tokio::test]
    async fn slow_subscriber_does_not_block_local_pubsub_ingress() {
        let pubsub = test_pubsub("pubsub-slow-subscriber");
        let topic = topic_key("slow-subscriber");
        let type_hash = 203;
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started_for_subscriber = Arc::clone(&started);
        let release_for_subscriber = Arc::clone(&release);
        let _subscription = pubsub.subscribe_bytes(topic, type_hash, move |_| {
            started_for_subscriber.store(true, Ordering::Release);
            while !release_for_subscriber.load(Ordering::Acquire) {
                std::thread::park_timeout(Duration::from_millis(1));
            }
        });

        pubsub
            .publish_bytes(
                topic,
                type_hash,
                Bytes::from_static(b"first"),
                PubSubScope::LocalOnly,
                PubSubDeliveryPolicy::default(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscriber worker should start");

        let started_at = std::time::Instant::now();
        for _ in 0..=SUBSCRIBER_QUEUE_CAPACITY {
            pubsub
                .publish_bytes(
                    topic,
                    type_hash,
                    Bytes::from_static(b"queued"),
                    PubSubScope::LocalOnly,
                    PubSubDeliveryPolicy::default(),
                )
                .unwrap();
        }
        assert!(
            started_at.elapsed() < Duration::from_millis(100),
            "a blocked subscriber must not hold pubsub ingress"
        );
        assert!(
            pubsub.stats().subscriber_queue_drops > 0,
            "a full subscriber queue must drop rather than blocking ingress"
        );
        release.store(true, Ordering::Release);
    }

    #[test]
    fn subscriptions_created_outside_tokio_use_fallback_worker() {
        let pubsub = test_pubsub("pubsub-no-runtime");
        let topic = topic_key("no-runtime");
        let type_hash = 204;
        let delivered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let delivered_for_subscriber = Arc::clone(&delivered);
        let _subscription = pubsub.subscribe_bytes(topic, type_hash, move |_| {
            delivered_for_subscriber.store(true, Ordering::Release);
        });

        pubsub
            .publish_bytes(
                topic,
                type_hash,
                Bytes::from_static(b"fallback"),
                PubSubScope::LocalOnly,
                PubSubDeliveryPolicy::default(),
            )
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !delivered.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            delivered.load(Ordering::Acquire),
            "fallback subscriber worker should deliver without a Tokio runtime"
        );
    }

    #[test]
    fn interest_name_round_trips() {
        let peer = crate::KeyPair::new_for_testing("pubsub-interest").peer_id();
        let topic = topic_key("orders");
        let name = interest_name(topic, &peer);
        assert_eq!(parse_interest_name(&name), Some((topic, peer)));
    }

    #[tokio::test]
    async fn dropping_subscription_handle_unsubscribes_and_releases_worker() {
        let pubsub = test_pubsub("pubsub-raii-drop");
        let topic = topic_key("raii-drop");
        let type_hash = 301;
        let captured = Arc::new(());
        let weak_callback = Arc::downgrade(&captured);
        let captured_by_callback = Arc::clone(&captured);
        let subscription = pubsub.subscribe_bytes(topic, type_hash, move |_| {
            let _keepalive = &captured_by_callback;
        });
        drop(captured);
        assert!(pubsub.subscribers.load().contains_key(&(topic, type_hash)));

        drop(subscription);
        assert!(
            !pubsub.subscribers.load().contains_key(&(topic, type_hash)),
            "dropping the RAII handle must remove the subscriber entry"
        );
        assert!(
            !pubsub
                .interest_state
                .lock()
                .unwrap()
                .local_counts
                .contains_key(&topic),
            "dropping the RAII handle must release topic interest"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak_callback.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the RAII handle must tear down the subscriber worker");
    }

    #[tokio::test]
    async fn dropping_handles_unsubscribes_every_variant() {
        let pubsub = test_pubsub("pubsub-raii-all-variants");
        let topic = topic_key("raii-all-variants");
        let type_hash = 302;

        let borrowed = pubsub.subscribe_borrowed_bytes(topic, type_hash, |_| {});
        let borrowed_meta =
            pubsub.subscribe_borrowed_bytes_with_metadata(topic, type_hash, |_, _| {});
        let typed = pubsub.subscribe_type_bytes(type_hash, |_, _| {});
        assert_eq!(
            pubsub
                .borrowed_subscribers
                .load()
                .get(&(topic, type_hash))
                .map(|subs| subs.len()),
            Some(2)
        );
        assert!(pubsub.type_subscribers.load().contains_key(&type_hash));

        drop(borrowed);
        drop(borrowed_meta);
        drop(typed);
        assert!(
            !pubsub
                .borrowed_subscribers
                .load()
                .contains_key(&(topic, type_hash)),
            "dropping borrowed handles must remove borrowed subscriber entries"
        );
        assert!(
            pubsub.hot_borrowed_subscriber.load().is_none(),
            "dropping the final borrowed handle must clear the hot subscriber cache"
        );
        assert!(
            !pubsub.type_subscribers.load().contains_key(&type_hash),
            "dropping the type handle must remove the type subscriber entry"
        );
    }

    #[tokio::test]
    async fn explicit_unsubscribe_is_idempotent_with_drop() {
        let pubsub = test_pubsub("pubsub-raii-idempotent");
        let topic = topic_key("raii-idempotent");
        let type_hash = 303;
        let subscription = pubsub.subscribe_bytes(topic, type_hash, |_| {});
        assert!(pubsub.unsubscribe(subscription));
        // `unsubscribe` consumed the handle; its Drop ran after release() had
        // already fired and must not have removed anything twice. A fresh
        // subscription proves the interest counter did not underflow.
        let second = pubsub.subscribe_bytes(topic, type_hash, |_| {});
        assert_eq!(
            pubsub
                .interest_state
                .lock()
                .unwrap()
                .local_counts
                .get(&topic),
            Some(&1)
        );
        assert!(pubsub.unsubscribe(second));
    }

    #[tokio::test]
    async fn subscription_handle_does_not_keep_pubsub_alive() {
        let pubsub = test_pubsub("pubsub-raii-weak");
        let weak = Arc::downgrade(&pubsub);
        let subscription = pubsub.subscribe_bytes(topic_key("raii-weak"), 304, |_| {});
        drop(pubsub);
        assert!(
            weak.upgrade().is_none(),
            "a subscription handle must hold only a Weak back-reference"
        );
        // Dropping the handle after the engine is gone must be a no-op.
        drop(subscription);
    }

    /// Lost-update race (2026-07-17 QA wave, T1): two concurrent
    /// `subscribe_bytes` calls both `load_full` the same subscriber map,
    /// mutate private clones, and `store` — the second store erases the
    /// first writer's entry while its `PubSubSubscription` handle stays
    /// live, so that subscriber silently never receives anything.
    ///
    /// Thread A's first RMW window is widened via the test hook: it signals
    /// entry and parks ~100ms between `load_full` and `store`, during which
    /// thread B (the test thread) runs a full subscribe on a distinct
    /// topic. A delay hook (not a two-party barrier) is used deliberately:
    /// once writers are serialized, B blocks until A leaves the window, so
    /// the green run cannot deadlock.
    #[test]
    fn concurrent_subscribes_do_not_lose_a_subscriber_entry() {
        let pubsub = test_pubsub("pubsub-rmw-delay");
        let topic_a = topic_key("rmw-delay-topic-a");
        let topic_b = topic_key("rmw-delay-topic-b");
        let type_hash = 401;

        // Armed thread id: the hook fires for every subscriber-map writer in
        // the process, so it must self-filter to thread A's first window.
        let armed: Arc<std::sync::OnceLock<std::thread::ThreadId>> =
            Arc::new(std::sync::OnceLock::new());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let entered_tx = Mutex::new(entered_tx);
        let fired = AtomicBool::new(false);
        let armed_for_hook = Arc::clone(&armed);
        assert!(
            crate::test_helpers::install_pubsub_subscriber_rmw_hook(Arc::new(move || {
                if armed_for_hook.get() != Some(&std::thread::current().id()) {
                    return;
                }
                if fired.swap(true, Ordering::AcqRel) {
                    return;
                }
                let _ = entered_tx.lock().unwrap().send(());
                std::thread::sleep(Duration::from_millis(100));
            })),
            "subscriber RMW hook already installed by another test"
        );

        let delivered_a = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let delivered_b = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let pubsub_for_a = Arc::clone(&pubsub);
        let delivered_a_cb = Arc::clone(&delivered_a);
        let armed_for_a = Arc::clone(&armed);
        let thread_a = std::thread::spawn(move || {
            armed_for_a
                .set(std::thread::current().id())
                .expect("armed thread id set once");
            pubsub_for_a.subscribe_bytes(topic_a, type_hash, move |_| {
                delivered_a_cb.store(true, Ordering::Release);
            })
        });
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("thread A must enter the subscriber-map RMW window");

        // Thread B: full subscribe while A is parked inside its window.
        let delivered_b_cb = Arc::clone(&delivered_b);
        let _sub_b = pubsub.subscribe_bytes(topic_b, type_hash, move |_| {
            delivered_b_cb.store(true, Ordering::Release);
        });
        let _sub_a = thread_a.join().expect("thread A subscribe must not panic");

        for (topic, payload) in [(topic_a, &b"to-a"[..]), (topic_b, &b"to-b"[..])] {
            pubsub
                .publish_bytes(
                    topic,
                    type_hash,
                    Bytes::from_static(payload),
                    PubSubScope::LocalOnly,
                    PubSubDeliveryPolicy::default(),
                )
                .unwrap();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while (!delivered_a.load(Ordering::Acquire) || !delivered_b.load(Ordering::Acquire))
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            delivered_a.load(Ordering::Acquire),
            "subscriber A must receive its topic's publication"
        );
        assert!(
            delivered_b.load(Ordering::Acquire),
            "subscriber B holds a live subscription handle but its entry was \
             erased by thread A's stale subscriber-map store (lost update)"
        );
    }

    /// Barrier-aligned stress: two threads repeatedly subscribe and drop on
    /// distinct topics. After each quiescent phase, worker 0 checks the map:
    /// both entries must exist after the concurrent subscribes, and none
    /// after the concurrent drops (a leftover entry delivers into a dropped
    /// subscription forever). Failures are recorded and both workers exit
    /// together so a detected race cannot wedge the barrier.
    #[test]
    fn stress_concurrent_subscribe_and_drop_keep_subscriber_map_consistent() {
        const ITERATIONS: usize = 500;
        let pubsub = test_pubsub("pubsub-rmw-stress");
        let type_hash = 402;
        let keys = [
            (topic_key("rmw-stress-0"), type_hash),
            (topic_key("rmw-stress-1"), type_hash),
        ];
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        let workers: Vec<_> = (0..2)
            .map(|worker| {
                let pubsub = Arc::clone(&pubsub);
                let barrier = Arc::clone(&barrier);
                let failure = Arc::clone(&failure);
                std::thread::spawn(move || {
                    let (topic, type_hash) = keys[worker];
                    for iteration in 0..ITERATIONS {
                        barrier.wait();
                        let sub = pubsub.subscribe_bytes(topic, type_hash, |_| {});
                        barrier.wait();
                        if worker == 0 {
                            let map = pubsub.subscribers.load();
                            if !keys.iter().all(|key| map.contains_key(key)) {
                                *failure.lock().unwrap() = Some(format!(
                                    "iteration {iteration}: concurrent subscribes lost a \
                                     subscriber-map entry (lost update)"
                                ));
                            }
                        }
                        barrier.wait();
                        let abort = failure.lock().unwrap().is_some();
                        drop(sub);
                        if abort {
                            return;
                        }
                        barrier.wait();
                        if worker == 0 && !pubsub.subscribers.load().is_empty() {
                            *failure.lock().unwrap() = Some(format!(
                                "iteration {iteration}: concurrent unsubscribes left a stale \
                                 subscriber-map entry (delivery into a dropped subscription)"
                            ));
                        }
                        barrier.wait();
                        if failure.lock().unwrap().is_some() {
                            return;
                        }
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("stress worker must not panic");
        }
        if let Some(message) = failure.lock().unwrap().take() {
            panic!("{message}");
        }
    }

    #[tokio::test]
    async fn non_psf1_frame_is_dropped_with_decode_stat() {
        let pubsub = test_pubsub("pubsub-legacy-frame-drop");
        let source = crate::KeyPair::new_for_testing("pubsub-legacy-source").peer_id();
        pubsub
            .handle_pubsub_frame_borrowed(&source, b"not-a-psf1-frame")
            .expect("unrecognized frames are dropped, not errored");
        let stats = pubsub.stats();
        assert_eq!(stats.decode_drops, 1);
        assert_eq!(stats.accepted, 0);
        assert_eq!(stats.delivered_local, 0);
    }
}
