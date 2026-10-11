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
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::warn;

use crate::{GossipError, PeerId, RemoteActorLocation, Result};

const CONTROL_PLANE_FALLBACK_INTERVAL: Duration = Duration::from_secs(1);
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
/// Best-effort subscriber queues reject larger deliveries before retaining
/// them. Each admitted worker holds at most 64 queued plus one active payload.
const SUBSCRIBER_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const CONTROL_PLANE_REFRESH_ATTEMPTS: usize = 4;
const CALLBACK_MAX_DURATION: Duration = Duration::from_secs(30);
const PROVIDER_MAX_DURATION: Duration = Duration::from_millis(5);

/// Explicit cooperative callback contract. The constructor and each future poll
/// and cancellation destructors must return promptly, never block a thread,
/// and perform bounded work/memory
/// allocation. Use asynchronous waits; do not spawn detached or blocking work.
/// Captures and application-retained output are the application's memory budget.
/// The engine owns and drops the active future on unsubscribe, shutdown, or the
/// 30-second delivery deadline. Violating the poll contract cannot be safely
/// preempted by Rust and is outside this API's guarantees.
///
/// A finite synchronous operation can return `std::future::ready(())`. Borrowed
/// subscription input may only be used during invocation; pending work must own
/// its inputs. Each worker retains at most one callback future.
pub struct CooperativeCallback<F>(F);

impl<F> CooperativeCallback<F> {
    pub fn new(callback: F) -> Self {
        Self(callback)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PubSubRouteError {
    BudgetExhausted,
    InvalidDestination,
    DuplicateDestination,
    MissingDestination,
}

/// Library-owned, pre-admitted route output. Providers cannot add destinations,
/// allocate an output map, or partially publish a rejected result. Every input
/// index must receive exactly one next hop. `step` also charges provider-side
/// traversal: call it before every bounded unit of work in a variable-size loop.
pub struct PubSubRouteSink {
    next_hops: Vec<Option<PeerId>>,
    remaining_steps: usize,
    deadline: std::time::Instant,
    failure: Option<PubSubRouteError>,
}

impl PubSubRouteSink {
    fn new(destinations: usize) -> Self {
        Self {
            next_hops: vec![None; destinations],
            remaining_steps: destinations.saturating_mul(8).max(1),
            deadline: std::time::Instant::now() + PROVIDER_MAX_DURATION,
            failure: None,
        }
    }

    pub fn step(&mut self) -> std::result::Result<(), PubSubRouteError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if self.remaining_steps == 0 || std::time::Instant::now() >= self.deadline {
            self.failure = Some(PubSubRouteError::BudgetExhausted);
            return Err(PubSubRouteError::BudgetExhausted);
        }
        self.remaining_steps -= 1;
        Ok(())
    }

    pub fn route(
        &mut self,
        destination_index: usize,
        next_hop: PeerId,
    ) -> std::result::Result<(), PubSubRouteError> {
        self.step()?;
        let Some(entry) = self.next_hops.get_mut(destination_index) else {
            self.failure = Some(PubSubRouteError::InvalidDestination);
            return Err(PubSubRouteError::InvalidDestination);
        };
        if entry.is_some() {
            self.failure = Some(PubSubRouteError::DuplicateDestination);
            return Err(PubSubRouteError::DuplicateDestination);
        }
        *entry = Some(next_hop);
        Ok(())
    }

    fn finish(self, destinations: &[PeerId]) -> std::result::Result<RouteGroups, PubSubRouteError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if std::time::Instant::now() >= self.deadline {
            return Err(PubSubRouteError::BudgetExhausted);
        }
        if self.next_hops.iter().any(Option::is_none) {
            return Err(PubSubRouteError::MissingDestination);
        }
        let mut groups: HashMap<PeerId, Vec<PeerId>> = HashMap::new();
        for (next_hop, destination) in self.next_hops.into_iter().zip(destinations) {
            groups
                .entry(next_hop.expect("validated complete route"))
                .or_default()
                .push(destination.clone());
        }
        Ok(groups
            .into_iter()
            .map(|(hop, peers)| (hop, Arc::from(peers)))
            .collect())
    }
}

/// Control-plane subscription admission limits. A worker retains its permit
/// through any running callback, even after its subscription is removed.
#[derive(Clone, Copy, Debug)]
pub struct PubSubLimits {
    pub subscribers: NonZeroUsize,
    pub subscribers_per_topic: NonZeroUsize,
    /// Bounds live topic identities, including canceled dispatches waiting
    /// for registry cleanup. Churn cannot bypass this bound by unsubscribing.
    pub topics: NonZeroUsize,
    /// Maximum actor-owner records inspected and total destinations retained.
    pub routing_records: NonZeroUsize,
}

impl Default for PubSubLimits {
    fn default() -> Self {
        Self {
            subscribers: NonZeroUsize::new(256).unwrap(),
            subscribers_per_topic: NonZeroUsize::new(64).unwrap(),
            topics: NonZeroUsize::new(256).unwrap(),
            routing_records: NonZeroUsize::new(4096).unwrap(),
        }
    }
}

/// Admission failed without creating a task or publishing subscription state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PubSubAdmissionError {
    SubscriberCapacity,
    TopicCapacity,
    TopicSubscriberCapacity,
    WorkerUnavailable,
}

impl std::fmt::Display for PubSubAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pubsub admission rejected: {self:?}")
    }
}

impl std::error::Error for PubSubAdmissionError {}

#[cfg(test)]
const UDP_MAX_DATAGRAM_SIZE: usize = 65_507;

async fn wait_for_route_provider_change(
    revision: &AtomicU64,
    notify: &tokio::sync::Notify,
    after: u64,
) -> u64 {
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let current = revision.load(Ordering::Acquire);
        if current != after {
            return current;
        }
        notified.await;
    }
}

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
    cancel: tokio::sync::watch::Sender<bool>,
}

impl<T> Drop for SubscriberWorker<T> {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(abort) = &self.abort {
            abort.abort();
        }
    }
}

struct PubSubBackgroundTask {
    cancel: tokio::sync::watch::Sender<bool>,
    abort: Option<tokio::task::AbortHandle>,
}

impl Drop for PubSubBackgroundTask {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(abort) = &self.abort {
            abort.abort();
        }
    }
}

fn spawn_pubsub_background<F>(
    runtime: Option<&tokio::runtime::Handle>,
    future: F,
) -> std::result::Result<PubSubBackgroundTask, PubSubAdmissionError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (cancel, mut canceled) = tokio::sync::watch::channel(false);
    let work = async move {
        tokio::select! {
            biased;
            _ = canceled.changed() => {},
            _ = future => {},
        }
    };
    let abort = if let Some(runtime) = runtime {
        Some(runtime.spawn(work).abort_handle())
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| PubSubAdmissionError::WorkerUnavailable)?;
        std::thread::Builder::new()
            .name("icanact-pubsub-background".into())
            .spawn(move || runtime.block_on(work))
            .map_err(|_| PubSubAdmissionError::WorkerUnavailable)?;
        None
    };
    Ok(PubSubBackgroundTask { cancel, abort })
}

// Declaration order is intentional: callback captures must be destroyed before
// its admission permit. Keeping both behind a method call also prevents precise
// async capture from splitting them into independently dropped upvars.
struct SubscriberResources<F> {
    deliver: F,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<F> SubscriberResources<F> {
    fn call<T, Fut>(&self, message: T) -> Fut
    where
        F: Fn(T) -> Fut,
    {
        (self.deliver)(message)
    }
}

fn spawn_subscriber_worker<T, F, Fut>(
    runtime: Option<&tokio::runtime::Handle>,
    deliver: F,
    permit: tokio::sync::OwnedSemaphorePermit,
    expirations: Arc<AtomicU64>,
) -> std::result::Result<Arc<SubscriberWorker<T>>, PubSubAdmissionError>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
    let (cancel, mut canceled) = tokio::sync::watch::channel(false);
    let resources = SubscriberResources {
        deliver,
        _permit: permit,
    };
    let drain = async move {
        loop {
            tokio::select! {
                biased;
                _ = canceled.changed() => return,
                message = receiver.recv() => {
                    let Some(message) = message else {
                        return;
                    };
                    let delivery = resources.call(message);
                    tokio::select! {
                        biased;
                        _ = canceled.changed() => return,
                        result = tokio::time::timeout(CALLBACK_MAX_DURATION, delivery) => {
                            if result.is_err() {
                                expirations.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }
    };
    let abort = if let Some(runtime) = runtime {
        Some(runtime.spawn(drain).abort_handle())
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| PubSubAdmissionError::WorkerUnavailable)?;
        std::thread::Builder::new()
            .name("icanact-pubsub-delivery".into())
            .spawn(move || runtime.block_on(drain))
            .map_err(|_| PubSubAdmissionError::WorkerUnavailable)?;
        None
    };
    Ok(Arc::new(SubscriberWorker {
        sender,
        abort,
        cancel,
    }))
}

impl SubscriberEntry {
    fn new<F, Fut>(
        id: u64,
        runtime: Option<&tokio::runtime::Handle>,
        deliver: F,
        permit: tokio::sync::OwnedSemaphorePermit,
        expirations: Arc<AtomicU64>,
    ) -> std::result::Result<Self, PubSubAdmissionError>
    where
        F: Fn(Bytes) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let worker = spawn_subscriber_worker(runtime, deliver, permit, expirations)?;
        Ok(Self { id, worker })
    }

    #[inline]
    fn enqueue(&self, payload: Bytes) -> bool {
        payload.len() <= SUBSCRIBER_MAX_PAYLOAD_BYTES
            && self.worker.sender.try_send(payload).is_ok()
    }
}

impl BorrowedSubscriberEntry {
    fn new<F, Fut>(
        id: u64,
        runtime: Option<&tokio::runtime::Handle>,
        deliver: F,
        permit: tokio::sync::OwnedSemaphorePermit,
        expirations: Arc<AtomicU64>,
    ) -> std::result::Result<Self, PubSubAdmissionError>
    where
        F: Fn(&[u8]) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let worker = spawn_subscriber_worker(
            runtime,
            move |(payload, _metadata): (Bytes, PubSubFrameMetadata)| deliver(payload.as_ref()),
            permit,
            expirations,
        )?;
        Ok(Self { id, worker })
    }

    fn new_with_metadata<F, Fut>(
        id: u64,
        runtime: Option<&tokio::runtime::Handle>,
        deliver: F,
        permit: tokio::sync::OwnedSemaphorePermit,
        expirations: Arc<AtomicU64>,
    ) -> std::result::Result<Self, PubSubAdmissionError>
    where
        F: Fn(&[u8], PubSubFrameMetadata) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let worker = spawn_subscriber_worker(
            runtime,
            move |(payload, metadata): (Bytes, PubSubFrameMetadata)| {
                deliver(payload.as_ref(), metadata)
            },
            permit,
            expirations,
        )?;
        Ok(Self { id, worker })
    }

    #[inline]
    fn enqueue(&self, payload: Bytes, metadata: PubSubFrameMetadata) -> bool {
        payload.len() <= SUBSCRIBER_MAX_PAYLOAD_BYTES
            && self.worker.sender.try_send((payload, metadata)).is_ok()
    }
}

impl TypeSubscriberEntry {
    fn new<F, Fut>(
        id: u64,
        runtime: Option<&tokio::runtime::Handle>,
        deliver: F,
        permit: tokio::sync::OwnedSemaphorePermit,
        expirations: Arc<AtomicU64>,
    ) -> std::result::Result<Self, PubSubAdmissionError>
    where
        F: Fn(u64, Bytes) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let worker = spawn_subscriber_worker(
            runtime,
            move |(topic_key, payload)| deliver(topic_key, payload),
            permit,
            expirations,
        )?;
        Ok(Self { id, worker })
    }

    #[inline]
    fn enqueue(&self, topic_key: u64, payload: Bytes) -> bool {
        payload.len() <= SUBSCRIBER_MAX_PAYLOAD_BYTES
            && self.worker.sender.try_send((topic_key, payload)).is_ok()
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
    pub callback_expirations: u64,
    pub refresh_budget_rejections: u64,
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
    callback_expirations: Arc<AtomicU64>,
    refresh_budget_rejections: AtomicU64,
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
            callback_expirations: self.callback_expirations.load(Ordering::Relaxed),
            refresh_budget_rejections: self.refresh_budget_rejections.load(Ordering::Relaxed),
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
            )?,
        )
    }
}

/// Cooperatively bounded routing. Return within the sink's work/time budget;
/// never block, spawn detached work, or allocate variable-size temporary output.
/// Charge variable traversal with `routes.step()` and propagate errors. Fixed-size
/// next-hop decisions go through `routes.route(index, hop)`. This cooperative
/// contract is mandatory; Rust cannot preempt a deliberately non-returning call.
pub trait PubSubRouteProvider: Send + Sync {
    fn group_destinations(
        &self,
        topic_key: u64,
        destinations: &[PeerId],
        routes: &mut PubSubRouteSink,
    ) -> std::result::Result<(), PubSubRouteError>;
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
    /// Desired-state generation per topic: bumped on every 0->1 / 1->0
    /// local-subscriber-count transition. Shares its lifecycle with
    /// `local_counts` — an entry is only removed once the dispatch loop in
    /// `drive_interest_dispatch` has converged on the "absent" state for
    /// that topic, so a topic that churns to zero subscribers does not
    /// leak an entry here forever (contrast the pre-fix version of this
    /// map, which only ever inserted).
    generations: HashMap<TopicKey, u64>,
    /// Topics with a `note_interest` register/unregister dispatch loop
    /// currently running. Single-flights dispatch per topic: while a loop
    /// is in flight, further transitions only bump `generations` (observed
    /// by the in-flight loop's post-await revalidation) instead of
    /// spawning a second, independently-racing task. This is what
    /// eliminates the TOCTOU between two temporally-overlapping
    /// register/unregister calls for the same topic — there is only ever
    /// one registry call for a topic in flight at a time, and the last one
    /// dispatched is always for the most recent generation.
    dispatch_in_flight: HashSet<TopicKey>,
    dispatch_tasks: HashMap<TopicKey, PubSubBackgroundTask>,
    dispatch_receipts: HashMap<TopicKey, Arc<crate::registry::PubSubInterestDispatch>>,
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
    /// Serializes the seven subscriber-map writers (subscribe_* /
    /// remove_*_subscriber). Each writer performs a
    /// `load_full -> clone -> mutate -> store` read-modify-write on an
    /// `ArcSwap` map; without mutual exclusion two concurrent writers both
    /// clone the same snapshot and the second `store` silently erases the
    /// first writer's update (a subscriber then holds a live
    /// `PubSubSubscription` handle with no map entry — permanent silent
    /// delivery loss — or a dropped subscription keeps its stale entry).
    /// Holding the guard across `refresh_hot_borrowed_subscriber` also
    /// linearizes the hot-cache with the map. Readers stay lock-free
    /// (`ArcSwap::load`). Writers take interest_state only while holding this
    /// guard; interest workers never take the subscriber lock.
    subscriber_write_lock: Mutex<()>,
    limits: PubSubLimits,
    subscriber_permits: Arc<tokio::sync::Semaphore>,
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
    route_provider_revision: Arc<AtomicU64>,
    route_provider_change_notify: Arc<tokio::sync::Notify>,
    /// Serializes route-snapshot rebuilds. This is control-plane only;
    /// publish and ingress never acquire it.
    control_plane_refresh_lock: AsyncMutex<()>,
    last_actor_routing_revision: AtomicU64,
    last_connection_routing_revision: AtomicU64,
    last_route_provider_revision: AtomicU64,
    #[cfg(test)]
    control_plane_refreshes: AtomicU64,
    #[cfg(test)]
    control_plane_snapshot_hook_enabled: AtomicBool,
    #[cfg(test)]
    control_plane_snapshot_hook_entered: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    control_plane_snapshot_hook_release: Arc<tokio::sync::Notify>,
    /// Test-only gate for opening the exact gap between the revision check and
    /// construction of the control-plane select. A provider notification can
    /// be consumed in this window, so the provider wait future itself must
    /// re-check the revision instead of relying on the permit.
    #[cfg(test)]
    control_plane_wait_hook_enabled: AtomicBool,
    #[cfg(test)]
    control_plane_wait_hook_entered: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    control_plane_wait_hook_release: Arc<tokio::sync::Notify>,
}

impl Drop for RoutedPubSub {
    fn drop(&mut self) {
        let mut state = self
            .interest_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for receipt in state.dispatch_receipts.values() {
            receipt.cancel();
        }
        state.dispatch_receipts.clear();
        state.dispatch_tasks.clear();
        state.dispatch_in_flight.clear();
    }
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

/// Fires the test-only interest-dispatch hook (see
/// `test_helpers::fire_pubsub_interest_dispatch_hook`) immediately before
/// a `note_interest` background task issues its register/unregister
/// registry call. Compiles to a no-op in production builds.
#[inline]
#[cfg(any(test, feature = "test-helpers"))]
async fn interest_dispatch_hook(topic_key: u64, present: bool) {
    crate::test_helpers::fire_pubsub_interest_dispatch_hook(topic_key, present).await;
}

#[inline]
#[cfg(not(any(test, feature = "test-helpers")))]
async fn interest_dispatch_hook(_topic_key: u64, _present: bool) {}

impl RoutedPubSub {
    pub async fn install(registry: Arc<crate::registry::GossipRegistry>) -> Arc<Self> {
        Self::install_inner(registry, PubSubLimits::default()).await
    }

    pub async fn install_with_limits(
        registry: Arc<crate::registry::GossipRegistry>,
        limits: PubSubLimits,
    ) -> Result<Arc<Self>> {
        if limits.subscribers.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(GossipError::InvalidConfig(
                "pubsub subscriber capacity exceeds semaphore maximum".into(),
            ));
        }
        Ok(Self::install_inner(registry, limits).await)
    }

    async fn install_inner(
        registry: Arc<crate::registry::GossipRegistry>,
        limits: PubSubLimits,
    ) -> Arc<Self> {
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
            subscriber_write_lock: Mutex::new(()),
            limits,
            subscriber_permits: Arc::new(tokio::sync::Semaphore::new(limits.subscribers.get())),
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
            route_provider_revision: Arc::new(AtomicU64::new(0)),
            route_provider_change_notify: Arc::new(tokio::sync::Notify::new()),
            control_plane_refresh_lock: AsyncMutex::new(()),
            last_actor_routing_revision: AtomicU64::new(u64::MAX),
            last_connection_routing_revision: AtomicU64::new(u64::MAX),
            last_route_provider_revision: AtomicU64::new(u64::MAX),
            #[cfg(test)]
            control_plane_refreshes: AtomicU64::new(0),
            #[cfg(test)]
            control_plane_snapshot_hook_enabled: AtomicBool::new(false),
            #[cfg(test)]
            control_plane_snapshot_hook_entered: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            control_plane_snapshot_hook_release: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            control_plane_wait_hook_enabled: AtomicBool::new(false),
            #[cfg(test)]
            control_plane_wait_hook_entered: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            control_plane_wait_hook_release: Arc::new(tokio::sync::Notify::new()),
        });
        this.registry
            .set_pubsub_ingress_handler(Arc::clone(&this))
            .await;
        Self::spawn_control_plane(&this);
        this
    }

    pub fn set_route_provider(&self, provider: Arc<dyn PubSubRouteProvider>) {
        self.route_provider.store(Arc::new(Some(provider)));
        self.route_provider_revision.fetch_add(1, Ordering::AcqRel);
        self.route_provider_change_notify.notify_one();
    }

    fn group_destinations_bounded(
        &self,
        topic: u64,
        destinations: &[PeerId],
    ) -> std::result::Result<RouteGroups, PubSubRouteError> {
        if destinations.len() > self.limits.routing_records.get() {
            return Err(PubSubRouteError::BudgetExhausted);
        }
        let mut routes = PubSubRouteSink::new(destinations.len());
        if let Some(provider) = self.route_provider.load().as_ref() {
            provider.group_destinations(topic, destinations, &mut routes)?;
        } else {
            for (index, peer) in destinations.iter().enumerate() {
                routes.route(index, peer.clone())?;
            }
        }
        routes.finish(destinations)
    }

    fn next_msg_id(&self) -> u128 {
        let counter = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        (u128::from(self.msg_id_epoch) << 64) | u128::from(counter)
    }

    pub fn stats(&self) -> PubSubIngressStats {
        self.counters.snapshot()
    }

    /// Takes the subscriber-map writer lock (see `subscriber_write_lock`).
    /// A poisoned lock is recovered: the maps are only ever replaced
    /// wholesale via `store`, so a writer that panicked mid-window leaves
    /// the last consistent snapshot in place.
    fn lock_subscriber_writes(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.subscriber_write_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn admit_subscriber(
        &self,
        topic: Option<u64>,
        type_hash: u64,
    ) -> std::result::Result<tokio::sync::OwnedSemaphorePermit, PubSubAdmissionError> {
        let count = if let Some(topic) = topic {
            let state = self
                .interest_state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !state.generations.contains_key(&topic)
                && state.generations.len() >= self.limits.topics.get()
            {
                return Err(PubSubAdmissionError::TopicCapacity);
            }
            self.subscribers
                .load()
                .iter()
                .filter(|((key, _), _)| *key == topic)
                .map(|(_, entries)| entries.len())
                .sum::<usize>()
                + self
                    .borrowed_subscribers
                    .load()
                    .iter()
                    .filter(|((key, _), _)| *key == topic)
                    .map(|(_, entries)| entries.len())
                    .sum::<usize>()
        } else {
            self.type_subscribers
                .load()
                .get(&type_hash)
                .map_or(0, |entries| entries.len())
        };
        if count >= self.limits.subscribers_per_topic.get() {
            return Err(PubSubAdmissionError::TopicSubscriberCapacity);
        }
        Arc::clone(&self.subscriber_permits)
            .try_acquire_owned()
            .map_err(|_| PubSubAdmissionError::SubscriberCapacity)
    }

    /// Register a best-effort callback. Saturation returns a typed admission
    /// error before publication. The explicit cooperative callback is owned by
    /// its worker and canceled before its admission permit is released.
    pub fn subscribe_bytes<F, Fut>(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: CooperativeCallback<F>,
    ) -> std::result::Result<PubSubSubscription, PubSubAdmissionError>
    where
        F: Fn(Bytes) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let write_guard = self.lock_subscriber_writes();
        let permit = self.admit_subscriber(Some(topic_key), type_hash)?;
        let mut next = (*self.subscribers.load_full()).clone();
        subscriber_rmw_window_hook();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        topic_subs.push(SubscriberEntry::new(
            id,
            runtime.as_ref(),
            deliver.0,
            permit,
            Arc::clone(&self.counters.callback_expirations),
        )?);
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.note_interest(topic_key, true)?;
        self.subscribers.store(Arc::new(next));
        drop(write_guard);
        Ok(self.make_handle(
            SubscriptionKind::Owned {
                topic_key,
                type_hash,
            },
            id,
        ))
    }

    pub fn subscribe_borrowed_bytes<F, Fut>(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: CooperativeCallback<F>,
    ) -> std::result::Result<PubSubSubscription, PubSubAdmissionError>
    where
        F: Fn(&[u8]) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let write_guard = self.lock_subscriber_writes();
        let permit = self.admit_subscriber(Some(topic_key), type_hash)?;
        let mut next = (*self.borrowed_subscribers.load_full()).clone();
        subscriber_rmw_window_hook();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        let entry = BorrowedSubscriberEntry::new(
            id,
            runtime.as_ref(),
            deliver.0,
            permit,
            Arc::clone(&self.counters.callback_expirations),
        )?;
        topic_subs.push(entry);
        self.note_interest(topic_key, true)?;
        self.refresh_hot_borrowed_subscriber(key, &topic_subs);
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.borrowed_subscribers.store(Arc::new(next));
        drop(write_guard);
        Ok(self.make_handle(
            SubscriptionKind::Borrowed {
                topic_key,
                type_hash,
            },
            id,
        ))
    }

    pub fn subscribe_borrowed_bytes_with_metadata<F, Fut>(
        &self,
        topic_key: u64,
        type_hash: u64,
        deliver: CooperativeCallback<F>,
    ) -> std::result::Result<PubSubSubscription, PubSubAdmissionError>
    where
        F: Fn(&[u8], PubSubFrameMetadata) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let key = (topic_key, type_hash);
        let write_guard = self.lock_subscriber_writes();
        let permit = self.admit_subscriber(Some(topic_key), type_hash)?;
        let mut next = (*self.borrowed_subscribers.load_full()).clone();
        subscriber_rmw_window_hook();
        let mut topic_subs = next.get(&key).map(|subs| subs.to_vec()).unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        let entry = BorrowedSubscriberEntry::new_with_metadata(
            id,
            runtime.as_ref(),
            deliver.0,
            permit,
            Arc::clone(&self.counters.callback_expirations),
        )?;
        topic_subs.push(entry);
        self.note_interest(topic_key, true)?;
        self.refresh_hot_borrowed_subscriber(key, &topic_subs);
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.borrowed_subscribers.store(Arc::new(next));
        drop(write_guard);
        Ok(self.make_handle(
            SubscriptionKind::Borrowed {
                topic_key,
                type_hash,
            },
            id,
        ))
    }

    pub fn subscribe_type_bytes<F, Fut>(
        &self,
        type_hash: u64,
        deliver: CooperativeCallback<F>,
    ) -> std::result::Result<PubSubSubscription, PubSubAdmissionError>
    where
        F: Fn(u64, Bytes) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let _write_guard = self.lock_subscriber_writes();
        let permit = self.admit_subscriber(None, type_hash)?;
        let mut next = (*self.type_subscribers.load_full()).clone();
        subscriber_rmw_window_hook();
        let mut subs = next
            .get(&type_hash)
            .map(|subs| subs.to_vec())
            .unwrap_or_default();
        let runtime = tokio::runtime::Handle::try_current().ok();
        subs.push(TypeSubscriberEntry::new(
            id,
            runtime.as_ref(),
            deliver.0,
            permit,
            Arc::clone(&self.counters.callback_expirations),
        )?);
        next.insert(type_hash, Arc::from(subs.into_boxed_slice()));
        self.type_subscribers.store(Arc::new(next));
        Ok(self.make_handle(SubscriptionKind::Type { type_hash }, id))
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
        let write_guard = self.lock_subscriber_writes();
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
        let _ = self.note_interest(topic_key, false);
        drop(write_guard);
        true
    }

    fn remove_borrowed_subscriber(&self, topic_key: u64, type_hash: u64, id: u64) -> bool {
        let key = (topic_key, type_hash);
        let write_guard = self.lock_subscriber_writes();
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
        let _ = self.note_interest(topic_key, false);
        drop(write_guard);
        true
    }

    fn remove_type_subscriber(&self, type_hash: u64, id: u64) -> bool {
        let _write_guard = self.lock_subscriber_writes();
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
        let payload = crate::typed::encode_typed_owned(msg)?;
        self.publish_bytes(topic_key, M::TYPE_HASH, payload, scope, policy)
    }

    /// Publish a payload whose complete backing allocation is known. Admission
    /// charges retained capacity, so a small view cannot hide an oversized owner.
    /// Arbitrary Bytes require explicit ForwardPayload::copy_from_slice at the
    /// caller; Vec, boxed slice and pooled payload ownership moves without copy.
    pub fn publish_bytes(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: crate::ForwardPayload,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
    ) -> Result<PubSubPublishStats> {
        Self::validate_delivery_mode(policy.mode)?;
        if payload.retained_bytes() > SUBSCRIBER_MAX_PAYLOAD_BYTES {
            return Err(GossipError::WriteQueueFull);
        }
        let payload = payload.into_bytes();
        let mut stats = PubSubPublishStats::default();
        // Provider admission must succeed before any local callback is queued:
        // returning overload after local delivery would make a retry duplicate it.
        self.publish_remote_bytes_inner(
            topic_key,
            type_hash,
            Bytes::clone(&payload),
            scope,
            policy,
            &mut stats,
        )?;
        stats.local_delivered = self.deliver_local(topic_key, type_hash, payload);
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
                    // R-1: no route for this topic (e.g. the owning
                    // subscriber's interest actor was TTL-reaped). Surface it
                    // via the route-miss stat instead of a silent Ok with zero
                    // sends.
                    stats.remote_route_miss = stats.remote_route_miss.saturating_add(1);
                    warn!(
                        topic = ?topic_key,
                        "publish to topic with no remote route (remote_route_miss)"
                    );
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
                if peers.len() > self.limits.routing_records.get() {
                    return Err(GossipError::WriteQueueFull);
                }
                let peers: Vec<_> = peers
                    .into_iter()
                    .filter(|peer| peer != &self.local_peer_id)
                    .collect();
                let groups = self
                    .group_destinations_bounded(topic_key, &peers)
                    .map_err(|_| GossipError::WriteQueueFull)?;
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

    /// Rebuild a complete bounded route snapshot. Concurrent requests coalesce;
    /// excess records or four changing snapshots retain the previous revision,
    /// increment `refresh_budget_rejections`, and impose a one-second cooldown.
    pub async fn refresh_control_plane(&self) {
        self.refresh_control_plane_inner().await;
    }

    async fn refresh_control_plane_if_changed(&self) {
        self.refresh_control_plane_inner().await;
    }

    async fn refresh_control_plane_inner(&self) {
        // Concurrent refresh requests coalesce; never retain an unbounded
        // queue of lock waiters behind one provider or route rebuild.
        let Ok(_refresh_guard) = self.control_plane_refresh_lock.try_lock() else {
            return;
        };
        #[cfg(test)]
        self.control_plane_refreshes.fetch_add(1, Ordering::Relaxed);

        // A route rebuild reads several independently revisioned projections.
        // Keep the refresh lock across the whole operation, then verify that
        // none of those revisions changed while the snapshot was assembled.
        // Publishing the route map with a revision captured before a concurrent
        // mutation would acknowledge that mutation without incorporating it;
        // the control-plane loop could then park until the fallback timer.
        for _ in 0..CONTROL_PLANE_REFRESH_ATTEMPTS {
            let actor_revision = self.registry.actor_state.pubsub_routing_revision();
            let connection_revision = self.registry.connection_pool.routing_revision();
            let provider_revision = self.route_provider_revision.load(Ordering::Acquire);
            let has_provider = self.route_provider.load().is_some();
            if self.last_actor_routing_revision.load(Ordering::Acquire) == actor_revision
                && self
                    .last_connection_routing_revision
                    .load(Ordering::Acquire)
                    == connection_revision
                && self.last_route_provider_revision.load(Ordering::Acquire) == provider_revision
                && self.conns.load().values().all(|conn| !conn.is_closed())
                // A provider is intentionally re-evaluated on every actual
                // refresh. Its public trait accepts `&self`, so interior
                // mutable topology has no revision to wake this loop; the
                // one-second fallback is the provider's invalidation path.
                && !has_provider
            {
                return;
            }

            let Some(actors) = self
                .registry
                .try_snapshot_actor_owners(self.limits.routing_records.get(), 256)
            else {
                self.reject_refresh_budget().await;
                return;
            };
            let mut interests: HashMap<TopicKey, HashSet<PeerId>> = HashMap::new();
            for (name, actor_owner) in actors {
                if let Some((topic, peer)) = parse_interest_name(&name) {
                    // REMOTE-3: a name cannot widen an actor owner's claim
                    // into a subscription on behalf of another identity.
                    if peer == actor_owner {
                        interests.entry(topic).or_default().insert(peer);
                    }
                }
            }

            let mut next_routes = HashMap::new();
            let mut route_records = 0usize;
            let mut next_conns = (*self.conns.load_full()).clone();
            for (topic, peers) in interests {
                let peers: Vec<PeerId> = peers
                    .into_iter()
                    .filter(|peer| peer != &self.local_peer_id)
                    .collect();
                if peers.is_empty() {
                    continue;
                }
                let grouped = match self.group_destinations_bounded(topic, &peers) {
                    Ok(groups) => groups,
                    Err(_) => {
                        self.reject_refresh_budget().await;
                        return;
                    }
                };

                // A provider may regroup destinations, but cannot amplify
                // retained route state beyond the configured record budget.
                for destinations in grouped.values() {
                    route_records = route_records.saturating_add(destinations.len().max(1));
                    if route_records > self.limits.routing_records.get() {
                        self.reject_refresh_budget().await;
                        return;
                    }
                }

                // Subscriptions are gated on the pool already holding a live
                // connection to each next-hop. We deliberately do NOT call
                // `client.lookup_peer` here: it goes through
                // `pool.get_connection_to_peer` → `get_connection_by_peer_id`,
                // which warn-logs the "No connection found for peer" pair on
                // every miss (`connection_pool::pool_connect.rs:669-682`).
                // Before route changes became event-driven, this refresh ticked
                // every 25 ms. An unreachable peer whose interest entry is being
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

            #[cfg(test)]
            if self
                .control_plane_snapshot_hook_enabled
                .swap(false, Ordering::AcqRel)
            {
                self.control_plane_snapshot_hook_entered.notify_one();
                self.control_plane_snapshot_hook_release.notified().await;
            }

            // Do not acknowledge a revision that changed while the snapshot
            // was being built. The next pass incorporates that mutation before
            // this refresh publishes any state or baseline.
            let actor_after = self.registry.actor_state.pubsub_routing_revision();
            let connection_after = self.registry.connection_pool.routing_revision();
            let provider_after = self.route_provider_revision.load(Ordering::Acquire);
            if actor_after != actor_revision
                || connection_after != connection_revision
                || provider_after != provider_revision
            {
                tokio::task::yield_now().await;
                continue;
            }

            self.refresh_hot_route_groups(&next_routes, &next_conns);
            self.route_groups.store(Arc::new(next_routes));
            self.conns.store(Arc::new(next_conns));
            self.last_actor_routing_revision
                .store(actor_revision, Ordering::Release);
            self.last_connection_routing_revision
                .store(connection_revision, Ordering::Release);
            self.last_route_provider_revision
                .store(provider_revision, Ordering::Release);
            return;
        }
        self.reject_refresh_budget().await;
    }

    async fn reject_refresh_budget(&self) {
        self.counters
            .refresh_budget_rejections
            .fetch_add(1, Ordering::Relaxed);
        // Retain the last complete snapshot without acknowledging revisions.
        // A one-second cooldown prevents continuous churn from busy-spinning.
        tokio::time::sleep(CONTROL_PLANE_FALLBACK_INTERVAL).await;
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
        if payload.len() > SUBSCRIBER_MAX_PAYLOAD_BYTES {
            self.counters
                .subscriber_queue_drops
                .fetch_add(1, Ordering::Relaxed);
            return 0;
        }
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

    fn note_interest(
        &self,
        topic_key: u64,
        present: bool,
    ) -> std::result::Result<(), PubSubAdmissionError> {
        let mut state = self
            .interest_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let previous = state.local_counts.get(&topic_key).copied().unwrap_or(0);
        let next = if present {
            previous.saturating_add(1)
        } else {
            previous.saturating_sub(1)
        };
        let transition = (present && previous == 0) || (!present && previous == 1);
        if transition && !state.dispatch_in_flight.contains(&topic_key) {
            let registry = Arc::clone(&self.registry);
            let peer = self.local_peer_id.clone();
            let interest_state = Arc::clone(&self.interest_state);
            let runtime = tokio::runtime::Handle::try_current().ok();
            match spawn_pubsub_background(runtime.as_ref(), async move {
                Self::drive_interest_dispatch(interest_state, registry, peer, topic_key).await;
            }) {
                Ok(task) => {
                    state.dispatch_tasks.insert(topic_key, task);
                    state.dispatch_in_flight.insert(topic_key);
                }
                Err(error) if present => return Err(error),
                Err(_) => {
                    // Cleanup does not depend on task creation. The receipt's
                    // pre-reserved registry queue owns lossless removal gossip.
                    if let Some(receipt) = state.dispatch_receipts.remove(&topic_key) {
                        receipt.cancel();
                    }
                    state.local_counts.remove(&topic_key);
                    state.generations.remove(&topic_key);
                    return Ok(());
                }
            }
        }
        if next == 0 {
            state.local_counts.remove(&topic_key);
        } else {
            state.local_counts.insert(topic_key, next);
        }
        if transition {
            let generation = state
                .generations
                .get(&topic_key)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            state.generations.insert(topic_key, generation);
            if let Some(receipt) = state.dispatch_receipts.remove(&topic_key) {
                receipt.cancel();
            }
            if present {
                state.dispatch_receipts.insert(
                    topic_key,
                    Arc::new(crate::registry::PubSubInterestDispatch::default()),
                );
            }
        }
        Ok(())
    }

    /// One cancellation-owned task per admitted topic. Changing desired state
    /// cancels the old receipt before the next operation can publish. Failed
    /// admission remains pending and retries at a bounded rate; it is never
    /// acknowledged as a completed registration. Registry cleanup is reserved
    /// before publication and survives task cancellation without detached work.
    async fn drive_interest_dispatch(
        interest_state: Arc<Mutex<InterestState>>,
        registry: Arc<crate::registry::GossipRegistry>,
        peer: PeerId,
        topic_key: u64,
    ) {
        loop {
            let (want_present, generation, receipt) = {
                let state = interest_state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                (
                    state.local_counts.get(&topic_key).copied().unwrap_or(0) > 0,
                    state.generations.get(&topic_key).copied().unwrap_or(0),
                    state.dispatch_receipts.get(&topic_key).cloned(),
                )
            };
            interest_dispatch_hook(topic_key, want_present).await;
            let completed = if want_present {
                let Some(receipt) = receipt.as_ref() else {
                    return;
                };
                let location =
                    RemoteActorLocation::new_with_peer(registry.advertised_addr(), peer.clone());
                tokio::select! {
                    biased;
                    _ = receipt.cancelled() => false,
                    result = tokio::time::timeout(
                        CALLBACK_MAX_DURATION,
                        registry.register_pubsub_interest(
                            interest_name(topic_key, &peer), location, &receipt,
                        ),
                    ) => {
                        match result {
                            Ok(Ok(())) => true,
                            Ok(Err(error)) => {
                                warn!(
                                    topic_key,
                                    %error,
                                    "pubsub interest registration remains pending"
                                );
                                false
                            }
                            Err(_) => false,
                        }
                    }
                }
            } else {
                // `note_interest(false)` already canceled the exact receipt
                // synchronously. Only its lossless gossip journal needs flush.
                tokio::time::timeout(
                    CALLBACK_MAX_DURATION,
                    registry.flush_pubsub_interest_cleanup(),
                )
                .await
                .is_ok()
            };
            if completed {
                // Fanout futures are scoped to this call. Regular pending
                // gossip remains authoritative if this best-latency attempt is
                // canceled, times out, or fails to reach a peer.
                if let Some(receipt) = receipt.as_ref() {
                    tokio::select! {
                        biased;
                        _ = receipt.cancelled() => {},
                        _ = tokio::time::timeout(
                            CALLBACK_MAX_DURATION,
                            registry.trigger_immediate_gossip(),
                        ) => {},
                    }
                } else {
                    let _ = tokio::time::timeout(
                        CALLBACK_MAX_DURATION,
                        registry.trigger_immediate_gossip(),
                    )
                    .await;
                }
            }
            let (converged, changed) = {
                let mut state = interest_state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let current = state.generations.get(&topic_key).copied().unwrap_or(0);
                if current == generation && completed {
                    state.dispatch_in_flight.remove(&topic_key);
                    state.dispatch_tasks.remove(&topic_key);
                    if !want_present {
                        state.generations.remove(&topic_key);
                    }
                    (true, false)
                } else {
                    (false, current != generation)
                }
            };
            if converged {
                return;
            }
            if completed || changed {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(CONTROL_PLANE_FALLBACK_INTERVAL).await;
            }
        }
    }

    fn spawn_control_plane(this: &Arc<Self>) {
        let weak = Arc::downgrade(this);
        let actor_state = Arc::clone(&this.registry.actor_state);
        let connection_pool = Arc::clone(&this.registry.connection_pool);
        let provider_revision_signal = Arc::clone(&this.route_provider_revision);
        let provider_changed = Arc::clone(&this.route_provider_change_notify);
        tokio::spawn(async move {
            let Some(initial) = weak.upgrade() else {
                return;
            };
            initial.refresh_control_plane().await;

            let mut actor_revision = initial.last_actor_routing_revision.load(Ordering::Acquire);
            let mut connection_revision = initial
                .last_connection_routing_revision
                .load(Ordering::Acquire);
            let mut provider_revision =
                initial.last_route_provider_revision.load(Ordering::Acquire);
            drop(initial);

            loop {
                // A provider notification can be consumed or lost while a
                // refresh holds its serialization guard. Re-check every
                // revision before constructing the next wait future so a
                // mutation that landed between refresh completion and this
                // loop iteration forces an immediate rebuild instead of
                // parking until the fallback timer.
                let current_actor_revision = actor_state.pubsub_routing_revision();
                let current_connection_revision = connection_pool.routing_revision();
                let current_provider_revision = provider_revision_signal.load(Ordering::Acquire);
                if current_actor_revision != actor_revision
                    || current_connection_revision != connection_revision
                    || current_provider_revision != provider_revision
                {
                    let Some(this) = weak.upgrade() else {
                        return;
                    };
                    this.refresh_control_plane_if_changed().await;
                    actor_revision = this.last_actor_routing_revision.load(Ordering::Acquire);
                    connection_revision = this
                        .last_connection_routing_revision
                        .load(Ordering::Acquire);
                    provider_revision = this.last_route_provider_revision.load(Ordering::Acquire);
                    continue;
                }

                #[cfg(test)]
                if let Some(this) = weak.upgrade()
                    && this
                        .control_plane_wait_hook_enabled
                        .swap(false, Ordering::AcqRel)
                {
                    this.control_plane_wait_hook_entered.notify_one();
                    this.control_plane_wait_hook_release.notified().await;
                }

                tokio::select! {
                    _ = actor_state.wait_for_pubsub_routing_change(actor_revision) => false,
                    _ = connection_pool.wait_for_routing_change(connection_revision) => false,
                    _ = wait_for_route_provider_change(
                        provider_revision_signal.as_ref(),
                        provider_changed.as_ref(),
                        provider_revision,
                    ) => false,
                    _ = tokio::time::sleep(CONTROL_PLANE_FALLBACK_INTERVAL) => true,
                };
                let Some(this) = weak.upgrade() else {
                    return;
                };
                // Both event-driven and fallback passes use the same stable
                // snapshot builder. A provider has no revision contract for
                // interior state, so every actual pass re-evaluates it; the
                // fallback is the bounded polling path when no notification
                // exists for that state change.
                this.refresh_control_plane_if_changed().await;
                actor_revision = this.last_actor_routing_revision.load(Ordering::Acquire);
                connection_revision = this
                    .last_connection_routing_revision
                    .load(Ordering::Acquire);
                provider_revision = this.last_route_provider_revision.load(Ordering::Acquire);
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

        if decoded.destination_count > self.limits.routing_records.get() {
            self.counters
                .queue_full_drops
                .fetch_add(1, Ordering::Relaxed);
            return Err(GossipError::WriteQueueFull);
        }

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
        let grouped = match self.group_destinations_bounded(decoded.topic_key, &remaining) {
            Ok(groups) => groups,
            Err(_) => {
                self.counters
                    .queue_full_drops
                    .fetch_add(1, Ordering::Relaxed);
                return Err(GossipError::WriteQueueFull);
            }
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
    })
    .ok()?;
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
    let header = crate::framing::try_write_pubsub_frame_prefix(payload_len).ok()?;
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
    .ok()
}

fn fast_frame_len(destination_peers: &[PeerId], payload: &[u8]) -> usize {
    FAST_FRAME_HEADER_LEN
        .saturating_add(
            destination_peers
                .len()
                .min(u16::MAX as usize)
                .saturating_mul(FAST_FRAME_DEST_PEER_LEN),
        )
        .saturating_add(payload.len())
}

// Statically dispatched bulk writes preserve the existing one-copy-per-slice
// frame assembly; generic Extend<u8> would add a capacity branch per byte.
trait FastFrameBuffer {
    fn extend_from_slice(&mut self, bytes: &[u8]);

    fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }
}

impl FastFrameBuffer for Vec<u8> {
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes);
    }
}

impl FastFrameBuffer for crate::typed::BoundedByteWriter {
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        crate::typed::BoundedByteWriter::extend_from_slice(self, bytes);
    }
}

#[allow(clippy::too_many_arguments)]
fn write_fast_frame(
    out: &mut impl FastFrameBuffer,
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

pub(crate) fn is_interest_actor_name(name: &str) -> bool {
    name.strip_prefix(INTEREST_PREFIX)
        .is_some_and(|suffix| suffix.starts_with('/'))
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

    #[test]
    fn fast_frame_length_matches_encoded_destination_cap() {
        let peer = crate::KeyPair::new_for_testing("qa-fast-frame-count").peer_id();
        let destinations = vec![peer.clone(); u16::MAX as usize + 1];
        let payload = b"payload after destinations";
        for count in [1, u16::MAX as usize, u16::MAX as usize + 1] {
            let (encoded, prefix, declared_len) = encode_fast_frame_pooled(
                1,
                2,
                3,
                &peer,
                &peer,
                4,
                PubSubDeliveryMode::AtMostOnce,
                PubSubFrameMetadata::default(),
                &destinations[..count],
                payload,
            )
            .expect("bounded frame must encode");
            assert!(prefix.is_none());
            assert_eq!(declared_len, encoded.remaining());
            let parsed = FastFrameView::parse(encoded.chunk()).expect("valid fast frame");
            assert_eq!(parsed.destination_count, count.min(u16::MAX as usize));
            assert_eq!(parsed.payload, payload);
        }
    }

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
            subscriber_write_lock: Mutex::new(()),
            limits: PubSubLimits::default(),
            subscriber_permits: Arc::new(tokio::sync::Semaphore::new(
                PubSubLimits::default().subscribers.get(),
            )),
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
            route_provider_revision: Arc::new(AtomicU64::new(0)),
            route_provider_change_notify: Arc::new(tokio::sync::Notify::new()),
            control_plane_refresh_lock: AsyncMutex::new(()),
            last_actor_routing_revision: AtomicU64::new(u64::MAX),
            last_connection_routing_revision: AtomicU64::new(u64::MAX),
            last_route_provider_revision: AtomicU64::new(u64::MAX),
            control_plane_refreshes: AtomicU64::new(0),
            #[cfg(test)]
            control_plane_snapshot_hook_enabled: AtomicBool::new(false),
            #[cfg(test)]
            control_plane_snapshot_hook_entered: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            control_plane_snapshot_hook_release: Arc::new(tokio::sync::Notify::new()),
            control_plane_wait_hook_enabled: AtomicBool::new(false),
            control_plane_wait_hook_entered: Arc::new(tokio::sync::Notify::new()),
            control_plane_wait_hook_release: Arc::new(tokio::sync::Notify::new()),
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
                vec![
                    SubscriberEntry::new(
                        1,
                        tokio::runtime::Handle::try_current().ok().as_ref(),
                        move |message| {
                            deliver(message);
                            std::future::ready(())
                        },
                        Arc::clone(&pubsub.subscriber_permits)
                            .try_acquire_owned()
                            .unwrap(),
                        Arc::clone(&pubsub.counters.callback_expirations),
                    )
                    .expect("test subscriber worker"),
                ]
                .into_boxed_slice(),
            ),
        );
        pubsub.subscribers.store(Arc::new(next));
    }

    #[tokio::test]
    async fn unchanged_control_plane_tick_reuses_published_routes() {
        let pubsub = test_pubsub("pubsub-idle-control-plane");

        pubsub.refresh_control_plane().await;
        let first = pubsub.route_groups.load_full();
        pubsub.refresh_control_plane().await;
        let second = pubsub.route_groups.load_full();

        assert!(
            Arc::ptr_eq(&first, &second),
            "an unchanged 25 ms control-plane tick must not rebuild and republish route state"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_control_plane_parks_instead_of_ticking_every_25_ms() {
        let pubsub = test_pubsub("pubsub-idle-control-plane-parks");
        RoutedPubSub::spawn_control_plane(&pubsub);

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let refreshes = pubsub.control_plane_refreshes.load(Ordering::Relaxed);
        assert!(
            refreshes <= 1,
            "idle control plane refreshed {refreshes} times in 150 ms"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn interest_actor_change_wakes_parked_control_plane_without_waiting_for_fallback() {
        let pubsub = test_pubsub("pubsub-control-plane-notify");
        RoutedPubSub::spawn_control_plane(&pubsub);
        tokio::task::yield_now().await;
        let before = pubsub.control_plane_refreshes.load(Ordering::Relaxed);

        let interested_peer =
            crate::KeyPair::new_for_testing("pubsub-control-plane-notify-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(topic_key("control-plane-notify-probe"), &interested_peer),
                RemoteActorLocation::new_with_peer(
                    "127.0.0.1:19002".parse().unwrap(),
                    pubsub.local_peer_id.clone(),
                ),
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            pubsub.control_plane_refreshes.load(Ordering::Relaxed),
            before + 1,
            "actor mutation must wake route refresh immediately without advancing time"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unrelated_actor_change_does_not_wake_pubsub_control_plane() {
        let pubsub = test_pubsub("pubsub-control-plane-unrelated-actor");
        RoutedPubSub::spawn_control_plane(&pubsub);
        tokio::task::yield_now().await;
        let before = pubsub.control_plane_refreshes.load(Ordering::Relaxed);

        pubsub
            .registry
            .register_actor(
                "unrelated-service".to_owned(),
                RemoteActorLocation::new_with_peer(
                    "127.0.0.1:19003".parse().unwrap(),
                    pubsub.local_peer_id.clone(),
                ),
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            pubsub.control_plane_refreshes.load(Ordering::Relaxed),
            before,
            "unrelated actor churn must not rebuild pubsub routes"
        );
    }

    struct CountingRouteProvider {
        calls: AtomicU64,
    }

    impl PubSubRouteProvider for CountingRouteProvider {
        fn group_destinations(
            &self,
            _topic_key: u64,
            destinations: &[PeerId],
            routes: &mut PubSubRouteSink,
        ) -> std::result::Result<(), PubSubRouteError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            for (index, peer) in destinations.iter().enumerate() {
                routes.route(index, peer.clone())?;
            }
            Ok(())
        }
    }

    struct NotifyingRouteProvider {
        calls: AtomicU64,
        called: Arc<tokio::sync::Notify>,
    }

    impl PubSubRouteProvider for NotifyingRouteProvider {
        fn group_destinations(
            &self,
            _topic_key: u64,
            destinations: &[PeerId],
            routes: &mut PubSubRouteSink,
        ) -> std::result::Result<(), PubSubRouteError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.called.notify_one();
            for (index, peer) in destinations.iter().enumerate() {
                routes.route(index, peer.clone())?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn interest_name_cannot_claim_a_different_location_owner() {
        let pubsub = test_pubsub("pubsub-interest-owner");
        let provider = Arc::new(CountingRouteProvider {
            calls: AtomicU64::new(0),
        });
        pubsub.set_route_provider(provider.clone());
        let claimed_peer = crate::KeyPair::new_for_testing("pubsub-claimed-peer").peer_id();
        let topic = topic_key("interest-owner");
        let name = interest_name(topic, &claimed_peer);
        let addr = "127.0.0.1:19009".parse().unwrap();
        pubsub
            .registry
            .register_actor(
                name.clone(),
                RemoteActorLocation::new_with_peer(addr, pubsub.local_peer_id.clone()),
            )
            .await
            .unwrap();
        let snapshot = pubsub.registry.snapshot_known_actors();
        assert!(snapshot.iter().any(|(actor, location)| {
            actor == &name && location.peer_id == pubsub.local_peer_id
        }));
        pubsub.refresh_control_plane().await;
        assert_eq!(provider.calls.load(Ordering::Relaxed), 0);

        pubsub.registry.unregister_actor(&name).await.unwrap();
        pubsub
            .registry
            .register_actor(name, RemoteActorLocation::new_with_peer(addr, claimed_peer))
            .await
            .unwrap();
        pubsub.refresh_control_plane().await;
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn explicit_refresh_rechecks_stateful_route_provider() {
        let pubsub = test_pubsub("pubsub-stateful-route-provider");
        let provider = Arc::new(CountingRouteProvider {
            calls: AtomicU64::new(0),
        });
        pubsub.set_route_provider(provider.clone());
        let interested_peer =
            crate::KeyPair::new_for_testing("pubsub-stateful-route-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(topic_key("stateful-provider"), &interested_peer),
                RemoteActorLocation::new_with_peer(
                    "127.0.0.1:19004".parse().unwrap(),
                    interested_peer,
                ),
            )
            .await
            .unwrap();

        pubsub.refresh_control_plane().await;
        pubsub.refresh_control_plane().await;

        assert_eq!(
            provider.calls.load(Ordering::Relaxed),
            2,
            "explicit/fallback refresh must re-evaluate provider interior state"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_refresh_rechecks_stateful_route_provider() {
        let pubsub = test_pubsub("pubsub-stateful-route-provider-fallback");
        let provider = Arc::new(CountingRouteProvider {
            calls: AtomicU64::new(0),
        });
        pubsub.set_route_provider(provider.clone());
        let interested_peer =
            crate::KeyPair::new_for_testing("pubsub-stateful-fallback-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(topic_key("stateful-fallback"), &interested_peer),
                RemoteActorLocation::new_with_peer(
                    "127.0.0.1:19008".parse().unwrap(),
                    interested_peer,
                ),
            )
            .await
            .unwrap();

        RoutedPubSub::spawn_control_plane(&pubsub);
        tokio::task::yield_now().await;
        let before_fallback = provider.calls.load(Ordering::Acquire);
        tokio::time::advance(CONTROL_PLANE_FALLBACK_INTERVAL).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            provider.calls.load(Ordering::Acquire) > before_fallback,
            "the fallback tick must re-evaluate an installed provider whose interior state has no revision"
        );
    }

    #[tokio::test]
    async fn control_plane_provider_revision_recheck_survives_consumed_notification() {
        let pubsub = test_pubsub("pubsub-provider-revision-wait");
        let initial_provider = Arc::new(CountingRouteProvider {
            calls: AtomicU64::new(0),
        });
        pubsub.set_route_provider(initial_provider.clone());
        let interested_peer =
            crate::KeyPair::new_for_testing("pubsub-provider-revision-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(topic_key("provider-revision"), &interested_peer),
                RemoteActorLocation::new_with_peer(
                    "127.0.0.1:19009".parse().unwrap(),
                    interested_peer,
                ),
            )
            .await
            .unwrap();

        // Pause the real control-plane task after its revision pre-check and
        // before it constructs the select. This is the race in which another
        // branch can consume the provider Notify permit.
        pubsub
            .control_plane_wait_hook_enabled
            .store(true, Ordering::Release);
        RoutedPubSub::spawn_control_plane(&pubsub);
        pubsub.control_plane_wait_hook_entered.notified().await;
        assert_eq!(initial_provider.calls.load(Ordering::Acquire), 1);
        let refreshes_before_change = pubsub.control_plane_refreshes.load(Ordering::Acquire);

        let replacement_called = Arc::new(tokio::sync::Notify::new());
        let replacement_provider = Arc::new(NotifyingRouteProvider {
            calls: AtomicU64::new(0),
            called: Arc::clone(&replacement_called),
        });
        pubsub.set_route_provider(replacement_provider.clone());

        // Consume the only notification while the control-plane task is held
        // before select construction. A revision-only implementation would
        // now park until the one-second fallback; the wait future's revision
        // check must still wake this actual control-plane turn.
        pubsub.route_provider_change_notify.notified().await;
        pubsub.control_plane_wait_hook_release.notify_one();

        tokio::time::timeout(Duration::from_millis(100), replacement_called.notified())
            .await
            .expect("provider revision change must rebuild without the fallback timer");
        assert!(replacement_provider.calls.load(Ordering::Acquire) > 0);
        assert!(
            pubsub.control_plane_refreshes.load(Ordering::Acquire) > refreshes_before_change,
            "the provider revision change must drive an actual control-plane refresh"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_control_plane_refreshes_coalesce_without_waiter_backlog() {
        let pubsub = test_pubsub("pubsub-serialized-control-plane-refresh");
        pubsub
            .control_plane_snapshot_hook_enabled
            .store(true, Ordering::Release);
        let provider = Arc::new(CountingRouteProvider {
            calls: AtomicU64::new(0),
        });
        pubsub.set_route_provider(provider.clone());
        let interested_peer =
            crate::KeyPair::new_for_testing("pubsub-serialized-refresh-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(topic_key("serialized-refresh"), &interested_peer),
                RemoteActorLocation::new_with_peer(
                    "127.0.0.1:19005".parse().unwrap(),
                    interested_peer,
                ),
            )
            .await
            .unwrap();

        let first_pubsub = Arc::clone(&pubsub);
        let first = tokio::spawn(async move { first_pubsub.refresh_control_plane().await });
        pubsub.control_plane_snapshot_hook_entered.notified().await;

        let second_pubsub = Arc::clone(&pubsub);
        let second = tokio::spawn(async move { second_pubsub.refresh_control_plane().await });
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("coalesced refresh must not queue behind the provider")
            .unwrap();
        assert_eq!(
            provider.calls.load(Ordering::Acquire),
            1,
            "a second refresh must not evaluate or publish ahead of an in-flight snapshot"
        );

        pubsub.control_plane_snapshot_hook_release.notify_one();
        first.await.unwrap();
        assert_eq!(provider.calls.load(Ordering::Acquire), 1);
        // A concurrent request coalesces, but an independent later request
        // still re-evaluates a stateful provider.
        pubsub.refresh_control_plane().await;
        assert_eq!(provider.calls.load(Ordering::Acquire), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn refresh_retries_when_actor_revision_changes_during_snapshot() {
        let pubsub = test_pubsub("pubsub-refresh-retries-on-actor-change");
        pubsub
            .control_plane_snapshot_hook_enabled
            .store(true, Ordering::Release);
        let provider = Arc::new(CountingRouteProvider {
            calls: AtomicU64::new(0),
        });
        pubsub.set_route_provider(provider.clone());
        let first_peer = crate::KeyPair::new_for_testing("pubsub-refresh-first-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(topic_key("refresh-first"), &first_peer),
                RemoteActorLocation::new_with_peer("127.0.0.1:19006".parse().unwrap(), first_peer),
            )
            .await
            .unwrap();

        let refresh_pubsub = Arc::clone(&pubsub);
        let refresh = tokio::spawn(async move {
            refresh_pubsub.refresh_control_plane().await;
        });
        pubsub.control_plane_snapshot_hook_entered.notified().await;

        let second_peer = crate::KeyPair::new_for_testing("pubsub-refresh-second-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                // Keep both interests on one topic so the provider is called
                // once per snapshot; the count below then measures the
                // initial build plus exactly one retry.
                interest_name(topic_key("refresh-first"), &second_peer),
                RemoteActorLocation::new_with_peer("127.0.0.1:19007".parse().unwrap(), second_peer),
            )
            .await
            .unwrap();

        pubsub.control_plane_snapshot_hook_release.notify_one();
        refresh.await.unwrap();

        assert_eq!(
            provider.calls.load(Ordering::Acquire),
            2,
            "a routing mutation during snapshot assembly must force a retry"
        );
        assert_eq!(
            pubsub.last_actor_routing_revision.load(Ordering::Acquire),
            pubsub.registry.actor_state.pubsub_routing_revision(),
            "the published route baseline must be the revision actually incorporated"
        );
    }

    #[tokio::test]
    async fn actor_directory_change_invalidates_idle_control_plane_snapshot() {
        let pubsub = test_pubsub("pubsub-control-plane-actor-change");
        pubsub.refresh_control_plane().await;
        let before = pubsub.route_groups.load_full();

        let interested_peer =
            crate::KeyPair::new_for_testing("pubsub-control-plane-interested-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(topic_key("control-plane-change"), &interested_peer),
                RemoteActorLocation::new_with_peer(
                    "127.0.0.1:19001".parse().unwrap(),
                    pubsub.local_peer_id.clone(),
                ),
            )
            .await
            .unwrap();

        pubsub.refresh_control_plane().await;
        let after_change = pubsub.route_groups.load_full();
        assert!(!Arc::ptr_eq(&before, &after_change));

        pubsub.refresh_control_plane().await;
        let after_idle_tick = pubsub.route_groups.load_full();
        assert!(Arc::ptr_eq(&after_change, &after_idle_tick));
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

    /// Payloads above the byte-pool retention bound (1 MiB) must still encode
    /// and send: the pool bound is a pooling decision, not a message bound.
    /// Returning `None` here is what dropped a multi-MiB dashboard keyframe
    /// between a healthy publisher and its subscriber with no error anywhere.
    #[tokio::test]
    async fn oversize_pubsub_payload_encodes_above_pool_retention_bound() {
        let pubsub = test_pubsub("pubsub-oversize-encode");
        let topic = topic_key("oversize-encode");
        let destination = crate::KeyPair::new_for_testing("pubsub-oversize-dest").peer_id();
        let payload = vec![0xABu8; 2 * 1024 * 1024];
        let (frame, _prefix, payload_len) = encode_fast_frame_pooled(
            topic,
            7,
            7,
            &pubsub.local_peer_id,
            &pubsub.local_peer_id,
            2,
            PubSubDeliveryMode::AtMostOnce,
            PubSubFrameMetadata::default(),
            std::slice::from_ref(&destination),
            &payload,
        )
        .expect("oversize pubsub frame must still encode on an unpooled buffer");
        assert_eq!(
            payload_len,
            fast_frame_len(std::slice::from_ref(&destination), &payload)
        );
        assert_eq!(frame.remaining(), payload_len);
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
        let first = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    delivered_first.lock().unwrap().push("first");
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");
        let delivered_second = Arc::clone(&delivered);
        let second = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    delivered_second.lock().unwrap().push("second");
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");

        assert!(pubsub.unsubscribe(first));
        pubsub
            .publish_bytes(
                topic,
                type_hash,
                crate::ForwardPayload::from_static(b"payload"),
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
        let _subscription = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    delivered_for_subscriber.fetch_add(1, Ordering::Relaxed);
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");
        let policy = PubSubDeliveryPolicy {
            mode: PubSubDeliveryMode::AtLeastOnceHopAck,
            ..PubSubDeliveryPolicy::default()
        };

        let error = pubsub
            .publish_bytes(
                topic,
                type_hash,
                crate::ForwardPayload::from_static(b"must-not-deliver"),
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
        let subscription = pubsub
            .subscribe_bytes(
                77,
                205,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    let _keepalive = &captured_by_callback;
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");
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
        let _subscription = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    started_for_subscriber.store(true, Ordering::Release);
                    let release = Arc::clone(&release_for_subscriber);
                    async move {
                        while !release.load(Ordering::Acquire) {
                            tokio::task::yield_now().await;
                        }
                    }
                }),
            )
            .expect("subscription admission");

        pubsub
            .publish_bytes(
                topic,
                type_hash,
                crate::ForwardPayload::from_static(b"first"),
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
                    crate::ForwardPayload::from_static(b"queued"),
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
        let _subscription = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    delivered_for_subscriber.store(true, Ordering::Release);
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");

        pubsub
            .publish_bytes(
                topic,
                type_hash,
                crate::ForwardPayload::from_static(b"fallback"),
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
        let subscription = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    let _keepalive = &captured_by_callback;
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");
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

        let borrowed = pubsub
            .subscribe_borrowed_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(|_: &[u8]| std::future::ready(())),
            )
            .expect("subscription admission");
        let borrowed_meta = pubsub
            .subscribe_borrowed_bytes_with_metadata(
                topic,
                type_hash,
                CooperativeCallback::new(|_: &[u8], _: crate::pubsub::PubSubFrameMetadata| {
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");
        let typed = pubsub
            .subscribe_type_bytes(
                type_hash,
                CooperativeCallback::new(|_: u64, _: bytes::Bytes| std::future::ready(())),
            )
            .expect("subscription admission");
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
        let subscription = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .expect("subscription admission");
        assert!(pubsub.unsubscribe(subscription));
        // `unsubscribe` consumed the handle; its Drop ran after release() had
        // already fired and must not have removed anything twice. A fresh
        // subscription proves the interest counter did not underflow.
        let second = pubsub
            .subscribe_bytes(
                topic,
                type_hash,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .expect("subscription admission");
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
        let subscription = pubsub
            .subscribe_bytes(
                topic_key("raii-weak"),
                304,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .expect("subscription admission");
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
            pubsub_for_a
                .subscribe_bytes(
                    topic_a,
                    type_hash,
                    CooperativeCallback::new(move |_: bytes::Bytes| {
                        delivered_a_cb.store(true, Ordering::Release);
                        std::future::ready(())
                    }),
                )
                .expect("subscription admission")
        });
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("thread A must enter the subscriber-map RMW window");

        // Thread B: full subscribe while A is parked inside its window.
        let delivered_b_cb = Arc::clone(&delivered_b);
        let _sub_b = pubsub
            .subscribe_bytes(
                topic_b,
                type_hash,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    delivered_b_cb.store(true, Ordering::Release);
                    std::future::ready(())
                }),
            )
            .expect("subscription admission");
        let _sub_a = thread_a.join().expect("thread A subscribe must not panic");

        for (topic, payload) in [(topic_a, &b"to-a"[..]), (topic_b, &b"to-b"[..])] {
            pubsub
                .publish_bytes(
                    topic,
                    type_hash,
                    crate::ForwardPayload::from_static(payload),
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
                        let sub = pubsub
                            .subscribe_bytes(
                                topic,
                                type_hash,
                                CooperativeCallback::new(|_: bytes::Bytes| {
                                    std::future::ready(())
                                }),
                            )
                            .expect("subscription admission");
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
                        if worker == 0 {
                            // This is a map-RMW stress test, not worker-admission
                            // saturation: finish each generation's shutdown
                            // before creating the next pair of workers.
                            let deadline = std::time::Instant::now() + Duration::from_secs(5);
                            while pubsub.subscriber_permits.available_permits()
                                != pubsub.limits.subscribers.get()
                            {
                                if std::time::Instant::now() >= deadline {
                                    *failure.lock().unwrap() = Some(format!(
                                        "iteration {iteration}: idle workers did not release admission"
                                    ));
                                    break;
                                }
                                std::thread::yield_now();
                            }
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

    /// Polls `cond` until it returns `true` or `timeout` elapses. Returns the
    /// last observed value.
    async fn poll_until<F, Fut>(mut cond: F, timeout: Duration) -> bool
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if cond().await {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    // --- R6(a): TOCTOU in interest advertisement ---------------------------
    //
    // `note_interest` snapshots `(present, generation)` once before the
    // `.await` on the registry register/unregister call and never
    // re-validates afterward. Two independent background tasks — one
    // finishing an unregister for a topic's last-subscriber-leaves
    // transition, one finishing a register for the very next
    // new-subscriber-arrives transition — can both pass their pre-await
    // gate and then race unsynchronized on the registry itself. Whichever
    // registry call *completes* last wins, regardless of which transition
    // was chronologically (or logically) correct.
    //
    // This test forces exactly that interleaving deterministically using
    // `test_helpers::install_pubsub_interest_dispatch_hook`: the hook
    // pauses the unregister dispatch immediately before its registry call,
    // lets a subsequent resubscribe's register dispatch run to completion
    // first, and only then releases the unregister so it completes last.
    // At baseline this reliably reproduces the bug every run (not just
    // "intermittently") because the interleaving is forced rather than
    // left to scheduler luck.
    #[tokio::test]
    async fn final_advertised_state_matches_final_subscriber_state_under_churn() {
        let pubsub = test_pubsub("pubsub-interest-convergence");
        let topic = topic_key("interest-convergence-churn");
        let name = interest_name(topic, &pubsub.local_peer_id);

        // Exclusive access for the whole test. The hook installed below is
        // filtered to this test's own `topic`, so it no longer pauses an
        // unrelated test's `note_interest` dispatch — but
        // `install_pubsub_interest_dispatch_hook` replaces the process-global
        // hook outright, and `PubsubInterestDispatchHookGuard::exclusive()`
        // (used by `generations_map_returns_to_baseline_after_topic_churn`
        // below) unconditionally *clears* it. Without this mutual exclusion,
        // that guard's clear could tear this hook out from under this test
        // mid-flight — after `reached_unregister.notified()` below has
        // returned but before `release_unregister.notify_one()` runs — which
        // would strand this test's own unregister dispatch parked forever,
        // regardless of topic filtering.
        let _hook_guard = crate::test_helpers::PubsubInterestDispatchHookGuard::exclusive();

        // Establish one live local subscriber so the topic starts "present"
        // and let the initial registration land (present=true dispatches
        // are never delayed by the hook installed below). `note_interest`
        // is the subscribe/unsubscribe side effect under test here and is
        // independent of the `subscribers` delivery map, so driving it
        // directly is a faithful, minimal reproduction.
        pubsub.note_interest(topic, true).unwrap();
        assert!(
            poll_until(
                || async { pubsub.registry.lookup_actor(&name).await.is_some() },
                Duration::from_millis(500),
            )
            .await,
            "initial registration must land before the churn phase starts"
        );

        // Gate the *unregister* dispatch right before its registry call.
        //
        // The closure below only acts on `dispatch_topic_key == topic`: the
        // hook fires for EVERY `note_interest` register/unregister dispatch
        // in the process while installed, regardless of which test's
        // `PubsubInterestDispatchHookGuard` (if any) is currently held —
        // the guard only serializes installation, it does not scope
        // invocation. Without this filter, any other test's unregister
        // dispatch for a *different* topic could park on `release_unregister`
        // and either consume the sole `notify_one` intended for this test's
        // own unregister or, worse, leave this test's `reached_unregister`
        // never signaled at all. Filtering by topic removes that
        // cross-test interference at the source, independent of the guard.
        let reached_unregister = Arc::new(tokio::sync::Notify::new());
        let release_unregister = Arc::new(tokio::sync::Notify::new());
        {
            let reached = Arc::clone(&reached_unregister);
            let release = Arc::clone(&release_unregister);
            crate::test_helpers::install_pubsub_interest_dispatch_hook(Arc::new(
                move |dispatch_topic_key, present| {
                    let reached = Arc::clone(&reached);
                    let release = Arc::clone(&release);
                    Box::pin(async move {
                        if dispatch_topic_key == topic && !present {
                            reached.notify_one();
                            release.notified().await;
                        }
                    }) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
                },
            ));
        }

        // Last subscriber leaves: spawns/coalesces the unregister dispatch
        // (generation g+1). It will park just before calling the registry.
        pubsub.note_interest(topic, false).unwrap();
        reached_unregister.notified().await;

        // A new subscriber arrives immediately after: spawns/coalesces the
        // register dispatch (generation g+2). `present=true` is never
        // delayed by the hook, so this runs to completion right away,
        // *before* the stale unregister is released.
        pubsub.note_interest(topic, true).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now let the stale unregister complete last.
        release_unregister.notify_one();
        tokio::time::sleep(Duration::from_millis(150)).await;
        crate::test_helpers::clear_pubsub_interest_dispatch_hook();

        assert!(
            pubsub.registry.lookup_actor(&name).await.is_some(),
            "topic has a live local subscriber; advertised interest must be \
             registered even though a stale unregister completed after the \
             resubscribe's register"
        );
    }

    // --- R6(b): unbounded `InterestState.generations` leak ------------------
    //
    // `generations.insert(topic_key, ...)` is the only mutation to that map;
    // nothing ever removes an entry (contrast `local_counts`, which *is*
    // correctly removed once a topic's subscriber count returns to 0). Under
    // ephemeral/per-session topic churn the map grows for the life of the
    // process.
    #[tokio::test]
    async fn generations_map_returns_to_baseline_after_topic_churn() {
        let pubsub = test_pubsub("pubsub-interest-generations-leak");
        // Exclusive access for the whole test. `final_advertised_state_
        // matches_final_subscriber_state_under_churn`'s hook is filtered to
        // its own topic, so this loop's differently-keyed churn topics can
        // never be routed through it even if both tests run concurrently.
        // This guard's remaining job is `exclusive()`'s own clear: without
        // it, this call could tear that other test's hook out from under it
        // mid-flight (after it has parked a dispatch on `release_unregister`
        // but before that test signals it) instead of blocking until the
        // other test's own guard releases. `exclusive()` acquiring the same
        // process-wide install lock the other test's guard holds for its
        // entire run is what makes these two tests fully serialize against
        // each other here, not topic scoping.
        let _hook_guard = crate::test_helpers::PubsubInterestDispatchHookGuard::exclusive();

        let baseline_len = {
            let state = pubsub.interest_state.lock().unwrap();
            state.generations.len()
        };

        const CHURN_TOPICS: u64 = 64;
        for i in 0..CHURN_TOPICS {
            let topic = topic_key(&format!("ephemeral-generations-churn-{i}"));
            pubsub.note_interest(topic, true).unwrap();
            pubsub.note_interest(topic, false).unwrap();
        }

        // Let any spawned register/unregister dispatch tasks settle.
        assert!(
            poll_until(
                || async {
                    let state = pubsub.interest_state.lock().unwrap();
                    state.local_counts.is_empty()
                },
                Duration::from_millis(500),
            )
            .await,
            "local_counts must return to empty once every churned topic has 0 subscribers"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let final_len = {
            let state = pubsub.interest_state.lock().unwrap();
            state.generations.len()
        };
        assert_eq!(
            final_len, baseline_len,
            "generations map must return to its baseline size once every churned \
             topic has 0 local subscribers again, not grow by {CHURN_TOPICS} entries"
        );
    }

    #[tokio::test]
    async fn subscriber_admission_is_global_and_preserves_existing_delivery() {
        let pubsub = test_pubsub("subscriber-admission-global");
        let held = Arc::clone(&pubsub.subscriber_permits)
            .try_acquire_many_owned((pubsub.limits.subscribers.get() - 1) as u32)
            .unwrap();
        let (delivered, received) = tokio::sync::oneshot::channel();
        let delivered = Mutex::new(Some(delivered));
        let subscription = pubsub
            .subscribe_bytes(
                1,
                2,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    if let Some(delivered) = delivered.lock().unwrap().take() {
                        let _ = delivered.send(());
                    }
                    std::future::ready(())
                }),
            )
            .unwrap();
        assert_eq!(
            pubsub
                .subscribe_type_bytes(
                    3,
                    CooperativeCallback::new(|_: u64, _: bytes::Bytes| { std::future::ready(()) }),
                )
                .unwrap_err(),
            PubSubAdmissionError::SubscriberCapacity
        );
        assert_eq!(pubsub.deliver_local(1, 2, Bytes::from_static(b"live")), 1);
        received.await.unwrap();
        assert_eq!(pubsub.subscribers.load().len(), 1);
        assert!(pubsub.type_subscribers.load().is_empty());
        drop(subscription);
        drop(held);
    }

    #[tokio::test]
    async fn subscriber_topic_budget_combines_owned_and_borrowed_entries() {
        let pubsub = test_pubsub("subscriber-admission-topic");
        let mut subscriptions = Vec::new();
        for hash in 0..pubsub.limits.subscribers_per_topic.get() {
            subscriptions.push(
                pubsub
                    .subscribe_bytes(
                        9,
                        hash as u64,
                        CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
                    )
                    .unwrap(),
            );
        }
        assert_eq!(
            pubsub
                .subscribe_borrowed_bytes(
                    9,
                    500,
                    CooperativeCallback::new(|_: &[u8]| { std::future::ready(()) }),
                )
                .unwrap_err(),
            PubSubAdmissionError::TopicSubscriberCapacity
        );
        let other = pubsub
            .subscribe_bytes(
                10,
                500,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .unwrap();
        assert_eq!(
            pubsub.deliver_local(10, 500, Bytes::from_static(b"other")),
            1
        );
        drop(other);
        drop(subscriptions);
    }

    #[tokio::test]
    async fn unsubscribe_cancels_pending_callback_before_reusing_its_permit() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        struct CaptureDropProbe {
            permits: Arc<tokio::sync::Semaphore>,
            observed: Arc<AtomicU64>,
        }
        impl Drop for CaptureDropProbe {
            fn drop(&mut self) {
                self.observed
                    .store(self.permits.available_permits() as u64, Ordering::Release);
            }
        }
        let pubsub = test_pubsub("subscriber-admission-cancel");
        let held = Arc::clone(&pubsub.subscriber_permits)
            .try_acquire_many_owned((pubsub.limits.subscribers.get() - 1) as u32)
            .unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let started_callback = Arc::clone(&started);
        let dropped_callback = Arc::clone(&dropped);
        let observed_at_capture_drop = Arc::new(AtomicU64::new(u64::MAX));
        let capture = CaptureDropProbe {
            permits: Arc::clone(&pubsub.subscriber_permits),
            observed: Arc::clone(&observed_at_capture_drop),
        };
        let subscription = pubsub
            .subscribe_bytes(
                1,
                2,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    let _capture = &capture;
                    let started = Arc::clone(&started_callback);
                    let probe = DropProbe(Arc::clone(&dropped_callback));
                    async move {
                        let _probe = probe;
                        started.notify_one();
                        std::future::pending::<()>().await;
                    }
                }),
            )
            .unwrap();
        assert_eq!(pubsub.deliver_local(1, 2, Bytes::from_static(b"held")), 1);
        started.notified().await;
        assert_eq!(
            pubsub
                .subscribe_bytes(
                    3,
                    4,
                    CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
                )
                .unwrap_err(),
            PubSubAdmissionError::SubscriberCapacity
        );
        drop(subscription);
        tokio::time::timeout(Duration::from_secs(1), async {
            while pubsub.subscriber_permits.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cooperative cancellation releases the worker permit");
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(
            observed_at_capture_drop.load(Ordering::Acquire),
            0,
            "the callback capture must drop before the worker releases admission"
        );
        let successor = pubsub
            .subscribe_bytes(
                3,
                4,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .unwrap();
        drop(successor);
        drop(held);
    }

    #[tokio::test]
    async fn subscriber_queue_rejects_oversized_visible_payload() {
        let pubsub = test_pubsub("subscriber-payload-budget");
        let subscription = pubsub
            .subscribe_bytes(
                1,
                2,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .unwrap();
        let oversized = Bytes::from(vec![0; SUBSCRIBER_MAX_PAYLOAD_BYTES + 1]);
        assert_eq!(pubsub.deliver_local(1, 2, oversized), 0);
        assert_eq!(pubsub.stats().subscriber_queue_drops, 1);
        assert_eq!(pubsub.deliver_local(1, 2, Bytes::from_static(b"valid")), 1);
        drop(subscription);
    }

    #[tokio::test(start_paused = true)]
    async fn revision_churn_has_a_finite_refresh_budget_and_recovers() {
        struct ChurningProvider {
            revision: Arc<AtomicU64>,
            calls: AtomicU64,
            churn: AtomicBool,
        }
        impl PubSubRouteProvider for ChurningProvider {
            fn group_destinations(
                &self,
                _topic_key: u64,
                destinations: &[PeerId],
                routes: &mut PubSubRouteSink,
            ) -> std::result::Result<(), PubSubRouteError> {
                let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
                // Stop changing after the budget, so the pre-fix loop returns
                // a fifth attempt and fails the count oracle rather than hangs.
                if self.churn.load(Ordering::Relaxed)
                    && call <= CONTROL_PLANE_REFRESH_ATTEMPTS as u64
                {
                    self.revision.fetch_add(1, Ordering::AcqRel);
                }
                for (index, peer) in destinations.iter().enumerate() {
                    routes.route(index, peer.clone())?;
                }
                Ok(())
            }
        }
        let pubsub = test_pubsub("refresh-budget");
        let interested = crate::KeyPair::new_for_testing("refresh-budget-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(33, &interested),
                RemoteActorLocation::new_with_peer("127.0.0.1:19222".parse().unwrap(), interested),
            )
            .await
            .unwrap();
        let provider = Arc::new(ChurningProvider {
            revision: Arc::clone(&pubsub.route_provider_revision),
            calls: AtomicU64::new(0),
            churn: AtomicBool::new(true),
        });
        pubsub.set_route_provider(provider.clone());
        pubsub.refresh_control_plane().await;
        assert_eq!(provider.calls.load(Ordering::Relaxed), 4);
        assert_eq!(pubsub.stats().refresh_budget_rejections, 1);
        assert_eq!(
            pubsub.last_route_provider_revision.load(Ordering::Acquire),
            u64::MAX,
            "an incomplete snapshot must not acknowledge a newer revision"
        );
        provider.churn.store(false, Ordering::Relaxed);
        pubsub.refresh_control_plane().await;
        assert_eq!(provider.calls.load(Ordering::Relaxed), 5);
        assert_eq!(
            pubsub.last_route_provider_revision.load(Ordering::Acquire),
            pubsub.route_provider_revision.load(Ordering::Acquire)
        );
    }

    #[tokio::test]
    async fn outstanding_interest_topics_share_subscription_admission_budget() {
        let pubsub = test_pubsub("interest-admission-budget");
        {
            let mut state = pubsub.interest_state.lock().unwrap();
            for topic in 0..pubsub.limits.topics.get() {
                state.generations.insert(topic as u64, 1);
                state.dispatch_in_flight.insert(topic as u64);
            }
        }
        assert_eq!(
            pubsub
                .subscribe_bytes(
                    u64::MAX,
                    1,
                    CooperativeCallback::new(|_: bytes::Bytes| { std::future::ready(()) }),
                )
                .unwrap_err(),
            PubSubAdmissionError::TopicCapacity
        );
        assert!(pubsub.subscribers.load().is_empty());
        assert_eq!(
            pubsub.subscriber_permits.available_permits(),
            pubsub.limits.subscribers.get()
        );
        // An existing outstanding topic does not consume a second topic slot.
        let admitted = pubsub
            .subscribe_bytes(
                0,
                1,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .unwrap();
        assert_eq!(pubsub.subscribers.load().len(), 1);
        drop(admitted);
    }

    #[tokio::test]
    async fn publication_rejects_tiny_payload_with_oversized_backing_owner() {
        let pubsub = test_pubsub("pubsub-retained-owner");
        let subscription = pubsub
            .subscribe_bytes(
                1,
                2,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .unwrap();
        let mut backing = Vec::with_capacity(SUBSCRIBER_MAX_PAYLOAD_BYTES + 1);
        backing.push(7);
        let payload = crate::ForwardPayload::from(backing);
        assert_eq!(payload.len(), 1);
        assert!(payload.retained_bytes() > SUBSCRIBER_MAX_PAYLOAD_BYTES);
        assert!(matches!(
            pubsub.publish_bytes(
                1,
                2,
                payload,
                PubSubScope::LocalOnly,
                PubSubDeliveryPolicy::default(),
            ),
            Err(GossipError::WriteQueueFull)
        ));
        let stats = pubsub
            .publish_bytes(
                1,
                2,
                crate::ForwardPayload::from_static(b"admitted"),
                PubSubScope::LocalOnly,
                PubSubDeliveryPolicy::default(),
            )
            .unwrap();
        assert_eq!(stats.local_delivered, 1);
        drop(subscription);
    }

    #[test]
    fn bounded_route_sink_is_complete_non_amplifying_and_sticky() {
        let first = crate::KeyPair::new_for_testing("route-sink-first").peer_id();
        let second = crate::KeyPair::new_for_testing("route-sink-second").peer_id();
        let destinations = [first.clone(), second.clone()];
        let mut routes = PubSubRouteSink::new(2);
        routes.deadline = std::time::Instant::now() + Duration::from_secs(60);
        routes.route(0, first.clone()).unwrap();
        routes.route(1, first.clone()).unwrap();
        let grouped = routes.finish(&destinations).unwrap();
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[&first].as_ref(), &destinations);

        let mut duplicate = PubSubRouteSink::new(2);
        duplicate.deadline = std::time::Instant::now() + Duration::from_secs(60);
        duplicate.route(0, first.clone()).unwrap();
        assert_eq!(
            duplicate.route(0, second.clone()),
            Err(PubSubRouteError::DuplicateDestination)
        );
        // Ignoring an insertion failure cannot turn a partial map into success.
        assert_eq!(
            duplicate.route(1, second.clone()),
            Err(PubSubRouteError::DuplicateDestination)
        );
        assert_eq!(
            duplicate.finish(&destinations).unwrap_err(),
            PubSubRouteError::DuplicateDestination
        );

        let mut missing = PubSubRouteSink::new(2);
        missing.deadline = std::time::Instant::now() + Duration::from_secs(60);
        missing.route(0, first.clone()).unwrap();
        assert_eq!(
            missing.finish(&destinations).unwrap_err(),
            PubSubRouteError::MissingDestination
        );

        let mut exhausted = PubSubRouteSink::new(1);
        exhausted.deadline = std::time::Instant::now() + Duration::from_secs(60);
        for _ in 0..8 {
            exhausted.step().unwrap();
        }
        assert_eq!(
            exhausted.route(0, first.clone()),
            Err(PubSubRouteError::BudgetExhausted)
        );
        assert_eq!(exhausted.next_hops.len(), 1);
        assert_eq!(
            exhausted.finish(&destinations[..1]).unwrap_err(),
            PubSubRouteError::BudgetExhausted
        );

        let mut invalid = PubSubRouteSink::new(1);
        invalid.deadline = std::time::Instant::now() + Duration::from_secs(60);
        assert_eq!(
            invalid.route(1, first),
            Err(PubSubRouteError::InvalidDestination)
        );
    }

    #[test]
    fn oversized_route_request_never_invokes_provider() {
        let pubsub = test_pubsub("route-input-admission");
        let provider = Arc::new(CountingRouteProvider {
            calls: AtomicU64::new(0),
        });
        pubsub.set_route_provider(provider.clone());
        let peer = crate::KeyPair::new_for_testing("route-input-peer").peer_id();
        let oversized = vec![peer.clone(); pubsub.limits.routing_records.get() + 1];
        assert_eq!(
            pubsub
                .group_destinations_bounded(1, &oversized)
                .unwrap_err(),
            PubSubRouteError::BudgetExhausted
        );
        assert_eq!(provider.calls.load(Ordering::Acquire), 0);
        let grouped = pubsub
            .group_destinations_bounded(1, std::slice::from_ref(&peer))
            .unwrap();
        assert_eq!(provider.calls.load(Ordering::Acquire), 1);
        assert_eq!(grouped[&peer].as_ref(), std::slice::from_ref(&peer));
    }

    #[tokio::test(start_paused = true)]
    async fn provider_partial_failure_preserves_complete_published_snapshot() {
        struct RejectingProvider;
        impl PubSubRouteProvider for RejectingProvider {
            fn group_destinations(
                &self,
                _topic: u64,
                peers: &[PeerId],
                routes: &mut PubSubRouteSink,
            ) -> std::result::Result<(), PubSubRouteError> {
                routes.route(0, peers[0].clone())?;
                let _ = routes.route(0, peers[0].clone());
                Ok(())
            }
        }
        let pubsub = test_pubsub("route-partial-rejection");
        let peer = crate::KeyPair::new_for_testing("route-partial-peer").peer_id();
        pubsub
            .registry
            .register_actor(
                interest_name(1, &peer),
                RemoteActorLocation::new_with_peer("127.0.0.1:19301".parse().unwrap(), peer),
            )
            .await
            .unwrap();
        pubsub.refresh_control_plane().await;
        let before = pubsub.route_groups.load_full();
        let acknowledged = pubsub.last_route_provider_revision.load(Ordering::Acquire);
        pubsub.set_route_provider(Arc::new(RejectingProvider));
        pubsub.refresh_control_plane().await;
        assert!(Arc::ptr_eq(&before, &pubsub.route_groups.load_full()));
        assert_eq!(pubsub.stats().refresh_budget_rejections, 1);
        assert_eq!(
            pubsub.last_route_provider_revision.load(Ordering::Acquire),
            acknowledged
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cooperative_callback_deadline_drops_active_future_and_continues_queue() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let pubsub = test_pubsub("callback-deadline");
        let entered = Arc::new(tokio::sync::Notify::new());
        let delivered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let calls = AtomicU64::new(0);
        let entered_callback = Arc::clone(&entered);
        let delivered_callback = Arc::clone(&delivered);
        let dropped_callback = Arc::clone(&dropped);
        let subscription = pubsub
            .subscribe_bytes(
                1,
                2,
                CooperativeCallback::new(move |_: bytes::Bytes| {
                    let first = calls.fetch_add(1, Ordering::AcqRel) == 0;
                    let entered = Arc::clone(&entered_callback);
                    let delivered = Arc::clone(&delivered_callback);
                    let dropped = Arc::clone(&dropped_callback);
                    async move {
                        if first {
                            let _probe = DropProbe(dropped);
                            entered.notify_one();
                            std::future::pending::<()>().await;
                        } else {
                            delivered.notify_one();
                        }
                    }
                }),
            )
            .unwrap();
        assert_eq!(pubsub.deliver_local(1, 2, Bytes::from_static(b"active")), 1);
        entered.notified().await;
        assert_eq!(
            pubsub.deliver_local(1, 2, Bytes::from_static(b"successor")),
            1
        );
        tokio::time::advance(CALLBACK_MAX_DURATION).await;
        delivered.notified().await;
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(pubsub.stats().callback_expirations, 1);
        drop(subscription);
    }

    #[tokio::test]
    async fn blocked_gossip_topic_churn_is_bounded_and_recovers_after_cleanup() {
        let pubsub = test_pubsub("interest-blocked-churn-budget");
        let lock = pubsub.registry.gossip_state.lock().await;
        let limit = pubsub.limits.topics.get();
        let subscribe = |topic| {
            pubsub.subscribe_bytes(
                topic,
                1,
                CooperativeCallback::new(|_: Bytes| std::future::ready(())),
            )
        };
        for topic in 0..limit as u64 {
            drop(subscribe(topic).unwrap());
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while pubsub.subscriber_permits.available_permits() != pubsub.limits.subscribers.get() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscriber workers must release independently of blocked gossip");
        {
            let state = pubsub.interest_state.lock().unwrap();
            assert!(state.local_counts.is_empty());
            assert_eq!(state.generations.len(), limit);
            assert_eq!(state.dispatch_in_flight.len(), limit);
            assert_eq!(state.dispatch_tasks.len(), limit);
        }
        assert!(matches!(
            subscribe(limit as u64),
            Err(PubSubAdmissionError::TopicCapacity)
        ));
        // Reusing an existing identity coalesces without another dispatch slot.
        drop(subscribe(0).unwrap());
        assert_eq!(
            pubsub.interest_state.lock().unwrap().generations.len(),
            limit
        );
        drop(lock);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if pubsub.interest_state.lock().unwrap().generations.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup must return every dispatch identity");
        let successor = subscribe(limit as u64).unwrap();
        let name = interest_name(limit as u64, &pubsub.local_peer_id);
        tokio::time::timeout(Duration::from_secs(2), async {
            while pubsub.registry.lookup_actor(&name).await.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a successor must actually publish after capacity returns");
        drop(successor);
        assert!(pubsub.registry.lookup_actor(&name).await.is_none());
    }

    #[tokio::test]
    async fn engine_drop_cancels_interest_wait_before_registry_mutation() {
        let pubsub = test_pubsub("interest-cancellation-wait");
        let registry = Arc::clone(&pubsub.registry);
        let topic = 2718;
        let name = interest_name(topic, &pubsub.local_peer_id);
        let lock = registry.gossip_state.lock().await;
        let subscription = pubsub
            .subscribe_bytes(
                topic,
                1,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .unwrap();
        tokio::task::yield_now().await;
        let receipt = Arc::clone(&pubsub.interest_state.lock().unwrap().dispatch_receipts[&topic]);
        drop(pubsub);
        receipt.cancelled().await;
        drop(lock);
        tokio::task::yield_now().await;
        assert!(registry.lookup_actor(&name).await.is_none());
        assert!(registry.actor_state.local_actors.is_empty());
        drop(subscription);
    }

    #[tokio::test]
    async fn canceling_registered_interest_removes_actor_before_gossip_lock_is_available() {
        let pubsub = test_pubsub("interest-cancellation-published");
        let topic = 3141;
        let name = interest_name(topic, &pubsub.local_peer_id);
        let subscription = pubsub
            .subscribe_bytes(
                topic,
                1,
                CooperativeCallback::new(|_: bytes::Bytes| std::future::ready(())),
            )
            .unwrap();
        assert!(
            poll_until(
                || async { pubsub.registry.lookup_actor(&name).await.is_some() },
                Duration::from_secs(1),
            )
            .await
        );
        let lock = pubsub.registry.gossip_state.lock().await;
        drop(subscription);
        assert!(
            pubsub
                .registry
                .actor_state
                .local_actors
                .read_sync(name.as_str(), |_, _| ())
                .is_none()
        );
        drop(lock);
        pubsub.registry.flush_pubsub_interest_cleanup().await;
        assert!(
            pubsub
                .registry
                .actor_state
                .removed_actors
                .contains_sync(name.as_str())
        );
    }
}
