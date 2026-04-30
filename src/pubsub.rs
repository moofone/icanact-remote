use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tracing::warn;

use crate::{GossipError, PeerId, RegistrationPriority, RemoteActorLocation, Result};

const CONTROL_PLANE_INTERVAL: Duration = Duration::from_millis(25);
const DEFAULT_TTL: u8 = 8;
const DEFAULT_SEEN_CAPACITY: usize = 16_384;
const INTEREST_PREFIX: &str = "icanact/pubsub/interest/v1";

type TopicKey = u64;
type TypeHash = u64;
type SubscriberKey = (TopicKey, TypeHash);
type Subscriber = Arc<dyn Fn(Bytes) + Send + Sync + 'static>;
type TypeSubscriber = Arc<dyn Fn(u64, Bytes) + Send + Sync + 'static>;
type SubscriberMap = HashMap<SubscriberKey, Arc<[Subscriber]>>;
type TypeSubscriberMap = HashMap<TypeHash, Arc<[TypeSubscriber]>>;
type RouteGroups = HashMap<PeerId, Arc<[PeerId]>>;
type TopicRouteGroups = HashMap<TopicKey, Arc<RouteGroups>>;

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
    fn handle_pubsub_frame(&self, payload: crate::AlignedBytes) -> Result<()>;
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

pub struct RoutedPubSub {
    registry: Arc<crate::registry::GossipRegistry>,
    client: crate::GossipClient,
    local_peer_id: PeerId,
    subscribers: ArcSwap<SubscriberMap>,
    type_subscribers: ArcSwap<TypeSubscriberMap>,
    local_counts: Mutex<HashMap<TopicKey, usize>>,
    route_groups: ArcSwap<TopicRouteGroups>,
    conns: ArcSwap<HashMap<PeerId, crate::RemoteConnection>>,
    seen: Mutex<Vec<(PeerId, u128)>>,
    counters: PubSubIngressCounters,
    next_sub_id: AtomicU64,
    next_msg_id: AtomicU64,
    route_provider: ArcSwap<Option<Arc<dyn PubSubRouteProvider>>>,
}

impl RoutedPubSub {
    pub async fn install(registry: Arc<crate::registry::GossipRegistry>) -> Arc<Self> {
        let this = Arc::new(Self {
            local_peer_id: registry.peer_id.clone(),
            client: crate::GossipClient::from_registry(Arc::clone(&registry)),
            registry,
            subscribers: ArcSwap::from_pointee(HashMap::new()),
            type_subscribers: ArcSwap::from_pointee(HashMap::new()),
            local_counts: Mutex::new(HashMap::new()),
            route_groups: ArcSwap::from_pointee(HashMap::new()),
            conns: ArcSwap::from_pointee(HashMap::new()),
            seen: Mutex::new(Vec::with_capacity(DEFAULT_SEEN_CAPACITY)),
            counters: PubSubIngressCounters::default(),
            next_sub_id: AtomicU64::new(1),
            next_msg_id: AtomicU64::new(1),
            route_provider: ArcSwap::from_pointee(None),
        });
        this.registry
            .set_pubsub_ingress_handler(Arc::clone(&this) as Arc<dyn PubSubIngressHandler>)
            .await;
        Self::spawn_control_plane(&this);
        this
    }

    pub fn set_route_provider(&self, provider: Arc<dyn PubSubRouteProvider>) {
        self.route_provider.store(Arc::new(Some(provider)));
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
        topic_subs.push(Arc::new(deliver));
        next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        self.subscribers.store(Arc::new(next));
        self.note_interest(topic_key, true);
        PubSubSubscription {
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
        subs.push(Arc::new(deliver));
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
        topic_subs.pop();
        if topic_subs.is_empty() {
            next.remove(&key);
        } else {
            next.insert(key, Arc::from(topic_subs.into_boxed_slice()));
        }
        self.subscribers.store(Arc::new(next));
        self.note_interest(sub.topic_key, false);
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

    fn publish_remote_bytes_inner(
        &self,
        topic_key: u64,
        type_hash: u64,
        payload: Bytes,
        scope: PubSubScope,
        policy: PubSubDeliveryPolicy,
        stats: &mut PubSubPublishStats,
    ) -> Result<()> {
        if matches!(scope, PubSubScope::LocalOnly) || policy.hops_limit == 0 {
            return Ok(());
        }

        let groups = self.groups_for_scope(topic_key, scope);
        if groups.is_empty() {
            return Ok(());
        }

        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed) as u128;
        for (next_hop, destinations) in groups {
            let frame = encode_frame(
                topic_key,
                type_hash,
                msg_id,
                self.local_peer_id.clone(),
                self.local_peer_id.clone(),
                policy.hops_limit,
                policy.mode,
                destinations.as_ref(),
                payload.as_ref(),
            )?;
            stats.remote_attempted = stats.remote_attempted.saturating_add(1);
            match self.try_send_next_hop(&next_hop, frame) {
                Ok(()) => stats.remote_enqueued = stats.remote_enqueued.saturating_add(1),
                Err(GossipError::WriteQueueFull) => {
                    stats.remote_full = stats.remote_full.saturating_add(1);
                    self.counters
                        .queue_full_drops
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    stats.remote_transport_errors = stats.remote_transport_errors.saturating_add(1)
                }
            }
        }
        Ok(())
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
            for next_hop in grouped.keys() {
                if next_conns
                    .get(next_hop)
                    .map(|conn| conn.is_closed())
                    .unwrap_or(true)
                {
                    if let Ok(peer_ref) = self.client.lookup_peer(next_hop).await
                        && let Some(conn) = peer_ref.connection_ref()
                    {
                        next_conns.insert(next_hop.clone(), conn);
                    }
                }
            }
            next_routes.insert(topic, Arc::new(grouped));
        }
        self.route_groups.store(Arc::new(next_routes));
        self.conns.store(Arc::new(next_conns));
    }

    fn deliver_local(&self, topic_key: u64, type_hash: u64, payload: Bytes) -> u32 {
        let mut delivered = 0u32;
        let subscribers = self.subscribers.load();
        if let Some(callbacks) = subscribers.get(&(topic_key, type_hash)).cloned() {
            for callback in callbacks.iter() {
                callback(Bytes::clone(&payload));
                delivered = delivered.saturating_add(1);
            }
        }
        drop(subscribers);

        let type_subscribers = self.type_subscribers.load();
        if let Some(callbacks) = type_subscribers.get(&type_hash).cloned() {
            for callback in callbacks.iter() {
                callback(topic_key, Bytes::clone(&payload));
                delivered = delivered.saturating_add(1);
            }
        }
        delivered
    }

    fn groups_for_scope(&self, topic_key: u64, scope: PubSubScope) -> RouteGroups {
        match scope {
            PubSubScope::LocalOnly => HashMap::new(),
            PubSubScope::AutoExternal | PubSubScope::ClusterWide => self
                .route_groups
                .load()
                .get(&topic_key)
                .map(|groups| groups.as_ref().clone())
                .unwrap_or_default(),
            PubSubScope::SelectedPeers(peers) => {
                if let Some(provider) = self.route_provider.load().as_ref() {
                    provider.group_destinations(topic_key, &peers)
                } else {
                    peers
                        .into_iter()
                        .filter(|peer| peer != &self.local_peer_id)
                        .map(|peer| (peer.clone(), Arc::from(vec![peer].into_boxed_slice())))
                        .collect()
                }
            }
        }
    }

    fn try_send_next_hop(&self, next_hop: &PeerId, frame: Bytes) -> Result<()> {
        let conns = self.conns.load();
        let Some(conn) = conns.get(next_hop) else {
            self.counters
                .route_miss_drops
                .fetch_add(1, Ordering::Relaxed);
            return Err(GossipError::ActorNotFound("missing pubsub next-hop".into()));
        };
        conn.try_pubsub_frame(frame)
    }

    fn note_interest(&self, topic_key: u64, present: bool) {
        let mut counts = match self.local_counts.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        let prev = counts.get(&topic_key).copied().unwrap_or(0);
        let next = if present {
            prev.saturating_add(1)
        } else {
            prev.saturating_sub(1)
        };
        if next == 0 {
            counts.remove(&topic_key);
        } else {
            counts.insert(topic_key, next);
        }
        drop(counts);

        if (present && prev == 0) || (!present && prev == 1) {
            let registry = Arc::clone(&self.registry);
            let peer = self.local_peer_id.clone();
            tokio::spawn(async move {
                let name = interest_name(topic_key, &peer);
                if present {
                    let mut location = RemoteActorLocation::new_with_peer(registry.bind_addr, peer);
                    location.priority = RegistrationPriority::Immediate;
                    let _ = registry
                        .register_actor_with_priority(
                            name,
                            location,
                            RegistrationPriority::Immediate,
                        )
                        .await;
                } else {
                    let _ = registry.unregister_actor(&name).await;
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
        if seen
            .iter()
            .any(|entry| entry.0 == *origin && entry.1 == msg_id)
        {
            return false;
        }
        if seen.len() >= DEFAULT_SEEN_CAPACITY {
            seen.remove(0);
        }
        seen.push((origin.clone(), msg_id));
        true
    }
}

impl PubSubIngressHandler for RoutedPubSub {
    fn handle_pubsub_frame(&self, payload: crate::AlignedBytes) -> Result<()> {
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
