#![allow(
    clippy::items_after_test_module,
    reason = "legacy lint debt outside the focused gossip QA fix"
)]
#![expect(
    clippy::borrow_deref_ref,
    clippy::collapsible_if,
    clippy::default_constructed_unit_structs,
    clippy::field_reassign_with_default,
    clippy::manual_is_multiple_of,
    clippy::needless_borrow,
    clippy::needless_lifetimes,
    clippy::option_as_ref_deref,
    clippy::question_mark,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::vec_box,
    reason = "legacy lint debt outside the focused gossip QA fix"
)]

pub mod addr_ownership;
pub mod aligned;
mod ask_forwarder;
mod ask_responder;
pub mod config;
pub(crate) mod connection_pool;
pub mod dns;
pub mod framing;
mod handle;
mod handle_builder;
pub mod handshake;
pub mod lifecycle;
mod net;
mod net_security;
pub mod peer_discovery;
pub mod priority;
pub mod protocol;
pub mod pubsub;
pub mod registry;
pub mod registry_owner;
pub mod remote_actor_location;
pub mod remote_actor_ref;
mod route_interning;
#[cfg(any(test, feature = "test-helpers", debug_assertions))]
pub mod test_helpers;
pub mod tls;
pub mod transport;
pub mod typed;

use arc_swap::ArcSwap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

pub use aligned::{AlignedBytes, AlignedBytesPool, PAYLOAD_ALIGNMENT, PooledAlignedBuffer};
pub use ask_forwarder::{AskForwardObserver, AskForwarder};
pub use ask_responder::{AskContext, AskReplyObserver, AskResponder, TellContext, TryReplyError};
pub use config::{ConnectionRecoveryPolicy, GossipConfig};
pub use dns::{DnsResolver, TokioDnsResolver};

/// Maximum allowed size for streaming payloads (hard cap).
pub const MAX_STREAM_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// Maximum aggregate bytes a single connection may hold in eagerly-allocated
/// in-flight stream reassembly buffers at once. A `StreamStart` frame
/// pre-allocates its whole declared `total_size`, so without this cap a peer
/// could open `max_concurrent_streams` (16) streams each declaring
/// `MAX_STREAM_SIZE` and force ~1 GiB of eager allocation per connection at
/// near-zero cost. Bounding the *sum* of declared sizes caps that at a couple
/// of max-size streams' worth while still admitting many small streams.
pub const MAX_INFLIGHT_STREAM_BYTES: usize = 2 * MAX_STREAM_SIZE; // 128MB
pub use handle::{GossipClient, GossipRegistryHandle};
pub use handle_builder::BuilderTlsBootstrap;
pub use lifecycle::{
    SessionRemovalReason, TransportDirection, TransportLifecycleEvent, TransportLifecycleRecorder,
    TransportLifecycleRecorderGuard, set_transport_lifecycle_recorder,
};
pub use priority::RegistrationPriority;
pub use pubsub::{
    PubSubDeliveryMode, PubSubDeliveryPolicy, PubSubFrameMetadata, PubSubIngressHandler,
    PubSubIngressStats, PubSubPublishStats, PubSubRouteProvider, PubSubScope, PubSubSubscription,
    RoutedPubSub, topic_key,
};
pub use registry::{ClockEchoV1, ClockProbeV1, GossipExtensionsV1, PeerClockSnapshot};
pub use remote_actor_location::RemoteActorLocation;
pub use remote_actor_ref::{RemoteActorRef, RemoteConnection};
pub use transport::{RegistryTransportBootstrap, TransportWireKind};
pub use typed::{
    ArchivedBytes, WireEncode, WireType, decode_typed, decode_typed_archived, encode_typed,
};

/// Deferred ask handle exposed as high-level API.
///
/// This can be moved to another task and awaited later.
#[derive(Debug)]
pub struct DeferredAsk {
    inner: connection_pool::PendingAsk,
}

impl DeferredAsk {
    pub fn correlation_id(&self) -> u32 {
        self.inner.correlation_id()
    }

    pub async fn wait(self) -> Result<bytes::Bytes> {
        self.inner.wait().await
    }

    pub(crate) fn from_pending(inner: connection_pool::PendingAsk) -> Self {
        Self { inner }
    }
}

// =================== New Iroh-style types ===================

/// Public key for node identity - Ed25519 public key
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct PublicKey {
    inner: [u8; 32],
}

impl PublicKey {
    /// Create from raw bytes, validating they form a valid Ed25519 public key
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid public key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);

        // Validate it's a valid Ed25519 public key
        let _ = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))?;

        Ok(Self { inner: key_bytes })
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.inner
    }

    /// Convert to ed25519_dalek::VerifyingKey for crypto operations
    pub fn to_verifying_key(&self) -> Result<VerifyingKey> {
        VerifyingKey::from_bytes(&self.inner)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))
    }

    /// Verify a signature
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        let verifying_key = self.to_verifying_key()?;
        verifying_key.verify(message, signature).map_err(|e| {
            GossipError::InvalidSignature(format!("Signature verification failed: {}", e))
        })
    }

    /// Format first 5 bytes as hex for logging (like Iroh)
    pub fn fmt_short(&self) -> String {
        hex::encode(&self.inner[..5])
    }

    /// Convert to PeerId
    pub fn to_peer_id(&self) -> PeerId {
        PeerId::from_bytes(self.as_bytes())
            .expect("GossipNodeId should always convert to valid PeerId")
    }
}

impl Hash for PublicKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PublicKey({}…)", self.fmt_short())
    }
}

impl std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fmt_short())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            // Use base32 for human-readable formats
            let encoded = data_encoding::BASE32_NOPAD.encode(&self.inner);
            serializer.serialize_str(&encoded)
        } else {
            // Use raw bytes for binary formats
            serializer.serialize_bytes(&self.inner)
        }
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let bytes = data_encoding::BASE32_NOPAD
                .decode(s.as_bytes())
                .map_err(serde::de::Error::custom)?;
            Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; 32]>::deserialize(deserializer)?;
            Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
        }
    }
}

/// Gossip-registry / vector-clock identity - alias for PublicKey (like Iroh).
///
/// This is the key used in [`VectorClock`] and [`RemoteActorLocation`] to
/// track per-node causal history in the actor-location gossip registry. It is
/// intentionally NOT named `NodeId` (its pre-rename name): membership's stable
/// `NodeId` (actor-framework-core's SWIM membership module) is a distinct,
/// unrelated identity space -- a cluster-membership identity, not a
/// vector-clock/gossip key. Do not conflate the two.
pub type GossipNodeId = PublicKey;

/// Secret key for node identity - Ed25519 signing key with secure cleanup
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SecretKey {
    // `SigningKey` zeroizes itself on drop when ed25519-dalek's zeroize feature is enabled.
    // The outer derive skips it so cleanup remains owned by the key type.
    #[zeroize(skip)]
    secret: SigningKey,
}

impl SecretKey {
    /// Generate a new random secret key
    pub fn generate() -> Self {
        use rand::Rng;
        let mut rng = rand::rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        let secret = SigningKey::from_bytes(&bytes);
        bytes.zeroize(); // Clear the temporary bytes
        Self { secret }
    }

    /// Create from raw bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid secret key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);
        let secret = SigningKey::from_bytes(&key_bytes);
        key_bytes.zeroize(); // Clear the temporary bytes
        Ok(Self { secret })
    }

    /// Get the corresponding public key
    pub fn public(&self) -> PublicKey {
        let verifying_key = self.secret.verifying_key();
        PublicKey {
            inner: verifying_key.to_bytes(),
        }
    }

    /// Sign a message
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.secret.sign(message)
    }

    /// Get raw bytes (use with caution - these should be zeroized after use)
    pub fn to_bytes(&self) -> [u8; 32] {
        self.secret.to_bytes()
    }

    /// Convert to a KeyPair for existing APIs
    pub fn to_keypair(&self) -> KeyPair {
        let mut key_bytes = self.to_bytes();
        let keypair = KeyPair::from_private_key_bytes(&key_bytes)
            .expect("SecretKey should always convert to valid KeyPair");
        key_bytes.zeroize();
        keypair
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretKey(***)")
    }
}

// =================== End new types ===================

/// Vector clock for tracking causal relationships between events
pub struct VectorClock {
    // `ArcSwap` gives us lock-free reads with copy-on-write updates. This keeps
    // VectorClock mutation APIs (`&self`) usable throughout the codebase while
    // avoiding lock-based maps.
    clocks: ArcSwap<BTreeMap<GossipNodeId, u64>>,
}

impl Clone for VectorClock {
    fn clone(&self) -> Self {
        // Deep clone to preserve historical semantics: clones should not share
        // internal mutation state.
        let snapshot = self.clocks.load_full();
        Self {
            clocks: ArcSwap::from_pointee(snapshot.as_ref().clone()),
        }
    }
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: ArcSwap::from_pointee(BTreeMap::new()),
        }
    }

    pub fn with_node(node_id: GossipNodeId) -> Self {
        let mut map = BTreeMap::new();
        map.insert(node_id, 0);
        Self {
            clocks: ArcSwap::from_pointee(map),
        }
    }

    /// Increment the clock for a specific node.
    pub fn increment(&self, node_id: GossipNodeId) {
        // Copy-on-write update: clone the small map and CAS it in.
        loop {
            let current = self.clocks.load();
            let mut next = (**current).clone();
            let entry = next.entry(node_id).or_insert(0);
            *entry = entry.saturating_add(1);

            let swapped = self.clocks.compare_and_swap(&*current, Arc::new(next));
            if Arc::ptr_eq(&swapped, &current) {
                break;
            }
        }
    }

    /// Merge with another vector clock.
    pub fn merge(&self, other: &VectorClock) {
        let other_snapshot = other.clocks.load();
        loop {
            let current = self.clocks.load();
            let mut next = (**current).clone();

            for (other_node, other_clock) in other_snapshot.iter() {
                let entry = next.entry(*other_node).or_insert(0);
                *entry = (*entry).max(*other_clock);
            }

            let swapped = self.clocks.compare_and_swap(&*current, Arc::new(next));
            if Arc::ptr_eq(&swapped, &current) {
                break;
            }
        }
    }

    /// Compare vector clocks to determine causal relationship
    pub fn compare(&self, other: &VectorClock) -> ClockOrdering {
        let mut self_greater = false;
        let mut other_greater = false;

        let self_snapshot = self.clocks.load();
        let other_snapshot = other.clocks.load();

        // Collect all node IDs from both clocks.
        let mut all_nodes = std::collections::BTreeSet::new();
        all_nodes.extend(self_snapshot.keys().copied());
        all_nodes.extend(other_snapshot.keys().copied());

        for node_id in all_nodes {
            let self_clock = self_snapshot.get(&node_id).copied().unwrap_or(0);
            let other_clock = other_snapshot.get(&node_id).copied().unwrap_or(0);

            match self_clock.cmp(&other_clock) {
                std::cmp::Ordering::Greater => self_greater = true,
                std::cmp::Ordering::Less => other_greater = true,
                std::cmp::Ordering::Equal => {}
            }
        }

        match (self_greater, other_greater) {
            (true, false) => ClockOrdering::After,
            (false, true) => ClockOrdering::Before,
            (false, false) => ClockOrdering::Equal,
            (true, true) => ClockOrdering::Concurrent,
        }
    }

    /// Garbage collect entries for nodes not seen recently (thread-safe)
    pub fn gc_old_nodes(&self, active_nodes: &std::collections::HashSet<GossipNodeId>) {
        loop {
            let current = self.clocks.load();
            if current.is_empty() {
                break;
            }

            let mut next = (**current).clone();
            next.retain(|node_id, _| active_nodes.contains(node_id));

            let swapped = self.clocks.compare_and_swap(&*current, Arc::new(next));
            if Arc::ptr_eq(&swapped, &current) {
                break;
            }
        }
    }

    /// Get the number of entries in the vector clock
    pub fn len(&self) -> usize {
        self.clocks.load().len()
    }

    /// Check if the vector clock is empty
    pub fn is_empty(&self) -> bool {
        self.clocks.load().is_empty()
    }

    /// Compact the vector clock if it exceeds the maximum size (thread-safe)
    pub fn compact(&self, max_size: usize) {
        loop {
            let current = self.clocks.load();
            if current.len() <= max_size {
                break;
            }

            // Collect all entries and sort by clock value desc (drop small entries first).
            let mut entries: Vec<(GossipNodeId, u64)> =
                current.iter().map(|(k, v)| (*k, *v)).collect();
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.1));
            entries.truncate(max_size);

            let mut next = BTreeMap::new();
            for (node_id, clock) in entries {
                next.insert(node_id, clock);
            }

            let swapped = self.clocks.compare_and_swap(&*current, Arc::new(next));
            if Arc::ptr_eq(&swapped, &current) {
                break;
            }
        }
    }

    /// Convert to a sorted Vec for serialization
    fn to_vec(&self) -> Vec<(GossipNodeId, u64)> {
        self.clocks
            .load()
            .iter()
            .map(|(node_id, clock)| (*node_id, *clock))
            .collect()
    }

    /// Create from a Vec (used in deserialization)
    fn from_vec(vec: Vec<(GossipNodeId, u64)>) -> Self {
        let mut clocks = BTreeMap::new();
        for (node_id, clock) in vec {
            clocks.insert(node_id, clock);
        }
        Self {
            clocks: ArcSwap::from_pointee(clocks),
        }
    }

    /// Get the clock value for a specific node
    pub fn get(&self, node_id: &GossipNodeId) -> u64 {
        self.clocks.load().get(node_id).copied().unwrap_or(0)
    }

    /// Check if this vector clock happened before another
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::Before)
    }

    /// Check if this vector clock happened after another
    pub fn happens_after(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::After)
    }

    /// Check if this vector clock is concurrent with another
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::Concurrent)
    }

    /// Get all nodes referenced in this vector clock
    pub fn get_nodes(&self) -> std::collections::HashSet<GossipNodeId> {
        self.clocks.load().keys().copied().collect()
    }

    /// Check if this vector clock is "empty" (only has zero entries)
    pub fn is_effectively_empty(&self) -> bool {
        self.clocks.load().values().all(|v| *v == 0)
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for VectorClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vec = self.to_vec();
        f.debug_struct("VectorClock").field("clocks", &vec).finish()
    }
}

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.compare(other), ClockOrdering::Equal)
    }
}

impl Eq for VectorClock {}

impl std::hash::Hash for VectorClock {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let sorted = self.to_vec();
        for (node_id, clock) in sorted {
            node_id.hash(state);
            clock.hash(state);
        }
    }
}

// For rkyv serialization, we need a simple wrapper type
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone)]
#[rkyv(derive(Debug))]
pub struct VectorClockData {
    pub clocks: Vec<(GossipNodeId, u64)>,
}

impl From<&VectorClock> for VectorClockData {
    fn from(vc: &VectorClock) -> Self {
        VectorClockData {
            clocks: vc.to_vec(),
        }
    }
}

impl From<VectorClockData> for VectorClock {
    fn from(data: VectorClockData) -> Self {
        VectorClock::from_vec(data.clocks)
    }
}

// Custom rkyv implementation that uses VectorClockData
impl rkyv::Archive for VectorClock {
    type Archived = <VectorClockData as rkyv::Archive>::Archived;
    type Resolver = <VectorClockData as rkyv::Archive>::Resolver;

    fn resolve(&self, resolver: Self::Resolver, out: rkyv::Place<Self::Archived>) {
        let data = VectorClockData::from(self);
        data.resolve(resolver, out);
    }
}

impl<S> rkyv::Serialize<S> for VectorClock
where
    S: rkyv::rancor::Fallible + rkyv::ser::Writer + rkyv::ser::Allocator + ?Sized,
    S::Error: rkyv::rancor::Source,
{
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> std::result::Result<Self::Resolver, <S as rkyv::rancor::Fallible>::Error> {
        let data = VectorClockData::from(self);
        data.serialize(serializer)
    }
}

impl<D> rkyv::Deserialize<VectorClock, D> for <VectorClockData as rkyv::Archive>::Archived
where
    D: rkyv::rancor::Fallible + rkyv::de::Pooling + ?Sized,
    D::Error: rkyv::rancor::Source,
{
    fn deserialize(
        &self,
        deserializer: &mut D,
    ) -> std::result::Result<VectorClock, <D as rkyv::rancor::Fallible>::Error> {
        let data: VectorClockData = self.deserialize(deserializer)?;
        Ok(VectorClock::from(data))
    }
}

/// Ordering relationship between vector clocks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockOrdering {
    Before,
    After,
    Equal,
    Concurrent,
}

/// Key pair for node identity using Ed25519 cryptography
#[derive(Clone)]
pub struct KeyPair {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl KeyPair {
    /// Generate a new random keypair
    pub fn generate() -> Self {
        use rand::Rng;
        let mut rng = rand::rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        let signing_key = SigningKey::from_bytes(&bytes);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Create a keypair from private key bytes
    pub fn from_private_key_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid private key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Get the PeerId (public key) for this keypair
    pub fn peer_id(&self) -> PeerId {
        PeerId::from_verifying_key(self.verifying_key)
    }

    /// Get the private key bytes
    pub fn private_key_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    /// Get the public key bytes
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    /// Convert to SecretKey for TLS identity use
    pub fn to_secret_key(&self) -> SecretKey {
        let mut key_bytes = self.private_key_bytes();
        let secret =
            SecretKey::from_bytes(&key_bytes).expect("KeyPair should always convert to SecretKey");
        key_bytes.zeroize();
        secret
    }

    /// Sign a message
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }

    /// For testing - create a deterministic key pair from a seed string
    pub fn new_for_testing(id: impl Into<String>) -> Self {
        let id = id.into();
        let mut seed = [0u8; 32];
        let id_bytes = id.as_bytes();
        let len = id_bytes.len().min(32);
        seed[..len].copy_from_slice(&id_bytes[..len]);

        let signing_key = SigningKey::from_bytes(&seed);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
        }
    }
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair")
            .field("public_key", &hex::encode(self.verifying_key.as_bytes()))
            .finish()
    }
}

/// Peer identifier - contains the Ed25519 public key
#[derive(Clone, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PeerId([u8; 32]);

impl PeerId {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Create a PeerId from a verifying key
    pub fn from_verifying_key(key: VerifyingKey) -> Self {
        Self(key.to_bytes())
    }

    /// Create a PeerId from a PublicKey
    pub fn from_public_key(key: &PublicKey) -> Self {
        Self(*key.as_bytes())
    }

    /// Create a PeerId from public key bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid public key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);
        // Verify it's a valid public key
        let _ = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))?;
        Ok(Self(key_bytes))
    }

    /// Create a PeerId from hex string
    pub fn from_hex(hex: &str) -> Result<Self> {
        let bytes = hex::decode(hex)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid hex: {}", e)))?;
        Self::from_bytes(&bytes)
    }

    /// Get the public key bytes
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Get the verifying key
    pub fn to_verifying_key(&self) -> Result<VerifyingKey> {
        VerifyingKey::from_bytes(&self.0)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))
    }

    /// Get hex representation
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Verify a signature
    pub fn verify_signature(&self, message: &[u8], signature: &Signature) -> Result<()> {
        let verifying_key = self.to_verifying_key()?;
        verifying_key.verify(message, signature).map_err(|e| {
            GossipError::InvalidSignature(format!("Signature verification failed: {}", e))
        })
    }

    /// Convert to GossipNodeId (which is just an alias for PublicKey)
    pub fn to_node_id(&self) -> GossipNodeId {
        GossipNodeId::from_bytes(&self.0).expect("PeerId should always be a valid GossipNodeId")
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PeerId").field(&self.to_hex()).finish()
    }
}

impl From<PublicKey> for PeerId {
    fn from(key: PublicKey) -> Self {
        Self(*key.as_bytes())
    }
}

impl From<&PublicKey> for PeerId {
    fn from(key: &PublicKey) -> Self {
        Self(*key.as_bytes())
    }
}

/// Handle to a configured peer
#[derive(Clone)]
pub struct Peer<T = ()> {
    peer_id: PeerId,
    registry: std::sync::Arc<registry::GossipRegistry<T>>,
}

impl<T: 'static> Peer<T> {
    /// Connect to this peer at the specified address
    pub async fn connect(&self, addr: &SocketAddr) -> Result<()> {
        self.connect_with_route_mode(addr, true).await
    }

    /// Connect to a learned actor/service route without making it a required
    /// peer for the configured-peer supervisor.
    pub async fn connect_discovered(&self, addr: &SocketAddr) -> Result<()> {
        self.connect_with_route_mode(addr, false).await
    }

    /// A concurrent `configure_peer` can repin `self.peer_id` to a
    /// different address between accepting the route below and
    /// `connect_to_peer` actually resolving a connection, so the dial can
    /// land somewhere other than the caller's `addr`. Every outcome
    /// (health marking, gossip, `gossip_state` insertion) is therefore
    /// attributed to whatever `connect_to_peer` reports it ACTUALLY
    /// resolved (`effective_addr` below), re-derived fresh rather than
    /// trusted from an earlier check, and every insertion is deferred
    /// until after the dial resolves -- see each `match` arm below. The
    /// discovered-route pool writes and the existing-connection eviction
    /// check are unaffected: the former never goes through the owner's
    /// pin/route machinery, and the latter keys off whatever connection is
    /// currently published, not off `addr`.
    async fn connect_with_route_mode(&self, addr: &SocketAddr, required_peer: bool) -> Result<()> {
        if self.peer_id == self.registry.peer_id {
            tracing::warn!(
                peer_id = %self.peer_id,
                addr = %addr,
                "refusing to dial local registry identity as a remote peer"
            );
            return Ok(());
        }

        // Validate the address
        if addr.port() == 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid port 0 for peer {}", self.peer_id),
            )));
        }
        if addr.ip().is_unspecified() {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Refusing to connect peer {} via unspecified address {}",
                    self.peer_id, addr
                ),
            )));
        }

        // First configure the address for this peer
        if required_peer {
            // Routed through the owner (`set_ordinary_connect_route`)
            // rather than writing `ConnectionPool` directly, so the
            // pin-conflict check and the route write (plus its reindex)
            // are one step in the owner's own serialization -- a
            // caller-side read-then-write of the pin is never atomic with
            // a concurrent `configure_peer`/`migrate` committing a new pin
            // in the gap.
            //
            // `false` means the owner declined the route (`self.peer_id`
            // is pinned elsewhere) -- NOT treated as nothing left to do:
            // this falls through to dial and report on the authoritative
            // pinned address instead, which is safe because every outcome
            // below is keyed to whatever `connect_to_peer` actually
            // resolves, never to this stale `addr`.
            let route_accepted = self
                .registry
                .registry_owner
                .set_ordinary_connect_route(self.peer_id.clone(), *addr)
                .await;
            if !route_accepted {
                tracing::warn!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "ordinary connect declined: peer is operator-pinned to a different \
                     address; continuing through the pinned route instead of the requested \
                     one"
                );
            }
        } else {
            let pool = &self.registry.connection_pool;
            pool.set_discovered_peer_addr(&self.peer_id, *addr);
            let _ = pool
                .addr_to_peer_id
                .upsert_sync(*addr, self.peer_id.clone());
            pool.reindex_connection_addr(&self.peer_id, *addr);
        }

        // `gossip_state.peers` is deliberately NOT touched here, before the
        // dial: `route_accepted` above is only accurate at the instant the
        // owner command executed, and a concurrent repin before the dial
        // below would otherwise leave a fresh entry keyed to a stale
        // `addr`. Insertion is deferred to AFTER the dial resolves, keyed
        // to whatever it actually reports -- see each `match` arm below.

        if let Some(existing_conn) = self
            .registry
            .connection_pool
            .get_connection_by_peer_id(&self.peer_id)
        {
            let existing_is_outbound =
                existing_conn.direction == crate::connection_pool::ConnectionDirection::Outbound;
            if !self
                .registry
                .should_keep_connection(&self.peer_id, existing_is_outbound)
            {
                tracing::info!(
                    target: "icanact_remote_lifecycle",
                    peer_id = %self.peer_id,
                    addr = %existing_conn.addr,
                    existing_direction = ?existing_conn.direction,
                    "outbound_connect_drop_wrong_direction_before_redial"
                );
                crate::lifecycle::record_transport_event(
                    crate::lifecycle::TransportLifecycleEvent::WrongDirectionEvicted {
                        peer: self.peer_id.clone(),
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
                // Instance-scoped, not peer-wide: `should_keep_connection`
                // was evaluated against this specific `existing_conn`. A
                // peer-wide `disconnect_connection_by_peer_id` here would
                // tear down whatever is *currently* indexed for the peer,
                // which could be a fresh connection published between the
                // decision above and this call — exactly the collateral
                // teardown / reconnect-thrash race this crate's
                // instance-scoped teardown discipline exists to close.
                // `disconnect_connection_instance` CAS's against
                // `existing_conn` by `Arc` identity and is a safe no-op if a
                // concurrent publish has already superseded it.
                let _ = self
                    .registry
                    .connection_pool
                    .disconnect_connection_instance(&self.peer_id, &existing_conn);
            }
        }

        // Both arms produce the address ACTUALLY contacted, not just
        // `Result<()>`: `get_connection(*addr)` can, in a narrow dial-gate
        // race, resolve an already-published connection for the same peer
        // identity at a DIFFERENT address than `*addr` -- see the `Ok` arm
        // below.
        let connect_result: Result<crate::registry::ConnectOutcome> = if required_peer {
            // Sole authority for its own gossip_state bookkeeping on both
            // outcomes (see its own doc comment); this function does none
            // of its own for the required-peer path.
            self.registry
                .connect_to_peer_with_outcome(&self.peer_id)
                .await
                .1
        } else {
            self.registry.get_connection(*addr).await.map(|conn| {
                // `conn.addr` may be a reused inbound connection's raw
                // transport source rather than a corroborated dial target
                // -- `ResolvedRoute::from_connection` needs the direction.
                // Read from `conn` itself, captured when this exact handle
                // was built, not re-looked-up by address: `connections_by_
                // addr` can be evicted or reassigned to a DIFFERENT
                // connection between resolving `conn` and a fresh lookup
                // (pin-alias eviction is one such reassignment), which
                // would attribute the wrong connection's direction here.
                let direction = Some(conn.direction());
                crate::registry::ConnectOutcome::resolved(
                    crate::registry::ResolvedRoute::from_connection(conn.addr, direction),
                )
            })
        };
        match connect_result {
            Ok(outcome) => {
                let effective_addr = outcome.addr();
                tracing::info!(
                    peer_id = %self.peer_id,
                    requested_addr = %addr,
                    effective_addr = %effective_addr,
                    "Successfully connected to peer"
                );
                // `connect_to_peer` (required_peer) already did its own
                // bookkeeping internally; only the discovered/non-required
                // path has none of its own, so this remains the sole place
                // that path's success is recorded.
                if !required_peer {
                    // Insert-if-absent, keyed to `effective_addr` (never
                    // the bare `*addr` request) for the same reason as the
                    // function's own doc comment. Always `resolved(...)`,
                    // never `ConnectedUnverified` (only `connect_to_peer`
                    // produces that), so a route is always present here.
                    let route = outcome.resolved_route().expect(
                        "the discovered/non-required path always constructs \
                         ConnectOutcome::resolved -- see this function's own connect_result \
                         construction above",
                    );
                    let mut gossip_state = self.registry.gossip_state.lock().await;
                    let node_id = Some(self.peer_id.to_node_id());
                    let peer_info = gossip_state.peers.entry(effective_addr).or_insert_with(|| {
                        crate::registry::PeerInfo::for_connect_attempt(route, node_id)
                    });
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                    peer_info.last_failure_instant = None;
                    peer_info.last_success = crate::current_timestamp();
                    peer_info.last_response_received_ms = crate::current_timestamp_millis();
                }
                // Trigger an immediate gossip round to sync
                let _ = self.registry.trigger_immediate_gossip().await;
                Ok(())
            }
            Err(GossipError::Network(io_err)) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    error = %io_err,
                    "Network error connecting to peer"
                );

                // `connect_to_peer` (required_peer) already recorded
                // failure internally against the address it actually
                // attempted; recording a second one here keyed by `addr`
                // would risk a spurious entry. Only the discovered/
                // non-required path has no bookkeeping of its own, and
                // `addr` is unambiguous for it.
                if !required_peer {
                    let mut gossip_state = self.registry.gossip_state.lock().await;
                    let node_id = Some(self.peer_id.to_node_id());
                    let peer_info = gossip_state
                        .peers
                        .entry(*addr)
                        .or_insert_with(|| crate::registry::PeerInfo::for_failed_connect_attempt(
                            crate::registry::AttemptedRoute::new(*addr),
                            node_id,
                        ));
                    peer_info.failures = self.registry.config.max_peer_failures;
                    peer_info.last_failure_time = Some(crate::current_timestamp());
                    peer_info.last_failure_instant = Some(std::time::Instant::now());
                    tracing::debug!(
                        peer_id = %self.peer_id,
                        addr = %addr,
                        failures = peer_info.failures,
                        "Updated peer failure state after connection error"
                    );
                }

                Err(GossipError::Network(std::io::Error::new(
                    io_err.kind(),
                    format!(
                        "Failed to connect to peer {} at {}: {}",
                        self.peer_id, addr, io_err
                    ),
                )))
            }
            Err(GossipError::Timeout) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Connection timeout when connecting to peer"
                );

                // See the Network arm above for why this is conditional
                // and insert-if-absent.
                if !required_peer {
                    let mut gossip_state = self.registry.gossip_state.lock().await;
                    let node_id = Some(self.peer_id.to_node_id());
                    let peer_info = gossip_state
                        .peers
                        .entry(*addr)
                        .or_insert_with(|| crate::registry::PeerInfo::for_failed_connect_attempt(
                            crate::registry::AttemptedRoute::new(*addr),
                            node_id,
                        ));
                    peer_info.failures = self.registry.config.max_peer_failures;
                    peer_info.last_failure_time = Some(crate::current_timestamp());
                    peer_info.last_failure_instant = Some(std::time::Instant::now());
                }

                Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Connection timeout to peer {} at {}", self.peer_id, addr),
                )))
            }
            Err(GossipError::ConnectionExists) => {
                tracing::debug!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Connection already exists to peer"
                );
                // This is not really an error - connection already exists
                Ok(())
            }
            Err(GossipError::Shutdown) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Registry is shutting down, cannot connect to peer"
                );
                Err(GossipError::Shutdown)
            }
            Err(other_err) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    error = %other_err,
                    "Unexpected error connecting to peer"
                );
                Err(other_err)
            }
        }
    }

    /// Check if this peer is currently connected
    pub async fn is_connected(&self) -> bool {
        let pool = &self.registry.connection_pool;

        // Check if we have a connection by peer ID
        if let Some(conn) = pool.get_connection_by_peer_id(&self.peer_id) {
            conn.is_connected()
        } else {
            false
        }
    }

    /// Disconnect from this peer
    pub async fn disconnect(&self) -> Result<()> {
        let pool = &self.registry.connection_pool;

        if let Some(conn) = pool.get_connection_by_peer_id(&self.peer_id) {
            // Mark connection as disconnected
            conn.set_state(crate::connection_pool::ConnectionState::Disconnected);

            // Get the peer address for mark_disconnected
            let peer_addr = pool.get_configured_peer_addr(&self.peer_id);
            if let Some(addr) = peer_addr {
                pool.mark_disconnected(addr);
            }

            tracing::info!(
                peer_id = %self.peer_id,
                "Disconnected from peer"
            );
            Ok(())
        } else {
            tracing::debug!(
                peer_id = %self.peer_id,
                "No connection found to disconnect"
            );
            Ok(()) // Not an error if no connection exists
        }
    }

    /// Get the peer ID
    pub fn id(&self) -> &PeerId {
        &self.peer_id
    }
}

/// Message types for the request-response protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Gossip protocol message (registry sync, actor registrations, etc.)
    Gossip = 0,
    /// Request expecting a response (ask)
    Ask = 1,
    /// Response to an ask request
    Response = 2,
    /// Direct actor tell message (no wrapping)
    ActorTell = 3,
    /// Direct actor ask message (no wrapping)
    ActorAsk = 4,
    /// Start of a streaming REQUEST transfer
    StreamStart = 0x10,
    /// Streaming REQUEST data chunk
    StreamData = 0x11,
    /// End of streaming REQUEST transfer
    StreamEnd = 0x12,
    /// Start of a streaming RESPONSE transfer
    StreamResponseStart = 0x13,
    /// Streaming RESPONSE data chunk
    StreamResponseData = 0x14,
    /// End of streaming RESPONSE transfer
    StreamResponseEnd = 0x15,
    /// Fast-path direct ask (bypasses actor message handler)
    DirectAsk = 0x20,
    /// Fast-path direct response
    DirectResponse = 0x21,
    /// Routed PubSub data-plane frame
    PubSub = 0x30,
}

impl MessageType {
    /// Parse message type from byte
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(MessageType::Gossip),
            1 => Some(MessageType::Ask),
            2 => Some(MessageType::Response),
            3 => Some(MessageType::ActorTell),
            4 => Some(MessageType::ActorAsk),
            0x10 => Some(MessageType::StreamStart),
            0x11 => Some(MessageType::StreamData),
            0x12 => Some(MessageType::StreamEnd),
            0x13 => Some(MessageType::StreamResponseStart),
            0x14 => Some(MessageType::StreamResponseData),
            0x15 => Some(MessageType::StreamResponseEnd),
            0x20 => Some(MessageType::DirectAsk),
            0x21 => Some(MessageType::DirectResponse),
            0x30 => Some(MessageType::PubSub),
            _ => None,
        }
    }

    /// Check if this is a streaming response message type
    pub fn is_streaming_response(&self) -> bool {
        matches!(
            self,
            MessageType::StreamResponseStart
                | MessageType::StreamResponseData
                | MessageType::StreamResponseEnd
        )
    }

    /// Check if this is a streaming request message type
    pub fn is_streaming_request(&self) -> bool {
        matches!(
            self,
            MessageType::StreamStart | MessageType::StreamData | MessageType::StreamEnd
        )
    }
}

/// Header for streaming protocol messages
#[derive(Debug, Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StreamHeader {
    /// Unique stream identifier
    pub stream_id: u64,
    /// Total size of the complete message
    pub total_size: u64,
    /// Size of this chunk (0 for start/end markers)
    pub chunk_size: u32,
    /// Chunk sequence number
    pub chunk_index: u32,
    /// Message type hash
    pub type_hash: u32,
    /// Target actor ID
    pub actor_id: u64,
}

impl StreamHeader {
    /// Size of the serialized header
    pub const SERIALIZED_SIZE: usize = 8 + 8 + 4 + 4 + 4 + 8; // 36 bytes

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_SIZE);
        bytes.extend_from_slice(&self.stream_id.to_be_bytes());
        bytes.extend_from_slice(&self.total_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_index.to_be_bytes());
        bytes.extend_from_slice(&self.type_hash.to_be_bytes());
        bytes.extend_from_slice(&self.actor_id.to_be_bytes());
        bytes
    }

    /// Parse header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::SERIALIZED_SIZE {
            return None;
        }

        Some(Self {
            stream_id: u64::from_be_bytes(bytes[0..8].try_into().ok()?),
            total_size: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            chunk_size: u32::from_be_bytes(bytes[16..20].try_into().ok()?),
            chunk_index: u32::from_be_bytes(bytes[20..24].try_into().ok()?),
            type_hash: u32::from_be_bytes(bytes[24..28].try_into().ok()?),
            actor_id: u64::from_be_bytes(bytes[28..36].try_into().ok()?),
        })
    }
}

/// Errors that can occur in the gossip registry
#[derive(Error, Debug)]
pub enum GossipError {
    #[error("network error: {0}")]
    Network(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] rkyv::rancor::Error),

    #[error("message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("connection timeout")]
    Timeout,

    #[error("connection dropped while waiting for response")]
    ConnectionDropped,

    #[error("peer not found: {0}")]
    PeerNotFound(SocketAddr),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("TLS configuration error: {0}")]
    TlsConfigError(String),

    #[error("actor not found: {0}")]
    ActorNotFound(String),

    #[error("registry shutdown")]
    Shutdown,

    #[error("connection closed: {0}")]
    ConnectionClosed(SocketAddr),

    #[error("write queue full")]
    WriteQueueFull,

    #[error("invalid keypair: {0}")]
    InvalidKeyPair(String),

    #[error("invalid signature: {0}")]
    InvalidSignature(String),

    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("TLS handshake failed: {0}")]
    TlsHandshakeFailed(String),

    #[error("delta too old: requested {requested}, oldest available {oldest}")]
    DeltaTooOld { requested: u64, oldest: u64 },

    #[error("full sync required")]
    FullSyncRequired,

    #[error("connection already exists")]
    ConnectionExists,

    #[error("actor '{0}' already exists")]
    ActorAlreadyExists(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("non-zero-copy API disabled: {0}")]
    NonZeroCopyPath(&'static str),

    #[error("correlation tracker exhausted: all slots in use (slot leak suspected)")]
    CorrelationTrackerExhausted,

    /// The peer answered an ask with an explicit NACK instead of data: it
    /// received the request but could not or would not produce a response.
    /// Delivered to the waiter immediately through the correlation tracker,
    /// so an unanswerable ask fails fast instead of burning its timeout.
    #[error("ask was not answered: {0}")]
    AskNacked(crate::framing::AskNackReason),
}

impl From<crate::connection_pool::NoFreeSlots> for GossipError {
    fn from(_: crate::connection_pool::NoFreeSlots) -> Self {
        GossipError::CorrelationTrackerExhausted
    }
}

impl GossipError {
    /// Map a dispatch failure to the machine-readable reason an ask NACK
    /// carries back to the waiter. `ActorNotFound` and an explicit
    /// `AskNacked` (an `AskDisposition::Nack` the handler chose) keep their
    /// specific reason; anything else is a generic handler error.
    pub(crate) fn ask_nack_reason(&self) -> crate::framing::AskNackReason {
        match self {
            GossipError::ActorNotFound(_) => crate::framing::AskNackReason::UnknownActor,
            GossipError::AskNacked(reason) => *reason,
            _ => crate::framing::AskNackReason::HandlerError,
        }
    }
}

pub type Result<T> = std::result::Result<T, GossipError>;

#[inline]
fn strict_zero_copy_env_enabled() -> bool {
    static STRICT: OnceLock<bool> = OnceLock::new();
    *STRICT.get_or_init(|| {
        matches!(
            std::env::var("ICANACT_STRICT_ZERO_COPY")
                .ok()
                .as_deref()
                .map(str::trim)
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("1" | "true" | "yes" | "on")
        )
    })
}

#[inline]
pub(crate) fn reject_non_zero_copy_path(api: &'static str) -> Result<()> {
    if cfg!(feature = "strict-zero-copy") || strict_zero_copy_env_enabled() {
        return Err(GossipError::NonZeroCopyPath(api));
    }
    Ok(())
}

/// Get current timestamp in seconds (still used for TTL)
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

/// Get current timestamp in milliseconds.
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

/// Get current timestamp in nanoseconds for high precision timing
pub fn current_timestamp_nanos() -> u64 {
    timestamp_nanos_at(SystemTime::now())
}

fn timestamp_nanos_at(time: SystemTime) -> u64 {
    let nanos = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_nanos_saturates_before_unix_epoch() {
        let before_epoch = UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .expect("representable pre-epoch time");
        assert_eq!(timestamp_nanos_at(before_epoch), 0);
    }

    #[test]
    fn test_current_timestamp() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let timestamp = current_timestamp();

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        assert!(timestamp >= before);
        assert!(timestamp <= after);
    }

    #[test]
    fn test_gossip_error_display() {
        let err = GossipError::Network(io::Error::other("test error"));
        assert_eq!(err.to_string(), "network error: test error");

        let err = GossipError::MessageTooLarge {
            size: 1000,
            max: 500,
        };
        assert_eq!(err.to_string(), "message too large: 1000 bytes (max: 500)");

        let err = GossipError::Timeout;
        assert_eq!(err.to_string(), "connection timeout");

        let err = GossipError::PeerNotFound("127.0.0.1:8080".parse().unwrap());
        assert_eq!(err.to_string(), "peer not found: 127.0.0.1:8080");

        let err = GossipError::ActorNotFound("test_actor".to_string());
        assert_eq!(err.to_string(), "actor not found: test_actor");

        let err = GossipError::Shutdown;
        assert_eq!(err.to_string(), "registry shutdown");

        let err = GossipError::DeltaTooOld {
            requested: 10,
            oldest: 20,
        };
        assert_eq!(
            err.to_string(),
            "delta too old: requested 10, oldest available 20"
        );

        let err = GossipError::FullSyncRequired;
        assert_eq!(err.to_string(), "full sync required");

        let err = GossipError::ConnectionExists;
        assert_eq!(err.to_string(), "connection already exists");

        let err = GossipError::ActorAlreadyExists("test_actor".to_string());
        assert_eq!(err.to_string(), "actor 'test_actor' already exists");

        let err = GossipError::AskNacked(crate::framing::AskNackReason::UnknownActor);
        assert_eq!(err.to_string(), "ask was not answered: unknown actor");
    }

    #[test]
    fn test_error_conversions() {
        // Test From<io::Error>
        let io_err = io::Error::other("io error");
        let gossip_err: GossipError = io_err.into();
        match gossip_err {
            GossipError::Network(_) => (),
            _ => panic!("Expected Network error"),
        }

        // Test that error variants work correctly - using a different approach
        let timeout_err = GossipError::Timeout;
        match timeout_err {
            GossipError::Timeout => (),
            _ => panic!("Expected Timeout error"),
        }
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        match ok_result {
            Ok(value) => assert_eq!(value, 42),
            Err(_) => panic!("Expected Ok result"),
        }

        let err_result: Result<i32> = Err(GossipError::Timeout);
        assert!(err_result.is_err());
    }

    #[test]
    fn test_message_type_numeric_contract_is_stable() {
        assert_eq!(MessageType::Gossip as u8, 0);
        assert_eq!(MessageType::Ask as u8, 1);
        assert_eq!(MessageType::Response as u8, 2);
        assert_eq!(MessageType::ActorTell as u8, 3);
        assert_eq!(MessageType::ActorAsk as u8, 4);
        assert_eq!(MessageType::StreamStart as u8, 0x10);
        assert_eq!(MessageType::StreamData as u8, 0x11);
        assert_eq!(MessageType::StreamEnd as u8, 0x12);
        assert_eq!(MessageType::StreamResponseStart as u8, 0x13);
        assert_eq!(MessageType::StreamResponseData as u8, 0x14);
        assert_eq!(MessageType::StreamResponseEnd as u8, 0x15);
        assert_eq!(MessageType::DirectAsk as u8, 0x20);
        assert_eq!(MessageType::DirectResponse as u8, 0x21);
        assert_eq!(MessageType::PubSub as u8, 0x30);

        for byte in [
            0, 1, 2, 3, 4, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x20, 0x21, 0x30,
        ] {
            let parsed = MessageType::from_byte(byte).expect("known message type byte");
            assert_eq!(parsed as u8, byte);
        }
        assert!(MessageType::from_byte(0xFF).is_none());
    }

    #[tokio::test]
    async fn peer_connect_dials_even_when_tiebreak_prefers_inbound() {
        let (local_key, remote_key) = inbound_preferred_key_pair();
        let local_peer_id = local_key.peer_id();
        let remote_peer_id = remote_key.peer_id();
        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41001".parse().unwrap(),
            GossipConfig {
                key_pair: Some(local_key),
                connection_timeout: Duration::from_millis(10),
                ..GossipConfig::default()
            },
        ));
        assert!(
            !registry.should_keep_connection(&remote_peer_id, true),
            "test setup must choose local={local_peer_id} as inbound owner for remote={remote_peer_id}"
        );

        let addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let peer = Peer {
            peer_id: remote_peer_id.clone(),
            registry: registry.clone(),
        };

        let err = peer
            .connect(&addr)
            .await
            .expect_err("inbound-preferred side must still attempt the socket dial");
        assert!(matches!(err, GossipError::Network(_)));

        assert_eq!(
            registry
                .connection_pool
                .get_configured_peer_addr(&remote_peer_id),
            Some(addr)
        );
        assert!(
            registry
                .connection_pool
                .get_connection_by_peer_id(&remote_peer_id)
                .is_none(),
            "failed dial must not create a wrong-direction connection"
        );
    }

    #[tokio::test]
    async fn peer_connect_drops_existing_wrong_direction_outbound_before_redial() {
        let (local_key, remote_key) = inbound_preferred_key_pair();
        let remote_peer_id = remote_key.peer_id();
        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41003".parse().unwrap(),
            GossipConfig {
                key_pair: Some(local_key),
                connection_timeout: Duration::from_millis(10),
                ..GossipConfig::default()
            },
        ));
        assert!(!registry.should_keep_connection(&remote_peer_id, true));

        let wrong_addr: SocketAddr = "127.0.0.1:51003".parse().unwrap();
        let (io, _peer_io) = tokio::io::duplex(1024);
        let (stream_handle, _writer_task, _reader_task) =
            crate::connection_pool::LockFreeStreamHandle::new(
                io,
                wrong_addr,
                crate::connection_pool::ChannelId::Global,
                crate::connection_pool::BufferConfig::default(),
                None,
                None,
            );
        let mut connection = crate::connection_pool::LockFreeConnection::new(
            wrong_addr,
            crate::connection_pool::ConnectionDirection::Outbound,
        );
        connection.stream_handle = Some(std::sync::Arc::new(stream_handle));
        connection.set_state(crate::connection_pool::ConnectionState::Connected);
        assert!(registry.connection_pool.add_connection_by_peer_id(
            remote_peer_id.clone(),
            wrong_addr,
            std::sync::Arc::new(connection)
        ));
        assert!(
            registry
                .connection_pool
                .get_connection_by_peer_id(&remote_peer_id)
                .is_some(),
            "test must start with a live wrong-direction connection"
        );

        let peer = Peer {
            peer_id: remote_peer_id.clone(),
            registry: registry.clone(),
        };
        let err = peer
            .connect(&"127.0.0.1:9".parse().unwrap())
            .await
            .expect_err("redial to closed port should report network failure");
        assert!(matches!(err, GossipError::Network(_)));

        assert!(
            registry
                .connection_pool
                .get_connection_by_peer_id(&remote_peer_id)
                .is_none(),
            "redial path must remove a live wrong-direction outbound before dialing"
        );
    }

    #[tokio::test]
    async fn peer_connect_still_dials_when_tiebreak_prefers_outbound() {
        let (remote_key, local_key) = inbound_preferred_key_pair();
        let remote_peer_id = remote_key.peer_id();
        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41002".parse().unwrap(),
            GossipConfig {
                key_pair: Some(local_key),
                connection_timeout: Duration::from_millis(10),
                ..GossipConfig::default()
            },
        ));
        assert!(registry.should_keep_connection(&remote_peer_id, true));

        let addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let peer = Peer {
            peer_id: remote_peer_id,
            registry,
        };

        let err = peer
            .connect(&addr)
            .await
            .expect_err("outbound-preferred side should still attempt the socket dial");
        assert!(matches!(err, GossipError::Network(_)));
    }

    /// A caller-side read-then-write of the pin (`pinned_addr_for` then
    /// `set_configured_peer_addr`), however tightly held together, is
    /// still not on the same serialization as a concurrent
    /// `configure_peer`/`migrate`. Production code no longer has this
    /// shape (see `set_ordinary_connect_route`'s doc comment); this
    /// reconstructs it directly against the primitives to prove the
    /// vulnerability class.
    #[tokio::test]
    async fn a_caller_side_pin_read_then_route_write_is_vulnerable_to_an_interleaved_pin_publish()
     {
        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41010".parse().unwrap(),
            GossipConfig {
                key_pair: Some(KeyPair::new_for_testing("ordinary-connect-race-local")),
                ..GossipConfig::default()
            },
        ));
        let peer_id = KeyPair::new_for_testing("ordinary-connect-race-remote").peer_id();
        let stale_addr: SocketAddr = "127.0.0.1:41011".parse().unwrap();
        let new_pin_addr: SocketAddr = "127.0.0.1:41012".parse().unwrap();

        // The "read": no pin published yet.
        assert_eq!(registry.registry_owner.pinned_addr_for(&peer_id), None);

        // An owner command interleaves BETWEEN the read and the write
        // below, publishing a NEW pin -- exactly what a concurrent
        // `configure_peer` does.
        registry
            .configure_peer(peer_id.clone(), new_pin_addr)
            .await;
        assert_eq!(
            registry.connection_pool.get_required_peer_addr(&peer_id),
            Some(new_pin_addr)
        );

        // The stale "write" proceeds anyway, using the read from before
        // the interleaved publish -- exactly what the OLD ordinary-
        // connect path did.
        registry
            .connection_pool
            .set_configured_peer_addr(&peer_id, stale_addr);

        assert_eq!(
            registry.connection_pool.get_required_peer_addr(&peer_id),
            Some(stale_addr),
            "the stale write overwrites the newly-published pin's route anyway -- proving \
             a caller-side read-then-write, however tight, is not the same as one atomic \
             step with respect to owner commands"
        );
    }

    /// `configure_peer` must always win a concurrent race against an
    /// ordinary connect for the same peer, regardless of ordering --
    /// `set_ordinary_connect_route`'s pin-conflict check and the route
    /// write are one serialized owner step, so either the ordinary
    /// connect sees the pin already installed and declines, or its write
    /// is unconditionally overwritten by `configure_peer`'s own atomic
    /// pin-install.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_ordinary_connect_and_configure_peer_never_disagree_on_the_route() {
        for round in 0..20u16 {
            let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
                format!("127.0.0.1:{}", 41_100 + round).parse().unwrap(),
                GossipConfig {
                    key_pair: Some(KeyPair::new_for_testing(format!(
                        "concurrent-ordinary-connect-local-{round}"
                    ))),
                    connection_timeout: Duration::from_millis(10),
                    ..GossipConfig::default()
                },
            ));
            let peer_id =
                KeyPair::new_for_testing(format!("concurrent-ordinary-connect-remote-{round}"))
                    .peer_id();
            let ordinary_addr: SocketAddr =
                format!("127.0.0.1:{}", 41_200 + round).parse().unwrap();
            let pin_addr: SocketAddr = format!("127.0.0.1:{}", 41_300 + round).parse().unwrap();

            let peer = Peer {
                peer_id: peer_id.clone(),
                registry: registry.clone(),
            };
            let call_connect = tokio::spawn(async move {
                let _ = peer.connect(&ordinary_addr).await;
            });
            let registry_for_pin = registry.clone();
            let peer_id_for_pin = peer_id.clone();
            let call_configure = tokio::spawn(async move {
                registry_for_pin
                    .configure_peer(peer_id_for_pin, pin_addr)
                    .await;
            });
            call_connect.await.expect("call_connect task panicked");
            call_configure.await.expect("call_configure task panicked");

            assert_eq!(
                registry.connection_pool.get_required_peer_addr(&peer_id),
                Some(pin_addr),
                "round {round}: ConnectionPool's required route must end up exactly the pin \
                 address regardless of race ordering"
            );
            assert_eq!(
                registry.registry_owner.pinned_addr_for(&peer_id),
                Some(pin_addr),
                "round {round}: sanity -- the owner's own pin must agree"
            );
        }
    }

    /// A declined route must both (a) never let the requested address
    /// appear in `gossip_state`, and (b) still fall through to dial and
    /// report on the pinned route rather than returning `Ok(())` without
    /// trying anything: `configure_peer` pins the peer to A (unlistened),
    /// then `.connect(&B)` must surface an `Err` from actually attempting
    /// A, not a silent success.
    #[tokio::test]
    async fn ordinary_connect_falls_through_to_the_pinned_route_without_gossiping_the_declined_one()
     {
        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41013".parse().unwrap(),
            GossipConfig {
                key_pair: Some(KeyPair::new_for_testing("declined-route-local")),
                connection_timeout: Duration::from_millis(10),
                ..GossipConfig::default()
            },
        ));
        let peer_id = KeyPair::new_for_testing("declined-route-remote").peer_id();
        let pinned_addr: SocketAddr = "127.0.0.1:41014".parse().unwrap();
        let declined_addr: SocketAddr = "127.0.0.1:41015".parse().unwrap();

        registry
            .configure_peer(peer_id.clone(), pinned_addr)
            .await;

        let peer = Peer {
            peer_id: peer_id.clone(),
            registry: registry.clone(),
        };
        let err = peer
            .connect(&declined_addr)
            .await
            .expect_err(
                "a declined ordinary connect must still fall through to the pinned route and \
                 surface ITS failure -- nothing listens at the pin either, so silently \
                 returning Ok(()) here would prove the fallthrough never happened",
            );
        assert!(
            matches!(err, GossipError::Network(_)),
            "the surfaced error must come from actually attempting the pinned route, not some \
             unrelated failure -- got {err:?}"
        );

        {
            let gossip_state = registry.gossip_state.lock().await;
            assert!(
                !gossip_state.peers.contains_key(&declined_addr),
                "an address the owner declined to route to must never be inserted into \
                 gossip_state at all -- present-but-unhealthy would still be gossiped as a \
                 known peer address, which is exactly the original bug: advertising a route \
                 this node never actually connected to"
            );
        }
        assert_eq!(
            registry.connection_pool.get_required_peer_addr(&peer_id),
            Some(pinned_addr),
            "sanity: the pin's route must still be the only one ConnectionPool reports"
        );
    }

    /// A concurrent `configure_peer` pin must not leave a gossip_state
    /// entry for the address it raced out, even though nothing here
    /// listens on either address (every dial genuinely fails, so the
    /// property under test is entry attribution, not dial success). B's
    /// route write can be accepted then overridden by the pin, or
    /// declined outright, depending on which owner command the race
    /// resolves first -- and when B's route IS accepted first, the
    /// connect task genuinely dials B, gets an immediate
    /// connection-refused, and a `PeerInfo` entry for B is the correct,
    /// expected record of that real attempt, not a bug. So the property
    /// under test is not "B never gains an entry" (which depends on
    /// scheduler timing this test cannot control), but "an entry for B
    /// only exists because B was actually the resolved, attempted
    /// address" -- verified directly against the `AttemptedRoute`
    /// `connect_to_peer_with_outcome` itself resolves and dials against,
    /// not inferred from which address happened to win the race. Run
    /// over many rounds to cover every ordering the owner's serialization
    /// can produce.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ordinary_connect_never_inserts_a_gossip_state_entry_for_an_address_it_did_not_dial()
     {
        for round in 0..20u16 {
            let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
                format!("127.0.0.1:{}", 41_400 + round).parse().unwrap(),
                GossipConfig {
                    key_pair: Some(KeyPair::new_for_testing(format!(
                        "insert-attribution-local-{round}"
                    ))),
                    connection_timeout: Duration::from_millis(10),
                    ..GossipConfig::default()
                },
            ));
            let peer_id =
                KeyPair::new_for_testing(format!("insert-attribution-remote-{round}")).peer_id();
            let addr_b: SocketAddr = format!("127.0.0.1:{}", 41_500 + round).parse().unwrap();
            let addr_a: SocketAddr = format!("127.0.0.1:{}", 41_600 + round).parse().unwrap();

            let registry_for_connect = registry.clone();
            let peer_id_for_connect = peer_id.clone();
            let call_connect = tokio::spawn(async move {
                // Mirrors `Peer::connect`'s own required-peer path exactly
                // (route write, then dial), just keeping the
                // `AttemptedRoute` that path would otherwise discard, so
                // this test can check the actual fact instead of an
                // assumption about which address the race resolved to.
                let _ = registry_for_connect
                    .registry_owner
                    .set_ordinary_connect_route(peer_id_for_connect.clone(), addr_b)
                    .await;
                registry_for_connect
                    .connect_to_peer_with_outcome(&peer_id_for_connect)
                    .await
                    .0
            });
            let registry_for_pin = registry.clone();
            let peer_id_for_pin = peer_id.clone();
            let call_configure = tokio::spawn(async move {
                registry_for_pin
                    .configure_peer(peer_id_for_pin, addr_a)
                    .await;
            });
            let attempted = call_connect.await.expect("call_connect task panicked");
            call_configure.await.expect("call_configure task panicked");

            let gossip_state = registry.gossip_state.lock().await;
            if gossip_state.peers.contains_key(&addr_b) {
                assert_eq!(
                    attempted.map(|route| route.addr()),
                    Some(addr_b),
                    "round {round}: B must never gain a gossip_state entry unless B was \
                     actually the resolved, attempted address -- got {:?}",
                    gossip_state.peers.get(&addr_b)
                );
            }
        }
    }

    #[tokio::test]
    async fn peer_connect_refuses_self_peer_without_configuring_or_dialing() {
        let local_key = KeyPair::new_for_testing("self-peer-connect-guard");
        let local_peer_id = local_key.peer_id();
        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41004".parse().unwrap(),
            GossipConfig {
                key_pair: Some(local_key),
                connection_timeout: Duration::from_millis(10),
                ..GossipConfig::default()
            },
        ));
        let peer = Peer {
            peer_id: local_peer_id.clone(),
            registry: registry.clone(),
        };

        peer.connect(&registry.bind_addr)
            .await
            .expect("self peer connect should be a harmless no-op");

        assert_eq!(
            registry
                .connection_pool
                .get_configured_peer_addr(&local_peer_id),
            None,
            "self peers must not be configured as remote dial targets"
        );
        assert!(
            registry
                .connection_pool
                .get_connection_by_peer_id(&local_peer_id)
                .is_none(),
            "self peers must not create pooled remote connections"
        );
        let gossip_state = registry.gossip_state.lock().await;
        assert!(
            !gossip_state.peers.contains_key(&registry.bind_addr),
            "self peers must not enter gossip peer state"
        );
    }

    /// `get_connection` can resolve an already-published connection for
    /// the same peer identity at a different address (`A`) than requested
    /// (`B`) -- the tie-break reuses an existing outbound connection
    /// before dialing. The insert/mark must key to `effective_addr`
    /// (`A`), never the bare request, or a stale/malicious discovery hint
    /// could be marked healthy and gossiped without ever being dialed.
    /// Reproduced deterministically by holding `gossip_state`'s lock
    /// across `connect_discovered`'s own identity-resolution window and
    /// establishing the peer's one connection at `A` first; the key pair
    /// is chosen so `should_keep_connection` favors keeping it over
    /// dialing `B`.
    #[tokio::test]
    async fn connect_discovered_marks_the_address_actually_resolved_not_the_bare_request() {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        let (local_key, remote_key) = outbound_reuse_key_pair();
        let peer_id = remote_key.peer_id();

        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41010".parse().unwrap(),
            GossipConfig {
                key_pair: Some(local_key),
                connection_timeout: Duration::from_millis(200),
                ..GossipConfig::default()
            },
        ));
        // `get_connection`'s identity-resolution fallback (`lookup_node_id`)
        // upgrades a weak back-reference to the owning registry; without
        // wiring it, that resolution silently no-ops and this call would
        // fall straight through to a real dial instead of exercising the
        // reuse path under test.
        registry.connection_pool.set_registry(registry.clone());

        // A -- where the peer's only connection actually lives.
        let existing_addr: SocketAddr = "127.0.0.1:41011".parse().unwrap();
        // B -- a discovery hint this call is told to reach; never itself
        // dialed, since the tie-break below resolves via `A` first.
        let requested_addr: SocketAddr = "127.0.0.1:41012".parse().unwrap();

        let peer = Peer {
            peer_id: peer_id.clone(),
            registry: registry.clone(),
        };

        // Hold `gossip_state`'s lock across the window `connect_discovered`'s
        // own `get_connection` call needs it, inside `lookup_node_id`'s
        // identity resolution.
        let guard = registry.gossip_state.lock().await;

        let connect_task = {
            let peer = peer.clone();
            tokio::spawn(async move { peer.connect_discovered(&requested_addr).await })
        };

        // Let the spawned task run until it genuinely blocks on the lock
        // this test still holds -- deterministic on the current-thread
        // runtime `#[tokio::test]` defaults to: nothing else can make
        // progress until this task yields, and there is no `.await` point
        // between `connect_discovered`'s entry and that lock acquisition
        // for it to have reached instead.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Establish peer_id's ONLY connection -- an OUTBOUND connection at
        // `A`, never at `B` -- while the discovered call is provably
        // blocked before it can observe it.
        let (io, _peer_io) = tokio::io::duplex(1024);
        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            existing_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(existing_addr, ConnectionDirection::Outbound);
        conn.stream_handle = Some(std::sync::Arc::new(stream_handle));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = std::sync::Arc::new(conn);
        registry.connection_pool.add_connection_by_peer_id(
            peer_id.clone(),
            existing_addr,
            conn.clone(),
        );

        drop(guard);

        connect_task
            .await
            .expect("connect_discovered task must not panic")
            .expect("must resolve the peer's existing outbound connection without a real dial");

        let gossip_state = registry.gossip_state.lock().await;
        assert!(
            gossip_state
                .peers
                .get(&requested_addr)
                .is_none_or(|info| info.last_success == 0),
            "the requested address must never be marked healthy/gossipable -- this call \
             never actually verified anything about it, it reused a pre-existing outbound \
             connection at a completely different address instead: {:?}",
            gossip_state.peers.get(&requested_addr)
        );
        let existing_entry = gossip_state
            .peers
            .get(&existing_addr)
            .expect("the address actually resolved must gain a gossip_state entry");
        assert!(
            existing_entry.last_success > 0,
            "the address ACTUALLY resolved must be the one marked healthy"
        );
    }

    /// If `get_connection` reuses an INBOUND connection for the identity
    /// (tie-break flipped from the outbound case above), `conn.addr` is
    /// that connection's raw, ephemeral transport source, not a
    /// corroborated dial target. Enforced at the type level, not this
    /// call site: `ResolvedRoute`'s only constructor requires the
    /// connection's independently-looked-up direction, so an inbound
    /// source can only ever produce a route flagged unverified, and every
    /// `PeerInfo` builder reads that flag directly off it.
    #[tokio::test]
    async fn connect_discovered_reusing_an_inbound_connection_does_not_mark_the_ephemeral_source_healthy()
     {
        use crate::connection_pool::{
            BufferConfig, ChannelId, ConnectionDirection, ConnectionState, LockFreeConnection,
            LockFreeStreamHandle,
        };

        // Flipped from `outbound_reuse_key_pair`: `should_keep_connection`
        // keeps an existing INBOUND connection only when THIS side's
        // NodeId sorts ABOVE the peer's (see its own doc comment: `Greater
        // => !is_outbound`, and `is_outbound` is `false` for an inbound
        // existing connection).
        let (local_key, remote_key) = inbound_preferred_key_pair();
        let peer_id = remote_key.peer_id();

        let registry = std::sync::Arc::new(registry::GossipRegistry::<()>::new(
            "127.0.0.1:41020".parse().unwrap(),
            GossipConfig {
                key_pair: Some(local_key),
                connection_timeout: Duration::from_millis(200),
                enable_peer_discovery: true,
                allow_loopback_discovery: true,
                ..GossipConfig::default()
            },
        ));
        registry.connection_pool.set_registry(registry.clone());

        // A -- the peer's only connection, INBOUND: its own address is a
        // raw, ephemeral transport source, never a corroborated dial
        // target.
        let existing_addr: SocketAddr = "127.0.0.1:41021".parse().unwrap();
        // B -- a discovery hint this call is told to reach; never itself
        // dialed, since the tie-break below resolves via `A` first.
        let requested_addr: SocketAddr = "127.0.0.1:41022".parse().unwrap();

        let peer = Peer {
            peer_id: peer_id.clone(),
            registry: registry.clone(),
        };

        let guard = registry.gossip_state.lock().await;

        let connect_task = {
            let peer = peer.clone();
            tokio::spawn(async move { peer.connect_discovered(&requested_addr).await })
        };

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let (io, _peer_io) = tokio::io::duplex(1024);
        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            existing_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(existing_addr, ConnectionDirection::Inbound);
        conn.stream_handle = Some(std::sync::Arc::new(stream_handle));
        conn.embedded_peer_id = Some(peer_id.clone());
        conn.set_state(ConnectionState::Connected);
        let conn = std::sync::Arc::new(conn);
        registry.connection_pool.add_connection_by_peer_id(
            peer_id.clone(),
            existing_addr,
            conn.clone(),
        );

        drop(guard);

        connect_task
            .await
            .expect("connect_discovered task must not panic")
            .expect("must resolve the peer's existing inbound connection without a real dial");

        let gossip_state = registry.gossip_state.lock().await;
        assert!(
            gossip_state
                .peers
                .get(&requested_addr)
                .is_none_or(|info| info.last_success == 0),
            "the requested address must never be marked healthy/gossipable: {:?}",
            gossip_state.peers.get(&requested_addr)
        );
        let existing_entry = gossip_state
            .peers
            .get(&existing_addr)
            .expect("the address actually resolved must gain a gossip_state entry");
        assert!(
            existing_entry.transport_source_keyed,
            "an inbound connection's raw, ephemeral source must never be recorded as an \
             ordinary, dialable route -- the flag connect_to_peer already preserves for its \
             own inbound-reuse case must carry through the discovered path too"
        );
        assert!(
            existing_entry.inbound_observed,
            "the entry must also carry the inbound-observed evidence, not just the \
             transport-source flag"
        );
        // `select_best_alias_per_identity` still selects a
        // `transport_source_keyed` alias when it's the only live one for
        // its identity (silencing it entirely would be worse); the flag's
        // protective effect is in ranking, always losing to a dialable
        // alias once one exists -- covered separately by
        // `gossip_peer_list_target_selection_prefers_live_alias_over_stale`
        // in registry.rs. This test's scope is the flag reaching the entry
        // at all.
    }

    /// `(local, remote)` such that `local`'s `NodeId` sorts strictly below
    /// `remote`'s -- the ordering `should_keep_connection` uses to decide
    /// this side should KEEP an existing outbound connection to `remote`
    /// rather than treat it as wrong-direction and evict it. See
    /// `inbound_preferred_key_pair` below for the opposite ordering.
    fn outbound_reuse_key_pair() -> (KeyPair, KeyPair) {
        let first = KeyPair::new_for_testing("outbound-reuse-key-a");
        let second = KeyPair::new_for_testing("outbound-reuse-key-b");
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

    fn inbound_preferred_key_pair() -> (KeyPair, KeyPair) {
        let first = KeyPair::new_for_testing("collision-key-a");
        let second = KeyPair::new_for_testing("collision-key-b");
        if first
            .peer_id()
            .to_node_id()
            .as_bytes()
            .cmp(second.peer_id().to_node_id().as_bytes())
            .is_gt()
        {
            (first, second)
        } else {
            (second, first)
        }
    }
}
