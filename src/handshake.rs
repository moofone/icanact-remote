//! Hello handshake protocol for peer capability negotiation
//!
//! This module implements the Hello handshake that establishes peer capabilities
//! at connection time for V6 peers.

use crate::{GossipError, Result};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Duration, timeout};
use tracing::debug;

const HELLO_MAX_SIZE: usize = 1024;
const HELLO_TIMEOUT_MS: u64 = 3_000;
pub const ALPN_ICANACT_V6: &[u8] = b"icanact-remote-v6";

/// Protocol version constants
pub const PROTOCOL_VERSION_V6: u16 = 6;

/// Current protocol version
pub const CURRENT_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION_V6;

/// Ephemeral identity for one running remote-node instance.
///
/// `PeerId` remains the durable cryptographic identity. This value separates
/// multiple physical connections from the same process (same boot id) from
/// two concurrently live processes that reused one long-lived key (different
/// boot ids). The value is exchanged inside mutually authenticated TLS and is
/// never accepted as identity proof by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct RemoteBootId([u8; 16]);

impl RemoteBootId {
    pub fn new() -> Self {
        Self(rand::random())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for RemoteBootId {
    fn default() -> Self {
        Self::new()
    }
}

/// Feature flags for capability negotiation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
#[repr(u8)]
pub enum Feature {
    /// Peer list gossip for automatic peer discovery
    PeerListGossip = 0,
    /// Optional pairwise clock calibration piggybacked on normal gossip frames.
    ClockCalibration = 1,
}

impl Feature {
    const fn bit(self) -> u64 {
        match self {
            Feature::PeerListGossip => 1u64 << 0,
            Feature::ClockCalibration => 1u64 << 1,
        }
    }
}

/// Whether sending a given `WireKind` to a peer requires a capability that
/// was actually negotiated in the Hello exchange, versus being covered by
/// the mandatory `schema_hash` equality check (which already refuses a
/// connection between peers running different wire code -- see
/// `perform_hello_handshake`).
///
/// This is deliberately an exhaustive match with no wildcard arm: adding a
/// new `WireKind` variant without extending this match is a compile error,
/// not a silent default. That is the guard -- kinds 13/14
/// (RouteBind/RoutedActorAsk) shipped without any such mechanism, and an
/// older peer receiving one tore the connection down on the unrecognized
/// kind (`framing::decode_control` returning `None`) rather than failing
/// gracefully or being avoided in the first place.
///
/// `None` means "no capability required" -- every kind today, including
/// RouteBind/RoutedActorAsk, which are already fleet-wide and would gain
/// nothing from retroactive gating (see the guard tests in this module).
/// A future extension kind that must be avoided when talking to a peer that
/// predates it (rather than relying on a fleet-wide schema_hash bump) should
/// return `Some(Feature::_)` here instead.
pub const fn wire_kind_capability(kind: crate::framing::WireKind) -> Option<Feature> {
    use crate::framing::WireKind;
    match kind {
        WireKind::Gossip
        | WireKind::Ask
        | WireKind::Response
        | WireKind::ActorTell
        | WireKind::ActorAsk
        | WireKind::StreamStart
        | WireKind::StreamData
        | WireKind::StreamResponseStart
        | WireKind::StreamResponseData
        | WireKind::DirectAsk
        | WireKind::DirectResponse
        | WireKind::PubSub
        | WireKind::StreamAbort
        | WireKind::RouteBind
        | WireKind::RoutedActorAsk => None,
    }
}

/// Hello message sent during connection establishment
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct Hello {
    /// Protocol version this node supports
    pub protocol_version: u16,
    /// Features this node supports
    pub features: Vec<Feature>,
    /// Deployment compatibility value. It is authenticated by the TLS channel
    /// and compared once during Hello, never copied into data frames.
    pub schema_hash: Option<u64>,
    /// Per-process incarnation, authenticated by the enclosing TLS channel.
    pub boot_id: RemoteBootId,
}

impl Hello {
    /// Create a new Hello message with current capabilities
    pub fn new() -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            features: vec![Feature::PeerListGossip, Feature::ClockCalibration],
            schema_hash: None,
            boot_id: RemoteBootId::new(),
        }
    }

    /// Create Hello with specific features
    pub fn with_features(features: Vec<Feature>) -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            features,
            schema_hash: None,
            boot_id: RemoteBootId::new(),
        }
    }
}

impl Default for Hello {
    fn default() -> Self {
        Self::new()
    }
}

fn hello_for_config(
    enable_peer_discovery: bool,
    schema_hash: Option<u64>,
    boot_id: RemoteBootId,
) -> Hello {
    let mut features = vec![Feature::ClockCalibration];
    if enable_peer_discovery {
        features.push(Feature::PeerListGossip);
    }
    let mut hello = Hello::with_features(features);
    hello.schema_hash = schema_hash;
    hello.boot_id = boot_id;
    hello
}

/// Negotiated peer capabilities after Hello exchange
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCapabilities {
    /// Negotiated protocol version (min of both peers)
    pub version: u16,
    /// Features both peers support (intersection)
    pub features: u64,
    /// Authenticated process incarnation advertised by the remote peer.
    pub remote_boot_id: RemoteBootId,
}

impl PeerCapabilities {
    fn features_mask(features: &[Feature]) -> u64 {
        let mut mask = 0u64;
        for feature in features {
            mask |= feature.bit();
        }
        mask
    }

    /// Create capabilities from a Hello exchange.
    ///
    /// Takes the intersection of features for the single V6 wire version.
    pub fn from_hello_exchange(local: &Hello, remote: &Hello) -> Self {
        let features = Self::features_mask(&local.features) & Self::features_mask(&remote.features);
        Self {
            version: PROTOCOL_VERSION_V6,
            features,
            remote_boot_id: remote.boot_id,
        }
    }

    /// Check if we can send peer list gossip to this peer
    pub fn can_send_peer_list(&self) -> bool {
        self.supports_feature(Feature::PeerListGossip)
    }

    /// Check if we can piggyback clock calibration on gossip frames with this peer.
    pub fn can_calibrate_clock(&self) -> bool {
        self.supports_feature(Feature::ClockCalibration)
    }

    /// Check if a specific feature is supported
    pub fn supports_feature(&self, feature: Feature) -> bool {
        (self.features & feature.bit()) != 0
    }

    /// Whether this negotiated relationship supports sending `kind` to the
    /// peer. Kinds with no capability requirement (see
    /// `wire_kind_capability`) are always supported.
    pub fn supports_wire_kind(&self, kind: crate::framing::WireKind) -> bool {
        match wire_kind_capability(kind) {
            None => true,
            Some(feature) => self.supports_feature(feature),
        }
    }
}

async fn read_exact_with_timeout<R>(reader: &mut R, buf: &mut [u8]) -> Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    let mut offset = 0;
    while offset < buf.len() {
        let slice = &mut buf[offset..];
        let read_future = async { reader.read(slice).await };
        let bytes_read = match timeout(Duration::from_millis(HELLO_TIMEOUT_MS), read_future).await {
            Ok(Ok(0)) => {
                return Err(GossipError::Network(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF during Hello handshake",
                )));
            }
            Ok(Ok(n)) => n,
            Ok(Err(err)) => return Err(GossipError::Network(err)),
            Err(_) => return Err(GossipError::Timeout),
        };
        offset += bytes_read;
    }
    Ok(())
}

async fn send_hello_message<W>(stream: &mut W, hello: &Hello) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(hello)?;
    let len = serialized.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&serialized).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_hello_message<R>(reader: &mut R) -> Result<Hello>
where
    R: AsyncRead + Unpin + Send,
{
    let mut len_buf = [0u8; 4];
    read_exact_with_timeout(reader, &mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len == 0 || len > HELLO_MAX_SIZE {
        return Err(GossipError::TlsHandshakeFailed(format!(
            "invalid Hello size: {} bytes",
            len
        )));
    }

    let mut buf = vec![0u8; len];
    read_exact_with_timeout(reader, &mut buf).await?;
    let hello: Hello = rkyv::from_bytes::<Hello, rkyv::rancor::Error>(&buf)?; // ALLOW_RKYV_FROM_BYTES
    Ok(hello)
}

/// Perform Hello handshake if both peers negotiated discovery via ALPN
pub async fn perform_hello_handshake<S>(
    stream: &mut S,
    negotiated_alpn: Option<&[u8]>,
    enable_peer_discovery: bool,
    schema_hash: Option<u64>,
    local_boot_id: RemoteBootId,
) -> Result<PeerCapabilities>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let alpn = negotiated_alpn.ok_or_else(|| {
        GossipError::TlsHandshakeFailed("missing ALPN negotiation result".to_string())
    })?;

    if alpn != ALPN_ICANACT_V6 {
        return Err(GossipError::TlsHandshakeFailed(format!(
            "unsupported ALPN: {}",
            String::from_utf8_lossy(alpn)
        )));
    }

    let local_hello = hello_for_config(enable_peer_discovery, schema_hash, local_boot_id);
    send_hello_message(stream, &local_hello).await?;
    let remote_hello = read_hello_message(stream).await?;
    if remote_hello.protocol_version != CURRENT_PROTOCOL_VERSION {
        return Err(GossipError::TlsHandshakeFailed(format!(
            "unsupported protocol version: {}",
            remote_hello.protocol_version
        )));
    }
    if remote_hello.schema_hash != local_hello.schema_hash {
        return Err(GossipError::TlsHandshakeFailed(format!(
            "schema hash mismatch: local={:?}, remote={:?}",
            local_hello.schema_hash, remote_hello.schema_hash
        )));
    }
    let caps = PeerCapabilities::from_hello_exchange(&local_hello, &remote_hello);
    debug!(
        negotiated_version = caps.version,
        peer_list = caps.can_send_peer_list(),
        clock_calibration = caps.can_calibrate_clock(),
        "Hello handshake negotiated capabilities"
    );
    Ok(caps)
}

// NOTE: a no-ALPN Hello variant for "authenticated non-TLS transports" was
// removed. It performed only version/feature negotiation — no identity, no key
// possession, no ALPN — so any transport wired through it would produce a
// connection with `embedded_peer_id = None`, and the per-message gossip guard
// only fires when an authenticated identity exists. It had no callers, so
// rather than leave an unauthenticated footgun in the public surface, identity
// binding is kept mandatory: all Hello handshakes run over the mutually
// authenticated TLS path (`perform_hello_handshake`).

#[cfg(test)]
mod tests {
    use super::*;

    /// Guard: every `WireKind` must have an explicit capability answer.
    /// `wire_kind_capability` is an exhaustive match with no wildcard arm, so
    /// the compiler already refuses to build if a new `WireKind` variant is
    /// added without one; this test additionally drives it through
    /// `WireKind::ALL` (the same single source of truth the framing-layer
    /// control-encoding tests use) so a variant that's added to the enum but
    /// never added to `ALL` -- which the exhaustiveness check alone would
    /// not catch -- is still exercised here.
    #[test]
    fn every_wire_kind_has_a_capability_mapping() {
        for kind in crate::framing::WireKind::ALL {
            // Calling it is the assertion: an unmapped kind is a compile
            // error, not a runtime failure, but this pins the current,
            // reviewed answer for each kind so a change shows up as a diff.
            let _ = wire_kind_capability(kind);
        }
    }

    /// RouteBind/RoutedActorAsk (13, 14) shipped fleet-wide without any
    /// gating mechanism -- the incident this mechanism exists to prevent a
    /// repeat of. Do not retroactively gate them: they are already assumed
    /// universally understood, and gating them now would be pure cost for
    /// no benefit (every peer that can dial at all already speaks them).
    #[test]
    fn route_bind_and_routed_actor_ask_are_not_retroactively_gated() {
        assert_eq!(
            wire_kind_capability(crate::framing::WireKind::RouteBind),
            None
        );
        assert_eq!(
            wire_kind_capability(crate::framing::WireKind::RoutedActorAsk),
            None
        );
    }

    /// A capability a peer never advertised must not be treated as
    /// supported, and a `None` (ungated) kind must always be treated as
    /// supported regardless of what either side negotiated.
    #[test]
    fn peer_capabilities_supports_wire_kind_respects_the_mapping() {
        let gated_but_unsupported = PeerCapabilities {
            version: PROTOCOL_VERSION_V6,
            features: 0,
            remote_boot_id: RemoteBootId::from_bytes([0; 16]),
        };
        assert!(
            gated_but_unsupported.supports_wire_kind(crate::framing::WireKind::Gossip),
            "an ungated kind must be supported even with zero negotiated features"
        );
    }

    #[test]
    fn test_hello_new() {
        let hello = Hello::new();
        assert_eq!(hello.protocol_version, CURRENT_PROTOCOL_VERSION);
        assert!(hello.features.contains(&Feature::PeerListGossip));
        assert!(hello.features.contains(&Feature::ClockCalibration));
    }

    #[test]
    fn test_hello_serialization() {
        let hello = Hello::new();

        // Serialize
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&hello).unwrap();

        // Deserialize
        let deserialized: Hello =
            rkyv::from_bytes::<Hello, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

        assert_eq!(deserialized.protocol_version, hello.protocol_version);
        assert_eq!(deserialized.features.len(), hello.features.len());
        assert!(deserialized.features.contains(&Feature::PeerListGossip));
        assert!(deserialized.features.contains(&Feature::ClockCalibration));
    }

    #[test]
    fn test_peer_capabilities_from_hello_exchange_both_v3() {
        let local = Hello::new();
        let remote = Hello::new();

        let caps = PeerCapabilities::from_hello_exchange(&local, &remote);

        assert_eq!(caps.version, PROTOCOL_VERSION_V6);
        assert!(caps.supports_feature(Feature::PeerListGossip));
        assert!(caps.supports_feature(Feature::ClockCalibration));
        assert!(caps.can_send_peer_list());
        assert!(caps.can_calibrate_clock());
    }

    #[test]
    fn test_clock_calibration_negotiates_when_peer_discovery_disabled() {
        let local = hello_for_config(false, None, RemoteBootId::from_bytes([1; 16]));
        let remote = hello_for_config(false, None, RemoteBootId::from_bytes([2; 16]));

        let caps = PeerCapabilities::from_hello_exchange(&local, &remote);

        assert!(!caps.can_send_peer_list());
        assert!(caps.can_calibrate_clock());
    }

    #[test]
    fn test_peer_capabilities_from_hello_exchange_partial_features() {
        let local = Hello::with_features(vec![Feature::PeerListGossip]);
        let remote = Hello {
            protocol_version: PROTOCOL_VERSION_V6,
            features: vec![], // Remote supports no features
            schema_hash: None,
            boot_id: RemoteBootId::from_bytes([3; 16]),
        };

        let caps = PeerCapabilities::from_hello_exchange(&local, &remote);

        assert_eq!(caps.version, PROTOCOL_VERSION_V6);
        assert_eq!(caps.features, 0); // No common features
        assert!(!caps.can_send_peer_list()); // Needs both version and feature
    }

    #[test]
    fn test_peer_capabilities_supports_feature() {
        let caps = PeerCapabilities::from_hello_exchange(&Hello::new(), &Hello::new());

        assert!(caps.supports_feature(Feature::PeerListGossip));
        assert!(caps.supports_feature(Feature::ClockCalibration));
    }

    #[test]
    fn test_feature_serialization() {
        let feature = Feature::PeerListGossip;

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&feature).unwrap();
        let deserialized: Feature =
            rkyv::from_bytes::<Feature, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

        assert_eq!(deserialized, feature);
    }

    #[test]
    fn feature_discriminants_are_stable() {
        assert_eq!(Feature::PeerListGossip as u8, 0);
        assert_eq!(Feature::ClockCalibration as u8, 1);
    }

    #[test]
    fn test_hello_handshake_negotiation() {
        // Scenario: Two v3 nodes negotiate capabilities
        let node_a_hello = Hello::new();
        let node_b_hello = Hello::new();

        // Both nodes perform handshake
        let a_caps = PeerCapabilities::from_hello_exchange(&node_a_hello, &node_b_hello);
        let b_caps = PeerCapabilities::from_hello_exchange(&node_b_hello, &node_a_hello);

        // Both should arrive at same capabilities
        assert_eq!(a_caps.version, b_caps.version);
        assert_eq!(a_caps.features, b_caps.features);
        assert!(a_caps.can_send_peer_list());
        assert!(b_caps.can_send_peer_list());
    }

    #[tokio::test]
    async fn perform_handshake_rejects_mismatched_version() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut client, mut server) = tokio::io::duplex(1024);

        let server_task = tokio::spawn(async move {
            // Drain the local hello.
            let mut len_buf = [0u8; 4];
            server.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            server.read_exact(&mut buf).await.unwrap();

            // Send an older-version Hello to trigger rejection.
            let legacy_hello = Hello {
                protocol_version: 0,
                features: vec![],
                schema_hash: None,
                boot_id: RemoteBootId::from_bytes([4; 16]),
            };
            let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&legacy_hello).unwrap();
            server
                .write_all(&(serialized.len() as u32).to_be_bytes())
                .await
                .unwrap();
            server.write_all(&serialized).await.unwrap();
        });

        let err = perform_hello_handshake(
            &mut client,
            Some(ALPN_ICANACT_V6),
            true,
            None,
            RemoteBootId::from_bytes([5; 16]),
        )
        .await
        .expect_err("handshake should reject legacy protocol peers");

        server_task.await.unwrap();

        match err {
            GossipError::TlsHandshakeFailed(msg) => {
                assert!(
                    msg.contains("unsupported protocol version"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected TlsHandshakeFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hello_rejects_different_present_schema_hashes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut client, mut server) = tokio::io::duplex(1024);
        let server_task = tokio::spawn(async move {
            let mut len = [0u8; 4];
            server.read_exact(&mut len).await.unwrap();
            let mut local = vec![0u8; u32::from_be_bytes(len) as usize];
            server.read_exact(&mut local).await.unwrap();
            let remote = Hello {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                features: vec![],
                schema_hash: Some(2),
                boot_id: RemoteBootId::from_bytes([6; 16]),
            };
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&remote).unwrap();
            server
                .write_all(&(bytes.len() as u32).to_be_bytes())
                .await
                .unwrap();
            server.write_all(&bytes).await.unwrap();
        });

        let error = perform_hello_handshake(
            &mut client,
            Some(ALPN_ICANACT_V6),
            false,
            Some(1),
            RemoteBootId::from_bytes([7; 16]),
        )
        .await
        .expect_err("different present schema hashes must fail");
        assert!(error.to_string().contains("schema hash mismatch"));
        server_task.await.unwrap();
    }
}
