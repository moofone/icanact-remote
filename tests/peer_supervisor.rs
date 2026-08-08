//! Phase 0: p2p configured-peer supervisor.
//!
//! The supervisor must keep a *direct* connection to every `configure_peer`d
//! (required) peer alive and surface a prompt liveness signal when one is down —
//! point-to-point only, riding the connection pool, never gossip/broadcast.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use icanact_remote::registry::PeerLivenessHandler;
use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId, SecretKey,
};

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<(PeerId, bool)>>,
}

impl PeerLivenessHandler for RecordingSink {
    fn handle_peer_liveness(
        &self,
        peer_id: PeerId,
        _addr: SocketAddr,
        reachable: bool,
        _reason: String,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.events.lock().unwrap().push((peer_id, reachable));
        })
    }
}

/// A configured peer whose service is down must be dialed and reported
/// unreachable on the very first supervisor tick (~prompt), and the
/// edge-triggered handler must fire exactly once per state change (continuous
/// alerting is via the per-tick log, not a handler flood).
#[tokio::test]
async fn supervisor_reports_unreachable_for_down_peer() -> Result<(), Box<dyn std::error::Error>> {
    let registry = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        SecretKey::generate(),
        Some(GossipConfig::default()),
        BuilderTlsBootstrap,
    )
    .await?;

    // A dead address: grab a free port then drop the socket so nothing listens.
    let dead_addr: SocketAddr = {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = sock.local_addr()?;
        drop(sock);
        addr
    };
    let peer_id = KeyPair::generate().peer_id();

    let sink = Arc::new(RecordingSink::default());
    registry
        .registry
        .set_peer_liveness_handler(sink.clone())
        .await;
    let _ = registry
        .registry
        .configure_peer(peer_id.clone(), dead_addr)
        .await;

    // One tick: must attempt the direct dial, fail, and emit unreachable.
    registry.registry.supervise_configured_peers().await;
    {
        let ev = sink.events.lock().unwrap();
        assert_eq!(
            ev.len(),
            1,
            "expected exactly one liveness edge, got {ev:?}"
        );
        assert_eq!(
            ev[0],
            (peer_id.clone(), false),
            "expected unreachable=false"
        );
    }

    // A second tick while still down must NOT re-fire the handler — it is
    // edge-triggered, not per-tick.
    registry.registry.supervise_configured_peers().await;
    assert_eq!(
        sink.events.lock().unwrap().len(),
        1,
        "handler must be edge-triggered (one event per state change)"
    );

    Ok(())
}

/// With no configured peers, a supervisor tick is a no-op: it dials nothing and
/// emits nothing (no traffic, no signal, no storm).
#[tokio::test]
async fn supervisor_is_a_noop_without_configured_peers() -> Result<(), Box<dyn std::error::Error>> {
    let registry = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        SecretKey::generate(),
        Some(GossipConfig::default()),
        BuilderTlsBootstrap,
    )
    .await?;

    let sink = Arc::new(RecordingSink::default());
    registry
        .registry
        .set_peer_liveness_handler(sink.clone())
        .await;

    registry.registry.supervise_configured_peers().await;

    assert!(
        sink.events.lock().unwrap().is_empty(),
        "no configured peers must produce no liveness events"
    );
    Ok(())
}
