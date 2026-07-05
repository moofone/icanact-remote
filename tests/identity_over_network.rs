//! PEER_ID_REFACTOR headline coverage (spec/PEER_ID_REFACTOR.md §1, T2, T6):
//! a peer IS its cryptographic key. Routing and connection acceptance are
//! identity decisions; the gossiped address is best-effort decoration.
//!
//! - T2: an actor whose stored address is undialable is still fully
//!   reachable through its owning peer's live connection, because the send
//!   path (`GossipRegistryHandle::lookup`) routes remote actors by
//!   `location.peer_id`, never by `.address`.
//! - T6 (row 8) / §1.3: a node configured with a peer's KEY but a wrong,
//!   unreachable dial address is still fully functional the moment that
//!   peer dials US — inbound connections are accepted by key, with no
//!   requirement that we recognize the source address.

use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority,
};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::time::Duration;
use tokio::time::{Instant, sleep};

fn reserve_free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn test_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(50),
        cleanup_interval: Duration::from_millis(200),
        peer_retry_interval: Duration::from_millis(50),
        peer_supervisor_interval: Duration::from_millis(50),
        immediate_propagation_enabled: true,
        ..Default::default()
    }
}

async fn start_node(
    addr: SocketAddr,
    keypair: &KeyPair,
    config: GossipConfig,
) -> icanact_remote::Result<GossipRegistryHandle<BuilderTlsBootstrap>> {
    icanact_remote::tls::ensure_crypto_provider();
    GossipRegistryHandle::new_with_transport_stack(
        addr,
        keypair.to_secret_key(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await
}

/// Returns `(dialer, listener)` keypairs ordered so the DIALER is the
/// tie-break-preferred outbound side (`should_keep_connection` keeps the
/// connection dialed by the lower node id), making the connect direction
/// in these tests deterministic rather than racing the tie-break.
fn keys_ordered_dialer_first(seed_a: &str, seed_b: &str) -> (KeyPair, KeyPair) {
    let ka = KeyPair::new_for_testing(seed_a);
    let kb = KeyPair::new_for_testing(seed_b);
    let a_id = ka.peer_id().to_node_id();
    let b_id = kb.peer_id().to_node_id();
    if a_id.as_bytes() < b_id.as_bytes() {
        (ka, kb)
    } else {
        (kb, ka)
    }
}

async fn wait_for_pair_connection(
    a: &GossipRegistryHandle<BuilderTlsBootstrap>,
    b: &GossipRegistryHandle<BuilderTlsBootstrap>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if a.registry.has_connection_to_peer(&b.registry.peer_id).await
            || b.registry.has_connection_to_peer(&a.registry.peer_id).await
        {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

/// T2: node A advertises an actor at `0.0.0.0:0` — wildcard IP AND port 0,
/// the most undialable address expressible. Node B must (a) store the
/// location anyway (WP1 never-drop: the owner's IP is repaired from the
/// verified source, port 0 is preserved because the sender's source port is
/// ephemeral), and (b) still deliver to the actor, because delivery rides
/// the identity-keyed connection to peer A, not the stored address. Before
/// WP1 this failed at (a): the location was dropped and `lookup` returned
/// `None` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undialable_actor_address_still_delivers_by_identity() -> icanact_remote::Result<()> {
    let (key_a, key_b) = keys_ordered_dialer_first("identity-garbage-a", "identity-garbage-b");
    let addr_a: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();
    let addr_b: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();

    let node_a = start_node(addr_a, &key_a, test_config()).await?;
    let node_b = start_node(addr_b, &key_b, test_config()).await?;

    // A is the tie-break-preferred dialer.
    node_a
        .add_peer(&node_b.registry.peer_id)
        .await
        .connect(&addr_b)
        .await?;
    assert!(
        wait_for_pair_connection(&node_a, &node_b, Duration::from_secs(10)).await,
        "pair must connect before the actor is registered"
    );

    // A registers its actor advertising a fully undialable address.
    let garbage: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let actor_name = "identity/undialable/echo";
    node_a
        .register_urgent(
            actor_name.to_string(),
            garbage,
            RegistrationPriority::Immediate,
        )
        .await?;

    // (a) B stores the location keyed by A's identity, never drops it.
    let deadline = Instant::now() + Duration::from_secs(10);
    let stored = loop {
        if let Some(location) = node_b.registry.lookup_actor(actor_name).await {
            break location;
        }
        assert!(
            Instant::now() < deadline,
            "actor location must be stored on B (identity-routable), not dropped over its address"
        );
        sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        stored.peer_id, node_a.registry.peer_id,
        "stored location must carry the owner's identity"
    );
    let stored_addr: SocketAddr = stored
        .address
        .parse()
        .expect("stored address must remain a socket address");
    assert_eq!(
        stored_addr.port(),
        0,
        "port 0 is preserved (source port is ephemeral, nothing valid to substitute) — \
         so ONLY identity routing can possibly deliver to this actor"
    );

    // (b) Delivery still works: the send path routes by peer_id over the
    // existing connection. If it dialed the stored address instead, this
    // would fail (port 0 is unconnectable).
    let actor_ref = node_b
        .lookup(actor_name)
        .await
        .expect("lookup must return a routable ref for an identity-routable actor");
    actor_ref
        .tell(bytes::Bytes::from_static(b"delivered-by-identity"))
        .await
        .expect("tell must ride the identity-keyed peer connection, not the stored address");

    // The verified connection survives: no address-triggered eviction.
    assert!(
        node_a
            .registry
            .has_connection_to_peer(&node_b.registry.peer_id)
            .await
            || node_b
                .registry
                .has_connection_to_peer(&node_a.registry.peer_id)
                .await,
        "the verified connection must never be dropped over an address"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

/// T6 row 8 / §1.3: node B knows peer A only as KEY + a wrong, dead dial
/// address. B can never successfully dial A — but A dials B, arriving from
/// a source address B was never told about. B must accept the inbound by
/// key alone and become fully functional (learn A's actors, deliver to
/// them), proving connection acceptance is identity-gated, never
/// address-gated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_from_unknown_source_accepted_by_key_despite_wrong_configured_addr()
-> icanact_remote::Result<()> {
    // The node with the CORRECT config is the preferred dialer so the
    // wrong-configured side's dialer never has to succeed.
    let (key_a, key_b) = keys_ordered_dialer_first("identity-inbound-a", "identity-inbound-b");
    let addr_a: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();
    let addr_b: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();

    let node_a = start_node(addr_a, &key_a, test_config()).await?;
    let node_b = start_node(addr_b, &key_b, test_config()).await?;

    // B is configured with A's key at a dead address: connection refused,
    // forever. (Reserved-then-released port on localhost.)
    let wrong_addr: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();
    let peer_a_from_b = node_b.add_peer(&node_a.registry.peer_id).await;
    let _ = peer_a_from_b.connect(&wrong_addr).await; // expected to fail

    // A dials B's real address. B sees an inbound from an ephemeral source
    // address it has no configuration for — and must accept it by key.
    node_a
        .add_peer(&node_b.registry.peer_id)
        .await
        .connect(&addr_b)
        .await?;
    assert!(
        wait_for_pair_connection(&node_a, &node_b, Duration::from_secs(10)).await,
        "B must accept A's inbound by key despite knowing only a wrong dial address for A"
    );

    // Full functionality over the inbound-only relationship: B learns A's
    // actor and delivers to it.
    let actor_name = "identity/inbound-only/echo";
    node_a
        .register_urgent(
            actor_name.to_string(),
            addr_a,
            RegistrationPriority::Immediate,
        )
        .await?;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if node_b.registry.lookup_actor(actor_name).await.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "B must learn A's actors over the inbound connection it accepted by key"
        );
        sleep(Duration::from_millis(50)).await;
    }
    let actor_ref = node_b
        .lookup(actor_name)
        .await
        .expect("actor must be routable from B");
    actor_ref
        .tell(bytes::Bytes::from_static(b"delivered-over-inbound"))
        .await
        .expect("delivery must work over the key-accepted inbound connection");

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

async fn wait_for_stored_location(
    node: &GossipRegistryHandle<BuilderTlsBootstrap>,
    actor_name: &str,
    timeout: Duration,
) -> icanact_remote::RemoteActorLocation {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(location) = node.registry.lookup_actor(actor_name).await {
            return location;
        }
        assert!(
            Instant::now() < deadline,
            "actor location must be stored (identity-routable), not dropped"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// Codex P1 (round 2) / §1.6: the immediate-delta path must repair an
/// owner-sent wildcard from the AUTHENTICATED source address of the
/// connection that delivered the delta — not from the receiver's configured
/// route for the sender. Topology: the receiver never configured the
/// dialer at all (inbound-only, accept-by-key), so a config-derived lookup
/// has nothing trustworthy to offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wildcard_interest_from_unconfigured_inbound_peer_resolves_to_source_ip()
-> icanact_remote::Result<()> {
    // Dialer = tie-break-preferred side; the listener has ZERO config for it.
    let (key_dialer, key_listener) =
        keys_ordered_dialer_first("identity-unconfig-a", "identity-unconfig-b");
    let addr_dialer: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();
    let addr_listener: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();

    let dialer = start_node(addr_dialer, &key_dialer, test_config()).await?;
    let listener = start_node(addr_listener, &key_listener, test_config()).await?;

    dialer
        .add_peer(&listener.registry.peer_id)
        .await
        .connect(&addr_listener)
        .await?;
    assert!(
        wait_for_pair_connection(&dialer, &listener, Duration::from_secs(10)).await,
        "listener must accept the inbound by key"
    );

    // The dialer advertises a wildcard-bound actor over the immediate path.
    let actor_name = "identity/unconfigured-source/echo";
    dialer
        .register_urgent(
            actor_name.to_string(),
            "0.0.0.0:9400".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;

    let stored = wait_for_stored_location(&listener, actor_name, Duration::from_secs(10)).await;
    let stored_addr: SocketAddr = stored.address.parse().expect("stored address parses");
    assert!(
        !stored_addr.ip().is_unspecified(),
        "owner-sent wildcard must be repaired from the verified connection source, \
         even when the receiver has no configured route for the sender (got {})",
        stored.address
    );
    assert_eq!(stored_addr.port(), 9400, "advertised port preserved");

    dialer.shutdown().await;
    listener.shutdown().await;
    Ok(())
}

/// Codex P1 (round 2), stale-config flavor: when the receiver's configured
/// address for a peer is stale/dead but the peer is LIVE on a verified
/// connection, wildcard repair must use the live connection's source IP —
/// never the stale configured IP (that would re-advertise a dead route).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wildcard_interest_repair_prefers_live_source_over_stale_config()
-> icanact_remote::Result<()> {
    let (key_dialer, key_listener) =
        keys_ordered_dialer_first("identity-staleconf-a", "identity-staleconf-b");
    let addr_dialer: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();
    let addr_listener: SocketAddr = format!("127.0.0.1:{}", reserve_free_port())
        .parse()
        .unwrap();

    let dialer = start_node(addr_dialer, &key_dialer, test_config()).await?;
    let listener = start_node(addr_listener, &key_listener, test_config()).await?;

    // Listener is configured with the dialer's key at a STALE, unreachable
    // address whose IP differs from the dialer's real source IP.
    let stale_addr: SocketAddr = "10.255.255.1:9".parse().unwrap();
    let peer_from_listener = listener.add_peer(&dialer.registry.peer_id).await;
    let _ = peer_from_listener.connect(&stale_addr).await; // dead, expected to fail

    dialer
        .add_peer(&listener.registry.peer_id)
        .await
        .connect(&addr_listener)
        .await?;
    assert!(
        wait_for_pair_connection(&dialer, &listener, Duration::from_secs(10)).await,
        "pair must connect via the dialer's outbound"
    );

    let actor_name = "identity/stale-config-source/echo";
    dialer
        .register_urgent(
            actor_name.to_string(),
            "0.0.0.0:9400".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;

    let stored = wait_for_stored_location(&listener, actor_name, Duration::from_secs(10)).await;
    let stored_addr: SocketAddr = stored.address.parse().expect("stored address parses");
    assert_ne!(
        stored_addr.ip(),
        stale_addr.ip(),
        "repair must never use the stale configured IP over the live verified source"
    );
    assert!(
        !stored_addr.ip().is_unspecified(),
        "owner-sent wildcard must be repaired (got {})",
        stored.address
    );

    dialer.shutdown().await;
    listener.shutdown().await;
    Ok(())
}
