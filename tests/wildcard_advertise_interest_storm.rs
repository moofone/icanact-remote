//! RED-first regression coverage for the wildcard-bind routed-pubsub
//! interest advertisement bug observed on icemining devnet coins-backend
//! (2026-07): a node bound to `0.0.0.0:<port>` (a normal, legitimate
//! deployment pattern) advertises its routed-pubsub INTEREST actor location
//! using the raw bind address (`GossipRegistry::note_interest`,
//! `src/pubsub.rs`), producing a gossiped `RemoteActorLocation` with an
//! unspecified IP. The receiving peer's `validate_remote_actor_addr`
//! (`src/registry.rs`) then silently *drops* that location instead of
//! rewriting it from the TCP source address the way the adjacent
//! `resolve_peer_addr_checked` already does for peer bind-address
//! resolution. The dropped route starves the receiver of a path to the
//! interest actor, which — per the tie-break reconnect mechanics exercised
//! in `tests/tie_break_reconnect_storm.rs` (PR #81/#82) — feeds a
//! self-sustaining reconnect/re-gossip churn loop instead of a converged,
//! quiet steady state.
//!
//! This test exercises the real, public `RoutedPubSub` subscribe surface
//! (not `note_interest` directly) end-to-end through two real
//! `GossipRegistryHandle` nodes so it is a faithful reproduction of the live
//! coins-backend shape, not a unit-level poke.

use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, RoutedPubSub,
    TransportDirection, TransportLifecycleEvent, set_transport_lifecycle_recorder, topic_key,
};
use std::net::{IpAddr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::time::{Instant, sleep};

static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn reserve_free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn churn_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(40),
        cleanup_interval: Duration::from_millis(100),
        peer_retry_interval: Duration::from_millis(50),
        peer_supervisor_interval: Duration::from_millis(25),
        connection_timeout: Duration::from_millis(120),
        response_timeout: Duration::from_millis(120),
        ..Default::default()
    }
}

async fn start_node(
    addr: SocketAddr,
    keypair: KeyPair,
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

async fn configure_required_peer(
    node: &GossipRegistryHandle<BuilderTlsBootstrap>,
    peer_id: &icanact_remote::PeerId,
    addr: SocketAddr,
) {
    let peer = node.add_peer(peer_id).await;
    let _ = peer.connect(&addr).await;
}

async fn wait_for_pair_connection(
    a: &GossipRegistryHandle<BuilderTlsBootstrap>,
    b: &GossipRegistryHandle<BuilderTlsBootstrap>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut consecutive = 0;
    while Instant::now() < deadline {
        let up = a.registry.has_connection_to_peer(&b.registry.peer_id).await
            || b.registry.has_connection_to_peer(&a.registry.peer_id).await;
        if up {
            consecutive += 1;
            if consecutive >= 3 {
                return true;
            }
        } else {
            consecutive = 0;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Mirrors the crate-private `pubsub::interest_name` wire format exactly
/// (`src/pubsub.rs` `INTEREST_PREFIX`/`interest_name`) so the test can look
/// up the gossiped interest actor from the receiving side without needing a
/// crate-internal export.
fn interest_actor_name(topic_key: u64, peer: &icanact_remote::PeerId) -> String {
    format!(
        "icanact/pubsub/interest/v1/{topic_key:016x}/{}",
        peer.to_hex()
    )
}

async fn wait_for_interest_location(
    node: &GossipRegistryHandle<BuilderTlsBootstrap>,
    name: &str,
    timeout: Duration,
) -> Option<icanact_remote::RemoteActorLocation> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(location) = node.registry.lookup_actor(name).await {
            return Some(location);
        }
        sleep(Duration::from_millis(50)).await;
    }
    None
}

/// (a) Core reproduction: node A binds a wildcard listener (`0.0.0.0:<port>`)
/// with no `advertise_address` override — a normal deployment pattern, not a
/// misconfiguration — registers routed-pubsub interest in a topic through
/// the real public `RoutedPubSub::subscribe_bytes` surface, and node B must
/// end up with a *routable* location for that interest actor rather than no
/// location at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wildcard_bind_interest_advertises_routable_address() -> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let a_port = reserve_free_port();
    let a_wildcard_addr: SocketAddr = format!("0.0.0.0:{a_port}").parse().unwrap();
    let a_dial_addr: SocketAddr = format!("127.0.0.1:{a_port}").parse().unwrap();

    let a_keypair = KeyPair::new_for_testing("wildcard_interest_storm_a");
    let b_keypair = KeyPair::new_for_testing("wildcard_interest_storm_b");
    let a_peer_id = a_keypair.peer_id();
    let b_peer_id = b_keypair.peer_id();

    // Node A: real wildcard bind, exactly the coins-backend devnet shape.
    // `advertise_address` is intentionally left at its default (None) —
    // that is the whole point of the bug: no explicit escape hatch was
    // configured, and none should be required for correctness.
    let a = start_node(a_wildcard_addr, a_keypair.clone(), churn_config()).await?;
    assert!(
        a.registry.bind_addr.ip().is_unspecified(),
        "test setup invariant: node A must actually be bound to a wildcard address"
    );

    let b = start_node(
        "127.0.0.1:0".parse().unwrap(),
        b_keypair.clone(),
        churn_config(),
    )
    .await?;
    let b_addr = b.registry.bind_addr;

    configure_required_peer(&a, &b_peer_id, b_addr).await;
    configure_required_peer(&b, &a_peer_id, a_dial_addr).await;

    assert!(
        wait_for_pair_connection(&a, &b, Duration::from_secs(5)).await,
        "nodes A (wildcard bind) and B failed to establish a connection"
    );

    // Register routed-pubsub interest on A through the real public surface.
    let pubsub_a = RoutedPubSub::install(Arc::clone(&a.registry)).await;
    let topic = topic_key("coins/wildcard-interest-storm/prices");
    let type_hash: u64 = 0x000C_01E5;
    let _subscription = pubsub_a.subscribe_bytes(topic, type_hash, |_bytes| {});

    let interest_name = interest_actor_name(topic, &a_peer_id);

    let location = wait_for_interest_location(&b, &interest_name, Duration::from_secs(5)).await;
    let location = location.unwrap_or_else(|| {
        panic!(
            "node B never learned a location for A's routed-pubsub interest actor \
             ({interest_name}) — the wildcard-bind advertised address was dropped instead of \
             resolved, starving B of a route (src/registry.rs validate_remote_actor_addr)"
        )
    });

    let advertised: SocketAddr = location.address.parse().unwrap_or_else(|e| {
        panic!(
            "B's stored location address '{}' did not parse: {e}",
            location.address
        )
    });
    assert!(
        !advertised.ip().is_unspecified(),
        "node B stored an unspecified (routeless) address {advertised} for A's routed-pubsub \
         interest actor — note_interest (src/pubsub.rs) advertised registry.bind_addr verbatim \
         instead of resolving through GossipConfig::advertise_address / the TCP source address"
    );
    assert_ne!(
        advertised.ip(),
        IpAddr::from([0, 0, 0, 0]),
        "advertised interest location must not be the wildcard IP"
    );

    a.shutdown_and_wait().await;
    b.shutdown_and_wait().await;
    Ok(())
}

/// (b)/(c) Chaos-matrix extension: the same wildcard-bind interest
/// advertisement, but under restart churn on the wildcard-bound side, with a
/// storm-rate bound and a zero-drop/zero-reset steady-state assertion after
/// settle — the same shape as `tests/tie_break_reconnect_storm.rs`
/// (#81/#82) extended to cover this defect's interaction with that
/// machinery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wildcard_bind_interest_storm_settles_to_quiet_steady_state() -> icanact_remote::Result<()>
{
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let outbound_starts = Arc::new(AtomicUsize::new(0));
    let evictions = Arc::new(AtomicUsize::new(0));
    {
        let outbound_starts = outbound_starts.clone();
        let evictions = evictions.clone();
        set_transport_lifecycle_recorder(Some(Arc::new(move |event| match event {
            TransportLifecycleEvent::OutboundStart { .. } => {
                outbound_starts.fetch_add(1, Ordering::SeqCst);
            }
            TransportLifecycleEvent::WrongDirectionEvicted {
                direction: TransportDirection::Outbound | TransportDirection::Inbound,
                ..
            } => {
                evictions.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        })));
    }

    let a_port = reserve_free_port();
    let a_wildcard_addr: SocketAddr = format!("0.0.0.0:{a_port}").parse().unwrap();
    let a_dial_addr: SocketAddr = format!("127.0.0.1:{a_port}").parse().unwrap();
    let a_keypair = KeyPair::new_for_testing("wildcard_interest_storm_churn_a");
    let b_keypair = KeyPair::new_for_testing("wildcard_interest_storm_churn_b");
    let a_peer_id = a_keypair.peer_id();
    let b_peer_id = b_keypair.peer_id();

    let b = start_node(
        "127.0.0.1:0".parse().unwrap(),
        b_keypair.clone(),
        churn_config(),
    )
    .await?;
    let b_addr = b.registry.bind_addr;
    {
        let peer = b.add_peer(&a_peer_id).await;
        let _ = peer.connect(&a_dial_addr).await; // expected to fail: A not up yet
    }

    // Restart churn: bring the wildcard-bound side up/down repeatedly on the
    // same address/identity, mirroring a Gate-E-style failover drill against
    // a coins-backend node that binds 0.0.0.0.
    const UP_MS: &[u64] = &[15, 120, 10, 180, 15, 20, 150, 10];
    const DOWN_MS: &[u64] = &[10, 15, 10, 20, 10, 10, 15, 10];
    for (up_ms, down_ms) in UP_MS.iter().zip(DOWN_MS.iter()) {
        let a = start_node(a_wildcard_addr, a_keypair.clone(), churn_config()).await?;
        {
            let peer = a.add_peer(&b_peer_id).await;
            let _ = peer.connect(&b_addr).await;
        }
        sleep(Duration::from_millis(*up_ms)).await;
        a.shutdown_and_wait().await;
        sleep(Duration::from_millis(*down_ms)).await;
    }

    let a_final = start_node(a_wildcard_addr, a_keypair.clone(), churn_config()).await?;
    configure_required_peer(&a_final, &b_peer_id, b_addr).await;
    configure_required_peer(&b, &a_peer_id, a_dial_addr).await;

    assert!(
        wait_for_pair_connection(&a_final, &b, Duration::from_secs(5)).await,
        "wildcard-bound node A and node B failed to converge after restart churn"
    );

    let pubsub_a = RoutedPubSub::install(Arc::clone(&a_final.registry)).await;
    let topic = topic_key("coins/wildcard-interest-storm/steady-state");
    let type_hash: u64 = 0x000C_01E6;
    let _subscription = pubsub_a.subscribe_bytes(topic, type_hash, |_bytes| {});
    let interest_name = interest_actor_name(topic, &a_peer_id);

    let location = wait_for_interest_location(&b, &interest_name, Duration::from_secs(5)).await;
    assert!(
        location
            .as_ref()
            .and_then(|loc| loc.address.parse::<SocketAddr>().ok())
            .is_some_and(|addr| !addr.ip().is_unspecified()),
        "node B never resolved a routable location for A's interest actor after restart churn \
         (location={location:?})"
    );

    // Reset counters: only post-churn, post-subscribe steady state is under
    // test now.
    sleep(Duration::from_millis(300)).await;
    outbound_starts.store(0, Ordering::SeqCst);
    evictions.store(0, Ordering::SeqCst);

    let quiet_window = Duration::from_millis(1200);
    sleep(quiet_window).await;

    let quiet_outbound = outbound_starts.load(Ordering::SeqCst);
    let quiet_evictions = evictions.load(Ordering::SeqCst);

    assert!(
        quiet_outbound <= 3,
        "reconnect storm: {quiet_outbound} outbound connect attempts in a {quiet_window:?} \
         quiet window after wildcard-interest convergence; expected a converged, near-zero-dial \
         steady state"
    );
    assert!(
        quiet_evictions <= 1,
        "reconnect storm: {quiet_evictions} duplicate-connection tie-break evictions in the \
         quiet window after wildcard-interest convergence"
    );

    // Steady-state location must remain stable/routable, not flap back to
    // dropped/unspecified.
    let final_location = b.registry.lookup_actor(&interest_name).await;
    assert!(
        final_location
            .as_ref()
            .and_then(|loc| loc.address.parse::<SocketAddr>().ok())
            .is_some_and(|addr| !addr.ip().is_unspecified()),
        "node B's location for A's interest actor regressed to missing/unspecified in the quiet \
         window (final_location={final_location:?})"
    );

    set_transport_lifecycle_recorder(None);
    a_final.shutdown_and_wait().await;
    b.shutdown_and_wait().await;
    Ok(())
}
