//! Regression test for the SWIM Dead-verdict reconnect amplifier.
//!
//! ## The amplifier
//!
//! A SWIM membership consumer (icanact-core) tears down a peer's TLS session
//! when it (transiently, possibly falsely) marks that peer `Dead`, then relies
//! on the transport re-establishing the session inside a short
//! disconnect-debounce window (~4s). If reconnect takes longer than the
//! window, the peer is re-torn-down before it can refute — a liveness deadlock.
//!
//! The higher-`NodeId` side of a duplicate-connection tie-break suppresses its
//! outbound dial and waits for the lower-`NodeId` side to dial in
//! (`wait_for_preferred_connection`). That wait used to be bounded by
//! `connection_timeout` (10s default). Under the configured-peer supervisor,
//! each reconnect attempt is wrapped in a bounded per-attempt budget
//! (`min(connection_timeout, 900ms)`), so the supervisor *cancelled the 10s
//! wait every tick* and the higher-`NodeId` side never reached the option-2
//! fallback dial. It stalled until the remote happened to dial in — far past
//! the consumer's debounce window on a real network.
//!
//! ## The fix this pins
//!
//! The wait is now bounded by `preferred_inbound_wait` (500ms default), which
//! is deliberately kept under the supervisor's per-attempt budget so a single
//! supervisor tick can wait out the preferred-inbound window AND still complete
//! the fallback dial. This test drives the supervisor path directly with a
//! large `connection_timeout` and a small `preferred_inbound_wait`, and asserts
//! the higher-`NodeId` side re-establishes well within `connection_timeout`
//! without any help from the remote.
//!
//! Before the fix, with `connection_timeout = 5s`, the higher-`NodeId`
//! supervisor never reached the fallback dial (900ms budget < 5s wait) and this
//! test would never observe a connection within its 2.5s bound.

use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, tls};
use std::sync::Once;
use std::time::{Duration, Instant};

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        tls::ensure_crypto_provider();
    });
}

/// Large `connection_timeout` so that if the preferred-inbound wait were still
/// coupled to it, the supervisor's bounded per-attempt budget could never reach
/// the fallback dial. Small `preferred_inbound_wait` so a single supervisor
/// tick can. Manual supervisor driving (long timer interval) keeps the test
/// deterministic.
fn test_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(100),
        cleanup_interval: Duration::from_millis(200),
        connection_timeout: Duration::from_secs(5),
        preferred_inbound_wait: Duration::from_millis(200),
        peer_retry_interval: Duration::from_millis(100),
        peer_supervisor_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

async fn spawn(seed: &str) -> GossipRegistryHandle<BuilderTlsBootstrap> {
    init_crypto();
    let keypair = KeyPair::new_for_testing(seed);
    let mut cfg = test_cfg();
    cfg.key_pair = Some(keypair.clone());
    GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        keypair.to_secret_key(),
        Some(cfg),
        BuilderTlsBootstrap,
    )
    .await
    .expect("registry start")
}

/// Return (high, low) seeds such that `high.peer_id > low.peer_id` under the
/// same byte ordering the tie-breaker uses. Asserted at runtime.
fn pick_high_low_seeds() -> (&'static str, &'static str) {
    let a_id = KeyPair::new_for_testing("supervisor-reconnect-seed-A").peer_id();
    let b_id = KeyPair::new_for_testing("supervisor-reconnect-seed-B").peer_id();
    if a_id.to_node_id().as_bytes() > b_id.to_node_id().as_bytes() {
        ("supervisor-reconnect-seed-A", "supervisor-reconnect-seed-B")
    } else {
        ("supervisor-reconnect-seed-B", "supervisor-reconnect-seed-A")
    }
}

/// The higher-`NodeId` side has the lower-`NodeId` side as a required
/// (configured) peer; the lower side is fully online but does NOT know about
/// the higher side, so it never dials. Only the higher side's supervisor can
/// establish the link — and only by reaching the option-2 fallback dial within
/// one bounded supervisor tick. This is the exact supervisor path the amplifier
/// stalled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn higher_id_supervisor_reconnect_completes_within_debounce_window() {
    let (high_seed, low_seed) = pick_high_low_seeds();

    let high = spawn(high_seed).await;
    let low = spawn(low_seed).await;

    assert!(
        high.registry.peer_id.to_node_id().as_bytes()
            > low.registry.peer_id.to_node_id().as_bytes(),
        "test precondition: high seed must produce greater node id"
    );

    // Asymmetric: only `high` knows `low`. `low` has no entry for `high`, so
    // the lower-id side never dials — the higher-id supervisor is the only
    // path to a connection.
    high.registry
        .add_peer_with_node_id(
            low.registry.bind_addr,
            Some(low.registry.peer_id.to_node_id()),
        )
        .await;
    high.registry
        .configure_peer(low.registry.peer_id.clone(), low.registry.bind_addr)
        .await;

    // Drive the supervisor manually on a tight cadence and measure how long the
    // higher-id side needs to establish the link. The bound (2.5s) is well
    // under `connection_timeout` (5s): if the preferred-inbound wait were still
    // coupled to `connection_timeout`, no supervisor tick's 900ms budget could
    // ever complete it and this would never see a connection.
    let started = Instant::now();
    let deadline = started + Duration::from_millis(2500);
    let mut connected = false;
    while Instant::now() < deadline {
        high.registry.supervise_configured_peers().await;
        if high
            .registry
            .has_connection_to_peer(&low.registry.peer_id)
            .await
        {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        connected,
        "higher-NodeId supervisor failed to re-establish within {}ms (< {}s \
         connection_timeout). The preferred-inbound wait is coupled to \
         connection_timeout again: the supervisor's bounded per-attempt budget \
         cancels the wait every tick, so the fallback dial is never reached — \
         the SWIM Dead-verdict reconnect amplifier.",
        started.elapsed().as_millis(),
        test_cfg().connection_timeout.as_secs(),
    );

    high.shutdown().await;
    low.shutdown().await;
}
