//! Seed-only peers must recover from a socket close.
//!
//! A peer introduced by a seed dial (`bootstrap_seed` in `icanact-core`, which
//! bottoms out in `GossipRegistryHandle::lookup_address`) is a first-class peer
//! relationship, not a one-shot lookup. Every deployed service in the
//! `icemining` mesh joins this way and never calls `configure_peer`, so if a
//! seeded relationship cannot survive a socket close, nothing in that mesh can.
//!
//! The rest of the reconnect suite -- `reconnect_convergence_e2e`,
//! `preferred_inbound_supervisor_reconnect`, `tie_break_reconnect_storm`,
//! `gossip_required_peer_liveness_chaos` -- establishes its peers with
//! `configure_peer` or `add_peer().connect()`, both of which mark the session
//! `required` and hand it to `supervise_configured_peers`. That shared setup is
//! why this gap survived: every existing test opts into the supervised path
//! before exercising reconnect, so none of them can observe what happens to a
//! relationship that was never supervised.

mod common;

use common::{
    DynError, TlsHandle, create_ordered_tls_pair, force_disconnect, ordered_keypair_pair,
    wait_for_condition, wait_for_pair_connection,
};
use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;

/// How long a reconnect may take before we call it a wedge.
///
/// Generous relative to `fast_gossip_config`'s 100ms gossip interval and 50ms
/// retry interval: the point of the assertion is "ever", not "quickly", so a
/// failure here means no driver exists rather than that one was slow.
const RECONNECT_BUDGET: Duration = Duration::from_secs(15);

/// A seed-dialled peer reconnects after the socket dies.
///
/// This is the production wedge in miniature. `force_disconnect` calls
/// `handle_peer_connection_failure` on both sides, which is the exact entry
/// point the deployed collector took when its relay restarted underneath it.
#[tokio::test]
async fn seed_only_peer_reconnects_after_socket_close() -> Result<(), DynError> {
    let (a, b) = create_ordered_tls_pair("seed-only-reconnect-a", "seed-only-reconnect-b").await?;
    let b_addr = b.registry.bind_addr;
    let b_peer_id = b.registry.peer_id.clone();

    // The seed dial, and nothing else. No `configure_peer`, no
    // `add_peer().connect()` -- introducing either here would silently move the
    // peer onto the supervised path and make this test pass for the wrong
    // reason.
    a.lookup_address(b_addr).await?;

    assert!(
        wait_for_pair_connection(&a, &b, Duration::from_secs(10)).await,
        "seed dial never established a connection, so the reconnect assertion \
         below would be vacuous"
    );

    force_disconnect(&a, &b).await;

    assert!(
        wait_for_condition(Duration::from_secs(5), || async {
            !a.registry.has_connection_to_peer(&b_peer_id).await
        })
        .await,
        "the forced disconnect did not actually drop A's connection, so the \
         reconnect assertion below would be vacuous"
    );

    assert!(
        wait_for_condition(RECONNECT_BUDGET, || async {
            a.registry.has_connection_to_peer(&b_peer_id).await
        })
        .await,
        "seed-dialled peer never reconnected within {RECONNECT_BUDGET:?}: the \
         relationship has no outbound reconnect driver, so it stays down until \
         the process restarts"
    );

    Ok(())
}

/// Bring a node up on one specific address, so a restart can reclaim it.
///
/// The shared helpers all bind `127.0.0.1:0`, which is right for tests that
/// only need *a* node but useless here: a restart that lands on a fresh port is
/// a different peer as far as the dialer's stored address is concerned, and the
/// reconnect it then fails to perform would be unremarkable rather than a bug.
async fn node_on_addr(
    keypair: KeyPair,
    addr: SocketAddr,
    mut config: GossipConfig,
) -> Result<TlsHandle, DynError> {
    icanact_remote::tls::ensure_crypto_provider();
    config.key_pair = Some(keypair.clone());
    Ok(GossipRegistryHandle::new_with_transport_stack(
        addr,
        keypair.to_secret_key(),
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?)
}

/// Config whose dead-peer reaper fires in test time rather than 15 minutes.
fn reaping_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(100),
        cleanup_interval: Duration::from_millis(100),
        peer_retry_interval: Duration::from_millis(50),
        connection_timeout: Duration::from_millis(500),
        response_timeout: Duration::from_millis(500),
        // The production default is 900s. Every property under test here is a
        // function of "the peer outlived `dead_peer_timeout` while down", not
        // of the specific duration, so compressing it keeps the test honest and
        // fast.
        dead_peer_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

/// A seed-dialled peer that stays down past `dead_peer_timeout` is still
/// redialled once it returns on the same address.
///
/// This is the deployed sequence: the relay restarted, the collector's dial
/// target was unreachable for far longer than the reaper's window, and the
/// collector never reconnected across three days. Note that the restarted node
/// is constructed knowing nothing about the dialer -- exactly like the relay,
/// which has no configured route back to the collector. That asymmetry is
/// load-bearing: it removes the restarted peer's ability to heal the
/// relationship by dialing in, leaving the dialer's own outbound driver as the
/// only thing that can pass this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_only_peer_reconnects_after_peer_restart_past_dead_peer_timeout()
-> Result<(), DynError> {
    let (keypair_a, keypair_b) = ordered_keypair_pair("seed-restart-a", "seed-restart-b");
    let b_peer_id = keypair_b.peer_id();

    let b = node_on_addr(keypair_b.clone(), "127.0.0.1:0".parse()?, reaping_config()).await?;
    let b_addr = b.registry.bind_addr;
    let a = node_on_addr(keypair_a, "127.0.0.1:0".parse()?, reaping_config()).await?;

    a.lookup_address(b_addr).await?;
    assert!(
        wait_for_condition(Duration::from_secs(10), || async {
            a.registry.has_connection_to_peer(&b_peer_id).await
        })
        .await,
        "seed dial never established a connection, so everything below is vacuous"
    );

    // The peer goes away entirely, freeing its port.
    b.shutdown_and_wait().await;

    // Surface the close to the dialer the way the transport does in
    // production. A pooled connection to a process that has exited is not
    // noticed until something fails against it, and the deployed collector's
    // own logs show it took this exact entry point
    // (`handle_peer_connection_failure`) when its relay went away.
    let _ = a
        .registry
        .handle_peer_connection_failure(b_addr, None)
        .await;
    assert!(
        wait_for_condition(Duration::from_secs(10), || async {
            !a.registry.has_connection_to_peer(&b_peer_id).await
        })
        .await,
        "dialer still reports a connection to a peer that has shut down"
    );

    // Outlive the reaper, with margin for a cleanup tick to actually run.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Same identity, same address: from the dialer's perspective the peer it
    // has been failing to reach is simply reachable again.
    let _b_restarted = node_on_addr(keypair_b, b_addr, reaping_config()).await?;

    assert!(
        wait_for_condition(RECONNECT_BUDGET, || async {
            a.registry.has_connection_to_peer(&b_peer_id).await
        })
        .await,
        "dialer never reconnected to the restarted peer within {RECONNECT_BUDGET:?}, \
         even though it is listening again on the same address under the same \
         identity: the peer entry has been latched out of gossip target \
         selection with no path back short of a process restart"
    );

    Ok(())
}
