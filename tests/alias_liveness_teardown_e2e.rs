//! A healthy multi-alias relationship must remain connected.
//!
//! This is an observation/regression guard, not a reproduction of the devnet
//! flap. The original alias-starvation hypothesis was refuted: gossip selection
//! deduplicates by identity before building tasks, so a non-selected alias never
//! enters `apply_gossip_results` and cannot accrue failures merely by losing the
//! selection slot.
//!
//! The test still matters as a high-level invariant: multiple address aliases
//! are normal when a node both dials and accepts a connection, and a quiet,
//! healthy identity must remain reachable throughout that steady state.

use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::{Instant, sleep};

type Node = GossipRegistryHandle<BuilderTlsBootstrap>;

/// Legacy liveness side-table retention horizon, kept small enough that several
/// windows elapse inside the test. It is not a peer-health threshold and is not
/// normalized against the gossip interval.
const LIVENESS_WINDOW: Duration = Duration::from_millis(400);
const GOSSIP_INTERVAL: Duration = Duration::from_millis(100);

/// Several liveness windows of steady state. A correct implementation holds the
/// connection for all of it; the alias bug fires within the first window or two.
const OBSERVE: Duration = Duration::from_secs(6);

fn config() -> GossipConfig {
    GossipConfig {
        gossip_interval: GOSSIP_INTERVAL,
        peer_liveness_window: LIVENESS_WINDOW,
        cleanup_interval: Duration::from_secs(3_600),
        peer_retry_interval: Duration::from_millis(50),
        peer_supervisor_interval: Duration::from_millis(50),
        connection_timeout: Duration::from_millis(500),
        response_timeout: Duration::from_millis(500),
        ..Default::default()
    }
}

async fn start_node(addr: SocketAddr, keypair: KeyPair) -> icanact_remote::Result<Node> {
    icanact_remote::tls::ensure_crypto_provider();
    GossipRegistryHandle::new_with_transport_stack(
        addr,
        keypair.to_secret_key(),
        Some(config()),
        BuilderTlsBootstrap,
    )
    .await
}

async fn wait_until(timeout: Duration, mut check: impl AsyncFnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check().await {
            return true;
        }
        sleep(Duration::from_millis(20)).await;
    }
    false
}

/// A quiet, healthy, multi-alias peer relationship stays connected.
///
/// Nothing here breaks anything: both nodes stay up, neither is asked to
/// disconnect, and no failure is synthesised. The test simply establishes a
/// connection in both directions -- so each node holds two aliases for the other
/// -- and then watches an idle but perfectly healthy link.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_alias_does_not_tear_down_its_identitys_live_connection() -> icanact_remote::Result<()>
{
    let a_keys = KeyPair::new_for_testing("alias_liveness_a");
    let b_keys = KeyPair::new_for_testing("alias_liveness_b");
    let b_id = b_keys.peer_id();

    let a = start_node("127.0.0.1:0".parse().unwrap(), a_keys).await?;
    let a_addr = a.registry.bind_addr;
    let b = start_node("127.0.0.1:0".parse().unwrap(), b_keys).await?;
    let b_addr = b.registry.bind_addr;

    // Both directions, B first so that A *accepts* a socket and learns B's
    // ephemeral source address, then A dials B's advertised bind address and
    // learns that too. Either dial may be refused with `ConnectionExists` once
    // the other direction has already landed; that is a normal race here and
    // not what is under test, so the outcome is checked by the alias-count
    // precondition below rather than by these calls.
    let _ = b.lookup_address(a_addr).await;
    sleep(Duration::from_millis(300)).await;
    let _ = a.lookup_address(b_addr).await;

    assert!(
        wait_until(Duration::from_secs(10), async || {
            a.registry.has_connection_to_peer(&b_id).await
        })
        .await,
        "the pair never connected, so the observation below would be vacuous"
    );

    // Confirm the multi-alias precondition actually holds: more than one
    // address in A's peer table resolves to B's identity. Without this the test
    // would silently degrade into a single-alias case and prove nothing.
    let alias_count = a.registry.peer_alias_count(&b_id).await;
    assert!(
        alias_count >= 2,
        "expected A to hold at least two address aliases for B's identity, \
         found {alias_count}; the stale-alias path cannot be exercised with one \
         alias, so this test would be vacuous"
    );

    // Watch an idle, healthy link. Any drop is a false teardown: both processes
    // are alive and nothing asked for a disconnect.
    let deadline = Instant::now() + OBSERVE;
    while Instant::now() < deadline {
        assert!(
            a.registry.has_connection_to_peer(&b_id).await,
            "connection to a healthy peer was torn down while both nodes were \
             up and idle and A held {alias_count} address aliases for the identity"
        );
        sleep(Duration::from_millis(50)).await;
    }

    b.shutdown_and_wait().await;
    a.shutdown_and_wait().await;
    Ok(())
}
