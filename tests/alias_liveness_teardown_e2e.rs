//! A stale address alias must not tear down its identity's live connection.
//!
//! Failure accounting in `apply_gossip_results` is keyed by `SocketAddr`, but
//! the teardown it triggers is keyed by identity: `newly_dead` holds addresses,
//! and for each one `registry.rs:8457` resolves `addr -> peer_id` and calls
//! `disconnect_connection_by_peer_id`, which drops that peer's *current*
//! connection regardless of which address that connection is on.
//!
//! Those two keyings disagree whenever a peer is known under more than one
//! address, which is the normal steady state: a node that both dials a peer and
//! accepts a connection from it holds that peer under the advertised bind
//! address *and* under the ephemeral TCP source address of the accepted socket.
//!
//! Only one of those aliases can be refreshed. `select_best_alias_per_identity`
//! deliberately gives each identity exactly one gossip slot per round, and
//! `last_response_received_ms` is refreshed per connection -- so the aliases that
//! lose the slot receive no gossip, get no response, and can never refresh. They
//! accrue response-asymmetry failures on a timer until they cross
//! `max_peer_failures`, at which point a *stale* alias executes an
//! identity-scoped teardown of a *healthy* connection.
//!
//! This is self-sustaining. Each teardown is followed by a reconnect from a new
//! ephemeral source port, which adds another alias, which becomes another stale
//! timer. Devnet's collector accumulated six aliases for one relay
//! (`10.77.0.38:38188`, `:53950`, `:53404`, `:33924`, `:58236`, `:52978`, all
//! peer_id `3e4773bd...`) and flapped every 30-60s indefinitely, with
//! `stale_side_table_entries` climbing 69 -> 75 as the reaper fell behind.

use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::{Instant, sleep};

type Node = GossipRegistryHandle<BuilderTlsBootstrap>;

/// Liveness window kept comfortably above `gossip_interval * 2` so
/// `GossipConfig::normalize` does not clamp it, and small enough that several
/// windows elapse inside the test.
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
             up and idle. A holds {alias_count} address aliases for this \
             identity; only the one that wins `select_best_alias_per_identity` \
             is ever refreshed, so the losing aliases accrue response-asymmetry \
             failures until one of them crosses the threshold and executes an \
             identity-scoped `disconnect_connection_by_peer_id` against the live \
             connection"
        );
        sleep(Duration::from_millis(50)).await;
    }

    b.shutdown_and_wait().await;
    a.shutdown_and_wait().await;
    Ok(())
}
