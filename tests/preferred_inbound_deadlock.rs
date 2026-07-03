//! Reproducer for the production stall observed at icemining devnet on
//! 2026-05-14, where stratum repeatedly logged:
//!
//! ```text
//! gossip: failed to gossip to peer peer=10.77.0.61:9301
//!   error=network error: timed out waiting for preferred inbound connection
//! ```
//!
//! and coin-proxy's gossip subsystem was completely silent.
//!
//! ## Failure pattern
//!
//! The `should_keep_connection` tie-breaker in `GossipRegistry` is strictly
//! asymmetric:
//!
//! - `local_id < remote_id` → keep outbound (this side dials).
//! - `local_id > remote_id` → keep inbound (this side waits; suppresses
//!   outbound dial in `transport_stream.rs:124`).
//!
//! The implicit protocol contract is that the lower-ID side actively dials
//! the higher-ID side. The higher-ID side enters `wait_for_preferred_connection`
//! and polls for an inbound to arrive, ultimately giving up with the literal
//! error string from this test.
//!
//! In production, **only one direction of the peer relationship was seeded**:
//! the higher-ID side had the lower-ID side in its peer set, but not the
//! reverse. The higher-ID side suppressed its outbound dial as designed, and
//! the lower-ID side, never having heard of the higher-ID side, never dialed.
//! Permanent stall, no error on the silent side.
//!
//! ## What this test pins down
//!
//! Two registries A and B. A's GossipNodeId > B's GossipNodeId. Only A is seeded with B's
//! address. We `connect_to_peer` from A and assert the dial succeeds within
//! a small timeout. **Today this test fails** with the exact production error,
//! reproducing the stall.
//!
//! ## Remediation contract
//!
//! Whatever the fix, this test must pass: a peer relationship where only one
//! side has the other's address must still converge to a usable connection,
//! because production topology cannot be relied on to seed both directions
//! before any gossip has run.

use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, tls};
use std::sync::Once;
use std::time::Duration;
use tokio::time::timeout;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        tls::ensure_crypto_provider();
    });
}

fn test_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(100),
        cleanup_interval: Duration::from_millis(200),
        peer_retry_interval: Duration::from_millis(50),
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

/// Pick two seeds and return (high, low) such that `high.peer_id > low.peer_id`
/// by the same byte ordering the tie-breaker uses.
fn pick_high_low_seeds() -> (&'static str, &'static str) {
    // These two seeds happen to produce predictable ordering under the
    // ed25519 key derivation used by `KeyPair::new_for_testing`. We assert
    // ordering at runtime so the test is self-checking if derivation
    // changes.
    let a_id = KeyPair::new_for_testing("preferred-inbound-seed-A").peer_id();
    let b_id = KeyPair::new_for_testing("preferred-inbound-seed-B").peer_id();
    if a_id.to_node_id().as_bytes() > b_id.to_node_id().as_bytes() {
        ("preferred-inbound-seed-A", "preferred-inbound-seed-B")
    } else {
        ("preferred-inbound-seed-B", "preferred-inbound-seed-A")
    }
}

/// Repro: only the higher-ID side is seeded with the lower-ID side's
/// address. The higher-ID side calls `connect_to_peer`, which today
/// suppresses the outbound dial and stalls because the lower-ID side
/// never dials back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn higher_id_side_stalls_when_lower_id_side_is_unseeded() {
    let (high_seed, low_seed) = pick_high_low_seeds();

    let high = spawn(high_seed).await;
    let low = spawn(low_seed).await;

    // Sanity: high's id really is greater. This is the case where
    // `should_keep_connection(low, is_outbound=true)` returns false on
    // `high`, causing `transport_stream.rs` to suppress the outbound and
    // wait for an inbound from `low`.
    assert!(
        high.registry.peer_id.to_node_id().as_bytes()
            > low.registry.peer_id.to_node_id().as_bytes(),
        "test precondition: high seed must produce greater node id"
    );

    // Production-shaped asymmetric bootstrap: only `high` is told about
    // `low`. `low` has no entry for `high`.
    high.add_peer(&low.registry.peer_id)
        .await
        .connect(&low.registry.bind_addr)
        .await
        .ok();

    // Acceptance is POSITIVE, not the absence of a warning:
    //
    // 1. The pair must end up with a usable connection.
    // 2. A locally-registered actor on `high` must propagate via gossip
    //    to `low` (i.e. `low.lookup_actor("…")` returns `high`'s addr).
    //
    // Merely silencing the "timed out waiting for preferred inbound
    // connection" log would not move either signal. Both must flip
    // from "never" to "within bounded time".
    let probe_actor = "preferred_inbound_repro_probe";
    high.register(probe_actor.to_string(), high.registry.bind_addr)
        .await
        .expect("local actor registration");

    let propagated = timeout(Duration::from_secs(5), async {
        loop {
            let connected = high
                .registry
                .has_connection_to_peer(&low.registry.peer_id)
                .await
                || low
                    .registry
                    .has_connection_to_peer(&high.registry.peer_id)
                    .await;

            let actor_visible_on_low = low.registry.lookup_actor(probe_actor).await.is_some();

            if connected && actor_visible_on_low {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);

    assert!(
        propagated,
        "asymmetric seeding stalled — higher-ID side suppressed outbound and \
         lower-ID side, having no peer entry, never dialed back. Production \
         symptom: \"timed out waiting for preferred inbound connection\" with \
         zero gossip messages crossing the link, no actor visible on the \
         silent side. Acceptance must be positive: connection up AND \
         gossip-propagated actor visible across the pair."
    );
}
