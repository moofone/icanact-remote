//! Repeated connection churn must not permanently strand a publisher.
//!
//! The single clean disconnect/reconnect cycle recovers fine (see
//! `pubsub_publisher_reconnect_e2e`). The production outage did not follow one
//! clean cycle: a relay restarted underneath a long-lived collector that had
//! already accumulated peer state from connections in both directions, and the
//! collector was left publishing into `remote_transport_errors` for three days.
//!
//! These are production-shaped regression guards, not reproductions of that
//! outage. They exercise bidirectional aliases, unannounced restart, the two
//! disconnect entry points seen in production, and the deployed pubsub scope.
//! All remained recoverable in the healthy test mesh before the liveness fix,
//! so they pin adjacent recovery invariants without claiming a root cause.

use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PubSubDeliveryPolicy,
    PubSubScope, RoutedPubSub, topic_key,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::{Instant, sleep};

type Node = GossipRegistryHandle<BuilderTlsBootstrap>;

/// Per-round recovery budget. Generous relative to the intervals below, so a
/// failure means the round never recovers rather than that it was slow.
const ROUND_BUDGET: Duration = Duration::from_secs(15);

/// Enough rounds to exercise both disconnect entry points several times each,
/// including back-to-back repeats that arm the tie-break reconnect cooldown.
const ROUNDS: usize = 12;

fn fast_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(40),
        cleanup_interval: Duration::from_millis(100),
        peer_retry_interval: Duration::from_millis(50),
        peer_supervisor_interval: Duration::from_millis(25),
        connection_timeout: Duration::from_millis(300),
        response_timeout: Duration::from_millis(300),
        ..Default::default()
    }
}

async fn start_node(
    addr: SocketAddr,
    keypair: KeyPair,
    config: GossipConfig,
) -> icanact_remote::Result<Node> {
    icanact_remote::tls::ensure_crypto_provider();
    GossipRegistryHandle::new_with_transport_stack(
        addr,
        keypair.to_secret_key(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await
}

/// Mirrors the crate-private `pubsub::interest_name` wire format.
fn interest_actor_name(topic: u64, peer: &icanact_remote::PeerId) -> String {
    format!("icanact/pubsub/interest/v1/{topic:016x}/{}", peer.to_hex())
}

async fn wait_until(timeout: Duration, mut check: impl AsyncFnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check().await {
            return true;
        }
        sleep(Duration::from_millis(25)).await;
    }
    false
}

/// Publishing survives sustained connection churn in both directions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_recovers_from_every_round_of_connection_churn() -> icanact_remote::Result<()> {
    let publisher_keys = KeyPair::new_for_testing("flap_storm_publisher");
    let subscriber_keys = KeyPair::new_for_testing("flap_storm_subscriber");
    let publisher_id = publisher_keys.peer_id();
    let subscriber_id = subscriber_keys.peer_id();

    let publisher = start_node(
        "127.0.0.1:0".parse().unwrap(),
        publisher_keys,
        fast_config(),
    )
    .await?;
    let subscriber = start_node(
        "127.0.0.1:0".parse().unwrap(),
        subscriber_keys,
        fast_config(),
    )
    .await?;
    let publisher_addr = publisher.registry.bind_addr;
    let subscriber_addr = subscriber.registry.bind_addr;

    // Both directions, the way the deployed mesh actually runs. This is what
    // leaves each side holding two aliases for the other: the bind address it
    // dialled and the ephemeral source address of the connection it accepted.
    publisher.lookup_address(subscriber_addr).await?;
    subscriber.lookup_address(publisher_addr).await?;
    assert!(
        wait_until(Duration::from_secs(10), async || {
            publisher
                .registry
                .has_connection_to_peer(&subscriber_id)
                .await
        })
        .await,
        "seed dials never connected, so everything below would be vacuous"
    );

    let pubsub_pub = RoutedPubSub::install(Arc::clone(&publisher.registry)).await;
    let pubsub_sub = RoutedPubSub::install(Arc::clone(&subscriber.registry)).await;

    let topic = topic_key("icanact/flap-storm/telemetry");
    let type_hash: u64 = 0x00D3_2203;
    let received = Arc::new(AtomicUsize::new(0));
    let _sub = {
        let received = Arc::clone(&received);
        pubsub_sub.subscribe_bytes(topic, type_hash, move |_bytes| {
            received.fetch_add(1, Ordering::SeqCst);
        })
    };

    let interest_name = interest_actor_name(topic, &subscriber_id);
    assert!(
        wait_until(Duration::from_secs(10), async || {
            publisher
                .registry
                .lookup_actor(&interest_name)
                .await
                .is_some()
        })
        .await,
        "publisher never learned the subscriber's interest actor"
    );
    sleep(Duration::from_millis(300)).await;

    let publish = |body: &'static [u8]| {
        pubsub_pub.publish_bytes(
            topic,
            type_hash,
            bytes::Bytes::from_static(body),
            PubSubScope::ClusterWide,
            PubSubDeliveryPolicy::default(),
        )
    };

    let baseline = publish(b"baseline")?;
    assert!(
        baseline.remote_enqueued >= 1,
        "baseline publish never reached the subscriber (stats={baseline:?})"
    );
    assert!(
        wait_until(Duration::from_secs(5), async || {
            received.load(Ordering::SeqCst) >= 1
        })
        .await,
        "baseline was enqueued but never delivered"
    );

    for round in 0..ROUNDS {
        let delivered_before = received.load(Ordering::SeqCst);
        // Alternate the two entry points the production logs recorded: the
        // address-keyed `socket disconnection detected` handler, and the
        // identity-keyed one behind `reason="disconnect_by_peer_id"`.
        match round % 4 {
            0 => {
                let _ = publisher
                    .registry
                    .handle_peer_connection_failure(subscriber_addr, None)
                    .await;
            }
            1 => {
                let _ = publisher
                    .registry
                    .handle_peer_connection_failure_by_peer_id(&subscriber_id)
                    .await;
            }
            2 => {
                // Both sides drop at once, as they do when a process restarts.
                let _ = publisher
                    .registry
                    .handle_peer_connection_failure_by_peer_id(&subscriber_id)
                    .await;
                let _ = subscriber
                    .registry
                    .handle_peer_connection_failure_by_peer_id(&publisher_id)
                    .await;
            }
            _ => {
                // Back-to-back repeat with no settling time in between, which
                // is the oscillation signature `note_tie_break_eviction` arms
                // its cooldown on.
                let _ = publisher
                    .registry
                    .handle_peer_connection_failure_by_peer_id(&subscriber_id)
                    .await;
                let _ = publisher
                    .registry
                    .handle_peer_connection_failure(subscriber_addr, None)
                    .await;
            }
        }

        let recovered = wait_until(ROUND_BUDGET, async || {
            let enqueued = publish(b"probe")
                .map(|stats| stats.remote_enqueued >= 1)
                .unwrap_or(false);
            enqueued && received.load(Ordering::SeqCst) > delivered_before
        })
        .await;

        let stats = publish(b"post-round")?;
        assert!(
            recovered,
            "round {round} (disconnect variant {}) never recovered a route to a \
             subscriber that stayed up and interested throughout; last publish \
             was {stats:?}",
            round % 4
        );
        assert!(
            received.load(Ordering::SeqCst) > delivered_before,
            "round {round} enqueued a frame but did not deliver one to the subscriber"
        );
    }

    subscriber.shutdown_and_wait().await;
    publisher.shutdown_and_wait().await;
    Ok(())
}

/// The subscriber's process restarts and the publisher is told nothing.
///
/// This is a production-shaped restart guard, not a reproduction. Nothing here
/// synthesises a failure into the publisher: the subscriber exits, a new process
/// with the same identity binds the same address and dials back, and the
/// publisher must recover. The test pins that invariant without asserting which
/// mechanism caused the field outage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_recovers_when_subscriber_process_restarts_unannounced()
-> icanact_remote::Result<()> {
    // Far beyond the configured intervals below, so a failure means the guard
    // did not recover rather than that it was merely slow.
    const RECOVERY_BUDGET: Duration = Duration::from_secs(45);

    let publisher_keys = KeyPair::new_for_testing("unannounced_restart_publisher");
    let subscriber_keys = KeyPair::new_for_testing("unannounced_restart_subscriber");
    let subscriber_id = subscriber_keys.peer_id();

    let publisher = start_node(
        "127.0.0.1:0".parse().unwrap(),
        publisher_keys,
        fast_config(),
    )
    .await?;
    let publisher_addr = publisher.registry.bind_addr;
    let subscriber = start_node(
        "127.0.0.1:0".parse().unwrap(),
        subscriber_keys.clone(),
        fast_config(),
    )
    .await?;
    let subscriber_addr = subscriber.registry.bind_addr;

    publisher.lookup_address(subscriber_addr).await?;
    subscriber.lookup_address(publisher_addr).await?;
    assert!(
        wait_until(Duration::from_secs(10), async || {
            publisher
                .registry
                .has_connection_to_peer(&subscriber_id)
                .await
        })
        .await,
        "seed dials never connected, so everything below would be vacuous"
    );

    let pubsub_pub = RoutedPubSub::install(Arc::clone(&publisher.registry)).await;
    let pubsub_sub = RoutedPubSub::install(Arc::clone(&subscriber.registry)).await;

    let topic = topic_key("icanact/unannounced-restart/telemetry");
    let type_hash: u64 = 0x00D3_2204;
    let received = Arc::new(AtomicUsize::new(0));
    let sub = {
        let received = Arc::clone(&received);
        pubsub_sub.subscribe_bytes(topic, type_hash, move |_bytes| {
            received.fetch_add(1, Ordering::SeqCst);
        })
    };

    let interest_name = interest_actor_name(topic, &subscriber_id);
    assert!(
        wait_until(Duration::from_secs(10), async || {
            publisher
                .registry
                .lookup_actor(&interest_name)
                .await
                .is_some()
        })
        .await,
        "publisher never learned the subscriber's interest actor"
    );
    sleep(Duration::from_millis(300)).await;

    let publish = |body: &'static [u8]| {
        pubsub_pub.publish_bytes(
            topic,
            type_hash,
            bytes::Bytes::from_static(body),
            PubSubScope::ClusterWide,
            PubSubDeliveryPolicy::default(),
        )
    };
    let baseline = publish(b"baseline")?;
    assert!(
        baseline.remote_enqueued >= 1,
        "baseline publish never reached the subscriber (stats={baseline:?})"
    );
    // The subscriber's process exits. The publisher is told nothing and still
    // holds a pooled connection to an address nobody is listening on.
    drop(sub);
    drop(pubsub_sub);
    subscriber.shutdown_and_wait().await;

    // A new process, same identity, same address -- and it dials back, exactly
    // as the restarted relay did.
    let restarted = start_node(subscriber_addr, subscriber_keys, fast_config()).await?;
    let pubsub_restarted = RoutedPubSub::install(Arc::clone(&restarted.registry)).await;
    let received_after = Arc::new(AtomicUsize::new(0));
    let _sub_after = {
        let received_after = Arc::clone(&received_after);
        pubsub_restarted.subscribe_bytes(topic, type_hash, move |_bytes| {
            received_after.fetch_add(1, Ordering::SeqCst);
        })
    };
    restarted.lookup_address(publisher_addr).await?;

    let recovered = wait_until(RECOVERY_BUDGET, async || {
        publish(b"after-restart")
            .map(|stats| stats.remote_enqueued >= 1)
            .unwrap_or(false)
    })
    .await;

    let stats = publish(b"final")?;
    assert!(
        recovered,
        "publisher never recovered after the subscriber restarted unannounced; \
         last publish was {stats:?}"
    );

    assert!(
        wait_until(Duration::from_secs(10), async || {
            received_after.load(Ordering::SeqCst) >= 1
        })
        .await,
        "publisher reported an enqueued frame but the restarted subscriber never \
         received anything"
    );

    restarted.shutdown_and_wait().await;
    publisher.shutdown_and_wait().await;
    Ok(())
}

/// A `SelectedPeers` publisher recovers from a socket close.
///
/// This exercises the deployed scope. The collector is started with
/// `--public-relay-peer-ids` and publishes with
/// `PubSubScope::SelectedPeers(relay_peer_ids)` -- a fixed list of relay
/// identities, not a discovered one.
///
/// `SelectedPeers` takes a different routing path from `ClusterWide`, so this is
/// retained as a scope-specific recovery guard. It does not claim that this path
/// reproduced or diagnosed the field flap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_peers_publisher_recovers_after_socket_close() -> icanact_remote::Result<()> {
    const RECOVERY_BUDGET: Duration = Duration::from_secs(30);

    let publisher_keys = KeyPair::new_for_testing("selected_peers_publisher");
    let subscriber_keys = KeyPair::new_for_testing("selected_peers_subscriber");
    let subscriber_id = subscriber_keys.peer_id();

    let publisher = start_node(
        "127.0.0.1:0".parse().unwrap(),
        publisher_keys,
        fast_config(),
    )
    .await?;
    let subscriber = start_node(
        "127.0.0.1:0".parse().unwrap(),
        subscriber_keys,
        fast_config(),
    )
    .await?;
    let subscriber_addr = subscriber.registry.bind_addr;

    publisher.lookup_address(subscriber_addr).await?;
    assert!(
        wait_until(Duration::from_secs(10), async || {
            publisher
                .registry
                .has_connection_to_peer(&subscriber_id)
                .await
        })
        .await,
        "seed dial never connected, so everything below would be vacuous"
    );

    let pubsub_pub = RoutedPubSub::install(Arc::clone(&publisher.registry)).await;
    let pubsub_sub = RoutedPubSub::install(Arc::clone(&subscriber.registry)).await;

    let topic = topic_key("icanact/selected-peers/telemetry");
    let type_hash: u64 = 0x00D3_2205;
    let received = Arc::new(AtomicUsize::new(0));
    let _sub = {
        let received = Arc::clone(&received);
        pubsub_sub.subscribe_bytes(topic, type_hash, move |_bytes| {
            received.fetch_add(1, Ordering::SeqCst);
        })
    };
    sleep(Duration::from_millis(300)).await;

    // The deployed scope: a fixed relay identity list.
    let publish = |body: &'static [u8]| {
        pubsub_pub.publish_bytes(
            topic,
            type_hash,
            bytes::Bytes::from_static(body),
            PubSubScope::SelectedPeers(vec![subscriber_id.clone()]),
            PubSubDeliveryPolicy::default(),
        )
    };

    let baseline = publish(b"baseline")?;
    assert!(
        baseline.remote_enqueued >= 1,
        "baseline publish never reached the subscriber (stats={baseline:?})"
    );
    assert!(
        wait_until(Duration::from_secs(5), async || {
            received.load(Ordering::SeqCst) >= 1
        })
        .await,
        "baseline SelectedPeers frame was enqueued but never delivered"
    );
    let delivered_before = received.load(Ordering::SeqCst);

    // The socket dies. The subscriber stays up and subscribed throughout.
    let _ = publisher
        .registry
        .handle_peer_connection_failure(subscriber_addr, None)
        .await;
    assert!(
        wait_until(Duration::from_secs(5), async || {
            !publisher
                .registry
                .has_connection_to_peer(&subscriber_id)
                .await
        })
        .await,
        "the forced failure did not drop the publisher's connection"
    );

    let recovered = wait_until(RECOVERY_BUDGET, async || {
        let enqueued = publish(b"after-disconnect")
            .map(|stats| stats.remote_enqueued >= 1)
            .unwrap_or(false);
        enqueued && received.load(Ordering::SeqCst) > delivered_before
    })
    .await;

    let stats = publish(b"final")?;
    assert!(
        recovered,
        "a SelectedPeers publisher never re-established a connection to a \
         configured peer that stayed up and interested throughout; last publish \
         was {stats:?}"
    );
    assert!(
        received.load(Ordering::SeqCst) > delivered_before,
        "SelectedPeers recovery enqueued frames but delivered none after reconnect"
    );

    subscriber.shutdown_and_wait().await;
    publisher.shutdown_and_wait().await;
    Ok(())
}
