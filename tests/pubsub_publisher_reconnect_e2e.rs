//! A publisher that loses its socket to a live subscriber must recover.
//!
//! This is the shape of a three-day production outage. A collector process
//! published telemetry to two relays over routed pubsub. One relay restarted;
//! the collector observed the socket close, logged "will retry after interval",
//! and then published every five minutes for three days with
//! `remote_attempted=0 remote_route_miss=0 remote_transport_errors=2`.
//!
//! Those counters are the whole diagnosis. `remote_route_miss=0` means routing
//! resolved the destinations perfectly -- the subscriber was still known, still
//! interested, still named in the routing table. `remote_attempted=0` means no
//! frame was ever handed to a connection. The publish path found a subscriber it
//! could not reach and, per `PubSubRouter::publish_frame_to_next_hop`, counted
//! the miss and returned. Nothing in that path asks anything to re-establish the
//! connection, so the publisher stayed wedged until it was restarted by hand.
//!
//! The topology below preserves the two details that make the outage possible
//! and that the existing reconnect suite does not reproduce:
//!
//! 1. **The subscriber never dials the publisher.** The relay had no configured
//!    route back to the collector, so nothing inbound could heal the
//!    relationship. Tests that call `configure_peer` on both sides -- which is
//!    every reconnect test in this suite -- let the *other* side perform the
//!    reconnect and therefore cannot observe a broken publisher-side driver.
//! 2. **The subscriber stays up throughout.** The relay was healthy and serving
//!    other publishers for the entire outage; only this one publisher's socket
//!    was gone. So the failure is not "peer is down", it is "peer is up and I
//!    can't reach it and I never try again".

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

/// How long the publisher may take to notice and repair a dead route.
///
/// The production publisher had three days and a gossip interval measured in
/// seconds. This budget is enormous relative to the intervals configured below,
/// so a failure means no repair path exists rather than that one was slow.
const REPAIR_BUDGET: Duration = Duration::from_secs(20);

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

/// The publisher repairs its own route to a subscriber that never dials back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_recovers_route_to_live_subscriber_after_socket_close()
-> icanact_remote::Result<()> {
    let publisher_keys = KeyPair::new_for_testing("pubsub_reconnect_publisher");
    let subscriber_keys = KeyPair::new_for_testing("pubsub_reconnect_subscriber");
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

    // Seed-only, one-directional: exactly how every deployed service joins this
    // mesh. The subscriber is told nothing about the publisher and so has no
    // way to dial it.
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

    let topic = topic_key("icanact/pubsub-reconnect/telemetry");
    let type_hash: u64 = 0x00D3_2201;
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

    // Baseline: the route works before anything is broken.
    let baseline = pubsub_pub.publish_bytes(
        topic,
        type_hash,
        bytes::Bytes::from_static(b"baseline"),
        PubSubScope::ClusterWide,
        PubSubDeliveryPolicy::default(),
    )?;
    assert!(
        baseline.remote_enqueued >= 1,
        "baseline publish never reached the subscriber, so the assertion after \
         the disconnect would prove nothing (stats={baseline:?})"
    );

    // The publisher's socket dies. The subscriber is untouched: still running,
    // still subscribed, still advertising interest -- and still unable to dial.
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

    // Publish on a loop the way the collector did every five minutes. The
    // publisher must repair the route on its own.
    let repaired = wait_until(REPAIR_BUDGET, async || {
        pubsub_pub
            .publish_bytes(
                topic,
                type_hash,
                bytes::Bytes::from_static(b"after-disconnect"),
                PubSubScope::ClusterWide,
                PubSubDeliveryPolicy::default(),
            )
            .map(|stats| stats.remote_enqueued >= 1)
            .unwrap_or(false)
    })
    .await;

    let final_stats = pubsub_pub.publish_bytes(
        topic,
        type_hash,
        bytes::Bytes::from_static(b"final"),
        PubSubScope::ClusterWide,
        PubSubDeliveryPolicy::default(),
    )?;
    assert!(
        repaired,
        "publisher never re-established a route to a subscriber that stayed up \
         and stayed interested the whole time; last publish was {final_stats:?}. \
         `remote_route_miss=0` with `remote_transport_errors>0` is the production \
         signature: routing still names the subscriber, no connection exists, and \
         no code path dials it again"
    );

    assert!(
        wait_until(Duration::from_secs(5), async || {
            received.load(Ordering::SeqCst) >= 2
        })
        .await,
        "route reported repaired but the subscriber never received a post-\
         disconnect message"
    );

    subscriber.shutdown_and_wait().await;
    publisher.shutdown_and_wait().await;
    Ok(())
}
