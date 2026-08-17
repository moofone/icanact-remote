//! Repeated connection churn must not permanently strand a publisher.
//!
//! The single clean disconnect/reconnect cycle recovers fine (see
//! `pubsub_publisher_reconnect_e2e`). The production outage did not follow one
//! clean cycle: a relay restarted underneath a long-lived collector that had
//! already accumulated peer state from connections in both directions, and the
//! collector was left publishing into `remote_transport_errors` for three days.
//!
//! The topology here reproduces that accumulated state rather than a pristine
//! pair. Both nodes dial each other, so each ends up holding entries for the
//! other under two different keys -- the advertised bind address it dialled, and
//! the ephemeral TCP source address of the connection it accepted. The deployed
//! collector demonstrably had both: the only peer it ever retried across three
//! days was `10.77.0.38:40184`, an ephemeral source port, not the relay's bind
//! address.
//!
//! Peer selection ranks a *live* alias above a merely *dialable* one
//! (`select_best_alias_per_identity`), and each identity gets exactly one gossip
//! slot per round. So which alias holds that slot when the connection dies
//! decides whether anything ever dials the peer again. This test flaps the link
//! repeatedly, through both of the disconnect entry points production took, and
//! requires the publisher to recover every single time.

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

    for round in 0..ROUNDS {
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
            publish(b"probe")
                .map(|stats| stats.remote_enqueued >= 1)
                .unwrap_or(false)
        })
        .await;

        let stats = publish(b"post-round")?;
        assert!(
            recovered,
            "round {round} (disconnect variant {}) never recovered a route to a \
             subscriber that stayed up and interested throughout; last publish \
             was {stats:?}. `remote_route_miss=0` with \
             `remote_transport_errors>0` is the production signature: routing \
             still names the subscriber, no connection exists, and nothing \
             dials it again",
            round % 4
        );
    }

    assert!(
        received.load(Ordering::SeqCst) >= ROUNDS,
        "publisher reported enqueued frames every round but the subscriber \
         received only {} of them",
        received.load(Ordering::SeqCst)
    );

    subscriber.shutdown_and_wait().await;
    publisher.shutdown_and_wait().await;
    Ok(())
}

/// The subscriber's process restarts and the publisher is told nothing.
///
/// This is the closest reproduction of the deployed sequence. Nothing in this
/// test synthesises a failure into the publisher: the subscriber's process
/// exits, a new one with the same identity binds the same address and dials
/// back, and the publisher has to work out on its own that the connection it
/// still holds is dead.
///
/// That distinction is the whole point. Every other test here calls
/// `handle_peer_connection_failure`, which is the publisher *already knowing*
/// its connection is gone -- it tears the pooled connection down and clears the
/// indices, so the peer entry is unambiguously disconnected and the next gossip
/// round redials it. The deployed collector took two minutes and twelve seconds
/// to reach that point, and while a stale pooled connection is still indexed for
/// the peer, `has_connection_by_peer_id` answers yes. Gossip asks exactly that
/// question before dialing (`peer_has_live_connection` via
/// `should_attempt_outbound_dial`), so a stale-but-indexed connection suppresses
/// the redial, while routed pubsub -- which needs a connection it can actually
/// write to -- gets nothing and counts `remote_transport_errors`. Publisher
/// believes it is connected; publishes go nowhere; no code path disagrees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_recovers_when_subscriber_process_restarts_unannounced()
-> icanact_remote::Result<()> {
    // The deployed publisher needed 2m12s just to notice. This budget is far
    // beyond any configured interval below, so a failure means it never
    // recovers rather than that it was slow.
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
         last publish was {stats:?}. This is the deployed failure: the publisher \
         holds a stale pooled connection, so gossip believes the peer is \
         connected and never redials, while routed pubsub cannot obtain a \
         writable connection and counts transport errors forever"
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
/// This is the deployed configuration. The collector is started with
/// `--public-relay-peer-ids` and publishes with
/// `PubSubScope::SelectedPeers(relay_peer_ids)` -- a fixed list of relay
/// identities, not a discovered one.
///
/// That scope takes a different path through `PubSubRouter::publish_to_scope`
/// than every other test in this suite. `ClusterWide` resolves destinations from
/// the route table built by interest-actor discovery, and that discovery -- the
/// `refresh_control_plane` tick, the actor lookups behind it -- is itself
/// connection-establishing traffic. A `ClusterWide` publisher therefore has a
/// standing reason to keep talking to its subscribers, which is why every
/// `ClusterWide` test here recovers within a second.
///
/// `SelectedPeers` short-circuits all of it: each configured peer is mapped
/// straight to itself as a next hop and handed to `publish_frame_to_next_hop`.
/// There is no route table to miss (hence the deployed `remote_route_miss=0`)
/// and no interest discovery to re-establish anything. If the connection to a
/// configured peer is gone, publishing is the only thing that ever notices, and
/// publishing only counts the failure.
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
        publish(b"after-disconnect")
            .map(|stats| stats.remote_enqueued >= 1)
            .unwrap_or(false)
    })
    .await;

    let stats = publish(b"final")?;
    assert!(
        recovered,
        "a SelectedPeers publisher never re-established a connection to a \
         configured peer that stayed up and interested throughout; last publish \
         was {stats:?}. This is the deployed wedge: the peer list is fixed, so \
         routing always names the peer and never misses, but nothing in the \
         publish path -- and nothing else, because SelectedPeers needs no \
         interest discovery -- ever asks for the connection to be re-established"
    );

    subscriber.shutdown_and_wait().await;
    publisher.shutdown_and_wait().await;
    Ok(())
}

/// A publisher whose peer was learned from an inbound connection can still
/// re-establish it.
///
/// The remaining structural difference between the tests above and the deployed
/// collector is *which address the publisher holds for its peer*. Every test
/// above dials the subscriber's advertised bind address first, so the publisher
/// always has a dialable address on file and its gossip round redials happily.
///
/// The deployed collector did not. Across three days the only peer it ever
/// retried was `10.77.0.38:40184` -- an ephemeral TCP source port, which is what
/// a peer entry looks like when it was learned from a connection the peer opened
/// *to us* rather than one we opened to it. Such an entry is
/// `transport_source_keyed`, and `is_effectively_dialable` refuses it unless an
/// owner pin is current, so `select_best_alias_per_identity` drops it outright
/// once it is no longer live (`if !dialable && !live { continue; }`). The peer
/// then cannot appear in any gossip round, ever.
///
/// Meanwhile `PubSubScope::SelectedPeers` keeps naming that same peer by
/// identity, because its list is configuration rather than discovery. Routing
/// never misses, the connection never returns, and the only fix is a restart --
/// which re-runs `bootstrap_seed` and recreates the bind-address entry. That is
/// precisely the observed behaviour, including why restarting the collector
/// fixed it instantly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_recovers_peer_learned_only_from_inbound_connection() -> icanact_remote::Result<()>
{
    const RECOVERY_BUDGET: Duration = Duration::from_secs(30);

    let publisher_keys = KeyPair::new_for_testing("inbound_learned_publisher");
    let subscriber_keys = KeyPair::new_for_testing("inbound_learned_subscriber");
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
        subscriber_keys,
        fast_config(),
    )
    .await?;

    // Only the subscriber dials. The publisher therefore learns the subscriber
    // from the inbound connection's ephemeral source address and never holds
    // its advertised bind address -- exactly the state the deployed collector
    // was in.
    subscriber.lookup_address(publisher_addr).await?;
    assert!(
        wait_until(Duration::from_secs(10), async || {
            publisher
                .registry
                .has_connection_to_peer(&subscriber_id)
                .await
        })
        .await,
        "inbound dial never connected, so everything below would be vacuous"
    );

    let pubsub_pub = RoutedPubSub::install(Arc::clone(&publisher.registry)).await;
    let pubsub_sub = RoutedPubSub::install(Arc::clone(&subscriber.registry)).await;

    let topic = topic_key("icanact/inbound-learned/telemetry");
    let type_hash: u64 = 0x00D3_2206;
    let received = Arc::new(AtomicUsize::new(0));
    let _sub = {
        let received = Arc::clone(&received);
        pubsub_sub.subscribe_bytes(topic, type_hash, move |_bytes| {
            received.fetch_add(1, Ordering::SeqCst);
        })
    };
    sleep(Duration::from_millis(400)).await;

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

    // The connection dies. The subscriber stays up and subscribed, but -- like
    // the relay, which had no configured route back -- does not redial.
    let _ = publisher
        .registry
        .handle_peer_connection_failure_by_peer_id(&subscriber_id)
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
        publish(b"after-disconnect")
            .map(|stats| stats.remote_enqueued >= 1)
            .unwrap_or(false)
    })
    .await;

    let stats = publish(b"final")?;
    assert!(
        recovered,
        "the publisher never re-established a connection to a configured peer \
         it had only ever learned from an inbound connection; last publish was \
         {stats:?}. The peer entry is transport-source-keyed, so gossip target \
         selection discards it the moment it stops being live, while \
         SelectedPeers keeps naming it by identity forever"
    );

    subscriber.shutdown_and_wait().await;
    publisher.shutdown_and_wait().await;
    Ok(())
}
