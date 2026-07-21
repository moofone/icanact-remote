//! Delivery-level regression coverage for R6(a): the routed-pubsub
//! `note_interest` TOCTOU (`src/pubsub.rs`). `tests/interest_advertise_...`
//! unit coverage in `src/pubsub.rs` (`final_advertised_state_matches_final_...`)
//! exercises the bug at the `InterestState`/registry level; this test proves
//! the fix end-to-end across two real `GossipRegistryHandle` nodes: a
//! subscriber node whose topic churns 0 -> 1 local subscribers while its
//! register/unregister registry calls are under (forced) contention must
//! still end up receiving a remote publisher's message for that topic, not
//! silently stop routing it.
//!
//! Node B subscribes to a topic, then its last subscriber leaves and a new
//! one immediately arrives (mirroring the "last subscriber leaves, new
//! subscriber appears" scenario from the bug report). The interest-dispatch
//! test hook (`icanact_remote::test_helpers::install_pubsub_interest_dispatch_hook`)
//! forces the stale unregister's registry call to *complete after* the
//! resubscribe's register call — the exact out-of-order completion the bug
//! allowed to win. Node A then publishes to the topic with cluster-wide
//! scope (real interest-actor gossip discovery via `refresh_control_plane`,
//! not a hand-fed peer list) and the test asserts B's live subscriber
//! actually receives it.

use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PubSubDeliveryPolicy,
    PubSubScope, RoutedPubSub, topic_key,
};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::time::{Instant, sleep};

static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn churn_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(40),
        cleanup_interval: Duration::from_millis(100),
        peer_retry_interval: Duration::from_millis(50),
        peer_supervisor_interval: Duration::from_millis(25),
        connection_timeout: Duration::from_millis(200),
        response_timeout: Duration::from_millis(200),
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

/// Mirrors the crate-private `pubsub::interest_name` wire format
/// (`src/pubsub.rs`).
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
        sleep(Duration::from_millis(25)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_publisher_reaches_topic_that_churned_zero_to_one_under_registry_contention()
-> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    icanact_remote::test_helpers::clear_pubsub_interest_dispatch_hook();

    let a_keypair = KeyPair::new_for_testing("interest_convergence_delivery_a");
    let b_keypair = KeyPair::new_for_testing("interest_convergence_delivery_b");
    let a_peer_id = a_keypair.peer_id();
    let b_peer_id = b_keypair.peer_id();

    let a = start_node("127.0.0.1:0".parse().unwrap(), a_keypair, churn_config()).await?;
    let b = start_node("127.0.0.1:0".parse().unwrap(), b_keypair, churn_config()).await?;
    let a_addr = a.registry.bind_addr;
    let b_addr = b.registry.bind_addr;

    configure_required_peer(&a, &b_peer_id, b_addr).await;
    configure_required_peer(&b, &a_peer_id, a_addr).await;
    assert!(
        wait_for_pair_connection(&a, &b, Duration::from_secs(5)).await,
        "nodes A and B failed to establish a connection"
    );

    let pubsub_a = RoutedPubSub::install(Arc::clone(&a.registry)).await;
    let pubsub_b = RoutedPubSub::install(Arc::clone(&b.registry)).await;

    let topic = topic_key("icanact/interest-convergence/delivery-under-churn");
    let type_hash: u64 = 0x00D3_1109;
    let interest_name = interest_actor_name(topic, &b_peer_id);

    let received = Arc::new(AtomicUsize::new(0));
    let last_payload: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sub1 = {
        let received = Arc::clone(&received);
        let last_payload = Arc::clone(&last_payload);
        pubsub_b.subscribe_bytes(topic, type_hash, move |bytes| {
            received.fetch_add(1, Ordering::SeqCst);
            *last_payload.lock().unwrap() = bytes.to_vec();
        })
    };

    // Baseline: A must be able to discover and reach B's interest actor
    // before the churn phase, so the churn phase is the only variable.
    assert!(
        wait_for_interest_location(&a, &interest_name, Duration::from_secs(5))
            .await
            .is_some(),
        "node A never learned node B's routed-pubsub interest actor location"
    );
    // Give `refresh_control_plane` (ticks every `CONTROL_PLANE_INTERVAL`)
    // a few rounds to turn the discovered location into a route.
    sleep(Duration::from_millis(300)).await;

    let baseline_stats = pubsub_a.publish_bytes(
        topic,
        type_hash,
        bytes::Bytes::from_static(b"baseline"),
        PubSubScope::ClusterWide,
        PubSubDeliveryPolicy::default(),
    )?;
    assert!(
        baseline_stats.remote_enqueued >= 1,
        "baseline publish before churn must find a route to B (stats={baseline_stats:?})"
    );
    assert!(
        {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut ok = false;
            while Instant::now() < deadline {
                if received.load(Ordering::SeqCst) >= 1 {
                    ok = true;
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
            ok
        },
        "baseline delivery before churn never reached B's subscriber"
    );

    // Force the interleaving: pause the stale unregister's registry call
    // until the resubscribe's register call has already gone through.
    let reached_unregister = Arc::new(tokio::sync::Notify::new());
    let release_unregister = Arc::new(tokio::sync::Notify::new());
    {
        let reached = Arc::clone(&reached_unregister);
        let release = Arc::clone(&release_unregister);
        icanact_remote::test_helpers::install_pubsub_interest_dispatch_hook(Arc::new(
            move |_topic_key, present| {
                let reached = Arc::clone(&reached);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    if !present {
                        reached.notify_one();
                        release.notified().await;
                    }
                }) as Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            },
        ));
    }

    // Last subscriber leaves: B's unregister dispatch parks right before
    // its registry call.
    drop(sub1);
    reached_unregister.notified().await;

    // A new subscriber arrives immediately after: B's register dispatch
    // (never delayed by the hook) runs to completion right away.
    let received2 = Arc::new(AtomicUsize::new(0));
    let sub2 = {
        let received2 = Arc::clone(&received2);
        pubsub_b.subscribe_bytes(topic, type_hash, move |_bytes| {
            received2.fetch_add(1, Ordering::SeqCst);
        })
    };
    sleep(Duration::from_millis(80)).await;

    // Now release the stale unregister so it completes last.
    release_unregister.notify_one();
    icanact_remote::test_helpers::clear_pubsub_interest_dispatch_hook();

    // Let B's interest converge back to "registered" and gossip/re-propagate
    // to A, and give A's control plane a few refresh ticks to pick the
    // route back up.
    assert!(
        wait_for_interest_location(&a, &interest_name, Duration::from_secs(5))
            .await
            .is_some(),
        "node A never re-learned node B's interest actor location after the churn \
         (advertised state stuck de-registered while B has a live subscriber)"
    );
    sleep(Duration::from_millis(300)).await;

    let post_churn_stats = pubsub_a.publish_bytes(
        topic,
        type_hash,
        bytes::Bytes::from_static(b"post-churn"),
        PubSubScope::ClusterWide,
        PubSubDeliveryPolicy::default(),
    )?;
    assert!(
        post_churn_stats.remote_enqueued >= 1,
        "post-churn publish must still find a route to B (stats={post_churn_stats:?})"
    );

    let delivered = {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut ok = false;
        while Instant::now() < deadline {
            if received2.load(Ordering::SeqCst) >= 1 {
                ok = true;
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        ok
    };
    assert!(
        delivered,
        "B has a live local subscriber after the 0->1 churn under registry contention, \
         but the remote publisher's post-churn message never arrived — the advertised \
         interest state silently stuck de-registered"
    );

    drop(sub2);
    a.shutdown_and_wait().await;
    b.shutdown_and_wait().await;
    Ok(())
}
