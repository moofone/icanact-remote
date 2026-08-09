//! Regression coverage for registry gossip quiescence.
//!
//! Transport send-time metadata must never rewrite an actor registration's
//! conflict-resolution version. Otherwise an unchanged actor becomes newer on
//! every hop and a peer that keeps asking for sync turns ordinary retries into
//! a self-sustaining registry storm.

mod common;

use common::{DynError, connect_bidirectional, create_tls_node, wait_for_condition};
use icanact_remote::GossipConfig;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_transport_timing_preserves_registration_version() -> Result<(), DynError> {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(2),
        cleanup_interval: Duration::from_secs(3_600),
        peer_retry_interval: Duration::from_secs(3_600),
        peer_supervisor_interval: Duration::from_secs(3_600),
        immediate_propagation_enabled: false,
        enable_peer_discovery: false,
        ..Default::default()
    };
    let source = create_tls_node(config.clone()).await?;
    let sink = create_tls_node(config).await?;
    connect_bidirectional(&source, &sink).await?;

    // Wait for one periodic exchange, then register immediately after that
    // boundary. The next delta send is therefore more than one second later,
    // making a send-time rewrite of the second-granularity wall clock
    // deterministic rather than scheduler-sensitive.
    let stats_before = source.stats().await;
    let exchanges_before = stats_before.delta_exchanges + stats_before.full_sync_exchanges;
    assert!(
        wait_for_condition(Duration::from_secs(5), || async {
            let stats = source.stats().await;
            stats.delta_exchanges + stats.full_sync_exchanges > exchanges_before
        })
        .await,
        "precondition: source must complete a periodic gossip exchange"
    );

    let actor_name = "registry/quiescence/version-stability";
    source
        .register(actor_name.to_string(), source.registry.bind_addr)
        .await?;
    let registered = source
        .registry
        .lookup_actor(actor_name)
        .await
        .expect("source owns the actor it just registered");

    assert!(
        wait_for_condition(Duration::from_secs(6), || async {
            sink.registry.lookup_actor(actor_name).await.is_some()
        })
        .await,
        "the actor registration must cross the real periodic delta boundary"
    );
    let received = sink
        .registry
        .lookup_actor(actor_name)
        .await
        .expect("sink observed the actor");

    assert_eq!(
        received.wall_clock_time, registered.wall_clock_time,
        "transport timing must not turn an unchanged actor into a newer registry version"
    );
    assert_eq!(
        received.local_registration_time, registered.local_registration_time,
        "the original registration timestamp is immutable actor metadata"
    );

    source.shutdown().await;
    sink.shutdown().await;
    Ok(())
}
