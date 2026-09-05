use icanact_remote::{BuilderTlsBootstrap, GossipRegistryHandle, SecretKey};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn qa_accepted_registry_can_bootstrap_after_snapshot_exceeds_frame_limit() {
    let config = icanact_remote::GossipConfig {
        gossip_interval: Duration::from_secs(3600),
        cleanup_interval: Duration::from_secs(3600),
        peer_supervisor_interval: Duration::from_secs(3600),
        peer_gossip_interval: None,
        connection_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        SecretKey::generate(),
        Some(config.clone()),
        BuilderTlsBootstrap,
    )
    .await
    .unwrap();
    let b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        SecretKey::generate(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await
    .unwrap();
    for i in 0..81 {
        a.register_with_metadata(
            format!("qa/snapshot/{i}"),
            a.registry.bind_addr,
            vec![7; 128 * 1024],
        )
        .await
        .unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        a.add_peer(&b.registry.peer_id)
            .await
            .connect(&b.registry.bind_addr)
            .await
    })
    .await;
    a.shutdown().await;
    b.shutdown().await;
    assert!(
        matches!(result, Ok(Ok(_))),
        "accepted registry state must remain bootstrap-able, got {result:?}"
    );
}

#[tokio::test]
async fn qa_drop_handle_releases_installed_pubsub_and_registry() {
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        SecretKey::generate(),
        None,
        BuilderTlsBootstrap,
    )
    .await
    .unwrap();
    let pubsub = icanact_remote::pubsub::RoutedPubSub::install(handle.registry.clone()).await;
    let weak_registry = Arc::downgrade(&handle.registry);
    let weak_pubsub = Arc::downgrade(&pubsub);
    drop(pubsub);
    drop(handle);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let retained_registry = weak_registry.strong_count();
    let retained_pubsub = weak_pubsub.strong_count();
    if let Some(registry) = weak_registry.upgrade() {
        registry.shutdown().await;
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        retained_registry, 0,
        "dropping the owner must release registry resources"
    );
    assert_eq!(
        retained_pubsub, 0,
        "dropping the owner must release pubsub resources"
    );
}
