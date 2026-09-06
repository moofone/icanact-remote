use icanact_remote::{BuilderTlsBootstrap, GossipRegistryHandle, SecretKey};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn qa_accepted_registry_can_bootstrap_after_snapshot_exceeds_frame_limit() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
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
    let names: Vec<String> = (0..81).map(|i| format!("qa/snapshot/{i}")).collect();
    for name in &names {
        a.register_with_metadata(name.clone(), a.registry.bind_addr, vec![7; 128 * 1024])
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
    assert!(
        matches!(result, Ok(Ok(_))),
        "accepted registry state must remain bootstrap-able, got {result:?}"
    );
    for name in &names {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if b.lookup(name).await.is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("peer B must observe accepted actor {name}"));
    }
    a.shutdown().await;
    b.shutdown().await;
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

#[tokio::test]
async fn qa_repeated_create_install_drop_releases_registry_pubsub() {
    for cycle in 0..1000 {
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
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak_registry.strong_count() != 0 || weak_pubsub.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("create/install/drop cycle {cycle} leaked registry/pubsub"));
    }
}
