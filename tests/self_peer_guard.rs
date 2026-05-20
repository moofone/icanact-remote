use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn registry_handle_self_peer_connect_is_noop() {
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        KeyPair::new_for_testing("integration-self-peer-guard").to_secret_key(),
        Some(GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..GossipConfig::default()
        }),
        BuilderTlsBootstrap,
    )
    .await
    .expect("registry handle");
    let local_peer_id = handle.registry.peer_id.clone();
    let local_addr = handle.registry.bind_addr;

    let self_peer = handle.add_peer(&local_peer_id).await;
    self_peer
        .connect(&local_addr)
        .await
        .expect("self peer connect should be a harmless no-op");
    sleep(Duration::from_millis(50)).await;

    let stats = handle.stats().await;
    assert_eq!(
        stats.active_peers, 0,
        "a registry must never count itself as an active remote peer"
    );
    assert!(
        handle
            .client()
            .lookup_connected_peer(&local_peer_id)
            .is_none(),
        "a registry must never keep a pooled connection to itself"
    );
    let gossip_state = handle.registry.gossip_state.lock().await;
    assert!(
        !gossip_state.peers.contains_key(&local_addr),
        "a registry must never insert its own bind address into remote gossip state"
    );
    drop(gossip_state);

    handle.shutdown().await;
}
