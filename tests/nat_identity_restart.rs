use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_with_different_identity_is_treated_as_new_peer() -> icanact_remote::Result<()> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(300),
        connection_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        nat_role_reconnect_enabled: true,
        ..Default::default()
    };

    let keypair_b = KeyPair::new_for_testing("identity_change_public_b");
    let peer_id_b = keypair_b.peer_id();
    let handle_b = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_b,
        Some(config.clone()),
    )
    .await?;
    let node_b_addr = handle_b.registry.bind_addr;

    let keypair_a1 = KeyPair::new_for_testing("identity_change_private_a_old");
    let peer_id_a1 = keypair_a1.peer_id();
    let handle_a1 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_a1,
        Some(config.clone()),
    )
    .await?;

    let peer_b = handle_a1.add_peer(&peer_id_b).await;
    peer_b.connect(&node_b_addr).await?;
    sleep(Duration::from_secs(1)).await;

    handle_a1
        .register_with_priority(
            "identity_old_actor".to_string(),
            "127.0.0.1:9301".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;

    for _ in 0..30 {
        if handle_b.lookup("identity_old_actor").await.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(handle_b.lookup("identity_old_actor").await.is_some());

    handle_a1.shutdown().await;
    for _ in 0..40 {
        if handle_b.stats().await.active_peers == 0 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let keypair_a2 = KeyPair::new_for_testing("identity_change_private_a_new");
    let peer_id_a2 = keypair_a2.peer_id();
    assert_ne!(peer_id_a1, peer_id_a2);
    let handle_a2 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_a2,
        Some(config.clone()),
    )
    .await?;

    let peer_b = handle_a2.add_peer(&peer_id_b).await;
    peer_b.connect(&node_b_addr).await?;
    sleep(Duration::from_secs(1)).await;

    handle_a2
        .register_with_priority(
            "identity_new_actor".to_string(),
            "127.0.0.1:9302".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;

    for _ in 0..30 {
        if handle_b.lookup("identity_new_actor").await.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(handle_b.lookup("identity_new_actor").await.is_some());
    assert_eq!(handle_b.stats().await.active_peers, 1);

    handle_a2.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}
