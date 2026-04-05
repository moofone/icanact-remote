use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use std::time::{Duration, Instant};
use tokio::time::sleep;

async fn wait_for_lookup(
    handle: &GossipRegistryHandle,
    actor_name: &str,
    timeout: Duration,
) -> Option<icanact_remote::RemoteActorRef> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(actor) = handle.lookup(actor_name).await {
            return Some(actor);
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn test_multi_node_gossip_propagation() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let node1_keypair = KeyPair::new_for_testing("gossip_prop_node1");
    let node1_id = node1_keypair.peer_id();
    let node1 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        node1_keypair.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let node2_keypair = KeyPair::new_for_testing("gossip_prop_node2");
    let node2_id = node2_keypair.peer_id();
    let node2 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        node2_keypair.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let node3_keypair = KeyPair::new_for_testing("gossip_prop_node3");
    let node3 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        node3_keypair.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let peer1_from_2 = node2.add_peer(&node1_id).await;
    peer1_from_2.connect(&node1.registry.bind_addr).await?;
    let peer1_from_3 = node3.add_peer(&node1_id).await;
    peer1_from_3.connect(&node1.registry.bind_addr).await?;
    let peer2_from_3 = node3.add_peer(&node2_id).await;
    peer2_from_3.connect(&node2.registry.bind_addr).await?;

    sleep(Duration::from_millis(300)).await;

    node1
        .register_urgent(
            "actor1".to_string(),
            "127.0.0.1:9001".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node2
        .register_urgent(
            "actor2".to_string(),
            "127.0.0.1:9002".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node3
        .register_urgent(
            "actor3".to_string(),
            "127.0.0.1:9003".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;

    for node in [&node1, &node2, &node3] {
        assert!(
            wait_for_lookup(node, "actor1", Duration::from_secs(3))
                .await
                .is_some()
        );
        assert!(
            wait_for_lookup(node, "actor2", Duration::from_secs(3))
                .await
                .is_some()
        );
        assert!(
            wait_for_lookup(node, "actor3", Duration::from_secs(3))
                .await
                .is_some()
        );
    }

    let stats1 = node1.stats().await;
    let stats2 = node2.stats().await;
    let stats3 = node3.stats().await;

    assert_eq!(stats1.known_actors, 3);
    assert_eq!(stats2.known_actors, 3);
    assert_eq!(stats3.known_actors, 3);

    node1.shutdown().await;
    node2.shutdown().await;
    node3.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_actor_update_propagation() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let node1_keypair = KeyPair::new_for_testing("gossip_prop_update_node1");
    let node1_id = node1_keypair.peer_id();
    let node1 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        node1_keypair.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let node2_keypair = KeyPair::new_for_testing("gossip_prop_update_node2");
    let node2 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        node2_keypair.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let peer1_from_2 = node2.add_peer(&node1_id).await;
    peer1_from_2.connect(&node1.registry.bind_addr).await?;
    sleep(Duration::from_millis(300)).await;

    node1
        .register_urgent(
            "actor1".to_string(),
            "127.0.0.1:9001".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;

    let actor = wait_for_lookup(&node2, "actor1", Duration::from_secs(3))
        .await
        .ok_or("actor1 did not propagate")?;
    assert_eq!(actor.location.address, "127.0.0.1:9001");

    node1
        .register_urgent(
            "actor1".to_string(),
            "127.0.0.1:9999".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;

    let updated = wait_for_lookup(&node2, "actor1", Duration::from_secs(3))
        .await
        .ok_or("updated actor1 did not propagate")?;
    assert_eq!(updated.location.address, "127.0.0.1:9999");

    node1.shutdown().await;
    node2.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_actor_removal_propagation() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let node1_keypair = KeyPair::new_for_testing("gossip_prop_remove_node1");
    let node1_id = node1_keypair.peer_id();
    let node1 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        node1_keypair.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let node2_keypair = KeyPair::new_for_testing("gossip_prop_remove_node2");
    let node2 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        node2_keypair.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let peer1_from_2 = node2.add_peer(&node1_id).await;
    peer1_from_2.connect(&node1.registry.bind_addr).await?;
    sleep(Duration::from_millis(300)).await;

    node1
        .register_urgent(
            "actor1".to_string(),
            "127.0.0.1:9001".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    assert!(
        wait_for_lookup(&node2, "actor1", Duration::from_secs(3))
            .await
            .is_some()
    );

    node1.unregister("actor1").await?;
    sleep(Duration::from_millis(400)).await;
    assert!(node2.lookup("actor1").await.is_none());

    node1.shutdown().await;
    node2.shutdown().await;
    Ok(())
}
