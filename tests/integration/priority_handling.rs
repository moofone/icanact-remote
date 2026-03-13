use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use std::time::{Duration, Instant};
use tokio::time::sleep;

async fn create_test_registry(
    bind_addr: &str,
    keypair: KeyPair,
    config: Option<GossipConfig>,
) -> GossipRegistryHandle {
    GossipRegistryHandle::new_with_transport_stack(bind_addr.parse().unwrap(), keypair.to_secret_key(), config, icanact_remote::BuilderTlsBootstrap)
        .await
        .unwrap()
}

async fn connect_bidirectional(a: &GossipRegistryHandle, b: &GossipRegistryHandle) {
    let b_id = b.registry.peer_id.clone();
    let a_id = a.registry.peer_id.clone();

    let peer_b = a.add_peer(&b_id).await;
    peer_b.connect(&b.registry.bind_addr).await.unwrap();

    let peer_a = b.add_peer(&a_id).await;
    peer_a.connect(&a.registry.bind_addr).await.unwrap();
}

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
async fn test_priority_registration_timing() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        immediate_propagation_enabled: true,
        ..Default::default()
    };

    let registry = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_single"),
        Some(config),
    )
    .await;

    let start_normal = Instant::now();
    registry
        .register_with_priority(
            "normal_priority_actor".to_string(),
            "127.0.0.1:9001".parse().unwrap(),
            RegistrationPriority::Normal,
        )
        .await
        .unwrap();
    let normal_registration_time = start_normal.elapsed();

    let start_immediate = Instant::now();
    registry
        .register_with_priority(
            "immediate_priority_actor".to_string(),
            "127.0.0.1:9002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    let immediate_registration_time = start_immediate.elapsed();

    let normal_actor = registry.lookup("normal_priority_actor").await.unwrap();
    let immediate_actor = registry.lookup("immediate_priority_actor").await.unwrap();

    assert_eq!(normal_actor.location.priority, RegistrationPriority::Normal);
    assert_eq!(
        immediate_actor.location.priority,
        RegistrationPriority::Immediate
    );

    println!(
        "normal={:?} immediate={:?}",
        normal_registration_time, immediate_registration_time
    );

    registry.shutdown().await;
}

#[tokio::test]
async fn test_two_node_gossip_propagation() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        immediate_propagation_enabled: true,
        bootstrap_readiness_timeout: Duration::from_secs(5),
        bootstrap_readiness_check_interval: Duration::from_millis(50),
        ..Default::default()
    };

    let node1 = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_node1"),
        Some(config.clone()),
    )
    .await;
    let node2 = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_node2"),
        Some(config),
    )
    .await;

    connect_bidirectional(&node1, &node2).await;
    sleep(Duration::from_millis(300)).await;

    node1
        .register_with_priority(
            "test_actor_normal".to_string(),
            "127.0.0.1:9001".parse().unwrap(),
            RegistrationPriority::Normal,
        )
        .await
        .unwrap();

    assert!(
        wait_for_lookup(&node2, "test_actor_normal", Duration::from_secs(5))
            .await
            .is_some()
    );

    node1.shutdown().await;
    node2.shutdown().await;
}

#[tokio::test]
async fn test_immediate_vs_normal_priority() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(1000),
        immediate_propagation_enabled: true,
        urgent_gossip_fanout: 1,
        bootstrap_readiness_timeout: Duration::from_secs(5),
        bootstrap_readiness_check_interval: Duration::from_millis(50),
        ..Default::default()
    };

    let node1 = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_compare_node1"),
        Some(config.clone()),
    )
    .await;
    let node2 = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_compare_node2"),
        Some(config),
    )
    .await;

    connect_bidirectional(&node1, &node2).await;
    sleep(Duration::from_millis(300)).await;

    let normal_start = Instant::now();
    node1
        .register_with_priority(
            "normal_actor".to_string(),
            "127.0.0.1:9001".parse().unwrap(),
            RegistrationPriority::Normal,
        )
        .await
        .unwrap();
    let normal_seen = wait_for_lookup(&node2, "normal_actor", Duration::from_secs(4))
        .await
        .map(|_| normal_start.elapsed());

    let immediate_start = Instant::now();
    node1
        .register_with_priority(
            "immediate_actor".to_string(),
            "127.0.0.1:9002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    let immediate_seen = wait_for_lookup(&node2, "immediate_actor", Duration::from_secs(4))
        .await
        .map(|_| immediate_start.elapsed());

    assert!(normal_seen.is_some() || immediate_seen.is_some());

    if let Some(v) = normal_seen {
        println!("normal propagation: {:?}", v);
    }
    if let Some(v) = immediate_seen {
        println!("immediate propagation: {:?}", v);
    }

    node1.shutdown().await;
    node2.shutdown().await;
}

#[tokio::test]
async fn test_immediate_priority_config() {
    let registry_disabled = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_cfg_disabled"),
        Some(GossipConfig {
            immediate_propagation_enabled: false,
            ..Default::default()
        }),
    )
    .await;

    registry_disabled
        .register_with_priority(
            "immediate_actor".to_string(),
            "127.0.0.1:9001".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    let lookup = registry_disabled.lookup("immediate_actor").await.unwrap();
    assert_eq!(lookup.location.priority, RegistrationPriority::Immediate);
    registry_disabled.shutdown().await;

    let registry_enabled = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_cfg_enabled"),
        Some(GossipConfig {
            immediate_propagation_enabled: true,
            urgent_gossip_fanout: 2,
            ..Default::default()
        }),
    )
    .await;

    registry_enabled
        .register_with_priority(
            "immediate_actor2".to_string(),
            "127.0.0.1:9002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    let lookup2 = registry_enabled.lookup("immediate_actor2").await.unwrap();
    assert_eq!(lookup2.location.priority, RegistrationPriority::Immediate);
    registry_enabled.shutdown().await;
}
