use icanact_remote::*;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::time::sleep;

const PEER_INIT_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const PEER_INIT_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const PEER_INIT_WORKERS: usize = 4;

async fn wait_for_active_peers(
    handle: &GossipRegistryHandle,
    expected: usize,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    loop {
        if handle.stats().await.active_peers >= expected {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn run_peer_init_test<F, Fut>(name: &'static str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(format!("peer-init-test-{}", name))
        .stack_size(PEER_INIT_THREAD_STACK_SIZE)
        .spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(PEER_INIT_WORKERS)
                .thread_stack_size(PEER_INIT_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build peer init test runtime");
            runtime.block_on(test());
        })
        .expect("failed to spawn peer init test thread");

    handle.join().expect("peer init test panicked");
}

/// Test the proposed fix: Allow specifying peer names when initializing
async fn test_peer_initialization_with_names_inner() {
    println!("🧪 Testing proposed peer initialization with names");

    let node1_addr = "127.0.0.1:0".parse().unwrap();
    let node2_addr = "127.0.0.1:0".parse().unwrap();
    let node3_addr = "127.0.0.1:0".parse().unwrap();

    let node1_keypair = KeyPair::new_for_testing("node1");
    let node2_keypair = KeyPair::new_for_testing("node2");
    let node3_keypair = KeyPair::new_for_testing("node3");

    // Use default config which reads from ICANACT_GOSSIP_INTERVAL_MS
    let config1 = GossipConfig {
        key_pair: Some(node1_keypair.clone()),
        enable_peer_discovery: true, // Enable for service discovery
        gossip_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let config2 = GossipConfig {
        key_pair: Some(node2_keypair.clone()),
        enable_peer_discovery: true, // Enable for service discovery
        gossip_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let config3 = GossipConfig {
        key_pair: Some(node3_keypair.clone()),
        enable_peer_discovery: true, // Enable for service discovery
        gossip_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let handle1 = GossipRegistryHandle::new_with_transport_stack(
        node1_addr,
        node1_keypair.to_secret_key(),
        Some(config1),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    // Get actual addresses after port assignment
    let node1_actual_addr = handle1.registry.bind_addr;

    let handle2 = GossipRegistryHandle::new_with_transport_stack(
        node2_addr,
        node2_keypair.to_secret_key(),
        Some(config2),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let node2_actual_addr = handle2.registry.bind_addr;

    let handle3 = GossipRegistryHandle::new_with_transport_stack(
        node3_addr,
        node3_keypair.to_secret_key(),
        Some(config3),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let node3_actual_addr = handle3.registry.bind_addr;

    // Now add peers with correct names
    // Node1 knows Node2 and Node3
    let peer2_from_1 = handle1.add_peer(&node2_keypair.peer_id()).await;
    let peer3_from_1 = handle1.add_peer(&node3_keypair.peer_id()).await;
    peer2_from_1.connect(&node2_actual_addr).await.unwrap();
    peer3_from_1.connect(&node3_actual_addr).await.unwrap();

    // Node2 knows Node1 and Node3
    let peer1_from_2 = handle2.add_peer(&node1_keypair.peer_id()).await;
    let peer3_from_2 = handle2.add_peer(&node3_keypair.peer_id()).await;
    peer1_from_2.connect(&node1_actual_addr).await.unwrap();
    peer3_from_2.connect(&node3_actual_addr).await.unwrap();

    // Node3 knows Node1 and Node2
    let peer1_from_3 = handle3.add_peer(&node1_keypair.peer_id()).await;
    let peer2_from_3 = handle3.add_peer(&node2_keypair.peer_id()).await;
    peer1_from_3.connect(&node1_actual_addr).await.unwrap();
    peer2_from_3.connect(&node2_actual_addr).await.unwrap();

    // Wait for connections to become active on all nodes before registering actors.
    assert!(
        wait_for_active_peers(&handle1, 2, Duration::from_secs(10)).await,
        "node1 should have 2 active peers"
    );
    assert!(
        wait_for_active_peers(&handle2, 2, Duration::from_secs(10)).await,
        "node2 should have 2 active peers"
    );
    assert!(
        wait_for_active_peers(&handle3, 2, Duration::from_secs(10)).await,
        "node3 should have 2 active peers"
    );

    // Register actors using real bind addresses with immediate priority to reduce gossip flake.
    handle1
        .register_with_priority(
            "service1".to_string(),
            handle1.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    handle2
        .register_with_priority(
            "service2".to_string(),
            handle2.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    handle3
        .register_with_priority(
            "service3".to_string(),
            handle3.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    // Trigger immediate gossip from all nodes to prime propagation.
    let _ = handle1.registry.trigger_immediate_gossip().await;
    let _ = handle2.registry.trigger_immediate_gossip().await;
    let _ = handle3.registry.trigger_immediate_gossip().await;

    // Wait for gossip (minimal initial sleep).
    sleep(Duration::from_millis(1000)).await;

    // Test discovery from all nodes with retry
    println!("\n🔍 Testing full mesh discovery:");

    let services = ["service1", "service2", "service3"];
    let handles = [
        (&handle1, "node1"),
        (&handle2, "node2"),
        (&handle3, "node3"),
    ];

    let start = Instant::now();
    loop {
        let mut all_found = true;
        for (handle, _) in &handles {
            for service in services {
                if handle.lookup(service).await.is_none() {
                    all_found = false;
                    break;
                }
            }
            if !all_found {
                break;
            }
        }

        if all_found || start.elapsed() >= Duration::from_secs(20) {
            break;
        }

        for (handle, _) in &handles {
            let _ = handle.registry.trigger_immediate_gossip().await;
        }
        sleep(Duration::from_millis(200)).await;
    }

    // Each node should discover all other services.
    for (handle, node_name) in handles {
        println!("\nFrom {}:", node_name);
        for service in services {
            let found = handle.lookup(service).await.is_some();
            println!("  lookup('{}') = {}", service, found);
            assert!(found, "{} should discover {}", node_name, service);
        }
    }

    // Cleanup
    handle1.shutdown().await;
    handle2.shutdown().await;
    handle3.shutdown().await;
}

#[test]
fn test_peer_initialization_with_names() {
    run_peer_init_test("with-names", test_peer_initialization_with_names_inner);
}
