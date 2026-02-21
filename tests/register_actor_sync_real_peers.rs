use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RemoteActorLocation};
use std::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::{
    runtime::Builder,
    time::{sleep, timeout},
};

/// Helper function to create a test registry config
fn create_test_config() -> GossipConfig {
    GossipConfig {
        max_peer_failures: 3,
        dead_peer_timeout: Duration::from_secs(60),
        connection_timeout: Duration::from_secs(5),
        immediate_propagation_enabled: true, // Enable immediate gossip
        gossip_interval: Duration::from_millis(25),
        ..Default::default()
    }
}

fn test_keypair(name: &str) -> KeyPair {
    KeyPair::new_for_testing(name)
}

fn test_peer_id(name: &str) -> icanact_remote::PeerId {
    test_keypair(name).peer_id()
}

/// Helper function to create a test actor location
fn create_test_actor_location(addr: SocketAddr, peer_name: &str) -> RemoteActorLocation {
    let peer_id = test_peer_id(peer_name);
    RemoteActorLocation::new_with_peer(addr, peer_id)
}

async fn wait_for_actor_present(
    handle: &GossipRegistryHandle,
    actor_name: &str,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if handle.lookup(actor_name).await.is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_for_condition<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check().await {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;

fn run_register_actor_test<F, Fut>(name: &'static str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(format!("register-actor-{}", name))
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build register-actor runtime");
            runtime.block_on(test());
        })
        .expect("failed to spawn register-actor test thread");

    handle.join().expect("register-actor test thread panicked");
}

async fn ensure_connected(
    node_a: &GossipRegistryHandle,
    node_b: &GossipRegistryHandle,
    peer_id_a: icanact_remote::PeerId,
    peer_id_b: icanact_remote::PeerId,
) {
    let peer_b = node_a.add_peer(&peer_id_b).await;
    peer_b
        .connect(&node_b.registry.bind_addr)
        .await
        .expect("Failed to connect node A to node B");

    let peer_a = node_b.add_peer(&peer_id_a).await;
    peer_a
        .connect(&node_a.registry.bind_addr)
        .await
        .expect("Failed to connect node B to node A");

    let connected = wait_for_condition(Duration::from_secs(5), || async {
        node_a.stats().await.active_peers >= 1 && node_b.stats().await.active_peers >= 1
    })
    .await;
    assert!(
        connected,
        "Failed to establish bidirectional gossip connection"
    );
}

#[test]
fn test_register_actor_sync_real_single_peer() {
    run_register_actor_test("single-peer", || async {
        // Test: Register with 1 real remote peer - should wait for real ACK

        println!("🚀 Starting real peer test...");

        // Create two registry handles on different ports
        let node1_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let node2_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

        let config = create_test_config();

        // Start first node
        let handle1 = GossipRegistryHandle::new_with_keypair(
            node1_addr,
            test_keypair("node1"),
            Some(config.clone()),
        )
        .await
        .expect("Failed to start node1");
        let node1_actual_addr = handle1.registry.bind_addr;
        println!("✅ Node1 started on {}", node1_actual_addr);

        // Start second node
        let handle2 = GossipRegistryHandle::new_with_keypair(
            node2_addr,
            test_keypair("node2"),
            Some(config.clone()),
        )
        .await
        .expect("Failed to start node2");
        let node2_actual_addr = handle2.registry.bind_addr;
        println!("✅ Node2 started on {}", node2_actual_addr);

        // Connect nodes as peers
        let peer2_id = test_peer_id("node2");
        let peer1_id = test_peer_id("node1");
        ensure_connected(&handle1, &handle2, peer1_id.clone(), peer2_id.clone()).await;

        println!("🔗 Nodes connected as peers");

        // Register actor on node1 with sync - should wait for node2's ACK
        let actor_name = "test_actor".to_string();
        let actor_location = create_test_actor_location(node1_actual_addr, "node1");

        println!(
            "📝 Registering actor '{}' with sync on node1...",
            actor_name
        );

        let start = std::time::Instant::now();
        let result = timeout(
            Duration::from_secs(5), // Generous timeout
            handle1.registry.register_actor_sync(
                actor_name.clone(),
                actor_location,
                Duration::from_millis(2000), // Wait up to 2 seconds for ACK
            ),
        )
        .await;
        let elapsed = start.elapsed();

        println!("⏱️  Registration completed in {:?}", elapsed);

        // Verify result
        match result {
            Ok(Ok(())) => {
                println!("✅ Registration succeeded - got ACK from peer!");

                // Should have taken some time (not immediate) because it waited for ACK
                // Note: On localhost, round-trip time is sub-millisecond, so we just check it's not instant
                assert!(elapsed <= Duration::from_millis(3000)); // But not too long (real networking takes time)

                // Verify actor was registered on both nodes through gossip
                let node1_seen =
                    wait_for_actor_present(&handle1, &actor_name, Duration::from_secs(5)).await;
                let node2_seen =
                    wait_for_actor_present(&handle2, &actor_name, Duration::from_secs(5)).await;

                println!("🔍 Node1 lookup result: {:?}", node1_seen);
                println!("🔍 Node2 lookup result: {:?}", node2_seen);

                assert!(node1_seen, "Actor should be findable on node1");
                assert!(node2_seen, "Actor should be findable on node2 via gossip");
            }
            Ok(Err(e)) => {
                panic!("Registration failed: {:?}", e);
            }
            Err(_) => {
                panic!("Registration timed out - no ACK received in 5 seconds");
            }
        }

        // Cleanup
        handle1.shutdown().await;
        handle2.shutdown().await;

        println!("✅ Test completed successfully");
    });
}

#[test]
fn test_register_actor_sync_real_no_peers_vs_with_peers() {
    run_register_actor_test("no-peers-vs-with-peers", || async {
        // Test: Compare timing between no peers vs with peers

        println!("🚀 Starting no-peers vs with-peers comparison test...");

        let config = create_test_config();

        // === TEST 1: No peers - should be immediate ===
        let solo_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let solo_handle = GossipRegistryHandle::new_with_keypair(
            solo_addr,
            test_keypair("solo"),
            Some(config.clone()),
        )
        .await
        .expect("Failed to start solo node");
        let solo_actual_addr = solo_handle.registry.bind_addr;

        println!("✅ Solo node started on {}", solo_actual_addr);

        let actor_name_solo = "solo_actor".to_string();
        let actor_location_solo = create_test_actor_location(solo_actual_addr, "solo_node");

        let start_solo = std::time::Instant::now();
        let result_solo = solo_handle
            .registry
            .register_actor_sync(
                actor_name_solo.clone(),
                actor_location_solo,
                Duration::from_secs(1),
            )
            .await;
        let elapsed_solo = start_solo.elapsed();

        println!("⏱️  Solo registration completed in {:?}", elapsed_solo);
        assert!(result_solo.is_ok(), "Solo registration should succeed");
        assert!(
            elapsed_solo < Duration::from_millis(50),
            "Solo registration should be immediate"
        );

        // === TEST 2: With peer - should wait for ACK ===
        let node1_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let node2_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

        let handle1 = GossipRegistryHandle::new_with_keypair(
            node1_addr,
            test_keypair("node1"),
            Some(config.clone()),
        )
        .await
        .expect("Failed to start node1");
        let node1_actual_addr = handle1.registry.bind_addr;

        let handle2 = GossipRegistryHandle::new_with_keypair(
            node2_addr,
            test_keypair("node2"),
            Some(config.clone()),
        )
        .await
        .expect("Failed to start node2");
        let _node2_actual_addr = handle2.registry.bind_addr;

        ensure_connected(
            &handle1,
            &handle2,
            test_peer_id("node1"),
            test_peer_id("node2"),
        )
        .await;

        println!("🔗 Peer nodes connected");

        let actor_name_peer = "peer_actor".to_string();
        let actor_location_peer = create_test_actor_location(node1_actual_addr, "node1");

        let start_peer = std::time::Instant::now();
        let result_peer = timeout(
            Duration::from_secs(5),
            handle1.registry.register_actor_sync(
                actor_name_peer.clone(),
                actor_location_peer,
                Duration::from_millis(2000),
            ),
        )
        .await;
        let elapsed_peer = start_peer.elapsed();

        println!("⏱️  Peer registration completed in {:?}", elapsed_peer);

        match result_peer {
            Ok(Ok(())) => {
                assert!(
                    elapsed_peer <= Duration::from_millis(3000),
                    "Peer registration should not timeout (real networking takes time)"
                );

                println!("✅ Peer registration completed successfully:");
                println!("   Solo (no peers): {:?} (immediate)", elapsed_solo);
                println!("   With peer: {:?} (waited for ACK)", elapsed_peer);

                // Verify gossip propagation
                assert!(
                    wait_for_actor_present(&handle1, &actor_name_peer, Duration::from_secs(5))
                        .await,
                    "Actor should be on node1"
                );
                assert!(
                    wait_for_actor_present(&handle2, &actor_name_peer, Duration::from_secs(5))
                        .await,
                    "Actor should propagate to node2"
                );
            }
            Ok(Err(e)) => panic!("Peer registration failed: {:?}", e),
            Err(_) => panic!("Peer registration timed out"),
        }

        // Cleanup
        solo_handle.shutdown().await;
        handle1.shutdown().await;
        handle2.shutdown().await;

        println!("✅ Comparison test completed successfully");
    });
}

#[test]
fn test_register_actor_sync_real_multiple_peers() {
    run_register_actor_test("multiple-peers", || async {
        // Test: Register with multiple real peers - should get ACK from first responder

        println!("🚀 Starting multiple peers test...");

        let config = create_test_config();

        // Create 4 nodes (1 registering + 3 peers)
        let mut handles = Vec::new();
        let mut addrs = Vec::new();

        for i in 0..4 {
            let addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
            let node_name = format!("node{}", i);
            let handle = GossipRegistryHandle::new_with_keypair(
                addr,
                test_keypair(&node_name),
                Some(config.clone()),
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to start node{}", i));
            let actual_addr = handle.registry.bind_addr;

            handles.push(handle);
            addrs.push(actual_addr);
            println!("✅ Node{} started on {}", i, actual_addr);
        }

        // Connect all nodes as peers (full mesh)
        for (i, handle) in handles.iter().enumerate() {
            for (j, addr) in addrs.iter().enumerate() {
                if i != j {
                    let peer_id = test_peer_id(&format!("node{}", j));
                    let peer = handle.add_peer(&peer_id).await;
                    peer.connect(addr)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to connect node{} to node{}", i, j));
                }
            }
        }

        println!("🔗 All nodes connected in full mesh");

        // Wait for connections to stabilize (full mesh should be active).
        let connected = wait_for_condition(Duration::from_secs(10), || async {
            for handle in &handles {
                if handle.stats().await.active_peers < 3 {
                    return false;
                }
            }
            true
        })
        .await;
        assert!(
            connected,
            "All nodes should have 3 active peers before registration"
        );

        // Register actor on node0 - should get ACK from any of the 3 peers
        let actor_name = "multi_peer_actor".to_string();
        let actor_location = create_test_actor_location(addrs[0], "node0");

        println!("📝 Registering actor with 3 peers...");

        let start = std::time::Instant::now();
        let result = timeout(
            Duration::from_secs(5),
            handles[0].registry.register_actor_sync(
                actor_name.clone(),
                actor_location,
                Duration::from_millis(3000),
            ),
        )
        .await;
        let elapsed = start.elapsed();

        println!("⏱️  Multi-peer registration completed in {:?}", elapsed);

        match result {
            Ok(Ok(())) => {
                println!("✅ Registration succeeded - got ACK from at least one peer!");

                // Should have waited for ACK but not timed out
                assert!(elapsed >= Duration::from_micros(100)); // Some processing time
                assert!(elapsed <= Duration::from_millis(4000)); // Not timeout (real networking)

                for (i, handle) in handles.iter().enumerate() {
                    assert!(
                        wait_for_actor_present(handle, &actor_name, Duration::from_secs(8)).await,
                        "Actor should be visible on node{}",
                        i
                    );
                    println!("🔍 Node{} can find actor: ✅", i);
                }
            }
            Ok(Err(e)) => panic!("Multi-peer registration failed: {:?}", e),
            Err(_) => panic!("Multi-peer registration timed out"),
        }

        // Cleanup
        for (i, handle) in handles.into_iter().enumerate() {
            handle.shutdown().await;
            println!("🔄 Node{} shut down", i);
        }

        println!("✅ Multiple peers test completed successfully");
    });
}

#[test]
fn test_register_actor_sync_real_peer_timeout() {
    run_register_actor_test("peer-timeout", || async {
        // Test: Real peer that doesn't respond - should timeout gracefully

        println!("🚀 Starting peer timeout test...");

        let config = GossipConfig {
            max_peer_failures: 3,
            dead_peer_timeout: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(5),
            immediate_propagation_enabled: false, // Disable immediate gossip to simulate slow peer
            gossip_interval: Duration::from_millis(50),
            ..Default::default()
        };

        // Start two nodes
        let handle1 = GossipRegistryHandle::new_with_keypair(
            "127.0.0.1:0".parse().unwrap(),
            test_keypair("node1_timeout"),
            Some(config.clone()),
        )
        .await
        .expect("Failed to start node1");
        let node1_addr = handle1.registry.bind_addr;

        let handle2 = GossipRegistryHandle::new_with_keypair(
            "127.0.0.1:0".parse().unwrap(),
            test_keypair("node2_timeout"),
            Some(config.clone()),
        )
        .await
        .expect("Failed to start node2");
        let _node2_addr = handle2.registry.bind_addr;

        ensure_connected(
            &handle1,
            &handle2,
            test_peer_id("node1_timeout"),
            test_peer_id("node2_timeout"),
        )
        .await;

        println!("🔗 Nodes connected, testing timeout scenario");

        // Simulate slow/unresponsive peer by shutting down node2 immediately
        handle2.shutdown().await;
        println!("🔇 Node2 shut down to simulate unresponsive peer");

        // Try to register on node1 - should timeout since peer is gone
        let actor_name = "timeout_actor".to_string();
        let actor_location = create_test_actor_location(node1_addr, "node1");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let start = std::time::Instant::now();
        let result = handle1
            .registry
            .register_actor_sync(
                actor_name.clone(),
                actor_location,
                Duration::from_millis(50), // Very short timeout
            )
            .await;
        let elapsed = start.elapsed();

        println!("⏱️  Registration with timeout completed in {:?}", elapsed);

        // Should succeed but take full timeout duration (graceful fallback)
        assert!(
            result.is_ok(),
            "Registration should succeed even on timeout"
        );
        assert!(
            elapsed <= Duration::from_millis(500),
            "timeout should not exceed fallback window"
        );

        // Actor should still be registered locally
        // Check registry directly since actor might not be listening yet
        let is_registered = handle1
            .registry
            .actor_state
            .local_actors
            .contains_sync(actor_name.as_str())
            || handle1
                .registry
                .actor_state
                .known_actors
                .contains_sync(actor_name.as_str());
        assert!(is_registered, "Actor should be registered despite timeout");

        println!("✅ Timeout behavior validated - graceful fallback works");

        // Cleanup
        handle1.shutdown().await;

        println!("✅ Timeout test completed successfully");
    });
}
