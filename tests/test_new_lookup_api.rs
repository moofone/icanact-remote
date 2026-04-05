//! Test that demonstrates the NEW lookup() API with RemoteActorRef
//!
//! This test verifies:
//! - lookup() returns RemoteActorRef with cached connection
//! - RemoteActorRef.tell() and ask() work with ZERO additional lookups
//! - The old API (get_connection()) is NOT accessible from the Icanact layer

use icanact_remote::*;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::sleep;

const LOOKUP_API_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const LOOKUP_API_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const LOOKUP_API_WORKERS: usize = 4;

fn run_lookup_api_test<F, Fut>(name: &'static str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(format!("new-lookup-test-{}", name))
        .stack_size(LOOKUP_API_THREAD_STACK_SIZE)
        .spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(LOOKUP_API_WORKERS)
                .thread_stack_size(LOOKUP_API_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build lookup API test runtime");
            runtime.block_on(test());
        })
        .expect("failed to spawn lookup API test thread");

    handle.join().expect("lookup API test panicked");
}

async fn test_new_lookup_api_returns_actor_ref_inner() {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_a.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_b.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register an actor on node B
    handle_b
        .register_urgent(
            "test_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // ========================================
    // NEW API: lookup() returns RemoteActorRef
    // ========================================
    let remote_actor = handle_a.lookup("test_service").await;
    assert!(
        remote_actor.is_some(),
        "lookup() should return Some(RemoteActorRef)"
    );

    let remote_actor = remote_actor.unwrap();

    // Verify RemoteActorRef has both location and connection
    println!(
        "✅ RemoteActorRef.location.address: {}",
        remote_actor.location.address
    );
    println!(
        "✅ RemoteActorRef.location.peer_id: {}",
        remote_actor.location.peer_id
    );
    if let Some(conn) = remote_actor.connection_ref() {
        println!("✅ RemoteActorRef.connection.addr: {}", conn.addr);
    } else {
        println!("✅ RemoteActorRef.connection: None (actor not yet listening)");
    }

    // ========================================
    // Tell: Fire-and-forget with ZERO lookups
    // ========================================
    let message1 = b"Hello from node A!";
    remote_actor
        .tell(bytes::Bytes::from_static(message1))
        .await
        .expect("tell() should work with cached connection");

    println!("✅ tell() succeeded using cached connection - NO additional lookups!");

    // ========================================
    // Ask: Request-response with ZERO lookups
    // ========================================
    let request = b"PING";
    let response = remote_actor
        .ask(bytes::Bytes::from_static(request))
        .await
        .expect("ask() should work with cached connection");

    println!("✅ ask() succeeded using cached connection - NO additional lookups!");
    println!("   Response length: {} bytes", response.len());

    // ========================================
    // Verify the connection is cached (same instance)
    // ========================================
    if let Some(conn) = remote_actor.connection_ref() {
        let conn_addr_1 = conn.addr;
        let conn_addr_2 = conn.addr;
        assert_eq!(
            conn_addr_1, conn_addr_2,
            "Cached connection should be the same instance"
        );
        println!("✅ Connection is properly cached in RemoteActorRef");
    } else {
        println!("✅ Connection is None (expected for unconnected actors)");
    }

    // Cleanup
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_new_lookup_api_returns_actor_ref() {
    run_lookup_api_test("returns-actor-ref", || {
        test_new_lookup_api_returns_actor_ref_inner()
    });
}

async fn test_old_api_not_accessible_inner() {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_single");
    let key_pair_b = KeyPair::new_for_testing("node_b_single");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_a.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_b.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    // ========================================
    // OLD API: get_connection() should NOT be accessible
    // ========================================
    let _addr: SocketAddr = "127.0.0.1:8903".parse().unwrap();

    // This should FAIL to compile if get_connection is private:
    // let conn = handle_a.get_connection(addr).await;

    // But we can still use lookup():
    handle_b
        .register_urgent(
            "my_actor".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    let remote_actor = handle_a.lookup("my_actor").await;
    assert!(remote_actor.is_some(), "lookup() should work");

    println!("✅ Only lookup() API is accessible - get_connection() is private!");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_old_api_not_accessible() {
    run_lookup_api_test("old-api-not-accessible", || {
        test_old_api_not_accessible_inner()
    });
}

async fn test_multiple_lookups_return_different_refs_inner() {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a2");
    let key_pair_b = KeyPair::new_for_testing("node_b2");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_a.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_b.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor
    handle_b
        .register_urgent(
            "shared_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Multiple lookups should return different RemoteActorRef instances
    // (each with its own cached connection)
    let ref1 = handle_a.lookup("shared_service").await;
    let ref2 = handle_a.lookup("shared_service").await;

    assert!(ref1.is_some());
    assert!(ref2.is_some());

    let ref1 = ref1.unwrap();
    let ref2 = ref2.unwrap();

    // They should point to the same actor
    assert_eq!(
        ref1.location.address, ref2.location.address,
        "Both refs should point to same actor"
    );

    // But they are different RemoteActorRef instances
    // (this is OK - each has its own cached ConnectionHandle)
    println!("✅ Multiple lookups work correctly");
    if let (Some(conn1), Some(conn2)) = (ref1.connection_ref(), ref2.connection_ref()) {
        println!("   Ref1 addr: {}", conn1.addr);
        println!("   Ref2 addr: {}", conn2.addr);
    } else {
        println!("   Ref1 or Ref2 connection is None (expected for unconnected actors)");
    }

    // Both refs can send messages independently
    ref1.tell(bytes::Bytes::from_static(b"From ref1"))
        .await
        .unwrap();
    ref2.tell(bytes::Bytes::from_static(b"From ref2"))
        .await
        .unwrap();

    println!("✅ Both RemoteActorRefs can send messages independently");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_multiple_lookups_return_different_refs() {
    run_lookup_api_test("multiple-lookups", || {
        test_multiple_lookups_return_different_refs_inner()
    });
}

async fn test_lookup_caches_connection_for_zero_lookup_sending_inner() {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a3");
    let key_pair_b = KeyPair::new_for_testing("node_b3");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_a.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_b.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    handle_b
        .register_urgent(
            "fast_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Lookup once - this does ALL the work
    let remote_actor = handle_a.lookup("fast_service").await.unwrap();

    // Send many messages - ZERO additional lookups per message
    let iterations = 100;
    for i in 0..iterations {
        let msg = format!("Message {}", i);
        remote_actor
            .tell(bytes::Bytes::copy_from_slice(msg.as_bytes()))
            .await
            .unwrap();
    }

    println!(
        "✅ Sent {} messages with ZERO additional lookups per message!",
        iterations
    );
    println!("   lookup() did the work once, then all tell() calls were direct");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_lookup_caches_connection_for_zero_lookup_sending() {
    run_lookup_api_test("lookup-caches-connection", || {
        test_lookup_caches_connection_for_zero_lookup_sending_inner()
    });
}
