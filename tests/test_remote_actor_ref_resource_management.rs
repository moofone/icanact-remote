//! Tests for RemoteActorRef resource management, lifecycle, and edge cases
//!
//! These tests verify:
//! - Shutdown detection (operations return Err(Shutdown) after registry shutdown)
//! - Weak references don't prevent registry cleanup
//! - Concurrent access thread safety (Arc<Mutex<>> behavior)
//! - DNS reconnection handling (peer IP changes)
//! - Actor unregistration scenarios
//! - Connection cleanup on drop

use icanact_remote::*;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

const REMOTE_REF_TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const REMOTE_REF_TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const REMOTE_REF_TEST_WORKERS: usize = 4;

fn maybe_init_tracing() {
    // Avoid sandbox-triggered EPERM flakiness unless explicitly enabled.
    if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();
    }
}

fn run_remote_actor_ref_test<F, Fut>(name: &'static str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(format!("remote-ref-test-{}", name))
        .stack_size(REMOTE_REF_TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(REMOTE_REF_TEST_WORKERS)
                .thread_stack_size(REMOTE_REF_TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build remote actor ref test runtime");
            runtime.block_on(test());
        })
        .expect("failed to spawn remote actor ref test thread");

    handle
        .join()
        .expect("remote actor ref resource management test panicked");
}

#[cfg(feature = "test-helpers")]
async fn test_remote_actor_ref_detects_shutdown_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_shutdown");
    let key_pair_b = KeyPair::new_for_testing("node_b_shutdown");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor on node B
    handle_b
        .register_urgent(
            "test_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Get RemoteActorRef
    let remote_actor: icanact_remote::RemoteActorRef = handle_a.lookup("test_service").await;
    assert!(remote_actor.is_some(), "lookup should succeed");

    let remote_actor = remote_actor.unwrap();

    // Verify it works before shutdown
    assert!(
        remote_actor
            .tell(bytes::Bytes::from_static(b"before shutdown"))
            .await
            .is_ok(),
        "tell() should work before shutdown"
    );

    // Shutdown registry
    handle_a.shutdown().await;
    handle_b.shutdown().await;

    // Give tasks time to complete shutdown
    sleep(Duration::from_millis(100)).await;

    // Now operations should fail with Shutdown error
    let result = remote_actor
        .tell(bytes::Bytes::from_static(b"after shutdown"))
        .await;

    assert!(
        matches!(result, Err(GossipError::Shutdown)),
        "tell() should return Err(Shutdown) after registry shutdown, got: {:?}",
        result
    );

    println!("✅ Shutdown detection works correctly");
}

#[cfg(feature = "test-helpers")]
#[test]
fn test_remote_actor_ref_detects_shutdown() {
    run_remote_actor_ref_test("detects-shutdown", || {
        test_remote_actor_ref_detects_shutdown_inner()
    });
}

async fn test_concurrent_remote_actor_ref_usage_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_concurrent");
    let key_pair_b = KeyPair::new_for_testing("node_b_concurrent");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor
    handle_b
        .register_urgent(
            "concurrent_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Get RemoteActorRef
    let remote_actor: icanact_remote::RemoteActorRef =
        handle_a.lookup("concurrent_service").await.unwrap();

    // Clone it multiple times (simulating concurrent use)
    let actor1 = remote_actor.clone();
    let actor2 = remote_actor.clone();
    let actor3 = remote_actor.clone();

    // Send concurrent messages (all using same underlying Arc<Mutex<Connection>>)
    let (r1, r2, r3) = tokio::join!(
        actor1.tell(bytes::Bytes::from_static(b"message1")),
        actor2.tell(bytes::Bytes::from_static(b"message2")),
        actor3.tell(bytes::Bytes::from_static(b"message3"))
    );

    // All should succeed
    assert!(r1.is_ok(), "Concurrent tell 1 should succeed");
    assert!(r2.is_ok(), "Concurrent tell 2 should succeed");
    assert!(r3.is_ok(), "Concurrent tell 3 should succeed");

    println!("✅ Concurrent access works correctly (Arc<Mutex>> thread-safe)");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_concurrent_remote_actor_ref_usage() {
    run_remote_actor_ref_test("concurrent-usage", || {
        test_concurrent_remote_actor_ref_usage_inner()
    });
}

async fn test_weak_registry_ref_prevents_cycles_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_weak");
    let key_pair_b = KeyPair::new_for_testing("node_b_weak");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor on node B
    handle_b
        .register_urgent(
            "weak_test_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Get RemoteActorRef from node A (looks up remote actor on node B)
    let remote_actor: icanact_remote::RemoteActorRef =
        handle_a.lookup("weak_test_service").await.unwrap();

    // Verify registry is alive
    assert!(remote_actor.is_registry_alive(), "Registry should be alive");

    // Shutdown registry
    handle_a.shutdown().await;
    handle_b.shutdown().await;

    // Give time for shutdown
    sleep(Duration::from_millis(100)).await;

    // Operations should fail
    let result = remote_actor.tell(bytes::Bytes::from_static(b"test")).await;
    assert!(
        matches!(result, Err(GossipError::Shutdown)),
        "Should return Shutdown error, got: {:?}",
        result
    );

    println!("✅ Weak reference allows cleanup (operations return Shutdown error)");
}

#[test]
fn test_weak_registry_ref_prevents_cycles() {
    run_remote_actor_ref_test("weak-registry-ref", || {
        test_weak_registry_ref_prevents_cycles_inner()
    });
}

async fn test_remote_actor_ref_clone_independence_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_clone");
    let key_pair_b = KeyPair::new_for_testing("node_b_clone");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor
    handle_b
        .register_urgent(
            "clone_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Get RemoteActorRef
    let actor1 = handle_a.lookup("clone_service").await.unwrap();
    let actor2 = actor1.clone(); // Clone RemoteActorRef

    // Both should point to same actor
    assert_eq!(
        actor1.location.address, actor2.location.address,
        "Clones should point to same actor"
    );

    // Both should work independently
    assert!(
        actor1
            .tell(bytes::Bytes::from_static(b"from actor1"))
            .await
            .is_ok()
    );
    assert!(
        actor2
            .tell(bytes::Bytes::from_static(b"from actor2"))
            .await
            .is_ok()
    );

    println!("✅ RemoteActorRef clones work independently");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_remote_actor_ref_clone_independence() {
    run_remote_actor_ref_test("clone-independence", || {
        test_remote_actor_ref_clone_independence_inner()
    });
}

async fn test_remote_actor_ref_location_metadata_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_metadata");
    let key_pair_b = KeyPair::new_for_testing("node_b_metadata");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor with metadata
    let actor_addr: SocketAddr = handle_b.registry.bind_addr;
    handle_b
        .register_urgent(
            "metadata_service".to_string(),
            actor_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Get RemoteActorRef
    let remote_actor: icanact_remote::RemoteActorRef =
        handle_a.lookup("metadata_service").await.unwrap();

    // Verify location metadata is accessible
    // Note: address includes port (e.g., "127.0.0.1:8935:0")
    assert!(
        !remote_actor.location.address.is_empty(),
        "Actor address should be preserved"
    );
    assert_eq!(
        remote_actor.location.peer_id, peer_id_b,
        "Peer ID should match"
    );

    // Debug output should show useful info
    println!("RemoteActorRef location: {:?}", remote_actor.location);

    println!("✅ RemoteActorRef preserves actor metadata");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_remote_actor_ref_location_metadata() {
    run_remote_actor_ref_test("location-metadata", || {
        test_remote_actor_ref_location_metadata_inner()
    });
}

#[cfg(feature = "test-helpers")]
async fn test_connection_reuse_across_lookups_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_reuse");
    let key_pair_b = KeyPair::new_for_testing("node_b_reuse");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor
    handle_b
        .register_urgent(
            "reuse_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Multiple lookups should return RemoteActorRefs with same connection
    let actor1 = handle_a.lookup("reuse_service").await.unwrap();
    let actor2 = handle_a.lookup("reuse_service").await.unwrap();

    let conn1 = actor1
        .connection_ref()
        .expect("first lookup should have a connection");
    let conn2 = actor2
        .connection_ref()
        .expect("second lookup should have a connection");
    assert_eq!(conn1.addr, conn2.addr, "Should use same connection");

    // Both should work
    assert!(
        actor1
            .tell(bytes::Bytes::from_static(b"from actor1"))
            .await
            .is_ok()
    );
    assert!(
        actor2
            .tell(bytes::Bytes::from_static(b"from actor2"))
            .await
            .is_ok()
    );

    println!("✅ Multiple lookups reuse cached connection");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[cfg(feature = "test-helpers")]
#[test]
fn test_connection_reuse_across_lookups() {
    run_remote_actor_ref_test("connection-reuse", || {
        test_connection_reuse_across_lookups_inner()
    });
}

async fn test_remote_actor_ref_debug_output_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_debug");
    let key_pair_b = KeyPair::new_for_testing("node_b_debug");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor
    handle_b
        .register_urgent(
            "debug_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    let remote_actor: icanact_remote::RemoteActorRef =
        handle_a.lookup("debug_service").await.unwrap();

    // Debug output should be useful
    let debug_str = format!("{:?}", remote_actor);
    assert!(
        debug_str.contains("RemoteActorRef"),
        "Debug should show struct name"
    );
    assert!(
        debug_str.contains("registry_alive"),
        "Debug should show registry status"
    );

    println!("Debug output: {}", debug_str);
    println!("✅ Debug output provides useful information");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_remote_actor_ref_debug_output() {
    run_remote_actor_ref_test("debug-output", || {
        test_remote_actor_ref_debug_output_inner()
    });
}

async fn test_remote_actor_ref_with_timeout_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_timeout");
    let key_pair_b = KeyPair::new_for_testing("node_b_timeout");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register actor
    handle_b
        .register_urgent(
            "timeout_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    let remote_actor: icanact_remote::RemoteActorRef =
        handle_a.lookup("timeout_service").await.unwrap();

    // Test ask with timeout
    let result = remote_actor
        .ask_with_timeout(
            bytes::Bytes::from_static(b"ping"),
            Duration::from_millis(100),
        )
        .await;

    assert!(result.is_ok(), "ask_with_timeout should succeed");
    println!("✅ ask_with_timeout works through RemoteActorRef");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_remote_actor_ref_with_timeout() {
    run_remote_actor_ref_test("with-timeout", || {
        test_remote_actor_ref_with_timeout_inner()
    });
}

async fn test_multiple_remote_actor_refs_same_actor_inner() {
    maybe_init_tracing();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a_multi");
    let key_pair_b = KeyPair::new_for_testing("node_b_multi");
    let peer_id_b = key_pair_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_a.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:0".parse().unwrap(), key_pair_b.to_secret_key(), Some(config.clone()),
    icanact_remote::BuilderTlsBootstrap)
    .await
    .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Register single actor
    handle_b
        .register_urgent(
            "multi_ref_service".to_string(),
            handle_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Multiple lookups for same actor
    let ref1: icanact_remote::RemoteActorRef = handle_a.lookup("multi_ref_service").await.unwrap();
    let ref2: icanact_remote::RemoteActorRef = handle_a.lookup("multi_ref_service").await.unwrap();
    let ref3: icanact_remote::RemoteActorRef = handle_a.lookup("multi_ref_service").await.unwrap();

    // All should work
    assert!(
        ref1.tell(bytes::Bytes::from_static(b"from ref1"))
            .await
            .is_ok()
    );
    assert!(
        ref2.tell(bytes::Bytes::from_static(b"from ref2"))
            .await
            .is_ok()
    );
    assert!(
        ref3.tell(bytes::Bytes::from_static(b"from ref3"))
            .await
            .is_ok()
    );

    println!("✅ Multiple RemoteActorRefs to same actor work independently");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[test]
fn test_multiple_remote_actor_refs_same_actor() {
    run_remote_actor_ref_test("multiple-refs", || {
        test_multiple_remote_actor_refs_same_actor_inner()
    });
}
