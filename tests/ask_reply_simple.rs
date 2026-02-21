use std::net::SocketAddr;
use tokio::time::{Duration, sleep};
use tracing::info;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};

fn maybe_init_tracing(level: &'static str) {
    // In this sandboxed environment, enabling tracing can trigger EPERM ("Operation not
    // permitted") on subsequent networking syscalls. Only enable when explicitly requested.
    if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
        let directive = format!("icanact_remote={level}").parse().unwrap();
        let _ = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(EnvFilter::from_default_env().add_directive(directive)),
            )
            .try_init();
    }
}

/// Test basic ask() functionality with correlation ID tracking
#[test]
fn test_basic_ask_correlation() {
    let handle = std::thread::Builder::new()
        .name("ask-basic-correlation".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("failed to build runtime");
            rt.block_on(async {
                run_test_basic_ask_correlation().await;
            });
        })
        .expect("failed to spawn basic ask test thread");
    handle.join().expect("basic ask test panicked");
}

async fn run_test_basic_ask_correlation() {
    maybe_init_tracing("debug");

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:7911".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7912".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");

    let _peer_id_a = key_pair_a.peer_id();
    let peer_id_b = key_pair_b.peer_id();

    let config_a = GossipConfig {
        gossip_interval: Duration::from_secs(300), // 5 minutes to avoid interference during test
        ..Default::default()
    };

    let config_b = GossipConfig {
        gossip_interval: Duration::from_secs(300), // 5 minutes to avoid interference during test
        ..Default::default()
    };

    // Start nodes
    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config_a))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config_b))
        .await
        .unwrap();

    // Connect nodes - single direction to avoid duplicate tie-breaker churn
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    // Wait for initial gossip protocol to establish and settle
    sleep(Duration::from_millis(100)).await;

    info!("Test: Basic ask with correlation tracking");

    // Test 1: Use the ask() method that waits for response
    {
        info!("Test 1: Testing ask() with processed response");
        info!("Getting connection from {} to {}", addr_a, addr_b);
        let conn = handle_a.lookup_address(addr_b).await.unwrap();
        info!("Got connection handle");

        // Test ECHO command
        let request = b"ECHO:Hello from Node A";
        info!(
            "Sending ask request: {:?}",
            String::from_utf8_lossy(request)
        );
        let response = conn.ask(request).await.unwrap();
        assert_eq!(response.as_ref(), b"ECHOED:Hello from Node A");
        info!("ECHO test passed: {:?}", String::from_utf8_lossy(&response));

        // Test REVERSE command
        let request = b"REVERSE:12345";
        let response = conn.ask(request).await.unwrap();
        assert_eq!(response.as_ref(), b"REVERSED:54321");
        info!(
            "REVERSE test passed: {:?}",
            String::from_utf8_lossy(&response)
        );

        // Test COUNT command
        let request = b"COUNT:Hello World";
        let response = conn.ask(request).await.unwrap();
        assert_eq!(response.as_ref(), b"COUNTED:11 chars");
        info!(
            "COUNT test passed: {:?}",
            String::from_utf8_lossy(&response)
        );

        // Test HASH command
        let request = b"HASH:test";
        let response = conn.ask(request).await.unwrap();
        let response_str = String::from_utf8_lossy(&response);
        assert!(response_str.starts_with("HASHED:"));
        info!("HASH test passed: {}", response_str);

        // Test default processing
        let request = b"Just a plain message";
        let response = conn.ask(request).await.unwrap();
        let expected = b"RECEIVED:20 bytes, content: 'Just a plain message'";
        assert_eq!(response.as_ref(), expected.as_slice());
        info!(
            "Default processing test passed: {:?}",
            String::from_utf8_lossy(&response)
        );
    }

    // Test 2: Verify correlation IDs work with multiple concurrent asks
    {
        info!("Test 2: Testing multiple concurrent asks");
        let conn = handle_a.lookup_address(addr_b).await.unwrap();

        // Send multiple asks concurrently with different commands
        let mut futures = Vec::new();

        // Mix different types of requests
        let requests = [
            ("ECHO:Request 0", "ECHOED:Request 0"),
            ("REVERSE:Request 1", "REVERSED:1 tseuqeR"),
            ("COUNT:Request 2", "COUNTED:9 chars"),
            ("HASH:Request 3", "HASHED:"), // We'll check this starts with HASHED:
            (
                "Plain Request 4",
                "RECEIVED:15 bytes, content: 'Plain Request 4'",
            ),
        ];

        for (i, (request, expected_prefix)) in requests.iter().enumerate() {
            let conn_clone = conn.clone();
            let request = request.to_string().into_bytes();
            let expected_prefix = expected_prefix.to_string();
            let future = tokio::spawn(async move {
                let response = conn_clone.ask(&request).await.unwrap();
                (i, response, expected_prefix)
            });
            futures.push(future);
        }

        // Verify all responses match their requests
        for future in futures {
            let (i, response, expected_prefix) = future.await.unwrap();
            let response_str = String::from_utf8_lossy(&response);

            if expected_prefix == "HASHED:" {
                assert!(response_str.starts_with(&expected_prefix));
            } else {
                assert_eq!(response_str, expected_prefix);
            }

            info!("Request {} got correct response: {}", i, response_str);
        }
    }

    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test high throughput ask operations
#[test]
fn test_ask_high_throughput() {
    let handle = std::thread::Builder::new()
        .name("ask-high-throughput".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("failed to build runtime");
            rt.block_on(async {
                run_test_ask_high_throughput().await;
            });
        })
        .expect("failed to spawn throughput test thread");
    handle.join().expect("throughput test panicked");
}

async fn run_test_ask_high_throughput() {
    maybe_init_tracing("warn");

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:7913".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7914".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("perf_a");
    let key_pair_b = KeyPair::new_for_testing("perf_b");
    let _peer_id_a = key_pair_a.peer_id();
    let peer_id_b = key_pair_b.peer_id();

    let config_a = GossipConfig {
        gossip_interval: Duration::from_secs(300), // 5 minutes to avoid interference during test
        ..Default::default()
    };

    let config_b = GossipConfig {
        gossip_interval: Duration::from_secs(300), // 5 minutes to avoid interference during test
        ..Default::default()
    };

    // Start nodes
    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config_a))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config_b))
        .await
        .unwrap();

    // Connect single direction to avoid duplicate tie-breaker churn
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // Get connection
    let conn = handle_a.lookup_address(addr_b).await.unwrap();

    let num_requests = 1000;
    let start = std::time::Instant::now();

    // Send many concurrent asks with actual responses from the other node
    let mut handles = Vec::new();
    for i in 0..num_requests {
        let conn_clone = conn.clone();
        let handle = tokio::spawn(async move {
            // Use ECHO to verify the request is transmitted correctly
            let request = format!("ECHO:High throughput request {}", i).into_bytes();
            let response = conn_clone.ask(&request).await.unwrap();

            // Verify we got the correct echoed response
            let expected = format!("ECHOED:High throughput request {}", i).into_bytes();
            assert_eq!(response, expected);
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    let throughput = num_requests as f64 / elapsed.as_secs_f64();

    info!("Processed {} asks in {:?}", num_requests, elapsed);
    info!("Throughput: {:.0} req/sec", throughput);

    assert!(throughput > 2000.0, "Throughput should exceed 2k req/sec");

    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}
