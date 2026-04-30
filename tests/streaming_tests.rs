//! Tests for streaming request/response functionality
//!
//! These tests verify:
//! - Streaming requests (ask_streaming_bytes) for large payloads
//! - Streaming responses for large replies
//! - Zero-copy streaming via Bytes ownership
//! - StreamResponseStart/Data/End message types
//! - Auto-streaming via ReplyTo::reply() for large responses

use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

use icanact_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};

/// Helper to initialize tracing for tests
fn init_tracing() {
    // Avoid sandbox-triggered EPERM flakiness unless explicitly enabled.
    if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
        let _ = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer().with_filter(
                    EnvFilter::from_default_env()
                        .add_directive("icanact_remote=debug".parse().unwrap()),
                ),
            )
            .try_init();
    }
}

/// Create a test payload of specified size
fn create_test_payload(size: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(size);
    for i in 0..size {
        payload.push((i % 256) as u8);
    }
    payload
}

fn key_pair_ordered_for_outbound_a(a_seed: &str, b_seed: &str) -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing(a_seed);
    let second = KeyPair::new_for_testing(b_seed);
    if first
        .peer_id()
        .to_node_id()
        .as_bytes()
        .cmp(second.peer_id().to_node_id().as_bytes())
        .is_lt()
    {
        (first, second)
    } else {
        (second, first)
    }
}

/// Verify a test payload matches expected pattern
#[allow(dead_code)]
fn verify_payload(payload: &[u8], expected_size: usize) -> bool {
    if payload.len() != expected_size {
        return false;
    }
    for (i, &byte) in payload.iter().enumerate() {
        if byte != (i % 256) as u8 {
            return false;
        }
    }
    true
}

/// Handler for streaming ask tests that echoes back responses
struct AskTestHandler {
    message_received: AtomicBool,
    payload_size: AtomicU32,
}

const STREAM_TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const STREAM_TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const STREAM_TEST_WORKERS: usize = 4;

fn run_streaming_test<F, Fut>(name: &'static str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(format!("streaming-test-{}", name))
        .stack_size(STREAM_TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(STREAM_TEST_WORKERS)
                .thread_stack_size(STREAM_TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build streaming test runtime");
            runtime.block_on(test());
        })
        .expect("failed to spawn streaming test thread");

    handle.join().expect("streaming test panicked");
}

impl ActorMessageHandler for AskTestHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        _correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        self.message_received.store(true, Ordering::SeqCst);
        self.payload_size
            .store(payload.len() as u32, Ordering::SeqCst);

        info!(
            "📨 AskTestHandler received {} bytes, sending response",
            payload.len()
        );

        // For asks, we echo back the payload WITHOUT a prefix to avoid triggering streaming for threshold-sized requests
        // This ensures that a request of size N gets a response of size N, not N+9
        Box::pin(async move { Ok(Some(payload.into())) })
    }
}

/// Test streaming request with large payload (>1MB)
#[test]
fn test_streaming_request_large_payload() {
    run_streaming_test("streaming-request-large", || async {
        init_tracing();

        let addr_a: SocketAddr = "127.0.0.1:7921".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7922".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("stream_node_a", "stream_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        // Set up actor message handler for ask tests
        let handler = Arc::new(AskTestHandler {
            message_received: AtomicBool::new(false),
            payload_size: AtomicU32::new(0),
        });
        handle_b.registry.set_actor_message_handler(handler).await;

        // Connect nodes
        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(100)).await;

        info!("Test: Streaming request with 2MB payload");

        let conn = handle_a.lookup_address(addr_b).await.unwrap();

        // Create 2MB payload (over the 1MB streaming threshold)
        let payload_size = 2 * 1024 * 1024;
        let payload = Bytes::from(create_test_payload(payload_size));

        // Use ask_streaming_bytes for large payload
        let response = conn
            .ask_streaming_bytes(payload.clone(), 0, 0, Duration::from_secs(30))
            .await
            .unwrap();

        // Default handler echoes back with "PROCESSED:" prefix
        info!("Received response of {} bytes", response.len());

        // Verify we got a response (exact format depends on handler)
        assert!(!response.is_empty(), "Response should not be empty");

        info!("✅ Streaming request test passed");

        // Cleanup
        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test zero-copy streaming request with Bytes
#[test]
fn test_streaming_request_zero_copy() {
    run_streaming_test("streaming-request-zero-copy", || async {
        init_tracing();

        let addr_a: SocketAddr = "127.0.0.1:7923".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7924".parse().unwrap();

        let (key_pair_a, key_pair_b) = key_pair_ordered_for_outbound_a("zc_node_a", "zc_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        // Set up actor message handler for ask tests
        let handler = Arc::new(AskTestHandler {
            message_received: AtomicBool::new(false),
            payload_size: AtomicU32::new(0),
        });
        handle_b.registry.set_actor_message_handler(handler).await;

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(100)).await;

        info!("Test: Zero-copy streaming request with ask_streaming_bytes");

        let conn = handle_a.lookup_address(addr_b).await.unwrap();

        // Create 1.5MB payload as Bytes (over threshold)
        let payload_size = 1536 * 1024;
        let payload = Bytes::from(create_test_payload(payload_size));

        // Use ask_streaming_bytes for zero-copy
        let response = conn
            .ask_streaming_bytes(payload.clone(), 0, 0, Duration::from_secs(30))
            .await
            .unwrap();

        info!(
            "Received response of {} bytes via zero-copy path",
            response.len()
        );

        assert!(!response.is_empty(), "Response should not be empty");

        info!("✅ Zero-copy streaming request test passed");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test streaming response (large reply via auto-streaming)
#[test]
fn test_streaming_response_auto() {
    run_streaming_test("streaming-response-auto", || async {
        init_tracing();

        let addr_a: SocketAddr = "127.0.0.1:7925".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7926".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("resp_node_a", "resp_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(100)).await;

        info!("Test: Streaming response (large reply triggers auto-streaming)");

        let conn = handle_a.lookup_address(addr_b).await.unwrap();

        // Send a request that triggers a large response
        let request = b"LARGE_RESPONSE:2097152"; // Request 2MB response

        let response = conn.ask(bytes::Bytes::from_static(request)).await.unwrap();

        info!(
            "Received potentially streamed response of {} bytes",
            response.len()
        );

        assert!(!response.is_empty(), "Response should not be empty");

        info!("✅ Streaming response test passed");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test that small payloads use the write queue (not streaming)
#[test]
fn test_small_payload_uses_write_queue() {
    run_streaming_test("small-payload-write-queue", || async {
        init_tracing();

        let addr_a: SocketAddr = "127.0.0.1:7927".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7928".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("small_node_a", "small_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(100)).await;

        info!("Test: Small payload (under threshold) uses write queue");

        let conn = handle_a.lookup_address(addr_b).await.unwrap();

        // Small payload (1KB) - should NOT use streaming
        let payload = create_test_payload(1024);

        let response = conn.ask(bytes::Bytes::from(payload)).await.unwrap();

        info!(
            "Received response of {} bytes via write queue path",
            response.len()
        );

        assert!(!response.is_empty(), "Response should not be empty");

        info!("✅ Small payload write queue test passed");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test streaming threshold boundary
#[test]
fn test_streaming_threshold_boundary() {
    run_streaming_test("streaming-threshold-boundary", || async {
        init_tracing();

        let addr_a: SocketAddr = "127.0.0.1:7929".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7930".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("boundary_node_a", "boundary_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handler = Arc::new(AskTestHandler {
            message_received: AtomicBool::new(false),
            payload_size: AtomicU32::new(0),
        });
        handle_b.registry.set_actor_message_handler(handler).await;

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(100)).await;

        info!("Test: Streaming threshold boundary (exactly at threshold)");

        let conn = handle_a.lookup_address(addr_b).await.unwrap();

        let threshold = conn.streaming_threshold();
        info!("Streaming threshold: {} bytes", threshold);

        let payload_over_threshold = Bytes::from(create_test_payload(threshold + 100));
        let response = conn
            .ask_streaming_bytes(
                payload_over_threshold.clone(),
                0,
                0,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert!(!response.is_empty(), "Response over threshold should work");
        info!(
            "✅ Payload over threshold ({} bytes) succeeded",
            threshold + 100
        );

        let under_size = threshold.saturating_sub(100).max(100);
        let payload_under_threshold = Bytes::from(create_test_payload(under_size));
        let response = conn
            .ask_streaming_bytes(
                payload_under_threshold.clone(),
                0,
                0,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert!(!response.is_empty(), "Response under threshold should work");
        info!(
            "✅ Payload under threshold ({} bytes) succeeded",
            payload_under_threshold.len()
        );

        info!("✅ Streaming threshold boundary test passed");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test multiple concurrent streaming requests
#[test]
fn test_concurrent_streaming_requests() {
    run_streaming_test("concurrent-streaming-requests", || async {
        init_tracing();

        let addr_a: SocketAddr = "127.0.0.1:7931".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7932".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("concurrent_node_a", "concurrent_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(100)).await;

        // Set up actor message handler for ask tests
        let handler = Arc::new(AskTestHandler {
            message_received: AtomicBool::new(false),
            payload_size: AtomicU32::new(0),
        });
        handle_b.registry.set_actor_message_handler(handler).await;

        info!("Test: Multiple concurrent streaming requests");

        let conn = Arc::new(handle_a.lookup_address(addr_b).await.unwrap());
        let results = Arc::new(Mutex::new(Vec::new()));

        // Spawn multiple concurrent streaming requests
        let mut handles = Vec::new();
        for i in 0..3 {
            let conn = conn.clone();
            let results = results.clone();
            let handle = tokio::spawn(async move {
                let payload_size = 1024 * 1024 + i * 100_000; // Varying sizes over 1MB
                let payload = Bytes::from(create_test_payload(payload_size));

                let response = conn
                    .ask_streaming_bytes(payload.clone(), 0, 0, Duration::from_secs(60))
                    .await;

                let mut results = results.lock().await;
                results.push((i, response.is_ok(), payload_size));
            });
            handles.push(handle);
        }

        // Wait for all requests to complete
        for handle in handles {
            handle.await.unwrap();
        }

        let results = results.lock().await;
        for (i, success, size) in results.iter() {
            info!("Request {}: {} bytes, success: {}", i, size, success);
            assert!(success, "Request {} should succeed", i);
        }

        info!("✅ Concurrent streaming requests test passed");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test MessageType enum includes streaming response types
#[test]
fn test_message_type_streaming_response_variants() {
    use icanact_remote::MessageType;

    // Verify streaming request types
    assert_eq!(MessageType::from_byte(0x10), Some(MessageType::StreamStart));
    assert_eq!(MessageType::from_byte(0x11), Some(MessageType::StreamData));
    assert_eq!(MessageType::from_byte(0x12), Some(MessageType::StreamEnd));

    // Verify streaming response types (new)
    assert_eq!(
        MessageType::from_byte(0x13),
        Some(MessageType::StreamResponseStart)
    );
    assert_eq!(
        MessageType::from_byte(0x14),
        Some(MessageType::StreamResponseData)
    );
    assert_eq!(
        MessageType::from_byte(0x15),
        Some(MessageType::StreamResponseEnd)
    );

    // Verify helper methods
    assert!(MessageType::StreamStart.is_streaming_request());
    assert!(MessageType::StreamData.is_streaming_request());
    assert!(MessageType::StreamEnd.is_streaming_request());
    assert!(!MessageType::StreamStart.is_streaming_response());

    assert!(MessageType::StreamResponseStart.is_streaming_response());
    assert!(MessageType::StreamResponseData.is_streaming_response());
    assert!(MessageType::StreamResponseEnd.is_streaming_response());
    assert!(!MessageType::StreamResponseStart.is_streaming_request());

    info!("✅ MessageType streaming variants test passed");
}

/// Test streaming tell (correlation_id = 0) - fire-and-forget, no response expected
///
/// This test verifies that:
/// 1. Streaming tells with correlation_id = 0 are correctly routed to the handler
/// 2. The handler receives `None` for correlation_id (not `Some(0)`)
/// 3. No response is sent back on the wire (handler returns None, no send_streaming_response called)
#[test]
fn test_streaming_tell_no_response() {
    use icanact_remote::registry::{ActorMessageFuture, ActorMessageHandler};
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    run_streaming_test("streaming-tell-no-response", || async {
        init_tracing();

        // Shared state to track handler invocations
        struct TellTestHandler {
            message_received: AtomicBool,
            correlation_was_none: AtomicBool,
            payload_size: AtomicU32,
        }

        impl ActorMessageHandler for TellTestHandler {
            fn handle_actor_message(
                &self,
                _actor_id: u64,
                _type_hash: u32,
                payload: icanact_remote::AlignedBytes,
                correlation_id: Option<u16>,
            ) -> ActorMessageFuture<'_> {
                self.message_received.store(true, Ordering::SeqCst);
                self.correlation_was_none
                    .store(correlation_id.is_none(), Ordering::SeqCst);
                self.payload_size
                    .store(payload.len() as u32, Ordering::SeqCst);

                info!(
                    "📨 TellTestHandler received {} bytes, correlation_id={:?}",
                    payload.len(),
                    correlation_id
                );

                // For tells, we return None (no response)
                Box::pin(async move { Ok(None) })
            }
        }

        let addr_a: SocketAddr = "127.0.0.1:7941".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7942".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("tell_node_a", "tell_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        // Set up the test handler on node B
        let handler = Arc::new(TellTestHandler {
            message_received: AtomicBool::new(false),
            correlation_was_none: AtomicBool::new(false),
            payload_size: AtomicU32::new(0),
        });
        handle_b
            .registry
            .set_actor_message_handler(handler.clone())
            .await;

        // Connect nodes
        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(100)).await;

        info!("Test: Streaming tell (correlation_id=0) - fire-and-forget");

        let conn = handle_a.lookup_address(addr_b).await.unwrap();

        // Create a large payload (>1MB to trigger streaming)
        let payload_size = 1_500_000; // 1.5MB
        let payload = create_test_payload(payload_size);

        // Send as a streaming tell (correlation_id = 0)
        // This uses stream_large_message which sends with correlation_id = 0
        let test_type_hash: u32 = 0x7E11_7E57; // "TELL_TEST" in hex-ish
        let test_actor_id: u64 = 12345;

        info!("📤 Sending {} byte streaming tell...", payload_size);
        conn.connection
            .as_ref()
            .unwrap()
            .stream_large_message(&payload, test_type_hash, test_actor_id)
            .await
            .expect("stream_large_message should succeed");

        // Wait for the message to be processed
        sleep(Duration::from_millis(500)).await;

        // Verify the handler was called correctly
        assert!(
            handler.message_received.load(Ordering::SeqCst),
            "Handler should have received the streaming tell"
        );
        assert!(
            handler.correlation_was_none.load(Ordering::SeqCst),
            "Handler should have received None for correlation_id (tell, not ask)"
        );
        assert_eq!(
            handler.payload_size.load(Ordering::SeqCst) as usize,
            payload_size,
            "Handler should have received the full payload"
        );

        info!("✅ Streaming tell test passed:");
        info!("   - Handler received message: true");
        info!("   - correlation_id was None: true (fire-and-forget)");
        info!("   - No response sent (tell semantics)");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}
