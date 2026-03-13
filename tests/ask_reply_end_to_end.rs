mod common;

use common::{create_tls_node, wait_for_condition};
use icanact_remote::GossipConfig;
use icanact_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::time::{Duration, sleep};
use tracing::info;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

const TEST_ACTOR_ID: u64 = 7;
const TEST_TYPE_HASH: u32 = 0xA57A_A5C0;
const TEST_THREAD_STACK: usize = 8 * 1024 * 1024;

static E2E_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

struct TestActorHandler;

impl ActorMessageHandler for TestActorHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let payload_len = payload.len();
        Box::pin(async move {
            tracing::info!(
                target: "icanact_remote",
                payload_len,
                correlation_id = correlation_id.unwrap_or(0),
                "TestActorHandler invoked"
            );
            if correlation_id.is_some() {
                let response = format!("RESPONSE:{}", payload_len).into_bytes();
                Ok(Some(response.into()))
            } else {
                Ok(None)
            }
        })
    }
}

struct LargeResponseHandler {
    response: bytes::Bytes,
}

impl ActorMessageHandler for LargeResponseHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let response = self.response.clone();
        Box::pin(async move {
            if correlation_id.is_some() {
                Ok(Some(response.into()))
            } else {
                Ok(None)
            }
        })
    }
}

fn run_with_large_stack<F, Fut>(name: &str, fut: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("test-{name}"))
        .stack_size(TEST_THREAD_STACK)
        .spawn(move || {
            // These tests open real sockets and can trip OS-level sandbox/limits when run in
            // parallel (test harness default). Serialize to keep them deterministic.
            let _guard = E2E_TEST_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|e| e.into_inner());

            // Using a multi-thread runtime here increases the likelihood of transient EPERM in
            // sandboxed macOS environments. A current-thread runtime is sufficient for these
            // tests (we're not CPU-bound) and is substantially more deterministic.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build test runtime");
            runtime.block_on(fut());
        })
        .expect("spawn large stack test thread")
        .join()
        .expect("join large stack test thread");
}

/// End-to-end ask/reply over real TLS sockets.
///
/// This is intentionally kept as a single test to reduce socket bind/connect churn in
/// sandboxed environments where transient EPERM can occur under heavy socket activity.
#[test]
fn test_end_to_end_ask_reply() {
    run_with_large_stack("end_to_end_ask_reply", || async {
        // Tracing in this sandboxed environment can trigger EPERM ("Operation not permitted")
        // on subsequent networking syscalls. Only enable it when explicitly requested.
        if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
            let _ = tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer().with_filter(
                        EnvFilter::from_default_env()
                            .add_directive("icanact_remote=info".parse().unwrap()),
                    ),
                )
                .try_init();
        }

        // Keep the gossip timer from creating opportunistic outbound connections during
        // the ask/reply assertions (tie-breakers can invalidate the connection mid-ask).
        let mut config = GossipConfig::default();
        config.gossip_interval = Duration::from_secs(3600);
        let handle_a = create_tls_node(config.clone()).await.unwrap();
        let handle_b = create_tls_node(config).await.unwrap();
        handle_b
            .registry
            .set_actor_message_handler(Arc::new(TestActorHandler))
            .await;

        let addr_b = handle_b.registry.bind_addr;
        let peer_b_id = handle_b.registry.peer_id.clone();
        let peer_b = handle_a.add_peer(&peer_b_id).await;
        peer_b.connect(&addr_b).await.unwrap();

        assert!(
            wait_for_condition(Duration::from_secs(3), || async {
                handle_a.stats().await.active_peers > 0
            })
            .await,
            "TLS peer should connect"
        );

        info!("=== Test 1: Basic ask/reply with actor handler ===");
        {
            let conn = handle_a.lookup_peer(&peer_b_id).await.unwrap();
            let conn = conn.connection_ref().expect("connection should be ready");
            let request = bytes::Bytes::from_static(b"Hello from test");
            let start = std::time::Instant::now();
            let before_written = conn.bytes_written();
            let before_seq = conn.sequence_number();

            let ask_task = {
                let conn = conn.clone();
                tokio::spawn(async move {
                    conn.ask_actor_frame(
                        TEST_ACTOR_ID,
                        TEST_TYPE_HASH,
                        request,
                        Duration::from_secs(5),
                    )
                    .await
                })
            };

            sleep(Duration::from_millis(100)).await;
            let after_written = conn.bytes_written();
            let after_seq = conn.sequence_number();
            tracing::info!(
                target: "icanact_remote",
                before_written,
                after_written,
                delta = after_written.saturating_sub(before_written),
                before_seq,
                after_seq,
                seq_delta = after_seq.saturating_sub(before_seq),
                streaming = conn.is_streaming_active(),
                "client bytes_written after sending ask"
            );

            let response = ask_task.await.unwrap().unwrap();
            let elapsed = start.elapsed();

            info!(
                "Got response in {:?}: {:?}",
                elapsed,
                String::from_utf8_lossy(&response)
            );
            assert_eq!(response.as_ref(), b"RESPONSE:15"); // Mock response with request length
        }

        info!("=== Test 2: Multiple concurrent asks with correlation tracking ===");
        {
            let conn = handle_a.lookup_peer(&peer_b_id).await.unwrap();
            let conn = conn
                .connection_ref()
                .expect("connection should be ready")
                .clone();

            let mut handles = Vec::new();
            let start = std::time::Instant::now();

            for i in 0..10 {
                let conn_clone = conn.clone();
                let handle = tokio::spawn(async move {
                    let request = format!("Concurrent request {}", i).into_bytes();
                    let payload_len = request.len();
                    let response = conn_clone
                        .ask_actor_frame(
                            TEST_ACTOR_ID,
                            TEST_TYPE_HASH,
                            bytes::Bytes::from(request),
                            Duration::from_secs(5),
                        )
                        .await
                        .unwrap();
                    (i, response, payload_len)
                });
                handles.push(handle);
            }

            // Verify all responses are correctly correlated
            for handle in handles {
                let (i, response, payload_len) = handle.await.unwrap();
                let expected = format!("RESPONSE:{}", payload_len).into_bytes();
                assert_eq!(response, expected);
                info!("Request {} correctly correlated", i);
            }

            let elapsed = start.elapsed();
            info!("Processed 10 concurrent asks in {:?}", elapsed);
        }

        info!("=== Test 3: Delegated reply pattern ===");
        {
            // This simulates the pattern where:
            // 1. Node A sends an ask to Node B
            // 2. Node B's connection handler creates a ReplyTo
            // 3. Node B passes the ReplyTo to an actor for processing
            // 4. The actor sends the response back through the ReplyTo

            let conn = handle_a.lookup_peer(&peer_b_id).await.unwrap();
            let conn = conn
                .connection_ref()
                .expect("connection should be ready")
                .clone();
            // Simulate what would happen on Node B:
            // When an Ask arrives, the connection handler would create a ReplyTo
            // and pass it to an actor. We'll simulate this with ask_with_reply_to

            let request_payload = b"Process this request".to_vec();

            // On Node A: Send the ask and wait for response
            let response_future = {
                let conn_clone = conn.clone();
                tokio::spawn(async move {
                    conn_clone
                        .ask_actor_frame(
                            TEST_ACTOR_ID,
                            TEST_TYPE_HASH,
                            bytes::Bytes::from(request_payload),
                            Duration::from_secs(5),
                        )
                        .await
                })
            };

            // Give the message time to arrive at Node B
            sleep(Duration::from_millis(10)).await;

            let response = response_future.await.unwrap().unwrap();
            info!("Got response: {:?}", String::from_utf8_lossy(&response));
            assert_eq!(response.as_ref(), b"RESPONSE:20");
        }

        info!("=== Test 4: Performance test ===");
        {
            let conn = handle_a.lookup_peer(&peer_b_id).await.unwrap();
            let conn = conn.connection_ref().expect("connection should be ready");

            let num_requests = 100;
            let start = std::time::Instant::now();

            for i in 0..num_requests {
                let request = bytes::Bytes::from(format!("Perf test {}", i).into_bytes());
                let response = conn
                    .ask_actor_frame(
                        TEST_ACTOR_ID,
                        TEST_TYPE_HASH,
                        request,
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                assert!(response.starts_with(b"RESPONSE:"));
            }

            let elapsed = start.elapsed();
            let throughput = num_requests as f64 / elapsed.as_secs_f64();

            info!("Sequential: {} requests in {:?}", num_requests, elapsed);
            info!("Throughput: {:.0} req/sec", throughput);
        }

        info!("=== Test 5: Oversize ActorAsk response must stream (not inline) ===");
        {
            // Make the response payload exceed `max_message_size` once framed inline:
            // msg_len = ASK_RESPONSE_HEADER_LEN (12) + payload_len. If payload_len == max_message_size,
            // the receiver will reject the inline frame as MessageTooLarge.
            let payload_len = handle_b.registry.config.max_message_size;
            let response_bytes = bytes::Bytes::from(vec![b'Z'; payload_len]);
            handle_b
                .registry
                .set_actor_message_handler(Arc::new(LargeResponseHandler {
                    response: response_bytes.clone(),
                }))
                .await;

            let conn = handle_a.lookup_peer(&peer_b_id).await.unwrap();
            let conn = conn.connection_ref().expect("connection should be ready");
            let request = bytes::Bytes::from_static(b"ping");

            let response = conn
                .ask_actor_frame(
                    TEST_ACTOR_ID,
                    TEST_TYPE_HASH,
                    request,
                    Duration::from_secs(30),
                )
                .await
                .unwrap();

            assert_eq!(response.len(), response_bytes.len());
            assert_eq!(response[0], b'Z');
            assert_eq!(response[response.len() - 1], b'Z');
        }

        info!("=== Test 6: Connection failure path (connect to missing peer) ===");
        {
            let addr_missing: SocketAddr = "127.0.0.1:8004".parse().unwrap(); // should not exist
            let missing_peer_id =
                icanact_remote::KeyPair::new_for_testing("node_missing").peer_id();
            let missing_peer = handle_a.add_peer(&missing_peer_id).await;

            match missing_peer.connect(&addr_missing).await {
                Err(e) => info!("Expected connection failure: {}", e),
                Ok(_) => panic!("Should not connect to non-existent node"),
            }
        }

        // Shutdown
        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}
