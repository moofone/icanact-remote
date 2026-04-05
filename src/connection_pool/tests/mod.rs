use super::*;
use futures::StreamExt;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::runtime::Builder;
use tokio::time::sleep;

struct TestActor;

impl crate::registry::ActorMessageHandlerSync for TestActor {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> crate::Result<Option<crate::registry::ActorResponse>> {
        if actor_id != 0xC0DE_BEEF || type_hash != 0xA11C_0001 {
            return Ok(None);
        }
        if correlation_id.is_some() {
            Ok(Some(crate::registry::ActorResponse::from(payload)))
        } else {
            Ok(None)
        }
    }
}

const TEST_TELL_ACTOR_ID: u64 = 0xC0DE_BEEF;
const TEST_TELL_HASH: u32 = 0xA11C_0001;
struct TestActorCounter {
    delivered: Arc<AtomicU64>,
}

impl crate::registry::ActorMessageHandlerSync for TestActorCounter {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        _payload: crate::AlignedBytes,
        _correlation_id: Option<u16>,
    ) -> crate::Result<Option<crate::registry::ActorResponse>> {
        if actor_id != TEST_TELL_ACTOR_ID {
            return Ok(None);
        }
        if type_hash == TEST_TELL_HASH {
            self.delivered.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        Ok(None)
    }
}

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024; // Prevent stack overflow during large test runs
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;

fn run_multi_thread_test<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("icanact-conn-test".into())
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build test runtime");
            rt.block_on(future);
        })
        .expect("failed to spawn test thread");
    handle.join().expect("test thread panicked unexpectedly");
}

fn reset_io_perf() {
    let _ = IoPerfCounters::global().snapshot_and_reset();
}

fn print_io_perf(label: &str) {
    let (
        read_calls,
        read_ns,
        handle_calls,
        handle_ns,
        write_calls,
        write_ns,
        ask_write_calls,
        ask_write_ns,
    ) = IoPerfCounters::global().snapshot_and_reset();
    println!(
        "[{label}_io_perf] read_calls={} read_avg_us={:.3} handle_calls={} handle_avg_us={:.3} write_calls={} write_avg_us={:.3} ask_write_calls={} ask_write_avg_us={:.3}",
        read_calls,
        (read_ns as f64 / 1000.0) / (read_calls.max(1) as f64),
        handle_calls,
        (handle_ns as f64 / 1000.0) / (handle_calls.max(1) as f64),
        write_calls,
        (write_ns as f64 / 1000.0) / (write_calls.max(1) as f64),
        ask_write_calls,
        (ask_write_ns as f64 / 1000.0) / (ask_write_calls.max(1) as f64),
    );
}

/// Simple in-memory writer that records bytes for verification without
/// requiring a TCP socket. Used to keep the send_data tests fully
/// deterministic and stack-friendly.
#[derive(Clone, Default)]
struct RecordingWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl RecordingWriter {
    fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
        let writer = Self::default();
        (writer.clone(), writer.buffer.clone())
    }
}

impl Unpin for RecordingWriter {}

impl tokio::io::AsyncRead for RecordingWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // No readable bytes. The IO task doesn't use reads in these tests (read_context=None),
        // but LockFreeStreamHandle requires AsyncRead + AsyncWrite.
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for RecordingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Ok(mut guard) = self.buffer.lock() {
            guard.extend_from_slice(buf); // ALLOW_COPY
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Copy, Default)]
struct ClosedWriter;

impl Unpin for ClosedWriter {}

impl tokio::io::AsyncRead for ClosedWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ClosedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, "writer closed")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, "writer closed")))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn test_connection_handle_debug() {
    // Compile-time test to ensure Debug is implemented
    use std::fmt::Debug;
    fn assert_debug<T: Debug>() {}
    assert_debug::<ConnectionHandle>();
}

#[test]
fn test_buffer_config_validation() {
    // Should reject buffers < 256KB
    let result = BufferConfig::new(100 * 1024);
    assert!(result.is_err());

    // Should accept valid sizes
    let config = BufferConfig::new(512 * 1024).unwrap();
    assert_eq!(config.tcp_buffer_size(), 512 * 1024);

    // Streaming threshold should be buffer_size - 1KB
    assert_eq!(config.streaming_threshold(), 511 * 1024);
}

#[test]
fn test_streaming_threshold_calculation() {
    let config = BufferConfig::new(1024 * 1024).unwrap();

    // 1MB buffer should have ~1MB-1KB threshold
    let threshold = config.streaming_threshold();
    assert!(threshold < config.tcp_buffer_size());
    assert!(threshold > 1020 * 1024); // At least 1020KB
    assert_eq!(threshold, 1023 * 1024); // Exactly 1023KB
}

#[test]
fn test_buffer_config_default() {
    let config = BufferConfig::default();
    assert_eq!(config.tcp_buffer_size(), 1024 * 1024); // 1MB
    assert_eq!(config.streaming_threshold(), 1023 * 1024); // 1MB - 1KB
    assert_eq!(config.ask_window(), crate::config::DEFAULT_ASK_WINDOW);
}

#[test]
fn test_buffer_config_minimum_size() {
    // Test exactly at minimum boundary
    let config = BufferConfig::new(256 * 1024).unwrap();
    assert_eq!(config.tcp_buffer_size(), 256 * 1024);
    assert_eq!(config.streaming_threshold(), 255 * 1024);

    // Test just below minimum (should fail)
    let result = BufferConfig::new(256 * 1024 - 1);
    assert!(result.is_err());
}

#[test]
fn test_streaming_threshold_saturation() {
    // Test that streaming_threshold handles edge cases properly
    let config = BufferConfig::new(256 * 1024).unwrap(); // Minimum buffer (256KB)
    // Should be 255KB (256KB - 1KB)
    assert_eq!(config.streaming_threshold(), 255 * 1024);

    // Test with exactly 1KB buffer would be rejected by validation,
    // but we can verify saturating_sub behavior directly
    let large_config = BufferConfig::new(2 * 1024 * 1024).unwrap(); // 2MB
    assert_eq!(large_config.streaming_threshold(), 2 * 1024 * 1024 - 1024);
}

#[test]
fn test_should_flush_rules() {
    assert!(!should_flush(
        0,
        Duration::from_millis(1),
        4 * 1024,
        WRITER_MAX_LATENCY
    ));
    assert!(should_flush(
        4096,
        Duration::from_millis(1),
        4 * 1024,
        WRITER_MAX_LATENCY
    ));
    assert!(should_flush(
        1,
        Duration::from_millis(1),
        4 * 1024,
        WRITER_MAX_LATENCY
    ));
    assert!(should_flush(
        1,
        WRITER_MAX_LATENCY + Duration::from_micros(1),
        4 * 1024,
        WRITER_MAX_LATENCY
    ));
}

#[tokio::test]
async fn test_connection_pool_new() {
    let pool = ConnectionPool::<()>::new(10, Duration::from_secs(5));
    assert_eq!(pool.connection_count(), 0);
    assert_eq!(pool.max_connections, 10);
    assert_eq!(pool.connection_timeout, Duration::from_secs(5));
}

#[tokio::test]
async fn test_set_registry() {
    use crate::{GossipConfig, KeyPair, registry::GossipRegistry};
    let pool = ConnectionPool::<()>::new(10, Duration::from_secs(5));
    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:8080".parse().unwrap(),
        GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("conn_pool_registry")),
            ..Default::default()
        },
    ));

    pool.set_registry(registry.clone());
    assert!(pool.registry.load().upgrade().is_some());
}

#[test]
fn test_connection_handle_send_data() {
    run_multi_thread_test(async {
        let (writer, recorded) = RecordingWriter::new();

        let (stream_handle, _writer_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);

        let handle = ConnectionHandle::<()>::new_stream(
            "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            CorrelationTracker::new(),
        );

        let data = vec![1, 2, 3, 4];
        handle.send_data(data.clone()).await.unwrap();

        // Allow the background writer to drain the queue
        sleep(Duration::from_millis(10)).await;

        let recorded = recorded.lock().unwrap().clone();
        assert_eq!(recorded, data);
    });
}

#[test]
fn test_writer_owner_batch_preserves_order() {
    run_multi_thread_test(async {
        let (writer, recorded) = RecordingWriter::new();

        let (stream_handle, _writer_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );

        let payloads = [
            bytes::Bytes::from_static(b"one"),
            bytes::Bytes::from_static(b"two"),
            bytes::Bytes::from_static(b"three"),
        ];

        for payload in &payloads {
            stream_handle
                .write_bytes_nonblocking(payload.clone())
                .expect("enqueue payload");
        }

        // Allow the background writer to drain the queue
        sleep(Duration::from_millis(10)).await;

        let recorded = recorded.lock().unwrap().clone();
        let expected = payloads.concat();
        assert_eq!(recorded, expected);
    });
}

#[test]
fn test_writer_vectored_sequence_header_payload() {
    run_multi_thread_test(async {
        let (writer, recorded) = RecordingWriter::new();

        let (stream_handle, _writer_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );

        let first = bytes::Bytes::from_static(b"first");
        let second = bytes::Bytes::from_static(b"second");
        let header = bytes::Bytes::from_static(b"HEAD");
        let payload = bytes::Bytes::from_static(b"PAYLOAD");

        stream_handle
            .write_bytes_nonblocking(first.clone())
            .expect("enqueue first");
        stream_handle
            .write_bytes_nonblocking(second.clone())
            .expect("enqueue second");
        stream_handle
            .write_header_and_payload_nonblocking(header.clone(), payload.clone())
            .expect("enqueue header+payload");

        // Allow the background writer to drain the queue
        sleep(Duration::from_millis(10)).await;

        let recorded = recorded.lock().unwrap().clone();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first);
        expected.extend_from_slice(&second);
        expected.extend_from_slice(&header);
        expected.extend_from_slice(&payload);
        assert_eq!(recorded, expected);
    });
}

#[test]
fn parse_direct_message_payload_success() {
    let mut frame = vec![crate::MessageType::DirectAsk as u8, 0x12, 0x34];
    frame.extend_from_slice(&(4u32).to_be_bytes()); /* ALLOW_COPY */
    frame.extend_from_slice(&[0u8; 5]); /* ALLOW_COPY */
    frame.extend_from_slice(b"PING"); /* ALLOW_COPY */

    let payload = super::parse_direct_message_payload(&frame).expect("parse ok");
    assert_eq!(payload, b"PING");
}

#[test]
fn parse_direct_message_payload_truncated() {
    let mut frame = vec![crate::MessageType::DirectAsk as u8, 0x12, 0x34];
    frame.extend_from_slice(&(4u32).to_be_bytes()); /* ALLOW_COPY */
    frame.extend_from_slice(&[0u8; 5]); /* ALLOW_COPY */
    frame.extend_from_slice(b"PI"); /* ALLOW_COPY */

    match super::parse_direct_message_payload(&frame) {
        Err(super::DirectPayloadError::PayloadTruncated {
            expected,
            available,
        }) => {
            assert_eq!(expected, 4);
            assert_eq!(available, 2);
        }
        other => panic!("unexpected parse result: {:?}", other),
    }
}

#[test]
fn parse_direct_message_payload_header_too_short() {
    let frame = vec![0u8; 3];
    assert_eq!(
        super::parse_direct_message_payload(&frame),
        Err(super::DirectPayloadError::HeaderTooShort)
    );
}

#[test]
fn test_connection_handle_send_data_closed() {
    run_multi_thread_test(async {
        let (stream_handle, _writer_task) = LockFreeStreamHandle::new(
            ClosedWriter,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);

        let handle = ConnectionHandle::<()>::new_stream(
            "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            CorrelationTracker::new(),
        );

        let result = handle.send_data(vec![1, 2, 3]).await;
        assert!(result.is_ok());
    });
}

#[tokio::test]
async fn test_task_tracker_aborts_on_drop() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let task_started = Arc::new(AtomicBool::new(false));
    let task_completed = Arc::new(AtomicBool::new(false));
    let started_clone = task_started.clone();
    let completed_clone = task_completed.clone();

    let handle = tokio::spawn(async move {
        started_clone.store(true, Ordering::SeqCst);
        // Long sleep that should be aborted
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        completed_clone.store(true, Ordering::SeqCst);
    });

    // Give task time to start
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        task_started.load(Ordering::SeqCst),
        "Task should have started"
    );

    // Create tracker and set the handle
    let tracker = TaskTracker::new();
    tracker.set_writer(handle.abort_handle());

    // Drop the tracker - this should abort the task
    drop(tracker);

    // Give task time to be aborted
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Task should NOT have completed (it was aborted)
    assert!(
        !task_completed.load(Ordering::SeqCst),
        "Task should have been aborted, not completed"
    );
}

#[tokio::test]
async fn test_task_tracker_replaces_old_handle() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let task2_started = Arc::new(AtomicBool::new(false));

    let handle1 = tokio::spawn(async move {
        // Long sleep that should be aborted when handle2 replaces it
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });

    let started_clone = task2_started.clone();
    let handle2 = tokio::spawn(async move {
        started_clone.store(true, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });

    let tracker = TaskTracker::new();

    // Set first handle
    tracker.set_writer(handle1.abort_handle());

    // Set second handle - first should be aborted
    tracker.set_writer(handle2.abort_handle());

    // Give task2 time to start
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        task2_started.load(Ordering::SeqCst),
        "Second task should have started"
    );

    // Clean up
    drop(tracker);
}

#[tokio::test]
async fn test_wait_for_response_returns_on_cancelled_slot() {
    let tracker = CorrelationTracker::new();
    let correlation_id = tracker.allocate();

    // Simulate a connection drop cancelling all pending requests.
    tracker.cancel_all();

    let res = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        tracker.wait_for_response(correlation_id, std::time::Duration::from_millis(50)),
    )
    .await;

    let err = res
        .expect("wait_for_response hung")
        .expect_err("expected error");
    assert!(matches!(err, GossipError::Timeout));
}

#[tokio::test]
async fn test_ask_backpressure_no_write_buffer_full() {
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);

    let (handle, _writer_task) = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);

    let reader_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let mut tasks = Vec::new();
    for _ in 0..100 {
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                handle
                    .write_bytes_ask(bytes::Bytes::from_static(b"ping"))
                    .await?;
            }
            Ok::<(), crate::GossipError>(())
        }));
    }

    for task in tasks {
        task.await.unwrap().unwrap();
    }

    handle.shutdown();
    drop(handle);
    reader_task.abort();
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
struct WireMsg {
    value: u64,
}

crate::wire_type!(WireMsg, "connection_pool_tests::WireMsg");

#[tokio::test]
async fn test_pooled_typed_send_matches_wire_bytes() {
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);
    let (handle, _writer_task) = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );

    let msg = WireMsg { value: 99 };
    let expected = crate::typed::encode_typed(&msg).expect("encode_typed");

    let pooled = crate::typed::encode_typed_pooled(&msg).expect("encode_typed_pooled");
    let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<WireMsg>(pooled);
    let mut header = [0u8; 16];
    header[..4].copy_from_slice(&(payload_len as u32).to_be_bytes());
    let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
    handle
        .write_pooled_control_inline(header, 4, prefix, prefix_len, payload)
        .await
        .unwrap();

    let mut len_buf = [0u8; 4];
    tokio::io::AsyncReadExt::read_exact(&mut reader, &mut len_buf)
        .await
        .unwrap();
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; payload_len];
    tokio::io::AsyncReadExt::read_exact(&mut reader, &mut payload)
        .await
        .unwrap();

    assert_eq!(payload, expected.as_ref());
    handle.shutdown();
}

#[test]
fn stream_direct_ask_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:41001".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:41002".parse().unwrap();
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_direct_ask_throughput_bench",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, mut server_io) = tokio::io::duplex(1024 * 1024);
        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            sync_actor_handler: None,
        };
        let (client_writer, _writer_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let responder = tokio::spawn(async move {
            let mut len_buf = [0u8; crate::framing::LENGTH_PREFIX_LEN];
            loop {
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut len_buf)
                    .await
                    .is_err()
                {
                    break;
                }
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut msg = vec![0u8; msg_len];
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut msg)
                    .await
                    .is_err()
                {
                    break;
                }

                if msg_len >= crate::framing::DIRECT_ASK_HEADER_LEN
                    && msg[0] == crate::MessageType::DirectAsk as u8
                {
                    let correlation_id = u16::from_be_bytes([msg[1], msg[2]]);
                    let payload_len = u32::from_be_bytes([msg[3], msg[4], msg[5], msg[6]]) as usize;
                    let payload = &msg[crate::framing::DIRECT_ASK_HEADER_LEN
                        ..crate::framing::DIRECT_ASK_HEADER_LEN + payload_len];
                    let header =
                        crate::framing::write_direct_response_header(correlation_id, payload_len);
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, &header)
                        .await
                        .unwrap();
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, payload)
                        .await
                        .unwrap();
                } else if msg_len >= crate::framing::ACTOR_HEADER_LEN
                    && msg[0] == crate::MessageType::ActorAsk as u8
                {
                    let correlation_id = u16::from_be_bytes([msg[1], msg[2]]);
                    let payload_len =
                        u32::from_be_bytes([msg[24], msg[25], msg[26], msg[27]]) as usize;
                    let payload = &msg[crate::framing::ACTOR_HEADER_LEN
                        ..crate::framing::ACTOR_HEADER_LEN + payload_len];
                    let header = crate::framing::write_ask_response_header(
                        crate::MessageType::Response,
                        correlation_id,
                        payload_len,
                    );
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, &header)
                        .await
                        .unwrap();
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, payload)
                        .await
                        .unwrap();
                }
            }
        });

        let timeout = std::time::Duration::from_secs(2);
        let warmup = 5_000u64;
        let iters = 50_000u64;

        for _ in 0..warmup {
            let reply = conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_direct_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_direct_ask_no_timeout] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        for _ in 0..warmup {
            let reply = conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_actor_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_actor_frame_no_timeout(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_actor_ask_no_timeout] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let inflight = 64usize;
        let drive_direct = |count: u64| {
            let conn = conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let conn = conn.clone();
                    pending.push(Box::pin(async move {
                        conn.ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let conn = conn.clone();
                        pending.push(Box::pin(async move {
                            conn.ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let start = std::time::Instant::now();
        let checksum = drive_direct(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_direct_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );

        let drive_actor = |count: u64| {
            let conn = conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let conn = conn.clone();
                    pending.push(Box::pin(async move {
                        conn.ask_actor_frame_no_timeout(
                            0xC0DE_BEEF,
                            0xA11C_0001,
                            bytes::Bytes::from_static(b"pingpong"),
                        )
                        .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let conn = conn.clone();
                        pending.push(Box::pin(async move {
                            conn.ask_actor_frame_no_timeout(
                                0xC0DE_BEEF,
                                0xA11C_0001,
                                bytes::Bytes::from_static(b"pingpong"),
                            )
                            .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let start = std::time::Instant::now();
        let checksum = drive_actor(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_actor_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );

        client_writer.shutdown();
        responder.abort();
    });
}

#[test]
fn stream_tell_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:42001".parse().unwrap();
        let _client_addr: std::net::SocketAddr = "127.0.0.1:42002".parse().unwrap();

        let (client_io, mut server_io) = tokio::io::duplex(1024 * 1024);
        let (client_writer, _writer_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            None,
        );
        let client_writer = Arc::new(client_writer);
        let conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            CorrelationTracker::new(),
        );

        let delivered = Arc::new(AtomicU64::new(0));
        let delivered_task = Arc::clone(&delivered);
        let responder = tokio::spawn(async move {
            let mut len_buf = [0u8; crate::framing::LENGTH_PREFIX_LEN];
            loop {
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut len_buf)
                    .await
                    .is_err()
                {
                    break;
                }
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut msg = vec![0u8; msg_len];
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut msg)
                    .await
                    .is_err()
                {
                    break;
                }
                delivered_task.fetch_add(1, Ordering::Relaxed);
            }
        });

        let payload = bytes::Bytes::from(vec![0u8; 256]);
        let warmup = 10_000u64;
        let iters = 1_000_000u64;

        for _ in 0..warmup {
            conn.tell_actor_frame(0xC0DE_BEEF, 0xA11C_0001, payload.clone())
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < warmup {
            assert!(
                tokio::time::Instant::now() < deadline,
                "raw warmup tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        delivered.store(0, Ordering::Release);

        let start = std::time::Instant::now();
        for _ in 0..iters {
            conn.tell_actor_frame(0xC0DE_BEEF, 0xA11C_0001, payload.clone())
                .await
                .unwrap();
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "raw tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_tell_actor_frame_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_tell_actor_frame_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        delivered.store(0, Ordering::Release);
        let start = std::time::Instant::now();
        for _ in 0..iters {
            loop {
                match conn.try_tell_actor_frame(0xC0DE_BEEF, 0xA11C_0001, payload.clone()) {
                    Ok(()) => break,
                    Err(crate::GossipError::WriteQueueFull) => std::hint::spin_loop(),
                    Err(err) => panic!("try_tell stream bench failed: {err}"),
                }
            }
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "raw try_tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_try_tell_actor_frame_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_try_tell_actor_frame_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        client_writer.shutdown();
        responder.abort();
    });
}

#[test]
fn stream_protocol_ask_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:43001".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:43002".parse().unwrap();

        let _delivered = Arc::new(AtomicU64::new(0));
        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_ask_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_ask_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            sync_actor_handler: None,
        };
        let (client_writer, _client_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let timeout = std::time::Duration::from_secs(2);
        let warmup = 5_000u64;
        let iters = 50_000u64;

        for _ in 0..warmup {
            let reply = client_conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = client_conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_direct_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        for _ in 0..warmup {
            let reply = client_conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = client_conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_actor_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let inflight = 64usize;
        let drive_direct = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let start = std::time::Instant::now();
        let checksum = drive_direct(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_direct_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );

        let drive_actor = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_actor_frame_no_timeout(
                                0xC0DE_BEEF,
                                0xA11C_0001,
                                bytes::Bytes::from_static(b"pingpong"),
                            )
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_actor_frame_no_timeout(
                                    0xC0DE_BEEF,
                                    0xA11C_0001,
                                    bytes::Bytes::from_static(b"pingpong"),
                                )
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let start = std::time::Instant::now();
        let checksum = drive_actor(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_actor_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );

        client_writer.shutdown();
    });
}

#[test]
fn stream_protocol_direct_ask_inflight64_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:43101".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:43102".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_direct_ask_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_direct_ask_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            sync_actor_handler: None,
        };
        let (client_writer, _client_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let warmup = 5_000u64;
        let iters = 50_000u64;
        let inflight = 64usize;

        let drive_direct = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let _ = drive_direct(warmup).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let start = std::time::Instant::now();
        let checksum = drive_direct(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_direct_ask_only_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );
        client_writer.shutdown();
    });
}

#[test]
fn stream_protocol_actor_ask_inflight64_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:43201".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:43202".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_actor_ask_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_actor_ask_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            sync_actor_handler: None,
        };
        let (client_writer, _client_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let warmup = 5_000u64;
        let iters = 50_000u64;
        let inflight = 64usize;

        let drive_actor = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_actor_frame_no_timeout(
                                0xC0DE_BEEF,
                                0xA11C_0001,
                                bytes::Bytes::from_static(b"pingpong"),
                            )
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_actor_frame_no_timeout(
                                    0xC0DE_BEEF,
                                    0xA11C_0001,
                                    bytes::Bytes::from_static(b"pingpong"),
                                )
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let _ = drive_actor(warmup).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let start = std::time::Instant::now();
        let checksum = drive_actor(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_actor_ask_only_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );
        client_writer.shutdown();
    });
}

#[test]
fn stream_protocol_tell_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:44001".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:44002".parse().unwrap();
        let delivered = Arc::new(AtomicU64::new(0));

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_tell_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActorCounter {
                delivered: Arc::clone(&delivered),
            }))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_tell_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            CorrelationTracker::new(),
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let payload = bytes::Bytes::from_static(b"pingpong");
        let warmup = 10_000u64;
        let iters = 1_000_000u64;

        for _ in 0..warmup {
            client_conn
                .tell_actor_frame(TEST_TELL_ACTOR_ID, TEST_TELL_HASH, payload.clone())
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < warmup {
            assert!(
                tokio::time::Instant::now() < deadline,
                "warmup tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        delivered.store(0, Ordering::Release);

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            client_conn
                .tell_actor_frame(TEST_TELL_ACTOR_ID, TEST_TELL_HASH, payload.clone())
                .await
                .unwrap();
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_tell_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_protocol_tell_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        println!(
            "[stream_protocol_tell_observed_delivery] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_tell");
        delivered.store(0, Ordering::Release);

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            loop {
                match client_conn.try_tell_actor_frame(
                    TEST_TELL_ACTOR_ID,
                    TEST_TELL_HASH,
                    payload.clone(),
                ) {
                    Ok(()) => break,
                    Err(crate::GossipError::WriteQueueFull) => std::hint::spin_loop(),
                    Err(err) => panic!("protocol try_tell failed: {err}"),
                }
            }
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "try_tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_try_tell_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_protocol_try_tell_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_try_tell");

        client_writer.shutdown();
    });
}

#[test]
fn correlation_tracker_throughput_bench() {
    run_multi_thread_test(async {
        let tracker = CorrelationTracker::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(256));
        let iters = 100_000u64;

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let correlation_id = tracker.allocate();
            let mut payload = Some(crate::AlignedBytes::from_pooled_slice(
                b"pingpong",
                Arc::clone(&pool),
            ));
            assert!(tracker.complete(correlation_id, &mut payload));
            let reply = tracker
                .wait_for_response_no_timeout(correlation_id)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[correlation_seq] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let inflight = 64usize;
        let start = std::time::Instant::now();
        let mut pending: futures::stream::FuturesUnordered<
            futures::future::BoxFuture<'static, crate::Result<crate::AlignedBytes>>,
        > = futures::stream::FuturesUnordered::new();
        let mut next = 0u64;
        while next < iters && pending.len() < inflight {
            let correlation_id = tracker.allocate();
            let tracker_clone = Arc::clone(&tracker);
            pending.push(Box::pin(async move {
                tracker_clone
                    .wait_for_response_no_timeout(correlation_id)
                    .await
            }));
            let mut payload = Some(crate::AlignedBytes::from_pooled_slice(
                b"pingpong",
                Arc::clone(&pool),
            ));
            assert!(tracker.complete(correlation_id, &mut payload));
            next += 1;
        }
        while let Some(result) = pending.next().await {
            let reply = result.unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
            if next < iters {
                let correlation_id = tracker.allocate();
                let tracker_clone = Arc::clone(&tracker);
                pending.push(Box::pin(async move {
                    tracker_clone
                        .wait_for_response_no_timeout(correlation_id)
                        .await
                }));
                let mut payload = Some(crate::AlignedBytes::from_pooled_slice(
                    b"pingpong",
                    Arc::clone(&pool),
                ));
                assert!(tracker.complete(correlation_id, &mut payload));
                next += 1;
            }
        }
        let elapsed = start.elapsed();
        println!(
            "[correlation_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
    });
}
