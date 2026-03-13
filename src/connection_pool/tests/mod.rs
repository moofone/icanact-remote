    use super::*;
    use std::io::{Error, ErrorKind};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::runtime::Builder;
    use tokio::time::sleep;

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
                    handle.write_bytes_ask(bytes::Bytes::from_static(b"ping")).await?;
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
