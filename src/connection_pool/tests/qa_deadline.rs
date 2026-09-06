use super::*;

fn deadline_fixture() -> (
    Arc<LockFreeStreamHandle>,
    ConnectionHandle<()>,
    JoinHandle<()>,
    Option<JoinHandle<()>>,
) {
    let addr = "127.0.0.1:9991".parse().unwrap();
    let (io, _peer) = tokio::io::duplex(1024);
    let (stream, writer, reader) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let stream = Arc::new(stream);
    let conn = ConnectionHandle::<()>::new_stream(
        addr,
        ConnectionDirection::Outbound,
        stream.clone(),
        CorrelationTracker::new(),
    );
    (stream, conn, writer, reader)
}

#[tokio::test]
async fn actor_ask_timeout_covers_route_admission() {
    let (stream, conn, writer, reader) = deadline_fixture();
    let gate = stream.route_bind_gate.lock().await;
    let outcome = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame(1, 1, bytes::Bytes::new(), Duration::from_millis(10)),
    )
    .await;
    drop(gate);
    stream.shutdown();
    writer.abort();
    if let Some(reader) = reader {
        reader.abort();
    }
    assert!(
        matches!(outcome, Ok(Err(GossipError::Timeout))),
        "10ms ask budget must expire while admission is blocked, got {outcome:?}"
    );
}

#[tokio::test]
async fn actor_ask_aligned_timeout_covers_route_admission() {
    let (stream, conn, writer, reader) = deadline_fixture();
    let gate = stream.route_bind_gate.lock().await;
    let outcome = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame_aligned(1, 1, bytes::Bytes::new(), Duration::from_millis(10)),
    )
    .await;
    drop(gate);
    stream.shutdown();
    writer.abort();
    if let Some(reader) = reader {
        reader.abort();
    }
    assert!(
        matches!(outcome, Ok(Err(GossipError::Timeout))),
        "10ms aligned ask budget must expire while admission is blocked, got {outcome:?}"
    );
}

#[tokio::test]
async fn actor_ask_with_request_id_timeout_covers_route_admission() {
    let (stream, conn, writer, reader) = deadline_fixture();
    // Request-id asks skip the route-bind gate and wait on identification /
    // write enqueue. Hold identification instead.
    stream.begin_identify_gate();
    let outcome = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame_with_request_id(
            1,
            1,
            bytes::Bytes::new(),
            Duration::from_millis(10),
            7,
        ),
    )
    .await;
    stream.shutdown();
    writer.abort();
    if let Some(reader) = reader {
        reader.abort();
    }
    assert!(
        matches!(outcome, Ok(Err(GossipError::Timeout))),
        "10ms request-id ask budget must expire while identification is blocked, got {outcome:?}"
    );
}

#[tokio::test]
async fn actor_ask_timeout_covers_identification() {
    let (stream, conn, writer, reader) = deadline_fixture();
    stream.begin_identify_gate();
    let outcome = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame(1, 1, bytes::Bytes::new(), Duration::from_millis(10)),
    )
    .await;
    stream.shutdown();
    writer.abort();
    if let Some(reader) = reader {
        reader.abort();
    }
    assert!(
        matches!(outcome, Ok(Err(GossipError::Timeout))),
        "10ms ask budget must expire while identification is blocked, got {outcome:?}"
    );
}

#[tokio::test]
async fn actor_ask_timeout_releases_correlation_for_reuse() {
    let (stream, conn, writer, reader) = deadline_fixture();
    let gate = stream.route_bind_gate.lock().await;
    let first = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame(1, 1, bytes::Bytes::new(), Duration::from_millis(10)),
    )
    .await;
    drop(gate);
    assert!(
        matches!(first, Ok(Err(GossipError::Timeout))),
        "first blocked ask must time out, got {first:?}"
    );
    stream.begin_identify_gate();
    let second = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame(1, 1, bytes::Bytes::new(), Duration::from_millis(10)),
    )
    .await;
    stream.shutdown();
    writer.abort();
    if let Some(reader) = reader {
        reader.abort();
    }
    assert!(
        matches!(second, Ok(Err(GossipError::Timeout))),
        "timed-out ask must release its correlation slot for a later attempt, got {second:?}"
    );
}

#[tokio::test]
async fn actor_ask_timeout_covers_full_write_queue() {
    let addr = "127.0.0.1:9992".parse().unwrap();
    let (io, _peer) = tokio::io::duplex(64);
    let buffer_config = BufferConfig::default().with_write_queue_capacity(128);
    let cap = buffer_config.write_queue_capacity();
    let (stream, writer, reader) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        buffer_config,
        None,
        None,
    );
    let stream = Arc::new(stream);
    let conn = ConnectionHandle::<()>::new_stream(
        addr,
        ConnectionDirection::Outbound,
        stream.clone(),
        CorrelationTracker::new(),
    );
    for _ in 0..cap {
        match stream.write_queue.try_push(WriteCommand::Payload(
            WritePayload::TrustedFrame(bytes::Bytes::from_static(&[0u8; 32])),
        )) {
            Ok(()) => {}
            Err(WriteTryPushError::Full(_)) => break,
            Err(other) => panic!("unexpected fill error: {other:?}"),
        }
    }
    let outcome = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame(1, 1, bytes::Bytes::new(), Duration::from_millis(10)),
    )
    .await;
    stream.shutdown();
    writer.abort();
    if let Some(reader) = reader {
        reader.abort();
    }
    assert!(
        matches!(outcome, Ok(Err(GossipError::Timeout))),
        "10ms ask budget must expire while the write queue is full, got {outcome:?}"
    );
}

#[tokio::test]
async fn actor_ask_cancel_beyond_ring_capacity_still_admits() {
    use std::future::Future;
    use std::task::{Context, Poll};

    let (stream, conn, writer, reader) = deadline_fixture();
    stream.begin_identify_gate();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..(8192 + 1) {
        let ask = conn.ask_actor_frame(1, 1, bytes::Bytes::new(), Duration::from_millis(10));
        tokio::pin!(ask);
        let polled = ask.as_mut().poll(&mut cx);
        assert!(
            matches!(polled, Poll::Pending),
            "cancelled asks must park on the identify gate, got {polled:?}"
        );
        drop(ask);
    }
    let outcome = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_actor_frame(1, 1, bytes::Bytes::new(), Duration::from_millis(10)),
    )
    .await;
    stream.shutdown();
    writer.abort();
    if let Some(reader) = reader {
        reader.abort();
    }
    assert!(
        matches!(outcome, Ok(Err(GossipError::Timeout))),
        "after more than one correlation ring of cancelled asks, a new ask must still be admitted, got {outcome:?}"
    );
}
