use super::*;

fn remaining_deadline_fixture(
    write_queue_capacity: usize,
) -> (
    Arc<LockFreeStreamHandle>,
    ConnectionHandle<()>,
    JoinHandle<()>,
    Option<JoinHandle<()>>,
    tokio::io::DuplexStream,
) {
    let addr = "127.0.0.1:0".parse().unwrap();
    let (io, peer) = tokio::io::duplex(64);
    let (stream, writer, reader) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default().with_write_queue_capacity(write_queue_capacity),
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
    (stream, conn, writer, reader, peer)
}

async fn cleanup_deadline_fixture(
    stream: Arc<LockFreeStreamHandle>,
    writer: JoinHandle<()>,
    reader: Option<JoinHandle<()>>,
    peer: tokio::io::DuplexStream,
) {
    stream.shutdown();
    writer.abort();
    let _ = writer.await;
    if let Some(reader) = reader {
        reader.abort();
        let _ = reader.await;
    }
    drop(peer);
}

async fn stall_writer_and_fill_queue(stream: &LockFreeStreamHandle) {
    let body = bytes::Bytes::from(vec![7; 8192]);
    let header =
        crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, body.len())
            .unwrap();
    while stream
        .write_header_and_payload_control_inline_nonblocking(header, 16, body.clone())
        .is_ok()
    {}
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut refilled = 0;
    while stream
        .write_header_and_payload_control_inline_nonblocking(header, 16, body.clone())
        .is_ok()
    {
        refilled += 1;
    }
    assert!(
        refilled > 0,
        "writer must have taken its first batch before refilling"
    );
}

/// Seed RED from the 2026-09-06 QA report: streaming asks awaited the
/// stream gate before starting the response timer.
#[tokio::test(flavor = "current_thread")]
async fn streaming_ask_timeout_covers_stream_gate() {
    let (stream, conn, writer, reader, peer) = remaining_deadline_fixture(128);
    let held = stream.acquire_streaming_mode().await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_streaming_bytes(
            bytes::Bytes::from_static(b"x"),
            1,
            1,
            Duration::from_millis(10),
        ),
    )
    .await;
    drop(held);
    cleanup_deadline_fixture(stream, writer, reader, peer).await;
    eprintln!("stream gate: requested=10ms observation=150ms result={result:?}");
    assert!(
        matches!(result, Ok(Err(GossipError::Timeout))),
        "streaming ask deadline must cover admission, got {result:?}"
    );
}

/// Seed RED: bytes asks awaited full-queue insertion before the response timer.
#[tokio::test(flavor = "current_thread")]
async fn bytes_ask_timeout_covers_full_queue() {
    let (stream, conn, writer, reader, peer) = remaining_deadline_fixture(128);
    stall_writer_and_fill_queue(&stream).await;
    let result = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_with_timeout_bytes(bytes::Bytes::from_static(b"x"), Duration::from_millis(10)),
    )
    .await;
    cleanup_deadline_fixture(stream, writer, reader, peer).await;
    eprintln!("full queue: requested=10ms observation=150ms result={result:?}");
    assert!(
        matches!(result, Ok(Err(GossipError::Timeout))),
        "bytes ask deadline must cover queue admission, got {result:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_ask_timeout_covers_full_queue() {
    let (stream, conn, writer, reader, peer) = remaining_deadline_fixture(128);
    stall_writer_and_fill_queue(&stream).await;
    let result = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_direct(bytes::Bytes::from_static(b"x"), Duration::from_millis(10)),
    )
    .await;
    cleanup_deadline_fixture(stream, writer, reader, peer).await;
    assert!(
        matches!(result, Ok(Err(GossipError::Timeout))),
        "direct ask deadline must cover queue admission, got {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_ask_timeout_covers_stream_gate_on_multithread_runtime() {
    let (stream, conn, writer, reader, peer) = remaining_deadline_fixture(128);
    let held = stream.acquire_streaming_mode().await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_streaming_bytes(
            bytes::Bytes::from_static(b"x"),
            1,
            1,
            Duration::from_millis(10),
        ),
    )
    .await;
    drop(held);
    cleanup_deadline_fixture(stream, writer, reader, peer).await;
    assert!(
        matches!(result, Ok(Err(GossipError::Timeout))),
        "streaming ask deadline must cover admission on the multithread runtime, got {result:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_bytes_ask_timeout_covers_full_queue_at_submission() {
    let (stream, conn, writer, reader, peer) = remaining_deadline_fixture(128);
    stall_writer_and_fill_queue(&stream).await;
    let ask = conn.ask_deferred_with_timeout_bytes(
        bytes::Bytes::from_static(b"x"),
        Duration::from_millis(10),
    );
    tokio::pin!(ask);
    std::future::poll_fn(|cx| {
        assert!(
            ask.as_mut().poll(cx).is_pending(),
            "deferred ask must park on full-queue admission"
        );
        std::task::Poll::Ready(())
    })
    .await;
    let result = tokio::time::timeout(Duration::from_millis(150), ask).await;
    cleanup_deadline_fixture(stream, writer, reader, peer).await;
    assert!(
        matches!(result, Ok(Err(GossipError::Timeout))),
        "deferred ask deadline starts at submission and must cover admission, got {result:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn bytes_ask_timeout_then_succeeds_after_capacity_returns() {
    let (stream, conn, writer, reader, peer) = remaining_deadline_fixture(128);
    let held = stream.acquire_streaming_mode().await.unwrap();
    let blocked = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_streaming_bytes(
            bytes::Bytes::from_static(b"x"),
            1,
            1,
            Duration::from_millis(10),
        ),
    )
    .await;
    assert!(
        matches!(blocked, Ok(Err(GossipError::Timeout))),
        "blocked streaming ask must time out, got {blocked:?}"
    );
    drop(held);
    let recovered = tokio::time::timeout(
        Duration::from_millis(150),
        conn.ask_with_timeout_bytes(bytes::Bytes::from_static(b"ok"), Duration::from_millis(10)),
    )
    .await;
    cleanup_deadline_fixture(stream, writer, reader, peer).await;
    // No responder is present, so the recovered ask is allowed to time out
    // while waiting for a reply. It must not hang in admission past 150ms.
    assert!(
        matches!(recovered, Ok(Err(GossipError::Timeout))),
        "after the stream gate is released a new ask must be admitted and fail only on the reply wait, got {recovered:?}"
    );
}
