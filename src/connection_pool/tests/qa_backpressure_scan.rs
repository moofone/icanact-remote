// Independent QA probes; no production implementation changes.
use super::{BufferConfig, ChannelId, LockFreeStreamHandle};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

struct CountWrites {
    io: tokio::io::DuplexStream,
    polls: Arc<AtomicUsize>,
}
impl AsyncRead for CountWrites {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, b)
    }
}
impl AsyncWrite for CountWrites {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.io).poll_write(cx, b)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qa_backpressured_writer_parks_and_then_recovers() {
    let (io, mut peer) = tokio::io::duplex(64);
    let polls = Arc::new(AtomicUsize::new(0));
    let (writer, task, _) = LockFreeStreamHandle::new(
        CountWrites {
            io,
            polls: polls.clone(),
        },
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let payload = bytes::Bytes::from(vec![7; 8192]);
    let header = crate::framing::try_write_ask_response_header(
        crate::MessageType::Response,
        1,
        payload.len(),
    )
    .unwrap();
    writer
        .write_header_and_payload_control_inline_nonblocking(header, 16, payload.clone())
        .unwrap();
    let mut first = [0; 1];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    // No reads or writes wake the writer during this window.
    let before = polls.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let idle_polls = polls.load(Ordering::Relaxed) - before;
    eprintln!("backpressured write polls in 100ms without readiness events: {idle_polls}");
    let mut received = vec![0; header.len() + payload.len()];
    received[0] = first[0];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut received[1..]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received[..header.len()], &header);
    assert_eq!(&received[header.len()..], payload.as_ref());
    writer.shutdown();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        idle_polls < 1000,
        "writer busy-polls a socket with no capacity: {idle_polls} polls/100ms"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn qa_partial_nack_write_observes_shutdown() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("qa-partial-nack")),
            ..Default::default()
        },
    ));
    let ctx = super::response_budget_read_context(&registry, addr);
    let (io, mut peer) = tokio::io::duplex(8);
    let (writer, mut task, _) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        Some(ctx),
    );
    // Missing handler produces a transport NACK. Only half its header fits.
    tokio::time::timeout(
        Duration::from_secs(2),
        super::write_empty_asks(&mut peer, 1),
    )
    .await
    .unwrap();
    let mut first = [0; 1];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    writer.shutdown();
    let exited = tokio::time::timeout(Duration::from_secs(2), &mut task)
        .await
        .is_ok();
    eprintln!("partial NACK: shutdown_completed_within_2s={exited}");
    if !exited {
        task.abort();
        let _ = task.await;
    }
    drop(peer);
    assert!(
        exited,
        "partial NACK writer prevents the IO owner from observing shutdown"
    );
}

struct ForwardCompletions(AtomicUsize);
impl crate::ask_forwarder::AskForwardObserver for ForwardCompletions {
    fn record_success(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn record_error(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qa_dropped_forwarder_reclaims_pending_worker() {
    use super::{ConnectionDirection, ConnectionHandle, CorrelationTracker};
    let addr = "127.0.0.1:0".parse().unwrap();
    let (io, mut peer) = tokio::io::duplex(65536);
    let (writer, task, _) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let writer = Arc::new(writer);
    let correlation = CorrelationTracker::new();
    let connection = crate::RemoteConnection::from_handle(ConnectionHandle::<()>::new_stream(
        addr,
        ConnectionDirection::Outbound,
        writer.clone(),
        correlation.clone(),
    ));
    let observer = Arc::new(ForwardCompletions(AtomicUsize::new(0)));
    let weak_observer = Arc::downgrade(&observer);
    let forwarder =
        crate::ask_forwarder::AskForwarder::new_with_observer(1, 128, Some(observer.clone()));
    forwarder
        .try_forward_actor_ask_no_timeout(
            connection,
            1,
            1,
            bytes::Bytes::from_static(b"request"),
            crate::AskResponder::from_stream_handle(
                1,
                writer.clone(),
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ),
        )
        .unwrap();
    let mut first = [0; 1];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    drop(observer);
    drop(forwarder);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let retained = weak_observer.strong_count();
    eprintln!("dropped forwarder: worker observer owners after drop={retained}");
    // Test-only cleanup stands in for an unrelated transport disconnect.
    correlation.cancel_all();
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak_observer.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    writer.shutdown();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained, 0,
        "dropping the forwarder leaves a detached worker and its pending request alive"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn qa_forward_timeout_includes_worker_queue_wait() {
    use super::{ConnectionDirection, ConnectionHandle, CorrelationTracker};
    let addr = "127.0.0.1:0".parse().unwrap();
    let (io, peer) = tokio::io::duplex(65536);
    let (writer, task, _) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let writer = Arc::new(writer);
    let connection = crate::RemoteConnection::from_handle(ConnectionHandle::<()>::new_stream(
        addr,
        ConnectionDirection::Outbound,
        writer.clone(),
        CorrelationTracker::new(),
    ));
    let completed = Arc::new(ForwardCompletions(AtomicUsize::new(0)));
    let forwarder =
        crate::ask_forwarder::AskForwarder::new_with_observer(1, 128, Some(completed.clone()));
    for id in 1..=32 {
        let responder = crate::AskResponder::from_stream_handle(
            id,
            writer.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        forwarder
            .try_forward_actor_ask_combined_timeout(
                connection.clone(),
                1,
                1,
                bytes::Bytes::from_static(b"request"),
                Duration::from_millis(200),
                responder,
                bytes::Bytes::from_static(b"timeout"),
                bytes::Bytes::from_static(b"error"),
            )
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let by_deadline = completed.0.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while completed.0.load(Ordering::SeqCst) < 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    eprintln!(
        "forwarder: 32 accepted at t=0 with 200ms timeout; completed by 300ms={by_deadline}, eventual_completed=32"
    );
    drop(forwarder);
    writer.shutdown();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    drop(peer);
    assert_eq!(
        by_deadline, 32,
        "timed forwards restart their budget when a worker finally dispatches them"
    );
}
