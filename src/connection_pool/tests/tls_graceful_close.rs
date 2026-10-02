//! Local teardown must be an orderly TLS close (`close_notify`) wherever a
//! frame boundary allows it, bounded for peers that never read, while the
//! forced-abort path stays an abrupt (diagnosable) truncation for the peer.
use super::{
    BufferConfig, ChannelId, GRACEFUL_CLOSE_TIMEOUT, LockFreeStreamHandle, close_stream_gracefully,
};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{client, server};

const ADDR: &str = "127.0.0.1:9990";

/// A real, mutually authenticated TLS session over loopback TCP.
async fn tls_pair() -> (client::TlsStream<TcpStream>, server::TlsStream<TcpStream>) {
    let client_cfg = crate::tls::TlsConfig::new(crate::SecretKey::generate()).unwrap();
    let server_cfg = crate::tls::TlsConfig::new(crate::SecretKey::generate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        server_cfg.acceptor().accept(tcp).await.unwrap()
    });
    let tcp = TcpStream::connect(addr).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("peer-1.icanact.invalid").unwrap();
    let client = client_cfg.connector().connect(name, tcp).await.unwrap();
    (client, accept.await.unwrap())
}

fn frame_bytes() -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(&crate::framing::write_stream_abort_header(1, 2))
}

type Handle = (
    Arc<LockFreeStreamHandle>,
    tokio::task::JoinHandle<()>,
    client::TlsStream<TcpStream>,
);

/// A write-only handle over the server side, with one frame already
/// delivered to (and read by) the client so no write is in flight.
async fn idle_handle_with_delivered_frame() -> Handle {
    let (mut client, server) = tls_pair().await;
    let (handle, task, _) = LockFreeStreamHandle::new(
        server,
        ADDR.parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let frame = frame_bytes();
    handle.write_bytes_control(frame.clone()).await.unwrap();
    let mut got = vec![0u8; frame.len()];
    tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut got))
        .await
        .expect("frame delivery must not hang")
        .unwrap();
    assert_eq!(got, frame.as_ref());
    (Arc::new(handle), task, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_shutdown_at_frame_boundary_sends_close_notify() {
    let (handle, task, mut client) = idle_handle_with_delivered_frame().await;

    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_exit())
        .await
        .expect("shutdown must complete");
    task.await.unwrap();

    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
        .await
        .expect("peer read must not hang")
        .expect(
            "an orderly local shutdown must reach the peer as TLS close_notify, not truncation",
        );
    assert!(rest.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_and_repeated_shutdown_closes_exactly_once_cleanly() {
    let (handle, task, mut client) = idle_handle_with_delivered_frame().await;

    let mut waiters = Vec::new();
    for _ in 0..4 {
        let handle = Arc::clone(&handle);
        waiters.push(tokio::spawn(async move {
            handle.shutdown();
            handle.shutdown();
            handle.wait_for_exit().await;
        }));
    }
    for waiter in waiters {
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("every concurrent closer must return")
            .unwrap();
    }
    handle.shutdown();
    task.await.unwrap();

    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
        .await
        .expect("peer read must not hang")
        .expect("repeated shutdown must still be one clean close_notify");
    assert!(rest.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_abort_reaches_the_peer_as_truncation_without_close_notify() {
    let (handle, task, mut client) = idle_handle_with_delivered_frame().await;

    // Fault/eviction path: the task is aborted, the stream dropped abruptly.
    task.abort();
    let _ = task.await;
    drop(handle);

    let mut rest = Vec::new();
    let error = tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
        .await
        .expect("peer read must not hang")
        .expect_err("a forced abort must not look like an orderly TLS close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_peer_that_never_reads_is_bounded() {
    let (client, server) = tls_pair().await; // `client` is held open but never read.
    let (handle, task, _) = LockFreeStreamHandle::new(
        server,
        ADDR.parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let payload = bytes::Bytes::from(vec![0u8; 16 * 1024]);
    let header = crate::framing::try_write_ask_response_header(
        crate::MessageType::Response,
        1,
        payload.len(),
    )
    .unwrap();
    let mut frame = Vec::from(&header[..16]);
    frame.extend_from_slice(&payload);
    let frame = bytes::Bytes::from(frame);
    for _ in 0..4096 {
        if handle.write_bytes_nonblocking(frame.clone()).is_err() {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = Instant::now();
    handle.shutdown();
    tokio::time::timeout(
        GRACEFUL_CLOSE_TIMEOUT + Duration::from_secs(3),
        handle.wait_for_exit(),
    )
    .await
    .expect("teardown of a non-reading peer must be bounded");
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(started.elapsed() < GRACEFUL_CLOSE_TIMEOUT + Duration::from_secs(3));
    drop(client);
}

/// `poll_shutdown` never completes, like a TLS close against a full send
/// buffer on a half-open connection.
struct StuckShutdown {
    shutdown_polled: Arc<AtomicBool>,
}

impl AsyncRead for StuckShutdown {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for StuckShutdown {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(b.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.shutdown_polled.store(true, Ordering::SeqCst);
        Poll::Pending
    }
}

#[tokio::test]
async fn graceful_close_gives_up_after_the_bound_without_awaiting_the_peer() {
    let polled = Arc::new(AtomicBool::new(false));
    let mut stream = StuckShutdown {
        shutdown_polled: polled.clone(),
    };
    let started = Instant::now();
    let completed = close_stream_gracefully(&mut stream).await;
    let elapsed = started.elapsed();
    assert!(polled.load(Ordering::SeqCst), "shutdown must be attempted");
    assert!(!completed, "a stuck close must report failure");
    assert!(
        elapsed >= GRACEFUL_CLOSE_TIMEOUT
            && elapsed < GRACEFUL_CLOSE_TIMEOUT + Duration::from_secs(1),
        "close must be bounded by GRACEFUL_CLOSE_TIMEOUT, took {elapsed:?}"
    );
}

#[tokio::test]
async fn graceful_close_completes_promptly_when_the_peer_is_reading() {
    let (mut local, mut peer) = tokio::io::duplex(1024);
    local.write_all(b"x").await.unwrap();
    assert!(close_stream_gracefully(&mut local).await);
    let mut got = Vec::new();
    peer.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"x");
}

/// Wait (bounded) until the handle has stopped making write progress because
/// the peer is not reading, i.e. the IO task is parked inside a frame.
async fn wait_until_write_stalled(handle: &LockFreeStreamHandle) {
    let mut last = handle.bytes_written();
    let mut stable_since = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = handle.bytes_written();
        if now != last {
            last = now;
            stable_since = Instant::now();
        } else if last > 0 && stable_since.elapsed() >= Duration::from_millis(400) {
            return;
        }
        assert!(Instant::now() < deadline, "write never stalled");
    }
}

/// Peer reads everything the closed connection delivered and must observe
/// truncation (no `close_notify`), never an orderly end of stream.
async fn assert_peer_sees_truncation(mut peer: client::TlsStream<TcpStream>) {
    let mut sink = Vec::new();
    let error = tokio::time::timeout(Duration::from_secs(15), peer.read_to_end(&mut sink))
        .await
        .expect("peer read must not hang")
        .expect_err("a mid-frame shutdown must not look like an orderly TLS close");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(!sink.is_empty(), "the partial frame bytes were delivered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_parked_ordinary_write_stays_abrupt() {
    let (peer, server) = tls_pair().await; // peer never reads until after shutdown
    let (handle, task, _) = LockFreeStreamHandle::new(
        server,
        ADDR.parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let payload = bytes::Bytes::from(vec![7u8; 16 * 1024]);
    let header = crate::framing::try_write_ask_response_header(
        crate::MessageType::Response,
        1,
        payload.len(),
    )
    .unwrap();
    let mut frame = Vec::from(&header[..16]);
    frame.extend_from_slice(&payload);
    let frame = bytes::Bytes::from(frame);
    for _ in 0..8192 {
        if handle.write_bytes_nonblocking(frame.clone()).is_err() {
            break;
        }
    }
    wait_until_write_stalled(&handle).await;

    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_exit())
        .await
        .expect("teardown bounded");
    assert!(
        !handle.exited_orderly(),
        "a parked partial write must not be reported as an orderly exit"
    );
    let _ = task.await;
    assert_peer_sees_truncation(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_mid_stream_frame_stays_abrupt() {
    let (peer, server) = tls_pair().await;
    let (handle, task, _) = LockFreeStreamHandle::new(
        server,
        ADDR.parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let streamer = {
        let handle = Arc::clone(&handle);
        tokio::spawn(async move {
            let _ = handle
                .stream_large_message_bytes(bytes::Bytes::from(vec![9u8; 8 * 1024 * 1024]), 1, 2)
                .await;
        })
    };
    wait_until_write_stalled(&handle).await;

    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_exit())
        .await
        .expect("teardown bounded");
    assert!(
        !handle.exited_orderly(),
        "a mid-frame stream must not be reported as an orderly exit"
    );
    let _ = task.await;
    streamer.abort();
    assert_peer_sees_truncation(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orderly_local_shutdown_is_flagged_orderly() {
    let (handle, task, _client) = idle_handle_with_delivered_frame().await;
    handle.shutdown();
    handle.wait_for_exit().await;
    let _ = task.await;
    assert!(handle.exited_orderly());
}
