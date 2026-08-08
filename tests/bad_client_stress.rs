//! "Bad client" stress tests for framing robustness.
//!
//! These tests intentionally do not use the normal icanact_remote client stack. They establish a
//! raw TLS connection and write framed messages in adversarial patterns:
//! - TCP fragmentation (1 byte writes)
//! - truncated frames (EOF mid-frame)
//! - unknown message types
//!
//! The server must not panic or deadlock, and must continue accepting subsequent connections.

use std::net::SocketAddr;
use std::sync::OnceLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use icanact_remote::registry::RegistryMessage;
use icanact_remote::{GossipRegistryHandle, SecretKey};

static BAD_CLIENT_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn connect_tls(
    server_addr: SocketAddr,
    server_node_id: icanact_remote::GossipNodeId,
    schema_hash: Option<u64>,
) -> (
    tokio_rustls::client::TlsStream<TcpStream>,
    icanact_remote::PeerId,
) {
    let client_secret = SecretKey::generate();
    let client_peer_id = client_secret.to_keypair().peer_id();
    let tls_cfg = icanact_remote::tls::TlsConfig::new(client_secret).expect("tls config");
    let server_name = icanact_remote::tls::name::encode(&server_node_id);
    let server_name = rustls::pki_types::ServerName::try_from(server_name).expect("server name");

    let tcp = TcpStream::connect(server_addr).await.expect("tcp connect");
    tcp.set_nodelay(true).expect("nodelay");

    let mut tls = tls_cfg
        .connector()
        .connect(server_name, tcp)
        .await
        .expect("tls connect");

    // The server expects the Hello handshake immediately after TLS.
    // Without it, it closes the connection before it will read framed messages.
    let negotiated_alpn = tls.get_ref().1.alpn_protocol().map(|p| p.to_vec());
    icanact_remote::handshake::perform_hello_handshake(
        &mut tls,
        negotiated_alpn.as_deref(),
        false,
        schema_hash,
        icanact_remote::handshake::RemoteBootId::new(),
    )
    .await
    .expect("hello handshake");

    (tls, client_peer_id)
}

async fn send_fullsync<S: tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    sender_peer_id: icanact_remote::PeerId,
) {
    // The server side only considers the peer "identified" after it receives a RegistryMessage
    // (FullSync/FullSyncResponse/etc). Until then, it will close the connection if it receives
    // DirectAsk/Ask/Response frames. This mimics the normal client bootstrap behavior.
    let msg = RegistryMessage::FullSync {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id,
        sender_bind_addr: None,
        sequence: 0,
        wall_clock_time: icanact_remote::current_timestamp(),
        extensions: None,
    };

    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).expect("serialize fullsync");
    let header = icanact_remote::framing::write_gossip_frame_prefix(data.len());
    stream
        .write_all(&header)
        .await
        .expect("write fullsync header");
    stream
        .write_all(data.as_ref())
        .await
        .expect("write fullsync payload");
    stream.flush().await.expect("flush fullsync");
}

async fn read_length_prefixed_frame<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> (icanact_remote::framing::Control, Vec<u8>) {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.expect("read len");
    let control = icanact_remote::framing::decode_control(len_bytes).expect("V5 control");
    let len = control.body_len;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.expect("read frame");
    (control, buf)
}

/// Wait for the ask-NACK `Response` frame for `correlation_id`. A DirectAsk
/// has no registered application handler in any build mode (see
/// `protocol::process_read_result`), so the server always answers with a
/// NACK, never a `DirectResponse` -- receiving the right NACK, for the right
/// correlation id, is what proves the server actually reassembled the
/// fragmented frame correctly rather than choking on it silently.
async fn read_until_ask_nack<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
    correlation_id: u32,
) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            panic!("timed out waiting for the ask NACK");
        }
        let remaining = deadline - now;
        let (control, frame) = tokio::time::timeout(remaining, read_length_prefixed_frame(stream))
            .await
            .expect("timeout");
        if control.kind == icanact_remote::framing::WireKind::Response {
            let got_corr = u32::from_be_bytes(frame[..4].try_into().expect("correlation id"));
            if got_corr == correlation_id
                && icanact_remote::framing::ask_nack_reason(&frame).is_some()
            {
                return frame;
            }
        }
        // Ignore other frames (gossip, fullsync, etc.) that may be emitted by the registry.
    }
}

#[tokio::test(flavor = "current_thread")]
async fn direct_ask_roundtrip_with_tcp_fragmentation() {
    let _guard = BAD_CLIENT_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        server_secret.clone(),
        None,
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();
    let schema_hash = handle.registry.config.schema_hash;

    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id, schema_hash).await;
    send_fullsync(&mut tls, client_peer_id).await;

    let correlation_id: u32 = 0x1_0000;
    let request_id: u64 = 0xfeed_face_1234_5678;
    let payload = b"hello-bad-client".to_vec();
    let header =
        icanact_remote::framing::write_direct_ask_header(correlation_id, request_id, payload.len());

    // Send 1 byte at a time to force heavy fragmentation.
    for b in header {
        tls.write_all(&[b]).await.expect("write header byte");
    }
    for b in payload.iter().copied() {
        tls.write_all(&[b]).await.expect("write payload byte");
    }
    tls.flush().await.expect("flush");

    let frame = read_until_ask_nack(&mut tls, correlation_id).await;
    let got_corr = u32::from_be_bytes(frame[..4].try_into().expect("correlation id"));
    assert_eq!(got_corr, correlation_id);
    assert_eq!(
        icanact_remote::framing::ask_nack_reason(&frame),
        Some(icanact_remote::framing::AskNackReason::NoDispatcher),
        "a fragmented-but-well-formed DirectAsk must still be reassembled and NACKed correctly"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn truncated_frame_does_not_crash_server() {
    let _guard = BAD_CLIENT_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        server_secret.clone(),
        None,
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();
    let schema_hash = handle.registry.config.schema_hash;

    // Send a DirectAsk header that claims a payload, then drop before sending payload bytes.
    {
        let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id, schema_hash).await;
        send_fullsync(&mut tls, client_peer_id).await;
        let correlation_id: u32 = 7;
        let header = icanact_remote::framing::write_direct_ask_header(correlation_id, 1, 32);
        tls.write_all(&header).await.expect("write header");
        tls.write_all(b"x").await.expect("write partial payload");
        // Drop TLS stream abruptly: server should handle EOF without panicking.
    }

    // Give the single-threaded test runtime a chance to drive the server-side EOF handling
    // before establishing the replacement connection.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Prove server is still alive: open a new connection and complete the normal handshake
    // and peer-identification bootstrap.
    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id, schema_hash).await;
    send_fullsync(&mut tls, client_peer_id).await;
    handle.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_message_type_is_ignored_and_server_continues() {
    let _guard = BAD_CLIENT_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        server_secret.clone(),
        None,
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();
    let schema_hash = handle.registry.config.schema_hash;

    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id, schema_hash).await;
    send_fullsync(&mut tls, client_peer_id).await;

    // Unknown message with minimal payload.
    let payload = b"zzz".to_vec();
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(((31u32) << 27) | payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    tls.write_all(&frame).await.expect("write unknown frame");
    tls.flush().await.expect("flush");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    drop(tls);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Then use a fresh connection and ensure the server still accepts TLS, hello, and peer
    // identification bootstrap after the malformed frame.
    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id, schema_hash).await;
    send_fullsync(&mut tls, client_peer_id).await;
    handle.shutdown().await;
}

/// Direct evidence that a raw (unaddressed) `Ask` NACKs in production,
/// rather than answering with a transformation of the caller's own request
/// bytes (the test/benchmark-only ECHO:/REVERSE:/COUNT:/HASH: command
/// processor, gated on `cfg(any(test, feature = "test-helpers"))` --
/// `handle::handle_raw_ask_request`).
///
/// This file is NOT gated behind `feature = "test-helpers"`, and this test
/// is run explicitly under `cargo test --release` with no extra features
/// (see the PR verification output) specifically so it exercises the exact
/// build configuration under dispute: a real release binary, not `cargo
/// test`'s dev profile, and not an emulation via a direct function call
/// like `handle::tests::raw_ask_with_no_dispatcher_nacks_instead_of_silence`.
///
/// This test itself needs `test-helpers` OFF: the server under test is the
/// same crate build as the test binary, so `cargo test --all-features`
/// (CI's config) would enable the mock on the server too, and it would
/// legitimately answer with the ECHOED: transformation instead of a NACK --
/// that's `test_basic_ask_correlation`'s scenario, not this one.
#[cfg(not(feature = "test-helpers"))]
#[tokio::test(flavor = "current_thread")]
async fn raw_ask_nacks_in_a_true_release_build() {
    let _guard = BAD_CLIENT_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        server_secret.clone(),
        None,
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();
    let schema_hash = handle.registry.config.schema_hash;

    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id, schema_hash).await;
    send_fullsync(&mut tls, client_peer_id).await;

    // A command the test/benchmark-only mock command processor DOES
    // recognize (ECHO:) -- if production were still fabricating a reply
    // instead of NACKing, this would come back as "ECHOED:release-should-nack",
    // not a NACK.
    let correlation_id: u32 = 0x2_a5c1;
    let payload = b"ECHO:release-should-nack".to_vec();
    let header = icanact_remote::framing::write_ask_response_header(
        icanact_remote::MessageType::Ask,
        correlation_id,
        payload.len(),
    );
    tls.write_all(&header).await.expect("write ask header");
    tls.write_all(&payload).await.expect("write ask payload");
    tls.flush().await.expect("flush");

    let frame = read_until_ask_nack(&mut tls, correlation_id).await;
    let got_corr = u32::from_be_bytes(frame[..4].try_into().expect("correlation id"));
    assert_eq!(got_corr, correlation_id);
    assert_eq!(
        icanact_remote::framing::ask_nack_reason(&frame),
        Some(icanact_remote::framing::AskNackReason::NoDispatcher),
        "a raw ask must NACK in production, never fabricate a reply from the request bytes"
    );

    handle.shutdown().await;
}

/// P1: the production no-dispatcher NACK for a raw `Ask` used to be sent via
/// `handle::send_ask_nack`, which enqueues onto the connection's shared,
/// bounded `write_queue` and falls back to *awaiting* it once full. That
/// enqueue runs from inside `process_read_result_io`, on the unified stream
/// I/O task -- the queue's only consumer, and a task that can read many
/// frames (`READ_BATCH_LIMIT`) before ever returning to drain a write. A
/// burst of raw asks large enough to fill `write_queue`
/// (`DEFAULT_ASK_WINDOW * 8` = 1024 entries) while still inside that same
/// read batch made the next NACK's enqueue block on space only the
/// now-blocked task itself could ever free -- a permanent hang, with every
/// asker past the queue's capacity timing out and every later frame on the
/// connection stuck behind it, including asks that have nothing to do with
/// the burst.
///
/// Sends 1536 raw asks (1.5x the old `write_queue` bound, comfortably
/// inside the 2048-frame read-batch limit so the whole burst lands in one
/// drain pass) in a single write, then sends one more, distinguishable ask
/// afterward and asserts *that* one still gets answered promptly. It
/// deliberately does not assert every burst member gets a reply: the fix
/// routes these NACKs through `LocalStreamingQueue::queue_ask_nack`, whose
/// own `PENDING_ASK_NACK_CAP` (64) is a separate, already-accepted
/// best-effort bound -- entries past it are dropped in favor of newer ones,
/// by design, independent of this finding. What this test isolates is
/// whether the io_task ever *permanently* wedges itself on `write_queue`;
/// a connection that is still responsive to new traffic after the burst
/// proves it did not, regardless of how the bounded NACK queue's own
/// eviction played out. `#[cfg(not(feature = "test-helpers"))]` for the
/// same reason as `raw_ask_nacks_in_a_true_release_build`: needs the real
/// production no-dispatcher path, not the test/benchmark echo mock.
#[cfg(not(feature = "test-helpers"))]
#[tokio::test(flavor = "current_thread")]
async fn a_burst_of_raw_asks_past_the_write_queue_capacity_does_not_deadlock_the_io_task() {
    let _guard = BAD_CLIENT_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        server_secret.clone(),
        None,
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();
    let schema_hash = handle.registry.config.schema_hash;

    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id, schema_hash).await;
    send_fullsync(&mut tls, client_peer_id).await;

    const BURST: u32 = 1536;
    let mut frames = Vec::new();
    for correlation_id in 0..BURST {
        let payload = b"x";
        let header = icanact_remote::framing::write_ask_response_header(
            icanact_remote::MessageType::Ask,
            correlation_id,
            payload.len(),
        );
        frames.extend_from_slice(&header);
        frames.extend_from_slice(payload);
    }
    // One more, distinguishable ask right after the burst, in the same
    // write -- this is the one whose answer proves the connection is still
    // alive, not stuck inside the burst.
    let final_correlation_id = BURST + 1_000_000;
    let final_payload = b"y";
    let final_header = icanact_remote::framing::write_ask_response_header(
        icanact_remote::MessageType::Ask,
        final_correlation_id,
        final_payload.len(),
    );
    frames.extend_from_slice(&final_header);
    frames.extend_from_slice(final_payload);

    tls.write_all(&frames)
        .await
        .expect("write burst of raw asks plus one trailing ask");
    tls.flush().await.expect("flush");

    let outcome = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let frame = read_until_ask_nack(&mut tls, final_correlation_id).await;
        assert_eq!(
            icanact_remote::framing::ask_nack_reason(&frame),
            Some(icanact_remote::framing::AskNackReason::NoDispatcher)
        );
    })
    .await;
    assert!(
        outcome.is_ok(),
        "the io_task must remain responsive to new traffic after a burst of raw asks past \
         write_queue capacity, not wedge itself awaiting space in a queue only it drains: \
         {outcome:?}"
    );

    handle.shutdown().await;
}
