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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use icanact_remote::registry::RegistryMessage;
use icanact_remote::{GossipRegistryHandle, SecretKey};

async fn connect_tls(
    server_addr: SocketAddr,
    server_node_id: icanact_remote::NodeId,
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
    icanact_remote::handshake::perform_hello_handshake(&mut tls, negotiated_alpn.as_deref(), false)
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

async fn read_length_prefixed_frame<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.expect("read len");
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.expect("read frame");
    buf
}

async fn read_until_direct_response<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
    correlation_id: u16,
) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            panic!("timed out waiting for DirectResponse");
        }
        let remaining = deadline - now;
        let frame = tokio::time::timeout(remaining, read_length_prefixed_frame(stream))
            .await
            .expect("timeout");
        let frame = frame;
        if frame.first().copied() == Some(icanact_remote::MessageType::DirectResponse as u8) {
            let got_corr = u16::from_be_bytes([frame[1], frame[2]]);
            if got_corr == correlation_id {
                return frame;
            }
        }
        // Ignore other frames (gossip, fullsync, etc.) that may be emitted by the registry.
    }
}

#[tokio::test(flavor = "current_thread")]
async fn direct_ask_roundtrip_with_tcp_fragmentation() {
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle =
        GossipRegistryHandle::new("127.0.0.1:0".parse().unwrap(), server_secret.clone(), None)
            .await
            .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();

    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id).await;
    send_fullsync(&mut tls, client_peer_id).await;

    let correlation_id: u16 = 42;
    let payload = b"hello-bad-client".to_vec();
    let header = icanact_remote::framing::write_direct_ask_header(correlation_id, payload.len());

    // Send 1 byte at a time to force heavy fragmentation.
    for b in header {
        tls.write_all(&[b]).await.expect("write header byte");
    }
    for b in payload.iter().copied() {
        tls.write_all(&[b]).await.expect("write payload byte");
    }
    tls.flush().await.expect("flush");

    let frame = read_until_direct_response(&mut tls, correlation_id).await;
    assert_eq!(frame[0], icanact_remote::MessageType::DirectResponse as u8);
    let got_corr = u16::from_be_bytes([frame[1], frame[2]]);
    assert_eq!(got_corr, correlation_id);
    let got_len = u32::from_be_bytes([frame[3], frame[4], frame[5], frame[6]]) as usize;
    assert_eq!(got_len, payload.len());
    let got_payload = &frame[icanact_remote::framing::DIRECT_RESPONSE_HEADER_LEN..];
    assert_eq!(got_payload, payload.as_slice());
}

#[tokio::test(flavor = "current_thread")]
async fn truncated_frame_does_not_crash_server() {
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle =
        GossipRegistryHandle::new("127.0.0.1:0".parse().unwrap(), server_secret.clone(), None)
            .await
            .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();

    // Send a DirectAsk header that claims a payload, then drop before sending payload bytes.
    {
        let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id).await;
        send_fullsync(&mut tls, client_peer_id).await;
        let correlation_id: u16 = 7;
        let header = icanact_remote::framing::write_direct_ask_header(correlation_id, 32);
        tls.write_all(&header).await.expect("write header");
        tls.write_all(b"x").await.expect("write partial payload");
        // Drop TLS stream abruptly: server should handle EOF without panicking.
    }

    // Prove server is still alive: open a new connection and get a valid response.
    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id).await;
    send_fullsync(&mut tls, client_peer_id).await;
    let correlation_id: u16 = 9;
    let payload = b"ok".to_vec();
    let header = icanact_remote::framing::write_direct_ask_header(correlation_id, payload.len());
    tls.write_all(&header).await.expect("write header");
    tls.write_all(&payload).await.expect("write payload");
    tls.flush().await.expect("flush");

    let frame = read_until_direct_response(&mut tls, correlation_id).await;
    assert_eq!(frame[0], icanact_remote::MessageType::DirectResponse as u8);
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_message_type_is_ignored_and_server_continues() {
    icanact_remote::tls::ensure_crypto_provider();

    let server_secret = SecretKey::generate();
    let handle =
        GossipRegistryHandle::new("127.0.0.1:0".parse().unwrap(), server_secret.clone(), None)
            .await
            .expect("start server");
    let server_addr = handle.registry.bind_addr;
    let server_node_id = server_secret.public();

    let (mut tls, client_peer_id) = connect_tls(server_addr, server_node_id).await;
    send_fullsync(&mut tls, client_peer_id).await;

    // Unknown message with minimal payload.
    let payload = b"zzz".to_vec();
    let total_size = icanact_remote::framing::ASK_RESPONSE_HEADER_LEN + payload.len();
    let mut frame = Vec::with_capacity(4 + total_size);
    frame.extend_from_slice(&(total_size as u32).to_be_bytes());
    frame.push(0xFF); // unknown msg type
    frame.extend_from_slice(&0u16.to_be_bytes());
    frame.extend_from_slice(&[0u8; 9]); // pad to 12-byte ask/response header
    frame.extend_from_slice(&payload);
    tls.write_all(&frame).await.expect("write unknown frame");
    tls.flush().await.expect("flush");

    // Then send a valid DirectAsk and ensure we still get a response.
    let correlation_id: u16 = 11;
    let payload = b"still-ok".to_vec();
    let header = icanact_remote::framing::write_direct_ask_header(correlation_id, payload.len());
    tls.write_all(&header).await.expect("write header");
    tls.write_all(&payload).await.expect("write payload");
    tls.flush().await.expect("flush");

    let frame = read_until_direct_response(&mut tls, correlation_id).await;
    assert_eq!(frame[0], icanact_remote::MessageType::DirectResponse as u8);
}
