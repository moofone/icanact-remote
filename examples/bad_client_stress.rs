use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::{BufMut as _, Bytes, BytesMut};
use icanact_remote::{
    GossipConfig, GossipRegistryHandle, MessageType, SecretKey,
    aligned::AlignedBytes,
    framing::ASK_RESPONSE_HEADER_LEN,
    registry::{ActorMessageFuture, ActorMessageHandler, ActorResponse},
    tls,
};
use rand::Rng as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{sleep, timeout},
};

/// A deliberately tiny "exactly once" implementation intended for stressing the
/// transport framing and reconnect/retry patterns, not for production use.
///
/// Semantics:
/// - Client provides a request_id (u128) and opaque payload bytes.
/// - First request reserves the request_id as Pending, then executes logic, then marks Done.
/// - Duplicate with same payload returns the original response.
/// - Duplicate with different payload returns INTEGRITY_ERROR.
/// - Duplicate while Pending returns PENDING.
struct ExactlyOnceTestHandler {
    inner: tokio::sync::Mutex<HashMap<u128, Entry>>,
    applied_count: AtomicUsize,
}

#[derive(Clone)]
struct Done {
    request_payload: Bytes,
    response_payload: Bytes,
}

enum Entry {
    Pending { request_payload: Bytes },
    Done(Done),
}

impl ExactlyOnceTestHandler {
    fn new() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(HashMap::new()),
            applied_count: AtomicUsize::new(0),
        }
    }
}

impl ActorMessageHandler for ExactlyOnceTestHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        Box::pin(async move {
            // Payload format: [request_id:16][body:N]
            let p = payload.as_ref();
            if p.len() < 16 {
                return Ok(Some(ActorResponse::Bytes(Bytes::from_static(
                    b"BAD_REQUEST",
                ))));
            }

            let request_id = u128::from_be_bytes(p[..16].try_into().unwrap());
            let body = Bytes::copy_from_slice(&p[16..]);
            tracing::info!(
                request_id = %format!("{request_id:032x}"),
                body_len = body.len(),
                correlation_id = correlation_id.unwrap_or(0),
                "server: actor ask received"
            );

            // 1) Atomic reservation / dedup decision.
            {
                let mut m = self.inner.lock().await;
                match m.get(&request_id) {
                    None => {
                        tracing::info!(
                            request_id = %format!("{request_id:032x}"),
                            "server: reserve pending"
                        );
                        m.insert(
                            request_id,
                            Entry::Pending {
                                request_payload: body.clone(),
                            },
                        );
                    }
                    Some(Entry::Pending { request_payload }) => {
                        if request_payload.as_ref() != body.as_ref() {
                            tracing::warn!(
                                request_id = %format!("{request_id:032x}"),
                                "server: payload mismatch on pending"
                            );
                            return Ok(Some(ActorResponse::Bytes(Bytes::from_static(
                                b"INTEGRITY_ERROR",
                            ))));
                        }
                        tracing::info!(
                            request_id = %format!("{request_id:032x}"),
                            "server: pending replay -> PENDING"
                        );
                        return Ok(Some(ActorResponse::Bytes(Bytes::from_static(b"PENDING"))));
                    }
                    Some(Entry::Done(done)) => {
                        if done.request_payload.as_ref() != body.as_ref() {
                            tracing::warn!(
                                request_id = %format!("{request_id:032x}"),
                                "server: payload mismatch on done"
                            );
                            return Ok(Some(ActorResponse::Bytes(Bytes::from_static(
                                b"INTEGRITY_ERROR",
                            ))));
                        }
                        tracing::info!(
                            request_id = %format!("{request_id:032x}"),
                            "server: replay cached response"
                        );
                        return Ok(Some(ActorResponse::Bytes(done.response_payload.clone())));
                    }
                }
            }

            // 2) "Business logic" (side effect) runs outside the mutex.
            // If body begins with "STALL", simulate a zombie/unresponsive peer.
            if body.as_ref().starts_with(b"STALL") {
                sleep(Duration::from_millis(800)).await;
            }

            let applied = self.applied_count.fetch_add(1, Ordering::SeqCst) + 1;
            let response = Bytes::from(format!("OK applied={applied}"));
            tracing::info!(
                request_id = %format!("{request_id:032x}"),
                applied,
                "server: applied side effect"
            );

            // 3) Commit the response for replay. If the socket dies before ACK,
            // retries should get this exact same payload.
            {
                let mut m = self.inner.lock().await;
                let entry = m.get_mut(&request_id).expect("reserved above");
                *entry = Entry::Done(Done {
                    request_payload: body,
                    response_payload: response.clone(),
                });
            }

            Ok(Some(ActorResponse::Bytes(response)))
        })
    }
}

fn build_actor_ask_frame(
    correlation_id: u16,
    actor_id: u64,
    type_hash: u32,
    schema_hash: Option<u64>,
    payload: &[u8],
) -> Vec<u8> {
    let header = icanact_remote::framing::write_actor_frame_header(
        MessageType::ActorAsk,
        correlation_id,
        actor_id,
        type_hash,
        schema_hash,
        payload.len(),
    );
    let mut frame = Vec::with_capacity(header.len() + payload.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(payload);
    frame
}

async fn connect_tls(
    server_addr: SocketAddr,
    server_node_id: icanact_remote::NodeId,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let client_secret = SecretKey::generate();
    let tls = tls::TlsConfig::new(client_secret).expect("tls config");
    let connector = tls.connector();

    let sni = tls::name::encode(&server_node_id);
    let server_name = rustls::pki_types::ServerName::try_from(sni).expect("server name");

    let tcp = TcpStream::connect(server_addr).await.expect("tcp connect");
    let mut tls_stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls connect");

    // The server always performs the v3 Hello handshake immediately after TLS.
    let negotiated_alpn = tls_stream.get_ref().1.alpn_protocol().map(|p| p.to_vec());
    icanact_remote::handshake::perform_hello_handshake(
        &mut tls_stream,
        negotiated_alpn.as_deref(),
        false, // peer discovery disabled by default in GossipConfig
    )
    .await
    .expect("hello handshake");

    // The server requires the first post-handshake frame to identify the sender PeerId.
    // ConnectionPool does this by sending an initial FullSync; we mimic that here so
    // subsequent ActorAsk frames are accepted.
    let full_sync = icanact_remote::registry::RegistryMessage::FullSync {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id: tls.node_id.to_peer_id(),
        sender_bind_addr: None,
        sequence: 0,
        wall_clock_time: icanact_remote::current_timestamp(),
    };
    let serialized =
        rkyv::to_bytes::<rkyv::rancor::Error>(&full_sync).expect("serialize full sync");
    let header = icanact_remote::framing::write_gossip_frame_prefix(serialized.len());
    tls_stream
        .write_all(&header)
        .await
        .expect("write full sync header");
    tls_stream
        .write_all(&serialized)
        .await
        .expect("write full sync payload");
    tls_stream.flush().await.expect("flush full sync");

    tls_stream
}

async fn write_fragmented<S: AsyncWriteExt + Unpin>(
    mut s: S,
    frame: &[u8],
    per_byte_delay: Duration,
) {
    for b in frame {
        let _ = s.write_all(std::slice::from_ref(b)).await;
        if !per_byte_delay.is_zero() {
            sleep(per_byte_delay).await;
        }
    }
    let _ = s.flush().await;
}

async fn write_all<S: AsyncWriteExt + Unpin>(mut s: S, frame: &[u8]) {
    s.write_all(frame).await.expect("write");
    s.flush().await.expect("flush");
}

async fn read_response_payload<S: AsyncReadExt + Unpin>(
    mut s: S,
    expected_corr: u16,
    read_timeout: Duration,
) -> Result<Bytes, String> {
    // The server might emit gossip frames (e.g. as part of registry sync). Skip anything
    // that isn't the Response frame we're waiting for.
    for _ in 0..32 {
        let mut len_buf = [0u8; 4];
        timeout(read_timeout, s.read_exact(&mut len_buf))
            .await
            .map_err(|_| "timeout waiting length prefix".to_string())?
            .map_err(|e| format!("read length prefix: {e}"))?;

        let total_len = u32::from_be_bytes(len_buf) as usize;
        if total_len == 0 {
            return Err("zero-length frame".to_string());
        }

        let mut body = vec![0u8; total_len];
        timeout(read_timeout, s.read_exact(&mut body))
            .await
            .map_err(|_| "timeout waiting body".to_string())?
            .map_err(|e| format!("read body: {e}"))?;

        let msg_type = MessageType::from_byte(body[0]).ok_or("unknown msg type")?;
        if msg_type != MessageType::Response {
            continue;
        }

        if total_len < ASK_RESPONSE_HEADER_LEN {
            return Err(format!("response frame too short: {total_len}"));
        }

        let corr = u16::from_be_bytes([body[1], body[2]]);
        if corr != expected_corr {
            continue;
        }

        return Ok(Bytes::copy_from_slice(&body[ASK_RESPONSE_HEADER_LEN..]));
    }

    Err("did not observe expected Response frame".to_string())
}

fn make_payload(request_id: u128, body: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(16 + body.len());
    b.put_slice(&request_id.to_be_bytes());
    b.put_slice(body);
    b.freeze()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("icanact_remote=error,bad_client_stress=info")
        .init();

    // Start a local TLS server with a deterministic actor handler.
    let server_secret = SecretKey::generate();
    let server_node_id = server_secret.public();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(3600),
        ..Default::default()
    };

    let handle = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        server_secret,
        Some(config),
    )
    .await
    .expect("start server");
    let server_addr = handle.registry.bind_addr;

    let handler = Arc::new(ExactlyOnceTestHandler::new());
    handle
        .registry
        .set_actor_message_handler(handler.clone())
        .await;

    println!(
        "bad_client_stress: server listening on {server_addr} node_id={}",
        server_node_id.fmt_short()
    );

    // Common parameters.
    let actor_id = 0x0000_0000_c0ff_ee00u64;
    let type_hash = 0u32;
    let schema_hash = None;

    // Scenario 1: TCP fragmentation + lost ACK (client closes immediately after write).
    // Expectation: on retry with same request_id + payload, the response is replayed and
    // side effect runs exactly once.
    println!("\n[1] stutter + lost-ack retry");
    let mut rng = rand::rng();
    let request_id_1: u128 = rng.random();
    let corr1: u16 = rng.random_range(1..u16::MAX);
    let payload1 = make_payload(request_id_1, b"ECHO:hello");
    let frame1 = build_actor_ask_frame(corr1, actor_id, type_hash, schema_hash, payload1.as_ref());

    {
        let tls_stream = connect_tls(server_addr, server_node_id).await;
        write_fragmented(tls_stream, &frame1, Duration::from_millis(2)).await;
        // Drop without reading response (lost ACK).
    }

    // Retry: reconnect and re-send the exact same request_id + payload.
    let corr1b: u16 = rng.random_range(1..u16::MAX);
    let frame1b =
        build_actor_ask_frame(corr1b, actor_id, type_hash, schema_hash, payload1.as_ref());
    let response1 = {
        let mut tls_stream = connect_tls(server_addr, server_node_id).await;
        write_all(&mut tls_stream, &frame1b).await;
        read_response_payload(&mut tls_stream, corr1b, Duration::from_secs(2))
            .await
            .expect("read response")
    };
    let response1s = String::from_utf8_lossy(&response1);
    if !response1s.starts_with("OK applied=") {
        eprintln!("FAIL: unexpected response: {response1s}");
        std::process::exit(1);
    }
    let applied_1: usize = response1s
        .trim()
        .strip_prefix("OK applied=")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if applied_1 != 1 {
        eprintln!("FAIL: expected applied=1 after retry, got {applied_1}");
        std::process::exit(1);
    }
    println!("PASS: retry replayed response, applied={applied_1}");

    // Scenario 2: Payload tampering with same request_id.
    // Expectation: server returns INTEGRITY_ERROR and does not apply a second time.
    println!("\n[2] tamper (same id, different payload)");
    let corr2: u16 = rng.random_range(1..u16::MAX);
    let tampered = make_payload(request_id_1, b"ECHO:evil");
    let frame2 = build_actor_ask_frame(corr2, actor_id, type_hash, schema_hash, tampered.as_ref());
    let response2 = {
        let mut tls_stream = connect_tls(server_addr, server_node_id).await;
        write_all(&mut tls_stream, &frame2).await;
        read_response_payload(&mut tls_stream, corr2, Duration::from_secs(2))
            .await
            .expect("read response")
    };
    if response2.as_ref() != b"INTEGRITY_ERROR" {
        eprintln!(
            "FAIL: expected INTEGRITY_ERROR, got {:?}",
            String::from_utf8_lossy(&response2)
        );
        std::process::exit(1);
    }
    println!("PASS: tamper rejected");

    // Scenario 3: Zombie/unresponsive response (no wire heartbeats).
    // Expectation: client uses a read timeout, then retries with same request_id.
    // The server should return PENDING quickly during the stall, and eventually OK,
    // with applied_count increasing exactly once for this request_id.
    println!("\n[3] zombie (server stalls, client timeout + retry)");
    let request_id_3: u128 = rng.random();
    let payload3 = make_payload(request_id_3, b"STALL:slow");
    let corr3: u16 = rng.random_range(1..u16::MAX);
    let frame3 = build_actor_ask_frame(corr3, actor_id, type_hash, schema_hash, payload3.as_ref());

    // First attempt: should time out before server responds.
    {
        let mut tls_stream = connect_tls(server_addr, server_node_id).await;
        write_all(&mut tls_stream, &frame3).await;
        let res = read_response_payload(&mut tls_stream, corr3, Duration::from_millis(150)).await;
        if res.is_ok() {
            eprintln!("FAIL: expected timeout on first stall attempt");
            std::process::exit(1);
        }
    }

    // Retry loop: expect either PENDING or eventual OK applied=2.
    let mut ok_seen = None::<usize>;
    for attempt in 0..10 {
        let corr = rng.random_range(1..u16::MAX);
        let frame =
            build_actor_ask_frame(corr, actor_id, type_hash, schema_hash, payload3.as_ref());

        let mut tls_stream = connect_tls(server_addr, server_node_id).await;
        write_all(&mut tls_stream, &frame).await;
        let response =
            read_response_payload(&mut tls_stream, corr, Duration::from_millis(300)).await;

        match response {
            Ok(p) if p.as_ref() == b"PENDING" => {
                // Keep polling a bit.
                sleep(Duration::from_millis(80)).await;
                continue;
            }
            Ok(p) => {
                let s = String::from_utf8_lossy(&p);
                if let Some(applied) = s
                    .trim()
                    .strip_prefix("OK applied=")
                    .and_then(|x| x.parse::<usize>().ok())
                {
                    ok_seen = Some(applied);
                    break;
                }
                eprintln!("FAIL: unexpected zombie retry payload: {s}");
                std::process::exit(1);
            }
            Err(_) => {
                // retry
                sleep(Duration::from_millis(80)).await;
            }
        }

        if attempt == 9 {
            eprintln!("FAIL: zombie retry never observed OK");
            std::process::exit(1);
        }
    }

    let applied_3 = ok_seen.expect("ok seen");
    if applied_3 != 2 {
        eprintln!("FAIL: expected applied=2 after zombie scenario, got {applied_3}");
        std::process::exit(1);
    }
    println!("PASS: timeout + retry succeeded, applied={applied_3}");

    // Scenario 4: Check-then-act race, concurrent duplicates.
    // Expectation: two simultaneous requests with the same request_id only apply once.
    println!("\n[4] concurrent duplicates");
    let request_id_4: u128 = rng.random();
    let payload4 = make_payload(request_id_4, b"ECHO:race");
    let corr_a: u16 = rng.random_range(1..u16::MAX);
    let corr_b: u16 = rng.random_range(1..u16::MAX);
    let frame_a =
        build_actor_ask_frame(corr_a, actor_id, type_hash, schema_hash, payload4.as_ref());
    let frame_b =
        build_actor_ask_frame(corr_b, actor_id, type_hash, schema_hash, payload4.as_ref());

    let (ra, rb) = tokio::join!(
        async {
            let mut s = connect_tls(server_addr, server_node_id).await;
            write_all(&mut s, &frame_a).await;
            read_response_payload(&mut s, corr_a, Duration::from_secs(2)).await
        },
        async {
            let mut s = connect_tls(server_addr, server_node_id).await;
            write_all(&mut s, &frame_b).await;
            read_response_payload(&mut s, corr_b, Duration::from_secs(2)).await
        }
    );

    let ra = ra.expect("resp a");
    let rb = rb.expect("resp b");
    let sa = String::from_utf8_lossy(&ra);
    let sb = String::from_utf8_lossy(&rb);
    let aa = sa
        .trim()
        .strip_prefix("OK applied=")
        .and_then(|x| x.parse::<usize>().ok());
    let ab = sb
        .trim()
        .strip_prefix("OK applied=")
        .and_then(|x| x.parse::<usize>().ok());
    if aa.is_none() || ab.is_none() {
        eprintln!("FAIL: concurrent responses not OK: a={sa} b={sb}");
        std::process::exit(1);
    }
    if aa != ab {
        eprintln!("FAIL: concurrent responses differ: a={sa} b={sb}");
        std::process::exit(1);
    }
    let applied_4 = aa.unwrap();
    if applied_4 != 3 {
        eprintln!("FAIL: expected applied=3 after concurrent duplicates, got {applied_4}");
        std::process::exit(1);
    }
    println!("PASS: concurrent dedup OK applied={applied_4}");

    // Sanity: global applied count should be exactly 3 at the end.
    let final_applied = handler.applied_count.load(Ordering::SeqCst);
    if final_applied != 3 {
        eprintln!("FAIL: expected global applied=3, got {final_applied}");
        std::process::exit(1);
    }

    println!("\nALL PASS");
    handle.shutdown().await;
}
