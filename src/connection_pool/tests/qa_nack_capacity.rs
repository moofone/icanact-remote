//! Non-destructive reproduction of security finding cb0f1d4108348191ac0697f6e2bebb67.
//! Exercise production parsing/dispatch while the peer concurrently drains every reply.
use super::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

static NACK_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const ASK_COUNT: u32 = 20_000;
const BOUND: Duration = Duration::from_secs(15);

fn ask_frames() -> Vec<u8> {
    let mut frames = Vec::new();
    for id in 1..=ASK_COUNT {
        frames.extend_from_slice(&crate::framing::write_direct_ask_header(id, id as u64, 1));
        frames.push(b'x');
    }
    frames
}

async fn verify_all_replies<R: AsyncRead + Unpin>(reader: &mut R) {
    for expected in 1..=ASK_COUNT {
        loop {
            let mut prefix = [0; crate::framing::LENGTH_PREFIX_LEN];
            reader.read_exact(&mut prefix).await.unwrap();
            let control = crate::framing::decode_control(prefix).unwrap();
            assert!(control.body_len <= crate::GossipConfig::default().max_message_size);
            let mut body = vec![0; control.body_len];
            reader.read_exact(&mut body).await.unwrap();
            if control.kind != crate::framing::WireKind::Response {
                continue; // bootstrap gossip or the primary-path marker
            }
            assert_eq!(control.body_len, crate::framing::ASK_RESPONSE_HEADER_LEN);
            assert_eq!(u32::from_be_bytes(body[..4].try_into().unwrap()), expected);
            assert_eq!(
                crate::framing::ask_nack_reason(&body),
                Some(crate::framing::AskNackReason::NoDispatcher),
            );
            break;
        }
    }
}

fn assert_bounded(label: &str) {
    let peak = TEST_PENDING_NACK_PEAK.load(Ordering::Relaxed);
    eprintln!("NACK_CAPACITY {label}: asks={ASK_COUNT} cap={PENDING_ASK_NACK_CAP} peak={peak}");
    assert!(peak > 0, "observer must see real queued NACKs");
    assert!(
        peak <= PENDING_ASK_NACK_CAP,
        "{label}: pending NACK peak {peak} exceeds {PENDING_ASK_NACK_CAP}"
    );
}

#[test]
fn nack_insertion_rejects_overflow_without_evicting_existing_outcomes() {
    let mut queue = LocalStreamingQueue::new();
    for id in 1..=PENDING_ASK_NACK_CAP {
        queue
            .queue_ask_nack(crate::framing::write_ask_nack_header(
                id as u32,
                crate::framing::AskNackReason::NoDispatcher,
            ))
            .unwrap();
    }
    let error = queue
        .queue_ask_nack(crate::framing::write_ask_nack_header(
            999,
            crate::framing::AskNackReason::NoDispatcher,
        ))
        .unwrap_err();
    assert!(is_ask_capacity_violation(&error));
    assert!(
        !is_streaming_admission_backpressure(&error),
        "capacity invariant failure must close explicitly, not be swallowed as streaming pressure"
    );
    assert_eq!(queue.pending_ask_nack_count(), PENDING_ASK_NACK_CAP);
    let first = queue.pop_ask_nack().unwrap();
    assert_eq!(u32::from_be_bytes(first[4..8].try_into().unwrap()), 1);
    assert!(queue.has_room_for_ask_nack());
    assert!(
        !queue.has_room_for_ask_nack_occupying(1),
        "an already-popped partial NACK must occupy the final reserved slot"
    );
    for id in 2..=PENDING_ASK_NACK_CAP {
        let header = queue.pop_ask_nack().unwrap();
        assert_eq!(
            u32::from_be_bytes(header[4..8].try_into().unwrap()),
            id as u32
        );
    }
    assert_eq!(queue.pending_ask_nack_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn sustained_direct_asks_bound_nacks_in_primary_and_idle_paths() {
    let _guard = NACK_TEST_LOCK.lock().await;
    for (primary, capacity) in [
        (false, 1024 * 1024),
        (true, 1024 * 1024),
        (false, 8),
        (true, 8),
    ] {
        TEST_PENDING_NACK_PEAK.store(0, Ordering::Relaxed);
        let addr = "127.0.0.1:0".parse().unwrap();
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("bounded-direct-nacks")),
                ..Default::default()
            },
        ));
        let ctx = super::response_budget_read_context(&registry, addr);
        let (io, peer) = tokio::io::duplex(capacity);
        let (writer, task, _) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(ctx),
        );
        if primary {
            // Queue work before this single-thread runtime can poll the owner.
            // That forces the did_work/primary branch rather than the idle select.
            writer
                .write_trusted_bytes_control(bytes::Bytes::copy_from_slice(
                    &crate::framing::write_stream_abort_header(77, 1),
                ))
                .await
                .unwrap();
        }
        let (mut reader, mut sender) = tokio::io::split(peer);
        let frames = ask_frames();
        tokio::time::timeout(BOUND, async {
            tokio::try_join!(
                async {
                    sender.write_all(&frames).await?;
                    sender.flush().await
                },
                async {
                    verify_all_replies(&mut reader).await;
                    Ok::<_, std::io::Error>(())
                },
            )
            .unwrap();
        })
        .await
        .expect("every admitted ask must receive its ordered terminal reply");
        writer.shutdown();
        tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
        assert_bounded(&format!(
            "{} capacity={capacity}",
            if primary { "primary" } else { "idle" }
        ));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sustained_authenticated_tls_direct_asks_bound_nacks_and_deliver_all_replies() {
    let _guard = NACK_TEST_LOCK.lock().await;
    TEST_PENDING_NACK_PEAK.store(0, Ordering::Relaxed);
    crate::tls::ensure_crypto_provider();
    let secret = crate::SecretKey::generate();
    let handle = crate::GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        secret.clone(),
        None,
        crate::BuilderTlsBootstrap,
    )
    .await
    .unwrap();
    let client_secret = crate::SecretKey::generate();
    let client_peer = client_secret.to_keypair().peer_id();
    let tls_config = crate::tls::TlsConfig::new(client_secret).unwrap();
    let name = rustls::pki_types::ServerName::try_from(crate::tls::name::encode(&secret.public()))
        .unwrap();
    let tcp = tokio::net::TcpStream::connect(handle.registry.bind_addr)
        .await
        .unwrap();
    tcp.set_nodelay(true).unwrap();
    let mut tls = tls_config.connector().connect(name, tcp).await.unwrap();
    let alpn = tls.get_ref().1.alpn_protocol().map(|p| p.to_vec());
    crate::handshake::perform_hello_handshake(
        &mut tls,
        alpn.as_deref(),
        false,
        handle.registry.config.schema_hash,
        crate::handshake::RemoteBootId::new(),
    )
    .await
    .unwrap();
    let identify = crate::registry::RegistryMessage::FullSync {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id: client_peer,
        sender_bind_addr: None,
        sequence: 0,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&identify).unwrap();
    tls.write_all(&crate::framing::write_gossip_frame_prefix(data.len()))
        .await
        .unwrap();
    tls.write_all(data.as_ref()).await.unwrap();
    tls.flush().await.unwrap();
    let (mut reader, mut sender) = tokio::io::split(tls);
    let frames = ask_frames();
    tokio::time::timeout(BOUND, async {
        tokio::try_join!(
            async {
                sender.write_all(&frames).await?;
                sender.flush().await
            },
            async {
                verify_all_replies(&mut reader).await;
                Ok::<_, std::io::Error>(())
            },
        )
        .unwrap();
    })
    .await
    .expect("sustained accepted-peer TLS traffic must receive every ordered NACK");
    handle.shutdown().await;
    assert_bounded("authenticated TLS");
}
