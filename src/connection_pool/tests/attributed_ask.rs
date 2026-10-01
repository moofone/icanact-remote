// Attributed ask: a reply must carry the certificate-authenticated identity of
// the connection whose read context completed THAT reply, snapshotted with the
// reply itself (never a separate, racy addr -> peer lookup). A connection
// without an authenticated identity must fail closed.

fn attributed_test_peer(label: &str) -> crate::PeerId {
    crate::KeyPair::new_for_testing(label).peer_id()
}

/// Run one ActorAsk over an in-memory duplex between a "client" connection
/// (the one whose read context completes the reply) and a deferred-reply echo
/// server.
///
/// * `authenticated` -- what the client connection's TLS session proved
///   (`ReadContext::authenticated_peer_id`).
/// * `requested` -- the best-effort identity the pool bound the connection to
///   (`ReadContext::peer_id`); it models the identity the caller *asked for*,
///   which a reused addr-keyed connection may not actually have.
async fn attributed_ask_over_duplex(
    server_port: u16,
    authenticated: Option<crate::PeerId>,
    requested: Option<crate::PeerId>,
    payload: bytes::Bytes,
) -> crate::Result<(crate::PeerId, crate::AlignedBytes)> {
    let server_addr: std::net::SocketAddr = format!("127.0.0.1:{server_port}").parse().unwrap();
    let client_addr: std::net::SocketAddr = format!("127.0.0.1:{}", server_port + 1)
        .parse()
        .unwrap();

    let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        server_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("attributed_ask_server")),
            ..crate::GossipConfig::default()
        },
    ));
    server_registry
        .set_actor_ask_handler_sync(Arc::new(DeferredTestActor))
        .await;

    let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        client_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("attributed_ask_client")),
            ..crate::GossipConfig::default()
        },
    ));
    let correlation = CorrelationTracker::new();

    let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

    let client_read_ctx = ReadContext {
        streaming_state_handoff: None,
        registry_weak: Arc::downgrade(&client_registry),
        peer_addr: server_addr,
        session_source: server_addr,
        peer_id: requested,
        authenticated_peer_id: authenticated.map(Arc::new),
        max_message_size: MASTER_BUFFER_SIZE,
        expected_schema_hash: None,
        aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
        inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
        response_correlation: Some(correlation.clone()),
        response_writer: None,
        tell_handler_sync: None,
        tell_handler_sync_context: None,
        ask_immediate_handler_sync: None,
        ask_handler_sync: None,
        sync_actor_handler: None,
    };
    let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
        client_io,
        server_addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        Some(client_read_ctx),
    );
    let client_writer = Arc::new(client_writer);
    let client_conn = ConnectionHandle::<()>::new_stream(
        server_addr,
        ConnectionDirection::Outbound,
        Arc::clone(&client_writer),
        correlation,
    );

    let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
    let server_read_ctx = ReadContext {
        streaming_state_handoff: None,
        registry_weak: Arc::downgrade(&server_registry),
        peer_addr: client_addr,
        session_source: client_addr,
        peer_id: None,
        authenticated_peer_id: None,
        max_message_size: MASTER_BUFFER_SIZE,
        expected_schema_hash: None,
        aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
        inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
        response_correlation: None,
        response_writer: Some(response_writer.clone()),
        tell_handler_sync: server_registry.actor_tell_handler_sync.load_full(),
        tell_handler_sync_context: server_registry.actor_tell_handler_sync_context.load_full(),
        ask_immediate_handler_sync: None,
        ask_handler_sync: server_registry.actor_ask_handler_sync.load_full(),
        sync_actor_handler: None,
    };
    let (server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
        server_io,
        client_addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        Some(server_read_ctx),
    );
    let server_writer = Arc::new(server_writer);
    response_writer.bind_stream_handle(server_writer.clone());

    let result = client_conn
        .ask_actor_frame_aligned_attributed(
            0xD3F3_10AB,
            0xA55D_0001,
            payload,
            Duration::from_secs(5),
        )
        .await;

    client_writer.shutdown();
    server_writer.shutdown();
    result
}

/// (a) The attributed reply reports the authenticated peer of the answering
/// connection, alongside the reply bytes.
#[test]
fn attributed_ask_reports_authenticated_peer_of_answering_connection() {
    run_multi_thread_test(async {
        let data_peer = attributed_test_peer("attributed_ask_data_peer");
        let payload = bytes::Bytes::from_static(b"attributed-ping");

        let (peer, reply) = attributed_ask_over_duplex(
            40811,
            Some(data_peer.clone()),
            Some(data_peer.clone()),
            payload.clone(),
        )
        .await
        .expect("a connection with an authenticated identity must yield an attributed reply");

        assert_eq!(peer, data_peer);
        assert_eq!(reply.into_bytes(), payload);
    });
}

/// (b) Addr-keyed connection reuse can hand the caller a connection whose
/// authenticated identity differs from the identity the caller asked for. The
/// attributed reply must report the REAL authenticated peer, never the
/// requested one, so the mismatch is detectable.
#[test]
fn attributed_ask_reports_real_identity_not_the_requested_one() {
    run_multi_thread_test(async {
        let requested = attributed_test_peer("attributed_ask_requested_peer");
        let actual = attributed_test_peer("attributed_ask_actual_peer");
        assert_ne!(requested, actual);

        let (peer, reply) = attributed_ask_over_duplex(
            40821,
            Some(actual.clone()),
            Some(requested.clone()),
            bytes::Bytes::from_static(b"reused-connection"),
        )
        .await
        .expect("reply from an authenticated connection must be attributed");

        assert_eq!(peer, actual, "must report the connection's own identity");
        assert_ne!(peer, requested, "must not echo the requested identity");
        assert_eq!(reply.into_bytes(), bytes::Bytes::from_static(b"reused-connection"));
    });
}

/// (c) A connection with no authenticated identity must error -- never a
/// placeholder and never the best-effort `ReadContext::peer_id`.
#[test]
fn attributed_ask_without_authenticated_identity_fails_closed() {
    run_multi_thread_test(async {
        let best_effort = attributed_test_peer("attributed_ask_best_effort_only");

        let result = attributed_ask_over_duplex(
            40831,
            None,
            Some(best_effort),
            bytes::Bytes::from_static(b"unauthenticated"),
        )
        .await;

        assert!(
            matches!(result, Err(crate::GossipError::AuthenticationFailed(_))),
            "expected AuthenticationFailed for a connection without an authenticated identity"
        );
    });
}

/// The plain completion path is unchanged: a completion that carries no
/// identity still resolves through the ordinary waiter as plain bytes.
#[tokio::test(flavor = "current_thread")]
async fn correlation_plain_completion_stays_unattributed() {
    let tracker = CorrelationTracker::new();
    let guard = tracker.allocate().expect("slot should allocate");
    let id = guard.id();
    let pool = Arc::new(crate::AlignedBytesPool::default());
    let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"plain", pool));
    assert!(tracker.complete(id, &mut response));

    let reply = tracker
        .wait_for_response_no_timeout(id)
        .await
        .expect("plain completion resolves as before");
    let _ = guard.disarm();
    assert_eq!(&reply.into_bytes()[..], b"plain");
}

/// Correlation layer: attribution travels inside the completed slot, and a
/// completion that carries no identity is rejected by the attributed waiter.
#[tokio::test(flavor = "current_thread")]
async fn correlation_completion_carries_attribution_atomically() {
    let tracker = CorrelationTracker::new();
    let pool = Arc::new(crate::AlignedBytesPool::default());
    let peer = Arc::new(attributed_test_peer("attributed_correlation_peer"));

    let attributed_guard = tracker.allocate().expect("slot should allocate");
    let attributed_id = attributed_guard.id();
    let mut response = Some(crate::AlignedBytes::from_pooled_slice(
        b"attributed",
        pool.clone(),
    ));
    assert!(tracker.complete_attributed(attributed_id, &mut response, Some(&peer)));
    let (completed_by, bytes) = tracker
        .wait_for_response_no_timeout_outcome(attributed_id)
        .await
        .and_then(CorrelationOutcome::into_attributed_result)
        .expect("attributed completion must resolve with its identity");
    let _ = attributed_guard.disarm();
    assert_eq!(completed_by, *peer);
    assert_eq!(&bytes.into_bytes()[..], b"attributed");

    let bare_guard = tracker.allocate().expect("slot should allocate");
    let bare_id = bare_guard.id();
    let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"bare", pool));
    assert!(tracker.complete_attributed(bare_id, &mut response, None));
    let outcome = tracker
        .wait_for_response_no_timeout_outcome(bare_id)
        .await
        .and_then(CorrelationOutcome::into_attributed_result);
    let _ = bare_guard.disarm();
    assert!(
        matches!(outcome, Err(crate::GossipError::AuthenticationFailed(_))),
        "a completion without identity must not produce an attributed reply"
    );
}
