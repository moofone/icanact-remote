use bytes::Bytes;
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse};
use icanact_remote::{
    AlignedBytes, BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

const TEST_ACTOR_ID: u64 = 0x51A7_1C00;
const TEST_TYPE_HASH: u32 = 0x51A7_1C01;
const RECONNECT_SLA: Duration = Duration::from_millis(500);
const ASK_TIMEOUT: Duration = Duration::from_millis(75);

type TlsHandle = GossipRegistryHandle<BuilderTlsBootstrap>;

#[derive(Clone)]
struct EchoHandler {
    label: &'static str,
    asks: Arc<AtomicU64>,
}

impl ActorMessageHandlerSync for EchoHandler {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u32>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        assert_eq!(actor_id, TEST_ACTOR_ID);
        assert_eq!(type_hash, TEST_TYPE_HASH);
        if correlation_id.is_none() {
            return Ok(None);
        }

        self.asks.fetch_add(1, Ordering::AcqRel);
        if payload.as_ref() == b"slow-inflight" {
            std::thread::sleep(Duration::from_millis(500));
        }
        let response = format!(
            "{}:{}",
            self.label,
            String::from_utf8_lossy(payload.as_ref())
        );
        Ok(Some(ActorResponse::from(response.into_bytes())))
    }
}

fn static_mesh_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(50),
        connection_timeout: Duration::from_millis(125),
        response_timeout: Duration::from_millis(125),
        max_peer_failures: 1,
        peer_retry_interval: Duration::from_millis(25),
        max_gossip_peers: 2,
        small_cluster_threshold: 3,
        ..Default::default()
    }
}

async fn node(
    seed: &str,
    label: &'static str,
    asks: Arc<AtomicU64>,
) -> icanact_remote::Result<TlsHandle> {
    node_with_keypair(
        KeyPair::new_for_testing(seed),
        label,
        asks,
        static_mesh_cfg(),
    )
    .await
}

async fn node_with_keypair(
    keypair: KeyPair,
    label: &'static str,
    asks: Arc<AtomicU64>,
    config: GossipConfig,
) -> icanact_remote::Result<TlsHandle> {
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        keypair.to_secret_key(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await?;
    handle
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoHandler { label, asks }))
        .await;
    Ok(handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_identity_process_does_not_replace_live_incumbent()
-> icanact_remote::Result<()> {
    let first = KeyPair::new_for_testing("live-session-replacement-a");
    let second = KeyPair::new_for_testing("live-session-replacement-b");
    let (replica_key, observer_key) =
        if first.peer_id().to_node_id().as_bytes() < second.peer_id().to_node_id().as_bytes() {
            (first, second)
        } else {
            (second, first)
        };

    let quiet_config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        cleanup_interval: Duration::from_secs(300),
        peer_retry_interval: Duration::from_secs(300),
        peer_supervisor_interval: Duration::from_secs(300),
        ..Default::default()
    };
    let old_asks = Arc::new(AtomicU64::new(0));
    let new_asks = Arc::new(AtomicU64::new(0));
    let observer = node_with_keypair(
        observer_key,
        "observer",
        Arc::new(AtomicU64::new(0)),
        quiet_config.clone(),
    )
    .await?;
    let old_replica = node_with_keypair(
        replica_key.clone(),
        "old",
        old_asks.clone(),
        quiet_config.clone(),
    )
    .await?;

    assert!(
        old_replica
            .registry
            .should_keep_connection(&observer.registry.peer_id, true),
        "the replica identity must own the preferred outbound direction"
    );
    old_replica
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer.registry.bind_addr)
        .await?;

    ask_until_success(
        &observer,
        &old_replica.registry.peer_id,
        b"before",
        b"old:before",
        Duration::from_secs(2),
    )
    .await
    .map_err(icanact_remote::GossipError::InvalidKeyPair)?;
    // Start a second live registry with the same long-lived identity while the
    // incumbent remains healthy. This is a concurrent process clone, not a
    // restart: it must not displace the live handler or churn the published
    // session merely because its physical stream was created later.
    let clone = node_with_keypair(replica_key, "clone", new_asks.clone(), quiet_config).await?;
    clone
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer.registry.bind_addr)
        .await?;

    // Keep real actor traffic flowing for the entire admission window. A
    // one-shot assertion can race ahead of the incoming session publication
    // and falsely pass while the clone replaces the incumbent moments later.
    let admission_deadline = Instant::now() + Duration::from_micros(500_000);
    let mut admission_probes = 0_u64;
    let mut last_probe_error = None;
    while Instant::now() < admission_deadline {
        let probe = async {
            let peer = observer.lookup_peer(&clone.registry.peer_id).await?;
            let connection = peer.connection_ref().ok_or_else(|| {
                icanact_remote::GossipError::InvalidKeyPair(
                    "incumbent route disappeared during clone admission".to_string(),
                )
            })?;
            connection
                .ask_actor_frame(
                    TEST_ACTOR_ID,
                    TEST_TYPE_HASH,
                    Bytes::from_static(b"admission"),
                    ASK_TIMEOUT,
                )
                .await
        }
        .await;
        match probe {
            Ok(reply) => {
                assert_eq!(
                    reply.as_ref(),
                    b"old:admission",
                    "a concurrently live process clone displaced the healthy incumbent"
                );
                admission_probes += 1;
            }
            Err(err) => last_probe_error = Some(err.to_string()),
        }
        tokio::task::yield_now().await;
    }
    assert!(
        admission_probes > 0,
        "the incumbent delivered no actor probes during clone admission; last error: {}",
        last_probe_error.as_deref().unwrap_or("none")
    );

    let old_hits_after_clone = old_asks.load(Ordering::Acquire);
    ask_until_success(
        &observer,
        &clone.registry.peer_id,
        b"steady",
        b"old:steady",
        Duration::from_secs(2),
    )
    .await
    .map_err(icanact_remote::GossipError::InvalidKeyPair)?;

    assert_eq!(
        old_asks.load(Ordering::Acquire),
        old_hits_after_clone + 1,
        "the healthy incumbent must keep serving after a clone authenticates"
    );
    assert_eq!(
        new_asks.load(Ordering::Acquire),
        0,
        "a concurrent process clone must not receive incumbent traffic"
    );

    clone.shutdown_and_wait().await;
    old_replica.shutdown_and_wait().await;
    observer.shutdown_and_wait().await;
    Ok(())
}

async fn configure_static_peer(left: &TlsHandle, right: &TlsHandle) {
    let _ = left.registry
        .configure_peer(right.registry.peer_id.clone(), right.registry.bind_addr)
        .await;
}

async fn connect_pair(left: &TlsHandle, right: &TlsHandle) -> icanact_remote::Result<()> {
    configure_static_peer(left, right).await;
    configure_static_peer(right, left).await;

    if left
        .registry
        .should_keep_connection(&right.registry.peer_id, true)
    {
        left.add_peer(&right.registry.peer_id)
            .await
            .connect(&right.registry.bind_addr)
            .await?;
    } else {
        right
            .add_peer(&left.registry.peer_id)
            .await
            .connect(&left.registry.bind_addr)
            .await?;
    }

    Ok(())
}

async fn wait_active_peers(
    handle: &TlsHandle,
    expected: usize,
    timeout_for: Duration,
) -> icanact_remote::Result<()> {
    let deadline = Instant::now() + timeout_for;
    while Instant::now() < deadline {
        if handle.stats().await.active_peers == expected {
            return Ok(());
        }
        sleep(Duration::from_millis(10)).await;
    }
    Err(icanact_remote::GossipError::Timeout)
}

async fn ask_until_success(
    from: &TlsHandle,
    to: &PeerId,
    payload: &'static [u8],
    expected: &'static [u8],
    timeout_for: Duration,
) -> Result<(), String> {
    let mut last_error = "not attempted".to_string();
    let deadline = Instant::now() + timeout_for;
    while Instant::now() < deadline {
        match from.lookup_peer(to).await {
            Ok(peer_ref) => match peer_ref.connection_ref() {
                Some(conn) if !conn.is_closed() => {
                    match conn
                        .ask_actor_frame(
                            TEST_ACTOR_ID,
                            TEST_TYPE_HASH,
                            Bytes::from_static(payload),
                            ASK_TIMEOUT,
                        )
                        .await
                    {
                        Ok(reply) if reply.as_ref() == expected => return Ok(()),
                        Ok(reply) => {
                            last_error = format!(
                                "unexpected reply {:?}, expected {:?}",
                                String::from_utf8_lossy(reply.as_ref()),
                                String::from_utf8_lossy(expected)
                            );
                        }
                        Err(err) => last_error = err.to_string(),
                    }
                }
                _ => last_error = "lookup returned no live connection".to_string(),
            },
            Err(err) => last_error = err.to_string(),
        }
        sleep(Duration::from_millis(25)).await;
    }
    Err(last_error)
}

async fn bounded_ask_until_success(
    from: &TlsHandle,
    to: &PeerId,
    payload: &'static [u8],
    expected: &'static [u8],
) -> Result<(), String> {
    match timeout(
        RECONNECT_SLA,
        ask_until_success(from, to, payload, expected, RECONNECT_SLA),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(format!(
            "ask did not converge within {:?}; last peer={}",
            RECONNECT_SLA, to
        )),
    }
}

async fn wait_no_connected_peer(handle: &TlsHandle, peer_id: &PeerId) -> bool {
    timeout(Duration::from_secs(1), async {
        loop {
            if handle.client().lookup_connected_peer(peer_id).is_none() {
                return true;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or(false)
}

async fn ask_until_success_owned(
    from: &TlsHandle,
    to: &PeerId,
    payload: String,
    expected: String,
    timeout_for: Duration,
) -> Result<(), String> {
    let mut last_error = "not attempted".to_string();
    let deadline = Instant::now() + timeout_for;
    while Instant::now() < deadline {
        match from.lookup_peer(to).await {
            Ok(peer_ref) => match peer_ref.connection_ref() {
                Some(conn) if !conn.is_closed() => {
                    match conn
                        .ask_actor_frame(
                            TEST_ACTOR_ID,
                            TEST_TYPE_HASH,
                            Bytes::from(payload.clone()),
                            ASK_TIMEOUT,
                        )
                        .await
                    {
                        Ok(reply) if reply.as_ref() == expected.as_bytes() => return Ok(()),
                        Ok(reply) => {
                            last_error = format!(
                                "unexpected reply {:?}, expected {:?}",
                                String::from_utf8_lossy(reply.as_ref()),
                                expected
                            );
                        }
                        Err(err) => last_error = err.to_string(),
                    }
                }
                _ => last_error = "lookup returned no live connection".to_string(),
            },
            Err(err) => last_error = err.to_string(),
        }
        sleep(Duration::from_millis(25)).await;
    }
    Err(last_error)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_static_peer_reconnect_converges_under_500ms() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let asks_c = Arc::new(AtomicU64::new(0));

    let node_a = node("old-leader-reconnect-a", "a", Arc::clone(&asks_a)).await?;
    let node_b = node("old-leader-reconnect-b", "b", Arc::clone(&asks_b)).await?;
    let node_c = node("old-leader-reconnect-c", "c", Arc::clone(&asks_c)).await?;

    connect_pair(&node_a, &node_b).await?;
    connect_pair(&node_a, &node_c).await?;
    connect_pair(&node_b, &node_c).await?;

    wait_active_peers(&node_a, 2, Duration::from_secs(3)).await?;
    wait_active_peers(&node_b, 2, Duration::from_secs(3)).await?;
    wait_active_peers(&node_c, 2, Duration::from_secs(3)).await?;

    bounded_ask_until_success(
        &node_a,
        &node_b.registry.peer_id,
        b"baseline-a-b",
        b"b:baseline-a-b",
    )
    .await
    .expect("baseline A->B ask");
    bounded_ask_until_success(
        &node_b,
        &node_a.registry.peer_id,
        b"baseline-b-a",
        b"a:baseline-b-a",
    )
    .await
    .expect("baseline B->A ask");
    bounded_ask_until_success(
        &node_c,
        &node_a.registry.peer_id,
        b"baseline-c-a",
        b"a:baseline-c-a",
    )
    .await
    .expect("baseline C->A ask");

    // Model an isolated-node heal path: the isolated node's transport sessions to the
    // healthy quorum are gone, but all three static peer addresses are still configured.
    node_a.disconnect_peer_connection(&node_b.registry.peer_id);
    node_a.disconnect_peer_connection(&node_c.registry.peer_id);
    node_b.disconnect_peer_connection(&node_a.registry.peer_id);
    node_c.disconnect_peer_connection(&node_a.registry.peer_id);

    let heal_started = Instant::now();
    let (ab, ac, ba, ca, bc) = tokio::join!(
        bounded_ask_until_success(
            &node_a,
            &node_b.registry.peer_id,
            b"heal-a-b",
            b"b:heal-a-b"
        ),
        bounded_ask_until_success(
            &node_a,
            &node_c.registry.peer_id,
            b"heal-a-c",
            b"c:heal-a-c"
        ),
        bounded_ask_until_success(
            &node_b,
            &node_a.registry.peer_id,
            b"heal-b-a",
            b"a:heal-b-a"
        ),
        bounded_ask_until_success(
            &node_c,
            &node_a.registry.peer_id,
            b"heal-c-a",
            b"a:heal-c-a"
        ),
        bounded_ask_until_success(
            &node_b,
            &node_c.registry.peer_id,
            b"stable-b-c",
            b"c:stable-b-c"
        ),
    );
    for result in [ab, ac, ba, ca, bc] {
        result.expect("post-heal actor ask should converge");
    }
    assert!(
        heal_started.elapsed() <= RECONNECT_SLA,
        "isolated peer reconnect exceeded SLA: {:?} > {:?}",
        heal_started.elapsed(),
        RECONNECT_SLA
    );

    wait_active_peers(&node_a, 2, Duration::from_secs(2)).await?;
    wait_active_peers(&node_b, 2, Duration::from_secs(2)).await?;
    wait_active_peers(&node_c, 2, Duration::from_secs(2)).await?;

    // Blast radius: the recovered streams must stay usable after the initial convergence, not
    // enter a connect/drop loop that only briefly satisfies lookup_peer().
    sleep(Duration::from_millis(150)).await;
    bounded_ask_until_success(
        &node_b,
        &node_a.registry.peer_id,
        b"stable-b-a",
        b"a:stable-b-a",
    )
    .await
    .expect("B->A stable after heal");
    bounded_ask_until_success(
        &node_c,
        &node_a.registry.peer_id,
        b"stable-c-a",
        b"a:stable-c-a",
    )
    .await
    .expect("C->A stable after heal");

    assert!(
        asks_a.load(Ordering::Acquire) >= 4,
        "isolated node should handle baseline, heal, and stability asks"
    );
    assert!(
        asks_b.load(Ordering::Acquire) >= 2,
        "peer B should handle baseline and heal asks"
    );
    assert!(
        asks_c.load(Ordering::Acquire) >= 2,
        "peer C should handle heal and quorum-stability asks"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_ref_self_heals_after_disconnect_bare_connection_fails_closed()
-> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));

    let node_a = node("cached-fast-path-a", "a", Arc::clone(&asks_a)).await?;
    let node_b = node("cached-fast-path-b", "b", Arc::clone(&asks_b)).await?;

    connect_pair(&node_a, &node_b).await?;
    wait_active_peers(&node_a, 1, Duration::from_secs(3)).await?;
    wait_active_peers(&node_b, 1, Duration::from_secs(3)).await?;

    let cached_ref = node_a.lookup_peer(&node_b.registry.peer_id).await?;
    let cached_conn = cached_ref
        .connection_ref()
        .expect("lookup_peer should cache a connection for fast paths");

    let baseline = cached_conn
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"cached-baseline"),
            ASK_TIMEOUT,
        )
        .await
        .expect("cached connection baseline ask should work");
    assert_eq!(baseline.as_ref(), b"b:cached-baseline");

    node_a.disconnect_peer_connection(&node_b.registry.peer_id);
    node_b.disconnect_peer_connection(&node_a.registry.peer_id);

    assert!(
        wait_no_connected_peer(&node_a, &node_b.registry.peer_id).await,
        "lookup_connected_peer must stop returning stale cached sessions after disconnect"
    );
    assert!(
        cached_conn.is_closed(),
        "cloned fast-path RemoteConnection should observe closure after pool disconnect"
    );

    // A bare `RemoteConnection` (the bytes-in/bytes-out handle returned by
    // `connection_ref()`) has no ref-level slot to self-heal - it IS the one
    // specific transport session, and that session is gone. It must keep
    // failing closed forever; only `RemoteActorRef` re-resolves.
    let stale_conn_result = cached_conn
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"stale-conn"),
            ASK_TIMEOUT,
        )
        .await;
    assert!(
        stale_conn_result.is_err(),
        "stale cached RemoteConnection must fail closed, got {stale_conn_result:?}"
    );

    // `RemoteActorRef` repairs its cached connection after an ambiguous
    // failure, but must not replay this request: the remote may already have
    // processed it before the local transport reported failure.
    let stale_ref_result = cached_ref
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"stale-ref"),
            ASK_TIMEOUT,
        )
        .await;
    assert!(
        stale_ref_result.is_err(),
        "ambiguous cached RemoteActorRef ask must not be replayed, got {stale_ref_result:?}"
    );

    let healed_ref_result = cached_ref
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"healed-ref"),
            ASK_TIMEOUT,
        )
        .await;
    assert_eq!(
        healed_ref_result.as_deref().ok(),
        Some(b"b:healed-ref".as_slice()),
        "the next cached RemoteActorRef ask should use the healed connection, got {healed_ref_result:?}"
    );

    let (a_to_b, b_to_a) = tokio::join!(
        bounded_ask_until_success(
            &node_a,
            &node_b.registry.peer_id,
            b"fresh-after-stale",
            b"b:fresh-after-stale",
        ),
        bounded_ask_until_success(
            &node_b,
            &node_a.registry.peer_id,
            b"fresh-reverse-after-stale",
            b"a:fresh-reverse-after-stale",
        ),
    );
    a_to_b.expect("fresh lookup should reconnect after stale cached handles fail");
    b_to_a.expect("reverse fresh lookup should reconnect after stale cached handles fail");

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_ask_drop_does_not_poison_static_peer_reconnect() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));

    let node_a = node("inflight-drop-reconnect-a", "a", Arc::clone(&asks_a)).await?;
    let node_b = node("inflight-drop-reconnect-b", "b", Arc::clone(&asks_b)).await?;

    connect_pair(&node_a, &node_b).await?;
    wait_active_peers(&node_a, 1, Duration::from_secs(3)).await?;
    wait_active_peers(&node_b, 1, Duration::from_secs(3)).await?;

    bounded_ask_until_success(
        &node_a,
        &node_b.registry.peer_id,
        b"baseline",
        b"b:baseline",
    )
    .await
    .expect("baseline A->B ask");

    let peer_ref = node_a.lookup_peer(&node_b.registry.peer_id).await?;
    let pending = tokio::spawn(async move {
        peer_ref
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                Bytes::from_static(b"slow-inflight"),
                Duration::from_millis(400),
            )
            .await
    });

    sleep(Duration::from_millis(5)).await;
    node_a.disconnect_peer_connection(&node_b.registry.peer_id);
    node_b.disconnect_peer_connection(&node_a.registry.peer_id);

    let pending_result = timeout(Duration::from_secs(1), pending)
        .await
        .expect("in-flight ask task should finish after forced disconnect")
        .expect("in-flight ask task should not panic");
    assert!(
        matches!(
            pending_result,
            Err(icanact_remote::GossipError::ConnectionDropped)
                | Err(icanact_remote::GossipError::Timeout)
        ),
        "forced disconnect should complete pending ask with an explicit bounded error, got {pending_result:?}"
    );

    bounded_ask_until_success(
        &node_a,
        &node_b.registry.peer_id,
        b"after-inflight-drop",
        b"b:after-inflight-drop",
    )
    .await
    .expect("static peer should reconnect after in-flight drop");
    bounded_ask_until_success(
        &node_b,
        &node_a.registry.peer_id,
        b"after-inflight-drop-reverse",
        b"a:after-inflight-drop-reverse",
    )
    .await
    .expect("reverse static peer should reconnect after in-flight drop");

    wait_active_peers(&node_a, 1, Duration::from_secs(2)).await?;
    wait_active_peers(&node_b, 1, Duration::from_secs(2)).await?;

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_symmetric_reconnects_do_not_churn_live_streams() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));

    let node_a = node("repeated-static-reconnect-a", "a", Arc::clone(&asks_a)).await?;
    let node_b = node("repeated-static-reconnect-b", "b", Arc::clone(&asks_b)).await?;

    connect_pair(&node_a, &node_b).await?;
    wait_active_peers(&node_a, 1, Duration::from_secs(3)).await?;
    wait_active_peers(&node_b, 1, Duration::from_secs(3)).await?;

    for cycle in 0..5 {
        node_a.disconnect_peer_connection(&node_b.registry.peer_id);
        node_b.disconnect_peer_connection(&node_a.registry.peer_id);

        let payload_a_b = format!("cycle-{cycle}-a-b");
        let expected_a_b = format!("b:{payload_a_b}");
        let payload_b_a = format!("cycle-{cycle}-b-a");
        let expected_b_a = format!("a:{payload_b_a}");

        let started = Instant::now();
        let (a_to_b, b_to_a) = tokio::join!(
            ask_until_success_owned(
                &node_a,
                &node_b.registry.peer_id,
                payload_a_b,
                expected_a_b,
                RECONNECT_SLA,
            ),
            ask_until_success_owned(
                &node_b,
                &node_a.registry.peer_id,
                payload_b_a,
                expected_b_a,
                RECONNECT_SLA,
            ),
        );
        a_to_b.expect("A->B reconnect ask should converge");
        b_to_a.expect("B->A reconnect ask should converge");
        assert!(
            started.elapsed() <= RECONNECT_SLA,
            "cycle {cycle} reconnect exceeded SLA: {:?} > {:?}",
            started.elapsed(),
            RECONNECT_SLA
        );

        sleep(Duration::from_millis(100)).await;
        bounded_ask_until_success(
            &node_a,
            &node_b.registry.peer_id,
            b"post-cycle-a-b",
            b"b:post-cycle-a-b",
        )
        .await
        .expect("A->B stream should remain usable after reconnect");
        bounded_ask_until_success(
            &node_b,
            &node_a.registry.peer_id,
            b"post-cycle-b-a",
            b"a:post-cycle-b-a",
        )
        .await
        .expect("B->A stream should remain usable after reconnect");

        wait_active_peers(&node_a, 1, Duration::from_secs(2)).await?;
        wait_active_peers(&node_b, 1, Duration::from_secs(2)).await?;
    }

    assert!(
        asks_a.load(Ordering::Acquire) >= 10,
        "node A should handle every reconnect and stability ask"
    );
    assert!(
        asks_b.load(Ordering::Acquire) >= 10,
        "node B should handle every reconnect and stability ask"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}
