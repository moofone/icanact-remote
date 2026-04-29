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
        correlation_id: Option<u16>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        assert_eq!(actor_id, TEST_ACTOR_ID);
        assert_eq!(type_hash, TEST_TYPE_HASH);
        if correlation_id.is_none() {
            return Ok(None);
        }

        self.asks.fetch_add(1, Ordering::AcqRel);
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
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        KeyPair::new_for_testing(seed).to_secret_key(),
        Some(static_mesh_cfg()),
        BuilderTlsBootstrap,
    )
    .await?;
    handle
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoHandler { label, asks }))
        .await;
    Ok(handle)
}

async fn configure_static_peer(left: &TlsHandle, right: &TlsHandle) {
    left.registry
        .configure_peer(right.registry.peer_id.clone(), right.registry.bind_addr)
        .await;
}

async fn connect_pair(left: &TlsHandle, right: &TlsHandle) -> icanact_remote::Result<()> {
    configure_static_peer(left, right).await;
    configure_static_peer(right, left).await;

    left.add_peer(&right.registry.peer_id)
        .await
        .connect(&right.registry.bind_addr)
        .await?;
    right
        .add_peer(&left.registry.peer_id)
        .await
        .connect(&left.registry.bind_addr)
        .await?;

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

    // Model the raft old-leader heal path: the isolated node's transport sessions to the
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
