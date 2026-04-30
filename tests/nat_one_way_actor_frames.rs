use bytes::Bytes;
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse};
use icanact_remote::{AlignedBytes, GossipConfig, GossipRegistryHandle, KeyPair};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const TEST_ACTOR_ID: u64 = 77;
const TEST_TYPE_HASH: u32 = 0x77AA_55CC;
const DISCONNECT_SLA: Duration = Duration::from_secs(8);
const RECONNECT_BIDIR_SLA: Duration = Duration::from_secs(8);

#[derive(Clone)]
struct CountingHandler {
    tell_hits: Arc<AtomicU64>,
    ask_hits: Arc<AtomicU64>,
    label: &'static str,
}

impl ActorMessageHandlerSync for CountingHandler {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u16>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        assert_eq!(actor_id, TEST_ACTOR_ID);
        assert_eq!(type_hash, TEST_TYPE_HASH);
        if correlation_id.is_some() {
            self.ask_hits.fetch_add(1, Ordering::Relaxed);
            let response = format!("{}:{}", self.label, payload.len()).into_bytes();
            Ok(Some(ActorResponse::from(response)))
        } else {
            self.tell_hits.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }
}

fn test_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(250),
        connection_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        nat_role_reconnect_enabled: true,
        ..Default::default()
    }
}

fn unique_suffix() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn key_pair_ordered_for_outbound_a(a_seed: &str, b_seed: &str) -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing(a_seed);
    let second = KeyPair::new_for_testing(b_seed);
    if first
        .peer_id()
        .to_node_id()
        .as_bytes()
        .cmp(second.peer_id().to_node_id().as_bytes())
        .is_lt()
    {
        (first, second)
    } else {
        (second, first)
    }
}

async fn wait_for_active_peers(
    handle: &GossipRegistryHandle,
    expected: usize,
    timeout: Duration,
) -> icanact_remote::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if handle.stats().await.active_peers == expected {
            return Ok(());
        }
        sleep(Duration::from_millis(25)).await;
    }
    Err(icanact_remote::GossipError::Timeout)
}

async fn wait_for_connection(
    handle: &GossipRegistryHandle,
    peer_id: &icanact_remote::PeerId,
    timeout: Duration,
) -> icanact_remote::Result<icanact_remote::RemoteConnection> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(peer_ref) = handle.lookup_peer(peer_id).await
            && let Some(conn) = peer_ref.connection_ref()
            && !conn.is_closed()
        {
            return Ok(conn);
        }
        sleep(Duration::from_millis(25)).await;
    }
    Err(icanact_remote::GossipError::Timeout)
}

async fn wait_counter(
    counter: &AtomicU64,
    target: u64,
    timeout: Duration,
) -> icanact_remote::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if counter.load(Ordering::Acquire) >= target {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }
    Err(icanact_remote::GossipError::Timeout)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_way_nat_allows_unsolicited_bidirectional_actor_frames_and_reconnect()
-> icanact_remote::Result<()> {
    let suffix = unique_suffix();
    let a_seed = format!("nat-frames-a-{suffix}");
    let b_seed = format!("nat-frames-b-{suffix}");
    let (a_keypair, b_keypair) = key_pair_ordered_for_outbound_a(&a_seed, &b_seed);

    let tell_a = Arc::new(AtomicU64::new(0));
    let ask_a = Arc::new(AtomicU64::new(0));
    let tell_b = Arc::new(AtomicU64::new(0));
    let ask_b = Arc::new(AtomicU64::new(0));

    let handle_b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        b_keypair.to_secret_key(),
        Some(test_cfg()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    handle_b
        .registry
        .set_actor_message_handler_sync(Arc::new(CountingHandler {
            tell_hits: Arc::clone(&tell_b),
            ask_hits: Arc::clone(&ask_b),
            label: "from_b",
        }))
        .await;

    let handle_a_1 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        a_keypair.to_secret_key(),
        Some(test_cfg()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    handle_a_1
        .registry
        .set_actor_message_handler_sync(Arc::new(CountingHandler {
            tell_hits: Arc::clone(&tell_a),
            ask_hits: Arc::clone(&ask_a),
            label: "from_a",
        }))
        .await;

    let peer_id_a = handle_a_1.registry.peer_id.clone();
    let peer_id_b = handle_b.registry.peer_id.clone();

    // One-way establishment: only A dials B.
    let peer_b = handle_a_1.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await?;

    wait_for_active_peers(&handle_a_1, 1, Duration::from_secs(6)).await?;
    wait_for_active_peers(&handle_b, 1, Duration::from_secs(6)).await?;

    let conn_a_to_b = wait_for_connection(&handle_a_1, &peer_id_b, Duration::from_secs(6)).await?;
    let conn_b_to_a = wait_for_connection(&handle_b, &peer_id_a, Duration::from_secs(6)).await?;
    let conn_b_to_a_baseline = conn_b_to_a.clone();
    let seq_before = conn_b_to_a_baseline.sequence_number();

    // Unsolicited tell from inbound-observer side (B) to NAT-side (A) over same session.
    conn_b_to_a
        .tell_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"tell:b->a"),
        )
        .await?;
    wait_counter(&tell_a, 1, Duration::from_secs(4)).await?;

    // Tell in the opposite direction still works over the same connection.
    conn_a_to_b
        .tell_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"tell:a->b"),
        )
        .await?;
    wait_counter(&tell_b, 1, Duration::from_secs(4)).await?;

    // Ask from B -> A (unsolicited relative to original dial direction).
    let r1 = conn_b_to_a
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"ask:b->a"),
            Duration::from_secs(3),
        )
        .await?;
    assert_eq!(r1.as_ref(), b"from_a:8");
    wait_counter(&ask_a, 1, Duration::from_secs(4)).await?;

    // Ask from A -> B baseline.
    let r2 = conn_a_to_b
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"ask:a->b"),
            Duration::from_secs(3),
        )
        .await?;
    assert_eq!(r2.as_ref(), b"from_b:8");
    wait_counter(&ask_b, 1, Duration::from_secs(4)).await?;

    let conn_b_to_a_after =
        wait_for_connection(&handle_b, &peer_id_a, Duration::from_secs(4)).await?;
    assert!(
        !conn_b_to_a_after.is_closed(),
        "expected inbound-established session to remain live across bidirectional traffic"
    );
    assert!(
        conn_b_to_a_after.sequence_number() > seq_before,
        "connection write sequence should advance on reused session traffic"
    );
    let live_stats = handle_b.stats().await;
    assert_eq!(
        live_stats.active_peers, 1,
        "bidirectional sends should continue using a single active session"
    );
    assert_eq!(
        live_stats.failed_peers, 0,
        "single-session bidirectional sends should not trigger dial failure churn"
    );

    // Outage of NAT-side node; inbound side cannot dial back directly.
    let disconnect_start = Instant::now();
    handle_a_1.shutdown().await;
    let observed_disconnect = wait_for_active_peers(&handle_b, 0, Duration::from_secs(8))
        .await
        .is_ok();
    if !observed_disconnect {
        handle_b.disconnect_peer_connection(&peer_id_a);
    }
    if observed_disconnect {
        assert!(
            disconnect_start.elapsed() <= DISCONNECT_SLA,
            "idle disconnect detection exceeded SLA: {:?} > {:?}",
            disconnect_start.elapsed(),
            DISCONNECT_SLA
        );
    }

    // NAT-side restart with same identity and outbound reconnect.
    let handle_a_2 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        a_keypair.to_secret_key(),
        Some(test_cfg()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    handle_a_2
        .registry
        .set_actor_message_handler_sync(Arc::new(CountingHandler {
            tell_hits: Arc::clone(&tell_a),
            ask_hits: Arc::clone(&ask_a),
            label: "from_a",
        }))
        .await;

    let peer_b = handle_a_2.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await?;

    wait_for_active_peers(&handle_a_2, 1, Duration::from_secs(6)).await?;
    wait_for_active_peers(&handle_b, 1, Duration::from_secs(6)).await?;

    let reconnect_start = Instant::now();
    let conn_b_to_a2 = wait_for_connection(&handle_b, &peer_id_a, Duration::from_secs(6)).await?;

    let r3 = conn_b_to_a2
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"ask:after-reconnect"),
            Duration::from_secs(3),
        )
        .await?;
    assert_eq!(r3.as_ref(), b"from_a:19");
    wait_counter(&ask_a, 2, Duration::from_secs(4)).await?;
    assert!(
        reconnect_start.elapsed() <= RECONNECT_BIDIR_SLA,
        "reconnect bidirectional traffic restoration exceeded SLA: {:?} > {:?}",
        reconnect_start.elapsed(),
        RECONNECT_BIDIR_SLA
    );

    handle_a_2.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}
