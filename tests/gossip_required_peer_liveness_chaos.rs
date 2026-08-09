mod common;

use bytes::Bytes;
use common::{DynError, TlsHandle, connect_bidirectional, create_tls_node, wait_for_condition};
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse, RegistryChange};
use icanact_remote::{
    AlignedBytes, BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId,
    RegistrationPriority,
};
use std::net::SocketAddr;
use std::sync::{
    Arc, Once,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const ACTOR_ID: u64 = 0x1CA0_0001;
const TYPE_HASH: u32 = 0x1CA0_0002;
const ASK_TIMEOUT: Duration = Duration::from_millis(200);

static CRYPTO_INIT: Once = Once::new();

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
        assert_eq!(actor_id, ACTOR_ID);
        assert_eq!(type_hash, TYPE_HASH);
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

fn cadence_chaos_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_secs(3600),
        peer_gossip_interval: Some(Duration::from_millis(1500)),
        peer_liveness_window: Duration::from_millis(500),
        peer_supervisor_interval: Duration::from_secs(3600),
        peer_retry_interval: Duration::from_secs(3600),
        connection_timeout: Duration::from_millis(250),
        response_timeout: Duration::from_millis(250),
        max_peer_failures: 2,
        max_gossip_peers: 8,
        ..Default::default()
    }
}

fn discovery_chaos_config() -> GossipConfig {
    GossipConfig {
        enable_peer_discovery: true,
        allow_loopback_discovery: true,
        max_peers: 10,
        gossip_interval: Duration::from_secs(3600),
        peer_gossip_interval: Some(Duration::from_secs(3600)),
        peer_liveness_window: Duration::from_secs(7200),
        peer_supervisor_interval: Duration::from_millis(100),
        peer_retry_interval: Duration::from_millis(100),
        connection_timeout: Duration::from_millis(150),
        response_timeout: Duration::from_millis(150),
        max_peer_failures: 2,
        max_gossip_peers: 8,
        max_peer_gossip_targets: 8,
        ..Default::default()
    }
}

async fn wait_connected(from: &TlsHandle, to: &PeerId, timeout: Duration) -> bool {
    wait_for_condition(timeout, || async {
        from.client().lookup_connected_peer(to).is_some()
    })
    .await
}

async fn node(
    config: GossipConfig,
    label: &'static str,
    asks: Arc<AtomicU64>,
) -> Result<TlsHandle, DynError> {
    let handle = create_tls_node(config).await?;
    handle
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoHandler { label, asks }))
        .await;
    Ok(handle)
}

async fn node_at(
    addr: SocketAddr,
    keypair: KeyPair,
    config: GossipConfig,
    label: &'static str,
    asks: Arc<AtomicU64>,
) -> icanact_remote::Result<TlsHandle> {
    CRYPTO_INIT.call_once(icanact_remote::tls::ensure_crypto_provider);
    let handle = GossipRegistryHandle::new_with_transport_stack(
        addr,
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

async fn peer_failures(node: &TlsHandle, addr: SocketAddr) -> usize {
    let state = node.registry.gossip_state.lock().await;
    state.peers.get(&addr).map(|p| p.failures).unwrap_or(0)
}

async fn make_peer_silent(node: &TlsHandle, peer_addr: SocketAddr, silence: Duration) {
    let mut state = node.registry.gossip_state.lock().await;
    state
        .peers
        .get_mut(&peer_addr)
        .expect("peer must be present before silence simulation")
        .last_response_received_ms =
        icanact_remote::current_timestamp_millis().saturating_sub(silence.as_millis() as u64);
}

async fn apply_no_response_rounds(node: &TlsHandle, peer_addr: SocketAddr, rounds: usize) {
    for sequence in 0..rounds {
        node.registry
            .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                peer_addr,
                sent_sequence: sequence as u64,
                outcome: Ok(None),
            }])
            .await;
    }
}

async fn ask_peer(
    from: &TlsHandle,
    to: &PeerId,
    payload: &'static [u8],
) -> Result<Vec<u8>, String> {
    let peer_ref = from
        .lookup_peer(to)
        .await
        .map_err(|err| format!("lookup_peer failed: {err}"))?;
    let conn = peer_ref
        .connection_ref()
        .ok_or_else(|| "lookup_peer returned no connection".to_string())?;
    if conn.is_closed() {
        return Err("lookup_peer returned closed connection".to_string());
    }
    conn.ask_actor_frame(
        ACTOR_ID,
        TYPE_HASH,
        Bytes::from_static(payload),
        ASK_TIMEOUT,
    )
    .await
    .map(|reply| reply.as_ref().to_vec())
    .map_err(|err| err.to_string())
}

async fn ask_peer_until_success(
    from: &TlsHandle,
    to: &PeerId,
    payload: &'static [u8],
    timeout: Duration,
) -> Result<Vec<u8>, String> {
    let deadline = Instant::now() + timeout;
    let mut last_error = "not attempted".to_string();
    while Instant::now() < deadline {
        match ask_peer(from, to, payload).await {
            Ok(reply) => return Ok(reply),
            Err(err) => last_error = err,
        }
        sleep(Duration::from_millis(20)).await;
    }
    Err(last_error)
}

async fn assert_actor_visible(node: &TlsHandle, actor_name: &str, owner: &PeerId) {
    let location = node
        .registry
        .lookup_actor(actor_name)
        .await
        .expect("actor route must remain visible");
    assert_eq!(
        &location.peer_id, owner,
        "actor route must continue to point at its owning peer"
    );
}

async fn assert_no_actor_removed(node: &TlsHandle, actor_name: &str) {
    let state = node.registry.gossip_state.lock().await;
    let queued_or_historical = state
        .pending_changes
        .iter()
        .chain(state.urgent_changes.iter())
        .chain(
            state
                .delta_history
                .iter()
                .flat_map(|delta| delta.changes.iter()),
        )
        .any(|change| {
            matches!(
                change,
                RegistryChange::ActorRemoved { name, .. } if name == actor_name
            )
        });
    assert!(
        !queued_or_historical,
        "cadence-gap silence for a required peer must not publish ActorRemoved for {actor_name}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn required_peer_actor_route_and_ask_survive_cadence_gap_silence() -> Result<(), DynError> {
    let config = cadence_chaos_config();
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let node_a = node(config.clone(), "a", asks_a).await?;
    let node_b = node(config.clone(), "b", Arc::clone(&asks_b)).await?;
    connect_bidirectional(&node_a, &node_b).await?;

    let actor_name = "actor.required.cadence-gap";
    node_b
        .register_with_priority(
            actor_name.to_string(),
            node_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await?;
    assert!(
        wait_for_condition(Duration::from_secs(2), || async {
            node_a.registry.lookup_actor(actor_name).await.is_some()
        })
        .await,
        "actor route must propagate before silence simulation"
    );

    assert_eq!(
        ask_peer(&node_a, &node_b.registry.peer_id, b"baseline").await?,
        b"b:baseline"
    );

    make_peer_silent(
        &node_a,
        node_b.registry.bind_addr,
        Duration::from_millis(600),
    )
    .await;
    apply_no_response_rounds(&node_a, node_b.registry.bind_addr, config.max_peer_failures).await;

    assert_eq!(
        peer_failures(&node_a, node_b.registry.bind_addr).await,
        0,
        "required peer must not accrue failures before its peer-gossip cadence has elapsed"
    );
    assert_actor_visible(&node_a, actor_name, &node_b.registry.peer_id).await;
    assert_no_actor_removed(&node_a, actor_name).await;
    assert_eq!(
        ask_peer(&node_a, &node_b.registry.peer_id, b"after-gap").await?,
        b"b:after-gap"
    );
    assert_eq!(
        asks_b.load(Ordering::Acquire),
        2,
        "remote actor should receive exactly the baseline and post-gap asks"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn required_peer_mesh_does_not_cascade_false_failures_under_jitter() -> Result<(), DynError> {
    let config = cadence_chaos_config();
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let asks_c = Arc::new(AtomicU64::new(0));
    let node_a = node(config.clone(), "a", asks_a).await?;
    let node_b = node(config.clone(), "b", Arc::clone(&asks_b)).await?;
    let node_c = node(config.clone(), "c", asks_c).await?;
    connect_bidirectional(&node_a, &node_b).await?;
    connect_bidirectional(&node_b, &node_c).await?;

    let actor_name = "actor.required.mesh-owner-b";
    node_b
        .register_with_priority(
            actor_name.to_string(),
            node_b.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await?;
    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            node_a.registry.lookup_actor(actor_name).await.is_some()
                && node_c.registry.lookup_actor(actor_name).await.is_some()
        })
        .await,
        "B-owned actor route must propagate to both neighbours"
    );

    for (observer, peer_addr) in [
        (&node_a, node_b.registry.bind_addr),
        (&node_c, node_b.registry.bind_addr),
        (&node_b, node_a.registry.bind_addr),
        (&node_b, node_c.registry.bind_addr),
    ] {
        make_peer_silent(observer, peer_addr, Duration::from_millis(600)).await;
        apply_no_response_rounds(observer, peer_addr, config.max_peer_failures).await;
        assert_eq!(
            peer_failures(observer, peer_addr).await,
            0,
            "required-peer cadence jitter must not cascade false failures through the mesh"
        );
    }

    assert_actor_visible(&node_a, actor_name, &node_b.registry.peer_id).await;
    assert_actor_visible(&node_c, actor_name, &node_b.registry.peer_id).await;
    assert_no_actor_removed(&node_a, actor_name).await;
    assert_no_actor_removed(&node_c, actor_name).await;
    assert_eq!(
        ask_peer(&node_a, &node_b.registry.peer_id, b"from-a").await?,
        b"b:from-a"
    );
    assert_eq!(
        ask_peer(&node_c, &node_b.registry.peer_id, b"from-c").await?,
        b"b:from-c"
    );
    assert_eq!(
        asks_b.load(Ordering::Acquire),
        2,
        "B-owned actor should receive one ask from each neighbour after jitter"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_peers_retry_until_late_peer_comes_online() -> Result<(), DynError> {
    let mut config = cadence_chaos_config();
    config.peer_supervisor_interval = Duration::from_millis(100);
    config.peer_retry_interval = Duration::from_millis(100);
    config.connection_timeout = Duration::from_millis(100);

    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let node_a = node(config.clone(), "a", asks_a).await?;
    let key_b = KeyPair::new_for_testing("late-required-peer-b");
    let peer_b_id = key_b.peer_id();
    let reserved = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr_b = reserved.local_addr()?;
    drop(reserved);

    node_a
        .registry
        .add_peer_with_node_id(
            addr_b,
            Some(peer_b_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    node_a
        .registry
        .configure_peer(peer_b_id.clone(), addr_b)
        .await;
    node_a.registry.supervise_configured_peers().await;
    assert!(
        node_a.lookup_peer(&peer_b_id).await.is_err(),
        "precondition: peer B is not online yet"
    );

    let node_b = node_at(addr_b, key_b, config.clone(), "b", Arc::clone(&asks_b)).await?;
    node_b
        .registry
        .add_peer_with_node_id(
            node_a.registry.bind_addr,
            Some(node_a.registry.peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    node_b
        .registry
        .configure_peer(node_a.registry.peer_id.clone(), node_a.registry.bind_addr)
        .await;

    let started = Instant::now();
    assert!(
        wait_for_condition(Duration::from_secs(1), || async {
            node_a.lookup_peer(&peer_b_id).await.is_ok()
                && node_b.lookup_peer(&node_a.registry.peer_id).await.is_ok()
        })
        .await,
        "configured peers should establish direct routes within 1s once both are online"
    );
    assert!(
        started.elapsed() <= Duration::from_secs(1),
        "late peer convergence exceeded the 1s required-peer SLA"
    );
    assert_eq!(
        ask_peer(&node_a, &peer_b_id, b"late-online").await?,
        b"b:late-online"
    );
    assert_eq!(
        asks_b.load(Ordering::Acquire),
        1,
        "late peer actor should receive exactly one post-convergence ask"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn required_peer_drops_after_two_liveness_failures_and_recovers_on_reconnect()
-> Result<(), DynError> {
    let mut config = cadence_chaos_config();
    config.peer_gossip_interval = Some(Duration::from_millis(250));
    config.peer_liveness_window = Duration::from_millis(100);
    config.peer_supervisor_interval = Duration::from_secs(3600);
    config.peer_retry_interval = Duration::from_secs(3600);
    config.max_peer_failures = 2;
    // The registry normalizes required-peer liveness to at least two regular
    // gossip intervals. Keep the synthetic silence aligned with the effective
    // runtime configuration while the one-hour cadence suppresses background
    // rounds during this deterministic test.
    config.normalize();

    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let node_a = node(config.clone(), "a", asks_a).await?;
    let node_b = node(config.clone(), "b", Arc::clone(&asks_b)).await?;
    connect_bidirectional(&node_a, &node_b).await?;
    assert_eq!(
        ask_peer(&node_a, &node_b.registry.peer_id, b"before-drop").await?,
        b"b:before-drop"
    );

    make_peer_silent(
        &node_a,
        node_b.registry.bind_addr,
        config
            .peer_liveness_window
            .saturating_add(Duration::from_millis(1)),
    )
    .await;
    apply_no_response_rounds(&node_a, node_b.registry.bind_addr, 2).await;
    assert_eq!(
        peer_failures(&node_a, node_b.registry.bind_addr).await,
        2,
        "two consecutive post-window no-response rounds should mark the peer failed"
    );
    assert!(
        node_a
            .client()
            .lookup_connected_peer(&node_b.registry.peer_id)
            .is_none(),
        "failed peer connection should be dropped from direct lookup cache"
    );
    let stale_alias = "127.0.0.1:9".parse()?;
    {
        let mut state = node_a.registry.gossip_state.lock().await;
        let mut alias = state
            .peers
            .get(&node_b.registry.bind_addr)
            .expect("canonical peer must remain tracked")
            .clone();
        alias.address = stale_alias;
        alias.failures = 2;
        alias.last_failure_time = Some(icanact_remote::current_timestamp());
        alias.last_failure_instant = Some(std::time::Instant::now());
        state.peers.insert(stale_alias, alias);
    }

    node_a
        .registry
        .connect_to_peer(&node_b.registry.peer_id)
        .await?;
    assert_eq!(
        peer_failures(&node_a, node_b.registry.bind_addr).await,
        0,
        "successful reconnect must immediately clear liveness failures"
    );
    assert_eq!(
        peer_failures(&node_a, stale_alias).await,
        2,
        "successful reconnect must not clear stale same-node-id aliases"
    );
    assert_eq!(
        ask_peer(&node_a, &node_b.registry.peer_id, b"after-reconnect").await?,
        b"b:after-reconnect"
    );
    assert_eq!(
        asks_b.load(Ordering::Acquire),
        2,
        "actor should receive asks before failure and after reconnect"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_peer_reconnects_within_one_second_after_drop_and_return() -> Result<(), DynError>
{
    let mut config = cadence_chaos_config();
    config.peer_supervisor_interval = Duration::from_millis(100);
    config.peer_retry_interval = Duration::from_millis(100);
    config.connection_timeout = Duration::from_millis(100);

    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let node_a = node(config.clone(), "a", asks_a).await?;
    let key_b = KeyPair::new_for_testing("drop-return-required-peer-b");
    let peer_b_id = key_b.peer_id();
    let reserved = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr_b = reserved.local_addr()?;
    drop(reserved);

    let node_b = node_at(
        addr_b,
        key_b.clone(),
        config.clone(),
        "b",
        Arc::clone(&asks_b),
    )
    .await?;
    node_a
        .registry
        .add_peer_with_node_id(
            addr_b,
            Some(peer_b_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    node_a
        .registry
        .configure_peer(peer_b_id.clone(), addr_b)
        .await;
    node_b
        .registry
        .add_peer_with_node_id(
            node_a.registry.bind_addr,
            Some(node_a.registry.peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    node_b
        .registry
        .configure_peer(node_a.registry.peer_id.clone(), node_a.registry.bind_addr)
        .await;

    assert!(
        wait_connected(&node_a, &peer_b_id, Duration::from_secs(1)).await,
        "configured peers should connect before drop"
    );
    assert_eq!(
        ask_peer(&node_a, &peer_b_id, b"before-drop").await?,
        b"b:before-drop"
    );

    node_b.shutdown().await;
    assert!(
        wait_for_condition(Duration::from_secs(1), || async {
            node_a.client().lookup_connected_peer(&peer_b_id).is_none()
        })
        .await,
        "A should observe B disconnect before return"
    );

    let node_b = node_at(addr_b, key_b, config.clone(), "b", Arc::clone(&asks_b)).await?;
    node_b
        .registry
        .add_peer_with_node_id(
            node_a.registry.bind_addr,
            Some(node_a.registry.peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    node_b
        .registry
        .configure_peer(node_a.registry.peer_id.clone(), node_a.registry.bind_addr)
        .await;

    let returned_at = Instant::now();
    assert!(
        wait_connected(&node_a, &peer_b_id, Duration::from_secs(1)).await,
        "A must reconnect to returning configured peer within 1s"
    );
    assert!(
        returned_at.elapsed() <= Duration::from_secs(1),
        "configured peer reconnect exceeded 1s retry SLA"
    );
    assert_eq!(
        peer_failures(&node_a, addr_b).await,
        0,
        "successful reconnect must clear prior liveness failures"
    );
    assert_eq!(
        ask_peer(&node_a, &peer_b_id, b"after-return").await?,
        b"b:after-return"
    );
    assert_eq!(
        asks_b.load(Ordering::Acquire),
        2,
        "B actor should receive asks before drop and after return"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indirect_peer_is_rediscovered_immediately_when_seen_by_direct_neighbor()
-> Result<(), DynError> {
    let config = discovery_chaos_config();
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let asks_c = Arc::new(AtomicU64::new(0));
    let node_a = node(config.clone(), "a", asks_a).await?;
    let node_b = node(config.clone(), "b", asks_b).await?;
    connect_bidirectional(&node_a, &node_b).await?;
    assert!(
        wait_connected(&node_a, &node_b.registry.peer_id, Duration::from_secs(1)).await,
        "A and B should be directly connected before C appears"
    );

    let node_c = node(config.clone(), "c", Arc::clone(&asks_c)).await?;
    connect_bidirectional(&node_b, &node_c).await?;
    assert!(
        wait_connected(&node_b, &node_c.registry.peer_id, Duration::from_secs(1)).await,
        "B should see C directly before A learns it indirectly"
    );

    assert!(
        wait_for_condition(Duration::from_secs(1), || async {
            node_a.lookup_peer(&node_c.registry.peer_id).await.is_ok()
        })
        .await,
        "A should rediscover C via B's immediate peer-list broadcast, not wait for the \
         periodic peer_gossip_interval"
    );
    assert_eq!(
        ask_peer_until_success(
            &node_a,
            &node_c.registry.peer_id,
            b"indirect",
            Duration::from_secs(1),
        )
        .await?,
        b"c:indirect"
    );
    // `ask_peer_until_success` retries at the RPC layer on any local error
    // (including a client-side `ASK_TIMEOUT` when the fresh connection to a
    // just-discovered peer is still finalizing) — it is an at-least-once
    // helper, not exactly-once. A retried attempt can genuinely deliver to
    // C's handler even though the *prior* attempt's reply raced its own
    // timeout locally on A, so C legitimately observes more than one ask in
    // that case. This assertion existed as `== 1` and was flaky under load
    // (observed both with and without unrelated connection-pool changes)
    // for exactly this reason — it was asserting a stronger guarantee than
    // the helper actually provides. The correctness property under test is
    // "the ask reaches C at all through the rediscovered route", i.e.
    // at-least-once, so assert that instead of an exact count.
    assert!(
        asks_c.load(Ordering::Acquire) >= 1,
        "C actor should receive at least one ask through A's rediscovered direct route"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
    Ok(())
}
