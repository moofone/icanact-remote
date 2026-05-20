//! Transport-only stress tests for the RPC pattern shared-raft drives.
//!
//! Goal: validate that the icanact-remote primitives shared-raft relies on —
//! `lookup_peer → connection_ref → ask_actor_frame` (with raft-shaped
//! deadlines) — behave correctly under timeouts, in-flight drops, and
//! concurrent contention, *in complete isolation from the openraft layer*.
//!
//! These tests mirror the shapes from `crates/shared-raft-icanact/src/lib.rs`:
//!   - `RAFT_RPC_TIMEOUT_MS = 600`
//!   - `RAFT_TRANSPORT_TIMEOUT_MS = 1000`
//!   - `MIN_RAFT_APPEND_RPC_TIMEOUT = 175ms`
//!   - reconnect_timeout used by `call_payload_with_budgets` is 750ms
//!
//! Any regression here is a transport bug, not a raft bug.

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
use tokio::time::sleep;

const RAFT_RPC_ACTOR_ID: u64 = 0x5A48_5241_4654;
const RAFT_RPC_TYPE_HASH: u32 = 0x5352_4601;

// Shared-raft default budgets (deploy/deploy-three-vps.sh, main.rs):
const RAFT_RPC_TIMEOUT: Duration = Duration::from_millis(600);
const RAFT_TRANSPORT_TIMEOUT: Duration = Duration::from_millis(1000);
const RAFT_RECONNECT_TIMEOUT: Duration = Duration::from_millis(750);
const APPEND_ASK_TIMEOUT: Duration = Duration::from_millis(275);

type TlsHandle = GossipRegistryHandle<BuilderTlsBootstrap>;

#[derive(Clone)]
struct ScriptedHandler {
    label: &'static str,
    asks: Arc<AtomicU64>,
    delay_ms: Arc<AtomicU64>,
}

impl ActorMessageHandlerSync for ScriptedHandler {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u16>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        assert_eq!(actor_id, RAFT_RPC_ACTOR_ID);
        assert_eq!(type_hash, RAFT_RPC_TYPE_HASH);
        if correlation_id.is_none() {
            return Ok(None);
        }
        self.asks.fetch_add(1, Ordering::AcqRel);
        let delay_ms = self.delay_ms.load(Ordering::Acquire);
        if delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(delay_ms));
        }
        let response = format!(
            "{}:{}",
            self.label,
            String::from_utf8_lossy(payload.as_ref())
        );
        Ok(Some(ActorResponse::from(response.into_bytes())))
    }
}

fn raft_rpc_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(50),
        connection_timeout: APPEND_ASK_TIMEOUT,
        response_timeout: APPEND_ASK_TIMEOUT,
        max_peer_failures: 1,
        peer_retry_interval: Duration::from_millis(25),
        max_gossip_peers: 2,
        small_cluster_threshold: 2,
        ..Default::default()
    }
}

async fn node(
    seed: &str,
    label: &'static str,
    asks: Arc<AtomicU64>,
    delay_ms: Arc<AtomicU64>,
) -> icanact_remote::Result<TlsHandle> {
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        KeyPair::new_for_testing(seed).to_secret_key(),
        Some(raft_rpc_cfg()),
        BuilderTlsBootstrap,
    )
    .await?;
    handle
        .registry
        .set_actor_message_handler_sync(Arc::new(ScriptedHandler {
            label,
            asks,
            delay_ms,
        }))
        .await;
    Ok(handle)
}

async fn connect_pair(left: &TlsHandle, right: &TlsHandle) -> icanact_remote::Result<()> {
    left.registry
        .configure_peer(right.registry.peer_id.clone(), right.registry.bind_addr)
        .await;
    right
        .registry
        .configure_peer(left.registry.peer_id.clone(), left.registry.bind_addr)
        .await;

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

async fn wait_connected(handle: &TlsHandle, peer_id: &PeerId, timeout_for: Duration) -> bool {
    let deadline = Instant::now() + timeout_for;
    while Instant::now() < deadline {
        if handle.client().lookup_connected_peer(peer_id).is_some() {
            return true;
        }
        sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_disconnected(handle: &TlsHandle, peer_id: &PeerId, timeout_for: Duration) -> bool {
    let deadline = Instant::now() + timeout_for;
    while Instant::now() < deadline {
        if handle.client().lookup_connected_peer(peer_id).is_none() {
            return true;
        }
        sleep(Duration::from_millis(10)).await;
    }
    false
}

/// Mirrors the shared-raft RPC fast path: spin on `lookup_connected_peer` until
/// a live connection appears (bounded by `reconnect_timeout`), then send an
/// ask with `ask_timeout`.
async fn raft_rpc_lookup_then_ask(
    from: &TlsHandle,
    to: &PeerId,
    payload: &'static [u8],
    ask_timeout: Duration,
    reconnect_timeout: Duration,
) -> Result<(Duration, Vec<u8>), String> {
    let started = Instant::now();
    let lookup_deadline = started + reconnect_timeout;

    // The connection-fast-path loop, identical in shape to
    // `IcanactRaftRpcClient::call_payload_with_budgets`.
    loop {
        if from.client().lookup_connected_peer(to).is_some() {
            break;
        }
        if Instant::now() >= lookup_deadline {
            return Err(format!(
                "lookup_connected_peer never returned a live handle within {:?}",
                reconnect_timeout
            ));
        }
        sleep(Duration::from_millis(10)).await;
    }

    let peer_ref = from
        .lookup_peer(to)
        .await
        .map_err(|err| format!("lookup_peer failed: {err}"))?;
    let conn = peer_ref
        .connection_ref()
        .ok_or_else(|| "lookup returned no live connection".to_string())?;
    if conn.is_closed() {
        return Err("lookup returned a closed connection".to_string());
    }
    match conn
        .ask_actor_frame(
            RAFT_RPC_ACTOR_ID,
            RAFT_RPC_TYPE_HASH,
            Bytes::from_static(payload),
            ask_timeout,
        )
        .await
    {
        Ok(response) => Ok((started.elapsed(), response.as_ref().to_vec())),
        Err(err) => Err(err.to_string()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookup_then_ask_under_micro_timeout_succeeds() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    // Server delay = 100ms, well under the 275ms append_entries ask budget.
    let delay_b = Arc::new(AtomicU64::new(100));

    let a = node("raft-rpc-stress-micro-a", "a", asks_a, delay_a).await?;
    let b = node("raft-rpc-stress-micro-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    let (elapsed, response) = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"micro",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("micro-timeout RPC should succeed");

    assert_eq!(response.as_slice(), b"b:micro");
    assert!(
        elapsed < APPEND_ASK_TIMEOUT,
        "micro-timeout RPC must complete within ask budget, got {:?}",
        elapsed
    );
    assert_eq!(asks_b.load(Ordering::Acquire), 1);

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ask_at_or_above_deadline_returns_an_error_distinct_from_drop() -> icanact_remote::Result<()>
{
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    // Server delay strictly greater than the ask timeout.
    let delay_b = Arc::new(AtomicU64::new(400));

    let a = node("raft-rpc-stress-deadline-a", "a", asks_a, delay_a).await?;
    let b = node("raft-rpc-stress-deadline-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    let result = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"deadline",
        Duration::from_millis(150),
        RAFT_RECONNECT_TIMEOUT,
    )
    .await;

    let err = result.expect_err("ask must error when deadline expires before server replies");
    let lowered = err.to_ascii_lowercase();
    // Shared-raft classifies "timeout"/"timed out" as a replication timeout
    // (see is_raft_replication_timeout in shared-raft-icanact). It must NOT
    // be misclassified as a connection-dropped error, otherwise the streak
    // counter is bypassed and the peer is evicted on the very first slow ask.
    assert!(
        lowered.contains("timeout") || lowered.contains("timed out"),
        "expected timeout error, got: {err}"
    );
    assert!(
        !lowered.contains("connection dropped")
            && !lowered.contains("connection closed")
            && !lowered.contains("connection reset"),
        "deadline-only error must not be reported as a connection drop: {err}"
    );

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_disconnect_drops_in_flight_ask_and_clears_lookup_cache()
-> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    // Server is slow: 600ms; we'll drop the connection while the ask is in flight.
    let delay_b = Arc::new(AtomicU64::new(600));

    let a = node("raft-rpc-stress-drop-a", "a", asks_a, delay_a).await?;
    let b = node("raft-rpc-stress-drop-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    // Take a peer_ref pre-drop and call ask_actor_frame directly on it (the
    // pattern existing tests use to keep the connection alive across the
    // await). Disconnect from BOTH sides — that's the path that produces
    // GossipError::ConnectionDropped on the in-flight ask.
    let peer_ref = a.lookup_peer(&b.registry.peer_id).await?;
    let ask_task = tokio::spawn(async move {
        peer_ref
            .ask_actor_frame(
                RAFT_RPC_ACTOR_ID,
                RAFT_RPC_TYPE_HASH,
                Bytes::from_static(b"drop-mid-flight"),
                RAFT_TRANSPORT_TIMEOUT,
            )
            .await
    });

    // Drop the connection from both sides immediately after the ask is
    // dispatched, before the server's 600ms blocking sleep can finish — this
    // mirrors the timing used by the existing in_flight_ask_drop test in
    // reconnect_convergence_e2e.rs.
    sleep(Duration::from_millis(5)).await;
    a.disconnect_peer_connection(&b.registry.peer_id);
    b.disconnect_peer_connection(&a.registry.peer_id);

    let outcome = tokio::time::timeout(Duration::from_secs(2), ask_task)
        .await
        .expect("ask task must complete after forced disconnect")
        .expect("ask task must not panic");
    // Two outcomes are correct under the disconnect-vs-response race:
    //   1. The ask sees ConnectionDropped (transport-drop classification —
    //      the discriminant shared-raft's `should_disconnect_after_rpc_error`
    //      matches on, evicting the peer immediately).
    //   2. The server's response arrived before the disconnect propagated.
    // Both are valid; what is NOT valid is hanging, panicking, or returning
    // a corrupted reply. The post-drop lookup-cache-clear assertion below is
    // the harder invariant — it must always hold.
    match outcome {
        Err(icanact_remote::GossipError::ConnectionDropped) => {}
        Ok(ref reply) => assert_eq!(
            reply.as_ref(),
            b"b:drop-mid-flight",
            "if the ask wins the race, its reply must be the expected payload"
        ),
        Err(other) => panic!(
            "in-flight drop must yield ConnectionDropped or the original \
             reply, got: {other:?}"
        ),
    }

    // After the drop, lookup_connected_peer must stop returning the stale
    // handle — otherwise shared-raft's reconnect_timeout loop would never
    // make forward progress.
    assert!(
        wait_disconnected(&a, &b.registry.peer_id, Duration::from_secs(2)).await,
        "lookup_connected_peer must return None after a forced disconnect"
    );

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_after_drop_yields_a_fresh_working_handle() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    let delay_b = Arc::new(AtomicU64::new(0));

    let a = node("raft-rpc-stress-fresh-a", "a", asks_a, delay_a).await?;
    let b = node("raft-rpc-stress-fresh-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    let (_, response) = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"pre-drop",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("pre-drop RPC should succeed");
    assert_eq!(response.as_slice(), b"b:pre-drop");

    a.disconnect_peer_connection(&b.registry.peer_id);
    b.disconnect_peer_connection(&a.registry.peer_id);
    assert!(wait_disconnected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    a.add_peer(&b.registry.peer_id)
        .await
        .connect(&b.registry.bind_addr)
        .await?;
    b.add_peer(&a.registry.peer_id)
        .await
        .connect(&a.registry.bind_addr)
        .await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    let (_, response) = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"post-drop",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("post-drop RPC must succeed on the fresh handle");
    assert_eq!(response.as_slice(), b"b:post-drop");

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_disconnect_reconnect_cycles_do_not_leak_lookup_state()
-> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    let delay_b = Arc::new(AtomicU64::new(0));

    let a = node("raft-rpc-stress-cycle-a", "a", asks_a.clone(), delay_a).await?;
    let b = node("raft-rpc-stress-cycle-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    for cycle in 0..5u32 {
        let payload: &'static [u8] = match cycle {
            0 => b"cycle-0",
            1 => b"cycle-1",
            2 => b"cycle-2",
            3 => b"cycle-3",
            _ => b"cycle-4",
        };
        let (_, response) = raft_rpc_lookup_then_ask(
            &a,
            &b.registry.peer_id,
            payload,
            APPEND_ASK_TIMEOUT,
            RAFT_RECONNECT_TIMEOUT,
        )
        .await
        .unwrap_or_else(|err| panic!("cycle {cycle} pre-drop ask: {err}"));
        assert!(response.starts_with(b"b:cycle-"));

        a.disconnect_peer_connection(&b.registry.peer_id);
        b.disconnect_peer_connection(&a.registry.peer_id);
        assert!(
            wait_disconnected(&a, &b.registry.peer_id, Duration::from_secs(2)).await,
            "cycle {cycle} disconnect did not propagate to lookup cache"
        );

        a.add_peer(&b.registry.peer_id)
            .await
            .connect(&b.registry.bind_addr)
            .await?;
        b.add_peer(&a.registry.peer_id)
            .await
            .connect(&a.registry.bind_addr)
            .await?;
        assert!(
            wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await,
            "cycle {cycle} reconnect did not appear in lookup cache"
        );
    }

    let (_, response) = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"final",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("final RPC after 5 cycles must succeed");
    assert_eq!(response.as_slice(), b"b:final");

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_lookups_during_drop_never_share_a_torn_down_handle()
-> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    let delay_b = Arc::new(AtomicU64::new(0));

    let a = node("raft-rpc-stress-concurrent-a", "a", asks_a.clone(), delay_a).await?;
    let b = node("raft-rpc-stress-concurrent-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    // Pre-fetch peer_refs so the concurrent tasks don't need a handle clone.
    let mut peer_refs = Vec::new();
    for _ in 0..8 {
        peer_refs.push(a.lookup_peer(&b.registry.peer_id).await?);
    }

    let mut tasks = Vec::new();
    for (i, peer_ref) in peer_refs.into_iter().enumerate() {
        tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis((i as u64) * 8)).await;
            let conn = match peer_ref.connection_ref() {
                Some(conn) if !conn.is_closed() => conn,
                _ => return Err("no live connection".to_string()),
            };
            match conn
                .ask_actor_frame(
                    RAFT_RPC_ACTOR_ID,
                    RAFT_RPC_TYPE_HASH,
                    Bytes::from_static(b"concurrent"),
                    RAFT_RPC_TIMEOUT,
                )
                .await
            {
                Ok(response) => Ok(response.as_ref().to_vec()),
                Err(err) => Err(err.to_string()),
            }
        }));
    }

    sleep(Duration::from_millis(15)).await;
    a.disconnect_peer_connection(&b.registry.peer_id);

    let mut succeeded = 0u32;
    let mut errored = 0u32;
    for task in tasks {
        let outcome = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("ask task must not hang")
            .expect("ask task must not panic");
        match outcome {
            Ok(payload) => {
                succeeded += 1;
                assert_eq!(
                    payload.as_slice(),
                    b"b:concurrent",
                    "no concurrent task may receive a corrupted reply during a drop"
                );
            }
            Err(err) => {
                errored += 1;
                let lowered = err.to_ascii_lowercase();
                assert!(
                    lowered.contains("timeout")
                        || lowered.contains("timed out")
                        || lowered.contains("connection dropped")
                        || lowered.contains("connection closed")
                        || lowered.contains("connection reset")
                        || lowered.contains("no live connection"),
                    "every concurrent ask error must be a recognisable \
                     timeout/transport error, got: {err}"
                );
            }
        }
    }

    assert_eq!(
        succeeded + errored,
        8,
        "all 8 tasks must complete (no panics, no hangs)"
    );

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}
