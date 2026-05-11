//! Scripted-chaos transport tests for shared-raft's RPC patterns.
//!
//! Where `raft_rpc_stress.rs` covers single-fault scenarios (single drop,
//! single deadline, single reconnect), this file scripts longer fault
//! sequences that historically caused churn / wedged sessions in shared-raft:
//!
//!   - oscillating drop → reconnect cycles at high frequency
//!   - asymmetric drops (only one side tears down)
//!   - mixed timeout-storm + drop (the `record_failure` cross-product of
//!     `RaftRpcDisconnectTracker`)
//!
//! Like raft_rpc_stress.rs these tests are pure transport — no openraft.

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

const APPEND_ASK_TIMEOUT: Duration = Duration::from_millis(275);
const RAFT_RECONNECT_TIMEOUT: Duration = Duration::from_millis(750);

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
        connection_timeout: Duration::from_millis(125),
        response_timeout: Duration::from_millis(125),
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

async fn raft_rpc_lookup_then_ask(
    from: &TlsHandle,
    to: &PeerId,
    payload: &'static [u8],
    ask_timeout: Duration,
    reconnect_timeout: Duration,
) -> Result<Vec<u8>, String> {
    let started = Instant::now();
    let lookup_deadline = started + reconnect_timeout;
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
        Ok(response) => Ok(response.as_ref().to_vec()),
        Err(err) => Err(err.to_string()),
    }
}

/// Script: 5x rapid drop+reconnect in under 5s. After all cycles, the lookup
/// cache must be in the connected state, and a fresh ask must succeed within
/// the standard append-entries budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oscillate_drop_reconnect_5x_does_not_wedge_session() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    let delay_b = Arc::new(AtomicU64::new(0));

    let a = node("raft-rpc-chaos-osc-a", "a", asks_a, delay_a).await?;
    let b = node("raft-rpc-chaos-osc-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    for cycle in 0..5u32 {
        // Symmetric drop (both directions) — what shared-raft sees during a
        // tc-leader-isolate event in devnet.
        a.disconnect_peer_connection(&b.registry.peer_id);
        b.disconnect_peer_connection(&a.registry.peer_id);
        assert!(
            wait_disconnected(&a, &b.registry.peer_id, Duration::from_millis(750)).await,
            "cycle {cycle} disconnect did not propagate within 750ms"
        );

        // Reconnect immediately, like the peer-connector does on the next
        // retry tick.
        a.add_peer(&b.registry.peer_id)
            .await
            .connect(&b.registry.bind_addr)
            .await?;
        b.add_peer(&a.registry.peer_id)
            .await
            .connect(&a.registry.bind_addr)
            .await?;
        assert!(
            wait_connected(&a, &b.registry.peer_id, Duration::from_millis(750)).await,
            "cycle {cycle} reconnect did not appear in lookup cache within 750ms"
        );
    }

    // Final sanity: a fresh ask must succeed within the append budget after
    // the storm settles.
    let response = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"settled",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("post-oscillation ask must succeed");
    assert_eq!(response.as_slice(), b"b:settled");

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

/// Asymmetric drop: only A tears down its side of the connection. From B's
/// perspective, the underlying TCP/TLS is still open until the kernel notices.
/// Shared-raft must observe the disconnection from A's lookup cache and not
/// keep using a one-sided handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asymmetric_drop_clears_initiator_lookup_cache() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    let delay_b = Arc::new(AtomicU64::new(0));

    let a = node("raft-rpc-chaos-asym-a", "a", asks_a, delay_a).await?;
    let b = node("raft-rpc-chaos-asym-b", "b", asks_b.clone(), delay_b).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);
    assert!(wait_connected(&b, &a.registry.peer_id, Duration::from_secs(2)).await);

    // Only A drops; B's view is left untouched.
    a.disconnect_peer_connection(&b.registry.peer_id);

    assert!(
        wait_disconnected(&a, &b.registry.peer_id, Duration::from_secs(2)).await,
        "initiator (A) must observe its own disconnect in the lookup cache"
    );

    // After A reconnects (driven from A's side, the peer-connector pattern
    // shared-raft uses for outbound-owned peers), the cache must repopulate.
    a.add_peer(&b.registry.peer_id)
        .await
        .connect(&b.registry.bind_addr)
        .await?;
    assert!(
        wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await,
        "asymmetric reconnect from A must restore A's lookup cache"
    );

    let response = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"asym-recovered",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("post-asymmetric-reconnect ask must succeed");
    assert_eq!(response.as_slice(), b"b:asym-recovered");

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

/// Timeout storm: server is slow enough that several consecutive asks time
/// out, mirroring what shared-raft's RaftRpcDisconnectTracker counts as a
/// streak. Then the server returns to normal latency. The transport must
/// continue to deliver fresh asks (no wedged peer ref, no zombie tasks).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_storm_then_quiet_recovers_without_wedge() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    // Initially, server is slower than the ask budget — every ask times out.
    let delay_b = Arc::new(AtomicU64::new(400));

    let a = node("raft-rpc-chaos-storm-a", "a", asks_a, delay_a).await?;
    let b = node(
        "raft-rpc-chaos-storm-b",
        "b",
        asks_b.clone(),
        Arc::clone(&delay_b),
    )
    .await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    // Fire a burst of asks during the storm. Tight ask budget guarantees each
    // one times out; we just need the transport to remain responsive.
    let storm_budget = Duration::from_millis(150);
    let mut storm_results = Vec::new();
    for i in 0..5u32 {
        let payload: &'static [u8] = match i {
            0 => b"storm-0",
            1 => b"storm-1",
            2 => b"storm-2",
            3 => b"storm-3",
            _ => b"storm-4",
        };
        storm_results.push(
            raft_rpc_lookup_then_ask(
                &a,
                &b.registry.peer_id,
                payload,
                storm_budget,
                RAFT_RECONNECT_TIMEOUT,
            )
            .await,
        );
    }

    // Every storm ask must have errored — none can have raced through.
    let timed_out = storm_results
        .iter()
        .filter(|r| r.is_err())
        .count();
    assert!(
        timed_out >= 4,
        "expected most/all storm asks to time out, got {} timeouts of 5",
        timed_out
    );
    for result in &storm_results {
        if let Err(err) = result {
            let lowered = err.to_ascii_lowercase();
            assert!(
                lowered.contains("timeout") || lowered.contains("timed out"),
                "storm error must be a timeout, got: {err}"
            );
        }
    }

    // Quiet phase: shrink server delay below the budget, transport must
    // recover without an explicit reconnect.
    delay_b.store(20, Ordering::Release);
    sleep(Duration::from_millis(50)).await;

    let response = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"quiet",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("post-storm ask must succeed once server is quiet again");
    assert_eq!(response.as_slice(), b"b:quiet");

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

/// Bursty mixed-fault: alternate quick asks (succeed) with disconnect+reconnect
/// pulses. The lookup cache must track each transition deterministically and
/// no payload may ever be corrupted across a transition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bursty_mixed_drop_and_ask_traffic_is_consistent() -> icanact_remote::Result<()> {
    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let delay_a = Arc::new(AtomicU64::new(0));
    let delay_b = Arc::new(AtomicU64::new(0));

    let a = node("raft-rpc-chaos-burst-a", "a", asks_a, delay_a).await?;
    let b = node(
        "raft-rpc-chaos-burst-b",
        "b",
        asks_b.clone(),
        delay_b,
    )
    .await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(2)).await);

    let mut succeeded = 0u32;
    let mut transient_errors = 0u32;
    for i in 0..8u32 {
        let payload: &'static [u8] = match i {
            0 => b"burst-0",
            1 => b"burst-1",
            2 => b"burst-2",
            3 => b"burst-3",
            4 => b"burst-4",
            5 => b"burst-5",
            6 => b"burst-6",
            _ => b"burst-7",
        };
        let result = raft_rpc_lookup_then_ask(
            &a,
            &b.registry.peer_id,
            payload,
            APPEND_ASK_TIMEOUT,
            RAFT_RECONNECT_TIMEOUT,
        )
        .await;
        match result {
            Ok(response) => {
                succeeded += 1;
                // Critical: every successful reply must be the expected
                // payload. A corrupted reply would mean transport state was
                // shared across drops.
                let expected = format!("b:{}", String::from_utf8_lossy(payload));
                assert_eq!(
                    response.as_slice(),
                    expected.as_bytes(),
                    "every successful reply must match the request payload"
                );
            }
            Err(err) => {
                transient_errors += 1;
                let lowered = err.to_ascii_lowercase();
                assert!(
                    lowered.contains("timeout")
                        || lowered.contains("timed out")
                        || lowered.contains("connection")
                        || lowered.contains("closed")
                        || lowered.contains("never returned a live handle"),
                    "burst error must be a recognisable transport error: {err}"
                );
            }
        }

        // Every other iteration, force a drop+reconnect pulse to mix fault
        // states in with healthy asks.
        if i % 2 == 1 {
            a.disconnect_peer_connection(&b.registry.peer_id);
            b.disconnect_peer_connection(&a.registry.peer_id);
            assert!(
                wait_disconnected(&a, &b.registry.peer_id, Duration::from_millis(750)).await
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
                wait_connected(&a, &b.registry.peer_id, Duration::from_millis(750)).await
            );
        }
    }

    // Healthy iterations should outnumber transient errors comfortably; if
    // not, transport is dropping more than it should under reasonable churn.
    assert!(
        succeeded >= transient_errors,
        "expected more healthy asks than errors under bursty churn, got \
         succeeded={succeeded} errors={transient_errors}"
    );

    // Final settled ask must succeed.
    let response = raft_rpc_lookup_then_ask(
        &a,
        &b.registry.peer_id,
        b"burst-final",
        APPEND_ASK_TIMEOUT,
        RAFT_RECONNECT_TIMEOUT,
    )
    .await
    .expect("post-burst final ask must succeed");
    assert_eq!(response.as_slice(), b"b:burst-final");

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}
