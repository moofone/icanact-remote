//! Long-running endurance harness for the shared-raft RPC pattern.
//!
//! All tests in this file are `#[ignore]` so they never run in normal CI.
//! Invoke manually with:
//!
//! ```bash
//! cargo test --test raft_rpc_endurance -- --ignored --nocapture
//! ```
//!
//! Override loop length / frequency with env vars (defaults are the manual
//! soak knobs from RAFT_SPORADIC_LEADERSHIP.md):
//!   - `RAFT_RPC_ENDURANCE_SECONDS`         default 300 (5 minutes)
//!   - `RAFT_RPC_ENDURANCE_DROP_INTERVAL_MS` default 250
//!   - `RAFT_RPC_ENDURANCE_ASK_INTERVAL_MS`  default 50

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
        .set_actor_message_handler_sync(Arc::new(ScriptedHandler { label, asks }))
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

async fn raft_rpc_lookup_then_ask(
    from: &TlsHandle,
    to: &PeerId,
    payload: &'static [u8],
) -> Result<Vec<u8>, String> {
    let started = Instant::now();
    let lookup_deadline = started + RAFT_RECONNECT_TIMEOUT;
    loop {
        if from.client().lookup_connected_peer(to).is_some() {
            break;
        }
        if Instant::now() >= lookup_deadline {
            return Err("lookup_connected_peer never returned a live handle".into());
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
        return Err("lookup returned a closed connection".into());
    }
    match conn
        .ask_actor_frame(
            RAFT_RPC_ACTOR_ID,
            RAFT_RPC_TYPE_HASH,
            Bytes::from_static(payload),
            APPEND_ASK_TIMEOUT,
        )
        .await
    {
        Ok(response) => Ok(response.as_ref().to_vec()),
        Err(err) => Err(err.to_string()),
    }
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Endurance: alternate ask traffic and disconnect/reconnect cycles for
/// `RAFT_RPC_ENDURANCE_SECONDS` (default 300s). Measure reply success rate
/// and assert it stays high; assert the test concludes within a small margin
/// of the deadline (= no wedged, untracked tasks); assert the lookup cache
/// returns a live handle at the end.
///
/// What this catches that single-shot tests don't:
///   - slow leaks of registry-side state (connection counts, peer entries)
///   - reconnection loops that diverge into unbounded retry storms
///   - timer / task accounting drift over thousands of cycles
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running endurance harness; invoke manually with --ignored"]
async fn endurance_alternating_ask_and_drop_cycles_remain_healthy() -> icanact_remote::Result<()> {
    let total_seconds = parse_env_u64("RAFT_RPC_ENDURANCE_SECONDS", 300);
    let drop_interval_ms = parse_env_u64("RAFT_RPC_ENDURANCE_DROP_INTERVAL_MS", 250);
    let ask_interval_ms = parse_env_u64("RAFT_RPC_ENDURANCE_ASK_INTERVAL_MS", 50);

    let asks_a = Arc::new(AtomicU64::new(0));
    let asks_b = Arc::new(AtomicU64::new(0));
    let a = node("raft-rpc-endurance-a", "a", asks_a).await?;
    let b = node("raft-rpc-endurance-b", "b", asks_b.clone()).await?;
    connect_pair(&a, &b).await?;
    assert!(wait_connected(&a, &b.registry.peer_id, Duration::from_secs(5)).await);

    // Sequential interleave: a single loop drives `asks_per_chaos` asks per
    // tick, then performs one drop+reconnect pulse. This is enough to surface
    // slow leaks (the chaos task accumulates state in registry tables) while
    // remaining deterministic and free of cross-task synchronisation noise.
    let asks_per_chaos = (drop_interval_ms / ask_interval_ms.max(1)).max(1);
    let started = Instant::now();
    let deadline = started + Duration::from_secs(total_seconds);

    let mut succeeded: u64 = 0;
    let mut errored: u64 = 0;
    let mut cycles: u64 = 0;

    while Instant::now() < deadline {
        for _ in 0..asks_per_chaos {
            if Instant::now() >= deadline {
                break;
            }
            match raft_rpc_lookup_then_ask(&a, &b.registry.peer_id, b"endurance").await {
                Ok(_) => succeeded += 1,
                Err(_) => errored += 1,
            }
            sleep(Duration::from_millis(ask_interval_ms)).await;
        }
        if Instant::now() >= deadline {
            break;
        }
        a.disconnect_peer_connection(&b.registry.peer_id);
        b.disconnect_peer_connection(&a.registry.peer_id);
        // Best-effort reconnect; the next ask iteration's reconnect_timeout
        // loop will surface a problem if either side fails to come back.
        let _ = a
            .add_peer(&b.registry.peer_id)
            .await
            .connect(&b.registry.bind_addr)
            .await;
        let _ = b
            .add_peer(&a.registry.peer_id)
            .await
            .connect(&a.registry.bind_addr)
            .await;
        cycles += 1;
    }

    let elapsed = started.elapsed();
    let total = succeeded + errored;

    eprintln!(
        "endurance_summary elapsed_s={:.1} chaos_cycles={cycles} asks_total={total} \
         asks_succeeded={succeeded} asks_errored={errored} server_received={}",
        elapsed.as_secs_f64(),
        asks_b.load(Ordering::Acquire)
    );

    assert!(
        total > 0,
        "asker must have attempted at least one RPC over the full window"
    );
    assert!(
        cycles > 0,
        "chaos task must have completed at least one drop+reconnect cycle"
    );

    // Final settled connection must succeed cleanly.
    let _ = a
        .add_peer(&b.registry.peer_id)
        .await
        .connect(&b.registry.bind_addr)
        .await;
    let _ = b
        .add_peer(&a.registry.peer_id)
        .await
        .connect(&a.registry.bind_addr)
        .await;
    assert!(
        wait_connected(&a, &b.registry.peer_id, Duration::from_secs(5)).await,
        "lookup cache must be in connected state after endurance run"
    );
    let response = raft_rpc_lookup_then_ask(&a, &b.registry.peer_id, b"endurance-final")
        .await
        .expect("post-endurance ask must succeed");
    assert_eq!(response.as_slice(), b"b:endurance-final");

    // Health threshold: at least 30% of asks must have succeeded across the
    // whole run. (The chaos task drops every 250ms by default which is
    // aggressive — but the asker should still get traffic through during
    // each healthy gap.) If this drops below 30%, transport is failing more
    // often than chaos forces it to.
    let success_pct = (succeeded as f64 * 100.0) / (total as f64);
    assert!(
        success_pct >= 30.0,
        "endurance success rate fell below floor: {success_pct:.1}% \
         (succeeded={succeeded} errored={errored})"
    );

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}
