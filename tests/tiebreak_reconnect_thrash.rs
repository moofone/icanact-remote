//! End-to-end guard for the single-node staggered-restart-into-a-live-peer
//! thrash observed on a devnet Gate-E failover drill (icemining membership
//! candidates over the icanact-remote registry mesh, 2026-07).
//!
//! Real-world shape: two membership candidates hold a stable :9300 connection
//! (lower NodeId `lo` dials, higher NodeId `hi` waits for and accepts the
//! inbound per the tie-break). One node is SIGKILL-restarted (systemd
//! RestartSec) while the other stays up. On the survivor the mutual connection
//! then thrashed ~1/sec forever: a freshly-accepted, tie-break-preferred
//! inbound was `disconnect_by_peer_id`'d immediately after acceptance, with a
//! fresh outbound attempt perpetually in flight, and SWIM never reconverged.
//!
//! HONESTY NOTE (same as tie_break_reconnect_storm.rs): the exact concurrent
//! interleaving — a superseded/wrong-direction connection's teardown
//! collaterally dropping the coexisting preferred inbound, and a superseded
//! stream's deferred socket-failure landing on the freshly-published session —
//! cannot be forced deterministically in-process on loopback (clean FIN and
//! tight scheduling make a single machine converge). The deterministic RED
//! proofs for this defect are the pool/registry-level tests
//! `socket_failure_of_superseded_connection_preserves_current_session` and
//! `address_change_reindexes_without_tearing_down_live_session` in
//! `src/registry.rs`, which reproduce the address-vs-identity teardown directly.
//! This end-to-end test is a convergence + bounded-steady-state guard: after
//! staggered restart churn stops, the pair must settle to exactly one stable,
//! usable connection and stay there (no repeating `disconnect_by_peer_id` on a
//! just-published session), and application traffic must flow.

use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId, SessionRemovalReason,
    TransportLifecycleEvent, set_transport_lifecycle_recorder, tls,
};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Once, OnceLock};
use std::time::{Duration, Instant};
use tokio::time::sleep;

static CRYPTO_INIT: Once = Once::new();
static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(tls::ensure_crypto_provider);
}

fn churn_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(40),
        cleanup_interval: Duration::from_millis(100),
        peer_retry_interval: Duration::from_millis(50),
        peer_supervisor_interval: Duration::from_millis(25),
        connection_timeout: Duration::from_millis(120),
        response_timeout: Duration::from_millis(120),
        ..Default::default()
    }
}

fn reserve_free_addr() -> SocketAddr {
    let l = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let a = l.local_addr().expect("local_addr");
    drop(l);
    a
}

async fn start(addr: SocketAddr, kp: KeyPair) -> GossipRegistryHandle<BuilderTlsBootstrap> {
    init_crypto();
    let mut c = churn_cfg();
    c.key_pair = Some(kp.clone());
    GossipRegistryHandle::new_with_transport_stack(
        addr,
        kp.to_secret_key(),
        Some(c),
        BuilderTlsBootstrap,
    )
    .await
    .expect("start node")
}

fn ordered(a: &str, b: &str) -> (KeyPair, KeyPair) {
    let x = KeyPair::new_for_testing(a);
    let y = KeyPair::new_for_testing(b);
    if x.peer_id().to_node_id().as_bytes() > y.peer_id().to_node_id().as_bytes() {
        (x, y)
    } else {
        (y, x)
    }
}

async fn pair_connected(
    a: &GossipRegistryHandle<BuilderTlsBootstrap>,
    b: &GossipRegistryHandle<BuilderTlsBootstrap>,
) -> bool {
    a.registry.has_connection_to_peer(&b.registry.peer_id).await
        || b.registry.has_connection_to_peer(&a.registry.peer_id).await
}

async fn wait_connected(
    a: &GossipRegistryHandle<BuilderTlsBootstrap>,
    b: &GossipRegistryHandle<BuilderTlsBootstrap>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut consec = 0;
    while Instant::now() < deadline {
        if pair_connected(a, b).await {
            consec += 1;
            if consec >= 3 {
                return true;
            }
        } else {
            consec = 0;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staggered_single_node_restart_converges_to_one_stable_connection()
-> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    // Count hi-side teardowns of the peer's session (the thrash signature).
    let disc_current = Arc::new(AtomicUsize::new(0));
    let (hi_kp, lo_kp) = ordered("tbrt-hi-seed", "tbrt-lo-seed");
    let hi_id: PeerId = hi_kp.peer_id();
    let lo_id: PeerId = lo_kp.peer_id();
    let lo_id_for_rec = lo_id.clone();
    let disc_for_rec = disc_current.clone();
    set_transport_lifecycle_recorder(Some(Arc::new(move |e| {
        if let TransportLifecycleEvent::SessionRemoved {
            peer,
            reason: SessionRemovalReason::DisconnectByPeerId,
            ..
        } = &e
            && peer == &lo_id_for_rec
        {
            disc_for_rec.fetch_add(1, Ordering::SeqCst);
        }
    })));

    let lo_addr = reserve_free_addr();
    let hi = start("127.0.0.1:0".parse().unwrap(), hi_kp.clone()).await;
    let hi_addr = hi.registry.bind_addr;
    {
        let p = hi.add_peer(&lo_id).await;
        let _ = p.connect(&lo_addr).await;
    }

    // Staggered restart churn: kill and restart `lo` on the same addr+identity
    // with varied up/down windows while `hi` stays up, keeping its
    // outbound-wait-preferred-inbound loop and supervisor running. Concurrent
    // connect pressure exercises the higher-id fallback-dial path.
    let up = [15u64, 120, 10, 90, 15, 130, 10, 20, 100, 15];
    let down = [10u64, 15, 10, 20, 10, 12, 10, 18, 10, 15];
    for (u, d) in up.iter().zip(down.iter()) {
        let lo = start(lo_addr, lo_kp.clone()).await;
        {
            let p = lo.add_peer(&hi_id).await;
            let _ = p.connect(&hi_addr).await;
        }
        for _ in 0..3 {
            let _ = tokio::join!(
                hi.registry.connect_to_peer(&lo_id),
                lo.registry.connect_to_peer(&hi_id),
            );
            sleep(Duration::from_millis(3)).await;
        }
        sleep(Duration::from_millis(*u)).await;
        drop(lo);
        sleep(Duration::from_millis(*d)).await;
    }

    // Final fresh instance; then nothing external disrupts the pair again.
    let lo_final = start(lo_addr, lo_kp.clone()).await;
    {
        let p = lo_final.add_peer(&hi_id).await;
        let _ = p.connect(&hi_addr).await;
    }

    assert!(
        wait_connected(&hi, &lo_final, Duration::from_secs(6)).await,
        "pair failed to converge to a stable connection after staggered restart churn"
    );

    // Quiet window: measure that the converged pair does NOT keep tearing down
    // its just-established session (the ~1/sec devnet thrash).
    sleep(Duration::from_millis(500)).await;
    let base = disc_current.load(Ordering::SeqCst);
    let quiet = Duration::from_millis(1500);
    sleep(quiet).await;
    let churn_in_quiet = disc_current.load(Ordering::SeqCst) - base;

    assert!(
        pair_connected(&hi, &lo_final).await,
        "pair lost its connection during the quiet window (thrash)"
    );
    assert!(
        churn_in_quiet <= 2,
        "reconnect thrash: {churn_in_quiet} hi-side disconnect_by_peer_id events in a \
         {quiet:?} quiet window after convergence; a converged pair must not keep tearing \
         down its session"
    );

    // Application traffic must flow after convergence.
    lo_final
        .register("tbrt_probe".to_string(), lo_addr)
        .await
        .expect("probe registration");
    let visible = {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut v = false;
        while Instant::now() < deadline {
            if hi.lookup("tbrt_probe").await.is_some() {
                v = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        v
    };
    assert!(visible, "registry traffic did not flow after convergence");

    set_transport_lifecycle_recorder(None);
    lo_final.shutdown_and_wait().await;
    hi.shutdown_and_wait().await;
    Ok(())
}
