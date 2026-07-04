//! Regression/convergence coverage for the duplicate-connection tie-break
//! reconnect-storm class of bug observed on icemining devnet (coins-backend
//! a/b + shared-sync witness, 2026-07): the mesh entered a self-sustaining
//! TLS-handshake-EOF storm (~4 accepted-connection failures/sec, 478/481 in
//! 2 minutes) and never recovered on its own — both sides sat
//! `role=Passive`, no authority-grant quorum could form.
//!
//! Root cause (see REMEDIATION_TIE_BREAK_RECONNECT_STORM.md): the
//! duplicate-connection tie-break (`GossipRegistry::should_keep_connection`)
//! is a *stateless* function of NodeId ordering — it has no memory of the
//! eviction/rejection it just performed. The p2p configured-peer supervisor
//! (`supervise_configured_peers`, driven by `peer_supervisor_interval`)
//! deliberately bypasses the slower `peer_retry_interval` backoff so a
//! genuinely-down required peer reconnects promptly. Live devnet logs
//! captured against the still-wedged incident (2026-07-04, coins .37/.38,
//! `journalctl -u icemining-coins-backend`) show the exact defect firing in
//! steady state, with no ongoing external disruption:
//!
//! ```text
//! outbound_connect_preferred_inbound_timeout_fallback_dial attempt_id=201 ...
//! tcp_connect_ok ... elapsed_ms=0
//! tls_handshake_ok ... elapsed_ms=0
//! hello_handshake_ok ...
//! transport_session_published ... direction=Outbound stream_instance_id=Some(202)
//! transport_session_removed ... stream_instance_id=Some(202) reason="current_connection_cleared"
//! transport_session_removed ... stream_instance_id=Some(202) reason="disconnect_by_peer_id"
//! ```
//!
//! A connection that just completed a full, successful TCP+TLS+hello
//! handshake is torn down by the framework's own duplicate-connection
//! bookkeeping in under 1ms, before any application traffic flows — the
//! higher-NodeId side's preferred-inbound-wait-then-fallback-dial path
//! (`transport_stream.rs`, `outbound_connect_preferred_inbound_timeout_fallback_dial`)
//! racing a genuinely concurrent inbound arrival, exactly the risk the prior
//! stall remediation flagged for this fallback ("publishes extra outbound
//! sessions during collision/reconnect scenarios"). Because eviction has no
//! memory, this repeats indefinitely at whatever cadence the supervisor
//! and/or gossip round redial that peer, which is the observed
//! never-recovers behavior.
//!
//! IMPORTANT HONESTY NOTE: this test does **not** reproduce that exact race
//! in-process. Three different in-process reproduction strategies were
//! attempted (repeated real process restart of one peer on a fixed
//! address/identity; forced concurrent `connect_to_peer` racing from both
//! sides with interleaved forced socket-failure injection; many-task lock
//! contention hammering) and none produced a sustained storm — loopback
//! TCP/TLS on a single machine completes fast enough, with tight enough
//! scheduling, that the specific "fallback dial publishes right as a
//! concurrent inbound tie-break arrives" interleaving is not reliably
//! forced without either real network latency/jitter or a dedicated fault
//! injection seam (e.g. an artificial delay knob between
//! `finalize_new_outbound_connection`'s publish and its return) that does
//! not currently exist in this crate's test harness. The live devnet log
//! excerpt above is the actual proof of the defect; this test is a
//! regression/convergence guard, not a red-first reproduction, and is
//! labeled as such below.
//!
//! This test exercises the scenario end-to-end through the public
//! `GossipRegistryHandle` surface: a long-running higher-NodeId peer `hi`
//! configures a lower-NodeId peer `lo` as a *required* peer at a fixed
//! address; `lo` is repeatedly killed and restarted (same identity, same
//! address) and, once back up, both sides are forced to race
//! `connect_to_peer` concurrently while their transport state is
//! artificially perturbed, simulating the kind of churn a Gate-E-style
//! failover drill produces. It asserts, on the pair that experienced that
//! churn once external disruption stops:
//!
//!   (a) the pair converges to a stable connection within a bounded time;
//!   (b) the outbound reconnect-attempt rate in the quiet window *after*
//!       convergence is bounded near zero — i.e. no continuing storm;
//!   (c) zero pre-handshake TLS EOF failures are logged in that same quiet
//!       window;
//!   (d) application traffic (registry actor lookup) flows mesh-wide after
//!       convergence.
//!
//! This test PASSES both before and after the `tie_break_reconnect_cooldown`
//! fix (it is a regression/pin guard for convergence + bounded steady-state
//! reconnect rate under churn, not a reproduction of the live incident's
//! exact race) — see the honesty note above for why a true red-first
//! reproduction was not achieved in-process.

use icanact_remote::{
    GossipConfig, GossipRegistryHandle, KeyPair, TransportDirection, TransportLifecycleEvent,
    set_transport_lifecycle_recorder,
};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// Deterministic (no RNG) up-time schedule for the restarted peer: a mix of
/// short (interrupts mid-negotiation) and long (lets it fully converge)
/// windows, so the churn loop exercises both "restart faster than settle"
/// and "one full settle cycle" without relying on wall-clock timing luck for
/// correctness — only for how many iterations manage to fully converge.
const RESTART_UP_MS: &[u64] = &[
    15, 120, 10, 180, 15, 20, 150, 10, 25, 15, 180, 20, 10, 150, 15,
];
const RESTART_DOWN_MS: &[u64] = &[10, 15, 10, 20, 10, 10, 15, 10, 20, 10, 15, 10, 10, 20, 10];
static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn reserve_free_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

fn churn_config() -> GossipConfig {
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

/// Counts reconnect *attempts* (outbound dial starts + tie-break evictions)
/// per peer via the public transport lifecycle recorder, plus pre-handshake
/// TLS EOF occurrences via a tracing layer — both scoped to this test.
#[derive(Default)]
struct StormCounters {
    outbound_starts: AtomicUsize,
    wrong_direction_evictions: AtomicUsize,
}

struct MessageContains(&'static str, Arc<AtomicUsize>);

impl Visit for MessageContains {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let rendered = format!("{value:?}");
            if rendered.contains(self.0) {
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

struct PreHandshakeEofLayer(Arc<AtomicUsize>);

impl<S: tracing::Subscriber> Layer<S> for PreHandshakeEofLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "icanact_remote_lifecycle" {
            return;
        }
        let mut visitor = MessageContains("inbound_pre_handshake_eof", self.0.clone());
        event.record(&mut visitor);
    }
}

fn init_storm_layer() -> Arc<AtomicUsize> {
    let counter = Arc::new(AtomicUsize::new(0));
    let _ = tracing_subscriber::registry()
        .with(PreHandshakeEofLayer(counter.clone()))
        .try_init();
    counter
}

async fn start_node(
    addr: SocketAddr,
    keypair: KeyPair,
    config: GossipConfig,
) -> icanact_remote::Result<GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>> {
    icanact_remote::tls::ensure_crypto_provider();
    GossipRegistryHandle::new_with_transport_stack(
        addr,
        keypair.to_secret_key(),
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
}

async fn configure_required_peer(
    node: &GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    peer_id: &icanact_remote::PeerId,
    addr: SocketAddr,
) {
    let peer = node.add_peer(peer_id).await;
    let _ = peer.connect(&addr).await;
}

async fn pre_handshake_eof_listener() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pre-handshake EOF listener");
    let addr = listener.local_addr().expect("pre-handshake local addr");
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = stream.shutdown().await;
            });
        }
    });
    (addr, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_churn_does_not_produce_unbounded_reconnect_storm() -> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let pre_handshake_eofs = init_storm_layer();
    let counters = Arc::new(StormCounters::default());

    // `KeyPair::new_for_testing` ordering is not guaranteed by seed text, so
    // pick whichever of the two seeds sorts higher by NodeId bytes — the
    // property under test (supervisor + stateless tie-break => storm) holds
    // regardless of which literal seed ends up "hi" vs "lo".
    let seed_x = KeyPair::new_for_testing("tie_break_storm_x");
    let seed_y = KeyPair::new_for_testing("tie_break_storm_y");
    let (hi_keypair, lo_keypair) =
        if seed_x.peer_id().to_node_id().as_bytes() > seed_y.peer_id().to_node_id().as_bytes() {
            (seed_x, seed_y)
        } else {
            (seed_y, seed_x)
        };
    let lo_peer_id = lo_keypair.peer_id();
    let hi_peer_id = hi_keypair.peer_id();

    let lo_addr = reserve_free_addr();

    // `hi` is the long-running side. It is started first, with `lo`
    // configured as a required peer at a fixed address before `lo` is ever
    // up — matching the real devnet shape where a/b/witness all list each
    // other as required peers regardless of boot order.
    let hi = start_node(
        "127.0.0.1:0".parse().unwrap(),
        hi_keypair.clone(),
        churn_config(),
    )
    .await?;
    let hi_addr = hi.registry.bind_addr;
    {
        let peer = hi.add_peer(&lo_peer_id).await;
        let _ = peer.connect(&lo_addr).await; // expected to fail: nothing listening yet
    }

    // Install the transport lifecycle recorder only once `hi` exists so we
    // don't count setup noise; record every OutboundStart / eviction event
    // regardless of peer, filtering by peer id when computing the
    // post-convergence window below.
    {
        let counters_for_recorder = counters.clone();
        set_transport_lifecycle_recorder(Some(Arc::new(move |event| match event {
            TransportLifecycleEvent::OutboundStart { .. } => {
                counters_for_recorder
                    .outbound_starts
                    .fetch_add(1, Ordering::SeqCst);
            }
            TransportLifecycleEvent::WrongDirectionEvicted {
                direction: TransportDirection::Outbound,
                ..
            }
            | TransportLifecycleEvent::WrongDirectionEvicted {
                direction: TransportDirection::Inbound,
                ..
            } => {
                counters_for_recorder
                    .wrong_direction_evictions
                    .fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        })));
    }

    // --- Churn, two kinds, back to back:
    //
    // 1. Restart churn: kill and restart `lo` M times on the same address
    //    and identity, faster than a full connect+settle cycle for most
    //    iterations. This is what a real Gate-E-style failover drill does to
    //    a required peer.
    //
    // 2. Simultaneous-open churn: with `lo` up, force *both* sides to race
    //    `connect_to_peer` concurrently, repeatedly, bypassing the
    //    supervisor's own timing so the race is forced on every iteration
    //    rather than left to timer-jitter luck. This directly exercises
    //    `should_keep_connection`'s duplicate-connection tie-break
    //    (`outbound_tiebreak_evict_wrong_direction` /
    //    `inbound_tiebreak_replace_wrong_direction` /
    //    `inbound_tiebreak_reject_live_duplicate`) and the higher-NodeId
    //    side's preferred-inbound-wait-then-fallback-dial path
    //    (`outbound_connect_preferred_inbound_timeout_fallback_dial`) under
    //    adversarial concurrent pressure — the exact mechanism the
    //    remediation doc for the *previous* stall bug flagged as a risk:
    //    "publishes extra outbound sessions during collision/reconnect
    //    scenarios".
    for (up_ms, down_ms) in RESTART_UP_MS.iter().zip(RESTART_DOWN_MS.iter()) {
        let lo = start_node(lo_addr, lo_keypair.clone(), churn_config()).await?;
        {
            let peer = lo.add_peer(&hi_peer_id).await;
            let _ = peer.connect(&hi_addr).await;
        }
        sleep(Duration::from_millis(*up_ms)).await;
        lo.shutdown_and_wait().await;
        sleep(Duration::from_millis(*down_ms)).await;
    }

    let lo_race = start_node(lo_addr, lo_keypair.clone(), churn_config()).await?;
    {
        let peer = lo_race.add_peer(&hi_peer_id).await;
        let _ = peer.connect(&hi_addr).await;
    }
    for _ in 0..40 {
        // Force both sides to independently decide "I need a connection to
        // this peer right now" on the same tick, concurrently, rather than
        // waiting for the supervisor's own cadence to happen to overlap.
        let hi_registry = hi.registry.clone();
        let lo_registry = lo_race.registry.clone();
        let hi_peer_for_race = lo_peer_id.clone();
        let lo_peer_for_race = hi_peer_id.clone();
        let _ = tokio::join!(
            hi_registry.connect_to_peer(&hi_peer_for_race),
            lo_registry.connect_to_peer(&lo_peer_for_race),
        );
        // Perturb the winner too: force both sides' transport state to drop
        // the connection as if it had just died (the observed devnet
        // signature — a connection dies almost immediately after forming),
        // then immediately race again next iteration.
        let _ = hi
            .registry
            .handle_peer_connection_failure_by_peer_id(&lo_peer_id)
            .await;
        let _ = lo_race
            .registry
            .handle_peer_connection_failure_by_peer_id(&hi_peer_id)
            .await;
        sleep(Duration::from_millis(5)).await;
    }
    // From here on nothing external disrupts the pair again — `lo_race`
    // stays up, untouched, on the same identity/address it just raced on.
    // This is the crux of the test: does the *same* pair that just went
    // through simultaneous-open + restart churn settle on its own, or does
    // the tie-break/supervisor interaction keep it oscillating forever?
    let lo_final = lo_race;

    // Reset counters: only the post-churn behavior is under test now. The
    // churn loop itself is *expected* to generate connect attempts (that's
    // the point of restarting/racing a required peer) — the bug is that it
    // never stops once the disruption stops.
    counters.outbound_starts.store(0, Ordering::SeqCst);
    counters
        .wrong_direction_evictions
        .store(0, Ordering::SeqCst);
    pre_handshake_eofs.store(0, Ordering::SeqCst);

    // (a) Convergence: the pair must reach a stable connection within a
    // bounded time despite the preceding churn.
    let converged = common_wait_for_pair_connection(&hi, &lo_final, Duration::from_secs(5)).await;
    assert!(
        converged,
        "pair failed to converge to a stable connection within 5s after restart churn \
         (outbound_starts={}, wrong_direction_evictions={})",
        counters.outbound_starts.load(Ordering::SeqCst),
        counters.wrong_direction_evictions.load(Ordering::SeqCst),
    );

    // Let any in-flight tie-break settle fully (a fresh connection can still
    // be evicted once more shortly after `has_connection_to_peer` first
    // flips true), then snapshot counts, then measure a genuinely quiet
    // window with nothing external happening.
    sleep(Duration::from_millis(500)).await;
    let settled_outbound = counters.outbound_starts.load(Ordering::SeqCst);
    let settled_evictions = counters.wrong_direction_evictions.load(Ordering::SeqCst);
    let settled_eofs = pre_handshake_eofs.load(Ordering::SeqCst);

    let quiet_window = Duration::from_millis(1500);
    sleep(quiet_window).await;

    let final_outbound = counters.outbound_starts.load(Ordering::SeqCst) - settled_outbound;
    let final_evictions =
        counters.wrong_direction_evictions.load(Ordering::SeqCst) - settled_evictions;
    let final_eofs = pre_handshake_eofs.load(Ordering::SeqCst) - settled_eofs;

    // (b) Storm-rate bound: in steady state the required-peer supervisor
    // ticks every 25ms (60 ticks in 1.5s) but must not dial at all once
    // connected (`supervise_configured_peers` short-circuits on
    // `get_connected_connection_to_peer(..).is_some()`). Allow a small
    // epsilon for one benign settle-related reconnect, not 60.
    assert!(
        final_outbound <= 3,
        "reconnect storm: {final_outbound} outbound connect attempts in a {quiet_window:?} \
         quiet window after convergence (evictions={final_evictions}); expected steady state \
         with ~0 further dials, not a sustained per-tick storm"
    );
    assert!(
        final_evictions <= 1,
        "reconnect storm: {final_evictions} duplicate-connection tie-break evictions in the \
         quiet window after convergence; a converged pair must not keep flapping"
    );

    // (c) Zero pre-handshake TLS EOF failures once settled.
    assert_eq!(
        final_eofs, 0,
        "storm: {final_eofs} pre-handshake TLS EOF failures logged in the quiet window after \
         convergence — this is the exact devnet incident signature \
         (`inbound_pre_handshake_eof`/`TLS accept failed ... tls handshake eof` at elapsed_ms=0)"
    );

    // (d) Application traffic flows mesh-wide after convergence.
    lo_final
        .register("storm_probe_actor".to_string(), lo_addr)
        .await
        .expect("probe actor registration");
    let visible = common_wait_for_actor(&hi, "storm_probe_actor", Duration::from_secs(5)).await;
    assert!(
        visible,
        "gossip/application traffic did not flow from lo to hi after convergence"
    );

    set_transport_lifecycle_recorder(None);
    lo_final.shutdown_and_wait().await;
    hi.shutdown_and_wait().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_handshake_eof_churn_does_not_arm_tie_break_cooldown() -> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let counters = Arc::new(StormCounters::default());
    let counters_for_recorder = counters.clone();
    set_transport_lifecycle_recorder(Some(Arc::new(move |event| match event {
        TransportLifecycleEvent::OutboundStart { .. } => {
            counters_for_recorder
                .outbound_starts
                .fetch_add(1, Ordering::SeqCst);
        }
        TransportLifecycleEvent::WrongDirectionEvicted { .. } => {
            counters_for_recorder
                .wrong_direction_evictions
                .fetch_add(1, Ordering::SeqCst);
        }
        _ => {}
    })));

    let (bad_addr, bad_listener) = pre_handshake_eof_listener().await;
    let mut config = churn_config();
    config.connection_timeout = Duration::from_millis(80);
    config.tie_break_reconnect_cooldown = Duration::from_millis(500);
    let node = start_node(
        "127.0.0.1:0".parse().unwrap(),
        KeyPair::new_for_testing("half-open-eof-supervisor"),
        config,
    )
    .await?;
    let bad_peer = KeyPair::new_for_testing("half-open-eof-peer").peer_id();

    configure_required_peer(&node, &bad_peer, bad_addr).await;
    let after_first = counters.outbound_starts.load(Ordering::SeqCst);
    assert!(
        after_first >= 1,
        "setup should have attempted at least one outbound dial to the half-open peer"
    );
    assert_eq!(
        counters.wrong_direction_evictions.load(Ordering::SeqCst),
        0,
        "pre-handshake EOF churn is not a duplicate-connection tie-break"
    );

    // This call is deliberately inside tie_break_reconnect_cooldown. If
    // generic socket/TLS EOF failures accidentally arm the tie-break storm
    // guard, the supervisor will skip this tick and the outbound count will
    // not increase. The desired contract is narrower: only repeated
    // duplicate-connection tie-break evictions can gate reconnect.
    node.registry.supervise_configured_peers().await;
    let after_second = counters.outbound_starts.load(Ordering::SeqCst);
    assert!(
        after_second > after_first,
        "ordinary half-open/pre-handshake EOF failure must not be throttled by tie-break cooldown"
    );

    set_transport_lifecycle_recorder(None);
    bad_listener.abort();
    node.shutdown_and_wait().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_mesh_restart_and_simultaneous_open_matrix_converges_quietly()
-> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let pre_handshake_eofs = init_storm_layer();
    let counters = Arc::new(StormCounters::default());
    let counters_for_recorder = counters.clone();
    set_transport_lifecycle_recorder(Some(Arc::new(move |event| match event {
        TransportLifecycleEvent::OutboundStart { .. } => {
            counters_for_recorder
                .outbound_starts
                .fetch_add(1, Ordering::SeqCst);
        }
        TransportLifecycleEvent::WrongDirectionEvicted { .. } => {
            counters_for_recorder
                .wrong_direction_evictions
                .fetch_add(1, Ordering::SeqCst);
        }
        _ => {}
    })));

    let churn_addr = reserve_free_addr();
    let churn_key = KeyPair::new_for_testing("witness-mesh-churn-node");
    let stable_a = start_node(
        "127.0.0.1:0".parse().unwrap(),
        KeyPair::new_for_testing("witness-mesh-stable-a"),
        churn_config(),
    )
    .await?;
    let stable_b = start_node(
        "127.0.0.1:0".parse().unwrap(),
        KeyPair::new_for_testing("witness-mesh-stable-b"),
        churn_config(),
    )
    .await?;

    configure_required_peer(
        &stable_a,
        &stable_b.registry.peer_id,
        stable_b.registry.bind_addr,
    )
    .await;
    configure_required_peer(
        &stable_b,
        &stable_a.registry.peer_id,
        stable_a.registry.bind_addr,
    )
    .await;
    configure_required_peer(&stable_a, &churn_key.peer_id(), churn_addr).await;
    configure_required_peer(&stable_b, &churn_key.peer_id(), churn_addr).await;

    for cycle in 0..10 {
        let churn = start_node(churn_addr, churn_key.clone(), churn_config()).await?;
        configure_required_peer(
            &churn,
            &stable_a.registry.peer_id,
            stable_a.registry.bind_addr,
        )
        .await;
        configure_required_peer(
            &churn,
            &stable_b.registry.peer_id,
            stable_b.registry.bind_addr,
        )
        .await;
        let _ = tokio::join!(
            stable_a.registry.connect_to_peer(&churn.registry.peer_id),
            stable_b.registry.connect_to_peer(&churn.registry.peer_id),
            churn.registry.connect_to_peer(&stable_a.registry.peer_id),
            churn.registry.connect_to_peer(&stable_b.registry.peer_id),
        );
        sleep(Duration::from_millis(if cycle % 3 == 0 { 140 } else { 20 })).await;
        churn.shutdown_and_wait().await;
        sleep(Duration::from_millis(15)).await;
    }

    let churn = start_node(churn_addr, churn_key.clone(), churn_config()).await?;
    configure_required_peer(
        &churn,
        &stable_a.registry.peer_id,
        stable_a.registry.bind_addr,
    )
    .await;
    configure_required_peer(
        &churn,
        &stable_b.registry.peer_id,
        stable_b.registry.bind_addr,
    )
    .await;

    for _ in 0..25 {
        let _ = tokio::join!(
            stable_a
                .registry
                .connect_to_peer(&stable_b.registry.peer_id),
            stable_b
                .registry
                .connect_to_peer(&stable_a.registry.peer_id),
            stable_a.registry.connect_to_peer(&churn.registry.peer_id),
            stable_b.registry.connect_to_peer(&churn.registry.peer_id),
            churn.registry.connect_to_peer(&stable_a.registry.peer_id),
            churn.registry.connect_to_peer(&stable_b.registry.peer_id),
        );
        sleep(Duration::from_millis(5)).await;
    }

    counters.outbound_starts.store(0, Ordering::SeqCst);
    counters
        .wrong_direction_evictions
        .store(0, Ordering::SeqCst);
    pre_handshake_eofs.store(0, Ordering::SeqCst);

    assert!(
        common_wait_for_mesh_connections(&[&stable_a, &stable_b, &churn], Duration::from_secs(8))
            .await,
        "three-node witness mesh failed to converge after restart + simultaneous-open matrix"
    );

    sleep(Duration::from_millis(500)).await;
    let settled_outbound = counters.outbound_starts.load(Ordering::SeqCst);
    let settled_evictions = counters.wrong_direction_evictions.load(Ordering::SeqCst);
    let settled_eofs = pre_handshake_eofs.load(Ordering::SeqCst);
    sleep(Duration::from_millis(1500)).await;

    let quiet_outbound = counters.outbound_starts.load(Ordering::SeqCst) - settled_outbound;
    let quiet_evictions =
        counters.wrong_direction_evictions.load(Ordering::SeqCst) - settled_evictions;
    let quiet_eofs = pre_handshake_eofs.load(Ordering::SeqCst) - settled_eofs;
    assert!(
        quiet_outbound <= 6,
        "witness mesh kept dialing after convergence: outbound_starts={quiet_outbound}, evictions={quiet_evictions}"
    );
    assert!(
        quiet_evictions <= 2,
        "witness mesh kept re-litigating duplicate tie-breaks after convergence: {quiet_evictions}"
    );
    assert_eq!(
        quiet_eofs, 0,
        "witness mesh logged pre-handshake EOFs after convergence: {quiet_eofs}"
    );

    churn
        .register("witness_mesh_probe".to_string(), churn.registry.bind_addr)
        .await
        .expect("witness mesh probe registration");
    assert!(
        common_wait_for_actor(&stable_a, "witness_mesh_probe", Duration::from_secs(5)).await
            && common_wait_for_actor(&stable_b, "witness_mesh_probe", Duration::from_secs(5)).await,
        "registry traffic did not flow through the converged witness mesh"
    );

    set_transport_lifecycle_recorder(None);
    churn.shutdown_and_wait().await;
    stable_a.shutdown_and_wait().await;
    stable_b.shutdown_and_wait().await;
    Ok(())
}

async fn common_wait_for_pair_connection(
    a: &GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    b: &GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    // Require the connection to be observed stable across three consecutive
    // checks 100ms apart, not just momentarily true — a flapping pair could
    // otherwise satisfy a single-sample check mid-oscillation.
    let mut consecutive = 0;
    while Instant::now() < deadline {
        let up = a.registry.has_connection_to_peer(&b.registry.peer_id).await
            || b.registry.has_connection_to_peer(&a.registry.peer_id).await;
        if up {
            consecutive += 1;
            if consecutive >= 3 {
                return true;
            }
        } else {
            consecutive = 0;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn common_wait_for_mesh_connections(
    nodes: &[&GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>],
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut consecutive = 0;
    while Instant::now() < deadline {
        let mut all_connected = true;
        for left in 0..nodes.len() {
            for right in (left + 1)..nodes.len() {
                let a = nodes[left];
                let b = nodes[right];
                let connected = a.registry.has_connection_to_peer(&b.registry.peer_id).await
                    || b.registry.has_connection_to_peer(&a.registry.peer_id).await;
                if !connected {
                    all_connected = false;
                    break;
                }
            }
            if !all_connected {
                break;
            }
        }
        if all_connected {
            consecutive += 1;
            if consecutive >= 3 {
                return true;
            }
        } else {
            consecutive = 0;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn common_wait_for_actor(
    node: &GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    name: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if node.lookup(name).await.is_some() {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}
