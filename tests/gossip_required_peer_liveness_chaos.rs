mod common;

use bytes::Bytes;
use common::{DynError, TlsHandle, connect_bidirectional, create_tls_node, wait_for_condition};
use icanact_remote::lifecycle::{TransportLifecycleEvent, TransportLifecycleRecorderGuard};
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse, RegistryChange};
use icanact_remote::{
    AlignedBytes, BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId,
    RegistrationPriority,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    Arc, Mutex, Once, OnceLock,
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

/// Bounds how many of this file's `node`/`node_at` calls (standing up a real
/// TLS listener) and `connect_bidirectional_bounded` calls (dialing a real
/// two-node connection, including whichever direction
/// `icanact_remote`'s duplicate-connection tie-break drops) may run at once.
/// This file's 6 tests carry no cross-test lock (unlike e.g.
/// `scripted_network_e2e.rs`'s `TEST_LOCK`), so the default parallel test
/// harness runs them fully concurrently, and enough concurrent real-TLS
/// setups collide on the tie-break's fallback wait
/// (`DEFAULT_PREFERRED_INBOUND_WAIT_MS`, 500ms default) at once to
/// occasionally leave a connection that completed setup unusable shortly
/// after — the same mechanism `icanact-core`'s `remote::network::tests`
/// module root-caused and mitigated with an identical bound=2 admission
/// semaphore (`CONNECT_NODES_ADMISSION`; see that constant's doc comment for
/// the full non-monotonic bound-tuning table — 1 and 8 were both worse than
/// 2, because a tighter bound trades self-inflicted concurrency for a
/// longer wall-clock exposure window to ambient host contention). Only the
/// setup helpers are bounded here, never a test's own post-setup traffic.
static NODE_SETUP_ADMISSION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

/// Per-peer count of transport lifecycle events observed by
/// [`ensure_lifecycle_quiescence_recorder_installed`], since process start.
/// Keyed by `PeerId` (not globally) so that one pair's settlement wait never
/// has to wait out unrelated, concurrently-running tests' own ongoing
/// gossip/reconnect churn (several of this file's configs run peer
/// supervision on short cadences) — only further lifecycle activity that
/// actually mentions one of *this* pair's two identities counts as
/// instability for that pair's own wait.
static PEER_LIFECYCLE_EVENT_COUNTS: OnceLock<Mutex<HashMap<PeerId, u64>>> = OnceLock::new();

fn peer_lifecycle_event_counts() -> &'static Mutex<HashMap<PeerId, u64>> {
    PEER_LIFECYCLE_EVENT_COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Extracts the peer identity a transport lifecycle event is about, if any.
/// `ConnectionCountMarkerAttempt`/`ConnectionCountIncrementAttempt` are
/// per-connection-instance instrumentation with no peer identity to key on
/// and are not relevant to connection tie-break settlement, so they are
/// intentionally excluded from quiescence tracking.
fn lifecycle_event_peer(event: &TransportLifecycleEvent) -> Option<&PeerId> {
    match event {
        TransportLifecycleEvent::OutboundStart { peer, .. } => peer.as_ref(),
        TransportLifecycleEvent::ConnectionCountMarkerAttempt { .. }
        | TransportLifecycleEvent::ConnectionCountIncrementAttempt { .. } => None,
        TransportLifecycleEvent::OutboundSuppressedWaitInbound { peer, .. }
        | TransportLifecycleEvent::OutboundSuppressedInboundReady { peer, .. }
        | TransportLifecycleEvent::OutboundSuppressedInboundTimeout { peer, .. }
        | TransportLifecycleEvent::WrongDirectionEvicted { peer, .. }
        | TransportLifecycleEvent::InboundReady { peer, .. }
        | TransportLifecycleEvent::SessionPublished { peer, .. }
        | TransportLifecycleEvent::DuplicateIdentityRejected { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizePublishAttempt { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeClearRaceRetry { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeAcceptIncomingRetryAttempt { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeReplaceExistingRetryAttempt { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeExistingSnapshotTaken { peer, .. }
        | TransportLifecycleEvent::SocketFailureMatchedInstanceTeardownAttempt { peer, .. }
        | TransportLifecycleEvent::SessionRemoved { peer, .. }
        | TransportLifecycleEvent::InboundAcceptPublishAttempt { peer, .. }
        | TransportLifecycleEvent::InboundAcceptClearRaceRetry { peer, .. }
        | TransportLifecycleEvent::InboundAcceptAcceptIncomingRetryAttempt { peer, .. }
        | TransportLifecycleEvent::InboundAcceptReplaceExistingRetryAttempt { peer, .. }
        | TransportLifecycleEvent::InboundAcceptIndexAttempt { peer, .. }
        | TransportLifecycleEvent::InboundAcceptEphemeralAliasAttempt { peer, .. }
        | TransportLifecycleEvent::GetConnectionSelfHealClearAttempt { peer, .. }
        | TransportLifecycleEvent::FullSyncApplyPendingMutation { peer, .. }
        | TransportLifecycleEvent::DeltaApplyPendingMutation { peer, .. } => Some(peer),
    }
}

/// Per-peer count of OUTBOUND-finalize transport lifecycle events, tracked
/// separately from [`PEER_LIFECYCLE_EVENT_COUNTS`] and keyed by the identity
/// of the peer being *dialed*. `connect_bidirectional` issues two physically
/// distinct dials (A to B, and B to A); an outbound-finalize event is
/// produced only by the dialing side's own registry, keyed by the target's
/// identity, so an entry here keyed by B's id can only have come from A's
/// own dial to B, never from B's dial to A. That asymmetry is exactly what
/// [`wait_for_dial_resolution_entered`] needs: an INBOUND-side event would
/// not work for this, because A's dial to B produces an inbound-accept event
/// on B's registry keyed by A's id — the same key B's own dial to A would
/// also produce evidence under — so counting inbound events here would let
/// one direction's success alone satisfy "evidence for both directions".
static PEER_OUTBOUND_DIAL_RESOLUTION_COUNTS: OnceLock<Mutex<HashMap<PeerId, u64>>> =
    OnceLock::new();

fn peer_outbound_dial_resolution_counts() -> &'static Mutex<HashMap<PeerId, u64>> {
    PEER_OUTBOUND_DIAL_RESOLUTION_COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Extracts the dialed peer's identity from an event that proves this
/// registry's own outbound dial to that peer has entered conflict
/// resolution, if `event` is such an event. `OutboundStart` fires merely on
/// dial *attempt*, before any conflict evaluation, so it is excluded.
/// `WrongDirectionEvicted` is excluded too: unlike the others here, its
/// `peer`/`direction` fields don't identify which side (inbound-accept or
/// outbound-finalize) triggered the eviction, so counting it here could
/// attribute an inbound-side win to the outbound direction.
/// `OutboundFinalizeExistingSnapshotTaken` alone is sufficient (it fires
/// unconditionally for every outbound finalize attempt, before the tie-break
/// decision is computed); the rest are included as belt-and-suspenders
/// evidence of the same direction.
fn lifecycle_outbound_dial_resolution_peer(event: &TransportLifecycleEvent) -> Option<&PeerId> {
    match event {
        TransportLifecycleEvent::OutboundSuppressedWaitInbound { peer, .. }
        | TransportLifecycleEvent::OutboundSuppressedInboundReady { peer, .. }
        | TransportLifecycleEvent::OutboundSuppressedInboundTimeout { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizePublishAttempt { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeClearRaceRetry { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeAcceptIncomingRetryAttempt { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeReplaceExistingRetryAttempt { peer, .. }
        | TransportLifecycleEvent::OutboundFinalizeExistingSnapshotTaken { peer, .. } => Some(peer),
        _ => None,
    }
}

/// Installs the process-wide transport lifecycle recorder exactly once, for
/// the remainder of this test binary's run, so [`wait_for_dial_resolution_
/// entered`] and [`wait_for_peer_quiescence`] can observe real tie-break/
/// finalization activity instead of guessing a fixed sleep covers it.
///
/// Deliberately never uninstalled: `TransportLifecycleRecorderGuard`'s
/// uninstall-on-drop exists so concurrently running tests never clobber each
/// other's recorder, but every test in this file wants the SAME recorder for
/// its entire run, and each `tests/*.rs` file is its own separate test
/// binary/process (`lifecycle`'s recorder statics are not shared with any
/// other file), so there is no other installer here to protect against by
/// ever uninstalling it.
fn ensure_lifecycle_quiescence_recorder_installed() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        std::mem::forget(TransportLifecycleRecorderGuard::install(Arc::new(
            |event: TransportLifecycleEvent| {
                if let Some(peer) = lifecycle_outbound_dial_resolution_peer(&event) {
                    let mut counts = peer_outbound_dial_resolution_counts()
                        .lock()
                        .expect("peer outbound dial resolution counts mutex poisoned");
                    *counts.entry(peer.clone()).or_insert(0) += 1;
                }
                let Some(peer) = lifecycle_event_peer(&event) else {
                    return;
                };
                let mut counts = peer_lifecycle_event_counts()
                    .lock()
                    .expect("peer lifecycle event counts mutex poisoned");
                *counts.entry(peer.clone()).or_insert(0) += 1;
            },
        )));
    });
}

/// Waits until at least one outbound-dial-resolution event (see
/// [`lifecycle_outbound_dial_resolution_peer`]) has been observed for EACH
/// of `peer_ids`, bounded by `deadline`. Returns `true` once both are
/// observed, `false` on timeout.
///
/// This is the positive precondition [`wait_for_peer_quiescence`] needs
/// before its "no new events for a window" check means anything: a
/// zero-event window is indistinguishable between "already resolved" and
/// "resolution hasn't started yet" for whichever direction is late under
/// scheduling contention, so a quiet window can elapse and this helper would
/// otherwise release the setup permit while a reciprocal dial's tie-break is
/// still pending. Requiring evidence that *both* of `connect_bidirectional`'s
/// two physically distinct dials (A to B, and B to A) have individually
/// entered resolution converts "nothing happened" from being the entire
/// proof into a secondary confirmation layered on top of an affirmative one.
async fn wait_for_dial_resolution_entered(peer_ids: &[PeerId], deadline: Duration) -> bool {
    wait_for_condition(deadline, || async {
        let counts = peer_outbound_dial_resolution_counts()
            .lock()
            .expect("peer outbound dial resolution counts mutex poisoned");
        peer_ids
            .iter()
            .all(|id| counts.get(id).copied().unwrap_or(0) >= 1)
    })
    .await
}

/// Waits until no transport lifecycle event mentioning any of `peer_ids` has
/// fired for a full `window`, proving any in-flight tie-break/finalization
/// for a just-established connection between them has actually concluded —
/// rather than sleeping a single fixed duration measured from an arbitrary
/// point and hoping it covers whatever internal timer the library is
/// running. Any qualifying event reset the wait to a fresh `window`, so if a
/// pending resolution (eviction, re-publish, wait-timeout fallback) lands
/// during the sleep, that activity itself extends the wait past it; only a
/// window with no such activity at all counts as settled. Bounded by
/// `deadline` so a genuinely stuck connection still fails the test loudly
/// instead of hanging it.
///
/// Callers MUST establish (via [`wait_for_dial_resolution_entered`] or
/// equivalent) that resolution has actually started before relying on this
/// alone: a window with zero events proves settlement only once it is known
/// that "zero events" isn't just "nothing has happened yet".
async fn wait_for_peer_quiescence(peer_ids: &[PeerId], window: Duration, deadline: Duration) {
    let snapshot = |ids: &[PeerId]| -> Vec<u64> {
        let counts = peer_lifecycle_event_counts()
            .lock()
            .expect("peer lifecycle event counts mutex poisoned");
        ids.iter()
            .map(|id| counts.get(id).copied().unwrap_or(0))
            .collect()
    };
    let start = Instant::now();
    loop {
        let before = snapshot(peer_ids);
        sleep(window).await;
        let after = snapshot(peer_ids);
        if before == after {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "transport lifecycle events for {peer_ids:?} never went quiet for a \
             full {window:?} window"
        );
    }
}

/// `connect_bidirectional` (from `common`) under `NODE_SETUP_ADMISSION` —
/// see that constant's doc comment.
///
/// `connect_bidirectional` returning only proves `active_peers >= 1` on both
/// sides — evidence a connection exists, not that it has survived whichever
/// direction `icanact_remote`'s duplicate-connection tie-break drops (the
/// same distinction documented at the `ask_peer_until_success` call sites
/// below). Releasing the permit right there, as this function originally
/// did, stops bounding concurrency for exactly the window the semaphore
/// exists to bound: up to two *more* setups could start while this one's
/// tie-break/finalization is still in flight, letting three or more overlap
/// the same `DEFAULT_PREFERRED_INBOUND_WAIT_MS` fallback window at once.
///
/// A fixed sleep after `connect_bidirectional` returns does not actually fix
/// this, and neither does a bare quiet-window check: `active_peers >= 1` can
/// become true as soon as this call's OWN outbound dial succeeds, which may
/// be before the reciprocal direction's connection — and the collision/
/// tie-break it can trigger — has even arrived, so a quiet window sampled
/// right away can elapse with zero events simply because that reciprocal
/// dial's own resolution hasn't started yet, not because it already
/// finished. [`wait_for_dial_resolution_entered`] closes that gap first,
/// with a positive precondition proving both of the two dials
/// `connect_bidirectional` issues have individually entered resolution; only
/// once that holds does [`wait_for_peer_quiescence`]'s "no further activity"
/// check mean anything, since by construction it cannot end early relative
/// to whatever internal timer is still running — any activity from that
/// timer resolving resets the wait.
async fn connect_bidirectional_bounded(a: &TlsHandle, b: &TlsHandle) -> Result<(), DynError> {
    ensure_lifecycle_quiescence_recorder_installed();
    let _permit = NODE_SETUP_ADMISSION
        .acquire()
        .await
        .expect("NODE_SETUP_ADMISSION is never closed");
    let result = connect_bidirectional(a, b).await;
    let peer_ids = [a.registry.peer_id.clone(), b.registry.peer_id.clone()];
    let resolution_entered =
        wait_for_dial_resolution_entered(&peer_ids, Duration::from_secs(3)).await;
    assert!(
        resolution_entered,
        "outbound dial resolution for {peer_ids:?} never started in at least one direction"
    );
    wait_for_peer_quiescence(
        &peer_ids,
        Duration::from_millis(icanact_remote::config::DEFAULT_PREFERRED_INBOUND_WAIT_MS),
        Duration::from_secs(5),
    )
    .await;
    result
}

async fn node(
    config: GossipConfig,
    label: &'static str,
    asks: Arc<AtomicU64>,
) -> Result<TlsHandle, DynError> {
    let _permit = NODE_SETUP_ADMISSION
        .acquire()
        .await
        .expect("NODE_SETUP_ADMISSION is never closed");
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
    let _permit = NODE_SETUP_ADMISSION
        .acquire()
        .await
        .expect("NODE_SETUP_ADMISSION is never closed");
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
    connect_bidirectional_bounded(&node_a, &node_b).await?;

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
    connect_bidirectional_bounded(&node_a, &node_b).await?;
    connect_bidirectional_bounded(&node_b, &node_c).await?;

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
    // `lookup_peer` succeeding above proves both sides observe a connection,
    // not that it has survived tie-break resolution: both sides configured
    // each other as peers, so B coming online races A's own retry against
    // B's own outbound dial, and the loser's session can still be settling
    // (or a fresh preferred-inbound landing can still be replacing the
    // other direction) at the exact moment this asks. `ask_peer` (single
    // attempt) flaked here for that reason; `ask_peer_until_success` is the
    // established fix for exactly this class of race elsewhere in this file
    // (see `indirect_peer_is_rediscovered_immediately_when_seen_by_direct_
    // neighbor`'s doc comment) — it retries at the RPC layer, so a retried
    // attempt can legitimately deliver even though a prior attempt raced
    // the connection settling, making this an at-least-once, not
    // exactly-once, assertion.
    assert_eq!(
        ask_peer_until_success(&node_a, &peer_b_id, b"late-online", Duration::from_secs(1)).await?,
        b"b:late-online"
    );
    assert!(
        asks_b.load(Ordering::Acquire) >= 1,
        "late peer actor should receive at least one post-convergence ask"
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
    connect_bidirectional_bounded(&node_a, &node_b).await?;
    // `connect_bidirectional` only waits for `active_peers >= 1` on both
    // sides, which — like `lookup_peer` elsewhere in this file — is
    // evidence a connection exists, not that it has survived whichever
    // direction the tie-break dropped. Same fix as the reconnect ask below.
    assert_eq!(
        ask_peer_until_success(
            &node_a,
            &node_b.registry.peer_id,
            b"before-drop",
            Duration::from_secs(1),
        )
        .await?,
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
    // Same class of race as `configured_peers_retry_until_late_peer_comes_
    // online`: `connect_to_peer` returning above proves the reconnect
    // attempt succeeded, not that the resulting session has settled past
    // whatever tie-break/finalization the fresh connection is still
    // completing. A single `ask_peer` here flaked for exactly that reason;
    // `ask_peer_until_success` is this file's established fix for asking
    // through a freshly (re)established connection (see
    // `indirect_peer_is_rediscovered_immediately_when_seen_by_direct_
    // neighbor`'s doc comment) — at-least-once, so the ask count below
    // allows for a legitimate extra retry rather than asserting exactly 2.
    assert_eq!(
        ask_peer_until_success(
            &node_a,
            &node_b.registry.peer_id,
            b"after-reconnect",
            Duration::from_secs(1),
        )
        .await?,
        b"b:after-reconnect"
    );
    assert!(
        asks_b.load(Ordering::Acquire) >= 2,
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
    connect_bidirectional_bounded(&node_a, &node_b).await?;
    assert!(
        wait_connected(&node_a, &node_b.registry.peer_id, Duration::from_secs(1)).await,
        "A and B should be directly connected before C appears"
    );

    let node_c = node(config.clone(), "c", Arc::clone(&asks_c)).await?;
    connect_bidirectional_bounded(&node_b, &node_c).await?;
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
