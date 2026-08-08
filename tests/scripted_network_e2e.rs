use bytes::Bytes;
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse, PeerInfoGossip};
use icanact_remote::{
    AlignedBytes, BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId,
    TransportDirection, TransportLifecycleEvent, set_transport_lifecycle_recorder,
};
use std::net::SocketAddr;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const TEST_ACTOR_ID: u64 = 0x51A7_2C00;
const TEST_TYPE_HASH: u32 = 0x51A7_2C01;

type TlsHandle = GossipRegistryHandle<BuilderTlsBootstrap>;

static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[derive(Clone)]
struct EchoHandler {
    label: &'static str,
    asks: Arc<AtomicU64>,
    slow_payload_delay: Option<Duration>,
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
        if let Some(delay) = self.slow_payload_delay
            && payload.as_ref() == b"slow"
        {
            thread::sleep(delay);
        }
        Ok(Some(ActorResponse::from(
            format!(
                "{}:{}",
                self.label,
                String::from_utf8_lossy(payload.as_ref())
            )
            .into_bytes(),
        )))
    }
}

#[derive(Clone, Copy, Default)]
struct ProxyPlan {
    connect_delay: Duration,
    client_first_byte_delay: Duration,
    server_first_byte_delay: Duration,
    close_after: Option<Duration>,
}

struct ScriptedProxy {
    listen_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl ScriptedProxy {
    async fn new(target_addr: SocketAddr, connect_delay: Duration) -> Self {
        Self::with_plan(
            target_addr,
            ProxyPlan {
                connect_delay,
                ..Default::default()
            },
        )
        .await
    }

    async fn with_plan(target_addr: SocketAddr, plan: ProxyPlan) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind scripted proxy");
        let listen_addr = listener.local_addr().expect("proxy local addr");
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                let plan = plan;
                tokio::spawn(async move {
                    if !plan.connect_delay.is_zero() {
                        sleep(plan.connect_delay).await;
                    }

                    let Ok(outbound) = TcpStream::connect(target_addr).await else {
                        let _ = inbound.shutdown().await;
                        return;
                    };
                    let (inbound_read, inbound_write) = inbound.into_split();
                    let (outbound_read, outbound_write) = outbound.into_split();
                    let client_to_server = tokio::spawn(relay_with_initial_delay(
                        inbound_read,
                        outbound_write,
                        plan.client_first_byte_delay,
                    ));
                    let server_to_client = tokio::spawn(relay_with_initial_delay(
                        outbound_read,
                        inbound_write,
                        plan.server_first_byte_delay,
                    ));
                    run_relay_pair(client_to_server, server_to_client, plan.close_after).await;
                });
            }
        });
        Self { listen_addr, task }
    }
}

async fn relay_with_initial_delay<R, W>(
    mut reader: R,
    mut writer: W,
    first_byte_delay: Duration,
) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut first_read = true;
    let mut buf = [0_u8; 16 * 1024];
    loop {
        let read = reader.read(&mut buf).await?;
        if read == 0 {
            let _ = writer.shutdown().await;
            return Ok(());
        }
        if first_read {
            first_read = false;
            if !first_byte_delay.is_zero() {
                sleep(first_byte_delay).await;
            }
        }
        writer.write_all(&buf[..read]).await?;
    }
}

async fn run_relay_pair(
    mut first: JoinHandle<std::io::Result<()>>,
    mut second: JoinHandle<std::io::Result<()>>,
    close_after: Option<Duration>,
) {
    if let Some(close_after) = close_after {
        tokio::select! {
            _ = sleep(close_after) => {
                first.abort();
                second.abort();
            }
            _ = &mut first => {
                second.abort();
            }
            _ = &mut second => {
                first.abort();
            }
        }
        return;
    }

    tokio::select! {
        _ = &mut first => {
            second.abort();
        }
        _ = &mut second => {
            first.abort();
        }
    }
}

impl Drop for ScriptedProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn static_mesh_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(50),
        connection_timeout: Duration::from_millis(750),
        response_timeout: Duration::from_millis(750),
        max_peer_failures: 1,
        peer_retry_interval: Duration::from_millis(25),
        max_gossip_peers: 2,
        small_cluster_threshold: 3,
        ..Default::default()
    }
}

async fn node(
    key_pair: KeyPair,
    label: &'static str,
    asks: Arc<AtomicU64>,
) -> icanact_remote::Result<TlsHandle> {
    node_with_slow_payload(key_pair, label, asks, None).await
}

async fn node_with_slow_payload(
    key_pair: KeyPair,
    label: &'static str,
    asks: Arc<AtomicU64>,
    slow_payload_delay: Option<Duration>,
) -> icanact_remote::Result<TlsHandle> {
    node_with_config_and_slow_payload(key_pair, label, asks, slow_payload_delay, static_mesh_cfg())
        .await
}

async fn node_with_config(
    key_pair: KeyPair,
    label: &'static str,
    asks: Arc<AtomicU64>,
    config: GossipConfig,
) -> icanact_remote::Result<TlsHandle> {
    node_with_config_and_slow_payload(key_pair, label, asks, None, config).await
}

async fn node_with_config_and_slow_payload(
    key_pair: KeyPair,
    label: &'static str,
    asks: Arc<AtomicU64>,
    slow_payload_delay: Option<Duration>,
    config: GossipConfig,
) -> icanact_remote::Result<TlsHandle> {
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair.to_secret_key(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await?;
    handle
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoHandler {
            label,
            asks,
            slow_payload_delay,
        }))
        .await;
    Ok(handle)
}

fn inbound_preferred_key_pair() -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing("scripted-collision-key-a");
    let second = KeyPair::new_for_testing("scripted-collision-key-b");
    if first
        .peer_id()
        .to_node_id()
        .as_bytes()
        .cmp(second.peer_id().to_node_id().as_bytes())
        .is_gt()
    {
        (first, second)
    } else {
        (second, first)
    }
}

async fn configure_static_peer(handle: &TlsHandle, peer_id: PeerId, addr: SocketAddr) {
    let _ = handle.registry.configure_peer(peer_id, addr).await;
}

async fn ask_once(from: &TlsHandle, to: &PeerId, payload: &'static [u8], expected: &'static [u8]) {
    let remote = from.lookup_peer(to).await.expect("lookup peer");
    let reply = remote
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(payload),
            Duration::from_millis(750),
        )
        .await
        .expect("actor ask");
    assert_eq!(reply.as_ref(), expected);
}

async fn wait_until_connected(handle: &TlsHandle, peer_id: &PeerId) -> icanact_remote::Result<()> {
    timeout(Duration::from_secs(2), async {
        loop {
            if handle.client().lookup_connected_peer(peer_id).is_some() {
                return Ok(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| icanact_remote::GossipError::Timeout)?
}

fn install_recorder() -> Arc<Mutex<Vec<TransportLifecycleEvent>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    set_transport_lifecycle_recorder(Some(Arc::new(move |event| {
        sink.lock().expect("event recorder poisoned").push(event);
    })));
    events
}

fn count_events(
    events: &Arc<Mutex<Vec<TransportLifecycleEvent>>>,
    predicate: impl Fn(&TransportLifecycleEvent) -> bool,
) -> usize {
    events
        .lock()
        .expect("event recorder poisoned")
        .iter()
        .filter(|event| predicate(event))
        .count()
}

fn with_events<R>(
    events: &Arc<Mutex<Vec<TransportLifecycleEvent>>>,
    f: impl FnOnce(&[TransportLifecycleEvent]) -> R,
) -> R {
    let guard = events.lock().expect("event recorder poisoned");
    f(&guard)
}

fn session_published_count(
    events: &[TransportLifecycleEvent],
    peer_id: &PeerId,
    direction: TransportDirection,
) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                event,
                TransportLifecycleEvent::SessionPublished {
                    peer,
                    direction: event_direction,
                    ..
                } if peer == peer_id && *event_direction == direction
            )
        })
        .count()
}

/// The direction of the currently LIVE session for `peer_id`, reconstructed
/// by replaying every `SessionPublished`/`SessionRemoved` event for that
/// peer IN ORDER — not just the most recent `SessionPublished`.
///
/// Reverse-searching `SessionPublished` alone is wrong: the publication it
/// finds can have a later `SessionRemoved` for that exact session, with no
/// replacement ever published afterward. That removal would be silently
/// ignored, so the check could pass on a preferred-direction session that
/// is no longer live at all — masking that a different (unwanted-direction)
/// session survived instead, or that no session survived. Tracks "current"
/// as `(addr, direction)` and only lets a `SessionRemoved` clear it when
/// the removed event's `addr` matches the currently-tracked session's own
/// `addr` — i.e. it is removing the exact session believed live, not some
/// older one a later publish already superseded (a `SessionRemoved` for an
/// already-superseded session, arriving out of order, must not clobber a
/// newer publish it has nothing to do with).
///
/// Returns `None` if the peer was never published, or if its last-known
/// session was removed and never replaced — both real "not converged"
/// outcomes a caller must fail on, not conflate with a wrong direction.
fn converged_session_direction(
    events: &[TransportLifecycleEvent],
    peer_id: &PeerId,
) -> Option<TransportDirection> {
    let mut current: Option<(SocketAddr, TransportDirection)> = None;
    for event in events {
        match event {
            TransportLifecycleEvent::SessionPublished {
                peer,
                addr,
                direction,
            } if peer == peer_id => {
                current = Some((*addr, *direction));
            }
            TransportLifecycleEvent::SessionRemoved { peer, addr, .. }
                if peer == peer_id
                    && current.is_some_and(|(current_addr, _)| current_addr == *addr) =>
            {
                current = None;
            }
            _ => {}
        }
    }
    current.map(|(_, direction)| direction)
}

fn session_removed_count(
    events: &[TransportLifecycleEvent],
    peer_id: &PeerId,
    direction: TransportDirection,
) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                event,
                TransportLifecycleEvent::SessionRemoved {
                    peer,
                    direction: event_direction,
                    ..
                } if peer == peer_id && *event_direction == direction
            )
        })
        .count()
}

async fn connect_preferred_direction(
    local: &TlsHandle,
    remote: &TlsHandle,
    remote_to_local: &ScriptedProxy,
) -> icanact_remote::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let connect_result = timeout(Duration::from_secs(2), async {
            remote
                .add_peer(&local.registry.peer_id)
                .await
                .connect(&remote_to_local.listen_addr)
                .await
        })
        .await;

        if matches!(connect_result, Ok(Ok(())))
            && timeout(
                Duration::from_secs(2),
                local.lookup_peer(&remote.registry.peer_id),
            )
            .await
            .is_ok_and(|result| result.is_ok())
        {
            return Ok(());
        }

        local.disconnect_peer_connection(&remote.registry.peer_id);
        remote.disconnect_peer_connection(&local.registry.peer_id);
        sleep(Duration::from_millis(10)).await;
    }

    Err(icanact_remote::GossipError::Network(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "preferred-direction connection did not converge",
    )))
}

async fn shutdown_pair(left: TlsHandle, right: TlsHandle) {
    set_transport_lifecycle_recorder(None);
    left.shutdown().await;
    right.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_preferred_lookup_waits_for_scripted_inbound() -> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let events = install_recorder();

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node(remote_key, "remote", Arc::clone(&asks_remote)).await?;
    assert!(
        !local
            .registry
            .should_keep_connection(&remote.registry.peer_id, true)
    );
    assert!(
        remote
            .registry
            .should_keep_connection(&local.registry.peer_id, true)
    );

    let remote_to_local =
        ScriptedProxy::new(local.registry.bind_addr, Duration::from_millis(200)).await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;
    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;

    let lookup_started = Instant::now();
    let (lookup_result, connect_result) = tokio::join!(
        timeout(
            Duration::from_secs(2),
            local.lookup_peer(&remote.registry.peer_id)
        ),
        async {
            sleep(Duration::from_millis(25)).await;
            remote
                .add_peer(&local.registry.peer_id)
                .await
                .connect(&remote_to_local.listen_addr)
                .await
        }
    );

    let remote_ref = lookup_result
        .expect("inbound-preferred lookup timed out")
        .expect("inbound-preferred lookup should succeed");
    connect_result.expect("outbound owner should connect through proxy");
    assert!(
        lookup_started.elapsed() >= Duration::from_millis(175),
        "lookup should wait for delayed preferred inbound before fallback dialing, elapsed={:?}",
        lookup_started.elapsed()
    );
    assert!(remote_ref.connection_ref().is_some());

    ask_once(
        &local,
        &remote.registry.peer_id,
        b"delayed",
        b"remote:delayed",
    )
    .await;
    assert_eq!(asks_remote.load(Ordering::Acquire), 1);

    assert!(
        count_events(&events, |event| matches!(
            event,
            TransportLifecycleEvent::OutboundSuppressedWaitInbound { .. }
        )) >= 1
    );
    assert!(
        count_events(&events, |event| matches!(
            event,
            TransportLifecycleEvent::OutboundSuppressedInboundReady { .. }
        )) >= 1
    );
    assert_eq!(
        count_events(&events, |event| matches!(
            event,
            TransportLifecycleEvent::OutboundSuppressedInboundTimeout { .. }
        )),
        0
    );
    assert_eq!(
        count_events(&events, |event| matches!(
            event,
            TransportLifecycleEvent::WrongDirectionEvicted { .. }
        )),
        0
    );

    set_transport_lifecycle_recorder(None);
    local.shutdown().await;
    remote.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_connect_collision_keeps_one_preferred_direction() -> icanact_remote::Result<()>
{
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let events = install_recorder();

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node(remote_key, "remote", Arc::clone(&asks_remote)).await?;
    let remote_to_local = ScriptedProxy::new(local.registry.bind_addr, Duration::ZERO).await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;

    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;

    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let local_lookup = {
        let barrier = Arc::clone(&barrier);
        let local = &local;
        let remote_peer_id = &remote.registry.peer_id;
        async move {
            barrier.wait().await;
            local.lookup_peer(remote_peer_id).await
        }
    };
    let remote_lookup = {
        let barrier = Arc::clone(&barrier);
        let remote = &remote;
        let local_peer_id = &local.registry.peer_id;
        async move {
            barrier.wait().await;
            remote.lookup_peer(local_peer_id).await
        }
    };
    let (_, local_result, remote_result) = tokio::join!(
        barrier.wait(),
        timeout(Duration::from_secs(2), local_lookup),
        timeout(Duration::from_secs(2), remote_lookup)
    );

    local_result
        .expect("local simultaneous lookup timed out")
        .expect("local lookup should converge");
    remote_result
        .expect("remote simultaneous lookup timed out")
        .expect("remote lookup should converge");

    ask_once(
        &local,
        &remote.registry.peer_id,
        b"collision-local",
        b"remote:collision-local",
    )
    .await;
    ask_once(
        &remote,
        &local.registry.peer_id,
        b"collision-remote",
        b"local:collision-remote",
    )
    .await;

    with_events(&events, |events| {
        // Both sides start with no existing rival for the other's identity,
        // and the inbound-accept path (`handle_incoming_connection_tls`'s
        // `None => always accept` arm, `src/handle.rs`) deliberately accepts
        // a peer's very first connection unconditionally, regardless of
        // tie-break direction — "so a legitimate first contact is never
        // rejected merely because this side happens to be the lower-NodeId
        // side" (see that arm's own doc comment). In a genuinely
        // simultaneous collision, whichever side's dial lands first on the
        // *accepting* side can therefore be transiently accepted in the
        // wrong direction before the tie-break's `ReplaceExisting` arm
        // (triggered once that side's own correctly-directed dial
        // completes) supersedes it — confirmed by a captured event trace:
        // `InboundAcceptPublishAttempt` -> `SessionPublished{Inbound}` for
        // the outbound owner's identity, followed shortly by
        // `OutboundFinalizePublishAttempt` -> `SessionPublished{Outbound}`
        // for the same identity, correcting it. This reproduced identically
        // (same event shape, different ports) across 2/30 whole-file runs;
        // 0/30 in isolation, consistent with it needing a second peer's real
        // dial landing inside a narrow window that ambient scheduling makes
        // rarer alone.
        //
        // The *outbound-dialing* side has a symmetric guard the
        // inbound-accept side does not: `OutboundSuppressedWaitInbound`
        // defers a non-preferred outbound dial instead of ever completing
        // it, which is why only the outbound-owner's transient-Inbound case
        // has ever been observed here, never the inbound-preferred side
        // transiently publishing Outbound (that path never even attempts
        // the wrong-direction publish in the first place).
        //
        // So "never even transiently publish the wrong direction" is not a
        // guarantee this code makes for first contact — only "converges to
        // the tie-break-correct direction," which the `WrongDirectionEvicted
        // == 0` check below and the two `ask_once` round-trips above already
        // demonstrate did happen. Assert the converged (most recent, not
        // cumulative) direction instead of a zero-transient-occurrences
        // count.
        assert_eq!(
            converged_session_direction(events, &remote.registry.peer_id),
            Some(TransportDirection::Inbound),
            "inbound-preferred side's session must converge to inbound"
        );
        assert_eq!(
            converged_session_direction(events, &local.registry.peer_id),
            Some(TransportDirection::Outbound),
            "outbound owner's session must converge to outbound"
        );
    });
    assert_eq!(
        count_events(&events, |event| matches!(
            event,
            TransportLifecycleEvent::WrongDirectionEvicted { .. }
        )),
        0,
        "simultaneous collision should converge without a replace_wrong_direction loop"
    );

    shutdown_pair(local, remote).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_timeout_does_not_destroy_healthy_session() -> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let events = install_recorder();

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node_with_slow_payload(
        remote_key,
        "remote",
        Arc::clone(&asks_remote),
        Some(Duration::from_millis(250)),
    )
    .await?;
    let remote_to_local = ScriptedProxy::new(local.registry.bind_addr, Duration::ZERO).await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;

    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;
    connect_preferred_direction(&local, &remote, &remote_to_local).await?;

    let remote_ref = local.lookup_peer(&remote.registry.peer_id).await?;
    let timed_out = remote_ref
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"slow"),
            Duration::from_millis(50),
        )
        .await;
    assert!(
        matches!(timed_out, Err(icanact_remote::GossipError::Timeout)),
        "slow actor ask should time out without being treated as transport death: {timed_out:?}"
    );

    ask_once(&local, &remote.registry.peer_id, b"fast", b"remote:fast").await;
    with_events(&events, |events| {
        assert_eq!(
            session_removed_count(
                events,
                &remote.registry.peer_id,
                TransportDirection::Inbound
            ),
            0,
            "ordinary actor timeout must not evict the healthy inbound session"
        );
    });

    shutdown_pair(local, remote).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_hello_slow_path_succeeds_within_budget_and_leaves_no_stale_timeout_session()
-> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node(remote_key, "remote", Arc::clone(&asks_remote)).await?;

    let within_budget_events = install_recorder();
    let remote_to_local = ScriptedProxy::with_plan(
        local.registry.bind_addr,
        ProxyPlan {
            client_first_byte_delay: Duration::from_millis(150),
            server_first_byte_delay: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;
    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;
    connect_preferred_direction(&local, &remote, &remote_to_local).await?;
    ask_once(
        &local,
        &remote.registry.peer_id,
        b"tls-ok",
        b"remote:tls-ok",
    )
    .await;
    with_events(&within_budget_events, |events| {
        assert!(
            session_published_count(
                events,
                &remote.registry.peer_id,
                TransportDirection::Inbound
            ) >= 1,
            "slow TLS/hello bytes inside the connection budget should publish a session"
        );
    });

    drop(remote_to_local);
    drop(local_to_remote);
    shutdown_pair(local, remote).await;

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node(remote_key, "remote", Arc::clone(&asks_remote)).await?;
    let timeout_events = install_recorder();
    let delayed_remote_to_local = ScriptedProxy::with_plan(
        local.registry.bind_addr,
        ProxyPlan {
            client_first_byte_delay: Duration::from_millis(900),
            server_first_byte_delay: Duration::from_millis(900),
            ..Default::default()
        },
    )
    .await;
    assert!(
        remote
            .registry
            .should_keep_connection(&local.registry.peer_id, true),
        "timeout phase must use the outbound owner so suppression cannot hide TLS/hello timing"
    );
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        delayed_remote_to_local.listen_addr,
    )
    .await;
    let result = timeout(
        Duration::from_secs(2),
        remote
            .add_peer(&local.registry.peer_id)
            .await
            .connect(&delayed_remote_to_local.listen_addr),
    )
    .await
    .expect("over-budget TLS/hello connect should return before outer timeout");
    assert!(
        result.is_err(),
        "over-budget TLS/hello delay should fail cleanly"
    );
    with_events(&timeout_events, |events| {
        assert_eq!(
            session_published_count(
                events,
                &local.registry.peer_id,
                TransportDirection::Outbound
            ),
            0,
            "failed TLS/hello attempt must not publish a stale outbound session: {events:#?}"
        );
    });

    shutdown_pair(local, remote).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_open_reset_evicts_dead_session_but_actor_timeout_does_not()
-> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let events = install_recorder();

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node(remote_key, "remote", Arc::clone(&asks_remote)).await?;
    let remote_to_local = ScriptedProxy::with_plan(
        local.registry.bind_addr,
        ProxyPlan {
            close_after: Some(Duration::from_millis(250)),
            ..Default::default()
        },
    )
    .await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;

    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;
    connect_preferred_direction(&local, &remote, &remote_to_local).await?;
    ask_once(
        &local,
        &remote.registry.peer_id,
        b"before-close",
        b"remote:before-close",
    )
    .await;

    sleep(Duration::from_millis(400)).await;
    let _ = local.lookup_peer(&remote.registry.peer_id).await;
    let removed = with_events(&events, |events| {
        session_removed_count(
            events,
            &remote.registry.peer_id,
            TransportDirection::Inbound,
        )
    });
    assert!(
        removed >= 1,
        "forced transport close should evict the stale inbound session"
    );

    shutdown_pair(local, remote).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_transport_during_actor_ask_self_heals_and_reconnects() -> icanact_remote::Result<()>
{
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let events = install_recorder();

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node_with_slow_payload(
        remote_key,
        "remote",
        Arc::clone(&asks_remote),
        Some(Duration::from_millis(1_500)),
    )
    .await?;

    let remote_to_local = ScriptedProxy::with_plan(
        local.registry.bind_addr,
        ProxyPlan {
            close_after: Some(Duration::from_millis(500)),
            ..Default::default()
        },
    )
    .await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;
    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;
    connect_preferred_direction(&local, &remote, &remote_to_local).await?;
    wait_until_connected(&local, &remote.registry.peer_id).await?;

    let remote_ref = local.lookup_peer(&remote.registry.peer_id).await?;
    // A transport-class failure on `ask_actor_frame` is never replayed by
    // `RemoteActorRef` itself: the request may already have reached the
    // remote, so the original error is always what this call returns: only
    // the *cached slot* gets repaired underneath, for the NEXT call (see
    // `tests/remote_actor_ref_self_heal.rs`). This call's own outcome
    // instead races the scripted proxy's forced close against the ask's own
    // request/response round trip: `Ok` means the round trip completed
    // before the close took effect, a transport-class `Err` means it did
    // not - both are legitimate depending on exactly when the close lands.
    // The dead transport session is still torn down and replaced underneath
    // regardless of which one this run hits - see the
    // `session_removed_count`/`session_published_count` assertions below.
    // What must never happen is success with the wrong payload, or a
    // non-transport error.
    let dropped = remote_ref
        .ask_actor_frame(
            TEST_ACTOR_ID,
            TEST_TYPE_HASH,
            Bytes::from_static(b"slow"),
            Duration::from_secs(3),
        )
        .await;
    match dropped {
        Ok(payload) => assert_eq!(
            payload.as_ref(),
            b"remote:slow",
            "a successful self-heal must still return the real response payload"
        ),
        Err(icanact_remote::GossipError::ConnectionDropped)
        | Err(icanact_remote::GossipError::ConnectionClosed(_))
        | Err(icanact_remote::GossipError::Network(_)) => {}
        Err(other) => panic!(
            "transport drop during actor ask should self-heal or surface a transport-class \
             error, got unexpected error: {other:?}"
        ),
    }

    drop(remote_to_local);
    drop(local_to_remote);
    let remote_to_local = ScriptedProxy::new(local.registry.bind_addr, Duration::ZERO).await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;
    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;
    connect_preferred_direction(&local, &remote, &remote_to_local).await?;
    wait_until_connected(&local, &remote.registry.peer_id).await?;

    // The ref only repairs its cached connection reactively, on the call
    // that discovers it dead, and never replays that ambiguous call itself
    // (see the "dropped" assertion above) - so the very next call after this
    // topology change can itself land on the pre-reconnect instance and pay
    // the same one-shot ambiguous failure before the slot is healed. Poll
    // the SAME held `remote_ref` (no fresh `lookup_peer`) so this proves the
    // ref itself converges, not just that a new lookup would.
    let heal_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let healed = loop {
        match remote_ref
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                Bytes::from_static(b"after"),
                Duration::from_secs(3),
            )
            .await
        {
            Ok(payload) => break payload,
            Err(icanact_remote::GossipError::ConnectionDropped)
            | Err(icanact_remote::GossipError::ConnectionClosed(_))
            | Err(icanact_remote::GossipError::Network(_))
                if tokio::time::Instant::now() < heal_deadline =>
            {
                sleep(Duration::from_millis(25)).await;
            }
            Err(other) => {
                panic!("the held ref must eventually reuse the reconnected session, got: {other:?}")
            }
        }
    };
    assert_eq!(healed.as_ref(), b"remote:after");

    with_events(&events, |events| {
        assert!(
            session_removed_count(
                events,
                &remote.registry.peer_id,
                TransportDirection::Inbound,
            ) >= 1,
            "dropped ask transport should remove the dead inbound session"
        );
        assert!(
            session_published_count(
                events,
                &remote.registry.peer_id,
                TransportDirection::Inbound,
            ) >= 2,
            "peer should publish a fresh inbound session after reconnect"
        );
    });

    shutdown_pair(local, remote).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_discovered_addr_does_not_override_configured_peer_connection()
-> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let mut cfg = static_mesh_cfg();
    cfg.enable_peer_discovery = true;
    cfg.allow_loopback_discovery = true;
    let local = node_with_config(local_key, "local", Arc::clone(&asks_local), cfg.clone()).await?;
    let remote = node_with_config(remote_key, "remote", Arc::clone(&asks_remote), cfg).await?;

    let stale_addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let now = icanact_remote::current_timestamp();
    let candidates = local
        .registry
        .on_peer_list_gossip(
            vec![PeerInfoGossip {
                address: stale_addr.to_string(),
                peer_address: None,
                node_id: Some(remote.registry.peer_id.to_node_id()),
                failures: 0,
                last_attempt: now,
                last_success: now,
                dns_name: None,
            }],
            "127.0.0.1:5000",
            now,
        )
        .await;
    assert_eq!(
        candidates.iter().map(|(addr, _)| *addr).collect::<Vec<_>>(),
        vec![stale_addr]
    );

    let remote_to_local = ScriptedProxy::new(local.registry.bind_addr, Duration::ZERO).await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;
    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;

    connect_preferred_direction(&local, &remote, &remote_to_local).await?;
    let remote_ref = local.lookup_peer(&remote.registry.peer_id).await?;
    assert_ne!(
        remote_ref.location.address,
        stale_addr.to_string(),
        "lookup_peer must use the configured peer address, not stale peer-discovery state"
    );
    ask_once(
        &local,
        &remote.registry.peer_id,
        b"configured",
        b"remote:configured",
    )
    .await;

    shutdown_pair(local, remote).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn high_frequency_scripted_reconnects_do_not_preserve_wrong_direction_sessions()
-> icanact_remote::Result<()> {
    let _guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let events = install_recorder();

    let (local_key, remote_key) = inbound_preferred_key_pair();
    let asks_local = Arc::new(AtomicU64::new(0));
    let asks_remote = Arc::new(AtomicU64::new(0));
    let local = node(local_key, "local", Arc::clone(&asks_local)).await?;
    let remote = node(remote_key, "remote", Arc::clone(&asks_remote)).await?;
    let remote_to_local = ScriptedProxy::new(local.registry.bind_addr, Duration::ZERO).await;
    let local_to_remote = ScriptedProxy::new(remote.registry.bind_addr, Duration::ZERO).await;

    configure_static_peer(
        &local,
        remote.registry.peer_id.clone(),
        local_to_remote.listen_addr,
    )
    .await;
    configure_static_peer(
        &remote,
        local.registry.peer_id.clone(),
        remote_to_local.listen_addr,
    )
    .await;

    const SOAK_ROUNDS: usize = 500;
    const MAX_FALLBACK_OUTBOUNDS: usize = SOAK_ROUNDS / 5;

    for cycle in 0..SOAK_ROUNDS {
        local.disconnect_peer_connection(&remote.registry.peer_id);
        remote.disconnect_peer_connection(&local.registry.peer_id);
        sleep(Duration::from_millis(5)).await;

        connect_preferred_direction(&local, &remote, &remote_to_local).await?;

        let payload = format!("cycle-{cycle}");
        let expected = format!("remote:{payload}");
        let deadline = Instant::now() + Duration::from_secs(10);
        let reply = loop {
            match local
                .lookup_peer(&remote.registry.peer_id)
                .await?
                .ask_actor_frame(
                    TEST_ACTOR_ID,
                    TEST_TYPE_HASH,
                    Bytes::from(payload.clone()),
                    Duration::from_secs(2),
                )
                .await
            {
                Ok(reply) => break reply,
                Err(err) if Instant::now() < deadline => {
                    local.disconnect_peer_connection(&remote.registry.peer_id);
                    remote.disconnect_peer_connection(&local.registry.peer_id);
                    sleep(Duration::from_millis(10)).await;
                    connect_preferred_direction(&local, &remote, &remote_to_local).await?;
                    let _ = err;
                }
                Err(err) => return Err(err),
            }
        };
        assert_eq!(reply.as_ref(), expected.as_bytes());

        let local_outbound = with_events(&events, |events| {
            session_published_count(
                events,
                &remote.registry.peer_id,
                TransportDirection::Outbound,
            )
        });
        assert!(
            local_outbound <= MAX_FALLBACK_OUTBOUNDS,
            "fallback outbound publishes should stay bounded during reconnect soak, observed {local_outbound}"
        );
    }

    assert!(
        asks_remote.load(Ordering::Acquire) >= SOAK_ROUNDS as u64,
        "remote should answer every scripted reconnect ask"
    );
    assert!(
        count_events(&events, |event| matches!(
            event,
            TransportLifecycleEvent::WrongDirectionEvicted { .. }
        )) <= MAX_FALLBACK_OUTBOUNDS,
        "fallback repair should not cause an unbounded wrong-direction eviction loop"
    );
    let suppressed_timeouts = count_events(&events, |event| {
        matches!(
            event,
            TransportLifecycleEvent::OutboundSuppressedInboundTimeout { .. }
        )
    });
    assert!(
        suppressed_timeouts <= MAX_FALLBACK_OUTBOUNDS,
        "soak should not spin in suppressed inbound timeouts, observed {suppressed_timeouts}"
    );

    shutdown_pair(local, remote).await;
    Ok(())
}
