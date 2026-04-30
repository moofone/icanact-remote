use bytes::Bytes;
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse};
use icanact_remote::{
    AlignedBytes, BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId,
    TransportDirection, TransportLifecycleEvent, set_transport_lifecycle_recorder,
};
use std::net::SocketAddr;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
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

struct ScriptedProxy {
    listen_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl ScriptedProxy {
    async fn new(target_addr: SocketAddr, connect_delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind scripted proxy");
        let listen_addr = listener.local_addr().expect("proxy local addr");
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    if !connect_delay.is_zero() {
                        sleep(connect_delay).await;
                    }

                    let Ok(mut outbound) = TcpStream::connect(target_addr).await else {
                        let _ = inbound.shutdown().await;
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                });
            }
        });
        Self { listen_addr, task }
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
    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_pair.to_secret_key(),
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
    handle.registry.configure_peer(peer_id, addr).await;
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
        "lookup should wait for delayed preferred inbound, elapsed={:?}",
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

    for cycle in 0..25 {
        local.disconnect_peer_connection(&remote.registry.peer_id);
        remote.disconnect_peer_connection(&local.registry.peer_id);

        let (local_lookup, remote_connect) = tokio::join!(
            timeout(
                Duration::from_secs(2),
                local.lookup_peer(&remote.registry.peer_id)
            ),
            async {
                remote
                    .add_peer(&local.registry.peer_id)
                    .await
                    .connect(&remote_to_local.listen_addr)
                    .await
            },
        );
        local_lookup
            .expect("lookup timed out")
            .expect("lookup should converge");
        remote_connect.expect("outbound owner connect should converge");

        let payload = format!("cycle-{cycle}");
        let expected = format!("remote:{payload}");
        let reply = local
            .lookup_peer(&remote.registry.peer_id)
            .await?
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                Bytes::from(payload),
                Duration::from_millis(750),
            )
            .await?;
        assert_eq!(reply.as_ref(), expected.as_bytes());

        let local_outbound = events
            .lock()
            .expect("event recorder poisoned")
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    TransportLifecycleEvent::SessionPublished {
                        peer,
                        direction: TransportDirection::Outbound,
                        ..
                    } if peer == &remote.registry.peer_id
                )
            })
            .count();
        assert_eq!(
            local_outbound, 0,
            "inbound-preferred side must never publish outbound to remote"
        );
    }

    assert!(
        asks_remote.load(Ordering::Acquire) >= 25,
        "remote should answer every scripted reconnect ask"
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
