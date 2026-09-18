use icanact_remote::{
    GossipConfig, GossipRegistryHandle, KeyPair, PeerId, SecretKey, TransportLifecycleEvent,
};
use std::fs::OpenOptions;
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
pub type TlsHandle = GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>;

const NATURAL_LIFECYCLE_EVIDENCE_PATH: &str =
    "/tmp/icanact-qa-20260918/r1/committed-accounting-evidence.log";

pub fn install_natural_lifecycle_recorder(
    on_event: Arc<dyn Fn(&TransportLifecycleEvent) + Send + Sync + 'static>,
) {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<TransportLifecycleEvent>(4096);
        let _ = std::thread::Builder::new()
            .name("icanact-r1-lifecycle-log".into())
            .spawn(move || {
                let _ = std::fs::create_dir_all("/tmp/icanact-qa-20260918/r1");
                let Ok(mut file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(NATURAL_LIFECYCLE_EVIDENCE_PATH)
                else {
                    return;
                };
                let command = std::env::args().collect::<Vec<_>>().join(" ");
                let revision = std::process::Command::new("git")
                    .args(["rev-parse", "HEAD"])
                    .output()
                    .ok()
                    .and_then(|output| String::from_utf8(output.stdout).ok())
                    .map(|value| value.trim().to_owned())
                    .unwrap_or_else(|| "unknown".into());
                let _ = writeln!(
                    file,
                    "capture_start command={command:?} features=test-helpers/all-features revision={revision}"
                );
                while let Ok(event) = receiver.recv() {
                    let _ = writeln!(file, "event={event:?}");
                }
            });
        let recorder = Arc::new(move |event: TransportLifecycleEvent| {
            on_event(&event);
            let _ = sender.try_send(event);
        });
        icanact_remote::set_transport_lifecycle_recorder(Some(recorder));
    });
}

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        // `rustls` only allows installing a default crypto provider once per process.
        // The library code may have already installed it by the time this runs, so
        // make init idempotent to avoid test flakes.
        icanact_remote::tls::ensure_crypto_provider();
    });
}

#[allow(dead_code)]
pub async fn create_tls_node(config: GossipConfig) -> Result<TlsHandle, DynError> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Sandbox note (macOS): transient EPERM ("Operation not permitted") can occur during bind()
    // in socket-heavy suites. Retrying at the test boundary with backoff is more effective than
    // hammering bind() in a tight loop.
    let deadline = Instant::now()
        + Duration::from_millis(
            std::env::var("ICANACT_TEST_EPERM_MAX_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60_000),
        );
    let mut backoff = Duration::from_millis(25);

    loop {
        match GossipRegistryHandle::new_with_transport_stack(
            bind_addr,
            secret_key.clone(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        {
            Ok(node) => return Ok(node),
            Err(e) => {
                let is_eperm = matches!(&e, icanact_remote::GossipError::Network(io) if io.raw_os_error() == Some(1));
                if is_eperm && Instant::now() < deadline {
                    sleep(backoff).await;
                    backoff =
                        std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(1_000));
                    continue;
                }
                return Err(Box::new(e));
            }
        }
    }
}

#[allow(dead_code)]
pub fn fast_gossip_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(100),
        cleanup_interval: Duration::from_millis(200),
        peer_retry_interval: Duration::from_millis(50),
        connection_timeout: Duration::from_millis(750),
        response_timeout: Duration::from_millis(750),
        ..Default::default()
    }
}

#[allow(dead_code)]
pub async fn create_tls_node_with_keypair(
    keypair: KeyPair,
    mut config: GossipConfig,
) -> Result<TlsHandle, DynError> {
    init_crypto();
    config.key_pair = Some(keypair.clone());
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;

    let deadline = Instant::now()
        + Duration::from_millis(
            std::env::var("ICANACT_TEST_EPERM_MAX_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60_000),
        );
    let mut backoff = Duration::from_millis(25);

    loop {
        match GossipRegistryHandle::new_with_transport_stack(
            bind_addr,
            keypair.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        {
            Ok(node) => return Ok(node),
            Err(e) => {
                let is_eperm = matches!(&e, icanact_remote::GossipError::Network(io) if io.raw_os_error() == Some(1));
                if is_eperm && Instant::now() < deadline {
                    sleep(backoff).await;
                    backoff =
                        std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(1_000));
                    continue;
                }
                return Err(Box::new(e));
            }
        }
    }
}

#[allow(dead_code)]
pub fn ordered_keypair_pair(seed_a: &str, seed_b: &str) -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing(seed_a);
    let second = KeyPair::new_for_testing(seed_b);
    if first.peer_id().to_node_id().as_bytes() > second.peer_id().to_node_id().as_bytes() {
        (first, second)
    } else {
        (second, first)
    }
}

#[allow(dead_code)]
pub async fn create_ordered_tls_pair(
    seed_a: &str,
    seed_b: &str,
) -> Result<(TlsHandle, TlsHandle), DynError> {
    let (high, low) = ordered_keypair_pair(seed_a, seed_b);
    let high = create_tls_node_with_keypair(high, fast_gossip_config()).await?;
    let low = create_tls_node_with_keypair(low, fast_gossip_config()).await?;
    assert!(
        high.registry.peer_id.to_node_id().as_bytes()
            > low.registry.peer_id.to_node_id().as_bytes(),
        "ordered pair helper must return high-id node first"
    );
    Ok((high, low))
}

#[allow(dead_code)]
pub async fn seed_peer(from: &TlsHandle, to: &TlsHandle) -> Result<(), DynError> {
    from.add_peer(&to.registry.peer_id)
        .await
        .connect(&to.registry.bind_addr)
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub async fn wait_for_pair_connection(a: &TlsHandle, b: &TlsHandle, timeout: Duration) -> bool {
    wait_for_condition(timeout, || async {
        a.registry.has_connection_to_peer(&b.registry.peer_id).await
            || b.registry.has_connection_to_peer(&a.registry.peer_id).await
    })
    .await
}

#[allow(dead_code)]
pub async fn register_probe_and_wait_visible(
    source: &TlsHandle,
    sink: &TlsHandle,
    name: &str,
    timeout: Duration,
) -> bool {
    source
        .register(name.to_string(), source.registry.bind_addr)
        .await
        .expect("probe actor registration");
    wait_for_condition(timeout, || async move {
        sink.registry.lookup_actor(name).await.is_some()
    })
    .await
}

#[allow(dead_code)]
pub async fn create_quic_node(config: GossipConfig) -> Result<TlsHandle, DynError> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;

    let deadline = Instant::now()
        + Duration::from_millis(
            std::env::var("ICANACT_TEST_EPERM_MAX_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60_000),
        );
    let mut backoff = Duration::from_millis(25);

    loop {
        match GossipRegistryHandle::new_with_transport_stack(
            bind_addr,
            secret_key.clone(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        {
            Ok(node) => return Ok(node),
            Err(e) => {
                let is_eperm = matches!(&e, icanact_remote::GossipError::Network(io) if io.raw_os_error() == Some(1));
                if is_eperm && Instant::now() < deadline {
                    sleep(backoff).await;
                    backoff =
                        std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(1_000));
                    continue;
                }
                return Err(Box::new(e));
            }
        }
    }
}

#[allow(dead_code)]
pub async fn create_native_quic_node(config: GossipConfig) -> Result<TlsHandle, DynError> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;

    let deadline = Instant::now()
        + Duration::from_millis(
            std::env::var("ICANACT_TEST_EPERM_MAX_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60_000),
        );
    let mut backoff = Duration::from_millis(25);

    loop {
        match GossipRegistryHandle::new_with_transport_stack(
            bind_addr,
            secret_key.clone(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        {
            Ok(node) => return Ok(node),
            Err(e) => {
                let is_eperm = matches!(&e, icanact_remote::GossipError::Network(io) if io.raw_os_error() == Some(1));
                if is_eperm && Instant::now() < deadline {
                    sleep(backoff).await;
                    backoff =
                        std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(1_000));
                    continue;
                }
                return Err(Box::new(e));
            }
        }
    }
}

#[allow(dead_code)]
pub async fn create_udp_node(config: GossipConfig) -> Result<TlsHandle, DynError> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;

    let deadline = Instant::now()
        + Duration::from_millis(
            std::env::var("ICANACT_TEST_EPERM_MAX_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60_000),
        );
    let mut backoff = Duration::from_millis(25);

    loop {
        match GossipRegistryHandle::new_with_transport_stack(
            bind_addr,
            secret_key.clone(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        {
            Ok(node) => return Ok(node),
            Err(e) => {
                let is_eperm = matches!(&e, icanact_remote::GossipError::Network(io) if io.raw_os_error() == Some(1));
                if is_eperm && Instant::now() < deadline {
                    sleep(backoff).await;
                    backoff =
                        std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(1_000));
                    continue;
                }
                return Err(Box::new(e));
            }
        }
    }
}

const CONNECTION_DIAGNOSTICS_PATH: &str = "/tmp/icanact-qa-20260918/r1/connection-diagnostics.log";

static CONNECTION_DIAGNOSTICS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn connection_diagnostics_lock() -> &'static Mutex<()> {
    CONNECTION_DIAGNOSTICS_LOCK.get_or_init(|| Mutex::new(()))
}

fn connected_peer_snapshot(node: &TlsHandle, peer_id: &PeerId) -> String {
    let connected = node.client().lookup_connected_peer(peer_id);
    let connection = connected.and_then(|peer| peer.connection_ref());
    let address = connection.as_ref().map(|conn| conn.addr);
    let closed = connection.as_ref().map(|conn| conn.is_closed());
    format!(
        "connected={} connection_addr={address:?} closed={closed:?} instance_id={:?}",
        connection.is_some(),
        node.client().current_peer_connection_instance(peer_id),
    )
}

/// Capture only public, read-only state after a connection setup failure.
///
/// This deliberately records no payloads or secrets and does not alter the
/// setup timeout, retry behavior, or test scheduling. The single append is
/// guarded because the target's default-parallel tests can fail concurrently.
#[allow(dead_code)]
pub async fn capture_connection_diagnostics(context: &str, a: &TlsHandle, b: &TlsHandle) {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;
    let peer_id_a = a.registry.peer_id.clone();
    let peer_id_b = b.registry.peer_id.clone();
    let stats_a = a.stats().await;
    let stats_b = b.stats().await;
    let actors_a = a.snapshot_known_actors();
    let actors_b = b.snapshot_known_actors();
    let connection_a_to_b = connected_peer_snapshot(a, &peer_id_b);
    let connection_b_to_a = connected_peer_snapshot(b, &peer_id_a);
    let record = format!(
        "=== connection-diagnostics context={context} captured_at_ms={} ===\n\
         node_a peer_id={peer_id_a:?} bind_addr={addr_a} dial_addr={addr_b}\n\
         node_a stats={stats_a:?} active_peers={} failed_peers={}\n\
         node_a known_actors={actors_a:?}\n\
         node_a peer_b={peer_id_b:?} {connection_a_to_b}\n\
         node_b peer_id={peer_id_b:?} bind_addr={addr_b} dial_addr={addr_a}\n\
         node_b stats={stats_b:?} active_peers={} failed_peers={}\n\
         node_b known_actors={actors_b:?}\n\
         node_b peer_a={peer_id_a:?} {connection_b_to_a}\n",
        icanact_remote::current_timestamp_millis(),
        stats_a.active_peers,
        stats_a.failed_peers,
        stats_b.active_peers,
        stats_b.failed_peers,
    );

    let lock = connection_diagnostics_lock()
        .lock()
        .expect("connection diagnostics mutex poisoned");
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all("/tmp/icanact-qa-20260918/r1")?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(CONNECTION_DIAGNOSTICS_PATH)?;
        file.write_all(record.as_bytes())
    })();
    drop(lock);
    if let Err(error) = result {
        eprintln!("failed to write {CONNECTION_DIAGNOSTICS_PATH}: {error}");
    }
}

#[allow(dead_code)]
pub async fn connect_bidirectional(a: &TlsHandle, b: &TlsHandle) -> Result<(), DynError> {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;
    let peer_id_a = a.registry.peer_id.clone();
    let peer_id_b = b.registry.peer_id.clone();

    a.registry.configure_peer(peer_id_b.clone(), addr_b).await;
    b.registry.configure_peer(peer_id_a.clone(), addr_a).await;

    let peer_b = a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await?;

    let peer_a = b.add_peer(&peer_id_a).await;
    peer_a.connect(&addr_a).await?;

    // Wait for peers to be fully registered in gossip state
    // This is necessary because add_peer is now async/spawned to avoid deadlocks
    let connected = wait_for_condition(Duration::from_secs(10), || async {
        a.registry.get_stats().await.active_peers >= 1
            && b.registry.get_stats().await.active_peers >= 1
    })
    .await;
    if !connected {
        capture_connection_diagnostics("common/connect_bidirectional", a, b).await;
    }
    assert!(connected, "Peers failed to connect in bidirectional setup");

    Ok(())
}

#[allow(dead_code)]
pub async fn force_disconnect(a: &TlsHandle, b: &TlsHandle) {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;

    let _ = a
        .registry
        .handle_peer_connection_failure(addr_b, None)
        .await;
    let _ = b
        .registry
        .handle_peer_connection_failure(addr_a, None)
        .await;
}

#[allow(dead_code)]
pub async fn wait_for_condition<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check().await {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

#[allow(dead_code)]
pub async fn wait_for_actor(node: &TlsHandle, actor: &str, timeout: Duration) -> bool {
    wait_for_condition(
        timeout,
        || async move { node.lookup(actor).await.is_some() },
    )
    .await
}

#[allow(dead_code)]
pub async fn wait_for_actor_absent(node: &TlsHandle, actor: &str, timeout: Duration) -> bool {
    wait_for_condition(
        timeout,
        || async move { node.lookup(actor).await.is_none() },
    )
    .await
}

#[allow(dead_code)]
pub async fn wait_for_active_peers(node: &TlsHandle, min_peers: usize, timeout: Duration) -> bool {
    wait_for_condition(timeout, || async move {
        node.stats().await.active_peers >= min_peers
    })
    .await
}

#[allow(dead_code)]
pub fn parse_addr(addr: &str) -> SocketAddr {
    addr.parse().expect("valid socket addr")
}
