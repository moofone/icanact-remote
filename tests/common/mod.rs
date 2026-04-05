use icanact_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub type DynError = Box<dyn std::error::Error + Send + Sync>;
type TlsHandle = GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>;

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
    assert!(connected, "Peers failed to connect in bidirectional setup");

    Ok(())
}

#[allow(dead_code)]
pub async fn force_disconnect(a: &TlsHandle, b: &TlsHandle) {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;

    let _ = a.registry.handle_peer_connection_failure(addr_b).await;
    let _ = b.registry.handle_peer_connection_failure(addr_a).await;
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
