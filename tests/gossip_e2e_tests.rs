use icanact_remote::{GossipConfig, GossipRegistryHandle, RegistrationPriority, SecretKey};

use std::future::Future;
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::time::sleep;

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;
type DynError = Box<dyn std::error::Error + Send + Sync>;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        // `rustls` only allows installing a default crypto provider once per process.
        // The library code may have already installed it by the time this runs, so
        // make init idempotent to avoid flakes.
        icanact_remote::tls::ensure_crypto_provider();
    });
}

fn run_gossip_test<F, R>(future: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    std::thread::Builder::new()
        .name("gossip-e2e-test".into())
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .with_test_writer()
                    .try_init();
            }
            let rt = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build gossip e2e runtime");
            rt.block_on(future)
        })
        .expect("failed to spawn gossip e2e thread")
        .join()
        .expect("gossip e2e test thread panicked unexpectedly")
}

async fn create_node(config: GossipConfig) -> Result<GossipRegistryHandle, DynError> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let node = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        secret_key,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    Ok(node)
}

async fn connect_pair(a: &GossipRegistryHandle, b: &GossipRegistryHandle) {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;

    a.registry
        .add_peer_with_node_id(addr_b, Some(b.registry.peer_id.to_node_id()))
        .await;
    b.registry
        .add_peer_with_node_id(addr_a, Some(a.registry.peer_id.to_node_id()))
        .await;

    a.bootstrap_non_blocking(vec![addr_b]).await;
    b.bootstrap_non_blocking(vec![addr_a]).await;
}

async fn wait_for_actor(node: &GossipRegistryHandle, actor: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if node.lookup(actor).await.is_some() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_for_exchange(
    node: &GossipRegistryHandle,
    initial_delta: u64,
    initial_full: u64,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let stats = node.stats().await;
        if stats.delta_exchanges > initial_delta || stats.full_sync_exchanges > initial_full {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

#[test]
fn test_full_sync_for_small_cluster() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(200),
            small_cluster_threshold: 10,
            ..Default::default()
        };

        let node_a = create_node(config.clone()).await?;
        let node_b = create_node(config.clone()).await?;
        connect_pair(&node_a, &node_b).await;

        node_a
            .register("actor.fullsync".to_string(), node_a.registry.bind_addr)
            .await?;

        assert!(
            wait_for_actor(&node_b, "actor.fullsync", Duration::from_secs(2)).await,
            "actor should propagate via full sync in small cluster"
        );

        let stats_b = node_b.stats().await;
        assert!(
            stats_b.full_sync_exchanges > 0,
            "full sync exchanges should be recorded for small cluster"
        );

        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    })
}

#[test]
fn test_delta_gossip_after_initial_full_sync() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(200),
            small_cluster_threshold: 0,
            full_sync_interval: 1000,
            ..Default::default()
        };

        let node_a = create_node(config.clone()).await?;
        let node_b = create_node(config.clone()).await?;
        connect_pair(&node_a, &node_b).await;

        node_a
            .register("actor.delta.1".to_string(), node_a.registry.bind_addr)
            .await?;
        assert!(
            wait_for_actor(&node_b, "actor.delta.1", Duration::from_secs(5)).await,
            "initial actor should propagate"
        );

        let stats_before = node_b.stats().await;
        let addr_a = node_a.registry.bind_addr;
        let addr_b = node_b.registry.bind_addr;
        let pool_a = &node_a.registry.connection_pool;
        let pool_b = &node_b.registry.connection_pool;
        let had_conn_a_to_b = pool_a.has_connection(&addr_b);
        let had_conn_b_to_a = pool_b.has_connection(&addr_a);
        let bytes_written_before = if had_conn_a_to_b {
            node_a
                .lookup_address(addr_b)
                .await
                .ok()
                .and_then(|r| r.connection_ref().map(|c| c.bytes_written()))
        } else {
            None
        };

        node_a
            .register("actor.delta.2".to_string(), node_a.registry.bind_addr)
            .await?;
        let ok = wait_for_actor(&node_b, "actor.delta.2", Duration::from_secs(5)).await;
        if !ok {
            let stats_a = node_a.stats().await;
            let stats_b = node_b.stats().await;
            let bytes_written_after = if pool_a.has_connection(&addr_b) {
                node_a
                    .lookup_address(addr_b)
                    .await
                    .ok()
                    .and_then(|r| r.connection_ref().map(|c| c.bytes_written()))
            } else {
                None
            };

            panic!(
                "second actor should propagate after initial sync\n\
                 addr_a={addr_a} addr_b={addr_b}\n\
                 conn_a_to_b={had_conn_a_to_b} conn_b_to_a={had_conn_b_to_a}\n\
                 bytes_written_a_to_b_before={bytes_written_before:?} after={bytes_written_after:?}\n\
                 node_a: seq={} local={} known={} peers(active={} failed={}) delta_ex={} full_ex={} delta_hist={}\n\
                 node_b: seq={} local={} known={} peers(active={} failed={}) delta_ex={} full_ex={} delta_hist={}\n\
                 pool_a.connected_peers={:?}\n\
                 pool_b.connected_peers={:?}",
                stats_a.current_sequence,
                stats_a.local_actors,
                stats_a.known_actors,
                stats_a.active_peers,
                stats_a.failed_peers,
                stats_a.delta_exchanges,
                stats_a.full_sync_exchanges,
                stats_a.delta_history_size,
                stats_b.current_sequence,
                stats_b.local_actors,
                stats_b.known_actors,
                stats_b.active_peers,
                stats_b.failed_peers,
                stats_b.delta_exchanges,
                stats_b.full_sync_exchanges,
                stats_b.delta_history_size,
                pool_a.get_connected_peers(),
                pool_b.get_connected_peers(),
            );
        }

        assert!(
            wait_for_exchange(
                &node_b,
                stats_before.delta_exchanges,
                stats_before.full_sync_exchanges,
                Duration::from_secs(5)
            )
            .await,
            "gossip exchange counters should advance after second update"
        );

        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    })
}

#[test]
fn test_immediate_propagation_for_urgent_registration() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(3),
            immediate_propagation_enabled: true,
            urgent_gossip_fanout: 1,
            ..Default::default()
        };

        let node_a = create_node(config.clone()).await?;
        let node_b = create_node(config.clone()).await?;
        connect_pair(&node_a, &node_b).await;

        node_a
            .register_with_priority(
                "actor.immediate".to_string(),
                node_a.registry.bind_addr,
                RegistrationPriority::Immediate,
            )
            .await?;

        assert!(
            wait_for_actor(&node_b, "actor.immediate", Duration::from_secs(1)).await,
            "urgent registration should propagate before normal gossip interval"
        );

        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    })
}
