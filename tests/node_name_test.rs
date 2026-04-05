use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::time::sleep;

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;

async fn wait_for_peer_mapping(
    node: &GossipRegistryHandle,
    peer_id: &icanact_remote::PeerId,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let pool = &node.registry.connection_pool;
        if pool.peer_id_to_addr.contains_sync(peer_id) && pool.connection_count() >= 1 {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

fn run_node_name_test<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    std::thread::Builder::new()
        .name("node-name-test".into())
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build node name test runtime");
            rt.block_on(future);
        })
        .expect("failed to spawn node name test thread")
        .join()
        .expect("node name test thread panicked unexpectedly");
}

fn maybe_init_tracing() {
    // Avoid sandbox-triggered EPERM flakiness unless explicitly enabled.
    if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("icanact_remote=debug")
            .try_init();
    }
}

#[test]
fn test_node_name_based_connections() {
    run_node_name_test(async {
        maybe_init_tracing();

        // Create configs
        let config_a = GossipConfig {
            gossip_interval: Duration::from_secs(5),
            ..Default::default()
        };

        let config_b = GossipConfig {
            gossip_interval: Duration::from_secs(5),
            ..Default::default()
        };

        // Start Node A first with TLS
        let key_pair_a = KeyPair::new_for_testing("test_node_a");
        let peer_id_a = key_pair_a.peer_id();
        let node_a = GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(), // Use port 0 for dynamic allocation
            key_pair_a.to_secret_key(),
            Some(config_a),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .expect("Failed to create node A");

        let node_a_addr = node_a.registry.bind_addr;
        println!("Node A started at {}", node_a_addr);

        // Start Node B with TLS
        let key_pair_b = KeyPair::new_for_testing("test_node_b");
        let peer_id_b = key_pair_b.peer_id();
        let node_b = GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            key_pair_b.to_secret_key(),
            Some(config_b),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .expect("Failed to create node B");

        let node_b_addr = node_b.registry.bind_addr;
        println!("Node B started at {}", node_b_addr);

        // Node B: Add node A as peer and connect
        let peer_a = node_b.add_peer(&peer_id_a).await;
        peer_a
            .connect(&node_a_addr)
            .await
            .expect("Failed to connect to node A");

        // Node A: Add node B as peer and connect
        let peer_b = node_a.add_peer(&peer_id_b).await;
        peer_b
            .connect(&node_b_addr)
            .await
            .expect("Failed to connect to node B");

        assert!(
            wait_for_peer_mapping(&node_b, &peer_id_a, Duration::from_secs(5)).await,
            "Node B failed to retain mapping for Node A"
        );

        assert!(
            wait_for_peer_mapping(&node_a, &peer_id_b, Duration::from_secs(5)).await,
            "Node A failed to retain mapping for Node B"
        );

        // Clean shutdown
        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}
