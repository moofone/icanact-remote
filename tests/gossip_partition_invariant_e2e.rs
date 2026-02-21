mod common;

use common::{
    DynError, connect_bidirectional, create_tls_node, force_disconnect, wait_for_condition,
};
use icanact_remote::{GossipConfig, RegistrationPriority};
use std::future::Future;
use std::time::Duration;
use tokio::runtime::Builder;

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;

fn run_partition_test<F, R>(future: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    std::thread::Builder::new()
        .name("gossip-partition-invariant".into())
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build gossip partition runtime");
            rt.block_on(future)
        })
        .expect("failed to spawn gossip partition thread")
        .join()
        .expect("gossip partition test thread panicked unexpectedly")
}

fn has_actor(node: &icanact_remote::GossipRegistryHandle, actor: &str) -> bool {
    node.registry.actor_state.local_actors.contains_sync(actor)
        || node.registry.actor_state.known_actors.contains_sync(actor)
}

async fn assert_absent_for(
    duration: Duration,
    mut check: impl FnMut() -> bool,
) -> Result<(), DynError> {
    let start = std::time::Instant::now();
    while start.elapsed() < duration {
        if check() {
            return Err(format!("actor appeared unexpectedly within {:?}", duration).into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

#[test]
fn test_partition_blocks_propagation_until_heal_without_lookup_dials() -> Result<(), DynError> {
    run_partition_test(async move {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(200),
            // Keep retries suppressed so our forced partition remains stable.
            peer_retry_interval: Duration::from_secs(5),
            enable_peer_discovery: false,
            peer_gossip_interval: None,
            ..Default::default()
        };

        let node_a = create_tls_node(config.clone()).await?;
        let node_b = create_tls_node(config.clone()).await?;
        let node_c = create_tls_node(config.clone()).await?;

        connect_bidirectional(&node_a, &node_b).await?;
        connect_bidirectional(&node_b, &node_c).await?;

        // Ensure peers are present in the gossip peer maps before we rely on propagation.
        // Under heavy parallel test load, the "add_peer" path can lag behind "socket connected"
        // stats, which makes immediate gossip miss the peer and causes flakes.
        let peers_ready = wait_for_condition(Duration::from_secs(10), || async {
            let a_has_b = {
                let state = node_a.registry.gossip_state.lock().await;
                state.peers.contains_key(&node_b.registry.bind_addr)
            };
            let b_has_a = {
                let state = node_b.registry.gossip_state.lock().await;
                state.peers.contains_key(&node_a.registry.bind_addr)
            };
            let b_has_c = {
                let state = node_b.registry.gossip_state.lock().await;
                state.peers.contains_key(&node_c.registry.bind_addr)
            };
            let c_has_b = {
                let state = node_c.registry.gossip_state.lock().await;
                state.peers.contains_key(&node_b.registry.bind_addr)
            };

            a_has_b && b_has_a && b_has_c && c_has_b
        })
        .await;
        assert!(peers_ready, "gossip peer maps did not become ready in time");

        // Register an actor on B and ensure A learns it (baseline propagation).
        node_b
            .register_with_priority(
                "actor.before".to_string(),
                node_b.registry.bind_addr,
                RegistrationPriority::Immediate,
            )
            .await?;

        // This can be slow under heavy parallel test load (TLS handshakes + gossip timer),
        // so keep the timeout generous to avoid flakes.
        let ok = wait_for_condition(Duration::from_secs(20), || async {
            has_actor(&node_a, "actor.before")
        })
        .await;
        assert!(
            ok,
            "baseline propagation failed (A must learn actor.before)"
        );

        // Partition B<->C and register a new actor on C.
        force_disconnect(&node_b, &node_c).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        node_c
            .register_with_priority(
                "actor.partitioned".to_string(),
                node_c.registry.bind_addr,
                RegistrationPriority::Immediate,
            )
            .await?;

        // While partition is active, A must not learn about actor.partitioned.
        assert_absent_for(Duration::from_secs(3), || {
            has_actor(&node_a, "actor.partitioned")
        })
        .await?;

        // Heal and ensure propagation completes.
        connect_bidirectional(&node_b, &node_c).await?;

        let healed = wait_for_condition(Duration::from_secs(20), || async {
            has_actor(&node_a, "actor.partitioned")
        })
        .await;
        assert!(healed, "actor.partitioned should propagate after heal");

        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;

        Ok(())
    })
}
