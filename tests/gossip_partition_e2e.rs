mod common;

use common::{
    TlsHandle, connect_bidirectional, create_tls_node, force_disconnect, wait_for_condition,
};
use icanact_remote::{GossipConfig, RegistrationPriority};
use std::future::Future;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::sleep;

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;
type DynError = Box<dyn std::error::Error + Send + Sync>;

fn has_actor(node: &TlsHandle, actor: &str) -> bool {
    node.registry.actor_state.local_actors.contains_sync(actor)
        || node.registry.actor_state.known_actors.contains_sync(actor)
}

async fn wait_line_peers_ready(a: &TlsHandle, b: &TlsHandle, c: &TlsHandle) -> bool {
    wait_for_condition(Duration::from_secs(10), || async {
        let a_has_b = {
            let state = a.registry.gossip_state.lock().await;
            state.peers.contains_key(&b.registry.bind_addr)
        };
        let b_has_a = {
            let state = b.registry.gossip_state.lock().await;
            state.peers.contains_key(&a.registry.bind_addr)
        };
        let b_has_c = {
            let state = b.registry.gossip_state.lock().await;
            state.peers.contains_key(&c.registry.bind_addr)
        };
        let c_has_b = {
            let state = c.registry.gossip_state.lock().await;
            state.peers.contains_key(&b.registry.bind_addr)
        };

        a_has_b && b_has_a && b_has_c && c_has_b
    })
    .await
}

fn run_partition_test<F, R>(future: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    std::thread::Builder::new()
        .name("gossip-partition-test".into())
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

#[test]
fn test_partition_heal_flow() -> Result<(), DynError> {
    run_partition_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(200),
            // Keep automatic peer retries suppressed long enough for the forced
            // partition to remain in place until we manually reconnect node B and
            // node C. Short retry windows (the default 300ms) caused node B to
            // reconnect on its own, letting the actor propagate prematurely.
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
        assert!(
            wait_line_peers_ready(&node_a, &node_b, &node_c).await,
            "initial gossip peer maps did not become ready in time"
        );

        // Use node C's bind address for actor registration
        let actor_addr = node_c.registry.bind_addr;
        node_c
            .register("actor.before".to_string(), actor_addr)
            .await?;

        let mut pre_attempts = 0;
        let pre_partition_visible = loop {
            let propagated = wait_for_condition(Duration::from_secs(6), || async {
                has_actor(&node_a, "actor.before")
            })
            .await;

            if propagated || pre_attempts >= 2 {
                break propagated;
            }

            pre_attempts += 1;

            force_disconnect(&node_a, &node_b).await;
            force_disconnect(&node_b, &node_c).await;
            connect_bidirectional(&node_a, &node_b).await?;
            connect_bidirectional(&node_b, &node_c).await?;
            assert!(
                wait_line_peers_ready(&node_a, &node_b, &node_c).await,
                "reconnected gossip peer maps did not become ready in time"
            );
        };

        assert!(
            pre_partition_visible,
            "pre-partition actor should propagate to node A"
        );

        force_disconnect(&node_b, &node_c).await;
        sleep(Duration::from_millis(100)).await;

        // Use node C's bind address for second actor
        let actor_addr_2 = node_c.registry.bind_addr;
        node_c
            .register_with_priority(
                "actor.partitioned".to_string(),
                actor_addr_2,
                RegistrationPriority::Immediate,
            )
            .await?;

        assert!(
            !wait_for_condition(Duration::from_millis(750), || async {
                has_actor(&node_a, "actor.partitioned")
            })
            .await,
            "actor registered on node C must not appear on node A while B-C is partitioned"
        );

        connect_bidirectional(&node_b, &node_c).await?;

        assert!(
            wait_for_condition(Duration::from_secs(6), || async {
                has_actor(&node_a, "actor.partitioned")
            })
            .await,
            "actor should propagate after heal"
        );

        node_a.shutdown().await;
        node_b.shutdown().await;
        node_c.shutdown().await;

        Ok(())
    })
}
