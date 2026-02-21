mod common;

use common::{
    DynError, connect_bidirectional, create_tls_node, force_disconnect, wait_for_condition,
};
use icanact_remote::{GossipConfig, RegistrationPriority};
use std::time::Duration;

#[test]
fn test_gossip_matrix_convergence_line_topology() {
    let handle = std::thread::Builder::new()
        .name("gossip-matrix-line".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("failed to build runtime");
            rt.block_on(async move {
                test_gossip_matrix_convergence_line_topology_inner()
                    .await
                    .expect("gossip matrix test failed")
            })
        })
        .expect("failed to spawn gossip matrix test thread");
    handle.join().expect("gossip matrix test panicked");
}

async fn test_gossip_matrix_convergence_line_topology_inner() -> Result<(), DynError> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        max_peer_failures: usize::MAX,
        peer_retry_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let node_a = create_tls_node(config.clone()).await?;
    let node_b = create_tls_node(config.clone()).await?;
    let node_c = create_tls_node(config.clone()).await?;
    let node_d = create_tls_node(config.clone()).await?;

    connect_bidirectional(&node_a, &node_b).await?;
    connect_bidirectional(&node_b, &node_c).await?;
    connect_bidirectional(&node_c, &node_d).await?;

    node_a
        .register_urgent(
            "actor.a".to_string(),
            "127.0.0.1:9301".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node_b
        .register_urgent(
            "actor.b".to_string(),
            "127.0.0.1:9302".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node_c
        .register_urgent(
            "actor.c".to_string(),
            "127.0.0.1:9303".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node_d
        .register_urgent(
            "actor.d".to_string(),
            "127.0.0.1:9304".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;

    let actors = ["actor.a", "actor.b", "actor.c", "actor.d"];
    let nodes = [&node_a, &node_b, &node_c, &node_d];

    let mut attempts = 0;
    let converged = loop {
        let converged = wait_for_condition(Duration::from_secs(12), || async {
            for node in nodes {
                for actor in &actors {
                    let has_actor = node.registry.actor_state.local_actors.contains_sync(*actor)
                        || node.registry.actor_state.known_actors.contains_sync(*actor);

                    if !has_actor {
                        return false;
                    }
                }
            }
            true
        })
        .await;

        if converged || attempts >= 2 {
            break converged;
        }

        attempts += 1;

        // Tear down and reconnect the line to recover from transient TLS churn.
        force_disconnect(&node_a, &node_b).await;
        force_disconnect(&node_b, &node_c).await;
        force_disconnect(&node_c, &node_d).await;

        connect_bidirectional(&node_a, &node_b).await?;
        connect_bidirectional(&node_b, &node_c).await?;
        connect_bidirectional(&node_c, &node_d).await?;
    };

    if !converged {
        for (idx, node) in nodes.iter().enumerate() {
            let mut missing = Vec::new();
            for actor in &actors {
                let has_actor = node.registry.actor_state.local_actors.contains_sync(*actor)
                    || node.registry.actor_state.known_actors.contains_sync(*actor);

                if !has_actor {
                    missing.push(*actor);
                }
            }
            let stats = node.stats().await;
            eprintln!("node {} missing actors: {:?}", idx, missing);
            eprintln!(
                "node {} stats: local_actors={} known_actors={} active_peers={} failed_peers={}",
                idx, stats.local_actors, stats.known_actors, stats.active_peers, stats.failed_peers
            );
        }
    }

    assert!(
        converged,
        "all nodes should converge on all actors in line topology"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
    node_d.shutdown().await;

    Ok(())
}
