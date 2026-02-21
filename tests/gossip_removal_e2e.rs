mod common;

use common::{
    DynError, connect_bidirectional, create_tls_node, wait_for_actor, wait_for_condition,
};
use icanact_remote::{GossipConfig, RegistrationPriority};
use std::time::Duration;

#[test]
fn test_actor_removal_propagates() {
    let handle = std::thread::Builder::new()
        .name("gossip-removal".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("failed to build runtime");
            rt.block_on(async {
                test_actor_removal_propagates_inner()
                    .await
                    .expect("actor removal test failed")
            });
        })
        .expect("failed to spawn gossip removal test thread");
    handle.join().expect("gossip removal test panicked");
}

async fn test_actor_removal_propagates_inner() -> Result<(), DynError> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        ..Default::default()
    };

    let node_a = create_tls_node(config.clone()).await?;
    let node_b = create_tls_node(config.clone()).await?;
    connect_bidirectional(&node_a, &node_b).await?;

    node_a
        .register_with_priority(
            "actor.remove".to_string(),
            node_a.registry.bind_addr,
            RegistrationPriority::Immediate,
        )
        .await?;

    assert!(
        wait_for_actor(&node_b, "actor.remove", Duration::from_secs(2)).await,
        "actor should propagate before removal"
    );

    let removed = node_a.unregister("actor.remove").await?;
    assert!(removed.is_some(), "local actor should be removed");

    let removal_propagated = wait_for_condition(Duration::from_secs(20), || async {
        node_b.lookup("actor.remove").await.is_none()
    })
    .await;

    assert!(removal_propagated, "actor removal should propagate to peer");

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}
