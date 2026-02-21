use icanact_remote::{GossipConfig, GossipRegistryHandle, RegistrationPriority, SecretKey};
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio::time::sleep;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    });
}

async fn wait_for_actor(handle: &GossipRegistryHandle, name: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if handle.lookup(name).await.is_some() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_for_active_peers(
    handle: &GossipRegistryHandle,
    expected: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if handle.stats().await.active_peers == expected {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_one_way_nat_outbound_reconnect_restores_bidirectional() -> icanact_remote::Result<()> {
    init_crypto();

    let config = GossipConfig {
        gossip_interval: Duration::from_millis(250),
        connection_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        nat_role_reconnect_enabled: true,
        ..Default::default()
    };

    let secret_b = SecretKey::generate();
    let node_id_b = secret_b.public();
    let handle_b = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_b,
        Some(config.clone()),
    )
    .await?;
    let addr_b = handle_b.registry.bind_addr;

    let secret_a = SecretKey::generate();
    let handle_a1 = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_a.clone(),
        Some(config.clone()),
    )
    .await?;

    // One-way establishment: only A dials B.
    let peer_b = handle_a1.add_peer(&node_id_b.to_peer_id()).await;
    peer_b.connect(&addr_b).await?;
    assert!(
        wait_for_active_peers(&handle_a1, 1, Duration::from_secs(5)).await,
        "A1 should establish one active peer"
    );

    handle_a1
        .register_with_priority(
            "tls_nat_actor_a".to_string(),
            "127.0.0.1:9201".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;
    handle_b
        .register_with_priority(
            "tls_nat_actor_b".to_string(),
            "127.0.0.1:9202".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;

    assert!(
        wait_for_actor(&handle_b, "tls_nat_actor_a", Duration::from_secs(8)).await,
        "B should receive updates from A over one-way-established TLS session"
    );
    assert!(
        wait_for_actor(&handle_a1, "tls_nat_actor_b", Duration::from_secs(8)).await,
        "A should receive updates from B over the same TLS session"
    );

    handle_a1.shutdown().await;
    assert!(
        wait_for_active_peers(&handle_b, 0, Duration::from_secs(8)).await,
        "B should observe A disconnect"
    );

    let handle_a2 = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_a,
        Some(config.clone()),
    )
    .await?;
    let peer_b = handle_a2.add_peer(&node_id_b.to_peer_id()).await;
    peer_b.connect(&addr_b).await?;
    assert!(
        wait_for_active_peers(&handle_a2, 1, Duration::from_secs(5)).await,
        "A2 should re-establish outbound TLS connection to B"
    );

    handle_a2
        .register_with_priority(
            "tls_nat_actor_a_after_restart".to_string(),
            "127.0.0.1:9203".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;
    handle_b
        .register_with_priority(
            "tls_nat_actor_b_after_restart".to_string(),
            "127.0.0.1:9204".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await?;

    assert!(
        wait_for_actor(
            &handle_b,
            "tls_nat_actor_a_after_restart",
            Duration::from_secs(8)
        )
        .await,
        "B should receive updates from restarted A over outbound reconnect"
    );
    assert!(
        wait_for_actor(
            &handle_a2,
            "tls_nat_actor_b_after_restart",
            Duration::from_secs(8)
        )
        .await,
        "A should receive updates from B after reconnect"
    );

    handle_a2.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}
