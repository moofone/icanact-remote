use icanact_remote::{BuilderTlsBootstrap, GossipRegistryHandle, SecretKey};
use std::time::Duration;

/// Seed RED from the 2026-09-06 QA report F1: an already-connected peer
/// must still observe registrations after the serialized FullSync exceeds
/// the default 10 MiB frame limit. Bootstrap split is not this path.
#[tokio::test]
async fn accepted_registrations_converge_over_existing_connection() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let config = icanact_remote::GossipConfig {
        gossip_interval: Duration::from_millis(100),
        cleanup_interval: Duration::from_secs(3600),
        peer_supervisor_interval: Duration::from_secs(3600),
        peer_gossip_interval: None,
        connection_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        SecretKey::generate(),
        Some(config.clone()),
        BuilderTlsBootstrap,
    )
    .await
    .unwrap();
    let b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        SecretKey::generate(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await
    .unwrap();
    a.add_peer(&b.registry.peer_id)
        .await
        .connect(&b.registry.bind_addr)
        .await
        .unwrap();
    a.register_with_metadata("warm".into(), a.registry.bind_addr, vec![1])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while b.lookup("warm").await.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("small registry must converge first");
    for i in 0..81 {
        a.register_with_metadata(
            format!("large/{i}"),
            a.registry.bind_addr,
            vec![7; 128 * 1024],
        )
        .await
        .unwrap();
    }
    let names: Vec<String> = (0..81).map(|i| format!("large/{i}")).collect();
    let observed = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let mut seen = 0;
            for name in &names {
                if b.lookup(name).await.is_some() {
                    seen += 1;
                }
            }
            if seen == 81 {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(0);
    let tasks = a.registry.prepare_gossip_round().await.unwrap();
    let sizes: Vec<_> = tasks
        .iter()
        .map(|t| {
            rkyv::to_bytes::<rkyv::rancor::Error>(&t.message)
                .unwrap()
                .len()
        })
        .collect();
    eprintln!(
        "accepted=81 observed={observed} prepared_payload_sizes={sizes:?} limit={}",
        a.registry.config.max_message_size
    );
    a.shutdown().await;
    b.shutdown().await;
    assert_eq!(
        observed, 81,
        "accepted registrations must propagate without reconnecting"
    );
}
