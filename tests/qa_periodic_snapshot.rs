use icanact_remote::{BuilderTlsBootstrap, GossipError, GossipRegistryHandle, SecretKey};
use std::time::Duration;

/// Seed RED from the 2026-09-06 QA report F1 / 2026-09-07 Q6: compact
/// snapshot admission rejects new names once the frame budget is full.
/// Every *accepted* registration must still converge over the existing
/// connection; rejected names must not mutate either registry.
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

    let mut accepted = Vec::new();
    let mut rejected = None;
    for i in 0..81 {
        let name = format!("large/{i}");
        match a
            .register_with_metadata(name.clone(), a.registry.bind_addr, vec![7; 128 * 1024])
            .await
        {
            Ok(()) => accepted.push(name),
            Err(err) => {
                rejected = Some((name, err));
                break;
            }
        }
    }
    let (rejected_name, rejected_err) =
        rejected.expect("compact snapshot admission must reject once the frame budget is full");
    assert!(
        matches!(rejected_err, GossipError::MessageTooLarge { .. }),
        "overflow must be MessageTooLarge, got {rejected_err:?}"
    );
    assert!(
        a.lookup(&rejected_name).await.is_none(),
        "rejected name must not mutate the local registry"
    );
    assert!(
        !accepted.is_empty(),
        "admission must accept records that still fit the compact snapshot budget"
    );

    let observed = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let mut seen = 0;
            for name in &accepted {
                if b.lookup(name).await.is_some() {
                    seen += 1;
                }
            }
            if seen == accepted.len() {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(0);
    assert!(
        b.lookup(&rejected_name).await.is_none(),
        "rejected name must not appear on the peer"
    );
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
        "accepted={} observed={observed} rejected={rejected_name} prepared_payload_sizes={sizes:?} limit={}",
        accepted.len(),
        a.registry.config.max_message_size
    );
    a.shutdown().await;
    b.shutdown().await;
    assert_eq!(
        observed,
        accepted.len(),
        "every accepted registration must propagate without reconnecting"
    );
}
