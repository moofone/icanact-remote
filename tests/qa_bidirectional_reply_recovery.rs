//! F03: finite bidirectional inline replies must complete without a
//! write/read deadlock, and a later request must still succeed.
use icanact_remote::{BuilderTlsBootstrap, GossipRegistryHandle, SecretKey};
use std::sync::Arc;
use std::time::Duration;

struct ReplyLarge(bytes::Bytes);

impl icanact_remote::registry::ActorAskImmediateHandlerSync for ReplyLarge {
    fn handle_actor_ask_sync_immediate(
        &self,
        _: u64,
        _: u32,
        _: icanact_remote::AlignedBytes,
    ) -> icanact_remote::Result<icanact_remote::registry::AskDisposition> {
        Ok(icanact_remote::registry::AskDisposition::ImmediateBytes(
            self.0.clone(),
        ))
    }
}

async fn run_burst(default_maintenance: bool) {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut config = icanact_remote::GossipConfig {
        gossip_interval: Duration::from_secs(3600),
        cleanup_interval: Duration::from_secs(3600),
        peer_supervisor_interval: Duration::from_secs(3600),
        peer_gossip_interval: None,
        connection_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    if default_maintenance {
        config = icanact_remote::GossipConfig::default();
    }
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
    let payload = bytes::Bytes::from(vec![7; 512 * 1024]);
    a.registry
        .set_actor_ask_immediate_handler_sync(Arc::new(ReplyLarge(payload.clone())))
        .await;
    b.registry
        .set_actor_ask_immediate_handler_sync(Arc::new(ReplyLarge(payload)))
        .await;
    a.add_peer(&b.registry.peer_id)
        .await
        .connect(&b.registry.bind_addr)
        .await
        .unwrap();
    let (ab, ba) = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ab = a
                .lookup_peer(&b.registry.peer_id)
                .await
                .ok()
                .and_then(|p| p.connection_ref());
            let ba = b
                .lookup_peer(&a.registry.peer_id)
                .await
                .ok()
                .and_then(|p| p.connection_ref());
            if let (Some(ab), Some(ba)) = (ab, ba) {
                break (ab, ba);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("peers must connect");
    for conn in [&ab, &ba] {
        assert_eq!(
            conn.ask_actor_frame(
                1,
                1,
                bytes::Bytes::from_static(b"warm"),
                Duration::from_secs(2)
            )
            .await
            .unwrap()
            .len(),
            512 * 1024
        );
    }
    let futures = (0..64).flat_map(|_| {
        [&ab, &ba].into_iter().map(|conn| {
            conn.ask_actor_frame(
                1,
                1,
                bytes::Bytes::from_static(b"burst"),
                Duration::from_secs(3),
            )
        })
    });
    let replies = futures::future::join_all(futures).await;
    let success = replies.iter().filter(|r| r.is_ok()).count();
    let later = ab
        .ask_actor_frame(
            1,
            1,
            bytes::Bytes::from_static(b"after"),
            Duration::from_secs(2),
        )
        .await;
    a.shutdown().await;
    b.shutdown().await;
    assert_eq!(
        success, 128,
        "healthy peers must drain finite simultaneous response bursts"
    );
    assert!(later.is_ok(), "connection must resume after burst");
}

#[tokio::test(flavor = "current_thread")]
async fn finite_inline_reply_burst_and_followup_complete() {
    run_burst(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn finite_inline_reply_burst_with_default_maintenance() {
    run_burst(true).await;
}
