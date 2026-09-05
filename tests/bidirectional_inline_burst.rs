use bytes::Bytes;
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse};
use icanact_remote::{
    AlignedBytes, BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, SecretKey,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct CountTells(Arc<AtomicUsize>);

impl ActorMessageHandlerSync for CountTells {
    fn handle_actor_message_sync(
        &self,
        _: u64,
        _: u32,
        _: AlignedBytes,
        _: Option<u32>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bidirectional_inline_burst_must_resume_delivery() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(3600),
        cleanup_interval: Duration::from_secs(3600),
        peer_supervisor_interval: Duration::from_secs(3600),
        peer_gossip_interval: None,
        enable_peer_discovery: false,
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
    let a_count = Arc::new(AtomicUsize::new(0));
    let b_count = Arc::new(AtomicUsize::new(0));
    a.registry
        .set_actor_message_handler_sync(Arc::new(CountTells(a_count.clone())))
        .await;
    b.registry
        .set_actor_message_handler_sync(Arc::new(CountTells(b_count.clone())))
        .await;
    a.add_peer(&b.registry.peer_id)
        .await
        .connect(&b.registry.bind_addr)
        .await
        .unwrap();
    let (ab, ba) = tokio::time::timeout(Duration::from_secs(5), async {
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
    ab.tell_actor_frame(1, 1, Bytes::from_static(b"warmup"))
        .await
        .unwrap();
    ba.tell_actor_frame(1, 1, Bytes::from_static(b"warmup"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while a_count.load(Ordering::Relaxed) < 1 || b_count.load(Ordering::Relaxed) < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("warmup must work");

    let payload = Bytes::from(vec![7; 512 * 1024]);
    let mut accepted_ab = 0;
    let mut accepted_ba = 0;
    for _ in 0..256 {
        accepted_ab += usize::from(ab.try_tell_actor_frame(1, 1, payload.clone()).is_ok());
        accepted_ba += usize::from(ba.try_tell_actor_frame(1, 1, payload.clone()).is_ok());
    }
    assert!(
        accepted_ab > 0 && accepted_ba > 0,
        "burst admitted no messages: accepted_ab={accepted_ab}, accepted_ba={accepted_ba}"
    );
    let progress = tokio::time::timeout(Duration::from_secs(12), async {
        while a_count.load(Ordering::Relaxed) < accepted_ba + 1
            || b_count.load(Ordering::Relaxed) < accepted_ab + 1
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    eprintln!(
        "accepted_ab={accepted_ab}, accepted_ba={accepted_ba}, delivered_a={}, delivered_b={}, closed_ab={}, closed_ba={}",
        a_count.load(Ordering::Relaxed),
        b_count.load(Ordering::Relaxed),
        ab.is_closed(),
        ba.is_closed()
    );
    a.shutdown().await;
    b.shutdown().await;
    assert!(
        progress.is_ok(),
        "healthy TLS peers failed to drain a finite inline burst"
    );
}
