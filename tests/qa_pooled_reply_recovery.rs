//! QA-01: finite bidirectional pooled inline replies must complete without a
//! write/read deadlock, and a later request must still succeed.
use icanact_remote::{BuilderTlsBootstrap, GossipRegistryHandle, SecretKey};
use std::sync::Arc;
use std::time::Duration;

const PAYLOAD_LEN: usize = 512 * 1024;
const PREFIX: [u8; 16] = [0xA5; 16];

struct ReplyPooled {
    payload: bytes::Bytes,
    prefix: Option<[u8; 16]>,
}

impl icanact_remote::registry::ActorAskImmediateHandlerSync for ReplyPooled {
    fn handle_actor_ask_sync_immediate(
        &self,
        _: u64,
        _: u32,
        _: icanact_remote::AlignedBytes,
    ) -> icanact_remote::Result<icanact_remote::registry::AskDisposition> {
        let payload_len = self.prefix.map(|p| p.len()).unwrap_or(0) + self.payload.len();
        Ok(icanact_remote::registry::AskDisposition::ImmediatePooled {
            payload: icanact_remote::typed::PooledPayload::try_from_pooled_bytes(
                self.payload.len(),
                |v| v.extend_from_slice(&self.payload),
            )
            .expect("pooled payload fits the byte pool"),
            prefix: self.prefix,
            payload_len,
        })
    }
}

struct ReplyImmediatePooled {
    payload: bytes::Bytes,
}

impl icanact_remote::registry::ActorAskImmediateHandlerSync for ReplyImmediatePooled {
    fn handle_actor_ask_sync_immediate(
        &self,
        _: u64,
        _: u32,
        _: icanact_remote::AlignedBytes,
    ) -> icanact_remote::Result<icanact_remote::registry::AskDisposition> {
        Ok(icanact_remote::registry::AskDisposition::Immediate(
            icanact_remote::registry::ActorResponse::Pooled {
                payload: icanact_remote::typed::PooledPayload::try_from_pooled_bytes(
                    self.payload.len(),
                    |v| v.extend_from_slice(&self.payload),
                )
                .expect("pooled payload fits the byte pool"),
                prefix: None,
                payload_len: self.payload.len(),
            },
        ))
    }
}

fn assert_payload(reply: &bytes::Bytes, prefix: Option<[u8; 16]>) {
    let expected_len = prefix.map(|p| p.len()).unwrap_or(0) + PAYLOAD_LEN;
    assert_eq!(reply.len(), expected_len);
    if let Some(prefix) = prefix {
        assert_eq!(&reply[..prefix.len()], &prefix);
        assert!(reply[prefix.len()..].iter().all(|&b| b == 7));
    } else {
        assert!(reply.iter().all(|&b| b == 7));
    }
}

async fn run_burst(default_maintenance: bool, use_immediate_enum: bool, prefix: Option<[u8; 16]>) {
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
    let payload = bytes::Bytes::from(vec![7; PAYLOAD_LEN]);
    if use_immediate_enum {
        a.registry
            .set_actor_ask_immediate_handler_sync(Arc::new(ReplyImmediatePooled {
                payload: payload.clone(),
            }))
            .await;
        b.registry
            .set_actor_ask_immediate_handler_sync(Arc::new(ReplyImmediatePooled { payload }))
            .await;
    } else {
        a.registry
            .set_actor_ask_immediate_handler_sync(Arc::new(ReplyPooled {
                payload: payload.clone(),
                prefix,
            }))
            .await;
        b.registry
            .set_actor_ask_immediate_handler_sync(Arc::new(ReplyPooled { payload, prefix }))
            .await;
    }
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
        let reply = conn
            .ask_actor_frame(
                1,
                1,
                bytes::Bytes::from_static(b"warm"),
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_payload(&reply, prefix);
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
    let success = replies
        .iter()
        .filter(|r| {
            r.as_ref().is_ok_and(|payload| {
                payload.len() == prefix.map(|p| p.len()).unwrap_or(0) + PAYLOAD_LEN
            })
        })
        .count();
    for reply in replies.iter().flatten() {
        assert_payload(reply, prefix);
    }
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
        "healthy peers must drain finite simultaneous pooled response bursts"
    );
    let later = later.expect("connection must resume after burst");
    assert_payload(&later, prefix);
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_inline_reply_burst_and_followup_complete() {
    run_burst(false, false, None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_inline_reply_burst_with_default_maintenance() {
    run_burst(true, false, None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn immediate_pooled_enum_reply_burst_and_followup_complete() {
    run_burst(false, true, None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_inline_reply_burst_with_prefix() {
    run_burst(false, false, Some(PREFIX)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pooled_inline_reply_burst_multi_thread() {
    run_burst(false, false, None).await;
}
