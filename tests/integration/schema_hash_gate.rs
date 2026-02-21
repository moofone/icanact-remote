use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, timeout, Duration};

struct TestHandler {
    hits: Arc<AtomicUsize>,
    notify: Arc<tokio::sync::Notify>,
}

impl icanact_remote::registry::ActorMessageHandler for TestHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        _correlation_id: Option<u16>,
    ) -> icanact_remote::registry::ActorMessageFuture<'_> {
        let hits = self.hits.clone();
        let notify = self.notify.clone();
        Box::pin(async move {
            hits.fetch_add(1, Ordering::SeqCst);
            notify.notify_waiters();
            Ok(None)
        })
    }
}

#[tokio::test]
async fn schema_hash_allows_matching_headers() {
    let addr_a: SocketAddr = "127.0.0.1:9111".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9112".parse().unwrap();
    let schema_hash = 0xAABBCCDDEEFF0011u64;

    let key_a = KeyPair::new_for_testing("schema_hash_a");
    let key_b = KeyPair::new_for_testing("schema_hash_b");
    let peer_id_b = key_b.peer_id();

    let config_a = GossipConfig {
        key_pair: Some(key_a.clone()),
        schema_hash: Some(schema_hash),
        ..Default::default()
    };
    let config_b = GossipConfig {
        key_pair: Some(key_b.clone()),
        schema_hash: Some(schema_hash),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_a, Some(config_a))
        .await
        .unwrap();
    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_b, Some(config_b))
        .await
        .unwrap();

    let hits = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());
    handle_b
        .registry
        .set_actor_message_handler(Arc::new(TestHandler {
            hits: hits.clone(),
            notify: notify.clone(),
        }))
        .await;

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    let remote_ref = handle_a.lookup_peer(&peer_id_b).await.unwrap();
    let conn = remote_ref
        .connection_ref()
        .expect("connection should be established");
    conn.tell_actor_frame(1, 0xDEAD_BEEF, bytes::Bytes::from_static(b"ping"))
        .await
        .unwrap();

    timeout(Duration::from_secs(2), notify.notified())
        .await
        .expect("handler should be invoked");
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

#[tokio::test]
async fn schema_hash_rejects_missing_headers() {
    let addr_a: SocketAddr = "127.0.0.1:9113".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9114".parse().unwrap();
    let schema_hash = 0x1122334455667788u64;

    let key_a = KeyPair::new_for_testing("schema_hash_miss_a");
    let key_b = KeyPair::new_for_testing("schema_hash_miss_b");
    let peer_id_b = key_b.peer_id();

    let config_a = GossipConfig {
        key_pair: Some(key_a.clone()),
        schema_hash: None,
        ..Default::default()
    };
    let config_b = GossipConfig {
        key_pair: Some(key_b.clone()),
        schema_hash: Some(schema_hash),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_a, Some(config_a))
        .await
        .unwrap();
    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_b, Some(config_b))
        .await
        .unwrap();

    let hits = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(tokio::sync::Notify::new());
    handle_b
        .registry
        .set_actor_message_handler(Arc::new(TestHandler {
            hits: hits.clone(),
            notify: notify.clone(),
        }))
        .await;

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    let remote_ref = handle_a.lookup_peer(&peer_id_b).await.unwrap();
    let conn = remote_ref
        .connection_ref()
        .expect("connection should be established");
    conn.tell_actor_frame(2, 0xFEED_FACE, bytes::Bytes::from_static(b"ping"))
        .await
        .unwrap();

    let result = timeout(Duration::from_millis(500), notify.notified()).await;
    assert!(result.is_err(), "handler should not be invoked");
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}
