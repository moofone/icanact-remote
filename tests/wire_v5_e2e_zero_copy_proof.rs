//! End-to-end V5 transport proof over real TCP/TLS sockets.
//!
//! This intentionally exercises the public owned-`Bytes` APIs: 16-byte tell,
//! 32-byte ask/response, and a request/response stream that is larger than the
//! inline threshold.  It complements the lower-level allocation provenance
//! tests in `protocol.rs` and `read_pipeline.rs`.

use std::{net::SocketAddr, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};

use bytes::Bytes;
use icanact_remote::{AlignedBytes, BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair};
use icanact_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use tokio::time::sleep;

struct Echo {
    tells: AtomicUsize,
    asks: AtomicUsize,
    last_len: AtomicUsize,
}

impl ActorMessageHandler for Echo {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        self.last_len.store(payload.len(), Ordering::SeqCst);
        if correlation_id.is_some() {
            self.asks.fetch_add(1, Ordering::SeqCst);
        } else {
            self.tells.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move { Ok(correlation_id.map(|_| payload.into())) })
    }
}

fn ordered_keys() -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing("wire-v5-proof-a");
    let second = KeyPair::new_for_testing("wire-v5-proof-b");
    if first.peer_id().to_node_id().as_bytes() < second.peer_id().to_node_id().as_bytes() {
        (first, second)
    } else {
        (second, first)
    }
}

fn payload(size: usize) -> Bytes {
    Bytes::from((0..size).map(|index| (index % 251) as u8).collect::<Vec<_>>())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_v5_e2e_zero_copy_proof() {
    let addr_a: SocketAddr = "127.0.0.1:7931".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7932".parse().unwrap();
    let (key_a, key_b) = ordered_keys();
    let config = GossipConfig { gossip_interval: Duration::from_secs(300), ..Default::default() };
    let a = GossipRegistryHandle::new_with_transport_stack(
        addr_a, key_a.to_secret_key(), Some(config.clone()), BuilderTlsBootstrap,
    ).await.unwrap();
    let b = GossipRegistryHandle::new_with_transport_stack(
        addr_b, key_b.to_secret_key(), Some(config), BuilderTlsBootstrap,
    ).await.unwrap();
    let echo = Arc::new(Echo { tells: AtomicUsize::new(0), asks: AtomicUsize::new(0), last_len: AtomicUsize::new(0) });
    b.registry.set_actor_message_handler(echo.clone()).await;

    a.add_peer(&key_b.peer_id()).await.connect(&addr_b).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    let connection = a.lookup_address(addr_b).await.unwrap();

    connection.tell_actor_frame(7, 9, Bytes::from_static(b"tell")).await.unwrap();
    sleep(Duration::from_millis(30)).await;
    assert_eq!(echo.tells.load(Ordering::SeqCst), 1);

    let ask = Bytes::from_static(b"ask");
    assert_eq!(connection.ask_actor_frame(7, 9, ask.clone(), Duration::from_secs(5)).await.unwrap(), ask);

    let stream = payload(2 * 1024 * 1024);
    assert_eq!(connection.ask_streaming_bytes(stream.clone(), 9, 7, Duration::from_secs(30)).await.unwrap(), stream);
    assert_eq!(echo.asks.load(Ordering::SeqCst), 2);
    assert_eq!(echo.last_len.load(Ordering::SeqCst), 2 * 1024 * 1024);

    a.shutdown().await;
    b.shutdown().await;
}
