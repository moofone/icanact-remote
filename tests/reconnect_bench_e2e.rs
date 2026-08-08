#![cfg(test)]

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use icanact_remote::tls;
use icanact_remote::{
    GossipConfig, GossipRegistryHandle, KeyPair, SecretKey,
    aligned::AlignedBytes,
    registry::{ActorMessageFuture, ActorMessageHandler, ActorResponse, PeerDisconnectHandler},
};
use tokio::sync::{Notify, watch};
use tokio::time::{sleep, timeout};

struct EchoAskHandler {
    asks: AtomicUsize,
}

impl EchoAskHandler {
    fn new() -> Self {
        Self {
            asks: AtomicUsize::new(0),
        }
    }
}

fn key_pair_ordered_for_outbound_a(seed_a: &str, seed_b: &str) -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing(seed_a);
    let second = KeyPair::new_for_testing(seed_b);
    if first
        .peer_id()
        .to_node_id()
        .as_bytes()
        .cmp(second.peer_id().to_node_id().as_bytes())
        .is_lt()
    {
        (first, second)
    } else {
        (second, first)
    }
}

impl ActorMessageHandler for EchoAskHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: AlignedBytes,
        _correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        Box::pin(async move {
            self.asks.fetch_add(1, Ordering::AcqRel);
            Ok(Some(ActorResponse::Bytes(Bytes::copy_from_slice(
                payload.as_ref(),
            ))))
        })
    }
}

struct ReconnectOnDisconnect {
    handle: Arc<GossipRegistryHandle>,
    peer_id: icanact_remote::PeerId,
    peer_addr: SocketAddr,
    current: watch::Sender<Option<icanact_remote::RemoteActorRef>>,
    disconnects: Arc<AtomicUsize>,
    reconnected: Arc<Notify>,
}

impl PeerDisconnectHandler for ReconnectOnDisconnect {
    fn handle_peer_disconnect(
        &self,
        peer_addr: SocketAddr,
        peer_id: Option<icanact_remote::PeerId>,
    ) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            tracing::info!(
                peer_addr = %peer_addr,
                peer_id = peer_id.as_ref().map(|p| p.to_hex()).unwrap_or_else(|| "none".to_string()),
                "test: PeerDisconnectHandler fired"
            );
            self.disconnects.fetch_add(1, Ordering::AcqRel);

            // Clear current handle immediately so send loop blocks rather than spamming a dead conn.
            let _ = self.current.send(None);

            // Best-effort: ensure mapping is set (restart may rebind but in this test we keep the same addr).
            let _ = self.handle
                .registry
                .configure_peer(self.peer_id.clone(), self.peer_addr)
                .await;

            // Reconnect loop.
            for attempt in 0..200 {
                match self.handle.lookup_peer(&self.peer_id).await {
                    Ok(r) => {
                        tracing::info!(attempt, "test: reconnected via lookup_peer");
                        let _ = self.current.send(Some(r));
                        self.reconnected.notify_waiters();
                        return;
                    }
                    Err(e) => {
                        tracing::debug!(attempt, error = %e, "test: reconnect attempt failed");
                        sleep(Duration::from_millis(25)).await;
                    }
                }
            }

            tracing::error!("test: reconnect attempts exhausted");
        })
    }
}

async fn start_server_at(
    bind_addr: SocketAddr,
    secret: SecretKey,
    handler: Arc<EchoAskHandler>,
) -> GossipRegistryHandle {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let h = GossipRegistryHandle::new_with_transport_stack(
        bind_addr,
        secret,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .expect("start server");
    h.registry.set_actor_message_handler(handler).await;
    h
}

#[tokio::test]
async fn reconnect_continues_ask_bench_after_server_restart() {
    tls::ensure_crypto_provider();

    // Avoid sandbox-triggered EPERM flakiness unless explicitly enabled.
    if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info,icanact_remote=info")
            .try_init();
    }

    let (client_keypair, server_keypair) =
        key_pair_ordered_for_outbound_a("reconnect_bench_client", "reconnect_bench_server");
    let server_secret = server_keypair.to_secret_key();
    let server_peer_id = server_secret.public().to_peer_id();
    let server_handler = Arc::new(EchoAskHandler::new());

    // Start server on an ephemeral port, then restart on the same port with the same identity.
    let server = start_server_at(
        "127.0.0.1:0".parse().unwrap(),
        server_secret.clone(),
        server_handler.clone(),
    )
    .await;
    let server_addr = server.registry.bind_addr;

    // Client.
    let client_secret = client_keypair.to_secret_key();
    let client = Arc::new(
        GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            client_secret,
            Some(GossipConfig {
                gossip_interval: Duration::from_secs(3600),
                ..Default::default()
            }),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .expect("start client"),
    );

    let _ = client
        .registry
        .configure_peer(server_peer_id.clone(), server_addr)
        .await;
    let initial_remote = client
        .lookup_peer(&server_peer_id)
        .await
        .expect("initial lookup_peer");

    let (tx, mut rx) =
        watch::channel::<Option<icanact_remote::RemoteActorRef>>(Some(initial_remote));
    let disconnects = Arc::new(AtomicUsize::new(0));
    let reconnected = Arc::new(Notify::new());

    client
        .registry
        .set_peer_disconnect_handler(Arc::new(ReconnectOnDisconnect {
            handle: client.clone(),
            peer_id: server_peer_id.clone(),
            peer_addr: server_addr,
            current: tx.clone(),
            disconnects: disconnects.clone(),
            reconnected: reconnected.clone(),
        }))
        .await;

    // Simple ask loop: should continue after the server restarts.
    let bench_ok = Arc::new(AtomicUsize::new(0));
    let bench_ok2 = bench_ok.clone();
    let bench = tokio::spawn(async move {
        let actor_id = 0x0000_0000_c0ff_ee00u64;
        let type_hash = 0u32;
        let mut i: u64 = 0;
        loop {
            // Drop the watch borrow guard before any await (required for Send + borrowck).
            let remote_opt = { rx.borrow().clone() };
            let remote = match remote_opt {
                Some(r) => r,
                None => {
                    rx.changed().await.expect("watch closed");
                    continue;
                }
            };

            let payload = Bytes::from(format!("ping#{i}"));
            let conn = match remote.connection_ref() {
                Some(c) => c.clone(),
                None => {
                    rx.changed().await.expect("watch closed");
                    continue;
                }
            };

            let r = conn
                .ask_actor_frame(actor_id, type_hash, payload.clone(), Duration::from_secs(2))
                .await;
            match r {
                Ok(reply) => {
                    if reply.as_ref() != format!("ping#{i}").as_bytes() {
                        return Err(format!("bad reply for i={i}: {:?}", reply));
                    }
                    bench_ok2.fetch_add(1, Ordering::AcqRel);
                    i += 1;
                    if i >= 500 {
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::info!(i, error = %e, "bench: ask failed (will wait for reconnect)");
                    // Wait for reconnect and try again.
                    rx.changed().await.expect("watch closed");
                }
            }
        }
    });

    // Let the bench make some progress.
    timeout(Duration::from_secs(3), async {
        loop {
            if bench_ok.load(Ordering::Acquire) >= 50 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bench did not progress before restart");

    // Restart server (simulate Ctrl+C / process restart).
    server.shutdown().await;
    drop(server);

    // Ensure the client observes a disconnect promptly.
    timeout(Duration::from_secs(3), async {
        loop {
            if disconnects.load(Ordering::Acquire) > 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("client did not observe peer disconnect");

    // Re-bind the server on the same addr/port with the same identity.
    let _server2 = start_server_at(server_addr, server_secret, server_handler).await;

    // Wait for reconnection.
    timeout(Duration::from_secs(5), reconnected.notified())
        .await
        .expect("did not reconnect in time");

    // Bench must complete.
    bench.await.expect("bench panicked").expect("bench failed");
    assert!(
        bench_ok.load(Ordering::Acquire) >= 500,
        "expected at least 500 successful asks"
    );
}
