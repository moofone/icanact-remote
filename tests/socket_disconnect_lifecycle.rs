use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Once;
use std::time::{Duration, Instant};

use icanact_remote::registry::PeerDisconnectHandler;
use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, PeerId};
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    });
}

#[derive(Clone, Default)]
struct DisconnectRecorder {
    events: Arc<Mutex<Vec<(SocketAddr, Option<PeerId>)>>>,
    notify: Arc<Notify>,
}

impl DisconnectRecorder {
    async fn wait_for_event_count(&self, expected: usize, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self.events.lock().await.len() >= expected {
                return;
            }
            if Instant::now() >= deadline {
                let events = self.events.lock().await.clone();
                panic!(
                    "timed out waiting for {expected} disconnect events, saw {}: {:?}",
                    events.len(),
                    events
                );
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = sleep(Duration::from_millis(20)) => {}
            }
        }
    }

    async fn snapshot(&self) -> Vec<(SocketAddr, Option<PeerId>)> {
        self.events.lock().await.clone()
    }
}

impl PeerDisconnectHandler for DisconnectRecorder {
    fn handle_peer_disconnect(
        &self,
        peer_addr: SocketAddr,
        peer_id: Option<PeerId>,
    ) -> futures::future::BoxFuture<'_, ()> {
        let events = Arc::clone(&self.events);
        let notify = Arc::clone(&self.notify);
        Box::pin(async move {
            events.lock().await.push((peer_addr, peer_id));
            notify.notify_waiters();
        })
    }
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
        sleep(Duration::from_millis(25)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_disconnect_emits_peer_disconnect_events_on_both_nodes() -> icanact_remote::Result<()>
{
    init_crypto();

    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        connection_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        ..Default::default()
    };

    let key_a = KeyPair::new_for_testing("socket-lifecycle-a");
    let key_b = KeyPair::new_for_testing("socket-lifecycle-b");
    let peer_id_a = key_a.peer_id();
    let peer_id_b = key_b.peer_id();

    let handle_a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_a.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    let handle_b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_b.to_secret_key(),
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    let recorder_a = DisconnectRecorder::default();
    let recorder_b = DisconnectRecorder::default();
    handle_a
        .registry
        .set_peer_disconnect_handler(Arc::new(recorder_a.clone()))
        .await;
    handle_b
        .registry
        .set_peer_disconnect_handler(Arc::new(recorder_b.clone()))
        .await;

    handle_a
        .registry
        .configure_peer(peer_id_b.clone(), handle_b.registry.bind_addr)
        .await;
    handle_b
        .registry
        .configure_peer(peer_id_a.clone(), handle_a.registry.bind_addr)
        .await;

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&handle_b.registry.bind_addr).await?;

    assert!(
        wait_for_active_peers(&handle_a, 1, Duration::from_secs(5)).await,
        "node A should observe one active peer before disconnect"
    );
    assert!(
        wait_for_active_peers(&handle_b, 1, Duration::from_secs(5)).await,
        "node B should observe one active peer before disconnect"
    );

    peer_b.disconnect().await?;

    recorder_a
        .wait_for_event_count(1, Duration::from_secs(5))
        .await;
    recorder_b
        .wait_for_event_count(1, Duration::from_secs(5))
        .await;

    let events_a = recorder_a.snapshot().await;
    let events_b = recorder_b.snapshot().await;

    assert!(
        events_a
            .iter()
            .any(|(addr, peer_id)| *addr == handle_b.registry.bind_addr
                && peer_id.as_ref() == Some(&peer_id_b)),
        "initiating node should receive a disconnect event for peer B, saw {:?}",
        events_a
    );
    assert!(
        events_b
            .iter()
            .any(|(_, peer_id)| peer_id.as_ref() == Some(&peer_id_a)),
        "remote node should receive a disconnect event tagged with peer A's identity, saw {:?}",
        events_b
    );

    assert!(
        wait_for_active_peers(&handle_a, 0, Duration::from_secs(5)).await,
        "node A should clear the active peer after disconnect"
    );

    handle_a.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}
