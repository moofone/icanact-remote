mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::{TlsHandle, create_tls_node, wait_for_condition};
use icanact_remote::registry::{
    ActorAskHandlerSync, ActorMessageFuture, ActorMessageHandler, AskDisposition,
};
use icanact_remote::{
    AskContext, AskForwardObserver, AskForwarder, GossipConfig, RemoteConnection, Result,
};

const ACTOR_ID: u64 = 41;
const TYPE_HASH: u32 = 0xF04D_0001;

#[derive(Default)]
struct ForwardObserver {
    successes: AtomicUsize,
    errors: AtomicUsize,
}

impl AskForwardObserver for ForwardObserver {
    fn record_success(&self) {
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

struct DownstreamHandler;

impl ActorMessageHandler for DownstreamHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        _correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        Box::pin(async move {
            if payload.as_ref() == b"slow" {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            let mut response = b"downstream:".to_vec();
            response.extend_from_slice(payload.as_ref());
            Ok(Some(response.into()))
        })
    }
}

struct ForwardingHandler {
    forwarder: AskForwarder,
    destination: RemoteConnection,
}

impl ActorAskHandlerSync for ForwardingHandler {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        context: AskContext<'_>,
    ) -> Result<AskDisposition> {
        let timeout = if payload.as_ref() == b"slow" {
            Duration::from_millis(40)
        } else {
            Duration::from_millis(500)
        };
        self.forwarder.try_forward_actor_ask_with_timeout(
            self.destination.clone(),
            actor_id,
            type_hash,
            Bytes::copy_from_slice(payload.as_ref()),
            timeout,
            context.responder(),
            Bytes::from_static(b"forward-timeout"),
            Bytes::from_static(b"forward-error"),
        )?;
        Ok(AskDisposition::Deferred)
    }
}

async fn connect_pair(a: &TlsHandle, b: &TlsHandle) {
    if a.registry.should_keep_connection(&b.registry.peer_id, true) {
        a.add_peer(&b.registry.peer_id)
            .await
            .connect(&b.registry.bind_addr)
            .await
            .expect("connect preferred peer");
    } else {
        b.add_peer(&a.registry.peer_id)
            .await
            .connect(&a.registry.bind_addr)
            .await
            .expect("connect preferred peer");
    }

    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            a.lookup_peer(&b.registry.peer_id)
                .await
                .ok()
                .and_then(|peer| peer.connection_ref())
                .is_some()
                && b.lookup_peer(&a.registry.peer_id)
                    .await
                    .ok()
                    .and_then(|peer| peer.connection_ref())
                    .is_some()
        })
        .await,
        "both peers must publish the established connection"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn forwarded_ask_maps_success_and_timeout_then_recovers() {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(3_600),
        ..Default::default()
    };
    let caller = create_tls_node(config.clone()).await.expect("caller node");
    let gateway = create_tls_node(config.clone()).await.expect("gateway node");
    let downstream = create_tls_node(config).await.expect("downstream node");

    downstream
        .registry
        .set_actor_message_handler(Arc::new(DownstreamHandler))
        .await;
    connect_pair(&gateway, &downstream).await;

    let downstream_connection = gateway
        .lookup_peer(&downstream.registry.peer_id)
        .await
        .expect("gateway lookup downstream")
        .connection_ref()
        .expect("gateway-to-downstream connection");
    let observer = Arc::new(ForwardObserver::default());
    let observer_trait: Arc<dyn AskForwardObserver> = observer.clone();
    let forwarder = AskForwarder::new_with_observer(1, 128, Some(observer_trait));
    gateway
        .registry
        .set_actor_ask_handler_sync(Arc::new(ForwardingHandler {
            forwarder,
            destination: downstream_connection,
        }))
        .await;

    connect_pair(&caller, &gateway).await;
    let gateway_connection = caller
        .lookup_peer(&gateway.registry.peer_id)
        .await
        .expect("caller lookup gateway")
        .connection_ref()
        .expect("caller-to-gateway connection");

    let first = gateway_connection
        .ask_actor_frame(
            ACTOR_ID,
            TYPE_HASH,
            Bytes::from_static(b"first"),
            Duration::from_secs(2),
        )
        .await
        .expect("forwarded success response");
    assert_eq!(first.as_ref(), b"downstream:first");

    let timeout = gateway_connection
        .ask_actor_frame(
            ACTOR_ID,
            TYPE_HASH,
            Bytes::from_static(b"slow"),
            Duration::from_secs(2),
        )
        .await
        .expect("forwarded timeout response");
    assert_eq!(timeout.as_ref(), b"forward-timeout");

    let after_timeout = gateway_connection
        .ask_actor_frame(
            ACTOR_ID,
            TYPE_HASH,
            Bytes::from_static(b"after-timeout"),
            Duration::from_secs(2),
        )
        .await
        .expect("forwarder must remain usable after timeout");
    assert_eq!(after_timeout.as_ref(), b"downstream:after-timeout");

    assert_eq!(observer.successes.load(Ordering::Relaxed), 2);
    assert_eq!(observer.errors.load(Ordering::Relaxed), 1);

    caller.shutdown().await;
    gateway.shutdown().await;
    downstream.shutdown().await;
}
