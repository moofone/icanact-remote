//! F5 QA: the forwarder's shutdown must drain admitted work, wake parked
//! workers, and report forced cancellation consistently.
mod common;

use bytes::Bytes;
use common::{create_tls_node, wait_for_condition};
use icanact_remote::registry::{
    ActorAskHandlerSync, ActorMessageFuture, ActorMessageHandler, ActorResponse, AskDisposition,
};
use icanact_remote::{
    AskContext, AskForwardObserver, AskForwarder, GossipConfig, RemoteConnection, Result,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ACTOR: u64 = 41;
const TYPE: u32 = 0xF04D_0001;

#[derive(Default)]
struct Counts {
    success: AtomicUsize,
    error: AtomicUsize,
}

impl AskForwardObserver for Counts {
    fn record_success(&self) {
        self.success.fetch_add(1, Ordering::SeqCst);
    }

    fn record_error(&self) {
        self.error.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReplyAfter(Duration);

impl ActorMessageHandler for ReplyAfter {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        _correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        let delay = self.0;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(Some(ActorResponse::Aligned(payload)))
        })
    }
}

struct ForwardingHandler {
    forwarder: AskForwarder,
    destination: RemoteConnection,
    submitted: Arc<AtomicUsize>,
    timeout: Option<Duration>,
}

impl ActorAskHandlerSync for ForwardingHandler {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        context: AskContext<'_>,
    ) -> Result<AskDisposition> {
        let responder = context.responder();
        let payload = Bytes::copy_from_slice(payload.as_ref());
        if let Some(timeout) = self.timeout {
            self.forwarder.try_forward_actor_ask_combined_timeout(
                self.destination.clone(),
                actor_id,
                type_hash,
                payload,
                timeout,
                responder,
                Bytes::from_static(b"f5-timeout"),
                Bytes::from_static(b"f5-error"),
            )?;
        } else {
            self.forwarder.try_forward_actor_ask_no_timeout(
                self.destination.clone(),
                actor_id,
                type_hash,
                payload,
                responder,
            )?;
        }
        self.submitted.fetch_add(1, Ordering::SeqCst);
        Ok(AskDisposition::Deferred)
    }
}

async fn connect_nodes(
    from: &icanact_remote::GossipRegistryHandle,
    to: &icanact_remote::GossipRegistryHandle,
) {
    if from
        .registry
        .should_keep_connection(&to.registry.peer_id, true)
    {
        from.add_peer(&to.registry.peer_id)
            .await
            .connect(&to.registry.bind_addr)
            .await
            .expect("connect pair");
    } else {
        to.add_peer(&from.registry.peer_id)
            .await
            .connect(&from.registry.bind_addr)
            .await
            .expect("connect pair");
    }
    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            from.lookup_peer(&to.registry.peer_id)
                .await
                .ok()
                .and_then(|peer| peer.connection_ref())
                .is_some()
                && to
                    .lookup_peer(&from.registry.peer_id)
                    .await
                    .ok()
                    .and_then(|peer| peer.connection_ref())
                    .is_some()
        })
        .await
    );
}

async fn pair() -> (
    icanact_remote::GossipRegistryHandle,
    icanact_remote::GossipRegistryHandle,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(3_600),
        ..Default::default()
    };
    let a = create_tls_node(config.clone()).await.expect("source node");
    let b = create_tls_node(config).await.expect("destination node");
    (a, b)
}

async fn connection(
    from: &icanact_remote::GossipRegistryHandle,
    to: &icanact_remote::GossipRegistryHandle,
) -> RemoteConnection {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(peer) = from.lookup_peer(&to.registry.peer_id).await
                && let Some(connection) = peer.connection_ref()
            {
                return connection;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection lookup")
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_drains_tasks_queued_before_first_worker_poll() {
    let (caller, gateway) = pair().await;
    let (downstream, sink) = pair().await;
    sink.registry
        .set_actor_message_handler(Arc::new(ReplyAfter(Duration::ZERO)))
        .await;
    connect_nodes(&gateway, &sink).await;
    let destination = connection(&gateway, &sink).await;
    let slot = Arc::new(Mutex::new(None));
    let submitted = Arc::new(AtomicUsize::new(0));

    // Constructing the worker from inside the synchronous handler places the
    // first accepted task before that worker can be polled on this executor.
    struct LazyHandler {
        slot: Arc<Mutex<Option<AskForwarder>>>,
        destination: RemoteConnection,
        submitted: Arc<AtomicUsize>,
    }
    impl ActorAskHandlerSync for LazyHandler {
        fn handle_actor_ask_sync(
            &self,
            actor_id: u64,
            type_hash: u32,
            payload: icanact_remote::AlignedBytes,
            context: AskContext<'_>,
        ) -> Result<AskDisposition> {
            let forwarder = {
                let mut slot = self.slot.lock().unwrap();
                slot.get_or_insert_with(|| AskForwarder::new(1, 128))
                    .clone()
            };
            forwarder.try_forward_actor_ask_combined_timeout(
                self.destination.clone(),
                actor_id,
                type_hash,
                Bytes::copy_from_slice(payload.as_ref()),
                Duration::ZERO,
                context.responder(),
                Bytes::from_static(b"queued-before-poll"),
                Bytes::from_static(b"f5-error"),
            )?;
            self.submitted.fetch_add(1, Ordering::SeqCst);
            Ok(AskDisposition::Deferred)
        }
    }
    gateway
        .registry
        .set_actor_ask_handler_sync(Arc::new(LazyHandler {
            slot: slot.clone(),
            destination,
            submitted: submitted.clone(),
        }))
        .await;
    connect_nodes(&caller, &gateway).await;
    let caller_connection = connection(&caller, &gateway).await;
    let ask = tokio::spawn(async move {
        caller_connection
            .ask_actor_frame(
                ACTOR,
                TYPE,
                Bytes::from_static(b"queued"),
                Duration::from_secs(2),
            )
            .await
    });
    assert!(
        wait_for_condition(Duration::from_secs(2), || async {
            submitted.load(Ordering::SeqCst) == 1
        })
        .await
    );
    let forwarder = slot
        .lock()
        .unwrap()
        .clone()
        .expect("lazy worker must be published");
    assert!(forwarder.shutdown(Duration::from_secs(1)).await.is_ok());
    assert_eq!(ask.await.unwrap().unwrap().as_ref(), b"queued-before-poll");
    caller.shutdown().await;
    gateway.shutdown().await;
    downstream.shutdown().await;
    sink.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_wakes_idle_worker_promptly() {
    let forwarder = AskForwarder::new(1, 128);
    tokio::task::yield_now().await;
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        forwarder.shutdown(Duration::from_secs(5)),
    )
    .await
    .expect("idle shutdown must not consume grace");
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_abandons_inflight_and_repeated_observers_see_timeout() {
    let (caller, gateway) = pair().await;
    let (downstream, sink) = pair().await;
    sink.registry
        .set_actor_message_handler(Arc::new(ReplyAfter(Duration::from_secs(30))))
        .await;
    connect_nodes(&gateway, &sink).await;
    let destination = connection(&gateway, &sink).await;
    let submitted = Arc::new(AtomicUsize::new(0));
    let observer = Arc::new(Counts::default());
    let forwarder = AskForwarder::new_with_observer(1, 128, Some(observer.clone()));
    gateway
        .registry
        .set_actor_ask_handler_sync(Arc::new(ForwardingHandler {
            forwarder: forwarder.clone(),
            destination,
            submitted: submitted.clone(),
            timeout: None,
        }))
        .await;
    connect_nodes(&caller, &gateway).await;
    let caller_connection = connection(&caller, &gateway).await;
    let ask = tokio::spawn(async move {
        caller_connection
            .ask_actor_frame(
                ACTOR,
                TYPE,
                Bytes::from_static(b"inflight"),
                Duration::from_secs(2),
            )
            .await
    });
    assert!(
        wait_for_condition(Duration::from_secs(2), || async {
            submitted.load(Ordering::SeqCst) == 1
        })
        .await
    );
    assert!(matches!(
        forwarder.shutdown(Duration::ZERO).await,
        Err(icanact_remote::GossipError::Timeout)
    ));
    assert!(matches!(
        forwarder.shutdown(Duration::from_secs(1)).await,
        Err(icanact_remote::GossipError::Timeout)
    ));
    let _ = ask.await;
    assert_eq!(observer.error.load(Ordering::SeqCst), 1);
    caller.shutdown().await;
    gateway.shutdown().await;
    downstream.shutdown().await;
    sink.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_drains_waiting_and_inflight_with_observer_accounting() {
    let (caller, gateway) = pair().await;
    let (downstream, sink) = pair().await;
    sink.registry
        .set_actor_message_handler(Arc::new(ReplyAfter(Duration::from_millis(5))))
        .await;
    connect_nodes(&gateway, &sink).await;
    let destination = connection(&gateway, &sink).await;
    let submitted = Arc::new(AtomicUsize::new(0));
    let observer = Arc::new(Counts::default());
    let forwarder = AskForwarder::new_with_observer(1, 128, Some(observer.clone()));
    gateway
        .registry
        .set_actor_ask_handler_sync(Arc::new(ForwardingHandler {
            forwarder: forwarder.clone(),
            destination,
            submitted: submitted.clone(),
            timeout: Some(Duration::from_secs(2)),
        }))
        .await;
    connect_nodes(&caller, &gateway).await;
    let caller_connection = connection(&caller, &gateway).await;
    let mut asks = Vec::new();
    for _ in 0..24 {
        let connection = caller_connection.clone();
        asks.push(tokio::spawn(async move {
            connection
                .ask_actor_frame(
                    ACTOR,
                    TYPE,
                    Bytes::from_static(b"drain"),
                    Duration::from_secs(3),
                )
                .await
        }));
    }
    assert!(
        wait_for_condition(Duration::from_secs(2), || async {
            submitted.load(Ordering::SeqCst) == 24
        })
        .await
    );
    assert!(forwarder.shutdown(Duration::from_secs(3)).await.is_ok());
    for ask in asks {
        assert_eq!(ask.await.unwrap().unwrap().as_ref(), b"drain");
    }
    assert_eq!(observer.success.load(Ordering::SeqCst), 24);
    assert_eq!(observer.error.load(Ordering::SeqCst), 0);
    caller.shutdown().await;
    gateway.shutdown().await;
    downstream.shutdown().await;
    sink.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_idle_shutdown_observers_share_reclamation() {
    let forwarder = AskForwarder::new(1, 128);
    let first = tokio::spawn({
        let forwarder = forwarder.clone();
        async move { forwarder.shutdown(Duration::ZERO).await }
    });
    let second = tokio::spawn({
        let forwarder = forwarder.clone();
        async move { forwarder.shutdown(Duration::from_secs(1)).await }
    });
    assert!(first.await.unwrap().is_ok());
    assert!(second.await.unwrap().is_ok());
}

/// Public transport coverage for the same ownership invariant exercised by
/// the deterministic cfg(test) module hooks: accepted asks span the worker's
/// queued, waiting, and in-flight sets when forced shutdown starts, and every
/// one receives exactly one terminal observer outcome.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_reconciles_queued_waiting_and_inflight_public_work() {
    let (caller, gateway) = pair().await;
    let (downstream, sink) = pair().await;
    sink.registry
        .set_actor_message_handler(Arc::new(ReplyAfter(Duration::from_secs(30))))
        .await;
    connect_nodes(&gateway, &sink).await;
    let destination = connection(&gateway, &sink).await;
    let submitted = Arc::new(AtomicUsize::new(0));
    let observer = Arc::new(Counts::default());
    let forwarder = AskForwarder::new_with_observer(1, 128, Some(observer.clone()));
    gateway
        .registry
        .set_actor_ask_handler_sync(Arc::new(ForwardingHandler {
            forwarder: forwarder.clone(),
            destination,
            submitted: submitted.clone(),
            timeout: None,
        }))
        .await;
    connect_nodes(&caller, &gateway).await;
    let caller_connection = connection(&caller, &gateway).await;
    let mut asks = Vec::new();
    for _ in 0..20 {
        let connection = caller_connection.clone();
        asks.push(tokio::spawn(async move {
            connection
                .ask_actor_frame(
                    ACTOR,
                    TYPE,
                    Bytes::from_static(b"ownership"),
                    Duration::from_secs(2),
                )
                .await
        }));
    }
    assert!(
        wait_for_condition(Duration::from_secs(2), || async {
            submitted.load(Ordering::SeqCst) == 20
        })
        .await
    );
    assert!(matches!(
        forwarder.shutdown(Duration::ZERO).await,
        Err(icanact_remote::GossipError::Timeout)
    ));
    for ask in asks {
        assert!(ask.await.unwrap().is_err());
    }
    assert_eq!(observer.success.load(Ordering::SeqCst), 0);
    assert_eq!(observer.error.load(Ordering::SeqCst), 20);

    caller.shutdown().await;
    gateway.shutdown().await;
    downstream.shutdown().await;
    sink.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_closed_receiver_does_not_spin_while_peer_progresses() {
    let (caller, gateway) = pair().await;
    let (downstream, sink) = pair().await;
    sink.registry
        .set_actor_message_handler(Arc::new(ReplyAfter(Duration::from_millis(20))))
        .await;
    connect_nodes(&gateway, &sink).await;
    let destination = connection(&gateway, &sink).await;
    let submitted = Arc::new(AtomicUsize::new(0));
    let forwarder = AskForwarder::new(1, 128);
    gateway
        .registry
        .set_actor_ask_handler_sync(Arc::new(ForwardingHandler {
            forwarder: forwarder.clone(),
            destination,
            submitted: submitted.clone(),
            timeout: Some(Duration::from_secs(2)),
        }))
        .await;
    connect_nodes(&caller, &gateway).await;
    let caller_connection = connection(&caller, &gateway).await;
    let ask = tokio::spawn(async move {
        caller_connection
            .ask_actor_frame(
                ACTOR,
                TYPE,
                Bytes::from_static(b"progress"),
                Duration::from_secs(3),
            )
            .await
    });
    assert!(
        wait_for_condition(Duration::from_secs(2), || async {
            submitted.load(Ordering::SeqCst) == 1
        })
        .await
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            forwarder.shutdown(Duration::from_secs(1)),
        )
        .await
        .expect("closed receiver must not spin")
        .is_ok()
    );
    assert_eq!(ask.await.unwrap().unwrap().as_ref(), b"progress");
    caller.shutdown().await;
    gateway.shutdown().await;
    downstream.shutdown().await;
    sink.shutdown().await;
}
