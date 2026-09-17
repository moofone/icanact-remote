mod common;

use common::{DynError, TlsHandle, connect_bidirectional, create_tls_node_with_keypair};
use icanact_remote::lifecycle::TransportTestHelperEvent;
use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PeerId, TransportDirection,
    TransportLifecycleEvent, TransportLifecycleRecorderGuard,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

const EVIDENCE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Gate {
    state: Arc<(Mutex<bool>, Condvar)>,
}

impl Gate {
    fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn wait(&self) {
        let (lock, wake) = &*self.state;
        let mut open = lock.lock().expect("gate mutex poisoned");
        while !*open {
            open = wake.wait(open).expect("gate mutex poisoned");
        }
    }

    fn open(&self) {
        let (lock, wake) = &*self.state;
        *lock.lock().expect("gate mutex poisoned") = true;
        wake.notify_all();
    }
}

fn event_sequence(event: &TransportTestHelperEvent) -> Option<u64> {
    match event {
        TransportTestHelperEvent::PublicationCommitted { sequence, .. }
        | TransportTestHelperEvent::PreRemark { sequence, .. }
        | TransportTestHelperEvent::MarkConnected { sequence, .. }
        | TransportTestHelperEvent::MarkFailed { sequence, .. }
        | TransportTestHelperEvent::TeardownAttempt { sequence, .. } => Some(*sequence),
        _ => None,
    }
}

fn publication_for(
    event: &TransportTestHelperEvent,
    peer: &PeerId,
    addr: std::net::SocketAddr,
    instance_id: Option<u64>,
) -> bool {
    matches!(
        event,
        TransportTestHelperEvent::PublicationCommitted {
            peer: event_peer,
            addr: event_addr,
            instance_id: event_instance,
            ..
        } if event_peer == peer && *event_addr == addr && instance_id.is_none_or(|id| id == *event_instance)
    )
}

fn pre_remark_for(
    event: &TransportTestHelperEvent,
    peer: &PeerId,
    addr: std::net::SocketAddr,
    instance_id: u64,
) -> bool {
    matches!(
        event,
        TransportTestHelperEvent::PreRemark {
            peer: event_peer,
            addr: event_addr,
            instance_id: event_instance,
            ..
        } if event_peer == peer && *event_addr == addr && *event_instance == instance_id
    )
}

fn mark_connected_for(
    event: &TransportTestHelperEvent,
    peer: &PeerId,
    addr: std::net::SocketAddr,
    instance_id: u64,
) -> bool {
    matches!(
        event,
        TransportTestHelperEvent::MarkConnected {
            peer: Some(event_peer),
            addr: event_addr,
            instance_id: Some(event_instance),
            require_live: true,
            ..
        } if event_peer == peer && *event_addr == addr && *event_instance == instance_id
    )
}

async fn next_event<F>(
    events: &mut UnboundedReceiver<TransportTestHelperEvent>,
    mut matches: F,
) -> TransportTestHelperEvent
where
    F: FnMut(&TransportTestHelperEvent) -> bool,
{
    tokio::time::timeout(EVIDENCE_TIMEOUT, async {
        loop {
            let event = events
                .recv()
                .await
                .expect("lifecycle recorder channel closed");
            if matches(&event) {
                return event;
            }
        }
    })
    .await
    .expect("ordered lifecycle evidence event did not arrive")
}

async fn node_at(
    addr: std::net::SocketAddr,
    key_pair: KeyPair,
    config: GossipConfig,
) -> Result<TlsHandle, DynError> {
    icanact_remote::tls::ensure_crypto_provider();
    Ok(GossipRegistryHandle::new_with_transport_stack(
        addr,
        key_pair.to_secret_key(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await?)
}

fn evidence_events(
    events: &Arc<Mutex<Vec<TransportTestHelperEvent>>>,
) -> Vec<TransportTestHelperEvent> {
    events.lock().expect("event log mutex poisoned").clone()
}

async fn peer_failures(node: &TlsHandle, addr: std::net::SocketAddr) -> usize {
    node.registry
        .gossip_state
        .lock()
        .await
        .peers
        .get(&addr)
        .map(|peer| peer.failures)
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_lifecycle_evidence_proves_publication_and_stale_teardown_fencing()
-> Result<(), DynError> {
    // Keep the real setup's normal 10-second bound and default retry/parallel
    // behavior. The gates below replace arbitrary sleeps; they do not change
    // transport policy or serialize the target binary.
    let config = GossipConfig {
        connection_timeout: EVIDENCE_TIMEOUT,
        response_timeout: EVIDENCE_TIMEOUT,
        ..Default::default()
    };
    let key_a = KeyPair::new_for_testing("qa-r1-ordered-evidence-a");
    let key_b = KeyPair::new_for_testing("qa-r1-ordered-evidence-b");
    let node_a = create_tls_node_with_keypair(key_a, config.clone()).await?;
    let node_b = create_tls_node_with_keypair(key_b.clone(), config.clone()).await?;
    let addr_a = node_a.registry.bind_addr;
    let addr_b = node_b.registry.bind_addr;
    let peer_a = node_a.registry.peer_id.clone();
    let peer_b = node_b.registry.peer_id.clone();

    let (event_sender, mut events) = unbounded_channel::<TransportTestHelperEvent>();
    let recorded = Arc::new(Mutex::new(Vec::<TransportTestHelperEvent>::new()));
    let publication_gate = Gate::new();
    let publication_once = Arc::new(AtomicBool::new(true));
    let stale_gate = Gate::new();
    let stale_gate_enabled = Arc::new(AtomicBool::new(false));
    let stale_once = Arc::new(AtomicBool::new(false));
    let recorder_events = Arc::clone(&recorded);
    let recorder_sender = event_sender.clone();
    let recorder_publication_gate = publication_gate.clone();
    let recorder_publication_once = Arc::clone(&publication_once);
    let recorder_stale_gate = stale_gate.clone();
    let recorder_stale_enabled = Arc::clone(&stale_gate_enabled);
    let recorder_stale_once = Arc::clone(&stale_once);
    let recorder_peer_b = peer_b.clone();
    let _guard =
        TransportLifecycleRecorderGuard::install(Arc::new(|_event: TransportLifecycleEvent| {}));
    _guard.install_test_helper_recorder(Arc::new(move |event| {
        recorder_events
            .lock()
            .expect("event log mutex poisoned")
            .push(event.clone());
        recorder_sender
            .send(event.clone())
            .expect("lifecycle recorder channel closed");

        if matches!(event, TransportTestHelperEvent::PublicationCommitted { .. })
            && recorder_publication_once.swap(false, Ordering::AcqRel)
        {
            recorder_publication_gate.wait();
        }
        if let TransportTestHelperEvent::TeardownAttempt { peer, addr, .. } = &event
            && *peer == recorder_peer_b
            && *addr == addr_b
            && recorder_stale_enabled.load(Ordering::Acquire)
            && recorder_stale_once.swap(true, Ordering::AcqRel) == false
        {
            recorder_stale_gate.wait();
        }
    }));

    // Run the real bidirectional setup concurrently with the recorder wait.
    // The publication callback blocks before returning, so no re-mark can be
    // observed until this test explicitly releases the publication gate.
    let mut setup = Box::pin(connect_bidirectional(&node_a, &node_b));
    let first_publication = tokio::time::timeout(EVIDENCE_TIMEOUT, async {
        loop {
            tokio::select! {
                result = &mut setup => panic!("setup completed before its publication gate: {result:?}"),
                event = events.recv() => {
                    let event = event.expect("lifecycle recorder channel closed");
                    if let TransportTestHelperEvent::PublicationCommitted { .. } = event {
                        break event;
                    }
                }
            }
        }
    })
    .await
    .expect("initial publication event did not arrive")
    .clone();
    let (initial_peer, initial_addr, initial_instance, initial_publication_sequence) =
        match first_publication {
            TransportTestHelperEvent::PublicationCommitted {
                peer,
                addr,
                instance_id,
                sequence,
                ..
            } => (peer, addr, instance_id, sequence),
            _ => unreachable!(),
        };
    let before_release = evidence_events(&recorded);
    assert!(
        !before_release.iter().any(|event| pre_remark_for(
            event,
            &initial_peer,
            initial_addr,
            initial_instance
        )),
        "pre-re-mark cannot precede the gated completed-publication event"
    );
    publication_gate.open();
    setup.await?;

    let initial_pre_remark = next_event(&mut events, |event| {
        pre_remark_for(event, &initial_peer, initial_addr, initial_instance)
    })
    .await;
    let initial_mark = next_event(&mut events, |event| {
        mark_connected_for(event, &initial_peer, initial_addr, initial_instance)
    })
    .await;
    assert!(
        initial_publication_sequence < event_sequence(&initial_pre_remark).unwrap()
            && event_sequence(&initial_pre_remark).unwrap()
                < event_sequence(&initial_mark).unwrap(),
        "publication -> pre-re-mark -> mark-connected order must be monotonic"
    );

    assert!(
        common::wait_for_condition(EVIDENCE_TIMEOUT, || async {
            node_a
                .client()
                .current_peer_connection_instance(&peer_b)
                .is_some()
        })
        .await,
        "A must expose the initial current B instance"
    );
    let old_instance = node_a
        .client()
        .current_peer_connection_instance(&peer_b)
        .expect("initial current B instance disappeared");
    stale_gate_enabled.store(true, Ordering::Release);
    let failure_registry = node_a.registry.clone();
    let failure_task = tokio::spawn(async move {
        failure_registry
            .handle_peer_connection_failure(addr_b, Some(old_instance))
            .await
    });
    let stale_teardown = next_event(&mut events, |event| {
        matches!(
            event,
            TransportTestHelperEvent::TeardownAttempt {
                peer,
                addr,
                instance_id,
                ..
            } if *peer == peer_b && *addr == addr_b && *instance_id == old_instance
        )
    })
    .await;
    let stale_teardown_sequence = event_sequence(&stale_teardown).unwrap();

    // Free B's listening address only after the failure path is held
    // immediately before its instance CAS teardown. The replacement's own
    // publication/re-mark events—not sequential snapshots—prove the causal
    // interleaving under test.
    node_b.shutdown().await;
    let replacement_b = node_at(addr_b, key_b, config).await?;
    replacement_b
        .registry
        .configure_peer(peer_a.clone(), addr_a)
        .await;
    replacement_b
        .add_peer(&peer_a)
        .await
        .connect(&addr_a)
        .await?;

    let replacement_publication = next_event(&mut events, |event| {
        publication_for(event, &peer_b, addr_b, None)
            && matches!(
                event,
                TransportTestHelperEvent::PublicationCommitted { instance_id, .. }
                    if *instance_id != old_instance
            )
    })
    .await;
    let (replacement_instance, replacement_publication_sequence) = match replacement_publication {
        TransportTestHelperEvent::PublicationCommitted {
            instance_id,
            sequence,
            direction: TransportDirection::Inbound,
            ..
        } => (instance_id, sequence),
        other => panic!("replacement publication was not an inbound A-side commit: {other:?}"),
    };
    let replacement_pre_remark = next_event(&mut events, |event| {
        pre_remark_for(event, &peer_b, addr_b, replacement_instance)
    })
    .await;
    let replacement_mark = next_event(&mut events, |event| {
        mark_connected_for(event, &peer_b, addr_b, replacement_instance)
    })
    .await;
    assert!(
        replacement_publication_sequence < event_sequence(&replacement_pre_remark).unwrap()
            && event_sequence(&replacement_pre_remark).unwrap()
                < event_sequence(&replacement_mark).unwrap()
            && stale_teardown_sequence < replacement_publication_sequence,
        "replacement must publish and re-mark after the stale teardown gate, in order"
    );
    assert_ne!(old_instance, replacement_instance);

    // The stale handler is now forced to revalidate its instance CAS. It must
    // decline the replacement and avoid applying failure state.
    stale_gate.open();
    failure_task
        .await
        .expect("stale failure task panicked")
        .expect("stale failure task failed");
    let stale_failure_mark = next_event(&mut events, |event| {
        matches!(
            event,
            TransportTestHelperEvent::MarkFailed {
                peer: Some(peer),
                addr,
                instance_id: Some(instance_id),
                applied: false,
                ..
            } if *peer == peer_b && *addr == addr_b && *instance_id == old_instance
        )
    })
    .await;
    let stale_failure_sequence = event_sequence(&stale_failure_mark).unwrap();
    assert!(stale_failure_sequence > replacement_publication_sequence);

    assert!(
        common::wait_for_condition(EVIDENCE_TIMEOUT, || async {
            node_a.client().current_peer_connection_instance(&peer_b) == Some(replacement_instance)
                && replacement_b
                    .client()
                    .lookup_connected_peer(&peer_a)
                    .is_some()
        })
        .await,
        "replacement must remain the current converged connection"
    );
    assert_eq!(
        node_a.client().current_peer_connection_instance(&peer_b),
        Some(replacement_instance),
        "final current instance must be the replacement, never the stale instance"
    );

    replacement_b.shutdown().await;
    node_a.shutdown().await;
    drop(event_sender);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_cas_tail_does_not_fence_replacement() -> Result<(), DynError> {
    let config = GossipConfig {
        connection_timeout: EVIDENCE_TIMEOUT,
        response_timeout: EVIDENCE_TIMEOUT,
        ..Default::default()
    };
    let key_a = KeyPair::new_for_testing("qa-r1-committed-accounting-a");
    let key_b = KeyPair::new_for_testing("qa-r1-committed-accounting-b");
    let node_a = create_tls_node_with_keypair(key_a, config.clone()).await?;
    let node_b = create_tls_node_with_keypair(key_b.clone(), config.clone()).await?;
    let addr_b = node_b.registry.bind_addr;
    let peer_b = node_b.registry.peer_id.clone();

    let (event_sender, mut events) = unbounded_channel::<TransportTestHelperEvent>();
    let recorded = Arc::new(Mutex::new(Vec::<TransportTestHelperEvent>::new()));
    let teardown_gate = Gate::new();
    let teardown_entered = Arc::new(AtomicBool::new(false));
    let teardown_once = Arc::new(AtomicBool::new(true));
    let failure_started = Arc::new(AtomicBool::new(false));
    let recorder_events = Arc::clone(&recorded);
    let recorder_sender = event_sender.clone();
    let recorder_gate = teardown_gate.clone();
    let recorder_entered = Arc::clone(&teardown_entered);
    let recorder_once = Arc::clone(&teardown_once);
    let recorder_failure_started = Arc::clone(&failure_started);
    let recorder_peer_b = peer_b.clone();
    let _guard = TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
        if let TransportLifecycleEvent::SocketFailurePoolTeardownComplete {
            peer: Some(peer),
            addr,
        } = &event
            && *peer == recorder_peer_b
            && *addr == addr_b
            && recorder_failure_started.load(Ordering::Acquire)
            && recorder_once.swap(false, Ordering::AcqRel)
        {
            // CAS retirement has succeeded before this event. The old
            // handler is now held before discovery/accounting, allowing a
            // replacement to publish and commit its own connected mark.
            recorder_entered.store(true, Ordering::Release);
            recorder_gate.wait();
        }
    }));
    _guard.install_test_helper_recorder(Arc::new(move |event| {
        recorder_events
            .lock()
            .expect("event log mutex poisoned")
            .push(event.clone());
        recorder_sender
            .send(event)
            .expect("lifecycle recorder channel closed");
    }));

    connect_bidirectional(&node_a, &node_b).await?;
    assert!(
        common::wait_for_condition(EVIDENCE_TIMEOUT, || async {
            node_a
                .client()
                .current_peer_connection_instance(&peer_b)
                .is_some()
        })
        .await,
        "initial current B instance did not settle"
    );
    let old_instance = node_a
        .client()
        .current_peer_connection_instance(&peer_b)
        .expect("initial current B instance");

    failure_started.store(true, Ordering::Release);
    let failure_registry = node_a.registry.clone();
    let failure_task = tokio::spawn(async move {
        failure_registry
            .handle_peer_connection_failure(addr_b, Some(old_instance))
            .await
    });
    assert!(
        common::wait_for_condition(EVIDENCE_TIMEOUT, || async {
            teardown_entered.load(Ordering::Acquire)
        })
        .await,
        "successful-CAS teardown-complete gate did not open"
    );
    let _successful_cas_teardown = next_event(&mut events, |event| {
        matches!(
            event,
            TransportTestHelperEvent::TeardownAttempt {
                peer,
                addr,
                instance_id,
                ..
            } if *peer == peer_b && *addr == addr_b && *instance_id == old_instance
        )
    })
    .await;

    node_b.shutdown().await;
    let replacement_b = node_at(addr_b, key_b, config).await?;
    connect_bidirectional(&node_a, &replacement_b).await?;

    let replacement_publication = next_event(&mut events, |event| {
        publication_for(event, &peer_b, addr_b, None)
            && matches!(
                event,
                TransportTestHelperEvent::PublicationCommitted { instance_id, .. }
                    if *instance_id != old_instance
            )
    })
    .await;
    let replacement_instance = match replacement_publication {
        TransportTestHelperEvent::PublicationCommitted { instance_id, .. } => instance_id,
        _ => unreachable!(),
    };
    let replacement_mark = next_event(&mut events, |event| {
        mark_connected_for(event, &peer_b, addr_b, replacement_instance)
    })
    .await;
    assert!(
        event_sequence(&replacement_mark).unwrap()
            > event_sequence(&replacement_publication).unwrap(),
        "replacement committed mark must follow replacement publication"
    );

    teardown_gate.open();
    failure_task
        .await
        .expect("stale failure task panicked")
        .expect("stale failure task failed");
    let old_failure = next_event(&mut events, |event| {
        matches!(
            event,
            TransportTestHelperEvent::MarkFailed {
                peer: Some(peer),
                addr,
                instance_id: Some(instance_id),
                ..
            } if *peer == peer_b && *addr == addr_b && *instance_id == old_instance
        )
    })
    .await;
    assert!(
        event_sequence(&old_failure).unwrap() > event_sequence(&replacement_mark).unwrap(),
        "old accounting decision must be observed after replacement commit"
    );
    assert!(
        matches!(
            old_failure,
            TransportTestHelperEvent::MarkFailed { applied: false, .. }
        ),
        "old successful-CAS cleanup must decline accounting after replacement commit"
    );
    assert_eq!(
        peer_failures(&node_a, addr_b).await,
        0,
        "old successful-CAS cleanup must not mark the replacement failed"
    );
    assert_eq!(
        node_a.client().current_peer_connection_instance(&peer_b),
        Some(replacement_instance),
        "replacement remains current after old cleanup release"
    );

    replacement_b.shutdown().await;
    node_a.shutdown().await;
    drop(event_sender);
    Ok(())
}
