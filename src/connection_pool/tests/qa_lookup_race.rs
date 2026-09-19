struct FallbackAdoptionHookReset;

impl Drop for FallbackAdoptionHookReset {
    fn drop(&mut self) {
        set_fallback_adoption_hook(None);
    }
}

/// An unresolved same-address replacement publishes only through the address
/// index. That publication must supersede a notifier's armed claim before the
/// claim can enter the detached disconnect callback.
#[test]
fn same_address_index_publication_supersedes_armed_disconnect_claim() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let addr: SocketAddr = "127.0.0.1:60700".parse().unwrap();
    let old = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Inbound));
    let replacement = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Inbound));
    pool.index_connection_by_addr(addr, old);

    let claim = try_arm_disconnect_delivery().expect("disconnect claim must arm");
    pool.index_connection_by_addr(addr, replacement.clone());

    assert!(
        !claim.enter(),
        "same-address publication must win before an unresolved disconnect callback enters"
    );
    assert!(
        pool.get_lock_free_connection(addr)
            .is_some_and(|current| Arc::ptr_eq(&current, &replacement)),
        "address index must retain the replacement that won the publication race"
    );
}

/// The address fallback is a capture-then-adopt path. A replacement may win
/// the peer session between those two operations, so adoption must use the
/// observed empty primary slot as a compare-and-publish fence rather than an
/// unconditional store.
#[tokio::test]
async fn fallback_capture_then_replacement_cannot_be_overwritten_on_resume() {
    let _test_lock = lock_fallback_adoption_test();
    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
    let peer_id = crate::KeyPair::new_for_testing("qa-fallback-capture-race-peer").peer_id();
    let fallback_addr: SocketAddr = "127.0.0.1:60701".parse().unwrap();
    let replacement_addr: SocketAddr = "127.0.0.1:60702".parse().unwrap();
    let fallback = make_live_connection(fallback_addr, ConnectionDirection::Inbound).await;
    let replacement = make_live_connection(replacement_addr, ConnectionDirection::Inbound).await;
    let fallback = Arc::new(LockFreeConnection {
        embedded_peer_id: Some(peer_id.clone()),
        ..(*fallback).clone()
    });

    pool.set_configured_peer_addr(&peer_id, fallback_addr);
    pool.index_connection_by_addr(fallback_addr, fallback.clone());
    pool.add_addr_to_peer_id(fallback_addr, peer_id.clone());
    assert!(
        pool.peer_sessions
            .read_sync(&peer_id, |_, session| session.current_connection())
            .flatten()
            .is_none()
    );

    let captured = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let hook_captured = captured.clone();
    let hook_resume = resume.clone();
    let hook_peer = peer_id.clone();
    set_fallback_adoption_hook(Some(Arc::new(move |connection| {
        if connection.embedded_peer_id.as_ref() == Some(&hook_peer) {
            hook_captured.wait();
            hook_resume.wait();
        }
    })));
    let _hook_reset = FallbackAdoptionHookReset;

    let lookup_pool = pool.clone();
    let lookup_peer = peer_id.clone();
    let lookup = tokio::task::spawn_blocking(move || {
        lookup_pool
            .get_connection_by_peer_id(&lookup_peer)
            .expect("fallback lookup should resolve a connection")
    });

    captured.wait();
    assert!(
        pool.peer_sessions
            .read_sync(&peer_id, |_, session| session.current_connection())
            .flatten()
            .is_none(),
        "the fallback lookup must still be paused before it adopts its captured connection"
    );
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), replacement_addr, replacement.clone()));
    resume.wait();

    let resolved = lookup.await.expect("fallback lookup task must not panic");
    assert!(
        Arc::ptr_eq(&resolved, &replacement),
        "lookup must return the replacement that won the compare-and-publish race"
    );
    let current = pool
        .peer_current_connection_snapshot(&peer_id)
        .expect("replacement must remain current");
    assert!(Arc::ptr_eq(&current, &replacement));
    assert!(
        fallback.has_live_stream(),
        "the captured fallback must not be aborted by adoption"
    );

    replacement.abort_tasks();
    fallback.abort_tasks();
}

/// A fallback captured from an address alias must survive a raced unusable
/// primary-session publication. The compare-and-publish loss is recoverable:
/// clear that exact unusable occupant and retry the already validated fallback
/// instead of returning a spurious lookup miss.
#[tokio::test]
async fn fallback_adoption_retries_after_raced_unusable_session() {
    let _test_lock = lock_fallback_adoption_test();
    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
    let peer_id = crate::KeyPair::new_for_testing("qa-fallback-unusable-race-peer").peer_id();
    let fallback_addr: SocketAddr = "127.0.0.1:60703".parse().unwrap();
    let stale_addr: SocketAddr = "127.0.0.1:60704".parse().unwrap();
    let fallback = make_live_connection(fallback_addr, ConnectionDirection::Inbound).await;
    let stale = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
    let fallback = Arc::new(LockFreeConnection {
        embedded_peer_id: Some(peer_id.clone()),
        ..(*fallback).clone()
    });
    stale.set_state(ConnectionState::Disconnected);

    pool.set_configured_peer_addr(&peer_id, fallback_addr);
    pool.index_connection_by_addr(fallback_addr, fallback.clone());
    pool.add_addr_to_peer_id(fallback_addr, peer_id.clone());

    let captured = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let hook_captured = captured.clone();
    let hook_resume = resume.clone();
    let hook_peer = peer_id.clone();
    set_fallback_adoption_hook(Some(Arc::new(move |connection| {
        if connection.embedded_peer_id.as_ref() == Some(&hook_peer) {
            hook_captured.wait();
            hook_resume.wait();
        }
    })));
    let _hook_reset = FallbackAdoptionHookReset;

    let lookup_pool = pool.clone();
    let lookup_peer = peer_id.clone();
    let lookup = tokio::task::spawn_blocking(move || {
        lookup_pool
            .get_connection_by_peer_id(&lookup_peer)
            .expect("fallback lookup should recover after the unusable race")
    });

    captured.wait();
    pool.publish_current_peer_connection(&peer_id, stale.clone());
    resume.wait();

    let resolved = lookup.await.expect("fallback lookup task must not panic");
    assert!(Arc::ptr_eq(&resolved, &fallback));
    let current = pool
        .peer_current_connection_snapshot(&peer_id)
        .expect("fallback must be adopted as current");
    assert!(Arc::ptr_eq(&current, &fallback));
    assert!(fallback.has_live_stream());

    stale.abort_tasks();
    fallback.abort_tasks();
}

/// The conditional current-session cleanup must revalidate ownership at the
/// clear itself. A replacement published after the first identity check must
/// remain in both current-session indices.
#[tokio::test]
async fn conditional_clear_does_not_remove_replacement_published_after_check() {
    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
    let peer_id = crate::KeyPair::new_for_testing("qa-conditional-clear-race-peer").peer_id();
    let stale_addr: SocketAddr = "127.0.0.1:60711".parse().unwrap();
    let fresh_addr: SocketAddr = "127.0.0.1:60712".parse().unwrap();
    let stale = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
    let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, stale.clone()));

    let _guard = {
        let pool = pool.clone();
        let peer_id = peer_id.clone();
        let fresh = fresh.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::SessionRemoved {
                peer,
                reason: crate::SessionRemovalReason::CurrentConnectionCleared,
                ..
            } = &event
                && *peer == peer_id
            {
                crate::set_transport_lifecycle_recorder(None);
                pool.publish_current_peer_connection(&peer_id, fresh.clone());
            }
        }))
    };

    pool.clear_current_peer_connection_if_matches(&peer_id, &stale);

    let current = pool
        .peer_current_connection_snapshot(&peer_id)
        .expect("replacement must remain current");
    assert!(Arc::ptr_eq(&current, &fresh));
    assert!(
        pool.connections_by_peer
            .read_sync(&peer_id, |_, value| Arc::ptr_eq(value, &fresh))
            .unwrap_or(false),
        "the peer mirror must remain on the replacement"
    );

    fresh.abort_tasks();
    stale.abort_tasks();
}

/// A failed compare-and-clear must not report a removal: the replacement was
/// already current when the stale instance attempted its cleanup.
#[tokio::test]
async fn conditional_clear_failed_cas_does_not_emit_session_removed() {
    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
    let peer_id = crate::KeyPair::new_for_testing("qa-conditional-clear-failed-cas-peer")
        .peer_id();
    let stale_addr: SocketAddr = "127.0.0.1:60721".parse().unwrap();
    let fresh_addr: SocketAddr = "127.0.0.1:60722".parse().unwrap();
    let stale = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
    let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, stale.clone()));
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), fresh_addr, fresh.clone()));

    let removals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_removals = removals.clone();
    let observed_peer_id = peer_id.clone();
    let _guard = crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(
        move |event| {
            if let crate::TransportLifecycleEvent::SessionRemoved {
                peer,
                reason: crate::SessionRemovalReason::CurrentConnectionCleared,
                ..
            } = &event
                && *peer == observed_peer_id
            {
                observed_removals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        },
    ));

    pool.clear_current_peer_connection_if_matches(&peer_id, &stale);

    assert_eq!(
        removals.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a failed compare-and-clear must not report a committed removal"
    );
    let current = pool
        .peer_current_connection_snapshot(&peer_id)
        .expect("replacement must remain current");
    assert!(Arc::ptr_eq(&current, &fresh));

    fresh.abort_tasks();
    stale.abort_tasks();
}

/// Session publication commits the new current connection while holding the
/// disconnect-delivery gate, but lifecycle observers run after that gate is
/// released. A recorder is allowed to publish a replacement synchronously;
/// this must not self-deadlock on the non-reentrant delivery mutex.
#[tokio::test]
async fn publication_lifecycle_recorder_can_reenter_after_gate_release() {
    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
    let peer_id = crate::KeyPair::new_for_testing("qa-publication-recorder-reentry-peer").peer_id();
    let initial_addr: SocketAddr = "127.0.0.1:60731".parse().unwrap();
    let replacement_addr: SocketAddr = "127.0.0.1:60732".parse().unwrap();
    let initial = make_live_connection(initial_addr, ConnectionDirection::Inbound).await;
    let replacement = make_live_connection(replacement_addr, ConnectionDirection::Inbound).await;
    let pool_for_recorder = pool.clone();
    let peer_for_recorder = peer_id.clone();
    let replacement_for_recorder = replacement.clone();
    let _guard = crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(
        move |event| {
            if matches!(
                event,
                crate::TransportLifecycleEvent::SessionPublished { ref peer, .. }
                    if *peer == peer_for_recorder
            ) {
                crate::set_transport_lifecycle_recorder(None);
                pool_for_recorder.publish_current_peer_connection(
                    &peer_for_recorder,
                    replacement_for_recorder.clone(),
                );
            }
        },
    ));

    tokio::time::timeout(
        Duration::from_secs(1),
        tokio::task::spawn_blocking({
            let pool = pool.clone();
            let peer_id = peer_id.clone();
            let initial = initial.clone();
            move || pool.publish_current_peer_connection(&peer_id, initial)
        }),
    )
    .await
    .expect("publication recorder re-entry must not deadlock")
    .expect("publication task must not panic");

    let current = pool
        .peer_current_connection_snapshot(&peer_id)
        .expect("recorder must publish the replacement");
    assert!(Arc::ptr_eq(&current, &replacement));
    initial.abort_tasks();
    replacement.abort_tasks();
}
