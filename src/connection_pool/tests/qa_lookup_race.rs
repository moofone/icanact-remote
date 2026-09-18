struct FallbackAdoptionHookReset;

impl Drop for FallbackAdoptionHookReset {
    fn drop(&mut self) {
        set_fallback_adoption_hook(None);
    }
}

/// The address fallback is a capture-then-adopt path. A replacement may win
/// the peer session between those two operations, so adoption must use the
/// observed empty primary slot as a compare-and-publish fence rather than an
/// unconditional store.
#[tokio::test]
async fn fallback_capture_then_replacement_cannot_be_overwritten_on_resume() {
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
