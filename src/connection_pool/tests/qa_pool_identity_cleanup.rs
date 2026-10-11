use super::*;

async fn identified_connection(addr: SocketAddr, peer: &crate::PeerId) -> Arc<LockFreeConnection> {
    let mut connection = make_live_connection(addr, ConnectionDirection::Outbound).await;
    Arc::get_mut(&mut connection).unwrap().embedded_peer_id = Some(peer.clone());
    connection
}

/// QA-POOL-001: a configured address never authenticates its current occupant.
/// Also exercise the same-identity positive case so rejecting every fallback
/// cannot satisfy the regression.
#[tokio::test]
async fn configured_address_fallback_requires_authenticated_identity() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(1));
    let requested = crate::KeyPair::new_for_testing("qa-pool-requested").peer_id();
    let other = crate::KeyPair::new_for_testing("qa-pool-other").peer_id();
    let addr: SocketAddr = "127.0.0.1:42901".parse().unwrap();
    pool.set_configured_peer_addr(&requested, addr)
        .expect("peer state admitted");
    let wrong = identified_connection(addr, &other).await;
    pool.connections_by_addr.upsert_sync(addr, wrong.clone());

    assert!(pool.peer_current_connection_snapshot(&requested).is_none());
    assert!(pool.get_connection_by_peer_id(&requested).is_none());
    let result = pool
        .get_connection_with_node_id(addr, Some(requested.to_node_id()))
        .await;
    assert!(matches!(
        result,
        Err(crate::GossipError::TlsHandshakeFailed(_))
    ));
    assert_eq!(pool.outbound_tcp_dial_attempts(), 0);
    assert!(
        wrong.has_live_stream(),
        "a mismatch must not tear down the other peer"
    );
    assert!(Arc::ptr_eq(
        &pool
            .connections_by_addr
            .read_sync(&addr, |_, value| value.clone())
            .unwrap(),
        &wrong,
    ));

    let right = identified_connection(addr, &requested).await;
    pool.connections_by_addr.upsert_sync(addr, right.clone());
    assert!(Arc::ptr_eq(
        &pool.peer_current_connection_snapshot(&requested).unwrap(),
        &right,
    ));
    assert!(Arc::ptr_eq(
        &pool.get_connection_by_peer_id(&requested).unwrap(),
        &right,
    ));
    assert!(
        pool.get_connection_with_node_id(addr, Some(requested.to_node_id()))
            .await
            .is_ok()
    );
}

/// QA-POOL-002: split the real cleanup snapshot and mutation stages to place
/// a replacement in the gap without sleeps, scheduling assumptions, or sockets.
#[tokio::test]
async fn stale_cleanup_snapshot_cannot_disconnect_successor() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(1));
    let peer = crate::KeyPair::new_for_testing("qa-pool-stale-cleanup").peer_id();
    let addr: SocketAddr = "127.0.0.1:42902".parse().unwrap();
    let stale = identified_connection(addr, &peer).await;
    stale.set_state(ConnectionState::Disconnected);
    pool.publish_current_peer_connection(&peer, stale.clone());
    pool.connections_by_addr.upsert_sync(addr, stale.clone());
    let observed = pool.stale_connection_snapshot();
    assert_eq!(observed.len(), 1, "must exercise a real stale observation");
    assert!(Arc::ptr_eq(&observed[0].1, &stale));

    let fresh = identified_connection(addr, &peer).await;
    pool.publish_current_peer_connection(&peer, fresh.clone());
    pool.connections_by_addr.upsert_sync(addr, fresh.clone());
    pool.retire_stale_connection_snapshot(observed);
    assert!(Arc::ptr_eq(
        &pool.peer_current_connection_snapshot(&peer).unwrap(),
        &fresh,
    ));
    assert!(Arc::ptr_eq(
        &pool
            .connections_by_addr
            .read_sync(&addr, |_, value| value.clone())
            .unwrap(),
        &fresh,
    ));
    assert!(fresh.has_live_stream());

    fresh.set_state(ConnectionState::Disconnected);
    pool.cleanup_stale_connections();
    assert!(pool.peer_current_connection_snapshot(&peer).is_none());
    assert!(
        !fresh.has_live_stream(),
        "an unchanged stale session must be retired"
    );
}

/// QA-POOL-004: use the production selection and retirement stages, replacing
/// the address occupant between them. No scheduler or socket timing is needed.
#[tokio::test]
async fn delayed_lru_victim_cannot_remove_replacement() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(1));
    let peer = crate::KeyPair::new_for_testing("qa-lru-old").peer_id();
    let successor_peer = crate::KeyPair::new_for_testing("qa-lru-new").peer_id();
    let addr: SocketAddr = "127.0.0.1:42903".parse().unwrap();
    let old = identified_connection(addr, &peer).await;
    pool.connections_by_addr.upsert_sync(addr, old.clone());
    let (selected_addr, selected) = pool.select_lru_eviction_victim().unwrap();
    assert_eq!(selected_addr, addr);
    assert!(Arc::ptr_eq(&selected, &old));

    let fresh = identified_connection(addr, &successor_peer).await;
    pool.connections_by_addr.upsert_sync(addr, fresh.clone());
    assert!(
        pool.remove_observed_connection(selected_addr, Some(&selected))
            .is_none()
    );
    assert!(fresh.has_live_stream());
    assert!(Arc::ptr_eq(
        &pool
            .connections_by_addr
            .read_sync(&addr, |_, c| c.clone())
            .unwrap(),
        &fresh,
    ));

    let (selected_addr, selected) = pool.select_lru_eviction_victim().unwrap();
    let removed = pool
        .remove_observed_connection(selected_addr, Some(&selected))
        .unwrap();
    assert!(Arc::ptr_eq(&removed, &fresh));
    assert!(!fresh.has_live_stream());
    assert!(!pool.connections_by_addr.contains_sync(&addr));
}
