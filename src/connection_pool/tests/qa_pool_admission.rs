use super::*;

/// REMOTE-5: count admission is one shared mutation boundary, including
/// concurrent distinct peers. Releases are exact and immediately reusable.
#[test]
fn concurrent_count_admission_has_a_strict_plateau() {
    let pool = ConnectionPool::<()>::new(1, Duration::from_secs(1));
    let start = Barrier::new(3);
    let admitted = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for instance in [u64::MAX - 1, u64::MAX] {
            let pool = &pool;
            let start = &start;
            let admitted = &admitted;
            scope.spawn(move || {
                start.wait();
                if pool.count_in_new_instance(instance) {
                    admitted.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
        start.wait();
    });
    assert_eq!(admitted.load(Ordering::SeqCst), 1);
    assert_eq!(pool.counted_instances.len(), 1);
    assert_eq!(pool.connection_counter.load(Ordering::SeqCst), 1);
    for instance in [u64::MAX - 1, u64::MAX] {
        pool.release_counted_instance(instance);
        pool.release_counted_instance(instance);
    }
    assert_eq!(pool.connection_counter.load(Ordering::SeqCst), 0);
    assert!(pool.count_in_new_instance(42));
    assert!(
        pool.count_in_new_instance(42),
        "same owner is not charged twice"
    );
    assert_eq!(pool.connection_counter.load(Ordering::SeqCst), 1);
    assert!(!pool.count_in_new_instance(43));
    pool.release_counted_instance(42);
}

#[test]
fn dial_gate_saturation_preserves_existing_leader_and_recovers_on_release() {
    let pool = ConnectionPool::<()>::new(1, Duration::from_secs(1));
    let first: SocketAddr = "127.0.0.1:42701".parse().unwrap();
    let other: SocketAddr = "127.0.0.1:42702".parse().unwrap();
    let leader = match pool.acquire_outbound_dial_gate(first).unwrap() {
        OutboundDialLease::Leader(gate) => gate,
        OutboundDialLease::Follower(_) => panic!("first admission owns gate"),
    };
    assert!(matches!(
        pool.acquire_outbound_dial_gate(first),
        Ok(OutboundDialLease::Follower(_))
    ));
    assert!(matches!(
        pool.acquire_outbound_dial_gate(other),
        Err(GossipError::WriteQueueFull)
    ));
    assert_eq!(pool.outbound_dial_gates.len(), 1);
    pool.finish_outbound_dial_gate(first, &leader, false);
    assert!(matches!(
        pool.acquire_outbound_dial_gate(other),
        Ok(OutboundDialLease::Leader(_))
    ));
    // A delayed old finish must not remove a successor gate.
    let successor = match pool.acquire_outbound_dial_gate(other).unwrap() {
        OutboundDialLease::Follower(gate) => gate,
        OutboundDialLease::Leader(_) => panic!("successor remains admitted"),
    };
    pool.finish_outbound_dial_gate(other, &leader, false);
    assert!(
        pool.outbound_dial_gates
            .read_sync(&other, |_, current| { Arc::ptr_eq(current, &successor) })
            .unwrap()
    );
}

#[test]
fn peer_state_admission_bounds_required_and_idle_identities() {
    let pool = ConnectionPool::<()>::new(1, Duration::from_secs(1));
    let peers: Vec<_> = (0..5)
        .map(|index| crate::KeyPair::new_for_testing(format!("pool-budget-{index}")).peer_id())
        .collect();
    let addr: SocketAddr = "127.0.0.1:42703".parse().unwrap();
    for peer in &peers[..4] {
        pool.set_configured_peer_addr(peer, addr).unwrap();
    }
    assert!(matches!(
        pool.set_configured_peer_addr(&peers[4], addr),
        Err(GossipError::WriteQueueFull)
    ));
    assert_eq!(pool.peer_sessions.len(), 4);
    assert_eq!(pool.peer_id_to_addr.len(), 4);
    assert!(peers[..4].iter().all(|peer| pool.is_required_peer(peer)));
    assert!(!pool.is_required_peer(&peers[4]));

    let idle_pool = ConnectionPool::<()>::new(1, Duration::from_secs(1));
    let receipt = idle_pool.reserve_peer_session(&peers[0]).unwrap();
    *receipt._session.last_touched.lock().unwrap() = Instant::now() - Duration::from_secs(301);
    idle_pool.prune_idle_peer_sessions();
    assert_eq!(
        idle_pool.peer_sessions.len(),
        1,
        "owner receipt prevents pruning"
    );
    drop(receipt);
    idle_pool.prune_idle_peer_sessions();
    assert_eq!(idle_pool.peer_sessions.len(), 0);
    assert!(idle_pool.reserve_peer_session(&peers[4]).is_ok());
}

#[tokio::test]
async fn finalizer_uses_own_hello_instead_of_mutable_address_capabilities() {
    let (local, remote) = hi_lo_keypairs("hello-owner-a", "hello-owner-b");
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        crate::GossipConfig {
            key_pair: Some(local),
            ..Default::default()
        },
    ));
    let pool = &registry.connection_pool;
    let peer = remote.peer_id();
    let addr: SocketAddr = "127.0.0.1:42704".parse().unwrap();
    let own = crate::handshake::PeerCapabilities {
        version: crate::handshake::CURRENT_PROTOCOL_VERSION,
        features: 0,
        remote_boot_id: crate::handshake::RemoteBootId::from_bytes([1; 16]),
        remote_client_only: false,
    };
    let foreign = crate::handshake::PeerCapabilities {
        remote_boot_id: crate::handshake::RemoteBootId::from_bytes([2; 16]),
        remote_client_only: true,
        ..own
    };
    registry.set_peer_capabilities(addr, foreign);
    let instance = LockFreeStreamHandle::allocate_instance_id();
    let handle = pool
        .finalize_new_outbound_connection_with_instance(
            addr,
            ReceiptFixtureIo,
            Arc::downgrade(&registry),
            OutboundSessionIdentity::authenticated(Some(peer.to_node_id())).unwrap(),
            addr,
            instance,
            Some(own),
        )
        .await
        .expect("own authenticated stream is admitted");
    assert_eq!(handle.instance_id(), Some(instance));
    let connection = pool.committed_peer_connection_snapshot(&peer).unwrap();
    assert_eq!(connection.peer_capabilities, Some(own));
    assert_eq!(connection.remote_boot_id, Some(own.remote_boot_id));
    assert_ne!(connection.remote_boot_id, Some(foreign.remote_boot_id));
    pool.disconnect_connection_instance(&peer, &connection);
}

#[test]
fn primary_and_mirror_publication_share_one_mutation_domain() {
    let pool = ConnectionPool::<()>::new(1, Duration::from_secs(1));
    let peer = crate::KeyPair::new_for_testing("mirror-domain").peer_id();
    let addr: SocketAddr = "127.0.0.1:42705".parse().unwrap();
    let connection = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Inbound));
    let session = pool.get_or_create_peer_session(&peer).unwrap();
    session.set_current_connection(Some(connection.clone()), || {
        assert!(matches!(
            pool.session_publication.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert!(Arc::ptr_eq(
            &session.current_connection().unwrap(),
            &connection
        ));
        let _ = pool
            .connections_by_peer
            .upsert_sync(peer.clone(), connection.clone());
    });
    assert!(pool.session_publication.try_lock().is_ok());
    assert!(
        pool.connections_by_peer
            .read_sync(&peer, |_, mirrored| { Arc::ptr_eq(mirrored, &connection) })
            .unwrap()
    );
}

#[test]
fn missing_authenticated_identity_cannot_construct_finalizer_evidence() {
    assert!(matches!(
        OutboundSessionIdentity::authenticated(None),
        Err(GossipError::TlsHandshakeFailed(_))
    ));
    let peer = crate::KeyPair::new_for_testing("finalizer-evidence").peer_id();
    assert!(matches!(
        OutboundSessionIdentity::authenticated(Some(peer.to_node_id())),
        Ok(OutboundSessionIdentity::Authenticated(node)) if node == peer.to_node_id()
    ));
}

#[tokio::test]
async fn stale_clock_reply_never_uses_successor_stream() {
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("stale-clock-local")),
            ..Default::default()
        },
    ));
    let peer = crate::KeyPair::new_for_testing("clock-reply-owner").peer_id();
    let addr: SocketAddr = "127.0.0.1:42706".parse().unwrap();
    let old = qa_r11_generation_race_connection(addr, &peer);
    let old_stream = old.stream_handle.as_ref().unwrap().clone();
    assert!(
        registry
            .connection_pool
            .add_connection_by_peer_id(peer.clone(), addr, old,)
    );
    let successor = qa_r11_generation_race_connection(addr, &peer);
    let successor_stream = successor.stream_handle.as_ref().unwrap().clone();
    assert!(
        registry
            .connection_pool
            .add_connection_by_peer_id(peer, addr, successor,)
    );
    let before = successor_stream.sequence_counter.load(Ordering::Acquire);
    answer_inbound_clock_probe(
        &registry,
        &old_stream,
        addr,
        crate::registry::GossipExtensionsV1::default(),
    )
    .await;
    assert_eq!(
        successor_stream.sequence_counter.load(Ordering::Acquire),
        before
    );
    // The exact live stream remains a usable reply destination.
    answer_inbound_clock_probe(
        &registry,
        &successor_stream,
        addr,
        crate::registry::GossipExtensionsV1::default(),
    )
    .await;
    assert_eq!(
        successor_stream.sequence_counter.load(Ordering::Acquire),
        before + 1
    );
    registry.connection_pool.close_all_connections();
}

#[test]
fn rollback_restoration_slots_remain_charged_while_indexes_are_absent() {
    let pool = ConnectionPool::<()>::new(1, Duration::from_secs(1));
    let peers: Vec<_> = (0..5)
        .map(|index| crate::KeyPair::new_for_testing(format!("rollback-slot-{index}")).peer_id())
        .collect();
    let addresses: Vec<SocketAddr> = (0..5)
        .map(|index| format!("127.0.0.1:{}", 42900 + index).parse().unwrap())
        .collect();
    for index in 0..4 {
        pool.set_discovered_peer_addr(&peers[index], addresses[index])
            .unwrap();
        let connection = Arc::new(LockFreeConnection::new(
            addresses[index],
            ConnectionDirection::Inbound,
        ));
        pool.publish_address_index(addresses[index], connection, None)
            .unwrap();
    }
    let reservation = pool.reserve_projection_restoration(&peers[..1], &addresses[..1]);
    pool.peer_id_to_addr.remove_sync(&peers[0]);
    pool.connections_by_addr.remove_sync(&addresses[0]);
    // Route capacity is tested directly, independently of peer-session capacity.
    {
        let _admission = pool.admission.lock().unwrap();
        assert!(pool.route_projection_full(&peers[4]));
        assert!(!pool.route_projection_full(&peers[0]));
    }
    let replacement = Arc::new(LockFreeConnection::new(
        addresses[4],
        ConnectionDirection::Inbound,
    ));
    assert!(matches!(
        pool.publish_address_index(addresses[4], replacement, None),
        Err(crate::GossipError::WriteQueueFull)
    ));
    pool.set_discovered_peer_addr(&peers[0], addresses[0])
        .unwrap();
    let restored = Arc::new(LockFreeConnection::new(
        addresses[0],
        ConnectionDirection::Inbound,
    ));
    pool.publish_address_index(addresses[0], restored, None)
        .unwrap();
    assert_eq!(pool.connections_by_addr.len(), 4);
    drop(reservation);
    pool.peer_id_to_addr.remove_sync(&peers[0]);
    pool.connections_by_addr.remove_sync(&addresses[0]);
    let replacement = Arc::new(LockFreeConnection::new(
        addresses[4],
        ConnectionDirection::Inbound,
    ));
    pool.publish_address_index(addresses[4], replacement, None)
        .unwrap();
    let _admission = pool.admission.lock().unwrap();
    assert!(!pool.route_projection_full(&peers[4]));
}

#[test]
fn sessionless_route_restoration_does_not_need_identity_admission() {
    let pool = ConnectionPool::<()>::new(1, Duration::from_secs(1));
    let route_peer = crate::KeyPair::new_for_testing("sessionless-restore").peer_id();
    let addr: SocketAddr = "127.0.0.1:42800".parse().unwrap();
    let _ = pool.peer_id_to_addr.upsert_sync(route_peer.clone(), addr);
    let reservation = pool.reserve_projection_restoration(std::slice::from_ref(&route_peer), &[]);
    pool.peer_id_to_addr.remove_sync(&route_peer);
    for index in 0..4 {
        let peer = crate::KeyPair::new_for_testing(format!("session-capacity-{index}")).peer_id();
        pool.get_or_create_peer_session(&peer).unwrap();
    }
    assert!(matches!(
        pool.set_discovered_peer_addr(&route_peer, addr),
        Err(crate::GossipError::WriteQueueFull)
    ));
    pool.restore_reserved_peer_route(&route_peer, addr, Some(addr))
        .unwrap();
    assert_eq!(pool.get_configured_peer_addr(&route_peer), Some(addr));
    assert!(!pool.peer_sessions.contains_sync(&route_peer));
    assert_eq!(pool.peer_sessions.len(), 4);
    drop(reservation);
}
