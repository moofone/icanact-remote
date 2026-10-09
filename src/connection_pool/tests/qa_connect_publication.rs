use super::*;

/// An unknown-identity/bootstrap dial may publish a nonpreferred outbound
/// before the preferred inbound arrives. If that inbound retires the outbound
/// while it is building its identify, this is a lost candidate, not a failed
/// peer connection. Drive the actual finalizer rather than model its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn superseded_mid_identify_reports_existing_survivor_not_peer_failure() {
    exercise_identify_supersession(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn superseded_mid_identify_with_dead_replacement_still_reports_failure() {
    exercise_identify_supersession(false).await;
}

async fn exercise_identify_supersession(survivor_alive: bool) {
    let (local, remote) = hi_lo_keypairs("identify-survivor-a", "identify-survivor-b");
    let peer = remote.peer_id();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        crate::GossipConfig {
            key_pair: Some(local),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();
    pool.set_registry(registry.clone());
    let addr: SocketAddr = "127.0.0.1:41802".parse().unwrap();
    let fresh_addr: SocketAddr = "127.0.0.1:41803".parse().unwrap();
    pool.add_addr_to_peer_id(addr, peer.clone());
    let (io, _keep) = tokio::io::duplex(4096);
    let gossip_guard = registry.gossip_state.lock().await;
    let finalize = {
        let pool = pool.clone();
        let registry = Arc::downgrade(&registry);
        tokio::spawn(async move {
            pool.finalize_new_outbound_connection(addr, io, registry, None, addr, None)
                .await
        })
    };
    let candidate = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(conn) = pool.peer_current_connection_snapshot(&peer) {
                break conn;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("outbound must publish before building identify");
    assert_eq!(candidate.direction, ConnectionDirection::Outbound);
    assert_eq!(pool.connection_count(), 1);

    let mut fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;
    let metadata = Arc::get_mut(&mut fresh).expect("unpublished connection is uniquely owned");
    metadata.embedded_peer_id = Some(peer.clone());
    metadata.correlation = candidate.correlation.clone();
    assert!(pool.publish_inbound_or_reresolve(
        &peer,
        &fresh,
        Some(&candidate),
        &Arc::downgrade(&registry)
    ));
    assert!(pool.finish_indexing_accepted_connection(&peer, fresh_addr, None, &fresh));
    let survivor = pool
        .peer_current_connection_snapshot(&peer)
        .expect("preferred inbound current");
    assert_eq!(survivor.direction, ConnectionDirection::Inbound);
    assert!(!Arc::ptr_eq(&candidate, &survivor));
    assert!(
        !candidate.has_live_stream(),
        "replacement must retire candidate IO"
    );
    if !survivor_alive {
        survivor.abort_tasks();
    }
    drop(gossip_guard);

    let result = tokio::time::timeout(Duration::from_secs(5), finalize)
        .await
        .expect("superseded finalize must terminate")
        .expect("finalize must not panic");
    if survivor_alive {
        assert!(
            matches!(result, Err(crate::GossipError::ConnectionExists)),
            "a lost candidate with a usable session survivor is not a failed peer dial: {result:?}"
        );
    } else {
        assert!(
            matches!(result, Err(crate::GossipError::Network(ref error))
            if error.kind() == std::io::ErrorKind::ConnectionAborted),
            "a dead replacement must not turn a failed identify into success: {result:?}"
        );
    }
    let current = pool
        .peer_current_connection_snapshot(&peer)
        .expect("survivor retained");
    assert!(Arc::ptr_eq(&current, &survivor));
    assert_eq!(survivor.has_live_stream(), survivor_alive);
    assert_eq!(
        pool.connection_count(),
        1,
        "only surviving instance counted"
    );
    assert!(
        pool.get_lock_free_connection(addr).is_none(),
        "no dead candidate alias"
    );
    assert!(pool.disconnect_connection_instance(&peer, &survivor));
    assert_eq!(pool.connection_count(), 0);
}
