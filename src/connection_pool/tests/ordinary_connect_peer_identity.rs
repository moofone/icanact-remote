// Ordinary `Peer::connect(B, X)` must not hand B another authenticated peer's
// live connection just because X is the address B was told to use.

#[tokio::test]
async fn ordinary_connect_does_not_adopt_another_authenticated_peer_connection() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("ordinary-connect-local")),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();
    let addr_x: SocketAddr = "127.0.0.1:40641".parse().unwrap();
    let peer_a = crate::KeyPair::new_for_testing("ordinary_connect_owner_a").peer_id();
    let peer_b = crate::KeyPair::new_for_testing("ordinary_connect_caller_b").peer_id();

    // A's normal verified-ownership premise: an accepted Verified claim for X,
    // as the outbound TLS path records before finalization. This models
    // ownership only; it is not a new authentication API or trust flag.
    let expected_owner = crate::addr_ownership::Owner {
        node_id: peer_a.clone(),
        kind: crate::addr_ownership::ClaimKind::Verified,
    };
    let claim = registry
        .registry_owner
        .claim(
            addr_x,
            crate::addr_ownership::Claim {
                node_id: peer_a.clone(),
                kind: crate::addr_ownership::ClaimKind::Verified,
            },
            false,
        )
        .await;
    assert!(claim.is_accepted(), "setup: A's Verified claim on X is accepted");
    assert_eq!(
        registry.registry_owner.owner_of(&addr_x),
        Some(expected_owner.clone()),
        "setup: A owns X as Verified before B's route"
    );

    // A: a usable outbound connection at X. The in-memory duplex does NOT
    // perform a real TLS handshake; it models the post-handshake result by
    // supplying A as both the TOFU identity and the fresh-session identity,
    // the two inputs production derives from the peer certificate.
    let (io, _peer_io) = tokio::io::duplex(64 * 1024);
    let _handle = pool
        .finalize_new_outbound_connection(
            addr_x,
            io,
            Arc::downgrade(&registry),
            Some(peer_a.to_node_id()),
            addr_x,
            Some(peer_a.to_node_id()),
        )
        .await
        .expect("finalize A's outbound connection");

    // Positive control: A is bound to X and resolves to its own connection.
    let conn_a = pool
        .get_connection_by_addr(&addr_x)
        .expect("A's connection is indexed at X");
    assert_eq!(conn_a.embedded_peer_id.as_ref(), Some(&peer_a));
    assert!(pool.is_usable_connection(&conn_a));
    let resolved_a = pool
        .get_connection_by_peer_id(&peer_a)
        .expect("A must resolve to its own connection");
    assert!(Arc::ptr_eq(&resolved_a, &conn_a));

    // B starts with no session and no connection.
    let b_session = |pool: &ConnectionPool<()>| {
        pool.peer_sessions
            .read_sync(&peer_b, |_, session| session.current_connection())
            .flatten()
    };
    assert!(b_session(&pool).is_none(), "setup: B has no session");

    // The ordinary connect route for B at A's address, through the real owner.
    assert!(
        registry
            .registry_owner
            .set_ordinary_connect_route(peer_b.clone(), addr_x)
            .await,
        "setup: B is not pinned elsewhere, so the route is accepted"
    );

    assert!(
        b_session(&pool).is_none(),
        "B must not adopt A's connection into its session"
    );
    assert!(
        pool.get_connection_by_peer_id(&peer_b).is_none(),
        "B must not resolve to the connection authenticated as A"
    );
    assert!(!pool.has_connection_by_peer_id(&peer_b));

    // A stays correctly owning X, bound and usable.
    assert_eq!(
        registry.registry_owner.owner_of(&addr_x),
        Some(expected_owner),
        "A must still own X as Verified after B's route"
    );
    assert_eq!(pool.get_peer_id_by_addr(&addr_x).as_ref(), Some(&peer_a));
    assert_eq!(conn_a.embedded_peer_id.as_ref(), Some(&peer_a));
    assert!(pool.is_usable_connection(&conn_a));
    let still_a = pool
        .get_connection_by_peer_id(&peer_a)
        .expect("A must still resolve");
    assert!(Arc::ptr_eq(&still_a, &conn_a));
    let at_x = pool
        .get_connection_by_addr(&addr_x)
        .expect("X must still index A's connection");
    assert!(Arc::ptr_eq(&at_x, &conn_a));
}
