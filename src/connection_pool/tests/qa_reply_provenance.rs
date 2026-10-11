use super::*;

/// REMOTE-3/4: a rejected physical stream tracker is authoritative. It must
/// not widen to a successor's address tracker with the same numeric wire ID.
#[tokio::test]
async fn qa_api_005_old_reply_and_nack_preserve_successor_tracker() {
    for (nack, missing) in [(false, false), (true, false), (false, true), (true, true)] {
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("reply-provenance-local")),
                ..Default::default()
            },
        ));
        let addr: SocketAddr = "127.0.0.1:42905".parse().unwrap();
        let peer = crate::KeyPair::new_for_testing("reply-provenance-peer").peer_id();
        let old = CorrelationTracker::new();
        let old_slot = old.allocate().unwrap();
        let id = old_slot.id();
        drop(old_slot);
        let current = CorrelationTracker::new();
        let slot = current.allocate().unwrap();
        assert_eq!(slot.id(), id, "precondition: colliding numeric IDs");
        let mut connection = make_live_connection(addr, ConnectionDirection::Outbound).await;
        let inner = Arc::get_mut(&mut connection).unwrap();
        inner.embedded_peer_id = Some(peer.clone());
        inner.correlation = Some(current.clone());
        assert!(
            registry
                .connection_pool
                .add_connection_by_peer_id(peer, addr, connection)
        );

        if nack {
            crate::handle::handle_response_nack_message(
                &registry,
                addr,
                id,
                crate::framing::AskNackReason::NoDispatcher,
                if missing { None } else { Some(&old) },
            )
            .await;
        } else {
            crate::handle::handle_response_message(
                &registry,
                addr,
                id,
                crate::AlignedBytes::from_pooled_slice(
                    b"stale",
                    registry.connection_pool.aligned_bytes_pool(),
                )
                .unwrap(),
                if missing { None } else { Some(&old) },
            )
            .await;
        }
        assert_eq!(
            current.pending[CorrelationTracker::slot_index(id)]
                .state
                .load(Ordering::Acquire),
            correlation_slot_word(id, SLOT_WAITING),
            "old response or NACK must not complete the successor",
        );
        // Supported bidirectional replies use the same captured shared tracker
        // even on a different physical endpoint, without an address lookup.
        let incoming_source: SocketAddr = "127.0.0.1:52905".parse().unwrap();
        assert!(
            registry
                .connection_pool
                .get_connection_by_addr(&incoming_source)
                .is_none()
        );
        if nack {
            crate::handle::handle_response_nack_message(
                &registry,
                incoming_source,
                id,
                crate::framing::AskNackReason::NoDispatcher,
                Some(&current),
            )
            .await;
            assert!(matches!(
                current.wait_for_response(id, Duration::from_secs(1)).await,
                Err(crate::GossipError::AskNacked(
                    crate::framing::AskNackReason::NoDispatcher
                )),
            ));
        } else {
            crate::handle::handle_response_message(
                &registry,
                incoming_source,
                id,
                crate::AlignedBytes::from_pooled_slice(
                    b"current",
                    registry.connection_pool.aligned_bytes_pool(),
                )
                .unwrap(),
                Some(&current),
            )
            .await;
            let response = current
                .wait_for_response(id, Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(response.as_ref(), b"current");
        }
        slot.disarm();
    }
}
