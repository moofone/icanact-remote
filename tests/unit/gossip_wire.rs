use icanact_remote::registry::{RegistryChange, RegistryDelta, RegistryMessage};
use icanact_remote::{KeyPair, RegistrationPriority, RemoteActorLocation};
use std::net::SocketAddr;

fn test_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[test]
fn registry_delta_rkyv_roundtrip() {
    let peer_id = KeyPair::new_for_testing("wire_delta").peer_id();
    let actor = "actor.roundtrip";
    let loc = RemoteActorLocation::new_with_peer(test_addr(9001), peer_id.clone());

    let delta = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![RegistryChange::ActorAdded {
            name: actor.to_string(),
            location: loc,
            priority: RegistrationPriority::Normal,
        }],
        sender_peer_id: peer_id,
        wall_clock_time: 123,
        precise_timing_nanos: 456,
    };

    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&delta).unwrap(); // ALLOW_RKYV_TO_BYTES
    let deserialized = rkyv::from_bytes::<RegistryDelta, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

    assert_eq!(deserialized.since_sequence, 0);
    assert_eq!(deserialized.current_sequence, 1);
    assert_eq!(deserialized.changes.len(), 1);
}

#[test]
fn registry_message_rkyv_roundtrip_delta_gossip() {
    let peer_id = KeyPair::new_for_testing("wire_msg").peer_id();
    let actor = "actor.msg";
    let loc = RemoteActorLocation::new_with_peer(test_addr(9002), peer_id.clone());

    let msg = RegistryMessage::DeltaGossip {
        delta: RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: actor.to_string(),
                location: loc,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: peer_id,
            wall_clock_time: 0,
            precise_timing_nanos: 0,
        },
        extensions: None,
    };

    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap(); // ALLOW_RKYV_TO_BYTES
    let deserialized =
        rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES

    match deserialized {
        RegistryMessage::DeltaGossip { delta, .. } => {
            assert_eq!(delta.current_sequence, 1);
            assert_eq!(delta.changes.len(), 1);
        }
        other => panic!("unexpected message variant: {other:?}"),
    }
}
