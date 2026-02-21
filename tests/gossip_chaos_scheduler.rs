use icanact_remote::registry::{GossipRegistry, RegistryChange, RegistryDelta, RegistryMessage};
use icanact_remote::{GossipConfig, KeyPair, NodeId, RegistrationPriority, RemoteActorLocation};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng as _, SeedableRng as _};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::time::Duration;

fn test_config(seed: &str) -> GossipConfig {
    GossipConfig {
        key_pair: Some(KeyPair::new_for_testing(seed)),
        gossip_interval: Duration::from_secs(3600), // disable background gossip effects in tests
        ..Default::default()
    }
}

fn test_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn snapshot_known(reg: &GossipRegistry) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    reg.actor_state.known_actors.iter_sync(|k, loc| {
        // Avoid relying on private VectorClock helpers; use the public node set + get().
        let mut nodes: Vec<NodeId> = loc.vector_clock.get_nodes().into_iter().collect();
        nodes.sort();
        let clock = nodes
            .into_iter()
            .map(|n| format!("{}:{}", n.fmt_short(), loc.vector_clock.get(&n)))
            .collect::<Vec<_>>()
            .join(",");
        out.insert(
            k.clone(),
            format!(
                "peer={} addr={} wall={} node={} clock=[{}]",
                loc.peer_id,
                loc.address,
                loc.wall_clock_time,
                loc.node_id.fmt_short(),
                clock
            ),
        );
        true
    });
    out
}

fn snapshot_local(reg: &GossipRegistry) -> HashMap<String, RemoteActorLocation> {
    let mut out = HashMap::new();
    reg.actor_state.local_actors.iter_sync(|k, v| {
        out.insert(k.clone(), v.clone());
        true
    });
    out
}

fn snapshot_known_map(reg: &GossipRegistry) -> HashMap<String, RemoteActorLocation> {
    let mut out = HashMap::new();
    reg.actor_state.known_actors.iter_sync(|k, v| {
        out.insert(k.clone(), v.clone());
        true
    });
    out
}

fn concurrent_locations(loc1: &mut RemoteActorLocation, loc2: &mut RemoteActorLocation) {
    // Force deterministic tie-break in concurrent case: same wall clock, same metadata, etc.
    loc1.wall_clock_time = 123;
    loc2.wall_clock_time = 123;
    loc1.metadata = vec![1, 2, 3];
    loc2.metadata = vec![1, 2, 3];
    loc1.local_registration_time = 0;
    loc2.local_registration_time = 0;
}

#[tokio::test]
async fn test_gossip_scheduler_concurrent_conflict_converges_across_seeds() {
    let actor = "actor.conflict";

    let base_a = GossipRegistry::new(test_addr(8011), test_config("sched_a"));
    let base_b = GossipRegistry::new(test_addr(8012), test_config("sched_b"));

    let peer_a = base_a.peer_id.clone();
    let peer_b = base_b.peer_id.clone();

    let mut loc_a = RemoteActorLocation::new_with_peer(test_addr(9011), peer_a.clone());
    let mut loc_b = RemoteActorLocation::new_with_peer(test_addr(9012), peer_b.clone());
    concurrent_locations(&mut loc_a, &mut loc_b);

    let delta_a = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![RegistryChange::ActorAdded {
            name: actor.to_string(),
            location: loc_a,
            priority: RegistrationPriority::Normal,
        }],
        sender_peer_id: peer_a,
        wall_clock_time: 0,
        precise_timing_nanos: 0,
    };
    let delta_b = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![RegistryChange::ActorAdded {
            name: actor.to_string(),
            location: loc_b,
            priority: RegistrationPriority::Normal,
        }],
        sender_peer_id: peer_b,
        wall_clock_time: 0,
        precise_timing_nanos: 0,
    };

    let mut reference: Option<BTreeMap<String, String>> = None;

    for seed in 0u64..25 {
        let reg_c = GossipRegistry::new(test_addr(8020 + seed as u16), test_config("sched_c"));

        // Deterministic "chaos": reorder + duplicates, but always deliver at least one copy of each.
        let mut rng = StdRng::seed_from_u64(seed);
        let mut events: Vec<&RegistryDelta> = vec![&delta_a, &delta_b];
        if rng.random_bool(0.75) {
            events.push(&delta_a);
        }
        if rng.random_bool(0.75) {
            events.push(&delta_b);
        }
        events.shuffle(&mut rng);

        for d in events {
            reg_c.apply_delta(d.clone()).await.unwrap();
        }

        let snap = snapshot_known(&reg_c);
        match &mut reference {
            None => reference = Some(snap),
            Some(r) => assert_eq!(&snap, r, "seed {seed} must converge identically"),
        }
    }
}

#[tokio::test]
async fn test_gossip_scheduler_full_sync_interleavings_are_order_independent() {
    let node_a = GossipRegistry::new(test_addr(8101), test_config("fs_a"));
    let node_b = GossipRegistry::new(test_addr(8102), test_config("fs_b"));

    // Disjoint actors to avoid conflict; the test is about interleaving/idempotency.
    node_a
        .register_actor(
            "actor.a".to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9111), node_a.peer_id.clone()),
        )
        .await
        .unwrap();
    node_b
        .register_actor(
            "actor.b".to_string(),
            RemoteActorLocation::new_with_peer(test_addr(9112), node_b.peer_id.clone()),
        )
        .await
        .unwrap();

    let (a_local, a_known) = (snapshot_local(&node_a), snapshot_known_map(&node_a));
    let (b_local, b_known) = (snapshot_local(&node_b), snapshot_known_map(&node_b));

    let msg_a = node_a
        .create_full_sync_response_from_state(&a_local, &a_known, 1)
        .await;
    let msg_b = node_b
        .create_full_sync_response_from_state(&b_local, &b_known, 1)
        .await;

    let RegistryMessage::FullSyncResponse {
        local_actors: a_la,
        known_actors: a_ka,
        sender_peer_id: a_pid,
        sequence: a_seq,
        wall_clock_time: a_time,
        ..
    } = msg_a
    else {
        panic!("expected full sync response");
    };
    let RegistryMessage::FullSyncResponse {
        local_actors: b_la,
        known_actors: b_ka,
        sender_peer_id: b_pid,
        sequence: b_seq,
        wall_clock_time: b_time,
        ..
    } = msg_b
    else {
        panic!("expected full sync response");
    };

    let mut reference: Option<BTreeMap<String, String>> = None;

    for seed in 0u64..20 {
        let reg_c = GossipRegistry::new(test_addr(8200 + seed as u16), test_config("fs_c"));
        let mut rng = StdRng::seed_from_u64(seed);

        let mut order = vec![0u8, 1u8];
        if rng.random_bool(0.5) {
            order.push(0);
        }
        if rng.random_bool(0.5) {
            order.push(1);
        }
        order.shuffle(&mut rng);

        for tag in order {
            match tag {
                0 => {
                    reg_c
                        .merge_full_sync(
                            a_la.clone().into_iter().collect(),
                            a_ka.clone().into_iter().collect(),
                            a_pid.clone(),
                            test_addr(8101),
                            a_seq,
                            a_time,
                        )
                        .await;
                }
                1 => {
                    reg_c
                        .merge_full_sync(
                            b_la.clone().into_iter().collect(),
                            b_ka.clone().into_iter().collect(),
                            b_pid.clone(),
                            test_addr(8102),
                            b_seq,
                            b_time,
                        )
                        .await;
                }
                _ => unreachable!(),
            }
        }

        let snap = snapshot_known(&reg_c);
        match &mut reference {
            None => reference = Some(snap),
            Some(r) => assert_eq!(&snap, r, "seed {seed} must converge identically"),
        }
    }
}
