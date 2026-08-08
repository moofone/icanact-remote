use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use icanact_remote::{
    GossipConfig, KeyPair, RegistrationPriority, RemoteActorLocation,
    registry::{GossipRegistry, RegistryChange},
};
use std::{hint::black_box, net::SocketAddr, time::Duration};

async fn make_registry(actor_count: usize, peer_count: usize) -> GossipRegistry<()> {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("bench_gossip_round")),
        gossip_interval: Duration::from_secs(60),
        immediate_propagation_enabled: false,
        ..Default::default()
    };
    let registry = GossipRegistry::new(bind_addr, config);

    for idx in 0..peer_count {
        let peer_addr: SocketAddr = format!("127.0.0.1:{}", 20_000 + idx).parse().unwrap();
        let peer_id = KeyPair::new_for_testing(format!("peer-{idx}")).peer_id();
        registry
            .connection_pool
            .peer_id_to_addr
            .upsert_sync(peer_id.clone(), peer_addr);
        registry
            .connection_pool
            .addr_to_peer_id
            .upsert_sync(peer_addr, peer_id.clone());
        let _ = registry.configure_peer(peer_id, peer_addr).await;
    }

    for idx in 0..actor_count {
        let actor_addr: SocketAddr = format!("127.0.0.1:{}", 30_000 + (idx % 10_000))
            .parse()
            .unwrap();
        let peer_id = KeyPair::new_for_testing(format!("actor-peer-{idx}")).peer_id();
        let mut location = RemoteActorLocation::new_with_peer(actor_addr, peer_id);
        location.priority = RegistrationPriority::Immediate;
        registry
            .actor_state
            .local_actors
            .insert_sync(format!("actor-{idx}"), location.clone())
            .ok();
    }

    {
        let mut gossip_state = registry.gossip_state.lock().await;
        for idx in 0..actor_count {
            let actor_addr: SocketAddr = format!("127.0.0.1:{}", 30_000 + (idx % 10_000))
                .parse()
                .unwrap();
            let peer_id = KeyPair::new_for_testing(format!("actor-peer-{idx}")).peer_id();
            let mut location = RemoteActorLocation::new_with_peer(actor_addr, peer_id);
            location.priority = RegistrationPriority::Immediate;
            gossip_state
                .pending_changes
                .push(RegistryChange::ActorAdded {
                    name: format!("actor-{idx}"),
                    location,
                    priority: RegistrationPriority::Immediate,
                });
        }
    }

    registry
}

fn bench_gossip_round(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("icanact_remote_gossip_round");
    group.sample_size(10);

    for (actors, peers) in [(100usize, 8usize), (1_000usize, 64usize)] {
        group.bench_function(
            format!("prepare_gossip_round_{actors}actors_{peers}peers"),
            |b| {
                b.to_async(&runtime).iter_batched(
                    || async move { make_registry(actors, peers).await },
                    |setup| async move {
                        let registry = setup.await;
                        let tasks = registry.prepare_gossip_round().await.unwrap();
                        black_box(tasks);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_gossip_round);
criterion_main!(benches);
