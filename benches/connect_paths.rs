use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair};
use std::{hint::black_box, time::Duration};

async fn create_registry(keypair: KeyPair) -> GossipRegistryHandle {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(60),
        ..Default::default()
    };
    GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        keypair.to_secret_key(),
        Some(config),
        BuilderTlsBootstrap,
    )
    .await
    .unwrap()
}

fn key_pair_ordered_for_outbound_a(seed_a: &str, seed_b: &str) -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing(seed_a);
    let second = KeyPair::new_for_testing(seed_b);
    if first
        .peer_id()
        .to_node_id()
        .as_bytes()
        .cmp(second.peer_id().to_node_id().as_bytes())
        .is_lt()
    {
        (first, second)
    } else {
        (second, first)
    }
}

fn bench_connect_paths(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("icanact_remote_connect_paths");
    group.sample_size(10);

    group.bench_function("lookup_peer_cold_connect", |b| {
        b.to_async(&runtime).iter_batched(
            || async {
                let (client_keypair, server_keypair) =
                    key_pair_ordered_for_outbound_a("bench_connect_client", "bench_connect_server");
                let server = create_registry(server_keypair).await;
                let client = create_registry(client_keypair).await;
                let peer = client.add_peer(&server.registry.peer_id).await;
                (client, server, peer)
            },
            |setup| async move {
                let (client, server, peer) = setup.await;
                peer.connect(&server.registry.bind_addr).await.unwrap();
                let remote = client.lookup_peer(&server.registry.peer_id).await.unwrap();
                black_box(remote);
                client.shutdown().await;
                server.shutdown().await;
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_connect_paths);
criterion_main!(benches);
