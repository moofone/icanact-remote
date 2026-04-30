use criterion::{Criterion, criterion_group, criterion_main};
use icanact_remote::{
    GossipConfig, KeyPair, PubSubDeliveryPolicy, PubSubScope, RoutedPubSub, WireType, topic_key,
};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug)]
struct BenchMsg {
    v: u64,
}

icanact_remote::wire_type!(
    BenchMsg,
    "icanact-remote.benches.pubsub_hotpath.BenchMsg/v1"
);

async fn setup() -> Arc<RoutedPubSub> {
    let cfg = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("pubsub-hotpath")),
        gossip_interval: Duration::from_secs(60),
        ..Default::default()
    };
    let registry = Arc::new(icanact_remote::registry::GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        cfg,
    ));
    RoutedPubSub::install(registry).await
}

fn bench_pubsub_hotpath(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let pubsub = runtime.block_on(setup());
    let topic = topic_key("bench.pubsub.hotpath");
    let policy = PubSubDeliveryPolicy::default();

    c.bench_function("pubsub_no_interest_short_circuit", |b| {
        b.iter(|| {
            let stats = pubsub
                .publish_typed(
                    black_box(topic),
                    black_box(&BenchMsg { v: 1 }),
                    PubSubScope::AutoExternal,
                    policy,
                )
                .unwrap();
            black_box(stats);
        });
    });

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = Arc::clone(&delivered);
    pubsub.subscribe_type_bytes(BenchMsg::TYPE_HASH, move |_topic, payload| {
        if let Ok(msg) = icanact_remote::decode_typed::<BenchMsg>(payload.as_ref()) {
            delivered_clone.fetch_add(msg.v, Ordering::Relaxed);
        }
    });

    c.bench_function("pubsub_local_type_delivery", |b| {
        b.iter(|| {
            let stats = pubsub
                .publish_typed(
                    black_box(topic),
                    black_box(&BenchMsg { v: 1 }),
                    PubSubScope::LocalOnly,
                    policy,
                )
                .unwrap();
            black_box(stats);
        });
    });

    let missing_peer = KeyPair::new_for_testing("pubsub-missing-peer").peer_id();
    c.bench_function("pubsub_selected_peer_route_miss", |b| {
        b.iter(|| {
            let stats = pubsub
                .publish_typed(
                    black_box(topic),
                    black_box(&BenchMsg { v: 1 }),
                    PubSubScope::SelectedPeers(vec![missing_peer.clone()]),
                    policy,
                )
                .unwrap();
            black_box(stats);
        });
    });

    black_box(delivered.load(Ordering::Relaxed));
}

criterion_group!(benches, bench_pubsub_hotpath);
criterion_main!(benches);
