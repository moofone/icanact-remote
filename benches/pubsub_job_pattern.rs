use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use criterion::{Criterion, criterion_group, criterion_main};
use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, PubSubDeliveryPolicy,
    PubSubScope, RoutedPubSub, WireType, topic_key,
};

const TOPIC: &str = "icemining.job-broadcast.v1.bench";
const JOB_TEMPLATE_BYTES: &[u8] = br#"{"job_id":"18427197330065254582","height":2732,"topoheight":2939,"seed_hash":"4f8a3c8a0f4d1ad7b56f5f08b48d7a6f1ab64002a20d77c8b2397fc806dd02bf","block_template_blob":"0100b7eafc9506c8f908a822bd9a74a47b8e38cd1db4dbb90c7f7b7d203aa623a79a36fb8dd7f1e4c0d19b9afc1df6f59c8920e1dc15a04f7caa48f99bf1f1f0e5a83b3fbf0cc8fb2a4a60000000000000000000000000000000000000000000000000000000000000000","miner_blob_prefix":"0100b7eafc9506c8f908a822bd9a74a47b8e38cd1db4dbb90c7f7b7d203aa623a79a36fb8dd7f1e4c0d19b9afc1df6f59c8920e1dc15a04f7caa48f99bf1f1f0e5a83b3fbf0cc8fb2a4a6","miner_blob_suffix":"0000000000000000000000000000000000000000000000000000000000000000","network_target":"00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff","share_target":"0000007fffffffffffffffffffffffffffffffffffffffffffffffffffffffff","extra_nonce_offset":80,"extra_nonce_size":4}"#;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
    static DEALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

struct CountingAllocator;

// SAFETY: This wrapper delegates all allocation behavior to `System` and only
// records relaxed counters around the measured hot path.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNT_ALLOCATIONS.with(|count| {
            if count.get() {
                ALLOCATIONS.with(|allocs| allocs.set(allocs.get().saturating_add(1)));
                ALLOCATED_BYTES
                    .with(|bytes| bytes.set(bytes.get().saturating_add(layout.size() as u64)));
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        COUNT_ALLOCATIONS.with(|count| {
            if count.get() {
                DEALLOCATIONS.with(|deallocs| deallocs.set(deallocs.get().saturating_add(1)));
                black_box(layout.size());
            }
        });
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Eq, PartialEq)]
struct JobBroadcastV1 {
    coin_id: u32,
    algo_id: u16,
    epoch: u64,
    job_id: u64,
    source_template_id: u64,
    priority_key: [u64; 4],
    dedupe_key: u64,
    clean_jobs: bool,
    payload_kind: u16,
    payload: Vec<u8>,
    proxy_observed_at_ns: u64,
    proxy_published_at_ns: u64,
}

icanact_remote::wire_type!(JobBroadcastV1, "icemining.coin_proxy.JobBroadcast/v1");

struct BenchMesh {
    _publisher: GossipRegistryHandle,
    _subscriber: GossipRegistryHandle,
    publisher_pubsub: Arc<RoutedPubSub>,
    _subscriber_pubsub: Arc<RoutedPubSub>,
    delivered: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
}

async fn create_registry(keypair: KeyPair) -> GossipRegistryHandle {
    let config = GossipConfig {
        key_pair: Some(keypair.clone()),
        gossip_interval: Duration::from_millis(25),
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

async fn setup_mesh(decode_on_receive: bool) -> Arc<BenchMesh> {
    let (publisher_key, subscriber_key) =
        key_pair_ordered_for_outbound_a("pubsub-job-pattern-publisher", "pubsub-job-pattern-sub");
    let publisher = create_registry(publisher_key).await;
    let subscriber = create_registry(subscriber_key).await;
    let publisher_pubsub = RoutedPubSub::install(Arc::clone(&publisher.registry)).await;
    let subscriber_pubsub = RoutedPubSub::install(Arc::clone(&subscriber.registry)).await;

    let delivered = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let topic = topic_key(TOPIC);
    let delivered_for_sub = Arc::clone(&delivered);
    let checksum_for_sub = Arc::clone(&checksum);
    subscriber_pubsub.subscribe_borrowed_bytes(topic, JobBroadcastV1::TYPE_HASH, move |payload| {
        if decode_on_receive {
            let msg = icanact_remote::decode_typed::<JobBroadcastV1>(payload).unwrap();
            checksum_for_sub.fetch_add(
                msg.epoch ^ msg.job_id ^ msg.proxy_published_at_ns ^ u64::from(msg.coin_id),
                Ordering::Relaxed,
            );
        } else {
            checksum_for_sub.fetch_add(
                payload.len() as u64 + u64::from(payload.first().copied().unwrap_or_default()),
                Ordering::Relaxed,
            );
        }
        delivered_for_sub.fetch_add(1, Ordering::Release);
    });

    let peer = publisher.add_peer(&subscriber.registry.peer_id).await;
    peer.connect(&subscriber.registry.bind_addr).await.unwrap();

    let mesh = Arc::new(BenchMesh {
        _publisher: publisher,
        _subscriber: subscriber,
        publisher_pubsub,
        _subscriber_pubsub: subscriber_pubsub,
        delivered,
        checksum,
    });
    wait_for_pubsub_route(&mesh, encoded_job_payload()).await;
    mesh
}

fn encoded_job_payload() -> Bytes {
    let payload = icanact_remote::typed::encode_typed_pooled(&JobBroadcastV1 {
        coin_id: 2_201_068_882,
        algo_id: 1,
        epoch: 42,
        job_id: 18_427_197_330_065_254_582,
        source_template_id: 18_427_197_330_065_254_582,
        priority_key: [2732, 2939, 18_427_197_330_065_254_582, 42],
        dedupe_key: 0x6c1f_0b7b_8e31_5a44,
        clean_jobs: true,
        payload_kind: 1,
        payload: JOB_TEMPLATE_BYTES.to_vec(),
        proxy_observed_at_ns: 999_990_000,
        proxy_published_at_ns: 1_000_000_000,
    })
    .unwrap();
    let (mut payload, prefix, payload_len) =
        icanact_remote::typed::typed_payload_parts::<JobBroadcastV1>(payload);
    let mut wire = bytes::BytesMut::with_capacity(payload_len);
    if let Some(prefix) = prefix {
        wire.extend_from_slice(&prefix);
    }
    let payload_bytes = payload.copy_to_bytes(payload.remaining());
    wire.extend_from_slice(payload_bytes.as_ref());
    wire.freeze()
}

async fn wait_for_pubsub_route(mesh: &Arc<BenchMesh>, payload: Bytes) {
    let topic = topic_key(TOPIC);
    let policy = PubSubDeliveryPolicy::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let before = mesh.delivered.load(Ordering::Acquire);
        let stats = mesh
            .publisher_pubsub
            .publish_remote_bytes(
                topic,
                JobBroadcastV1::TYPE_HASH,
                Bytes::clone(&payload),
                PubSubScope::AutoExternal,
                policy,
            )
            .unwrap();
        if stats.remote_enqueued > 0
            && wait_for_delivery(&mesh.delivered, before, Duration::from_millis(250)).await
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("routed pubsub job-pattern benchmark did not converge");
}

async fn wait_for_delivery(delivered: &AtomicU64, previous: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if delivered.load(Ordering::Acquire) > previous {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

fn reset_allocation_counters() {
    ALLOCATIONS.with(|v| v.set(0));
    ALLOCATED_BYTES.with(|v| v.set(0));
    DEALLOCATIONS.with(|v| v.set(0));
}

fn start_allocation_counting() {
    reset_allocation_counters();
    COUNT_ALLOCATIONS.with(|v| v.set(true));
}

fn stop_allocation_counting() -> (u64, u64, u64) {
    COUNT_ALLOCATIONS.with(|v| v.set(false));
    (
        ALLOCATIONS.with(Cell::get),
        ALLOCATED_BYTES.with(Cell::get),
        DEALLOCATIONS.with(Cell::get),
    )
}

fn add_alloc_counts(total: &mut (u64, u64, u64), sample: (u64, u64, u64)) {
    total.0 = total.0.saturating_add(sample.0);
    total.1 = total.1.saturating_add(sample.1);
    total.2 = total.2.saturating_add(sample.2);
}

async fn run_single_flight_loop(
    mesh: &Arc<BenchMesh>,
    payload: Bytes,
    iters: u64,
) -> (Duration, (u64, u64, u64)) {
    let topic = topic_key(TOPIC);
    let policy = PubSubDeliveryPolicy::default();
    let count_publish_allocs = COUNT_ALLOCATIONS.with(Cell::get);
    if count_publish_allocs {
        COUNT_ALLOCATIONS.with(|v| v.set(false));
    }
    let mut publish_allocs = (0, 0, 0);
    let start = Instant::now();
    for _ in 0..iters {
        let before = mesh.delivered.load(Ordering::Acquire);
        if count_publish_allocs {
            start_allocation_counting();
        }
        let stats = mesh
            .publisher_pubsub
            .publish_remote_bytes(
                topic,
                JobBroadcastV1::TYPE_HASH,
                Bytes::clone(&payload),
                PubSubScope::AutoExternal,
                policy,
            )
            .unwrap();
        if count_publish_allocs {
            add_alloc_counts(&mut publish_allocs, stop_allocation_counting());
        }
        assert_eq!(
            stats.remote_attempted, 1,
            "hot path must have one cached route"
        );
        assert_eq!(
            stats.remote_enqueued, 1,
            "hot path must enqueue to cached route"
        );
        assert_eq!(stats.remote_full, 0, "hot path write queue filled");
        assert_eq!(stats.remote_transport_errors, 0, "hot path transport error");
        assert!(
            wait_for_delivery(&mesh.delivered, before, Duration::from_secs(1)).await,
            "pubsub job-pattern delivery timed out"
        );
    }
    (start.elapsed(), publish_allocs)
}

async fn prime_single_flight(mesh: &Arc<BenchMesh>, payload: Bytes) {
    let _ = run_single_flight_loop(mesh, payload, 64).await;
}

fn bench_pubsub_job_pattern(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("icanact_remote_pubsub_job_pattern");
    group.sample_size(10);

    let raw_mesh = runtime.block_on(setup_mesh(false));
    let raw_payload = encoded_job_payload();
    group.bench_function("direct_routed_pubsub_preencoded_bytes_single_flight", |b| {
        b.to_async(&runtime).iter_custom(|iters| {
            let mesh = Arc::clone(&raw_mesh);
            let payload = Bytes::clone(&raw_payload);
            async move {
                prime_single_flight(&mesh, Bytes::clone(&payload)).await;
                COUNT_ALLOCATIONS.with(|v| v.set(true));
                let (elapsed, (allocs, bytes, deallocs)) =
                    run_single_flight_loop(&mesh, payload, iters).await;
                COUNT_ALLOCATIONS.with(|v| v.set(false));
                eprintln!(
                    "[pubsub_job_pattern::preencoded_bytes_publish_enqueue] iters={iters} allocs={allocs} allocated_bytes={bytes} deallocs={deallocs} allocs_per_iter={:.3}",
                    allocs as f64 / iters.max(1) as f64
                );
                if std::env::var_os("ICANACT_PUBSUB_ASSERT_ZERO_ALLOC").is_some() {
                    assert_eq!(allocs, 0, "routed pubsub preencoded hot path allocated");
                }
                black_box(mesh.checksum.load(Ordering::Relaxed));
                elapsed
            }
        });
    });

    let decode_mesh = runtime.block_on(setup_mesh(true));
    let decode_payload = encoded_job_payload();
    group.bench_function("direct_routed_pubsub_job_decode_single_flight", |b| {
        b.to_async(&runtime).iter_custom(|iters| {
            let mesh = Arc::clone(&decode_mesh);
            let payload = Bytes::clone(&decode_payload);
            async move {
                prime_single_flight(&mesh, Bytes::clone(&payload)).await;
                COUNT_ALLOCATIONS.with(|v| v.set(true));
                let (elapsed, (allocs, bytes, deallocs)) =
                    run_single_flight_loop(&mesh, payload, iters).await;
                COUNT_ALLOCATIONS.with(|v| v.set(false));
                eprintln!(
                    "[pubsub_job_pattern::job_decode_publish_enqueue] iters={iters} allocs={allocs} allocated_bytes={bytes} deallocs={deallocs} allocs_per_iter={:.3}",
                    allocs as f64 / iters.max(1) as f64
                );
                if std::env::var_os("ICANACT_PUBSUB_ASSERT_ZERO_ALLOC").is_some() {
                    assert_eq!(allocs, 0, "routed pubsub job decode hot path allocated");
                }
                black_box(mesh.checksum.load(Ordering::Relaxed));
                elapsed
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_pubsub_job_pattern);
criterion_main!(benches);
