use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
#[cfg(any(feature = "test-helpers", debug_assertions))]
use icanact_remote::wire_type;
use icanact_remote::{
    AlignedBytes, GossipConfig, GossipRegistryHandle, KeyPair,
    registry::{ActorMessageHandlerSync, ActorResponse},
};
use tokio::sync::Notify;
use tokio::time::sleep;

const BENCH_ACTOR_ID: u64 = 0xC0DE_BEEF;
const BENCH_TYPE_HASH: u32 = 0xA11C_0001;

const WARMUP_MESSAGES: u64 = 1_000;
const MESSAGE_COUNT: u64 = 10_000;
const PAYLOAD_BYTES: usize = 256;
const ASK_BENCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct EchoActor {
    received: Arc<AtomicU64>,
    notify_at: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

#[cfg(any(feature = "test-helpers", debug_assertions))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq, Eq, Clone)]
struct TypedBenchPing {
    id: u64,
    nonce: u64,
}

#[cfg(any(feature = "test-helpers", debug_assertions))]
wire_type!(TypedBenchPing, "icanact.remote.TypedBenchPing");

impl ActorMessageHandlerSync for EchoActor {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u16>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        if actor_id != BENCH_ACTOR_ID || type_hash != BENCH_TYPE_HASH {
            return Ok(None);
        }
        let received = self.received.fetch_add(1, Ordering::Relaxed) + 1;
        let notify_at = self.notify_at.load(Ordering::Relaxed);
        if notify_at != 0 && notify_at == received {
            self.notify.notify_waiters();
        }
        if correlation_id.is_some() {
            Ok(Some(payload.into()))
        } else {
            Ok(None)
        }
    }
}

async fn create_registry(seed: &str, config: GossipConfig) -> GossipRegistryHandle {
    let keypair = KeyPair::new_for_testing(seed);
    GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        keypair.to_secret_key(),
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap()
}

async fn connect_bidirectional(a: &GossipRegistryHandle, b: &GossipRegistryHandle) {
    let b_id = b.registry.peer_id.clone();
    let a_id = a.registry.peer_id.clone();

    let peer_b = a.add_peer(&b_id).await;
    peer_b.connect(&b.registry.bind_addr).await.unwrap();
    let peer_a = b.add_peer(&a_id).await;
    peer_a.connect(&a.registry.bind_addr).await.unwrap();
}

async fn wait_for_received(received: &AtomicU64, notify: &Notify, target: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if received.load(Ordering::Relaxed) >= target {
            return;
        }
        let notified = notify.notified();
        if received.load(Ordering::Relaxed) >= target {
            return;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "receiver did not process expected tell messages"
        );
        tokio::time::timeout(remaining, notified)
            .await
            .expect("receiver did not process expected tell messages");
    }
}

async fn run_actor_ask_inflight_benchmark(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: Arc::new(AtomicU64::new(0)),
            notify_at: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }))
        .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![3u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let remote = remote.clone();
        let payload = payload.clone();
        async move {
            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let remote = remote.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        remote
                            .ask_actor_frame(
                                BENCH_ACTOR_ID,
                                BENCH_TYPE_HASH,
                                payload,
                                ASK_BENCH_TIMEOUT,
                            )
                            .await
                    }
                    .boxed(),
                );
                next += 1;
            }

            while let Some(result) = pending.next().await {
                let reply = result.unwrap();
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < count {
                    let remote = remote.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            remote
                                .ask_actor_frame(
                                    BENCH_ACTOR_ID,
                                    BENCH_TYPE_HASH,
                                    payload,
                                    ASK_BENCH_TIMEOUT,
                                )
                                .await
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }

            checksum
        }
    };

    let _ = drive(WARMUP_MESSAGES).await;
    let start = Instant::now();
    let checksum = drive(ask_count).await;
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] requests={} inflight={} lanes={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={}",
        ask_count,
        inflight,
        1,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

async fn run_direct_ask_no_timeout_inflight_benchmark(
    label: &str,
    inflight: usize,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![11u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let remote = remote.clone();
        let payload = payload.clone();
        async move {
            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let remote = remote.clone();
                let payload = payload.clone();
                pending.push(async move { remote.ask_direct_no_timeout(payload).await }.boxed());
                next += 1;
            }

            while let Some(result) = pending.next().await {
                let reply = result.unwrap();
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < count {
                    let remote = remote.clone();
                    let payload = payload.clone();
                    pending
                        .push(async move { remote.ask_direct_no_timeout(payload).await }.boxed());
                    next += 1;
                }
            }

            checksum
        }
    };

    let _ = drive(WARMUP_MESSAGES).await;
    let start = Instant::now();
    let checksum = drive(ask_count).await;
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] requests={} inflight={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={}",
        ask_count,
        inflight,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

async fn run_actor_ask_no_timeout_inflight_benchmark(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: Arc::new(AtomicU64::new(0)),
            notify_at: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }))
        .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![13u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let remote = remote.clone();
        let payload = payload.clone();
        async move {
            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let remote = remote.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        remote
                            .ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload)
                            .await
                    }
                    .boxed(),
                );
                next += 1;
            }

            while let Some(result) = pending.next().await {
                let reply = result.unwrap();
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < count {
                    let remote = remote.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            remote
                                .ask_actor_frame_no_timeout(
                                    BENCH_ACTOR_ID,
                                    BENCH_TYPE_HASH,
                                    payload,
                                )
                                .await
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }

            checksum
        }
    };

    let _ = drive(WARMUP_MESSAGES).await;
    let start = Instant::now();
    let checksum = drive(ask_count).await;
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] requests={} inflight={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={}",
        ask_count,
        inflight,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

async fn run_actor_ask_outer_timeout_inflight_benchmark(
    label: &str,
    inflight: usize,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: Arc::new(AtomicU64::new(0)),
            notify_at: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }))
        .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![15u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let remote = remote.clone();
        let payload = payload.clone();
        async move {
            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let remote = remote.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        tokio::time::timeout(
                            ASK_BENCH_TIMEOUT,
                            remote.ask_actor_frame_no_timeout(
                                BENCH_ACTOR_ID,
                                BENCH_TYPE_HASH,
                                payload,
                            ),
                        )
                        .await
                        .map_err(|_| icanact_remote::GossipError::Timeout)?
                    }
                    .boxed(),
                );
                next += 1;
            }

            while let Some(result) = pending.next().await {
                let reply = result.unwrap();
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < count {
                    let remote = remote.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            tokio::time::timeout(
                                ASK_BENCH_TIMEOUT,
                                remote.ask_actor_frame_no_timeout(
                                    BENCH_ACTOR_ID,
                                    BENCH_TYPE_HASH,
                                    payload,
                                ),
                            )
                            .await
                            .map_err(|_| icanact_remote::GossipError::Timeout)?
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }

            checksum
        }
    };

    let _ = drive(WARMUP_MESSAGES).await;
    let start = Instant::now();
    let checksum = drive(ask_count).await;
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] requests={} inflight={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={}",
        ask_count,
        inflight,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

async fn run_actor_ask_no_timeout_single_flight_benchmark(label: &str, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: Arc::new(AtomicU64::new(0)),
            notify_at: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }))
        .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(200)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![21u8; PAYLOAD_BYTES]);

    for _ in 0..(WARMUP_MESSAGES / 10) {
        let reply = remote
            .ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone())
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    let start = Instant::now();
    for _ in 0..ask_count {
        let reply = remote
            .ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone())
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] requests={} payload={}B elapsed={:.6}s throughput={:.2} req/s",
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

async fn run_direct_ask_no_timeout_single_flight_benchmark(label: &str, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(200)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![23u8; PAYLOAD_BYTES]);

    for _ in 0..(WARMUP_MESSAGES / 10) {
        let reply = remote.ask_direct_no_timeout(payload.clone()).await.unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    let start = Instant::now();
    for _ in 0..ask_count {
        let reply = remote.ask_direct_no_timeout(payload.clone()).await.unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] requests={} payload={}B elapsed={:.6}s throughput={:.2} req/s",
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[cfg(any(feature = "test-helpers", debug_assertions))]
async fn run_typed_ask_benchmark(label: &str, archived: bool, ask_count: u64) {
    unsafe {
        std::env::set_var("ICANACT_REMOTE_TYPED_ECHO", "1");
    }

    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(200)).await;

    let remote = sender
        .lookup_address(receiver.registry.bind_addr)
        .await
        .unwrap();
    let request = TypedBenchPing {
        id: 42,
        nonce: 0xDEAD_BEEF_CAFE_BABE,
    };

    for _ in 0..(WARMUP_MESSAGES / 10) {
        if archived {
            let response = remote
                .ask_typed_archived::<TypedBenchPing, TypedBenchPing>(&request)
                .await
                .unwrap();
            let archived = response.archived().unwrap();
            assert_eq!(archived.id, request.id);
            assert_eq!(archived.nonce, request.nonce);
        } else {
            let response: TypedBenchPing = remote.ask_typed(&request).await.unwrap();
            assert_eq!(response, request);
        }
    }

    let start = Instant::now();
    for _ in 0..ask_count {
        if archived {
            let response = remote
                .ask_typed_archived::<TypedBenchPing, TypedBenchPing>(&request)
                .await
                .unwrap();
            let archived = response.archived().unwrap();
            assert_eq!(archived.id, request.id);
            assert_eq!(archived.nonce, request.nonce);
        } else {
            let response: TypedBenchPing = remote.ask_typed(&request).await.unwrap();
            assert_eq!(response, request);
        }
    }
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] requests={} payload={}B archived={} elapsed={:.6}s throughput={:.2} req/s",
        ask_count,
        std::mem::size_of::<TypedBenchPing>(),
        archived,
        elapsed.as_secs_f64(),
        req_per_sec
    );

    sender.shutdown().await;
    receiver.shutdown().await;

    unsafe {
        std::env::remove_var("ICANACT_REMOTE_TYPED_ECHO");
    }
}

#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_tell_actor_frame_enqueue_throughput() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry("throughput_tell_receiver", config.clone()).await;
    let sender = create_registry("throughput_tell_sender", config).await;

    let received = Arc::new(AtomicU64::new(0));
    let delivered = Arc::new(Notify::new());
    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: received.clone(),
            notify_at: Arc::new(AtomicU64::new(WARMUP_MESSAGES + MESSAGE_COUNT)),
            notify: delivered.clone(),
        }))
        .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(200)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![0u8; PAYLOAD_BYTES]);

    for _ in 0..WARMUP_MESSAGES {
        remote
            .tell_actor_frame(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone())
            .await
            .unwrap();
    }

    let start = Instant::now();
    for _ in 0..MESSAGE_COUNT {
        remote
            .tell_actor_frame(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone())
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();
    let msg_per_sec = MESSAGE_COUNT as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::tell_enqueue] messages={} payload={}B elapsed={:.6}s throughput={:.2} msg/s",
        MESSAGE_COUNT,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        msg_per_sec
    );

    wait_for_received(
        received.as_ref(),
        delivered.as_ref(),
        WARMUP_MESSAGES + MESSAGE_COUNT,
        Duration::from_secs(5),
    )
    .await;

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_tell_actor_frame_delivered_throughput() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry("throughput_tell_delivered_receiver", config.clone()).await;
    let sender = create_registry("throughput_tell_delivered_sender", config).await;

    let received = Arc::new(AtomicU64::new(0));
    let delivered_target = Arc::new(AtomicU64::new(WARMUP_MESSAGES));
    let delivered = Arc::new(Notify::new());
    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: received.clone(),
            notify_at: delivered_target.clone(),
            notify: delivered.clone(),
        }))
        .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(200)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![0u8; PAYLOAD_BYTES]);

    for _ in 0..WARMUP_MESSAGES {
        remote
            .tell_actor_frame(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone())
            .await
            .unwrap();
    }

    wait_for_received(
        received.as_ref(),
        delivered.as_ref(),
        WARMUP_MESSAGES,
        Duration::from_secs(5),
    )
    .await;

    received.store(0, Ordering::Relaxed);
    delivered_target.store(MESSAGE_COUNT, Ordering::Relaxed);

    let start = Instant::now();
    for _ in 0..MESSAGE_COUNT {
        remote
            .tell_actor_frame(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone())
            .await
            .unwrap();
    }
    wait_for_received(
        received.as_ref(),
        delivered.as_ref(),
        MESSAGE_COUNT,
        Duration::from_secs(5),
    )
    .await;
    let elapsed = start.elapsed();
    let msg_per_sec = MESSAGE_COUNT as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::tell_delivered] messages={} payload={}B elapsed={:.6}s throughput={:.2} msg/s",
        MESSAGE_COUNT,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        msg_per_sec
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_throughput() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry("throughput_ask_receiver", config.clone()).await;
    let sender = create_registry("throughput_ask_sender", config).await;

    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: Arc::new(AtomicU64::new(0)),
            notify_at: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }))
        .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(200)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![1u8; PAYLOAD_BYTES]);
    let timeout = Duration::from_secs(2);

    for _ in 0..(WARMUP_MESSAGES / 10) {
        let reply = remote
            .ask_actor_frame(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone(), timeout)
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    let ask_count = MESSAGE_COUNT / 10;
    let start = Instant::now();
    for _ in 0..ask_count {
        let reply = remote
            .ask_actor_frame(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.clone(), timeout)
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::ask] requests={} payload={}B elapsed={:.6}s throughput={:.2} req/s",
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_direct_throughput() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry("throughput_direct_ask_receiver", config.clone()).await;
    let sender = create_registry("throughput_direct_ask_sender", config).await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(200)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![2u8; PAYLOAD_BYTES]);
    let timeout = Duration::from_secs(2);

    for _ in 0..(WARMUP_MESSAGES / 10) {
        let reply = remote.ask_direct(payload.clone(), timeout).await.unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    let ask_count = MESSAGE_COUNT / 10;
    let start = Instant::now();
    for _ in 0..ask_count {
        let reply = remote.ask_direct(payload.clone(), timeout).await.unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }
    let elapsed = start.elapsed();
    let req_per_sec = ask_count as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::ask_direct] requests={} payload={}B elapsed={:.6}s throughput={:.2} req/s",
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_no_timeout_throughput() {
    run_actor_ask_no_timeout_single_flight_benchmark(
        "ask_actor_no_timeout_single_flight",
        MESSAGE_COUNT / 10,
    )
    .await;
}

#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_direct_no_timeout_throughput() {
    run_direct_ask_no_timeout_single_flight_benchmark(
        "ask_direct_no_timeout_single_flight",
        MESSAGE_COUNT / 10,
    )
    .await;
}

#[cfg(any(feature = "test-helpers", debug_assertions))]
#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_typed_throughput() {
    run_typed_ask_benchmark("ask_typed_single_flight", false, MESSAGE_COUNT / 10).await;
}

#[cfg(any(feature = "test-helpers", debug_assertions))]
#[tokio::test]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_typed_archived_throughput() {
    run_typed_ask_benchmark("ask_typed_archived_single_flight", true, MESSAGE_COUNT / 10).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_inflight512_throughput() {
    run_actor_ask_inflight_benchmark("ask_inflight512", 512, MESSAGE_COUNT * 10).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_inflight64_throughput() {
    run_actor_ask_inflight_benchmark("ask_inflight64", 64, MESSAGE_COUNT).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_direct_no_timeout_inflight512_throughput() {
    run_direct_ask_no_timeout_inflight_benchmark(
        "ask_direct_no_timeout_inflight512",
        512,
        MESSAGE_COUNT * 10,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_no_timeout_inflight512_throughput() {
    run_actor_ask_no_timeout_inflight_benchmark(
        "ask_actor_no_timeout_inflight512",
        512,
        MESSAGE_COUNT * 10,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_outer_timeout_inflight512_throughput() {
    run_actor_ask_outer_timeout_inflight_benchmark(
        "ask_actor_outer_timeout_inflight512",
        512,
        MESSAGE_COUNT * 10,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_no_timeout_inflight_scaling() {
    for (inflight, ask_count) in [
        (1usize, MESSAGE_COUNT / 10),
        (8usize, MESSAGE_COUNT),
        (64usize, MESSAGE_COUNT * 2),
        (512usize, MESSAGE_COUNT * 10),
    ] {
        run_actor_ask_no_timeout_inflight_benchmark(
            &format!("ask_actor_no_timeout_inflight{}", inflight),
            inflight,
            ask_count,
        )
        .await;
    }
}
