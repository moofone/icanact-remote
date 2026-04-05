use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use icanact_remote::{
    AlignedBytes, GossipConfig, GossipRegistryHandle, KeyPair,
    registry::{ActorMessageHandlerSync, ActorResponse},
};
use tokio::time::sleep;

const BENCH_ACTOR_ID: u64 = 0xC0DE_BEEF;
const BENCH_TYPE_HASH: u32 = 0xA11C_0001;

const WARMUP_MESSAGES: u64 = 1_000;
const MESSAGE_COUNT: u64 = 10_000;
const PAYLOAD_BYTES: usize = 256;

#[derive(Clone)]
struct EchoActor {
    received: Arc<AtomicU64>,
}

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
        self.received.fetch_add(1, Ordering::Relaxed);
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

#[tokio::test]
async fn test_tell_actor_frame_throughput() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry("throughput_tell_receiver", config.clone()).await;
    let sender = create_registry("throughput_tell_sender", config).await;

    let received = Arc::new(AtomicU64::new(0));
    receiver
        .registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received: received.clone(),
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
        "[throughput_benchmarks::tell] messages={} payload={}B elapsed={:.6}s throughput={:.2} msg/s",
        MESSAGE_COUNT,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        msg_per_sec
    );

    // Ensure receiver processed at least warmup+benchmark messages.
    let expected_min = WARMUP_MESSAGES + MESSAGE_COUNT;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if received.load(Ordering::Relaxed) >= expected_min {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "receiver did not process expected tell messages"
        );
        sleep(Duration::from_millis(10)).await;
    }

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
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
