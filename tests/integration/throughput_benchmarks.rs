use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
#[cfg(any(feature = "test-helpers", debug_assertions))]
use icanact_remote::wire_type;
use icanact_remote::{
    AlignedBytes, AskContext, AskForwarder, GossipConfig, GossipRegistryHandle, KeyPair,
    RemoteConnection,
    registry::{
        ActorAskHandlerSync, ActorAskImmediateHandlerSync, ActorMessageFuture, ActorMessageHandler,
        ActorMessageHandlerSync, ActorResponse, ActorTellHandlerSync, AskDisposition,
    },
};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, mpsc};
use tokio::time::sleep;

const BENCH_ACTOR_ID: u64 = 0xC0DE_BEEF;
const BENCH_TYPE_HASH: u32 = 0xA11C_0001;
const ASYNC_PROXY_ACTOR_ID: u64 = 0xC0DE_F44E;
const ASYNC_PROXY_TYPE_HASH: u32 = 0xA11C_0006;
const ALIGNED_TIMEOUT_PROXY_ACTOR_ID: u64 = 0xC0DE_F88E;
const ALIGNED_TIMEOUT_PROXY_TYPE_HASH: u32 = 0xA11C_000A;
const DEFERRED_PROXY_ACTOR_ID: u64 = 0xC0DE_F11E;
const DEFERRED_PROXY_TYPE_HASH: u32 = 0xA11C_0003;
const DEFERRED_TIMEOUT_PROXY_ACTOR_ID: u64 = 0xC0DE_F77E;
const DEFERRED_TIMEOUT_PROXY_TYPE_HASH: u32 = 0xA11C_0009;
const NONBLOCKING_DEFERRED_PROXY_ACTOR_ID: u64 = 0xC0DE_F33E;
const NONBLOCKING_DEFERRED_PROXY_TYPE_HASH: u32 = 0xA11C_0005;
const FORWARDER_PROXY_ACTOR_ID: u64 = 0xC0DE_F55E;
const FORWARDER_PROXY_TYPE_HASH: u32 = 0xA11C_0007;
const DROPPING_FORWARDER_PROXY_ACTOR_ID: u64 = 0xC0DE_F5DE;
const DROPPING_FORWARDER_PROXY_TYPE_HASH: u32 = 0xA11C_000E;
const BOUND_FORWARDER_PROXY_ACTOR_ID: u64 = 0xC0DE_FA5E;
const BOUND_FORWARDER_PROXY_TYPE_HASH: u32 = 0xA11C_000C;
const BOUND_TIMEOUT_PROXY_ACTOR_ID: u64 = 0xC0DE_FB6E;
const BOUND_TIMEOUT_PROXY_TYPE_HASH: u32 = 0xA11C_000D;
const OUTER_TIMEOUT_PROXY_ACTOR_ID: u64 = 0xC0DE_F99E;
const OUTER_TIMEOUT_PROXY_TYPE_HASH: u32 = 0xA11C_000B;
const TIMEOUT_PROXY_ACTOR_ID: u64 = 0xC0DE_F66E;
const TIMEOUT_PROXY_TYPE_HASH: u32 = 0xA11C_0008;
const WORKER_PROXY_ACTOR_ID: u64 = 0xC0DE_F22E;
const WORKER_PROXY_TYPE_HASH: u32 = 0xA11C_0004;
const PROXY_ACTOR_ID: u64 = 0xC0DE_F00D;
const PROXY_TYPE_HASH: u32 = 0xA11C_0002;

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

#[derive(Clone)]
struct SplitEchoActor {
    received: Arc<AtomicU64>,
    notify_at: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

#[derive(Clone)]
struct ProxyAskActor {
    destination: RemoteConnection,
}

#[derive(Clone)]
struct TimeoutProxyAskActor {
    destination: RemoteConnection,
}

#[derive(Clone)]
struct AlignedTimeoutProxyAskActor {
    destination: RemoteConnection,
}

#[derive(Clone)]
struct OuterTimeoutProxyAskActor {
    destination: RemoteConnection,
}

#[derive(Clone)]
struct DeferredEchoProxyActor;

#[derive(Clone)]
struct DeferredTimeoutProxyAskActor {
    destination: RemoteConnection,
}

#[derive(Clone)]
struct NonblockingDeferredEchoProxyActor;

#[derive(Clone)]
struct AsyncProxyActor {
    destination: RemoteConnection,
}

#[derive(Clone)]
struct WorkerProxyAskActor {
    tx: mpsc::UnboundedSender<(Bytes, icanact_remote::AskResponder)>,
}

#[derive(Clone)]
struct ForwarderProxyAskActor {
    destination: RemoteConnection,
    forwarder: AskForwarder,
}

#[derive(Clone)]
struct DroppingForwarderProxyAskActor {
    destination: RemoteConnection,
    forwarder: Arc<Mutex<Option<AskForwarder>>>,
}

#[derive(Clone)]
struct BoundForwarderProxyAskActor {
    workers: Arc<Vec<mpsc::Sender<(Bytes, icanact_remote::AskResponder)>>>,
    next_worker: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct BoundTimeoutProxyAskActor {
    workers: Arc<Vec<mpsc::Sender<(Bytes, icanact_remote::AskResponder)>>>,
    next_worker: Arc<AtomicUsize>,
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

impl ActorTellHandlerSync for SplitEchoActor {
    fn handle_actor_tell_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        _payload: AlignedBytes,
    ) -> icanact_remote::Result<()> {
        if actor_id != BENCH_ACTOR_ID || type_hash != BENCH_TYPE_HASH {
            return Ok(());
        }
        let received = self.received.fetch_add(1, Ordering::Relaxed) + 1;
        let notify_at = self.notify_at.load(Ordering::Relaxed);
        if notify_at != 0 && notify_at == received {
            self.notify.notify_waiters();
        }
        Ok(())
    }
}

impl ActorAskImmediateHandlerSync for SplitEchoActor {
    fn handle_actor_ask_sync_immediate(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != BENCH_ACTOR_ID || type_hash != BENCH_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }
        let received = self.received.fetch_add(1, Ordering::Relaxed) + 1;
        let notify_at = self.notify_at.load(Ordering::Relaxed);
        if notify_at != 0 && notify_at == received {
            self.notify.notify_waiters();
        }
        Ok(AskDisposition::ImmediateAligned(payload))
    }
}

impl ActorAskHandlerSync for ProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != PROXY_ACTOR_ID || type_hash != PROXY_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let destination = self.destination.clone();
        let responder = context.responder();
        let payload = payload.into_bytes();
        tokio::spawn(async move {
            if let Ok(reply) = destination
                .ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload)
                .await
            {
                let _ = responder.try_reply_bytes(reply);
            }
        });

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for DeferredEchoProxyActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != DEFERRED_PROXY_ACTOR_ID || type_hash != DEFERRED_PROXY_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let responder = context.responder();
        let payload = payload.into_bytes();
        tokio::spawn(async move {
            let _ = responder.reply_bytes(payload).await;
        });

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for TimeoutProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != TIMEOUT_PROXY_ACTOR_ID || type_hash != TIMEOUT_PROXY_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let destination = self.destination.clone();
        let responder = context.responder();
        let payload = payload.into_bytes();
        tokio::spawn(async move {
            if let Ok(reply) = destination
                .ask_actor_frame(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload, ASK_BENCH_TIMEOUT)
                .await
            {
                let _ = responder.try_reply_bytes(reply);
            }
        });

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for AlignedTimeoutProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != ALIGNED_TIMEOUT_PROXY_ACTOR_ID
            || type_hash != ALIGNED_TIMEOUT_PROXY_TYPE_HASH
        {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let destination = self.destination.clone();
        let responder = context.responder();
        let payload = payload.into_bytes();
        tokio::spawn(async move {
            if let Ok(reply) = destination
                .ask_actor_frame_aligned(
                    BENCH_ACTOR_ID,
                    BENCH_TYPE_HASH,
                    payload,
                    ASK_BENCH_TIMEOUT,
                )
                .await
            {
                let _ = responder.try_reply_bytes(reply.into_bytes());
            }
        });

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for OuterTimeoutProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != OUTER_TIMEOUT_PROXY_ACTOR_ID || type_hash != OUTER_TIMEOUT_PROXY_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let destination = self.destination.clone();
        let responder = context.responder();
        let payload = payload.into_bytes();
        tokio::spawn(async move {
            if let Ok(Ok(reply)) = tokio::time::timeout(
                ASK_BENCH_TIMEOUT,
                destination.ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload),
            )
            .await
            {
                let _ = responder.try_reply_bytes(reply);
            }
        });

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for DeferredTimeoutProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != DEFERRED_TIMEOUT_PROXY_ACTOR_ID
            || type_hash != DEFERRED_TIMEOUT_PROXY_TYPE_HASH
        {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let destination = self.destination.clone();
        let responder = context.responder();
        let payload = payload.into_bytes();
        tokio::spawn(async move {
            if let Ok(pending) = destination
                .ask_actor_frame_deferred(
                    BENCH_ACTOR_ID,
                    BENCH_TYPE_HASH,
                    payload,
                    ASK_BENCH_TIMEOUT,
                )
                .await
                && let Ok(reply) = pending.wait().await
            {
                let _ = responder.try_reply_bytes(reply);
            }
        });

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for NonblockingDeferredEchoProxyActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != NONBLOCKING_DEFERRED_PROXY_ACTOR_ID
            || type_hash != NONBLOCKING_DEFERRED_PROXY_TYPE_HASH
        {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        context.responder().try_reply_bytes(payload.into_bytes())?;
        Ok(AskDisposition::Deferred)
    }
}

impl ActorMessageHandler for AsyncProxyActor {
    fn handle_actor_message(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let destination = self.destination.clone();
        Box::pin(async move {
            if actor_id != ASYNC_PROXY_ACTOR_ID || type_hash != ASYNC_PROXY_TYPE_HASH {
                return Ok(None);
            }
            if correlation_id.is_none() {
                return Ok(None);
            }
            let reply = destination
                .ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload.into_bytes())
                .await?;
            Ok(Some(ActorResponse::Bytes(reply)))
        })
    }
}

impl ActorAskHandlerSync for WorkerProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != WORKER_PROXY_ACTOR_ID || type_hash != WORKER_PROXY_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        self.tx
            .send((payload.into_bytes(), context.responder()))
            .map_err(|_| icanact_remote::GossipError::Shutdown)?;

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for ForwarderProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != FORWARDER_PROXY_ACTOR_ID || type_hash != FORWARDER_PROXY_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        self.forwarder.try_forward_actor_ask_no_timeout(
            self.destination.clone(),
            BENCH_ACTOR_ID,
            BENCH_TYPE_HASH,
            payload.into_bytes(),
            context.responder(),
        )?;

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for BoundForwarderProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != BOUND_FORWARDER_PROXY_ACTOR_ID
            || type_hash != BOUND_FORWARDER_PROXY_TYPE_HASH
        {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let worker_idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[worker_idx]
            .try_send((payload.into_bytes(), context.responder()))
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => icanact_remote::GossipError::WriteQueueFull,
                mpsc::error::TrySendError::Closed(_) => icanact_remote::GossipError::Shutdown,
            })?;
        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for DroppingForwarderProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != DROPPING_FORWARDER_PROXY_ACTOR_ID
            || type_hash != DROPPING_FORWARDER_PROXY_TYPE_HASH
        {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let forwarder = {
            let mut guard = self.forwarder.lock().expect("forwarder mutex poisoned");
            guard.take().ok_or(icanact_remote::GossipError::Shutdown)?
        };

        forwarder.try_forward_actor_ask_no_timeout(
            self.destination.clone(),
            BENCH_ACTOR_ID,
            BENCH_TYPE_HASH,
            payload.into_bytes(),
            context.responder(),
        )?;
        drop(forwarder);

        Ok(AskDisposition::Deferred)
    }
}

impl ActorAskHandlerSync for BoundTimeoutProxyAskActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if actor_id != BOUND_TIMEOUT_PROXY_ACTOR_ID || type_hash != BOUND_TIMEOUT_PROXY_TYPE_HASH {
            return Ok(AskDisposition::Immediate(
                ActorResponse::Bytes(Bytes::new()),
            ));
        }

        let worker_idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[worker_idx]
            .try_send((payload.into_bytes(), context.responder()))
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => icanact_remote::GossipError::WriteQueueFull,
                mpsc::error::TrySendError::Closed(_) => icanact_remote::GossipError::Shutdown,
            })?;
        Ok(AskDisposition::Deferred)
    }
}

async fn create_registry(seed: &str, config: GossipConfig) -> GossipRegistryHandle {
    let keypair = testing_keypair(seed);
    GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        keypair.to_secret_key(),
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap()
}

fn testing_keypair(seed: &str) -> KeyPair {
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&Sha256::digest(seed.as_bytes()));
    KeyPair::from_private_key_bytes(&digest).expect("sha256 must produce 32 bytes")
}

fn bench_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn bench_env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|v| match v.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

async fn connect_bidirectional(a: &GossipRegistryHandle, b: &GossipRegistryHandle) {
    let b_id = b.registry.peer_id.clone();
    let a_id = a.registry.peer_id.clone();

    let peer_b = a.add_peer(&b_id).await;
    peer_b.connect(&b.registry.bind_addr).await.unwrap();
    let peer_a = b.add_peer(&a_id).await;
    peer_a.connect(&a.registry.bind_addr).await.unwrap();
}

async fn connect_unidirectional(from: &GossipRegistryHandle, to: &GossipRegistryHandle) {
    let peer = from.add_peer(&to.registry.peer_id).await;
    peer.connect(&to.registry.bind_addr).await.unwrap();
}

async fn run_connect_to_peer_contention_benchmark(label: &str, lanes: usize, rounds: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    let peer_id = receiver.registry.peer_id.clone();
    let target_addr = receiver.registry.bind_addr;
    let _ = sender
        .registry
        .connection_pool
        .peer_id_to_addr
        .upsert_sync(peer_id.clone(), target_addr);

    let restore_mapping =
        bench_env_bool("ICANACT_REMOTE_CONNECT_CONTENTION_RESTORE_MAPPING", false);
    let drive = |count: u64| {
        run_connect_to_peer_contention_rounds(
            sender.registry.clone(),
            receiver.registry.clone(),
            peer_id.clone(),
            target_addr,
            lanes,
            count,
            restore_mapping,
        )
    };

    let warmup_rounds = bench_env_u64("ICANACT_REMOTE_CONNECT_CONTENTION_WARMUP", 3);
    let _ = drive(warmup_rounds).await;

    let start = Instant::now();
    let (checksum, errors) = drive(rounds).await;
    let elapsed = start.elapsed();
    let ops = rounds * lanes as u64;
    let successful_ops = ops.saturating_sub(errors);
    let ops_per_sec = successful_ops as f64 / elapsed.as_secs_f64();
    let attempted_ops_per_sec = ops as f64 / elapsed.as_secs_f64();

    println!(
        "[throughput_benchmarks::{label}] rounds={} lanes={} operations={} successful_operations={} restore_mapping={} elapsed={:.6}s throughput={:.2} ops/s attempted_throughput={:.2} ops/s errors={} checksum={}",
        rounds,
        lanes,
        ops,
        successful_ops,
        restore_mapping,
        elapsed.as_secs_f64(),
        ops_per_sec,
        attempted_ops_per_sec,
        errors,
        checksum
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

async fn run_connect_to_peer_contention_rounds(
    sender_registry: Arc<icanact_remote::registry::GossipRegistry>,
    receiver_registry: Arc<icanact_remote::registry::GossipRegistry>,
    peer_id: icanact_remote::PeerId,
    target_addr: std::net::SocketAddr,
    lanes: usize,
    count: u64,
    restore_mapping: bool,
) -> (u64, u64) {
    let mut checksum = 0u64;
    let mut errors = 0u64;
    for _ in 0..count {
        sender_registry
            .connection_pool
            .disconnect_connection_by_peer_id(&peer_id);
        receiver_registry.connection_pool.close_all_connections();
        if restore_mapping {
            let _ = sender_registry
                .connection_pool
                .peer_id_to_addr
                .upsert_sync(peer_id.clone(), target_addr);
        }
        sleep(Duration::from_millis(20)).await;

        let mut pending = FuturesUnordered::new();
        for _ in 0..lanes {
            let sender_registry = sender_registry.clone();
            let peer_id = peer_id.clone();
            pending.push(async move { sender_registry.connect_to_peer(&peer_id).await });
        }

        while let Some(result) = pending.next().await {
            if result.is_err() {
                errors += 1;
            }
        }

        checksum = checksum
            .wrapping_add(sender_registry.connection_pool.connection_count() as u64)
            .wrapping_add(receiver_registry.connection_pool.connection_count() as u64);
    }
    (checksum, errors)
}

async fn register_echo_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    received: Arc<AtomicU64>,
    notify_at: Arc<AtomicU64>,
    notify: Arc<Notify>,
) {
    registry
        .set_actor_message_handler_sync(Arc::new(EchoActor {
            received,
            notify_at,
            notify,
        }))
        .await;
}

async fn register_split_echo_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    received: Arc<AtomicU64>,
    notify_at: Arc<AtomicU64>,
    notify: Arc<Notify>,
) {
    let handler = Arc::new(SplitEchoActor {
        received,
        notify_at,
        notify,
    });
    registry.set_actor_tell_handler_sync(handler.clone()).await;
    registry.set_actor_ask_immediate_handler_sync(handler).await;
}

async fn register_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
) {
    registry
        .set_actor_ask_handler_sync(Arc::new(ProxyAskActor { destination }))
        .await;
}

async fn register_deferred_echo_proxy_actor(registry: &icanact_remote::registry::GossipRegistry) {
    registry
        .set_actor_ask_handler_sync(Arc::new(DeferredEchoProxyActor))
        .await;
}

async fn register_timeout_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
) {
    registry
        .set_actor_ask_handler_sync(Arc::new(TimeoutProxyAskActor { destination }))
        .await;
}

async fn register_aligned_timeout_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
) {
    registry
        .set_actor_ask_handler_sync(Arc::new(AlignedTimeoutProxyAskActor { destination }))
        .await;
}

async fn register_outer_timeout_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
) {
    registry
        .set_actor_ask_handler_sync(Arc::new(OuterTimeoutProxyAskActor { destination }))
        .await;
}

async fn register_deferred_timeout_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
) {
    registry
        .set_actor_ask_handler_sync(Arc::new(DeferredTimeoutProxyAskActor { destination }))
        .await;
}

async fn register_nonblocking_deferred_echo_proxy_actor(
    registry: &icanact_remote::registry::GossipRegistry,
) {
    registry
        .set_actor_ask_handler_sync(Arc::new(NonblockingDeferredEchoProxyActor))
        .await;
}

async fn register_async_proxy_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
) {
    registry
        .set_actor_message_handler(Arc::new(AsyncProxyActor { destination }))
        .await;
}

async fn register_worker_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
    workers: usize,
) {
    let (tx, rx) = mpsc::unbounded_channel::<(Bytes, icanact_remote::AskResponder)>();
    let shared_rx = Arc::new(tokio::sync::Mutex::new(rx));
    for _ in 0..workers {
        let destination = destination.clone();
        let shared_rx = shared_rx.clone();
        tokio::spawn(async move {
            loop {
                let next = {
                    let mut rx = shared_rx.lock().await;
                    rx.recv().await
                };
                let Some((payload, responder)) = next else {
                    break;
                };
                if let Ok(reply) = destination
                    .ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload)
                    .await
                {
                    let _ = responder.try_reply_bytes(reply);
                }
            }
        });
    }

    registry
        .set_actor_ask_handler_sync(Arc::new(WorkerProxyAskActor { tx }))
        .await;
}

async fn register_forwarder_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
    workers: usize,
    capacity: usize,
) {
    let forwarder = AskForwarder::new(workers, capacity);
    registry
        .set_actor_ask_handler_sync(Arc::new(ForwarderProxyAskActor {
            destination,
            forwarder,
        }))
        .await;
}

async fn register_dropping_forwarder_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
    workers: usize,
    capacity: usize,
) {
    let forwarder = AskForwarder::new(workers, capacity);
    registry
        .set_actor_ask_handler_sync(Arc::new(DroppingForwarderProxyAskActor {
            destination,
            forwarder: Arc::new(Mutex::new(Some(forwarder))),
        }))
        .await;
}

async fn register_bound_forwarder_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
    workers: usize,
    capacity: usize,
) {
    let workers = workers.max(1);
    let capacity = capacity.max(128);
    let mut senders = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, mut rx) = mpsc::channel::<(Bytes, icanact_remote::AskResponder)>(capacity);
        let destination = destination.clone();
        tokio::spawn(async move {
            while let Some((payload, responder)) = rx.recv().await {
                if let Ok(reply) = destination
                    .ask_actor_frame_no_timeout(BENCH_ACTOR_ID, BENCH_TYPE_HASH, payload)
                    .await
                {
                    let _ = responder.try_reply_bytes(reply);
                }
            }
        });
        senders.push(tx);
    }

    registry
        .set_actor_ask_handler_sync(Arc::new(BoundForwarderProxyAskActor {
            workers: Arc::new(senders),
            next_worker: Arc::new(AtomicUsize::new(0)),
        }))
        .await;
}

async fn register_bound_timeout_proxy_ask_actor(
    registry: &icanact_remote::registry::GossipRegistry,
    destination: RemoteConnection,
    workers: usize,
    capacity: usize,
) {
    let workers = workers.max(1);
    let capacity = capacity.max(128);
    let mut senders = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, mut rx) = mpsc::channel::<(Bytes, icanact_remote::AskResponder)>(capacity);
        let destination = destination.clone();
        tokio::spawn(async move {
            while let Some((payload, responder)) = rx.recv().await {
                match tokio::time::timeout(
                    ASK_BENCH_TIMEOUT,
                    destination.ask_actor_frame_no_timeout(
                        BENCH_ACTOR_ID,
                        BENCH_TYPE_HASH,
                        payload,
                    ),
                )
                .await
                {
                    Ok(Ok(reply)) => {
                        let _ = responder.try_reply_bytes(reply);
                    }
                    Ok(Err(_)) => {
                        let _ =
                            responder.try_reply_bytes(Bytes::from_static(b"bound_timeout_error"));
                    }
                    Err(_) => {
                        let _ =
                            responder.try_reply_bytes(Bytes::from_static(b"bound_timeout_timeout"));
                    }
                }
            }
        });
        senders.push(tx);
    }

    registry
        .set_actor_ask_handler_sync(Arc::new(BoundTimeoutProxyAskActor {
            workers: Arc::new(senders),
            next_worker: Arc::new(AtomicUsize::new(0)),
        }))
        .await;
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

    register_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
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
            let mut completed = 0u64;
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
                let reply = result.unwrap_or_else(|error| {
                    panic!(
                        "proxy ask benchmark failed after completed={} scheduled={} target={} inflight={} error={error:?}",
                        completed,
                        next,
                        count,
                        inflight
                    )
                });
                completed += 1;
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

    register_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
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

async fn run_actor_ask_no_timeout_split_inflight_benchmark(
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

    register_split_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let payload = Bytes::from(vec![14u8; PAYLOAD_BYTES]);

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

async fn run_actor_ask_deferred_inflight_benchmark(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    register_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let conn = remote.connection_ref().expect("connected remote ref");
    let payload = Bytes::from(vec![16u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let conn = conn.clone();
        let payload = payload.clone();
        async move {
            if inflight == 1 {
                let mut checksum = 0u64;
                for _ in 0..count {
                    let reply = conn
                        .ask_actor_frame(
                            BENCH_ACTOR_ID,
                            BENCH_TYPE_HASH,
                            payload.clone(),
                            ASK_BENCH_TIMEOUT,
                        )
                        .await
                        .unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                }
                return checksum;
            }

            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let conn = conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        let pending = conn
                            .ask_actor_frame_deferred(
                                BENCH_ACTOR_ID,
                                BENCH_TYPE_HASH,
                                payload,
                                ASK_BENCH_TIMEOUT,
                            )
                            .await?;
                        pending.wait().await
                    }
                    .boxed(),
                );
                next += 1;
            }

            while let Some(result) = pending.next().await {
                let reply = result.unwrap();
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < count {
                    let conn = conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            let pending = conn
                                .ask_actor_frame_deferred(
                                    BENCH_ACTOR_ID,
                                    BENCH_TYPE_HASH,
                                    payload,
                                    ASK_BENCH_TIMEOUT,
                                )
                                .await?;
                            pending.wait().await
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

async fn run_actor_ask_deferred_split_inflight_benchmark(
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

    register_split_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let conn = remote.connection_ref().expect("connected remote ref");
    let payload = Bytes::from(vec![17u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let conn = conn.clone();
        let payload = payload.clone();
        async move {
            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let conn = conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        let pending = conn
                            .ask_actor_frame_deferred(
                                BENCH_ACTOR_ID,
                                BENCH_TYPE_HASH,
                                payload,
                                ASK_BENCH_TIMEOUT,
                            )
                            .await?;
                        pending.wait().await
                    }
                    .boxed(),
                );
                next += 1;
            }

            while let Some(result) = pending.next().await {
                let reply = result.unwrap();
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < count {
                    let conn = conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            let pending = conn
                                .ask_actor_frame_deferred(
                                    BENCH_ACTOR_ID,
                                    BENCH_TYPE_HASH,
                                    payload,
                                    ASK_BENCH_TIMEOUT,
                                )
                                .await?;
                            pending.wait().await
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

async fn run_actor_ask_split_inflight_benchmark(
    label: &str,
    inflight: usize,
    warmup_count: u64,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    register_split_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let remote = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap();
    let conn = remote.connection_ref().expect("connected remote ref");
    let payload = Bytes::from(vec![20u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let conn = conn.clone();
        let payload = payload.clone();
        async move {
            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let conn = conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        conn.ask_actor_frame(
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
                    let conn = conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            conn.ask_actor_frame(
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

    if warmup_count != 0 {
        let _ = drive(warmup_count).await;
    }
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

async fn run_actor_ask_proxy_split_inflight_benchmark(
    label: &str,
    inflight: usize,
    warmup_count: u64,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let proxy_conn = proxy_remote.connection_ref().expect("connected proxy ref");
    let payload = Bytes::from(vec![18u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        async move {
            if inflight == 1 {
                let mut checksum = 0u64;
                for idx in 0..count {
                    let reply = proxy_conn
                        .ask_actor_frame(
                            PROXY_ACTOR_ID,
                            PROXY_TYPE_HASH,
                            payload.clone(),
                            ASK_BENCH_TIMEOUT,
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!(
                                "proxy single-flight failed at idx={idx} count={count} error={error:?}"
                            )
                        });
                    checksum = checksum.wrapping_add(reply.len() as u64);
                }
                return checksum;
            }

            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let proxy_conn = proxy_conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        proxy_conn
                            .ask_actor_frame(
                                PROXY_ACTOR_ID,
                                PROXY_TYPE_HASH,
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
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            proxy_conn
                                .ask_actor_frame(
                                    PROXY_ACTOR_ID,
                                    PROXY_TYPE_HASH,
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

    if warmup_count != 0 {
        let _ = drive(warmup_count).await;
    }
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

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn run_actor_ask_timeout_proxy_inflight_benchmark(
    label: &str,
    inflight: usize,
    warmup_count: u64,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let proxy_conn = proxy_remote.connection_ref().expect("connected proxy ref");
    let payload = Bytes::from(vec![29u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        async move {
            if inflight == 1 {
                let mut checksum = 0u64;
                for idx in 0..count {
                    let reply = proxy_conn
                        .ask_actor_frame(
                            TIMEOUT_PROXY_ACTOR_ID,
                            TIMEOUT_PROXY_TYPE_HASH,
                            payload.clone(),
                            ASK_BENCH_TIMEOUT,
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!(
                                "timeout proxy single-flight failed at idx={idx} count={count} error={error:?}"
                            )
                        });
                    checksum = checksum.wrapping_add(reply.len() as u64);
                }
                return checksum;
            }

            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let proxy_conn = proxy_conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        proxy_conn
                            .ask_actor_frame(
                                TIMEOUT_PROXY_ACTOR_ID,
                                TIMEOUT_PROXY_TYPE_HASH,
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
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            proxy_conn
                                .ask_actor_frame(
                                    TIMEOUT_PROXY_ACTOR_ID,
                                    TIMEOUT_PROXY_TYPE_HASH,
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

    if warmup_count != 0 {
        let _ = drive(warmup_count).await;
    }
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

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn run_actor_ask_aligned_timeout_proxy_inflight_benchmark(
    label: &str,
    inflight: usize,
    warmup_count: u64,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_aligned_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let proxy_conn = proxy_remote.connection_ref().expect("connected proxy ref");
    let payload = Bytes::from(vec![33u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        async move {
            if inflight == 1 {
                let mut checksum = 0u64;
                for idx in 0..count {
                    let reply = proxy_conn
                        .ask_actor_frame(
                            ALIGNED_TIMEOUT_PROXY_ACTOR_ID,
                            ALIGNED_TIMEOUT_PROXY_TYPE_HASH,
                            payload.clone(),
                            ASK_BENCH_TIMEOUT,
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!(
                                "aligned timeout proxy single-flight failed at idx={idx} count={count} error={error:?}"
                            )
                        });
                    checksum = checksum.wrapping_add(reply.len() as u64);
                }
                return checksum;
            }

            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let proxy_conn = proxy_conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        proxy_conn
                            .ask_actor_frame(
                                ALIGNED_TIMEOUT_PROXY_ACTOR_ID,
                                ALIGNED_TIMEOUT_PROXY_TYPE_HASH,
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
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            proxy_conn
                                .ask_actor_frame(
                                    ALIGNED_TIMEOUT_PROXY_ACTOR_ID,
                                    ALIGNED_TIMEOUT_PROXY_TYPE_HASH,
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

    if warmup_count != 0 {
        let _ = drive(warmup_count).await;
    }
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

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn run_actor_ask_outer_timeout_proxy_inflight_benchmark(
    label: &str,
    inflight: usize,
    warmup_count: u64,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_outer_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let proxy_conn = proxy_remote.connection_ref().expect("connected proxy ref");
    let payload = Bytes::from(vec![35u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        async move {
            if inflight == 1 {
                let mut checksum = 0u64;
                for idx in 0..count {
                    let reply = proxy_conn
                        .ask_actor_frame(
                            OUTER_TIMEOUT_PROXY_ACTOR_ID,
                            OUTER_TIMEOUT_PROXY_TYPE_HASH,
                            payload.clone(),
                            ASK_BENCH_TIMEOUT,
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!(
                                "outer timeout proxy single-flight failed at idx={idx} count={count} error={error:?}"
                            )
                        });
                    checksum = checksum.wrapping_add(reply.len() as u64);
                }
                return checksum;
            }

            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let proxy_conn = proxy_conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        proxy_conn
                            .ask_actor_frame(
                                OUTER_TIMEOUT_PROXY_ACTOR_ID,
                                OUTER_TIMEOUT_PROXY_TYPE_HASH,
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
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            proxy_conn
                                .ask_actor_frame(
                                    OUTER_TIMEOUT_PROXY_ACTOR_ID,
                                    OUTER_TIMEOUT_PROXY_TYPE_HASH,
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

    if warmup_count != 0 {
        let _ = drive(warmup_count).await;
    }
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

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn run_actor_ask_deferred_timeout_proxy_inflight_benchmark(
    label: &str,
    inflight: usize,
    warmup_count: u64,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_deferred_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let proxy_conn = proxy_remote.connection_ref().expect("connected proxy ref");
    let payload = Bytes::from(vec![30u8; PAYLOAD_BYTES]);

    let drive = |count: u64| {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        async move {
            if inflight == 1 {
                let mut checksum = 0u64;
                for idx in 0..count {
                    let reply = proxy_conn
                        .ask_actor_frame(
                            DEFERRED_TIMEOUT_PROXY_ACTOR_ID,
                            DEFERRED_TIMEOUT_PROXY_TYPE_HASH,
                            payload.clone(),
                            ASK_BENCH_TIMEOUT,
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!(
                                "deferred timeout proxy single-flight failed at idx={idx} count={count} error={error:?}"
                            )
                        });
                    checksum = checksum.wrapping_add(reply.len() as u64);
                }
                return checksum;
            }

            let mut pending: FuturesUnordered<
                futures::future::BoxFuture<'static, icanact_remote::Result<Bytes>>,
            > = FuturesUnordered::new();
            let mut next = 0u64;
            let mut checksum = 0u64;

            while next < count && pending.len() < inflight {
                let proxy_conn = proxy_conn.clone();
                let payload = payload.clone();
                pending.push(
                    async move {
                        proxy_conn
                            .ask_actor_frame(
                                DEFERRED_TIMEOUT_PROXY_ACTOR_ID,
                                DEFERRED_TIMEOUT_PROXY_TYPE_HASH,
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
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    pending.push(
                        async move {
                            proxy_conn
                                .ask_actor_frame(
                                    DEFERRED_TIMEOUT_PROXY_ACTOR_ID,
                                    DEFERRED_TIMEOUT_PROXY_TYPE_HASH,
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

    if warmup_count != 0 {
        let _ = drive(warmup_count).await;
    }
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

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_split_inflight(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry(&format!("{}_receiver", label), config.clone()).await;
    let sender = create_registry(&format!("{}_sender", label), config).await;

    register_split_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_bidirectional(&sender, &receiver).await;
    sleep(Duration::from_millis(300)).await;

    let conn = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected remote ref");
    let payload = Bytes::from(vec![21u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let conn = conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    conn.ask_actor_frame(
                        BENCH_ACTOR_ID,
                        BENCH_TYPE_HASH,
                        payload,
                        ASK_BENCH_TIMEOUT,
                    )
                    .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let conn = conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                conn.ask_actor_frame(
                                    BENCH_ACTOR_ID,
                                    BENCH_TYPE_HASH,
                                    payload,
                                    ASK_BENCH_TIMEOUT,
                                )
                                .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

async fn probe_actor_ask_proxy_split_inflight(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![22u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            PROXY_ACTOR_ID,
                            PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                proxy_conn
                                    .ask_actor_frame(
                                        PROXY_ACTOR_ID,
                                        PROXY_TYPE_HASH,
                                        payload,
                                        ASK_BENCH_TIMEOUT,
                                    )
                                    .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_timeout_proxy_inflight(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![27u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            TIMEOUT_PROXY_ACTOR_ID,
                            TIMEOUT_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                proxy_conn
                                    .ask_actor_frame(
                                        TIMEOUT_PROXY_ACTOR_ID,
                                        TIMEOUT_PROXY_TYPE_HASH,
                                        payload,
                                        ASK_BENCH_TIMEOUT,
                                    )
                                    .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_deferred_timeout_proxy_inflight(
    label: &str,
    inflight: usize,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_deferred_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![28u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            DEFERRED_TIMEOUT_PROXY_ACTOR_ID,
                            DEFERRED_TIMEOUT_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                proxy_conn
                                    .ask_actor_frame(
                                        DEFERRED_TIMEOUT_PROXY_ACTOR_ID,
                                        DEFERRED_TIMEOUT_PROXY_TYPE_HASH,
                                        payload,
                                        ASK_BENCH_TIMEOUT,
                                    )
                                    .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_deferred_proxy_inflight(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_deferred_echo_proxy_actor(&middle.registry).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let middle_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![23u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let middle_conn = middle_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    middle_conn
                        .ask_actor_frame(
                            DEFERRED_PROXY_ACTOR_ID,
                            DEFERRED_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let middle_conn = middle_conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                middle_conn
                                    .ask_actor_frame(
                                        DEFERRED_PROXY_ACTOR_ID,
                                        DEFERRED_PROXY_TYPE_HASH,
                                        payload,
                                        ASK_BENCH_TIMEOUT,
                                    )
                                    .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    source.shutdown().await;
    middle.shutdown().await;
}

async fn probe_actor_ask_nonblocking_deferred_proxy_inflight(
    label: &str,
    inflight: usize,
    ask_count: u64,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_nonblocking_deferred_echo_proxy_actor(&middle.registry).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let middle_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![25u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let middle_conn = middle_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    middle_conn
                        .ask_actor_frame(
                            NONBLOCKING_DEFERRED_PROXY_ACTOR_ID,
                            NONBLOCKING_DEFERRED_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let middle_conn = middle_conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                middle_conn
                                    .ask_actor_frame(
                                        NONBLOCKING_DEFERRED_PROXY_ACTOR_ID,
                                        NONBLOCKING_DEFERRED_PROXY_TYPE_HASH,
                                        payload,
                                        ASK_BENCH_TIMEOUT,
                                    )
                                    .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    source.shutdown().await;
    middle.shutdown().await;
}

async fn probe_actor_ask_async_proxy_inflight(label: &str, inflight: usize, ask_count: u64) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_async_proxy_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![26u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            ASYNC_PROXY_ACTOR_ID,
                            ASYNC_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                proxy_conn
                                    .ask_actor_frame(
                                        ASYNC_PROXY_ACTOR_ID,
                                        ASYNC_PROXY_TYPE_HASH,
                                        payload,
                                        ASK_BENCH_TIMEOUT,
                                    )
                                    .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_worker_proxy_inflight(
    label: &str,
    inflight: usize,
    ask_count: u64,
    workers: usize,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_worker_proxy_ask_actor(&middle.registry, destination_remote, workers).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![24u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            WORKER_PROXY_ACTOR_ID,
                            WORKER_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    let mut status = "ok".to_string();
    while let Some((idx, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                completed += 1;
                checksum = checksum.wrapping_add(reply.len() as u64);
                if next < ask_count {
                    let proxy_conn = proxy_conn.clone();
                    let payload = payload.clone();
                    let next_idx = next;
                    pending.push(
                        async move {
                            (
                                next_idx,
                                proxy_conn
                                    .ask_actor_frame(
                                        WORKER_PROXY_ACTOR_ID,
                                        WORKER_PROXY_TYPE_HASH,
                                        payload,
                                        ASK_BENCH_TIMEOUT,
                                    )
                                    .await,
                            )
                        }
                        .boxed(),
                    );
                    next += 1;
                }
            }
            Err(error) => {
                status = format!("err idx={} error={error:?}", idx);
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} workers={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={} status={}",
        inflight,
        workers,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum,
        status
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_forwarder_proxy_inflight(
    label: &str,
    inflight: usize,
    ask_count: u64,
    workers: usize,
    capacity: usize,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_forwarder_proxy_ask_actor(&middle.registry, destination_remote, workers, capacity)
        .await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![26u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            FORWARDER_PROXY_ACTOR_ID,
                            FORWARDER_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    while let Some((idx, result)) = pending.next().await {
        let response = result.unwrap_or_else(|err| {
            panic!("forwarder proxy ask benchmark failed at request {idx}: {err:?}")
        });
        checksum ^= response.len() as u64 + idx;
        completed += 1;

        if next < ask_count {
            let proxy_conn = proxy_conn.clone();
            let payload = payload.clone();
            let next_idx = next;
            pending.push(
                async move {
                    (
                        next_idx,
                        proxy_conn
                            .ask_actor_frame(
                                FORWARDER_PROXY_ACTOR_ID,
                                FORWARDER_PROXY_TYPE_HASH,
                                payload,
                                ASK_BENCH_TIMEOUT,
                            )
                            .await,
                    )
                }
                .boxed(),
            );
            next += 1;
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} workers={} capacity={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={}",
        inflight,
        workers,
        capacity,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_bound_forwarder_proxy_inflight(
    label: &str,
    inflight: usize,
    ask_count: u64,
    workers: usize,
    capacity: usize,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_bound_forwarder_proxy_ask_actor(
        &middle.registry,
        destination_remote,
        workers,
        capacity,
    )
    .await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![37u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            BOUND_FORWARDER_PROXY_ACTOR_ID,
                            BOUND_FORWARDER_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    while let Some((idx, result)) = pending.next().await {
        let response = result.unwrap_or_else(|err| {
            panic!("bound forwarder proxy ask benchmark failed at request {idx}: {err:?}")
        });
        checksum ^= response.len() as u64 + idx;
        completed += 1;

        if next < ask_count {
            let proxy_conn = proxy_conn.clone();
            let payload = payload.clone();
            let next_idx = next;
            pending.push(
                async move {
                    (
                        next_idx,
                        proxy_conn
                            .ask_actor_frame(
                                BOUND_FORWARDER_PROXY_ACTOR_ID,
                                BOUND_FORWARDER_PROXY_TYPE_HASH,
                                payload,
                                ASK_BENCH_TIMEOUT,
                            )
                            .await,
                    )
                }
                .boxed(),
            );
            next += 1;
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} workers={} capacity={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={}",
        inflight,
        workers,
        capacity,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

async fn probe_actor_ask_bound_timeout_proxy_inflight(
    label: &str,
    inflight: usize,
    ask_count: u64,
    workers: usize,
    capacity: usize,
) {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(&format!("{}_destination", label), config.clone()).await;
    let middle = create_registry(&format!("{}_middle", label), config.clone()).await;
    let source = create_registry(&format!("{}_source", label), config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_bound_timeout_proxy_ask_actor(&middle.registry, destination_remote, workers, capacity)
        .await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_conn = source
        .lookup_peer(&middle.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected proxy ref");
    let payload = Bytes::from(vec![38u8; PAYLOAD_BYTES]);

    let mut pending: FuturesUnordered<
        futures::future::BoxFuture<'static, (u64, icanact_remote::Result<Bytes>)>,
    > = FuturesUnordered::new();
    let mut next = 0u64;
    let mut completed = 0u64;
    let mut checksum = 0u64;
    let start = Instant::now();

    while next < ask_count && pending.len() < inflight {
        let proxy_conn = proxy_conn.clone();
        let payload = payload.clone();
        let idx = next;
        pending.push(
            async move {
                (
                    idx,
                    proxy_conn
                        .ask_actor_frame(
                            BOUND_TIMEOUT_PROXY_ACTOR_ID,
                            BOUND_TIMEOUT_PROXY_TYPE_HASH,
                            payload,
                            ASK_BENCH_TIMEOUT,
                        )
                        .await,
                )
            }
            .boxed(),
        );
        next += 1;
    }

    while let Some((idx, result)) = pending.next().await {
        let response = result.unwrap_or_else(|err| {
            panic!("bound timeout proxy ask benchmark failed at request {idx}: {err:?}")
        });
        checksum ^= response.len() as u64 + idx;
        completed += 1;

        if next < ask_count {
            let proxy_conn = proxy_conn.clone();
            let payload = payload.clone();
            let next_idx = next;
            pending.push(
                async move {
                    (
                        next_idx,
                        proxy_conn
                            .ask_actor_frame(
                                BOUND_TIMEOUT_PROXY_ACTOR_ID,
                                BOUND_TIMEOUT_PROXY_TYPE_HASH,
                                payload,
                                ASK_BENCH_TIMEOUT,
                            )
                            .await,
                    )
                }
                .boxed(),
            );
            next += 1;
        }
    }

    let elapsed = start.elapsed();
    let req_per_sec = if completed == 0 {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    println!(
        "[throughput_benchmarks::{label}] inflight={} workers={} capacity={} completed={} requested={} payload={}B elapsed={:.6}s throughput={:.2} req/s checksum={}",
        inflight,
        workers,
        capacity,
        completed,
        ask_count,
        PAYLOAD_BYTES,
        elapsed.as_secs_f64(),
        req_per_sec,
        checksum
    );

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
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

    register_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
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

    register_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
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
    register_echo_actor(
        &receiver.registry,
        received.clone(),
        Arc::new(AtomicU64::new(WARMUP_MESSAGES + MESSAGE_COUNT)),
        delivered.clone(),
    )
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
    register_echo_actor(
        &receiver.registry,
        received.clone(),
        delivered_target.clone(),
        delivered.clone(),
    )
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

    register_echo_actor(
        &receiver.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
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
async fn test_connect_to_peer_contention_throughput() {
    let lanes = bench_env_u64("ICANACT_REMOTE_CONNECT_CONTENTION_LANES", 32) as usize;
    let rounds = bench_env_u64("ICANACT_REMOTE_CONNECT_CONTENTION_ROUNDS", 20);
    run_connect_to_peer_contention_benchmark("connect_to_peer_contention", lanes, rounds).await;
}

#[tokio::test]
async fn test_connect_to_peer_contention_has_no_errors() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let receiver = create_registry("contention_regression_receiver", config.clone()).await;
    let sender = create_registry("contention_regression_sender", config).await;

    let peer_id = receiver.registry.peer_id.clone();
    let target_addr = receiver.registry.bind_addr;
    let _ = sender
        .registry
        .connection_pool
        .peer_id_to_addr
        .upsert_sync(peer_id.clone(), target_addr);

    let (_checksum, errors) = run_connect_to_peer_contention_rounds(
        sender.registry.clone(),
        receiver.registry.clone(),
        peer_id,
        target_addr,
        16,
        6,
        false,
    )
    .await;

    assert_eq!(
        errors, 0,
        "concurrent connect_to_peer should not lose peer mapping"
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn test_disconnect_connection_by_peer_id_preserves_configured_address() {
    let config = GossipConfig::default();
    let receiver = create_registry("preserve_addr_receiver", config.clone()).await;
    let sender = create_registry("preserve_addr_sender", config).await;

    let peer_id = receiver.registry.peer_id.clone();
    let target_addr = receiver.registry.bind_addr;
    sender
        .registry
        .configure_peer(peer_id.clone(), target_addr)
        .await;
    sender.registry.connect_to_peer(&peer_id).await.unwrap();

    sender
        .registry
        .connection_pool
        .disconnect_connection_by_peer_id(&peer_id);

    let preserved = sender
        .registry
        .connection_pool
        .peer_id_to_addr
        .read_sync(&peer_id, |_, addr| *addr);
    assert_eq!(preserved, Some(target_addr));

    sender.shutdown().await;
    receiver.shutdown().await;
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
async fn test_ask_actor_frame_no_timeout_split_inflight512_throughput() {
    run_actor_ask_no_timeout_split_inflight_benchmark(
        "ask_actor_split_no_timeout_inflight512",
        512,
        MESSAGE_COUNT * 10,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_deferred_inflight512_throughput() {
    run_actor_ask_deferred_inflight_benchmark(
        "ask_actor_deferred_inflight512",
        512,
        MESSAGE_COUNT * 10,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_deferred_split_inflight512_throughput() {
    run_actor_ask_deferred_split_inflight_benchmark(
        "ask_actor_deferred_split_inflight512",
        512,
        MESSAGE_COUNT * 10,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_split_single_flight_throughput() {
    run_actor_ask_split_inflight_benchmark("ask_actor_split_single_flight", 1, 0, 64).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_proxy_split_single_flight_throughput() {
    run_actor_ask_proxy_split_inflight_benchmark("ask_actor_proxy_split_single_flight", 1, 0, 64)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_proxy_split_inflight64_throughput() {
    run_actor_ask_proxy_split_inflight_benchmark("ask_actor_proxy_split_inflight64", 64, 64, 1_024)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_timeout_proxy_inflight64_throughput() {
    run_actor_ask_timeout_proxy_inflight_benchmark(
        "ask_actor_timeout_proxy_inflight64",
        64,
        64,
        1_024,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_aligned_timeout_proxy_inflight64_throughput() {
    run_actor_ask_aligned_timeout_proxy_inflight_benchmark(
        "ask_actor_aligned_timeout_proxy_inflight64",
        64,
        64,
        1_024,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_outer_timeout_proxy_inflight64_throughput() {
    run_actor_ask_outer_timeout_proxy_inflight_benchmark(
        "ask_actor_outer_timeout_proxy_inflight64",
        64,
        64,
        1_024,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_deferred_timeout_proxy_inflight64_throughput() {
    run_actor_ask_deferred_timeout_proxy_inflight_benchmark(
        "ask_actor_deferred_timeout_proxy_inflight64",
        64,
        64,
        1_024,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_split_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_split_inflight(
            &format!("ask_actor_split_probe_inflight{}", inflight),
            inflight,
            256,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_proxy_split_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_proxy_split_inflight(
            &format!("ask_actor_proxy_split_probe_inflight{}", inflight),
            inflight,
            256,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_timeout_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_timeout_proxy_inflight(
            &format!("ask_actor_timeout_proxy_probe_inflight{}", inflight),
            inflight,
            256,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_deferred_timeout_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_deferred_timeout_proxy_inflight(
            &format!(
                "ask_actor_deferred_timeout_proxy_probe_inflight{}",
                inflight
            ),
            inflight,
            256,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_deferred_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_deferred_proxy_inflight(
            &format!("ask_actor_deferred_proxy_probe_inflight{}", inflight),
            inflight,
            256,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_nonblocking_deferred_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_nonblocking_deferred_proxy_inflight(
            &format!(
                "ask_actor_nonblocking_deferred_proxy_probe_inflight{}",
                inflight
            ),
            inflight,
            256,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_worker_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_worker_proxy_inflight(
            &format!("ask_actor_worker_proxy_probe_inflight{}", inflight),
            inflight,
            256,
            4,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_worker64_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_worker_proxy_inflight(
            &format!("ask_actor_worker64_proxy_probe_inflight{}", inflight),
            inflight,
            256,
            64,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_forwarder_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_forwarder_proxy_inflight(
            &format!("ask_actor_forwarder_proxy_probe_inflight{}", inflight),
            inflight,
            256,
            64,
            4_096,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_bound_forwarder_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_bound_forwarder_proxy_inflight(
            &format!("ask_actor_bound_forwarder_proxy_probe_inflight{}", inflight),
            inflight,
            256,
            11,
            8_192,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_bound_timeout_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_bound_timeout_proxy_inflight(
            &format!("ask_actor_bound_timeout_proxy_probe_inflight{}", inflight),
            inflight,
            256,
            11,
            8_192,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark-only; run explicitly when profiling"]
async fn test_ask_actor_frame_async_proxy_probe_scaling() {
    for inflight in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        probe_actor_ask_async_proxy_inflight(
            &format!("ask_actor_async_proxy_probe_inflight{}", inflight),
            inflight,
            256,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_proxy_actor_ask_round_trip() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry("proxy_round_trip_destination", config.clone()).await;
    let middle = create_registry("proxy_round_trip_middle", config.clone()).await;
    let source = create_registry("proxy_round_trip_source", config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let payload = Bytes::from(vec![19u8; PAYLOAD_BYTES]);

    for _ in 0..64 {
        let reply = proxy_remote
            .ask_actor_frame(
                PROXY_ACTOR_ID,
                PROXY_TYPE_HASH,
                payload.clone(),
                ASK_BENCH_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_timeout_proxy_actor_ask_round_trip() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry("timeout_proxy_round_trip_destination", config.clone()).await;
    let middle = create_registry("timeout_proxy_round_trip_middle", config.clone()).await;
    let source = create_registry("timeout_proxy_round_trip_source", config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let payload = Bytes::from(vec![31u8; PAYLOAD_BYTES]);

    for _ in 0..64 {
        let reply = proxy_remote
            .ask_actor_frame(
                TIMEOUT_PROXY_ACTOR_ID,
                TIMEOUT_PROXY_TYPE_HASH,
                payload.clone(),
                ASK_BENCH_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_aligned_timeout_proxy_actor_ask_round_trip() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(
        "aligned_timeout_proxy_round_trip_destination",
        config.clone(),
    )
    .await;
    let middle = create_registry("aligned_timeout_proxy_round_trip_middle", config.clone()).await;
    let source = create_registry("aligned_timeout_proxy_round_trip_source", config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_aligned_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let payload = Bytes::from(vec![34u8; PAYLOAD_BYTES]);

    for _ in 0..64 {
        let reply = proxy_remote
            .ask_actor_frame(
                ALIGNED_TIMEOUT_PROXY_ACTOR_ID,
                ALIGNED_TIMEOUT_PROXY_TYPE_HASH,
                payload.clone(),
                ASK_BENCH_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_outer_timeout_proxy_actor_ask_round_trip() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination =
        create_registry("outer_timeout_proxy_round_trip_destination", config.clone()).await;
    let middle = create_registry("outer_timeout_proxy_round_trip_middle", config.clone()).await;
    let source = create_registry("outer_timeout_proxy_round_trip_source", config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_outer_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let payload = Bytes::from(vec![36u8; PAYLOAD_BYTES]);

    for _ in 0..64 {
        let reply = proxy_remote
            .ask_actor_frame(
                OUTER_TIMEOUT_PROXY_ACTOR_ID,
                OUTER_TIMEOUT_PROXY_TYPE_HASH,
                payload.clone(),
                ASK_BENCH_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_deferred_timeout_proxy_actor_ask_round_trip() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry(
        "deferred_timeout_proxy_round_trip_destination",
        config.clone(),
    )
    .await;
    let middle = create_registry("deferred_timeout_proxy_round_trip_middle", config.clone()).await;
    let source = create_registry("deferred_timeout_proxy_round_trip_source", config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_deferred_timeout_proxy_ask_actor(&middle.registry, destination_remote).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let payload = Bytes::from(vec![32u8; PAYLOAD_BYTES]);

    for _ in 0..64 {
        let reply = proxy_remote
            .ask_actor_frame(
                DEFERRED_TIMEOUT_PROXY_ACTOR_ID,
                DEFERRED_TIMEOUT_PROXY_TYPE_HASH,
                payload.clone(),
                ASK_BENCH_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_forwarder_proxy_actor_ask_round_trip() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination =
        create_registry("forwarder_proxy_round_trip_destination", config.clone()).await;
    let middle = create_registry("forwarder_proxy_round_trip_middle", config.clone()).await;
    let source = create_registry("forwarder_proxy_round_trip_source", config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_forwarder_proxy_ask_actor(&middle.registry, destination_remote, 64, 4_096).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let payload = Bytes::from(vec![27u8; PAYLOAD_BYTES]);

    for _ in 0..64 {
        let reply = proxy_remote
            .ask_actor_frame(
                FORWARDER_PROXY_ACTOR_ID,
                FORWARDER_PROXY_TYPE_HASH,
                payload.clone(),
                ASK_BENCH_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(reply.len(), PAYLOAD_BYTES);
    }

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_forwarder_drop_drains_inflight_ask() {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ask_window: 65_536,
        ..Default::default()
    };

    let destination = create_registry("forwarder_drop_drains_destination", config.clone()).await;
    let middle = create_registry("forwarder_drop_drains_middle", config.clone()).await;
    let source = create_registry("forwarder_drop_drains_source", config).await;

    register_split_echo_actor(
        &destination.registry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(Notify::new()),
    )
    .await;

    connect_unidirectional(&middle, &destination).await;
    sleep(Duration::from_millis(300)).await;

    let destination_remote = middle
        .lookup_peer(&destination.registry.peer_id)
        .await
        .unwrap()
        .connection_ref()
        .expect("connected destination ref");
    register_dropping_forwarder_proxy_ask_actor(&middle.registry, destination_remote, 4, 256).await;

    connect_unidirectional(&source, &middle).await;
    sleep(Duration::from_millis(300)).await;

    let proxy_remote = source.lookup_peer(&middle.registry.peer_id).await.unwrap();
    let reply = proxy_remote
        .ask_actor_frame(
            DROPPING_FORWARDER_PROXY_ACTOR_ID,
            DROPPING_FORWARDER_PROXY_TYPE_HASH,
            Bytes::from(vec![31u8; PAYLOAD_BYTES]),
            ASK_BENCH_TIMEOUT,
        )
        .await
        .unwrap();
    assert_eq!(reply.len(), PAYLOAD_BYTES);

    source.shutdown().await;
    middle.shutdown().await;
    destination.shutdown().await;
}

#[test]
fn test_long_benchmark_labels_produce_distinct_peer_ids() {
    let a = testing_keypair("ask_actor_proxy_split_single_flight_destination").peer_id();
    let b = testing_keypair("ask_actor_proxy_split_single_flight_middle").peer_id();
    let c = testing_keypair("ask_actor_proxy_split_single_flight_source").peer_id();

    assert_ne!(a, b);
    assert_ne!(a, c);
    assert_ne!(b, c);
}
