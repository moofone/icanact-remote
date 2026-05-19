/// End-to-end tests for deferred ask over UDP transport.
///
/// These tests verify that `set_actor_ask_handler_sync` correctly dispatches to
/// UDP senders via the new `AskContext::from_udp` path and that
/// `AskResponder::from_udp` sends the reply datagram back to the originating
/// peer without a connection-pool lookup.
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Once};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use icanact_remote::registry::{ActorAskHandlerSync, AskDisposition};
use icanact_remote::transport::{
    TransportDatagramRuntime, TransportDatagramWriter, TransportWireKind,
};
use icanact_remote::{
    AskContext, AskResponder, GossipConfig, GossipRegistryHandle, RegistryTransportBootstrap,
    SecretKey,
};
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------------------------------------------------------------------------
// Minimal Tokio UDP runtime (mirrors actor-framework-core TokioUdpRuntime)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TokioUdpWriter {
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
}

impl TokioUdpWriter {
    fn new(socket: Arc<UdpSocket>, peer_addr: SocketAddr) -> Self {
        Self { socket, peer_addr }
    }

    fn concat_header_payload(header: &[u8], payload: &[u8]) -> Bytes {
        let mut out = BytesMut::with_capacity(header.len() + payload.len());
        out.extend_from_slice(header);
        out.extend_from_slice(payload);
        out.freeze()
    }

    fn try_send_bytes(
        socket: &UdpSocket,
        peer_addr: SocketAddr,
        bytes: Bytes,
    ) -> icanact_remote::Result<()> {
        socket
            .try_send_to(&bytes, peer_addr)
            .map(|_| ())
            .map_err(icanact_remote::GossipError::Network)
    }
}

impl TransportDatagramWriter for TokioUdpWriter {
    fn send_bytes(&self, datagram: Bytes) -> BoxFuture<'_, icanact_remote::Result<()>> {
        let socket = Arc::clone(&self.socket);
        let peer_addr = self.peer_addr;
        Box::pin(async move {
            socket
                .send_to(&datagram, peer_addr)
                .await
                .map(|_| ())
                .map_err(icanact_remote::GossipError::Network)
        })
    }

    fn send_header_and_payload16(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: Bytes,
    ) -> BoxFuture<'_, icanact_remote::Result<()>> {
        let datagram = Self::concat_header_payload(&header[..usize::from(header_len)], &payload);
        self.send_bytes(datagram)
    }

    fn send_header_and_payload32(
        &self,
        header: [u8; 32],
        payload: Bytes,
    ) -> BoxFuture<'_, icanact_remote::Result<()>> {
        let datagram = Self::concat_header_payload(&header, &payload);
        self.send_bytes(datagram)
    }

    fn try_send_header_and_payload16(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: Bytes,
    ) -> icanact_remote::Result<()> {
        let datagram = Self::concat_header_payload(&header[..usize::from(header_len)], &payload);
        Self::try_send_bytes(&self.socket, self.peer_addr, datagram)
    }

    fn try_send_header_and_payload32(
        &self,
        header: [u8; 32],
        payload: Bytes,
    ) -> icanact_remote::Result<()> {
        let datagram = Self::concat_header_payload(&header, &payload);
        Self::try_send_bytes(&self.socket, self.peer_addr, datagram)
    }

    fn send_header_prefix_pooled(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        mut payload: icanact_remote::typed::PooledPayload,
    ) -> BoxFuture<'_, icanact_remote::Result<()>> {
        let prefix_len = prefix.as_ref().map_or(0usize, |_| 16);
        let mut out =
            BytesMut::with_capacity(usize::from(header_len) + prefix_len + payload.remaining());
        out.extend_from_slice(&header[..usize::from(header_len)]);
        if let Some(p) = prefix {
            out.extend_from_slice(&p);
        }
        while payload.has_remaining() {
            let chunk = payload.chunk();
            let len = chunk.len();
            out.extend_from_slice(chunk);
            payload.advance(len);
        }
        self.send_bytes(out.freeze())
    }

    fn try_send_header_prefix_pooled(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        mut payload: icanact_remote::typed::PooledPayload,
    ) -> icanact_remote::Result<()> {
        let prefix_len = prefix.as_ref().map_or(0usize, |_| 16);
        let mut out =
            BytesMut::with_capacity(usize::from(header_len) + prefix_len + payload.remaining());
        out.extend_from_slice(&header[..usize::from(header_len)]);
        if let Some(p) = prefix {
            out.extend_from_slice(&p);
        }
        while payload.has_remaining() {
            let chunk = payload.chunk();
            let len = chunk.len();
            out.extend_from_slice(chunk);
            payload.advance(len);
        }
        Self::try_send_bytes(&self.socket, self.peer_addr, out.freeze())
    }

    fn try_send_pooled_datagram(
        &self,
        datagram: icanact_remote::typed::PooledPayload,
    ) -> icanact_remote::Result<()> {
        Self::try_send_bytes(&self.socket, self.peer_addr, Bytes::copy_from_slice(datagram.chunk()))
    }

    fn send_bytes_vectored(
        &self,
        header: Bytes,
        payload: Bytes,
    ) -> BoxFuture<'_, icanact_remote::Result<()>> {
        let datagram = Self::concat_header_payload(&header, &payload);
        self.send_bytes(datagram)
    }

    fn try_send_chunks(&self, chunks: &[Bytes]) -> icanact_remote::Result<()> {
        let total: usize = chunks.iter().map(Bytes::len).sum();
        let mut out = BytesMut::with_capacity(total);
        for chunk in chunks {
            out.extend_from_slice(chunk);
        }
        Self::try_send_bytes(&self.socket, self.peer_addr, out.freeze())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TokioUdpRuntime;

impl TransportDatagramRuntime for TokioUdpRuntime {
    type Writer = TokioUdpWriter;

    fn make_writer(
        socket: Arc<UdpSocket>,
        peer_addr: SocketAddr,
        _queue_capacity: usize,
    ) -> Self::Writer {
        TokioUdpWriter::new(socket, peer_addr)
    }

    fn try_send_bytes_to_addr(
        socket: &UdpSocket,
        addr: SocketAddr,
        data: Bytes,
    ) -> icanact_remote::Result<()> {
        TokioUdpWriter::try_send_bytes(socket, addr, data)
    }

    fn try_send_parts_to_addr(
        socket: &UdpSocket,
        addr: SocketAddr,
        header: Bytes,
        payload: Bytes,
    ) -> icanact_remote::Result<()> {
        let datagram = TokioUdpWriter::concat_header_payload(&header, &payload);
        TokioUdpWriter::try_send_bytes(socket, addr, datagram)
    }
}

// ---------------------------------------------------------------------------
// UDP Bootstrap (uses TokioUdpRuntime)
// ---------------------------------------------------------------------------

static CRYPTO_INIT: Once = Once::new();
static UDP_RUNTIME_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        icanact_remote::tls::ensure_crypto_provider();
    });
}

fn init_udp_runtime() {
    UDP_RUNTIME_INIT.call_once(|| {
        icanact_remote::transport::install_datagram_runtime::<TokioUdpRuntime>();
    });
}

#[derive(Debug, Clone, Copy, Default)]
struct UdpBootstrap;

impl RegistryTransportBootstrap for UdpBootstrap {
    fn stack_name(&self) -> &'static str {
        "test+udp"
    }

    fn wire_kind(&self) -> TransportWireKind {
        TransportWireKind::UdpDatagram
    }

    fn prepare_config(
        &self,
        secret_key: &SecretKey,
        config: &mut GossipConfig,
    ) -> icanact_remote::Result<()> {
        if config.key_pair.is_none() {
            config.key_pair = Some(secret_key.to_keypair());
        }
        Ok(())
    }

    fn configure_registry(
        &self,
        registry: &mut icanact_remote::registry::GossipRegistry,
        secret_key: SecretKey,
    ) -> icanact_remote::Result<()> {
        init_udp_runtime();
        registry.enable_udp(secret_key)
    }
}

type UdpHandle = GossipRegistryHandle<UdpBootstrap>;

async fn create_udp_node(cfg: GossipConfig) -> UdpHandle {
    init_crypto();
    let secret_key = SecretKey::generate();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut backoff = Duration::from_millis(25);
    loop {
        match GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            secret_key.clone(),
            Some(cfg.clone()),
            UdpBootstrap,
        )
        .await
        {
            Ok(h) => return h,
            Err(e) => {
                let is_eperm = matches!(
                    &e,
                    icanact_remote::GossipError::Network(io) if io.raw_os_error() == Some(1)
                );
                if is_eperm && Instant::now() < deadline {
                    sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(1));
                    continue;
                }
                panic!("create_udp_node failed: {e}");
            }
        }
    }
}

fn fast_cfg() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(200),
        cleanup_interval: Duration::from_millis(400),
        peer_retry_interval: Duration::from_millis(50),
        connection_timeout: Duration::from_millis(750),
        response_timeout: Duration::from_millis(750),
        ..Default::default()
    }
}

/// Teach `sender` about `receiver`'s address so UDP connections can be made.
async fn seed_udp(sender: &UdpHandle, receiver: &UdpHandle) {
    sender
        .registry
        .add_peer_with_node_id(
            receiver.registry.bind_addr,
            Some(receiver.registry.peer_id.to_node_id()),
        )
        .await;
}

/// Get a UDP RemoteConnection from `sender` to `receiver`.
///
/// Both nodes must know each other's peer_id before any datagram exchange can
/// succeed (the UDP receive loop requires an authenticated peer association for
/// every source address it sees). We therefore teach both sides before opening
/// the connection.
async fn udp_conn(sender: &UdpHandle, receiver: &UdpHandle) -> icanact_remote::RemoteConnection {
    // Bidirectional peer seeding: both nodes must know about each other so that
    // the receive loop can look up an authenticated_peer_id for any incoming datagram.
    seed_udp(sender, receiver).await;
    seed_udp(receiver, sender).await;
    let ref_handle = sender
        .lookup_peer(&receiver.registry.peer_id)
        .await
        .expect("lookup_peer");
    ref_handle
        .connection
        .expect("RemoteActorRef must have a connection for a known UDP peer")
}

fn run_test<F, Fut>(name: &str, f: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("udp-ask-e2e-{name}"))
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(f());
        })
        .unwrap()
        .join()
        .unwrap();
}

// ---------------------------------------------------------------------------
// Actor ID / type hash constants shared across tests
// ---------------------------------------------------------------------------

const ACTOR_ID: u64 = 0xDEAD_BEEF_0001;
const TYPE_HASH: u32 = 0xAABB_CCDD;

// ---------------------------------------------------------------------------
// Test 1: basic deferred ask reaches the handler and the reply is correct
// ---------------------------------------------------------------------------

struct DoubleHandler;

impl ActorAskHandlerSync for DoubleHandler {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        let bytes: &[u8] = &payload;
        let val = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let responder: AskResponder = context.responder();
        tokio::spawn(async move {
            let reply = Bytes::from((val * 2).to_le_bytes().to_vec());
            let _ = responder.reply(reply).await;
        });
        Ok(AskDisposition::Deferred)
    }
}

#[test]
fn udp_deferred_ask_reaches_handler_and_replies() {
    run_test("basic", || async {
        let node_a = create_udp_node(fast_cfg()).await;
        let node_b = create_udp_node(fast_cfg()).await;

        // Install deferred ask handler on A.
        node_a
            .registry
            .set_actor_ask_handler_sync(Arc::new(DoubleHandler))
            .await;

        // Get a UDP connection from B to A.
        let conn = udp_conn(&node_b, &node_a).await;

        let request: u64 = 42;
        let payload = Bytes::from(request.to_le_bytes().to_vec());

        let response = conn
            .ask_actor_frame(ACTOR_ID, TYPE_HASH, payload, Duration::from_secs(5))
            .await
            .expect("ask_actor_frame");

        let reply_val = u64::from_le_bytes(response[..8].try_into().unwrap());
        assert_eq!(reply_val, 84, "expected 42 * 2 = 84");

        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

// ---------------------------------------------------------------------------
// Test 2: multiple concurrent asks all get correctly demuxed correlation IDs
// ---------------------------------------------------------------------------

struct TripleHandler;

impl ActorAskHandlerSync for TripleHandler {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        let bytes: &[u8] = &payload;
        let val = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let responder: AskResponder = context.responder();
        tokio::spawn(async move {
            let reply = Bytes::from((val * 3).to_le_bytes().to_vec());
            let _ = responder.reply(reply).await;
        });
        Ok(AskDisposition::Deferred)
    }
}

#[test]
fn udp_ask_reply_has_correct_correlation_id() {
    run_test("concurrent", || async {
        let node_a = create_udp_node(fast_cfg()).await;
        let node_b = create_udp_node(fast_cfg()).await;

        node_a
            .registry
            .set_actor_ask_handler_sync(Arc::new(TripleHandler))
            .await;

        let conn = udp_conn(&node_b, &node_a).await;

        // Fire 3 concurrent asks with distinct values.
        let requests: Vec<u64> = vec![10, 20, 30];
        let mut handles = Vec::new();
        for &val in &requests {
            let conn = conn.clone();
            handles.push(tokio::spawn(async move {
                let payload = Bytes::from(val.to_le_bytes().to_vec());
                let resp = conn
                    .ask_actor_frame(ACTOR_ID, TYPE_HASH, payload, Duration::from_secs(5))
                    .await
                    .expect("ask");
                let reply = u64::from_le_bytes(resp[..8].try_into().unwrap());
                (val, reply)
            }));
        }

        for (i, handle) in handles.into_iter().enumerate() {
            let (sent, received) = handle.await.unwrap();
            assert_eq!(
                received,
                sent * 3,
                "request[{i}]: expected {sent} * 3 = {}, got {received}",
                sent * 3
            );
        }

        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

// ---------------------------------------------------------------------------
// Test 3: full actor-forwarder roundtrip via deferred ask over UDP
//
// Simulates the "BlockSubmitAskForwarder" pattern:
//   receive remote ask → forward to actor task → reply via AskResponder
// ---------------------------------------------------------------------------

struct ForwarderHandler {
    tx: tokio::sync::mpsc::UnboundedSender<(u64, AskResponder)>,
}

impl fmt::Debug for ForwarderHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForwarderHandler").finish()
    }
}

impl ActorAskHandlerSync for ForwarderHandler {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        let bytes: &[u8] = &payload;
        let val = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let responder = context.responder();
        let _ = self.tx.send((val, responder));
        Ok(AskDisposition::Deferred)
    }
}

#[test]
fn udp_ask_forwarder_actor_roundtrip() {
    run_test("forwarder", || async {
        let node_a = create_udp_node(fast_cfg()).await;
        let node_b = create_udp_node(fast_cfg()).await;

        // Spawn the "actor" task that processes ask messages.
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<(u64, AskResponder)>();
        tokio::spawn(async move {
            while let Some((val, responder)) = rx.recv().await {
                let reply = Bytes::from((val * 7).to_le_bytes().to_vec());
                let _ = responder.reply(reply).await;
            }
        });

        node_a
            .registry
            .set_actor_ask_handler_sync(Arc::new(ForwarderHandler { tx }))
            .await;

        let conn = udp_conn(&node_b, &node_a).await;

        for &val in &[1u64, 100, 999] {
            let payload = Bytes::from(val.to_le_bytes().to_vec());
            let resp = conn
                .ask_actor_frame(ACTOR_ID, TYPE_HASH, payload, Duration::from_secs(5))
                .await
                .unwrap_or_else(|e| panic!("ask failed for val={val}: {e}"));
            let reply = u64::from_le_bytes(resp[..8].try_into().unwrap());
            assert_eq!(reply, val * 7, "expected {val} * 7 = {}, got {reply}", val * 7);
        }

        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

// ---------------------------------------------------------------------------
// Test 4: ask times out when no handler is installed on the receiver
// ---------------------------------------------------------------------------

#[test]
fn udp_ask_timeout_when_handler_absent() {
    run_test("timeout", || async {
        let node_a = create_udp_node(fast_cfg()).await; // no handler installed
        let node_b = create_udp_node(fast_cfg()).await;

        let conn = udp_conn(&node_b, &node_a).await;

        let short_timeout = Duration::from_millis(400);
        let payload = Bytes::from(42u64.to_le_bytes().to_vec());

        let result = conn
            .ask_actor_frame(ACTOR_ID, TYPE_HASH, payload, short_timeout)
            .await;

        match result {
            Err(icanact_remote::GossipError::Timeout) => {
                // Expected — no handler means no response.
            }
            Err(other) => panic!("expected Timeout, got: {other}"),
            Ok(_) => panic!("expected timeout, but got a response"),
        }

        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

// ---------------------------------------------------------------------------
// Test 5: slow handler does not block the receive loop for fast asks
// ---------------------------------------------------------------------------

struct SlowHandler;

impl ActorAskHandlerSync for SlowHandler {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        let bytes: &[u8] = &payload;
        let tag = bytes[0];
        let responder = context.responder();
        tokio::spawn(async move {
            if tag == 0xFF {
                // Slow path: 80 ms delay.
                sleep(Duration::from_millis(80)).await;
            }
            // Echo the tag byte back.
            let _ = responder.reply(Bytes::from(vec![tag])).await;
        });
        Ok(AskDisposition::Deferred)
    }
}

#[test]
fn udp_deferred_ask_does_not_block_receive_loop() {
    run_test("non-blocking", || async {
        let node_a = create_udp_node(fast_cfg()).await;
        let node_b = create_udp_node(fast_cfg()).await;

        node_a
            .registry
            .set_actor_ask_handler_sync(Arc::new(SlowHandler))
            .await;

        let conn = udp_conn(&node_b, &node_a).await;

        // Start the slow ask first (tag=0xFF → 80 ms handler delay).
        let conn_slow = conn.clone();
        let slow: tokio::task::JoinHandle<icanact_remote::Result<Bytes>> =
            tokio::spawn(async move {
                conn_slow
                    .ask_actor_frame(
                        ACTOR_ID,
                        TYPE_HASH,
                        Bytes::from(vec![0xFF_u8]),
                        Duration::from_secs(5),
                    )
                    .await
            });

        // Give the slow ask time to be dispatched before sending the fast one.
        sleep(Duration::from_millis(10)).await;

        // Send a fast ask (tag=0x01 → no delay).
        let fast_start = Instant::now();
        let fast_resp = conn
            .ask_actor_frame(
                ACTOR_ID,
                TYPE_HASH,
                Bytes::from(vec![0x01_u8]),
                Duration::from_secs(5),
            )
            .await
            .expect("fast ask");
        let fast_elapsed = fast_start.elapsed();

        assert_eq!(fast_resp[0], 0x01, "fast reply tag mismatch");
        assert!(
            fast_elapsed < Duration::from_millis(70),
            "fast ask took too long ({fast_elapsed:?}); receive loop was likely blocked"
        );

        // The slow ask must also complete.
        let slow_resp = slow.await.unwrap().expect("slow ask");
        assert_eq!(slow_resp[0], 0xFF, "slow reply tag mismatch");

        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

/// Smoke: verify that UDP mode is actually enabled on nodes created with UdpBootstrap.
#[test]
fn udp_nodes_have_udp_mode_enabled() {
    run_test("smoke", || async {
        let node = create_udp_node(fast_cfg()).await;
        assert!(
            node.registry.udp_mode,
            "UDP bootstrap must set udp_mode on registry"
        );
        node.shutdown().await;
    });
}
