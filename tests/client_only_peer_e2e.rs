//! Client-only peer role: a node that only ever dials out (VPN/NAT laptop CLI)
//! and is pushed to by a LAN server over the client-initiated session.
//!
//! Invariants exercised: REMOTE-3 (the role is bound to the authenticated
//! identity, never an address), REMOTE-4 (session teardown is
//! generation-fenced), REMOTE-5 (no unbounded retry toward an undialable peer).

mod common;

use bytes::Bytes;
use common::{DynError, TlsHandle, wait_for_condition};
use futures::future::BoxFuture;
use icanact_remote::registry::{
    ActorMessageHandlerSync, ActorResponse, PeerConnectHandler, PeerDisconnectHandler,
};
use icanact_remote::{AlignedBytes, GossipConfig, GossipRegistryHandle, PeerId, SecretKey};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;

const ACTOR_ID: u64 = 4242;
const TYPE_HASH: u32 = 0x00C0_FFEE;

// ---- WARN+/ERROR log capture (process-global; tests filter by identity) ----

static LOG_LINES: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

#[derive(Clone)]
struct LogSink(Arc<Mutex<Vec<String>>>);

impl std::io::Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(mut lines) = self.0.lock() {
            lines.push(String::from_utf8_lossy(buf).into_owned());
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogSink;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn install_log_capture() -> Arc<Mutex<Vec<String>>> {
    LOG_LINES
        .get_or_init(|| {
            let lines = Arc::new(Mutex::new(Vec::new()));
            let _ = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .with_writer(LogSink(Arc::clone(&lines)))
                .try_init();
            lines
        })
        .clone()
}

fn log_lines_mentioning(lines: &Mutex<Vec<String>>, needles: &[String]) -> Vec<String> {
    lines
        .lock()
        .unwrap()
        .iter()
        .filter(|l| needles.iter().any(|n| l.contains(n.as_str())))
        .cloned()
        .collect()
}

// ---- fixtures ----

fn cfg(client_only: bool) -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(100),
        cleanup_interval: Duration::from_millis(200),
        peer_retry_interval: Duration::from_millis(50),
        peer_supervisor_interval: Duration::from_millis(200),
        connection_timeout: Duration::from_millis(750),
        response_timeout: Duration::from_secs(2),
        client_only,
        ..Default::default()
    }
}

async fn node_on(key: SecretKey, addr: SocketAddr, config: GossipConfig) -> TlsHandle {
    icanact_remote::tls::ensure_crypto_provider();
    GossipRegistryHandle::new_with_transport_stack(
        addr,
        key,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .expect("node starts")
}

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

struct EchoHandler {
    tells: Arc<AtomicU64>,
    label: &'static str,
}

impl ActorMessageHandlerSync for EchoHandler {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u32>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        assert_eq!((actor_id, type_hash), (ACTOR_ID, TYPE_HASH));
        if correlation_id.is_some() {
            let mut reply = format!("{}:", self.label).into_bytes();
            reply.extend_from_slice(payload.as_ref());
            Ok(Some(ActorResponse::from(reply)))
        } else {
            self.tells.fetch_add(1, Ordering::Release);
            Ok(None)
        }
    }
}

async fn serve_echo(node: &TlsHandle, label: &'static str) -> Arc<AtomicU64> {
    let tells = Arc::new(AtomicU64::new(0));
    node.registry
        .set_actor_message_handler_sync(Arc::new(EchoHandler {
            tells: Arc::clone(&tells),
            label,
        }))
        .await;
    tells
}

/// Client dials the server (supervised/required relationship).
async fn dial(client: &TlsHandle, server: &TlsHandle) {
    let peer = client.add_peer(&server.registry.peer_id).await;
    peer.connect(&server.registry.bind_addr)
        .await
        .expect("client-initiated session");
}

async fn server_sees(server: &TlsHandle, client_id: &PeerId) -> bool {
    wait_for_condition(Duration::from_secs(10), || async {
        server.registry.has_connection_to_peer(client_id).await
    })
    .await
}

// ---- tests ----

/// Once the client is gone the server must not dial toward it: zero TCP dial
/// attempts and no warning/error lines about the client, over a bounded window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_never_dials_client_only_peer_after_disconnect() -> Result<(), DynError> {
    let logs = install_log_capture();
    let server = node_on(SecretKey::generate(), loopback(), cfg(false)).await;
    let client = node_on(SecretKey::generate(), loopback(), cfg(true)).await;
    let client_id = client.registry.peer_id.clone();
    let client_addr = client.registry.bind_addr;

    dial(&client, &server).await;
    assert!(
        server_sees(&server, &client_id).await,
        "session never formed"
    );

    client.shutdown_and_wait().await;
    assert!(
        wait_for_condition(Duration::from_secs(10), || async {
            !server.registry.has_connection_to_peer(&client_id).await
        })
        .await,
        "server never noticed the client leaving (window would be vacuous)"
    );

    // Let the teardown's own (expected) log lines land before the window opens.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let dials_before = server.registry.connection_pool.outbound_tcp_dial_attempts();
    let log_mark = logs.lock().unwrap().len();
    // Absence can only be observed over time: several gossip (100ms) and
    // supervisor (200ms) periods.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        server.registry.connection_pool.outbound_tcp_dial_attempts(),
        dials_before,
        "server dialed a client-only peer after it disconnected"
    );
    assert_eq!(dials_before, 0, "server must never have dialed the client");
    let needles = vec![client_id.to_string(), client_addr.to_string()];
    let window_lines: Vec<String> = logs.lock().unwrap()[log_mark..].to_vec();
    let noisy = log_lines_mentioning(&Mutex::new(window_lines), &needles);
    assert!(
        noisy.is_empty(),
        "unexpected warn/error lines about the client-only peer: {noisy:#?}"
    );
    Ok(())
}

/// A server that itself configured (supervises) the client's address learns
/// from the authenticated Hello that the peer is client-only and stops
/// supervising/redialing it: no further dials, no "required peer unreachable"
/// error lines once the client is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_client_only_peer_is_not_supervised_or_redialed() -> Result<(), DynError> {
    let logs = install_log_capture();
    let server = node_on(SecretKey::generate(), loopback(), cfg(false)).await;
    let client = node_on(SecretKey::generate(), loopback(), cfg(true)).await;
    let client_id = client.registry.peer_id.clone();

    let peer = server.add_peer(&client_id).await;
    peer.connect(&client.registry.bind_addr).await?;
    assert!(
        server_sees(&server, &client_id).await,
        "session never formed"
    );

    let client_addr = client.registry.bind_addr;
    client.shutdown_and_wait().await;
    // A pooled outbound connection to an exited process is only noticed when
    // something fails against it; surface the close as the transport does.
    let _ = server
        .registry
        .handle_peer_connection_failure(client_addr, None)
        .await;
    assert!(
        wait_for_condition(Duration::from_secs(10), || async {
            !server.registry.has_connection_to_peer(&client_id).await
        })
        .await,
        "server never noticed the client leaving (window would be vacuous)"
    );
    // Let any attempt already in flight at disconnect time settle.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let dials_before = server.registry.connection_pool.outbound_tcp_dial_attempts();
    let log_mark = logs.lock().unwrap().len();
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        server.registry.connection_pool.outbound_tcp_dial_attempts(),
        dials_before,
        "server kept redialing a client-only peer it had configured"
    );
    let window_lines: Vec<String> = logs.lock().unwrap()[log_mark..].to_vec();
    let noisy = log_lines_mentioning(&Mutex::new(window_lines), &[client_id.to_string()]);
    assert!(
        noisy.is_empty(),
        "unexpected warn/error lines about the client-only peer: {noisy:#?}"
    );
    Ok(())
}

/// While connected, the server can tell and ask an actor the client exposes,
/// over the session the client initiated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_tells_and_asks_client_actor_over_client_initiated_session() -> Result<(), DynError>
{
    let server = node_on(SecretKey::generate(), loopback(), cfg(false)).await;
    let client = node_on(SecretKey::generate(), loopback(), cfg(true)).await;
    let client_id = client.registry.peer_id.clone();
    let tells = serve_echo(&client, "client").await;

    dial(&client, &server).await;
    assert!(server_sees(&server, &client_id).await);

    let conn = server
        .lookup_peer(&client_id)
        .await?
        .connection_ref()
        .expect("server holds the client's session");

    conn.tell_actor_frame(ACTOR_ID, TYPE_HASH, Bytes::from_static(b"push:1"))
        .await?;
    assert!(
        wait_for_condition(Duration::from_secs(5), || async {
            tells.load(Ordering::Acquire) >= 1
        })
        .await,
        "client never received the server's tell"
    );

    let reply = conn
        .ask_actor_frame(
            ACTOR_ID,
            TYPE_HASH,
            Bytes::from_static(b"ping"),
            Duration::from_secs(3),
        )
        .await?;
    assert_eq!(reply.as_ref(), b"client:ping");
    assert_eq!(
        server.registry.connection_pool.outbound_tcp_dial_attempts(),
        0,
        "pushing to the client must reuse its session, never dial"
    );
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Connected,
    Disconnected,
}

struct Recorder(mpsc::UnboundedSender<Event>);

impl PeerConnectHandler for Recorder {
    fn handle_peer_connect(&self, _addr: SocketAddr, _peer: Option<PeerId>) -> BoxFuture<'_, ()> {
        let _ = self.0.send(Event::Connected);
        Box::pin(async {})
    }
}

impl PeerDisconnectHandler for Recorder {
    fn handle_peer_disconnect(
        &self,
        _addr: SocketAddr,
        _peer: Option<PeerId>,
    ) -> BoxFuture<'_, ()> {
        let _ = self.0.send(Event::Disconnected);
        Box::pin(async {})
    }
}

async fn next_event(rx: &mut mpsc::UnboundedReceiver<Event>, want: Event, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(ev)) if ev == want => return,
            Ok(Some(_)) => continue,
            _ => panic!("never observed {what}"),
        }
    }
}

/// After the server restarts the client's supervisor reconnects; the app sees
/// disconnected then connected and can re-issue a request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_reconnects_after_server_restart_and_observes_events() -> Result<(), DynError> {
    let server_key = SecretKey::generate();
    let server = node_on(server_key.clone(), loopback(), cfg(false)).await;
    let server_addr = server.registry.bind_addr;
    let server_id = server.registry.peer_id.clone();
    serve_echo(&server, "server-1").await;

    let client = node_on(SecretKey::generate(), loopback(), cfg(true)).await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    client
        .registry
        .set_peer_connect_handler(Arc::new(Recorder(tx.clone())))
        .await;
    client
        .registry
        .set_peer_disconnect_handler(Arc::new(Recorder(tx)))
        .await;

    dial(&client, &server).await;
    next_event(&mut rx, Event::Connected, "initial connect").await;
    let ask = |client: &TlsHandle, id: PeerId| {
        let client = client.client();
        async move {
            client
                .lookup_peer(&id)
                .await?
                .connection_ref()
                .expect("connection")
                .ask_actor_frame(
                    ACTOR_ID,
                    TYPE_HASH,
                    Bytes::from_static(b"hi"),
                    Duration::from_secs(3),
                )
                .await
        }
    };
    assert_eq!(
        ask(&client, server_id.clone()).await?.as_ref(),
        b"server-1:hi"
    );

    server.shutdown_and_wait().await;
    next_event(&mut rx, Event::Disconnected, "disconnect after server stop").await;

    let server2 = node_on(server_key, server_addr, cfg(false)).await;
    serve_echo(&server2, "server-2").await;
    next_event(&mut rx, Event::Connected, "reconnect after server restart").await;

    // Re-issue the request on the fresh session (the app's re-subscribe).
    let mut reply = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while reply.is_none() && tokio::time::Instant::now() < deadline {
        match ask(&client, server_id.clone()).await {
            Ok(r) => reply = Some(r),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    assert_eq!(reply.expect("re-issued ask").as_ref(), b"server-2:hi");
    Ok(())
}
