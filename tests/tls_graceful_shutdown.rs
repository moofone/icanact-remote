//! A node that shuts down explicitly closes its TLS sessions with
//! `close_notify`; the surviving peer treats that as an expected close (no
//! read-error / unexpected-exit / handler warnings) while still retiring the
//! session and failing pending asks promptly. A peer that vanishes without the
//! alert (owner drop = forced abort), a truncated first frame, and pre-handshake
//! failures stay diagnosable warnings.
//!
//! One `#[test]` on purpose: the log capture is a process-global subscriber, so
//! scenarios run sequentially and reset it between them. Scenarios wait on
//! terminal log events (never a fixed settle delay) before asserting that a
//! warning is absent.
mod common;

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use icanact_remote::registry::{ActorAskHandlerSync, AskDisposition};
use icanact_remote::{AskContext, GossipError, RemoteConnection};
use tokio::io::AsyncWriteExt;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, registry::LookupSpan};

use common::{
    TlsHandle, create_ordered_tls_pair, create_tls_node, fast_gossip_config, seed_peer,
    wait_for_condition, wait_for_pair_connection,
};

const READ_ERROR: &str = "IO task read error";
const CURRENT_EXIT: &str = "transport_io_task_exit_current_connection";
const WRITER_EXIT: &str = "Background writer task EXITED";
const HANDLER_EXIT: &str = "Incoming TLS connection handler loop exited";
const NOT_USABLE: &str = "is not usable";
const FIRST_FRAME: &str = "Failed to read initial message";
const PRE_HANDSHAKE: &str = "inbound_pre_handshake_eof";
const WRITER_ORDERLY: &str = "background writer task exited after an orderly close";
const HANDLER_ORDERLY: &str = "incoming TLS connection handler loop exited after an orderly close";
const FIRST_FRAME_CLEAN: &str = "peer closed the TLS stream cleanly before its first frame";
const FAILURE_WARNINGS: [&str; 6] = [
    READ_ERROR,
    CURRENT_EXIT,
    WRITER_EXIT,
    HANDLER_EXIT,
    FIRST_FRAME,
    NOT_USABLE,
];

fn captured() -> &'static Mutex<Vec<(Level, String)>> {
    static LOG: OnceLock<Mutex<Vec<(Level, String)>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::new()))
}

struct Message(String);
impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        }
    }
}

struct Capture;
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() > Level::DEBUG {
            return;
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        if let Ok(mut log) = captured().lock() {
            log.push((*event.metadata().level(), message.0));
        }
    }
}

fn reset_log() {
    captured().lock().unwrap().clear();
}

fn count(level_at_most: Level, needle: &str) -> usize {
    captured()
        .lock()
        .unwrap()
        .iter()
        .filter(|(level, message)| *level <= level_at_most && message.contains(needle))
        .count()
}

fn warned(needle: &str) -> bool {
    count(Level::WARN, needle) > 0
}

async fn wait_for_log(level_at_most: Level, needle: &str, at_least: usize) {
    let ok = wait_for_condition(Duration::from_secs(15), || async move {
        count(level_at_most, needle) >= at_least
    })
    .await;
    assert!(
        ok,
        "terminal event {needle:?} x{at_least} never logged; log: {:#?}",
        captured().lock().unwrap()
    );
}

fn assert_no_failure_warnings(context: &str) {
    for needle in FAILURE_WARNINGS {
        assert!(
            !warned(needle),
            "{context}: orderly close must not warn {needle:?}; log: {:#?}",
            captured().lock().unwrap()
        );
    }
}

fn run(test: impl std::future::Future<Output = ()> + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(8 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(test);
        })
        .expect("spawn")
        .join()
        .expect("test panicked");
}

async fn established_pair(a: &str, b: &str) -> (TlsHandle, TlsHandle) {
    let (high, low) = create_ordered_tls_pair(a, b).await.expect("pair");
    connect_pair(high, low).await
}

async fn connect_pair(high: TlsHandle, low: TlsHandle) -> (TlsHandle, TlsHandle) {
    seed_peer(&high, &low).await.expect("seed");
    assert!(wait_for_pair_connection(&high, &low, Duration::from_secs(10)).await);
    assert!(
        wait_for_condition(Duration::from_secs(10), || async {
            high.registry
                .has_connection_to_peer(&low.registry.peer_id)
                .await
                && low
                    .registry
                    .has_connection_to_peer(&high.registry.peer_id)
                    .await
        })
        .await,
        "both sides must hold the session"
    );
    (high, low)
}

async fn assert_session_retired(survivor: &TlsHandle, gone: &icanact_remote::PeerId) {
    assert!(
        wait_for_condition(Duration::from_secs(5), || async {
            !survivor.registry.has_connection_to_peer(gone).await
        })
        .await,
        "the surviving node must retire the session promptly"
    );
}

/// Both IO tasks (one per side) and the inbound handler reached their terminal
/// orderly-exit log lines.
async fn wait_for_orderly_teardown() {
    wait_for_log(Level::DEBUG, WRITER_ORDERLY, 2).await;
    wait_for_log(Level::DEBUG, HANDLER_ORDERLY, 1).await;
}

/// Accepts every ask and never answers it (deferred forever), without blocking
/// the connection's IO task.
struct NeverReplies {
    invoked: Arc<AtomicBool>,
}

impl ActorAskHandlerSync for NeverReplies {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        _context: AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        self.invoked.store(true, Ordering::SeqCst);
        Ok(AskDisposition::Deferred)
    }
}

async fn connection_to(from: &TlsHandle, to: &TlsHandle) -> RemoteConnection {
    let found: Mutex<Option<RemoteConnection>> = Mutex::new(None);
    let slot = &found;
    let ok = wait_for_condition(Duration::from_secs(5), || async move {
        if let Ok(peer) = from.lookup_peer(&to.registry.peer_id).await
            && let Some(connection) = peer.connection_ref()
        {
            *slot.lock().unwrap() = Some(connection);
            return true;
        }
        false
    })
    .await;
    assert!(ok, "connection lookup");
    found.into_inner().unwrap().expect("connection")
}

/// An ask that is pending when the session closes resolves with a connection
/// error long before its (huge) deadline, on both the closing and the
/// surviving node.
async fn pending_asks_resolve_promptly_scenario() {
    let (high, low) = create_ordered_tls_pair("graceful_ask_a", "graceful_ask_b")
        .await
        .expect("pair");
    let high_invoked = Arc::new(AtomicBool::new(false));
    let low_invoked = Arc::new(AtomicBool::new(false));
    high.registry
        .set_actor_ask_handler_sync(Arc::new(NeverReplies {
            invoked: high_invoked.clone(),
        }))
        .await;
    low.registry
        .set_actor_ask_handler_sync(Arc::new(NeverReplies {
            invoked: low_invoked.clone(),
        }))
        .await;
    // Handlers are snapshotted into each connection, so install them first.
    let (high, low) = connect_pair(high, low).await;

    let to_low = connection_to(&high, &low).await;
    let to_high = connection_to(&low, &high).await;
    let deadline = Duration::from_secs(120);
    let survivor_ask = tokio::spawn(async move {
        to_low
            .ask_actor_frame_aligned(1, 0xA5C0_0001, bytes::Bytes::from_static(b"s"), deadline)
            .await
    });
    let closer_ask = tokio::spawn(async move {
        to_high
            .ask_actor_frame_aligned(1, 0xA5C0_0001, bytes::Bytes::from_static(b"c"), deadline)
            .await
    });
    assert!(
        wait_for_condition(Duration::from_secs(10), || async {
            high_invoked.load(Ordering::SeqCst) && low_invoked.load(Ordering::SeqCst)
        })
        .await,
        "both asks must be in flight on the remote handlers"
    );

    let started = Instant::now();
    low.shutdown().await;
    for (name, ask) in [
        ("surviving node", survivor_ask),
        ("closing node", closer_ask),
    ] {
        let result = tokio::time::timeout(Duration::from_secs(10), ask)
            .await
            .unwrap_or_else(|_| {
                panic!("{name}: pending ask must resolve, not wait for its deadline")
            })
            .expect("ask task");
        let error = result.expect_err("ask over a closed session must fail");
        assert!(
            !matches!(error, GossipError::Timeout),
            "{name}: ask must fail as dropped/closed, not by deadline timeout: {error:?}"
        );
    }
    assert!(started.elapsed() < Duration::from_secs(10));
    high.shutdown().await;
}

async fn explicit_shutdown_scenarios() {
    // Inbound side (low) shuts down with shutdown().await; high survives.
    let (high, low) = established_pair("graceful_tls_a", "graceful_tls_b").await;
    reset_log();
    let low_peer = low.registry.peer_id.clone();
    low.shutdown().await;
    assert_session_retired(&high, &low_peer).await;
    wait_for_orderly_teardown().await;
    assert_no_failure_warnings("inbound-side shutdown()");
    high.shutdown().await;

    // Outbound side (high) shuts down with shutdown_and_wait(); low survives
    // and observes the peer close on its inbound handler.
    let (high, low) = established_pair("graceful_tls_e", "graceful_tls_f").await;
    reset_log();
    let high_peer = high.registry.peer_id.clone();
    high.shutdown_and_wait().await;
    assert_session_retired(&low, &high_peer).await;
    wait_for_orderly_teardown().await;
    assert_no_failure_warnings("outbound-side shutdown_and_wait()");
    low.shutdown().await;
}

async fn owner_drop_scenario() {
    let (high, low) = established_pair("graceful_tls_c", "graceful_tls_d").await;
    reset_log();
    drop(low);
    wait_for_log(Level::WARN, READ_ERROR, 1).await;
    high.shutdown().await;
}

/// Raw, mutually-authenticated TLS client that completes the hello handshake
/// against `node` and returns the stream, positioned before the first frame.
async fn raw_client_after_hello(
    node: &TlsHandle,
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let cfg = icanact_remote::tls::TlsConfig::new(icanact_remote::SecretKey::generate()).unwrap();
    let tcp = tokio::net::TcpStream::connect(node.registry.bind_addr)
        .await
        .unwrap();
    let name = rustls::pki_types::ServerName::try_from("peer-1.icanact.invalid").unwrap();
    let mut tls = cfg.connector().connect(name, tcp).await.expect("tls");
    let alpn = tls.get_ref().1.alpn_protocol().map(|p| p.to_vec());
    icanact_remote::handshake::perform_hello_handshake_with_role(
        &mut tls,
        alpn.as_deref(),
        node.registry.config.enable_peer_discovery,
        node.registry.config.schema_hash,
        icanact_remote::handshake::RemoteBootId::new(),
        false,
    )
    .await
    .expect("hello");
    tls
}

async fn initial_frame_scenarios() {
    let node = create_tls_node(fast_gossip_config()).await.expect("node");

    // Clean close before any first-frame byte: expected, not a warning.
    reset_log();
    let mut tls = raw_client_after_hello(&node).await;
    tls.shutdown().await.expect("close_notify");
    drop(tls);
    wait_for_log(Level::DEBUG, FIRST_FRAME_CLEAN, 1).await;
    assert!(!warned(FIRST_FRAME), "clean pre-frame close must not warn");

    // close_notify after a truncated length prefix: still a diagnosable failure.
    reset_log();
    let mut tls = raw_client_after_hello(&node).await;
    tls.write_all(&[0, 0]).await.unwrap();
    tls.shutdown().await.expect("close_notify");
    drop(tls);
    wait_for_log(Level::WARN, FIRST_FRAME, 1).await;
    assert_eq!(count(Level::DEBUG, FIRST_FRAME_CLEAN), 0);

    // Abrupt TCP drop with no alert at all (even with zero prefix bytes).
    reset_log();
    let tls = raw_client_after_hello(&node).await;
    drop(tls);
    wait_for_log(Level::WARN, FIRST_FRAME, 1).await;
    assert_eq!(count(Level::DEBUG, FIRST_FRAME_CLEAN), 0);

    // Pre-handshake failures keep their own diagnostics.
    reset_log();
    drop(
        tokio::net::TcpStream::connect(node.registry.bind_addr)
            .await
            .unwrap(),
    );
    wait_for_log(Level::WARN, PRE_HANDSHAKE, 1).await;
    reset_log();
    let mut garbage = tokio::net::TcpStream::connect(node.registry.bind_addr)
        .await
        .unwrap();
    garbage
        .write_all(b"not a tls client hello at all\r\n\r\n")
        .await
        .unwrap();
    wait_for_log(Level::WARN, "TLS accept failed", 1).await;
    drop(garbage);

    node.shutdown().await;
}

#[test]
fn graceful_shutdown_is_orderly_and_faults_stay_diagnosable() {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture))
        .expect("single global subscriber for this test binary");
    run(async {
        explicit_shutdown_scenarios().await;
        pending_asks_resolve_promptly_scenario().await;
        owner_drop_scenario().await;
        initial_frame_scenarios().await;
    });
}
