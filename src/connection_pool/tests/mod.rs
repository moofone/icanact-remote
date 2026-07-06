use super::*;
use futures::StreamExt;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::runtime::Builder;
use tokio::time::sleep;

struct TestActor;

impl crate::registry::ActorMessageHandlerSync for TestActor {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> crate::Result<Option<crate::registry::ActorResponse>> {
        if actor_id != 0xC0DE_BEEF || type_hash != 0xA11C_0001 {
            return Ok(None);
        }
        if correlation_id.is_some() {
            Ok(Some(crate::registry::ActorResponse::from(payload)))
        } else {
            Ok(None)
        }
    }
}

struct DeferredTestActor;

impl crate::registry::ActorAskHandlerSync for DeferredTestActor {
    fn handle_actor_ask_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::AlignedBytes,
        context: crate::AskContext<'_>,
    ) -> crate::Result<crate::registry::AskDisposition> {
        if actor_id != 0xD3F3_10AB || type_hash != 0xA55D_0001 {
            return Ok(crate::registry::AskDisposition::Deferred);
        }

        let responder = context.responder();
        tokio::spawn(async move {
            let _ = responder.reply_bytes(payload.into_bytes()).await;
        });

        Ok(crate::registry::AskDisposition::Deferred)
    }
}

struct ImmediateMissActor;

impl crate::registry::ActorAskImmediateHandlerSync for ImmediateMissActor {
    fn can_handle_actor_ask_sync_immediate(&self, _actor_id: u64, _type_hash: u32) -> bool {
        false
    }

    fn handle_actor_ask_sync_immediate(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: crate::AlignedBytes,
    ) -> crate::Result<crate::registry::AskDisposition> {
        unreachable!("can_handle=false should prevent immediate ask dispatch")
    }
}

const TEST_TELL_ACTOR_ID: u64 = 0xC0DE_BEEF;
const TEST_TELL_HASH: u32 = 0xA11C_0001;
struct TestActorCounter {
    delivered: Arc<AtomicU64>,
}

impl crate::registry::ActorMessageHandlerSync for TestActorCounter {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        _payload: crate::AlignedBytes,
        _correlation_id: Option<u16>,
    ) -> crate::Result<Option<crate::registry::ActorResponse>> {
        if actor_id != TEST_TELL_ACTOR_ID {
            return Ok(None);
        }
        if type_hash == TEST_TELL_HASH {
            self.delivered.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        Ok(None)
    }
}

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024; // Prevent stack overflow during large test runs
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;

fn run_multi_thread_test<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("icanact-conn-test".into())
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build test runtime");
            rt.block_on(future);
        })
        .expect("failed to spawn test thread");
    handle.join().expect("test thread panicked unexpectedly");
}

fn reset_io_perf() {
    let _ = IoPerfCounters::global().snapshot_and_reset();
}

fn print_io_perf(label: &str) {
    let (
        read_calls,
        read_ns,
        handle_calls,
        handle_ns,
        write_calls,
        write_ns,
        ask_write_calls,
        ask_write_ns,
    ) = IoPerfCounters::global().snapshot_and_reset();
    println!(
        "[{label}_io_perf] read_calls={} read_avg_us={:.3} handle_calls={} handle_avg_us={:.3} write_calls={} write_avg_us={:.3} ask_write_calls={} ask_write_avg_us={:.3}",
        read_calls,
        (read_ns as f64 / 1000.0) / (read_calls.max(1) as f64),
        handle_calls,
        (handle_ns as f64 / 1000.0) / (handle_calls.max(1) as f64),
        write_calls,
        (write_ns as f64 / 1000.0) / (write_calls.max(1) as f64),
        ask_write_calls,
        (ask_write_ns as f64 / 1000.0) / (ask_write_calls.max(1) as f64),
    );
}

#[test]
fn resolve_connection_conflict_is_identity_only() {
    use super::ConnectionConflictDecision::*;
    // No live rival (stale/dead entry, or none at all) and incoming is
    // identity-preferred -> take the incoming.
    assert_eq!(
        resolve_connection_conflict(false, true, true),
        AcceptIncoming
    );
    assert_eq!(
        resolve_connection_conflict(false, false, true),
        AcceptIncoming
    );
    // No live rival, but incoming is *not* identity-preferred either -> evict
    // the stale rival, but do not accept the incoming as the session.
    assert_eq!(
        resolve_connection_conflict(false, true, false),
        EvictStaleRejectIncoming
    );
    assert_eq!(
        resolve_connection_conflict(false, false, false),
        EvictStaleRejectIncoming
    );
    // Live rival the tie-break prefers, incoming not preferred -> keep rival.
    assert_eq!(
        resolve_connection_conflict(true, true, false),
        RejectIncoming
    );
    // Live rival, tie-break does not prefer it, incoming preferred -> replace.
    assert_eq!(
        resolve_connection_conflict(true, false, true),
        ReplaceExisting
    );
    // Live rival the tie-break prefers AND incoming also nominally preferred
    // (a redundant simultaneous success on the same, already-correct,
    // direction) -> keep the rival rather than orphaning it for an
    // equally-valid duplicate. This is stricter than treating `keep_incoming`
    // as an automatic override: `keep_existing` already answered "is this
    // session tie-break-correct" and must not be second-guessed by a
    // redundant incoming candidate that asks the identical question.
    assert_eq!(
        resolve_connection_conflict(true, true, true),
        RejectIncoming
    );
    // Neither side is strictly preferred and the rival is live (degenerate
    // input; not reachable via `should_keep_connection` in practice, but the
    // function must still resolve it deterministically) -> keep the rival.
    assert_eq!(
        resolve_connection_conflict(true, false, false),
        RejectIncoming
    );
    // The decision signature carries no SocketAddr: the structural invariant
    // that a keep/drop outcome can never depend on a peer's address, only on
    // its verified identity. (Compile-time: the calls above pass only bools.)
}

/// Pins the exact (existing_usable, keep_existing, keep_incoming) -> decision
/// contract that each of the routed call sites documented on
/// `resolve_connection_conflict` relies on. This is the cross-site invariant:
/// if a future change to the shared function's logic breaks any one site's
/// assumption, it fails HERE — at the single shared authority — rather than
/// silently at one specific call site while the others still (by luck) work,
/// which is exactly the "drifting second copy" pattern that caused the
/// original address-keyed thrash. Each block below is labelled with the real
/// call site and mirrors the log-event name that site emits for that input.
#[test]
fn resolve_connection_conflict_matches_all_routed_call_sites() {
    use super::ConnectionConflictDecision::*;

    // --- Outbound finalize (pool_connect.rs, finalize_new_outbound_connection) ---
    // keep_incoming = should_keep_connection(peer, true) (fixed: a freshly
    // dialed outbound just succeeded).
    // existing usable, wrong direction, incoming preferred -> replace ("outbound
    // finalize" publish path).
    assert_eq!(
        resolve_connection_conflict(true, false, true),
        ReplaceExisting
    );
    // existing usable and preferred (redundant simultaneous outbound success)
    // -> keep existing, do not republish ("outbound finalize kept existing
    // preferred session; not displacing it").
    assert_eq!(
        resolve_connection_conflict(true, true, true),
        RejectIncoming
    );
    // existing stale, incoming (outbound) not preferred (higher-NodeId
    // fallback dial) -> evict stale, do not publish ("outbound finalize
    // evicted a stale rival but declined to publish...").
    assert_eq!(
        resolve_connection_conflict(false, false, false),
        EvictStaleRejectIncoming
    );
    // existing stale, incoming (outbound) preferred -> accept.
    assert_eq!(
        resolve_connection_conflict(false, false, true),
        AcceptIncoming
    );

    // --- Inbound accept (handle.rs, handle_incoming_connection_tls) ---
    // keep_incoming = should_keep_connection(peer, false) (a freshly accepted
    // inbound socket already exists). The "no existing at all" fast path is
    // an explicitly-documented exception and is not exercised here.
    // existing stale, new inbound preferred -> accept ("inbound_tiebreak_evict_stale"
    // + "inbound_connection_accepted").
    assert_eq!(
        resolve_connection_conflict(false, false, true),
        AcceptIncoming
    );
    // existing stale, new inbound NOT preferred -> evict stale, reject
    // ("inbound_tiebreak_evict_stale" + "inbound_tiebreak_reject_non_preferred_inbound").
    assert_eq!(
        resolve_connection_conflict(false, false, false),
        EvictStaleRejectIncoming
    );
    // existing usable, wrong direction, new inbound preferred -> replace
    // ("inbound_tiebreak_replace_wrong_direction").
    assert_eq!(
        resolve_connection_conflict(true, false, true),
        ReplaceExisting
    );
    // existing usable and preferred (duplicate inbound, or existing outbound
    // correctly kept) -> reject ("inbound_tiebreak_reject_live_duplicate").
    assert_eq!(
        resolve_connection_conflict(true, true, false),
        RejectIncoming
    );
    assert_eq!(
        resolve_connection_conflict(true, true, true),
        RejectIncoming
    );

    // --- Outbound top-of-dial, stale-rival branch only (transport_stream.rs,
    // connect_via_stream, the `!alive` arm) ---
    // Both outcomes this site can receive when the rival is stale lead to the
    // identical action there (evict); pinned here so a future change cannot
    // silently make the stale branch stop evicting for either outcome.
    for keep_incoming in [true, false] {
        let decision = resolve_connection_conflict(false, false, keep_incoming);
        assert!(
            matches!(decision, AcceptIncoming | EvictStaleRejectIncoming),
            "outbound top-of-dial's stale-rival branch expects only \
             AcceptIncoming/EvictStaleRejectIncoming (both evict), got {decision:?}"
        );
    }
    // The alive-rival branch at that same call site is a documented,
    // justified exception (see `resolve_connection_conflict`'s doc comment)
    // and is intentionally not exercised through this function.
}

#[test]
fn disconnect_by_peer_id_preserves_session_correlation_tracker() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("session_correlation").peer_id();
    let addr: SocketAddr = "127.0.0.1:40555".parse().unwrap();

    pool.set_configured_peer_addr(&peer_id, addr);
    let original = pool.get_or_create_correlation_tracker(&peer_id);

    let connection = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
    connection.set_state(ConnectionState::Connected);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, connection));

    pool.disconnect_connection_by_peer_id(&peer_id)
        .expect("expected connection to be removed");

    let preserved = pool.get_or_create_correlation_tracker(&peer_id);
    assert!(Arc::ptr_eq(&original, &preserved));
    assert_eq!(pool.get_configured_peer_addr(&peer_id), Some(addr));
}

#[tokio::test]
async fn disconnect_by_peer_id_removes_configured_addr_connection_without_alias_row() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("configured_addr_disconnect").peer_id();
    let addr: SocketAddr = "127.0.0.1:40557".parse().unwrap();

    let (io, _peer_io) = tokio::io::duplex(1024);
    let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut connection = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
    connection.stream_handle = Some(Arc::new(stream_handle));
    connection.set_state(ConnectionState::Connected);
    let connection = Arc::new(connection);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, connection));

    let _ = pool.addr_to_peer_id.remove_sync(&addr);
    pool.clear_current_peer_connection(&peer_id);
    assert!(
        pool.get_connection_by_peer_id(&peer_id).is_some(),
        "test setup: configured-address fallback must be able to find the connection"
    );

    pool.disconnect_connection_by_peer_id(&peer_id)
        .expect("expected connection to be removed");

    assert!(
        pool.get_connection_by_peer_id(&peer_id).is_none(),
        "disconnect must not leave a stale connection reachable by configured-address fallback"
    );
    assert_eq!(pool.get_configured_peer_addr(&peer_id), Some(addr));
}

#[tokio::test]
async fn get_connection_by_peer_id_uses_session_current_connection() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("session_current_connection").peer_id();
    let addr: SocketAddr = "127.0.0.1:40556".parse().unwrap();

    let (io, _peer_io) = tokio::io::duplex(1024);
    let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut connection = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
    connection.stream_handle = Some(Arc::new(stream_handle));
    connection.set_state(ConnectionState::Connected);
    let connection = Arc::new(connection);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, connection.clone()));

    let _ = pool.connections_by_peer.remove_sync(&peer_id);

    let resolved = pool
        .get_connection_by_peer_id(&peer_id)
        .expect("session should retain current connection");
    assert!(Arc::ptr_eq(&resolved, &connection));
}

#[tokio::test]
async fn get_connection_by_peer_id_recovers_live_alias_connection() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("session_alias_connection").peer_id();
    let configured_addr: SocketAddr = "127.0.0.1:40558".parse().unwrap();
    let alias_addr: SocketAddr = "127.0.0.1:50558".parse().unwrap();

    pool.set_configured_peer_addr(&peer_id, configured_addr);
    let (io, _peer_io) = tokio::io::duplex(1024);
    let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        alias_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut connection = LockFreeConnection::new(alias_addr, ConnectionDirection::Inbound);
    connection.stream_handle = Some(Arc::new(stream_handle));
    connection.embedded_peer_id = Some(peer_id.clone());
    connection.set_state(ConnectionState::Connected);
    let connection = Arc::new(connection);

    pool.index_connection_by_addr(alias_addr, connection.clone());
    pool.add_addr_to_peer_id(alias_addr, peer_id.clone());

    let resolved = pool
        .get_connection_by_peer_id(&peer_id)
        .expect("alias should retain live inbound connection");
    assert!(Arc::ptr_eq(&resolved, &connection));
    assert!(pool.has_connection_by_peer_id(&peer_id));
    assert_eq!(
        pool.get_configured_peer_addr(&peer_id),
        Some(configured_addr)
    );
}

#[tokio::test]
async fn get_connection_by_peer_id_rejects_alias_identity_mismatch() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let victim_peer_id = crate::KeyPair::new_for_testing("alias_victim_peer").peer_id();
    let attacker_peer_id = crate::KeyPair::new_for_testing("alias_attacker_peer").peer_id();
    let configured_addr: SocketAddr = "127.0.0.1:40560".parse().unwrap();
    let alias_addr: SocketAddr = "127.0.0.1:50560".parse().unwrap();

    pool.set_configured_peer_addr(&victim_peer_id, configured_addr);
    let (io, _peer_io) = tokio::io::duplex(1024);
    let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        alias_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut connection = LockFreeConnection::new(alias_addr, ConnectionDirection::Inbound);
    connection.stream_handle = Some(Arc::new(stream_handle));
    connection.embedded_peer_id = Some(attacker_peer_id);
    connection.set_state(ConnectionState::Connected);
    let connection = Arc::new(connection);

    pool.index_connection_by_addr(alias_addr, connection);
    pool.add_addr_to_peer_id(alias_addr, victim_peer_id.clone());

    assert!(pool.get_connection_by_peer_id(&victim_peer_id).is_none());
    assert!(!pool.has_connection_by_peer_id(&victim_peer_id));
}

#[tokio::test]
async fn remove_connection_cleans_all_address_aliases_for_same_stream() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("remove_alias_connection").peer_id();
    let configured_addr: SocketAddr = "127.0.0.1:40559".parse().unwrap();
    let alias_addr: SocketAddr = "127.0.0.1:50559".parse().unwrap();

    let (io, _peer_io) = tokio::io::duplex(1024);
    let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        alias_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut connection = LockFreeConnection::new(alias_addr, ConnectionDirection::Inbound);
    connection.stream_handle = Some(Arc::new(stream_handle));
    connection.set_state(ConnectionState::Connected);
    let connection = Arc::new(connection);

    assert!(pool.add_connection_by_peer_id(peer_id.clone(), configured_addr, connection.clone()));
    pool.index_connection_by_addr(alias_addr, connection.clone());
    pool.add_addr_to_peer_id(alias_addr, peer_id.clone());

    let removed = pool
        .remove_connection(alias_addr)
        .expect("alias removal should remove the connection");

    assert!(Arc::ptr_eq(&removed, &connection));
    assert!(pool.get_connection_by_addr(&alias_addr).is_none());
    assert!(pool.get_connection_by_addr(&configured_addr).is_none());
    assert!(pool.get_connection_by_peer_id(&peer_id).is_none());
}

#[test]
fn connection_count_uses_session_current_connection() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("session_connection_count").peer_id();
    let addr: SocketAddr = "127.0.0.1:40557".parse().unwrap();

    let connection = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
    connection.set_state(ConnectionState::Connected);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, connection));

    let _ = pool.connections_by_peer.remove_sync(&peer_id);

    assert_eq!(pool.connection_count(), 1);
}

#[test]
fn deferred_actor_ask_sync_replies_via_responder() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:40501".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:40502".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("deferred_actor_ask_server")),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_ask_handler_sync(Arc::new(DeferredTestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("deferred_actor_ask_client")),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: Some(response_writer.clone()),
            tell_handler_sync: server_registry.actor_tell_handler_sync.load_full(),
            tell_handler_sync_context: server_registry.actor_tell_handler_sync_context.load_full(),
            ask_immediate_handler_sync: None,
            ask_handler_sync: server_registry.actor_ask_handler_sync.load_full(),
            sync_actor_handler: None,
        };
        let (server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(server_read_ctx),
        );
        let server_writer = Arc::new(server_writer);
        response_writer.bind_stream_handle(server_writer.clone());

        let payload = bytes::Bytes::from_static(b"deferred-ping");
        let reply = client_conn
            .ask_actor_frame_no_timeout(0xD3F3_10AB, 0xA55D_0001, payload.clone())
            .await
            .unwrap();
        assert_eq!(reply, payload);

        client_writer.shutdown();
        server_writer.shutdown();
    });
}

#[test]
fn deferred_actor_ask_pending_wait_replies_repeatedly() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:40503".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:40504".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "deferred_actor_ask_pending_wait_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_ask_handler_sync(Arc::new(DeferredTestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "deferred_actor_ask_pending_wait_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: Some(response_writer.clone()),
            tell_handler_sync: server_registry.actor_tell_handler_sync.load_full(),
            tell_handler_sync_context: server_registry.actor_tell_handler_sync_context.load_full(),
            ask_immediate_handler_sync: None,
            ask_handler_sync: server_registry.actor_ask_handler_sync.load_full(),
            sync_actor_handler: None,
        };
        let (server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(server_read_ctx),
        );
        let server_writer = Arc::new(server_writer);
        response_writer.bind_stream_handle(server_writer.clone());

        for round in 0..128u8 {
            let payload = bytes::Bytes::from(vec![round; 32]);
            let pending = client_conn
                .ask_actor_frame_deferred(
                    0xD3F3_10AB,
                    0xA55D_0001,
                    payload.clone(),
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
            let reply = pending.wait().await.unwrap();
            assert_eq!(reply, payload);
        }

        client_writer.shutdown();
        server_writer.shutdown();
    });
}

#[test]
fn deferred_actor_ask_still_dispatches_when_immediate_handler_declines() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:40511".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:40512".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "deferred_actor_ask_server_immediate_miss",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_ask_immediate_handler_sync(Arc::new(ImmediateMissActor))
            .await;
        server_registry
            .set_actor_ask_handler_sync(Arc::new(DeferredTestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "deferred_actor_ask_client_immediate_miss",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: Some(response_writer.clone()),
            tell_handler_sync: server_registry.actor_tell_handler_sync.load_full(),
            tell_handler_sync_context: server_registry.actor_tell_handler_sync_context.load_full(),
            ask_immediate_handler_sync: server_registry
                .actor_ask_immediate_handler_sync
                .load_full(),
            ask_handler_sync: server_registry.actor_ask_handler_sync.load_full(),
            sync_actor_handler: None,
        };
        let (server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(server_read_ctx),
        );
        let server_writer = Arc::new(server_writer);
        response_writer.bind_stream_handle(server_writer.clone());

        let payload = bytes::Bytes::from_static(b"deferred-ping-immediate-miss");
        let reply = client_conn
            .ask_actor_frame_no_timeout(0xD3F3_10AB, 0xA55D_0001, payload.clone())
            .await
            .unwrap();
        assert_eq!(reply, payload);

        client_writer.shutdown();
        server_writer.shutdown();
    });
}

#[test]
fn get_connection_to_peer_reuses_existing_connection_correlation_tracker() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:40521".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:40522".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "existing_connection_correlation_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_ask_handler_sync(Arc::new(DeferredTestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "existing_connection_correlation_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));

        let peer_id = server_registry.peer_id.clone();
        let session_correlation = client_registry
            .connection_pool
            .get_or_create_correlation_tracker(&peer_id);

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
        let connection_correlation = CorrelationTracker::new();
        assert!(
            !Arc::ptr_eq(&session_correlation, &connection_correlation),
            "test requires a distinct live-connection correlation tracker"
        );

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: Some(peer_id.clone()),
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(connection_correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let mut connection = LockFreeConnection::new(server_addr, ConnectionDirection::Inbound);
        connection.stream_handle = Some(client_writer.clone());
        connection.correlation = Some(connection_correlation);
        connection.embedded_peer_id = Some(peer_id.clone());
        connection.set_state(ConnectionState::Connected);
        connection.update_last_used();
        assert!(client_registry.connection_pool.add_connection_by_peer_id(
            peer_id.clone(),
            server_addr,
            Arc::new(connection),
        ));

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: Some(client_registry.peer_id.clone()),
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: Some(response_writer.clone()),
            tell_handler_sync: server_registry.actor_tell_handler_sync.load_full(),
            tell_handler_sync_context: server_registry.actor_tell_handler_sync_context.load_full(),
            ask_immediate_handler_sync: None,
            ask_handler_sync: server_registry.actor_ask_handler_sync.load_full(),
            sync_actor_handler: None,
        };
        let (server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(server_read_ctx),
        );
        let server_writer = Arc::new(server_writer);
        response_writer.bind_stream_handle(server_writer.clone());

        let handle = client_registry
            .connection_pool
            .get_connection_to_peer(&peer_id)
            .await
            .expect("existing connection handle");

        let payload = bytes::Bytes::from_static(b"existing-connection-correlation");
        let reply = handle
            .ask_actor_frame_no_timeout(0xD3F3_10AB, 0xA55D_0001, payload.clone())
            .await
            .expect("reply should complete through the live connection tracker");
        assert_eq!(reply, payload);

        client_writer.shutdown();
        server_writer.shutdown();
    });
}

/// Simple in-memory writer that records bytes for verification without
/// requiring a TCP socket. Used to keep the send_data tests fully
/// deterministic and stack-friendly.
#[derive(Clone, Default)]
struct RecordingWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl RecordingWriter {
    fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
        let writer = Self::default();
        (writer.clone(), writer.buffer.clone())
    }
}

impl Unpin for RecordingWriter {}

impl tokio::io::AsyncRead for RecordingWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // No readable bytes. The IO task doesn't use reads in these tests (read_context=None),
        // but LockFreeStreamHandle requires AsyncRead + AsyncWrite.
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for RecordingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Ok(mut guard) = self.buffer.lock() {
            guard.extend_from_slice(buf); // ALLOW_COPY
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Copy, Default)]
struct ClosedWriter;

impl Unpin for ClosedWriter {}

impl tokio::io::AsyncRead for ClosedWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ClosedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, "writer closed")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(Error::new(ErrorKind::BrokenPipe, "writer closed")))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn test_connection_handle_debug() {
    // Compile-time test to ensure Debug is implemented
    use std::fmt::Debug;
    fn assert_debug<T: Debug>() {}
    assert_debug::<ConnectionHandle>();
}

#[test]
fn test_buffer_config_validation() {
    // Should reject buffers < 256KB
    let result = BufferConfig::new(100 * 1024);
    assert!(result.is_err());

    // Should accept valid sizes
    let config = BufferConfig::new(512 * 1024).unwrap();
    assert_eq!(config.tcp_buffer_size(), 512 * 1024);

    // Streaming threshold should be buffer_size - 1KB
    assert_eq!(config.streaming_threshold(), 511 * 1024);
}

#[test]
fn test_streaming_threshold_calculation() {
    let config = BufferConfig::new(1024 * 1024).unwrap();

    // 1MB buffer should have ~1MB-1KB threshold
    let threshold = config.streaming_threshold();
    assert!(threshold < config.tcp_buffer_size());
    assert!(threshold > 1020 * 1024); // At least 1020KB
    assert_eq!(threshold, 1023 * 1024); // Exactly 1023KB
}

#[test]
fn test_buffer_config_default() {
    let config = BufferConfig::default();
    assert_eq!(config.tcp_buffer_size(), 1024 * 1024); // 1MB
    assert_eq!(config.streaming_threshold(), 1023 * 1024); // 1MB - 1KB
    assert_eq!(config.ask_window(), crate::config::DEFAULT_ASK_WINDOW);
}

#[test]
fn test_buffer_config_minimum_size() {
    // Test exactly at minimum boundary
    let config = BufferConfig::new(256 * 1024).unwrap();
    assert_eq!(config.tcp_buffer_size(), 256 * 1024);
    assert_eq!(config.streaming_threshold(), 255 * 1024);

    // Test just below minimum (should fail)
    let result = BufferConfig::new(256 * 1024 - 1);
    assert!(result.is_err());
}

#[test]
fn test_streaming_threshold_saturation() {
    // Test that streaming_threshold handles edge cases properly
    let config = BufferConfig::new(256 * 1024).unwrap(); // Minimum buffer (256KB)
    // Should be 255KB (256KB - 1KB)
    assert_eq!(config.streaming_threshold(), 255 * 1024);

    // Test with exactly 1KB buffer would be rejected by validation,
    // but we can verify saturating_sub behavior directly
    let large_config = BufferConfig::new(2 * 1024 * 1024).unwrap(); // 2MB
    assert_eq!(large_config.streaming_threshold(), 2 * 1024 * 1024 - 1024);
}

#[tokio::test]
async fn test_connection_pool_new() {
    let pool = ConnectionPool::<()>::new(10, Duration::from_secs(5));
    assert_eq!(pool.connection_count(), 0);
    assert_eq!(pool.max_connections, 10);
    assert_eq!(pool.connection_timeout, Duration::from_secs(5));
}

#[tokio::test]
async fn test_outbound_dial_gate_is_released_when_leader_is_cancelled() {
    let pool = ConnectionPool::<()>::new(10, Duration::from_secs(5));
    let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    let gate = match pool.acquire_outbound_dial_gate(addr) {
        OutboundDialLease::Leader(gate) => gate,
        OutboundDialLease::Follower(_) => panic!("first dial should own the outbound gate"),
    };

    {
        let _completion = OutboundDialGateCompletion::new(&pool, addr, gate.clone());
    }

    tokio::time::timeout(Duration::from_millis(10), gate.wait())
        .await
        .expect("cancelled dial owner must wake outbound gate waiters");

    match pool.acquire_outbound_dial_gate(addr) {
        OutboundDialLease::Leader(_) => {}
        OutboundDialLease::Follower(_) => {
            panic!("cancelled dial owner must remove stale outbound gate")
        }
    }
}

#[tokio::test]
async fn test_set_registry() {
    use crate::{GossipConfig, KeyPair, registry::GossipRegistry};
    let pool = ConnectionPool::<()>::new(10, Duration::from_secs(5));
    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:8080".parse().unwrap(),
        GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("conn_pool_registry")),
            ..Default::default()
        },
    ));

    pool.set_registry(registry.clone());
    assert!(pool.registry.load().upgrade().is_some());
}

#[test]
fn test_connection_handle_send_data() {
    run_multi_thread_test(async {
        let (writer, recorded) = RecordingWriter::new();

        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);

        let handle = ConnectionHandle::<()>::new_stream(
            "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            CorrelationTracker::new(),
        );

        let data = vec![1, 2, 3, 4];
        handle.send_data(data.clone()).await.unwrap();

        // Allow the background writer to drain the queue
        sleep(Duration::from_millis(10)).await;

        let recorded = recorded.lock().unwrap().clone();
        assert_eq!(recorded, data);
    });
}

#[test]
fn test_writer_owner_batch_preserves_order() {
    run_multi_thread_test(async {
        let (writer, recorded) = RecordingWriter::new();

        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );

        let payloads = [
            bytes::Bytes::from_static(b"one"),
            bytes::Bytes::from_static(b"two"),
            bytes::Bytes::from_static(b"three"),
        ];

        for payload in &payloads {
            stream_handle
                .write_bytes_nonblocking(payload.clone())
                .expect("enqueue payload");
        }

        // Allow the background writer to drain the queue
        sleep(Duration::from_millis(10)).await;

        let recorded = recorded.lock().unwrap().clone();
        let expected = payloads.concat();
        assert_eq!(recorded, expected);
    });
}

#[test]
fn test_writer_vectored_sequence_header_payload() {
    run_multi_thread_test(async {
        let (writer, recorded) = RecordingWriter::new();

        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );

        let first = bytes::Bytes::from_static(b"first");
        let second = bytes::Bytes::from_static(b"second");
        let header = bytes::Bytes::from_static(b"HEAD");
        let payload = bytes::Bytes::from_static(b"PAYLOAD");

        stream_handle
            .write_bytes_nonblocking(first.clone())
            .expect("enqueue first");
        stream_handle
            .write_bytes_nonblocking(second.clone())
            .expect("enqueue second");
        stream_handle
            .write_header_and_payload_nonblocking(header.clone(), payload.clone())
            .expect("enqueue header+payload");

        // Allow the background writer to drain the queue
        sleep(Duration::from_millis(10)).await;

        let recorded = recorded.lock().unwrap().clone();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first);
        expected.extend_from_slice(&second);
        expected.extend_from_slice(&header);
        expected.extend_from_slice(&payload);
        assert_eq!(recorded, expected);
    });
}

#[test]
fn parse_direct_message_payload_success() {
    let mut frame = vec![crate::MessageType::DirectAsk as u8, 0x12, 0x34];
    frame.extend_from_slice(&(4u32).to_be_bytes()); /* ALLOW_COPY */
    frame.extend_from_slice(&[0u8; 5]); /* ALLOW_COPY */
    frame.extend_from_slice(b"PING"); /* ALLOW_COPY */

    let payload = super::parse_direct_message_payload(&frame).expect("parse ok");
    assert_eq!(payload, b"PING");
}

#[test]
fn parse_direct_message_payload_truncated() {
    let mut frame = vec![crate::MessageType::DirectAsk as u8, 0x12, 0x34];
    frame.extend_from_slice(&(4u32).to_be_bytes()); /* ALLOW_COPY */
    frame.extend_from_slice(&[0u8; 5]); /* ALLOW_COPY */
    frame.extend_from_slice(b"PI"); /* ALLOW_COPY */

    match super::parse_direct_message_payload(&frame) {
        Err(super::DirectPayloadError::PayloadTruncated {
            expected,
            available,
        }) => {
            assert_eq!(expected, 4);
            assert_eq!(available, 2);
        }
        other => panic!("unexpected parse result: {:?}", other),
    }
}

#[test]
fn parse_direct_message_payload_header_too_short() {
    let frame = vec![0u8; 3];
    assert_eq!(
        super::parse_direct_message_payload(&frame),
        Err(super::DirectPayloadError::HeaderTooShort)
    );
}

#[test]
fn test_connection_handle_send_data_closed() {
    run_multi_thread_test(async {
        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            ClosedWriter,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_handle = Arc::new(stream_handle);

        let handle = ConnectionHandle::<()>::new_stream(
            "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            CorrelationTracker::new(),
        );

        let result = handle.send_data(vec![1, 2, 3]).await;
        assert!(result.is_ok());
    });
}

#[tokio::test]
async fn test_task_tracker_aborts_on_drop() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let task_started = Arc::new(AtomicBool::new(false));
    let task_completed = Arc::new(AtomicBool::new(false));
    let started_clone = task_started.clone();
    let completed_clone = task_completed.clone();

    let handle = tokio::spawn(async move {
        started_clone.store(true, Ordering::SeqCst);
        // Long sleep that should be aborted
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        completed_clone.store(true, Ordering::SeqCst);
    });

    // Give task time to start
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        task_started.load(Ordering::SeqCst),
        "Task should have started"
    );

    // Create tracker and set the handle
    let tracker = TaskTracker::new();
    tracker.set_writer(handle.abort_handle());

    // Drop the tracker - this should abort the task
    drop(tracker);

    // Give task time to be aborted
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Task should NOT have completed (it was aborted)
    assert!(
        !task_completed.load(Ordering::SeqCst),
        "Task should have been aborted, not completed"
    );
}

#[tokio::test]
async fn test_task_tracker_replaces_old_handle() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let task2_started = Arc::new(AtomicBool::new(false));

    let handle1 = tokio::spawn(async move {
        // Long sleep that should be aborted when handle2 replaces it
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });

    let started_clone = task2_started.clone();
    let handle2 = tokio::spawn(async move {
        started_clone.store(true, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });

    let tracker = TaskTracker::new();

    // Set first handle
    tracker.set_writer(handle1.abort_handle());

    // Set second handle - first should be aborted
    tracker.set_writer(handle2.abort_handle());

    // Give task2 time to start
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        task2_started.load(Ordering::SeqCst),
        "Second task should have started"
    );

    // Clean up
    drop(tracker);
}

#[tokio::test]
async fn test_wait_for_response_returns_on_cancelled_slot() {
    let tracker = CorrelationTracker::new();
    // Existing tests pre-date the SlotGuard API. Disarming immediately keeps
    // the test's manual complete()/wait_for_response()/cancel() lifecycle
    // intact without leaking the slot.
    let correlation_id = tracker
        .allocate()
        .expect("ring should not be exhausted in test")
        .disarm();

    // Simulate a connection drop cancelling all pending requests.
    tracker.cancel_all();

    let res = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        tracker.wait_for_response(correlation_id, std::time::Duration::from_millis(50)),
    )
    .await;

    let err = res
        .expect("wait_for_response hung")
        .expect_err("expected error");
    assert!(matches!(err, GossipError::ConnectionDropped));
}

#[tokio::test]
async fn test_ask_backpressure_no_write_buffer_full() {
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);

    let (handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);

    let reader_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let mut tasks = Vec::new();
    for _ in 0..100 {
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                handle
                    .write_bytes_ask(bytes::Bytes::from_static(b"ping"))
                    .await?;
            }
            Ok::<(), crate::GossipError>(())
        }));
    }

    for task in tasks {
        task.await.unwrap().unwrap();
    }

    handle.shutdown();
    drop(handle);
    reader_task.abort();
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
struct WireMsg {
    value: u64,
}

crate::wire_type!(WireMsg, "connection_pool_tests::WireMsg");

#[tokio::test]
async fn test_pooled_typed_send_matches_wire_bytes() {
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);
    let (handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );

    let msg = WireMsg { value: 99 };
    let pooled = crate::typed::encode_typed_pooled(&msg).expect("encode_typed_pooled");
    let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<WireMsg>(pooled);
    let mut expected = Vec::with_capacity(payload_len);
    if let Some(prefix) = prefix.as_ref() {
        expected.extend_from_slice(prefix);
    }
    expected.extend_from_slice(payload.chunk());
    let mut header = [0u8; 16];
    header[..4].copy_from_slice(&(payload_len as u32).to_be_bytes());
    let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
    handle
        .write_pooled_control_inline(header, 4, prefix, prefix_len, payload)
        .await
        .unwrap();

    let mut len_buf = [0u8; 4];
    tokio::io::AsyncReadExt::read_exact(&mut reader, &mut len_buf)
        .await
        .unwrap();
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; payload_len];
    tokio::io::AsyncReadExt::read_exact(&mut reader, &mut payload)
        .await
        .unwrap();

    assert_eq!(payload.as_slice(), expected.as_slice());
    handle.shutdown();
}

#[test]
#[ignore = "benchmark-only; run explicitly when profiling"]
fn stream_direct_ask_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:41001".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:41002".parse().unwrap();
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_direct_ask_throughput_bench",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, mut server_io) = tokio::io::duplex(1024 * 1024);
        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let responder = tokio::spawn(async move {
            let mut len_buf = [0u8; crate::framing::LENGTH_PREFIX_LEN];
            loop {
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut len_buf)
                    .await
                    .is_err()
                {
                    break;
                }
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut msg = vec![0u8; msg_len];
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut msg)
                    .await
                    .is_err()
                {
                    break;
                }

                if msg_len >= crate::framing::DIRECT_ASK_HEADER_LEN
                    && msg[0] == crate::MessageType::DirectAsk as u8
                {
                    let correlation_id = u16::from_be_bytes([msg[1], msg[2]]);
                    let payload_len = u32::from_be_bytes([msg[3], msg[4], msg[5], msg[6]]) as usize;
                    let payload = &msg[crate::framing::DIRECT_ASK_HEADER_LEN
                        ..crate::framing::DIRECT_ASK_HEADER_LEN + payload_len];
                    let header =
                        crate::framing::write_direct_response_header(correlation_id, payload_len);
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, &header)
                        .await
                        .unwrap();
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, payload)
                        .await
                        .unwrap();
                } else if msg_len >= crate::framing::ACTOR_HEADER_LEN
                    && msg[0] == crate::MessageType::ActorAsk as u8
                {
                    let correlation_id = u16::from_be_bytes([msg[1], msg[2]]);
                    let payload_len =
                        u32::from_be_bytes([msg[24], msg[25], msg[26], msg[27]]) as usize;
                    let payload = &msg[crate::framing::ACTOR_HEADER_LEN
                        ..crate::framing::ACTOR_HEADER_LEN + payload_len];
                    let header = crate::framing::write_ask_response_header(
                        crate::MessageType::Response,
                        correlation_id,
                        payload_len,
                    );
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, &header)
                        .await
                        .unwrap();
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, payload)
                        .await
                        .unwrap();
                }
            }
        });

        let timeout = std::time::Duration::from_secs(2);
        let warmup = 5_000u64;
        let iters = 50_000u64;

        for _ in 0..warmup {
            let reply = conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_direct_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_direct_ask_no_timeout] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        for _ in 0..warmup {
            let reply = conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_actor_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = conn
                .ask_actor_frame_no_timeout(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_actor_ask_no_timeout] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let inflight = 64usize;
        let drive_direct = |count: u64| {
            let conn = conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let conn = conn.clone();
                    pending.push(Box::pin(async move {
                        conn.ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let conn = conn.clone();
                        pending.push(Box::pin(async move {
                            conn.ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let start = std::time::Instant::now();
        let checksum = drive_direct(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_direct_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );

        let drive_actor = |count: u64| {
            let conn = conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let conn = conn.clone();
                    pending.push(Box::pin(async move {
                        conn.ask_actor_frame_no_timeout(
                            0xC0DE_BEEF,
                            0xA11C_0001,
                            bytes::Bytes::from_static(b"pingpong"),
                        )
                        .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let conn = conn.clone();
                        pending.push(Box::pin(async move {
                            conn.ask_actor_frame_no_timeout(
                                0xC0DE_BEEF,
                                0xA11C_0001,
                                bytes::Bytes::from_static(b"pingpong"),
                            )
                            .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let start = std::time::Instant::now();
        let checksum = drive_actor(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_actor_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );

        client_writer.shutdown();
        responder.abort();
    });
}

#[test]
#[ignore = "benchmark-only; run explicitly when profiling"]
fn stream_tell_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:42001".parse().unwrap();

        let (client_io, mut server_io) = tokio::io::duplex(1024 * 1024);
        let (client_writer, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            None,
        );
        let client_writer = Arc::new(client_writer);
        let conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            CorrelationTracker::new(),
        );

        let delivered = Arc::new(AtomicU64::new(0));
        let delivered_task = Arc::clone(&delivered);
        let responder = tokio::spawn(async move {
            let mut len_buf = [0u8; crate::framing::LENGTH_PREFIX_LEN];
            loop {
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut len_buf)
                    .await
                    .is_err()
                {
                    break;
                }
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut msg = vec![0u8; msg_len];
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut msg)
                    .await
                    .is_err()
                {
                    break;
                }
                delivered_task.fetch_add(1, Ordering::Relaxed);
            }
        });

        let payload = bytes::Bytes::from(vec![0u8; 256]);
        let warmup = 10_000u64;
        let iters = 1_000_000u64;

        for _ in 0..warmup {
            conn.tell_actor_frame(0xC0DE_BEEF, 0xA11C_0001, payload.clone())
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < warmup {
            assert!(
                tokio::time::Instant::now() < deadline,
                "raw warmup tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        delivered.store(0, Ordering::Release);

        let start = std::time::Instant::now();
        for _ in 0..iters {
            conn.tell_actor_frame(0xC0DE_BEEF, 0xA11C_0001, payload.clone())
                .await
                .unwrap();
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "raw tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_tell_actor_frame_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_tell_actor_frame_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        delivered.store(0, Ordering::Release);
        let start = std::time::Instant::now();
        for _ in 0..iters {
            loop {
                match conn.try_tell_actor_frame(0xC0DE_BEEF, 0xA11C_0001, payload.clone()) {
                    Ok(()) => break,
                    Err(crate::GossipError::WriteQueueFull) => std::hint::spin_loop(),
                    Err(err) => panic!("try_tell stream bench failed: {err}"),
                }
            }
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "raw try_tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_try_tell_actor_frame_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_try_tell_actor_frame_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        client_writer.shutdown();
        responder.abort();
    });
}

#[test]
#[ignore = "benchmark-only; run explicitly when profiling"]
fn stream_protocol_ask_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:43001".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:43002".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_ask_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_ask_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let timeout = std::time::Duration::from_secs(2);
        let warmup = 5_000u64;
        let iters = 50_000u64;

        for _ in 0..warmup {
            let reply = client_conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = client_conn
                .ask_direct(bytes::Bytes::from_static(b"pingpong"), timeout)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_direct_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_direct_ask_timeout");

        for _ in 0..warmup {
            let reply = client_conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = client_conn
                .ask_actor_frame(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                    timeout,
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_actor_ask] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_actor_ask_timeout");

        for _ in 0..warmup {
            let reply = client_conn
                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = client_conn
                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_direct_ask_no_timeout_seq] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_direct_ask_no_timeout_seq");

        for _ in 0..warmup {
            let reply = client_conn
                .ask_actor_frame_no_timeout(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let reply = client_conn
                .ask_actor_frame_no_timeout(
                    0xC0DE_BEEF,
                    0xA11C_0001,
                    bytes::Bytes::from_static(b"pingpong"),
                )
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_actor_ask_no_timeout_seq] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_actor_ask_no_timeout_seq");

        let inflight = 64usize;
        let drive_direct = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        reset_io_perf();
        let start = std::time::Instant::now();
        let checksum = drive_direct(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_direct_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );
        print_io_perf("stream_protocol_direct_ask_no_timeout_inflight64");

        let drive_actor = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_actor_frame_no_timeout(
                                0xC0DE_BEEF,
                                0xA11C_0001,
                                bytes::Bytes::from_static(b"pingpong"),
                            )
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_actor_frame_no_timeout(
                                    0xC0DE_BEEF,
                                    0xA11C_0001,
                                    bytes::Bytes::from_static(b"pingpong"),
                                )
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        reset_io_perf();
        let start = std::time::Instant::now();
        let checksum = drive_actor(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_actor_ask_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );
        print_io_perf("stream_protocol_actor_ask_no_timeout_inflight64");

        client_writer.shutdown();
    });
}

#[test]
#[ignore = "benchmark-only; run explicitly when profiling"]
fn stream_protocol_direct_ask_inflight64_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:43101".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:43102".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_direct_ask_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_direct_ask_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let warmup = 5_000u64;
        let iters = 50_000u64;
        let inflight = 64usize;

        let drive_direct = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_direct_no_timeout(bytes::Bytes::from_static(b"pingpong"))
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let _ = drive_direct(warmup).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let start = std::time::Instant::now();
        let checksum = drive_direct(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_direct_ask_only_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );
        client_writer.shutdown();
    });
}

#[test]
#[ignore = "benchmark-only; run explicitly when profiling"]
fn stream_protocol_actor_ask_inflight64_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:43201".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:43202".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_actor_ask_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_actor_ask_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: Some(correlation.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let warmup = 5_000u64;
        let iters = 50_000u64;
        let inflight = 64usize;

        let drive_actor = |count: u64| {
            let client_conn = client_conn.clone();
            async move {
                let mut pending: futures::stream::FuturesUnordered<
                    futures::future::BoxFuture<'static, crate::Result<bytes::Bytes>>,
                > = futures::stream::FuturesUnordered::new();
                let mut next = 0u64;
                let mut checksum = 0u64;
                while next < count && pending.len() < inflight {
                    let client_conn = client_conn.clone();
                    pending.push(Box::pin(async move {
                        client_conn
                            .ask_actor_frame_no_timeout(
                                0xC0DE_BEEF,
                                0xA11C_0001,
                                bytes::Bytes::from_static(b"pingpong"),
                            )
                            .await
                    }));
                    next += 1;
                }
                while let Some(result) = pending.next().await {
                    let reply = result.unwrap();
                    checksum = checksum.wrapping_add(reply.len() as u64);
                    if next < count {
                        let client_conn = client_conn.clone();
                        pending.push(Box::pin(async move {
                            client_conn
                                .ask_actor_frame_no_timeout(
                                    0xC0DE_BEEF,
                                    0xA11C_0001,
                                    bytes::Bytes::from_static(b"pingpong"),
                                )
                                .await
                        }));
                        next += 1;
                    }
                }
                checksum
            }
        };

        let _ = drive_actor(warmup).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let start = std::time::Instant::now();
        let checksum = drive_actor(iters).await;
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_actor_ask_only_no_timeout_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3} checksum={}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64(),
            checksum
        );
        client_writer.shutdown();
    });
}

#[test]
#[ignore = "benchmark-only; run explicitly when profiling"]
fn stream_protocol_tell_throughput_bench() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:44001".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:44002".parse().unwrap();
        let delivered = Arc::new(AtomicU64::new(0));

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_tell_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActorCounter {
                delivered: Arc::clone(&delivered),
            }))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_protocol_tell_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (client_writer, _client_task, _client_reader_task) = LockFreeStreamHandle::new(
            client_io,
            server_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(client_read_ctx),
        );
        let client_writer = Arc::new(client_writer);
        let client_conn = ConnectionHandle::<()>::new_stream(
            server_addr,
            Arc::clone(&client_writer),
            CorrelationTracker::new(),
        );

        let server_read_ctx = ReadContext {
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: server_registry.actor_message_handler_sync.load_full(),
        };
        let (_server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            client_addr,
            ChannelId::TellAsk,
            BufferConfig {
                ask_window: 65_536,
                ..BufferConfig::default()
            },
            None,
            Some(server_read_ctx),
        );

        let payload = bytes::Bytes::from_static(b"pingpong");
        let warmup = 10_000u64;
        let iters = 1_000_000u64;

        for _ in 0..warmup {
            client_conn
                .tell_actor_frame(TEST_TELL_ACTOR_ID, TEST_TELL_HASH, payload.clone())
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < warmup {
            assert!(
                tokio::time::Instant::now() < deadline,
                "warmup tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        delivered.store(0, Ordering::Release);

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            client_conn
                .tell_actor_frame(TEST_TELL_ACTOR_ID, TEST_TELL_HASH, payload.clone())
                .await
                .unwrap();
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_tell_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_protocol_tell_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        println!(
            "[stream_protocol_tell_observed_delivery] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_tell");
        delivered.store(0, Ordering::Release);

        reset_io_perf();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            loop {
                match client_conn.try_tell_actor_frame(
                    TEST_TELL_ACTOR_ID,
                    TEST_TELL_HASH,
                    payload.clone(),
                ) {
                    Ok(()) => break,
                    Err(crate::GossipError::WriteQueueFull) => std::hint::spin_loop(),
                    Err(err) => panic!("protocol try_tell failed: {err}"),
                }
            }
        }
        let enqueue_elapsed = start.elapsed();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivered.load(Ordering::Acquire) < iters {
            assert!(
                tokio::time::Instant::now() < deadline,
                "try_tell delivery timeout"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let elapsed = start.elapsed();
        println!(
            "[stream_protocol_try_tell_enqueue] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            enqueue_elapsed.as_secs_f64(),
            iters as f64 / enqueue_elapsed.as_secs_f64()
        );
        println!(
            "[stream_protocol_try_tell_delivered] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
        print_io_perf("stream_protocol_try_tell");

        client_writer.shutdown();
    });
}

#[test]
#[ignore = "benchmark-only; run explicitly when profiling"]
fn correlation_tracker_throughput_bench() {
    run_multi_thread_test(async {
        let tracker = CorrelationTracker::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(256));
        let iters = 100_000u64;

        let start = std::time::Instant::now();
        for _ in 0..iters {
            // Existing tests pre-date the SlotGuard API. Disarming immediately keeps
            // the test's manual complete()/wait_for_response()/cancel() lifecycle
            // intact without leaking the slot.
            let correlation_id = tracker
                .allocate()
                .expect("ring should not be exhausted in test")
                .disarm();
            let mut payload = Some(crate::AlignedBytes::from_pooled_slice(
                b"pingpong",
                Arc::clone(&pool),
            ));
            assert!(tracker.complete(correlation_id, &mut payload));
            let reply = tracker
                .wait_for_response_no_timeout(correlation_id)
                .await
                .unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
        }
        let elapsed = start.elapsed();
        println!(
            "[correlation_seq] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );

        let inflight = 64usize;
        let start = std::time::Instant::now();
        let mut pending: futures::stream::FuturesUnordered<
            futures::future::BoxFuture<'static, crate::Result<crate::AlignedBytes>>,
        > = futures::stream::FuturesUnordered::new();
        let mut next = 0u64;
        while next < iters && pending.len() < inflight {
            // Existing tests pre-date the SlotGuard API. Disarming immediately keeps
            // the test's manual complete()/wait_for_response()/cancel() lifecycle
            // intact without leaking the slot.
            let correlation_id = tracker
                .allocate()
                .expect("ring should not be exhausted in test")
                .disarm();
            let tracker_clone = Arc::clone(&tracker);
            pending.push(Box::pin(async move {
                tracker_clone
                    .wait_for_response_no_timeout(correlation_id)
                    .await
            }));
            let mut payload = Some(crate::AlignedBytes::from_pooled_slice(
                b"pingpong",
                Arc::clone(&pool),
            ));
            assert!(tracker.complete(correlation_id, &mut payload));
            next += 1;
        }
        while let Some(result) = pending.next().await {
            let reply = result.unwrap();
            assert_eq!(reply.as_ref(), b"pingpong");
            if next < iters {
                // Existing tests pre-date the SlotGuard API. Disarming immediately keeps
                // the test's manual complete()/wait_for_response()/cancel() lifecycle
                // intact without leaking the slot.
                let correlation_id = tracker
                    .allocate()
                    .expect("ring should not be exhausted in test")
                    .disarm();
                let tracker_clone = Arc::clone(&tracker);
                pending.push(Box::pin(async move {
                    tracker_clone
                        .wait_for_response_no_timeout(correlation_id)
                        .await
                }));
                let mut payload = Some(crate::AlignedBytes::from_pooled_slice(
                    b"pingpong",
                    Arc::clone(&pool),
                ));
                assert!(tracker.complete(correlation_id, &mut payload));
                next += 1;
            }
        }
        let elapsed = start.elapsed();
        println!(
            "[correlation_inflight64] iters={} elapsed_s={:.6} ops_per_sec={:.3}",
            iters,
            elapsed.as_secs_f64(),
            iters as f64 / elapsed.as_secs_f64()
        );
    });
}

// ---------------------------------------------------------------------------
// Bug-hunt regression guards: CorrelationTracker livelock + slot leak.
//
// Production incident 2026-05-09: a node wedged at 100% CPU because the
// tokio current_thread runtime got monopolised by `CorrelationTracker::
// allocate()` spinning in a `loop {}` after every slot landed in
// SLOT_WAITING. Slots accumulated because in-flight `wait_for_response`
// futures were dropped by outer `tokio::time::timeout` cancellations
// without restoring slot state.
// ---------------------------------------------------------------------------

/// Tier 1: `allocate()` must terminate even when the entire ring is
/// already in a non-EMPTY state. Pre-fix this test panics with
/// "LIVELOCK"; post-fix it returns within milliseconds.
#[test]
fn allocate_terminates_when_every_slot_is_already_waiting() {
    let tracker = CorrelationTracker::new();

    // Force the ring to the exact state the bug produced in prod: every
    // slot in SLOT_WAITING with no consumer ever going to clear it.
    for slot in tracker.pending.iter() {
        slot.state.store(SLOT_WAITING, Ordering::Release);
    }

    // Probe on a separate OS thread. The current_thread tokio executor
    // is irrelevant here — the bug spins in pure user space, so a
    // same-runtime `tokio::time::timeout` would never fire. A blocking
    // mpsc channel with `recv_timeout` is the only reliable detector.
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let probe = Arc::clone(&tracker);
    std::thread::Builder::new()
        .name("correlation-livelock-probe".into())
        .spawn(move || {
            // `let _` is intentional: this works for both the pre-fix
            // signature (`-> u16`) and the post-fix signature
            // (`-> Result<SlotGuard<'_>, NoFreeSlots>`). We only care
            // that `allocate()` returns at all.
            let _ = probe.allocate();
            let _ = tx.send(());
        })
        .expect("spawn probe thread");

    if rx.recv_timeout(std::time::Duration::from_secs(3)).is_err() {
        panic!(
            "LIVELOCK: CorrelationTracker::allocate() did not return within 3s \
             after the ring was exhausted. The producer spins in user space \
             with no yield point; on a tokio current_thread runtime this \
             monopolises the executor and stalls every other task — exactly \
             the production wedge observed 2026-05-09 04:59:02Z."
        );
    }
}

/// Tier 2: a `wait_for_response` future that is cancelled mid-await
/// (e.g. by an outer `tokio::time::timeout` firing) MUST release its
/// correlation slot via the [`SlotGuard`] Drop. Otherwise slots leak,
/// the ring fills, and `allocate()` reaches the regression-guard test
/// above.
///
/// This is the ground-truth regression test for the production bug:
/// without the SlotGuard, this test would leak 256 slots and the
/// assertion below would fail with `0 != 256`.
#[test]
fn cancelled_wait_for_response_releases_slot_via_drop_guard() {
    run_multi_thread_test(async {
        let tracker = CorrelationTracker::new();
        let baseline_waiting = count_waiting_slots(&tracker);

        // 256 cancelled awaits — well below ring capacity (8192) so we
        // are isolating the leak signal rather than measuring exhaustion.
        for _ in 0..256 {
            // The async block needs an Arc<CorrelationTracker> it owns
            // (so the future can call wait_for_response across the await
            // boundary). The outer `tracker` Arc is also kept alive by the
            // surrounding scope, which keeps the SlotGuard borrow valid.
            let tracker_for_await = Arc::clone(&tracker);
            let slot = tracker.allocate().expect("ring should not be exhausted");
            let id = slot.id();
            let work = async move {
                // Hold the guard across an await that never resolves —
                // mimics `wait_for_response` blocked on a peer that
                // never responds.
                let _slot = slot;
                let _ = tracker_for_await.wait_for_response_no_timeout(id).await;
            };
            // Outer timeout fires before `work` can complete, dropping
            // the future and (post-fix) running the SlotGuard Drop.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(1), work).await;
        }

        let leaked = count_waiting_slots(&tracker) - baseline_waiting;
        assert_eq!(
            leaked, 0,
            "{leaked} slots leaked from cancelled wait_for_response futures. \
             SlotGuard Drop must run when the awaiter is cancelled — \
             otherwise leaked slots accumulate until the ring is exhausted, \
             at which point allocate() livelocks (see \
             allocate_terminates_when_every_slot_is_already_waiting)."
        );
    });
}

#[test]
fn cancelled_pending_ask_wait_releases_slot() {
    run_multi_thread_test(async {
        let tracker = CorrelationTracker::new();
        let baseline_waiting = count_waiting_slots(&tracker);

        for _ in 0..256 {
            let slot = tracker.allocate().expect("ring should not be exhausted");
            let pending = PendingAsk {
                correlation_id: slot.disarm(),
                correlation: Arc::clone(&tracker),
                timeout: std::time::Duration::from_secs(60),
                active: true,
            };

            let _ = tokio::time::timeout(std::time::Duration::from_millis(1), pending.wait()).await;
        }

        let leaked = count_waiting_slots(&tracker) - baseline_waiting;
        assert_eq!(
            leaked, 0,
            "{leaked} slots leaked from cancelled PendingAsk::wait futures. \
             PendingAsk Drop must remain armed while wait() is awaiting so \
             externally cancelled DeferredAsk waits cannot exhaust the ring."
        );
    });
}

fn count_waiting_slots(tracker: &CorrelationTracker) -> usize {
    tracker
        .pending
        .iter()
        .filter(|s| s.state.load(Ordering::Acquire) == SLOT_WAITING)
        .count()
}

/// Build a `PeerInfo` whose `last_response_received_ms` is set to `stale_time`
/// and that has the gossip protocol's response-asymmetry detector partway
/// through tripping (one accumulated failure).
fn stale_peer_info(addr: SocketAddr, stale_time: u64) -> crate::registry::PeerInfo {
    crate::registry::PeerInfo {
        address: addr,
        peer_address: None,
        inbound_observed: true,
        outbound_dial_success: true,
        node_id: None,
        dns_name: None,
        failures: 1,
        last_attempt: crate::current_timestamp(),
        last_success: stale_time,
        last_sequence: 0,
        last_sent_sequence: 0,
        consecutive_deltas: 0,
        last_failure_time: None,
        last_dns_refresh_attempt: None,
        last_response_received_ms: stale_time,
    }
}

#[tokio::test]
async fn full_sync_with_remote_loopback_bind_does_not_poison_peer_state() {
    let bind_addr: SocketAddr = "10.77.0.31:9501".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "remote-loopback-full-sync-local",
            )),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_keypair = crate::KeyPair::new_for_testing("remote-loopback-full-sync-remote");
    let peer_id = peer_keypair.peer_id();
    let tcp_source: SocketAddr = "10.77.0.31:38988".parse().unwrap();
    let loopback_bind = "127.0.0.1:26157";
    let synthesized_self_host_addr: SocketAddr = "10.77.0.31:26157".parse().unwrap();
    let actor_name = "poisoned/full-sync/actor";
    let poisoned_actor =
        crate::RemoteActorLocation::new_with_peer(synthesized_self_host_addr, peer_id.clone());

    let msg = crate::registry::RegistryMessage::FullSync {
        local_actors: vec![(actor_name.to_string(), poisoned_actor)],
        known_actors: Vec::new(),
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(loopback_bind.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(registry.clone(), tcp_source, msg)
        .await
        .expect("non-dialable FullSync should be ignored without crashing");

    let state = registry.gossip_state.lock().await;
    assert!(
        !state.peers.contains_key(&synthesized_self_host_addr),
        "remote loopback bind must not be synthesized into a same-host peer entry"
    );
    assert!(
        !state.peers.contains_key(&tcp_source),
        "remote loopback bind must not fall back to the ephemeral TCP source as a peer"
    );
    drop(state);

    assert!(
        registry
            .connection_pool
            .peer_id_to_addr
            .read_sync(&peer_id, |_, addr| *addr)
            .is_none(),
        "remote loopback bind must not install peer_id_to_addr mapping"
    );
    assert!(
        registry.lookup_actor(actor_name).await.is_none(),
        "actors from a non-dialable FullSync must not be merged into the registry"
    );
}

#[tokio::test]
async fn full_sync_response_with_remote_loopback_bind_does_not_reindex_connection() {
    let bind_addr: SocketAddr = "10.77.0.32:9501".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "remote-loopback-full-sync-response-local",
            )),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_keypair = crate::KeyPair::new_for_testing("remote-loopback-full-sync-response-remote");
    let peer_id = peer_keypair.peer_id();
    let tcp_source: SocketAddr = "10.77.0.32:47924".parse().unwrap();
    let loopback_bind = "127.0.0.1:3883";
    let synthesized_self_host_addr: SocketAddr = "10.77.0.32:3883".parse().unwrap();
    let actor_name = "poisoned/full-sync-response/actor";
    let poisoned_actor =
        crate::RemoteActorLocation::new_with_peer(synthesized_self_host_addr, peer_id.clone());

    let msg = crate::registry::RegistryMessage::FullSyncResponse {
        local_actors: vec![(actor_name.to_string(), poisoned_actor)],
        known_actors: Vec::new(),
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(loopback_bind.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(registry.clone(), tcp_source, msg)
        .await
        .expect("non-dialable FullSyncResponse should be ignored without crashing");

    let state = registry.gossip_state.lock().await;
    assert!(
        !state.peers.contains_key(&synthesized_self_host_addr),
        "remote loopback response bind must not be synthesized into a same-host peer entry"
    );
    drop(state);

    assert!(
        registry
            .connection_pool
            .peer_id_to_addr
            .read_sync(&peer_id, |_, addr| *addr)
            .is_none(),
        "remote loopback response bind must not reindex peer_id_to_addr"
    );
    assert!(
        registry.lookup_actor(actor_name).await.is_none(),
        "actors from a non-dialable FullSyncResponse must not be merged into the registry"
    );
}

// Regression test for the FullSyncResponse / DeltaGossip / FullSync inbound
// reset paths in `handle_incoming_message`. These paths previously reset
// `failures` and `last_success` when a peer sent us a message over the
// persistent bidirectional connection, but forgot to refresh
// `last_response_received_ms`. Because `apply_gossip_results` uses
// `last_response_received_ms` as its application-layer liveness signal (the
// response-asymmetry detector at registry.rs:3475), the omission caused a
// permanent log-spam loop on peers whose responses only ever arrived via
// the bidirectional path:
//
//   - outbound gossip round sees `Ok(None)` (no inline reply) and stale
//     `last_response_received_ms` → bumps `failures` from 0 to 1
//   - FullSyncResponse arrives moments later over the persistent stream →
//     resets `failures` back to 0
//   - `last_response_received_ms` never moves, so the next round repeats.
//
// These tests pin the post-fix invariant: any inbound payload from a peer
// must update `last_response_received_ms`, mirroring the inline response path
// in `GossipRegistry::handle_gossip_response`.
#[tokio::test]
async fn full_sync_response_updates_last_response_received_ms() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "lrr_full_sync_response_local",
            )),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_keypair = crate::KeyPair::new_for_testing("lrr_full_sync_response_remote");
    let peer_id = peer_keypair.peer_id();
    let peer_addr: SocketAddr = "10.77.0.63:9301".parse().unwrap();

    let stale_time = crate::current_timestamp_millis().saturating_sub(3_600_000);
    {
        let mut state = registry.gossip_state.lock().await;
        state
            .peers
            .insert(peer_addr, stale_peer_info(peer_addr, stale_time));
    }

    let test_start = crate::current_timestamp_millis();

    let msg = crate::registry::RegistryMessage::FullSyncResponse {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id: peer_id,
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(registry.clone(), peer_addr, msg)
        .await
        .expect("handle_incoming_message should succeed");

    let state = registry.gossip_state.lock().await;
    let info = state
        .peers
        .get(&peer_addr)
        .expect("peer should remain in gossip state after FullSyncResponse");
    assert_eq!(info.failures, 0, "failures should reset");
    assert!(
        info.last_response_received_ms >= test_start,
        "last_response_received_ms must be refreshed after FullSyncResponse \
         (got {}, test_start {}, stale_time {})",
        info.last_response_received_ms,
        test_start,
        stale_time,
    );
}

#[tokio::test]
async fn full_sync_updates_last_response_received_ms() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("lrr_full_sync_local")),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_keypair = crate::KeyPair::new_for_testing("lrr_full_sync_remote");
    let peer_id = peer_keypair.peer_id();
    let peer_addr: SocketAddr = "10.77.0.64:9301".parse().unwrap();

    let stale_time = crate::current_timestamp_millis().saturating_sub(3_600_000);
    {
        let mut state = registry.gossip_state.lock().await;
        state
            .peers
            .insert(peer_addr, stale_peer_info(peer_addr, stale_time));
    }

    let test_start = crate::current_timestamp_millis();

    let msg = crate::registry::RegistryMessage::FullSync {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id: peer_id,
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(registry.clone(), peer_addr, msg)
        .await
        .expect("handle_incoming_message should succeed");

    let state = registry.gossip_state.lock().await;
    let info = state
        .peers
        .get(&peer_addr)
        .expect("peer should remain in gossip state after FullSync");
    assert_eq!(info.failures, 0, "failures should reset");
    assert!(
        info.last_response_received_ms >= test_start,
        "last_response_received_ms must be refreshed after FullSync \
         (got {}, test_start {}, stale_time {})",
        info.last_response_received_ms,
        test_start,
        stale_time,
    );
}

#[tokio::test]
async fn delta_gossip_updates_last_response_received_ms() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("lrr_delta_gossip_local")),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_keypair = crate::KeyPair::new_for_testing("lrr_delta_gossip_remote");
    let peer_id = peer_keypair.peer_id();
    let peer_addr: SocketAddr = "10.77.0.65:9301".parse().unwrap();

    let stale_time = crate::current_timestamp_millis().saturating_sub(3_600_000);
    {
        let mut state = registry.gossip_state.lock().await;
        state
            .peers
            .insert(peer_addr, stale_peer_info(peer_addr, stale_time));
    }

    let test_start = crate::current_timestamp_millis();

    // Empty delta: no changes, just proves liveness.
    let delta = crate::registry::RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: Vec::new(),
        sender_peer_id: peer_id,
        wall_clock_time: crate::current_timestamp(),
        precise_timing_nanos: crate::current_timestamp_nanos(),
    };
    let msg = crate::registry::RegistryMessage::DeltaGossip {
        delta,
        extensions: None,
    };

    super::handle_incoming_message(registry.clone(), peer_addr, msg)
        .await
        .expect("handle_incoming_message should succeed");

    let state = registry.gossip_state.lock().await;
    let info = state
        .peers
        .get(&peer_addr)
        .expect("peer should remain in gossip state after DeltaGossip");
    assert_eq!(info.failures, 0, "failures should reset");
    assert!(
        info.last_response_received_ms >= test_start,
        "last_response_received_ms must be refreshed after DeltaGossip \
         (got {}, test_start {}, stale_time {})",
        info.last_response_received_ms,
        test_start,
        stale_time,
    );
}

/// DRY consolidation: the per-peer consecutive-timeout streak eviction
/// mechanism now lives in icanact-remote (the caller supplies only the
/// classification). Evict only once the streak threshold is reached; a success
/// resets it; a hard fault evicts immediately.
#[test]
fn streak_timeout_evicts_only_at_threshold_and_resets_on_success() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer = crate::KeyPair::new_for_testing("streak_threshold").peer_id();
    let addr: SocketAddr = "127.0.0.1:7310".parse().unwrap();

    let add = || {
        let conn = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
        conn.set_state(ConnectionState::Connected);
        assert!(pool.add_connection_by_peer_id(peer.clone(), addr, conn));
    };
    add();

    // Threshold 3: first two streak-timeouts must NOT evict.
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 3, None));
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 3, None));
    assert!(pool.get_lock_free_connection(addr).is_some());

    // A success resets the streak, so the next two again don't evict.
    pool.note_peer_ask_success(&peer);
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 3, None));
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 3, None));
    assert!(pool.get_lock_free_connection(addr).is_some());

    // Third consecutive timeout reaches the threshold and evicts.
    assert!(pool.note_peer_ask_streak_timeout(&peer, 3, None));
    assert!(pool.get_lock_free_connection(addr).is_none());
}

/// The per-peer streak is isolated: accruing timeouts for one peer never
/// evicts another, and evicting one leaves the other untouched.
#[test]
fn streak_is_isolated_per_peer() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_a = crate::KeyPair::new_for_testing("streak_iso_a").peer_id();
    let peer_b = crate::KeyPair::new_for_testing("streak_iso_b").peer_id();
    let addr_a: SocketAddr = "127.0.0.1:7320".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7321".parse().unwrap();
    for (peer, addr) in [(&peer_a, addr_a), (&peer_b, addr_b)] {
        let conn = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
        conn.set_state(ConnectionState::Connected);
        assert!(pool.add_connection_by_peer_id(peer.clone(), addr, conn));
    }

    // Accrue peer_a to just below the threshold; peer_b once. Neither evicts.
    assert!(!pool.note_peer_ask_streak_timeout(&peer_a, 4, None));
    assert!(!pool.note_peer_ask_streak_timeout(&peer_a, 4, None));
    assert!(!pool.note_peer_ask_streak_timeout(&peer_a, 4, None));
    assert!(!pool.note_peer_ask_streak_timeout(&peer_b, 4, None));
    assert!(pool.get_lock_free_connection(addr_a).is_some());
    assert!(pool.get_lock_free_connection(addr_b).is_some());

    // peer_a's 4th consecutive timeout evicts ONLY peer_a.
    assert!(pool.note_peer_ask_streak_timeout(&peer_a, 4, None));
    assert!(pool.get_lock_free_connection(addr_a).is_none());
    assert!(pool.get_lock_free_connection(addr_b).is_some());
}

/// A hard fault evicts and clears the pending streak, so timeouts on the
/// reconnected session restart from one rather than evicting immediately.
#[test]
fn hard_fault_clears_pending_streak() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer = crate::KeyPair::new_for_testing("hf_clears").peer_id();
    let addr: SocketAddr = "127.0.0.1:7322".parse().unwrap();
    let add = || {
        let conn = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
        conn.set_state(ConnectionState::Connected);
        assert!(pool.add_connection_by_peer_id(peer.clone(), addr, conn));
    };
    add();

    assert!(!pool.note_peer_ask_streak_timeout(&peer, 4, None)); // streak = 1
    assert!(pool.note_peer_ask_hard_fault(&peer, None)); // evict + clear
    assert!(pool.get_lock_free_connection(addr).is_none());

    add(); // reconnect
    // Must restart from 1, not carry the pre-fault count.
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 4, None));
    assert!(pool.get_lock_free_connection(addr).is_some());
}

/// Eviction requires `threshold` *consecutive* streak-timeouts: a success
/// anywhere in the run resets the counter.
#[test]
fn streak_only_evicts_on_consecutive_timeouts() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer = crate::KeyPair::new_for_testing("consec").peer_id();
    let addr: SocketAddr = "127.0.0.1:7323".parse().unwrap();
    let conn = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
    conn.set_state(ConnectionState::Connected);
    assert!(pool.add_connection_by_peer_id(peer.clone(), addr, conn));

    assert!(!pool.note_peer_ask_streak_timeout(&peer, 4, None)); // 1
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 4, None)); // 2
    pool.note_peer_ask_success(&peer); // reset
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 4, None)); // 1
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 4, None)); // 2
    assert!(!pool.note_peer_ask_streak_timeout(&peer, 4, None)); // 3
    assert!(
        pool.get_lock_free_connection(addr).is_some(),
        "max consecutive run was 3 (< threshold 4): must not evict"
    );
    assert!(pool.note_peer_ask_streak_timeout(&peer, 4, None)); // 4 consecutive -> evict
    assert!(pool.get_lock_free_connection(addr).is_none());
}

/// A hard transport fault evicts immediately, bypassing the streak threshold.
#[test]
fn hard_fault_evicts_immediately() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer = crate::KeyPair::new_for_testing("streak_hardfault").peer_id();
    let addr: SocketAddr = "127.0.0.1:7311".parse().unwrap();
    let conn = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
    conn.set_state(ConnectionState::Connected);
    assert!(pool.add_connection_by_peer_id(peer.clone(), addr, conn));

    assert!(
        pool.note_peer_ask_hard_fault(&peer, None),
        "a hard fault must evict on the first occurrence"
    );
    assert!(pool.get_lock_free_connection(addr).is_none());
}

/// `threshold == 0` disables the streak mechanism entirely.
#[test]
fn streak_threshold_zero_never_evicts() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer = crate::KeyPair::new_for_testing("streak_zero").peer_id();
    let addr: SocketAddr = "127.0.0.1:7312".parse().unwrap();
    let conn = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Outbound));
    conn.set_state(ConnectionState::Connected);
    assert!(pool.add_connection_by_peer_id(peer.clone(), addr, conn));

    for _ in 0..10 {
        assert!(!pool.note_peer_ask_streak_timeout(&peer, 0, None));
    }
    assert!(pool.get_lock_free_connection(addr).is_some());
}

/// C3 instance guard: a streak-timeout whose pinned instance no longer matches
/// the live session (it was reconnected mid-ask) must NOT evict the fresh,
/// healthy session.
#[test]
fn streak_timeout_with_stale_instance_does_not_evict_live_session() {
    run_multi_thread_test(async {
        let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
        let peer = crate::KeyPair::new_for_testing("streak_instance_guard").peer_id();
        let addr: SocketAddr = "127.0.0.1:7313".parse().unwrap();

        // Associate the address with the peer so finalize publishes it under
        // the peer id, giving us a real stream instance to pin.
        pool.add_addr_to_peer_id(addr, peer.clone());
        let (io, _keep) = tokio::io::duplex(1024);
        pool.finalize_new_outbound_connection(addr, io, std::sync::Weak::new(), None)
            .await
            .expect("finalize outbound");

        let live_instance = pool
            .current_peer_connection_instance(&peer)
            .expect("live session should have a stream instance");

        // Threshold 1, but a STALE instance: the guard must block eviction.
        let stale = live_instance.wrapping_add(1);
        assert!(
            !pool.note_peer_ask_streak_timeout(&peer, 1, Some(stale)),
            "a streak-timeout pinned to a stale instance must not evict"
        );
        assert!(
            pool.current_peer_connection_instance(&peer).is_some(),
            "the live, reconnected session must survive a stale-instance timeout"
        );

        // The correct instance does evict.
        assert!(
            pool.note_peer_ask_streak_timeout(&peer, 1, Some(live_instance)),
            "a streak-timeout pinned to the live instance must evict at threshold"
        );
        assert!(pool.current_peer_connection_instance(&peer).is_none());
    });
}

/// Sweep finding: `evict_peer_session_if_instance` (backing
/// `note_peer_ask_hard_fault`/`note_peer_ask_streak_timeout`, both reachable
/// public API on `GossipRegistryHandle` for application-classified ask
/// outcomes) matched `expected_instance` against the current session's
/// instance id, then acted via the PEER-WIDE `disconnect_connection_by_peer_id`
/// — the same match-then-peer-wide-disconnect gap as the primary
/// `handle_peer_connection_failure` finding. A fresh session published for
/// the same peer between the instance-id match and the peer-wide teardown
/// must survive; only the matched (now-retired) instance may be torn down.
///
/// Pinned deterministically via the same `set_transport_lifecycle_recorder`
/// technique as `stale_instance_cleanup_uses_atomic_cas_and_preserves_fresh_current_session`:
/// the buggy peer-wide path fires `SessionRemoved { reason: DisconnectByPeerId }`
/// BEFORE its unconditional `clear_current_peer_connection` store; a fixed,
/// CAS-scoped path fires the same event only AFTER the atomic clear already
/// succeeded, so an identical publish lands safely.
#[test]
fn hard_fault_matched_instance_eviction_is_instance_scoped_not_peer_wide() {
    run_multi_thread_test(async {
        let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
        let peer = crate::KeyPair::new_for_testing("hard_fault_instance_scoped").peer_id();
        let addr: SocketAddr = "127.0.0.1:7314".parse().unwrap();

        pool.add_addr_to_peer_id(addr, peer.clone());
        let (io, _keep) = tokio::io::duplex(1024);
        pool.finalize_new_outbound_connection(addr, io, std::sync::Weak::new(), None)
            .await
            .expect("finalize outbound");

        let live_instance = pool
            .current_peer_connection_instance(&peer)
            .expect("live session should have a stream instance");

        let fresh_addr: SocketAddr = "127.0.0.1:7315".parse().unwrap();
        let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;

        let _guard = RecorderGuard::acquire();
        {
            let pool = pool.clone();
            let peer = peer.clone();
            let fresh = fresh.clone();
            crate::set_transport_lifecycle_recorder(Some(Arc::new(move |event| {
                if let crate::TransportLifecycleEvent::SessionRemoved {
                    peer: event_peer,
                    reason: crate::SessionRemovalReason::DisconnectByPeerId,
                    ..
                } = &event
                    && *event_peer == peer
                {
                    crate::set_transport_lifecycle_recorder(None);
                    pool.publish_current_peer_connection(&peer, fresh.clone());
                }
            })));
        }

        // A hard transport fault pinned to the (currently live) instance —
        // models the caller's ask having actually failed on `live_instance`.
        // Per the hook installed above, a FRESH session for the same peer is
        // published from inside the match-then-disconnect gap.
        pool.note_peer_ask_hard_fault(&peer, Some(live_instance));

        let current = pool.get_connection_by_peer_id(&peer);
        assert!(
            current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
            "a fresh session published from inside the matched-instance hard-fault \
             eviction's check-then-act gap must survive — matching an instance must never \
             fall through to a peer-wide disconnect that clobbers it (got {current:?})"
        );
    });
}

/// Audit finding B2: the pool's LRU "make room" eviction picked the absolute
/// least-recently-used connection with no regard for whether it was a
/// configured/required cluster peer — so a new (often transient or discovered)
/// dial could tear down a live cluster member to fit under the pool cap. The
/// victim selector must skip configured peers.
#[test]
fn lru_eviction_spares_configured_peers() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));

    // A configured (required) peer connection that is the LEAST recently used.
    let cfg_peer = crate::KeyPair::new_for_testing("lru_cfg").peer_id();
    let cfg_addr: SocketAddr = "127.0.0.1:7200".parse().unwrap();
    pool.set_configured_peer_addr(&cfg_peer, cfg_addr);
    let cfg_conn = Arc::new(LockFreeConnection::new(
        cfg_addr,
        ConnectionDirection::Inbound,
    ));
    cfg_conn.set_state(ConnectionState::Connected);
    cfg_conn.last_used.store(1, Ordering::Relaxed);
    assert!(pool.add_connection_by_peer_id(cfg_peer.clone(), cfg_addr, cfg_conn));

    // An anonymous / discovered connection that is MORE recently used.
    let other_addr: SocketAddr = "127.0.0.1:7201".parse().unwrap();
    let other_conn = Arc::new(LockFreeConnection::new(
        other_addr,
        ConnectionDirection::Inbound,
    ));
    other_conn.set_state(ConnectionState::Connected);
    other_conn.last_used.store(100, Ordering::Relaxed);
    pool.index_connection_by_addr(other_addr, other_conn);

    let victim = pool.select_lru_eviction_victim();
    assert_eq!(
        victim,
        Some(other_addr),
        "LRU must evict the anonymous connection, never the configured (required) peer"
    );
}

/// Learned/discovered routes are useful for peer-id lookup and direct dials,
/// but they are not operator-configured required peers. The supervisor must
/// only monitor explicit `configure_peer` entries.
#[test]
fn learned_peer_route_is_not_a_required_configured_peer() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let learned_peer = crate::KeyPair::new_for_testing("learned_route").peer_id();
    let learned_addr: SocketAddr = "127.0.0.1:7210".parse().unwrap();
    let learned_conn = Arc::new(LockFreeConnection::new(
        learned_addr,
        ConnectionDirection::Inbound,
    ));
    learned_conn.set_state(ConnectionState::Connected);

    assert!(pool.add_connection_by_peer_id(learned_peer.clone(), learned_addr, learned_conn));
    assert_eq!(
        pool.get_configured_peer_addr(&learned_peer),
        Some(learned_addr),
        "learned route must remain available for peer-id lookup"
    );
    assert!(
        pool.list_configured_peers().is_empty(),
        "learned routes must not enter the required-peer supervisor set"
    );

    let required_peer = crate::KeyPair::new_for_testing("required_route").peer_id();
    let required_addr: SocketAddr = "127.0.0.1:7211".parse().unwrap();
    pool.set_configured_peer_addr(&required_peer, required_addr);

    assert_eq!(
        pool.list_configured_peers(),
        vec![(required_peer, required_addr)],
        "only explicitly configured peers are supervised"
    );
}

/// Audit finding B1: the outbound finalize path inserts a connection into the
/// pool but historically never incremented `connection_counter`, while every
/// teardown path decremented it. Each outbound connect→disconnect therefore
/// drove the counter toward underflow (wrapping to `usize::MAX`), which
/// permanently breaks the inbound admission gate keyed on this counter.
#[test]
fn outbound_finalize_balances_connection_counter() {
    run_multi_thread_test(async {
        let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
        let addr: SocketAddr = "127.0.0.1:7100".parse().unwrap();
        let (io, _peer) = tokio::io::duplex(1024);

        let _handle = pool
            .finalize_new_outbound_connection(addr, io, std::sync::Weak::new(), None)
            .await
            .expect("outbound finalize should succeed");

        assert_eq!(
            pool.connection_counter.load(Ordering::SeqCst),
            1,
            "a finalized outbound connection must be counted exactly once"
        );

        let removed = pool.remove_connection(addr);
        assert!(
            removed.is_some(),
            "the finalized connection should be removable by address"
        );

        assert_eq!(
            pool.connection_counter.load(Ordering::SeqCst),
            0,
            "removing the outbound connection must return the counter to zero, never underflow"
        );
    });
}

/// RED (review finding P2, outbound-finalize reject leaves the rejected
/// candidate indexed and served): the freshly-dialed outbound candidate is
/// inserted into `connections_by_addr` / `addr_to_peer_id` BEFORE the
/// tie-break decision is known (so `existing_before` can be snapshotted
/// without racing the insert). When `resolve_connection_conflict` returns
/// `RejectIncoming` — the existing session is live and tie-break-preferred,
/// the new outbound is not — the old code only logged and fell through:
/// `connection_counter` was still bumped, an initial FullSync was still sent
/// on the rejected candidate's stream, and `Ok(ConnectionHandle)` for the
/// REJECTED candidate was still returned to the caller, while the candidate
/// remained the live entry in `connections_by_addr` at its dial address —
/// silently overwriting address-based lookups for the preferred session with
/// a connection that was never installed as anyone's current session and
/// whose background tasks the caller has no way to ever tear down.
#[tokio::test]
async fn outbound_finalize_reject_fully_unpublishes_and_does_not_bump_counter() {
    use crate::{GossipConfig, registry::GossipRegistry};

    // Local is the HIGHER NodeId, so the existing INBOUND session for this
    // peer IS tie-break preferred (`should_keep_connection(remote, is_outbound=false) == true`),
    // while a fresh OUTBOUND dial to the same peer is NOT
    // (`should_keep_connection(remote, is_outbound=true) == false`) — the
    // textbook `RejectIncoming` case (`resolve_connection_conflict(true, true, false)`).
    let (hi_kp, lo_kp) = hi_lo_keypairs("finalize-reject-hi", "finalize-reject-lo");
    let remote_peer_id = lo_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));

    // The existing, live, preferred INBOUND session, published as this
    // peer's CURRENT connection.
    let existing_addr: SocketAddr = "127.0.0.1:7440".parse().unwrap();
    let (existing_io, _existing_keep) = tokio::io::duplex(1024);
    let (existing_sh, _existing_w, _existing_r) = LockFreeStreamHandle::new(
        existing_io,
        existing_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut existing_conn = LockFreeConnection::new(existing_addr, ConnectionDirection::Inbound);
    existing_conn.stream_handle = Some(Arc::new(existing_sh));
    existing_conn.set_state(ConnectionState::Connected);
    let existing = Arc::new(existing_conn);
    assert!(pool.add_connection_by_peer_id(
        remote_peer_id.clone(),
        existing_addr,
        existing.clone()
    ));

    let counter_before = pool.connection_counter.load(Ordering::SeqCst);

    // A fresh, non-preferred OUTBOUND dial to a DIFFERENT address for the
    // same peer identity (resolved via the configured-address fallback).
    let dial_addr: SocketAddr = "127.0.0.1:7441".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None)
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a rejected outbound candidate must be surfaced as an error, never handed back \
         to the caller as a live handle: got {result:?}"
    );

    assert!(
        pool.get_lock_free_connection(dial_addr).is_none(),
        "the rejected candidate must not remain indexed in connections_by_addr"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&dial_addr, |_, v| v.clone())
            .is_none(),
        "the rejected candidate's dial address must not remain mapped to the peer id"
    );

    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &existing)),
        "the preferred existing inbound session must remain the peer's current connection"
    );

    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        counter_before,
        "a rejected outbound candidate must never bump the live connection counter"
    );
}

/// RED (review finding P2, outbound-finalize reject leaves the live
/// session's address index erased): the freshly-dialed outbound candidate is
/// provisionally `connections_by_addr.upsert(addr, candidate)`-ed BEFORE the
/// tie-break decision is known. When the candidate dials the SAME address a
/// live, tie-break-preferred INBOUND session already owns (rather than a
/// different address, as in
/// `outbound_finalize_reject_fully_unpublishes_and_does_not_bump_counter`),
/// that provisional upsert overwrites the live inbound's own
/// `connections_by_addr` / `addr_to_peer_id` entry at that address. The old
/// `unpublish_rejected_outbound_candidate` only removed the candidate's
/// entry on reject — it never restored what the provisional upsert had
/// displaced — so after the reject, `connections_by_addr[addr]` /
/// `addr_to_peer_id[addr]` were left EMPTY even though the peer session
/// still points at the live inbound. Address-keyed lookups and
/// failure-canonicalization would then miss the live session entirely and
/// could redial a duplicate connection to a peer already fully connected.
#[tokio::test]
async fn outbound_finalize_reject_restores_displaced_live_session_address_index() {
    use crate::{GossipConfig, registry::GossipRegistry};

    // Local is the HIGHER NodeId, so the existing INBOUND session for this
    // peer IS tie-break preferred, while a fresh OUTBOUND dial is NOT — the
    // textbook `RejectIncoming` case, same as
    // `outbound_finalize_reject_fully_unpublishes_and_does_not_bump_counter`,
    // except the candidate here dials the EXACT SAME address the live
    // inbound already owns.
    let (hi_kp, lo_kp) = hi_lo_keypairs("finalize-reject-restore-hi", "finalize-reject-restore-lo");
    let remote_peer_id = lo_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));

    // The existing, live, preferred INBOUND session, published as this
    // peer's CURRENT connection AND indexed by address at `shared_addr`.
    let shared_addr: SocketAddr = "127.0.0.1:7460".parse().unwrap();
    let (existing_io, _existing_keep) = tokio::io::duplex(1024);
    let (existing_sh, _existing_w, _existing_r) = LockFreeStreamHandle::new(
        existing_io,
        shared_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut existing_conn = LockFreeConnection::new(shared_addr, ConnectionDirection::Inbound);
    existing_conn.stream_handle = Some(Arc::new(existing_sh));
    existing_conn.set_state(ConnectionState::Connected);
    let existing = Arc::new(existing_conn);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), shared_addr, existing.clone()));

    let counter_before = pool.connection_counter.load(Ordering::SeqCst);

    // A fresh, non-preferred OUTBOUND dial finalizes at the EXACT SAME
    // address the live inbound already owns.
    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(shared_addr, io, registry_weak, None)
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a rejected outbound candidate must be surfaced as an error: got {result:?}"
    );

    // The live inbound's address index must survive the reject: it was
    // displaced by the candidate's provisional upsert, and must be restored,
    // never left empty.
    let indexed_at_addr = pool.get_lock_free_connection(shared_addr);
    assert!(
        indexed_at_addr
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &existing)),
        "connections_by_addr at the shared address must resolve to the LIVE inbound \
         session after the reject, not be empty and not the rejected candidate \
         (got: {indexed_at_addr:?})"
    );
    assert_eq!(
        pool.addr_to_peer_id
            .read_sync(&shared_addr, |_, v| v.clone()),
        Some(remote_peer_id.clone()),
        "addr_to_peer_id at the shared address must still map to the live inbound's \
         peer id after the reject, never left empty"
    );

    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &existing)),
        "the preferred existing inbound session must remain the peer's current connection"
    );
    assert!(
        existing.has_live_stream(),
        "the live inbound's background tasks must not be touched by the reject"
    );

    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        counter_before,
        "a rejected outbound candidate must never bump the live connection counter"
    );
}

/// Deterministic ordering helper mirroring `tiebreak_reconnect_thrash.rs`'s
/// `ordered()`: returns `(higher_node_id_keypair, lower_node_id_keypair)`
/// regardless of which seed happens to hash higher, so tests do not depend on
/// which literal seed string wins the NodeId comparison.
fn hi_lo_keypairs(a: &str, b: &str) -> (crate::KeyPair, crate::KeyPair) {
    let x = crate::KeyPair::new_for_testing(a);
    let y = crate::KeyPair::new_for_testing(b);
    if x.peer_id().to_node_id().as_bytes() > y.peer_id().to_node_id().as_bytes() {
        (x, y)
    } else {
        (y, x)
    }
}

/// RED (review finding P1, outbound-finalize `AcceptIncoming` publish gap
/// AND its CAS-lost re-resolve reject arm): when `existing_before` is `None`
/// at snapshot time, the outbound-finalize decision is unconditionally
/// `AcceptIncoming`. That decision is enacted via
/// `publish_outbound_or_reresolve`'s compare-and-publish against the
/// `existing_before` snapshot — never an unconditional publish — so a
/// PREFERRED rival published for the same peer in the gap between that
/// snapshot and this call is never silently overwritten (the original P1
/// finding this test's name references). This test's remote peer / local
/// identity ordering additionally makes the re-resolved, address-blind
/// tie-break come back `RejectIncoming` against that concurrently published
/// rival (the rival is INBOUND and preferred; this candidate is OUTBOUND and
/// not) — the SECOND half of the P1 finding: before the fix,
/// `publish_outbound_or_reresolve`'s `RejectIncoming`/
/// `EvictStaleRejectIncoming` re-resolve arms only `debug!`-logged and
/// returned `()`, so `finalize_new_outbound_connection` fell straight
/// through into the unconditional counter bump / FullSync send / `Ok`
/// return for the LOSING candidate — which was still sitting, indexed, in
/// `connections_by_addr` at its own dial address. The fix makes
/// `publish_outbound_or_reresolve` return `false` on those arms and routes
/// that back through the IDENTICAL eager-reject cleanup
/// (`unpublish_rejected_outbound_candidate` + `Err(ConnectionExists)`) the
/// sibling `outbound_finalize_reject_*` tests already pin for the
/// pre-computed-decision reject path.
///
/// Pinned deterministically via `set_transport_lifecycle_recorder` on the new
/// `OutboundFinalizePublishAttempt` instrumentation event, which fires
/// unconditionally immediately before the outbound's own publish attempt
/// (success or failure) — the same technique
/// `hard_fault_matched_instance_eviction_is_instance_scoped_not_peer_wide`
/// and `stale_instance_cleanup_uses_atomic_cas_and_preserves_fresh_current_session`
/// use to pin a concurrent publish into a specific check-then-act gap.
///
/// RED at HEAD (before this fix): `result.is_ok()` (the loser is handed back
/// as a live `Ok(ConnectionHandle)`). GREEN after the fix:
/// `result == Err(GossipError::ConnectionExists)`.
#[tokio::test]
async fn outbound_finalize_accept_incoming_compare_and_publishes_against_snapshot() {
    use crate::{GossipConfig, registry::GossipRegistry};

    // Local uses the HIGHER NodeId keypair, so for this remote peer identity
    // the tie-break prefers INBOUND (`should_keep_connection(remote, is_outbound=false) == true`,
    // `should_keep_connection(remote, is_outbound=true) == false`) — the
    // freshly-dialed OUTBOUND candidate below is never the preferred
    // direction, so if it wins the peer session slot at all, it can only be
    // through the unconditional-publish bug, never through a legitimate
    // re-resolved tie-break.
    let (hi_kp, lo_kp) = hi_lo_keypairs("accept-incoming-cas-gap-hi", "accept-incoming-cas-gap-lo");
    let remote_peer_id = lo_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));

    // No prior session for this peer at all: `existing_before` snapshots to
    // `None`, and the decision is `AcceptIncoming` unconditionally.
    assert!(pool.get_connection_by_peer_id(&remote_peer_id).is_none());

    let dial_addr: SocketAddr = "127.0.0.1:7480".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    // The PREFERRED inbound that will be published concurrently, in the gap
    // between the `existing_before` snapshot and the outbound's own publish
    // attempt.
    let inbound_addr: SocketAddr = "127.0.0.1:7481".parse().unwrap();
    let inbound = make_live_connection(inbound_addr, ConnectionDirection::Inbound).await;

    let _guard = RecorderGuard::acquire();
    {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let inbound = inbound.clone();
        crate::set_transport_lifecycle_recorder(Some(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                peer: event_peer,
                ..
            } = &event
                && *event_peer == peer_id
            {
                // Deregister first: the nested `publish_current_peer_connection`
                // call below fires its own (non-matching) `SessionPublished`
                // event through this same global hook, and this avoids any
                // reentrant/recursive invocation of this closure.
                crate::set_transport_lifecycle_recorder(None);
                pool.publish_current_peer_connection(&peer_id, inbound.clone());
            }
        })));
    }

    let counter_before = pool.connection_counter.load(Ordering::SeqCst);

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None)
        .await;

    // The candidate lost the compare-and-publish re-resolve to a
    // tie-break-preferred rival (`RejectIncoming`) — the SAME reject outcome
    // the eager `outbound_finalize_reject_*` tests pin, so it gets identical
    // treatment: surfaced as an error, never handed back as a live handle.
    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a candidate that loses its compare-and-publish re-resolution to a tie-break-preferred \
         rival must be surfaced as an error, never handed back to the caller as a live handle: \
         got {result:?}"
    );

    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &inbound)),
        "the PREFERRED inbound published concurrently in the compare-and-publish gap must \
         remain the peer's current session — the fallback outbound's own publish, computed \
         from a stale `existing_before == None` snapshot, must never overwrite it \
         (got {current:?})"
    );
    assert!(
        inbound.has_live_stream(),
        "the preferred inbound's background tasks must survive untouched"
    );

    assert!(
        pool.get_lock_free_connection(dial_addr).is_none(),
        "the rejected candidate must not remain indexed in connections_by_addr at its own \
         dial address, where it could shadow the preferred rival in address lookups"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&dial_addr, |_, v| v.clone())
            .is_none(),
        "the rejected candidate's dial address must not remain mapped to the peer id"
    );
    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        counter_before,
        "a rejected outbound candidate re-resolved against a concurrent CAS loss must never \
         bump the live connection counter"
    );
}

/// RED (review finding P1, outbound-finalize `EvictStaleRejectIncoming`
/// CAS-lost re-resolve arm): same defect as
/// `outbound_finalize_accept_incoming_compare_and_publishes_against_snapshot`,
/// but exercising the OTHER reject arm `publish_outbound_or_reresolve` can
/// re-resolve to: the concurrently published rival is itself already
/// stale/dead (`existing_usable == false`) AND this candidate is not the
/// tie-break-preferred direction either (`keep_incoming == false`), so
/// `resolve_connection_conflict` returns `EvictStaleRejectIncoming` — evict
/// the stale rival, but still decline to publish this candidate as the
/// session. Before the fix this arm evicted the stale rival correctly but
/// then, identically to the `RejectIncoming` arm, only `debug!`-logged and
/// fell through to the unconditional counter bump / FullSync send / `Ok`
/// for the losing candidate.
///
/// RED at HEAD (before this fix): `result.is_ok()` and the candidate stays
/// indexed at its own dial address. GREEN after the fix:
/// `result == Err(GossipError::ConnectionExists)`, candidate fully
/// unpublished, counter unaffected.
#[tokio::test]
async fn outbound_finalize_evict_stale_reject_incoming_cas_lost_fully_unpublishes() {
    use crate::{GossipConfig, registry::GossipRegistry};

    // Same ordering as the `RejectIncoming` sibling above: local is the
    // HIGHER NodeId, so `should_keep_connection(remote, is_outbound=true) ==
    // false` — this candidate (a fresh OUTBOUND dial) is never the
    // tie-break-preferred direction for `remote_peer_id`, regardless of
    // whether the rival it loses to is itself alive or stale.
    let (hi_kp, lo_kp) = hi_lo_keypairs(
        "evict-stale-reject-cas-gap-hi",
        "evict-stale-reject-cas-gap-lo",
    );
    let remote_peer_id = lo_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));

    assert!(pool.get_connection_by_peer_id(&remote_peer_id).is_none());

    let dial_addr: SocketAddr = "127.0.0.1:7482".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    // The rival published concurrently in the CAS window is already
    // stale/dead (`has_live_stream() == false`) — `existing_usable == false`
    // — so the re-resolved decision is `EvictStaleRejectIncoming`, not
    // `RejectIncoming`.
    let rival_addr: SocketAddr = "127.0.0.1:7483".parse().unwrap();
    let rival = make_live_connection(rival_addr, ConnectionDirection::Inbound).await;
    if let Some(sh) = rival.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }
    assert!(
        !rival.has_live_stream(),
        "test precondition: rival must be stale/dead"
    );

    let _guard = RecorderGuard::acquire();
    {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let rival = rival.clone();
        crate::set_transport_lifecycle_recorder(Some(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                peer: event_peer,
                ..
            } = &event
                && *event_peer == peer_id
            {
                crate::set_transport_lifecycle_recorder(None);
                pool.publish_current_peer_connection(&peer_id, rival.clone());
            }
        })));
    }

    let counter_before = pool.connection_counter.load(Ordering::SeqCst);

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None)
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a candidate whose compare-and-publish re-resolution evicts a stale rival but is \
         itself not tie-break-preferred either must be surfaced as an error, never handed \
         back as a live handle: got {result:?}"
    );

    assert!(
        pool.get_lock_free_connection(dial_addr).is_none(),
        "the rejected candidate must not remain indexed in connections_by_addr"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&dial_addr, |_, v| v.clone())
            .is_none(),
        "the rejected candidate's dial address must not remain mapped to the peer id"
    );
    assert!(
        pool.get_connection_by_peer_id(&remote_peer_id).is_none(),
        "neither the rejected candidate nor the evicted stale rival may end up as the peer's \
         current session"
    );
    assert!(
        rival
            .stream_handle
            .as_ref()
            .is_some_and(|h| h.exit_flag.load(Ordering::Acquire)),
        "the stale rival must actually have been evicted (tasks aborted / left aborted)"
    );

    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        counter_before,
        "a rejected outbound candidate must never bump the live connection counter, even when \
         its re-resolve also evicted a stale rival"
    );
}

/// RED (review finding P1, outbound-finalize impure rival lookup):
/// `finalize_new_outbound_connection` used to call
/// `get_connection_by_peer_id` for its tie-break rival lookup *after* the
/// freshly-dialed candidate was already indexed into `connections_by_addr`
/// under the peer's configured dial address. That lookup is not a pure
/// function — its configured-address fallback reads straight out of that
/// map — so when a real, live, tie-break-losable rival (wrong direction, not
/// yet promoted to "current") sits at exactly that same address (the
/// documented "left indexed by address only, not published as the session"
/// state this very file's `RejectIncoming`/`EvictStaleRejectIncoming` arms
/// intentionally leave behind), overwriting that address entry with the new
/// candidate *before* looking permanently loses the real rival — it is
/// simply clobbered, never disconnected/evicted — and the lookup instead
/// resolves "the existing rival" as the brand-new candidate itself. Because
/// both are then compared under the identical `is_outbound = true`
/// direction, `keep_existing == keep_incoming` always, which can never
/// satisfy `ReplaceExisting`'s `!keep_existing && keep_incoming` — so a
/// rival that is genuinely wrong-direction and should be replaced instead
/// falls through to `RejectIncoming`, and NEITHER connection ends up as the
/// peer's session: the real rival is silently lost (never evicted, its
/// background tasks never aborted) and the new, tie-break-correct outbound
/// is declined.
#[tokio::test]
async fn outbound_finalize_stale_rival_lookup_is_pure_and_excludes_the_new_candidate() {
    use crate::{GossipConfig, registry::GossipRegistry};

    // Local registry identity is the LOWER NodeId, so a fresh OUTBOUND dial
    // to `remote_peer_id` IS tie-break preferred
    // (`should_keep_connection(remote, is_outbound=true) == true`), while an
    // INBOUND rival for the same peer is NOT
    // (`should_keep_connection(remote, is_outbound=false) == false`) — the
    // textbook "wrong-direction rival must be replaced" case.
    let (hi_kp, lo_kp) = hi_lo_keypairs("finalize-purity-hi", "finalize-purity-lo");
    let remote_peer_id = hi_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(lo_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));

    // A real, LIVE, wrong-direction (Inbound) rival for this peer, indexed
    // ONLY by address (not promoted to "current") — exactly the state this
    // file's own `RejectIncoming`/`EvictStaleRejectIncoming` arms document
    // leaving behind ("we leave the outbound indexed by address only ...
    // without making it the session"). Crucially, it sits at the SAME
    // address the fresh outbound below dials.
    let dial_addr: SocketAddr = "127.0.0.1:7422".parse().unwrap();
    let (rival_io, _rival_keep) = tokio::io::duplex(1024);
    let (rival_sh, _rival_w, _rival_r) = LockFreeStreamHandle::new(
        rival_io,
        dial_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut rival_conn = LockFreeConnection::new(dial_addr, ConnectionDirection::Inbound);
    rival_conn.stream_handle = Some(Arc::new(rival_sh));
    rival_conn.set_state(ConnectionState::Connected);
    let rival = Arc::new(rival_conn);
    assert!(
        rival.has_live_stream(),
        "test precondition: rival must be live"
    );
    pool.index_connection_by_addr(dial_addr, rival.clone());
    pool.add_addr_to_peer_id(dial_addr, remote_peer_id.clone());
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    let (io, _keep) = tokio::io::duplex(1024);
    pool.finalize_new_outbound_connection(dial_addr, io, registry_weak, None)
        .await
        .expect("outbound finalize should succeed");

    // The tie-break-correct outbound must become the peer's current
    // session — never silently declined because the impure lookup mistook
    // itself for its own rival.
    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current.as_ref().is_some_and(
            |c| c.direction == ConnectionDirection::Outbound && !Arc::ptr_eq(c, &rival)
        ),
        "the tie-break-preferred outbound must become the peer's current session, \
         not be declined because a pure-lookup bug mistook the new candidate for \
         its own rival"
    );

    // The real wrong-direction rival must have actually been evicted
    // (background tasks aborted), never silently clobbered/leaked by being
    // overwritten in `connections_by_addr` without ever being disconnected.
    assert!(
        rival
            .stream_handle
            .as_ref()
            .is_some_and(|h| h.exit_flag.load(Ordering::Acquire)),
        "the real wrong-direction rival must be properly evicted (tasks aborted), \
         not silently overwritten/leaked when the new candidate is indexed at its \
         same address"
    );
}

/// RED (review finding P1, `transport_stream.rs` stale-rival branch): the
/// `!alive` branch used to retire `existing_conn` by ADDRESS
/// (`remove_connection(existing_conn.addr)`), never by `Arc` instance
/// identity — even though the branch is explicitly routed through
/// `resolve_connection_conflict` (an identity-only chokepoint) for its
/// decision. If `existing_conn` dies and a fresh, preferred connection gets
/// reindexed at the exact same bind address before that removal actually
/// runs, the address-keyed removal deletes whatever is CURRENTLY at that
/// address — the fresh session — not the stale instance the eviction was
/// actually about. This is the identical address-vs-identity defect this
/// entire branch/PR exists to eliminate, just at the top-of-dial call site
/// (`ConnectionPool::connect_via_stream`) instead of the outbound-finalize
/// one.
///
/// This drives `existing_conn`'s eviction call using the EXACT arguments
/// `connect_via_stream`'s `!alive` branch computes (`existing_conn` snapshot
/// taken while it was still the peer's live connection, then the connection
/// dies, then a fresh preferred connection is reindexed at the identical
/// bind address before the eviction runs — precisely the sequence the
/// finding and this branch's own comment describe), and asserts on the
/// production primitive that call site actually invokes today. A "contrast"
/// section below then demonstrates, on the SAME race-arranged state, that
/// the address-keyed alternative this branch used to call
/// (`remove_connection(existing_conn.addr)`) would destroy the fresh
/// connection instead — the exact defect this fix eliminates.
#[tokio::test]
async fn stale_rival_eviction_at_reindexed_address_is_instance_scoped_not_address_keyed() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("stale-rival-reindex-peer").peer_id();

    // `existing_conn`: alive and published as the peer's current connection
    // at bind address B — exactly the snapshot `connect_via_stream` takes
    // via `get_connection_by_peer_id` before its own separate liveness
    // re-check.
    let bind_addr: SocketAddr = "127.0.0.1:7450".parse().unwrap();
    let (stale_io, _stale_peer_io) = tokio::io::duplex(1024);
    let (stale_sh, _stale_w, _stale_r) = LockFreeStreamHandle::new(
        stale_io,
        bind_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut stale_conn = LockFreeConnection::new(bind_addr, ConnectionDirection::Outbound);
    stale_conn.stream_handle = Some(Arc::new(stale_sh));
    stale_conn.set_state(ConnectionState::Connected);
    let existing_conn = Arc::new(stale_conn);
    assert!(existing_conn.has_live_stream(), "test precondition");
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), bind_addr, existing_conn.clone()));

    // `existing_conn` dies ...
    if let Some(sh) = existing_conn.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }
    assert!(
        !existing_conn.has_live_stream(),
        "test precondition: existing_conn must now be dead, matching the branch's \
         `!alive` condition"
    );

    // ... and a fresh, preferred connection is reindexed/published at the
    // EXACT SAME bind address, before the eviction call runs — modelling a
    // concurrent inbound accept landing in that window.
    let (fresh_io, _fresh_peer_io) = tokio::io::duplex(1024);
    let (fresh_sh, _fresh_w, _fresh_r) = LockFreeStreamHandle::new(
        fresh_io,
        bind_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut fresh_conn = LockFreeConnection::new(bind_addr, ConnectionDirection::Inbound);
    fresh_conn.stream_handle = Some(Arc::new(fresh_sh));
    fresh_conn.set_state(ConnectionState::Connected);
    let fresh_conn = Arc::new(fresh_conn);
    pool.index_connection_by_addr(bind_addr, fresh_conn.clone());
    pool.add_addr_to_peer_id(bind_addr, peer_id.clone());
    pool.publish_current_peer_connection(&peer_id, fresh_conn.clone());

    // The eviction call `connect_via_stream`'s `!alive` branch actually
    // makes today: instance-scoped, re-validating by `Arc` identity
    // immediately before acting.
    let _ = pool.disconnect_connection_instance(&peer_id, &existing_conn);

    let indexed_at_addr = pool.get_lock_free_connection(bind_addr);
    assert!(
        indexed_at_addr
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &fresh_conn)),
        "the fresh connection reindexed at the stale rival's bind address must survive \
         the stale-rival eviction unchanged (got: {indexed_at_addr:?})"
    );
    assert!(
        fresh_conn.has_live_stream(),
        "the fresh connection's background tasks must not be touched by the stale \
         rival's eviction"
    );
    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &fresh_conn)),
        "the fresh connection must remain the peer's current session"
    );

    // Contrast: the address-keyed primitive this branch used to call
    // (`remove_connection(existing_conn.addr)`) on this EXACT same
    // race-arranged state would instead destroy the fresh connection —
    // demonstrating the danger the instance-scoped call above eliminates.
    // Re-arrange fresh back into place first (the assertions above already
    // proved it survived the real fix).
    assert!(
        pool.get_lock_free_connection(bind_addr).is_some(),
        "sanity: fresh connection still indexed before the contrast"
    );
    let removed_by_address = pool.remove_connection(existing_conn.addr);
    assert!(
        removed_by_address
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &fresh_conn)),
        "contrast/danger check: an address-keyed removal at the stale rival's address \
         deletes whatever is CURRENTLY indexed there — the fresh connection, not the \
         stale instance — which is exactly the bug this fix eliminates (got: \
         {removed_by_address:?})"
    );
}

/// RED (review finding, `transport_stream.rs` LIVE wrong-direction rival
/// branch, distinct from the `!alive` stale-rival branch above): after
/// `keep_existing = registry_arc.should_keep_connection(&remote_peer_id,
/// existing_conn.direction == Outbound)` decides `existing_conn` (a live,
/// wrong-direction outbound) must be evicted, the branch used to retire it
/// with the PEER-WIDE `disconnect_connection_by_peer_id(&remote_peer_id)` —
/// "whatever is currently indexed for this peer", not the specific
/// `existing_conn` instance the decision was actually computed about. If a
/// fresh, tie-break-preferred INBOUND connection is published for the same
/// peer between that decision and the eviction call (e.g. a concurrent
/// inbound accept winning the tie-break first), the peer-wide disconnect
/// collaterally tears down that brand-new current session instead of the
/// stale `existing_conn` it evaluated — the identical collateral-teardown
/// thrash this whole branch/PR exists to eliminate, just for the live-rival
/// arm instead of the already-fixed `!alive` one.
///
/// Same race-arranged-state technique as
/// `stale_rival_eviction_at_reindexed_address_is_instance_scoped_not_address_keyed`:
/// `existing_conn` is published as the peer's current session while alive,
/// then a fresh preferred connection is published in its place (modelling the
/// concurrent inbound accept), then the eviction is driven with the exact
/// production primitive this branch calls today
/// (`disconnect_connection_instance`, scoped to `existing_conn`'s own `Arc`
/// identity) and asserted to leave the fresh session untouched. The
/// "contrast" section then demonstrates, on the identical race-arranged
/// state, that the PEER-WIDE alternative this branch used to call
/// (`disconnect_connection_by_peer_id`) would instead destroy the fresh
/// session — the exact defect this fix eliminates.
#[tokio::test]
async fn live_wrong_direction_rival_eviction_is_instance_scoped_not_peer_wide() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("live-wrong-direction-rival-peer").peer_id();

    // `existing_conn`: a LIVE, wrong-direction (Outbound) connection,
    // published as the peer's current session — exactly the snapshot
    // `connect_via_stream`'s live-rival branch evaluates `keep_existing`
    // against before deciding to evict it.
    let bind_addr: SocketAddr = "127.0.0.1:7451".parse().unwrap();
    let (existing_io, _existing_peer_io) = tokio::io::duplex(1024);
    let (existing_sh, _existing_w, _existing_r) = LockFreeStreamHandle::new(
        existing_io,
        bind_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut existing_conn = LockFreeConnection::new(bind_addr, ConnectionDirection::Outbound);
    existing_conn.stream_handle = Some(Arc::new(existing_sh));
    existing_conn.set_state(ConnectionState::Connected);
    let existing_conn = Arc::new(existing_conn);
    assert!(
        existing_conn.has_live_stream(),
        "test precondition: existing_conn must be LIVE, matching this branch's \
         (not the `!alive` branch's) condition"
    );
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), bind_addr, existing_conn.clone()));

    // ... and, before the eviction call runs, a FRESH preferred INBOUND
    // connection is published as the peer's current session at its own
    // address — modelling a concurrent inbound accept winning the tie-break
    // for this peer in the window between `keep_existing`'s decision and the
    // eviction call.
    let fresh_addr: SocketAddr = "127.0.0.1:7452".parse().unwrap();
    let (fresh_io, _fresh_peer_io) = tokio::io::duplex(1024);
    let (fresh_sh, _fresh_w, _fresh_r) = LockFreeStreamHandle::new(
        fresh_io,
        fresh_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut fresh_conn = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
    fresh_conn.stream_handle = Some(Arc::new(fresh_sh));
    fresh_conn.set_state(ConnectionState::Connected);
    let fresh_conn = Arc::new(fresh_conn);
    pool.index_connection_by_addr(fresh_addr, fresh_conn.clone());
    pool.add_addr_to_peer_id(fresh_addr, peer_id.clone());
    pool.publish_current_peer_connection(&peer_id, fresh_conn.clone());

    // The eviction call `connect_via_stream`'s live wrong-direction-rival
    // branch actually makes today: instance-scoped, re-validating by `Arc`
    // identity immediately before acting.
    let _ = pool.disconnect_connection_instance(&peer_id, &existing_conn);

    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &fresh_conn)),
        "the fresh preferred inbound published for this peer must remain the peer's \
         current session, unaffected by the wrong-direction rival's eviction (got: \
         {current:?})"
    );
    assert!(
        fresh_conn.has_live_stream(),
        "the fresh connection's background tasks must not be touched by the live \
         wrong-direction rival's eviction"
    );
    // `existing_conn` was already superseded (no longer the peer's current
    // session) by the time the eviction call runs — the exact race outcome
    // this test models. `disconnect_connection_instance` correctly declines
    // to touch it in that case (see its own doc comment: "a safe no-op if a
    // fresh preferred connection was already published/reindexed for that
    // peer"); its own retirement is the concurrently-winning accept path's
    // responsibility, not this call's. Asserting it stays untouched here
    // pins that documented no-op contract rather than fabricating a
    // touched/aborted expectation this call was never meant to satisfy.
    assert!(
        existing_conn.has_live_stream(),
        "disconnect_connection_instance must be a no-op on an already-superseded \
         target, never reaching in to abort a connection it declined to clear"
    );

    // Contrast: the peer-wide primitive this branch used to call
    // (`disconnect_connection_by_peer_id`) on this EXACT same race-arranged
    // state would instead destroy the fresh session — demonstrating the
    // danger the instance-scoped call above eliminates. Re-arrange fresh
    // back into place first (the assertions above already proved it survived
    // the real fix).
    assert!(
        pool.get_connection_by_peer_id(&peer_id)
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &fresh_conn)),
        "sanity: fresh connection still the peer's current session before the contrast"
    );
    let removed_peer_wide = pool.disconnect_connection_by_peer_id(&peer_id);
    assert!(
        removed_peer_wide
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &fresh_conn)),
        "contrast/danger check: a peer-wide disconnect at the wrong-direction rival's \
         eviction site tears down whatever is CURRENTLY indexed for the peer — the \
         fresh session, not the stale instance — which is exactly the bug this fix \
         eliminates (got: {removed_peer_wide:?})"
    );
}

/// RED (review finding P2, outbound-finalize peer-wide eviction race):
/// `disconnect_connection_by_peer_id` is peer-wide — it tears down
/// "whatever is currently indexed" for a peer, not a specific connection
/// instance. The outbound-finalize `EvictStaleRejectIncoming` (and
/// `ReplaceExisting`) arms compute their eviction target from a rival
/// snapshot taken *before* the candidate is indexed; if a concurrent inbound
/// accept (`handle_incoming_connection_tls`) publishes a fresh preferred
/// inbound for the same peer identity between that snapshot and the
/// eviction call, a peer-wide disconnect at eviction time collaterally tears
/// down the fresh inbound instead of the stale rival the decision was
/// actually about — the exact reconnect thrash from the outbound side.
/// `disconnect_connection_instance` is the fix: it must re-validate that its
/// target is still the connection actually indexed for the peer (by `Arc`
/// identity) immediately before acting, and must be a no-op — never touching
/// a different, concurrently-published connection — when it is not.
#[tokio::test]
async fn disconnect_connection_instance_never_removes_a_concurrently_published_replacement() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("finalize-instance-scope-peer").peer_id();

    // T1: the stale rival an outbound-finalize tie-break decision was
    // computed about (`existing_before` in `finalize_new_outbound_connection`).
    let stale_addr: SocketAddr = "127.0.0.1:7430".parse().unwrap();
    let (stale_io, _stale_peer_io) = tokio::io::duplex(1024);
    let (stale_sh, _stale_w, _stale_r) = LockFreeStreamHandle::new(
        stale_io,
        stale_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut stale_conn = LockFreeConnection::new(stale_addr, ConnectionDirection::Outbound);
    stale_conn.stream_handle = Some(Arc::new(stale_sh));
    stale_conn.set_state(ConnectionState::Connected);
    let stale_rival = Arc::new(stale_conn);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, stale_rival.clone()));

    // Between the decision and the eviction call, a concurrent inbound
    // accept publishes a fresh, live, preferred inbound for the SAME peer
    // identity — modelling `handle_incoming_connection_tls` racing the
    // outbound-finalize path.
    let fresh_addr: SocketAddr = "127.0.0.1:7431".parse().unwrap();
    let (fresh_io, _fresh_peer_io) = tokio::io::duplex(1024);
    let (fresh_sh, _fresh_w, _fresh_r) = LockFreeStreamHandle::new(
        fresh_io,
        fresh_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut fresh_conn = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
    fresh_conn.stream_handle = Some(Arc::new(fresh_sh));
    fresh_conn.set_state(ConnectionState::Connected);
    let fresh_inbound = Arc::new(fresh_conn);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), fresh_addr, fresh_inbound.clone()));

    // T2: eviction targeting the ORIGINAL stale rival instance captured at
    // T1 — must be a no-op now that the peer's indexed connection has moved
    // on to `fresh_inbound`.
    let evicted = pool.disconnect_connection_instance(&peer_id, &stale_rival);
    assert!(
        !evicted,
        "must decline to evict once the target is no longer the peer's indexed connection"
    );
    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &fresh_inbound)),
        "the concurrently published fresh inbound must survive the stale rival's \
         instance-scoped eviction unchanged"
    );

    // Contrast: the peer-wide primitive this instance-scoped one exists to
    // guard against removes "whatever is current" without re-validating —
    // exactly the bug. Demonstrating it here (on the now-current fresh
    // inbound) makes the danger, and the fix's necessity, explicit.
    let removed = pool.disconnect_connection_by_peer_id(&peer_id);
    assert!(
        removed.is_some(),
        "peer-wide disconnect removes whatever is current"
    );
    assert!(pool.current_peer_connection_instance(&peer_id).is_none());
}

/// Reviewer finding (P2, `disconnect_connection_instance`): extends
/// `disconnect_connection_instance_never_removes_a_concurrently_published_replacement`,
/// which only modelled the replacement being published *before* the
/// instance-scoped teardown call — a pure sequential ordering that the
/// existing `Arc`-identity check-then-return already handled correctly. The
/// actual defect was a SECOND check-then-act pair *inside*
/// `disconnect_connection_instance` itself: after confirming `target` was
/// still current, it called the unconditional `clear_current_peer_connection`,
/// which has a real gap (logging + lifecycle-event construction) before it
/// stores `None`. A concurrent `publish_current_peer_connection` landing in
/// that specific gap — *during* teardown, not before it — got clobbered by
/// the unconditional clear.
///
/// This drives the two operations with genuine OS-thread concurrency
/// (raw `std::thread::spawn` + a `std::sync::Barrier`, so both threads are
/// released at the same instant and can run truly in parallel on separate
/// cores — a cooperative-scheduling `tokio::spawn` race was not tight
/// enough to reliably land in the gap) across many iterations so the
/// scheduler actually explores it. With the
/// checked-then-unconditional-clear idiom, `current == None` was reachable
/// (the fresh publish landed in the gap and was then clobbered); with the
/// atomic `compare_and_clear_current_connection` CAS fix, exactly `fresh`
/// must survive on every single iteration, deterministically — the CAS
/// either observes `target` and clears it before `fresh` is installed (and
/// `fresh`'s later, unconditional publish then wins regardless), or it
/// observes `fresh` already installed and declines outright. Either way
/// `None` is never a reachable outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_connection_instance_atomic_clear_survives_concurrent_publish_mid_teardown() {
    for i in 0u16..300 {
        let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
        let peer_id = crate::KeyPair::new_for_testing(format!("race-mid-teardown-{i}")).peer_id();

        let stale_addr: SocketAddr = format!("127.0.0.1:{}", 21000 + i).parse().unwrap();
        let (stale_io, _stale_peer_io) = tokio::io::duplex(1024);
        let (stale_sh, _stale_w, _stale_r) = LockFreeStreamHandle::new(
            stale_io,
            stale_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut stale_conn = LockFreeConnection::new(stale_addr, ConnectionDirection::Outbound);
        stale_conn.stream_handle = Some(Arc::new(stale_sh));
        stale_conn.set_state(ConnectionState::Connected);
        let stale = Arc::new(stale_conn);
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, stale.clone()));

        let fresh_addr: SocketAddr = format!("127.0.0.1:{}", 31000 + i).parse().unwrap();
        let (fresh_io, _fresh_peer_io) = tokio::io::duplex(1024);
        let (fresh_sh, _fresh_w, _fresh_r) = LockFreeStreamHandle::new(
            fresh_io,
            fresh_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut fresh_conn = LockFreeConnection::new(fresh_addr, ConnectionDirection::Inbound);
        fresh_conn.stream_handle = Some(Arc::new(fresh_sh));
        fresh_conn.set_state(ConnectionState::Connected);
        let fresh = Arc::new(fresh_conn);

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let evict_task = {
            let pool = pool.clone();
            let peer_id = peer_id.clone();
            let target = stale.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                pool.disconnect_connection_instance(&peer_id, &target)
            })
        };
        let publish_task = {
            let pool = pool.clone();
            let peer_id = peer_id.clone();
            let fresh = fresh.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                pool.publish_current_peer_connection(&peer_id, fresh);
            })
        };

        evict_task.join().expect("evict task must not panic");
        publish_task.join().expect("publish task must not panic");

        let current = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
            "iteration {i}: a concurrently published replacement must survive a \
             same-instant instance-scoped teardown of a DIFFERENT instance — atomic \
             compare-and-clear must never clobber a publish landing mid-teardown \
             (got {current:?})"
        );
    }
}

/// Reviewer finding (P2, `disconnect_connection_instance`): before the fix
/// this only removed `target.addr` from `connections_by_addr`, but an
/// accepted inbound is commonly indexed under BOTH the
/// advertised/configured bind address AND the ephemeral socket address
/// (see the "Also indexed incoming connection by ephemeral address" block in
/// `handle_incoming_connection_tls`). Leaving the second alias behind is a
/// zombie: a later socket-failure cleanup keyed on that alias
/// double-decrements accounting, and direct address lookups through it
/// observe a dead connection. The fix scans `connections_by_addr` for every
/// entry whose value is `Arc::ptr_eq` to the torn-down instance and removes
/// all of them (plus their `addr_to_peer_id` rows) — still instance-scoped,
/// never a peer-id sweep.
#[test]
fn disconnect_connection_instance_removes_all_address_aliases() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("alias-removal-peer").peer_id();

    let bind_addr: SocketAddr = "127.0.0.1:7440".parse().unwrap();
    let ephemeral_addr: SocketAddr = "127.0.0.1:54440".parse().unwrap();

    let conn = LockFreeConnection::new(bind_addr, ConnectionDirection::Inbound);
    conn.set_state(ConnectionState::Connected);
    let target = Arc::new(conn);

    assert!(pool.add_connection_by_peer_id(peer_id.clone(), bind_addr, target.clone()));
    // Mirrors the ephemeral-address indexing `handle_incoming_connection_tls`
    // performs after acceptance for an accepted inbound: both the bind addr
    // and the ephemeral socket addr point at the same instance.
    pool.index_connection_by_addr(ephemeral_addr, target.clone());
    pool.add_addr_to_peer_id(ephemeral_addr, peer_id.clone());

    assert!(pool.disconnect_connection_instance(&peer_id, &target));

    assert!(
        pool.connections_by_addr
            .read_sync(&bind_addr, |_, _| ())
            .is_none(),
        "bind-address alias must be removed"
    );
    assert!(
        pool.connections_by_addr
            .read_sync(&ephemeral_addr, |_, _| ())
            .is_none(),
        "ephemeral-address alias must be removed — a lingering entry is exactly the \
         zombie that double-decrements accounting on a later socket-failure cleanup \
         keyed on this alias"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&bind_addr, |_, _| ())
            .is_none(),
        "bind-address addr_to_peer_id row must be removed"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&ephemeral_addr, |_, _| ())
            .is_none(),
        "ephemeral-address addr_to_peer_id row must be removed — no zombie row left \
         pointing at a torn-down instance"
    );
}

/// Reviewer finding (P2, `remove_connection_instance_by_id`'s defensive
/// current-session clear): the stale-instance cleanup path called
/// `clear_current_peer_connection_if_matches`, which is a genuine
/// check-then-act pair — it reads the peer session's current connection,
/// `Arc::ptr_eq`-compares it against the retiring instance, and only THEN
/// (after constructing a log line and a lifecycle event — real work, a real
/// gap) calls the unconditional `clear_current_peer_connection`, which stores
/// `None` and removes the `connections_by_peer` row regardless of what is
/// installed by that point. A concurrent `publish_current_peer_connection`
/// for a fresh instance landing in that gap — after the read observed the
/// stale instance still current, but before the unconditional store — is
/// clobbered: the fresh session is erased and its `connections_by_peer` entry
/// removed, even though the cleanup was only ever supposed to retire the
/// stale, already-superseded instance. This is exactly the
/// collateral-teardown/reconnect-thrash race this PR closes, reopened through
/// this one remaining check-then-clear call site.
///
/// Rather than chase the check-then-clear gap with a wall-clock OS-thread
/// race (unreliable here: `remove_connection_instance_by_id` does real
/// address-index bookkeeping — the `connections_by_addr`/`addr_to_peer_id`
/// removal and alias sweep — *before* it ever reaches the current-session
/// clear, which in practice lets a naive concurrent publish finish well
/// before the check runs, masking the bug under ordinary scheduling), this
/// pins the race deterministically using the crate's own
/// `set_transport_lifecycle_recorder` hook. `clear_current_peer_connection_if_matches`
/// unconditionally fires a `SessionRemoved { reason: CurrentConnectionCleared }`
/// lifecycle event *before* calling the unconditional
/// `clear_current_peer_connection` store — i.e. exactly inside the
/// check-then-act gap. Installing a recorder that publishes the FRESH
/// connection synchronously from within that event callback lands the
/// concurrent publish in that exact window on every run, with no scheduling
/// luck required.
/// Process-wide lock serializing every test that installs the global
/// `set_transport_lifecycle_recorder` hook. The recorder is shared, mutable,
/// global state (a single `OnceLock<RwLock<Option<...>>>` in `lifecycle.rs`);
/// the default multi-threaded test harness runs `#[test]`/`#[tokio::test]`
/// functions concurrently, so without this lock two such tests can install/
/// deregister each other's closures mid-test — a race entirely orthogonal to
/// (and far more likely to fire than) the specific check-then-act gap each
/// test pins deterministically. Acquired for the guard's entire lifetime,
/// alongside its deregister-on-drop behavior below.
static RECORDER_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct RecorderGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);
impl RecorderGuard {
    fn acquire() -> Self {
        Self(
            RECORDER_TEST_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}
impl Drop for RecorderGuard {
    fn drop(&mut self) {
        crate::set_transport_lifecycle_recorder(None);
    }
}

#[tokio::test]
async fn stale_instance_cleanup_uses_atomic_cas_and_preserves_fresh_current_session() {
    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
    let peer_id = crate::KeyPair::new_for_testing("stale-cleanup-cas-peer").peer_id();

    let stale_addr: SocketAddr = "127.0.0.1:42000".parse().unwrap();
    let stale = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
    let stale_instance_id = stale
        .stream_handle
        .as_ref()
        .map(|handle| handle.instance_id())
        .expect("stale connection must have a live stream handle");
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, stale.clone()));

    let fresh_addr: SocketAddr = "127.0.0.1:52000".parse().unwrap();
    let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;

    // Install the injection hook, guarded so it is always uninstalled again
    // (including on panic/assertion failure) — this is process-global state
    // shared with every other test in the binary.
    let _guard = RecorderGuard::acquire();
    {
        let pool = pool.clone();
        let peer_id = peer_id.clone();
        let fresh = fresh.clone();
        crate::set_transport_lifecycle_recorder(Some(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::SessionRemoved {
                peer,
                reason: crate::SessionRemovalReason::CurrentConnectionCleared,
                ..
            } = &event
                && *peer == peer_id
            {
                // Deregister first: the nested `publish_current_peer_connection`
                // call below fires its own (non-matching) `SessionPublished`
                // lifecycle event through this same global hook, and this
                // avoids any reentrant/recursive invocation of this closure.
                crate::set_transport_lifecycle_recorder(None);
                pool.publish_current_peer_connection(&peer_id, fresh.clone());
            }
        })));
    }

    // The defensive stale-instance cleanup path
    // (`remove_connection_instance_by_id`) retiring the OLD `stale`
    // instance — models a failed/superseded connection finally being torn
    // down. Per the hook installed above, the FRESH connection is published
    // as the peer's current session synchronously from inside the
    // check-then-act gap of the stale-instance clear.
    pool.remove_connection_instance_by_id(stale_addr, stale_instance_id);

    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
        "a fresh session published from inside the stale-instance clear's check-then-act \
         gap must survive — atomic compare-and-clear must never clobber a publish landing \
         mid-cleanup (got {current:?})"
    );
    assert!(
        pool.connections_by_peer
            .read_sync(&peer_id, |_, v| Arc::ptr_eq(v, &fresh))
            .unwrap_or(false),
        "`connections_by_peer` must still point at the fresh instance — its removal must be \
         conditional on the CAS actually having cleared the stale instance, never \
         unconditional"
    );
}

/// RED (review finding P2, `remote_actor_ref.rs` ask-timeout/cancellation
/// eviction depends on a stale `addr_to_peer_id` alias):
/// `recover_connection_after_actor_ask_timeout` and
/// `ActorAskCancellationGuard::drop` both retired the connection instance a
/// failed/cancelled ask ran on via `ConnectionPool::remove_connection_instance_by_id(addr,
/// instance_id)` alone — even though BOTH call sites already know the exact
/// `peer_id` the instance belongs to (`self.location.peer_id` /
/// `self.peer_id`, captured from `RemoteActorRef`/the guard at ask time).
/// `remove_connection_instance_by_id` derives the peer id it clears
/// `peer_sessions`/`connections_by_peer` for from `addr_to_peer_id[addr]`,
/// read BEFORE the address-indexed removal even runs — if that alias row is
/// missing or stale (a real possibility: reindexing, an unrelated cleanup
/// racing this same address, or simply never having been re-added), the
/// defensive current-session clear is silently skipped even when the
/// retiring instance genuinely IS the peer's live current session, leaving a
/// dead session published in `peer_sessions`/`connections_by_peer`.
///
/// This contrasts the two primitives directly at the level both call sites
/// actually invoke (the same "HONESTY NOTE" pattern used above for the
/// `handle.rs` inbound-accept finding): `remove_connection_instance_by_id`
/// (what both call sites used before this fix, unchanged by it — it retains
/// its original, narrower contract for the "instance already superseded"
/// case) versus `remove_connection_instance_for_peer` (what both call sites
/// use after this fix), on IDENTICAL pool state with a deliberately staled
/// `addr_to_peer_id` alias.
///
/// RED (observed against `remove_connection_instance_by_id`, the pre-fix
/// primitive, at HEAD and unchanged after the fix — this half of the
/// assertion documents the bug the fix routes AROUND, it does not itself
/// change): the dead instance remains published as the peer's current
/// session. GREEN (observed against `remove_connection_instance_for_peer`,
/// only added by this fix): the dead instance is cleared from
/// `peer_sessions`/`connections_by_peer`.
#[tokio::test]
async fn ask_timeout_eviction_current_session_survives_stale_alias_without_destroying_fresh_reconnect()
 {
    let peer_id = crate::KeyPair::new_for_testing("ask-timeout-stale-alias-peer").peer_id();
    let addr: SocketAddr = "127.0.0.1:7490".parse().unwrap();

    // --- Part 1: contrast the two primitives on identical state -----------
    async fn setup_dead_current_session_with_staled_alias(
        peer_id: &crate::PeerId,
        addr: SocketAddr,
    ) -> (ConnectionPool<()>, Arc<LockFreeConnection>, u64) {
        let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
        let conn = make_live_connection(addr, ConnectionDirection::Outbound).await;
        let instance_id = conn
            .stream_handle
            .as_ref()
            .map(|h| h.instance_id())
            .expect("live connection must have a stream instance");
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, conn.clone()));
        // The ask this instance ran on has since timed out/been cancelled and
        // the underlying transport is dead — model that directly, matching
        // what a real failed ask observes.
        if let Some(sh) = conn.stream_handle.as_ref() {
            sh.exit_flag.store(true, Ordering::Release);
        }
        // Simulate the alias having gone stale/missing by the time recovery
        // runs — e.g. raced by an unrelated reindex — while the instance is
        // STILL genuinely the peer's published current session.
        let _ = pool.addr_to_peer_id.remove_sync(&addr);
        assert!(
            pool.addr_to_peer_id
                .read_sync(&addr, |_, v| v.clone())
                .is_none(),
            "test precondition: the address alias must be gone"
        );
        assert!(
            pool.peer_sessions
                .read_sync(peer_id, |_, s| s
                    .current_connection()
                    .is_some_and(|c| Arc::ptr_eq(&c, &conn)))
                .unwrap_or(false),
            "test precondition: the dead instance must still be the peer's published current \
             session despite the missing alias"
        );
        (pool, conn, instance_id)
    }

    let (pool_old, dead_old, instance_id) =
        setup_dead_current_session_with_staled_alias(&peer_id, addr).await;
    pool_old.remove_connection_instance_by_id(addr, instance_id);
    let old_still_published = pool_old
        .peer_sessions
        .read_sync(&peer_id, |_, s| {
            s.current_connection()
                .is_some_and(|c| Arc::ptr_eq(&c, &dead_old))
        })
        .unwrap_or(false)
        || pool_old
            .connections_by_peer
            .read_sync(&peer_id, |_, v| Arc::ptr_eq(v, &dead_old))
            .unwrap_or(false);
    assert!(
        old_still_published,
        "documents the bug this fix routes both call sites AROUND: \
         `remove_connection_instance_by_id` alone, given a stale/missing address alias, \
         leaves a dead current session published in peer_sessions/connections_by_peer"
    );

    let (pool_new, dead_new, instance_id) =
        setup_dead_current_session_with_staled_alias(&peer_id, addr).await;
    let evicted = pool_new.remove_connection_instance_for_peer(&peer_id, addr, instance_id);
    assert!(
        evicted.as_ref().is_some_and(|c| Arc::ptr_eq(c, &dead_new)),
        "the peer-id-aware eviction must find and retire the dead instance even with a \
         stale/missing address alias"
    );
    assert!(
        pool_new
            .peer_sessions
            .read_sync(&peer_id, |_, s| s.current_connection().is_none())
            .unwrap_or(true),
        "the dead current session must actually be cleared from peer_sessions — never left \
         published just because the address alias was stale"
    );
    assert!(
        pool_new
            .connections_by_peer
            .read_sync(&peer_id, |_, _| ())
            .is_none(),
        "connections_by_peer must not still point at the dead instance either"
    );

    // --- Part 2: a concurrently reconnected FRESH session must survive ----
    // The failed ask's own instance/addr no longer matches ANYTHING (neither
    // the address slot, which a fresh reconnect has already taken over, nor
    // the peer's current session) — eviction must find nothing and must
    // never touch the fresh session.
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let stale_addr: SocketAddr = "127.0.0.1:7491".parse().unwrap();
    let stale = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
    let stale_instance_id = stale
        .stream_handle
        .as_ref()
        .map(|h| h.instance_id())
        .expect("stale connection must have a stream instance");
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, stale.clone()));
    if let Some(sh) = stale.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }

    // A fresh reconnect supersedes it at the EXACT SAME bind address before
    // recovery runs — modelling a concurrent reconnect landing between the
    // failed ask and its cleanup and reusing the same address. Both
    // `connections_by_addr[stale_addr]` and the peer's current session now
    // point at `fresh`, a DIFFERENT instance than the one the failed ask ran
    // on.
    let fresh = make_live_connection(stale_addr, ConnectionDirection::Inbound).await;
    pool.index_connection_by_addr(stale_addr, fresh.clone());
    pool.add_addr_to_peer_id(stale_addr, peer_id.clone());
    pool.publish_current_peer_connection(&peer_id, fresh.clone());

    let evicted = pool.remove_connection_instance_for_peer(&peer_id, stale_addr, stale_instance_id);
    assert!(
        evicted.is_none(),
        "an already-superseded instance whose address slot has since been reindexed to a \
         DIFFERENT instance must not be reported as evicted — nothing matching the failed \
         instance's own id remains indexed anywhere: {evicted:?}"
    );
    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
        "a concurrently reconnected FRESH session for the same peer must never be \
         collaterally destroyed by cleanup for an older, already-superseded ask (got {current:?})"
    );
    assert!(
        fresh.has_live_stream(),
        "the fresh session's background tasks must survive untouched"
    );
}

/// Test helper: a `LockFreeConnection` with a real, live stream handle (so
/// `has_live_stream()`/`get_connection_by_peer_id`'s usability filter treats
/// it as usable), backed by an in-memory `tokio::io::duplex` pair.
async fn make_live_connection(
    addr: SocketAddr,
    direction: ConnectionDirection,
) -> Arc<LockFreeConnection> {
    let (io, _peer_io) = tokio::io::duplex(1024);
    let (sh, _w, _r) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut conn = LockFreeConnection::new(addr, direction);
    conn.stream_handle = Some(Arc::new(sh));
    conn.set_state(ConnectionState::Connected);
    Arc::new(conn)
}

/// Reviewer finding 1 (P1, `handle.rs` inbound-accept tie-break): the
/// inbound-accept evict/replace arms compute their decision against
/// `existing_conn` but, before the fix, called the PEER-WIDE
/// `disconnect_connection_by_peer_id(peer_id)` to act on it. Between the
/// decision and that call, a concurrent accept/finalize can publish a fresh
/// connection for the same peer, and the peer-wide disconnect tears down
/// THAT replacement instead of the stale rival the decision was actually
/// about.
///
/// HONESTY NOTE: this pins the exact contrast at the primitive level (the
/// call shape `handle.rs`'s arms use), the same established pattern as
/// `disconnect_connection_instance_never_removes_a_concurrently_published_replacement`
/// for the outbound-finalize side. It does not drive the real async
/// `handle_incoming_connection_tls` accept path with genuine concurrency —
/// an attempt to do so found that racing a publish against that call can
/// itself change which decision branch is taken (the race can land before
/// the internal snapshot too, altering `existing_usable`), so it cannot
/// isolate this specific bug reliably and was intentionally not kept. The
/// routing fix in `handle.rs` (swapping `disconnect_connection_by_peer_id`
/// for `disconnect_connection_instance` in the three evict/replace arms) is
/// verified by direct code inspection plus the full end-to-end regression
/// suite (`tiebreak_reconnect_thrash`, `tie_break_reconnect_storm`,
/// `witness_mesh_restart_and_simultaneous_open_matrix_converges_quietly`)
/// remaining green, which exercises this exact inbound-accept-vs-concurrent
/// -finalize thrash scenario end-to-end.
#[tokio::test]
async fn inbound_accept_evict_arm_must_use_instance_scoped_disconnect_not_peer_wide() {
    let peer_id = crate::KeyPair::new_for_testing("inbound-accept-race-peer").peer_id();
    let stale_addr: SocketAddr = "127.0.0.1:7450".parse().unwrap();
    let fresh_addr: SocketAddr = "127.0.0.1:7451".parse().unwrap();

    // First pool: documents the WRONG (pre-fix) call shape used by the
    // inbound-accept arms — peer-wide disconnect keyed only on `peer_id`,
    // ignoring the specific `existing_conn` a tie-break decision was
    // actually about. Between the decision and the eviction call, a
    // concurrent accept/finalize publishes a fresh replacement for the SAME
    // peer.
    {
        let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
        let existing_conn = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, existing_conn.clone()));
        let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;
        pool.publish_current_peer_connection(&peer_id, fresh.clone());

        let wrongly_evicted = pool.disconnect_connection_by_peer_id(&peer_id);
        assert!(
            wrongly_evicted.is_some_and(|c| Arc::ptr_eq(&c, &fresh)),
            "documents the bug this finding fixes: the peer-wide primitive tears down \
             whatever is current for the peer — including a concurrently published \
             replacement the decision was never about"
        );
    }

    // Second pool, same shape: exercises the FIXED call shape, routed
    // through the instance-scoped primitive with the exact `existing_conn`
    // snapshot, exactly as the fixed `handle.rs` inbound-accept arms now do.
    {
        let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
        let existing_conn = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
        assert!(pool.add_connection_by_peer_id(peer_id.clone(), stale_addr, existing_conn.clone()));
        let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;
        pool.publish_current_peer_connection(&peer_id, fresh.clone());

        let correctly_declined = pool.disconnect_connection_instance(&peer_id, &existing_conn);
        assert!(
            !correctly_declined,
            "instance-scoped disconnect must decline: `existing_conn` is no longer the \
             peer's indexed connection"
        );
        let current = pool.get_connection_by_peer_id(&peer_id);
        assert!(
            current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
            "the concurrently published replacement must survive the inbound-accept \
             arm's instance-scoped eviction of the stale snapshot"
        );
    }
}

/// Audit finding D1: correlation slots are addressed by a 13-bit index, so
/// `id` and `id + 8192*k` collide on the same slot. A stale/delayed response
/// carrying a recycled, aliased id must NOT complete the slot currently owned
/// by a *different* in-flight request — otherwise one RPC silently receives
/// another RPC's response.
#[test]
fn complete_rejects_aliased_correlation_id() {
    let tracker = CorrelationTracker::new();
    let guard = tracker.allocate().expect("slot should be available");
    let id = guard.id();
    let aliased = id.wrapping_add(PENDING_RESPONSES_SIZE as u16);
    assert_eq!(
        CorrelationTracker::slot_index(id),
        CorrelationTracker::slot_index(aliased),
        "test precondition: the two ids must map to the same slot"
    );
    assert_ne!(id, aliased, "the two ids must be distinct");

    let pool = Arc::new(crate::AlignedBytesPool::default());
    let mut response = Some(crate::AlignedBytes::from_pooled_slice(
        b"stale-response",
        Arc::clone(&pool),
    ));
    assert!(
        !tracker.complete(aliased, &mut response),
        "an aliased correlation id must not complete a slot owned by a different id"
    );
    assert!(
        response.is_some(),
        "the response must be left intact when the full id does not match"
    );

    // The genuine owner still completes normally.
    assert!(
        tracker.complete(id, &mut response),
        "the owning correlation id must complete its own slot"
    );
    assert!(
        response.is_none(),
        "the response is consumed when the full id matches"
    );

    drop(guard);
}

/// R1 regression: a `push()` parked on a full write queue must wake and return
/// `ConnectionClosed` when the owning IO writer task tears down, instead of
/// hanging forever (the space notifier is otherwise only fired by `pop()`,
/// which has stopped). Before the fix this test times out.
#[test]
fn write_queue_push_unblocks_on_teardown() {
    let rt = Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        // Smallest capacity (constructor clamps to 128).
        let queue = WriteQueue::new(1, addr);

        // Fill the queue to capacity so the next push must park.
        let cap = 128usize;
        for _ in 0..cap {
            queue
                .try_push(WriteCommand::Payload(WritePayload::Single(
                    bytes::Bytes::new(),
                )))
                .expect("fill within capacity");
        }
        // Queue is full now.
        assert!(
            queue
                .try_push(WriteCommand::Payload(WritePayload::Single(
                    bytes::Bytes::new()
                )))
                .is_err(),
            "queue should be full"
        );

        let queue_for_push = queue.clone();
        let push_task = tokio::spawn(async move {
            queue_for_push
                .push(WriteCommand::Payload(WritePayload::Single(
                    bytes::Bytes::new(),
                )))
                .await
        });

        // Let the push task park on the full queue.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Simulate ExitGuard::drop teardown.
        queue.mark_closed_and_wake();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), push_task)
            .await
            .expect("push must not hang after teardown")
            .expect("push task panicked");

        match result {
            Err(GossipError::ConnectionClosed(a)) => assert_eq!(a, addr),
            other => panic!("expected ConnectionClosed, got {other:?}"),
        }
    });
}

/// R1 regression for the streaming queue (same teardown contract).
#[test]
fn streaming_queue_push_unblocks_on_teardown() {
    let rt = Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        let addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let queue = StreamingQueue::new(1, addr);

        let cap = 64usize;
        for _ in 0..cap {
            queue
                .try_push(StreamingCommand::Flush)
                .expect("fill within capacity");
        }
        assert!(
            queue.try_push(StreamingCommand::Flush).is_err(),
            "queue should be full"
        );

        let queue_for_push = queue.clone();
        let push_task =
            tokio::spawn(async move { queue_for_push.push(StreamingCommand::Flush).await });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        queue.mark_closed_and_wake();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), push_task)
            .await
            .expect("push must not hang after teardown")
            .expect("push task panicked");

        match result {
            Err(GossipError::ConnectionClosed(a)) => assert_eq!(a, addr),
            other => panic!("expected ConnectionClosed, got {other:?}"),
        }
    });
}
