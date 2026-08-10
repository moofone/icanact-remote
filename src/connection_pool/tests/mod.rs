use super::*;
use futures::StreamExt;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::{Arc, Barrier, Mutex};
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
        correlation_id: Option<u32>,
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

/// Always errors, for `ask_immediate_handler_sync_error_nacks_instead_of_letting_the_asker_time_out`.
struct ErroringImmediateAskActor;

impl crate::registry::ActorAskImmediateHandlerSync for ErroringImmediateAskActor {
    fn handle_actor_ask_sync_immediate(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: crate::AlignedBytes,
    ) -> crate::Result<crate::registry::AskDisposition> {
        Err(crate::GossipError::Network(std::io::Error::other(
            "erroring immediate ask handler (test)",
        )))
    }
}

/// Always errors, for `ask_handler_sync_error_nacks_instead_of_letting_the_asker_time_out`.
struct ErroringDeferredAskActor;

impl crate::registry::ActorAskHandlerSync for ErroringDeferredAskActor {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: crate::AlignedBytes,
        _context: crate::AskContext<'_>,
    ) -> crate::Result<crate::registry::AskDisposition> {
        Err(crate::GossipError::Network(std::io::Error::other(
            "erroring deferred ask handler (test)",
        )))
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
        _correlation_id: Option<u32>,
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

/// Counts every dispatched invocation for (`TEST_TELL_ACTOR_ID`,
/// `TEST_TELL_HASH`), tell or ask, and additionally echoes the payload back
/// for asks (correlation id present). Used where a test needs to observe
/// "the handler ran" for a message regardless of whether it arrived as a
/// streamed ask or a plain tell.
struct EchoAskCountAll {
    delivered: Arc<AtomicU64>,
}

impl crate::registry::ActorMessageHandlerSync for EchoAskCountAll {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: crate::AlignedBytes,
        correlation_id: Option<u32>,
    ) -> crate::Result<Option<crate::registry::ActorResponse>> {
        if actor_id != TEST_TELL_ACTOR_ID || type_hash != TEST_TELL_HASH {
            return Ok(None);
        }
        self.delivered.fetch_add(1, Ordering::Relaxed);
        if correlation_id.is_some() {
            Ok(Some(crate::registry::ActorResponse::from(payload)))
        } else {
            Ok(None)
        }
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

/// P0: the bidirectional IO task must keep reading while a large streamed
/// frame is only partially writable. If each side awaits a whole ~1 MiB
/// `VectoredWrite` before returning to its read loop, two simultaneous asks
/// deadlock as soon as both 64 KiB duplex directions fill: neither writer can
/// finish, and neither IO task reaches the reads that would drain its peer.
///
/// Keep the transport deliberately smaller than one stream chunk and make
/// both requests and their echoed responses multi-MiB. Completion proves the
/// writer yields at a bounded byte slice in both directions; payload equality
/// proves that yielding does not change ordinary ask semantics.
#[test]
fn simultaneous_multi_mib_asks_complete_over_constrained_duplex() {
    run_multi_thread_test(async {
        const DUPLEX_CAPACITY: usize = 64 * 1024;
        const PAYLOAD_BYTES: usize = 3 * 1024 * 1024;
        const COMPLETION_BOUND: Duration = Duration::from_secs(3);

        let addr_a: std::net::SocketAddr = "127.0.0.1:40491".parse().unwrap();
        let addr_b: std::net::SocketAddr = "127.0.0.1:40492".parse().unwrap();

        let registry_a = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_a,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_fairness_constrained_a",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        registry_a
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;
        let registry_b = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_b,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "stream_fairness_constrained_b",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        registry_b
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let correlation_a = CorrelationTracker::new();
        let correlation_b = CorrelationTracker::new();
        let (io_a, io_b) = tokio::io::duplex(DUPLEX_CAPACITY);

        let read_ctx_a = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_a),
            peer_addr: addr_b,
            session_source: addr_b,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_a.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_a.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: registry_a.actor_message_handler_sync.load_full(),
        };
        let read_ctx_b = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_b),
            peer_addr: addr_a,
            session_source: addr_a,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_b.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_b.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: registry_b.actor_message_handler_sync.load_full(),
        };

        let (writer_a, task_a, _) = LockFreeStreamHandle::new(
            io_a,
            addr_b,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_a),
        );
        let writer_a = Arc::new(writer_a);
        let conn_a =
            ConnectionHandle::<()>::new_stream(addr_b, ConnectionDirection::Outbound, Arc::clone(&writer_a), correlation_a);
        let (writer_b, task_b, _) = LockFreeStreamHandle::new(
            io_b,
            addr_a,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_b),
        );
        let writer_b = Arc::new(writer_b);
        let conn_b =
            ConnectionHandle::<()>::new_stream(addr_a, ConnectionDirection::Outbound, Arc::clone(&writer_b), correlation_b);

        let start = Arc::new(tokio::sync::Barrier::new(3));
        let start_a = Arc::clone(&start);
        let ask_a = tokio::spawn(async move {
            let payload = bytes::Bytes::from(vec![0xA5; PAYLOAD_BYTES]);
            start_a.wait().await;
            let response = conn_a
                .ask_streaming_bytes(
                    payload.clone(),
                    0xA11C_0001,
                    0xC0DE_BEEF,
                    Duration::from_secs(30),
                )
                .await?;
            crate::Result::Ok((response, payload))
        });
        let start_b = Arc::clone(&start);
        let ask_b = tokio::spawn(async move {
            let payload = bytes::Bytes::from(vec![0x5A; PAYLOAD_BYTES]);
            start_b.wait().await;
            let response = conn_b
                .ask_streaming_bytes(
                    payload.clone(),
                    0xA11C_0001,
                    0xC0DE_BEEF,
                    Duration::from_secs(30),
                )
                .await?;
            crate::Result::Ok((response, payload))
        });

        start.wait().await;
        let ((response_a, payload_a), (response_b, payload_b)) =
            tokio::time::timeout(COMPLETION_BOUND, async {
                let a = ask_a.await.expect("node A ask task panicked")?;
                let b = ask_b.await.expect("node B ask task panicked")?;
                crate::Result::Ok((a, b))
            })
            .await
            .expect(
                "simultaneous streamed asks deadlocked: each IO task must return to reads after a bounded write slice",
            )
            .expect("simultaneous streamed ask failed");

        assert_eq!(response_a, payload_a);
        assert_eq!(response_b, payload_b);

        writer_a.shutdown();
        writer_b.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(1), task_a).await;
        let _ = tokio::time::timeout(Duration::from_secs(1), task_b).await;
    });
}

/// A transport whose write side never completes: `poll_write`/`poll_flush`
/// always return `Pending` and never wake their waker. Models the residual
/// bidirectional-streaming deadlock's write side -- a peer that has stopped
/// draining its receive window, so real TCP backpressure would otherwise
/// park the single bounded slice write forever. Reads delegate straight
/// through to `inner` so the test can still feed the IO task real frames.
struct WriteWedgedStream<S> {
    inner: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteWedgedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: Unpin> AsyncWrite for WriteWedgedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// P0 (residual of #180): a partial streaming frame's bounded slice write is
/// a plain, unbounded `.await` on the shared socket. The IO task owns both
/// directions of the connection sequentially, so a write that never completes
/// -- not merely a slow one, one that genuinely never comes back -- starves
/// the read side forever along with it, including reads that have nothing to
/// do with the stuck frame. Drive the wedged side's own outbound streaming
/// ask into a permanently-`Pending` write, then prove a plain tell that
/// arrives on the same connection is still delivered: the write's deadline
/// must eventually hand control back to the loop instead of parking on it.
#[test]
fn wedged_streaming_write_does_not_stop_the_io_task_from_processing_a_buffered_read() {
    run_multi_thread_test(async {
        let addr_wedged: std::net::SocketAddr = "127.0.0.1:40495".parse().unwrap();
        let addr_peer: std::net::SocketAddr = "127.0.0.1:40496".parse().unwrap();

        let delivered = Arc::new(AtomicU64::new(0));
        let registry_wedged = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_wedged,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "wedged_write_read_progress",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        registry_wedged
            .set_actor_message_handler_sync(Arc::new(TestActorCounter {
                delivered: Arc::clone(&delivered),
            }))
            .await;

        let correlation_wedged = CorrelationTracker::new();
        let (io_wedged, io_peer) = tokio::io::duplex(64 * 1024);
        let wedged_stream = WriteWedgedStream { inner: io_wedged };

        let read_ctx_wedged = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_wedged),
            peer_addr: addr_peer,
            session_source: addr_peer,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_wedged.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_wedged.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: registry_wedged.actor_message_handler_sync.load_full(),
        };

        let (writer_wedged, task_wedged, _) = LockFreeStreamHandle::new(
            wedged_stream,
            addr_peer,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_wedged),
        );
        let writer_wedged = Arc::new(writer_wedged);
        let conn_wedged = ConnectionHandle::<()>::new_stream(
            addr_peer, ConnectionDirection::Outbound,
            Arc::clone(&writer_wedged),
            correlation_wedged,
        );

        let (writer_peer, _task_peer, _) = LockFreeStreamHandle::new(
            io_peer,
            addr_wedged,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer_peer = Arc::new(writer_peer);
        let conn_peer = ConnectionHandle::<()>::new_stream(
            addr_wedged, ConnectionDirection::Outbound,
            Arc::clone(&writer_peer),
            CorrelationTracker::new(),
        );

        // Occupy the wedged side's own streaming write path: this ask is
        // large enough to go through the chunked slice-write machinery
        // (`write_streaming_command_slice`), whose socket writes always
        // return `Pending` on `wedged_stream`. Never awaited -- it is meant
        // to hang; the assertion below is entirely about the read side.
        let stuck_payload = bytes::Bytes::from(vec![0x11u8; 3 * 1024 * 1024]);
        tokio::spawn(async move {
            let _ = conn_wedged
                .ask_streaming_bytes(
                    stuck_payload,
                    0xA11C_0001,
                    0xC0DE_BEEF,
                    Duration::from_secs(120),
                )
                .await;
        });

        // Let the wedged writer's IO task pick up the streaming ask and reach
        // (and get stuck on) its first bounded slice write.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        conn_peer
            .tell_actor_frame(
                TEST_TELL_ACTOR_ID,
                TEST_TELL_HASH,
                bytes::Bytes::from_static(b"still-alive"),
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            while delivered.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;

        assert!(
            outcome.is_ok(),
            "a permanently parked streaming slice write must not stop the IO task from \
             processing an already-buffered read: a wedged write direction must not disable \
             reads on the same connection"
        );

        writer_peer.shutdown();
        writer_wedged.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(1), task_wedged).await;
    });
}

/// A transport whose write side succeeds normally but whose flush never
/// completes: `poll_write` and `poll_read` delegate straight through to
/// `inner`, but `poll_flush` always returns `Pending` and never wakes its
/// waker. Distinct from `WriteWedgedStream` above (whose *write* side is
/// stuck): here ordinary writes complete immediately, so it is the
/// automatic post-frame flush -- not an explicit `StreamingCommand::Flush`
/// -- that gets stuck. Models a buffered/TLS transport whose `poll_flush`
/// genuinely stays `Pending` while still draining the socket, the shape
/// `bounded_stream_flush`/`STREAM_FLUSH_STUCK_TEARDOWN` exist for.
struct FlushWedgedStream<S> {
    inner: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for FlushWedgedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FlushWedgedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Review finding: the bounded-flush mechanism built for `StreamingCommand::
/// Flush` (`bounded_stream_flush`'s predecessor inside
/// `write_streaming_command_slice`, with its own `stream_flush_wedged_since`
/// wedge clock and `STREAM_FLUSH_STUCK_TEARDOWN` budget) only ever covered
/// that one call site. The automatic flushes after an ordinary
/// (non-streaming) write -- the ask-RTT fast path, `should_flush_stream_output`'s
/// throughput checkpoint, the immediate-payload flush, and the idle-branch
/// drain -- were plain, unbounded `stream.flush().await` calls. On a
/// transport whose `poll_flush` genuinely stays `Pending` while still
/// draining the socket, any one of those four call sites would park the
/// whole IO task -- and so its own read side -- forever: precisely the
/// bidirectional read/write deadlock this PR exists to close, just reached
/// through a caller the original fix never wired to
/// `STREAM_WRITE_SLICE_TIMEOUT`.
///
/// Sends a tell *from* the wedged side -- an ordinary write, not
/// `ask_streaming_bytes` (which would go through the explicit
/// `StreamingCommand::Flush` path that already worked) -- to get its own
/// automatic flush stuck, waits past `STREAM_WRITE_SLICE_TIMEOUT` so that
/// attempt has genuinely timed out at least once, then proves a tell
/// arriving *at* the wedged side is still delivered: the automatic flush's
/// deadline must hand control back to the loop instead of parking on it.
#[test]
fn wedged_automatic_flush_does_not_stop_the_io_task_from_processing_a_buffered_read() {
    run_multi_thread_test(async {
        let addr_wedged: std::net::SocketAddr = "127.0.0.1:44301".parse().unwrap();
        let addr_peer: std::net::SocketAddr = "127.0.0.1:44302".parse().unwrap();

        let delivered = Arc::new(AtomicU64::new(0));
        let registry_wedged = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_wedged,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "wedged_automatic_flush_read_progress",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        registry_wedged
            .set_actor_message_handler_sync(Arc::new(TestActorCounter {
                delivered: Arc::clone(&delivered),
            }))
            .await;

        let correlation_wedged = CorrelationTracker::new();
        let (io_wedged, io_peer) = tokio::io::duplex(64 * 1024);
        let wedged_stream = FlushWedgedStream { inner: io_wedged };

        let read_ctx_wedged = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_wedged),
            peer_addr: addr_peer,
            session_source: addr_peer,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_wedged.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_wedged.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: registry_wedged.actor_message_handler_sync.load_full(),
        };

        let (writer_wedged, task_wedged, _) = LockFreeStreamHandle::new(
            wedged_stream,
            addr_peer,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_wedged),
        );
        let writer_wedged = Arc::new(writer_wedged);
        let conn_wedged = ConnectionHandle::<()>::new_stream(
            addr_peer, ConnectionDirection::Outbound,
            Arc::clone(&writer_wedged),
            correlation_wedged,
        );

        let (writer_peer, _task_peer, _) = LockFreeStreamHandle::new(
            io_peer,
            addr_wedged,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer_peer = Arc::new(writer_peer);
        let conn_peer = ConnectionHandle::<()>::new_stream(
            addr_wedged, ConnectionDirection::Outbound,
            Arc::clone(&writer_peer),
            CorrelationTracker::new(),
        );

        // An ordinary write -- not ask_streaming_bytes -- so its eventual
        // flush goes through one of the automatic-flush call sites this
        // finding is about, never through write_streaming_command_slice's
        // explicit StreamingCommand::Flush.
        conn_wedged
            .tell_actor_frame(
                TEST_TELL_ACTOR_ID,
                TEST_TELL_HASH,
                bytes::Bytes::from_static(b"trigger-the-automatic-flush"),
            )
            .await
            .unwrap();

        // Let the wedged side's IO task write it (poll_write succeeds --
        // FlushWedgedStream delegates that straight through) and then reach,
        // and genuinely time out on, its automatic flush against
        // FlushWedgedStream's permanently-Pending poll_flush.
        tokio::time::sleep(Duration::from_millis(500)).await;

        conn_peer
            .tell_actor_frame(
                TEST_TELL_ACTOR_ID,
                TEST_TELL_HASH,
                bytes::Bytes::from_static(b"still-alive"),
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            while delivered.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;

        assert!(
            outcome.is_ok(),
            "a permanently-pending automatic (non-StreamingCommand::Flush) flush must not stop \
             the IO task from processing an already-buffered read: it must hand control back to \
             the loop on STREAM_WRITE_SLICE_TIMEOUT, the same as an explicit Flush already does"
        );

        writer_peer.shutdown();
        writer_wedged.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(1), task_wedged).await;
    });
}

/// P0 (residual of #180): once `LocalStreamingQueue::is_full()` is true (more
/// than `MAX_STREAM_SIZE` retained), the read loop's entry gate stops
/// attempting reads AT ALL for this connection -- not just admission of more
/// streaming payload, every read. If both peers in a bidirectional streaming
/// storm reach this state simultaneously, neither drains its socket, so
/// neither's TCP window reopens, and both bounded slice writes above retry
/// forever with zero progress: the write-side fix alone cannot resolve it,
/// because there is nothing left to retry into.
///
/// Push a connection's own local streaming-response queue over the
/// `is_full()` threshold with two real ask responses (one at exactly
/// `MAX_STREAM_SIZE`, one just over `STREAMING_THRESHOLD` more -- their sum
/// exceeds the ~64MiB reserve), then send a plain tell on the same connection
/// and assert its handler still runs. Ordering is driven structurally, not
/// by sleeping: `ask_streaming_bytes` is awaited to its own bounded timeout
/// fully sequentially (not concurrently), so the tell's frame cannot reach
/// the wire before ask1/ask2's do, which guarantees `is_full()` is already
/// true by the time the tell is parsed.
///
/// Deliberately a tell, not a third ask: dispatching an `ActorAsk` is itself
/// conditionally skipped while the streaming queue has no room (see
/// `ask_dispatch_is_skipped_not_consumed_when_streaming_queue_has_no_room`
/// below), so a probe ask would not distinguish "reads stopped" from
/// "reads continued but ask dispatch was correctly deferred". A tell has no
/// response to admit and is never subject to that gate, so it isolates the
/// property this test is actually about: the read loop itself keeps running.
#[test]
fn local_streaming_queue_full_does_not_stop_the_io_task_from_processing_a_later_read() {
    run_multi_thread_test(async {
        let addr_wedged: std::net::SocketAddr = "127.0.0.1:40497".parse().unwrap();
        let addr_peer: std::net::SocketAddr = "127.0.0.1:40498".parse().unwrap();

        let delivered = Arc::new(AtomicU64::new(0));
        let registry_wedged = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_wedged,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "local_streaming_queue_full_read_progress",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        registry_wedged
            .set_actor_message_handler_sync(Arc::new(EchoAskCountAll {
                delivered: Arc::clone(&delivered),
            }))
            .await;

        let correlation_wedged = CorrelationTracker::new();
        let (io_wedged, io_peer) = tokio::io::duplex(4 * 1024 * 1024);
        let wedged_stream = WriteWedgedStream { inner: io_wedged };

        let read_ctx_wedged = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_wedged),
            peer_addr: addr_peer,
            session_source: addr_peer,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_wedged.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_wedged.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: registry_wedged.actor_message_handler_sync.load_full(),
        };

        let (writer_wedged, task_wedged, _) = LockFreeStreamHandle::new(
            wedged_stream,
            addr_peer,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_wedged),
        );
        let writer_wedged = Arc::new(writer_wedged);

        let (writer_peer, _task_peer, _) = LockFreeStreamHandle::new(
            io_peer,
            addr_wedged,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer_peer = Arc::new(writer_peer);
        // All asks below are sent from `conn_peer`, whose transport
        // (`writer_peer`) writes normally onto the shared duplex; the wedged
        // side's transport (`writer_wedged`) never sends anything of its own.
        let conn_peer = ConnectionHandle::<()>::new_stream(
            addr_wedged, ConnectionDirection::Outbound,
            Arc::clone(&writer_peer),
            CorrelationTracker::new(),
        );

        // ask1: alone, exactly MAX_STREAM_SIZE -- the largest single response
        // admission allows (`admit_single_oversize`). Awaited sequentially (not
        // spawned) to its own bounded timeout: since nothing else runs
        // concurrently, ask2/ask3's frames structurally cannot reach the wire
        // before ask1's have, regardless of scheduling. The timeout always
        // elapses (wedged never responds); it exists only to bound how long
        // this step waits once sending -- which, for an in-memory duplex, is
        // far under it -- is done.
        let ask_one_payload = bytes::Bytes::from(vec![0x11u8; crate::MAX_STREAM_SIZE]);
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            conn_peer.ask_streaming_bytes(
                ask_one_payload,
                TEST_TELL_HASH,
                TEST_TELL_ACTOR_ID,
                Duration::from_secs(120),
            ),
        )
        .await;

        // ask2: on its own this response would fit the connection's normal
        // reserve, but stacked on ask1's already-retained MAX_STREAM_SIZE it
        // pushes retained_bytes() past the is_full() threshold (~MAX_STREAM_SIZE).
        let ask_two_payload = bytes::Bytes::from(vec![0x22u8; STREAMING_THRESHOLD + 1_048_576]);
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            conn_peer.ask_streaming_bytes(
                ask_two_payload,
                TEST_TELL_HASH,
                TEST_TELL_ACTOR_ID,
                Duration::from_secs(120),
            ),
        )
        .await;

        // A plain tell, sent last and only after ask1/ask2 have each had
        // their full bounded window to reach the wire: a pure read-progress
        // probe with no response to admit, so nothing about it is subject to
        // the ask-dispatch gate.
        conn_peer
            .tell_actor_frame(
                TEST_TELL_ACTOR_ID,
                TEST_TELL_HASH,
                bytes::Bytes::from_static(b"still-alive"),
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            while delivered.load(Ordering::Acquire) < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;

        assert!(
            outcome.is_ok(),
            "local_streaming_queue.is_full() must not stop the IO task from reading a later, \
             unrelated tell on the same connection: delivered={}",
            delivered.load(Ordering::Acquire)
        );

        writer_peer.shutdown();
        writer_wedged.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(1), task_wedged).await;
    });
}

/// P1 (residual of the two fixes above): removing `is_full()` from the read
/// loop's entry gate must not turn into "dispatch the ask handler and then
/// discard whatever it produces". The handler runs before
/// `can_admit_response` is ever consulted, so if dispatch is unconditional,
/// an ask that arrives once the streaming queue and its deferred slot are
/// both already full is consumed, its response computed, admission fails,
/// and the answer is thrown away -- the exact silent drop documented at
/// `queue_streaming_response_bytes`/`_pooled`, now happening systematically
/// instead of only at a tight byte-cap race. The fix answers with an
/// `AskNackReason::Backpressure` NACK instead of dropping silently.
///
/// Fill the connection's own retained streaming-response backlog past
/// `is_full()` (same construction as the test above: one response at exactly
/// `MAX_STREAM_SIZE`, one more just over `STREAMING_THRESHOLD`), then send a
/// third, large ask into that saturated state and assert its handler never
/// runs at all (`delivered` must not advance past 2) -- not "ran and its
/// answer vanished". A plain tell sent immediately after must still be
/// dispatched (`delivered` reaches 3), proving the connection did not fall
/// back to blocking all reads to get there.
///
/// This connection's write side (`WriteWedgedStream`) never completes any
/// write, which is exactly what keeps `is_full()` true for the whole test
/// (see the wedged-write test above) -- but it also means the NACK this test
/// exists to prove out is, on this specific transport, physically
/// undeliverable: not one byte can reach the peer. `ask3` can therefore only
/// be observed to fail, not to fail with `AskNacked(Backpressure)`
/// specifically; the wire-level content of the NACK is covered separately by
/// `try_write_ask_backpressure_nack_writes_a_decodable_nack_on_a_healthy_stream`,
/// against a real, non-wedged transport. What this test adds beyond the "not
/// consumed" property above is that attempting the NACK does not itself park
/// the IO task: if it did, the tell right after would never arrive either.
#[test]
fn ask_dispatch_is_skipped_not_consumed_when_streaming_queue_has_no_room() {
    run_multi_thread_test(async {
        let addr_wedged: std::net::SocketAddr = "127.0.0.1:40499".parse().unwrap();
        let addr_peer: std::net::SocketAddr = "127.0.0.1:40500".parse().unwrap();

        let delivered = Arc::new(AtomicU64::new(0));
        let registry_wedged = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_wedged,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ask_dispatch_skipped_not_dropped",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        registry_wedged
            .set_actor_message_handler_sync(Arc::new(EchoAskCountAll {
                delivered: Arc::clone(&delivered),
            }))
            .await;

        let correlation_wedged = CorrelationTracker::new();
        let (io_wedged, io_peer) = tokio::io::duplex(4 * 1024 * 1024);
        let wedged_stream = WriteWedgedStream { inner: io_wedged };

        let read_ctx_wedged = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_wedged),
            peer_addr: addr_peer,
            session_source: addr_peer,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_wedged.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_wedged.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: registry_wedged.actor_message_handler_sync.load_full(),
        };

        let (writer_wedged, task_wedged, _) = LockFreeStreamHandle::new(
            wedged_stream,
            addr_peer,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_wedged),
        );
        let writer_wedged = Arc::new(writer_wedged);

        let (writer_peer, _task_peer, _) = LockFreeStreamHandle::new(
            io_peer,
            addr_wedged,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer_peer = Arc::new(writer_peer);
        let conn_peer = ConnectionHandle::<()>::new_stream(
            addr_wedged, ConnectionDirection::Outbound,
            Arc::clone(&writer_peer),
            CorrelationTracker::new(),
        );

        // ask1: alone, exactly MAX_STREAM_SIZE (admit_single_oversize). delivered -> 1.
        let ask_one_payload = bytes::Bytes::from(vec![0x11u8; crate::MAX_STREAM_SIZE]);
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            conn_peer.ask_streaming_bytes(
                ask_one_payload,
                TEST_TELL_HASH,
                TEST_TELL_ACTOR_ID,
                Duration::from_secs(120),
            ),
        )
        .await;

        // ask2: pushes retained_bytes() past the is_full() threshold via the
        // deferred-response slot. delivered -> 2. The streaming queue and its
        // deferred slot are now both occupied -- exactly the precondition
        // this test targets.
        let ask_two_payload = bytes::Bytes::from(vec![0x22u8; STREAMING_THRESHOLD + 1_048_576]);
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            conn_peer.ask_streaming_bytes(
                ask_two_payload,
                TEST_TELL_HASH,
                TEST_TELL_ACTOR_ID,
                Duration::from_secs(120),
            ),
        )
        .await;

        // ask3: another large ask (its response, if computed, would need
        // streaming admission) arriving while the queue has zero room. Must
        // not be dispatched: `delivered` must not advance past 2 for this.
        // The IO task now attempts an `AskNackReason::Backpressure` NACK
        // instead of silently dropping it -- but on this specific transport
        // (`WriteWedgedStream`, never completes any write) not one byte of
        // that NACK can physically reach the peer, so the strongest honest
        // assertion here is that ask3 never resolves to a fabricated
        // success. The wire content of the NACK itself is proven on a real
        // transport by
        // `try_write_ask_backpressure_nack_writes_a_decodable_nack_on_a_healthy_stream`.
        let ask_three_payload = bytes::Bytes::from(vec![0x33u8; STREAMING_THRESHOLD + 4096]);
        let ask_three_result = tokio::time::timeout(
            Duration::from_secs(3),
            conn_peer.ask_streaming_bytes(
                ask_three_payload,
                TEST_TELL_HASH,
                TEST_TELL_ACTOR_ID,
                Duration::from_secs(120),
            ),
        )
        .await;
        assert!(
            !matches!(ask_three_result, Ok(Ok(_))),
            "ask3 must never resolve to a fabricated success: {ask_three_result:?}"
        );

        // A plain tell right behind it must still be dispatched: reads (and
        // dispatch of messages that need no streaming admission) must keep
        // flowing past the skipped ask, not stall behind it.
        conn_peer
            .tell_actor_frame(
                TEST_TELL_ACTOR_ID,
                TEST_TELL_HASH,
                bytes::Bytes::from_static(b"still-alive"),
            )
            .await
            .unwrap();

        let reached_three = tokio::time::timeout(Duration::from_secs(5), async {
            while delivered.load(Ordering::Acquire) < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            reached_three.is_ok(),
            "the tell must still be dispatched after the skipped ask: delivered={}",
            delivered.load(Ordering::Acquire)
        );

        // Give any (incorrect) delayed/duplicate dispatch of ask3 a generous
        // window to show up before asserting it never does.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            delivered.load(Ordering::Acquire),
            3,
            "ask3's handler must never run while the streaming queue has no room for its \
             response -- consuming it and then discarding the computed answer is exactly the \
             silent drop this test guards against"
        );

        writer_peer.shutdown();
        writer_wedged.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(1), task_wedged).await;
    });
}

// `write_ask_nack_header_bounded`'s own wire-content unit test
// (`write_ask_nack_header_bounded_writes_a_decodable_nack_on_a_healthy_stream`)
// and the `LocalStreamingQueue::queue_ask_nack`/`drain_pending_ask_nacks`
// queuing machinery it's called through now live on
// `feat/wire-batch-nack-and-capabilities` (#185), which this branch is
// stacked on -- see that PR for the function and its test.
// `ask_dispatch_is_skipped_not_consumed_when_streaming_queue_has_no_room`
// above still proves this branch's own property: the pre-dispatch gate
// answers with that NACK instead of consuming and dropping the ask. The
// test below proves the property #185 could not: that the queued NACK
// genuinely waits for a partially-written frame to finish, since only this
// branch keeps reads flowing while a write is stuck.

/// A transport that lets exactly `budget` (an externally adjustable, shared
/// counter) bytes through in total across all `poll_write` calls, then
/// returns `Pending` for anything beyond it. Models a peer whose receive
/// window stalls partway through a frame and later reopens: deterministic
/// (no real timing/socket race), so a test can raise the budget on its own
/// schedule and observe exactly what was written before and after. Unlike
/// `WriteWedgedStream` above (which never needs to resume and so never
/// wakes), this stores the waker and calls it via `raise_budget_and_wake`.
struct StallAfterNBytesStream<S> {
    inner: S,
    budget: Arc<AtomicUsize>,
    written: usize,
    waker: Arc<Mutex<Option<std::task::Waker>>>,
}

fn raise_budget_and_wake(budget: &Arc<AtomicUsize>, waker: &Arc<Mutex<Option<std::task::Waker>>>) {
    budget.store(usize::MAX, Ordering::Release);
    if let Some(w) = waker.lock().unwrap().take() {
        w.wake();
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for StallAfterNBytesStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for StallAfterNBytesStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let budget = this.budget.load(Ordering::Acquire);
        if this.written >= budget || buf.is_empty() {
            *this.waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let allowed = (budget - this.written).min(buf.len());
        match Pin::new(&mut this.inner).poll_write(cx, &buf[..allowed]) {
            Poll::Ready(Ok(n)) => {
                this.written += n;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// A queued backpressure NACK must never be attempted while a streaming
/// frame is only *partially* written -- exactly the shape a NACK "between
/// frames" cannot exercise, since that shape would pass whether or not the
/// frame-boundary guard exists at all. Stalls ask1's response mid-payload
/// (well past its much smaller V5 stream header, proven by `bytes_written()`
/// plateauing at the stall budget), drives a second ask into the deferred
/// slot (making `is_full()` true) and a third into the pre-dispatch gate
/// while ask1 sits stuck there, and asserts nothing more is written until
/// the stall lifts -- this branch's own read loop keeps ask2/ask3 flowing
/// through that whole window, unlike #185 alone. Once the stall lifts,
/// ask1's payload must still reassemble byte-for-byte (a splice would
/// corrupt V5 stream framing enough that it could not), and the queued NACK
/// must still reach the peer correctly right after.
///
/// Confirmed red by reverting the fix at this call site to a direct
/// `write_ask_nack_header_bounded` call (bypassing `queue_ask_nack`): under
/// this exact construction the direct write competes for the same stalled
/// transport and is abandoned rather than landing mid-frame, so the
/// observed failure is ask3 never resolving (the silent-drop symptom this
/// mechanism exists to remove) rather than a raw corrupted-bytes assertion
/// -- but it is a failure precisely because the old code still attempts a
/// direct write while genuinely mid-frame, which is the property this test
/// exists to rule out. A transport with real room to let that write
/// through -- the realistic case -- would show the same unconditional
/// attempt as actual byte splicing instead.
#[test]
fn ask_backpressure_nack_never_splices_into_an_in_flight_streaming_frame() {
    run_multi_thread_test(async {
        const ASK1_LEN: usize = 4 * 1024 * 1024;
        const ASK2_LEN: usize = 5 * 1024 * 1024;
        const ASK3_LEN: usize = 2 * 1024 * 1024;
        const STALL_BUDGET: usize = 200;

        let addr_wedged: std::net::SocketAddr = "127.0.0.1:40523".parse().unwrap();
        let addr_peer: std::net::SocketAddr = "127.0.0.1:40524".parse().unwrap();

        let registry_wedged = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_wedged,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ask_nack_never_splices_mid_frame",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        registry_wedged
            .set_actor_message_handler_sync(Arc::new(TestActor))
            .await;

        let correlation_wedged = CorrelationTracker::new();
        let (io_wedged, io_peer) = tokio::io::duplex(16 * 1024 * 1024);
        let budget = Arc::new(AtomicUsize::new(STALL_BUDGET));
        let stall_waker: Arc<Mutex<Option<std::task::Waker>>> = Arc::new(Mutex::new(None));
        let stalled_stream = StallAfterNBytesStream {
            inner: io_wedged,
            budget: Arc::clone(&budget),
            written: 0,
            waker: Arc::clone(&stall_waker),
        };

        let read_ctx_wedged = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_wedged),
            peer_addr: addr_peer,
            session_source: addr_peer,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_wedged.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_wedged.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: registry_wedged.actor_message_handler_sync.load_full(),
        };

        let (writer_wedged, task_wedged, _) = LockFreeStreamHandle::new(
            stalled_stream,
            addr_peer,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_wedged),
        );
        let writer_wedged = Arc::new(writer_wedged);

        // Unlike the wedged-transport tests above, this test needs the
        // client to actually receive and reassemble a real response (ask1's
        // echo), not just observe a timeout -- so, unlike those, its read
        // side needs a `ReadContext` wiring `response_correlation` to the
        // same tracker `ConnectionHandle` registers waiters on. It also
        // needs a *real* registry: `process_read_result_io` requires
        // `registry_weak.upgrade()` to succeed for every incoming frame,
        // including plain responses, and kills the IO task outright if it
        // does not -- a dangling `Weak::new()` is only safe for lower-level
        // tests that never reach that dispatch path.
        let registry_peer = Arc::new(crate::registry::GossipRegistry::<()>::new(
            addr_peer,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ask_nack_never_splices_mid_frame_peer",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation_peer = CorrelationTracker::new();
        let read_ctx_peer = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry_peer),
            peer_addr: addr_wedged,
            session_source: addr_wedged,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry_peer.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: Some(correlation_peer.clone()),
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (writer_peer, _task_peer, _) = LockFreeStreamHandle::new(
            io_peer,
            addr_wedged,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(read_ctx_peer),
        );
        let writer_peer = Arc::new(writer_peer);
        let conn_peer = ConnectionHandle::<()>::new_stream(
            addr_wedged, ConnectionDirection::Outbound,
            Arc::clone(&writer_peer),
            correlation_peer,
        );

        // ask1: echoed back by `TestActor`. Its response write starts, then
        // stalls at `STALL_BUDGET` bytes -- deep into the frame's payload,
        // well past its (much smaller) V5 stream header.
        let ask1_payload = bytes::Bytes::from(vec![0xA1u8; ASK1_LEN]);
        let ask1_task = tokio::spawn({
            let conn_peer = conn_peer.clone();
            let ask1_payload = ask1_payload.clone();
            async move {
                conn_peer
                    .ask_streaming_bytes(
                        ask1_payload,
                        TEST_TELL_HASH,
                        TEST_TELL_ACTOR_ID,
                        Duration::from_secs(10),
                    )
                    .await
            }
        });

        // Wait for the stall to actually bite: `bytes_written()` must reach
        // the budget (ask1 is underway) and then hold there.
        let reached_stall = tokio::time::timeout(Duration::from_secs(3), async {
            while writer_wedged.bytes_written() < STALL_BUDGET {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            reached_stall.is_ok(),
            "ask1's response write must reach the stall budget: bytes_written={}",
            writer_wedged.bytes_written()
        );

        // ask2: fills the deferred slot (declined `fits_queue` while ask1 is
        // in flight and not yet fully written, but small enough relative to
        // the hard cap to defer). This alone makes `is_full()` true. Reads
        // keep flowing on this branch, so this is read and dispatched
        // (and its response queued) despite ask1's frame still being stuck.
        let ask2_payload = bytes::Bytes::from(vec![0xA2u8; ASK2_LEN]);
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            conn_peer.ask_streaming_bytes(
                ask2_payload,
                TEST_TELL_HASH,
                TEST_TELL_ACTOR_ID,
                Duration::from_secs(30),
            ),
        )
        .await;

        // ask3: arrives once the deferred slot is occupied, so the
        // pre-dispatch `is_full()` gate queues a Backpressure NACK for it
        // instead of dispatching -- but must not write that NACK while
        // ask1's frame still owns the wire.
        let ask3_payload = bytes::Bytes::from(vec![0xA3u8; ASK3_LEN]);
        let ask3_task = tokio::spawn({
            let conn_peer = conn_peer.clone();
            async move {
                conn_peer
                    .ask_streaming_bytes(
                        ask3_payload,
                        TEST_TELL_HASH,
                        TEST_TELL_ACTOR_ID,
                        Duration::from_secs(10),
                    )
                    .await
            }
        });

        // Give ask3 ample turns to be read, gated, and its NACK queued --
        // then assert nothing beyond the stall budget has reached the wire.
        // If the NACK (or anything else) had been spliced into ask1's
        // in-flight frame, this would already have grown.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            writer_wedged.bytes_written(),
            STALL_BUDGET,
            "no bytes may be written while ask1's streaming frame is only partially sent -- a \
             queued NACK must wait, not splice in"
        );

        // Lift the stall: ask1's frame completes, then (only once
        // `pending_stream_cmd` is `None` again) the queued NACK drains.
        raise_budget_and_wake(&budget, &stall_waker);

        let ask1_result = tokio::time::timeout(Duration::from_secs(5), ask1_task)
            .await
            .expect("ask1 must complete once the stall lifts")
            .expect("ask1 task must not panic");
        assert_eq!(
            ask1_result.expect("ask1 must succeed"),
            ask1_payload,
            "ask1's reassembled payload must be byte-for-byte correct -- a NACK spliced into \
             its in-flight frame would corrupt V5 stream framing enough that it could not be"
        );

        let ask3_result = tokio::time::timeout(Duration::from_secs(5), ask3_task)
            .await
            .expect("ask3 must resolve once the stall lifts and the queued NACK can drain")
            .expect("ask3 task must not panic");
        match ask3_result {
            Err(crate::GossipError::AskNacked(reason)) => {
                assert_eq!(reason, crate::framing::AskNackReason::Backpressure);
            }
            other => panic!(
                "ask3 must resolve to AskNacked(Backpressure) once its queued NACK drains \
                 cleanly after ask1's frame completes: {other:?}"
            ),
        }

        writer_peer.shutdown();
        writer_wedged.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(1), task_wedged).await;
    });
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
fn resolve_connection_conflict_uses_identity_direction_then_session_epoch() {
    use super::ConnectionConflictDecision::*;
    // No live rival (stale/dead entry, or none at all) and incoming is
    // identity-preferred -> take the incoming.
    assert_eq!(
        resolve_connection_conflict(false, true, true, false),
        AcceptIncoming
    );
    assert_eq!(
        resolve_connection_conflict(false, false, true, false),
        AcceptIncoming
    );
    // No live rival, but incoming is *not* identity-preferred either -> evict
    // the stale rival, but do not accept the incoming as the session.
    assert_eq!(
        resolve_connection_conflict(false, true, false, false),
        EvictStaleRejectIncoming
    );
    assert_eq!(
        resolve_connection_conflict(false, false, false, false),
        EvictStaleRejectIncoming
    );
    // Live rival the tie-break prefers, incoming not preferred -> keep rival.
    assert_eq!(
        resolve_connection_conflict(true, true, false, true),
        RejectIncoming
    );
    // Live rival, tie-break does not prefer it, incoming preferred -> replace.
    assert_eq!(
        resolve_connection_conflict(true, false, true, false),
        ReplaceExisting
    );
    // Both sessions use the preferred direction. The later authenticated
    // session replaces the incumbent; an older candidate does not.
    assert_eq!(
        resolve_connection_conflict(true, true, true, true),
        ReplaceExisting
    );
    assert_eq!(
        resolve_connection_conflict(true, true, true, false),
        RejectIncoming
    );
    // Neither side is strictly preferred and the rival is live (degenerate
    // input; not reachable via `should_keep_connection` in practice, but the
    // function must still resolve it deterministically) -> keep the rival.
    assert_eq!(
        resolve_connection_conflict(true, false, false, true),
        RejectIncoming
    );
    // The decision signature carries no SocketAddr: the structural invariant
    // that a keep/drop outcome can never depend on a peer's address, only on
    // its verified identity, direction, and local session epoch.
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
        resolve_connection_conflict(true, false, true, false),
        ReplaceExisting
    );
    // Existing usable and preferred. An older concurrent candidate is
    // rejected; a later authenticated session supersedes it.
    assert_eq!(
        resolve_connection_conflict(true, true, true, false),
        RejectIncoming
    );
    assert_eq!(
        resolve_connection_conflict(true, true, true, true),
        ReplaceExisting
    );
    // existing stale, incoming (outbound) not preferred (higher-NodeId
    // fallback dial) -> evict stale, do not publish ("outbound finalize
    // evicted a stale rival but declined to publish...").
    assert_eq!(
        resolve_connection_conflict(false, false, false, false),
        EvictStaleRejectIncoming
    );
    // existing stale, incoming (outbound) preferred -> accept.
    assert_eq!(
        resolve_connection_conflict(false, false, true, false),
        AcceptIncoming
    );

    // --- Inbound accept (handle.rs, handle_incoming_connection_tls) ---
    // keep_incoming = should_keep_connection(peer, false) (a freshly accepted
    // inbound socket already exists). The "no existing at all" fast path is
    // an explicitly-documented exception and is not exercised here.
    // existing stale, new inbound preferred -> accept ("inbound_tiebreak_evict_stale"
    // + "inbound_connection_accepted").
    assert_eq!(
        resolve_connection_conflict(false, false, true, false),
        AcceptIncoming
    );
    // existing stale, new inbound NOT preferred -> evict stale, reject
    // ("inbound_tiebreak_evict_stale" + "inbound_tiebreak_reject_non_preferred_inbound").
    assert_eq!(
        resolve_connection_conflict(false, false, false, false),
        EvictStaleRejectIncoming
    );
    // existing usable, wrong direction, new inbound preferred -> replace
    // ("inbound_tiebreak_replace_wrong_direction").
    assert_eq!(
        resolve_connection_conflict(true, false, true, false),
        ReplaceExisting
    );
    // Existing usable and preferred. A non-preferred inbound remains rejected.
    assert_eq!(
        resolve_connection_conflict(true, true, false, true),
        RejectIncoming
    );
    // A later authenticated inbound in the preferred direction replaces the
    // incumbent; an older one remains rejected.
    assert_eq!(
        resolve_connection_conflict(true, true, true, true),
        ReplaceExisting
    );
    assert_eq!(
        resolve_connection_conflict(true, true, true, false),
        RejectIncoming
    );

    // --- Outbound top-of-dial, stale-rival branch only (transport_stream.rs,
    // connect_via_stream, the `!alive` arm) ---
    // Both outcomes this site can receive when the rival is stale lead to the
    // identical action there (evict); pinned here so a future change cannot
    // silently make the stale branch stop evicting for either outcome.
    for keep_incoming in [true, false] {
        let decision = resolve_connection_conflict(false, false, keep_incoming, false);
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

#[tokio::test]
async fn authenticated_boot_id_separates_socket_dedup_from_process_clone() {
    use super::ConnectionConflictDecision::*;

    fn live_connection(
        addr: SocketAddr,
        boot_byte: u8,
    ) -> (Arc<LockFreeConnection>, tokio::io::DuplexStream) {
        let (io, peer_io) = tokio::io::duplex(1024);
        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut connection = LockFreeConnection::new(addr, ConnectionDirection::Inbound);
        connection.stream_handle = Some(Arc::new(stream_handle));
        connection.remote_boot_id =
            Some(crate::handshake::RemoteBootId::from_bytes([boot_byte; 16]));
        connection.set_state(ConnectionState::Connected);
        (Arc::new(connection), peer_io)
    }

    let (incumbent, _incumbent_io) = live_connection("127.0.0.1:40550".parse().unwrap(), 1);
    let (same_process_socket, _same_process_io) =
        live_connection("127.0.0.1:40551".parse().unwrap(), 1);
    let (process_clone, _process_clone_io) = live_connection("127.0.0.1:40552".parse().unwrap(), 2);

    assert_eq!(
        resolve_authenticated_connection_conflict(
            &incumbent,
            &same_process_socket,
            true,
            true,
            true,
        ),
        ReplaceExisting,
        "later sockets from one process retain the existing session-epoch tie-break"
    );
    assert_eq!(
        resolve_authenticated_connection_conflict(&incumbent, &process_clone, false, true, true,),
        RejectIncoming,
        "a different live process must not replace the incumbent even when its socket is newer"
    );

    incumbent.set_state(ConnectionState::Disconnected);
    assert_eq!(
        resolve_authenticated_connection_conflict(&incumbent, &process_clone, false, true, true,),
        AcceptIncoming,
        "once the incumbent is dead, the next authenticated process may take over"
    );
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

/// Earlier versions of `configure_peer`'s follow-up
/// checked the pin, then separately (even if compare-and-applied against
/// a dedicated mirror updated in the same owner command) mutated
/// `ConnectionPool`'s index. Neither was actually atomic WITH the owner's
/// commands: the owner runs as its own task, and `ConnectionPool`'s maps
/// are not protected by one lock spanning a whole owner command, so a
/// caller-side read-then-mutate pair can still straddle a DIFFERENT owner
/// command's commit and publish a losing alias that no later pin check
/// can retract (`connections_by_addr` aliases are never un-published just
/// because a pin moved).
///
/// Reconstructs that exact shape directly against the primitives (the
/// production caller-side check-then-mutate this demonstrates no longer
/// exists at all -- see `RoutingPublisher::set_configured_peer_addr`'s doc
/// comment): a "check" read of the pin passes, then a genuine pin move
/// (mirroring what an interleaved owner `configure_peer` command does)
/// lands, then the stale "mutate" proceeds anyway using the
/// already-invalidated check. The losing address ends up reachable via
/// `connections_by_addr` regardless.
#[tokio::test]
async fn a_caller_side_check_then_mutate_reindex_is_vulnerable_to_an_interleaved_pin_move() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let peer_id = crate::KeyPair::new_for_testing("pin-races-reindex").peer_id();
    // The connection's own raw (e.g. ephemeral inbound source) address --
    // distinct from either candidate advertised address below, mirroring
    // the real "reindex under the advertised bind address" use case. Kept
    // separate so `has_connection(&losing_addr)` below can only become
    // true via the reindex this test is about, not via the connection's
    // own initial placement.
    let raw_addr: SocketAddr = "127.0.0.1:40559".parse().unwrap();
    let losing_addr: SocketAddr = "127.0.0.1:40560".parse().unwrap();
    let winning_addr: SocketAddr = "127.0.0.1:40561".parse().unwrap();

    let connection = qa_r11_generation_race_connection(raw_addr);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), raw_addr, connection));
    assert!(!pool.has_connection(&losing_addr), "precondition");

    // The "check": the pin is losing_addr right now -- general ownership
    // agrees too, exactly as it would immediately after a real
    // `configure_peer(peer_id, losing_addr)` commit and BEFORE any later
    // command's eviction has retracted it.
    let _ = pool.addr_to_peer_id.upsert_sync(losing_addr, peer_id.clone());
    pool.set_configured_peer_addr(&peer_id, losing_addr);
    assert_eq!(pool.get_required_peer_addr(&peer_id), Some(losing_addr));

    // An owner command interleaves BETWEEN the check and the mutate below,
    // moving the pin to winning_addr -- exactly what a concurrent
    // configure_peer/migrate does. Mirrors `install_pin`'s own ordering:
    // the `ConnectionPool` mirror for the NEW pin is updated before the
    // OLD address's ownership is retracted, so `addr_to_peer_id[losing_addr]`
    // deliberately still shows `peer_id` here, unretracted -- the exact
    // window the coordinator's ordering question asked about.
    pool.set_configured_peer_addr(&peer_id, winning_addr);

    // The stale "mutate" proceeds anyway, using the check from before the
    // interleaved move -- exactly what a caller-side compare-and-apply
    // amounts to once its own read is separated from the mutation by
    // ANYTHING that isn't the owner's own serialization.
    pool.reindex_connection_addr(&peer_id, losing_addr);

    assert!(
        pool.has_connection(&losing_addr),
        "the losing address ends up reachable via connections_by_addr anyway -- proving \
         a caller-side check-then-mutate, however tight, is not the same as one atomic \
         step with respect to owner commands"
    );
}

/// The fix: the reindex now happens synchronously INSIDE the owner's own
/// `configure_peer` command (`RoutingPublisher::set_configured_peer_addr`,
/// called from `PeerRegistryOwner::install_pin`), so there is no
/// caller-side check-then-mutate left to race at all. Proves it with a
/// genuine concurrent race through the real production path: two
/// `configure_peer` calls for the SAME peer, different addresses, fired
/// at the same time. Regardless of which the owner actually serializes
/// first (and last -- the pin, and therefore the reindex, belongs to
/// whichever command the owner processes LAST), the connection must end
/// up reachable at the address that currently wins the pin.
///
/// Deliberately does NOT assert the loser's address is unreachable:
/// `connections_by_addr` aliases are never removed just because a pin
/// later moved away from them (see `reindex_connection_addr`'s "both
/// addresses are valid for this peer" comment) -- that is a separate,
/// pre-existing, intentional property unrelated to this fix, and an
/// address that validly won the pin at the time ITS OWN command ran is
/// correctly reindexed then, even though a later command goes on to evict
/// it. What this fix rules out is a DIFFERENT thing: a reindex for an
/// address that ALREADY lost the pin race before its own reindex call
/// ever ran, which the scratch reconstruction above proves was possible
/// under the old caller-side shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_configure_peer_calls_always_reindex_the_current_pin_winner() {
    use std::sync::Arc;

    for round in 0..20 {
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            format!("127.0.0.1:{}", 41_000 + round).parse().unwrap(),
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(format!(
                    "concurrent-configure-local-{round}"
                ))),
                ..crate::GossipConfig::default()
            },
        ));
        let peer_id = crate::KeyPair::new_for_testing(format!("concurrent-configure-{round}"))
            .peer_id();
        let addr_a: SocketAddr = format!("127.0.0.1:{}", 41_100 + round).parse().unwrap();
        let addr_b: SocketAddr = format!("127.0.0.1:{}", 41_200 + round).parse().unwrap();

        let raw_addr: SocketAddr = format!("127.0.0.1:{}", 41_300 + round).parse().unwrap();
        let connection = qa_r11_generation_race_connection(raw_addr);
        assert!(registry.connection_pool.add_connection_by_peer_id(
            peer_id.clone(),
            raw_addr,
            connection
        ));

        let (r1, r2) = (registry.clone(), registry.clone());
        let (p1, p2) = (peer_id.clone(), peer_id.clone());
        let call_a = tokio::spawn(async move { r1.configure_peer(p1, addr_a).await });
        let call_b = tokio::spawn(async move { r2.configure_peer(p2, addr_b).await });
        call_a.await.expect("call_a task panicked");
        call_b.await.expect("call_b task panicked");

        let owner = &registry.registry_owner;
        let winner = match (owner.routes_to(&addr_a), owner.routes_to(&addr_b)) {
            (Some(p), None) if p == peer_id => addr_a,
            (None, Some(p)) if p == peer_id => addr_b,
            other => panic!("round {round}: expected exactly one address to win, got {other:?}"),
        };

        assert!(
            registry.connection_pool.has_connection(&winner),
            "round {round}: the connection must be reachable at the current pin winner \
             {winner} -- the reindex for whichever command the owner processed LAST must \
             have run, using that command's own, correct pin decision"
        );
    }
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
async fn finalize_binds_cert_identity_over_stale_addr_map_on_rekey() {
    // Rekey/restart: a peer B was previously at `addr`, so the addr->peer map
    // still points at B. The peer that ACTUALLY answers now is A (new identity
    // at the same address), proven by A's TLS certificate (`tofu_node_id`). The
    // finalized connection must bind A — the cert-verified identity — not the
    // stale cached B, otherwise the per-message identity guard black-holes every
    // frame A sends.
    use crate::{GossipConfig, registry::GossipRegistry};
    // A real (if otherwise unused) registry: finalize must actually be able
    // to send its identifying FullSync, or it now fails the connect outright
    // rather than silently publishing an unidentified candidate.
    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("rekey-test-local")),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();
    let addr: SocketAddr = "127.0.0.1:40611".parse().unwrap();

    let stale_b = crate::KeyPair::new_for_testing("rekey_stale_peer_b").peer_id();
    pool.add_addr_to_peer_id(addr, stale_b.clone());

    let new_a = crate::KeyPair::new_for_testing("rekey_new_peer_a").peer_id();
    let new_a_node_id = new_a.to_node_id();

    let (io, _peer_io) = tokio::io::duplex(1024);
    let _handle = pool
        .finalize_new_outbound_connection(
            addr,
            io,
            Arc::downgrade(&registry),
            Some(new_a_node_id),
            addr,
            None,
        )
        .await
        .expect("finalize outbound connection");

    let conn = pool
        .get_connection_by_addr(&addr)
        .expect("finalized connection is indexed by addr");
    assert_eq!(
        conn.embedded_peer_id.as_ref(),
        Some(&new_a),
        "cert-verified TOFU identity must take precedence over the stale addr->peer map"
    );
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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

/// Review finding (`read_pipeline.rs:2315`): `try_handle_fast_io`'s split
/// ask fast paths (`ask_immediate_handler_sync`/`ask_handler_sync`) used
/// `?` on a handler error, letting it escape the function entirely instead
/// of converting it to a NACK the way the legacy `sync_actor_handler` path
/// already did. The escaped error is only logged by the io_task caller in
/// `stream_writer.rs`, which has already consumed the ask off the wire --
/// so the requester timed out instead of receiving
/// `AskNackReason::HandlerError`. The invariant is: every ask either gets
/// an answer or an explicit NACK. This covers the `ask_immediate_handler_sync`
/// site; see `ask_handler_sync_error_nacks_instead_of_letting_the_asker_time_out`
/// for the `ask_handler_sync` (deferred-context) site.
#[test]
fn ask_immediate_handler_sync_error_nacks_instead_of_letting_the_asker_time_out() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:40571".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:40572".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ask_immediate_handler_sync_error_nacks_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_ask_immediate_handler_sync(Arc::new(ErroringImmediateAskActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ask_immediate_handler_sync_error_nacks_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: None,
            response_writer: Some(response_writer.clone()),
            tell_handler_sync: server_registry.actor_tell_handler_sync.load_full(),
            tell_handler_sync_context: server_registry.actor_tell_handler_sync_context.load_full(),
            ask_immediate_handler_sync: server_registry
                .actor_ask_immediate_handler_sync
                .load_full(),
            ask_handler_sync: None,
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

        let payload = bytes::Bytes::from_static(b"trigger-handler-error");
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client_conn.ask_actor_frame_no_timeout(0xE440_0001, 0xE440_0002, payload),
        )
        .await
        .expect("the ask must resolve (NACK or reply), not time out");

        match result {
            Err(crate::GossipError::AskNacked(reason)) => {
                assert_eq!(
                    reason,
                    crate::framing::AskNackReason::HandlerError,
                    "a handler error must NACK as HandlerError, got {reason:?}"
                );
            }
            other => panic!(
                "an error from ask_immediate_handler_sync must NACK the asker, not escape \
                 silently: {other:?}"
            ),
        }

        client_writer.shutdown();
        server_writer.shutdown();
    });
}

/// See `ask_immediate_handler_sync_error_nacks_instead_of_letting_the_asker_time_out`'s doc --
/// same finding, covering the `ask_handler_sync` (deferred-context) site.
#[test]
fn ask_handler_sync_error_nacks_instead_of_letting_the_asker_time_out() {
    run_multi_thread_test(async {
        let server_addr: std::net::SocketAddr = "127.0.0.1:40573".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:40574".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ask_handler_sync_error_nacks_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_ask_handler_sync(Arc::new(ErroringDeferredAskActor))
            .await;

        let client_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            client_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ask_handler_sync_error_nacks_client",
                )),
                ..crate::GossipConfig::default()
            },
        ));
        let correlation = CorrelationTracker::new();

        let (client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let client_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        // `ask_handler_sync` dispatch needs `ctx.response_writer` (see
        // `ask_context_from_context`), same as the deferred-reply test above.
        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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

        let payload = bytes::Bytes::from_static(b"trigger-handler-error");
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client_conn.ask_actor_frame_no_timeout(0xE441_0001, 0xE441_0002, payload),
        )
        .await
        .expect("the ask must resolve (NACK or reply), not time out");

        match result {
            Err(crate::GossipError::AskNacked(reason)) => {
                assert_eq!(
                    reason,
                    crate::framing::AskNackReason::HandlerError,
                    "a handler error must NACK as HandlerError, got {reason:?}"
                );
            }
            other => panic!(
                "an error from ask_handler_sync must NACK the asker, not escape silently: {other:?}"
            ),
        }

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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        let response_writer = Arc::new(crate::ask_responder::ResponseWriter::new(client_addr));
        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: Some(peer_id.clone()),
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: Some(client_registry.peer_id.clone()),
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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

#[tokio::test]
async fn streaming_slice_progress_notifies_shared_capacity_exactly_once() {
    let (mut writer, recorded) = RecordingWriter::new();
    let queue = StreamingQueue::new(1, "127.0.0.1:40493".parse().unwrap());
    let mut yielded_slot = None;
    let header =
        crate::framing::try_write_stream_data_header(false, 7, 1, STREAM_WRITE_SLICE_BYTES + 17)
            .unwrap();
    let payload = bytes::Bytes::from(vec![0xA7; STREAM_WRITE_SLICE_BYTES + 17]);
    let pending =
        PendingStreamingCommand::shared(StreamingCommand::VectoredWrite(VectoredSendItem {
            header: InlineFrameHeader::from_array(header),
            payload,
        }));
    let mut pending_slot = Some(pending);
    let mut turns = 0usize;
    loop {
        let mut pending = pending_slot.take().unwrap();
        let (written, complete) = write_streaming_command_slice(&mut writer, &mut pending)
            .await
            .unwrap();
        assert!(written > 0);
        assert!(written <= STREAM_WRITE_SLICE_BYTES);
        turns += 1;
        finish_streaming_command_slice(
            pending,
            complete,
            &queue,
            &mut yielded_slot,
            &mut pending_slot,
        );
        if complete {
            break;
        }
        assert_eq!(
            queue.space_notification_count(),
            0,
            "partial progress must not release shared queue capacity"
        );
    }

    assert!(turns >= 2, "an oversized command must remain resumable");
    assert!(pending_slot.is_none());
    assert_eq!(
        queue.space_notification_count(),
        1,
        "a shared queue slot is released once, after full command completion"
    );

    let mut local = PendingStreamingCommand::local(StreamingCommand::Flush);
    let (_, local_complete) = write_streaming_command_slice(&mut writer, &mut local)
        .await
        .unwrap();
    finish_streaming_command_slice(
        local,
        local_complete,
        &queue,
        &mut yielded_slot,
        &mut pending_slot,
    );
    assert_eq!(
        queue.space_notification_count(),
        1,
        "IO-owner-local responses must not fabricate shared queue capacity"
    );
    assert_eq!(
        recorded.lock().unwrap().len(),
        header.len() + STREAM_WRITE_SLICE_BYTES + 17
    );
}

#[test]
fn immediate_streaming_response_queue_bounds_byte_burst_with_deferred_admission() {
    let mut queue = LocalStreamingQueue::new();
    queue
        .try_extend([StreamingCommand::WriteBytes(bytes::Bytes::from(vec![
            0u8;
            RESPONSE_BATCH_BYTE_CAP
        ]))])
        .expect("the configured byte cap admits one bounded burst");
    assert!(queue.is_full());
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"x")),
            StreamingCommand::Flush,
        ])
        .expect("overflow is retained in the deferred response slot");
}

/// #189 regression: `is_full()` gates the pre-dispatch `AskNackReason::
/// Backpressure` decision in `stream_writer.rs::io_task` (see
/// `ask_dispatch_is_skipped_not_consumed_when_streaming_queue_has_no_room`
/// above). Before #189, `is_full()` only paused the read loop's entry gate
/// (harmless delay); #189 wired it to an immediate per-ask NACK instead, so
/// any pre-existing over-tight `is_full()` result now fails a live ask
/// outright rather than merely stalling it.
///
/// `with_response_reserve`'s `response_reserve_bytes` used to scale all the
/// way up to `max_message_size`, clamped only at
/// `STREAMING_RESPONSE_QUEUE_BYTE_CAP`. This crate's own default
/// `max_message_size` (10 MiB, `GossipConfig::default`) exceeds that cap
/// (~8 MiB), so the reserve used to clamp to *exactly* the cap -- making
/// the `queued_bytes + response_reserve_bytes > cap` check degenerate to
/// simply `queued_bytes` being nonzero. One connection-local response of
/// any size at all then marked the queue full for every other concurrently
/// in-flight streaming ask, even though the aggregate footprint was nowhere
/// near either byte cap.
///
/// Red (pre-fix) with the old `min(max_message_size.max(STREAM_CHUNK_SIZE),
/// STREAMING_RESPONSE_QUEUE_BYTE_CAP)` formula: constructing the queue with
/// the crate's real default `max_message_size` and admitting one ~1 MiB
/// response (a fraction of the ~8 MiB soft cap) already reported `is_full()
/// == true`. Green with the fix: the reserve is pinned to one response frame
/// (`STREAM_CHUNK_SIZE`) regardless of `max_message_size`, so the same
/// one-response queue reports room for more.
#[test]
fn concurrent_responses_admit_when_configured_max_message_size_exceeds_the_queue_byte_cap() {
    let default_max_message_size = crate::GossipConfig::default().max_message_size;
    assert!(
        default_max_message_size > STREAMING_RESPONSE_QUEUE_BYTE_CAP,
        "this test's premise requires the crate's default max_message_size to exceed the \
         queue's soft byte cap -- otherwise it cannot reproduce the degenerate reserve"
    );

    let mut queue = LocalStreamingQueue::with_response_reserve(default_max_message_size);
    let one_response = 1024 * 1024 + 100_000; // matches the streaming regression test's payload
    queue_streaming_response_bytes(
        &mut queue,
        1,
        bytes::Bytes::from(vec![0xA5u8; one_response]),
        default_max_message_size,
        None,
    )
    .expect("a single ~1 MiB response must be admitted");

    assert!(
        !queue.is_full(),
        "one ~1 MiB response must leave room for a concurrent ask's response under the \
         crate's own default max_message_size ({default_max_message_size} bytes) -- the byte \
         reserve must not consume the entire {STREAMING_RESPONSE_QUEUE_BYTE_CAP}-byte soft cap \
         by itself"
    );
}

/// `write_ask_nack_header_bounded` (the bounded single-attempt write
/// `drain_pending_ask_nacks` uses to flush `LocalStreamingQueue`'s queued
/// backpressure NACKs) must produce exactly the frame a peer decodes as
/// `AskNackReason::Backpressure` for the right correlation id, against a
/// real, healthy transport.
#[test]
fn write_ask_nack_header_bounded_writes_a_decodable_nack_on_a_healthy_stream() {
    run_multi_thread_test(async {
        let (mut server_half, mut client_half) = tokio::io::duplex(4096);
        let bytes_written_counter = Arc::new(AtomicUsize::new(0));
        let mut bytes_since_flush = 0usize;
        let header = crate::framing::write_ask_nack_header(
            0x2468_ACE0,
            crate::framing::AskNackReason::Backpressure,
        );

        let wrote = write_ask_nack_header_bounded(
            &mut server_half,
            &bytes_written_counter,
            &mut bytes_since_flush,
            header,
        )
        .await
        .expect("a healthy transport must not error writing a 16-byte NACK");
        assert!(
            wrote,
            "a healthy transport must complete the NACK write in one attempt"
        );

        let mut received = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
        tokio::io::AsyncReadExt::read_exact(&mut client_half, &mut received)
            .await
            .expect("the peer must receive the full NACK header");

        let control = crate::framing::decode_control(received[..4].try_into().unwrap())
            .expect("a NACK header must decode as a valid control word");
        assert_eq!(control.kind, crate::framing::WireKind::Response);
        assert_eq!(
            u32::from_be_bytes(received[4..8].try_into().unwrap()),
            0x2468_ACE0,
            "the NACK must carry the ask's own correlation id"
        );
        assert_eq!(
            crate::framing::ask_nack_reason(&received[4..]),
            Some(crate::framing::AskNackReason::Backpressure)
        );
        assert_eq!(
            bytes_written_counter.load(Ordering::Acquire),
            received.len()
        );
        assert_eq!(bytes_since_flush, received.len());

        drop(server_half);
        drop(client_half);
    });
}

/// The post-dispatch admission path has a "silent drop" shape: by the time
/// `queue_streaming_response_bytes`/`_pooled` runs inside
/// `write_ask_disposition_io`, the handler has already produced a real
/// answer -- there is no request left to hand back to a caller -- so a
/// `WouldBlock` here used to propagate straight out and lose that computed
/// response with no signal to the peer.
/// `queue_streaming_response_bytes_or_nack` closes it: an
/// `AskNackReason::Backpressure` NACK instead of a drop.
///
/// Construct admission failure directly against a `LocalStreamingQueue`: one
/// response at exactly `MAX_STREAM_SIZE` fills the queue via
/// `admit_single_oversize`, one more just over `STREAMING_THRESHOLD` fills
/// the deferred slot. Then call the `_or_nack` wrapper directly with a third
/// response as if a handler had already produced it -- this isolates the
/// post-handler path in isolation, with no `io_task` read loop involved.
///
/// `queue_streaming_response_bytes_or_nack` only *queues* the NACK
/// (`LocalStreamingQueue::queue_ask_nack`) rather than writing it, since it
/// has no way to know whether a partial streaming frame owns the wire right
/// now -- only `io_task` knows that. This asserts the queuing directly, then
/// drains it (`drain_pending_ask_nacks`, the same function `io_task` calls
/// once it has proven the wire free) to confirm the eventual wire content.
#[test]
fn streaming_admission_backpressure_nacks_instead_of_dropping_the_computed_response() {
    run_multi_thread_test(async {
        let mut queue = LocalStreamingQueue::with_response_reserve(MASTER_BUFFER_SIZE);

        queue_streaming_response_bytes(
            &mut queue,
            1,
            bytes::Bytes::from(vec![0x11u8; crate::MAX_STREAM_SIZE]),
            MASTER_BUFFER_SIZE,
            None,
        )
        .expect("the first, exactly-max-sized response admits via admit_single_oversize");

        queue_streaming_response_bytes(
            &mut queue,
            2,
            bytes::Bytes::from(vec![0x22u8; STREAMING_THRESHOLD + 1_048_576]),
            MASTER_BUFFER_SIZE,
            None,
        )
        .expect("the second response fills the deferred slot");

        let correlation_id = 3u32;
        assert_eq!(queue.pending_ask_nack_count(), 0);
        queue_streaming_response_bytes_or_nack(
            &mut queue,
            correlation_id,
            bytes::Bytes::from(vec![0x33u8; STREAMING_THRESHOLD + 4096]),
            MASTER_BUFFER_SIZE,
            None,
        )
        .expect(
            "admission backpressure must queue a NACK, not propagate an error that tears down \
             the read loop's current batch",
        );
        assert_eq!(
            queue.pending_ask_nack_count(),
            1,
            "the dropped response's NACK must be queued, not written inline -- this function \
             cannot know whether a partial streaming frame currently owns the wire"
        );

        let (mut server_half, mut client_half) = tokio::io::duplex(4096);
        let bytes_written_counter = Arc::new(AtomicUsize::new(0));
        let mut bytes_since_flush = 0usize;
        drain_pending_ask_nacks(
            &mut server_half,
            &bytes_written_counter,
            &mut bytes_since_flush,
            &mut queue,
        )
        .await
        .expect("draining a queued NACK against a healthy transport must not error");
        assert_eq!(queue.pending_ask_nack_count(), 0);

        let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
        tokio::io::AsyncReadExt::read_exact(&mut client_half, &mut header)
            .await
            .expect("the peer must receive the full NACK header");
        assert_eq!(
            u32::from_be_bytes(header[4..8].try_into().unwrap()),
            correlation_id,
            "the NACK must carry the dropped response's own correlation id"
        );
        assert_eq!(
            crate::framing::ask_nack_reason(&header[4..]),
            Some(crate::framing::AskNackReason::Backpressure)
        );

        drop(server_half);
        drop(client_half);
    });
}

/// P1: `drain_pending_ask_nacks` only writes `MAX_PER_TURN` (8) queued
/// entries per call. Before this fix, it reported nothing about whatever
/// was left over, so its caller (`io_task`) had no way to distinguish "queue
/// drained" from "queue still has work" and could treat the turn as idle.
/// A 9-entry burst must leave the call reporting outstanding work, and a
/// follow-up call must finish draining it.
#[test]
fn drain_pending_ask_nacks_reports_outstanding_work_past_the_per_turn_cap() {
    run_multi_thread_test(async {
        let mut queue = LocalStreamingQueue::new();
        for i in 0..9u32 {
            queue.queue_ask_nack(crate::framing::write_ask_nack_header(
                i,
                crate::framing::AskNackReason::Backpressure,
            ));
        }
        assert_eq!(queue.pending_ask_nack_count(), 9);

        let (mut server_half, mut client_half) = tokio::io::duplex(4096);
        let bytes_written_counter = Arc::new(AtomicUsize::new(0));
        let mut bytes_since_flush = 0usize;

        let more_pending = drain_pending_ask_nacks(
            &mut server_half,
            &bytes_written_counter,
            &mut bytes_since_flush,
            &mut queue,
        )
        .await
        .expect("draining against a healthy transport must not error");
        assert_eq!(
            queue.pending_ask_nack_count(),
            1,
            "the bounded per-turn drain must stop after MAX_PER_TURN (8) entries"
        );
        assert!(
            more_pending,
            "drain_pending_ask_nacks must report outstanding work when entries remain after \
             the bounded burst, so the io_task call site knows not to treat this turn as idle"
        );

        let more_pending = drain_pending_ask_nacks(
            &mut server_half,
            &bytes_written_counter,
            &mut bytes_since_flush,
            &mut queue,
        )
        .await
        .expect("draining the remainder must not error");
        assert!(
            !more_pending,
            "the queue must report no outstanding work once fully drained"
        );
        assert_eq!(queue.pending_ask_nack_count(), 0);

        // Drain all ten writes off the wire (8 from the first call, 1 from
        // the second, both against the same live duplex) to confirm nothing
        // was silently dropped along the way.
        for expected_correlation_id in 0..9u32 {
            let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
            tokio::io::AsyncReadExt::read_exact(&mut client_half, &mut header)
                .await
                .expect("every queued NACK must have reached the wire");
            assert_eq!(
                u32::from_be_bytes(header[4..8].try_into().unwrap()),
                expected_correlation_id
            );
        }

        drop(server_half);
        drop(client_half);
    });
}

/// P1: reproduces the reported deadlock shape end-to-end through the real
/// `io_task`, not just the drain primitive above. Nine raw `ActorAsk` frames
/// land in a single `write_all` so all nine are read and dispatched (each
/// NACKed with `UnknownActor`, since the server registry below has no ask
/// handler registered at all -- a real production NACK path, not a
/// manufactured test hook) inside one read-batch pass, before
/// `drain_pending_ask_nacks` ever gets a turn. That reproduces "more than
/// `MAX_PER_TURN` (8) queued at once, then nothing else happens" exactly.
/// No further traffic follows the initial write, so nothing but the
/// drain/wakeup fix itself can deliver the ninth NACK -- before the fix,
/// this hangs until `COMPLETION_BOUND` and fails.
#[test]
fn nine_queued_ask_nacks_all_reach_the_wire_without_further_traffic() {
    run_multi_thread_test(async {
        const ASK_COUNT: u32 = 9;
        const COMPLETION_BOUND: Duration = Duration::from_secs(5);

        let server_addr: std::net::SocketAddr = "127.0.0.1:44201".parse().unwrap();
        let peer_addr: std::net::SocketAddr = "127.0.0.1:44202".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "nine_queued_ask_nacks_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));

        let (server_io, mut peer_io) = tokio::io::duplex(1024 * 1024);
        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr,
            session_source: peer_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            peer_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(server_read_ctx),
        );

        let mut frames = Vec::new();
        for i in 1..=ASK_COUNT {
            let payload = b"x";
            let header = crate::framing::write_actor_ask_header(
                i,
                0xBAD0_0000_0000_0000 + i as u64,
                0xF00D_0001,
                payload.len(),
            );
            frames.extend_from_slice(&header);
            frames.extend_from_slice(payload);
        }
        tokio::io::AsyncWriteExt::write_all(&mut peer_io, &frames)
            .await
            .expect("writing all nine ActorAsk frames at once must succeed");

        let outcome = tokio::time::timeout(COMPLETION_BOUND, async {
            let mut received = Vec::with_capacity(ASK_COUNT as usize);
            for _ in 0..ASK_COUNT {
                let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
                tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut header)
                    .await
                    .expect("the peer must receive a NACK header for every queued ask");
                let correlation_id = u32::from_be_bytes(header[4..8].try_into().unwrap());
                let reason = crate::framing::ask_nack_reason(&header[4..]);
                received.push((correlation_id, reason));
            }
            received
        })
        .await;

        let received = outcome.expect(
            "all nine queued ask NACKs must reach the wire without further traffic -- a burst \
             past the 8-per-turn drain cap must not park the I/O task with entries still queued",
        );

        assert_eq!(received.len(), ASK_COUNT as usize);
        for (idx, (correlation_id, reason)) in received.iter().enumerate() {
            assert_eq!(
                *correlation_id,
                idx as u32 + 1,
                "NACKs must arrive in dispatch order"
            );
            assert_eq!(
                *reason,
                Some(crate::framing::AskNackReason::UnknownActor),
                "every ask targeted an actor with no registered handler"
            );
        }

        server_writer.shutdown();
        drop(peer_io);
    });
}

/// P1: a burst past `PENDING_ASK_NACK_CAP` (64) used to silently *evict*
/// the oldest not-yet-written NACK to make room for the newest -- dropping
/// the only remaining record that a specific, already-consumed ask existed
/// at all. That ask's requester then timed out instead of getting the fast
/// NACK this whole mechanism exists to deliver: the exact failure class
/// this line of work removes, reintroduced one layer down. 96 raw
/// `ActorAsk` frames (comfortably past the 64-entry cap, comfortably under
/// `READ_BATCH_LIMIT`) land in one `write_all` so all 96 are read and
/// dispatched -- each `UnknownActor`-NACKed, since the server registry has
/// no ask handler registered -- inside one read-batch pass. The fix gates
/// further reads on `LocalStreamingQueue::has_room_for_ask_nack`, so the
/// batch pauses at 64 queued, drains, and resumes -- every one of the 96
/// must still arrive, including the earliest ones the old eviction would
/// have discarded first.
#[test]
fn ninety_six_queued_ask_nacks_all_reach_the_wire_none_evicted() {
    run_multi_thread_test(async {
        const ASK_COUNT: u32 = 96;
        const COMPLETION_BOUND: Duration = Duration::from_secs(5);

        let server_addr: std::net::SocketAddr = "127.0.0.1:44203".parse().unwrap();
        let peer_addr: std::net::SocketAddr = "127.0.0.1:44204".parse().unwrap();

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(
                    "ninety_six_queued_ask_nacks_server",
                )),
                ..crate::GossipConfig::default()
            },
        ));

        let (server_io, mut peer_io) = tokio::io::duplex(1024 * 1024);
        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr,
            session_source: peer_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };
        let (server_writer, _server_task, _server_reader_task) = LockFreeStreamHandle::new(
            server_io,
            peer_addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            Some(server_read_ctx),
        );

        let mut frames = Vec::new();
        for i in 1..=ASK_COUNT {
            let payload = b"x";
            let header = crate::framing::write_actor_ask_header(
                i,
                0xBAD1_0000_0000_0000 + i as u64,
                0xF00D_0002,
                payload.len(),
            );
            frames.extend_from_slice(&header);
            frames.extend_from_slice(payload);
        }
        tokio::io::AsyncWriteExt::write_all(&mut peer_io, &frames)
            .await
            .expect("writing all 96 ActorAsk frames at once must succeed");

        let outcome = tokio::time::timeout(COMPLETION_BOUND, async {
            let mut received = Vec::with_capacity(ASK_COUNT as usize);
            for _ in 0..ASK_COUNT {
                let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
                tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut header)
                    .await
                    .expect("the peer must receive a NACK header for every queued ask");
                let correlation_id = u32::from_be_bytes(header[4..8].try_into().unwrap());
                let reason = crate::framing::ask_nack_reason(&header[4..]);
                received.push((correlation_id, reason));
            }
            received
        })
        .await;

        let received = outcome.expect(
            "every one of the 96 asks must reach a terminal outcome (a NACK) -- a burst past \
             the 64-entry pending-NACK cap must gate further reads, not evict an \
             already-consumed ask's only remaining record",
        );

        assert_eq!(
            received.len(),
            ASK_COUNT as usize,
            "no queued NACK may be silently dropped, including the earliest ones an eviction \
             policy would discard first"
        );
        for (idx, (correlation_id, reason)) in received.iter().enumerate() {
            assert_eq!(
                *correlation_id,
                idx as u32 + 1,
                "NACKs must arrive in dispatch order, and every correlation id from 1..=96 must \
                 be present -- none evicted"
            );
            assert_eq!(
                *reason,
                Some(crate::framing::AskNackReason::UnknownActor),
                "every ask targeted an actor with no registered handler"
            );
        }

        server_writer.shutdown();
        drop(peer_io);
    });
}

/// A transport whose `poll_write` legally returns `Ready(Ok(0))` on every
/// call, from the very first one -- modelling a half-closed write side, per
/// the `AsyncWrite` contract (distinct from `Pending`, which must be
/// re-polled; `Ok(0)` on a non-empty buffer means no further progress is
/// possible). `poll_write_calls` counts how many times the transport was
/// actually polled, so a test can assert the caller stopped promptly instead
/// of spinning.
struct AlwaysZeroWrite {
    poll_write_calls: Arc<AtomicUsize>,
}

impl AsyncWrite for AlwaysZeroWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.poll_write_calls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Review finding: `write_ask_nack_header_bounded`'s retry loop matches
/// `write_vectored_once`'s result as `Ok(Ok(n)) => { ... offset += n; ...
/// stuck_since = None; }` with no check that `n > 0`. Read in isolation,
/// nothing stops `n == 0` from taking that branch on every iteration --
/// adding zero to `offset`, never reaching `offset >= header.len()`,
/// resetting `stuck_since` so the stuck-mid-frame teardown timer can never
/// accumulate, and looping straight back into another poll with no
/// `Pending` and no timeout in between. That shape is exactly the CPU-spin
/// livelock R4 fixed for `OwnedChunks`' raw `.write()` tail
/// (`owned_chunks_zero_write_past_max_iov_exits_instead_of_livelocking`).
///
/// This does not actually reach that branch today: `write_vectored_once`
/// (its only caller here) already folds a real `result == 0` into
/// `Err(WriteZero)` before returning, so `Ok(Ok(0))` is unreachable through
/// this call path, and `Ok(Ok(n))` is safe to assume `n > 0` -- confirmed
/// here by proving a transport that legally returns `Ok(0)` from its very
/// first poll makes `write_ask_nack_header_bounded` return an `Err`
/// immediately, after exactly one poll, rather than spinning. Kept as an
/// explicit regression guard bounded by a hard timeout (fails loudly rather
/// than hanging the suite) so that if `write_vectored_once`'s zero-check
/// above it is ever weakened or bypassed, this test starts failing instead
/// of the bug going unnoticed until a stuck peer finds it in production.
#[tokio::test]
async fn write_ask_nack_header_bounded_does_not_spin_on_a_legal_zero_write() {
    let poll_write_calls = Arc::new(AtomicUsize::new(0));
    let mut stream = AlwaysZeroWrite {
        poll_write_calls: poll_write_calls.clone(),
    };
    let bytes_written_counter = Arc::new(AtomicUsize::new(0));
    let mut bytes_since_flush = 0usize;
    let header = crate::framing::write_ask_nack_header(
        0x1111_2222,
        crate::framing::AskNackReason::Backpressure,
    );

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        write_ask_nack_header_bounded(
            &mut stream,
            &bytes_written_counter,
            &mut bytes_since_flush,
            header,
        ),
    )
    .await;

    assert!(
        result.is_ok(),
        "write_ask_nack_header_bounded did not return within 5s against a transport that \
         legally returns Ok(0) on every poll -- livelocked instead of erroring (poll_write \
         calls observed: {})",
        poll_write_calls.load(Ordering::SeqCst)
    );
    assert!(
        result.unwrap().is_err(),
        "a transport that can never make progress must surface as an error, not Ok(true)/Ok(false)"
    );
    assert_eq!(
        poll_write_calls.load(Ordering::SeqCst),
        1,
        "must stop after the first Ok(0) rather than retrying a transport that has already \
         reported it can never make progress"
    );
    assert_eq!(
        bytes_written_counter.load(Ordering::Acquire),
        0,
        "no bytes were actually written"
    );
}

// The genuinely-mid-frame "a queued NACK must never splice into a partially
// written streaming frame" property needs reads to keep flowing while a
// write is stuck -- otherwise the second/third ask that would trigger the
// NACK never even gets read off the wire while the first is stalled (this
// branch, standing alone, still gates all reads behind
// `local_streaming_queue.is_full()`, the exact behavior
// `fix/bidirectional-streaming-deadlock` (#186) removes). That end-to-end
// proof lives there, stacked on this branch, where it holds; see
// `ask_backpressure_nack_never_splices_into_an_in_flight_streaming_frame`.
// `write_ask_nack_header_bounded_writes_a_decodable_nack_on_a_healthy_stream`
// and `streaming_admission_backpressure_nacks_instead_of_dropping_the_computed_response`
// above already cover the structural piece this branch owns: a NACK is
// queued, never written inline, by any caller that cannot itself know
// whether the wire is free.

#[test]
fn immediate_bytes_response_admission_stays_lazy_for_many_frames() {
    let payload_len = STREAM_CHUNK_SIZE * 8;
    let mut queue = LocalStreamingQueue::with_response_reserve(payload_len);
    queue_streaming_response_bytes(
        &mut queue,
        0xC0DE,
        bytes::Bytes::from(vec![0xA7; payload_len]),
        payload_len,
        None,
    )
    .expect("large bytes response should be admitted");

    assert_eq!(
        queue.queue.len(),
        2,
        "a large response must retain one lazy command plus its terminal flush"
    );
}

#[test]
fn lazy_stream_admission_counts_owned_payload_not_generated_headers() {
    let payload_len = STREAMING_RESPONSE_QUEUE_BYTE_CAP + 1;
    let mut queue = LocalStreamingQueue::with_response_reserve(20);
    queue_streaming_response_bytes(
        &mut queue,
        0xC0DE,
        bytes::Bytes::from(vec![0xA7; payload_len]),
        20,
        None,
    )
    .expect("lazy framing headers must not consume the retained-byte budget");
}

#[test]
fn sliced_bytes_stream_is_compacted_before_admission() {
    let backing = bytes::Bytes::from(vec![0xA7; 4 * STREAM_CHUNK_SIZE]);
    let payload = backing.slice(..STREAM_CHUNK_SIZE);
    let mut queue = LocalStreamingQueue::new();

    queue_streaming_response_bytes(&mut queue, 0xC0DE, payload, 20, None)
        .expect("a sliced streaming payload should be admitted after compaction");

    let Some(StreamingCommand::BytesResponse(response)) = queue.queue.front() else {
        panic!("expected a lazy bytes response command");
    };
    assert_eq!(response.payload_len, STREAM_CHUNK_SIZE);
    assert_eq!(
        response.retained_bytes, STREAM_CHUNK_SIZE,
        "queue accounting must charge the compacted payload, not the old backing allocation"
    );
}

#[test]
fn response_in_flight_with_only_flush_keeps_read_admission_open() {
    let mut queue = LocalStreamingQueue::new();
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"response")),
            StreamingCommand::Flush,
        ])
        .expect("response should be admitted");

    let response = queue.pop_front().expect("response command");
    assert!(matches!(response, StreamingCommand::WriteBytes(_)));
    assert!(
        !queue.is_full(),
        "a small in-flight response must not stop reciprocal reads"
    );
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"next-response")),
            StreamingCommand::Flush,
        ])
        .expect("a bounded response may queue behind the in-flight terminal flush");
    assert_eq!(queue.queue.len(), 3);
}

#[test]
fn oversized_response_in_flight_keeps_read_admission_open() {
    let mut queue = LocalStreamingQueue::new();
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from(vec![
                0u8;
                RESPONSE_BATCH_BYTE_CAP + 1
            ])),
            StreamingCommand::Flush,
        ])
        .expect("one oversized response is the explicit admission exception");
    let response = queue.pop_front().expect("oversized response command");
    assert!(matches!(response, StreamingCommand::WriteBytes(_)));
    assert!(
        !queue.is_full(),
        "a bounded oversized response must keep reciprocal reads flowing"
    );
}

#[test]
fn maximum_in_flight_response_keeps_reciprocal_reads_open() {
    let mut queue = LocalStreamingQueue::new();
    queue.response_in_flight = true;
    queue.in_flight_bytes = crate::MAX_STREAM_SIZE;
    queue.queue.push_back(StreamingCommand::Flush);

    assert!(
        !queue.is_full(),
        "a maximum-size response must not deadlock the reciprocal read path"
    );
}

#[test]
fn oversized_in_flight_response_admits_one_bounded_deferred_response() {
    let mut queue = LocalStreamingQueue::new();
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from(vec![
                0u8;
                RESPONSE_BATCH_BYTE_CAP + 1
            ])),
            StreamingCommand::Flush,
        ])
        .expect("one oversized response is the explicit admission exception");
    let response = queue.pop_front().expect("oversized response command");
    assert!(matches!(response, StreamingCommand::WriteBytes(_)));

    let deferred_bytes = RESPONSE_BATCH_BYTE_CAP - 1;
    assert!(
        queue.can_admit_response(2, deferred_bytes),
        "the bounded deferred slot must preserve a valid response behind the in-flight one"
    );
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from(vec![0u8; deferred_bytes])),
            StreamingCommand::Flush,
        ])
        .expect("the bounded deferred response must not be dropped");
    assert!(
        queue.is_full(),
        "the deferred slot must stop reads before a third response is consumed"
    );
}

#[test]
fn near_hard_cap_in_flight_response_backpressures_large_deferred_response() {
    let mut queue = LocalStreamingQueue::new();
    let current_bytes = STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP
        .saturating_sub(MAX_STREAMING_RESPONSE_RETAINED_BYTES)
        .saturating_add(1);
    queue.response_in_flight = true;
    queue.in_flight_bytes = current_bytes;
    queue.queue.push_back(StreamingCommand::Flush);

    let deferred_bytes = MAX_STREAMING_RESPONSE_RETAINED_BYTES;
    assert!(
        queue.is_full(),
        "the read gate must close before the aggregate hard cap is exceeded"
    );
    assert!(
        !queue.can_admit_response(2, deferred_bytes),
        "a deferred response must stay within the aggregate hard retained-byte cap"
    );
}

#[test]
fn near_hard_cap_queued_response_closes_read_admission_before_followup() {
    let mut queue = LocalStreamingQueue::new();
    queue.queued_bytes = STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP
        .saturating_sub(MAX_STREAMING_RESPONSE_RETAINED_BYTES)
        .saturating_add(1);
    queue.queue.push_back(StreamingCommand::Flush);

    assert!(
        queue.is_full(),
        "the read gate must stop before a follow-up handler can exceed the hard cap"
    );
}

#[test]
fn queued_response_retains_large_followup_within_hard_cap() {
    let mut queue = LocalStreamingQueue::new();
    let queued_bytes = RESPONSE_BATCH_BYTE_CAP - STREAM_CHUNK_SIZE;
    queue.queued_bytes = queued_bytes;
    queue.queue.push_back(StreamingCommand::Flush);

    let followup_bytes = MAX_STREAMING_RESPONSE_RETAINED_BYTES.saturating_sub(1);
    assert!(
        queue.can_admit_response(2, followup_bytes),
        "a valid large follow-up must fit the single deferred slot"
    );
    assert!(
        !queue.is_full(),
        "read admission remains open while the aggregate hard cap has room"
    );
}

#[test]
fn partial_streaming_output_defers_flush_until_terminal_flush() {
    let pending = PendingStreamingCommand::local(StreamingCommand::WriteBytes(
        bytes::Bytes::from_static(b"partial"),
    ));

    assert!(!should_flush_stream_output(1, Some(&pending), None));
    assert!(!should_flush_stream_output(1, None, Some(&pending)));
    assert!(should_flush_stream_output(1, None, None));
    assert!(!should_flush_stream_output(0, None, None));
}

/// A pooled response must retain its original allocation while it waits for
/// the connection-owned writer. Materializing a `BytesMut` here doubles the
/// peak resident payload and defeats the pooled encoder's bounded-memory
/// contract.
#[test]
fn pooled_streaming_response_retains_owned_payload_without_materializing_bytes() {
    let payload_len = STREAM_CHUNK_SIZE;
    let prefix = Some([0xD3; 16]);
    let payload = crate::typed::PooledPayload::try_from_pooled_bytes(
        payload_len - prefix.as_ref().map(|value| value.len()).unwrap_or(0),
        |out| out.extend(std::iter::repeat_n(0xA7, payload_len - 16)),
    )
    .expect("pooled payload allocation");
    let mut queue = LocalStreamingQueue::with_response_reserve(payload_len + 1024);

    queue_streaming_response_pooled(
        &mut queue,
        0xC0DE,
        payload,
        prefix,
        payload_len,
        128 * 1024,
        None,
    )
    .expect("pooled response should be admitted");

    assert!(matches!(
        queue.queue.front(),
        Some(StreamingCommand::PooledResponse(_))
    ));
    assert!(matches!(queue.queue.get(1), Some(StreamingCommand::Flush)));
}

#[test]
fn pooled_streaming_admission_accounts_surplus_payload() {
    let expected_payload_len = STREAM_CHUNK_SIZE / 4;
    let retained_payload_len = expected_payload_len * 2;
    let payload = crate::typed::PooledPayload::try_from_pooled_bytes(retained_payload_len, |out| {
        out.extend(std::iter::repeat_n(0xA7, retained_payload_len))
    })
    .expect("pooled payload allocation");
    let mut queue = LocalStreamingQueue::new();

    queue_streaming_response_pooled(
        &mut queue,
        0xC0DE,
        payload,
        None,
        expected_payload_len,
        STREAM_CHUNK_SIZE,
        None,
    )
    .expect("surplus pooled payload should remain bounded and be admitted");

    let Some(StreamingCommand::PooledResponse(response)) = queue.queue.front() else {
        panic!("expected a pooled response command");
    };
    assert_eq!(
        response.retained_bytes, retained_payload_len,
        "admission must charge all pooled bytes retained by the response command"
    );
}

#[tokio::test]
async fn pooled_streaming_response_writes_prefix_and_payload_in_frame_order() {
    let payload_len = 64;
    let prefix = [0xD3; 16];
    let payload_bytes = vec![0xA7; payload_len - prefix.len()];
    let payload = crate::typed::PooledPayload::try_from_pooled_bytes(payload_bytes.len(), |out| {
        out.extend_from_slice(&payload_bytes)
    })
    .expect("pooled payload allocation");
    // Keep the frame payload below the 16-byte prefix so the prefix itself
    // crosses a frame boundary; the writer must treat it as part of the
    // logical payload rather than assuming it fits in the start frame.
    let max_message_size = 20;
    let mut queue = LocalStreamingQueue::with_response_reserve(max_message_size);
    queue_streaming_response_pooled(
        &mut queue,
        0xC0DE,
        payload,
        Some(prefix),
        payload_len,
        max_message_size,
        None,
    )
    .expect("pooled response should be admitted");

    let command = queue.pop_front().expect("pooled response command");
    let (stream_id, chunk_size) = match &command {
        StreamingCommand::PooledResponse(response) => (response.stream_id, response.chunk_size),
        other => panic!("expected pooled response command, got {other:?}"),
    };
    let mut pending = PendingStreamingCommand::local(command);
    let (mut writer, recorded) = RecordingWriter::new();
    loop {
        let (written, complete) = write_streaming_command_slice(&mut writer, &mut pending)
            .await
            .expect("pooled response write");
        assert!(written > 0);
        if complete {
            break;
        }
    }
    let flush = queue.pop_front().expect("terminal flush");
    let mut pending_flush = PendingStreamingCommand::local(flush);
    let (_, complete) = write_streaming_command_slice(&mut writer, &mut pending_flush)
        .await
        .expect("pooled response flush");
    assert!(complete);

    let first_len = payload_len.min(chunk_size);
    let mut logical_payload = Vec::with_capacity(payload_len);
    logical_payload.extend_from_slice(&prefix);
    logical_payload.extend_from_slice(&payload_bytes);
    let mut expected = Vec::new();
    expected.extend_from_slice(
        &crate::framing::try_write_stream_response_start_header(
            stream_id,
            0xC0DE,
            payload_len as u32,
            first_len,
        )
        .unwrap(),
    );
    expected.extend_from_slice(&logical_payload[..first_len]);
    let mut wire_offset = first_len;
    let mut chunk_index = 1u32;
    while wire_offset < payload_len {
        let frame_payload_start = wire_offset;
        let frame_payload_end = (wire_offset + chunk_size).min(payload_len);
        expected.extend_from_slice(
            &crate::framing::try_write_stream_data_header(
                true,
                stream_id,
                chunk_index,
                frame_payload_end - frame_payload_start,
            )
            .unwrap(),
        );
        expected.extend_from_slice(&logical_payload[frame_payload_start..frame_payload_end]);
        wire_offset = frame_payload_end;
        chunk_index += 1;
    }

    assert_eq!(*recorded.lock().unwrap(), expected);
}

#[tokio::test]
async fn pooled_streaming_response_yields_at_each_frame_boundary() {
    let payload_len = 128;
    let payload = crate::typed::PooledPayload::try_from_pooled_bytes(payload_len, |out| {
        out.extend(std::iter::repeat_n(0xA7, payload_len));
    })
    .expect("pooled payload allocation");
    let mut local_queue = LocalStreamingQueue::with_response_reserve(64);
    queue_streaming_response_pooled(
        &mut local_queue,
        0xC0DE,
        payload,
        None,
        payload_len,
        64,
        None,
    )
    .expect("pooled response should be admitted");

    let command = local_queue.pop_front().expect("pooled response command");
    let mut pending = PendingStreamingCommand::local(command);
    let (mut writer, _) = RecordingWriter::new();
    let mut complete = false;
    let mut written = 0;
    while !pending.yield_after_frame && !complete {
        let (turn_written, turn_complete) =
            write_streaming_command_slice(&mut writer, &mut pending)
                .await
                .expect("first pooled frame write");
        written += turn_written;
        complete = turn_complete;
    }
    assert!(written > 0);
    assert!(!complete, "the response has more than one frame");
    assert!(
        pending.yield_after_frame,
        "a completed frame must yield the local command to the scheduler"
    );

    let shared_queue = StreamingQueue::new(1, "127.0.0.1:40494".parse().unwrap());
    let mut yielded_slot = None;
    let mut pending_slot = None;
    finish_streaming_command_slice(
        pending,
        complete,
        &shared_queue,
        &mut yielded_slot,
        &mut pending_slot,
    );
    assert!(pending_slot.is_none());
    assert!(matches!(
        yielded_slot.as_ref().map(|pending| &pending.command),
        Some(StreamingCommand::PooledResponse(_))
    ));
}

#[tokio::test]
async fn bytes_streaming_response_writes_frame_order() {
    let payload_len = 128;
    let payload_bytes = vec![0xA7; payload_len];
    let mut local_queue = LocalStreamingQueue::with_response_reserve(64);
    queue_streaming_response_bytes(
        &mut local_queue,
        0xC0DE,
        bytes::Bytes::from(payload_bytes.clone()),
        64,
        None,
    )
    .expect("bytes response should be admitted");

    let command = local_queue.pop_front().expect("bytes response command");
    let stream_id = match &command {
        StreamingCommand::BytesResponse(response) => response.stream_id,
        other => panic!("expected bytes response command, got {other:?}"),
    };
    let mut pending = PendingStreamingCommand::local(command);
    let (mut writer, recorded) = RecordingWriter::new();
    loop {
        let (_, complete) = write_streaming_command_slice(&mut writer, &mut pending)
            .await
            .expect("bytes response write");
        if complete {
            break;
        }
    }

    let chunk_size = 64 - crate::framing::STREAM_RESPONSE_START_HEADER_LEN;
    let mut expected = Vec::new();
    expected.extend_from_slice(
        &crate::framing::try_write_stream_response_start_header(
            stream_id,
            0xC0DE,
            payload_len as u32,
            payload_len.min(chunk_size),
        )
        .unwrap(),
    );
    expected.extend_from_slice(&payload_bytes[..payload_len.min(chunk_size)]);
    let mut offset = payload_len.min(chunk_size);
    let mut chunk_index = 1u32;
    while offset < payload_len {
        let end = (offset + chunk_size).min(payload_len);
        expected.extend_from_slice(
            &crate::framing::try_write_stream_data_header(
                true,
                stream_id,
                chunk_index,
                end - offset,
            )
            .unwrap(),
        );
        expected.extend_from_slice(&payload_bytes[offset..end]);
        offset = end;
        chunk_index += 1;
    }

    assert_eq!(*recorded.lock().unwrap(), expected);
}

#[tokio::test]
async fn bytes_streaming_response_yields_at_each_frame_boundary() {
    let payload_len = 128;
    let mut local_queue = LocalStreamingQueue::with_response_reserve(64);
    queue_streaming_response_bytes(
        &mut local_queue,
        0xC0DE,
        bytes::Bytes::from(vec![0xA7; payload_len]),
        64,
        None,
    )
    .expect("bytes response should be admitted");

    let command = local_queue.pop_front().expect("bytes response command");
    let mut pending = PendingStreamingCommand::local(command);
    let (mut writer, _) = RecordingWriter::new();
    let mut complete = false;
    let mut written = 0;
    while !pending.yield_after_frame && !complete {
        let (turn_written, turn_complete) =
            write_streaming_command_slice(&mut writer, &mut pending)
                .await
                .expect("bytes response write");
        written += turn_written;
        complete = turn_complete;
    }
    assert!(written > 0);
    assert!(!complete, "the response has more than one frame");
    assert!(
        pending.yield_after_frame,
        "a completed Bytes frame must yield the local command to the scheduler"
    );
}

#[test]
fn immediate_streaming_response_queue_defers_overflow_until_prior_response_drains() {
    let mut queue = LocalStreamingQueue::new();
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from(vec![0u8; RESPONSE_BATCH_BYTE_CAP])),
            StreamingCommand::Flush,
        ])
        .expect("the first response fits the bounded queue");

    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"deferred")),
            StreamingCommand::Flush,
        ])
        .expect("the next response is retained for deferred admission");
    assert!(
        queue.is_full(),
        "deferred response must close read admission"
    );

    let mut drained = 0;
    while queue.pop_front().is_some() {
        drained += 1;
    }
    assert_eq!(
        drained, 4,
        "the deferred response must be promoted after flush"
    );
    assert!(
        !queue.is_full(),
        "admission reopens after both responses drain"
    );
}

#[test]
fn immediate_streaming_response_queue_reserves_command_slots() {
    let mut queue = LocalStreamingQueue::with_response_reserve(STREAM_CHUNK_SIZE * 2);
    let reserve = queue.response_reserve_commands;
    for _ in 0..(STREAMING_RESPONSE_QUEUE_COMMAND_CAP - reserve) {
        queue
            .try_extend([StreamingCommand::Flush])
            .expect("reserved queue slots still admit bounded commands");
    }
    assert!(!queue.is_full(), "the reserved response still fits exactly");
    queue
        .try_extend([StreamingCommand::Flush])
        .expect("overflow is retained as a deferred response");
    assert!(
        queue.is_full(),
        "the command reserve must close read admission"
    );
}

#[test]
fn streaming_scheduler_alternates_local_and_shared_sources() {
    assert_eq!(
        choose_streaming_source(true, true, true),
        Some(StreamingSource::Shared)
    );
    assert_eq!(
        choose_streaming_source(false, true, true),
        Some(StreamingSource::Local)
    );
    assert_eq!(
        choose_streaming_source(true, true, false),
        Some(StreamingSource::Local)
    );
    assert_eq!(
        choose_streaming_source(false, false, true),
        Some(StreamingSource::Shared)
    );
    assert_eq!(choose_streaming_source(true, false, false), None);
}

#[test]
fn immediate_streaming_response_queue_admits_one_oversized_response() {
    let mut queue = LocalStreamingQueue::new();
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from(vec![
                0u8;
                RESPONSE_BATCH_BYTE_CAP + 1
            ])),
            StreamingCommand::Flush,
        ])
        .expect("one response may exceed the queue cap");
    assert!(queue.is_full());
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"x")),
            StreamingCommand::Flush,
        ])
        .expect("a bounded follow-up response fits the hard resident cap");

    while queue.pop_front().is_some() {}
    assert!(
        !queue.is_full(),
        "admission reopens after the oversized response flushes"
    );
    queue
        .try_extend([StreamingCommand::WriteBytes(bytes::Bytes::from_static(
            b"x",
        ))])
        .expect("a later response is admitted after the first one drains");
}

#[test]
fn sole_response_above_normal_queue_cap_remains_admissible() {
    let mut queue = LocalStreamingQueue::new();
    let payload_len = STREAMING_RESPONSE_QUEUE_BYTE_CAP + 1;
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from(vec![0u8; payload_len])),
            StreamingCommand::Flush,
        ])
        .expect("one sole lazy response remains valid above the queue cap");
    assert!(queue.is_full());
}

#[test]
fn response_admission_rejects_beyond_hard_retained_footprints() {
    let mut queue = LocalStreamingQueue::new();
    queue
        .try_extend([
            StreamingCommand::WriteBytes(bytes::Bytes::from(vec![0u8; RESPONSE_BATCH_BYTE_CAP])),
            StreamingCommand::Flush,
        ])
        .expect("the first bounded response fits");

    let oversized = STREAMING_RESPONSE_QUEUE_HARD_BYTE_CAP + 1;
    assert!(
        !queue.can_admit_response(2, oversized),
        "a deferred response must not exceed the aggregate hard footprint"
    );
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

/// R-15: pins that a `finish()` landing between `wait()`'s state load and its
/// await still wakes the waiter.
///
/// The original finding claimed this was a live lost-wakeup bug. It was not —
/// `Notified` captures `Notify`'s `notify_waiters_calls` generation counter at
/// *construction*, and `finish()` bumps it via `notify_waiters()`, so the
/// pre-existing construct-before-load ordering already covered this window.
/// But that made correctness depend on an undocumented statement order, and
/// getting it wrong hangs the dial forever (the follower branch of
/// `get_connection*` has no timeout). `wait()` now registers the waiter with
/// `enable()` up front so the ordering no longer matters.
///
/// The race hook fires at exactly the vulnerable point, making the
/// interleaving deterministic rather than probabilistic. Verified to
/// discriminate: moving the construction+enable below the state load makes
/// this test fail in 1s.
#[tokio::test]
async fn qa_r15_finish_between_check_and_await_wakes_waiter() {
    let gate = Arc::new(OutboundDialGate::new());

    // Weak, so the hook does not keep the gate alive through a reference cycle.
    let hook_gate = Arc::downgrade(&gate);
    gate.set_race_hook(move || {
        if let Some(gate) = hook_gate.upgrade() {
            gate.finish(true);
        }
    });

    tokio::time::timeout(Duration::from_secs(1), gate.wait())
        .await
        .expect("R-15: finish() landing in the notified() registration gap must still wake wait()");
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
async fn outbound_retry_allows_one_immediate_retry_then_reopens_after_floor() {
    let retry = OutboundDialRetry::with_retry_floor(Duration::from_millis(10));

    let attempt = retry
        .try_claim_attempt()
        .expect("an untouched peer may dial immediately");
    retry.record_failure(attempt);
    let attempt = retry
        .try_claim_attempt()
        .expect("first retry must be immediate");
    retry.record_failure(attempt);
    assert!(
        retry.try_claim_attempt().is_none(),
        "second consecutive failure must arm the retry floor"
    );

    retry.record_published_connection();
    let attempt = retry
        .try_claim_attempt()
        .expect("publication must clear an active retry floor");
    retry.record_success(attempt);
    let attempt = retry
        .try_claim_attempt()
        .expect("successful completion must clear its reservation");
    retry.record_failure(attempt);
    let attempt = retry
        .try_claim_attempt()
        .expect("the first failure after success must regain the immediate retry");
    retry.record_failure(attempt);
    assert!(
        retry.try_claim_attempt().is_none(),
        "the reset streak's second failure must re-arm the floor"
    );

    tokio::time::sleep(Duration::from_millis(15)).await;
    assert!(
        retry.try_claim_attempt().is_some(),
        "a caller must be able to claim the dial after the floor expires"
    );
}

#[tokio::test]
async fn stale_outbound_completion_cannot_replace_a_newer_reservation() {
    let retry = OutboundDialRetry::with_retry_floor(Duration::from_millis(10));

    let attempt_a = retry
        .try_claim_attempt()
        .expect("attempt A must claim the slot");
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert!(
        retry.try_claim_attempt().is_some(),
        "attempt B must replace A after the bounded reservation expires"
    );

    retry.record_failure(attempt_a);
    assert!(
        retry.try_claim_attempt().is_none(),
        "A's stale completion must not clear B's active reservation"
    );
}

#[tokio::test]
async fn connection_published_after_retry_claim_is_reused_before_dial() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("retry-claim-local")),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();
    let peer = crate::KeyPair::new_for_testing("retry_claim_publish_race").peer_id();
    let addr: SocketAddr = "127.0.0.1:7314".parse().unwrap();
    pool.add_addr_to_peer_id(addr, peer.clone());
    let session = pool.get_or_create_peer_session(&peer);
    let attempt = session
        .outbound_dial_retry
        .try_claim_attempt()
        .expect("retry attempt must be claimed before publication");

    let (io, _keep) = tokio::io::duplex(1024);
    pool.finalize_new_outbound_connection(addr, io, Arc::downgrade(&registry), None, addr, None)
        .await
        .expect("publish outbound connection");

    assert!(
        pool.reuse_published_connection(&session).is_some(),
        "a connection published while the retry floor is active must be reused before WouldBlock"
    );
    assert!(
        pool.reuse_published_connection_after_retry_claim(&session, attempt)
            .is_some(),
        "a connection published after the initial lookup must be reused before dialing"
    );
}

#[test]
fn neutral_outbound_completion_releases_reservation_without_resetting_streak() {
    let retry = OutboundDialRetry::with_retry_floor(Duration::from_secs(1));
    let attempt = retry
        .try_claim_attempt()
        .expect("initial attempt must claim");
    retry.record_failure(attempt);

    let neutral_attempt = retry
        .try_claim_attempt()
        .expect("first retry must remain immediate");
    retry.record_neutral(neutral_attempt);

    let failed_attempt = retry
        .try_claim_attempt()
        .expect("neutral completion must release its reservation");
    retry.record_failure(failed_attempt);
    assert!(
        retry.try_claim_attempt().is_none(),
        "neutral completion must preserve the prior failure streak"
    );
}

#[test]
fn outbound_retry_claim_is_atomic_per_peer() {
    const CALLERS: usize = 8;
    let retry = Arc::new(OutboundDialRetry::with_retry_floor(Duration::from_secs(1)));
    let checked = Arc::new(Barrier::new(CALLERS));
    let mut callers = Vec::with_capacity(CALLERS);

    for _ in 0..CALLERS {
        let retry = Arc::clone(&retry);
        let checked = Arc::clone(&checked);
        callers.push(std::thread::spawn(move || {
            let attempt = retry.try_claim_attempt();
            checked.wait();
            if let Some(attempt) = attempt {
                retry.record_failure(attempt);
            }
            attempt.is_some()
        }));
    }

    let eligible = callers
        .into_iter()
        .map(|caller| caller.join().expect("retry claimant panicked"))
        .filter(|eligible| *eligible)
        .count();
    assert_eq!(
        eligible, 1,
        "exactly one caller may claim a peer's dial slot"
    );
}

#[tokio::test]
async fn outbound_retry_failure_streak_never_wraps_to_an_immediate_retry() {
    let retry = OutboundDialRetry::with_retry_floor(Duration::from_millis(1));

    let attempt = retry
        .try_claim_attempt()
        .expect("an untouched peer may dial immediately");
    retry
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .consecutive_failures = u8::MAX;
    retry.record_failure(attempt);

    tokio::time::sleep(Duration::from_millis(2)).await;
    let attempt = retry
        .try_claim_attempt()
        .expect("the retry floor must eventually reopen");
    retry.record_failure(attempt);

    assert!(
        retry.try_claim_attempt().is_none(),
        "a saturated failure streak must keep the retry floor armed"
    );
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
            ConnectionDirection::Outbound,
            stream_handle,
            CorrelationTracker::new(),
        );

        // PR #183 review, round 12: `send_data` carries a complete V5
        // frame -- this crate's wire protocol has no opaque-bytes case
        // (see the module doc comment above `reject_oversize_write_payload`
        // in stream_writer.rs), so this must be a genuine, complete frame,
        // not a bare literal.
        let payload = vec![9u8; 4];
        let header = crate::framing::write_gossip_frame_prefix(payload.len());
        let mut data = Vec::with_capacity(header.len() + payload.len());
        data.extend_from_slice(&header);
        data.extend_from_slice(&payload);
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

        // PR #183 review, round 12: `write_bytes_nonblocking` carries a
        // complete V5 frame per call -- this crate's wire protocol has no
        // opaque-bytes case (see the module doc comment above
        // `reject_oversize_write_payload` in stream_writer.rs), so each of
        // these must be a genuine, complete frame, not a bare literal.
        let make_frame = |fill: u8, len: usize| {
            let payload = vec![fill; len];
            let header = crate::framing::write_gossip_frame_prefix(payload.len());
            let mut frame = Vec::with_capacity(header.len() + payload.len());
            frame.extend_from_slice(&header);
            frame.extend_from_slice(&payload);
            bytes::Bytes::from(frame)
        };
        let payloads = [make_frame(1, 3), make_frame(2, 3), make_frame(3, 5)];

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

        // PR #183 review, round 12: same reasoning as `payloads` in
        // `test_writer_owner_batch_preserves_order` above -- `first`/
        // `second` go through `write_bytes_nonblocking`, which carries a
        // complete V5 frame per call, so each must be genuine, not a bare
        // literal.
        let make_frame = |fill: u8, len: usize| {
            let payload = vec![fill; len];
            let header = crate::framing::write_gossip_frame_prefix(payload.len());
            let mut frame = Vec::with_capacity(header.len() + payload.len());
            frame.extend_from_slice(&header);
            frame.extend_from_slice(&payload);
            bytes::Bytes::from(frame)
        };
        let first = make_frame(1, 5);
        let second = make_frame(2, 6);
        let payload = bytes::Bytes::from_static(b"PAYLOAD");
        // A real V5 control word declaring `payload`'s exact length: the
        // gate in `enqueue_write_nonblocking` decodes `body_len` from the
        // header's first four bytes on every `HeaderPayload` write, so an
        // arbitrary 4-byte placeholder here (as opposed to a genuine
        // control word) can decode to an oversize `body_len` and be
        // rejected before this purely-plumbing ordering check ever runs.
        let header = bytes::Bytes::copy_from_slice(
            &crate::framing::try_encode_control(crate::framing::WireKind::Gossip, payload.len())
                .unwrap(),
        );

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
            ConnectionDirection::Outbound,
            stream_handle,
            CorrelationTracker::new(),
        );

        // PR #183 review, round 13: `send_data` carries a complete V5
        // frame (this crate's wire protocol has no opaque-bytes case), so
        // this must be genuine, complete frame bytes, not a bare 3-byte
        // literal -- a nonempty remainder shorter than a control word is
        // now refused regardless of the underlying writer's health, which
        // isn't what this test is exercising.
        let payload = vec![9u8; 4];
        let header = crate::framing::write_gossip_frame_prefix(payload.len());
        let mut data = Vec::with_capacity(header.len() + payload.len());
        data.extend_from_slice(&header);
        data.extend_from_slice(&payload);

        let result = handle.send_data(data).await;
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

    // PR #183 review, round 12: `write_bytes_ask` carries a complete V5
    // frame -- this crate's wire protocol has no opaque-bytes case (see
    // the module doc comment above `reject_oversize_write_payload` in
    // stream_writer.rs), so this must be a genuine, complete frame, not a
    // bare literal like "ping".
    let ping_payload = vec![7u8; 4];
    let ping_header = crate::framing::write_gossip_frame_prefix(ping_payload.len());
    let mut ping_bytes = Vec::with_capacity(ping_header.len() + ping_payload.len());
    ping_bytes.extend_from_slice(&ping_header);
    ping_bytes.extend_from_slice(&ping_payload);
    let ping = bytes::Bytes::from(ping_bytes);

    let mut tasks = Vec::new();
    for _ in 0..100 {
        let handle = handle.clone();
        let ping = ping.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                handle.write_bytes_ask(ping.clone()).await?;
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
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
                let control = crate::framing::decode_control(len_buf).expect("valid V5 control");
                let msg_len = control.body_len;
                let mut msg = vec![0u8; msg_len];
                if tokio::io::AsyncReadExt::read_exact(&mut server_io, &mut msg)
                    .await
                    .is_err()
                {
                    break;
                }

                if control.kind == crate::framing::WireKind::DirectAsk
                    && msg_len >= crate::framing::DIRECT_ASK_HEADER_LEN
                {
                    let correlation_id = u32::from_be_bytes(msg[..4].try_into().unwrap());
                    let payload_len = msg_len - crate::framing::DIRECT_ASK_HEADER_LEN;
                    let payload = &msg[crate::framing::DIRECT_ASK_HEADER_LEN
                        ..crate::framing::DIRECT_ASK_HEADER_LEN + payload_len];
                    let header = crate::framing::try_write_direct_response_header(
                        correlation_id,
                        payload_len,
                    )
                    .unwrap();
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, &header)
                        .await
                        .unwrap();
                    tokio::io::AsyncWriteExt::write_all(&mut server_io, payload)
                        .await
                        .unwrap();
                } else if control.kind == crate::framing::WireKind::ActorAsk
                    && msg_len >= crate::framing::ACTOR_ASK_HEADER_LEN
                {
                    let correlation_id = u32::from_be_bytes(msg[..4].try_into().unwrap());
                    let payload_len = msg_len - crate::framing::ACTOR_ASK_HEADER_LEN;
                    let payload = &msg[crate::framing::ACTOR_ASK_HEADER_LEN
                        ..crate::framing::ACTOR_ASK_HEADER_LEN + payload_len];
                    let header = crate::framing::try_write_ask_response_header(
                        crate::MessageType::Response,
                        correlation_id,
                        payload_len,
                    )
                    .unwrap();
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
            server_addr, ConnectionDirection::Outbound,
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            correlation,
        );

        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&client_registry),
            peer_addr: server_addr,
            session_source: server_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: client_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            server_addr, ConnectionDirection::Outbound,
            Arc::clone(&client_writer),
            CorrelationTracker::new(),
        );

        let server_read_ctx = ReadContext {
            streaming_state_handoff: None,
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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

/// R-6: the accept path's first-frame `StreamingState` must be inherited by
/// the connection's IO task via `streaming_state_handoff`, not discarded for
/// a fresh one. This reproduces that handoff directly: a `StreamingState` is
/// built exactly the way `process_read_result`'s `MessageReadResult::Streaming`
/// arm builds it when the connection's very first frame is a multi-chunk
/// `StreamStart` (the accept path processes that frame before the IO task,
/// and its own read loop, ever starts), with the first of two chunks already
/// applied. That state is placed in the handoff cell and the IO task is
/// spawned exactly as the real accept path spawns it. Only the second chunk
/// is then written to the wire.
///
/// If the IO task started from a fresh `StreamingState` instead of inheriting
/// this one, that second chunk would have no matching `active_streams` entry:
/// `reserve_v5_chunk_or_discard` would fall through to `reserve_v5_chunk`'s
/// fatal "unknown stream_id" (not a tombstoned id, so not a discard), tearing
/// the connection down before the tell is ever delivered.
#[test]
fn accept_path_streaming_state_handoff_completes_a_stream_split_across_the_first_frame() {
    run_multi_thread_test(async {
        const STRIDE: usize = 8;
        let server_addr: std::net::SocketAddr = "127.0.0.1:44101".parse().unwrap();
        let client_addr: std::net::SocketAddr = "127.0.0.1:44102".parse().unwrap();
        let delivered = Arc::new(AtomicU64::new(0));

        let server_registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            server_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("r6_handoff_server")),
                ..crate::GossipConfig::default()
            },
        ));
        server_registry
            .set_actor_message_handler_sync(Arc::new(TestActorCounter {
                delivered: Arc::clone(&delivered),
            }))
            .await;

        // Build the "already processed the first frame" StreamingState the
        // same way the accept path does: start the stream and apply its
        // inline first chunk via the legacy correlation-based API, before
        // the IO task (and its V5 direct-read reservation machinery) exists.
        let stream_id = 4_242u64;
        let total_size = (STRIDE * 2) as u64;
        let start_header = crate::StreamHeader {
            stream_id,
            total_size,
            chunk_size: STRIDE as u32,
            chunk_index: 0,
            type_hash: TEST_TELL_HASH,
            actor_id: TEST_TELL_ACTOR_ID,
        };
        let mut inherited = crate::protocol::StreamingState::new();
        inherited
            .start_stream_with_correlation_and_kind(
                start_header,
                0,
                server_registry.connection_pool.aligned_bytes_pool(),
                None,
                false,
            )
            .expect("accept path starts the stream");
        assert!(
            inherited
                .add_chunk_with_correlation(
                    start_header,
                    bytes::Bytes::from_static(&[0xAAu8; STRIDE]),
                    None,
                )
                .expect("first chunk is accepted")
                .is_none(),
            "a 1-of-2 chunk stream must not be complete yet"
        );

        let handoff = Arc::new(StreamingStateHandoff {
            cell: std::sync::Mutex::new(Some(inherited)),
            ready: tokio::sync::Notify::new(),
        });
        handoff.ready.notify_one();

        let (mut client_io, server_io) = tokio::io::duplex(1024 * 1024);

        let server_read_ctx = ReadContext {
            streaming_state_handoff: Some(Arc::clone(&handoff)),
            registry_weak: Arc::downgrade(&server_registry),
            peer_addr: client_addr,
            session_source: client_addr,
            peer_id: None,
            max_message_size: MASTER_BUFFER_SIZE,
            expected_schema_hash: None,
            aligned_pool: server_registry.connection_pool.aligned_bytes_pool(),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
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
            BufferConfig::default(),
            None,
            Some(server_read_ctx),
        );

        // The peer, unaware the accept path already consumed the first
        // frame, sends only the second (and final) chunk.
        let second_header =
            crate::framing::try_write_stream_data_header(false, stream_id as u32, 1, STRIDE)
                .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client_io, &second_header)
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client_io, &[0xBBu8; STRIDE])
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while delivered.load(Ordering::Acquire) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the inherited stream's second chunk was never delivered -- the accept \
                 path's StreamingState was not inherited by the IO task"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(delivered.load(Ordering::Acquire), 1);
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
        last_failure_instant: None,
        last_dns_refresh_attempt: None,
        last_response_received_ms: stale_time,
        accept_lower_sequence_from: None,
        current_session_source: None,
        current_session_connection: None,
        current_session_epoch: 0,
        identity_verified: false,
        transport_source_keyed: false,
    }
}

#[tokio::test]
async fn authenticated_full_sync_with_remote_loopback_bind_uses_transport_source() {
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
    let tcp_source: SocketAddr = "10.77.0.32:38988".parse().unwrap();
    let loopback_bind = "127.0.0.1:26157";
    let actor_name = "authenticated/full-sync/actor";
    let advertised_actor_addr: SocketAddr = "127.0.0.1:26158".parse().unwrap();
    let expected_actor_addr: SocketAddr = "10.77.0.32:26158".parse().unwrap();
    let advertised_actor =
        crate::RemoteActorLocation::new_with_peer(advertised_actor_addr, peer_id.clone());

    let msg = crate::registry::RegistryMessage::FullSync {
        local_actors: vec![(actor_name.to_string(), advertised_actor)],
        known_actors: Vec::new(),
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(loopback_bind.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(
        registry.clone(),
        tcp_source,
        tcp_source,
        Some(peer_id.clone()),
        msg,
    )
    .await
    .expect("authenticated FullSync should use the transport source");

    let state = registry.gossip_state.lock().await;
    let peer = state
        .peers
        .get(&tcp_source)
        .expect("the authenticated transport source must own address-keyed peer state");
    assert!(
        peer.transport_source_keyed,
        "an inbound transport source is authenticated but not a proven dial target"
    );
    assert!(!state.peers.contains_key(&loopback_bind.parse().unwrap()));
    drop(state);

    assert!(
        registry
            .connection_pool
            .peer_id_to_addr
            .read_sync(&peer_id, |_, addr| *addr)
            .is_some_and(|addr| addr == tcp_source),
        "the non-dialable self-report must not replace the authenticated transport source"
    );
    let actor = registry
        .lookup_actor(actor_name)
        .await
        .expect("authenticated actor state must not be discarded with a bad address hint");
    assert_eq!(actor.peer_id, peer_id);
    assert_eq!(
        actor.address,
        expected_actor_addr.to_string(),
        "the actor route must be repaired from the verified transport IP while preserving its service port"
    );
}

#[tokio::test]
async fn authenticated_full_sync_response_with_remote_loopback_bind_uses_transport_source() {
    let bind_addr: SocketAddr = "10.77.0.31:9501".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("loopback-response-local")),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_keypair = crate::KeyPair::new_for_testing("loopback-response-remote");
    let peer_id = peer_keypair.peer_id();
    let tcp_source: SocketAddr = "10.77.0.32:47924".parse().unwrap();
    let loopback_bind = "127.0.0.1:3883";
    let actor_name = "authenticated/full-sync-response/actor";
    let advertised_actor_addr: SocketAddr = "127.0.0.1:3884".parse().unwrap();
    let expected_actor_addr: SocketAddr = "10.77.0.32:3884".parse().unwrap();
    let advertised_actor =
        crate::RemoteActorLocation::new_with_peer(advertised_actor_addr, peer_id.clone());

    let msg = crate::registry::RegistryMessage::FullSyncResponse {
        local_actors: vec![(actor_name.to_string(), advertised_actor)],
        known_actors: Vec::new(),
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(loopback_bind.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(
        registry.clone(),
        tcp_source,
        tcp_source,
        Some(peer_id.clone()),
        msg,
    )
    .await
    .expect("authenticated FullSyncResponse should use the transport source");

    let state = registry.gossip_state.lock().await;
    let peer = state
        .peers
        .get(&tcp_source)
        .expect("the authenticated transport source must own address-keyed peer state");
    assert!(
        peer.transport_source_keyed,
        "an inbound transport source is authenticated but not a proven dial target"
    );
    assert!(!state.peers.contains_key(&loopback_bind.parse().unwrap()));
    drop(state);

    assert!(
        registry
            .connection_pool
            .peer_id_to_addr
            .read_sync(&peer_id, |_, addr| *addr)
            .is_some_and(|addr| addr == tcp_source),
        "the non-dialable self-report must not replace the authenticated transport source"
    );
    let actor = registry
        .lookup_actor(actor_name)
        .await
        .expect("authenticated actor state must not be discarded with a bad address hint");
    assert_eq!(actor.peer_id, peer_id);
    assert_eq!(
        actor.address,
        expected_actor_addr.to_string(),
        "the actor route must be repaired from the verified transport IP while preserving its service port"
    );
}

/// `handle_incoming_message`'s FullSync-response arm gates its outbound send
/// through `framing::reject_oversize_for_inline_send(GOSSIP_HEADER_LEN, ...)`
/// (the same helper `ConnectionHandle::reject_oversize_inline` uses), not a
/// hand-rolled `payload.len() > max_message_size` comparison. This measures
/// the real, current response size with an effectively unbounded
/// `max_message_size`, then reconfigures a second registry with
/// `max_message_size` set to exactly that measured payload length -- a
/// payload-only check would admit it (`payload.len() == max`), but the
/// encoded body (`GOSSIP_HEADER_LEN` + payload) exceeds it by exactly the
/// header size, and must still be rejected locally instead of reaching the
/// peer and being fatally rejected there.
#[tokio::test]
async fn full_sync_response_body_len_with_gossip_header_overhead_over_limit_is_rejected() {
    async fn registry_with_connection(
        bind_addr: SocketAddr,
        key_seed: &str,
        sender_peer_id: crate::PeerId,
        sender_addr: SocketAddr,
        max_message_size: usize,
    ) -> (
        Arc<crate::registry::GossipRegistry>,
        tokio::io::DuplexStream,
    ) {
        let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
            bind_addr,
            crate::GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing(key_seed)),
                max_message_size,
                ..crate::GossipConfig::default()
            },
        ));
        let (io, peer_io) = tokio::io::duplex(8 * 1024 * 1024);
        let (stream_handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            sender_addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut connection = LockFreeConnection::new(sender_addr, ConnectionDirection::Inbound);
        connection.stream_handle = Some(Arc::new(stream_handle));
        connection.set_state(ConnectionState::Connected);
        let connection = Arc::new(connection);
        assert!(registry.connection_pool.add_connection_by_peer_id(
            sender_peer_id,
            sender_addr,
            connection
        ));
        (registry, peer_io)
    }

    fn full_sync_msg(sender_peer_id: crate::PeerId) -> crate::registry::RegistryMessage {
        // Enough known_actors that the response the registry echoes back is
        // comfortably larger than a handful of bytes -- the exact size does
        // not matter, only that it is reproducible across both passes below.
        let known_actors: Vec<(String, crate::RemoteActorLocation)> = (0..200)
            .map(|i| {
                (
                    format!("full-sync-body-len-boundary-actor-{i:04}"),
                    crate::RemoteActorLocation::new_with_peer(
                        "10.0.0.1:9999".parse().unwrap(),
                        sender_peer_id.clone(),
                    ),
                )
            })
            .collect();
        crate::registry::RegistryMessage::FullSync {
            local_actors: Vec::new(),
            known_actors,
            sender_peer_id,
            sender_bind_addr: None,
            sequence: 1,
            wall_clock_time: crate::current_timestamp(),
            extensions: None,
        }
    }

    async fn read_gossip_frame(
        peer_io: &mut tokio::io::DuplexStream,
    ) -> Option<crate::framing::Control> {
        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            tokio::io::AsyncReadExt::read_exact(peer_io, &mut ctrl),
        )
        .await
        .ok()?
        .ok()?;
        crate::framing::decode_control(ctrl)
    }

    let sender_keypair = crate::KeyPair::new_for_testing("full-sync-body-len-boundary-remote");
    let sender_peer_id = sender_keypair.peer_id();
    let sender_addr: SocketAddr = "10.90.0.9:9401".parse().unwrap();

    // Pass 1: max_message_size effectively unbounded (still within the V5
    // 27-bit limit), so the response always gets built and sent. Read its
    // real, current encoded body length off the wire.
    //
    // The two registries' bind addresses ("10.90.0.11:9501" /
    // "10.90.0.12:9501") must be the same *string length*: each registry
    // embeds its own `advertised_addr().to_string()` as `sender_bind_addr`
    // in the response it builds, so an address-length mismatch between
    // passes would change the measured payload by that delta and the test
    // would no longer isolate GOSSIP_HEADER_LEN -- it would pass or fail for
    // reasons unrelated to the header-overhead accounting it exists to pin.
    let (registry_a, mut peer_a) = registry_with_connection(
        "10.90.0.11:9501".parse().unwrap(),
        "full-sync-body-len-boundary-local-a",
        sender_peer_id.clone(),
        sender_addr,
        crate::framing::CONTROL_BODY_LEN_MASK as usize,
    )
    .await;
    super::handle_incoming_message(
        registry_a.clone(),
        sender_addr,
        sender_addr,
        Some(sender_peer_id.clone()),
        full_sync_msg(sender_peer_id.clone()),
    )
    .await
    .expect("FullSync handling must not error");
    let control = read_gossip_frame(&mut peer_a)
        .await
        .expect("an unbounded max_message_size must let the FullSync response through");
    assert_eq!(control.kind, crate::framing::WireKind::Gossip);
    let payload_len = control.body_len - crate::framing::GOSSIP_HEADER_LEN;

    // Pass 2: same message, but max_message_size set to exactly the
    // measured payload length. A payload-only check (`payload.len() >
    // max_message_size`) would admit this -- they are equal -- but the
    // encoded body is `GOSSIP_HEADER_LEN` bytes larger than the limit, so
    // the response must be rejected locally: nothing reaches the wire.
    let (registry_b, mut peer_b) = registry_with_connection(
        "10.90.0.12:9501".parse().unwrap(),
        "full-sync-body-len-boundary-local-b",
        sender_peer_id.clone(),
        sender_addr,
        payload_len,
    )
    .await;
    super::handle_incoming_message(
        registry_b.clone(),
        sender_addr,
        sender_addr,
        Some(sender_peer_id.clone()),
        full_sync_msg(sender_peer_id.clone()),
    )
    .await
    .expect("rejecting an oversize response locally must not surface as an error");
    assert!(
        read_gossip_frame(&mut peer_b).await.is_none(),
        "a FullSync response whose encoded body exceeds max_message_size by \
         exactly GOSSIP_HEADER_LEN must be rejected locally, not sent"
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
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(
        registry.clone(),
        peer_addr,
        peer_addr,
        Some(peer_id.clone()),
        msg,
    )
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
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };

    super::handle_incoming_message(
        registry.clone(),
        peer_addr,
        peer_addr,
        Some(peer_id.clone()),
        msg,
    )
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
        sender_peer_id: peer_id.clone(),
        wall_clock_time: crate::current_timestamp(),
        precise_timing_nanos: crate::current_timestamp_nanos(),
    };
    let msg = crate::registry::RegistryMessage::DeltaGossip {
        delta,
        extensions: None,
    };

    super::handle_incoming_message(
        registry.clone(),
        peer_addr,
        peer_addr,
        Some(peer_id.clone()),
        msg,
    )
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

/// The `DeltaGossip` arm never
/// verified `delta.sender_peer_id` -- a SELF-REPORTED wire field, not an
/// authority for identity -- against the connection's actual authenticated
/// identity, unlike the `FullSync` arm right below it. An authenticated
/// peer (the "attacker" here) could send a delta CLAIMING to be a
/// different peer (the "victim") and have this arm's failure-bookkeeping
/// reset attributed to the impersonated victim's address instead of the
/// connection that actually sent it, using nothing but a forged
/// `sender_peer_id`.
///
/// Proves the fix: an authenticated connection for `attacker_id` sends a
/// `DeltaGossip` claiming `sender_peer_id: victim_id`. Asserts the call
/// still succeeds (the forged delta is silently ignored, not an error) but
/// leaves the victim's `gossip_state` failure bookkeeping completely
/// untouched -- proving the whole delta is rejected before ANY of its
/// claimed identity is trusted for anything.
#[tokio::test]
async fn delta_gossip_with_mismatched_sender_identity_is_ignored() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("lrr_delta_gossip_impersonation_local")),
            ..crate::GossipConfig::default()
        },
    ));

    let attacker_keypair = crate::KeyPair::new_for_testing("lrr_delta_gossip_attacker");
    let attacker_id = attacker_keypair.peer_id();
    let attacker_addr: SocketAddr = "10.77.0.67:9303".parse().unwrap();

    let victim_keypair = crate::KeyPair::new_for_testing("lrr_delta_gossip_victim");
    let victim_id = victim_keypair.peer_id();
    let victim_addr: SocketAddr = "10.77.0.68:9304".parse().unwrap();

    // The registry already knows the victim's real, legitimate address --
    // exactly what would exist for a genuine peer this node has
    // previously connected to or verified through other means. Without
    // this, `resolve_peer_state_addr` cannot resolve the claimed
    // `sender_peer_id` to any address at all and falls back to the raw
    // TCP source (the attacker's own address), which would make this
    // test pass regardless of whether the identity check exists --
    // proving nothing about the actual finding.
    let _ = registry
        .connection_pool
        .peer_id_to_addr
        .upsert_sync(victim_id.clone(), victim_addr);

    let stale_time = crate::current_timestamp_millis().saturating_sub(3_600_000);
    {
        let mut state = registry.gossip_state.lock().await;
        // The attacker's own connection, entirely legitimate on its own.
        state
            .peers
            .insert(attacker_addr, stale_peer_info(attacker_addr, stale_time));
        // The victim: a separate, genuinely dead-looking peer this delta
        // will try to impersonate liveness evidence for.
        state
            .peers
            .insert(victim_addr, stale_peer_info(victim_addr, stale_time));
    }

    // The delta arrives on the ATTACKER's authenticated connection but
    // CLAIMS to be from the victim.
    let delta = crate::registry::RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: Vec::new(),
        sender_peer_id: victim_id.clone(),
        wall_clock_time: crate::current_timestamp(),
        precise_timing_nanos: crate::current_timestamp_nanos(),
    };
    let msg = crate::registry::RegistryMessage::DeltaGossip {
        delta,
        extensions: None,
    };

    super::handle_incoming_message(
        registry.clone(),
        attacker_addr,
        attacker_addr,
        Some(attacker_id.clone()),
        msg,
    )
    .await
    .expect("a forged DeltaGossip must be silently ignored, not an error");

    let state = registry.gossip_state.lock().await;
    let victim_info = state
        .peers
        .get(&victim_addr)
        .expect("victim's gossip_state entry must survive untouched");
    assert_eq!(
        victim_info.failures, 1,
        "the victim's gossip_state failure bookkeeping must be untouched too -- the whole \
         forged delta must be rejected, not merely the new liveness call skipped"
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
        use crate::{GossipConfig, registry::GossipRegistry};
        // A real (if otherwise unused) registry: finalize must actually be
        // able to send its identifying FullSync, or it now fails the
        // connect outright rather than silently publishing an unidentified
        // candidate.
        let registry = Arc::new(GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("streak-guard-local")),
                ..Default::default()
            },
        ));
        let pool = registry.connection_pool.clone();
        let peer = crate::KeyPair::new_for_testing("streak_instance_guard").peer_id();
        let addr: SocketAddr = "127.0.0.1:7313".parse().unwrap();

        // Associate the address with the peer so finalize publishes it under
        // the peer id, giving us a real stream instance to pin.
        pool.add_addr_to_peer_id(addr, peer.clone());
        let (io, _keep) = tokio::io::duplex(1024);
        pool.finalize_new_outbound_connection(
            addr,
            io,
            Arc::downgrade(&registry),
            None,
            addr,
            None,
        )
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
        use crate::{GossipConfig, registry::GossipRegistry};
        // A real (if otherwise unused) registry: finalize must actually be
        // able to send its identifying FullSync, or it now fails the
        // connect outright rather than silently publishing an unidentified
        // candidate.
        let registry = Arc::new(GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("hard-fault-scoped-local")),
                ..Default::default()
            },
        ));
        let pool = registry.connection_pool.clone();
        let peer = crate::KeyPair::new_for_testing("hard_fault_instance_scoped").peer_id();
        let addr: SocketAddr = "127.0.0.1:7314".parse().unwrap();

        pool.add_addr_to_peer_id(addr, peer.clone());
        let (io, _keep) = tokio::io::duplex(1024);
        pool.finalize_new_outbound_connection(
            addr,
            io,
            Arc::downgrade(&registry),
            None,
            addr,
            None,
        )
        .await
        .expect("finalize outbound");

        let live_instance = pool
            .current_peer_connection_instance(&peer)
            .expect("live session should have a stream instance");

        let fresh_addr: SocketAddr = "127.0.0.1:7315".parse().unwrap();
        let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;

        let _guard = {
            let pool = pool.clone();
            let peer = peer.clone();
            let fresh = fresh.clone();
            crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
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
            }))
        };

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
        use crate::{GossipConfig, registry::GossipRegistry};
        // A real (if otherwise unused) registry: finalize must actually be
        // able to send its identifying FullSync, or it now fails the
        // connect outright rather than silently publishing an unidentified
        // candidate.
        let registry = Arc::new(GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("counter-balance-local")),
                ..Default::default()
            },
        ));
        let pool = registry.connection_pool.clone();
        let addr: SocketAddr = "127.0.0.1:7100".parse().unwrap();
        let (io, _peer) = tokio::io::duplex(1024);

        let _handle = pool
            .finalize_new_outbound_connection(addr, io, Arc::downgrade(&registry), None, addr, None)
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
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
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

/// R-11: a candidate that loses the outbound-finalize tie-break must not
/// strand the sequence-reset exemption on a socket that never becomes live,
/// and must not disturb the surviving live connection's own gossip.
///
/// Arming used to happen in the caller BEFORE `finalize_new_outbound_connection`
/// decided whether this candidate would actually become the peer's live
/// connection. A losing candidate (this exact scenario: a live,
/// tie-break-preferred INBOUND session already exists) would still arm
/// `current_session_source`/`accept_lower_sequence_from` to the LOSING
/// candidate's own local ephemeral port -- a value the surviving inbound
/// session's traffic can never present, since its own session source is
/// different. Every subsequent FullSync on the surviving connection would
/// then be gated against a session that never went live, silently breaking
/// its gossip.
#[tokio::test]
async fn outbound_finalize_reject_does_not_strand_the_sequence_reset_exemption() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs(
        "finalize-reject-exemption-hi",
        "finalize-reject-exemption-lo",
    );
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

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
    // peer's CURRENT connection. Its own session source (what its actual
    // traffic will present as `verified_sender_addr`) is `existing_addr`.
    let existing_addr: SocketAddr = "127.0.0.1:7450".parse().unwrap();
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

    // The peer is already known to the registry under its dial address, with
    // a session already armed for the EXISTING (surviving) connection --
    // exactly what a real prior inbound accept would have done.
    let dial_addr: SocketAddr = "127.0.0.1:7451".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);
    registry
        .add_peer_with_node_id(
            dial_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;
    registry
        .arm_sequence_reset_for_new_session(
            dial_addr,
            remote_node_id,
            existing_addr,
            &remote_peer_id,
            &existing,
        )
        .await;

    // A fresh, non-preferred OUTBOUND dial to the same peer loses the
    // tie-break (`RejectIncoming`), simulating a redial racing the peer's
    // already-live preferred inbound session.
    let losing_candidate_local_port: SocketAddr = "127.0.0.1:57999".parse().unwrap();
    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(
            dial_addr,
            io,
            registry_weak,
            None,
            losing_candidate_local_port,
            Some(remote_node_id),
        )
        .await;
    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "sanity: this must be the same reject outcome as the sibling test: got {result:?}"
    );

    // The exemption must still point at the EXISTING (surviving) session's
    // source, never at the losing candidate's local ephemeral port.
    let gossip_state = registry.gossip_state.lock().await;
    let peer_info = gossip_state
        .peers
        .get(&dial_addr)
        .expect("peer must still be tracked");
    assert_ne!(
        peer_info.current_session_source,
        Some(losing_candidate_local_port),
        "R-11: a losing candidate must not arm current_session_source to its \
         own (never-live) local ephemeral port"
    );
    assert_eq!(
        peer_info.current_session_source,
        Some(existing_addr),
        "R-11: the surviving session's own source must remain the armed \
         session, untouched by the losing candidate"
    );
    assert_ne!(
        peer_info.accept_lower_sequence_from,
        Some(losing_candidate_local_port),
        "R-11: a losing candidate must not strand the one-shot exemption on \
         a socket that never becomes live"
    );
}

/// R-11: arming and publication are two separate operations. A candidate
/// can be the peer's live connection at the moment its finalize logic
/// decides to arm, yet be superseded by a NEWER connection for the same
/// peer before that arm's own `.await` completes (e.g. `finalize_new_outbound_connection`'s
/// arm call, `connection_pool/pool_connect.rs`, runs after publication but is
/// still a separate async step a faster concurrent finalize can race past).
/// If the stale finalizer's arm is allowed to complete regardless, it
/// overwrites the newer session's `current_session_source` with its own
/// obsolete local port, and the ACTUALLY-live connection's subsequent
/// gossip then fails the `from_current_session` gate until another
/// reconnect.
///
/// Exercises `arm_sequence_reset_for_new_session` directly against real,
/// published `connection_pool` state (the exact primitive
/// `finalize_new_outbound_connection`'s arm call delegates to) rather than
/// re-driving the whole TLS/finalize pipeline, since the race is entirely
/// about the ordering between publication and this specific call.
#[tokio::test]
async fn outbound_stale_finalizer_arm_after_supersession_does_not_clobber_newer_session() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("stale-arm-supersede-hi", "stale-arm-supersede-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    // The instance-supersession check consults `registry.connection_pool`
    // directly, so publication must go through that SAME pool (not a
    // standalone one) for the check to observe it.
    let pool = registry.connection_pool.clone();

    let dial_addr: SocketAddr = "127.0.0.1:7460".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);
    registry
        .add_peer_with_node_id(
            dial_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    // The STALE outbound candidate: published first, but its own arm call
    // is delayed (simulating a slow task scheduling / lost race).
    let stale_local_port: SocketAddr = "127.0.0.1:58001".parse().unwrap();
    let (stale_io, _stale_keep) = tokio::io::duplex(1024);
    let (stale_sh, _stale_w, _stale_r) = LockFreeStreamHandle::new(
        stale_io,
        dial_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut stale_conn = LockFreeConnection::new(dial_addr, ConnectionDirection::Outbound);
    stale_conn.stream_handle = Some(Arc::new(stale_sh));
    stale_conn.set_state(ConnectionState::Connected);
    let stale = Arc::new(stale_conn);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), dial_addr, stale.clone()));

    // A NEWER outbound candidate wins a concurrent redial and is published,
    // superseding the stale one as the peer's current connection -- and
    // arms correctly, since it IS current at that moment.
    let newer_local_port: SocketAddr = "127.0.0.1:58002".parse().unwrap();
    let (newer_io, _newer_keep) = tokio::io::duplex(1024);
    let (newer_sh, _newer_w, _newer_r) = LockFreeStreamHandle::new(
        newer_io,
        dial_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut newer_conn = LockFreeConnection::new(dial_addr, ConnectionDirection::Outbound);
    newer_conn.stream_handle = Some(Arc::new(newer_sh));
    newer_conn.set_state(ConnectionState::Connected);
    let newer = Arc::new(newer_conn);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), dial_addr, newer.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            dial_addr,
            remote_node_id,
            newer_local_port,
            &remote_peer_id,
            &newer,
        )
        .await;

    // The stale finalizer's own arm call FINALLY completes now, after
    // having been superseded. It must be a no-op.
    registry
        .arm_sequence_reset_for_new_session(
            dial_addr,
            remote_node_id,
            stale_local_port,
            &remote_peer_id,
            &stale,
        )
        .await;

    {
        let gossip_state = registry.gossip_state.lock().await;
        let peer_info = gossip_state
            .peers
            .get(&dial_addr)
            .expect("peer must still be tracked");
        assert_ne!(
            peer_info.current_session_source,
            Some(stale_local_port),
            "R-11: a stale finalizer's delayed arm must not overwrite the \
             newer, currently-published session's discriminator"
        );
        assert_eq!(
            peer_info.current_session_source,
            Some(newer_local_port),
            "R-11: the newer session's discriminator must remain untouched \
             by the stale finalizer"
        );
    }

    // The newer (actually live) connection's subsequent, advancing-sequence
    // FullSync must still be accepted -- proof the stale arm did not
    // silently break its gossip.
    let mut local_actors = std::collections::HashMap::new();
    local_actors.insert(
        "stale-arm/Q".to_string(),
        crate::RemoteActorLocation::new_with_peer(dial_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            local_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            dial_addr,
            Some(newer_local_port),
            Some(newer_local_port),
            1,
            crate::current_timestamp(),
        )
        .await;
    assert!(
        registry.lookup_actor("stale-arm/Q").await.is_some(),
        "R-11: the actually-live (newer) connection's FullSync must still \
         be accepted after the stale arm attempt"
    );
}

/// R-11: the supersession recheck inside `arm_sequence_reset_for_new_session`
/// must be atomic with the write it guards -- checked WHILE holding the
/// `gossip_state` lock, not checked, then released, then reacquired to
/// write. A version with a gap between the two would let a stale task pass
/// the check, get descheduled before it ever touches the lock, let a newer
/// connection publish in the meantime, and then resume and clobber that
/// newer session's discriminator anyway even though it is no longer
/// current.
///
/// Forces exactly that interleaving deterministically: the test itself
/// holds the `gossip_state` lock while spawning the stale task and letting
/// it run up to (but not past) its own lock acquisition -- proving whether
/// the connection-pool check happens before or after that point. A version
/// that checks BEFORE attempting the lock would observe "still current"
/// here (the newer connection hasn't published yet) and only discover it
/// was wrong after acquiring the lock and blindly writing; the fixed
/// version, which acquires the lock FIRST and checks only once it holds
/// it, sees the newer connection because publication happens while the
/// stale task is still parked waiting for the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_sequence_reset_stale_task_racing_the_lock_does_not_clobber_newer_session() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("stale-arm-race-hi", "stale-arm-race-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let dial_addr: SocketAddr = "127.0.0.1:7461".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);
    registry
        .add_peer_with_node_id(
            dial_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    fn make_connection(addr: SocketAddr) -> Arc<LockFreeConnection> {
        let (io, _keep) = tokio::io::duplex(1024);
        let (sh, _w, _r) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::Global,
            BufferConfig::default(),
            None,
            None,
        );
        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        conn.stream_handle = Some(Arc::new(sh));
        conn.set_state(ConnectionState::Connected);
        Arc::new(conn)
    }

    let stale_local_port: SocketAddr = "127.0.0.1:58101".parse().unwrap();
    let stale = make_connection(dial_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), dial_addr, stale.clone()));

    // Hold `gossip_state` from the test task, so the stale task's own
    // `arm_sequence_reset_for_new_session` call -- whose first move under
    // the fixed design is to acquire this exact lock before doing anything
    // else -- cannot proceed past that point until released below.
    let guard = registry.gossip_state.lock().await;

    let stale_registry = registry.clone();
    let stale_peer_id = remote_peer_id.clone();
    let stale_conn = stale.clone();
    let stale_task = tokio::spawn(async move {
        stale_registry
            .arm_sequence_reset_for_new_session(
                dial_addr,
                remote_node_id,
                stale_local_port,
                &stale_peer_id,
                &stale_conn,
            )
            .await;
    });

    // Give the stale task a real chance to run up to its lock acquisition
    // (or, on a pre-fix build, to run its whole pre-lock check) before the
    // newer connection is published.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // The NEWER connection is published WHILE the stale task is still
    // parked waiting for the lock this test task holds.
    let newer = make_connection(dial_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), dial_addr, newer.clone()));

    // Release the lock: the stale task's arm call can now proceed. Its
    // in-lock recheck must observe the newer connection published above.
    drop(guard);
    stale_task.await.expect("stale task must not panic");

    let gossip_state = registry.gossip_state.lock().await;
    let peer_info = gossip_state
        .peers
        .get(&dial_addr)
        .expect("peer must still be tracked");
    assert_ne!(
        peer_info.current_session_source,
        Some(stale_local_port),
        "R-11: a stale task that started its arm call before a newer \
         connection published, but only reaches its recheck after, must \
         not clobber the newer session's discriminator"
    );
    assert!(
        peer_info.current_session_source.is_none(),
        "R-11: no session was ever successfully armed here (the newer \
         connection was published but never armed), so the discriminator \
         must remain unset rather than being set by the stale task"
    );
}

/// R-11: an old, still-draining connection's DeltaGossip must not be able
/// to restore a restarted peer's pre-restart high-water mark. Before this
/// fix, only `FullSync`/`FullSyncResponse` were gated on
/// `current_session_source`; both `DeltaGossip` request and response arms
/// in `handle_incoming_message` accepted traffic from ANY connection and
/// the request arm unconditionally advanced `last_sequence`. After a
/// restart's exemption is consumed and the sequence resets low, a single
/// in-flight delta from the OLD connection carrying the numerically HIGH
/// pre-restart sequence would silently restore it -- after which the
/// current session's own subsequent low FullSyncs look stale again with no
/// exemption left to rescue them.
#[tokio::test]
async fn old_draining_connection_delta_cannot_restore_pre_restart_high_water() {
    let bind_addr: SocketAddr = "10.77.0.40:9500".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("delta-gate-local")),
            ..crate::GossipConfig::default()
        },
    ));
    let owner_kp = crate::KeyPair::new_for_testing("delta-gate-owner");
    let owner = owner_kp.peer_id();
    let node_id = owner.to_node_id();
    let peer_addr: SocketAddr = "10.77.0.41:9500".parse().unwrap();

    // Every message in this test resolves to the SAME peer_state_addr
    // (`peer_addr`) regardless of which physical connection delivered it.
    registry
        .connection_pool
        .peer_id_to_addr
        .upsert_sync(owner.clone(), peer_addr);

    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    // Pre-restart: peer is at sequence 40 via a genuine FullSync from the
    // OLD connection.
    let old_connection: SocketAddr = "10.77.0.40:51001".parse().unwrap();
    let full_sync = crate::registry::RegistryMessage::FullSync {
        local_actors: vec![],
        known_actors: vec![],
        sender_peer_id: owner.clone(),
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 40,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        old_connection,
        old_connection,
        Some(owner.clone()),
        full_sync,
    )
    .await
    .expect("pre-restart FullSync must succeed");

    // Peer restarts: new session armed and its first sync (seq=1) accepted.
    let new_connection: SocketAddr = "10.77.0.40:51002".parse().unwrap();
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            node_id,
            new_connection,
            &owner,
            &qa_r11_delta_gate_dummy_connection(new_connection),
        )
        .await;
    let restart_sync = crate::registry::RegistryMessage::FullSync {
        local_actors: vec![],
        known_actors: vec![],
        sender_peer_id: owner.clone(),
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        new_connection,
        new_connection,
        Some(owner.clone()),
        restart_sync,
    )
    .await
    .expect("restart FullSync must succeed");
    {
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(
            gossip_state.peers.get(&peer_addr).map(|p| p.last_sequence),
            Some(1),
            "sanity: the restart sync must have reset last_sequence to 1"
        );
    }

    // The OLD connection is still draining and delivers an in-flight,
    // pre-restart (numerically HIGH) delta AFTER the reset.
    let stale_delta = crate::registry::RegistryMessage::DeltaGossip {
        delta: crate::registry::RegistryDelta {
            since_sequence: 39,
            current_sequence: 41,
            changes: Vec::new(),
            sender_peer_id: owner.clone(),
            wall_clock_time: crate::current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        },
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        old_connection,
        old_connection,
        Some(owner.clone()),
        stale_delta,
    )
    .await
    .expect("stale delta must not error, only be ignored");

    {
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(
            gossip_state.peers.get(&peer_addr).map(|p| p.last_sequence),
            Some(1),
            "R-11: the old draining connection's delta must not restore the \
             pre-restart high-water mark"
        );
    }

    // The restarted peer's SECOND genuine sync (seq=2), from the NEW
    // connection, must still be accepted -- proven by a brand-new actor
    // actually being added.
    let mut local_actors = std::collections::HashMap::new();
    local_actors.insert(
        "delta-gate/Q".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, owner.clone()),
    );
    let second_sync = crate::registry::RegistryMessage::FullSync {
        local_actors: local_actors.into_iter().collect(),
        known_actors: vec![],
        sender_peer_id: owner.clone(),
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 2,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        new_connection,
        new_connection,
        Some(owner.clone()),
        second_sync,
    )
    .await
    .expect("second restart-session FullSync must succeed");

    assert!(
        registry.lookup_actor("delta-gate/Q").await.is_some(),
        "R-11: the restarted peer's second genuine sync must still be \
         accepted, not rejected as stale because the old connection's \
         delta silently restored the pre-restart high-water mark"
    );
}

fn qa_r11_delta_gate_dummy_connection(addr: SocketAddr) -> Arc<LockFreeConnection> {
    let (io, _keep) = tokio::io::duplex(1024);
    let (sh, _w, _r) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Inbound);
    conn.stream_handle = Some(Arc::new(sh));
    conn.set_state(ConnectionState::Connected);
    Arc::new(conn)
}

/// R-11: a non-arming successor connection (e.g. a reconnect whose TLS
/// certificate doesn't decode to a `GossipNodeId`, so `arm_sequence_reset_for_new_session`
/// is never called for it) must not be permanently locked out once the
/// connection that DID arm a session for this peer is gone. Before this
/// fix, `current_session_source` was set only inside the arm path and
/// never cleared anywhere -- if the arming connection closed and was
/// succeeded by a connection that never re-armed, EVERY subsequent
/// FullSync from that live successor would fail the `from_current_session`
/// gate forever, recreating the exact stale-actor outage via a new
/// mechanism.
#[tokio::test]
async fn non_arming_successor_connections_full_sync_is_accepted_after_armed_connection_closes() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("non-arming-successor-hi", "non-arming-successor-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let bind_addr: SocketAddr = "127.0.0.1:7470".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, bind_addr);
    registry
        .add_peer_with_node_id(
            bind_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    // Connection A: authenticates with a decodable GossipNodeId and arms a
    // session.
    let a_addr: SocketAddr = "127.0.0.1:59001".parse().unwrap();
    let (a_io, _a_keep) = tokio::io::duplex(1024);
    let (a_sh, _a_w, _a_r) = LockFreeStreamHandle::new(
        a_io,
        a_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut a_conn = LockFreeConnection::new(a_addr, ConnectionDirection::Inbound);
    a_conn.stream_handle = Some(Arc::new(a_sh));
    a_conn.set_state(ConnectionState::Connected);
    let a = Arc::new(a_conn);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), a_addr, a.clone()));
    registry
        .arm_sequence_reset_for_new_session(bind_addr, remote_node_id, a_addr, &remote_peer_id, &a)
        .await;

    // A baseline FullSync from A establishes last_sequence.
    let mut local_actors = std::collections::HashMap::new();
    local_actors.insert(
        "non-arming/X".to_string(),
        crate::RemoteActorLocation::new_with_peer(bind_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            local_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            bind_addr,
            Some(a_addr),
            Some(a_addr),
            5,
            crate::current_timestamp(),
        )
        .await;

    // Connection A closes and is dropped entirely (no strong refs left);
    // connection B succeeds it but is a non-arming successor (e.g. its
    // certificate never decoded to a GossipNodeId, so nothing ever calls
    // `arm_sequence_reset_for_new_session` for it) -- it is simply
    // published as the peer's new current connection.
    drop(a);
    let b_addr: SocketAddr = "127.0.0.1:59002".parse().unwrap();
    let (b_io, _b_keep) = tokio::io::duplex(1024);
    let (b_sh, _b_w, _b_r) = LockFreeStreamHandle::new(
        b_io,
        b_addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut b_conn = LockFreeConnection::new(b_addr, ConnectionDirection::Inbound);
    b_conn.stream_handle = Some(Arc::new(b_sh));
    b_conn.set_state(ConnectionState::Connected);
    let b = Arc::new(b_conn);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), b_addr, b.clone()));

    // B's own, advancing-sequence FullSync must be accepted -- proven by a
    // brand-new actor actually being added.
    let mut local_actors2 = std::collections::HashMap::new();
    local_actors2.insert(
        "non-arming/X".to_string(),
        crate::RemoteActorLocation::new_with_peer(bind_addr, remote_peer_id.clone()),
    );
    local_actors2.insert(
        "non-arming/R".to_string(),
        crate::RemoteActorLocation::new_with_peer(bind_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            local_actors2,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            bind_addr,
            Some(b_addr),
            Some(b_addr),
            6,
            crate::current_timestamp(),
        )
        .await;

    assert!(
        registry.lookup_actor("non-arming/R").await.is_some(),
        "R-11: a live, non-arming successor connection's advancing FullSync \
         must be accepted once the connection that armed the old session \
         is gone, not rejected forever by a stale current_session_source"
    );
}

fn qa_r11_generation_race_connection(addr: SocketAddr) -> Arc<LockFreeConnection> {
    let (io, _keep) = tokio::io::duplex(1024);
    let (sh, _w, _r) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Inbound);
    conn.stream_handle = Some(Arc::new(sh));
    conn.set_state(ConnectionState::Connected);
    Arc::new(conn)
}

/// R-11: a `FullSync` apply that validated against the OLD (still current
/// at the time) session, but whose actual `known_actors`/`peer_to_actors`
/// mutation runs only after a NEWER session has since armed and completed
/// its own (lower-sequence, restart) `FullSync`, must not be allowed to
/// overwrite the newer session's state with its own stale snapshot.
///
/// `merge_full_sync_from` validates the session and updates
/// `last_sequence` under one lock, then collects and resolves candidate
/// actor updates with NO lock held, then re-acquires the lock to actually
/// apply them. `FullSyncApplyPendingMutation` fires in exactly that gap,
/// letting this test deterministically land a newer session's full
/// arm-and-restart there -- the same gap a genuinely delayed/descheduled
/// task would pause in -- and prove the generation recheck drops the
/// stale apply rather than applying it on top of the newer session's
/// already-correct state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_sync_stale_apply_paused_between_validation_and_mutation_is_dropped() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("fullsync-race-hi", "fullsync-race-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let peer_addr: SocketAddr = "127.0.0.1:7480".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, peer_addr);
    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    let old_addr: SocketAddr = "127.0.0.1:59101".parse().unwrap();
    let old_conn = qa_r11_generation_race_connection(old_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), old_addr, old_conn.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            remote_node_id,
            old_addr,
            &remote_peer_id,
            &old_conn,
        )
        .await;

    // Baseline: OLD's own prior FullSync establishes "SURVIVOR" at sequence 40.
    let mut baseline_actors = std::collections::HashMap::new();
    baseline_actors.insert(
        "fs-race/SURVIVOR".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            baseline_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(old_addr),
            Some(old_addr),
            40,
            crate::current_timestamp(),
        )
        .await;

    let new_addr: SocketAddr = "127.0.0.1:59102".parse().unwrap();

    let _guard = {
        let pool = pool.clone();
        let registry_for_hook = registry.clone();
        let peer_id = remote_peer_id.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            let crate::TransportLifecycleEvent::FullSyncApplyPendingMutation {
                peer: event_peer,
                ..
            } = &event
            else {
                return;
            };
            if *event_peer != peer_id {
                return;
            }
            // Fire once: deregister before doing anything else so this
            // hook cannot recursively re-enter itself via the FullSync
            // this closure is about to drive.
            crate::set_transport_lifecycle_recorder(None);

            let new_conn = qa_r11_generation_race_connection(new_addr);
            assert!(pool.add_connection_by_peer_id(peer_id.clone(), new_addr, new_conn.clone()));

            let registry_for_hook = registry_for_hook.clone();
            let peer_id = peer_id.clone();
            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    registry_for_hook
                        .arm_sequence_reset_for_new_session(
                            peer_addr,
                            remote_node_id,
                            new_addr,
                            &peer_id,
                            &new_conn,
                        )
                        .await;

                    // NEW's restart: lower sequence, a full snapshot
                    // advertising ONLY "NEW" -- correctly omission-prunes
                    // "SURVIVOR".
                    let mut restart_actors = std::collections::HashMap::new();
                    restart_actors.insert(
                        "fs-race/NEW".to_string(),
                        crate::RemoteActorLocation::new_with_peer(peer_addr, peer_id.clone()),
                    );
                    registry_for_hook
                        .merge_full_sync_from(
                            restart_actors,
                            std::collections::HashMap::new(),
                            peer_id.clone(),
                            peer_addr,
                            Some(new_addr),
                            Some(new_addr),
                            1,
                            crate::current_timestamp(),
                        )
                        .await;
                })
            });
        }))
    };

    // OLD's own subsequent, advancing-sequence FullSync -- its own stale,
    // pre-restart-continuation snapshot, listing only "STALE". Validates
    // fine (OLD is still current at STEP 1 time), but by the time its
    // STEP 2 actually runs (right after the hook above completes), NEW's
    // restart has already superseded it.
    let mut stale_actors = std::collections::HashMap::new();
    stale_actors.insert(
        "fs-race/STALE".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            stale_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(old_addr),
            Some(old_addr),
            41,
            crate::current_timestamp(),
        )
        .await;

    assert!(
        registry.lookup_actor("fs-race/NEW").await.is_some(),
        "R-11: the newer session's restart FullSync must have been applied"
    );
    assert!(
        registry.lookup_actor("fs-race/STALE").await.is_none(),
        "R-11: the stale FullSync's actor-mutation must be dropped once the \
         generation recheck detects it was superseded, not applied on top \
         of the newer session's already-correct state"
    );
}

/// R-11: the session epoch must be a globally non-recycled value, not a
/// per-peer counter reset to 0 on every fresh `PeerInfo`. A per-peer
/// counter is an ABA hole: if a `current_session_epoch == 1` (that
/// peer's own first-ever arm) apply pauses between validation and
/// mutation while the peer entry is removed and recreated at the SAME
/// address -- e.g. a dead-peer sweep followed by a fresh accept -- the
/// replacement's own first-ever arm would ALSO produce `1` under a
/// locally-reset scheme, and the stale apply's captured-epoch recheck
/// would wrongly pass, applying the pre-removal snapshot on top of the
/// replacement session's state.
///
/// Reuses the `FullSyncApplyPendingMutation` seam: the hook removes the
/// peer entry outright and recreates it at the same address before
/// arming the replacement, so the replacement's arm is genuinely that
/// fresh `PeerInfo`'s first-ever arm (the exact ABA precondition), not
/// merely a second arm on the same still-live entry (already covered by
/// the sibling supersession tests).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_sync_stale_apply_survives_peer_entry_removal_and_recreation_at_same_addr() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("epoch-aba-hi", "epoch-aba-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let peer_addr: SocketAddr = "127.0.0.1:7483".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, peer_addr);

    // A brand-new peer entry; this is its FIRST-EVER arm (current_session_epoch
    // starts at the "never armed" sentinel 0), the precondition an
    // ABA-vulnerable per-peer counter would collide on after recreation.
    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;
    let old_addr: SocketAddr = "127.0.0.1:59401".parse().unwrap();
    let old_conn = qa_r11_generation_race_connection(old_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), old_addr, old_conn.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            remote_node_id,
            old_addr,
            &remote_peer_id,
            &old_conn,
        )
        .await;

    let mut baseline_actors = std::collections::HashMap::new();
    baseline_actors.insert(
        "epoch-aba/SURVIVOR".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            baseline_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(old_addr),
            Some(old_addr),
            40,
            crate::current_timestamp(),
        )
        .await;

    let new_addr: SocketAddr = "127.0.0.1:59402".parse().unwrap();

    let _guard = {
        let pool = pool.clone();
        let registry_for_hook = registry.clone();
        let peer_id = remote_peer_id.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            let crate::TransportLifecycleEvent::FullSyncApplyPendingMutation {
                peer: event_peer,
                ..
            } = &event
            else {
                return;
            };
            if *event_peer != peer_id {
                return;
            }
            crate::set_transport_lifecycle_recorder(None);

            let new_conn = qa_r11_generation_race_connection(new_addr);
            assert!(pool.add_connection_by_peer_id(peer_id.clone(), new_addr, new_conn.clone()));

            let registry_for_hook = registry_for_hook.clone();
            let peer_id = peer_id.clone();
            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    // The peer entry is removed OUTRIGHT and recreated at
                    // the SAME address -- the ABA precondition, not merely
                    // a second arm on the same still-live entry.
                    {
                        let mut gossip_state = registry_for_hook.gossip_state.lock().await;
                        gossip_state.peers.remove(&peer_addr);
                    }
                    registry_for_hook
                        .add_peer_with_node_id(
                            peer_addr,
                            Some(remote_node_id),
                            crate::addr_ownership::ClaimKind::Verified,
                        )
                        .await;

                    // The replacement's arm is this FRESH PeerInfo's
                    // first-ever arm -- exactly what would reproduce `1`
                    // under a locally-reset, per-peer counter.
                    registry_for_hook
                        .arm_sequence_reset_for_new_session(
                            peer_addr,
                            remote_node_id,
                            new_addr,
                            &peer_id,
                            &new_conn,
                        )
                        .await;

                    let mut restart_actors = std::collections::HashMap::new();
                    restart_actors.insert(
                        "epoch-aba/NEW".to_string(),
                        crate::RemoteActorLocation::new_with_peer(peer_addr, peer_id.clone()),
                    );
                    registry_for_hook
                        .merge_full_sync_from(
                            restart_actors,
                            std::collections::HashMap::new(),
                            peer_id.clone(),
                            peer_addr,
                            Some(new_addr),
                            Some(new_addr),
                            1,
                            crate::current_timestamp(),
                        )
                        .await;
                })
            });
        }))
    };

    // OLD's own subsequent, advancing-sequence FullSync -- validates fine
    // (OLD is still current at STEP 1 time, before the removal/recreation
    // above), but by the time its STEP 2 actually runs, the entry it
    // validated against has been removed and replaced outright.
    let mut stale_actors = std::collections::HashMap::new();
    stale_actors.insert(
        "epoch-aba/STALE".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            stale_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(old_addr),
            Some(old_addr),
            41,
            crate::current_timestamp(),
        )
        .await;

    assert!(
        registry.lookup_actor("epoch-aba/NEW").await.is_some(),
        "R-11: the replacement session's restart FullSync must have been applied"
    );
    assert!(
        registry.lookup_actor("epoch-aba/STALE").await.is_none(),
        "R-11: the stale apply captured against the REMOVED peer entry must \
         not be accepted just because the recreated entry's first-ever arm \
         happens to look like the same session generation number -- the \
         epoch must be a globally non-recycled value that a recreated entry \
         can never reproduce"
    );
}

/// R-11: `peer_info_is_from_current_session`'s three explicit cases,
/// exercised end to end: (1) the armed connection's own traffic is
/// accepted while nothing supersedes it; (2) once `connection_pool` shows
/// a DIFFERENT connection as current, a LATE message that still arrives on
/// the OLD armed source is REJECTED outright -- not merely "not
/// self-healed but still accepted" (an earlier, incomplete fix), and not
/// treated as evidence of a live successor; (3) the successor's own
/// traffic (a different `session_source`) still self-heals and is
/// accepted normally.
///
/// Case 2 is the crux: before this fix, the OLD connection's own traffic
/// falling through to the ordinary (unhealed) "matches
/// `current_session_source`" path would still be ACCEPTED -- consuming
/// the exemption and/or restoring a stale high-water mark -- even though
/// `connection_pool` already shows it superseded. That let the OLD
/// connection stay authoritative indefinitely as long as it kept talking,
/// silently reintroducing the exact stale-write class of bug R-11 exists
/// to close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_message_from_superseded_armed_source_is_rejected_not_self_healed() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("superseded-armed-source-hi", "superseded-armed-source-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let peer_addr: SocketAddr = "127.0.0.1:7484".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, peer_addr);
    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    // Baseline established BEFORE any session is armed: last_sequence=40,
    // no exemption in play yet.
    let mut baseline_actors = std::collections::HashMap::new();
    baseline_actors.insert(
        "superseded-armed/SURVIVOR".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            baseline_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            None,
            None,
            40,
            crate::current_timestamp(),
        )
        .await;

    // Connection A: authenticates and arms -- its restart exemption is now
    // live and unconsumed.
    let a_addr: SocketAddr = "127.0.0.1:59501".parse().unwrap();
    let a_conn = qa_r11_generation_race_connection(a_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), a_addr, a_conn.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            remote_node_id,
            a_addr,
            &remote_peer_id,
            &a_conn,
        )
        .await;

    // Case 1 sanity: A's own traffic is accepted while nothing supersedes
    // it yet -- checked via `current_session_source` rather than another
    // FullSync, so the still-unconsumed exemption armed above survives
    // intact into case 2 below.
    {
        let gossip_state = registry.gossip_state.lock().await;
        let peer_info = gossip_state
            .peers
            .get(&peer_addr)
            .expect("peer must still be tracked");
        assert_eq!(
            peer_info.current_session_source,
            Some(a_addr),
            "sanity: A is the armed, current session before anything supersedes it"
        );
        assert_eq!(
            peer_info.accept_lower_sequence_from,
            Some(a_addr),
            "sanity: A's restart exemption is armed and unconsumed"
        );
    }

    // Connection B (a live successor) is published, becoming
    // `connection_pool`'s current entry for the peer WITHOUT itself ever
    // arming (e.g. a cert-type migration, a non-mTLS client, or simply a
    // node_id mismatch on B's own accept).
    let b_addr: SocketAddr = "127.0.0.1:59502".parse().unwrap();
    let b_conn = qa_r11_generation_race_connection(b_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), b_addr, b_conn.clone()));

    // Case 2: a LATE message, still on A's own (now-superseded) armed
    // source, tries to restore a high-water mark and consume the
    // exemption via a lower-sequence "restart".
    let mut stale_a_actors = std::collections::HashMap::new();
    stale_a_actors.insert(
        "superseded-armed/STALE".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            stale_a_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(a_addr),
            Some(a_addr),
            1,
            crate::current_timestamp(),
        )
        .await;

    assert!(
        registry
            .lookup_actor("superseded-armed/STALE")
            .await
            .is_none(),
        "R-11: a late message from the superseded armed source must be \
         rejected outright, never accepted via the exemption or any \
         fallback"
    );
    assert!(
        registry
            .lookup_actor("superseded-armed/SURVIVOR")
            .await
            .is_some(),
        "R-11: the rejected message must not omission-prune actors either -- \
         it must have no effect on state at all"
    );
    {
        let gossip_state = registry.gossip_state.lock().await;
        let peer_info = gossip_state
            .peers
            .get(&peer_addr)
            .expect("peer must still be tracked");
        assert_eq!(
            peer_info.current_session_source,
            Some(a_addr),
            "R-11: a rejected message from the superseded armed source must \
             not trigger the self-heal clear either -- current_session_source \
             stays exactly as it was until the successor's OWN traffic heals it"
        );
        assert_eq!(
            peer_info.last_sequence, 40,
            "R-11: the rejected message must not have restored/advanced the \
             high-water mark"
        );
        assert_eq!(
            peer_info.accept_lower_sequence_from,
            Some(a_addr),
            "R-11: the rejected message must not have consumed the exemption"
        );
    }

    // Case 3: B's OWN traffic (a different session_source) still
    // self-heals and is accepted normally.
    let mut b_actors = std::collections::HashMap::new();
    b_actors.insert(
        "superseded-armed/FROM_B".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            b_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(b_addr),
            Some(b_addr),
            42,
            crate::current_timestamp(),
        )
        .await;
    assert!(
        registry
            .lookup_actor("superseded-armed/FROM_B")
            .await
            .is_some(),
        "R-11: the live successor's own traffic must still self-heal and be \
         accepted"
    );
}

/// R-11: self-heal must confirm the RECEIVING connection instance is the
/// pool's actual CURRENT PUBLISHED connection for the peer, not merely
/// that the message's `session_source` differs from the armed source.
/// During rapid reconnects a THIRD connection -- neither the armed one nor
/// the genuine live successor -- can also present a `session_source`
/// different from the armed one (e.g. a stale in-flight candidate, or one
/// that lost a tie-break and never itself got published). Before this fix,
/// any such traffic satisfied the case-3 self-heal condition, clearing the
/// session guards and getting accepted -- restoring a stale snapshot and
/// leaving the real successor's own subsequent traffic rejected because
/// the guards it should have inherited were clobbered by an impostor.
#[tokio::test]
async fn stale_third_connection_cannot_self_heal_while_a_different_successor_is_published() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("third-conn-hi", "third-conn-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let peer_addr: SocketAddr = "127.0.0.1:7486".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, peer_addr);
    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    // Baseline, before any session is armed.
    let mut baseline_actors = std::collections::HashMap::new();
    baseline_actors.insert(
        "third-conn/SURVIVOR".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            baseline_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            None,
            None,
            40,
            crate::current_timestamp(),
        )
        .await;

    // Connection A arms the session; its restart exemption is live.
    let a_addr: SocketAddr = "127.0.0.1:59601".parse().unwrap();
    let a_conn = qa_r11_generation_race_connection(a_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), a_addr, a_conn.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            remote_node_id,
            a_addr,
            &remote_peer_id,
            &a_conn,
        )
        .await;

    // Connection B is published as the pool's current connection for the
    // peer -- the genuine live successor -- but never arms itself (e.g. a
    // cert-type migration or node_id mismatch on its own accept).
    let b_addr: SocketAddr = "127.0.0.1:59602".parse().unwrap();
    let b_conn = qa_r11_generation_race_connection(b_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), b_addr, b_conn.clone()));

    // Connection C is a THIRD connection: its own `session_source` differs
    // from both A (armed) and B (published current), and it was never
    // itself published into the pool (simulating a stale in-flight
    // candidate or a tie-break loser). Its traffic must be rejected, not
    // self-healed.
    let c_addr: SocketAddr = "127.0.0.1:59603".parse().unwrap();
    let mut c_actors = std::collections::HashMap::new();
    c_actors.insert(
        "third-conn/FROM_C".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            c_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(c_addr),
            Some(c_addr),
            1,
            crate::current_timestamp(),
        )
        .await;

    assert!(
        registry.lookup_actor("third-conn/FROM_C").await.is_none(),
        "R-11: a third connection that is neither the armed connection nor \
         the pool's actual current published connection must not be \
         accepted, even though its session_source differs from the armed \
         source"
    );
    assert!(
        registry.lookup_actor("third-conn/SURVIVOR").await.is_some(),
        "R-11: the rejected third-connection message must not omission-prune \
         actors either"
    );
    {
        let gossip_state = registry.gossip_state.lock().await;
        let peer_info = gossip_state
            .peers
            .get(&peer_addr)
            .expect("peer must still be tracked");
        assert_eq!(
            peer_info.current_session_source,
            Some(a_addr),
            "R-11: a rejected third-connection message must not trigger the \
             self-heal clear -- current_session_source stays exactly as it \
             was until the ACTUAL published successor's own traffic heals it"
        );
        assert_eq!(
            peer_info.last_sequence, 40,
            "R-11: the rejected third-connection message must not have \
             advanced/restored the high-water mark"
        );
        assert_eq!(
            peer_info.accept_lower_sequence_from,
            Some(a_addr),
            "R-11: the rejected third-connection message must not have \
             consumed A's still-unspent exemption"
        );
    }

    // The ACTUAL published successor's (B's) own traffic still self-heals
    // and is accepted normally.
    let mut b_actors = std::collections::HashMap::new();
    b_actors.insert(
        "third-conn/FROM_B".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            b_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(b_addr),
            Some(b_addr),
            42,
            crate::current_timestamp(),
        )
        .await;
    assert!(
        registry.lookup_actor("third-conn/FROM_B").await.is_some(),
        "R-11: the genuinely published successor's own traffic must still \
         self-heal and be accepted"
    );
}

/// R-11: a stale `FullSync`/`FullSyncResponse` arriving on an old,
/// no-longer-current connection must not reset the peer's failure/health
/// bookkeeping (`failures`, `last_failure_time`, `last_success`,
/// `last_response_received_ms`) or `consecutive_deltas`. Before this fix
/// those fields were reset UNCONDITIONALLY, before (FullSync) or
/// independent of (FullSyncResponse) the session-scoped merge -- so even
/// though the merge itself correctly dropped the stale content, the
/// failure-state reset had already applied, masking real peer
/// unresponsiveness and perturbing `should_use_delta_state`'s strategy
/// choice via a stale `consecutive_deltas`.
#[tokio::test]
async fn stale_full_sync_and_response_on_old_connection_do_not_reset_health_bookkeeping() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("health-gate-hi", "health-gate-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let peer_addr: SocketAddr = "127.0.0.1:7485".parse().unwrap();
    registry
        .connection_pool
        .peer_id_to_addr
        .upsert_sync(remote_peer_id.clone(), peer_addr);
    pool.set_configured_peer_addr(&remote_peer_id, peer_addr);
    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    // Seed failure/health state that a stale message must NOT be able to
    // touch.
    let stale_last_failure_time = 123_456;
    let stale_last_response_ms = 1;
    {
        let mut gossip_state = registry.gossip_state.lock().await;
        let peer_info = gossip_state
            .peers
            .get_mut(&peer_addr)
            .expect("peer must be tracked");
        peer_info.failures = 3;
        peer_info.last_failure_time = Some(stale_last_failure_time);
        peer_info.last_failure_instant = Some(std::time::Instant::now());
        peer_info.last_success = 0;
        peer_info.last_response_received_ms = stale_last_response_ms;
        peer_info.consecutive_deltas = 7;
    }

    // A NEW connection arms and becomes current -- everything after this
    // is superseded.
    let new_addr: SocketAddr = "127.0.0.1:59601".parse().unwrap();
    let new_conn = qa_r11_generation_race_connection(new_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), new_addr, new_conn.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            remote_node_id,
            new_addr,
            &remote_peer_id,
            &new_conn,
        )
        .await;

    // An OLD, no-longer-current connection sends a FullSync.
    let old_addr: SocketAddr = "127.0.0.1:59602".parse().unwrap();
    let full_sync_msg = crate::registry::RegistryMessage::FullSync {
        local_actors: vec![],
        known_actors: vec![],
        sender_peer_id: remote_peer_id.clone(),
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 99,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        old_addr,
        old_addr,
        Some(remote_peer_id.clone()),
        full_sync_msg,
    )
    .await
    .expect("stale FullSync must not error, only be ignored");

    // ...and a FullSyncResponse, also on the OLD connection.
    let full_sync_response_msg = crate::registry::RegistryMessage::FullSyncResponse {
        local_actors: vec![],
        known_actors: vec![],
        sender_peer_id: remote_peer_id.clone(),
        sender_bind_addr: Some(peer_addr.to_string()),
        sequence: 99,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        old_addr,
        old_addr,
        Some(remote_peer_id.clone()),
        full_sync_response_msg,
    )
    .await
    .expect("stale FullSyncResponse must not error, only be ignored");

    let gossip_state = registry.gossip_state.lock().await;
    let peer_info = gossip_state
        .peers
        .get(&peer_addr)
        .expect("peer must still be tracked");
    assert_eq!(
        peer_info.failures, 3,
        "R-11: a stale FullSync/FullSyncResponse must not reset `failures`"
    );
    assert_eq!(
        peer_info.last_failure_time,
        Some(stale_last_failure_time),
        "R-11: a stale FullSync/FullSyncResponse must not clear `last_failure_time`"
    );
    assert!(
        peer_info.last_failure_instant.is_some(),
        "R-11: a stale FullSync/FullSyncResponse must not clear `last_failure_instant` either"
    );
    assert_eq!(
        peer_info.last_response_received_ms, stale_last_response_ms,
        "R-11: a stale FullSync/FullSyncResponse must not advance \
         `last_response_received_ms` -- it must not be treated as proof \
         of the current session's liveness"
    );
    assert_eq!(
        peer_info.consecutive_deltas, 7,
        "R-11: a stale FullSync must not reset `consecutive_deltas`"
    );
    assert_eq!(
        peer_info.last_success, 0,
        "R-11: a stale FullSync/FullSyncResponse must not advance `last_success`"
    );
}

/// R-11: same race as the FullSync one above, for delta apply. Both
/// `DeltaGossip` (request) and `DeltaGossipResponse` branches in
/// `handle_incoming_message` validate the session while holding
/// `gossip_state`, then release that lock before calling
/// `apply_delta_from`. `DeltaApplyPendingMutation` fires immediately
/// before `apply_delta_from`'s own critical section re-acquires the lock
/// (and re-checks the caller-supplied `session_guard` generation), letting
/// this test deterministically land a newer session's full arm-and-restart
/// into that exact gap for the DeltaGossip (request) branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_gossip_stale_apply_paused_between_validation_and_mutation_is_dropped() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("delta-race-hi", "delta-race-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let peer_addr: SocketAddr = "127.0.0.1:7481".parse().unwrap();
    registry
        .connection_pool
        .peer_id_to_addr
        .upsert_sync(remote_peer_id.clone(), peer_addr);
    pool.set_configured_peer_addr(&remote_peer_id, peer_addr);
    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    let old_addr: SocketAddr = "127.0.0.1:59201".parse().unwrap();
    let old_conn = qa_r11_generation_race_connection(old_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), old_addr, old_conn.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            remote_node_id,
            old_addr,
            &remote_peer_id,
            &old_conn,
        )
        .await;

    // Baseline: OLD's own prior FullSync establishes "SURVIVOR" at sequence 40.
    let mut baseline_actors = std::collections::HashMap::new();
    baseline_actors.insert(
        "delta-race/SURVIVOR".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            baseline_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(old_addr),
            Some(old_addr),
            40,
            crate::current_timestamp(),
        )
        .await;

    let new_addr: SocketAddr = "127.0.0.1:59202".parse().unwrap();

    let _guard = {
        let pool = pool.clone();
        let registry_for_hook = registry.clone();
        let peer_id = remote_peer_id.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            let crate::TransportLifecycleEvent::DeltaApplyPendingMutation {
                peer: event_peer, ..
            } = &event
            else {
                return;
            };
            if *event_peer != peer_id {
                return;
            }
            crate::set_transport_lifecycle_recorder(None);

            let new_conn = qa_r11_generation_race_connection(new_addr);
            assert!(pool.add_connection_by_peer_id(peer_id.clone(), new_addr, new_conn.clone()));

            let registry_for_hook = registry_for_hook.clone();
            let peer_id = peer_id.clone();
            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    registry_for_hook
                        .arm_sequence_reset_for_new_session(
                            peer_addr,
                            remote_node_id,
                            new_addr,
                            &peer_id,
                            &new_conn,
                        )
                        .await;

                    let mut restart_actors = std::collections::HashMap::new();
                    restart_actors.insert(
                        "delta-race/NEW".to_string(),
                        crate::RemoteActorLocation::new_with_peer(peer_addr, peer_id.clone()),
                    );
                    registry_for_hook
                        .merge_full_sync_from(
                            restart_actors,
                            std::collections::HashMap::new(),
                            peer_id.clone(),
                            peer_addr,
                            Some(new_addr),
                            Some(new_addr),
                            1,
                            crate::current_timestamp(),
                        )
                        .await;
                })
            });
        }))
    };

    // OLD's own subsequent delta -- validates fine at STEP 1 (OLD is still
    // current then), but by the time `apply_delta_from`'s critical section
    // actually runs (right after the hook above completes), NEW's restart
    // has already superseded it.
    let stale_delta_msg = crate::registry::RegistryMessage::DeltaGossip {
        delta: crate::registry::RegistryDelta {
            since_sequence: 40,
            current_sequence: 41,
            changes: vec![crate::registry::RegistryChange::ActorAdded {
                name: "delta-race/STALE".to_string(),
                location: crate::RemoteActorLocation::new_with_peer(
                    peer_addr,
                    remote_peer_id.clone(),
                ),
                priority: crate::priority::RegistrationPriority::Normal,
            }],
            sender_peer_id: remote_peer_id.clone(),
            wall_clock_time: crate::current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        },
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        old_addr,
        old_addr,
        Some(remote_peer_id.clone()),
        stale_delta_msg,
    )
    .await
    .expect("stale delta must not error, only be ignored");

    assert!(
        registry.lookup_actor("delta-race/NEW").await.is_some(),
        "R-11: the newer session's restart FullSync must have been applied"
    );
    assert!(
        registry.lookup_actor("delta-race/STALE").await.is_none(),
        "R-11: the stale delta's apply must be dropped once the generation \
         recheck detects it was superseded, not applied on top of the \
         newer session's already-correct state"
    );
}

/// R-11: the `DeltaGossipResponse` counterpart of the test above -- same
/// race, same `DeltaApplyPendingMutation` seam, different
/// `handle_incoming_message` branch (`apply_delta_from`'s `session_guard`
/// recheck is the single shared mechanism both branches route through, so
/// this pins the wiring at the response branch's own call site rather than
/// re-proving the mechanism itself).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_gossip_response_stale_apply_paused_between_validation_and_mutation_is_dropped() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("delta-resp-race-hi", "delta-resp-race-lo");
    let remote_peer_id = lo_kp.peer_id();
    let remote_node_id = remote_peer_id.to_node_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(hi_kp),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let peer_addr: SocketAddr = "127.0.0.1:7482".parse().unwrap();
    registry
        .connection_pool
        .peer_id_to_addr
        .upsert_sync(remote_peer_id.clone(), peer_addr);
    pool.set_configured_peer_addr(&remote_peer_id, peer_addr);
    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(remote_node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    let old_addr: SocketAddr = "127.0.0.1:59301".parse().unwrap();
    let old_conn = qa_r11_generation_race_connection(old_addr);
    assert!(pool.add_connection_by_peer_id(remote_peer_id.clone(), old_addr, old_conn.clone()));
    registry
        .arm_sequence_reset_for_new_session(
            peer_addr,
            remote_node_id,
            old_addr,
            &remote_peer_id,
            &old_conn,
        )
        .await;

    let mut baseline_actors = std::collections::HashMap::new();
    baseline_actors.insert(
        "delta-resp-race/SURVIVOR".to_string(),
        crate::RemoteActorLocation::new_with_peer(peer_addr, remote_peer_id.clone()),
    );
    registry
        .merge_full_sync_from(
            baseline_actors,
            std::collections::HashMap::new(),
            remote_peer_id.clone(),
            peer_addr,
            Some(old_addr),
            Some(old_addr),
            40,
            crate::current_timestamp(),
        )
        .await;

    let new_addr: SocketAddr = "127.0.0.1:59302".parse().unwrap();

    let _guard = {
        let pool = pool.clone();
        let registry_for_hook = registry.clone();
        let peer_id = remote_peer_id.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            let crate::TransportLifecycleEvent::DeltaApplyPendingMutation {
                peer: event_peer, ..
            } = &event
            else {
                return;
            };
            if *event_peer != peer_id {
                return;
            }
            crate::set_transport_lifecycle_recorder(None);

            let new_conn = qa_r11_generation_race_connection(new_addr);
            assert!(pool.add_connection_by_peer_id(peer_id.clone(), new_addr, new_conn.clone()));

            let registry_for_hook = registry_for_hook.clone();
            let peer_id = peer_id.clone();
            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    registry_for_hook
                        .arm_sequence_reset_for_new_session(
                            peer_addr,
                            remote_node_id,
                            new_addr,
                            &peer_id,
                            &new_conn,
                        )
                        .await;

                    let mut restart_actors = std::collections::HashMap::new();
                    restart_actors.insert(
                        "delta-resp-race/NEW".to_string(),
                        crate::RemoteActorLocation::new_with_peer(peer_addr, peer_id.clone()),
                    );
                    registry_for_hook
                        .merge_full_sync_from(
                            restart_actors,
                            std::collections::HashMap::new(),
                            peer_id.clone(),
                            peer_addr,
                            Some(new_addr),
                            Some(new_addr),
                            1,
                            crate::current_timestamp(),
                        )
                        .await;
                })
            });
        }))
    };

    let stale_delta_response_msg = crate::registry::RegistryMessage::DeltaGossipResponse {
        delta: crate::registry::RegistryDelta {
            since_sequence: 40,
            current_sequence: 41,
            changes: vec![crate::registry::RegistryChange::ActorAdded {
                name: "delta-resp-race/STALE".to_string(),
                location: crate::RemoteActorLocation::new_with_peer(
                    peer_addr,
                    remote_peer_id.clone(),
                ),
                priority: crate::priority::RegistrationPriority::Normal,
            }],
            sender_peer_id: remote_peer_id.clone(),
            wall_clock_time: crate::current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        },
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        old_addr,
        old_addr,
        Some(remote_peer_id.clone()),
        stale_delta_response_msg,
    )
    .await
    .expect("stale delta response must not error, only be ignored");

    assert!(
        registry.lookup_actor("delta-resp-race/NEW").await.is_some(),
        "R-11: the newer session's restart FullSync must have been applied"
    );
    assert!(
        registry
            .lookup_actor("delta-resp-race/STALE")
            .await
            .is_none(),
        "R-11: the stale delta response's apply must be dropped once the \
         generation recheck detects it was superseded, not applied on top \
         of the newer session's already-correct state"
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
        .finalize_new_outbound_connection(shared_addr, io, registry_weak, None, shared_addr, None)
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

/// Exercises the outbound-finalize `AcceptIncoming` publish gap and its
/// CAS-lost re-resolve reject arm: when `existing_before` is `None`
/// at snapshot time, the outbound-finalize decision is unconditionally
/// `AcceptIncoming`. That decision is enacted via
/// `publish_outbound_or_reresolve`'s compare-and-publish against the
/// `existing_before` snapshot — never an unconditional publish — so a
/// PREFERRED rival published for the same peer in the gap between that
/// snapshot and this call is never silently overwritten. This test's remote
/// peer / local identity ordering additionally makes the re-resolved,
/// address-blind tie-break come back `RejectIncoming` against that
/// concurrently published rival (the rival is INBOUND and preferred; this
/// candidate is OUTBOUND and not) — a second failure mode this test also
/// covers: before the fix,
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

    let _guard = {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let inbound = inbound.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
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
        }))
    };

    let counter_before = pool.connection_counter.load(Ordering::SeqCst);

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
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

    let _guard = {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let rival = rival.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                peer: event_peer,
                ..
            } = &event
                && *event_peer == peer_id
            {
                crate::set_transport_lifecycle_recorder(None);
                pool.publish_current_peer_connection(&peer_id, rival.clone());
            }
        }))
    };

    let counter_before = pool.connection_counter.load(Ordering::SeqCst);

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
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

/// RED (review finding, `publish_outbound_or_reresolve` clear-race retry):
/// the first compare-and-publish can lose not to a concurrently published
/// rival but to a concurrent CLEAR — the slot is observed empty
/// (`rival == None`). Before the fix, the retry against that now-empty slot
/// discarded its own result and returned `true` unconditionally. If a
/// PREFERRED rival publishes in the narrow window between that first
/// CAS-loss and the retry, the retry ALSO loses (`Err(Some(rival))`) — the
/// candidate was never actually installed as the session — yet the old code
/// still told the caller "installed", which went on to bump
/// `connection_counter`, send FullSync, and hand back a live `Ok` handle for
/// a connection that was never the peer's session.
///
/// `expected` threaded into `publish_outbound_or_reresolve`'s compare-and-
/// publish is `existing_before.as_ref()` — and `existing_before` only comes
/// back `Some` from `get_connection_by_peer_id` when it observed the rival
/// as LIVE (that lookup filters every fallback by usability). So the only
/// realistic way for the primary CAS's `expected` to be `Some(rival)` while
/// still reaching the `AcceptIncoming` branch (the only branch that calls
/// `publish_outbound_or_reresolve` at all) is `existing_before`'s OWN link
/// dying in the narrow real gap between that snapshot and the tie-break
/// decision computed a few lines later from the SAME `existing_before`
/// value — exactly the race `OutboundFinalizeExistingSnapshotTaken` exists
/// to pin deterministically.
///
/// Local uses the LOWER NodeId, so `should_keep_connection(remote,
/// is_outbound) == is_outbound`: `keep_incoming == true` makes the eager
/// decision `AcceptIncoming` once `existing_before` is observed dead, and
/// the SECOND-race rival below is a genuinely live OUTBOUND connection too —
/// since `keep_existing` for an outbound rival is also `true`, the
/// re-resolved tie-break keeps that already-installed incumbent and rejects
/// this duplicate outbound candidate (`RejectIncoming`), exactly mirroring
/// the real "avoid duplicate/flapping outbound dials to the same peer" case
/// this retry path exists for.
///
/// Pinned deterministically via `set_transport_lifecycle_recorder` on THREE
/// events in sequence: `OutboundFinalizeExistingSnapshotTaken` (fires right
/// after `existing_before` is snapshotted, live) flips `existing_before`'s
/// own stream to dead, so the decision computed immediately afterward sees
/// `existing_usable == false` and becomes `AcceptIncoming` with
/// `expected = Some(existing_before)`; `OutboundFinalizePublishAttempt`
/// (fires before the FIRST compare-and-publish) then clears the peer's
/// current-connection slot outright, forcing that first CAS to lose with
/// `rival == None`; then `OutboundFinalizeClearRaceRetry` (fires immediately
/// before the retry CAS) publishes the preferred rival, forcing the retry to
/// ALSO lose, this time with `Err(Some(rival))`.
///
/// RED at HEAD (before this fix): `result.is_ok()`, the loser stays
/// indexed/counted, and `connection_counter` is bumped for a connection that
/// was never installed as the session. GREEN after the fix:
/// `result == Err(GossipError::ConnectionExists)`, the loser is fully
/// unpublished, `connection_counter` reflects only the legitimately
/// published preferred rival, and the preferred rival remains current.
#[tokio::test]
async fn outbound_finalize_clear_race_retry_loss_to_second_rival_fully_unpublishes() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("clear-race-retry-hi", "clear-race-retry-lo");
    let remote_peer_id = hi_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(lo_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));

    let dial_addr: SocketAddr = "127.0.0.1:7490".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    // The pre-existing session at the candidate's own dial address — LIVE at
    // snapshot time (so `get_connection_by_peer_id` actually returns it as
    // `existing_before`, threading `expected = Some(existing_before)` into
    // the compare-and-publish below). The `OutboundFinalizeExistingSnapshotTaken`
    // hook flips it to dead immediately afterward, before the decision is
    // computed from it.
    let existing_before = make_live_connection(dial_addr, ConnectionDirection::Outbound).await;
    assert!(pool.add_connection_by_peer_id(
        remote_peer_id.clone(),
        dial_addr,
        existing_before.clone()
    ));

    let baseline = pool.connection_counter.load(Ordering::SeqCst);
    assert_eq!(
        baseline, 1,
        "test precondition: exactly one counted session"
    );

    // The genuinely live, PREFERRED (outbound) rival published into the
    // clear-race retry window — a different instance, at a different
    // address, than both the pre-existing session and the new candidate.
    let preferred_addr: SocketAddr = "127.0.0.1:7491".parse().unwrap();
    let preferred_rival = make_live_connection(preferred_addr, ConnectionDirection::Outbound).await;

    let _guard = {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let existing_before = existing_before.clone();
        let preferred_rival = preferred_rival.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            match &event {
                crate::TransportLifecycleEvent::OutboundFinalizeExistingSnapshotTaken {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    // `existing_before`'s own link dies in the real gap between
                    // the snapshot and the tie-break decision computed from it —
                    // the decision a few lines below now observes it as dead.
                    if let Some(sh) = existing_before.stream_handle.as_ref() {
                        sh.exit_flag.store(true, Ordering::Release);
                    }
                }
                crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    // A concurrent CLEAR (not a publish) races the FIRST
                    // compare-and-publish: the slot is empty by the time that
                    // CAS actually runs, so it loses with `rival == None`.
                    pool.clear_current_peer_connection(&peer_id);
                }
                crate::TransportLifecycleEvent::OutboundFinalizeClearRaceRetry {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    // Deregister first: `add_connection_by_peer_id` below fires
                    // its own (non-matching) `SessionPublished` event through
                    // this same global hook, and this avoids any
                    // reentrant/recursive invocation of this closure.
                    crate::set_transport_lifecycle_recorder(None);
                    // A PREFERRED rival publishes — for real, counted — into the
                    // exact gap between the first CAS loss and the retry, so the
                    // retry ALSO loses, this time to an actually-installed rival.
                    assert!(pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        preferred_addr,
                        preferred_rival.clone()
                    ));
                }
                _ => {}
            }
        }))
    };

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a candidate whose retry against a clear-race also loses to a preferred rival must be \
         surfaced as an error, never handed back to the caller as a live handle: got {result:?}"
    );

    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &preferred_rival)),
        "the preferred rival published into the clear-race retry window must remain the \
         peer's current session (got {current:?})"
    );
    assert!(
        preferred_rival.has_live_stream(),
        "the preferred rival's background tasks must survive untouched"
    );

    assert!(
        pool.get_lock_free_connection(dial_addr).is_none(),
        "the rejected candidate must not remain indexed in connections_by_addr at its own \
         dial address"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&dial_addr, |_, v| v.clone())
            .is_none(),
        "the rejected candidate's dial address must not remain mapped to the peer id (the \
         pre-existing session it displaced was already dead by decision time, so it is not \
         restored either)"
    );

    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        baseline + 1,
        "connection_counter must reflect only the one legitimately published preferred rival \
         — the candidate that lost both the primary CAS and its clear-race retry must never \
         be counted"
    );
}

/// RED (review finding P1, `resolve_and_act_on_outbound_rival`'s
/// `AcceptIncoming` "evict-and-replace" retry): once the FIRST
/// compare-and-publish in `publish_outbound_or_reresolve` loses to an
/// actually-installed STALE rival (not a clear — `Err(Some(rival))` directly,
/// so `resolve_and_act_on_outbound_rival` re-resolves against it), the
/// re-resolved decision comes back `AcceptIncoming` (the rival is dead and
/// our outbound is still preferred) and retries its own compare-and-publish
/// against that stale rival. Before this fix, that retry's result was
/// discarded (`let _ = ...; true`) and the function returned `true`
/// unconditionally. If a THIRD, genuinely live, tie-break-PREFERRED session
/// publishes in the exact window between the re-resolved decision and this
/// retry, the retry itself loses (`Err(Some(new_rival))`) — our candidate was
/// NEVER installed as the peer's session — yet the old code still reported
/// success, which went on to bump `connection_counter`, send FullSync, and
/// hand back a live `Ok` handle for a connection that was never the peer's
/// current session. This is the exact fallback-outbound/shadow-session race
/// the surrounding CAS-loss handling exists to close.
///
/// Setup mirrors `outbound_finalize_clear_race_retry_loss_to_second_rival_fully_unpublishes`:
/// local uses the LOWER NodeId, so `should_keep_connection(remote,
/// is_outbound) == is_outbound`, and `existing_before` is snapshotted LIVE
/// then flipped dead via `OutboundFinalizeExistingSnapshotTaken` so the eager
/// decision is `AcceptIncoming` with `expected = Some(existing_before)`.
///
/// Pinned deterministically via `set_transport_lifecycle_recorder` on THREE
/// events: `OutboundFinalizeExistingSnapshotTaken` kills `existing_before`'s
/// stream; `OutboundFinalizePublishAttempt` (fires before the FIRST
/// compare-and-publish) publishes a genuinely-installed but STALE
/// `stale_rival` (dead stream, different `Arc`/address than
/// `existing_before`) into the peer's session slot, forcing that first CAS to
/// lose with `rival == Some(stale_rival)` directly (no clear-race retry
/// involved) and routing straight into `resolve_and_act_on_outbound_rival`,
/// whose re-resolved decision against the dead `stale_rival` is again
/// `AcceptIncoming`; then `OutboundFinalizeAcceptIncomingRetryAttempt` (fires
/// immediately before THAT arm's own retry compare-and-publish) publishes a
/// genuinely live, tie-break-preferred `preferred_rival` for real (counted),
/// forcing the retry to ALSO lose, this time to an actually-installed,
/// preferred rival — the re-resolved tie-break against `preferred_rival`
/// comes back `RejectIncoming` (a live preferred outbound rival is kept), so
/// the bounded nested re-resolve rejects rather than reporting success.
///
/// RED at HEAD (before this fix): `result.is_ok()`, the loser stays
/// indexed/counted, and `connection_counter` is bumped for a connection that
/// was never installed as the session. GREEN after the fix:
/// `result == Err(GossipError::ConnectionExists)`, the loser is fully
/// unpublished, `connection_counter` reflects only the legitimately
/// published `existing_before` and `preferred_rival`, and `preferred_rival`
/// remains the peer's current session.
#[tokio::test]
async fn outbound_finalize_evict_replace_retry_loss_to_new_rival_fully_unpublishes() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("evict-replace-retry-hi", "evict-replace-retry-lo");
    let remote_peer_id = hi_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(lo_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));

    let dial_addr: SocketAddr = "127.0.0.1:7492".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    // The pre-existing session at the candidate's own dial address — LIVE at
    // snapshot time (so `get_connection_by_peer_id` returns it as
    // `existing_before`, threading `expected = Some(existing_before)` into
    // the first compare-and-publish). The `OutboundFinalizeExistingSnapshotTaken`
    // hook flips it dead immediately afterward, before the decision computed
    // from it.
    let existing_before = make_live_connection(dial_addr, ConnectionDirection::Outbound).await;
    assert!(pool.add_connection_by_peer_id(
        remote_peer_id.clone(),
        dial_addr,
        existing_before.clone()
    ));

    let baseline = pool.connection_counter.load(Ordering::SeqCst);
    assert_eq!(
        baseline, 1,
        "test precondition: exactly one counted session"
    );

    // Published into the peer's session slot (real `ArcSwapOption` install,
    // never counted) in the `OutboundFinalizePublishAttempt` window, standing
    // in for the "yet another session already replaced `existing_before` by
    // the time the first CAS actually runs" case. Dead stream so the
    // re-resolved decision against it is again `AcceptIncoming`.
    let stale_addr: SocketAddr = "127.0.0.1:7493".parse().unwrap();
    let stale_rival = make_live_connection(stale_addr, ConnectionDirection::Outbound).await;
    if let Some(sh) = stale_rival.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }
    assert!(
        !stale_rival.has_live_stream(),
        "test precondition: stale_rival must be dead"
    );

    // The genuinely live, PREFERRED (outbound) rival published for real
    // (counted) into the `AcceptIncoming` arm's own retry window.
    let preferred_addr: SocketAddr = "127.0.0.1:7494".parse().unwrap();
    let preferred_rival = make_live_connection(preferred_addr, ConnectionDirection::Outbound).await;

    let _guard = {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let existing_before = existing_before.clone();
        let stale_rival = stale_rival.clone();
        let preferred_rival = preferred_rival.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            match &event {
                crate::TransportLifecycleEvent::OutboundFinalizeExistingSnapshotTaken {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    // `existing_before`'s own link dies in the real gap between
                    // the snapshot and the tie-break decision computed from it.
                    if let Some(sh) = existing_before.stream_handle.as_ref() {
                        sh.exit_flag.store(true, Ordering::Release);
                    }
                }
                crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    // A different, already-stale session replaces `existing_before`
                    // in the peer's slot before the FIRST compare-and-publish
                    // actually runs, so that CAS loses directly to
                    // `Some(stale_rival)` — not a clear.
                    pool.publish_current_peer_connection(&peer_id, stale_rival.clone());
                }
                crate::TransportLifecycleEvent::OutboundFinalizeAcceptIncomingRetryAttempt {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    // Deregister first: `add_connection_by_peer_id` below fires
                    // its own (non-matching) `SessionPublished` event through
                    // this same global hook, avoiding reentrant invocation.
                    crate::set_transport_lifecycle_recorder(None);
                    // A PREFERRED rival publishes — for real, counted — into the
                    // exact gap between the re-resolved `AcceptIncoming` decision
                    // and its own retry, so that retry ALSO loses, this time to
                    // an actually-installed, preferred rival.
                    assert!(pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        preferred_addr,
                        preferred_rival.clone()
                    ));
                }
                _ => {}
            }
        }))
    };

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a candidate whose AcceptIncoming evict-and-replace retry also loses to a preferred \
         rival must be surfaced as an error, never handed back to the caller as a live handle: \
         got {result:?}"
    );

    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &preferred_rival)),
        "the preferred rival published into the AcceptIncoming retry window must remain the \
         peer's current session (got {current:?})"
    );
    assert!(
        preferred_rival.has_live_stream(),
        "the preferred rival's background tasks must survive untouched"
    );

    assert!(
        pool.get_lock_free_connection(dial_addr).is_none(),
        "the rejected candidate must not remain indexed in connections_by_addr at its own \
         dial address"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&dial_addr, |_, v| v.clone())
            .is_none(),
        "the rejected candidate's dial address must not remain mapped to the peer id (the \
         pre-existing session it displaced was already dead by decision time, so it is not \
         restored either)"
    );

    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        baseline + 1,
        "connection_counter must reflect only the legitimately published existing_before and \
         preferred_rival sessions — the candidate that lost both the primary CAS and its \
         AcceptIncoming evict-and-replace retry must never be counted"
    );
}

/// RED (audit finding, same class as review finding A, one level deeper:
/// `resolve_and_act_on_outbound_rival_bounded`'s OWN nested `ReplaceExisting`
/// arm): once the first compare-and-publish loses to an actually-installed,
/// LIVE, wrong-direction rival (`Err(Some(rival))`, not a clear), the
/// re-resolved decision against that rival can itself be `ReplaceExisting`
/// (the rival is live but not tie-break-preferred, and this candidate still
/// is). That arm evicts the rival and retries its own compare-and-publish —
/// which is the exact same "evict, then publish" shape as review finding A,
/// just reached through the re-resolve path instead of the eager decision.
/// This exercises that identical fix one level deeper: if a THIRD,
/// genuinely live, tie-break-preferred session publishes into the gap
/// between that second eviction and its own retry, the retry must lose and
/// bound its own nested re-resolve (consuming the one-retry budget) rather
/// than reporting success for a candidate that was never installed.
///
/// Local uses the LOWER NodeId, so `should_keep_connection(remote,
/// is_outbound) == is_outbound`. `existing_before` is snapshotted LIVE then
/// flipped dead via `OutboundFinalizeExistingSnapshotTaken`, making the
/// eager decision `AcceptIncoming`. The rival published into the FIRST CAS's
/// window is a LIVE, wrong-direction (inbound) connection — not stale — so
/// the re-resolved decision against it is `ReplaceExisting`, not
/// `AcceptIncoming`. A further, genuinely live, PREFERRED (outbound) rival
/// then publishes into THAT arm's own eviction-to-retry gap, forcing its
/// retry to lose too; re-resolving once more against that final preferred
/// rival comes back `RejectIncoming` (a live, already-preferred rival is
/// kept over a not-yet-installed duplicate), consuming the bounded nested
/// retry budget exactly once.
///
/// RED at HEAD (before this fix, i.e. before the nested `ReplaceExisting`
/// arm routed through compare-and-publish): `result.is_ok()`, the loser
/// stays indexed/counted, and `connection_counter` is bumped for a
/// connection that was never installed as the session. GREEN after the fix:
/// `result == Err(GossipError::ConnectionExists)`, the loser is fully
/// unpublished, and the final preferred rival remains current.
#[tokio::test]
async fn outbound_finalize_nested_replace_existing_retry_loss_to_new_rival_fully_unpublishes() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs(
        "nested-replace-existing-retry-hi",
        "nested-replace-existing-retry-lo",
    );
    let remote_peer_id = hi_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(lo_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));

    let dial_addr: SocketAddr = "127.0.0.1:7498".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    // Snapshotted LIVE (so `expected = Some(existing_before)` threads into
    // the first compare-and-publish), flipped dead immediately afterward so
    // the eager decision computed from it is `AcceptIncoming`.
    let existing_before = make_live_connection(dial_addr, ConnectionDirection::Outbound).await;
    assert!(pool.add_connection_by_peer_id(
        remote_peer_id.clone(),
        dial_addr,
        existing_before.clone()
    ));

    let baseline = pool.connection_counter.load(Ordering::SeqCst);
    assert_eq!(
        baseline, 1,
        "test precondition: exactly one counted session"
    );

    // Genuinely LIVE, wrong-direction (inbound) rival published into the
    // FIRST compare-and-publish's window — not stale, so the re-resolved
    // decision against it is `ReplaceExisting`, exercising the nested arm.
    let wrong_direction_addr: SocketAddr = "127.0.0.1:7499".parse().unwrap();
    let wrong_direction_rival =
        make_live_connection(wrong_direction_addr, ConnectionDirection::Inbound).await;

    // The final, genuinely live, PREFERRED (outbound) rival published into
    // the nested `ReplaceExisting` arm's own eviction-to-retry gap.
    let preferred_addr: SocketAddr = "127.0.0.1:7500".parse().unwrap();
    let preferred_rival = make_live_connection(preferred_addr, ConnectionDirection::Outbound).await;

    let _guard = {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let existing_before = existing_before.clone();
        let wrong_direction_rival = wrong_direction_rival.clone();
        let preferred_rival = preferred_rival.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            match &event {
                crate::TransportLifecycleEvent::OutboundFinalizeExistingSnapshotTaken {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    if let Some(sh) = existing_before.stream_handle.as_ref() {
                        sh.exit_flag.store(true, Ordering::Release);
                    }
                }
                crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    pool.publish_current_peer_connection(&peer_id, wrong_direction_rival.clone());
                }
                crate::TransportLifecycleEvent::OutboundFinalizeReplaceExistingRetryAttempt {
                    peer: event_peer,
                    ..
                } if *event_peer == peer_id => {
                    // Deregister first: `add_connection_by_peer_id` below
                    // fires its own (non-matching) `SessionPublished` event
                    // through this same global hook, avoiding reentrant
                    // invocation.
                    crate::set_transport_lifecycle_recorder(None);
                    assert!(pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        preferred_addr,
                        preferred_rival.clone()
                    ));
                }
                _ => {}
            }
        }))
    };

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a candidate whose nested ReplaceExisting retry also loses to a preferred rival must be \
         surfaced as an error, never handed back to the caller as a live handle: got {result:?}"
    );

    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &preferred_rival)),
        "the preferred rival published into the nested ReplaceExisting retry window must remain \
         the peer's current session (got {current:?})"
    );
    assert!(
        preferred_rival.has_live_stream(),
        "the preferred rival's background tasks must survive untouched"
    );
    assert!(
        !wrong_direction_rival.has_live_stream(),
        "the evicted wrong-direction rival must actually have been torn down"
    );

    assert!(
        pool.get_lock_free_connection(dial_addr).is_none(),
        "the rejected candidate must not remain indexed in connections_by_addr at its own \
         dial address"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&dial_addr, |_, v| v.clone())
            .is_none(),
        "the rejected candidate's dial address must not remain mapped to the peer id"
    );

    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        baseline + 1,
        "connection_counter must reflect only the legitimately published `existing_before` and \
         `preferred_rival` sessions (`wrong_direction_rival` was published test-internally via \
         `publish_current_peer_connection`, never counted, mirroring the sibling \
         `outbound_finalize_evict_replace_retry_loss_to_new_rival_fully_unpublishes` test) — the \
         candidate that lost its nested re-resolution must never be counted"
    );
}

/// RED (review finding A, P1, outbound-finalize `ReplaceExisting` eager
/// decision arm): the decision above is computed against `existing_before` —
/// a live, wrong-direction rival — and evicts it via the instance-scoped
/// `disconnect_connection_instance` (a self-validating CAS that declines
/// harmlessly if `existing_before` was already superseded). Before this fix,
/// the arm then called `publish_current_peer_connection` UNCONDITIONALLY
/// afterward, on the theory that "the tie-break already decided we win
/// regardless of what is indexed at this instant" — which is exactly wrong
/// when a FRESH, tie-break-preferred session is published for the same peer
/// in the gap between the eviction attempt and that publish: the eviction
/// correctly leaves the fresh session alone (it is no longer `existing_before`),
/// but the old unconditional publish still clobbered it, orphaning the fresh
/// session and reporting the stale, now-invalid outbound as current.
///
/// Local uses the LOWER NodeId, so `should_keep_connection(remote,
/// is_outbound=true) == true` — a fresh OUTBOUND dial is tie-break preferred
/// — and `existing_before` is a live INBOUND rival (`keep_existing == false`),
/// so the eager decision is `ReplaceExisting`.
///
/// Pinned deterministically via `TransportLifecycleRecorderGuard` on
/// `OutboundFinalizePublishAttempt` (fires unconditionally immediately before
/// the outbound's own compare-and-publish attempt, AFTER the eviction of
/// `existing_before` has already run): when it fires, a FRESH, genuinely
/// live, tie-break-preferred OUTBOUND connection is published for real
/// (counted) into the peer's session slot — modelling a concurrent
/// accept/finalize landing in the gap between this candidate's eviction of
/// `existing_before` and its own publish. The re-resolved, address-blind
/// tie-break against that fresh outbound comes back `RejectIncoming` (an
/// equally-preferred, already-live rival is kept over a not-yet-installed
/// duplicate) — the "avoid duplicate/flapping outbound dials" case.
///
/// RED at HEAD (before this fix): `result.is_ok()` (the stale candidate is
/// handed back as a live handle, clobbering `fresh`). GREEN after the fix:
/// `result == Err(GossipError::ConnectionExists)`, `fresh` remains current,
/// and `connection_counter` reflects only `fresh` (`existing_before` was
/// evicted and its own count released).
#[tokio::test]
async fn outbound_finalize_replace_existing_compare_and_publishes_against_snapshot() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) =
        hi_lo_keypairs("replace-existing-cas-gap-hi", "replace-existing-cas-gap-lo");
    let remote_peer_id = hi_kp.peer_id();

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(lo_kp),
            ..Default::default()
        },
    ));
    let registry_weak = Arc::downgrade(&registry);

    let pool = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));

    // The pre-existing, live, WRONG-DIRECTION (inbound) rival the decision
    // is computed about, at its own address — distinct from the fresh
    // outbound's own dial address below.
    let existing_addr: SocketAddr = "127.0.0.1:7495".parse().unwrap();
    let existing_before = make_live_connection(existing_addr, ConnectionDirection::Inbound).await;
    assert!(pool.add_connection_by_peer_id(
        remote_peer_id.clone(),
        existing_addr,
        existing_before.clone()
    ));

    let baseline = pool.connection_counter.load(Ordering::SeqCst);
    assert_eq!(
        baseline, 1,
        "test precondition: exactly one counted session"
    );

    let dial_addr: SocketAddr = "127.0.0.1:7496".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    // The FRESH, genuinely live, tie-break-preferred (outbound) rival
    // published concurrently, into the exact gap between this candidate's
    // eviction of `existing_before` and its own publish attempt.
    let fresh_addr: SocketAddr = "127.0.0.1:7497".parse().unwrap();
    let fresh = make_live_connection(fresh_addr, ConnectionDirection::Outbound).await;

    let _guard = {
        let pool = pool.clone();
        let peer_id = remote_peer_id.clone();
        let fresh = fresh.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                peer: event_peer,
                ..
            } = &event
                && *event_peer == peer_id
            {
                // Deregister first: `add_connection_by_peer_id` below fires
                // its own (non-matching) `SessionPublished` event through
                // this same global hook, avoiding reentrant invocation.
                crate::set_transport_lifecycle_recorder(None);
                assert!(pool.add_connection_by_peer_id(peer_id.clone(), fresh_addr, fresh.clone()));
            }
        }))
    };

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "a stale outbound candidate whose compare-and-publish re-resolution loses to a \
         concurrently published, tie-break-preferred rival must be surfaced as an error, never \
         handed back to the caller as a live handle: got {result:?}"
    );

    let current = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
        "the FRESH outbound published concurrently in the compare-and-publish gap must remain \
         the peer's current session — the stale candidate's own publish, computed from a \
         superseded `existing_before` snapshot, must never overwrite it (got {current:?})"
    );
    assert!(
        fresh.has_live_stream(),
        "the fresh outbound's background tasks must survive untouched"
    );
    assert!(
        !existing_before.has_live_stream(),
        "the evicted wrong-direction rival must actually have been torn down"
    );

    assert!(
        pool.get_lock_free_connection(dial_addr).is_none(),
        "the rejected candidate must not remain indexed in connections_by_addr at its own \
         dial address"
    );
    assert!(
        pool.addr_to_peer_id
            .read_sync(&dial_addr, |_, v| v.clone())
            .is_none(),
        "the rejected candidate's dial address must not remain mapped to the peer id"
    );

    assert_eq!(
        pool.connection_counter.load(Ordering::SeqCst),
        baseline,
        "connection_counter must reflect only `fresh` — `existing_before` was evicted (its own \
         count released) and the stale candidate that lost its compare-and-publish \
         re-resolution must never be counted, so the total stays exactly where it started"
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
    pool.finalize_new_outbound_connection(dial_addr, io, registry_weak, None, dial_addr, None)
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

/// RED (review finding P1, outbound-finalize decision-snapshot side effect):
/// `finalize_new_outbound_connection`'s `existing_before` snapshot used to be
/// computed via the SELF-HEALING `get_connection_by_peer_id`, not a pure
/// lookup. When the peer's current session was an unusable/stale instance,
/// that call cleared it as a side effect of merely being READ — a decision
/// snapshot must never mutate. This pins that in two complementary parts.
///
/// PART 1 (deterministic, no race injection needed) drives the REAL
/// `finalize_new_outbound_connection` outbound path with a genuinely stale
/// (dead) current session, using an identity ordering where this side's
/// fallback outbound is NOT the tie-break-preferred direction
/// (`should_keep_connection(remote, is_outbound=true) == false` — the
/// higher-NodeId "fell back to dialing after a preferred-inbound wait
/// timed out" case described throughout this file). At HEAD,
/// `get_connection_by_peer_id`'s unconditional self-heal clears the stale
/// session BEFORE `existing_before` is even captured, so `existing_before`
/// comes back `None` — "proceed as if there were no rival", exactly the
/// finding's defect — and the decision takes the `None => AcceptIncoming`
/// fast path: this non-preferred outbound wrongly becomes the peer's
/// session and the stale rival is never evicted. With the fix
/// (`peer_current_connection_snapshot`, which never mutates),
/// `existing_before` correctly observes the stale connection, producing
/// `EvictStaleRejectIncoming`: the stale entry is cleaned up and this
/// non-preferred candidate is correctly rejected.
///
/// PART 2 (primitive-level, deterministic race injection) pins the
/// self-heal primitive `get_connection_by_peer_id` itself — still the
/// self-healing lookup many other callers (message routing/dialing) rely
/// on: a fresh, live, genuinely-published session for the SAME peer landing
/// exactly as `get_connection_by_peer_id` attempts its self-heal clear must
/// survive. Pinned via the new `GetConnectionSelfHealClearAttempt`
/// instrumentation event, fired unconditionally immediately before that
/// clear attempt — the same technique `OutboundFinalizePublishAttempt` uses
/// elsewhere in this file to pin a concurrent publish into a specific
/// check-then-act gap.
///
/// HONESTY NOTE: PART 1 is genuinely RED at HEAD via a full, unmodified
/// production call with no instrumentation needed at all — the erasure is
/// deterministic, not a race. PART 2 exercises the fix's own new CAS
/// primitive (`compare_and_clear_current_peer_connection`), which does not
/// exist at HEAD, so it cannot be run unmodified there; RED for PART 2's
/// specific shape (a concurrent publish landing between the internal
/// ptr_eq-check and the unconditional clear) was separately confirmed at
/// HEAD by temporarily instrumenting that exact gap inside the (unfixed)
/// `clear_current_peer_connection_if_matches` and observing the
/// concurrently published fresh session get silently erased — see the
/// fix's commit message for that evidence. After the fix, this call path
/// (`get_connection_by_peer_id`'s self-heal) is the only remaining
/// production caller of the new atomic clear, so PART 2 exercises exactly
/// the primitive `existing_before` used to rely on and closes the same
/// class of gap it had.
#[tokio::test]
async fn outbound_finalize_decision_snapshot_does_not_clear_fresh_session() {
    use crate::{GossipConfig, registry::GossipRegistry};

    // ---- PART 1: `existing_before` must observe a stale session, never
    // erase it into `None`, when driven through the real outbound-finalize
    // path. ----
    let (hi_kp, lo_kp) = hi_lo_keypairs("decision-snapshot-hi", "decision-snapshot-lo");
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
    let dial_addr: SocketAddr = "127.0.0.1:7460".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    // A stale (dead) current session for this peer — e.g. an inbound whose
    // IO task has already exited but whose entry has not yet been reaped.
    let stale = make_live_connection(dial_addr, ConnectionDirection::Inbound).await;
    if let Some(sh) = stale.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }
    assert!(
        !stale.has_live_stream(),
        "test precondition: stale must be dead"
    );
    pool.publish_current_peer_connection(&remote_peer_id, stale.clone());
    assert!(
        pool.connections_by_peer
            .read_sync(&remote_peer_id, |_, v| Arc::ptr_eq(v, &stale))
            .unwrap_or(false),
        "test precondition: stale must be indexed in connections_by_peer"
    );

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(
            dial_addr,
            io,
            registry_weak.clone(),
            None,
            dial_addr,
            None,
        )
        .await;

    assert!(
        matches!(result, Err(crate::GossipError::ConnectionExists)),
        "local is the HIGHER NodeId here, so its own fallback OUTBOUND dial is never the \
         tie-break-preferred direction regardless of the existing session's liveness — a \
         correct decision must reject this candidate (`EvictStaleRejectIncoming`), never \
         silently accept it because a buggy self-heal erased the stale rival into `None` and \
         mistook this for the peer's very first connection: got {result:?}"
    );
    assert!(
        pool.connections_by_peer
            .read_sync(&remote_peer_id, |_, v| v.clone())
            .is_none(),
        "the stale rival must have been (instance-scoped) evicted as part of \
         `EvictStaleRejectIncoming` — proving the decision correctly identified it as the \
         existing rival rather than treating it as absent"
    );
    assert!(
        pool.get_connection_by_peer_id(&remote_peer_id).is_none(),
        "neither the stale rival nor the rejected candidate may remain as the peer's current \
         session"
    );

    // ---- PART 2: the underlying self-heal primitive itself must be atomic
    // against a concurrent publish. ----
    let peer_id2 = crate::KeyPair::new_for_testing("decision-snapshot-primitive-peer").peer_id();
    let pool2 = Arc::new(ConnectionPool::<()>::new(8, Duration::from_secs(5)));
    let stale2_addr: SocketAddr = "127.0.0.1:7461".parse().unwrap();
    let stale2 = make_live_connection(stale2_addr, ConnectionDirection::Outbound).await;
    if let Some(sh) = stale2.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }
    pool2.publish_current_peer_connection(&peer_id2, stale2.clone());

    let fresh_addr: SocketAddr = "127.0.0.1:7462".parse().unwrap();
    let fresh = make_live_connection(fresh_addr, ConnectionDirection::Inbound).await;

    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let _guard = {
        let pool2 = pool2.clone();
        let peer_id2 = peer_id2.clone();
        let fresh = fresh.clone();
        let fired = fired.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::GetConnectionSelfHealClearAttempt {
                peer: event_peer,
                ..
            } = &event
                && *event_peer == peer_id2
            {
                fired.store(true, Ordering::Release);
                // A fresh, live, genuinely-published session for the SAME
                // peer lands exactly as the self-heal is about to clear the
                // stale one — the concurrent-publish gap this fix closes.
                pool2.publish_current_peer_connection(&peer_id2, fresh.clone());
            }
        }))
    };

    // The first call's OWN return value is not the interesting assertion
    // here: `get_connection_by_peer_id` does not re-check the primary slot
    // after a declined self-heal before falling through to its
    // address/alias fallbacks (a separate, pre-existing, non-buggy aspect of
    // its "best effort" lookup contract — a caller can simply call again).
    // What matters is whether the CONCURRENTLY PUBLISHED session survived in
    // the peer session's own slot, which a SECOND call (now hitting the
    // primary-slot fast path directly, no self-heal involved) observes.
    let _ = pool2.get_connection_by_peer_id(&peer_id2);
    drop(_guard);

    assert!(
        fired.load(Ordering::Acquire),
        "test precondition: the self-heal clear attempt must actually have fired"
    );
    let observed = pool2.get_connection_by_peer_id(&peer_id2);
    assert!(
        observed.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
        "a session published for this peer exactly as the self-heal clear attempt fires must \
         survive as the peer's current session, never silently erased by an unconditional \
         clear racing the concurrent publish: got {observed:?}"
    );
    assert!(
        fresh.has_live_stream(),
        "the fresh session's background tasks must be untouched"
    );
}

/// ACTOR_REM_2 R10: a concurrent `get_connection_by_peer_id` address-fallback
/// can publish finalize's OWN provisionally-addr-indexed candidate into the peer
/// session slot (out of band, via the non-CAS `publish_current_peer_connection`)
/// exactly as finalize is about to publish it. Finalize's compare-and-publish
/// then finds its own candidate installed as the slot value; without the fix it
/// treated that as a "rival", re-resolved the address-blind tie-break against it,
/// and — since a higher-NodeId local's own outbound is never the preferred
/// direction — REJECTED and `abort_tasks()`d its own uncontested connection,
/// returning `ConnectionExists` to the caller. The fix makes compare-and-publish
/// recognize "the slot already holds THIS connection" as idempotent success.
#[tokio::test]
async fn outbound_finalize_does_not_abort_its_own_out_of_band_published_candidate() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let (hi_kp, lo_kp) = hi_lo_keypairs("r10-hi", "r10-lo");
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
    let dial_addr: SocketAddr = "127.0.0.1:7480".parse().unwrap();
    pool.set_configured_peer_addr(&remote_peer_id, dial_addr);

    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _guard = {
        let pool = pool.clone();
        let peer = remote_peer_id.clone();
        let fired = fired.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            if let crate::TransportLifecycleEvent::OutboundFinalizePublishAttempt {
                peer: event_peer,
                ..
            } = &event
                && *event_peer == peer
            {
                fired.store(true, Ordering::Release);
                // T1: the address fallback adopts finalize's own candidate
                // (still only indexed by address at this instant) as the
                // peer's session, out of band.
                let _ = pool.get_connection_by_peer_id(&peer);
            }
        }))
    };

    let (io, _keep) = tokio::io::duplex(1024);
    let result = pool
        .finalize_new_outbound_connection(
            dial_addr,
            io,
            registry_weak.clone(),
            None,
            dial_addr,
            None,
        )
        .await;
    drop(_guard);

    assert!(
        fired.load(Ordering::Acquire),
        "test precondition: the outbound-finalize publish attempt must have fired"
    );
    assert!(
        result.is_ok(),
        "R10: finalize must accept its own out-of-band-published candidate, not \
         re-resolve and abort it as a rival: got {result:?}"
    );
    let session = pool.get_connection_by_peer_id(&remote_peer_id);
    assert!(
        session.as_ref().is_some_and(|c| c.has_live_stream()),
        "R10: the accepted candidate must remain the peer's live session, not aborted: \
         got {session:?}"
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

/// `connections_by_addr` can be reassigned to a DIFFERENT connection
/// instance after a `ConnectionHandle` was already resolved from it —
/// pin-alias eviction is one such reassignment. A caller re-deriving
/// direction from a fresh `get_lock_free_connection(handle.addr)` lookup
/// at that point gets the NEW occupant's direction, silently misattributed
/// to the handle it already holds. `ConnectionHandle::direction()` must
/// keep reporting the direction of the connection this handle was
/// actually built from, unaffected by whatever the address index moves on
/// to afterward.
#[tokio::test]
async fn connection_handle_direction_survives_a_same_address_reassignment() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let addr: SocketAddr = "127.0.0.1:7441".parse().unwrap();
    let peer_id = crate::KeyPair::new_for_testing("direction-survives-reassignment").peer_id();

    let (io_inbound, _peer_inbound) = tokio::io::duplex(256);
    let (inbound_stream, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        io_inbound,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut inbound_conn = LockFreeConnection::new(addr, ConnectionDirection::Inbound);
    inbound_conn.stream_handle = Some(Arc::new(inbound_stream));
    inbound_conn.set_state(ConnectionState::Connected);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, Arc::new(inbound_conn)));

    let handle = pool
        .get_existing_connection(addr)
        .expect("the inbound connection must resolve to a handle");
    assert_eq!(
        handle.direction(),
        ConnectionDirection::Inbound,
        "sanity: the handle must report the connection it was actually built from"
    );

    // Reassign the SAME address to a DIFFERENT, outbound connection --
    // exactly what pin-alias eviction (or any other address-keyed
    // reindex) can do after `handle` above was already resolved.
    let (io_outbound, _peer_outbound) = tokio::io::duplex(256);
    let (outbound_stream, _writer_task2, _reader_task2) = LockFreeStreamHandle::new(
        io_outbound,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let mut outbound_conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
    outbound_conn.stream_handle = Some(Arc::new(outbound_stream));
    outbound_conn.set_state(ConnectionState::Connected);
    pool.index_connection_by_addr(addr, Arc::new(outbound_conn));

    // The address-keyed index now genuinely reports the NEW occupant --
    // proving the reassignment is real, not merely assumed.
    assert_eq!(
        pool.get_lock_free_connection(addr).map(|c| c.direction),
        Some(ConnectionDirection::Outbound),
        "sanity: a fresh address-keyed lookup must now see the reassigned connection"
    );

    // The ALREADY-HELD handle must still report its own, original
    // connection's direction -- not the new occupant's.
    assert_eq!(
        handle.direction(),
        ConnectionDirection::Inbound,
        "a handle's direction must survive the address it was resolved at being \
         reassigned to a different connection -- re-deriving it from a fresh lookup would \
         wrongly report the new occupant's direction instead"
    );
}

/// `remove_connection_instance_by_id`'s defensive
/// current-session clear: the stale-instance cleanup path called
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
/// stale, already-superseded instance. This is the same
/// collateral-teardown/reconnect-thrash race class closed elsewhere in this
/// file, reopened through this one remaining check-then-clear call site.
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
// Every recorder installation in this module goes through
// `crate::lifecycle::TransportLifecycleRecorderGuard::install`, which holds
// the single process-wide install lock (owned by `lifecycle.rs`) for its
// entire lifetime and deregisters on drop — so concurrently running tests
// that each install a recorder can never clobber each other's registration
// under the default parallel test harness. See that type's doc comment.

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
    let _guard = {
        let pool = pool.clone();
        let peer_id = peer_id.clone();
        let fresh = fresh.clone();
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
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
        }))
    };

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

/// RED-first (Finding A, P2, `remove_connection_instance_for_peer`,
/// `pool_connect.rs` ~2755): the ask-timeout/hard-fault eviction helper
/// resolves its target either via `connections_by_addr[addr]` or, as a
/// fallback, via the peer's current-connection slot — both filtered by
/// `instance_id`. When a same-address reconnect has ALREADY displaced the
/// failed ask's instance from BOTH indices (a fresh session now occupies
/// `connections_by_addr[addr]` AND the peer's current slot) neither lookup
/// matches, the `?` returns `None`, and the function returns WITHOUT ever
/// releasing the failed instance's `counted_instances` marker — the fresh
/// session survives correctly, but the old instance's `connection_counter`
/// contribution is permanently orphaned (unreachable by `Arc` from anywhere
/// else), a capacity leak.
///
/// RED (observed at `cceadd9`, pre-fix): `raw_connection_counter()` stays at
/// 2 after the eviction call (the stale instance's marker plus the fresh
/// instance's marker), instead of returning to 1 (just the fresh, live
/// session). GREEN (post-fix): the `None` branch releases the failed
/// instance's marker directly via `release_displaced_connection_count`
/// before returning, so the counter returns to 1.
#[tokio::test]
async fn ask_eviction_already_displaced_instance_releases_counter_marker() {
    let peer_id = crate::KeyPair::new_for_testing("ask-eviction-displaced-peer").peer_id();
    let addr: SocketAddr = "127.0.0.1:7492".parse().unwrap();

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));

    // The instance the (now-failed) ask actually ran on: established and
    // counted exactly like a real accepted/finalized connection.
    let stale = make_live_connection(addr, ConnectionDirection::Outbound).await;
    let stale_instance_id = stale
        .stream_handle
        .as_ref()
        .map(|h| h.instance_id())
        .expect("stale connection must have a stream instance");
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, stale.clone()));
    assert_eq!(
        pool.raw_connection_counter_signed(),
        1,
        "test precondition: exactly one counted session (the stale one)"
    );
    // The ask that ran on `stale` has since timed out / hard-faulted.
    if let Some(sh) = stale.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }

    // A fresh reconnect at the SAME bind address lands before the failed
    // ask's own eviction runs — reindexed and published exactly like a real
    // accept/finalize, which displaces `stale` from BOTH the addr index and
    // the peer's current slot in one unconditional publish.
    let fresh = make_live_connection(addr, ConnectionDirection::Inbound).await;
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, fresh.clone()));
    assert_eq!(
        pool.raw_connection_counter_signed(),
        2,
        "test precondition: both the stale and fresh instances are counted"
    );
    assert!(
        pool.connections_by_addr
            .read_sync(&addr, |_, v| Arc::ptr_eq(v, &fresh))
            .unwrap_or(false),
        "test precondition: the addr index must already point at the fresh instance"
    );
    assert!(
        pool.peer_sessions
            .read_sync(&peer_id, |_, s| s
                .current_connection()
                .is_some_and(|c| Arc::ptr_eq(&c, &fresh)))
            .unwrap_or(false),
        "test precondition: the peer's current slot must already point at the fresh instance"
    );

    let before = pool.raw_connection_counter_signed();

    // The failed ask's own recovery path now runs, naming the OLD instance
    // it actually observed — which is unreachable by `Arc` through either
    // index any more.
    let evicted = pool.remove_connection_instance_for_peer(&peer_id, addr, stale_instance_id);
    assert!(
        evicted.is_none(),
        "an instance already displaced from both the addr index and the peer's current slot \
         must not be reported as evicted (nothing matching it remains indexed anywhere)"
    );

    let after = pool.raw_connection_counter_signed();
    assert_eq!(
        after,
        1,
        "the stale instance's connection_counter marker must be released even though it was \
         unreachable by Arc through either index — before={before}, after={after} (leaked \
         {} if unfixed)",
        before.saturating_sub(1)
    );

    // The fresh session must never be disturbed by cleanup for the
    // already-superseded failed ask.
    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &fresh)),
        "the fresh session must survive untouched (got {current:?})"
    );
    assert!(
        fresh.has_live_stream(),
        "the fresh session's background tasks must survive untouched"
    );
}

/// RED-first (Finding B, P1, `compare_and_publish_peer_connection` /
/// `pool_connect.rs` ~334-368): the `AcceptIncoming` call shape — used when
/// `expected` is a known stale/dead existing connection that was never
/// separately evicted first (e.g. `finalize_new_outbound_connection`'s eager
/// `existing_before` rival, or the nested outbound/inbound re-resolve
/// retries' `Some(rival)`) — displaces `expected` from the peer's
/// current-connection slot on CAS success, but (pre-fix) never sweeps
/// `expected`'s own `connections_by_addr`/`addr_to_peer_id` aliases and never
/// releases its `counted_instances` marker the way `ReplaceExisting` does via
/// `evict_before_replace`/`disconnect_connection_instance`. The displaced
/// instance's address aliases go stale (a later lookup by that address would
/// find a dead connection) and its `connection_counter` contribution leaks
/// forever.
///
/// RED (observed at `cceadd9`, pre-fix): after the CAS succeeds,
/// `connections_by_addr[addr]`/`addr_to_peer_id[addr]` still point at the
/// displaced `expected` instance, and `raw_connection_counter()` is 2 (the
/// leaked `expected` marker plus the freshly-counted incoming winner) instead
/// of 1. GREEN (post-fix): both aliases are gone and the counter is exactly
/// 1.
#[tokio::test]
async fn accept_incoming_preferred_over_stale_expected_retires_displaced_instance() {
    let peer_id = crate::KeyPair::new_for_testing("accept-incoming-stale-expected-peer").peer_id();
    let addr: SocketAddr = "127.0.0.1:7493".parse().unwrap();
    let other_addr: SocketAddr = "127.0.0.1:7494".parse().unwrap();

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));

    // `expected`: a stale/dead current session, established and counted
    // exactly like a real accepted/finalized connection — never separately
    // evicted before the compare-and-publish below, exactly like the
    // `AcceptIncoming` call sites' own `Some(rival)`/`existing_before`.
    let expected = make_live_connection(addr, ConnectionDirection::Outbound).await;
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, expected.clone()));
    if let Some(sh) = expected.stream_handle.as_ref() {
        sh.exit_flag.store(true, Ordering::Release);
    }
    assert!(
        !expected.has_live_stream(),
        "test precondition: `expected` must be stale/dead"
    );

    // The freshly-dialed/accepted incoming candidate the tie-break prefers.
    let incoming = make_live_connection(addr, ConnectionDirection::Inbound).await;

    // The exact primitive every `AcceptIncoming` call site routes through.
    let result =
        pool.compare_and_publish_peer_connection(&peer_id, Some(&expected), incoming.clone());
    assert!(
        result.is_ok(),
        "compare-and-publish against the still-installed stale `expected` must succeed: \
         {result:?}"
    );

    // The incoming winner must be intact/current.
    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &incoming)),
        "the incoming winner must be published as the peer's current session (got {current:?})"
    );
    assert!(
        incoming.has_live_stream(),
        "the incoming winner's background tasks must never be swept"
    );

    // `expected`'s own address aliases must be gone — no
    // connections_by_addr/addr_to_peer_id entry pointing at it any more.
    let addr_alias_is_expected = pool
        .connections_by_addr
        .read_sync(&addr, |_, v| Arc::ptr_eq(v, &expected))
        .unwrap_or(false);
    assert!(
        !addr_alias_is_expected,
        "the displaced `expected` instance's connections_by_addr alias must be swept, not left \
         pointing at a dead connection"
    );
    assert!(
        pool.addr_to_peer_id.read_sync(&addr, |_, _| ()).is_none(),
        "the displaced `expected` instance's addr_to_peer_id alias must be swept"
    );

    // Count the incoming winner via the public API (a distinct bind address
    // so this does not disturb the assertions above), mirroring how a real
    // caller separately indexes/counts an accepted/finalized connection.
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), other_addr, incoming.clone()));

    let counter = pool.raw_connection_counter_signed();
    assert_eq!(
        counter,
        1,
        "the displaced `expected` instance's counted_instances marker must be released exactly \
         once, leaving exactly one live session counted (the incoming winner) — got {counter} \
         (leaked {} if unfixed)",
        counter.saturating_sub(1)
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
    let aliased = id.wrapping_add(PENDING_RESPONSES_SIZE as u32);
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

#[test]
fn idle_non_required_peer_session_is_pruned_without_orphaning_live_state() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(1));
    let peer_id = crate::KeyPair::new_for_testing("idle-session").peer_id();
    let addr: SocketAddr = "127.0.0.1:9111".parse().unwrap();

    pool.set_discovered_peer_addr(&peer_id, addr);
    let session = pool
        .peer_sessions
        .read_sync(&peer_id, |_, session| Arc::clone(session))
        .expect("discovered peer has a session");
    *session.last_touched.lock().unwrap() = Instant::now() - Duration::from_secs(301);
    drop(session);

    pool.prune_idle_peer_sessions();

    assert!(
        pool.peer_sessions.read_sync(&peer_id, |_, _| ()).is_none(),
        "idle non-required session must not survive churn indefinitely"
    );
    assert!(
        pool.peer_id_to_addr
            .read_sync(&peer_id, |_, _| ())
            .is_some(),
        "session reclamation must not delete the independently owned route"
    );
}

#[test]
fn route_index_reclaims_superseded_identity_but_keeps_live_routes() {
    // ACTOR_REM_2 R7: `peer_id_to_addr` must not grow unbounded under identity
    // churn. When a NEW peer_id takes over an address (e.g. a pod restarting
    // with a fresh key at the same endpoint), the old identity's route entry is
    // reclaimed; a route whose address is not superseded is retained — the
    // independent-route-authority invariant checked by the test above.
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(1));
    let old = crate::KeyPair::new_for_testing("r7-old").peer_id();
    let new = crate::KeyPair::new_for_testing("r7-new").peer_id();
    let live = crate::KeyPair::new_for_testing("r7-live").peer_id();
    let shared_addr: SocketAddr = "127.0.0.1:9311".parse().unwrap();
    let live_addr: SocketAddr = "127.0.0.1:9312".parse().unwrap();

    pool.set_discovered_peer_addr(&old, shared_addr);
    pool.set_discovered_peer_addr(&live, live_addr);
    assert!(pool.peer_id_to_addr.read_sync(&old, |_, _| ()).is_some());
    assert!(pool.peer_id_to_addr.read_sync(&live, |_, _| ()).is_some());

    // A new identity takes over `shared_addr` in the bounded address index.
    let _ = pool.addr_to_peer_id.upsert_sync(shared_addr, new.clone());

    pool.prune_idle_peer_sessions(); // runs reconcile_route_index

    assert!(
        pool.peer_id_to_addr.read_sync(&old, |_, _| ()).is_none(),
        "R7: a route superseded by a new identity at the same addr must be reclaimed"
    );
    assert!(
        pool.peer_id_to_addr.read_sync(&live, |_, _| ()).is_some(),
        "a route whose address is not superseded must be retained"
    );
    assert!(
        pool.peer_id_to_addr.read_sync(&new, |_, _| ()).is_none(),
        "the superseding identity has no route entry it did not create"
    );
}

#[test]
fn idle_session_with_external_tracker_or_required_route_is_retained() {
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(1));
    let peer_id = crate::KeyPair::new_for_testing("retained-session").peer_id();
    let addr: SocketAddr = "127.0.0.1:9112".parse().unwrap();

    pool.set_discovered_peer_addr(&peer_id, addr);
    let tracker = pool.get_or_create_correlation_tracker(&peer_id);
    let session = pool
        .peer_sessions
        .read_sync(&peer_id, |_, session| Arc::clone(session))
        .unwrap();
    *session.last_touched.lock().unwrap() = Instant::now() - Duration::from_secs(301);
    drop(session);

    pool.prune_idle_peer_sessions();
    assert!(
        pool.peer_sessions.read_sync(&peer_id, |_, _| ()).is_some(),
        "an externally held correlation tracker must fence eviction"
    );
    drop(tracker);

    pool.set_configured_peer_addr(&peer_id, addr);
    let session = pool
        .peer_sessions
        .read_sync(&peer_id, |_, session| Arc::clone(session))
        .unwrap();
    *session.last_touched.lock().unwrap() = Instant::now() - Duration::from_secs(301);
    drop(session);
    pool.prune_idle_peer_sessions();
    assert!(
        pool.peer_sessions.read_sync(&peer_id, |_, _| ()).is_some(),
        "configured peer sessions must remain available for the supervisor"
    );
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

/// T8 regression: non-blocking streaming sends must reject a writer that has
/// already exited instead of accepting work that can only time out.
#[test]
fn streaming_queue_try_push_rejects_teardown() {
    let addr: SocketAddr = "127.0.0.1:9003".parse().unwrap();
    let queue = StreamingQueue::new(1, addr);
    queue.mark_closed_and_wake();

    match queue.try_push(StreamingCommand::Flush) {
        Err(StreamingTryPushError::Closed(actual)) => assert_eq!(actual, addr),
        other => panic!("expected closed streaming queue, got {other:?}"),
    }
}

/// Guard 2 (investigate-self-connect-loop, defense-in-depth): when the
/// target identity resolved for a dial is this node's own peer_id,
/// `connect_via_stream` must refuse immediately at the top, before it ever
/// reaches the tie-break / `should_keep_connection` /
/// `wait_for_preferred_connection` machinery. `should_keep_connection` is
/// unconditionally `false` for self in both directions, so without this
/// short-circuit a self-dial free-runs
/// `outbound_connect_wait_preferred_inbound` ->
/// `outbound_connect_preferred_inbound_timeout_fallback_dial` every
/// `connection_timeout`, forever. This test proves convergence is
/// immediate (well under `connection_timeout`), not merely eventual.
#[tokio::test]
async fn connect_via_stream_short_circuits_self_dial_without_waiting_or_fallback_dial() {
    let self_addr: SocketAddr = "127.0.0.1:40997".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        self_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "self_dial_short_circuit_guard2",
            )),
            ..crate::GossipConfig::default()
        },
    ));
    let self_node_id = registry.peer_id.to_node_id();
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let registry_weak = Arc::downgrade(&registry);

    // connection_timeout is deliberately long: if the short-circuit did not
    // fire, `wait_for_preferred_connection` would block for (up to) this
    // entire duration before falling back to a dial that would also self-loop.
    let connection_timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let result = pool
        .connect_via_stream(
            self_addr,
            Some(self_node_id),
            8,
            connection_timeout,
            registry_weak,
        )
        .await;
    let elapsed = start.elapsed();

    let err = result.expect_err("dialing self must be refused, never silently succeed");
    assert!(
        err.to_string().contains("refusing to dial self peer_id"),
        "expected the self-dial short-circuit error, got: {err}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "self-dial must be refused immediately (no wait_for_preferred_connection, \
         no timeout_fallback_dial); took {elapsed:?} against a {connection_timeout:?} timeout"
    );
}

/// Guard 2 companion: the self-dial identity check must never misfire on a
/// distinct, legitimate peer_id. With a peer_id that differs from the
/// registry's own, `connect_via_stream` must proceed past the guard and
/// attempt a genuine dial (which fails here only because nothing is
/// listening at the target address, not because of the self-dial guard).
#[tokio::test]
async fn connect_via_stream_still_dials_distinct_peer_id_normally() {
    // Order the two generated keys so the local (self) side has the lower
    // GossipNodeId, matching `should_keep_connection`'s NodeId-ordering
    // comparator (`Ordering::Less => is_outbound`) — this keeps the
    // dial-vs-wait-for-preferred-inbound tie-break out of the way so the
    // test observes the guard's pass-through, not tie-break scheduling.
    let key_a = crate::KeyPair::new_for_testing("guard2_real_peer_side_a");
    let key_b = crate::KeyPair::new_for_testing("guard2_real_peer_side_b");
    let (self_key, remote_key) =
        if key_a.peer_id().to_node_id().as_bytes() < key_b.peer_id().to_node_id().as_bytes() {
            (key_a, key_b)
        } else {
            (key_b, key_a)
        };

    let self_addr: SocketAddr = "127.0.0.1:40996".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        self_addr,
        crate::GossipConfig {
            key_pair: Some(self_key),
            ..crate::GossipConfig::default()
        },
    ));
    let remote_node_id = remote_key.peer_id().to_node_id();
    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(2));
    let registry_weak = Arc::downgrade(&registry);

    // Nothing listens here: the dial itself must fail, but with a genuine
    // connect error, never the self-dial short-circuit — proving the
    // identity-keyed guard is specific to true self-dials.
    let unreachable_addr: SocketAddr = "127.0.0.1:40995".parse().unwrap();

    let result = pool
        .connect_via_stream(
            unreachable_addr,
            Some(remote_node_id),
            8,
            Duration::from_secs(2),
            registry_weak,
        )
        .await;

    let err = result.expect_err("dialing an unreachable address must fail");
    assert!(
        !err.to_string().contains("refusing to dial self peer_id"),
        "self-dial guard must never fire for a distinct peer_id, got: {err}"
    );
}

/// Guard 2b (investigate-self-connect-loop, address-only path, codex P1
/// completeness gap): Guard 2 above only fires when `resolved_node_id` is
/// already populated *before* dialing. For an address-only outbound dial —
/// a bootstrap/configured-seed mistake, a DNS refresh that lands on a self
/// address, or a stale `connections_by_addr`/discovery entry with no
/// `node_id` attached — `resolved_node_id` is `None` on entry, so Guard 2 is
/// skipped entirely and (pre-fix) the dial proceeds through TCP/TLS all the
/// way to `finalize_new_outbound_connection`, publishing a connection keyed
/// to this registry's OWN `PeerId`.
///
/// This drives that exact path against a real TLS listener that happens to
/// be serving *this same node's own* identity (the production scenario a
/// stale/misconfigured address-only entry produces: the address just
/// happens to route back to yourself). The listener performs a genuine TLS
/// accept + Hello handshake, so the dialer runs exactly as far as
/// production code does before the post-cert guard must intervene.
///
/// Before the fix: `discovered_node_id` is populated from the peer
/// certificate but never re-checked against identity, so the dial proceeds
/// to `finalize_new_outbound_connection` and succeeds — a genuine self-dial
/// gets indexed/published under this registry's own `PeerId`. After the
/// fix: the cert-verified identity is checked immediately after extraction
/// and the connection is refused before `finalize_new_outbound_connection`
/// is ever reached — never indexed by peer_id or by address, and no
/// retry/wait machinery is armed (the error returns immediately, well under
/// `connection_timeout`).
#[tokio::test]
async fn connect_via_stream_rejects_self_after_cert_identity_discovery_on_address_only_dial() {
    let key_pair = crate::KeyPair::new_for_testing("guard2b_address_only_self_dial");
    let self_addr: SocketAddr = "127.0.0.1:40994".parse().unwrap();

    let mut registry = crate::registry::GossipRegistry::<()>::new(
        self_addr,
        crate::GossipConfig {
            key_pair: Some(key_pair.clone()),
            ..crate::GossipConfig::default()
        },
    );
    registry
        .enable_tls(key_pair.to_secret_key())
        .expect("enable tls");
    let registry = Arc::new(registry);

    // Stand-in for a real inbound listener presenting THIS SAME node's
    // identity — modelling a stale discovery/config entry whose address
    // happens to route back to ourselves. It performs a genuine TLS accept
    // + Hello handshake (mirroring `handle_connection` in handle.rs) so the
    // dialer proceeds exactly as far as production code would.
    let listener = tokio::net::TcpListener::bind(self_addr)
        .await
        .expect("bind self listener");
    let acceptor = registry.tls_config.clone().unwrap().acceptor();
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept inbound tcp");
        let mut tls_stream = acceptor.accept(stream).await.expect("tls accept");
        let alpn = tls_stream
            .get_ref()
            .1
            .alpn_protocol()
            .map(|proto| proto.to_vec());
        // Best-effort: after the fix, the dialer never sends its Hello (it
        // returns before reaching the handshake), so this will simply not
        // complete — the task is aborted by the test, not awaited to
        // success.
        let _ = crate::handshake::perform_hello_handshake(
            &mut tls_stream,
            alpn.as_deref(),
            false,
            None,
            crate::handshake::RemoteBootId::from_bytes([8; 16]),
        )
        .await;
    });

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let registry_weak = Arc::downgrade(&registry);
    let connection_timeout = Duration::from_secs(5);

    let start = std::time::Instant::now();
    let result = pool
        .connect_via_stream(self_addr, None, 8, connection_timeout, registry_weak)
        .await;
    let elapsed = start.elapsed();

    server_task.abort();

    let err = result.expect_err(
        "an address-only dial whose TLS cert proves the peer is this node itself must be \
         refused, never finalized/published as a self-keyed connection",
    );
    assert!(
        err.to_string().contains("refusing to dial self peer_id"),
        "expected the post-cert self-dial guard error, got: {err}"
    );
    assert!(
        pool.get_connection_by_peer_id(&registry.peer_id).is_none(),
        "a rejected self-dial must never be indexed/published under this registry's own peer_id"
    );
    assert_eq!(
        pool.connections_by_addr.len(),
        0,
        "a rejected self-dial must never be indexed by address either"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "the post-cert self-dial guard must reject immediately once the TLS handshake \
         completes, not after waiting on connection_timeout or arming a retry/wait path; \
         took {elapsed:?}"
    );
}

/// R4: a mock transport whose `poll_write` legally returns `Ok(0)` on a
/// non-empty buffer once `threshold` bytes have been accepted — modelling a
/// half-closed write side. Distinct from `Pending`: per the `AsyncWrite`
/// contract this must be treated as no-more-forward-progress-possible, not
/// as "nothing written yet, poll again".
struct ZeroWriteAfterThreshold {
    written: AtomicUsize,
    threshold: usize,
    zero_write_calls: Arc<AtomicUsize>,
}

impl AsyncRead for ZeroWriteAfterThreshold {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Never exercised: `read_context` is `None` in this test, so the
        // writer task's io_task loop never polls the read side.
        Poll::Pending
    }
}

impl AsyncWrite for ZeroWriteAfterThreshold {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let cur = self.written.load(Ordering::SeqCst);
        if cur >= self.threshold {
            self.zero_write_calls.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Ok(0));
        }
        let n = buf.len();
        self.written.fetch_add(n, Ordering::SeqCst);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// R4 (RED -> GREEN): `StreamingCommand::OwnedChunks` batches more chunks
/// than fit in the `MAX_IOV` (64) vectored-write slice array. The vectored
/// write completes only the first 64 chunks; the remaining chunk(s) are
/// drained via a raw, un-wrapped `AsyncWriteExt::write()` call (the only one
/// in this crate — every other write site goes through `write_all` /
/// `write_vectored_all`, which both already fold `Ok(0)` into `WriteZero`).
///
/// Pre-fix, an `Ok(0)` reply from `poll_write` on that raw call leaves
/// `remaining`/`offset_in_chunk` unchanged, so the loop condition never
/// changes and the writer task spins on `.write().await` forever: no
/// progress, no `Pending`, no crash, no log, no timeout — 100% CPU on that
/// task with the connection's queues permanently undrained.
///
/// Post-fix, `Ok(0)` on a non-empty remaining buffer must be treated as
/// `ErrorKind::WriteZero` (matching `write_all`) and routed into the
/// existing write-error teardown, so the writer task exits promptly instead
/// of livelocking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_chunks_zero_write_past_max_iov_exits_instead_of_livelocking() {
    const CHUNK_LEN: usize = 4;
    const MAX_IOV: usize = 64; // must match stream_writer.rs's OwnedChunks MAX_IOV
    const TOTAL_CHUNKS: usize = MAX_IOV + 1; // forces the tail chunk past the vectored batch

    let threshold = MAX_IOV * CHUNK_LEN;
    let zero_write_calls = Arc::new(AtomicUsize::new(0));

    let stream = ZeroWriteAfterThreshold {
        written: AtomicUsize::new(0),
        threshold,
        zero_write_calls: zero_write_calls.clone(),
    };

    let addr: SocketAddr = "127.0.0.1:40999".parse().unwrap();
    let (stream_handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        stream,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );

    let chunks: Vec<bytes::Bytes> = (0..TOTAL_CHUNKS)
        .map(|_| bytes::Bytes::from_static(b"AAAA"))
        .collect();
    stream_handle
        .write_owned_chunks(chunks)
        .expect("queue has room for a single batch");

    // Pre-fix this task never completes (no progress, no `Pending`); bound
    // the wait so the defect fails the test instead of hanging the suite.
    let joined = tokio::time::timeout(Duration::from_secs(5), writer_task).await;

    assert!(
        joined.is_ok(),
        "writer task did not exit within 5s: livelocked on a legal Ok(0) short write \
         past MAX_IOV (observed zero-write polls = {})",
        zero_write_calls.load(Ordering::SeqCst)
    );
    joined.unwrap().expect("writer task must not panic");

    assert_eq!(
        zero_write_calls.load(Ordering::SeqCst),
        1,
        "expected the writer to treat the first Ok(0) as WriteZero and stop, not spin on it"
    );
}

/// R4 regression guard: `write_all` / `write_vectored_all` already fold a
/// legal `Ok(0)` `poll_write` reply into `ErrorKind::WriteZero`. A bare
/// `AsyncWriteExt::write()` call bypasses that and can livelock (this is
/// exactly what R4 fixed in `stream_writer.rs`'s `OwnedChunks` short-write
/// tail). Scan the `connection_pool` source files for any *new* raw
/// `.write(` call site so a future edit can't reintroduce the bug silently.
///
/// This is a coarse text scan, not a type-aware lint, so it allowlists the
/// other legitimate uses of a zero-arg-ish `.write(` that are NOT
/// `AsyncWriteExt::write`:
///   - `MaybeUninit<IoSlice>` slot initialization (always has `IoSlice::new(`
///     in the same call)
///   - raw-pointer/`UnsafeCell` slot writes (`(*hdr_slot).write(header)`,
///     `(*slot.get()).write(response)`)
///   - `std::sync::RwLock::write()` lock acquisition (always a bare
///     `.write()` on its own line in this codebase's chaining style)
///
/// Anything else containing `.write(` is treated as a potential raw
/// `AsyncWriteExt::write()` call and fails the test.
#[test]
fn connection_pool_has_no_unwrapped_raw_async_write_calls() {
    let dir = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src/connection_pool"));

    let is_allowed = |trimmed: &str| -> bool {
        trimmed.contains("IoSlice::new(")
            || trimmed == "(*hdr_slot).write(header);"
            || trimmed == "(*slot_ref.response.get()).write(outcome);"
            || trimmed == ".write()"
    };

    let mut violations = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read connection_pool dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        // Only the flat `include!`d source files; the `tests/` submodule
        // (this file) and `transport_stream.rs` (a real submodule, not part
        // of the io_task write path) are out of scope for this guard.
        if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let contents = std::fs::read_to_string(&path).expect("read source file");
        for (idx, line) in contents.lines().enumerate() {
            let trimmed = line.trim();
            if !trimmed.contains(".write(") {
                continue;
            }
            // `.write_all(` / `.write_vectored(` / `.write_vectored_all(` are
            // the sanctioned wrappers and never match the literal `.write(`
            // substring check (the char after `write` is `_`, not `(`), so
            // no separate exclusion is needed for them here.
            if !is_allowed(trimmed) {
                violations.push(format!("{}:{}: {}", path.display(), idx + 1, trimmed));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "found raw `.write(` call site(s) outside the write_all/write_vectored_all wrappers \
         (R4 livelock class — Ok(0) on a non-empty buffer must be treated as WriteZero, not \
         retried in a loop):\n{}",
        violations.join("\n")
    );
}

/// Test helper: like `make_live_connection`, but installs a caller-supplied
/// (typically shared) correlation tracker instead of getting a fresh one
/// from `LockFreeConnection::new`. Mirrors how a real peer-session tracker
/// (`ConnectionPool::get_or_create_correlation_tracker`) ends up installed on
/// more than one connection instance for the same peer.
async fn make_live_connection_with_correlation(
    addr: SocketAddr,
    direction: ConnectionDirection,
    correlation: Arc<CorrelationTracker>,
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
    conn.correlation = Some(correlation);
    Arc::new(conn)
}

/// RED-first (QA finding R-C, P1, `abort_tasks`/`retire_displaced_expected`,
/// `types.rs` ~108-122 / `pool_connect.rs` ~436-465): `conn.correlation` is a
/// SESSION-level `Arc<CorrelationTracker>`, shared BY POINTER across
/// reconnect instances for a peer (installed via
/// `get_or_create_correlation_tracker`/`add_connection_by_peer_id`). When a
/// displaced/losing connection instance is retired via
/// `retire_displaced_expected` — which runs AFTER the winner is already
/// published as the peer's current session — `expected.abort_tasks()`
/// unconditionally called `correlation.cancel_all()` on that SAME shared
/// tracker, cancelling the WINNER's in-flight ask slots too. This fires on
/// every `AcceptIncoming`-shaped displacement (routine single-node-restart
/// reconnect churn), producing spurious ask failures on a perfectly healthy
/// replacement connection.
///
/// RED (pre-fix): the winner's in-flight slot is cancelled back to
/// `SLOT_EMPTY` by retiring a loser it merely shares a tracker with. GREEN
/// (post-fix): the slot survives, still `SLOT_WAITING`.
#[tokio::test]
async fn retire_displaced_expected_must_not_cancel_winners_shared_correlation() {
    let peer_id = crate::KeyPair::new_for_testing("shared-correlation-peer").peer_id();
    let addr: SocketAddr = "127.0.0.1:7496".parse().unwrap();

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));

    // The peer session's shared, SESSION-level tracker — created up front,
    // exactly like the real outbound/inbound connect paths do via
    // `get_or_create_correlation_tracker` BEFORE installing it onto a raw
    // `conn.correlation` (production `LockFreeConnection::new` always seeds
    // a fresh PRIVATE tracker; only this explicit overwrite makes it
    // session-shared — see `pool_connect.rs` outbound ~3216-3219 / inbound
    // ~3634).
    let tracker = pool.get_or_create_correlation_tracker(&peer_id);

    // `expected`: the losing/displaced instance, indexed+counted exactly
    // like a real, previously accepted/finalized connection for this peer,
    // using the shared tracker.
    let expected =
        make_live_connection_with_correlation(addr, ConnectionDirection::Outbound, tracker.clone())
            .await;
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, expected.clone()));
    assert!(
        expected
            .correlation
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, &tracker)),
        "test precondition: `expected` must use the peer session's shared tracker"
    );

    // `winner`: the fresh instance the tie-break selects, installed with the
    // IDENTICAL shared tracker Arc — exactly what a real reconnect for this
    // peer gets from `handle_correlation`/`add_connection_by_peer_id`.
    let winner =
        make_live_connection_with_correlation(addr, ConnectionDirection::Inbound, tracker.clone())
            .await;

    // An in-flight ask slot on the WINNER's tracker, exactly as if a real ask
    // were awaiting a reply on the connection that is about to become
    // current.
    let guard = tracker.allocate().expect("slot should allocate");
    let id = guard.id();
    let slot = CorrelationTracker::slot_index(id);
    assert_eq!(
        tracker.pending[slot].state.load(Ordering::Acquire),
        SLOT_WAITING
    );

    // The exact primitive every `AcceptIncoming` call site uses, which on
    // success retires `expected` via `retire_displaced_expected` — AFTER
    // `winner` is already published as the peer's current connection.
    let result =
        pool.compare_and_publish_peer_connection(&peer_id, Some(&expected), winner.clone());
    assert!(
        result.is_ok(),
        "compare-and-publish against the still-installed `expected` must succeed: {result:?}"
    );

    let current = pool.get_connection_by_peer_id(&peer_id);
    assert!(
        current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &winner)),
        "the winner must be published as the peer's current session (got {current:?})"
    );

    // The core assertion: retiring the displaced `expected` instance must
    // NOT cancel the shared tracker's in-flight slots — those belong to the
    // still-live `winner`.
    assert_eq!(
        tracker.pending[slot].state.load(Ordering::Acquire),
        SLOT_WAITING,
        "retiring the displaced/losing instance cancelled the shared correlation tracker's \
         in-flight slot — this kills the WINNER's in-flight asks (QA finding R-C)"
    );

    guard.disarm();
}

/// Control for the fix above: a GENUINELY FINAL teardown (no surviving
/// sibling instance for the peer) must still cancel every in-flight slot on
/// the connection's correlation tracker, exactly as before this change —
/// callers awaiting those asks must observe `ConnectionDropped` instead of
/// hanging until timeout.
#[tokio::test]
async fn disconnect_connection_by_peer_id_still_cancels_correlation_on_final_teardown() {
    let peer_id = crate::KeyPair::new_for_testing("final-teardown-peer").peer_id();
    let addr: SocketAddr = "127.0.0.1:7497".parse().unwrap();

    let pool = ConnectionPool::<()>::new(8, Duration::from_secs(5));
    let tracker = pool.get_or_create_correlation_tracker(&peer_id);
    let conn =
        make_live_connection_with_correlation(addr, ConnectionDirection::Outbound, tracker.clone())
            .await;
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, conn.clone()));

    let guard = tracker.allocate().expect("slot should allocate");
    let id = guard.id();
    let slot = CorrelationTracker::slot_index(id);
    assert_eq!(
        tracker.pending[slot].state.load(Ordering::Acquire),
        SLOT_WAITING
    );

    // No rival/winner is ever published for this peer: this is a genuinely
    // final teardown, not a displacement.
    let removed = pool.disconnect_connection_by_peer_id(&peer_id);
    assert!(removed.is_some_and(|c| Arc::ptr_eq(&c, &conn)));

    assert_eq!(
        tracker.pending[slot].state.load(Ordering::Acquire),
        SLOT_EMPTY,
        "a genuinely final teardown (no surviving sibling instance) must still cancel the \
         connection's correlation tracker"
    );

    guard.disarm();
}

/// RED-first: skipping the DIRECT `correlation.cancel_all()` call in
/// `abort_tasks_keep_correlation` is not sufficient on its own. The
/// connection's real IO task carries its own `ExitGuard`, which cancels the
/// same tracker on Drop unless it independently infers this instance is
/// superseded — and that inference can miss:
///
/// - the exiting instance's `ExitGuard` captured `peer_id: None` in its
///   `ReadContext` (the realistic shape for an outbound dial made before the
///   peer's identity was learned, even though the connection is later
///   associated with a peer via `add_connection_by_peer_id`), so it falls
///   back to an ADDRESS-keyed lookup instead of a peer-keyed one;
/// - `retire_displaced_expected`'s own address-alias sweep removes the
///   exiting instance's `connections_by_addr` entry, and the higher-level
///   caller that would re-index the WINNER at that same address (e.g.
///   `finalize_new_outbound_connection`) has not run yet — exactly the
///   window between `compare_and_publish_peer_connection` returning and that
///   caller's own follow-up indexing step.
///
/// In that window the address-keyed fallback resolves to nothing, the
/// `ExitGuard` concludes "not superseded", and unconditionally cancels the
/// shared tracker via the task-abort path — even though
/// `abort_tasks_keep_correlation` correctly skipped its own direct call.
#[tokio::test]
async fn retire_displaced_expected_exit_guard_must_not_cancel_via_task_abort_when_peer_id_unresolved()
 {
    let peer_id = crate::KeyPair::new_for_testing("shared-correlation-exit-guard-peer").peer_id();
    let addr: SocketAddr = "127.0.0.1:7498".parse().unwrap();

    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("exit-guard-race-registry")),
            ..crate::GossipConfig::default()
        },
    ));
    let pool = &registry.connection_pool;

    let tracker = pool.get_or_create_correlation_tracker(&peer_id);

    // `expected`: the losing/displaced instance, with a REAL IO task whose
    // `ExitGuard` captured NO peer_id at spawn time.
    let (io, _keep) = tokio::io::duplex(1024);
    let read_ctx = ReadContext {
        streaming_state_handoff: None,
        registry_weak: Arc::downgrade(&registry),
        peer_addr: addr,
        session_source: addr,
        peer_id: None,
        max_message_size: MASTER_BUFFER_SIZE,
        expected_schema_hash: None,
        aligned_pool: pool.aligned_bytes_pool(),
        inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
        response_correlation: Some(tracker.clone()),
        response_writer: None,
        tell_handler_sync: None,
        tell_handler_sync_context: None,
        ask_immediate_handler_sync: None,
        ask_handler_sync: None,
        sync_actor_handler: None,
    };
    let (sh, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::Global,
        BufferConfig::default(),
        None,
        Some(read_ctx),
    );
    let mut expected_conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
    expected_conn.stream_handle = Some(Arc::new(sh));
    expected_conn.set_state(ConnectionState::Connected);
    expected_conn.correlation = Some(tracker.clone());
    expected_conn
        .task_tracker
        .set_writer(writer_task.abort_handle());
    let expected = Arc::new(expected_conn);
    assert!(pool.add_connection_by_peer_id(peer_id.clone(), addr, expected.clone()));

    // Let the writer task actually start running (construct its `ExitGuard`
    // and park on its first real await point) before we retire `expected` —
    // otherwise `.abort()` can cancel the task before it is ever polled even
    // once, which drops the future without ever constructing `_exit_guard`
    // and trivially (uninterestingly) "wins" this test.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // `winner`: the fresh instance the tie-break selects, sharing the
    // IDENTICAL tracker Arc. Published in `connections_by_peer` by the
    // compare-and-publish below but — exactly like the real
    // `AcceptIncoming` call sites, which index it by address SEPARATELY,
    // outside `compare_and_publish_peer_connection` — never installed in
    // `connections_by_addr` here.
    let winner =
        make_live_connection_with_correlation(addr, ConnectionDirection::Inbound, tracker.clone())
            .await;

    // An in-flight ask slot on the WINNER's (shared) tracker.
    let guard = tracker.allocate().expect("slot should allocate");
    let id = guard.id();
    let slot = CorrelationTracker::slot_index(id);

    let result =
        pool.compare_and_publish_peer_connection(&peer_id, Some(&expected), winner.clone());
    assert!(
        result.is_ok(),
        "compare-and-publish against the still-installed `expected` must succeed: {result:?}"
    );

    // Give the aborted writer task's `ExitGuard` a bounded chance to run.
    for _ in 0..200 {
        if tracker.pending[slot].state.load(Ordering::Acquire) != SLOT_WAITING {
            break;
        }
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    debug_assert!(
        writer_task.is_finished(),
        "test setup: the writer task must actually exit"
    );

    assert_eq!(
        tracker.pending[slot].state.load(Ordering::Acquire),
        SLOT_WAITING,
        "the exiting loser's OWN IO-task ExitGuard cancelled the shared correlation tracker's \
         in-flight slot via the task-abort path (peer_id unresolved + winner not yet \
         addr-indexed) — this kills the WINNER's in-flight asks even though the direct \
         cancel_all() call was skipped"
    );

    guard.disarm();
}

// RED: a fresh outbound connect publishes the connection (making it
// resolvable via `get_connection_by_addr`) before the identifying FullSync
// is built and enqueued -- `finalize_new_outbound_connection` awaits a
// `gossip_state` lock and an actor-pairs snapshot between those two steps.
// A routed ask that discovers the freshly published connection in that
// window enqueues its RouteBind onto the SAME per-connection write queue,
// so the acceptor can see RouteBind arrive before the identifying FullSync
// and drop the connection ("RouteBind arrived before connection setup").
//
// This test holds `gossip_state`'s lock from the test task so a task
// finalizing a fresh outbound connect parks exactly at that await, publishes
// a racing routed ask's RouteBind while it is parked, then releases the
// lock and asserts what actually lands on the wire first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_outbound_connect_sends_identifying_fullsync_before_racing_routed_ask() {
    use crate::{GossipConfig, registry::GossipRegistry};
    use tokio::io::AsyncReadExt;

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("fullsync-race-local")),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();
    let addr: SocketAddr = "127.0.0.1:41777".parse().unwrap();

    let (io, mut peer_io) = tokio::io::duplex(4096);

    // Hold `gossip_state` from the test task. In the unfixed code the
    // connection is published (discoverable via `get_connection_by_addr`)
    // long before the identify build ever touches this lock, so a racing
    // task spun up below can find and exploit that window while the lock is
    // still held. In the fixed code the identify build -- which now runs
    // BEFORE publish -- itself needs this same lock, so publish cannot
    // happen at all until this test task releases it; the racing task
    // then only ever discovers the connection after the identify has
    // already been enqueued.
    let guard = registry.gossip_state.lock().await;

    let finalize_registry = Arc::downgrade(&registry);
    let finalize_task = tokio::spawn(async move {
        pool.finalize_new_outbound_connection(addr, io, finalize_registry, None, addr, None)
            .await
            .expect("finalize outbound connection")
    });

    // Race a routed ask against the connect on a separate task: spin trying
    // to discover the connection and, the instant it is, enqueue a
    // RouteBind onto its write queue exactly as a real routed ask would.
    let racer_pool = registry.connection_pool.clone();
    let racer_task = tokio::spawn(async move {
        loop {
            if let Some(conn) = racer_pool.get_connection_by_addr(&addr) {
                let stream = conn
                    .stream_handle
                    .clone()
                    .expect("finalized connection must have a stream handle");
                stream
                    .write_routed_actor_ask(1, 42, 99, bytes::Bytes::from_static(b"race"))
                    .await
                    .expect("racing routed ask must be able to enqueue");
                return;
            }
            tokio::task::yield_now().await;
        }
    });

    // Hold the lock long enough that, on the multi-thread runtime, the
    // racer task -- spinning freely on another worker thread this entire
    // time -- has effectively unbounded opportunity to win the unfixed
    // code's (synchronous, lock-independent) publish-then-later-identify
    // window if it exists at all. Only then release it, letting a
    // fixed-code finalize proceed to build and enqueue its identify.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(guard);

    let handle = finalize_task.await.expect("finalize task must not panic");
    racer_task.await.expect("racer task must not panic");
    drop(handle);

    let mut control = [0u8; 4];
    peer_io
        .read_exact(&mut control)
        .await
        .expect("a first frame must have been written to the wire");
    let kind = crate::framing::decode_control(control).unwrap().kind;
    assert_eq!(
        kind,
        crate::framing::WireKind::Gossip,
        "the identifying FullSync must be the first frame on a fresh \
         outbound connection, even when a routed ask races the connect and \
         discovers the connection while the identify is still being built \
         -- got {kind:?} first instead, which is exactly what makes the \
         acceptor drop the connection (\"RouteBind arrived before \
         connection setup\")"
    );
}

// RED: the reader/writer IO tasks for a fresh outbound connection are
// already running by the time its identify build reaches the `gossip_state`
// await (identify is built and sent before this candidate is published
// anywhere). If the peer disconnects while that await is contended, the IO
// tasks notice and exit -- but nothing is indexed for this candidate yet,
// so no other cleanup path can reach it. The identify send that follows
// then fails on the now-dead stream. A finalize that merely logs that
// failure and continues would go on to publish and count this dead
// candidate as the peer's live "current" connection: it can never actually
// identify itself, nothing will ever reap it, and it permanently suppresses
// a redial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_outbound_connect_aborts_when_identify_send_fails_instead_of_publishing_dead_candidate()
 {
    use crate::{GossipConfig, registry::GossipRegistry};

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("identify-fail-local")),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();
    let addr: SocketAddr = "127.0.0.1:41778".parse().unwrap();

    let (io, peer_io) = tokio::io::duplex(4096);

    // Hold `gossip_state` so the finalize task's identify build parks here,
    // exactly as in the identify-first race test above.
    let guard = registry.gossip_state.lock().await;

    let finalize_registry = Arc::downgrade(&registry);
    let pool_for_finalize = pool.clone();
    let finalize_task = tokio::spawn(async move {
        pool_for_finalize
            .finalize_new_outbound_connection(addr, io, finalize_registry, None, addr, None)
            .await
    });

    // Simulate the peer disconnecting while the candidate is parked here --
    // before anything is indexed for it, so the IO tasks' own dead-stream
    // cleanup has nothing to reach.
    drop(peer_io);

    // Give the reader/writer IO tasks, already running against the now-
    // closed stream, real time to notice and mark themselves exited before
    // the finalize task is allowed to proceed past the lock.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(guard);

    let result = finalize_task.await.expect("finalize task must not panic");

    assert!(
        result.is_err(),
        "finalize must fail when the identifying FullSync could not be \
         sent to a dead stream, not silently succeed with a dead candidate"
    );
    assert!(
        pool.get_connection_by_addr(&addr).is_none(),
        "a candidate whose identify send failed must never be published -- \
         it can never actually identify itself to the acceptor and nothing \
         can later reap it, which would otherwise suppress a redial forever"
    );
    assert_eq!(
        pool.connection_count(),
        0,
        "a candidate whose identify send failed must not leak a counted \
         connection-instance"
    );
}

// R-11 regression coverage for `finalize_new_outbound_connection`'s own
// arm-then-identify sequencing (as opposed to `arm_sequence_reset_for_new_session`'s
// own internal race-safety, already covered by the `qa_r11_*` tests in
// `registry.rs`): a fresh outbound connect to a peer that has already been
// seen at a HIGH gossip sequence over an old session must still accept that
// peer's post-restart sync at a LOW sequence over the NEW session finalize
// just established. This is only possible if finalize armed the exemption
// for the new session before anything could act on it -- exactly what
// restoring the original arm-before-identify ordering guarantees, since the
// two run strictly sequentially within `finalize_new_outbound_connection`
// with no interleaving possible.
#[tokio::test]
async fn fresh_outbound_connect_arms_restart_exemption_so_post_restart_sync_is_accepted() {
    use crate::{GossipConfig, RemoteActorLocation, registry::GossipRegistry};
    use std::collections::HashMap;

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("r11-wiring-local")),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();

    let owner = crate::KeyPair::new_for_testing("r11-wiring-owner").peer_id();
    let node_id = owner.to_node_id();
    let peer_addr: SocketAddr = "127.0.0.1:41780".parse().unwrap();
    let old_session_addr: SocketAddr = "127.0.0.1:57201".parse().unwrap();
    let new_session_addr: SocketAddr = "127.0.0.1:57202".parse().unwrap();

    registry
        .add_peer_with_node_id(
            peer_addr,
            Some(node_id),
            crate::addr_ownership::ClaimKind::Verified,
        )
        .await;

    // Pre-restart: peer is at a high sequence over an old session.
    let mut pre_restart_actors = HashMap::new();
    pre_restart_actors.insert(
        "r11-wiring/x".to_string(),
        RemoteActorLocation::new_with_peer(peer_addr, owner.clone()),
    );
    registry
        .merge_full_sync_from(
            pre_restart_actors,
            HashMap::new(),
            owner.clone(),
            peer_addr,
            Some(old_session_addr),
            None,
            40,
            crate::current_timestamp(),
        )
        .await;

    // The peer restarts and we dial it fresh, threading `fresh_session_node_id`
    // through exactly as the real dial path does -- this is what arms the
    // one-shot lower-sequence exemption for `new_session_addr`.
    let (io, _peer_io) = tokio::io::duplex(4096);
    let handle = pool
        .finalize_new_outbound_connection(
            peer_addr,
            io,
            Arc::downgrade(&registry),
            Some(node_id),
            new_session_addr,
            Some(node_id),
        )
        .await
        .expect("finalize outbound connection");
    drop(handle);

    // The restarted peer's first sync after reconnecting resets to a LOW
    // sequence over the NEW session. Accepted only if the exemption armed
    // above actually took effect -- proven by a brand-new actor replacing
    // the pre-restart set.
    let mut restart_actors = HashMap::new();
    restart_actors.insert(
        "r11-wiring/y".to_string(),
        RemoteActorLocation::new_with_peer(peer_addr, owner.clone()),
    );
    registry
        .merge_full_sync_from(
            restart_actors,
            HashMap::new(),
            owner.clone(),
            peer_addr,
            Some(new_session_addr),
            None,
            1,
            crate::current_timestamp(),
        )
        .await;

    assert!(
        registry.lookup_actor("r11-wiring/y").await.is_some(),
        "R-11: the restarted peer's post-reconnect sync (low sequence, new \
         session) must be accepted -- finalize_new_outbound_connection must \
         actually arm the lower-sequence exemption for the session it just \
         established"
    );
    assert!(
        registry.lookup_actor("r11-wiring/x").await.is_none(),
        "the restart sync must have pruned the pre-restart actor"
    );
}

// RED: if the registry is already gone (e.g. shutdown) by the time
// `finalize_new_outbound_connection` reaches its identify step,
// `registry_weak.upgrade()` fails there. That must be treated exactly like
// a failed identify send -- abort finalization and tear the candidate back
// down -- not silently fall through to publishing it. Silently continuing
// would leave the connection published and counted but permanently
// un-identified (the identify gate armed by `begin_identify_gate` never
// gets resolved), so any `write_routed_actor_ask` racing the connect and
// parked in `wait_until_identified` would hang forever instead of being
// released with an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_outbound_connect_releases_racing_waiter_when_registry_is_gone_during_identify() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let addr: SocketAddr = "127.0.0.1:41781".parse().unwrap();

    let (registry_weak, pool) = {
        let registry = Arc::new(GossipRegistry::<()>::new(
            "127.0.0.1:0".parse().unwrap(),
            GossipConfig {
                key_pair: Some(crate::KeyPair::new_for_testing("gate-stuck-registry-gone")),
                ..Default::default()
            },
        ));
        (Arc::downgrade(&registry), registry.connection_pool.clone())
        // `registry` (the only strong reference) drops here; `pool` is an
        // independent `Arc` clone that survives, exactly like a real
        // connection pool outliving a registry mid-shutdown.
    };
    assert!(
        registry_weak.upgrade().is_none(),
        "sanity: the registry must actually be gone before finalize runs"
    );

    let (io, _peer_io) = tokio::io::duplex(4096);

    // Race a routed ask against the connect on a separate task, exactly as
    // in the identify-first race test above: spin trying to discover the
    // connection and, the instant it is, enqueue onto its write queue --
    // which must park in `wait_until_identified` until the gate resolves
    // one way or the other.
    let racer_pool = pool.clone();
    let racer_task = tokio::spawn(async move {
        // Bounded discovery poll: if this candidate is torn down (correctly)
        // before ever being observed, there was nothing to race against --
        // `None` here is a fine, unproblematic outcome, distinct from
        // actually racing and getting back a success (`Some(Ok(()))`, the
        // real bug this guards against) or an expected failure
        // (`Some(Err(_))`).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Some(conn) = racer_pool.get_connection_by_addr(&addr)
                && let Some(stream) = conn.stream_handle.clone()
            {
                return Some(
                    stream
                        .write_routed_actor_ask(1, 42, 99, bytes::Bytes::from_static(b"race"))
                        .await,
                );
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::task::yield_now().await;
        }
    });

    let finalize_result = pool
        .finalize_new_outbound_connection(addr, io, registry_weak, None, addr, None)
        .await;

    assert!(
        finalize_result.is_err(),
        "finalize must fail when the registry is gone during identify, not \
         silently publish an unidentified candidate"
    );
    assert!(
        pool.get_connection_by_addr(&addr).is_none(),
        "a candidate that could never identify itself must not be left \
         published"
    );
    assert_eq!(
        pool.connection_count(),
        0,
        "a candidate that could never identify itself must not leak a \
         counted connection-instance"
    );

    // The racer must have been released -- with an error, since the
    // connection was torn down before ever identifying -- rather than
    // hanging forever waiting for a gate that would otherwise never
    // resolve. Bounded so a real regression (a hang) fails the test instead
    // of blocking the suite.
    let racer_outcome = tokio::time::timeout(std::time::Duration::from_secs(5), racer_task).await;
    match racer_outcome {
        Ok(join_result) => match join_result.expect("racer task must not panic") {
            Some(write_result) => {
                assert!(
                    write_result.is_err(),
                    "a routed ask racing a connect whose identify never \
                     resolves must fail, not silently succeed"
                );
            }
            None => {
                // Never discovered the candidate before it was torn down --
                // nothing raced, so nothing more to assert here; the
                // finalize-side assertions above already cover correctness.
            }
        },
        Err(_elapsed) => {
            panic!(
                "a routed ask racing a connect whose registry disappeared \
                 during identify hung instead of being released with an \
                 error -- the identify gate was left stuck"
            );
        }
    }
}

// RED (without `IdentifyGateGuard`): `finalize_new_outbound_connection`'s
// only real caller awaits it inside a `tokio::time::timeout`, so the whole
// call can be cancelled out from under it at any await point -- including
// while parked building/sending its own identify. A candidate that was
// already published and counted by the time that happens must still be
// fully retired (and its identify gate resolved with an error for any
// racing waiter), not left behind as a published-but-unidentified zombie
// with no task left to ever finish it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_outbound_connect_cancelled_mid_identify_does_not_strand_a_published_candidate() {
    use crate::{GossipConfig, registry::GossipRegistry};

    let registry = Arc::new(GossipRegistry::<()>::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "gate-stuck-cancel-mid-identify",
            )),
            ..Default::default()
        },
    ));
    let pool = registry.connection_pool.clone();
    let addr: SocketAddr = "127.0.0.1:41782".parse().unwrap();

    let (io, _peer_io) = tokio::io::duplex(4096);

    // Hold `gossip_state` so the finalize task parks building its identify
    // (the same chokepoint the earlier race tests use), then cancel the
    // whole finalize call from the outside while it is parked there --
    // mirroring the real caller's connect-attempt timeout elapsing at
    // exactly this point.
    let guard = registry.gossip_state.lock().await;

    let finalize_registry = Arc::downgrade(&registry);
    let pool_for_finalize = pool.clone();
    let finalize_task = tokio::spawn(async move {
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            pool_for_finalize.finalize_new_outbound_connection(
                addr,
                io,
                finalize_registry,
                None,
                addr,
                None,
            ),
        )
        .await
    });

    // A routed ask racing the connect, parked in `wait_until_identified`
    // once it discovers the (published, but not yet identified) candidate.
    let racer_pool = pool.clone();
    let racer_task = tokio::spawn(async move {
        loop {
            if let Some(conn) = racer_pool.get_connection_by_addr(&addr)
                && let Some(stream) = conn.stream_handle.clone()
            {
                return stream
                    .write_routed_actor_ask(1, 42, 99, bytes::Bytes::from_static(b"race"))
                    .await;
            }
            tokio::task::yield_now().await;
        }
    });

    // Hold the lock well past the 50ms timeout above so the cancellation
    // actually fires while finalize is genuinely parked, then release it --
    // finalize's own future is already gone by then; releasing just lets
    // anything else contending for the lock proceed.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    drop(guard);

    let finalize_outcome = finalize_task.await.expect("finalize task must not panic");
    assert!(
        finalize_outcome.is_err(),
        "sanity: the outer timeout must actually have elapsed while finalize \
         was parked, cancelling it"
    );

    assert!(
        pool.get_connection_by_addr(&addr).is_none(),
        "a candidate cancelled mid-identify must not be left published"
    );
    assert_eq!(
        pool.connection_count(),
        0,
        "a candidate cancelled mid-identify must not leak a counted \
         connection-instance"
    );

    let racer_outcome = tokio::time::timeout(std::time::Duration::from_secs(5), racer_task).await;
    match racer_outcome {
        Ok(join_result) => {
            let write_result = join_result.expect("racer task must not panic");
            assert!(
                write_result.is_err(),
                "a routed ask racing a connect that gets cancelled mid-identify \
                 must fail, not silently succeed"
            );
        }
        Err(_elapsed) => {
            panic!(
                "a routed ask racing a connect cancelled mid-identify hung \
                 instead of being released with an error -- the identify \
                 gate was left stuck"
            );
        }
    }
}

/// A full-sync handler that resumes after its address has been claimed by a
/// newer commit must apply NOTHING that is keyed on that address.
///
/// The displacement is constructed statically: the address is fenced at a
/// position no claim can beat before the handler runs, which is the same state
/// the handler would observe had a competing claim committed and projected
/// while it was suspended.
#[tokio::test]
async fn displaced_full_sync_claim_records_no_address_keyed_state() {
    let bind_addr: SocketAddr = "10.77.0.90:9601".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("displaced-full-sync-local")),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_id = crate::KeyPair::new_for_testing("displaced-full-sync-remote").peer_id();
    let tcp_source: SocketAddr = "10.77.0.91:40001".parse().unwrap();
    let advertised: SocketAddr = "10.77.0.91:9601".parse().unwrap();

    // The address has already moved past anything this handler can commit.
    registry
        .gossip_state
        .lock()
        .await
        .tombstone_ownership_projection(advertised, crate::registry_owner::CommitSeq::MAX);

    let msg = crate::registry::RegistryMessage::FullSync {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(advertised.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: Some(crate::registry::GossipExtensionsV1 {
            clock_probe: Some(crate::registry::ClockProbeV1 {
                sample_id: 7,
                sender_wall_ns: crate::current_timestamp_nanos(),
            }),
            clock_echo: None,
        }),
    };

    super::handle_incoming_message(
        registry.clone(),
        tcp_source,
        tcp_source,
        Some(peer_id.clone()),
        msg,
    )
    .await
    .expect("a displaced FullSync must be dropped, not error");

    assert!(
        !registry.has_pending_clock_echo(&advertised),
        "a displaced claim must not record address-keyed clock state"
    );
    let state = registry.gossip_state.lock().await;
    assert!(
        !state.peers.contains_key(&advertised),
        "a displaced claim must not create a peer entry at the contested address"
    );
    assert_eq!(
        state.full_sync_exchanges, 1,
        "the authenticated frame remains valid after rebinding to its observed transport"
    );
    drop(state);
    assert_eq!(
        registry.connection_pool.get_configured_peer_addr(&peer_id),
        Some(tcp_source),
        "a refused advertised alias must route only through the verified transport source"
    );
}

/// The handler receives both the wire claim and the identity authenticated by
/// the transport. Ownership must be derived from the latter, and a mismatch
/// must be rejected before it reaches the owner actor.
#[tokio::test]
async fn full_sync_address_claim_is_bound_to_authenticated_transport_identity() {
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        "10.77.0.94:9601".parse().unwrap(),
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "full-sync-auth-binding-local",
            )),
            ..crate::GossipConfig::default()
        },
    ));
    let authenticated =
        crate::KeyPair::new_for_testing("full-sync-auth-binding-attacker").peer_id();
    let payload_claim = crate::KeyPair::new_for_testing("full-sync-auth-binding-victim").peer_id();
    let tcp_source: SocketAddr = "10.77.0.95:42001".parse().unwrap();
    let advertised: SocketAddr = "10.77.0.95:9601".parse().unwrap();

    let msg = crate::registry::RegistryMessage::FullSync {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id: payload_claim,
        sender_bind_addr: Some(advertised.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: None,
    };
    super::handle_incoming_message(
        registry.clone(),
        tcp_source,
        tcp_source,
        Some(authenticated),
        msg,
    )
    .await
    .expect("identity mismatch must be dropped, not surfaced as a transport error");

    assert_eq!(
        registry.registry_owner.routes_to(&advertised),
        None,
        "a payload identity must not create an ownership claim for the authenticated connection"
    );
    let state = registry.gossip_state.lock().await;
    assert!(!state.peers.contains_key(&advertised));
    assert_eq!(state.full_sync_exchanges, 0);
}

/// Same rule on the response arm: a FullSyncResponse whose claim has been
/// superseded records no extensions, no peer/session state and no connection
/// index at the contested address.
#[tokio::test]
async fn displaced_full_sync_response_claim_records_no_address_keyed_state() {
    let bind_addr: SocketAddr = "10.77.0.92:9601".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "displaced-full-sync-response-local",
            )),
            ..crate::GossipConfig::default()
        },
    ));

    let peer_id = crate::KeyPair::new_for_testing("displaced-full-sync-response-remote").peer_id();
    let tcp_source: SocketAddr = "10.77.0.93:40002".parse().unwrap();
    let advertised: SocketAddr = "10.77.0.93:9601".parse().unwrap();

    let stale_time = crate::current_timestamp_millis().saturating_sub(3_600_000);
    {
        let mut state = registry.gossip_state.lock().await;
        let mut peer = stale_peer_info(advertised, stale_time);
        peer.failures = 3;
        state.peers.insert(advertised, peer);
        state.tombstone_ownership_projection(advertised, crate::registry_owner::CommitSeq::MAX);
    }

    let msg = crate::registry::RegistryMessage::FullSyncResponse {
        local_actors: Vec::new(),
        known_actors: Vec::new(),
        sender_peer_id: peer_id.clone(),
        sender_bind_addr: Some(advertised.to_string()),
        sequence: 1,
        wall_clock_time: crate::current_timestamp(),
        extensions: Some(crate::registry::GossipExtensionsV1 {
            clock_probe: Some(crate::registry::ClockProbeV1 {
                sample_id: 11,
                sender_wall_ns: crate::current_timestamp_nanos(),
            }),
            clock_echo: None,
        }),
    };

    super::handle_incoming_message(
        registry.clone(),
        tcp_source,
        tcp_source,
        Some(peer_id.clone()),
        msg,
    )
    .await
    .expect("a displaced FullSyncResponse must be dropped, not error");

    assert!(
        !registry.has_pending_clock_echo(&advertised),
        "a displaced claim must not record address-keyed clock state"
    );
    let state = registry.gossip_state.lock().await;
    assert_eq!(
        state
            .peers
            .get(&advertised)
            .expect("the pre-existing peer entry must survive")
            .failures,
        3,
        "a displaced claim must not reset another owner's failure state"
    );
    assert_eq!(
        state.full_sync_exchanges, 1,
        "the authenticated response remains valid after rebinding to its observed transport"
    );
    drop(state);
    assert_eq!(
        registry.connection_pool.get_configured_peer_addr(&peer_id),
        Some(tcp_source),
        "a refused advertised alias must route only through the verified transport source"
    );
}

/// A claim can be current during the handler's first projection and then be
/// displaced while `merge_full_sync_from` is collecting actor candidates.
/// The ownership commit must fence the merge itself, not only the handler's
/// pre/post bookkeeping sections.
async fn run_full_sync_displaced_during_merge_is_dropped(response: bool) {
    let bind_addr: SocketAddr = if response {
        "10.78.0.90:9602".parse().unwrap()
    } else {
        "10.78.0.90:9601".parse().unwrap()
    };
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        bind_addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(format!(
                "full-sync-owner-race-local-{response}"
            ))),
            ..crate::GossipConfig::default()
        },
    ));
    let stale_peer =
        crate::KeyPair::new_for_testing(format!("full-sync-owner-race-stale-{response}")).peer_id();
    let successor =
        crate::KeyPair::new_for_testing(format!("full-sync-owner-race-successor-{response}"))
            .peer_id();
    let tcp_source: SocketAddr = if response {
        "10.78.0.91:41002".parse().unwrap()
    } else {
        "10.78.0.91:41001".parse().unwrap()
    };
    let advertised: SocketAddr = if response {
        "10.78.0.91:9602".parse().unwrap()
    } else {
        "10.78.0.91:9601".parse().unwrap()
    };
    let stale_actor = if response {
        "ownership-race/response/stale"
    } else {
        "ownership-race/full-sync/stale"
    };

    assert_eq!(
        registry
            .add_peer_with_node_id(
                advertised,
                Some(stale_peer.to_node_id()),
                crate::addr_ownership::ClaimKind::Verified,
            )
            .await,
        crate::addr_ownership::AddrClaimOutcome::Accepted,
        "test precondition: the current authenticated session owns its advertised address"
    );

    let registry_for_hook = Arc::clone(&registry);
    let stale_for_hook = stale_peer.clone();
    let successor_for_hook = successor.clone();
    let _guard =
        crate::lifecycle::TransportLifecycleRecorderGuard::install(Arc::new(move |event| {
            let crate::TransportLifecycleEvent::FullSyncApplyPendingMutation { peer, addr } = event
            else {
                return;
            };
            if peer != stale_for_hook || addr != advertised {
                return;
            }
            crate::set_transport_lifecycle_recorder(None);
            let registry = Arc::clone(&registry_for_hook);
            let successor = successor_for_hook.clone();
            let stale = stale_for_hook.clone();
            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let stale_generation = registry
                        .registry_owner
                        .claim_generation_for_test(advertised)
                        .await
                        .expect("in-flight FullSync claim generation");
                    registry
                        .registry_owner
                        .release(advertised, stale, stale_generation)
                        .await
                        .expect("stale session releases before successor claim");
                    let outcome = registry
                        .add_peer_with_node_id(
                            advertised,
                            Some(successor.to_node_id()),
                            crate::addr_ownership::ClaimKind::Verified,
                        )
                        .await;
                    assert_eq!(
                        outcome,
                        crate::addr_ownership::AddrClaimOutcome::Accepted,
                        "the verified successor must displace the stale provisional claim"
                    );
                });
            });
        }));

    let local_actors = vec![(
        stale_actor.to_string(),
        crate::RemoteActorLocation::new_with_peer(advertised, stale_peer.clone()),
    )];
    let extensions = Some(crate::registry::GossipExtensionsV1 {
        clock_probe: Some(crate::registry::ClockProbeV1 {
            sample_id: 99,
            sender_wall_ns: crate::current_timestamp_nanos(),
        }),
        clock_echo: None,
    });
    let msg = if response {
        crate::registry::RegistryMessage::FullSyncResponse {
            local_actors,
            known_actors: Vec::new(),
            sender_peer_id: stale_peer.clone(),
            sender_bind_addr: Some(advertised.to_string()),
            sequence: 77,
            wall_clock_time: crate::current_timestamp(),
            extensions,
        }
    } else {
        crate::registry::RegistryMessage::FullSync {
            local_actors,
            known_actors: Vec::new(),
            sender_peer_id: stale_peer.clone(),
            sender_bind_addr: Some(advertised.to_string()),
            sequence: 77,
            wall_clock_time: crate::current_timestamp(),
            extensions,
        }
    };

    super::handle_incoming_message(
        registry.clone(),
        tcp_source,
        tcp_source,
        Some(stale_peer.clone()),
        msg,
    )
    .await
    .expect("superseded full sync must be dropped, not error");

    assert_eq!(
        registry.registry_owner.routes_to(&advertised),
        Some(successor.clone()),
        "the verified successor remains authoritative"
    );
    assert_eq!(
        registry
            .connection_pool
            .addr_to_peer_id
            .read_sync(&advertised, |_, peer| peer.clone()),
        Some(successor.clone()),
        "address routing must retain the verified successor"
    );
    assert_eq!(
        registry
            .connection_pool
            .get_configured_peer_addr(&stale_peer),
        None,
        "a superseded handler phase must not leave a reverse route for the displaced claimant"
    );
    assert!(
        registry.lookup_actor(stale_actor).await.is_none(),
        "the superseded claim must not apply actor ownership after displacement"
    );
    assert!(
        !registry.has_pending_clock_echo(&advertised),
        "the successor projection must not retain stale extension state"
    );
    let state = registry.gossip_state.lock().await;
    let peer = state
        .peers
        .get(&advertised)
        .expect("successor peer projection must exist");
    assert_eq!(peer.node_id, Some(successor.to_node_id()));
    assert_eq!(
        peer.last_sequence, 0,
        "superseded FullSync sequence must not mutate the successor"
    );
    assert_eq!(
        state.full_sync_exchanges, 0,
        "superseded exchange must not be booked after displacement"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_sync_claim_displaced_during_merge_records_no_stale_projection() {
    run_full_sync_displaced_during_merge_is_dropped(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_sync_response_claim_displaced_during_merge_records_no_stale_projection() {
    run_full_sync_displaced_during_merge_is_dropped(true).await;
}
